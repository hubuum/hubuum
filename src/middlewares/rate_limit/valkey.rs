use std::time::Duration;

use redis::{Client, Connection, FromRedisValue};

use super::{
    LoginAttemptOutcome, LoginAttemptPermit, LoginRateLimitConfig, LoginRateLimitStore,
    ScopeSnapshot,
};
use crate::errors::ApiError;

const BEGIN_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local prefix = ARGV[1]
local window = tonumber(ARGV[2])
local reservation_ttl = tonumber(ARGV[3])
local state_ttl = tonumber(ARGV[4])
local reservation = ARGV[5]
local max_keys = tonumber(ARGV[6])
local scope_count = tonumber(ARGV[7])
local index = prefix .. ':{login-rate-limit}:index'

local function keys(raw)
  local base = prefix .. ':{login-rate-limit}:scope:' .. raw
  return base .. ':attempts', base .. ':inflight', base .. ':state'
end

for i = 0, scope_count - 1 do
  local raw = ARGV[8 + (i * 2)]
  local threshold = tonumber(ARGV[9 + (i * 2)])
  local attempts, inflight, state = keys(raw)
  redis.call('ZREMRANGEBYSCORE', attempts, '-inf', now - window)
  redis.call('ZREMRANGEBYSCORE', inflight, '-inf', now - reservation_ttl)
  local locked_until = tonumber(redis.call('HGET', state, 'locked_until') or '0')
  if locked_until > 0 and now >= locked_until and now - locked_until >= window then
    redis.call('HDEL', state, 'locked_until', 'level')
    locked_until = 0
  end
  if locked_until > now then
    return 0
  end
  if redis.call('ZCARD', attempts) + redis.call('ZCARD', inflight) >= threshold then
    return 0
  end
end

redis.call('ZREMRANGEBYSCORE', index, '-inf', now - state_ttl)
for i = 0, scope_count - 1 do
  local raw = ARGV[8 + (i * 2)]
  if not redis.call('ZSCORE', index, raw) then
    while redis.call('ZCARD', index) >= max_keys do
      local evicted = redis.call('ZPOPMIN', index, 1)
      if #evicted == 0 then
        return 0
      end
      local attempts, inflight, state = keys(evicted[1])
      redis.call('DEL', attempts, inflight, state)
    end
  end
  local attempts, inflight, state = keys(raw)
  redis.call('ZADD', inflight, now, reservation)
  redis.call('ZADD', index, now, raw)
  redis.call('PEXPIRE', attempts, state_ttl)
  redis.call('PEXPIRE', inflight, state_ttl)
  redis.call('PEXPIRE', state, state_ttl)
end
redis.call('PEXPIRE', index, state_ttl)
return 1
"#;

const FINISH_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local prefix = ARGV[1]
local outcome = ARGV[2]
local window = tonumber(ARGV[3])
local backoff_base = tonumber(ARGV[4])
local backoff_max = tonumber(ARGV[5])
local state_ttl = tonumber(ARGV[6])
local reservation = ARGV[7]
local scope_count = tonumber(ARGV[8])
local user_key = ARGV[9 + (scope_count * 2)]
local index = prefix .. ':{login-rate-limit}:index'
local lockouts = {}

local function keys(raw)
  local base = prefix .. ':{login-rate-limit}:scope:' .. raw
  return base .. ':attempts', base .. ':inflight', base .. ':state'
end

for i = 0, scope_count - 1 do
  local raw = ARGV[9 + (i * 2)]
  local threshold = tonumber(ARGV[10 + (i * 2)])
  local attempts, inflight, state = keys(raw)
  redis.call('ZREM', inflight, reservation)
  redis.call('ZREMRANGEBYSCORE', attempts, '-inf', now - window)
  if outcome == 'failed' then
    redis.call('ZADD', attempts, now, reservation)
    if redis.call('ZCARD', attempts) >= threshold then
      local level = tonumber(redis.call('HGET', state, 'level') or '0') + 1
      local exponent = math.min(level - 1, 62)
      local duration = math.min(backoff_base * (2 ^ exponent), backoff_max)
      local locked_until = now + duration
      redis.call('HSET', state, 'level', level, 'locked_until', locked_until)
      redis.call('DEL', attempts)
      redis.call('ZADD', index, locked_until, raw)
      table.insert(lockouts, raw)
    else
      redis.call('ZADD', index, now, raw)
    end
  else
    redis.call('ZADD', index, now, raw)
  end
  redis.call('PEXPIRE', attempts, state_ttl)
  redis.call('PEXPIRE', inflight, state_ttl)
  redis.call('PEXPIRE', state, state_ttl)
end

if outcome == 'succeeded' then
  local attempts, inflight, state = keys(user_key)
  redis.call('DEL', attempts, state)
  if redis.call('ZCARD', inflight) == 0 then
    redis.call('DEL', inflight)
    redis.call('ZREM', index, user_key)
  else
    redis.call('ZADD', index, now, user_key)
  end
end
redis.call('PEXPIRE', index, state_ttl)
return lockouts
"#;

const SNAPSHOT_SCRIPT: &str = r#"
local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local prefix = ARGV[1]
local window = tonumber(ARGV[2])
local reservation_ttl = tonumber(ARGV[3])
local index = prefix .. ':{login-rate-limit}:index'
local result = {}

local function keys(raw)
  local base = prefix .. ':{login-rate-limit}:scope:' .. raw
  return base .. ':attempts', base .. ':inflight', base .. ':state'
end

for _, raw in ipairs(redis.call('ZRANGE', index, 0, -1)) do
  local attempts, inflight, state = keys(raw)
  redis.call('ZREMRANGEBYSCORE', attempts, '-inf', now - window)
  redis.call('ZREMRANGEBYSCORE', inflight, '-inf', now - reservation_ttl)
  local locked_until = tonumber(redis.call('HGET', state, 'locked_until') or '0')
  local level = tonumber(redis.call('HGET', state, 'level') or '0')
  if locked_until > 0 and now >= locked_until and now - locked_until >= window then
    redis.call('HDEL', state, 'locked_until', 'level')
    locked_until = 0
    level = 0
  end
  local attempt_count = redis.call('ZCARD', attempts)
  local inflight_count = redis.call('ZCARD', inflight)
  local cooling = locked_until > 0 and now - locked_until < window
  if attempt_count > 0 or inflight_count > 0 or locked_until > now or cooling then
    table.insert(result, raw)
    table.insert(result, tostring(attempt_count))
    table.insert(result, locked_until > now and '1' or '0')
    table.insert(result, tostring(math.max(locked_until - now, 0)))
    table.insert(result, tostring(level))
  else
    redis.call('ZREM', index, raw)
    redis.call('DEL', attempts, inflight, state)
  end
end
return result
"#;

const RELEASE_SCRIPT: &str = r#"
local prefix = ARGV[1]
local raw = ARGV[2]
local index = prefix .. ':{login-rate-limit}:index'
local base = prefix .. ':{login-rate-limit}:scope:' .. raw
local existed = redis.call('ZREM', index, raw)
redis.call('DEL', base .. ':attempts', base .. ':inflight', base .. ':state')
return existed
"#;

const CLEAR_SCRIPT: &str = r#"
local prefix = ARGV[1]
local index = prefix .. ':{login-rate-limit}:index'
local members = redis.call('ZRANGE', index, 0, -1)
for _, raw in ipairs(members) do
  local base = prefix .. ':{login-rate-limit}:scope:' .. raw
  redis.call('DEL', base .. ':attempts', base .. ':inflight', base .. ':state')
end
redis.call('DEL', index)
return #members
"#;

pub(super) struct ValkeyLoginRateLimitStore {
    client: Client,
    prefix: String,
    io_timeout: Duration,
}

impl ValkeyLoginRateLimitStore {
    pub(super) async fn connect(
        url: String,
        prefix: String,
        io_timeout: Duration,
    ) -> Result<Self, ApiError> {
        let client = Client::open(url).map_err(|error| {
            ApiError::BadRequest(format!("Invalid login rate-limit Valkey URL: {error}"))
        })?;
        let store = Self {
            client,
            prefix,
            io_timeout,
        };
        Ok(store)
    }

    async fn run<T, F>(&self, operation: &'static str, query: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> redis::RedisResult<T> + Send + 'static,
    {
        let client = self.client.clone();
        let io_timeout = self.io_timeout;
        tokio::task::spawn_blocking(move || {
            let mut connection = client.get_connection_with_timeout(io_timeout)?;
            connection.set_read_timeout(Some(io_timeout))?;
            connection.set_write_timeout(Some(io_timeout))?;
            query(&mut connection)
        })
        .await
        .map_err(|error| {
            ApiError::ServiceUnavailable(format!(
                "Login rate-limit Valkey {operation} task failed: {error}"
            ))
        })?
        .map_err(|error| {
            ApiError::ServiceUnavailable(format!(
                "Login rate-limit Valkey {operation} failed: {error}"
            ))
        })
    }

    fn window_ms(config: &LoginRateLimitConfig) -> u64 {
        config.window_seconds.saturating_mul(1_000)
    }

    fn reservation_ttl_ms(config: &LoginRateLimitConfig) -> u64 {
        Self::window_ms(config).clamp(5_000, 60_000)
    }

    fn state_ttl_ms(config: &LoginRateLimitConfig) -> u64 {
        config
            .backoff_max_seconds
            .saturating_add(config.window_seconds)
            .saturating_add(60)
            .saturating_mul(1_000)
    }

    async fn eval<T>(
        &self,
        operation: &'static str,
        script: &'static str,
        arguments: Vec<String>,
    ) -> Result<T, ApiError>
    where
        T: FromRedisValue + Send + 'static,
    {
        self.run(operation, move |connection| {
            let mut command = redis::cmd("EVAL");
            command.arg(script).arg(0);
            for argument in arguments {
                command.arg(argument);
            }
            command.query(connection)
        })
        .await
    }
}

impl LoginRateLimitStore for ValkeyLoginRateLimitStore {
    async fn begin(
        &self,
        permit: &LoginAttemptPermit,
        config: &LoginRateLimitConfig,
    ) -> Result<bool, ApiError> {
        let mut arguments = vec![
            self.prefix.clone(),
            Self::window_ms(config).to_string(),
            Self::reservation_ttl_ms(config).to_string(),
            Self::state_ttl_ms(config).to_string(),
            permit.reservation_id.to_string(),
            super::MAX_LOGIN_ATTEMPT_KEYS.to_string(),
            permit.scopes.len().to_string(),
        ];
        for (key, threshold) in &permit.scopes {
            arguments.push(key.clone());
            arguments.push(threshold.to_string());
        }
        self.eval::<i64>("begin", BEGIN_SCRIPT, arguments)
            .await
            .map(|available| available == 1)
    }

    async fn finish(
        &self,
        permit: &LoginAttemptPermit,
        outcome: LoginAttemptOutcome,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<String>, ApiError> {
        let outcome = match outcome {
            LoginAttemptOutcome::Succeeded => "succeeded",
            LoginAttemptOutcome::Failed => "failed",
            LoginAttemptOutcome::Aborted => "aborted",
        };
        let mut arguments = vec![
            self.prefix.clone(),
            outcome.to_string(),
            Self::window_ms(config).to_string(),
            config
                .backoff_base_seconds
                .saturating_mul(1_000)
                .to_string(),
            config.backoff_max_seconds.saturating_mul(1_000).to_string(),
            Self::state_ttl_ms(config).to_string(),
            permit.reservation_id.to_string(),
            permit.scopes.len().to_string(),
        ];
        for (key, threshold) in &permit.scopes {
            arguments.push(key.clone());
            arguments.push(threshold.to_string());
        }
        arguments.push(permit.user_ip_key.clone());
        self.eval("finish", FINISH_SCRIPT, arguments).await
    }

    async fn snapshot(
        &self,
        config: &LoginRateLimitConfig,
    ) -> Result<Vec<ScopeSnapshot>, ApiError> {
        let values: Vec<String> = self
            .eval(
                "snapshot",
                SNAPSHOT_SCRIPT,
                vec![
                    self.prefix.clone(),
                    Self::window_ms(config).to_string(),
                    Self::reservation_ttl_ms(config).to_string(),
                ],
            )
            .await?;
        values
            .chunks_exact(5)
            .map(|entry| {
                let attempts = entry[1].parse::<usize>().map_err(|error| {
                    ApiError::InternalServerError(format!(
                        "Invalid attempt count returned by login rate-limit Valkey script: {error}"
                    ))
                })?;
                let locked_for_ms = entry[3].parse::<u64>().map_err(|error| {
                    ApiError::InternalServerError(format!(
                        "Invalid lock duration returned by login rate-limit Valkey script: {error}"
                    ))
                })?;
                let lockout_level = entry[4].parse::<u32>().map_err(|error| {
                    ApiError::InternalServerError(format!(
                        "Invalid lockout level returned by login rate-limit Valkey script: {error}"
                    ))
                })?;
                Ok(ScopeSnapshot {
                    key: entry[0].clone(),
                    attempts,
                    locked: entry[2] == "1",
                    locked_for: (locked_for_ms > 0).then(|| Duration::from_millis(locked_for_ms)),
                    lockout_level,
                })
            })
            .collect()
    }

    async fn release_entry(&self, key: &str) -> Result<bool, ApiError> {
        self.eval::<i64>(
            "release",
            RELEASE_SCRIPT,
            vec![self.prefix.clone(), key.to_string()],
        )
        .await
        .map(|removed| removed == 1)
    }

    async fn clear_all(&self) -> Result<usize, ApiError> {
        let removed = self
            .eval::<i64>("clear", CLEAR_SCRIPT, vec![self.prefix.clone()])
            .await?;
        usize::try_from(removed).map_err(|error| {
            ApiError::InternalServerError(format!(
                "Invalid clear count returned by login rate-limit Valkey script: {error}"
            ))
        })
    }
}
