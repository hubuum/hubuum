use std::sync::LazyLock;

use uuid::Uuid;

struct TokenHashKeyConfig {
    key: Vec<u8>,
    is_ephemeral: bool,
}

static TOKEN_HASH_KEY_CONFIG: LazyLock<TokenHashKeyConfig> = LazyLock::new(|| {
    if let Ok(env_key) = std::env::var("HUBUUM_TOKEN_HASH_KEY") {
        let trimmed = env_key.trim();
        if !trimmed.is_empty() {
            return TokenHashKeyConfig {
                key: trimmed.as_bytes().to_vec(),
                is_ephemeral: false,
            };
        }
    }

    let generated = format!("{}{}", Uuid::new_v4(), Uuid::new_v4());
    TokenHashKeyConfig {
        key: generated.into_bytes(),
        is_ephemeral: true,
    }
});

pub fn token_hash_key_bytes() -> &'static [u8] {
    &TOKEN_HASH_KEY_CONFIG.key
}

pub fn token_hash_key_is_ephemeral() -> bool {
    TOKEN_HASH_KEY_CONFIG.is_ephemeral
}
