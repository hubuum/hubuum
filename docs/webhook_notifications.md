# Set Up Chat And Notification Webhooks

Use Hubuum's `webhook` sink to send selected events directly to Slack,
Mattermost, or Discord, or through a notification bridge such as Apprise.
The destination controls its message format; Hubuum supplies a JSON template,
secret lookup, delivery tracking, retries, and optional pacing.

| Destination | Start here | Payload | Successful acknowledgement |
| --- | --- | --- | --- |
| Slack | [Create a Slack webhook](#slack) | `text` | HTTP 200 with `ok` |
| Mattermost | [Create a Mattermost webhook](#mattermost) | `text` | HTTP 200 with `ok` |
| Discord | [Discord recipe](#discord) | `content` | HTTP 200 with `wait=true` |
| Apprise API | [Bridge recipe](#apprise-and-other-bridges) | `title` and `body` | Match the deployed API; the recipe uses HTTP 200 |

These recipes use incoming webhooks. Bot installation, channel discovery,
interactive actions, and OAuth token refresh belong in an integration service.
For payload fields, response rules, and upgrade requirements, see the
[webhook reference](events.md#configurable-webhook-notifications).

## Prerequisites

Use an [administrator token](auth_model.md) to create sinks and send tests.
Collection subscriptions require `ManageEventSubscription` on that collection;
system subscriptions require an unscoped administrator.

Enable delivery in the environment of each `hubuum-server` process that should
run workers, then restart those processes:

```bash
HUBUUM_EVENT_FANOUT_WORKERS=1
HUBUUM_EVENT_DELIVERY_WORKERS=1
```

Fan-out is enabled by default, but delivery workers default to zero. Setting
these variables in an API client's terminal does not configure a running server.
Ensure every delivery-worker process can resolve the same secret aliases.

Destinations must use HTTPS with a valid, trusted certificate. Hubuum refuses
redirects. For a Mattermost server or bridge on a private network, set
`HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS=true` on the worker processes and
restart them. This setting also permits private destinations for other outbound
remote calls; use it only where that deployment-wide access is intended.
Use the service's DNS name and final HTTPS endpoint, not a login page or an
HTTP URL that redirects to HTTPS.

## Slack

1. Create or open an app in [Slack's app dashboard](https://api.slack.com/apps)
   for the destination workspace.
2. Enable **Incoming Webhooks**, then select **Add New Webhook to Workspace**.
3. Select the channel and authorize the installation. Join a private channel
   before selecting it; follow any workspace approval requirements.
4. Copy the generated URL, usually beginning
   `https://hooks.slack.com/services/`. Store the entire URL as described below.

Slack chooses the channel, sender name, and icon from the webhook/app settings;
a payload cannot override them. Create another webhook and Hubuum sink for a
different channel. See [Slack's incoming webhook guide](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).

The shared recipe below uses one-second spacing, consistent with Slack's
documented incoming-webhook rate. Other senders to the same channel still count
toward Slack's limits. HTTP 429 triggers a cooldown using `Retry-After`.
See [Slack's rate limits](https://docs.slack.dev/apis/web-api/rate-limits/).

## Mattermost

1. Open **Product Menu > Integrations > Incoming Webhooks** and add a webhook.
   If the option is unavailable, an administrator must enable incoming webhooks
   or grant access under **System Console > Integrations > Integration Management**.
2. Choose a name and destination channel. Select **Lock to this channel** for
   a fixed destination, then save.
3. Copy the complete generated URL, such as
   `https://mattermost.example.com/hooks/GENERATED_KEY`.

The shared `text` recipe below works with Mattermost's incoming-webhook API.
Unlocked webhooks may accept a `channel` field in the rendered JSON, subject to
server policy and the creator's access. Username and icon overrides also depend
on server settings. These are payload fields, not Hubuum routing settings.
One-second pacing is a starting choice here; adjust it to your server's policy.
See [Mattermost's incoming webhook guide](https://docs.mattermost.com/integrations-guide/incoming-webhooks).

## Store The Destination Secret

For either service, use the alias `ops_chat_webhook`. Its value is the complete
webhook URL, including its secret path. Choose the mapping for your existing
[secret source](secret_sources.md):

| Secret source | Where to put the complete webhook URL |
| --- | --- |
| Environment | `HUBUUM_EVENT_SINK_SECRET_OPS_CHAT_WEBHOOK` in each delivery worker's environment |
| Mounted files | `event-sink/ops_chat_webhook` relative to `HUBUUM_SECRET_FILE_ROOT` |

For example, with the default environment source, configure the worker's
environment using the URL from the chosen service:

```bash
HUBUUM_EVENT_SINK_SECRET_OPS_CHAT_WEBHOOK='https://hooks.slack.com/services/REPLACE/WITH/REAL_VALUE'
```

Restart workers after changing environment-backed values. For file-backed
secrets, mount a readable file containing only the URL, without a trailing
newline, and use the file-source settings from the secret-source guide.
To connect both services, use distinct aliases and create a sink for each.

Keep the URL out of subscription JSON and version control.
`config.url_secret_ref` selects the URL alias. The separate top-level
`secret_ref` supplies an HTTP bearer token and is unnecessary for Slack,
Mattermost, and Discord incoming webhooks.

## Create A Sink

Save the following as `chat-sink.json`. It works for Slack and Mattermost:

```json
{
  "name": "ops-chat",
  "kind": "webhook",
  "config": {
    "url_secret_ref": "ops_chat_webhook",
    "body_template": "{\"text\": {{ (test_marker ~ 'Hubuum: ' ~ summary) | tojson }}}",
    "response": {
      "success_statuses": [200],
      "rate_limit": true,
      "retry_statuses": [408, 500, 502, 503, 504],
      "body": { "kind": "text_equals", "value": "ok" }
    }
  },
  "delivery_policy": { "min_interval_ms": 1000 },
  "enabled": true
}
```

With `HUBUUM_TOKEN` containing your administrator token, create the sink:

```bash
export HUBUUM_URL='https://hubuum.example.com'
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @chat-sink.json \
  "$HUBUUM_URL/api/v1/event-sinks"
```

Record the response's numeric `id`. The following examples use `3`; replace
it with your sink ID. For several subscriptions going to the same destination,
reuse this sink so they share its pacing. Separate sinks have separate schedules.

The template uses `tojson` to escape dynamic JSON values and displays `[TEST]`
for test deliveries. Rich messages can add Slack blocks or Mattermost fields
inside the JSON template. Follow the destination's payload and length limits;
Hubuum checks valid JSON and rendering budgets, not each provider's schema.

The response policy requires HTTP 200 and the trimmed body `ok`. It retries the
listed transient statuses, defers HTTP 429 without spending a failure attempt,
and makes other HTTP failures permanent. This avoids repeatedly sending an
invalid payload or using a revoked webhook URL. Transport errors still retry.

## Subscribe To Events

For object changes, save this as `chat-subscription.json`:

```json
{
  "sink_id": 3,
  "name": "object-changes-to-chat",
  "description": "Notify operations of object changes in this collection",
  "entity_types": ["object"],
  "actions": ["created", "updated"],
  "routing": {},
  "enabled": true
}
```

Replace `12` with a collection you manage:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @chat-subscription.json \
  "$HUBUUM_URL/api/v1/collections/12/event-subscriptions"
```

Record the subscription's `id`; the test below uses `7`. Leave `routing` empty
when `url_secret_ref` supplies the destination. A `routing.url` override is
rejected. New matching events fan out only while both sink and subscription
are enabled; creating a subscription does not replay the audit history.

For failed backups, create a separate **system** subscription by posting this
body to `POST /api/v1/system-event-subscriptions`:

```json
{
  "sink_id": 3,
  "name": "failed-backups-to-chat",
  "description": "Notify operations when a backup task fails",
  "entity_types": ["task"],
  "actions": ["failed"],
  "filter": { "task_kinds": ["backup"] },
  "routing": {},
  "enabled": true
}
```

System subscriptions match events with neither a direct nor a related
collection. Use [Prometheus alerts](https://github.com/hubuum/hubuum/blob/main/observability/README.md) for thresholds
such as queue age or database pressure; event subscriptions match emitted facts.

## Preview, Test, And Check Delivery

For the collection example, find a saved event in that collection:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  "$HUBUUM_URL/api/v1/events?collection_id=12&entity_type=object&sort=-occurred_at&limit=1"
```

If none exists, create or update an object there and query again. Copy the
event's UUID `event_id`, not its integer `id`. For a system subscription,
select a collection-less event instead, such as an existing backup task event.

Save `chat-test.json`, replacing both example values with saved records:

```json
{
  "subscription_id": 7,
  "event_id": "00000000-0000-4000-8000-000000000001"
}
```

Preview the rendered payload:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @chat-test.json \
  "$HUBUUM_URL/api/v1/event-sinks/3/preview"
```

Preview performs no secret lookup or network request, so success verifies the
template and event scope but not connectivity or credentials. Check the JSON
and `[TEST]` label, then queue a real message:

```bash
curl --fail-with-body --silent --show-error --include \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-binary @chat-test.json \
  "$HUBUUM_URL/api/v1/event-sinks/3/test"
```

HTTP `202 Accepted` means queued, not delivered. The response headers include a
`Location` such as `/api/v1/event-deliveries/42`. Use its actual path to inspect
the delivery:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  "$HUBUUM_URL/api/v1/event-deliveries/42"
```

Check until the delivery is `succeeded`, `failed`, or `dead`. A `failed` delivery
may still be retryable.

Tests bypass subscription filters and enabled flags, while retaining scope
checks and pacing. They do not require a new failed backup. Verify normal
delivery separately by making a matching change after enabling the subscription.
See [delivery semantics](events.md#delivery-semantics) for inspection and retry
endpoints. Sink/subscription updates use the current ETag with `If-Match`.

## Discord

Create a webhook for a regular text channel through **Server Settings >
Integrations > Webhooks** and copy its URL. See
[Discord's setup guide](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks).
Store the complete URL under `ops_discord_webhook`, appending `?wait=true`
(`&wait=true` if it already has a query). Waiting makes Discord confirm message
creation before responding. Use this sink with the subscription/test steps above:

```json
{
  "name": "ops-discord",
  "kind": "webhook",
  "config": {
    "url_secret_ref": "ops_discord_webhook",
    "body_template": "{\"content\": {{ (test_marker ~ 'Hubuum: ' ~ summary)[:1900] | tojson }}, \"allowed_mentions\": {\"parse\": []}}",
    "response": {
      "success_statuses": [200],
      "rate_limit": true,
      "retry_statuses": [408, 500, 502, 503, 504]
    }
  },
  "delivery_policy": { "min_interval_ms": 1000 },
  "enabled": true
}
```

The template limits content below Discord's 2,000-character maximum and disables
automatic mentions. Discord returns a message object, so omit the Slack/Mattermost
`text_equals` rule. Forum and media channels need additional thread settings;
see the [execute-webhook contract](https://docs.discord.com/developers/resources/webhook#execute-webhook).

## Apprise And Other Bridges

An external bridge can manage multiple chat and notification services while
Hubuum maintains one webhook contract. With Apprise API, save destinations in a
stateful configuration, then use its `/notify/{KEY}` endpoint. The example below
assumes `https://notify.example.com/notify/hubuum` is exposed through an HTTPS
gateway accepting a bearer token; the gateway handles the backend's required
authentication. Store that full URL as `ops_apprise_url` and the gateway token
as `apprise_gateway_token` in Hubuum's secret source.

```json
{
  "name": "ops-notification-bridge",
  "kind": "webhook",
  "config": {
    "url_secret_ref": "ops_apprise_url",
    "body_template": "{\"title\": \"Hubuum\", \"body\": {{ (test_marker ~ summary) | tojson }}}",
    "response": {
      "success_statuses": [200],
      "rate_limit": true,
      "retry_statuses": [408, 500, 502, 503, 504]
    }
  },
  "secret_ref": "apprise_gateway_token",
  "enabled": true
}
```

Apprise's own authentication is deployment-dependent; Hubuum's `secret_ref`
always sends **Bearer**, not Basic authentication. Do not embed credentials in
the URL. Match the response policy to your deployed bridge's synchronous or
queued acknowledgement contract; acceptance by a bridge may not prove delivery
to every downstream service. See the [Apprise API configuration and notification reference](https://github.com/caronc/apprise-api#stateful-solution).

Other services fit directly when they accept HTTPS JSON `POST`, can authenticate
using a secret URL or bearer token, and have an acknowledgement expressible with
Hubuum's [response rules](events.md#response-and-delivery-policies). Use a bridge
for token refresh, signing, form uploads, or provider-specific stateful workflows.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Preview works, test fails | Preview does not check secrets or networking. Check the worker's secret source, full URL, DNS, HTTPS certificate, and private-target policy. |
| Test remains pending | Confirm delivery workers are running; inspect `deferred_reason` and `next_attempt_at` for configured pacing or HTTP cooldowns. |
| No delivery row for a new event | Confirm fan-out workers, both enabled flags, scope, entity/action selection, and filters. Inspect `/api/v1/event-deliveries/health`. |
| Delivery becomes dead immediately | Check for a revoked URL, wrong channel access, invalid payload, provider size limit, or acknowledgement mismatch. This recipe treats unlisted HTTP errors as permanent. |
| Repeated cooldowns | Reduce traffic or increase `min_interval_ms`. Several sinks or applications can share one provider limit. |
| Duplicate messages | Delivery is at least once. A lost acknowledgement can cause another post; chat services need not deduplicate Hubuum's event or idempotency headers. |

Delivery errors omit secret URLs and provider response bodies. Use the provider
or bridge's own diagnostics when a sanitized failure needs more detail. After
fixing the cause, explicitly retry a dead delivery through the administrator
retry endpoint. Adding `event_id` to the message can help identify duplicates.
