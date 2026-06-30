ALTER TABLE permissions
    ADD COLUMN has_manage_event_subscription BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE event_sinks (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    kind VARCHAR NOT NULL CHECK (kind IN ('webhook', 'amqp', 'valkey_stream', 'email')),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(config) = 'object'),
    CHECK (secret_ref IS NULL OR length(trim(secret_ref)) > 0)
);

CREATE TABLE event_subscriptions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    sink_id INT REFERENCES event_sinks (id) ON DELETE CASCADE NOT NULL,
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    entity_types JSONB NOT NULL,
    actions JSONB NOT NULL,
    routing JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name),
    CHECK (jsonb_typeof(entity_types) = 'array'),
    CHECK (jsonb_array_length(entity_types) > 0),
    CHECK (jsonb_typeof(actions) = 'array'),
    CHECK (jsonb_array_length(actions) > 0),
    CHECK (jsonb_typeof(routing) = 'object')
);

CREATE INDEX idx_event_sinks_enabled ON event_sinks(enabled);
CREATE INDEX idx_event_subscriptions_namespace_id ON event_subscriptions(namespace_id);
CREATE INDEX idx_event_subscriptions_sink_id ON event_subscriptions(sink_id);
CREATE INDEX idx_event_subscriptions_enabled ON event_subscriptions(enabled);

CREATE TRIGGER update_event_sinks_updated_at
BEFORE UPDATE ON event_sinks
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_event_subscriptions_updated_at
BEFORE UPDATE ON event_subscriptions
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();
