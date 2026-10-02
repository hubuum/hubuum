DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM events WHERE entity_type = 'event_sink' AND action = 'invoked') THEN
        RAISE EXCEPTION 'Cannot roll back while event_sink.invoked audit events remain. Stop API and worker processes, archive these audit records, then remove them and their dependent deliveries before retrying rollback';
    END IF;
    IF EXISTS (SELECT 1 FROM event_sinks WHERE kind IN ('slack','mattermost') OR delivery_policy->>'min_interval_ms' IS NOT NULL)
       OR EXISTS (SELECT 1 FROM event_subscriptions WHERE collection_id IS NULL)
       OR EXISTS (SELECT 1 FROM event_deliveries WHERE purpose = 'test' OR deferred_reason IS NOT NULL) THEN
        RAISE EXCEPTION 'Remove chat sinks, system subscriptions, configured rate limits and test/deferred deliveries before rollback';
    END IF;
END $$;
DROP TABLE event_sink_delivery_state;
DROP INDEX event_deliveries_event_subscription_idx;
ALTER TABLE event_deliveries ADD CONSTRAINT event_deliveries_event_id_subscription_id_key UNIQUE(event_id,subscription_id);
ALTER TABLE event_deliveries DROP COLUMN deferred_reason, DROP COLUMN purpose;
DROP INDEX event_subscriptions_system_name_idx;
ALTER TABLE event_subscriptions ALTER COLUMN collection_id SET NOT NULL;
ALTER TABLE event_sinks DROP COLUMN delivery_policy;
ALTER TABLE event_sinks DROP CONSTRAINT event_sinks_kind_check;
ALTER TABLE event_sinks ADD CONSTRAINT event_sinks_kind_check CHECK (kind IN ('webhook','amqp','valkey_stream','email'));
