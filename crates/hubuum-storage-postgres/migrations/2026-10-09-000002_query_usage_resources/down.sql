-- Refuse to discard the only ownership evidence while native resources exist.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM query_usage_resources) THEN
        RAISE EXCEPTION 'Withdraw query usage declarations and run the query usage executor until cleanup completes before reverting this migration';
    END IF;
END $$;
DROP TRIGGER query_usage_pattern_withdrawal ON query_usage_declarations;
DROP TABLE query_usage_executor_cursor;
DROP TABLE query_usage_resource_owners;
DROP TABLE query_usage_resources;
DROP FUNCTION hubuum_withdraw_query_usage_resource();
