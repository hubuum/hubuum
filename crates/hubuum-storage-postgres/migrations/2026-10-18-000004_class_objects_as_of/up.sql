-- Diesel executes a migration as one batch; bound both builds in one transaction.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE INDEX hubuumobject_history_population_order_idx
ON hubuumobject_history (hubuum_class_id, history_id)
INCLUDE (valid_from, valid_to) WHERE op <> 'D'; -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_history_population_validity_idx
ON hubuumobject_history USING gist (tstzrange(valid_from, valid_to, '[)'))
WHERE op <> 'D'; -- hubuum-compat: bounded-transactional-index
