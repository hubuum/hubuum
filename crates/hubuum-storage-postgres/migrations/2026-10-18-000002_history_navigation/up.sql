-- Diesel sends a migration as one batch, so two concurrent builds cannot share
-- this single migration. Bound the transactional build and run during maintenance.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE INDEX hubuumclass_history_navigation_idx ON hubuumclass_history (id, valid_from, history_id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumobject_history_navigation_idx ON hubuumobject_history (id, hubuum_class_id, valid_from, history_id); -- hubuum-compat: bounded-transactional-index
