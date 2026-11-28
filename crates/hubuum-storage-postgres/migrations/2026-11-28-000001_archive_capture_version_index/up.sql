SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '600s';

-- At most 1,024 durable captures can be retained at once.
CREATE INDEX history_archive_capture_version ON hubuum_control.history_archive_captures
    ((material #>> '{archive,identity,resource}'), (material #>> '{archive,identity,version}'))
    WHERE material #>> '{archive,identity,version}' IS NOT NULL; -- hubuum-compat: bounded-transactional-index
