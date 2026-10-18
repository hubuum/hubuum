CREATE INDEX CONCURRENTLY hubuumobject_history_class_name_idx
    ON hubuumobject_history (hubuum_class_id, name, history_id);
