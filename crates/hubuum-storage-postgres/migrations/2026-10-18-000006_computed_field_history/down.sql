DROP TRIGGER computed_field_history_class ON hubuumclass;
DROP FUNCTION track_class_computed_field_history();
DROP TRIGGER computed_field_history_change ON computed_field_definitions;
DROP FUNCTION track_shared_computed_field_history();
DROP FUNCTION record_shared_computed_field_history(INTEGER);
DROP TABLE computed_field_history;
