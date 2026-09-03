UPDATE hubuumclass
SET validate_schema = FALSE
WHERE validate_schema
  AND json_schema IS NULL;

UPDATE hubuumclass_history
SET validate_schema = FALSE
WHERE validate_schema
  AND json_schema IS NULL;

ALTER TABLE hubuumclass
ADD CONSTRAINT hubuumclass_enforced_schema_present
CHECK (NOT validate_schema OR json_schema IS NOT NULL);

ALTER TABLE hubuumclass_history
ADD CONSTRAINT hubuumclass_history_enforced_schema_present
CHECK (NOT validate_schema OR json_schema IS NOT NULL);
