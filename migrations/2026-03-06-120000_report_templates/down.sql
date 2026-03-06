DROP TRIGGER IF EXISTS update_report_templates_updated_at ON report_templates;
DROP TABLE IF EXISTS report_templates;

ALTER TABLE permissions
    DROP COLUMN IF EXISTS has_read_template,
    DROP COLUMN IF EXISTS has_create_template,
    DROP COLUMN IF EXISTS has_update_template,
    DROP COLUMN IF EXISTS has_delete_template;
