ALTER TABLE permissions
    ADD COLUMN has_read_template BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_create_template BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_update_template BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN has_delete_template BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE permissions
SET
    has_read_template = TRUE,
    has_create_template = TRUE,
    has_update_template = TRUE,
    has_delete_template = TRUE
WHERE has_delegate_namespace = TRUE;

CREATE TABLE report_templates (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    content_type VARCHAR NOT NULL,
    template TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name),
    CHECK (content_type IN ('text/plain', 'text/html', 'text/csv'))
);

CREATE INDEX idx_report_templates_namespace_id ON report_templates(namespace_id);

DROP TRIGGER IF EXISTS update_report_templates_updated_at ON report_templates;
CREATE TRIGGER update_report_templates_updated_at
BEFORE UPDATE ON report_templates
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
