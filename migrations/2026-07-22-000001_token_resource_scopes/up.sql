ALTER TABLE tokens RENAME COLUMN scoped TO permission_scoped;
ALTER TABLE tokens
    ADD COLUMN resource_scoped BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE token_collection_scopes (
    token_id INT REFERENCES tokens (id) ON DELETE CASCADE NOT NULL,
    collection_id INT REFERENCES collections (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (token_id, collection_id)
);

CREATE TABLE token_class_scopes (
    token_id INT REFERENCES tokens (id) ON DELETE CASCADE NOT NULL,
    class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (token_id, class_id)
);

CREATE TABLE token_object_scopes (
    token_id INT REFERENCES tokens (id) ON DELETE CASCADE NOT NULL,
    object_id INT REFERENCES hubuumobject (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (token_id, object_id)
);

CREATE INDEX idx_token_collection_scopes_collection_id
    ON token_collection_scopes(collection_id);
CREATE INDEX idx_token_class_scopes_class_id
    ON token_class_scopes(class_id);
CREATE INDEX idx_token_object_scopes_object_id
    ON token_object_scopes(object_id);
