-- Your SQL goes here
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,
    password VARCHAR NOT NULL,
    email VARCHAR NULL
);

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    groupname VARCHAR NOT NULL UNIQUE,
    description VARCHAR NOT NULL
);

CREATE TABLE user_groups (
    user_id INT REFERENCES users (id) ON DELETE CASCADE,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE tokens (
    token VARCHAR NOT NULL,
    user_id INT REFERENCES users (id) ON DELETE CASCADE,
    issued TIMESTAMP NOT NULL,
    PRIMARY KEY (token, user_id)
);

CREATE TABLE namespaces (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    description VARCHAR NOT NULL
);

CREATE TABLE namespacepermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE,
    user_id INT REFERENCES users (id) ON DELETE CASCADE,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    has_delegate BOOLEAN NOT NULL,
    UNIQUE (namespace_id, group_id, user_id)
);

CREATE TABLE objectpermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE,
    user_id INT REFERENCES users (id) ON DELETE CASCADE,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    UNIQUE (namespace_id, group_id, user_id)
);

CREATE TABLE hubuumclass (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE,
    json_schema JSONB NOT NULL,
    validate_schema BOOLEAN NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE hubuumobject (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE,
    hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE,
    data JSONB NOT NULL,
    description VARCHAR NOT NULL,
    UNIQUE (name, namespace_id)
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_groups_groupname ON groups(groupname);
CREATE INDEX idx_namespaces_name ON namespaces(name);
CREATE INDEX idx_user_groups_user_id ON user_groups(user_id);
CREATE INDEX idx_user_groups_group_id ON user_groups(group_id);
CREATE INDEX idx_tokens_user_id ON tokens(user_id);
CREATE INDEX idx_namespacepermissions_namespace_id ON namespacepermissions(namespace_id);
CREATE INDEX idx_namespacepermissions_group_id ON namespacepermissions(group_id);
CREATE INDEX idx_namespacepermissions_user_id ON namespacepermissions(user_id);
CREATE INDEX idx_objectpermissions_namespace_id ON objectpermissions(namespace_id);
CREATE INDEX idx_objectpermissions_group_id ON objectpermissions(group_id);
CREATE INDEX idx_objectpermissions_user_id ON objectpermissions(user_id);
CREATE INDEX idx_hubuumclass_namespace_id ON hubuumclass(namespace_id);
CREATE INDEX idx_hubuumobject_namespace_id ON hubuumobject(namespace_id);
CREATE INDEX idx_hubuumobject_hubuum_class_id ON hubuumobject(hubuum_class_id);

