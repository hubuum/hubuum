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
    user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE tokens (
    token VARCHAR NOT NULL,
    user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    issued TIMESTAMP NOT NULL,
    PRIMARY KEY (token, user_id)
);

CREATE TABLE namespaces (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    description VARCHAR NOT NULL
);

CREATE TABLE user_namespacepermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    has_delegate BOOLEAN NOT NULL,
    UNIQUE (namespace_id, user_id)
);

CREATE TABLE group_namespacepermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    has_delegate BOOLEAN NOT NULL,
    UNIQUE (namespace_id, group_id)
);

CREATE TABLE user_datapermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    user_id INT DEFAULT NULL REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    UNIQUE (namespace_id, user_id)
);

CREATE TABLE group_datapermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    group_id INT DEFAULT NULL REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    UNIQUE (namespace_id, group_id)
);

CREATE TABLE hubuumclass (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    json_schema JSONB NOT NULL,
    validate_schema BOOLEAN NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE hubuumobject (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
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

CREATE INDEX idx_user_namespacepermissions_namespace_id ON user_namespacepermissions(namespace_id);
CREATE INDEX idx_user_namespacepermissions_user_id ON user_namespacepermissions(user_id);

CREATE INDEX idx_user_datapermissions_namespace_id ON user_datapermissions(namespace_id);
CREATE INDEX idx_user_datapermissions_user_id ON user_datapermissions(user_id);

CREATE INDEX idx_group_namespacepermissions_namespace_id ON group_namespacepermissions(namespace_id);
CREATE INDEX idx_group_namespacepermissions_group_id ON group_namespacepermissions(group_id);

CREATE INDEX idx_group_datapermissions_namespace_id ON group_datapermissions(namespace_id);
CREATE INDEX idx_group_datapermissions_group_id ON group_datapermissions(group_id);

CREATE INDEX idx_hubuumclass_namespace_id ON hubuumclass(namespace_id);
CREATE INDEX idx_hubuumobject_namespace_id ON hubuumobject(namespace_id);
CREATE INDEX idx_hubuumobject_hubuum_class_id ON hubuumobject(hubuum_class_id);

