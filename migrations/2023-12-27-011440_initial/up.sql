-- Your SQL goes here
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,
    password VARCHAR NOT NULL,
    email VARCHAR NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    groupname VARCHAR NOT NULL UNIQUE,
    description VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE user_groups (
    user_id INT REFERENCES users (id) ON DELETE CASCADE NOT NULL,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    PRIMARY KEY (user_id, group_id),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
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
    description VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE namespacepermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    group_id INT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    has_create_object BOOLEAN NOT NULL,
    has_create_class BOOLEAN NOT NULL,
    has_read_namespace BOOLEAN NOT NULL,
    has_update_namespace BOOLEAN NOT NULL,
    has_delete_namespace BOOLEAN NOT NULL,
    has_delegate_namespace BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, group_id)
);

CREATE TABLE classpermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    group_id INT DEFAULT NULL REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    has_create_object BOOLEAN NOT NULL,
    has_read_class BOOLEAN NOT NULL,
    has_update_class BOOLEAN NOT NULL,
    has_delete_class BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, group_id)
);

CREATE TABLE objectpermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    group_id INT DEFAULT NULL REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    has_read_object BOOLEAN NOT NULL,
    has_update_object BOOLEAN NOT NULL,
    has_delete_object BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, group_id)
);


CREATE TABLE hubuumclass (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    json_schema JSONB DEFAULT '{}'::jsonb NOT NULL,
    validate_schema BOOLEAN NOT NULL,
    description VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE hubuumobject (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace_id INT REFERENCES namespaces (id) ON DELETE CASCADE NOT NULL,
    hubuum_class_id INT REFERENCES hubuumclass (id) ON DELETE CASCADE NOT NULL,
    data JSONB DEFAULT '{}'::jsonb NOT NULL,
    description VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (name, namespace_id)
);


----------------------
---- Indexes
----------------------

---- Users and groups
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_groups_groupname ON groups(groupname);
CREATE INDEX idx_user_groups_user_id ON user_groups(user_id);
CREATE INDEX idx_user_groups_group_id ON user_groups(group_id);

---- Namespaces and tokens
CREATE INDEX idx_namespaces_name ON namespaces(name);
CREATE INDEX idx_tokens_user_id ON tokens(user_id);

---- Classes and objects
CREATE INDEX idx_hubuumclass_namespace_id ON hubuumclass(namespace_id);
CREATE INDEX idx_hubuumobject_namespace_id ON hubuumobject(namespace_id);
CREATE INDEX idx_hubuumobject_hubuum_class_id ON hubuumobject(hubuum_class_id);

---- Permissions
CREATE INDEX idx_namespacepermissions_namespace_id ON namespacepermissions(namespace_id);
CREATE INDEX idx_namespacepermissions_group_id ON namespacepermissions(group_id);

CREATE INDEX idx_classpermissions_namespace_id ON classpermissions(namespace_id);
CREATE INDEX idx_classpermissions_group_id ON classpermissions(group_id);

CREATE INDEX idx_objectpermissions_namespace_id ON classpermissions(namespace_id);
CREATE INDEX idx_objectpermissions_group_id ON classpermissions(group_id);


----------------------
---- Triggers
----------------------

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_groups_updated_at
BEFORE UPDATE ON groups
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_user_groups_updated_at
BEFORE UPDATE ON user_groups
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_namespaces_updated_at
BEFORE UPDATE ON namespaces
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_namespacepermissions_updated_at
BEFORE UPDATE ON namespacepermissions
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_classpermissions_updated_at
BEFORE UPDATE ON classpermissions
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_objectpermissions_updated_at
BEFORE UPDATE ON objectpermissions
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_hubuumclass_updated_at
BEFORE UPDATE ON hubuumclass
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_hubuumobject_updated_at
BEFORE UPDATE ON hubuumobject
FOR EACH ROW EXECUTE FUNCTION update_modified_column();
