-- Your SQL goes here
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR NOT NULL,
    password VARCHAR NOT NULL,
    email VARCHAR NULL
);

CREATE TABLE groups (
    id SERIAL PRIMARY KEY,
    groupname VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE user_groups (
    user_id INT REFERENCES users (id),
    group_id INT REFERENCES groups (id),
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE tokens (
    token VARCHAR NOT NULL,
    user_id INT REFERENCES users (id),
    expires TIMESTAMP NOT NULL,
    PRIMARY KEY (token, user_id)
);

CREATE TABLE namespaces (
    id SERIAL PRIMARY KEY,
    namespace VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE namespacepermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id),
    group_id INT REFERENCES groups (id),
    user_id INT REFERENCES users (id),
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL,
    has_delegate BOOLEAN NOT NULL
);

CREATE TABLE objectpermissions (
    id SERIAL PRIMARY KEY,
    namespace_id INT REFERENCES namespaces (id),
    group_id INT REFERENCES groups (id),
    user_id INT REFERENCES users (id),
    has_create BOOLEAN NOT NULL,
    has_read BOOLEAN NOT NULL,
    has_update BOOLEAN NOT NULL,
    has_delete BOOLEAN NOT NULL
);

CREATE TABLE hubuumclass (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace_id INT REFERENCES namespaces (id),
    json_schema JSONB NOT NULL,
    validate_schema BOOLEAN NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE hubuumobject (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    namespace_id INT REFERENCES namespaces (id),
    hubuum_class_id INT REFERENCES hubuumclass (id),
    data JSONB NOT NULL,
    description VARCHAR NOT NULL
);