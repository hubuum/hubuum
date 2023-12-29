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
