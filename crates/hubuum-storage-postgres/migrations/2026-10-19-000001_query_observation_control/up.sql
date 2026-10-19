-- Deployment configuration deliberately survives logical backup restores.
CREATE TABLE query_observation_control (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    enabled BOOLEAN
);

INSERT INTO query_observation_control DEFAULT VALUES;
