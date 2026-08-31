SELECT pg_catalog.format('CREATE ROLE %I NOLOGIN', :'owner_role') \gexec
SELECT pg_catalog.format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'migrator_role',
    :'role_password'
) \gexec
SELECT pg_catalog.format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'runtime_role',
    :'role_password'
) \gexec
SELECT pg_catalog.format(
    'GRANT %I TO %I',
    :'owner_role',
    :'migrator_role'
) \gexec
