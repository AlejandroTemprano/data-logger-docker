CREATE ROLE data_logger WITH LOGIN PASSWORD '1234';
GRANT CREATE ON SCHEMA public TO data_logger;
GRANT USAGE ON SCHEMA public TO data_logger;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO data_logger;