CREATE ROLE dydx_logger WITH LOGIN PASSWORD '1234';
GRANT CREATE ON SCHEMA public TO dydx_logger;
GRANT USAGE ON SCHEMA public TO dydx_logger;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dydx_logger;
