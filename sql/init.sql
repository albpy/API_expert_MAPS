DO $do$
BEGIN
   IF EXISTS (SELECT FROM pg_database WHERE datname = 'bmaps') THEN
      RAISE NOTICE 'Database already exists';
   ELSE
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE bmaps');
   END IF;
END $do$;

DO $do$
BEGIN
    CREATE USER rebo WITH PASSWORD 'password';
EXCEPTION
    WHEN duplicate_object THEN 
    -- User already exists. Do nothing.
END
$do$;

ALTER ROLE rebo WITH SUPERUSER LOGIN;