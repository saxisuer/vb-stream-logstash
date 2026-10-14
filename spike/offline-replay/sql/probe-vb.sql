SELECT current_database();
SELECT version();
SELECT attlen FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attname = 'relname';
SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user;
SELECT datname, datcompatibility FROM pg_database ORDER BY oid;
