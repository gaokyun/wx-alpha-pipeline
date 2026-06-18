#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USERNAME" --dbname "$POSTGRES_DB" <<-EOSQL
    -- 1. Create JupyterHub User & DB
    DO \$$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$POSTGRES_JH_USER') THEN
            CREATE USER $POSTGRES_JH_USER WITH PASSWORD '$POSTGRES_JH_PASSWORD';
        END IF;
    END
    \$$;
    SELECT 'CREATE DATABASE jupyterhub OWNER $POSTGRES_JH_USER' 
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'jupyterhub')\gexec
EOSQL