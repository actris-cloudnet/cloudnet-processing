#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ss WITH PASSWORD 'dev';
    CREATE DATABASE ss OWNER ss;
    CREATE USER dataportal WITH PASSWORD 'dev';
    CREATE DATABASE dataportal OWNER dataportal;
EOSQL
