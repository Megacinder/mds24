#!/usr/bin/env bash

if docker ps | grep -q mssql
then
    docker compose -f docker-compose.yml down
else
    docker compose -f docker-compose.yml up -d
fi


# /opt/mssql-tools/bin/sqlcmd -U sa -P UserPassword11@ -i /docker-entrypoint-initdb.d/init.sql
