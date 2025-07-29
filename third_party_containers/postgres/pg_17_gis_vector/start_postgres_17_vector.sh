#!/bin/bash

# Use official pgvector image with PostgreSQL 17
echo "Starting PostgreSQL 17 with pgvector (official image)..."

docker run -d \
  --name postgres-vector \
  -e POSTGRES_DB=knowledge_base \
  -e POSTGRES_USER=gedgar \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e "PGDATA=/var/lib/postgresql/data" \
  -p 5432:5432 \
  -v "/home/gedgar/postgres_data:/home/postgres/postgres_data" \
  -v "pg_data_volume:/var/lib/postgresql/data" \
  --restart no \
  pgvector/pgvector:pg17

echo "Waiting for PostgreSQL to start..."
sleep 10

# Initialize pgvector extension
echo "Setting up pgvector extension..."
docker exec postgres-vector psql -U gedgar -d knowledge_base -c "CREATE EXTENSION IF NOT EXISTS vector;"

echo "PostgreSQL 17 with pgvector is ready!"
echo "Container name: postgres-vector"
echo "Database: knowledge_base"
echo "User: gedgar"
echo "Port: 5432"

