#!/bin/bash

# Use official pgvector image with PostgreSQL 17
echo "Starting PostgreSQL 17 with pgvector (official image)..."

# Option 2: More secure with specific host binding (recommended for production)
# Option 2: More secure with specific host binding (recommended for production)
docker run -d \
  --network bridge \
  --name postgres-vector \
  -e POSTGRES_DB=knowledge_base \
  -e POSTGRES_USER=gedgar \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e PGDATA=/var/lib/postgresql/data \
  -p 0.0.0.0:5432:5432 \
  -v "/home/gedgar/postgres_data:/var/lib/postgresql/data" \
  --restart unless-stopped \
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

