#!/bin/bash

# Test runner script for PostgreSQL with pgvector
echo "Running PostgreSQL pgvector tests..."
echo "Connecting to database: knowledge_base"
echo "User: gedgar"
echo ""

# Set environment variable for password to avoid prompts
export PGPASSWORD=$POSTGRES_PASSWORD

# Run the test SQL script
psql -h localhost -U gedgar -d knowledge_base -f test.sql

# Clear password from environment
unset PGPASSWORD

echo ""
echo "Test completed. Check output above for any errors."

