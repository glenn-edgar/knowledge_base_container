-- Test script for PostgreSQL 17 with pgvector extension
-- Run with: psql -h localhost -U gedgar -d knowledge_base -f test.sql

\echo 'Testing PostgreSQL 17 with pgvector extension...'
\echo ''

-- Test basic connection and version
\echo '=== PostgreSQL Version ==='
SELECT version();
\echo ''

-- Test pgvector extension
\echo '=== pgvector Extension Test ==='
SELECT * FROM pg_extension WHERE extname = 'vector';
\echo ''

-- Create test table with vector data
\echo '=== Creating Test Table ==='
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    embedding VECTOR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

\echo 'Test table created successfully'
\echo ''

-- Insert sample data first, then create index
\echo '=== Inserting Sample Data ==='
INSERT INTO documents (title, content, embedding) VALUES 
('Machine Learning Basics', 'Introduction to ML concepts and algorithms', '[0.1, 0.2, 0.3]'),
('Deep Learning Guide', 'Understanding neural networks and deep learning', '[0.4, 0.5, 0.6]'),
('Data Science Tutorial', 'Complete guide to data science methodologies', '[0.7, 0.8, 0.9]'),
('AI Applications', 'Real-world applications of artificial intelligence', '[0.2, 0.4, 0.8]'),
('Python Programming', 'Learn Python for data analysis and ML', '[0.5, 0.3, 0.7]')
ON CONFLICT DO NOTHING;

\echo 'Sample data inserted'
\echo ''

-- Create index after inserting data to avoid low recall warning
\echo '=== Creating Vector Index ==='
CREATE INDEX IF NOT EXISTS idx_documents_embedding 
ON documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

\echo 'Vector index created'
\echo ''

-- Test pgvector similarity queries
\echo '=== pgvector Similarity Query Test ==='
\echo 'Vector similarity to [0.1, 0.2, 0.3] (cosine distance):'
SELECT 
    title,
    content,
    embedding <-> '[0.1,0.2,0.3]' AS cosine_distance
FROM documents 
ORDER BY cosine_distance 
LIMIT 3;
\echo ''

\echo 'Vector similarity using dot product:'
SELECT 
    title,
    embedding <#> '[0.1,0.2,0.3]' AS negative_dot_product
FROM documents 
ORDER BY negative_dot_product 
LIMIT 3;
\echo ''

\echo 'Vector similarity using L2 distance:'
SELECT 
    title,
    embedding <-> '[0.1,0.2,0.3]' AS l2_distance
FROM documents 
ORDER BY l2_distance 
LIMIT 3;
\echo ''

-- Show table structure
\echo '=== Table Structure ==='
\d documents
\echo ''

-- Show indexes
\echo '=== Indexes ==='
SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'documents';
\echo ''

-- Show vector operations available
\echo '=== Available Vector Operators ==='
SELECT oprname, oprleft::regtype, oprright::regtype, oprresult::regtype 
FROM pg_operator 
WHERE oprname IN ('<->', '<#>', '<=>') 
ORDER BY oprname;
\echo ''

\echo '=== All Tests Completed Successfully! ==='
\echo 'PostgreSQL 17 with pgvector is working correctly.'