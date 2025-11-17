# Distributed System Knowledge Base - Tree-Based Graph Path Strategy

## Overview

This document defines the ltree path strategy for a hierarchical knowledge base storing distributed system configurations. The system uses PostgreSQL's ltree extension combined with a Neo4j-inspired graph model to manage relationships between systems, sites, nodes, containers, and reusable parts. This approach provides tree-based traversal with graph-like flexibility.

## Core Concepts

Our knowledge base combines:
- **Tree Structure** - Hierarchical paths using PostgreSQL ltree for efficient tree traversal
- **Graph Semantics** - Neo4j-style nodes and labels for rich relationship modeling
- **Separate Namespaces** - Clear separation between definition and deployment

### Neo4j-Inspired Design
- **Node Name** - Unique identifier extracted from the last element of the path
- **Label** - Category/type extracted from the second-to-last element of the path
- **Properties** - JSON columns for both node and label metadata

## Database Schema

### Main Table Structure
```sql
CREATE TABLE knowledge_base (
    id SERIAL PRIMARY KEY,
    path LTREE NOT NULL UNIQUE,
    node_name VARCHAR(255) NOT NULL,      -- Last element of path
    label VARCHAR(100) NOT NULL,          -- Second-to-last element of path  
    node_data JSONB NOT NULL DEFAULT '{}', -- Node-specific properties
    label_data JSONB NOT NULL DEFAULT '{}', -- Label/type properties
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Required indexes for performance
CREATE INDEX idx_path_gist ON knowledge_base USING GIST (path);
CREATE INDEX idx_path_btree ON knowledge_base USING BTREE (path);
CREATE INDEX idx_node_name ON knowledge_base(node_name);
CREATE INDEX idx_label ON knowledge_base(label);
CREATE INDEX idx_node_data ON knowledge_base USING GIN (node_data);
CREATE INDEX idx_label_data ON knowledge_base USING GIN (label_data);
```

## Path Structure and Label/Node Extraction

### Path Anatomy
```
path: system.production
├── label: "system"
└── node_name: "production"

path: deploy.production.us_east_1.web_server_01.nginx.ssl_certificate
├── label: "nginx"  
└── node_name: "ssl_certificate"
```

### Extraction Rules
- **node_name** = Last element of path (e.g., `ssl_certificate`)
- **label** = Second-to-last element of path (e.g., `nginx`)
- For single-element paths, label equals the root category

## Namespace Structure

### Static Definitions (Graph Nodes)
```
system.{system_id}        # label: "system", node_name: "{system_id}"
site.{site_id}           # label: "site", node_name: "{site_id}"
node.{node_id}           # label: "node", node_name: "{node_id}"
container.{container_id}  # label: "container", node_name: "{container_id}"
part.{part_id}           # label: "part", node_name: "{part_id}"
```

### Deployment Hierarchy (Tree Relationships)
```
deploy.{system_id}.{site_id}
deploy.{system_id}.{site_id}.{node_id}
deploy.{system_id}.{site_id}.{node_id}.{container_id}
deploy.{system_id}.{site_id}.{node_id}.{container_id}.{part_id}
```

## Data Examples with Node/Label Properties

### Inserting Static Entities
```sql
-- System with properties
INSERT INTO knowledge_base (path, node_name, label, node_data, label_data) 
VALUES (
    'system.production',
    'production',
    'system',
    '{"environment": "prod", "version": "2.0", "owner": "platform-team"}'::jsonb,
    '{"type": "system", "category": "environment", "mutable": false}'::jsonb
);

-- Site with geographic data
INSERT INTO knowledge_base (path, node_name, label, node_data, label_data) 
VALUES (
    'site.us_east_1',
    'us_east_1',
    'site',
    '{"region": "us-east-1", "availability_zones": ["us-east-1a", "us-east-1b"], "vpc_id": "vpc-12345"}'::jsonb,
    '{"type": "site", "category": "infrastructure", "cloud_provider": "aws"}'::jsonb
);

-- Node (server) with specifications
INSERT INTO knowledge_base (path, node_name, label, node_data, label_data) 
VALUES (
    'node.web_server_01',
    'web_server_01',
    'node',
    '{"ip": "10.0.1.10", "cpu_cores": 16, "memory_gb": 64, "storage_gb": 500}'::jsonb,
    '{"type": "node", "category": "compute", "hardware_class": "m5.4xlarge"}'::jsonb
);
```

### Inserting Deployment Relationships
```sql
-- Complex deployment with rich metadata
INSERT INTO knowledge_base (path, node_name, label, node_data, label_data) 
VALUES (
    'deploy.production.us_east_1.web_server_01.nginx.ssl_certificate',
    'ssl_certificate',
    'nginx',
    '{"cert_id": "cert-abc123", "expires": "2025-12-31", "domain": "*.example.com", "issuer": "LetsEncrypt"}'::jsonb,
    '{"container_type": "nginx", "container_version": "1.21", "port": 443}'::jsonb
);

-- Application deployment
INSERT INTO knowledge_base (path, node_name, label, node_data, label_data) 
VALUES (
    'deploy.production.us_east_1.web_server_01.app_service.user_api_v3',
    'user_api_v3',
    'app_service',
    '{"version": "3.2.1", "port": 8080, "replicas": 3, "health_check": "/health"}'::jsonb,
    '{"service_type": "api", "runtime": "nodejs", "framework": "express"}'::jsonb
);
```

## Tree-Based Graph Traversal Patterns

### Neo4j-Style Queries Using Tree Structure

#### Find All Nodes with Specific Label
```sql
-- Find all nginx containers (Neo4j: MATCH (n:nginx) RETURN n)
SELECT * FROM knowledge_base 
WHERE label = 'nginx';
```

#### Find Node by Name
```sql
-- Find specific node (Neo4j: MATCH (n {name: 'web_server_01'}) RETURN n)
SELECT * FROM knowledge_base 
WHERE node_name = 'web_server_01';
```

#### Tree Traversal - Find Children
```sql
-- Find all children of a node (Neo4j: MATCH (p)-[:HAS_CHILD]->(c) WHERE p.name = 'us_east_1')
SELECT * FROM knowledge_base 
WHERE path <@ 'deploy.production.us_east_1'
AND path != 'deploy.production.us_east_1';
```

#### Find by Properties in JSON
```sql
-- Find all nodes expiring soon (Neo4j: MATCH (n) WHERE n.expires < '2025-06-01')
SELECT * FROM knowledge_base 
WHERE node_data->>'expires' < '2025-06-01';

-- Find all nodes of a specific runtime
SELECT * FROM knowledge_base 
WHERE label_data->>'runtime' = 'nodejs';
```

#### Path-Based Pattern Matching
```sql
-- Find all SSL certificates across all nginx instances
SELECT * FROM knowledge_base 
WHERE label = 'nginx' 
AND node_name LIKE '%ssl%';

-- Find deployment depth (tree level)
SELECT *, nlevel(path) as depth 
FROM knowledge_base 
WHERE path <@ 'deploy.production';
```

## Complex Graph-Like Queries

### Relationship Traversal Examples

```sql
-- Find all parts deployed to a specific site (multi-hop relationship)
WITH site_path AS (
    SELECT 'deploy.production.us_east_1'::ltree AS base_path
)
SELECT 
    k.node_name,
    k.label,
    k.node_data,
    subpath(k.path, -3, 1)::text AS deployed_on_node,
    subpath(k.path, -2, 1)::text AS in_container
FROM knowledge_base k, site_path sp
WHERE k.path <@ sp.base_path
AND k.label = 'part';

-- Find all services and their deployment locations
SELECT 
    node_name AS service,
    label AS container_type,
    subpath(path, 2, 1)::text AS site,
    subpath(path, 3, 1)::text AS server,
    node_data->>'version' AS version,
    node_data->>'replicas' AS replicas
FROM knowledge_base
WHERE path ~ 'deploy.*'
AND nlevel(path) >= 5;
```

### Graph Aggregations

```sql
-- Count nodes by label (Neo4j: MATCH (n) RETURN labels(n), count(*))
SELECT label, COUNT(*) as node_count
FROM knowledge_base
GROUP BY label
ORDER BY node_count DESC;

-- Find all unique relationships between label types
SELECT 
    k1.label AS from_label,
    k2.label AS to_label,
    COUNT(*) AS relationship_count
FROM knowledge_base k1
JOIN knowledge_base k2 ON k2.path <@ k1.path AND k2.path != k1.path
WHERE nlevel(k2.path) = nlevel(k1.path) + 1
GROUP BY k1.label, k2.label;
```

## Tree-Based Neo4j Design Patterns

### Pattern 1: Service Discovery
```sql
-- Find all instances of a service across the infrastructure
CREATE OR REPLACE FUNCTION find_service_instances(service_name TEXT)
RETURNS TABLE (
    path ltree,
    site TEXT,
    server TEXT,
    container TEXT,
    properties JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        k.path,
        subpath(k.path, 2, 1)::text,
        subpath(k.path, 3, 1)::text,
        subpath(k.path, 4, 1)::text,
        k.node_data
    FROM knowledge_base k
    WHERE k.node_name = service_name
    AND k.path ~ 'deploy.*';
END;
$$ LANGUAGE plpgsql;
```

### Pattern 2: Dependency Analysis
```sql
-- Find all dependencies of a container
CREATE OR REPLACE FUNCTION find_dependencies(container_path ltree)
RETURNS TABLE (
    dependency_name TEXT,
    dependency_type TEXT,
    dependency_data JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        node_name,
        label,
        node_data
    FROM knowledge_base
    WHERE path <@ container_path
    AND path != container_path;
END;
$$ LANGUAGE plpgsql;
```

### Pattern 3: Impact Analysis
```sql
-- Find all affected components if a node goes down
CREATE OR REPLACE FUNCTION analyze_node_impact(node_id TEXT)
RETURNS TABLE (
    affected_path ltree,
    component_type TEXT,
    component_name TEXT,
    criticality JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        path,
        label,
        node_name,
        node_data
    FROM knowledge_base
    WHERE path ~ ('deploy.*.*.{}.*'::text)::lquery
    AND path ~ node_id;
END;
$$ LANGUAGE plpgsql;
```

## Best Practices for Tree-Based Graph Model

### 1. Node Naming Conventions
- Use **consistent suffixes** for versioning (e.g., `_v1`, `_v2`)
- Keep node names **unique within their label context**
- Use **descriptive names** that work well in both tree and graph contexts

### 2. Label Design
- Labels should represent **node types/categories**
- Keep labels **consistent and predictable**
- Use labels for **type-based queries and filtering**

### 3. JSON Property Guidelines

#### Node Data (Instance-Specific)
```json
{
  "id": "unique-identifier",
  "version": "1.2.3",
  "config": {...},
  "status": "active",
  "created_by": "user",
  "custom_properties": {...}
}
```

#### Label Data (Type-Specific)
```json
{
  "category": "infrastructure|application|data",
  "tier": "frontend|backend|data",
  "compliance": ["pci", "hipaa"],
  "sla": "99.99%",
  "cost_center": "engineering"
}
```

### 4. Relationship Modeling
- Use **path hierarchy** for parent-child relationships
- Use **JSON references** for cross-tree relationships
- Consider **materialized views** for complex graph traversals

## Migration from Pure Tree to Graph-Tree Hybrid

### Step 1: Add Neo4j-style columns
```sql
ALTER TABLE knowledge_base 
ADD COLUMN node_name VARCHAR(255),
ADD COLUMN label VARCHAR(100),
ADD COLUMN node_data JSONB DEFAULT '{}',
ADD COLUMN label_data JSONB DEFAULT '{}';
```

### Step 2: Populate from existing paths
```sql
UPDATE knowledge_base 
SET 
    node_name = subpath(path, -1, 1)::text,
    label = CASE 
        WHEN nlevel(path) > 1 THEN subpath(path, -2, 1)::text
        ELSE subpath(path, 0, 1)::text
    END;
```

### Step 3: Create indexes
```sql
CREATE INDEX idx_node_name ON knowledge_base(node_name);
CREATE INDEX idx_label ON knowledge_base(label);
CREATE INDEX idx_node_data ON knowledge_base USING GIN (node_data);
CREATE INDEX idx_label_data ON knowledge_base USING GIN (label_data);
```

## Advantages of Tree-Based Graph Model

1. **Efficient Hierarchical Queries** - ltree provides optimal tree traversal
2. **Graph-Like Flexibility** - Node/label model enables graph-style queries
3. **Rich Metadata** - Dual JSON columns for instance and type properties
4. **Neo4j Compatibility** - Familiar patterns for graph database users
5. **Hybrid Queries** - Combine tree traversal with property-based filtering
6. **Scalability** - Indexes on both structure (path) and content (JSON)

## Example Use Cases

### Infrastructure Discovery
```sql
-- Find all database nodes and their configurations
SELECT 
    node_name,
    node_data->>'ip' as ip_address,
    node_data->>'memory_gb' as memory,
    label_data->>'hardware_class' as instance_type,
    subpath(path, 2, 1)::text as deployed_site
FROM knowledge_base
WHERE label = 'node'
AND node_data->>'type' ? 'database';
```

### Service Mesh Topology
```sql
-- Build service communication graph
WITH service_nodes AS (
    SELECT 
        path,
        node_name as service,
        node_data->>'endpoints' as endpoints
    FROM knowledge_base
    WHERE label IN ('app_service', 'api_gateway', 'microservice')
)
SELECT 
    s1.service as source_service,
    s2.service as target_service,
    s1.endpoints
FROM service_nodes s1
CROSS JOIN service_nodes s2
WHERE s1.path @> s2.path OR s1.path <@ s2.path;
```

## Monitoring and Maintenance

### Path Integrity Check
```sql
-- Verify node_name and label match path structure
SELECT path, node_name, label,
    subpath(path, -1, 1)::text = node_name as name_matches,
    subpath(path, -2, 1)::text = label as label_matches
FROM knowledge_base
WHERE subpath(path, -1, 1)::text != node_name
   OR subpath(path, -2, 1)::text != label;
```

### Graph Statistics
```sql
-- Graph overview statistics
SELECT 
    COUNT(DISTINCT label) as unique_labels,
    COUNT(DISTINCT node_name) as unique_nodes,
    MAX(nlevel(path)) as max_tree_depth,
    COUNT(*) as total_relationships
FROM knowledge_base;
```

## Support and Future Extensions

This tree-based graph model can be extended to support:
- **Graph algorithms** via recursive CTEs
- **Cypher-like query language** via stored procedures  
- **Real-time graph updates** via triggers
- **Graph visualizations** via D3.js/vis.js integration
- **Neo4j synchronization** for hybrid deployments


