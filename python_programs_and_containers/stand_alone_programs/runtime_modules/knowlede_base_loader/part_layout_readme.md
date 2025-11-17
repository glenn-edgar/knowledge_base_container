Distributed System Knowledge Base - Path Strategy
Overview
This document defines the ltree path strategy for a hierarchical knowledge base storing distributed system configurations. The system uses PostgreSQL's ltree extension to manage relationships between systems, sites, nodes, containers, and reusable parts.
Core Concepts
Our knowledge base uses separate namespaces to maintain clear separation between:

Definition (what exists) - static entities like sites, nodes, parts
Deployment (where things are deployed) - the actual configuration hierarchy

Namespace Structure
Static Definitions
system.{system_id}        # Top-level system/environment
site.{site_id}           # Geographic or logical location  
node.{node_id}           # Server/machine instance
container.{container_id}  # Container type/template
part.{part_id}           # Reusable component/service
Deployment Hierarchy
deploy.{system_id}.{site_id}
deploy.{system_id}.{site_id}.{node_id}
deploy.{system_id}.{site_id}.{node_id}.{container_id}
deploy.{system_id}.{site_id}.{node_id}.{container_id}.{part_id}
Path Examples
Static Entity Definitions
# Systems
system.production
system.staging
system.development

# Sites (data centers/regions)
site.us_east_1
site.us_west_2
site.eu_central_1

# Nodes (servers)
node.web_server_01
node.web_server_02
node.db_primary
node.db_replica_01

# Container Types
container.nginx
container.app_service
container.postgres_14
container.redis_cache

# Reusable Parts/Components
part.ssl_certificate
part.env_config
part.log_agent
part.monitoring_exporter
part.app_binary_v2
Deployment Paths
# Full deployment hierarchy examples
deploy.production.us_east_1
deploy.production.us_east_1.web_server_01
deploy.production.us_east_1.web_server_01.nginx
deploy.production.us_east_1.web_server_01.nginx.ssl_certificate
deploy.production.us_east_1.web_server_01.nginx.env_config

deploy.production.us_east_1.web_server_01.app_service
deploy.production.us_east_1.web_server_01.app_service.app_binary_v2
deploy.production.us_east_1.web_server_01.app_service.log_agent
deploy.production.us_east_1.web_server_01.app_service.monitoring_exporter

deploy.production.us_east_1.db_primary.postgres_14
deploy.production.us_east_1.db_primary.postgres_14.env_config
deploy.production.us_east_1.db_primary.postgres_14.monitoring_exporter
Naming Conventions
General Rules

Use lowercase with underscores for multi-word identifiers
Keep names descriptive but concise
Include version numbers where relevant (e.g., part.app_binary_v2_1_0)
Use consistent prefixes for similar items

Identifier Examples
# Good
site.us_east_1
node.web_server_01
part.nginx_config_v2

# Avoid
site.USEast1
node.WebServer-01
part.NGINX_CONFIG_FINAL_FINAL_v2
Common Query Patterns
Find All Components at a Site
sqlSELECT * FROM knowledge_base 
WHERE path <@ 'deploy.production.us_east_1';
Find All Deployments of a Specific Part
sqlSELECT * FROM knowledge_base 
WHERE path ~ '*.monitoring_exporter';
Get All Nodes at a Specific Site
sqlSELECT * FROM knowledge_base 
WHERE path ~ 'deploy.production.us_east_1.*' 
AND nlevel(path) = 4;
Find What's Deployed on a Specific Node
sqlSELECT * FROM knowledge_base 
WHERE path <@ 'deploy.production.us_east_1.web_server_01';
Get Direct Children Only (Not Recursive)
sql-- Direct containers on a node
SELECT * FROM knowledge_base 
WHERE path ~ 'deploy.production.us_east_1.web_server_01.*{1}';
Use Case Examples
Example 1: Web Application Stack
# Define components
system.web_platform
site.aws_us_east
site.aws_us_west
node.web_01
node.web_02
node.cache_01
container.nginx_lb
container.app_runner
container.redis
part.tls_cert
part.app_code_v3
part.redis_config

# Deploy to us_east
deploy.web_platform.aws_us_east.web_01.nginx_lb.tls_cert
deploy.web_platform.aws_us_east.web_01.app_runner.app_code_v3
deploy.web_platform.aws_us_east.cache_01.redis.redis_config

# Deploy to us_west (disaster recovery)
deploy.web_platform.aws_us_west.web_02.nginx_lb.tls_cert
deploy.web_platform.aws_us_west.web_02.app_runner.app_code_v3
Example 2: Microservices Architecture
# Define microservices system
system.microservices
site.datacenter_primary
node.k8s_master
node.k8s_worker_01
node.k8s_worker_02
container.api_gateway
container.auth_service
container.user_service
part.envoy_proxy
part.jwt_validator
part.database_client

# Deployment configuration
deploy.microservices.datacenter_primary.k8s_worker_01.api_gateway.envoy_proxy
deploy.microservices.datacenter_primary.k8s_worker_01.auth_service.jwt_validator
deploy.microservices.datacenter_primary.k8s_worker_02.user_service.database_client
Benefits of This Approach

Reusability - Parts can be used across multiple containers/nodes without duplication
Clear Hierarchy - Easy to understand system → site → node → container → part relationships
Flexible Queries - Efficient traversal at any level using ltree operators
Scalability - Easy to add new sites, nodes, or containers
Version Control - Parts can be versioned independently
Multi-tenancy - Multiple systems can share infrastructure

