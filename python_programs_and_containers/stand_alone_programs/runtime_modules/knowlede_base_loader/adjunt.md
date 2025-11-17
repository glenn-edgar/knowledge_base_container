Path Examples with Composition
Static Entity Definitions
Systems and Infrastructure
# Systems
system.production                    # label: "system", node: "production"
system.staging                       # label: "system", node: "staging"

# Sites
site.us_east_1                      # label: "site", node: "us_east_1"
site.eu_central_1                   # label: "site", node: "eu_central_1"

# Nodes (servers)
node.web_server_01                  # label: "node", node: "web_server_01"
node.db_primary                     # label: "node", node: "db_primary"
Containers with Required Processes
# Container definitions
container.nginx                                    # label: "container", node: "nginx"
container.nginx.process.master                     # label: "process", node: "master"
container.nginx.process.worker                     # label: "process", node: "worker"
container.nginx.process.cache_manager              # label: "process", node: "cache_manager"

container.postgres_14                              # label: "container", node: "postgres_14"
container.postgres_14.process.postmaster           # label: "process", node: "postmaster"
container.postgres_14.process.wal_writer           # label: "process", node: "wal_writer"
container.postgres_14.process.checkpointer         # label: "process", node: "checkpointer"
container.postgres_14.process.autovacuum           # label: "process", node: "autovacuum"

container.app_service                              # label: "container", node: "app_service"
container.app_service.process.web_server           # label: "process", node: "web_server"
container.app_service.process.worker_pool          # label: "process", node: "worker_pool"
container.app_service.process.scheduler            # label: "process", node: "scheduler"
Parts with Components
# SSL Certificate part with components
part.ssl_certificate                               # label: "part", node: "ssl_certificate"
part.ssl_certificate.component.private_key         # label: "component", node: "private_key"
part.ssl_certificate.component.public_cert         # label: "component", node: "public_cert"
part.ssl_certificate.component.ca_bundle           # label: "component", node: "ca_bundle"

# Application binary with components
part.app_binary_v2                                 # label: "part", node: "app_binary_v2"
part.app_binary_v2.component.executable            # label: "component", node: "executable"
part.app_binary_v2.component.config_schema         # label: "component", node: "config_schema"
part.app_binary_v2.component.static_assets         # label: "component", node: "static_assets"
part.app_binary_v2.component.migrations            # label: "component", node: "migrations"

# Monitoring stack with components
part.monitoring_exporter                           # label: "part", node: "monitoring_exporter"
part.monitoring_exporter.component.metrics_collector # label: "component", node: "metrics_collector"
part.monitoring_exporter.component.log_shipper     # label: "component", node: "log_shipper"
part.monitoring_exporter.component.health_checker  # label: "component", node: "health_checker"
Deployment Paths
# Full deployment hierarchy
deploy.production.us_east_1
deploy.production.us_east_1.web_server_01
deploy.production.us_east_1.web_server_01.nginx
deploy.production.us_east_1.web_server_01.nginx.ssl_certificate
deploy.production.us_east_1.web_server_01.nginx.monitoring_exporter

deploy.production.us_east_1.db_primary
deploy.production.us_east_1.db_primary.postgres_14
deploy.production.us_east_1.db_primary.postgres_14.monitoring_exporter
	
