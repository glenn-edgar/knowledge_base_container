kb_data_structures Rust Bindings
Comprehensive safe Rust bindings for the kb_data_structures C static library, providing full access to knowledge base operations, RPC server/client management, job queues, streaming, and status operations.
Setup

Create the project directory structure:
bashmkdir -p include lib src examples

Copy your files:

Copy libkb_data_structures_static.a to the lib/ directory
Copy all .h files to the include/ directory:

kb_all.h
kb_search.h
postgres_setup.h
system_def.h
kb_stream_table.h
kb_status_table.h
kb_rpc_server_table.h
kb_rpc_client_table.h
kb_job_table.h




Build the project:
bashcargo build


Project Structure
kb_data_structures_rust/
├── Cargo.toml              # Project configuration
├── build.rs                # Build script for FFI bindings
├── include/
│   ├── kb_all.h           # Master header file
│   ├── kb_search.h        # Search operations
│   ├── postgres_setup.h   # Database connection
│   ├── system_def.h       # System definitions
│   ├── kb_stream_table.h  # Stream operations
│   ├── kb_status_table.h  # Status operations
│   ├── kb_rpc_server_table.h  # RPC server operations
│   ├── kb_rpc_client_table.h  # RPC client operations
│   └── kb_job_table.h     # Job operations
├── lib/
│   └── libkb_data_structures_static.a  # Static library
├── src/
│   ├── lib.rs             # Main library file
│   └── safe_wrappers.rs   # Safe Rust wrappers (2000+ lines)
└── examples/
    └── basic_usage.rs     # Comprehensive usage examples
Core Data Types
DatabaseConnection
Safe wrapper for database connections:
rust// Create PostgreSQL connection
let conn = DatabaseConnection::new_postgres(
    "dbname", "user", "password", "host", "port"
)?;

// Use existing connection pointer
let conn = DatabaseConnection::from_raw(conn_ptr)?;
KnowledgeBaseRow
Represents a knowledge base entry:
rustpub struct KnowledgeBaseRow {
    pub id: i32,
    pub knowledge_base: Option<String>,
    pub label: Option<String>,
    pub name: Option<String>,
    pub properties: Option<String>,
    pub data: Option<String>,
    pub has_link: bool,
    pub has_link_mount: bool,
    pub path: Option<String>,
}
RpcServerRow
Represents RPC server queue entries:
rustpub struct RpcServerRow {
    pub id: i32,
    pub server_path: Option<String>,
    pub request_id: Option<String>,
    pub rpc_action: Option<String>,
    pub request_payload: Option<String>,
    pub transaction_tag: Option<String>,
    pub state: Option<String>,
    pub priority: i32,
    // ... timestamps and other fields
}
RpcClientRow
Represents RPC client responses:
rustpub struct RpcClientRow {
    pub id: i32,
    pub request_id: Option<String>,
    pub client_path: Option<String>,
    pub server_path: Option<String>,
    pub rpc_action: Option<String>,
    pub response_payload: Option<String>,
    pub is_new_result: bool,
    // ... other fields
}
JobInformation
Represents job queue information:
rustpub struct JobInformation {
    pub found: bool,
    pub id: i32,
    pub data: Option<String>,
}
API Overview
1. Database Connection
rustuse kb_data_structures::*;

// Create connection
let conn = DatabaseConnection::new_postgres(
    "mydb", "user", "pass", "localhost", "5432"
)?;
2. Knowledge Base Queries
rust// Create and configure query
let mut query = KnowledgeBaseQuery::new("my_table")?;
query.search_kb("production")?;
query.search_name("api_server")?;
query.search_property_value("status", "active")?;
query.search_path("/services/*")?;

// Execute and get results
query.execute(&conn)?;
let results = query.get_results()?;

for result in results {
    println!("Found: ID={}, Name={:?}, Path={:?}", 
             result.id, result.name, result.path);
}
3. Search Operations
rust// Find RPC servers
let properties = [("env", "prod"), ("version", "2.0")];
let servers = find_rpc_servers(
    &conn, "servers", "main_kb", "web_server", &properties, None
)?;

// Find jobs
let jobs = find_jobs(
    &conn, "jobs", "system_kb", "backup", &[], Some("/jobs/*")
)?;

// Find streams
let streams = find_streams(
    &conn, "streams", "data_kb", "input", &[], None
)?;
4. Stream Operations
rust// Push stream data
push_stream_data(
    &conn,
    "streams_table",
    "/data/sensor1",
    r#"{"temperature": 23.5}"#,
    3,   // max_retries
    1.0  // retry_delay
)?;
5. Status Operations
rust// Set status
let success = set_status_data(
    &conn,
    "status_table",
    "/system/health",
    r#"{"status": "ok"}"#,
    3,   // retry_count
    0.5  // retry_delay
)?;

// Get status
if let Some(data) = get_status_data(&conn, "status_table", "/system/health")? {
    println!("Status: {}", data);
}
6. RPC Server Operations
rust// Count jobs
let new_jobs = count_new_jobs(&conn, "rpc_table", "/rpc/server")?;
let processing = count_processing_jobs(&conn, "rpc_table", "/rpc/server")?;

// Push job to server queue
let job = push_rpc_server_job(
    &conn,
    "rpc_table",
    "/rpc/server",
    "req_123",
    "process_data",
    r#"{"data": "payload"}"#,
    "tx_456",
    1,    // priority
    "/rpc/client_queue",
    3,    // max_retries
    1.0   // wait_time
)?;

// Peek at next job
if let Some(job) = peek_server_queue(&conn, "rpc_table", "/rpc/server", 3, 1.0)? {
    println!("Next job: {:?}", job.rpc_action);
}

// Mark job completed
mark_job_completion(&conn, "rpc_table", "/rpc/server", job_id, 3, 1.0)?;

// Clear queue
clear_server_queue(&conn, "rpc_table", "/rpc/server", 3, 1.0)?;
7. RPC Client Operations
rust// Check slots
let free_slots = find_free_slots(&conn, "client_table", "/rpc/client")?;
let queued_slots = find_queued_slots(&conn, "client_table", "/rpc/client")?;

// Push reply data
push_and_claim_reply_data(
    &conn,
    "client_table",
    "/rpc/client",
    "req_123",
    "/rpc/server",
    "process_data",
    "tx_456",
    r#"{"result": "success"}"#,
    3,   // max_retries
    1.0  // retry_delay
)?;

// Peek and claim reply
if let Some(reply) = peek_and_claim_reply_data(
    &conn, "client_table", "/rpc/client", 3, 1.0
)? {
    println!("Reply: {:?}", reply.response_payload);
}

// Clear reply queue
clear_reply_queue(&conn, "client_table", "/rpc/client", 3, 1.0)?;
8. Job Operations
rust// Get job counts
let (free_count, msg) = get_free_job_count(&conn, "jobs", "/jobs/backup")?;
let (queued_count, msg) = get_queued_job_count(&conn, "jobs", "/jobs/backup")?;

// Push job
let message = push_job_data(
    &conn,
    "jobs",
    "/jobs/backup",
    r#"{"type": "incremental"}"#,
    3,   // max_retries
    1.0  // retry_delay
)?;

// Peek job
let (job_info, message) = peek_job_data(&conn, "jobs", "/jobs/backup", 3, 1.0)?;
if job_info.found {
    println!("Job ID: {}, Data: {:?}", job_info.id, job_info.data);
}

// Mark completed
let message = mark_job_completed(&conn, "jobs", job_id, 3, 1.0)?;

// Clear queue
let message = clear_job_queue(&conn, "jobs", "/jobs/backup")?;
Error Handling
All operations return Result<T, KBError>:
rustpub enum KBError {
    NullPointer,
    InvalidString,
    QueryExecutionFailed,
    ConnectionError,
    MemoryAllocation,
    DatabaseError(String),
    RetryExhausted,
    JobNotFound,
}
Error handling patterns:
rustmatch find_rpc_servers(&conn, "table", "kb", "name", &[], None) {
    Ok(servers) => {
        for server in servers {
            println!("Server: {:?}", server.name);
        }
    }
    Err(KBError::ConnectionError) => {
        eprintln!("Database connection failed");
    }
    Err(KBError::DatabaseError(msg)) => {
        eprintln!("Database error: {}", msg);
    }
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
Memory Management
The safe wrappers handle all memory management automatically:

Automatic cleanup: RAII pattern with Drop implementations
Safe string conversion: All C strings safely converted to Rust types
Result memory management: C-allocated arrays automatically freed
Connection management: Safe wrapper around connection pointers
Struct cleanup: Automatic freeing of C structs via dedicated free functions

Safety Features

All C function calls wrapped in unsafe blocks within safe APIs
Comprehensive null pointer checks
Proper error propagation
String encoding validation
Memory leak prevention
Thread-safe design (when used with appropriate synchronization)

Development
Building
bashcargo build
Running Examples
bashcargo run --example basic_usage
Running Tests
bashcargo test
Integration with Your Application

Add to your Cargo.toml:
toml[dependencies]
kb_data_structures = { path = "../kb_data_structures_rust" }

Use in your code:
rustuse kb_data_structures::*;

fn main() -> Result<(), KBError> {
    let conn = DatabaseConnection::new_postgres(
        "mydb", "user", "pass", "localhost", "5432"
    )?;
    
    let mut query = KnowledgeBaseQuery::new("my_table")?;
    query.search_kb("production")?;
    query.execute(&conn)?;
    
    let results = query.get_results()?;
    println!("Found {} results", results.len());
    
    Ok(())
}


Performance Notes

Minimal overhead: Thin wrappers around C functions
Zero-copy where possible: Direct pointer access when safe
Efficient string handling: Minimized allocations
Batch operations: Support for bulk operations where available
Connection reuse: Single connection for multiple operations

Contributing

Follow Rust naming conventions and idioms
Maintain memory safety in all unsafe blocks
Add comprehensive error handling
Include tests for new functionality
Update documentation for API changes
Ensure all new code includes proper resource cleanup

License
MIT OR Apache-2.0