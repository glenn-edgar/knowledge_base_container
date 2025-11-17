# Process scheduler

A robust Python-based process supervisor and scheduler that manages multiple child processes with automatic restart capabilities, error logging, and zombie process reaping.

## Features

- **Process Supervision**: Manages multiple child processes concurrently
- **Automatic Restart**: Implements exponential backoff for failed processes
- **Error Logging**: Captures stderr output from each child process to separate log files
- **Zombie Reaping**: Automatically cleans up zombie processes
- **Graceful Shutdown**: Handles SIGTERM with fallback to SIGKILL for stubborn processes
- **Error Reporting**: JSON-formatted error reports with Base64-encoded stderr content
- **Customizable Error Handling**: Support for custom error handler callbacks

## Requirements

- Python 3.7+
- asyncio support
- Standard library modules: `os`, `signal`, `time`, `base64`, `json`, `pathlib`, `typing`, `datetime`

## Installation

Simply copy the script to your project directory. No external dependencies required.

```bash
chmod +x ProcessScheduler.py
```

## Usage

### Basic Usage

```python
from pathlib import Path
from proc import ProcessScheduler

BASE_DIR = Path("/path/to/your/programs")
PROGRAMS = {
    "service1": ["python3", "service1.py"],
    "service2": ["python3", "service2.py"],
    "service3": ["node", "service3.js"],
}

agg = ProcessScheduler(BASE_DIR, PROGRAMS)
try:
    agg.start()
except KeyboardInterrupt:
    agg.stop()
```

### Custom Error Handler

```python
def custom_error_handler(process_name: str, json_data: str):
    data = json.loads(json_data)
    # Log to external service, send alerts, etc.
    print(f"Process {process_name} failed with code {data['return_code']}")
    
agg = ProcessScheduler(
    BASE_DIR, 
    PROGRAMS, 
    err_dir=Path("/var/log/myapp"),
    error_handler=custom_error_handler
)
```

## API Reference

### `ProcessScheduler` Class

#### Constructor Parameters

- `base_dir` (Path): Base directory for resolving relative paths
- `programs` (Dict[str, List[str]]): Dictionary mapping process names to command arguments
- `err_dir` (Path, optional): Directory for stderr log files (default: `/tmp`)
- `error_handler` (Callable, optional): Custom error handler function

#### Methods

- `start()`: Starts the ProcessScheduler and all child processes
- `stop()`: Initiates graceful shutdown of all processes

### `ChildSpec` Class

Internal class representing a managed child process.

#### Attributes

- `name`: Process identifier
- `cmd`: Command and arguments list
- `process`: asyncio subprocess instance
- `backoff`: Current backoff delay in seconds
- `restarts`: Number of restart attempts
- `last_start_time`: Timestamp of last start attempt

## Error Handler Format

The error handler receives JSON-formatted data with the following structure:

```json
{
    "process": "process_name",
    "timestamp": 1234567890.123,
    "utc_iso": "2024-01-01T12:00:00.000000+00:00",
    "data": "base64_encoded_stderr_content",
    "return_code": -1
}
```

## Backoff Strategy

- Initial backoff: 1 second
- Exponential increase: doubles after each failure
- Maximum backoff: 60 seconds
- Reset condition: 30 seconds of successful running

## Signal Handling

- **SIGTERM**: Graceful shutdown sent to all child processes
- **SIGKILL**: Force kill after 10-second timeout
- **Process Groups**: Each child runs in its own process group for clean termination

## Logging

- Each child process has a dedicated stderr log file
- Log files are created in the specified `err_dir`
- Naming convention: `{process_name}.stderr.log`
- Files are opened with unbuffered writes for real-time logging

## Examples

### Running Multiple Python Scripts

```python
PROGRAMS = {
    "data_processor": ["python3", "processor.py", "--config", "prod.yml"],
    "api_server": ["python3", "-m", "uvicorn", "app:app", "--port", "8000"],
    "scheduler": ["python3", "scheduler.py"],
}
```

### Mixed Language Services

```python
PROGRAMS = {
    "python_service": ["python3", "service.py"],
    "node_service": ["node", "index.js"],
    "go_service": ["./service_binary"],
    "shell_script": ["/bin/bash", "monitor.sh"],
}
```

## Limitations

- Stdout is redirected to `/dev/null` (only stderr is captured)
- No inter-process communication mechanism
- No process health checks beyond exit code monitoring
- Single-threaded event loop per ProcessScheduler instance

## License

[Specify your license here]

## Contributing

[Add contribution guidelines if applicable]

to run full test suit 
python3 ProcessScheduler.py --test

