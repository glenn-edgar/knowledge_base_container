# Configuration Update Script

## Overview

This script automates the process of loading, executing, and managing configuration updates for a knowledge base system. It handles configuration script deployment, execution validation, and update flag management to ensure reliable system updates.

## Features

- **Automated Configuration Loading**: Loads configuration scripts from specified directories
- **Master Node Validation**: Only executes updates on master nodes or when updates are scheduled
- **Script Execution**: Runs start scripts and configuration scripts with error handling
- **Update Flag Management**: Automatically resets update flags upon successful completion
- **Force Update Option**: Command line parameter to force updates regardless of current flags

## Prerequisites

- Python 3.x
- Required Python modules:
  - `update_file_loader` (custom module)
  - `run_kb_config_scripts` (custom module)
  - Standard library modules: `json`, `subprocess`, `time`, `sys`, `os`, `argparse`, `pathlib`

## Directory Structure

The script expects the following directory structure:

```
~/
├── mount_startup/
│   ├── node_definition.json    # Node configuration file
│   └── update.json             # Update control file with update_flag
├── mount_kb_definitions/       # Target directory for configuration scripts
│   └── start.py               # Main startup script (created after loading)
└── [working_directory]/
    └── configs/
        └── mount_kb_definitions/  # Source configuration scripts
```

## Usage

### Basic Usage

Run the script without any parameters to perform a standard update check:

```bash
./script.py
```

### Force Update

Use the `--force-update` flag to force the update process regardless of current update flags:

```bash
./script.py --force-update
```

### Command Line Options

- `--force-update`: Forces the update process by passing `True` to `load_configuration_scripts`. Without this flag, `False` is passed.

## Process Flow

1. **Initialization**
   - Parses command line arguments
   - Determines home directory and sets up file paths
   - Prepares configuration mappings

2. **Configuration Loading**
   - Calls `load_configuration_scripts()` with appropriate parameters
   - Checks if the current node is master or if updates are scheduled
   - Transfers configuration files if needed

3. **Validation**
   - Verifies that `start.py` exists in the target directory
   - Exits gracefully if no update is needed or if files aren't transferred

4. **Script Execution**
   - Runs the `start.py` script using subprocess
   - Handles execution errors and provides detailed error messages
   - Stops execution if the start script fails

5. **Configuration Scripts**
   - Executes knowledge base configuration scripts via `run_kb_config_scripts()`
   - Waits for 15 seconds (configurable timeout)

6. **Update Flag Reset**
   - Upon successful completion of all operations
   - Sets `update_flag` to `false` in `update.json`
   - Ensures the system won't repeat the update unnecessarily

## Configuration Files

### node_definition.json
Contains node-specific configuration and determines if the current node is a master node.

### update.json
Controls the update process with the following key parameter:
```json
{
  "update_flag": true
}
```
- `update_flag`: When `true`, triggers the update process. Automatically set to `false` after successful completion.

## Exit Codes

- `0`: Normal exit (no update needed or successful completion)
- Non-zero: Error occurred during execution

## Error Handling

The script includes comprehensive error handling for:
- Missing configuration files
- Failed script execution
- JSON parsing errors
- File I/O operations

Error messages are printed to stdout with descriptive information about the failure point.

## Functions

### `update_flag_to_false(update_file_path)`
Updates the `update_flag` to `false` in the update.json file after successful execution.

**Parameters:**
- `update_file_path`: Path to the update.json file

**Returns:**
- `True` if successful, `False` if an error occurs

### `main()`
Main execution function that orchestrates the entire update process.

## Dependencies

### External Modules

- **update_file_loader**: Must provide `load_configuration_scripts()` function
  - Expected signature: `load_configuration_scripts(config_file, update_file, config_map, update_dir, force_update)`
  
- **run_kb_config_scripts**: Must provide `run_kb_config_scripts()` function
  - Expected signature: `run_kb_config_scripts(update_dir, timeout)`

## Logging

The script provides console output for:
- Current operation status
- Directory paths being used
- Success/failure messages
- Error details when operations fail

## Security Considerations

- The script executes Python files from the configuration directory
- Ensure proper file permissions on configuration directories
- Validate the source of configuration scripts before deployment

## Troubleshooting

### "not master or no update scheduled"
- The current node is not designated as a master node, or
- No update is currently scheduled in update.json

### "Start script not found"
- The `start.py` file doesn't exist in `~/mount_kb_definitions/`
- Check if configuration files were properly transferred

### "Start script execution failed"
- The `start.py` script encountered an error
- Check the script's error output for specific issues

### "Could not reset update_flag"
- File permissions issue with update.json
- JSON formatting error in update.json

## License

[Specify your license here]

## Author

[Specify author information here]

## Version

[Specify version information here]

