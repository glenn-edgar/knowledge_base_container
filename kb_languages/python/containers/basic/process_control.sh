#!/bin/bash

# Configuration
CHECK_INTERVAL=10

# Process list with commands and parameters
# Format: "process_name:command with parameters"
PROCESSES=(
    "process_control:./process_control --config /etc/config.conf"
    "web_server:./web_server -p 8080 --log-level debug"
    "database:./db_daemon --data-dir /var/data --port 5432"
)

# Function to parse process configuration from command line
# Usage: ./script.sh "name1:command1 args" "name2:command2 args"
if [ $# -gt 0 ]; then
    PROCESSES=("$@")
fi

# Associative arrays to track PIDs, commands, and error logs
declare -A PROCESS_PIDS
declare -A PROCESS_COMMANDS
declare -A ERROR_LOGS

# Parse process configurations
for process_config in "${PROCESSES[@]}"; do
    # Split on first colon to separate name from command
    if [[ "$process_config" == *":"* ]]; then
        process_name="${process_config%%:*}"
        process_command="${process_config#*:}"
    else
        # If no colon, treat as both name and command
        process_name="$process_config"
        process_command="./$process_config"
    fi
    
    PROCESS_COMMANDS["$process_name"]="$process_command"
    ERROR_LOGS["$process_name"]="/tmp/${process_name}.err"
    
    echo "Configured: $process_name -> $process_command"
done

# Function to cleanup on script exit
cleanup() {
    echo "Cleaning up all processes..."
    for process_name in "${!PROCESS_COMMANDS[@]}"; do
        pid="${PROCESS_PIDS[$process_name]}"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $process_name (PID: $pid)"
            kill "$pid"
            wait "$pid" 2>/dev/null
        fi
    done
    exit 0
}

# Trap signals to cleanup properly
trap cleanup SIGTERM SIGINT

echo "Starting process monitor for ${#PROCESS_COMMANDS[@]} processes"
echo "Check interval: ${CHECK_INTERVAL}s"

while [ 1 ]
do
    for process_name in "${!PROCESS_COMMANDS[@]}"; do
        pid="${PROCESS_PIDS[$process_name]}"
        command="${PROCESS_COMMANDS[$process_name]}"
        error_log="${ERROR_LOGS[$process_name]}"
        
        # Check if process is currently running
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "$(date): $process_name is running (PID: $pid)"
        else
            # Process has stopped or never started
            if [ -n "$pid" ]; then
                echo "$(date): $process_name has stopped (was PID: $pid)"
                
                # Process any errors from the stopped process
                if [ -f "$error_log" ] && [ -s "$error_log" ]; then
                    echo "Processing errors from stopped $process_name"
                    if command -v error_logger >/dev/null 2>&1; then
                        ./error_logger "$error_log"
                    else
                        echo "Error log for $process_name:"
                        cat "$error_log"
                        echo "--- End of error log ---"
                    fi
                    # Clear the error log after processing
                    > "$error_log"
                fi
            fi
            
            # Start/restart the process
            echo "$(date): Starting $process_name with command: $command"
            
            # Execute the command with parameters
            eval "$command" 2> "$error_log" &
            new_pid=$!
            PROCESS_PIDS["$process_name"]=$new_pid
            
            # Verify the process started successfully
            sleep 2
            if kill -0 "$new_pid" 2>/dev/null; then
                echo "$(date): Successfully started $process_name with PID: $new_pid"
            else
                echo "$(date): Failed to start $process_name"
                PROCESS_PIDS["$process_name"]=""
                
                # Show immediate error if startup failed
                if [ -f "$error_log" ] && [ -s "$error_log" ]; then
                    echo "Startup error for $process_name:"
                    cat "$error_log"
                fi
            fi
        fi
    done
    
    sleep $CHECK_INTERVAL
done


