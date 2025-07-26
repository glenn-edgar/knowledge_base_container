#!/bin/bash

# Zombie Process Management Script
# This script monitors and cleans up zombie processes

# Configuration
CHECK_INTERVAL=30  # Check every 30 seconds
LOG_FILE="/var/log/zombie_killer.log"
MAX_ZOMBIES_THRESHOLD=10  # Alert if more than this many zombies

# Function to log with timestamp
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" | tee -a "$LOG_FILE"
}

# Function to count zombie processes
count_zombies() {
    ps aux | awk '$8 ~ /^Z/ { count++ } END { print count+0 }'
}

# Function to list zombie processes with details
list_zombies() {
    echo "Current zombie processes:"
    ps aux | awk '$8 ~ /^Z/ { printf "  PID: %-8s PPID: %-8s CMD: %s\n", $2, $3, $11 }'
}

# Function to get parent processes of zombies
get_zombie_parents() {
    ps -eo pid,ppid,stat,comm | awk '$3 ~ /^Z/ { print $2 }' | sort -u
}

# Function to attempt zombie cleanup
cleanup_zombies() {
    local zombie_count=$(count_zombies)
    
    if [ "$zombie_count" -eq 0 ]; then
        return 0
    fi
    
    log_message "Found $zombie_count zombie processes"
    list_zombies | tee -a "$LOG_FILE"
    
    # Get unique parent PIDs of zombie processes
    local parent_pids=($(get_zombie_parents))
    
    if [ ${#parent_pids[@]} -eq 0 ]; then
        log_message "No parent processes found for zombies"
        return 1
    fi
    
    log_message "Found ${#parent_pids[@]} parent processes of zombies: ${parent_pids[*]}"
    
    # Try to signal parent processes to clean up their children
    for ppid in "${parent_pids[@]}"; do
        if [ "$ppid" -ne 1 ] && kill -0 "$ppid" 2>/dev/null; then
            log_message "Sending SIGCHLD to parent process $ppid"
            kill -CHLD "$ppid" 2>/dev/null
            sleep 2
            
            # If that doesn't work, try SIGTERM
            if [ $(count_zombies) -gt 0 ]; then
                log_message "SIGCHLD didn't work, trying SIGTERM on parent $ppid"
                kill -TERM "$ppid" 2>/dev/null
                sleep 5
            fi
        fi
    done
    
    # Check if cleanup was successful
    local remaining_zombies=$(count_zombies)
    if [ "$remaining_zombies" -lt "$zombie_count" ]; then
        log_message "Successfully cleaned up $((zombie_count - remaining_zombies)) zombies"
    else
        log_message "Warning: Unable to clean up zombies. Manual intervention may be required."
    fi
    
    return "$remaining_zombies"
}

# Function to send alerts for excessive zombies
check_zombie_threshold() {
    local zombie_count=$(count_zombies)
    
    if [ "$zombie_count" -gt "$MAX_ZOMBIES_THRESHOLD" ]; then
        log_message "ALERT: High zombie count ($zombie_count) exceeds threshold ($MAX_ZOMBIES_THRESHOLD)"
        
        # Send system notification if available
        if command -v notify-send >/dev/null 2>&1; then
            notify-send "Zombie Alert" "High zombie process count: $zombie_count"
        fi
        
        # Log to syslog if available
        if command -v logger >/dev/null 2>&1; then
            logger -t zombie_killer "High zombie count: $zombie_count processes"
        fi
        
        return 1
    fi
    
    return 0
}

# Function to show zombie statistics
show_stats() {
    local zombie_count=$(count_zombies)
    echo "=== Zombie Process Statistics ==="
    echo "Current zombies: $zombie_count"
    echo "Threshold: $MAX_ZOMBIES_THRESHOLD"
    echo "Check interval: ${CHECK_INTERVAL}s"
    echo "Log file: $LOG_FILE"
    
    if [ "$zombie_count" -gt 0 ]; then
        echo ""
        list_zombies
    fi
}

# Function for cleanup on exit
cleanup_on_exit() {
    log_message "Zombie killer script stopping"
    exit 0
}

# Main execution based on command line arguments
case "${1:-monitor}" in
    "monitor")
        # Continuous monitoring mode
        log_message "Starting zombie process monitor (PID: $$)"
        log_message "Check interval: ${CHECK_INTERVAL}s, Threshold: $MAX_ZOMBIES_THRESHOLD"
        
        trap cleanup_on_exit SIGTERM SIGINT
        
        while true; do
            cleanup_zombies
            check_zombie_threshold
            sleep "$CHECK_INTERVAL"
        done
        ;;
        
    "check")
        # One-time check and cleanup
        echo "Performing one-time zombie check..."
        show_stats
        cleanup_zombies
        ;;
        
    "stats")
        # Show current statistics
        show_stats
        ;;
        
    "clean")
        # Force cleanup attempt
        echo "Attempting to clean up zombie processes..."
        cleanup_zombies
        echo "Cleanup attempt completed"
        show_stats
        ;;
        
    "help"|"-h"|"--help")
        echo "Zombie Process Killer Script"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  monitor  - Continuously monitor and clean zombies (default)"
        echo "  check    - One-time check and cleanup"
        echo "  stats    - Show current zombie statistics"
        echo "  clean    - Force cleanup attempt"
        echo "  help     - Show this help message"
        echo ""
        echo "Configuration:"
        echo "  Check interval: ${CHECK_INTERVAL}s"
        echo "  Alert threshold: $MAX_ZOMBIES_THRESHOLD zombies"
        echo "  Log file: $LOG_FILE"
        ;;
        
    *)
        echo "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac

