import json
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, Any, Optional
import cycling_logging

class Incident:
    """
    Manages incident alerts with cycling detection and logging.
    
    Integrates with external logging systems and provides flapping detection
    for alert management.
    """
    
    def __init__(self, 
                 stream_logger: Callable[[Dict], None],
                 status_logger: Callable[[Dict], None], 
                 get_status_data: Callable[[], Dict],
                 buffer_size: int = 100,
                 flapping_threshold: int = 5,
                 flapping_window_minutes: int = 60):
        """
        Initialize the Incident manager.
        
        Args:
            stream_logger: Function to log streaming alert data
            status_logger: Function to log status and analysis data  
            get_status_data: Function to retrieve current status configuration
            buffer_size: Size of the cycling analysis buffer
            flapping_threshold: Number of state changes to trigger flapping detection
            flapping_window_minutes: Time window for flapping detection
        """
        self.stream_logger = stream_logger
        self.status_logger = status_logger
        self.get_status_data = get_status_data
        
        self.cycling_buffer = cycling_logging.AlertCyclingBuffer(
            buffer_size=buffer_size, 
            flapping_threshold=flapping_threshold, 
            flapping_window_minutes=flapping_window_minutes
        )
        
        self.alert_state = False
        self.last_logged_state = None
    
    def add_alert(self, state: bool, data: Dict[str, Any]) -> None:
        """
        Process a new alert with state and data.
        
        Args:
            state: Alert state (True = active, False = cleared)
            data: Alert data dictionary
        """
        # Get current status configuration
        status_config = self.get_status_data()
        ignore_alert = status_config.get("ignore_alert", False)
        
        # Add to cycling buffer for analysis
        self.cycling_buffer.add_json_sample(state, data)
        
        # Stream logging - only log state changes when not ignoring
        if not ignore_alert and state != self.last_logged_state:
            stream_data = data.copy()
            stream_data['alert_state'] = state
            stream_data['timestamp'] = datetime.now(timezone.utc).isoformat()
            self.stream_logger(stream_data)
            self.last_logged_state = state
        
        # Update current state
        self.alert_state = state
        
        # Perform cycling analysis
        analysis = self.cycling_buffer.analyze_cycling()
        
        # Prepare status data
        status_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'ignore_alert': ignore_alert,
            'alert_state': state,
            'state_changed': state != self.last_logged_state,
            'analysis': {
                'total_samples': analysis.total_samples,
                'total_state_changes': analysis.total_state_changes,
                'is_flapping': analysis.is_flapping,
                'flapping_score': analysis.flapping_score,
                'flapping_intensity': analysis.flapping_intensity,
                'current_state': analysis.current_state,
                'time_in_current_state_seconds': analysis.time_in_current_state_seconds,
                'state_changes_last_5min': analysis.state_changes_last_5min,
                'state_changes_last_15min': analysis.state_changes_last_15min,
                'state_changes_last_1hour': analysis.state_changes_last_1hour
            },
            'alert_data': data
        }
        
        # Log status
        self.status_logger(status_data)
    
    def get_current_analysis(self) -> cycling_logging.CyclingAnalysis:
        """Get the current cycling analysis."""
        return self.cycling_buffer.analyze_cycling()
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get buffer statistics."""
        return self.cycling_buffer.get_buffer_stats()


# Test Driver
if __name__ == "__main__":
    import time
    
    # Mock configuration storage
    config_storage = {
        "ignore_alert": False
    }
    
    # Mock loggers and storage
    stream_logs = []
    status_logs = []
    
    def mock_stream_logger(data: Dict) -> None:
        """Mock stream logger that stores data."""
        log_entry = {
            'type': 'stream',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': data
        }
        stream_logs.append(log_entry)
        print(f"STREAM LOG: {json.dumps(data, indent=2)}")
    
    def mock_status_logger(data: Dict) -> None:
        """Mock status logger that stores data."""
        log_entry = {
            'type': 'status', 
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': data
        }
        status_logs.append(log_entry)
        print(f"STATUS LOG: Alert={data['alert_state']}, Flapping={data['analysis']['is_flapping']}, Score={data['analysis']['flapping_score']:.1f}")
    
    def mock_get_status_data() -> Dict:
        """Mock status data retriever."""
        return config_storage.copy()
    
    # Create incident manager
    incident = Incident(
        stream_logger=mock_stream_logger,
        status_logger=mock_status_logger,
        get_status_data=mock_get_status_data,
        buffer_size=50,
        flapping_threshold=3,
        flapping_window_minutes=10
    )
    
    print("=== Testing Incident Manager ===\n")
    
    # Test 1: Normal alert cycle
    print("1. Testing normal alert cycle:")
    incident.add_alert(False, {"sensor": "cpu", "value": 70.0, "host": "server1"})
    time.sleep(0.1)
    incident.add_alert(True, {"sensor": "cpu", "value": 85.0, "host": "server1"})
    time.sleep(0.1)
    incident.add_alert(False, {"sensor": "cpu", "value": 72.0, "host": "server1"})
    print()
    
    # Test 2: Flapping scenario
    print("2. Testing flapping detection:")
    for i in range(6):
        state = i % 2 == 0  # Alternate True/False
        incident.add_alert(state, {"sensor": "memory", "value": 80.0 + i, "host": "server2"})
        time.sleep(0.05)
    print()
    
    # Test 3: Ignore alerts mode
    print("3. Testing ignore alerts mode:")
    config_storage["ignore_alert"] = True
    incident.add_alert(True, {"sensor": "disk", "value": 95.0, "host": "server3"})
    incident.add_alert(False, {"sensor": "disk", "value": 80.0, "host": "server3"})
    print()
    
    # Test 4: Re-enable alerts
    print("4. Re-enabling alerts:")
    config_storage["ignore_alert"] = False
    incident.add_alert(True, {"sensor": "network", "value": 90.0, "host": "server4"})
    print()
    
    # Test 5: Analysis summary
    print("5. Final analysis:")
    analysis = incident.get_current_analysis()
    buffer_stats = incident.get_buffer_stats()
    
    print(f"Total samples processed: {analysis.total_samples}")
    print(f"Total state changes: {analysis.total_state_changes}")
    print(f"Currently flapping: {analysis.is_flapping}")
    print(f"Flapping score: {analysis.flapping_score:.1f}")
    print(f"Current state: {'ACTIVE' if analysis.current_state else 'CLEARED'}")
    print(f"Time in current state: {analysis.time_in_current_state_seconds:.1f} seconds")
    print()
    
    # Test 6: Log summaries
    print("6. Log summaries:")
    print(f"Stream log entries: {len(stream_logs)}")
    print(f"Status log entries: {len(status_logs)}")
    
    print("\nStream log sample:")
    if stream_logs:
        print(json.dumps(stream_logs[-1], indent=2))
    
    print("\nStatus log sample:")
    if status_logs:
        sample_status = status_logs[-1]['data']
        print(f"  Alert State: {sample_status['alert_state']}")
        print(f"  Ignore Mode: {sample_status['ignore_alert']}")
        print(f"  Flapping: {sample_status['analysis']['is_flapping']}")
        print(f"  Score: {sample_status['analysis']['flapping_score']:.1f}")
    
    print("\n=== Test Complete ===")
           
    