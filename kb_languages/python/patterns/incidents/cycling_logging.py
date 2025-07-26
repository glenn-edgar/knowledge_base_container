import json
from datetime import datetime, timezone, timedelta
from collections import deque
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Union, Tuple
import statistics

@dataclass
class AlarmSample:
    """Single alarm sample from JSON stream."""
    state: bool
    raw_data: Optional[Dict] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        result = {
            'state': self.state
        }
        if self.raw_data:
            result['raw_data'] = self.raw_data
        return result

@dataclass
class TimestampedAlarmSample:
    """Alarm sample with timestamp added during processing."""
    timestamp: datetime
    sample: AlarmSample
    
    @property
    def state(self) -> bool:
        return self.sample.state
    
    @property
    def raw_data(self) -> Optional[Dict]:
        return self.sample.raw_data
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        result = {
            'timestamp': self.timestamp.isoformat(),
            'state': self.sample.state
        }
        if self.sample.raw_data:
            result['raw_data'] = self.sample.raw_data
        return result

@dataclass
class StateTransition:
    """Represents a state change between two samples."""
    from_sample: TimestampedAlarmSample
    to_sample: TimestampedAlarmSample
    duration_seconds: float
    
    @property
    def from_state(self) -> bool:
        return self.from_sample.state
    
    @property
    def to_state(self) -> bool:
        return self.to_sample.state
    
    @property
    def is_state_change(self) -> bool:
        return self.from_state != self.to_state

@dataclass
class CyclingAnalysis:
    """Results of cycling analysis on buffered samples."""
    buffer_size: int
    analysis_window_hours: float
    
    # Basic counts
    total_samples: int
    total_state_changes: int
    
    # Time window analysis
    state_changes_last_5min: int
    state_changes_last_15min: int
    state_changes_last_1hour: int
    
    # Duration analysis
    avg_active_duration_seconds: float
    avg_cleared_duration_seconds: float
    min_state_duration_seconds: float
    max_state_duration_seconds: float
    
    # Cycling metrics
    complete_cycles_count: int
    avg_cycle_duration_seconds: float
    shortest_cycle_seconds: float
    longest_cycle_seconds: float
    
    # Flapping detection
    is_flapping: bool
    flapping_score: float
    flapping_intensity: str  # "low", "medium", "high"
    
    # Time distribution
    time_active_percentage: float
    time_cleared_percentage: float
    
    # Recent activity
    current_state: bool
    time_in_current_state_seconds: float
    last_state_change_ago_seconds: float

class AlertCyclingBuffer:
    """
    Buffer-based alert cycling tracker that analyzes JSON stream data.
    Maintains a rolling buffer of the last N samples with Zulu timestamps.
    """
    
    def __init__(self, 
                 buffer_size: int = 1000,
                 flapping_threshold: int = 5,
                 flapping_window_minutes: int = 10):
        self.buffer_size = buffer_size
        self.flapping_threshold = flapping_threshold
        self.flapping_window_minutes = flapping_window_minutes
        
        # Rolling buffer of timestamped samples
        self.samples: deque[TimestampedAlarmSample] = deque(maxlen=buffer_size)
        
        # Cache for expensive calculations
        self._transitions_cache: Optional[List[StateTransition]] = None
        self._cache_valid_until: Optional[datetime] = None
        
    def add_json_sample(self, state: bool, alarm_data: Union[str, Dict]) -> None:
        """
        Add a new sample with explicit state and alarm data.
        
        Args:
            state: Boolean alarm state (True = active/alarm, False = cleared/normal)
            alarm_data: JSON data as string or dictionary containing optional fields
        
        Expected alarm_data format:
        {
            "host": "server1",
            "cpu_usage": 85.5,
            "threshold": 80.0
        }
        
        Or with embedded timestamp:
        {
            "timestamp": "2024-01-15T14:30:45.123Z",
            "host": "server1", 
            "cpu_usage": 85.5
        }
        
        If no timestamp is provided in alarm_data, current Zulu time will be used.
        """
        if isinstance(alarm_data, str):
            data = json.loads(alarm_data)
        else:
            data = alarm_data.copy()
            
        # Get timestamp - either from alarm_data or use current Zulu time
        timestamp_str = data.get('timestamp')
        if not timestamp_str:
            # Use current Zulu time if no timestamp provided
            timestamp = datetime.now(timezone.utc)
        else:
            # Parse provided timestamp
            if timestamp_str.endswith('Z'):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.fromisoformat(timestamp_str)
                
            # Ensure timezone aware
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            
        # Create alarm sample (without timestamp)
        alarm_sample = AlarmSample(
            state=state,
            raw_data=data
        )
        
        # Create timestamped sample
        timestamped_sample = TimestampedAlarmSample(
            timestamp=timestamp,
            sample=alarm_sample
        )
        
        # Add to buffer
        self.samples.append(timestamped_sample)
        
        # Invalidate cache
        self._transitions_cache = None
        
    def _get_transitions(self, force_refresh: bool = False) -> List[StateTransition]:
        """Get state transitions from buffered samples with caching."""
        now = datetime.now(timezone.utc)
        
        # Check cache validity (cache for 1 minute)
        if (not force_refresh and 
            self._transitions_cache is not None and 
            self._cache_valid_until and 
            now < self._cache_valid_until):
            return self._transitions_cache
            
        transitions = []
        if len(self.samples) < 2:
            self._transitions_cache = transitions
            self._cache_valid_until = now + timedelta(minutes=1)
            return transitions
            
        # Calculate transitions between consecutive samples
        for i in range(len(self.samples) - 1):
            current_sample = self.samples[i]
            next_sample = self.samples[i + 1]
            
            duration = (next_sample.timestamp - current_sample.timestamp).total_seconds()
            
            transition = StateTransition(
                from_sample=current_sample,
                to_sample=next_sample,
                duration_seconds=duration
            )
            transitions.append(transition)
            
        self._transitions_cache = transitions
        self._cache_valid_until = now + timedelta(minutes=1)
        return transitions
    
    def _get_samples_in_window(self, window_minutes: int) -> List[TimestampedAlarmSample]:
        """Get samples within the last N minutes."""
        if not self.samples:
            return []
            
        cutoff_time = self.samples[-1].timestamp - timedelta(minutes=window_minutes)
        return [s for s in self.samples if s.timestamp >= cutoff_time]
    
    def _count_state_changes_in_window(self, window_minutes: int) -> int:
        """Count state changes in the last N minutes."""
        window_samples = self._get_samples_in_window(window_minutes)
        if len(window_samples) < 2:
            return 0
            
        changes = 0
        for i in range(len(window_samples) - 1):
            if window_samples[i].state != window_samples[i + 1].state:
                changes += 1
        return changes
    
    def _get_state_durations(self) -> Dict[bool, List[float]]:
        """Get durations spent in each state."""
        durations = {True: [], False: []}
        
        transitions = self._get_transitions()
        for transition in transitions:
            durations[transition.from_sample.state].append(transition.duration_seconds)
                
        return durations
    
    def _find_complete_cycles(self) -> List[Tuple[datetime, datetime, float]]:
        """
        Find complete cycles (ACTIVE -> CLEARED -> ACTIVE).
        Returns list of (start_time, end_time, duration_seconds).
        """
        cycles = []
        transitions = self._get_transitions()
        
        if len(transitions) < 2:
            return cycles
            
        i = 0
        while i < len(transitions) - 1:
            # Look for True -> False -> True pattern (active -> cleared -> active)
            if (transitions[i].from_state == True and
                transitions[i].to_state == False and
                i + 1 < len(transitions) and
                transitions[i + 1].from_state == False and
                transitions[i + 1].to_state == True):
                
                start_time = transitions[i].from_sample.timestamp
                end_time = transitions[i + 1].to_sample.timestamp
                duration = (end_time - start_time).total_seconds()
                
                cycles.append((start_time, end_time, duration))
                i += 2  # Skip to next potential cycle
            else:
                i += 1
                
        return cycles
    
    def _calculate_flapping_score(self) -> Tuple[float, str]:
        """Calculate flapping score and intensity."""
        if len(self.samples) < 2:
            return 0.0, "low"
            
        # Weight recent activity more heavily
        weights_and_windows = [
            (3.0, 5),    # 5 min window, high weight
            (2.0, 15),   # 15 min window, medium weight  
            (1.0, 60),   # 1 hour window, low weight
        ]
        
        weighted_score = 0.0
        max_possible_score = 0.0
        
        for weight, window_min in weights_and_windows:
            changes = self._count_state_changes_in_window(window_min)
            weighted_score += changes * weight
            # Assume max reasonable is 20 changes per window for normalization
            max_possible_score += 20 * weight
            
        # Normalize to 0-100
        if max_possible_score > 0:
            score = min(100.0, (weighted_score / max_possible_score) * 100)
        else:
            score = 0.0
            
        # Determine intensity
        if score < 20:
            intensity = "low"
        elif score < 60:
            intensity = "medium"
        else:
            intensity = "high"
            
        return score, intensity
    
    def analyze_cycling(self) -> CyclingAnalysis:
        """Perform comprehensive cycling analysis on buffered samples."""
        if not self.samples:
            return self._empty_analysis()
            
        transitions = self._get_transitions()
        state_durations = self._get_state_durations()
        cycles = self._find_complete_cycles()
        
        # Time window analysis
        changes_5min = self._count_state_changes_in_window(5)
        changes_15min = self._count_state_changes_in_window(15)
        changes_1hour = self._count_state_changes_in_window(60)
        
        # Duration statistics
        all_durations = []
        for duration_list in state_durations.values():
            all_durations.extend(duration_list)
            
        active_durations = state_durations[True]
        cleared_durations = state_durations[False]
        
        # Cycle statistics
        cycle_durations = [cycle[2] for cycle in cycles]
        
        # Time distribution
        total_active_time = sum(active_durations)
        total_cleared_time = sum(cleared_durations)
        total_time = total_active_time + total_cleared_time
        
        # Current state analysis
        current_sample = self.samples[-1]
        current_state = current_sample.state
        
        # Time in current state
        time_in_current_state = 0.0
        last_change_ago = 0.0
        
        if len(transitions) > 0:
            # Find last state change
            for transition in reversed(transitions):
                if transition.is_state_change:
                    last_change_ago = (current_sample.timestamp - transition.to_sample.timestamp).total_seconds()
                    time_in_current_state = last_change_ago
                    break
        
        # Flapping analysis
        flapping_score, flapping_intensity = self._calculate_flapping_score()
        is_flapping = changes_5min >= self.flapping_threshold or flapping_score > 40
        
        # Analysis window
        if self.samples:
            analysis_window = (self.samples[-1].timestamp - self.samples[0].timestamp).total_seconds() / 3600
        else:
            analysis_window = 0.0
            
        return CyclingAnalysis(
            buffer_size=len(self.samples),
            analysis_window_hours=analysis_window,
            total_samples=len(self.samples),
            total_state_changes=sum(1 for t in transitions if t.is_state_change),
            state_changes_last_5min=changes_5min,
            state_changes_last_15min=changes_15min,
            state_changes_last_1hour=changes_1hour,
            avg_active_duration_seconds=statistics.mean(active_durations) if active_durations else 0.0,
            avg_cleared_duration_seconds=statistics.mean(cleared_durations) if cleared_durations else 0.0,
            min_state_duration_seconds=min(all_durations) if all_durations else 0.0,
            max_state_duration_seconds=max(all_durations) if all_durations else 0.0,
            complete_cycles_count=len(cycles),
            avg_cycle_duration_seconds=statistics.mean(cycle_durations) if cycle_durations else 0.0,
            shortest_cycle_seconds=min(cycle_durations) if cycle_durations else 0.0,
            longest_cycle_seconds=max(cycle_durations) if cycle_durations else 0.0,
            is_flapping=is_flapping,
            flapping_score=flapping_score,
            flapping_intensity=flapping_intensity,
            time_active_percentage=(total_active_time / total_time * 100) if total_time > 0 else 0.0,
            time_cleared_percentage=(total_cleared_time / total_time * 100) if total_time > 0 else 0.0,
            current_state=current_state,
            time_in_current_state_seconds=time_in_current_state,
            last_state_change_ago_seconds=last_change_ago
        )
    
    def _empty_analysis(self) -> CyclingAnalysis:
        """Return empty analysis for when no samples exist."""
        return CyclingAnalysis(
            buffer_size=0,
            analysis_window_hours=0.0,
            total_samples=0,
            total_state_changes=0,
            state_changes_last_5min=0,
            state_changes_last_15min=0,
            state_changes_last_1hour=0,
            avg_active_duration_seconds=0.0,
            avg_cleared_duration_seconds=0.0,
            min_state_duration_seconds=0.0,
            max_state_duration_seconds=0.0,
            complete_cycles_count=0,
            avg_cycle_duration_seconds=0.0,
            shortest_cycle_seconds=0.0,
            longest_cycle_seconds=0.0,
            is_flapping=False,
            flapping_score=0.0,
            flapping_intensity="low",
            time_active_percentage=0.0,
            time_cleared_percentage=0.0,
            current_state=False,
            time_in_current_state_seconds=0.0,
            last_state_change_ago_seconds=0.0
        )
    
    def get_recent_samples(self, minutes: int = 60) -> List[TimestampedAlarmSample]:
        """Get samples from the last N minutes."""
        return self._get_samples_in_window(minutes)
    
    def export_buffer_as_json(self) -> str:
        """Export entire buffer as JSON string."""
        return json.dumps([sample.to_dict() for sample in self.samples], indent=2)
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get basic buffer statistics."""
        if not self.samples:
            return {"buffer_empty": True}
            
        return {
            "buffer_size": len(self.samples),
            "buffer_capacity": self.buffer_size,
            "oldest_sample": self.samples[0].timestamp.isoformat(),
            "newest_sample": self.samples[-1].timestamp.isoformat(),
            "time_span_hours": (self.samples[-1].timestamp - self.samples[0].timestamp).total_seconds() / 3600
        }

# Example usage and testing
if __name__ == "__main__":
    # Create buffer
    buffer = AlertCyclingBuffer(buffer_size=100, flapping_threshold=3)
    
    # Method 1: State and data without timestamp (will use current Zulu time)
    buffer.add_json_sample(False, {"host": "server1", "cpu": 75.0})  # cleared/inactive
    
    import time
    time.sleep(0.1)  # Small delay to show time progression
    
    buffer.add_json_sample(True, {"host": "server1", "cpu": 85.0})   # active
    
    # Method 2: State and data with embedded timestamp
    base_time = datetime.now(timezone.utc)
    
    buffer.add_json_sample(False, {
        "timestamp": (base_time + timedelta(minutes=2)).isoformat().replace('+00:00', 'Z'), 
        "host": "server1", 
        "cpu": 78.0
    })
    
    buffer.add_json_sample(True, {
        "timestamp": (base_time + timedelta(minutes=3)).isoformat().replace('+00:00', 'Z'), 
        "host": "server1", 
        "cpu": 87.0
    })
    
    buffer.add_json_sample(False, {
        "timestamp": (base_time + timedelta(minutes=4)).isoformat().replace('+00:00', 'Z'), 
        "host": "server1", 
        "cpu": 76.0
    })
    
    # Analyze cycling
    analysis = buffer.analyze_cycling()
    
    print(f"Total samples: {analysis.total_samples}")
    print(f"State changes: {analysis.total_state_changes}")
    print(f"Flapping: {analysis.is_flapping} (score: {analysis.flapping_score:.1f})")
    print(f"Current state: {analysis.current_state}")
    print(f"Buffer stats:")
    print(json.dumps(buffer.get_buffer_stats(), indent=2))