#!/usr/bin/env python3
"""
Test driver for WatchDog class
"""

import json
import time
from typing import Dict, Optional

import sys


# Fixed version of WatchDog class for testing
class WatchDog:
    def __init__(self, status_writer, get_status_data, timeout: int):
        self.status_writer = status_writer
        self.get_status_data = get_status_data
        self.timeout = timeout
        self.status_data = {}
        self.status_data["current_time"] = time.time()
        self.status_data["timeout"] = timeout
        # Fixed: call status_writer with dict instead of JSON string
        self.status_writer(self.status_data)

    def toggle_watchdog(self):
        status_data = self.get_status_data()
        if status_data is None or not isinstance(status_data, dict):
            status_data = {}
            
        if "timeout" not in status_data:
            status_data["timeout"] = self.timeout
        status_data["current_time"] = time.time()
        # Fixed: call status_writer instead of status_write
        self.status_writer(status_data)

    def check_watchdog(self):
        status_data = self.get_status_data()
        if status_data is None or not isinstance(status_data, dict):
            return False
        if "timeout" not in status_data:
            return False
        if "current_time" not in status_data:
            return False
        current_time = time.time()
        if current_time - status_data["current_time"] < status_data["timeout"]:
            return True
        return False


if __name__ == "__main__":
    from unittest.mock import Mock
    # Mock storage for testing
    class MockStorage:
        def __init__(self):
            self.data: Optional[Dict] = None
        
        def write_status(self, status_data: Dict) -> None:
            """Mock status writer function"""
            print(f"Writing status: {status_data}")
            self.data = status_data.copy()
        
        def get_status(self) -> Optional[Dict]:
            """Mock status getter function"""
            print(f"Getting status: {self.data}")
            return self.data.copy() if self.data else None

    def test_watchdog_initialization():
        """Test WatchDog initialization"""
        print("\n=== Testing WatchDog Initialization ===")
    
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=5
        )
        
        # Check if status was written during initialization
        assert storage.data is not None, "Status should be written during initialization"
        assert "current_time" in storage.data, "Status should contain current_time"
        assert "timeout" in storage.data, "Status should contain timeout"
        assert storage.data["timeout"] == 5, "Timeout should be set correctly"
        
        print("✓ Initialization test passed")

    def test_toggle_watchdog():
        """Test toggle_watchdog functionality"""
        print("\n=== Testing toggle_watchdog ===")
        
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=3
        )
        
        # Get initial time
        initial_time = storage.data["current_time"]
        
        # Wait a bit and toggle
        time.sleep(0.1)
        watchdog.toggle_watchdog()
        
        # Check if time was updated
        assert storage.data["current_time"] > initial_time, "Time should be updated after toggle"
        assert storage.data["timeout"] == 3, "Timeout should remain the same"
        
        print("✓ toggle_watchdog test passed")

    def test_check_watchdog_valid():
        """Test check_watchdog with valid (non-expired) watchdog"""
        print("\n=== Testing check_watchdog (valid) ===")
        
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=5
        )
        
        # Should be valid immediately after initialization
        result = watchdog.check_watchdog()
        assert result == True, "Watchdog should be valid immediately after initialization"
        
        print("✓ check_watchdog (valid) test passed")

    def test_check_watchdog_expired():
        """Test check_watchdog with expired watchdog"""
        print("\n=== Testing check_watchdog (expired) ===")
        
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=1  # 1 second timeout
        )
        
        # Wait for timeout to expire
        print("Waiting for watchdog to expire...")
        time.sleep(1.5)
        
        result = watchdog.check_watchdog()
        assert result == False, "Watchdog should be expired after timeout"
        
        print("✓ check_watchdog (expired) test passed")

    def test_check_watchdog_no_data():
        """Test check_watchdog with no status data"""
        print("\n=== Testing check_watchdog (no data) ===")
        
        storage = MockStorage()
        # Don't initialize storage with any data
        storage.data = None
        
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=5
        )
        
        # Manually clear the data to simulate no data scenario
        storage.data = None
        
        result = watchdog.check_watchdog()
        assert result == False, "Watchdog should return False when no data available"
        
        print("✓ check_watchdog (no data) test passed")

    def test_check_watchdog_invalid_data():
        """Test check_watchdog with invalid status data"""
        print("\n=== Testing check_watchdog (invalid data) ===")
        
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=5
        )
        
        # Test with missing timeout
        storage.data = {"current_time": time.time()}
        result = watchdog.check_watchdog()
        assert result == False, "Should return False when timeout is missing"
        
        # Test with missing current_time
        storage.data = {"timeout": 5}
        result = watchdog.check_watchdog()
        assert result == False, "Should return False when current_time is missing"
        
        print("✓ check_watchdog (invalid data) test passed")

    def test_watchdog_workflow():
        """Test complete watchdog workflow"""
        print("\n=== Testing Complete Workflow ===")
        
        storage = MockStorage()
        watchdog = WatchDog(
            status_writer=storage.write_status,
            get_status_data=storage.get_status,
            timeout=2
        )
        
        # Initial check - should be valid
        assert watchdog.check_watchdog() == True, "Initial check should be valid"
        
        # Wait and toggle before timeout
        time.sleep(0.5)
        watchdog.toggle_watchdog()
        assert watchdog.check_watchdog() == True, "Should still be valid after toggle"
        
        # Wait again and toggle
        time.sleep(0.5)
        watchdog.toggle_watchdog()
        assert watchdog.check_watchdog() == True, "Should still be valid after second toggle"
        
        # Wait for timeout without toggling
        time.sleep(2.5)
        assert watchdog.check_watchdog() == False, "Should be expired after timeout"
        
        # Toggle to reset
        watchdog.toggle_watchdog()
        assert watchdog.check_watchdog() == True, "Should be valid again after toggle"
        
        print("✓ Complete workflow test passed")

    def run_all_tests():
        """Run all tests"""
        print("Starting WatchDog tests...")
        
        try:
            test_watchdog_initialization()
            test_toggle_watchdog()
            test_check_watchdog_valid()
            test_check_watchdog_expired()
            test_check_watchdog_no_data()
            test_check_watchdog_invalid_data()
            test_watchdog_workflow()
            
            print("\n" + "="*50)
            print("🎉 ALL TESTS PASSED! 🎉")
            print("="*50)
            
        except AssertionError as e:
            print(f"\n❌ TEST FAILED: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\n💥 UNEXPECTED ERROR: {e}")
            sys.exit(1)


    run_all_tests()