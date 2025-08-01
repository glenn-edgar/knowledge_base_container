"""
Watch Dog Module

This module provides a watchdog timer implementation for monitoring system health
and detecting timeouts. It integrates with external status logging systems to
track and report system activity.

Main Components:
- WatchDog: Main watchdog timer class for monitoring system health
"""

from .watch_dog import WatchDog

__all__ = [
    # Main watchdog component
    'WatchDog'
]

__version__ = "1.0.0"
__author__ = "Knowledge Base Container"
__description__ = "Watchdog timer for system health monitoring and timeout detection"
