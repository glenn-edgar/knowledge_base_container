"""
Incident Management Module

This module provides incident alert management with cycling detection and logging capabilities.
It integrates with external logging systems and provides flapping detection for alert management.

Main Components:
- Incident: Main incident manager class
- AlertCyclingBuffer: Buffer for analyzing alert cycling patterns
- CyclingAnalysis: Analysis results data structure
- Various supporting data classes for alarm samples and state transitions
"""

from .incidents import Incident

__version__ = "1.0.0"
__author__ = "Knowledge Base Container"
__description__ = "Incident alert management with cycling detection and logging"
