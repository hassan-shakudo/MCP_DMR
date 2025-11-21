"""
MCP Database Connection Module
Mountain Capital Partners - Ski Resort Data Analysis

A modular package for connecting to and querying the MCP ski resort database.
"""

__version__ = "1.0.0"
__author__ = "Mountain Capital Partners"

# Import main classes and functions for easy access
from .config import DatabaseConfig, ResortConfig
from .db_connection import DatabaseConnection, create_connection
from .stored_procedures import (
    StoredProcedures,
    execute_revenue_proc,
    execute_payroll_proc,
    execute_visits_proc,
    execute_weather_proc
)

from .report_generator import ReportGenerator

__all__ = [
    # Config
    'DatabaseConfig',
    'ResortConfig',
    
    # Connection
    'DatabaseConnection',
    'create_connection',
    
    # Stored Procedures
    'StoredProcedures',
    'execute_revenue_proc',
    'execute_payroll_proc',
    'execute_visits_proc',
    'execute_weather_proc',
    
    # Report Generator
    'ReportGenerator',
]

