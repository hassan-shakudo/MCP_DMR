"""
MCP Database Connection Module
Mountain Capital Partners - Ski Resort Data Analysis

A modular package for connecting to and querying the MCP ski resort database.
"""

__version__ = "1.0.0"
__author__ = "Mountain Capital Partners"

# Import main classes and functions for easy access
from .config import DatabaseConfig, ResortConfig, QueryConfig
from .db_connection import DatabaseConnection, create_connection
from .stored_procedures import (
    StoredProcedures,
    execute_revenue_proc,
    execute_payroll_proc,
    execute_visits_proc,
    execute_weather_proc
)
from .db_queries import (
    execute_query,
    execute_query_to_dataframe,
    map_db_tables_and_columns,
    check_field_is_key,
    check_table_dependencies,
    get_revenue_query
)
from .data_utils import (
    pyodbc_rows_to_dataframe,
    cursor_to_dataframe,
    drop_unnamed_columns,
    parse_markdown_table_no_unnamed
)
from .report_generator import ReportGenerator

__all__ = [
    # Config
    'DatabaseConfig',
    'ResortConfig',
    'QueryConfig',
    
    # Connection
    'DatabaseConnection',
    'create_connection',
    
    # Stored Procedures
    'StoredProcedures',
    'execute_revenue_proc',
    'execute_payroll_proc',
    'execute_visits_proc',
    'execute_weather_proc',
    
    # Queries
    'execute_query',
    'execute_query_to_dataframe',
    'map_db_tables_and_columns',
    'check_field_is_key',
    'check_table_dependencies',
    'get_revenue_query',
    
    # Data Utils
    'pyodbc_rows_to_dataframe',
    'cursor_to_dataframe',
    'drop_unnamed_columns',
    'parse_markdown_table_no_unnamed',
    
    # Report Generator
    'ReportGenerator',
]

