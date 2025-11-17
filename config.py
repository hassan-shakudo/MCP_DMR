"""
Configuration module for MCP Database Connection
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
from typing import Dict, List


class DatabaseConfig:
    """Database connection configuration"""
    
    def __init__(self):
        # Database credentials - prefer environment variables for security
        self.username = os.getenv('MCP_DB_USERNAME', 'shakudo')
        self.password = os.getenv('MCP_DB_PASSWORD', '6?jsV4Mb{&1)q34v')
        self.server = os.getenv('MCP_DB_SERVER', '63.158.251.204')
        self.port = int(os.getenv('MCP_DB_PORT', '1433'))
        self.database_name = os.getenv('MCP_DB_NAME', 'siriusware')
        
        # ODBC driver configuration
        self.driver = 'ODBC Driver 18 for SQL Server'
        self.encrypt = 'yes'
        self.trust_server_certificate = 'yes'
        self.tls_version = '1.1'
    
    def get_connection_string(self) -> str:
        """Generate ODBC connection string"""
        return (
            f'DRIVER={{{self.driver}}};'
            f'SERVER={self.server};'
            f'DATABASE={self.database_name};'
            f'UID={self.username};'
            f'PWD={self.password};'
            f'Encrypt={self.encrypt};'
            f'TrustServerCertificate={self.trust_server_certificate};'
            f'TlsVersion={self.tls_version}'
        )


class ResortConfig:
    """Resort and stored procedure configuration"""
    
    # Available resorts
    RESORTS: List[str] = [
        'Snowbowl',
        'Purgatory', 
        'Brian Head',
        'Lee Canyon',
        'Nordic Valley',
        'Sipapu',
        'Willamette'
    ]
    
    # Group numbers for Purgatory database
    PURGATORY_GROUPS: Dict[int, str] = {
        46: '*PURGATORY',
        54: '*HESPERUS',
        59: '*SNOWCAT',
        67: '*SPIDER MOUNTAIN',
        70: '*DMMA',
        71: '*WILLAMETTE'
    }
    
    # Group numbers for MCP database
    MCP_GROUPS: Dict[int, str] = {
        9: '** PAJARITO',
        10: '** SANDIA',
        12: '** WILLAMETTE',
        13: '** AZ SNOWBOWL (temporary, only used week of 4th July 2025)'
    }
    
    # Stored procedure names
    STORED_PROCEDURES: Dict[str, str] = {
        'Revenue': 'exec Shakudo_DMRGetRevenue @database=?, @group_no=?, @date_ini=?, @date_end=?',
        'Payroll': 'exec Shakudo_DMRGetPayroll @resort=?, @date_ini=?, @date_end=?',
        'Visits': 'exec Shakudo_DMRGetVists @resort=?, @date_ini=?, @date_end=?',
        'Weather': 'exec Shakudo_GetSnow @resort=?, @date_ini=?, @date_end=?'
    }


class QueryConfig:
    """Query configuration and thresholds"""
    
    # Threshold for determining if a field is a primary key
    # (percentage of unique values relative to total rows)
    PRIMARY_KEY_THRESHOLD = 0.98
    
    # Revenue query account code range
    REVENUE_ACCOUNT_MIN = '40000'
    REVENUE_ACCOUNT_MAX = '49999'

