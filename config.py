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

    RESORT_MAPPING = [
    {"dbName": "Purgatory", "resortName": "PURGATORY", "groupNum": 46},
    {"dbName": "Purgatory", "resortName": "HESPERUS", "groupNum": 54},
    {"dbName": "Purgatory", "resortName": "SNOWCAT", "groupNum": 59},
    {"dbName": "Purgatory", "resortName": "SPIDER MOUNTAIN", "groupNum": 67},
    {"dbName": "Purgatory", "resortName": "DMMA", "groupNum": 70},
    {"dbName": "Purgatory", "resortName": "WILLAMETTE", "groupNum": 71},
    {"dbName": "MCP", "resortName": "PAJARITO", "groupNum": 9},
    {"dbName": "MCP", "resortName": "SANDIA", "groupNum": 10},
    {"dbName": "MCP", "resortName": "WILLAMETTE", "groupNum": 12},
    {"dbName": "Snowbowl", "resortName": "Snowbowl", "groupNum": -1},
    {"dbName": "Lee Canyon", "resortName": "Lee Canyon", "groupNum": -1},
    {"dbName": "Sipapu", "resortName": "Sipapu", "groupNum": -1},
    {"dbName": "Nordic", "resortName": "Nordic", "groupNum": -1},
    {"dbName": "Brian", "resortName": "Brian", "groupNum": -1},
]
    
    # Stored procedure names
    STORED_PROCEDURES: Dict[str, str] = {
        'Revenue': 'exec Shakudo_DMRGetRevenue @database=?, @group_no=?, @date_ini=?, @date_end=?',
        'Payroll': 'exec Shakudo_DMRGetPayroll @resort=?, @date_ini=?, @date_end=?',
        'Visits': 'exec Shakudo_DMRGetVists @resort=?, @date_ini=?, @date_end=?',
        'Weather': 'exec Shakudo_GetSnow @resort=?, @date_ini=?, @date_end=?'
    }

