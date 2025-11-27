"""
Configuration module for MCP Database Connection
Mountain Capital Partners - Ski Resort Data Analysis
"""

import os
from typing import Dict
from types import SimpleNamespace
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig:
    """Database connection configuration"""
    
    def __init__(self):
        # Database credentials - loaded from .env file or environment variables
        self.username = os.getenv('MCP_DB_USERNAME')
        self.password = os.getenv('MCP_DB_PASSWORD')
        self.server = os.getenv('MCP_DB_SERVER')
        self.port = int(os.getenv('MCP_DB_PORT', '1433'))
        self.database_name = os.getenv('MCP_DB_NAME')
        
        # Validate that required environment variables are set
        if not all([self.username, self.password, self.server, self.database_name]):
            raise ValueError(
                "Missing required database configuration. "
                "Please ensure .env file exists with MCP_DB_USERNAME, MCP_DB_PASSWORD, "
                "MCP_DB_SERVER, and MCP_DB_NAME set."
            )
        
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


# Stored procedure names
STORED_PROCEDURES: Dict[str, str] = {
    'Revenue': 'exec Shakudo_DMRGetRevenue @database=?, @group_no=?, @date_ini=?, @date_end=?',
    'PayrollContract': 'exec Shakudo_DMRGetPayroll @resort=?, @date_ini=?, @date_end=?',
    'PayrollSalaryActive': 'exec Shakudo_DMRGetPayrollSalary @resort=?',
    'PayrollSalaryHistory': 'exec Shakudo_DMRGetPayrollHistory @resort=?, @date_ini=?, @date_end=?',
    'Visits': 'exec Shakudo_DMRGetVists @resort=?, @date_ini=?, @date_end=?',
    'Weather': 'exec Shakudo_GetSnow @resort=?, @date_ini=?, @date_end=?'
}

# Resort mapping configuration
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

CandidateColumns = SimpleNamespace(
    # Snow/Weather Data Columns
    snow=['snow_24hrs', 'Snow24Hrs', 'Snow_24hrs'],
    baseDepth=['base_depth', 'BaseDepth', 'Base_Depth'],
    
    # Visits Data Columns
    location=['Location', 'location', 'Resort', 'resort'],
    visits=['Visits', 'visits', 'Count', 'count'],
    
    # Department Columns (used across Revenue, Payroll, Salary Payroll, History Payroll)
    department=[
        'Department', 'department', 
        'DepartmentCode', 'department_code', 
        'deptCode', 'DeptCode', 'dept_code',
        'Dept', 'dept'
    ],
    departmentTitle=[
        'DepartmentTitle', 'department_title', 
        'departmentTitle', 'DeptTitle', 'dept_title'
    ],
    
    # Revenue Data Columns
    revenue=['Revenue', 'revenue', 'Amount', 'amount'],
    
    # Payroll Data Columns
    payrollStartTime=['start_punchtime', 'StartPunchTime', 'StartTime'],
    payrollEndTime=['end_punchtime', 'EndPunchTime', 'EndTime'],
    payrollRate=['rate', 'Rate', 'HourlyRate'],
    
    # Salary Payroll Data Columns
    salaryDeptcode=['deptcode', 'DeptCode', 'dept_code', 'Department', 'department'],
    salaryRatePerDay=['rate_per_day', 'RatePerDay', 'Rate'],
    
    # History Payroll Data Columns
    historyDepartment=['department', 'Department', 'Dept', 'dept'],
    historyTotal=['total', 'Total', 'amount', 'Amount']
)

