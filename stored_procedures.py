"""
Stored procedure execution utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pyodbc
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Union
from config import STORED_PROCEDURES
from data_utils import pyodbc_rows_to_dataframe


class StoredProcedures:
    """Handle execution of stored procedures for ski resort data"""
    
    def __init__(self, conn: pyodbc.Connection):
        """
        Initialize stored procedure handler
        
        Args:
            conn: Active database connection
        """
        self.conn = conn
        self.procedures = STORED_PROCEDURES
    
    def execute_revenue(self, 
                       database: str,
                       group_no: int,
                       date_ini: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Revenue stored procedure (Shakudo_DMRGetRevenue)
        
        Args:
            database: Database/resort name (e.g., 'Purgatory', 'Snowbowl')
            group_no: Group number for the resort
            date_ini: Start date (datetime or string 'YYYY-MM-DD')
            date_end: End date (datetime or string 'YYYY-MM-DD HH:MM:SS')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Revenue data as DataFrame or list of tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['Revenue'], (database, group_no, date_ini, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll(self,
                       resort: str,
                       date_ini: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Payroll stored procedure (Shakudo_DMRGetPayroll)
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            date_ini: Start date (datetime or string 'YYYY-MM-DD')
            date_end: End date (datetime or string 'YYYY-MM-DD HH:MM:SS')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Payroll data as DataFrame or list of tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['PayrollContract'], (resort, date_ini, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll_salary(self,
                               resort: str,
                               return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Salary Payroll stored procedure (Shakudo_DMRGetPayrollSalary)
        Returns per-day payroll rate for each department for salaried employees.
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Salary payroll data as DataFrame with columns: deptcode, DepartmentTitle, rate_per_day
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['PayrollSalaryActive'], (resort,))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll_history(self,
                               resort: str,
                               date_ini: Union[datetime, str],
                               date_end: Union[datetime, str],
                               return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Payroll History stored procedure (Shakudo_DMRGetPayrollHistory)
        Returns historical payroll totals for departments for date ranges older than 7 days.
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            date_ini: Start date (datetime or string 'YYYY-MM-DD')
            date_end: End date (datetime or string 'YYYY-MM-DD HH:MM:SS')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Historical payroll data as DataFrame with columns: department, total
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['PayrollSalaryHistory'], (resort, date_ini, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_visits(self,
                      resort: str,
                      date_ini: Union[datetime, str],
                      date_end: Union[datetime, str],
                      return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Visits stored procedure (Shakudo_DMRGetVists)
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            date_ini: Start date (datetime or string 'YYYY-MM-DD')
            date_end: End date (datetime or string 'YYYY-MM-DD HH:MM:SS')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Visit data as DataFrame or list of tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['Visits'], (resort, date_ini, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_weather(self,
                       resort: str,
                       date_ini: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Weather/Snow stored procedure (Shakudo_GetSnow)
        
        Args:
            resort: Resort name (e.g., 'Purgatory', 'Snowbowl')
            date_ini: Start date (datetime or string 'YYYY-MM-DD')
            date_end: End date (datetime or string 'YYYY-MM-DD HH:MM:SS')
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Weather/snow data as DataFrame or list of tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(self.procedures['Weather'], (resort, date_ini, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_custom(self,
                      procedure_name: str,
                      params: Tuple,
                      return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute a custom stored procedure
        
        Args:
            procedure_name: Full stored procedure execution string 
                          (e.g., 'exec MyProc @param1=?, @param2=?')
            params: Tuple of parameters to pass to the procedure
            return_dataframe: If True, return pd.DataFrame; if False, return raw rows
            
        Returns:
            Query results as DataFrame or list of tuples
        """
        cursor = self.conn.cursor()
        cursor.execute(procedure_name, params)
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()


# Convenience functions for backwards compatibility
def execute_revenue_proc(conn: pyodbc.Connection,
                        database: str,
                        group_no: int,
                        date_ini: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    """
    Execute Revenue stored procedure (convenience function)
    
    Args:
        conn: Active database connection
        database: Database/resort name
        group_no: Group number for the resort
        date_ini: Start date
        date_end: End date
        
    Returns:
        Revenue data as DataFrame
    """
    stored_procedures = StoredProcedures(conn)
    return stored_procedures.execute_revenue(database, group_no, date_ini, date_end)


def execute_payroll_proc(conn: pyodbc.Connection,
                        resort: str,
                        date_ini: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    """
    Execute Payroll stored procedure (convenience function)
    
    Args:
        conn: Active database connection
        resort: Resort name
        date_ini: Start date
        date_end: End date
        
    Returns:
        Payroll data as DataFrame
    """
    stored_procedures = StoredProcedures(conn)
    return stored_procedures.execute_payroll(resort, date_ini, date_end)


def execute_visits_proc(conn: pyodbc.Connection,
                       resort: str,
                       date_ini: Union[datetime, str],
                       date_end: Union[datetime, str]) -> pd.DataFrame:
    """
    Execute Visits stored procedure (convenience function)
    
    Args:
        conn: Active database connection
        resort: Resort name
        date_ini: Start date
        date_end: End date
        
    Returns:
        Visit data as DataFrame
    """
    stored_procedures = StoredProcedures(conn)
    return stored_procedures.execute_visits(resort, date_ini, date_end)


def execute_weather_proc(conn: pyodbc.Connection,
                        resort: str,
                        date_ini: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    """
    Execute Weather stored procedure (convenience function)
    
    Args:
        conn: Active database connection
        resort: Resort name
        date_ini: Start date
        date_end: End date
        
    Returns:
        Weather data as DataFrame
    """
    stored_procedures = StoredProcedures(conn)
    return stored_procedures.execute_weather(resort, date_ini, date_end)

