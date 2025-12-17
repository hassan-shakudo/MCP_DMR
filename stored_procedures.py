"""
Stored procedure execution utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pyodbc
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Union
from config import STORED_PROCEDURES
from utils import pyodbc_rows_to_dataframe


class StoredProcedures:
    """Handle execution of stored procedures for ski resort data"""
    
    def __init__(self, connection: pyodbc.Connection):
        """
        Initialize stored procedure handler
        
        Args:
            connection: Active database connection
        """
        self.connection = connection
        self.procedures = STORED_PROCEDURES
    
    def execute_revenue(self, 
                       database: str,
                       group_number: int,
                       date_start: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Revenue stored procedure (Shakudo_DMRGetRevenue)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['Revenue'], (database, group_number, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll(self,
                       resort_name: str,
                       date_start: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Payroll stored procedure (Shakudo_DMRGetPayroll)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['PayrollContract'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll_salary(self,
                               resort_name: str,
                               date_start: Union[datetime, str],
                               date_end: Union[datetime, str],
                               return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Salary Payroll stored procedure (Shakudo_DMRGetPayrollSalary)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['PayrollSalaryActive'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_budget(self,
                      resort_name: str,
                      date_start: Union[datetime, str],
                      date_end: Union[datetime, str],
                      return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Budget stored procedure (Shakudo_DMRBudget)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['Budget'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_payroll_history(self,
                               resort_name: str,
                               date_start: Union[datetime, str],
                               date_end: Union[datetime, str],
                               return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Payroll History stored procedure (Shakudo_DMRGetPayrollHistory)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['PayrollSalaryHistory'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_visits(self,
                      resort_name: str,
                      date_start: Union[datetime, str],
                      date_end: Union[datetime, str],
                      return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Visits stored procedure (Shakudo_DMRGetVists)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['Visits'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_weather(self,
                       resort_name: str,
                       date_start: Union[datetime, str],
                       date_end: Union[datetime, str],
                       return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute the Weather/Snow stored procedure (Shakudo_GetSnow)
        """
        cursor = self.connection.cursor()
        cursor.execute(self.procedures['Weather'], (resort_name, date_start, date_end))
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()
    
    def execute_custom(self,
                      procedure_name: str,
                      parameters: Tuple,
                      return_dataframe: bool = True) -> Union[pd.DataFrame, List[Tuple]]:
        """
        Execute a custom stored procedure
        """
        cursor = self.connection.cursor()
        cursor.execute(procedure_name, parameters)
        
        if return_dataframe:
            return pyodbc_rows_to_dataframe(cursor)
        else:
            return cursor.fetchall()


# Convenience functions for backwards compatibility
def execute_revenue_proc(connection: pyodbc.Connection,
                        database: str,
                        group_number: int,
                        date_start: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    stored_procedures = StoredProcedures(connection)
    return stored_procedures.execute_revenue(database, group_number, date_start, date_end)


def execute_payroll_proc(connection: pyodbc.Connection,
                        resort_name: str,
                        date_start: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    stored_procedures = StoredProcedures(connection)
    return stored_procedures.execute_payroll(resort_name, date_start, date_end)


def execute_visits_proc(connection: pyodbc.Connection,
                       resort_name: str,
                       date_start: Union[datetime, str],
                       date_end: Union[datetime, str]) -> pd.DataFrame:
    stored_procedures = StoredProcedures(connection)
    return stored_procedures.execute_visits(resort_name, date_start, date_end)


def execute_weather_proc(connection: pyodbc.Connection,
                        resort_name: str,
                        date_start: Union[datetime, str],
                        date_end: Union[datetime, str]) -> pd.DataFrame:
    stored_procedures = StoredProcedures(connection)
    return stored_procedures.execute_weather(resort_name, date_start, date_end)
