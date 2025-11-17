"""
Database query utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pandas as pd
import pyodbc
from typing import List, Dict, Optional
from tqdm import tqdm
from config import QueryConfig


def execute_query(conn: pyodbc.Connection, query: str) -> List:
    """
    Execute a SQL query and return the first column values
    
    Args:
        conn: Active database connection
        query: SQL query string
        
    Returns:
        List of values from the first column
    """
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    column_values = [row[0] for row in rows]
    return column_values


def execute_query_to_dataframe(conn: pyodbc.Connection, query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame
    
    Args:
        conn: Active database connection
        query: SQL query string
        
    Returns:
        pd.DataFrame with query results
    """
    return pd.read_sql(query, conn)


def map_db_tables_and_columns(conn: pyodbc.Connection) -> pd.DataFrame:
    """
    Returns a DataFrame with all tables and columns in the database.
    
    Args:
        conn: An open connection to the MSSQL database.
            
    Returns:
        pd.DataFrame with columns: 'table', 'column_name'
    """
    query = """
    SELECT TABLE_NAME AS [table], COLUMN_NAME AS column_name
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = DB_NAME()  -- current database
    ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    
    df = pd.read_sql(query, conn)
    return df


def check_field_is_key(conn: pyodbc.Connection, 
                       table_name: str, 
                       column_name: str, 
                       threshold: float = None) -> bool:
    """
    Check whether a given column in a table is a primary key or not.
    
    This function determines if a column is a key by checking if the ratio of
    unique values to total values exceeds a threshold (default 98%).
    
    Args:
        conn: Active database connection
        table_name: Name of the table
        column_name: Name of the column
        threshold: The percentage that the len of unique values should be of 
                  the total len of the column (default from QueryConfig)
        
    Returns:
        bool: True if field appears to be a primary key, False otherwise
    """
    if threshold is None:
        threshold = QueryConfig.PRIMARY_KEY_THRESHOLD
    
    query = f"SELECT {column_name} FROM {table_name}"
    result = execute_query(conn, query)
    
    # Check unique values
    result_set = set(result)
    ratio = len(result_set) / len(result) if len(result) > 0 else 0
    
    return ratio >= threshold


def check_table_dependencies(conn: pyodbc.Connection, 
                             db_mapping_df: Optional[pd.DataFrame] = None) -> Dict[str, List[str]]:
    """
    Analyze all tables in the database and identify primary key columns.
    
    This function runs through all tables and returns a dictionary of tables
    and their identified primary key columns.
    
    Args:
        conn: Active database connection
        db_mapping_df: Optional DataFrame with table/column mapping. 
                      If None, will call map_db_tables_and_columns()
        
    Returns:
        Dict mapping table names to lists of primary key column names
    """
    if db_mapping_df is None:
        db_mapping_df = map_db_tables_and_columns(conn)
    
    table_names = db_mapping_df['table'].unique()
    primary_key_columns_dict = {}
    primary_key_columns_counter = 0
    
    print('Analyzing table columns...')
    
    for ii, table_name in enumerate(table_names):
        table_columns = list(db_mapping_df[db_mapping_df['table'] == table_name]['column_name'])
        
        for jj, column_name in enumerate(table_columns):
            print(f'Analyzing table No.{ii+1} out of {len(table_names)} - '
                  f'column No.{jj+1} out of {len(table_columns)}', end='\r')
            
            if check_field_is_key(conn, table_name, column_name):
                if table_name not in primary_key_columns_dict:
                    primary_key_columns_dict[table_name] = []
                primary_key_columns_dict[table_name].append(column_name)
                primary_key_columns_counter += 1
    
    print('\nDONE.')
    print(f'{primary_key_columns_counter}\tPrimary key columns were found in '
          f'{len(primary_key_columns_dict)} tables')
    
    return primary_key_columns_dict


def get_revenue_query(database: str = 'Snowbowl', 
                      group_no: int = -1,
                      date_ini: str = '2024-03-05',
                      date_end: str = '2025-07-05 23:59:59') -> str:
    """
    Generate the complex revenue query with parameters
    
    Args:
        database: Resort database name (e.g., 'Snowbowl', 'Purgatory', 'MCP')
        group_no: Group number for shared databases (-1 for all groups)
        date_ini: Start date in 'YYYY-MM-DD' format
        date_end: End date in 'YYYY-MM-DD HH:MM:SS' format
        
    Returns:
        SQL query string with parameters embedded
    """
    query = f"""
declare @database nvarchar(20) = '{database}'
declare @group_no int = {group_no}
declare @date_ini datetime = '{date_ini}'
declare @date_end datetime = '{date_end}'

SELECT
d.title as DepartmentTitle,
r.*
FROM
(
	select 
	p.user_code as account, 
	p.user_code2 as department, 
	sum(revenue) as revenue
	from
	(
		select t.resort, 
		t.pr_ctr_1 as pr_ctr_no,
		sum(pcsplit_1) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_1 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_1

		union all
		
		select t.resort, 
		t.pr_ctr_2 as pr_ctr_no,
		sum(pcsplit_2) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_2 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_2

		union all
		
		select t.resort, 
		t.pr_ctr_3 as pr_ctr_no,
		sum(pcsplit_3) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_3 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_3
		
		union all
		
		select t.resort, 
		t.pr_ctr_4 as pr_ctr_no,
		sum(pcsplit_4) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_4 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_4
		
		union all
		
		select t.resort, 
		t.pr_ctr_5 as pr_ctr_no,
		sum(pcsplit_5) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_5 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_5
		
		union all
		
		select t.resort, 
		t.pr_ctr_6 as pr_ctr_no,
		sum(pcsplit_6) as revenue
		from transact t
		where date_time between @date_ini and @date_end
		and department <> '**TRANS**'
		and t.resort = @database
		and t.pcsplit_6 <> 0
		and (
				@group_no = -1 OR 
				(t.salespoint in (select salespoint from sp_link where resort = @database and group_no = @group_no))
			)
		group by t.resort, t.pr_ctr_6
	) t
	left join prof_ctr p on t.resort = p.resort and t.pr_ctr_no = p.pr_ctr_no
	where p.user_code >= '{QueryConfig.REVENUE_ACCOUNT_MIN}' and p.user_code <= '{QueryConfig.REVENUE_ACCOUNT_MAX}'
	group by p.user_code, p.user_code2
	having sum(revenue) <> 0
) r
left join intacct.dbo.Department d on d.DepartmentId = r.department
"""
    return query

