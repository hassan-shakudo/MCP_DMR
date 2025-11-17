"""
Data processing utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pandas as pd
import pyodbc
from typing import Optional


def get_substring_between(text: str, delimiter: str, occurrence: int = 1) -> Optional[str]:
    """
    Returns the substring between the N-th and (N+1)-th occurrence of a delimiter.
    
    Parameters:
        text: The string to search in.
        delimiter: The delimiter to search for.
        occurrence: Which occurrence to consider (default is 1, i.e., between first and second delimiter).
    
    Returns:
        Substring between the delimiters, or None if not found.
    """
    parts = text.split(delimiter)
    if len(parts) > occurrence:
        return parts[occurrence]
    return None


def parse_markdown_table_no_unnamed(md_table: str) -> pd.DataFrame:
    """
    Parse a Markdown-style table into a pandas DataFrame without creating 'Unnamed' columns.
    
    Args:
        md_table: Markdown formatted table string
        
    Returns:
        pd.DataFrame: Parsed table data
    """
    # Split lines and remove the separator line
    lines = [line for line in md_table.strip().splitlines() if not line.startswith('|---')]
    
    # Split each line by '|' and strip whitespace
    table_data = []
    for line in lines:
        # Remove leading/trailing pipes, then split
        row = [cell.strip() for cell in line.strip().strip('|').split('|')]
        table_data.append(row)
    
    # First row is headers
    headers = table_data[0]
    data = table_data[1:]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=headers)
    df = drop_unnamed_columns(df)
    
    return df


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops all columns from the DataFrame whose name contains 'Unnamed'.
    
    Parameters:
        df: The input DataFrame.
        
    Returns:
        DataFrame without columns containing 'Unnamed'.
    """
    # Keep only columns that do NOT contain 'Unnamed'
    df_clean = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]
    return df_clean


def pyodbc_rows_to_dataframe(cursor: pyodbc.Cursor) -> pd.DataFrame:
    """
    Convert pyodbc cursor results to a pandas DataFrame
    
    Args:
        cursor: pyodbc cursor with executed query
        
    Returns:
        pd.DataFrame: Query results as DataFrame
    """
    rows = cursor.fetchall()
    if not rows:
        return pd.DataFrame()
    
    columns = [col[0] for col in cursor.description]
    data = [tuple(row) for row in rows]  # Critical conversion
    return pd.DataFrame(data, columns=columns)


def cursor_to_dataframe(cursor: pyodbc.Cursor) -> pd.DataFrame:
    """
    Alias for pyodbc_rows_to_dataframe for better naming convention
    
    Args:
        cursor: pyodbc cursor with executed query
        
    Returns:
        pd.DataFrame: Query results as DataFrame
    """
    return pyodbc_rows_to_dataframe(cursor)

