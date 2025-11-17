"""
Database connection utilities for MCP Database
Mountain Capital Partners - Ski Resort Data Analysis
"""

import pyodbc
from typing import Optional
from config import DatabaseConfig


class DatabaseConnection:
    """Manages database connections and basic operations"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database connection manager
        
        Args:
            config: DatabaseConfig object. If None, uses default configuration.
        """
        self.config = config or DatabaseConfig()
        self.conn: Optional[pyodbc.Connection] = None
    
    def connect(self) -> pyodbc.Connection:
        """
        Establish connection to the SQL Server database
        
        Returns:
            pyodbc.Connection: Active database connection
            
        Raises:
            Exception: If connection fails
        """
        try:
            connection_string = self.config.get_connection_string()
            self.conn = pyodbc.connect(connection_string)
            print("Connection successful!")
            return self.conn
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Connection closed.")
    
    def get_connection(self) -> pyodbc.Connection:
        """
        Get the current connection, establishing one if needed
        
        Returns:
            pyodbc.Connection: Active database connection
        """
        if self.conn is None:
            return self.connect()
        return self.conn
    
    def __enter__(self):
        """Context manager entry"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def create_connection(username: Optional[str] = None, 
                     password: Optional[str] = None,
                     server: Optional[str] = None,
                     port: Optional[int] = None,
                     database_name: Optional[str] = None) -> pyodbc.Connection:
    """
    Legacy function for backwards compatibility
    Create a database connection with custom parameters
    
    Args:
        username: Database username
        password: Database password
        server: Server address
        port: Server port
        database_name: Database name
        
    Returns:
        pyodbc.Connection: Active database connection
    """
    config = DatabaseConfig()
    
    # Override config with provided parameters
    if username:
        config.username = username
    if password:
        config.password = password
    if server:
        config.server = server
    if port:
        config.port = port
    if database_name:
        config.database_name = database_name
    
    db = DatabaseConnection(config)
    return db.connect()

