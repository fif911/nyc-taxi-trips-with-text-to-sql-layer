"""
Custom Athena SQL Runner for Vanna 2.0

This module provides a custom SQL runner for AWS Athena that integrates with
Vanna 2.0's Tool system.
"""
from typing import Any, Dict, List, Optional, Tuple
import re
import asyncio
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import boto3
from config import Config


class AthenaRunner:
    """
    SQL Runner for AWS Athena compatible with Vanna 2.0's RunSqlTool
    
    This runner executes SQL queries against AWS Athena and returns results
    in a format compatible with Vanna 2.0.
    """
    
    def __init__(
        self,
        database: Optional[str] = None,
        workgroup: Optional[str] = None,
        output_location: Optional[str] = None,
        region_name: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize Athena runner
        
        Args:
            database: Athena database name (defaults to Config.ATHENA_DATABASE)
            workgroup: Athena workgroup (defaults to Config.ATHENA_WORKGROUP)
            output_location: S3 location for query results (defaults to Config.ATHENA_S3_STAGING)
            region_name: AWS region (defaults to Config.AWS_REGION)
            **kwargs: Additional connection parameters
        """
        self.database = database or Config.ATHENA_DATABASE
        self.workgroup = workgroup or Config.ATHENA_WORKGROUP
        self.output_location = output_location or Config.ATHENA_S3_STAGING
        self.region_name = region_name or Config.AWS_REGION
        
        # Create connection (will be lazy-initialized on first use)
        self._connection = None
        self._cursor = None
    
    def _get_connection(self):
        """Get or create Athena connection"""
        if self._connection is None:
            # Ensure database name is properly formatted (remove quotes if present)
            db_name = self.database.strip('"').strip("'")
            self._connection = connect(
                s3_staging_dir=self.output_location,
                region_name=self.region_name,
                database=db_name,  # Use unquoted database name for connection
                work_group=self.workgroup
            )
        return self._connection
    
    def _translate_metadata_queries(self, sql: str) -> str:
        """
        Translate PostgreSQL-style metadata queries to Athena equivalents
        
        Args:
            sql: Original SQL query
            
        Returns:
            str: Translated SQL query
        """
        sql_lower = sql.lower()
        
        # Replace pg_tables with Glue catalog query
        if 'pg_tables' in sql_lower or 'information_schema.tables' in sql_lower:
            # Return a query that shows tables - use default database context
            # Note: SHOW TABLES works in the current database context set by connection
            return "SHOW TABLES"
        
        # Replace pg_catalog or information_schema column queries
        if 'information_schema.columns' in sql_lower or 'pg_attribute' in sql_lower:
            # Extract table name if possible, otherwise show all columns
            # For now, return a simple show columns - this can be enhanced
            # Note: SHOW COLUMNS needs a table name, but we don't have context here
            # Better to return empty result and let training data be used instead
            return "SELECT 'Use training data - tables are already loaded in memory' as info"
        
        return sql
    
    def _qualify_table_names(self, sql: str) -> str:
        """
        Qualify unqualified table names with database name.
        
        Args:
            sql: SQL query string
            
        Returns:
            str: SQL with qualified table names
        """
        # Simple heuristic: if a table name appears without database qualification,
        # add the database name. This is a basic implementation.
        # For production, consider using a SQL parser.
        
        # Don't modify if already has database qualification
        if f'"{self.database}"' in sql or f"`{self.database}`" in sql:
            return sql
        
        # For now, return as-is since the connection already has database context
        # The issue might be that Vanna is generating queries without database context
        return sql
    
    def _convert_to_athena_syntax(self, sql: str) -> str:
        """
        Convert PostgreSQL-specific SQL syntax to Athena/Presto-compatible syntax
        
        Args:
            sql: SQL query string
            
        Returns:
            str: Converted SQL query compatible with Athena
        """
        # Convert ILIKE to case-insensitive pattern matching using LOWER() and LIKE
        # Athena doesn't support ILIKE, so we convert: column ILIKE 'pattern' 
        # to: LOWER(column) LIKE LOWER('pattern')
        
        # Simple replacement for ILIKE (handles both regular and NOT ILIKE)
        # This regex matches: column ILIKE 'pattern' or column NOT ILIKE 'pattern'
        def replace_ilike(match):
            not_part = match.group(1) or ''  # 'NOT ' if present
            column = match.group(2)  # Column name (could be table.column)
            pattern = match.group(3)  # Pattern with quotes
            
            # Return: LOWER(column) LIKE/NOT LIKE LOWER(pattern)
            return f"LOWER({column}) {not_part}LIKE LOWER({pattern})"
        
        # Pattern matches: [NOT ]column ILIKE 'pattern'
        # Group 1: "NOT " (optional)
        # Group 2: column name (can include dots for qualified names)
        # Group 3: pattern (handles single-quoted, double-quoted, or unquoted strings)
        sql = re.sub(
            r'\b(NOT\s+)?(\w+(?:\.\w+)?)\s+ILIKE\s+((?:["\'][^"\']*["\']|[^\s,)]+))',
            replace_ilike,
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
    
    async def run_sql(self, sql: str, user=None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame (async)
        
        Args:
            sql: SQL query string to execute (or RunSqlToolArgs object)
            user: User context (optional, for user-aware queries in Vanna 2.0)
            
        Returns:
            pandas.DataFrame: Query results
            
        Raises:
            Exception: If query execution fails
        """
        # Run in thread pool since pyathena is synchronous
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._run_sql_sync, sql, user)
    
    def _run_sql_sync(self, sql, user=None) -> pd.DataFrame:
        """Synchronous SQL execution (called by async wrapper)"""
        try:
            # Handle if sql is a RunSqlToolArgs object instead of a string
            if hasattr(sql, 'sql'):
                sql_query = sql.sql
            elif isinstance(sql, str):
                sql_query = sql
            else:
                sql_query = str(sql)
            
            # Clean the SQL query
            sql_query = sql_query.strip()
            
            # Translate PostgreSQL-style metadata queries to Athena equivalents
            sql_query = self._translate_metadata_queries(sql_query)
            
            # Convert PostgreSQL-specific syntax to Athena/Presto syntax
            sql_query = self._convert_to_athena_syntax(sql_query)
            
            # Ensure database context is set - add USE statement if not present
            # This ensures queries run in the correct database
            if not sql_query.upper().startswith('USE ') and not sql_query.upper().startswith('SET '):
                # Add database qualification to unqualified table names
                # Only if the query doesn't already have database qualification
                if f'"{self.database}"' not in sql_query and self.database not in sql_query:
                    # For simple queries, we can prepend USE statement
                    # But Athena doesn't support USE, so we'll qualify table names instead
                    # The connection already has database set, so this should work
                    pass
            
            # Execute query
            conn = self._get_connection()
            cursor = conn.cursor(PandasCursor)
            
            # Ensure we're using the correct database context
            # Athena uses the database from the connection, but we can also set it explicitly
            try:
                df = cursor.execute(sql_query).as_pandas()
            except Exception as e:
                # If error mentions database, try to add database qualification
                error_str = str(e).lower()
                if 'database' in error_str or 'does not exist' in error_str:
                    # Try to qualify table names with database
                    db_qualified_sql = self._qualify_table_names(sql_query)
                    if db_qualified_sql != sql_query:
                        df = cursor.execute(db_qualified_sql).as_pandas()
                    else:
                        raise
                else:
                    raise
            cursor.close()
            return df
        except Exception as e:
            # Re-raise with more context
            raise Exception(f"Athena query execution failed: {str(e)}") from e
    
    async def run_sql_return_string(self, sql: str, user=None) -> str:
        """
        Execute SQL query and return results as formatted string (async)
        
        This method is used by Vanna 2.0's RunSqlTool to get string results
        that can be included in LLM responses.
        
        Args:
            sql: SQL query string to execute (or RunSqlToolArgs object)
            user: User context (optional, for user-aware queries in Vanna 2.0)
            
        Returns:
            str: Formatted query results
        """
        try:
            df = await self.run_sql(sql, user=user)
            # Convert DataFrame to string representation
            # Limit rows to avoid huge responses
            max_rows = 100
            if len(df) > max_rows:
                result_str = df.head(max_rows).to_string()
                result_str += f"\n... (showing {max_rows} of {len(df)} rows)"
            else:
                result_str = df.to_string()
            return result_str
        except Exception as e:
            return f"Error executing query: {str(e)}"
    
    def get_schema(self) -> str:
        """
        Get database schema information
        
        Returns:
            str: Schema information as string
        """
        # This could be implemented to fetch schema from Glue
        # For now, return a placeholder
        return f"Database: {self.database}, Workgroup: {self.workgroup}"
    
    def close(self):
        """Close database connection"""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None

