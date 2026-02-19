"""
Data Bridge (SQL Server) operations.

Provides SQL Server connectivity via pyodbc with named connections,
parameterized query execution using {{ param }} template syntax,
and file-based SQL execution. Designed for interacting with Moody's
Data Bridge databases.

Connection Management:
    Supports multiple named MSSQL connections configured via environment
    variables. Each connection requires:

    MSSQL_{CONNECTION_NAME}_SERVER   - Server hostname or IP (required)
    MSSQL_{CONNECTION_NAME}_USER     - SQL auth username (required)
    MSSQL_{CONNECTION_NAME}_PASSWORD - SQL auth password (required)
    MSSQL_{CONNECTION_NAME}_PORT     - Port (optional, defaults to 1433)

    Global settings:
    MSSQL_DRIVER     - ODBC driver name (default: 'ODBC Driver 18 for SQL Server')
    MSSQL_TRUST_CERT - Trust server certificate (default: 'yes')
    MSSQL_TIMEOUT    - Connection timeout in seconds (default: '30')

Parameter Substitution:
    SQL queries support named parameters using {{ param_name }} syntax.
    Parameters are context-aware: identifiers (inside brackets or as part
    of table names) are substituted raw, while values are escaped with
    SQL injection protection.
"""

import gc
import os
import re
import logging
from contextlib import contextmanager
from typing import List, Optional, Dict, Any, Union, Tuple
from string import Template

import pandas as pd
import numpy as np

from .exceptions import (
    IRPDataBridgeError,
    IRPDataBridgeConnectionError,
    IRPDataBridgeQueryError,
    IRPValidationError,
)
from .validators import validate_non_empty_string, validate_file_exists

logger = logging.getLogger(__name__)


# ==========================================================================
# LAZY PYODBC IMPORT
# ==========================================================================

_pyodbc = None


def _get_pyodbc():
    """
    Lazy-import pyodbc, raising a clear error if not installed.

    Returns:
        The pyodbc module.

    Raises:
        ImportError: If pyodbc is not installed.
    """
    global _pyodbc
    if _pyodbc is None:
        try:
            import pyodbc
            _pyodbc = pyodbc
        except ImportError as e:
            raise ImportError(
                "pyodbc is required for Data Bridge (SQL Server) operations. "
                "Install it with: pip install irp-integration[databridge]\n"
                "Note: Microsoft ODBC Driver 18 for SQL Server must also be installed.\n"
                "See: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
            ) from e
    return _pyodbc


# ==========================================================================
# TEMPLATE SYSTEM
# ==========================================================================

class ExpressionTemplate(Template):
    """
    Custom Template class for SQL parameter substitution.

    Uses {{ PARAM }} syntax with space padding to avoid conflicts with SQL syntax.
    Example: SELECT * FROM table WHERE id = {{ ID }}
    """
    delimiter = '{{'
    pattern = r'''
    \{\{\s*
    (?:
    (?P<escaped>\{\{)|
    (?P<named>[_a-zA-Z][_a-zA-Z0-9]*)\s*\}\}|
    (?P<braced>[_a-zA-Z][_a-zA-Z0-9]*)\s*\}\}|
    (?P<invalid>)
    )
    '''  # type: ignore


# ==========================================================================
# DATABRIDGE MANAGER
# ==========================================================================

class DataBridgeManager:
    """
    Manager for SQL Server (Data Bridge) operations.

    Unlike other managers, DataBridgeManager does not depend on the HTTP
    Client. It connects directly to SQL Server via pyodbc. It can be used
    standalone or attached to IRPClient as client.databridge.

    Args:
        default_connection: Default connection name used when no connection
            is specified in method calls. Defaults to 'DATABRIDGE'.

    Environment Variables (per connection):
        MSSQL_{CONNECTION_NAME}_SERVER   - Server hostname or IP (required)
        MSSQL_{CONNECTION_NAME}_USER     - SQL auth username (required)
        MSSQL_{CONNECTION_NAME}_PASSWORD - SQL auth password (required)
        MSSQL_{CONNECTION_NAME}_PORT     - Port (optional, defaults to 1433)

    Global Environment Variables:
        MSSQL_DRIVER     - ODBC driver name (default: 'ODBC Driver 18 for SQL Server')
        MSSQL_TRUST_CERT - Trust server certificate (default: 'yes')
        MSSQL_TIMEOUT    - Connection timeout in seconds (default: '30')

    Example:
        # Via IRPClient
        from irp_integration import IRPClient
        client = IRPClient()
        df = client.databridge.execute_query(
            "SELECT * FROM portfolios WHERE value > {{ min_value }}",
            params={'min_value': 1000000},
            connection='DATABRIDGE',
            database='DataWarehouse'
        )

        # Standalone
        from irp_integration.databridge import DataBridgeManager
        db = DataBridgeManager(default_connection='DATABRIDGE')
        df = db.execute_query("SELECT 1 AS test", database='master')
    """

    def __init__(self, default_connection: str = 'DATABRIDGE') -> None:
        self._default_connection = default_connection.upper()

    # ======================================================================
    # CONNECTION MANAGEMENT
    # ======================================================================

    def get_connection_config(self, connection_name: Optional[str] = None) -> Dict[str, str]:
        """
        Get connection configuration for a named MSSQL connection.

        Reads configuration from environment variables following the pattern
        MSSQL_{CONNECTION_NAME}_{SETTING}.

        Args:
            connection_name: Name of the connection (e.g., 'DATABRIDGE', 'ANALYTICS').
                Defaults to the manager's default_connection.

        Returns:
            Dictionary with connection parameters: server, port, driver,
            trust_cert, timeout, user, password.

        Raises:
            IRPValidationError: If required environment variables are missing.

        Example:
            config = db.get_connection_config('DATABRIDGE')
            # Returns: {'server': 'db.company.com', 'user': 'svc', ...}
        """
        connection_name = (connection_name or self._default_connection).upper()
        prefix = f'MSSQL_{connection_name}_'

        config = {
            'server': os.getenv(f'{prefix}SERVER'),
            'port': os.getenv(f'{prefix}PORT', '1433'),
            'driver': os.getenv('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server'),
            'trust_cert': os.getenv('MSSQL_TRUST_CERT', 'yes'),
            'timeout': os.getenv('MSSQL_TIMEOUT', '30'),
            'user': os.getenv(f'{prefix}USER'),
            'password': os.getenv(f'{prefix}PASSWORD'),
        }

        required = ['server', 'user', 'password']
        missing = [field for field in required if not config.get(field)]

        if missing:
            raise IRPValidationError(
                f"SQL Server connection '{connection_name}' is not properly configured.\n"
                f"Missing environment variables: {', '.join([f'{prefix}{f.upper()}' for f in missing])}\n"
                f"Required format:\n"
                f"  MSSQL_{connection_name}_SERVER=<server>\n"
                f"  MSSQL_{connection_name}_USER=<user>\n"
                f"  MSSQL_{connection_name}_PASSWORD=<password>"
            )

        return config

    def build_connection_string(
        self,
        connection_name: Optional[str] = None,
        database: Optional[str] = None
    ) -> str:
        """
        Build ODBC connection string for SQL Server.

        Args:
            connection_name: Name of the connection. Defaults to the
                manager's default_connection.
            database: Optional database name to connect to.

        Returns:
            ODBC connection string.

        Example:
            conn_str = db.build_connection_string('DATABRIDGE', database='MyDB')
        """
        config = self.get_connection_config(connection_name)

        connection_string = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']},{config['port']};"
        )

        if database:
            connection_string += f"DATABASE={database};"

        connection_string += (
            f"UID={config['user']};"
            f"PWD={config['password']};"
            f"TrustServerCertificate={config['trust_cert']};"
            f"Connection Timeout={config['timeout']};"
        )

        return connection_string

    @contextmanager
    def get_connection(
        self,
        connection_name: Optional[str] = None,
        database: Optional[str] = None
    ):
        """
        Context manager for SQL Server database connections.

        Automatically handles connection lifecycle: opens connection,
        yields it for use, and closes on exit (even if exception occurs).

        Args:
            connection_name: Name of the connection to use. Defaults to
                the manager's default_connection.
            database: Optional database name to connect to.

        Yields:
            pyodbc.Connection object.

        Raises:
            IRPDataBridgeConnectionError: If connection fails.

        Example:
            with db.get_connection('DATABRIDGE', database='MyDB') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM portfolios")
                rows = cursor.fetchall()
        """
        pyodbc = _get_pyodbc()
        conn_name = (connection_name or self._default_connection).upper()
        connection_string = self.build_connection_string(conn_name, database=database)
        conn = None

        try:
            conn = pyodbc.connect(connection_string)
            yield conn
        except pyodbc.Error as e:
            if conn is None:
                raise IRPDataBridgeConnectionError(
                    f"Failed to connect to SQL Server (connection: {conn_name}): {e}"
                ) from e
            else:
                raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def test_connection(self, connection_name: Optional[str] = None) -> bool:
        """
        Test if a SQL Server connection is working.

        Args:
            connection_name: Name of the connection to test. Defaults to
                the manager's default_connection.

        Returns:
            True if connection successful, False otherwise.

        Example:
            if db.test_connection('DATABRIDGE'):
                print("Connection successful!")
        """
        try:
            with self.get_connection(connection_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logger.warning(f"Connection test failed: {e}")
            return False

    # ======================================================================
    # PARAMETER SUBSTITUTION (PRIVATE)
    # ======================================================================

    @staticmethod
    def _escape_sql_value(value: Any) -> str:
        """
        Escape parameter values for safe SQL substitution.

        Prevents SQL injection by properly escaping values based on type.

        Args:
            value: Parameter value to escape.

        Returns:
            Escaped string representation safe for SQL substitution.
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, bool):
            return '1' if value else '0'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        else:
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

    @staticmethod
    def _convert_param_value(value: Any) -> Any:
        """
        Convert numpy/pandas types to native Python types for pyodbc.

        Args:
            value: Parameter value (may be numpy/pandas type).

        Returns:
            Native Python type that pyodbc can handle.
        """
        if value is None:
            return None

        if isinstance(value, np.ndarray):
            return value.tolist()

        if isinstance(value, pd.Series):
            return value.tolist()

        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass

        if hasattr(value, 'item'):
            return value.item()

        return value

    @staticmethod
    def _convert_params_to_native_types(
        params: Union[Dict[str, Any], Tuple, None]
    ) -> Union[Dict[str, Any], Tuple, None]:
        """
        Convert all parameter values to native Python types.

        Args:
            params: Dictionary or tuple of parameters.

        Returns:
            Converted parameters in same structure.
        """
        if params is None:
            return None

        if isinstance(params, dict):
            return {
                key: DataBridgeManager._convert_param_value(value)
                for key, value in params.items()
            }
        elif isinstance(params, (tuple, list)):
            return tuple(
                DataBridgeManager._convert_param_value(value)
                for value in params
            )
        else:
            return params

    def _substitute_named_parameters(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Substitute named parameters {{ param_name }} with context-aware escaping.

        Parameters are escaped differently based on their context:
        - IDENTIFIER CONTEXTS (no quoting — raw substitution):
          * Inside square brackets: [{{ param }}]
          * Inside string literals: '...{{ param }}...'
          * As part of object names: table_{{ param }}_suffix
        - VALUE CONTEXTS (escaped and quoted appropriately):
          * Strings: Quoted with single quotes, SQL injection protected
          * Numbers: Unquoted
          * NULL: NULL keyword

        Args:
            query: SQL query with {{ param_name }} placeholders.
            params: Dictionary of parameter values.

        Returns:
            SQL query with parameters substituted.

        Raises:
            IRPDataBridgeQueryError: If parameter substitution fails.

        Example:
            # Value context (quoted/escaped):
            query = "SELECT * FROM t WHERE id = {{ user_id }}"
            result = db._substitute_named_parameters(query, {'user_id': 123})
            # Returns: "SELECT * FROM t WHERE id = 123"

            # Identifier in brackets (not quoted):
            query = "USE [{{ db_name }}]"
            result = db._substitute_named_parameters(query, {'db_name': 'my_db'})
            # Returns: "USE [my_db]"
        """
        logger.debug('Parameterizing query ...')
        if not params:
            return query

        converted_params = self._convert_params_to_native_types(params)

        if not isinstance(converted_params, dict):
            return query

        escaped_params = {}

        for key, value in converted_params.items():
            identifier_patterns = [
                rf'\[\s*\{{\{{\s*{re.escape(key)}\s*\}}\}}\s*\]',
                rf"'[^'\n\r]*\{{\{{\s*{re.escape(key)}\s*\}}\}}[^'\n\r]*'",
                rf'\w+_\{{\{{\s*{re.escape(key)}\s*\}}\}}',
                rf'\{{\{{\s*{re.escape(key)}\s*\}}\}}_\w+',
            ]

            is_identifier = any(re.search(pattern, query) for pattern in identifier_patterns)

            if is_identifier:
                if isinstance(value, str):
                    if not all(c.isalnum() or c in ('_', '-', ' ', '/') for c in value):
                        raise ValueError(
                            f"Invalid identifier value for parameter '{key}': {value}. "
                            f"Identifiers can only contain alphanumeric characters, "
                            f"underscores, hyphens, and spaces."
                        )
                escaped_params[key] = str(value)
            else:
                escaped_params[key] = self._escape_sql_value(value)

        try:
            template = ExpressionTemplate(query)
            substituted_query = template.substitute(escaped_params)
            return substituted_query
        except KeyError as e:
            raise IRPDataBridgeQueryError(
                f"Missing required parameter: {str(e)}\n"
                f"Query requires parameter that was not provided.\n"
                f"Provided parameters: {', '.join(converted_params.keys())}"
            ) from e
        except ValueError as e:
            raise IRPDataBridgeQueryError(
                f"Parameter substitution error: {e}"
            ) from e

    # ======================================================================
    # QUERY EXECUTION
    # ======================================================================

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        connection: Optional[str] = None,
        database: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Execute SELECT query and return results as DataFrame.

        Args:
            query: SQL SELECT query (supports {{ param_name }} placeholders).
            params: Query parameters as dictionary.
            connection: Name of the SQL Server connection to use.
                Defaults to the manager's default_connection.
            database: Optional database name to connect to.

        Returns:
            pandas DataFrame with query results.

        Raises:
            IRPDataBridgeQueryError: If query execution fails.

        Example:
            df = db.execute_query(
                "SELECT * FROM portfolios WHERE value > {{ min_value }}",
                params={'min_value': 1000000},
                connection='DATABRIDGE',
                database='DataWarehouse'
            )
        """
        try:
            if isinstance(params, dict):
                query = self._substitute_named_parameters(query, params)

            with self.get_connection(connection, database=database) as conn:
                df = pd.read_sql(query, conn)

            return df

        except (IRPDataBridgeConnectionError, IRPValidationError):
            raise
        except Exception as e:
            conn_name = (connection or self._default_connection).upper()
            raise IRPDataBridgeQueryError(
                f"Query execution failed (connection: {conn_name}): {e}"
            ) from e

    def execute_scalar(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        connection: Optional[str] = None,
        database: Optional[str] = None
    ) -> Any:
        """
        Execute query and return single scalar value (first column of first row).

        Args:
            query: SQL query returning single value.
            params: Query parameters.
            connection: Name of the SQL Server connection to use.
                Defaults to the manager's default_connection.
            database: Optional database name to connect to.

        Returns:
            Single value from query result (or None if no results).

        Raises:
            IRPDataBridgeQueryError: If query execution fails.

        Example:
            count = db.execute_scalar(
                "SELECT COUNT(*) FROM portfolios WHERE value > {{ min_value }}",
                params={'min_value': 1000000},
                connection='DATABRIDGE',
                database='DataWarehouse'
            )
        """
        try:
            if isinstance(params, dict):
                query = self._substitute_named_parameters(query, params)

            with self.get_connection(connection, database=database) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                row = cursor.fetchone()
                return row[0] if row else None

        except (IRPDataBridgeConnectionError, IRPValidationError):
            raise
        except Exception as e:
            conn_name = (connection or self._default_connection).upper()
            raise IRPDataBridgeQueryError(
                f"Scalar query execution failed (connection: {conn_name}): {e}\n"
                f"Query: {query[:200]}{'...' if len(query) > 200 else ''}"
            ) from e

    def execute_command(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        connection: Optional[str] = None,
        database: Optional[str] = None
    ) -> int:
        """
        Execute non-query command (INSERT, UPDATE, DELETE) and return rows affected.

        Args:
            query: SQL command.
            params: Query parameters.
            connection: Name of the SQL Server connection to use.
                Defaults to the manager's default_connection.
            database: Optional database name to connect to.

        Returns:
            Number of rows affected.

        Raises:
            IRPDataBridgeQueryError: If command execution fails.

        Example:
            rows = db.execute_command(
                "UPDATE portfolios SET status = {{ status }} WHERE value < {{ min_value }}",
                params={'status': 'INACTIVE', 'min_value': 100000},
                connection='DATABRIDGE',
                database='DataWarehouse'
            )
            print(f"Updated {rows} rows")
        """
        try:
            if isinstance(params, dict):
                query = self._substitute_named_parameters(query, params)

            with self.get_connection(connection, database=database) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
                return cursor.rowcount

        except (IRPDataBridgeConnectionError, IRPValidationError):
            raise
        except Exception as e:
            conn_name = (connection or self._default_connection).upper()
            raise IRPDataBridgeQueryError(
                f"Command execution failed (connection: {conn_name}): {e}\n"
                f"Query: {query[:200]}{'...' if len(query) > 200 else ''}"
            ) from e

    # ======================================================================
    # FILE-BASED EXECUTION
    # ======================================================================

    def _read_sql_file(self, file_path: str) -> str:
        """
        Read SQL script from file.

        Args:
            file_path: Path to SQL file (absolute or relative to cwd).

        Returns:
            SQL script content.

        Raises:
            IRPValidationError: If file does not exist.
            IRPDataBridgeQueryError: If file cannot be read.
        """
        validate_file_exists(file_path, "file_path")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            raise IRPDataBridgeQueryError(
                f"Failed to read SQL script file '{file_path}': {e}"
            ) from e

    def execute_query_from_file(
        self,
        file_path: str,
        params: Optional[Dict[str, Any]] = None,
        connection: Optional[str] = None,
        database: Optional[str] = None
    ) -> List[pd.DataFrame]:
        """
        Execute SQL query from file and return results as list of DataFrames.

        Handles both single-statement queries and multi-statement scripts
        (e.g., scripts with USE statements followed by SELECT). Each result
        set is returned as a separate DataFrame in the list.

        Args:
            file_path: Path to SQL file (absolute or relative to cwd).
            params: Query parameters (supports {{ param_name }} placeholders).
            connection: Name of the SQL Server connection to use.
                Defaults to the manager's default_connection.
            database: Optional database name to connect to.

        Returns:
            List of pandas DataFrames, one per result set.

        Raises:
            IRPValidationError: If SQL file does not exist.
            IRPDataBridgeQueryError: If query execution fails.

        Example:
            results = db.execute_query_from_file(
                'C:/sql/extract_policies.sql',
                params={'cycle_name': 'Q1-2025', 'run_date': '2025-01-15'},
                connection='DATABRIDGE',
                database='AnalyticsDB'
            )
            df = results[0]  # First result set
        """
        query = self._read_sql_file(file_path)

        try:
            if isinstance(params, dict):
                query = self._substitute_named_parameters(query, params)

            dataframes = []
            conn_name = (connection or self._default_connection).upper()

            logger.info(f"Executing query from file: {file_path}")

            with self.get_connection(connection, database=database) as conn:
                cursor = conn.cursor()
                cursor.execute(query)

                stmt_num = 1
                if cursor.description is not None:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()

                    data = [tuple(row) for row in rows]
                    del rows
                    gc.collect()

                    df = pd.DataFrame.from_records(data, columns=columns)
                    del data
                    gc.collect()

                    dataframes.append(df)
                    logger.debug(f"Statement {stmt_num}: Retrieved {len(df)} rows")
                    stmt_num += 1

                while cursor.nextset():
                    if cursor.description is not None:
                        columns = [column[0] for column in cursor.description]
                        rows = cursor.fetchall()

                        data = [tuple(row) for row in rows]
                        del rows
                        gc.collect()

                        df = pd.DataFrame.from_records(data, columns=columns)
                        del data
                        gc.collect()

                        dataframes.append(df)
                        logger.debug(f"Statement {stmt_num}: Retrieved {len(df)} rows")
                        stmt_num += 1

                conn.commit()

            if not dataframes:
                logger.warning(
                    f"No result sets returned from {file_path}. "
                    f"This may occur if the script contains only DDL/DML "
                    f"or uses dynamic SQL (EXEC)."
                )

            logger.info(
                f"Query completed: {len(dataframes)} result set(s) returned"
            )
            return dataframes

        except (IRPDataBridgeConnectionError, IRPValidationError):
            raise
        except Exception as e:
            conn_name = (connection or self._default_connection).upper()
            raise IRPDataBridgeQueryError(
                f"Query execution failed (connection: {conn_name}, "
                f"file: {file_path}): {e}"
            ) from e
