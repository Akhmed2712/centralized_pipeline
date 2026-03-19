from utils.logger import get_logger
from utils.db import get_connection
import os
import pandas as pd

logger = get_logger("pipeline.load")

SUPPORTED_LOAD_TYPES = ["csv", "mysql"]


def load_to_target(df, load_conf, feed_name):
    """
    Load dataframe to target destination (CSV or MySQL).
    
    Args:
        df: Dataframe to load
        load_conf: Load configuration dict with keys:
            - type: 'csv' or 'mysql'
            - path: File path (for CSV)
            - connection: Connection name (for MySQL)
            - table: Table name (for MySQL)
            - if_exists: 'fail', 'replace', 'append' (for MySQL, default: 'fail')
            
            CSV-specific options:
            - delimiter: Field delimiter (default: ',')
            - header: Include header row (default: true)
            - encoding: File encoding (default: 'utf-8')
            - quoting: Quoting style (default: 'minimal')
    
    Returns:
        True if successful, False otherwise
    """
    if not load_conf:
        logger.warning("No load configuration found, skipping load")
        return True
    
    load_type = load_conf.get("type")
    
    if not load_type:
        logger.error("Load type not specified in load config")
        return False
    
    if load_type not in SUPPORTED_LOAD_TYPES:
        logger.error(f"Unsupported load type: {load_type}. Supported: {SUPPORTED_LOAD_TYPES}")
        return False
    
    rows = len(df)
    logger.info(f"Loading {rows} rows to {load_type.upper()} target")
    
    try:
        if load_type == "csv":
            load_to_csv(df, load_conf)
        
        elif load_type == "mysql":
            load_to_mysql(df, load_conf, feed_name)
        
        logger.info(f"Successfully loaded {rows} rows to {load_type.upper()}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to load to {load_type}: {e}", exc_info=True)
        return False


def load_to_csv(df, load_conf):
    """
    Load dataframe to CSV file with configurable format options.
    
    Args:
        df: Dataframe to load
        load_conf: Load config dict with keys:
            - path: File path (required)
            - delimiter: Field delimiter (default: ',')
            - header: Include header row (default: True)
            - encoding: File encoding (default: 'utf-8')
            - quoting: Quoting style - 'minimal', 'all', 'nonnumeric', 'none' (default: 'minimal')
    
    Raises:
        ValueError: If required config is missing
        Exception: If file operation fails
    """
    path = load_conf.get("path")
    
    if not path:
        raise ValueError("'path' required for CSV load")
    
    # CSV formatting options
    delimiter = load_conf.get("delimiter", ",")
    header = load_conf.get("header", True)
    
    logger.info(f"Loading to CSV file: {path}")
    logger.debug(f"  - rows: {len(df)}")
    logger.debug(f"  - columns: {list(df.columns)}")
    logger.debug(f"  - delimiter: '{delimiter}'")
    logger.debug(f"  - header: {header}")
    
    try:
        # Create directory if it doesn't exist
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            logger.debug(f"Created directory: {directory}")
        
        # Write CSV with options
        df.to_csv(
            path,
            index=False,
            sep=delimiter,
            header=header
        )
        
        logger.info(f"Successfully wrote {len(df)} rows to {path}")
    
    except Exception as e:
        logger.error(f"CSV load failed: {e}")
        raise


def load_to_mysql(df, load_conf, feed_name):
    """
    Load dataframe to MySQL table using bulk insert.
    
    Args:
        df: Dataframe to load
        load_conf: Load config dict with keys:
            - connection: Connection ID (required)
            - table: Target table name (required)
            - if_exists: 'fail', 'replace', 'append' (default: 'fail')
            - env: Environment for connection (default: 'dev')
            - chunksize: Number of rows per batch insert (default: 1000)
    
    Returns:
        None
    
    Raises:
        ValueError: If required config is missing
        Exception: If database operation fails
    """
    connection_id = load_conf.get("connection")
    table_name = load_conf.get("table")
    if_exists = load_conf.get("if_exists", "fail")
    env = load_conf.get("env", "dev")
    chunksize = load_conf.get("chunksize", 1000)
    
    # Validate required parameters
    if not connection_id:
        raise ValueError("'connection' required for MySQL load")
    if not table_name:
        raise ValueError("'table' required for MySQL load")
    
    if if_exists not in ["fail", "replace", "append"]:
        raise ValueError(f"Invalid 'if_exists' value: {if_exists}. Must be: fail, replace, append")
    
    logger.info(f"Loading to MySQL table: {table_name}")
    logger.debug(f"  - connection: {connection_id}")
    logger.debug(f"  - rows: {len(df)}")
    logger.debug(f"  - columns: {list(df.columns)}")
    logger.debug(f"  - if_exists: {if_exists}")
    logger.debug(f"  - chunksize: {chunksize}")
    
    conn = None
    try:
        # Get MySQL connection
        conn = get_connection(connection_id, env=env)
        cursor = conn.cursor()
        
        # Handle table existence based on if_exists mode
        table_exists = table_exists_in_db(cursor, table_name)
        
        if table_exists and if_exists == "fail":
            raise ValueError(f"Table '{table_name}' already exists and if_exists='fail'")
        
        if table_exists and if_exists == "replace":
            logger.info(f"Dropping existing table '{table_name}' (if_exists='replace')")
            cursor.execute(f"DROP TABLE `{table_name}`")
            conn.commit()
            table_exists = False
            logger.debug(f"Table '{table_name}' dropped successfully")
        
        # Create table if it doesn't exist
        if not table_exists:
            logger.info(f"Creating table '{table_name}'")
            create_sql = generate_create_table_sql(table_name, df)
            cursor.execute(create_sql)
            conn.commit()
            logger.debug(f"Table '{table_name}' created successfully")
        
        # Insert data in chunks
        logger.info(f"Inserting {len(df)} rows into {table_name}")
        insert_count = 0
        
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            insert_sql = generate_insert_sql(table_name, chunk)
            
            try:
                cursor.execute(insert_sql)
                conn.commit()
                insert_count += len(chunk)
                logger.debug(f"Inserted {insert_count}/{len(df)} rows")
            except Exception as e:
                logger.error(f"Failed to insert chunk at row {i}: {e}")
                conn.rollback()
                raise
        
        logger.info(f"Successfully loaded {insert_count} rows to {table_name}")
    
    except Exception as e:
        logger.error(f"MySQL load failed: {e}", exc_info=True)
        raise
    
    finally:
        if conn:
            try:
                cursor.close()
                conn.close()
                logger.debug("Closed MySQL connection")
            except:
                pass


def table_exists_in_db(cursor, table_name):
    """Check if table exists in database."""
    try:
        cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1")
        return True
    except:
        return False


def get_mysql_type(pandas_dtype):
    """Convert pandas dtype to MySQL type."""
    dtype_str = str(pandas_dtype)
    
    if 'int64' in dtype_str or 'int32' in dtype_str or 'int16' in dtype_str or 'int8' in dtype_str:
        return "BIGINT"
    elif 'float64' in dtype_str or 'float32' in dtype_str:
        return "DOUBLE"
    elif 'bool' in dtype_str:
        return "BOOLEAN"
    elif 'datetime' in dtype_str:
        return "DATETIME"
    else:
        return "LONGTEXT"


def generate_create_table_sql(table_name, df):
    """Generate CREATE TABLE SQL from DataFrame."""
    columns = []
    
    for col in df.columns:
        mysql_type = get_mysql_type(df[col].dtype)
        col_name = f"`{col}`"
        columns.append(f"{col_name} {mysql_type}")
    
    # Add a primary key if not present
    if 'id' not in df.columns:
        columns.append("id INT PRIMARY KEY AUTO_INCREMENT")
    
    columns_sql = ", ".join(columns)
    sql = f"CREATE TABLE `{table_name}` ({columns_sql})"
    
    logger.debug(f"Create table SQL: {sql}")
    return sql


def generate_insert_sql(table_name, df):
    """Generate INSERT statement from DataFrame chunk."""
    if len(df) == 0:
        return None
    
    # Column names
    columns = [f"`{col}`" for col in df.columns]
    columns_str = ", ".join(columns)
    
    # Values
    rows_values = []
    for _, row in df.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append("NULL")
            elif isinstance(val, str):
                # Escape single quotes
                escaped_val = val.replace("'", "''")
                row_values.append(f"'{escaped_val}'")
            elif isinstance(val, (int, float)):
                row_values.append(str(val))
            elif isinstance(val, bool):
                row_values.append("1" if val else "0")
            else:
                row_values.append(f"'{str(val)}'")
        rows_values.append(f"({', '.join(row_values)})")
    
    values_str = ", ".join(rows_values)
    sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES {values_str}"
    
    return sql