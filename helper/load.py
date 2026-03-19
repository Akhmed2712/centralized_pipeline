from utils.logger import get_logger
import os

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