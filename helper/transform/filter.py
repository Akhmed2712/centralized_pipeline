from utils.logger import get_logger
import pandas as pd

logger = get_logger("pipeline.filter")

# Supported operators and their implementations
OPERATORS = {
    "notnull": lambda col, val: col.notna(),
    "isnull": lambda col, val: col.isna(),
    "eq": lambda col, val: col == val,
    "ne": lambda col, val: col != val,
    "gt": lambda col, val: col > val,
    "gte": lambda col, val: col >= val,
    "lt": lambda col, val: col < val,
    "lte": lambda col, val: col <= val,
    "in": lambda col, val: col.isin(val),
    "nin": lambda col, val: ~col.isin(val),
    "contains": lambda col, val: col.astype(str).str.contains(val, na=False),
    "startswith": lambda col, val: col.astype(str).str.startswith(val, na=False),
    "endswith": lambda col, val: col.astype(str).str.endswith(val, na=False),
}

def apply_filter(df, filter_conf):
    """
    Apply filter to DataFrame.
    
    Supports:
    - string expression (query)
    - structured list of conditions
    
    Args:
        df: Input DataFrame
        filter_conf: Filter configuration (str or list of dicts)
    
    Returns:
        Filtered DataFrame
    
    Raises:
        ValueError: If filter format is invalid
        KeyError: If column doesn't exist
    """
    
    if not filter_conf:
        logger.debug("No filter provided")
        return df
    
    rows_before = len(df)
    
    try:
        if isinstance(filter_conf, str):
            # String filter using DataFrame.query()
            logger.info(f"Applying string filter: {filter_conf}")
            df = df.query(filter_conf)
        
        elif isinstance(filter_conf, list):
            # Structured filters (list of conditions)
            logger.info(f"Applying {len(filter_conf)} structured filter conditions")
            
            for f in filter_conf:
                col = f.get("column")
                op = f.get("op")
                val = f.get("value")
                
                if not col or not op:
                    raise ValueError(f"Missing 'column' or 'op' in filter: {f}")
                
                if col not in df.columns:
                    raise KeyError(f"Column '{col}' not found in DataFrame. Available: {list(df.columns)}")
                
                if op not in OPERATORS:
                    raise ValueError(f"Unsupported operator '{op}'. Supported: {list(OPERATORS.keys())}")
                
                # Apply the operator
                mask = OPERATORS[op](df[col], val)
                df = df[mask]
                
                logger.debug(f"Applied {op} on '{col}': {len(df)} rows remaining")
        
        else:
            raise ValueError(f"Invalid filter format: expected str or list, got {type(filter_conf)}")
        
        rows_after = len(df)
        rows_removed = rows_before - rows_after
        pct_removed = (rows_removed / rows_before * 100) if rows_before > 0 else 0
        
        logger.info(f"Filter applied: {rows_before} → {rows_after} rows ({pct_removed:.1f}% removed)")
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to apply filter: {filter_conf} | Error: {e}", exc_info=True)
        raise