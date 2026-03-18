from utils.logger import get_logger

logger = get_logger("pipeline.filter")

def apply_filter(df, filter_expr):
    """
    Apply filter expression to DataFrame.
    
    Args:
        df: pandas DataFrame to filter
        filter_expr: Filter expression string (e.g., "status == 'active'", "age > 18")
    
    Returns:
        Filtered DataFrame
    """
    if not filter_expr:
        logger.debug("No filter expression provided")
        return df
    
    try:
        logger.info(f"Applying filter: {filter_expr}")
        rows_before = len(df)
        
        # Apply the filter using DataFrame.eval() for safe expression evaluation
        df_filtered = df.query(filter_expr)
        
        rows_after = len(df_filtered)
        logger.info(f"Filter applied: {rows_before} rows → {rows_after} rows")
        
        return df_filtered
    
    except Exception as e:
        logger.error(f"Failed to apply filter '{filter_expr}': {e}")
        raise