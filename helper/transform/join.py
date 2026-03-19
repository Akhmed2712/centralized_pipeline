from utils.logger import get_logger

logger = get_logger("pipeline.join")

SUPPORTED_JOINS = ["inner", "left", "right", "outer"]

def join_two_sources(left_df, right_df, left_id, right_id, join_conf):
    """
    Join two dataframes.
    
    Args:
        left_df: Left dataframe
        right_df: Right dataframe
        left_id: Left source ID (for logging)
        right_id: Right source ID (for logging)
        join_conf: Join config with keys: how, on
    
    Returns:
        Joined dataframe
    """
    join_how = join_conf.get("how", "inner").lower()
    join_on = join_conf.get("on")
    
    if not join_on:
        raise ValueError("'on' column(s) required in join config")
    
    if join_how not in SUPPORTED_JOINS:
        raise ValueError(f"Unsupported join type: {join_how}. Supported: {SUPPORTED_JOINS}")
    
    logger.info(f"Joining '{left_id}' ({len(left_df)} rows) with '{right_id}' ({len(right_df)} rows)") 
    logger.info(f"Join method: {join_how}, on: {join_on}")
    
    rows_before = len(left_df)
    
    try:
        # Handle dict-style join (different column names)
        if isinstance(join_on, dict):
            left_col = join_on.get("left")
            right_col = join_on.get("right")
            
            if not left_col or not right_col:
                raise ValueError("'left' and 'right' column names required in 'on' dict")
            
            # Validate columns exist
            if left_col not in left_df.columns:
                raise KeyError(f"Column '{left_col}' not found in source '{left_id}'")
            if right_col not in right_df.columns:
                raise KeyError(f"Column '{right_col}' not found in source '{right_id}'")
            
            logger.debug(f"Joining on: {left_id}.{left_col} = {right_id}.{right_col}")
            
            result_df = left_df.merge(
                right_df,
                how=join_how,
                left_on=left_col,
                right_on=right_col
            )
        
        else:
            # Same column name in both tables
            if join_on not in left_df.columns:
                raise KeyError(f"Column '{join_on}' not found in source '{left_id}'")
            if join_on not in right_df.columns:
                raise KeyError(f"Column '{join_on}' not found in source '{right_id}'")
            
            logger.debug(f"Joining on: {join_on}")
            
            result_df = left_df.merge(right_df, how=join_how, on=join_on)
        
        rows_after = len(result_df)
        logger.info(f"After {join_how} join: {rows_before} → {rows_after} rows")
        
        return result_df
    
    except Exception as e:
        logger.error(f"Failed to join {left_id} with {right_id}: {e}", exc_info=True)
        raise


def join_sources(dfs, join_conf):
    """
    Join multiple sources in sequence.
    
    Supports: inner, left, right, outer joins
    
    Args:
        dfs: Dict of {source_id: dataframe}
        join_conf: Join configuration (list or dict)
            
            Format 1 - List of sequential joins (applied left to right):
            [
              {
                "left": "employees",
                "right": "performance",
                "how": "left",      # or: inner, right, outer
                "on": "id"
              },
              {
                "left": <r>,   # Result of previous join becomes new left
                "right": "salary",
                "how": "inner",
                "on": "id"
              }
            ]
            
            Format 2 - Simple dict (2 sources only):
            {
              "left": "employees",
              "right": "performance",
              "how": "inner",       # or: left, right, outer
              "on": "id"
            }
    
    Returns:
        Joined dataframe
    
    Raises:
        ValueError: If join config is invalid or sources not found
    """
    
    # Handle dict format (2 sources)
    if isinstance(join_conf, dict):
        left_id = join_conf.get("left")
        right_id = join_conf.get("right")
        
        if not left_id or not right_id:
            raise ValueError("'left' and 'right' source IDs required in join config")
        
        if left_id not in dfs:
            raise ValueError(f"Left source '{left_id}' not found")
        if right_id not in dfs:
            raise ValueError(f"Right source '{right_id}' not found")
        
        left_df = dfs[left_id]
        right_df = dfs[right_id]
        
        return join_two_sources(left_df, right_df, left_id, right_id, join_conf)
    
    # Handle list format (multiple sources)
    elif isinstance(join_conf, list):
        if not join_conf:
            raise ValueError("Join config list is empty")
        
        logger.info(f"Applying {len(join_conf)} sequential joins")
        
        # Process first join
        first_join = join_conf[0]
        left_id = first_join.get("left")
        right_id = first_join.get("right")
        
        if not left_id or not right_id:
            raise ValueError("First join requires 'left' and 'right' source IDs")
        
        if left_id not in dfs:
            raise ValueError(f"Left source '{left_id}' not found")
        if right_id not in dfs:
            raise ValueError(f"Right source '{right_id}' not found")
        
        result_df = join_two_sources(dfs[left_id], dfs[right_id], left_id, right_id, first_join)
        
        # Apply remaining joins
        for i, join_step in enumerate(join_conf[1:], start=1):
            right_id = join_step.get("right")
            
            if not right_id:
                raise ValueError(f"Join step {i+1} requires 'right' source ID")
            
            if right_id not in dfs:
                raise ValueError(f"Right source '{right_id}' not found")
            
            right_df = dfs[right_id]
            
            # Result of previous join becomes the new left
            result_df = join_two_sources(
                result_df,
                right_df,
                f"<result of join {i}>",
                right_id,
                join_step
            )
        
        return result_df
    
    else:
        raise ValueError("join_conf must be dict or list of dicts")