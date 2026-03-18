import pandas as pd
from utils.logger import get_logger

logger = get_logger("pipeline.join")

def apply_join(left_df, right_df, join_conf):
    """
    Apply join operation between two DataFrames.
    
    Args:
        left_df: Left DataFrame
        right_df: Right DataFrame
        join_conf: Join configuration dict with:
            - type: 'inner', 'left', 'right', 'outer'
            - on: Column name(s) to join on
            - left_on: (optional) Column name in left_df
            - right_on: (optional) Column name in right_df
            - suffix: (optional) Tuple of suffixes for duplicate columns
    
    Returns:
        Joined DataFrame
    """
    try:
        join_type = join_conf.get("type", "inner").lower()
        on = join_conf.get("on")
        left_on = join_conf.get("left_on")
        right_on = join_conf.get("right_on")
        suffix = join_conf.get("suffix", ("_x", "_y"))
        
        logger.info(f"Performing {join_type} join")
        logger.info(f"Left DataFrame shape: {left_df.shape}")
        logger.info(f"Right DataFrame shape: {right_df.shape}")
        
        # Determine join keys
        if on:
            # Join on same column name in both DataFrames
            df_joined = pd.merge(
                left_df, 
                right_df, 
                on=on, 
                how=join_type,
                suffixes=suffix
            )
        elif left_on and right_on:
            # Join on different column names
            df_joined = pd.merge(
                left_df, 
                right_df, 
                left_on=left_on, 
                right_on=right_on, 
                how=join_type,
                suffixes=suffix
            )
        else:
            raise ValueError("Must specify either 'on' or both 'left_on' and 'right_on'")
        
        logger.info(f"Joined result shape: {df_joined.shape}")
        logger.info(f"{join_type.upper()} join successful")
        
        return df_joined
    
    except Exception as e:
        logger.error(f"Failed to perform join: {e}")
        raise


def apply_joins(df, joins_conf, extract_func):
    """
    Apply multiple sequential joins to a DataFrame.
    
    Args:
        df: Initial DataFrame (left side of first join)
        joins_conf: List of join configurations
        extract_func: Function to extract data sources (called for each join)
    
    Returns:
        Final joined DataFrame
    """
    if not joins_conf:
        logger.debug("No joins configured")
        return df
    
    result_df = df
    
    for idx, join_conf in enumerate(joins_conf, 1):
        try:
            logger.info(f"Applying join #{idx}")
            
            # Extract the right DataFrame
            source_conf = join_conf.get("source")
            source_conf["env"] = join_conf.get("env", "dev")
            
            right_df = extract_func(source_conf)
            logger.info(f"Join #{idx} - Extracted {len(right_df)} rows for right side")
            
            # Apply the join
            result_df = apply_join(result_df, right_df, join_conf)
            
        except Exception as e:
            logger.error(f"Failed at join #{idx}: {e}")
            raise
    
    return result_df