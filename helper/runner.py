import yaml
import pandas as pd
from helper.extract import extract_source
from helper.transform.filter import apply_filter
from helper.transform.join import join_sources
from utils.logger import get_logger
from helper.load import load_to_target

logger = get_logger("pipeline.runner")

def load_pipeline_config(path):
    """Load YAML pipeline configuration."""
    with open(path) as f:
        return yaml.safe_load(f)

def extract_all_sources(feed_conf, env):
    """Extract all configured sources into dataframes."""
    dfs = {}
    
    for source in feed_conf.get("sources", []):
        source["env"] = env
        try:
            df = extract_source(source)
            dfs[source["id"]] = df
            logger.info(f"Extracted source '{source['id']}': {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to extract source '{source['id']}': {e}")
            raise
    
    return dfs

def apply_feed_transforms(df, feed_conf):
    """Apply feed-level transforms."""
    if "transform" not in feed_conf:
        logger.debug("No feed-level transforms")
        return df
    
    transform_conf = feed_conf["transform"]
    rows_before = len(df)
    
    logger.info("Applying feed-level transforms")
    
    # Apply filter
    if "filter" in transform_conf:
        logger.debug(f"  - Applying feed-level filter")
        df = apply_filter(df, transform_conf["filter"])
    
    rows_after = len(df)
    logger.info(f"After feed-level transforms: {rows_before} → {rows_after} rows")
    
    return df

def run_feed(feed_name, feed_conf, env="dev"):
    """
    Run the feed pipeline:
    1. Extract all sources
    2. Join sources if join config exists
    3. Apply feed-level transforms
    
    Args:
        feed_name: Name of the feed
        feed_conf: Feed configuration dict
        env: Environment (dev, prod, etc.)
    
    Returns:
        Processed DataFrame or None on error
    """
    try:
        logger.info(f"{'='*60}")
        logger.info(f"Running feed: {feed_name}")
        logger.info(f"{'='*60}")
        
        # 1. Extract all sources
        dfs = extract_all_sources(feed_conf, env)
        
        if not dfs:
            logger.error(f"{feed_name} - No sources extracted")
            return None
        
        # 2. Join sources if configured, otherwise use first source
        if "join" in feed_conf:
            logger.info("Join configuration found, joining sources")
            df = join_sources(dfs, feed_conf["join"])
        else:
            # Use first source only
            source_id = feed_conf["sources"][0]["id"]
            df = dfs[source_id]
            logger.info(f"No join config, using source '{source_id}': {len(df)} rows")
        
        # 3. Apply feed-level transforms
        df = apply_feed_transforms(df, feed_conf)
        
        logger.info(f"{'='*60}")
        logger.info(f"Feed '{feed_name}' completed successfully: {len(df)} rows")
        logger.info(f"{'='*60}")

        # 4. Load to target (CSV or MySQL)
        if "target" in feed_conf:
            success = load_to_target(df, feed_conf["target"], feed_name)
            if not success:
                logger.error(f"{feed_name} - Failed to load to target")
                return None
        else:
            logger.debug("No load configuration found, returning dataframe only")
        
        logger.info(f"{'='*60}")
        logger.info(f"Feed '{feed_name}' completed successfully: {len(df)} rows")
        logger.info(f"{'='*60}")
        
        return df
    
    except Exception as e:
        logger.error(f"{feed_name} - Failed: {e}", exc_info=True)
        return None


# Example usage:
if __name__ == "__main__":
    config = load_pipeline_config("pipeline.yaml")
    feed_conf = config["fg_pegawai"]
    
    df = run_feed(
        feed_name="fg_pegawai",
        feed_conf=feed_conf,
        env="prod",
        primary_source_id="orders"
    )
    
    if df is not None:
        print(f"\nFinal result: {len(df)} rows")
        print(df.head())