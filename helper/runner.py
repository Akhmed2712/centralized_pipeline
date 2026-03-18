import yaml
from helper.extract import extract_source
from utils.logger import get_logger

logger = get_logger("pipeline.runner")

def load_pipeline_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

def run_feed(feed_name, feed_conf, env="dev"):
    try:
        logger.info(f"Running feed: {feed_name}")

        # 1. Extract
        source_conf = feed_conf.get("sources")
        source_conf["env"] = env

        df = extract_source(source_conf)
        logger.info(f"{feed_name} - Extracted {len(df)} rows")

        # (Transform nanti bisa diaktifkan lagi)
        # df = apply_filter(...)

        return df

    except Exception as e:
        logger.error(f"{feed_name} - Failed: {e}", exc_info=True)
        return None