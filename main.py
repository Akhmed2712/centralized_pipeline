import os
import argparse
from dotenv import load_dotenv
from helper.runner import run_feed, load_pipeline_config
from utils.logger import get_logger

logger = get_logger("pipeline.main")

def load_env_file(env):
    env_file = f".env.{env}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
        logger.info(f"Loaded env file: {env_file}")
    else:
        logger.info(f"No env file found for {env}, using system env vars")
        
def main():
    parser = argparse.ArgumentParser(description="Centralized Data Pipeline")
    parser.add_argument("--feed", required=True, help="Feed name (e.g. fg_customer)")
    parser.add_argument("--env", required=True, help="Environment (dev/uat/prod)")
    args = parser.parse_args()

    load_env_file(args.env)

    logger.info(f"Starting pipeline | feed={args.feed} | env={args.env}")

    config_path = f"config/{args.env}/pipelines.yml"
    if not os.path.exists(config_path):
        logger.error(f"Pipeline config not found: {config_path}")
        return

    config = load_pipeline_config(config_path)

    feed_conf = config.get(args.feed)
    if not feed_conf:
        logger.error(f"Feed '{args.feed}' not found in {config_path}")
        return

    df = run_feed(feed_name=args.feed, feed_conf=feed_conf, env=args.env)

    if df is not None:
        logger.info(f"Pipeline finished | feed={args.feed} | rows={len(df)}")
    else:
        logger.error(f"Pipeline failed | feed={args.feed}")

if __name__ == "__main__":
    main()