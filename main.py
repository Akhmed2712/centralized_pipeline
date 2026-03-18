import argparse
from helper.runner import run_feed
from utils.logger import get_logger

logger = get_logger("pipeline.main")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feed", required=True, help="Feed name")
    parser.add_argument("--env", required=True, help="Environment (dev/uat/prod)")

    args = parser.parse_args()

    logger.info(f"Running feed={args.feed} env={args.env}")

    run_feed(feed_name=args.feed, env=args.env)

if __name__ == "__main__":
    main()