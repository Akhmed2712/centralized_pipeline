import logging

def get_logger(name=__name__):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)

        logger.addHandler(ch)

    return logger