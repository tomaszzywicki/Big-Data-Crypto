import logging
import sys


def get_logger(name: str):
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger
