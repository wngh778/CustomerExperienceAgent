import logging
import yaml
import os

yaml_path = "/".join(os.path.abspath(__file__).split("/")[:-1]) + "/logging.yaml"

def get_logger(name: str) -> logging.Logger:
    """로거 생성 (logging.yaml 로드)"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    return logger