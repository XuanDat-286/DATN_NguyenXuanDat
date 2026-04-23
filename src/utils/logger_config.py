# src/utils/logger_config.py

import logging
import yaml
from pathlib import Path

def setup_logger(config_path='config/config.yaml'):
    """Setup logging configuration"""
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    logger = logging.getLogger('ETL')
    logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
    
    file_handler = logging.FileHandler(log_config.get('file', 'logs/etl.log'), encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger