import logging
import os
from utils.config import config

def setup_logging(config_file='config/config.ini'):
    """设置全局日志配置"""
    
    log_level = config.get('logging', 'level', fallback='INFO')
    log_file = config.get('logging', 'file', fallback='logs/stocks_strategy.log')
    
    # 创建日志目录（如果不存在）
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 配置日志格式
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

# 创建全局日志记录器
logger = setup_logging()