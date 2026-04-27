import sys
from loguru import logger
from config import config

# 日志配置
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=config.server.log_level if hasattr(config.server, 'log_level') else "INFO",
    enqueue=True
)

# 按日志级别输出到文件
logger.add(
    "./logs/error.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="ERROR",
    rotation="1 day",
    retention="30 days",
    compression="zip",
    enqueue=True
)

logger.add(
    "./logs/debug.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    rotation="1 day",
    retention="7 days",
    compression="zip",
    enqueue=True
)

__all__ = ["logger"]