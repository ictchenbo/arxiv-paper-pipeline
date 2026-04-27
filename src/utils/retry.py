from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.utils.logger import logger

def retry_on_exception(max_attempts: int = 3, wait_min: int = 1, wait_max: int = 10, exceptions: tuple = (Exception,)):
    """通用重试装饰器"""
    def decorator(func):
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
            retry=retry_if_exception_type(exceptions),
            before_sleep=lambda retry_state: logger.warning(
                f"函数 {func.__name__} 执行失败，正在进行第 {retry_state.attempt_number} 次重试，错误: {retry_state.outcome.exception()}"
            )
        )
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator