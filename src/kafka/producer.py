import json
from kafka import KafkaProducer
from config import config
from src.utils.logger import logger
from src.utils.retry import retry_on_exception

class KafkaProducerClient:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_producer()
        return cls._instance
    
    @retry_on_exception(max_attempts=5, exceptions=(Exception,))
    def _init_producer(self):
        """初始化Kafka生产者"""
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            retries=3,
            acks="all",
            linger_ms=5,
            batch_size=16384,
            buffer_memory=33554432
        )
        logger.info("Kafka生产者初始化成功")
    
    def send(self, topic: str, value: dict, key: str = None):
        """发送消息到Kafka"""
        try:
            if key:
                key_bytes = key.encode("utf-8")
            else:
                key_bytes = None
            
            future = self.producer.send(topic, value=value, key=key_bytes)
            # 等待发送结果（可选，生产环境可以异步）
            future.get(timeout=10)
            logger.debug(f"消息发送成功到topic {topic}, key: {key}")
            return True
        except Exception as e:
            logger.error(f"消息发送失败到topic {topic}, key: {key}, 错误: {e}")
            return False
    
    def close(self):
        """关闭生产者"""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka生产者已关闭")

kafka_producer = KafkaProducerClient()
