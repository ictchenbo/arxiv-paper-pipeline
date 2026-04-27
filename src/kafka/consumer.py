import json
import os
from kafka import KafkaConsumer, TopicPartition
from concurrent.futures import ThreadPoolExecutor
from config import config
from src.utils.logger import logger
from src.utils.retry import retry_on_exception
from src.parser.pdf_parser import PDFParser
from src.parser.markdown_parser import MarkdownParser
from src.processor.paper_processor import PaperProcessor
from src.models.paper import Paper
from src.storage.es_client import es_client

class KafkaConsumerClient:
    _instance = None
    
    def __new__(cls, topic_type: str = None):
        if cls._instance is None:
            if topic_type is None:
                raise ValueError("首次初始化Consumer必须传入topic_type参数")
            cls._instance = super().__new__(cls)
            cls._instance._init_consumer(topic_type)
        return cls._instance
    
    @retry_on_exception(max_attempts=5, exceptions=(Exception,))
    def _init_consumer(self, topic_type: str):
        """初始化Kafka消费者"""
        self.topic_type = topic_type
        self.topic = config.kafka.topics.get(topic_type)
        if not self.topic:
            raise ValueError(f"不支持的topic类型: {topic_type}, 支持的类型: {list(config.kafka.topics.keys())}")
        
        # 初始化处理器
        if topic_type == "pdf":
            self.processor = PDFParser()
        elif topic_type == "markdown":
            self.markdown_parser = MarkdownParser()
            self.processor = PaperProcessor()
        else:
            raise ValueError(f"无对应处理器的topic类型: {topic_type}")
        
        # 并发配置：默认等于MinerU集群节点数*2，最大化利用集群能力
        self.max_concurrent = config.kafka.max_concurrent or len(config.mineru.api_servers) * 2
        self.max_poll_records = config.kafka.max_poll_records or self.max_concurrent
        self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent, thread_name_prefix=f"kafka-consumer-{topic_type}")
        
        # 初始化消费者
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=config.kafka.bootstrap_servers.split(","),
            group_id=config.kafka.consumer_group,
            auto_offset_reset="earliest",  # 首次消费从最早的消息开始
            enable_auto_commit=False,  # 手动提交偏移量，保证消息处理成功才提交
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_records=self.max_poll_records,  # 每次最多拉取匹配并发数的消息
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
        logger.info(f"Kafka {topic_type}消费者初始化成功，监听topic: {self.topic}, 并发数: {self.max_concurrent}")
    
    def _process_message(self, message) -> bool:
        """处理单条消息"""
        try:
            data = message.value
            logger.debug(f"收到消息, offset: {message.offset}, 内容: {data}")
            
            paper_id = data.get("paper_id")
            file_path = data.get("file_path")
            if not paper_id or not file_path:
                logger.error(f"消息格式错误，缺少paper_id或file_path: {data}")
                return False

            # 检查ES中是否已存在，避免重复处理
            if es_client.paper_exists(paper_id):
                logger.info(f"论文已在ES中存在，跳过处理: {paper_id}")
                return True  # 返回成功让offset正常提交

            # 调用对应处理器处理
            if self.topic_type == "pdf":
                success = self.processor.parse(file_path, paper_id)
            elif self.topic_type == "markdown":
                # 调用MarkdownParser解析文件
                paper = self.markdown_parser.parse(paper_id=paper_id, file_path=file_path)
                if not paper:
                    logger.error(f"Markdown解析失败: {paper_id}, 文件: {file_path}")
                    return False
                # 调用PaperProcessor处理
                success = self.processor.process(paper)
            else:
                logger.error(f"未知topic类型: {self.topic_type}")
                return False
            
            if not success:
                logger.error(f"消息处理失败, offset: {message.offset}, paper_id: {paper_id}")
                return False
            
            logger.debug(f"消息处理成功, offset: {message.offset}, paper_id: {paper_id}")
            return True
        except Exception as e:
            logger.error(f"消息处理异常, offset: {message.offset}, 错误: {e}", exc_info=True)
            return False
    
    def start(self):
        """启动消费者循环：批量拉取+多线程并发处理"""
        logger.info(f"开始消费topic {self.topic}...")
        try:
            while True:
                # 批量拉取消息
                messages = self.consumer.poll(timeout_ms=1000)
                if not messages:
                    continue
                
                try:
                    for tp, records in messages.items():
                        futures = {}
                        # 提交所有消息到线程池并发处理，记录offset
                        for record in records:
                            future = self.executor.submit(self._process_message, record)
                            futures[future] = record

                        # 等待当前批次所有消息处理完成
                        success_offsets = []
                        fail_count = 0
                        for future in futures:
                            record = futures[future]
                            try:
                                success = future.result()
                                if success:
                                    success_offsets.append(record.offset)
                                else:
                                    fail_count += 1
                                    logger.warning(f"消息处理失败，offset: {record.offset}, paper_id: {record.value.get('paper_id', 'unknown')}")
                            except Exception as e:
                                logger.error(f"消息处理异常 offset:{record.offset}, paper_id:{record.value.get('paper_id', 'unknown')}, 错误:{e}", exc_info=True)
                                fail_count += 1

                        # 计算当前批次成功消息的最大offset
                        if success_offsets:
                            max_success_offset = max(success_offsets)
                            next_offset = max_success_offset + 1
                            # 提交到最大成功offset + 1（下次从这个位置开始）
                            try:
                                self.consumer.commit()
                                logger.info(f"分区 {tp} 批次处理完成：共{len(records)}条，成功{len(success_offsets)}条，失败{fail_count}条，offset提交到 {next_offset}")
                            except Exception as e:
                                logger.error(f"Offset提交失败 offset:{next_offset}, 错误:{e}", exc_info=True)
                        else:
                            logger.warning(f"分区 {tp} 批次全部失败：共{len(records)}条，不提交offset，下次重试")
                except Exception as batch_e:
                    logger.error(f"批次处理异常: {batch_e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("消费者被用户中断，正在停止...")
        finally:
            self.executor.shutdown(wait=True)
            self.consumer.close()
            logger.info("Kafka消费者已关闭")

# 全局实例按需初始化
kafka_consumer = None
