#!/usr/bin/env python3
"""
创建Kafka Topic
无需安装kafka命令行工具，依赖kafka-python，已在requirements.txt中
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config import config
from src.utils.logger import logger

# 可修改配置
TOPICS = [
    {"name": config.kafka.topics["pdf"], "partitions": 8, "replication_factor": 1},
    {"name": config.kafka.topics["markdown"], "partitions": 4, "replication_factor": 1}
]

def init_kafka_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=config.kafka.bootstrap_servers.split(","),
            client_id="arxiv-init-client"
        )
        
        existing_topics = admin_client.list_topics()
        topic_list = []
        
        for topic_info in TOPICS:
            topic_name = topic_info["name"]
            if topic_name in existing_topics:
                logger.info(f"ℹ️ Kafka Topic {topic_name} 已存在，跳过创建")
                continue
            
            topic_list.append(NewTopic(
                name=topic_name,
                num_partitions=topic_info["partitions"],
                replication_factor=topic_info["replication_factor"]
            ))
        
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            for topic in topic_list:
                logger.info(f"✅ Kafka Topic 创建成功: {topic.name}, 分区数: {topic.num_partitions}, 副本数: {topic.replication_factor}")
        else:
            logger.info("ℹ️ 所有Topic已存在，无需创建")
        
        admin_client.close()
        logger.info("🎉 Kafka Topic初始化完成")
    
    except TopicAlreadyExistsError:
        logger.info("ℹ️ 部分Topic已存在，跳过")
    except Exception as e:
        logger.error(f"❌ Kafka Topic创建失败: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    init_kafka_topics()
