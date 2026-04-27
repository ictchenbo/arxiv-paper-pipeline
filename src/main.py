#!/usr/bin/env python3
"""
统一入口文件
命令：
python src/main.py metadata          # 已废弃，元数据自动API补全
python src/main.py downloader        # 启动自动下载服务
python src/main.py scanner           # 启动文件扫描监测服务
python src/main.py consumer <topic>  # 启动消费者，topic可选：pdf/markdown
"""
import os
import sys
import argparse
from src.utils.logger import logger

# 自动创建必要目录
os.makedirs("./data/downloads", exist_ok=True)
os.makedirs("./logs", exist_ok=True)

def run_metadata_service():
    """启动元数据处理服务（已废弃，现在元数据通过API自动补全）"""
    logger.info("ℹ️ 元数据服务已升级为自动API补全模式，无需单独启动")

def run_downloader_service():
    """启动自动下载服务"""
    logger.info("🚀 启动arXiv自动下载服务...")
    from src.downloader.arxiv_downloader import ArXivDownloader
    downloader = ArXivDownloader()
    downloader.run()
    logger.info("✅ 下载服务启动完成")

def run_scanner_service():
    """启动文件扫描监听服务"""
    logger.info("🚀 启动文件扫描监听服务...")
    from src.scanner import FileScanner
    watcher = FileScanner()
    watcher.start()
    logger.info("✅ 文件扫描监听服务启动完成")

def run_consumer_service(topic_type: str):
    """启动消费者服务"""
    logger.info(f"🚀 启动{topic_type}消费者服务...")
    from src.kafka.consumer import KafkaConsumerClient
    
    consumer = KafkaConsumerClient(topic_type)
    consumer.start()
    logger.info(f"✅ {topic_type}消费者服务启动完成")

def main():
    parser = argparse.ArgumentParser(description="arXiv论文处理pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True, help="可用命令")
    
    # 子命令定义
    # subparsers.add_parser("metadata", help="(已废弃)启动元数据处理服务")
    subparsers.add_parser("download", help="启动自动下载服务")
    subparsers.add_parser("scan", help="启动文件扫描监听服务")
    
    consumer_parser = subparsers.add_parser("consumer", help="启动消费者服务")
    consumer_parser.add_argument("topic_type", choices=["pdf", "markdown"], help="消费的topic类型")
    
    args = parser.parse_args()
    
    try:
        if args.command == "metadata":
            run_metadata_service()
        elif args.command == "download":
            run_downloader_service()
        elif args.command == "scan":
            run_scanner_service()
        elif args.command == "consumer":
            run_consumer_service(args.topic_type)
    except KeyboardInterrupt:
        logger.info("🛑 服务被用户中断，正在退出...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ 服务运行失败: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
