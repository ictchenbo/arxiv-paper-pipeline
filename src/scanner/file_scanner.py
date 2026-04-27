import os
import json
import time
from typing import Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from config import config
from src.utils.logger import logger
from src.parser.html_parser import HTMLParser
from src.parser.markdown_parser import MarkdownParser
from src.processor.paper_processor import PaperProcessor
from src.kafka.producer import kafka_producer
from src.storage.es_client import es_client

class FileHandler(FileSystemEventHandler):
    """文件变化事件处理器"""
    def __init__(self, scanner):
        self.scanner = scanner
    
    def on_created(self, event):
        if event.is_directory:
            return
        file_path = event.src_path
        if file_path.endswith(".html") or file_path.endswith(".pdf"):
            logger.debug(f"监测到新文件: {file_path}")
            self.scanner._process_file(file_path)

class FileScanner:
    """文件扫描监测器，支持实时监测+全量定期扫描"""
    
    def __init__(self):
        self.save_dir = config.download.save_dir
        self.processed_file = config.scan.processed_file_record
        self.min_file_age = config.scan.min_file_age
        self.scan_interval = config.scan.scan_interval
        
        # 确保目录存在
        os.makedirs(self.save_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
        
        # 加载已处理文件列表
        self.processed_files: Set[str] = self._load_processed_files()
        logger.info(f"已加载历史处理文件数: {len(self.processed_files)}")
        
        # 初始化观察者
        self.event_handler = FileHandler(self)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.save_dir, recursive=True)
    
    def _load_processed_files(self) -> Set[str]:
        """加载已处理文件列表"""
        if os.path.exists(self.processed_file):
            try:
                with open(self.processed_file, "r", encoding="utf-8") as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(f"已处理文件记录加载失败: {e}")
        return set()
    
    def _save_processed_files(self):
        """保存已处理文件列表"""
        os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
        with open(self.processed_file, "w", encoding="utf-8") as f:
            json.dump(list(self.processed_files), f, indent=2, ensure_ascii=False)
    
    def _is_file_ready(self, file_path: str) -> bool:
        """判断文件是否写入完成（大小不再变化）"""
        try:
            if not os.path.exists(file_path):
                return False
            
            # 检查文件年龄
            mtime = os.path.getmtime(file_path)
            age = time.time() - mtime
            if age < self.min_file_age:
                return False
            
            # 检查大小是否稳定
            size1 = os.path.getsize(file_path)
            time.sleep(0.5)
            size2 = os.path.getsize(file_path)
            return size1 == size2 and size1 > 0
        
        except Exception as e:
            logger.debug(f"文件状态检查失败 {file_path}: {e}")
            return False
    
    def _extract_paper_id(self, file_path: str) -> str:
        """从文件路径提取paper_id"""
        filename = os.path.basename(file_path)
        return filename.rsplit(".", 1)[0]
    
    def _process_file(self, file_path: str):
        """处理单个文件"""
        try:
            # 跳过已处理文件
            if file_path in self.processed_files:
                return
            
            # 等待文件写入完成
            if not self._is_file_ready(file_path):
                logger.debug(f"文件未准备好，稍后重试: {file_path}")
                return
            
            paper_id = self._extract_paper_id(file_path)
            file_type = file_path.rsplit(".", 1)[1].lower()

            # 检查ES中是否已存在，避免重复处理
            if es_client.paper_exists(paper_id):
                logger.info(f"论文已在ES中存在，跳过处理: {paper_id}")
                self.processed_files.add(file_path)
                self._save_processed_files()
                return

            logger.info(f"开始处理新文件: {paper_id}.{file_type}")

            if file_type == "html":
                # HTML直接本地解析+处理+入库
                paper = HTMLParser.parse(file_path, paper_id)
                if paper:
                    success = PaperProcessor.process(paper)
                    if success:
                        self.processed_files.add(file_path)
                        self._save_processed_files()
                        logger.info(f"✅ HTML处理完成: {paper_id}")
                else:
                    logger.error(f"❌ HTML解析失败: {file_path}")
            
            elif file_type == "pdf":
                # 先检查是否已有对应的Markdown文件
                md_path = file_path.replace(".pdf", ".md")
                if os.path.exists(md_path):
                    # 已有Markdown文件，直接本地解析处理，不走Kafka和MinerU
                    logger.info(f"发现已存在的Markdown文件，直接解析处理: {paper_id}")
                    md_parser = MarkdownParser()
                    paper = md_parser.parse(paper_id=paper_id, file_path=md_path)
                    if paper:
                        success = PaperProcessor.process(paper)
                        if success:
                            self.processed_files.add(file_path)
                            self._save_processed_files()
                            logger.info(f"✅ PDF处理完成（复用已有Markdown）: {paper_id}")
                        else:
                            logger.error(f"❌ 论文处理失败: {paper_id}")
                    else:
                        logger.error(f"❌ Markdown解析失败: {md_path}")
                else:
                    # 没有Markdown文件，发送到Kafka队列异步处理
                    success = kafka_producer.send(
                        topic=config.kafka.topics["pdf"],
                        key=paper_id,
                        value={
                            "paper_id": paper_id,
                            "file_path": file_path,
                            "file_type": "pdf"
                        }
                    )
                    if success:
                        self.processed_files.add(file_path)
                        self._save_processed_files()
                        logger.info(f"✅ PDF已发送到解析队列: {paper_id}")
        
        except Exception as e:
            logger.error(f"❌ 文件处理失败 {file_path}: {e}", exc_info=True)
    
    def _full_scan(self):
        """全量扫描目录，处理遗漏文件"""
        logger.info("🔍 开始全量目录扫描...")
        processed_count = 0
        
        for root, dirs, files in os.walk(self.save_dir):
            for file in files:
                if file.endswith(".html") or file.endswith(".pdf"):
                    file_path = os.path.join(root, file)
                    if file_path not in self.processed_files:
                        self._process_file(file_path)
                        processed_count += 1
        
        logger.info(f"✅ 全量扫描完成，处理遗漏文件数: {processed_count}")
    
    def start(self):
        """启动扫描服务"""
        # 先做一次全量扫描
        self._full_scan()
        
        # 启动实时监测
        self.observer.start()
        logger.info(f"✅ 文件监测服务启动成功，监听目录: {self.save_dir}")
        
        try:
            while True:
                # 定期全量扫描兜底
                time.sleep(self.scan_interval)
                self._full_scan()
        except KeyboardInterrupt:
            self.observer.stop()
            logger.info("🛑 文件监测服务停止")
        self.observer.join()
