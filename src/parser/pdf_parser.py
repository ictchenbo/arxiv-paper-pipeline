import os
from config import config
from src.utils.logger import logger
from src.parser.mineru_api_client import mineru_api_client
from src.parser.markdown_parser import MarkdownParser
from src.processor.paper_processor import PaperProcessor

class PDFParser:
    """PDF解析器，基于MinerU API集群转成Markdown"""
    _markdown_parser = MarkdownParser()

    @classmethod
    def parse(cls, file_path: str, paper_id: str) -> bool:
        """解析PDF文件，转成Markdown后直接解析处理，不再走Kafka
        如果已存在对应的Markdown文件，直接解析Markdown，跳过MinerU API调用
        """
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                logger.error(f"PDF文件不存在: {file_path}")
                return False

            # 计算Markdown路径
            md_path = file_path.replace(".pdf", ".md")

            # 如果已存在Markdown文件，直接解析，跳过MinerU API
            if os.path.exists(md_path):
                logger.info(f"发现已存在的Markdown文件，跳过MinerU解析: {paper_id} -> {md_path}")
                paper = cls._markdown_parser.parse(paper_id=paper_id, file_path=md_path)
                if not paper:
                    logger.error(f"Markdown解析失败: {paper_id}")
                    return False

                # 直接调用PaperProcessor处理
                success = PaperProcessor.process(paper)
                if success:
                    logger.info(f"PDF全流程处理成功（复用已有Markdown）: {paper_id}")
                else:
                    logger.error(f"论文处理失败: {paper_id}")
                return success

            # 调用MinerU API集群解析PDF
            logger.info(f"开始调用MinerU集群解析PDF: {paper_id}")
            markdown_content = mineru_api_client.parse_pdf(file_path)

            if not markdown_content:
                logger.error(f"MinerU解析PDF失败: {paper_id}")
                return False

            # 直接调用MarkdownParser解析（传内容，省掉一次文件IO）
            paper = cls._markdown_parser.parse(paper_id=paper_id, md_content=markdown_content)
            if not paper:
                logger.error(f"Markdown解析失败: {paper_id}")
                return False

            # 直接调用PaperProcessor处理
            success = PaperProcessor.process(paper)
            if not success:
                logger.error(f"论文处理失败: {paper_id}")
                return False

            # 保存Markdown到本地作为备份（方便后续复用）
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            logger.debug(f"Markdown备份已保存: {md_path}")

            logger.info(f"PDF全流程处理成功: {paper_id}")
            return True

        except Exception as e:
            logger.error(f"PDF解析失败 {file_path}: {e}", exc_info=True)
            return False
