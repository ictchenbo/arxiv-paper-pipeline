import requests
import feedparser
from typing import Optional, Dict, List
from datetime import datetime
from config import config
from src.models.paper import Paper, PaperAuthor, PaperCategory
from src.storage.es_client import es_client
from src.utils.logger import logger
from src.utils.retry import retry_on_exception

class ArXivAPIClient:
    """arXiv API客户端，用于元数据查询"""
    
    def __init__(self):
        self.api_url = config.arxiv.api_url
        self.headers = {"User-Agent": config.arxiv.user_agent}
    
    @retry_on_exception(max_attempts=3, exceptions=(requests.RequestException,))
    def get_paper_metadata(self, paper_id: str) -> Optional[Dict]:
        """调用arXiv API查询单篇论文元数据"""
        try:
            params = {
                "id_list": paper_id,
                "max_results": 1
            }
            
            response = requests.get(
                self.api_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            # 解析XML返回
            feed = feedparser.parse(response.content)
            if not feed.entries:
                logger.warning(f"API未查询到论文元数据: {paper_id}")
                return None
            
            entry = feed.entries[0]
            
            # 解析作者
            authors = []
            for author in entry.get("authors", []):
                authors.append(PaperAuthor(name=author.get("name", "")))
            
            # 解析分类
            primary_category = ""
            secondary_categories = []
            if "arxiv_primary_category" in entry:
                primary_category = entry.arxiv_primary_category.get("term", "")
            for tag in entry.get("tags", []):
                if tag.get("scheme") == "http://arxiv.org/schemas/atom":
                    term = tag.get("term", "")
                    if term != primary_category and term not in secondary_categories:
                        secondary_categories.append(term)
            
            # 解析日期
            submitted_date = None
            if "published" in entry:
                submitted_date = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ")
            
            updated_date = None
            if "updated" in entry:
                updated_date = datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%SZ")
            
            metadata = {
                "paper_id": paper_id,
                "title": entry.get("title", "").replace("\n", " ").strip(),
                "authors": [a.dict() for a in authors],
                "abstract": entry.get("summary", "").replace("\n", " ").strip(),
                "categories": PaperCategory(
                    primary=primary_category,
                    secondary=secondary_categories
                ).dict(),
                "submitted_date": submitted_date,
                "updated_date": updated_date,
                "doi": entry.get("arxiv_doi", ""),
                "journal_ref": entry.get("arxiv_journal_ref", ""),
                "comment": entry.get("arxiv_comment", "")
            }
            
            logger.debug(f"API查询元数据成功: {paper_id}")
            return metadata
        
        except Exception as e:
            logger.error(f"API查询元数据失败 {paper_id}: {e}", exc_info=True)
            return None

class MetadataCompleter:
    """元数据补全器：解析结果字段缺失时，优先从ES缓存查询，没有则调用arXiv API补全"""
    
    # 需要补全的字段列表（优先级：解析结果>ES缓存>API）
    COMPLEMENT_FIELDS = [
        "title", "authors", "abstract", "categories",
        "submitted_date", "updated_date", "doi", "journal_ref", "comment"
    ]
    
    _api_client = ArXivAPIClient()
    
    @classmethod
    def complement(cls, paper: Paper) -> Paper:
        """补全Paper缺失的字段"""
        # 先查ES缓存
        cached_metadata = es_client.get_metadata(paper.paper_id)
        metadata = cached_metadata
        
        # 缓存没有则调用API
        if not metadata:
            metadata = cls._api_client.get_paper_metadata(paper.paper_id)
            # 查到后存入缓存
            if metadata:
                es_client.save_metadata(paper.paper_id, metadata)
        
        if not metadata:
            logger.warning(f"无可用元数据用于补全: {paper.paper_id}")
            return paper
        
        complemented_count = 0
        for field in cls.COMPLEMENT_FIELDS:
            # 如果解析结果字段为空，且元数据有该字段，则补全
            paper_value = getattr(paper, field, None)
            metadata_value = metadata.get(field)
            
            if paper_value is None or (isinstance(paper_value, str) and not paper_value.strip()):
                if metadata_value is not None:
                    # 特殊处理：categories需要从dict转换为PaperCategory对象
                    if field == "categories" and isinstance(metadata_value, dict):
                        from src.models.paper import PaperCategory
                        metadata_value = PaperCategory(**metadata_value)
                    
                    # 特殊处理：authors需要从dict列表转换为PaperAuthor对象列表
                    if field == "authors" and isinstance(metadata_value, list) and len(metadata_value) > 0:
                        if isinstance(metadata_value[0], dict):
                            from src.models.paper import PaperAuthor
                            metadata_value = [PaperAuthor(**a) for a in metadata_value]
                    
                    setattr(paper, field, metadata_value)
                    complemented_count += 1
                    logger.debug(f"补全字段 {field}: {paper.paper_id}")
        
        if complemented_count > 0:
            logger.info(f"论文 {paper.paper_id} 补全了 {complemented_count} 个缺失字段")
        
        return paper
