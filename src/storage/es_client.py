from datetime import datetime
from typing import Optional, Dict, List
import requests
import json
from requests.auth import HTTPBasicAuth
from config import config
from src.utils.logger import logger
from src.models.paper import Paper

class ESClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_client()
        return cls._instance

    def _init_client(self):
        """初始化ES客户端（基于requests）"""
        self.hosts = config.storage.es.hosts
        self.base_url = self.hosts[0].rstrip('/')  # 使用第一个节点
        self.paper_index = config.storage.es.paper_index
        self.metadata_index = "arxiv_metadata_cache"
        self.timeout = 60

        # ES认证配置
        self.username = config.storage.es.username
        self.password = config.storage.es.password
        self.auth = HTTPBasicAuth(self.username, self.password) if self.username and self.password else None

        if not self.ping():
            raise ConnectionError("ES服务连接失败")
        logger.info("ES客户端初始化成功" + ("（已启用认证）" if self.auth else ""))

    def _request(self, method: str, path: str, **kwargs) -> Optional[Dict]:
        """发送ES REST请求"""
        url = f"{self.base_url}/{path.lstrip('/')}"
        kwargs.setdefault('timeout', self.timeout)
        if self.auth:
            kwargs.setdefault('auth', self.auth)

        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            # 404是正常情况（文档不存在），不记录ERROR日志
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
                return e.response.json() if e.response.content else {}
            logger.error(f"ES请求失败 {method} {url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"响应内容: {e.response.text[:500]}")
            return None

    def ping(self) -> bool:
        """检查ES连接"""
        try:
            kwargs = {'timeout': 5}
            if self.auth:
                kwargs['auth'] = self.auth
            r = requests.get(f"{self.base_url}/", **kwargs)
            return r.status_code == 200
        except:
            return False
    
    @staticmethod
    def _get_routing(paper_id: str) -> str:
        """根据paper_id前缀计算routing，保证同月份论文在同一分片
        paper_id格式: 2401.xxxxx，取前4位作为路由键
        """
        if len(paper_id) >= 4 and paper_id[4] == '.':
            return paper_id[:4]
        return paper_id[:2] if len(paper_id) >= 2 else paper_id
    
    def _serialize_datetimes(self, data: Dict) -> Dict:
        """递归序列化datetime对象为ISO格式字符串"""
        result = {}
        for k, v in data.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, dict):
                result[k] = self._serialize_datetimes(v)
            elif isinstance(v, list):
                result[k] = [self._serialize_datetimes(item) if isinstance(item, dict) else item for item in v]
            else:
                result[k] = v
        return result

    def save_metadata(self, paper_id: str, metadata: Dict) -> bool:
        """保存元数据到缓存索引，用于后续补全"""
        try:
            doc = self._serialize_datetimes({
                **metadata,
                "updated_at": datetime.now()
            })
            routing = self._get_routing(paper_id)

            result = self._request(
                'PUT',
                f"{self.metadata_index}/_doc/{paper_id}?routing={routing}&refresh=wait_for",
                json=doc
            )

            return result is not None
        except Exception as e:
            logger.error(f"元数据缓存保存失败 {paper_id}: {e}")
            return False
    
    def get_metadata(self, paper_id: str) -> Optional[Dict]:
        """从缓存查询元数据，用于补全解析结果"""
        try:
            routing = self._get_routing(paper_id)
            result = self._request(
                'GET',
                f"{self.metadata_index}/_doc/{paper_id}?routing={routing}"
            )
            
            if result and result.get('found'):
                return result['_source']
            return None
        except Exception as e:
            logger.error(f"元数据查询失败 {paper_id}: {e}")
            return None
    
    def save_paper(self, paper: Paper, abstract_vector: List[float] = None, content_chunks: List[Dict] = None) -> bool:
        """保存完整论文数据到ES，包含结构化信息、全文字段、向量字段
        优化：使用routing保证同月份论文在同一分片，减少跨分片查询开销
        """
        try:
            paper_dict = paper.dict()
            # 向量字段
            if abstract_vector:
                paper_dict["abstract_vector"] = abstract_vector
            if content_chunks:
                paper_dict["content_chunks"] = content_chunks

            paper_dict["created_at"] = datetime.now()
            paper_dict["updated_at"] = datetime.now()

            # 序列化datetime对象
            paper_dict = self._serialize_datetimes(paper_dict)

            # 计算routing键
            routing = self._get_routing(paper.paper_id)

            # 幂等写入，存在则更新
            result = self._request(
                'PUT',
                f"{self.paper_index}/_doc/{paper.paper_id}?routing={routing}&refresh=wait_for",
                json=paper_dict
            )

            if result:
                logger.info(f"论文数据保存成功: {paper.paper_id} - {paper.title}")
                return True
            return False
        except Exception as e:
            logger.error(f"论文数据保存失败 {paper.paper_id}: {e}", exc_info=True)
            return False
    
    def bulk_save_papers(self, papers_data: List[Dict]) -> int:
        """批量保存论文，高性能批量写入
        papers_data格式: [{"paper": Paper, "abstract_vector": [...], "content_chunks": [...]}, ...]
        """
        actions = []
        for item in papers_data:
            paper = item["paper"]
            paper_dict = paper.dict()

            if item.get("abstract_vector"):
                paper_dict["abstract_vector"] = item["abstract_vector"]
            if item.get("content_chunks"):
                paper_dict["content_chunks"] = item["content_chunks"]

            paper_dict["created_at"] = datetime.now()
            paper_dict["updated_at"] = datetime.now()

            # 序列化datetime
            paper_dict = self._serialize_datetimes(paper_dict)

            routing = self._get_routing(paper.paper_id)

            # Bulk API 格式: 元数据行 + 文档行
            actions.append(json.dumps({
                "index": {
                    "_index": self.paper_index,
                    "_id": paper.paper_id,
                    "routing": routing
                }
            }))
            actions.append(json.dumps(paper_dict))

        if not actions:
            return 0

        bulk_body = "\n".join(actions) + "\n"

        try:
            result = self._request(
                'POST',
                "_bulk?refresh=wait_for",
                data=bulk_body,
                headers={"Content-Type": "application/x-ndjson"}
            )

            if result:
                success = sum(1 for item in result.get('items', [])
                            if 'index' in item and item['index'].get('status', 0) < 400)
                failed = len(result.get('items', [])) - success
                logger.info(f"批量写入完成: 成功{success}条, 失败{failed}条")
                return success
            return 0
        except Exception as e:
            logger.error(f"批量写入失败: {e}", exc_info=True)
            return 0
    
    def paper_exists(self, paper_id: str) -> bool:
        """检查论文是否已存在（带routing优化）"""
        routing = self._get_routing(paper_id)
        url = f"{self.base_url}/{self.paper_index}/_doc/{paper_id}?routing={routing}"
        try:
            response = requests.head(url, timeout=10, auth=self.auth)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"检查论文是否存在失败 {paper_id}: {e}")
            return False
    
    def get_paper(self, paper_id: str) -> Optional[Dict]:
        """获取单篇论文（带routing优化）"""
        try:
            routing = self._get_routing(paper_id)
            result = self._request(
                'GET',
                f"{self.paper_index}/_doc/{paper_id}?routing={routing}"
            )
            
            if result and result.get('found'):
                return result['_source']
            return None
        except Exception as e:
            logger.error(f"获取论文失败 {paper_id}: {e}")
            return None
    
    # ==================== 高性能检索接口 ====================
    
    def hybrid_search(
        self,
        query_text: str = None,
        query_vector: List[float] = None,
        category: str = None,
        year: int = None,
        offset: int = 0,
        limit: int = 20,
        min_score: float = 0.5
    ) -> Dict:
        """混合检索：全文+向量+结构化过滤，单查询高性能完成
        这是论文检索的核心接口，优化了过滤下推和routing
        """
        try:
            # 构建过滤条件（提前下推，减少向量计算量）
            filter_conditions = []
            if category:
                filter_conditions.append({
                    "term": {"categories.primary": category}
                })
            if year:
                filter_conditions.append({
                    "range": {"submitted_date": {"gte": f"{year}-01-01", "lte": f"{year}-12-31"}}
                })
            
            search_body = {
                "from": offset,
                "size": limit,
                "_source": ["paper_id", "title", "abstract", "authors", "categories", "submitted_date"]
            }
            
            knn_query = None
            if query_vector:
                knn_query = {
                    "field": "abstract_vector",
                    "query_vector": query_vector,
                    "k": limit * 2,
                    "num_candidates": min(limit * 10, 1000),
                    "similarity": min_score
                }
                if filter_conditions:
                    knn_query["filter"] = filter_conditions
                search_body["knn"] = knn_query
            
            # 构建全文检索query
            if query_text:
                match_query = {
                    "multi_match": {
                        "query": query_text,
                        "fields": ["title^3", "abstract^2", "content_text"],
                        "type": "best_fields",
                        "minimum_should_match": "70%"
                    }
                }
                if filter_conditions:
                    match_query = {
                        "bool": {
                            "must": match_query,
                            "filter": filter_conditions
                        }
                    }
                search_body["query"] = match_query
            
            # RRF重排序（同时有全文和向量时）
            if knn_query and query_text:
                search_body["rank"] = {"rrf": {"rank_constant": 60}}
            
            result = self._request(
                'POST',
                f"{self.paper_index}/_search",
                json=search_body
            )
            
            if not result:
                return {"total": 0, "results": []}
            
            results = []
            for hit in result.get("hits", {}).get("hits", []):
                results.append({
                    **hit["_source"],
                    "score": hit.get("_score", 0)
                })
            
            return {
                "total": result.get("hits", {}).get("total", {}).get("value", 0),
                "results": results
            }
            
        except Exception as e:
            logger.error(f"混合检索失败: {e}", exc_info=True)
            return {"total": 0, "results": [], "error": str(e)}
    
    def chunk_vector_search(
        self,
        query_vector: List[float],
        category: str = None,
        limit: int = 10,
        min_score: float = 0.5
    ) -> Dict:
        """正文chunk向量检索：nested字段内knn搜索
        注意：ES 8.x+支持nested内的knn搜索，此方法返回匹配的chunk信息
        """
        try:
            search_body = {
                "_source": ["paper_id", "title", "categories", "submitted_date"],
                "query": {
                    "nested": {
                        "path": "content_chunks",
                        "query": {
                            "knn": {
                                "field": "content_chunks.vector",
                                "query_vector": query_vector,
                                "k": limit,
                                "num_candidates": limit * 5,
                                "similarity": min_score
                            }
                        },
                        "inner_hits": {
                            "_source": ["content_chunks.chunk_id", "content_chunks.text"],
                            "size": 3
                        }
                    }
                },
                "size": limit
            }
            
            # 增加分类过滤
            if category:
                search_body["query"]["nested"]["query"] = {
                    "bool": {
                        "must": search_body["query"]["nested"]["query"],
                        "filter": [{"term": {"categories.primary": category}}]
                    }
                }
            
            result = self._request(
                'POST',
                f"{self.paper_index}/_search",
                json=search_body
            )
            
            if not result:
                return {"total": 0, "results": []}
            
            results = []
            for hit in result.get("hits", {}).get("hits", []):
                inner_hits = hit.get("inner_hits", {}).get("content_chunks", {}).get("hits", {}).get("hits", [])
                chunks = [h["_source"] for h in inner_hits]
                
                results.append({
                    **hit["_source"],
                    "score": hit.get("_score", 0),
                    "matched_chunks": chunks
                })
            
            return {
                "total": result.get("hits", {}).get("total", {}).get("value", 0),
                "results": results
            }
            
        except Exception as e:
            logger.error(f"chunk向量检索失败: {e}", exc_info=True)
            return {"total": 0, "results": [], "error": str(e)}
    
    def close(self):
        """requests不需要显式关闭连接"""
        logger.info("ES客户端已关闭")

es_client = ESClient()
