#!/usr/bin/env python3
"""
初始化ES索引
自动读取config/es_index_config.json配置，替换向量维度为配置文件中的值
依赖：requests
"""
import os
import json
import requests
from config import config
from src.utils.logger import logger

def init_es(force_recreate=False):
    es_config = config.storage.es
    base_url = es_config.hosts[0].rstrip('/')
    
    # 检查ES连接
    try:
        r = requests.get(f"{base_url}/", timeout=10)
        r.raise_for_status()
        es_version = r.json().get('version', {}).get('number', 'unknown')
        logger.info(f"ES连接成功，版本: {es_version}")
    except Exception as e:
        logger.error(f"ES服务连接失败，请检查ES是否正常运行: {e}")
        raise ConnectionError("无法连接到Elasticsearch服务")
    
    # 读取索引配置模板
    config_path = os.path.join(os.path.dirname(__file__), "../config/es_index_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        index_config = f.read()
    
    # 替换向量维度为实际配置值
    index_config = index_config.replace("{{VECTOR_DIM}}", str(config.embed.vector_dim))
    index_config = json.loads(index_config)
    
    # 创建论文主索引
    paper_index = es_config.paper_index
    
    # 检查索引是否存在
    r = requests.head(f"{base_url}/{paper_index}", timeout=10)
    index_exists = r.status_code == 200
    
    if force_recreate and index_exists:
        logger.warning(f"Deleting existing index {paper_index}, all data will be lost!")
        r = requests.delete(f"{base_url}/{paper_index}", timeout=60)
        r.raise_for_status()
        index_exists = False

    if not index_exists:
        r = requests.put(
            f"{base_url}/{paper_index}",
            json=index_config,
            timeout=120
        )
        r.raise_for_status()

        logger.info(f"ES paper index {paper_index} created successfully, vector dim: {config.embed.vector_dim}")
        logger.info(f"   - Shards: {index_config['settings']['number_of_shards']}")
        logger.info(f"   - Routing: enabled, by paper_id prefix")
        logger.info(f"   - HNSW params: m=16, ef_construction=100")
    else:
        logger.info(f"ES paper index {paper_index} already exists, skipping creation")
        logger.info("   To use new config: python scripts/init_es.py --force")
    
    # 创建元数据缓存索引
    metadata_index = "arxiv_metadata_cache"
    
    r = requests.head(f"{base_url}/{metadata_index}", timeout=10)
    metadata_exists = r.status_code == 200
    
    if not metadata_exists:
        metadata_mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "index": {
                    "refresh_interval": "60s"
                }
            },
            "mappings": {
                "_routing": {"required": True},
                "properties": {
                    "paper_id": {"type": "keyword", "store": True},
                    "title": {"type": "text", "index": False},
                    "authors": {"type": "nested"},
                    "abstract": {"type": "text", "index": False},
                    "categories": {"type": "object"},
                    "submitted_date": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            }
        }
        r = requests.put(
            f"{base_url}/{metadata_index}",
            json=metadata_mapping,
            timeout=60
        )
        r.raise_for_status()
        logger.info(f"ES metadata cache index {metadata_index} created successfully")
    else:
        logger.info(f"ES metadata cache index {metadata_index} already exists, skipping creation")

    logger.info("ES initialization complete")

if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv or "-f" in sys.argv
    init_es(force_recreate=force)
