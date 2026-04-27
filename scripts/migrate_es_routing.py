#!/usr/bin/env python3
"""
ES索引数据迁移工具
将无routing的旧索引数据迁移到启用routing的新索引
使用场景：索引配置更新后，迁移历史数据
依赖：requests
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import json
from tqdm import tqdm
from config import config
from src.utils.logger import logger

def scroll_query(base_url: str, index: str, batch_size: int = 500):
    """使用scroll API遍历整个索引"""
    # 初始化scroll
    r = requests.post(
        f"{base_url}/{index}/_search?scroll=5m",
        json={
            "size": batch_size,
            "query": {"match_all": {}}
        },
        timeout=60
    )
    r.raise_for_status()
    result = r.json()
    
    scroll_id = result.get("_scroll_id")
    hits = result.get("hits", {}).get("hits", [])
    
    # 返回第一批
    for hit in hits:
        yield hit
    
    # 持续滚动直到没有数据
    while len(hits) > 0:
        r = requests.post(
            f"{base_url}/_search/scroll",
            json={
                "scroll": "5m",
                "scroll_id": scroll_id
            },
            timeout=60
        )
        r.raise_for_status()
        result = r.json()
        
        hits = result.get("hits", {}).get("hits", [])
        for hit in hits:
            yield hit

def migrate_data(old_index: str, new_index: str, batch_size: int = 500):
    """
    从旧索引迁移数据到新索引，自动添加routing
    """
    base_url = config.storage.es.hosts[0].rstrip('/')
    
    # 检查索引是否存在
    r = requests.head(f"{base_url}/{old_index}", timeout=10)
    if r.status_code != 200:
        logger.error(f"旧索引 {old_index} 不存在")
        return False
    
    r = requests.head(f"{base_url}/{new_index}", timeout=10)
    if r.status_code != 200:
        logger.error(f"新索引 {new_index} 不存在，请先运行 init_es.py")
        return False
    
    # 获取旧索引文档总数
    r = requests.get(f"{base_url}/{old_index}/_count", timeout=10)
    count = r.json()["count"]
    logger.info(f"开始迁移数据，总数: {count} 条")
    
    # 扫描旧索引数据
    batch = []
    success_count = 0
    failed_count = 0
    doc_ids = set()
    
    pbar = tqdm(total=count, desc="迁移进度")
    
    for doc in scroll_query(base_url, old_index, batch_size):
        _source = doc["_source"]
        paper_id = _source.get("paper_id", doc["_id"])
        
        # 计算routing
        if len(paper_id) >= 4 and paper_id[4] == '.':
            routing = paper_id[:4]
        else:
            routing = paper_id[:2] if len(paper_id) >= 2 else paper_id
        
        # 去重（scroll可能返回重复
        if paper_id in doc_ids:
            continue
        doc_ids.add(paper_id)
        
        # Bulk API格式
        batch.append(json.dumps({
            "index": {
                "_index": new_index,
                "_id": paper_id,
                "routing": routing
            }
        }))
        batch.append(json.dumps(_source))
        
        if len(batch) // 2 >= batch_size:
            # 执行bulk
            bulk_body = "\n".join(batch) + "\n"
            try:
                r = requests.post(
                    f"{base_url}/_bulk?refresh=wait_for",
                    data=bulk_body,
                    headers={"Content-Type": "application/x-ndjson"},
                    timeout=120
                )
                r.raise_for_status()
                result = r.json()
                
                batch_success = sum(1 for item in result.get('items', []) 
                                if 'index' in item and item['index'].get('status', 0) < 400)
                batch_failed = len(result.get('items', [])) - batch_success
                
                success_count += batch_success
                failed_count += batch_failed
                
                pbar.update(len(batch) // 2)
                pbar.set_postfix(成功=success_count, 失败=failed_count)
                
            except Exception as e:
                logger.error(f"批量写入失败: {e}")
                failed_count += len(batch) // 2
            
            batch = []
    
    # 处理剩余数据
    if batch:
        bulk_body = "\n".join(batch) + "\n"
        try:
            r = requests.post(
                f"{base_url}/_bulk?refresh=wait_for",
                data=bulk_body,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=120
            )
            r.raise_for_status()
            result = r.json()
            
            batch_success = sum(1 for item in result.get('items', []) 
                            if 'index' in item and item['index'].get('status', 0) < 400)
            batch_failed = len(result.get('items', [])) - batch_success
            
            success_count += batch_success
            failed_count += batch_failed
            
            pbar.update(len(batch) // 2)
            
        except Exception as e:
            logger.error(f"批量写入失败: {e}")
            failed_count += len(batch) // 2
    
    pbar.close()
    logger.info(f"迁移完成! 成功: {success_count}, 失败: {failed_count}")
    
    # 刷新索引
    requests.post(f"{base_url}/{new_index}/_refresh", timeout=30)
    
    # 验证数据
    r = requests.get(f"{base_url}/{new_index}/_count", timeout=10)
    new_count = r.json()["count"]
    logger.info(f"新索引文档数: {new_count}")
    
    return True

def verify_index_stats(index_name: str):
    """查看索引统计信息"""
    base_url = config.storage.es.hosts[0].rstrip('/')
    
    r = requests.get(f"{base_url}/{index_name}/_stats", timeout=30)
    r.raise_for_status()
    stats = r.json()
    
    logger.info(f"索引 {index_name} 统计:")
    logger.info(f"  文档数: {stats['indices'][index_name]['total']['docs']['count']}")
    logger.info(f"  存储大小: {stats['indices'][index_name]['total']['store']['size_in_bytes'] / 1024 / 1024:.2f} MB")
    logger.info(f"  分片数: {len(stats['indices'][index_name]['shards'])}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ES索引数据迁移工具")
    parser.add_argument("--old", default=None, help="旧索引名")
    parser.add_argument("--new", default=config.storage.es.paper_index, help="新索引名")
    parser.add_argument("--batch", type=int, default=500, help="批量大小")
    parser.add_argument("--stats", action="store_true", help="仅查看统计信息")
    
    args = parser.parse_args()
    
    if args.stats:
        verify_index_stats(args.new)
    elif args.old:
        migrate_data(args.old, args.new, args.batch)
    else:
        print("使用方法:")
        print("  查看索引统计: python migrate_es_routing.py --stats")
        print("  迁移数据: python migrate_es_routing.py --old papers_old --new papers")
