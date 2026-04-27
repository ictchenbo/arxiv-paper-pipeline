#!/usr/bin/env python3
"""
ES检索接口测试与性能验证
验证各种检索模式的效果和性能
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import time
import requests
from src.storage.es_client import es_client
from src.processor.embed_client import embed_client
from src.utils.logger import logger

def test_hybrid_search():
    """测试混合检索"""
    print("\n" + "="*60)
    print("测试1: 纯全文检索")
    print("="*60)
    
    start = time.time()
    result = es_client.hybrid_search(
        query_text="transformer attention mechanism",
        limit=10
    )
    elapsed = time.time() - start
    
    print(f"查询耗时: {elapsed*1000:.2f}ms")
    print(f"命中结果: {result['total']}")
    for r in result["results"][:3]:
        print(f"  - {r['paper_id']}: {r['title'][:60]}... (score={r['score']:.3f})")
    
    print("\n" + "="*60)
    print("测试2: 纯向量检索")
    print("="*60)
    
    # 生成查询向量
    query_vector = embed_client.embed_text("large language model training optimization")
    if not query_vector:
        print("向量生成失败，跳过此测试")
        return
    
    start = time.time()
    result = es_client.hybrid_search(
        query_vector=query_vector,
        limit=10,
        min_score=0.6
    )
    elapsed = time.time() - start
    
    print(f"查询耗时: {elapsed*1000:.2f}ms")
    print(f"命中结果: {result['total']}")
    for r in result["results"][:3]:
        print(f"  - {r['paper_id']}: {r['title'][:60]}... (score={r['score']:.3f})")
    
    print("\n" + "="*60)
    print("测试3: 全文+向量混合检索 (RRF重排)")
    print("="*60)
    
    start = time.time()
    result = es_client.hybrid_search(
        query_text="deep learning neural network",
        query_vector=query_vector,
        category="cs.CL",
        limit=10
    )
    elapsed = time.time() - start
    
    print(f"查询耗时: {elapsed*1000:.2f}ms")
    print(f"命中结果: {result['total']}")
    for r in result["results"][:3]:
        print(f"  - {r['paper_id']}: {r['title'][:60]}... (score={r['score']:.3f})")
    
    print("\n" + "="*60)
    print("测试4: 带年份过滤的检索")
    print("="*60)
    
    start = time.time()
    result = es_client.hybrid_search(
        query_text="large language model",
        year=2024,
        limit=10
    )
    elapsed = time.time() - start
    
    print(f"查询耗时: {elapsed*1000:.2f}ms")
    print(f"命中结果: {result['total']}")
    for r in result["results"][:3]:
        print(f"  - {r['paper_id']}: {r['title'][:60]}... (score={r['score']:.3f})")

def test_chunk_search():
    """测试正文chunk向量检索"""
    print("\n" + "="*60)
    print("测试5: 正文chunk向量检索 (nested内knn)")
    print("="*60)
    
    query_vector = embed_client.embed_text("transformer attention mechanism explanation")
    if not query_vector:
        print("向量生成失败，跳过此测试")
        return
    
    start = time.time()
    result = es_client.chunk_vector_search(
        query_vector=query_vector,
        limit=5
    )
    elapsed = time.time() - start
    
    print(f"查询耗时: {elapsed*1000:.2f}ms")
    print(f"命中结果: {result['total']}")
    for r in result["results"][:2]:
        print(f"\n  论文: {r['paper_id']} - {r['title'][:50]}... (score={r['score']:.3f})")
        for chunk in r.get("matched_chunks", []):
            text = chunk.get("text", "")[:100]
            print(f"    Chunk {chunk.get('chunk_id')}: {text}...")

def test_routing_optimization():
    """测试routing优化效果"""
    print("\n" + "="*60)
    print("验证: routing键计算逻辑")
    print("="*60)
    
    test_ids = [
        "2401.12345",
        "2402.67890",
        "2312.00001",
        "old_format_id"
    ]
    
    for pid in test_ids:
        routing = es_client._get_routing(pid)
        print(f"  paper_id: {pid:20s} -> routing: {routing}")
    
    print("\n此设计确保同月份论文落在同一分片，减少跨分片查询开销")

def test_direct_rest():
    """测试直接REST API调用"""
    print("\n" + "="*60)
    print("测试6: 直接REST API健康检查")
    print("="*60)
    
    base_url = es_client.base_url
    
    try:
        r = requests.get(f"{base_url}/", timeout=5)
        info = r.json()
        print(f"ES版本: {info['version']['number']}")
        print(f"集群名称: {info['cluster_name']}")
        print(f"状态: {info['tagline']}")
        print("✅ REST API工作正常")
    except Exception as e:
        print(f"❌ REST API测试失败: {e}")

def main():
    print("ES检索优化验证工具 (requests版本)")
    print("="*60)
    
    # 检查ES连接
    if not es_client.ping():
        print("❌ ES连接失败，请先启动ES服务")
        return
    
    print("✅ ES连接成功 (requests REST API)")
    
    # 验证routing逻辑
    test_routing_optimization()
    
    # 测试REST API
    test_direct_rest()
    
    # 测试检索接口
    try:
        test_hybrid_search()
    except Exception as e:
        print(f"混合检索测试失败: {e}")
        print("提示: 如果索引为空，请先导入测试数据")
    
    try:
        test_chunk_search()
    except Exception as e:
        print(f"Chunk检索测试失败: {e}")
    
    print("\n" + "="*60)
    print("✅ 所有测试完成!")
    print("="*60)
    
    es_client.close()

if __name__ == "__main__":
    main()
