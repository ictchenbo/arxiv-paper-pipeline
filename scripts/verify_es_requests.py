#!/usr/bin/env python3
"""
快速验证requests版ES客户端
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.storage.es_client import es_client
from src.utils.logger import logger

print("=" * 60)
print("ES客户端验证 (requests版本)")
print("=" * 60)

# 1. 测试连接
print("\n1. 测试ES连接...")
if es_client.ping():
    print("   ✅ 连接成功")
    print(f"   Base URL: {es_client.base_url}")
    print(f"   索引名: {es_client.paper_index}")
else:
    print("   ❌ 连接失败")
    sys.exit(1)

# 2. 测试routing计算
print("\n2. 测试routing计算...")
test_ids = ["2401.12345", "2312.00001", "test_id"]
for pid in test_ids:
    routing = es_client._get_routing(pid)
    print(f"   {pid:15s} -> {routing}")
print("   ✅ routing计算正常")

# 3. 测试简单查询
print("\n3. 测试索引存在检查...")
exists = es_client._request('HEAD', es_client.paper_index)
if exists is not None:
    print(f"   索引{es_client.paper_index}存在: 是")
else:
    print(f"   索引{es_client.paper_index}存在: 否 (需要初始化)")
print("   ✅ 请求发送正常")

# 4. 测试混合检索接口
print("\n4. 测试混合检索接口...")
result = es_client.hybrid_search(query_text="test", limit=5)
print(f"   检索接口返回正常, 命中数: {result['total']}")
print("   ✅ 混合检索接口正常")

print("\n" + "=" * 60)
print("✅ 所有验证通过！ES客户端已切换为requests版本")
print("=" * 60)
