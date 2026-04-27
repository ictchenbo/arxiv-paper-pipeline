import os
import time
import requests
import random
from typing import List, Dict, Optional
from contextlib import contextmanager
from config import config
from src.utils.logger import logger
from src.utils.retry import retry_on_exception

class MinerUAPIClient:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_client()
        return cls._instance
    
    def _init_client(self):
        """初始化MinerU API客户端，加载集群配置"""
        self.api_servers = config.mineru.api_servers
        self.api_path = config.mineru.api_path
        self.health_check_path = config.mineru.health_check_path
        self.timeout = config.mineru.api_timeout
        self.max_retry = config.mineru.max_retry
        self.load_balance_strategy = config.mineru.load_balance_strategy
        self.health_check_interval = config.mineru.health_check_interval
        self.shared_storage = config.mineru.shared_storage
        self.auth_token = config.mineru.auth_token
        
        # 服务健康状态记录
        self.server_health: Dict[str, Dict] = {}
        for server in self.api_servers:
            self.server_health[server] = {
                "healthy": True,
                "last_check_time": 0,
                "fail_count": 0,
                "active_conn": 0  # 活跃连接数统计，用于least_conn策略
            }
        
        self.headers = {}
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
        
        logger.info(f"MinerU API客户端初始化完成，集群节点数: {len(self.api_servers)}, 负载均衡策略: {self.load_balance_strategy}")
    
    @contextmanager
    def _connection_counter(self, server: str):
        """连接数统计上下文管理器，用于least_conn策略"""
        try:
            self.server_health[server]["active_conn"] += 1
            yield
        finally:
            self.server_health[server]["active_conn"] -= 1
            if self.server_health[server]["active_conn"] < 0:
                self.server_health[server]["active_conn"] = 0
    
    def _check_server_health(self, server: str) -> bool:
        """检查指定服务节点的健康状态"""
        now = time.time()
        # 距离上次检查未超过间隔，直接返回上次状态
        if now - self.server_health[server]["last_check_time"] < self.health_check_interval:
            return self.server_health[server]["healthy"]
        
        try:
            resp = requests.get(
                f"{server}{self.health_check_path}",
                timeout=5,
                headers=self.headers
            )
            healthy = resp.status_code == 200
            self.server_health[server]["healthy"] = healthy
            if healthy:
                self.server_health[server]["fail_count"] = 0
            logger.debug(f"MinerU节点 {server} 健康检查结果: {'正常' if healthy else '异常'}")
        except Exception as e:
            self.server_health[server]["healthy"] = False
            self.server_health[server]["fail_count"] += 1
            logger.warning(f"MinerU节点 {server} 健康检查失败: {e}")
        
        self.server_health[server]["last_check_time"] = now
        return self.server_health[server]["healthy"]
    
    def _get_available_server(self) -> Optional[str]:
        """根据负载均衡策略获取可用服务节点"""
        healthy_servers = [s for s in self.api_servers if self._check_server_health(s)]
        if not healthy_servers:
            logger.error("所有MinerU服务节点都不可用")
            return None
        
        if self.load_balance_strategy == "random":
            return random.choice(healthy_servers)
        elif self.load_balance_strategy == "round_robin":
            # 简单轮询实现
            if not hasattr(self, "_rr_index"):
                self._rr_index = 0
            server = healthy_servers[self._rr_index % len(healthy_servers)]
            self._rr_index += 1
            return server
        elif self.load_balance_strategy == "least_fail":
            # 最少失败次数优先
            return sorted(healthy_servers, key=lambda s: self.server_health[s]["fail_count"])[0]
        elif self.load_balance_strategy == "least_conn" or self.load_balance_strategy == "least_connection":
            # 最少活跃连接数优先
            return sorted(healthy_servers, key=lambda s: self.server_health[s]["active_conn"])[0]
        else:
            # 默认随机
            return random.choice(healthy_servers)
    
    @retry_on_exception(max_attempts=3, exceptions=(Exception,))
    def parse_pdf(self, file_path: str, extract_images: bool = False) -> Optional[str]:
        try:
            # 获取可用服务节点
            server = self._get_available_server()
            if not server:
                logger.error(f"无可用MinerU服务节点，无法解析: {file_path}")
                return None
            
            logger.debug(f"使用MinerU节点 {server} 解析PDF: {file_path}")

            with self._connection_counter(server):
                # 上传文件模式
                with open(file_path, "rb") as f:
                    files = {"file": (os.path.basename(file_path), f, "application/pdf")}
                    data = {
                        "response_content": "markdown",
                        # "extract_images": extract_images,
                        "method": "auto"
                    }
                    resp = requests.post(
                        f"{server}{self.api_path}",
                        files=files,
                        data=data,
                        headers=self.headers,
                        timeout=self.timeout
                    )
                
                if resp.status_code != 200:
                    logger.error(f"MinerU解析失败，状态码: {resp.status_code}, 响应: {resp.text}, 节点: {server}")
                    # 标记节点异常
                    self.server_health[server]["fail_count"] += 1
                    if self.server_health[server]["fail_count"] >= 3:
                        self.server_health[server]["healthy"] = False
                    raise RuntimeError(f"MinerU节点 {server} 返回状态码 {resp.status_code}")
                
                resp_data = resp.json()
                if "data" not in resp_data:
                    logger.error(f"MinerU解析返回错误: {resp_data}, 节点: {server}")
                    return None
                
                # 兼容两种返回格式：参考extract_v2的返回结构
                data = resp_data.get("data", resp_data)
                if isinstance(data, dict) and "extract_data" in data:
                    markdown_content = data["extract_data"]
                elif isinstance(data, str):
                    markdown_content = data
                else:
                    markdown_content = str(data)
                
                if not markdown_content or len(markdown_content.strip()) < 10:
                    logger.warning(f"MinerU解析返回内容为空或过短: {file_path}")
                    return None
                
                # 请求成功，重置失败计数
                self.server_health[server]["fail_count"] = 0
                
                logger.debug(f"PDF解析成功: {file_path}, 内容长度: {len(markdown_content)}")
                return markdown_content
        
        except Exception as e:
            logger.error(f"MinerU解析PDF异常 {file_path}, 错误: {e}", exc_info=True)
            raise  # 抛出异常让重试装饰器处理

# 全局单例
mineru_api_client = MinerUAPIClient()
