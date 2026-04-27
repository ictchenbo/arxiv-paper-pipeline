"""
统一配置模块，所有配置相关定义/加载逻辑都集中在此目录
配置文件：config/default.yaml / config/prod.yaml等
"""
import os
import yaml
from typing import Dict, Any, List
from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()

class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topics: Dict[str, str]
    consumer_group: str
    max_concurrent: int | None = None
    max_poll_records: int | None = None

class ESConfig(BaseModel):
    hosts: list[str]
    paper_index: str
    vector_dim: int
    username: str = ""
    password: str = ""

class StorageConfig(BaseModel):
    es: ESConfig

class ArxivConfig(BaseModel):
    api_url: str
    request_interval: int
    max_missing_count: int
    current_month_wait: int
    user_agent: str
    download_types: List[str]

class DownloadConfig(BaseModel):
    save_dir: str
    auto_download: bool
    download_concurrency: int
    start_month: str
    progress_file: str

class ScanConfig(BaseModel):
    scan_interval: int
    processed_file_record: str
    min_file_age: int

class MinerUConfig(BaseModel):
    api_servers: List[str]
    api_path: str
    health_check_path: str
    api_timeout: int
    max_retry: int
    load_balance_strategy: str
    health_check_interval: int
    shared_storage: bool
    auth_token: str

class ParserConfig(BaseModel):
    pdf_parser: str
    chunk_size: int
    chunk_overlap: int
    metadata_complement: bool

class EmbedConfig(BaseModel):
    base_url: str
    api_key: str
    model: str
    vector_dim: int
    batch_size: int
    timeout: int
    max_input_length: int

class ServerConfig(BaseModel):
    env: str
    log_level: str

class AppConfig(BaseModel):
    server: ServerConfig
    kafka: KafkaConfig
    storage: StorageConfig
    arxiv: ArxivConfig
    download: DownloadConfig
    scan: ScanConfig
    mineru: MinerUConfig
    parser: ParserConfig
    embed: EmbedConfig

def load_config(env: str = None) -> AppConfig:
    """加载配置文件，优先读取环境变量ENV指定的环境，否则用default"""
    if env is None:
        env = os.getenv("ENV", "default")
    
    # 配置文件都在当前config目录下
    config_dir = os.path.dirname(__file__)
    default_config_path = os.path.join(config_dir, "default.yaml")
    env_config_path = os.path.join(config_dir, f"{env}.yaml")
    
    # 加载默认配置
    with open(default_config_path, "r", encoding="utf-8") as f:
        default_config = yaml.safe_load(f)
    
    # 加载环境配置覆盖
    if os.path.exists(env_config_path):
        with open(env_config_path, "r", encoding="utf-8") as f:
            env_config = yaml.safe_load(f)
        # 合并配置
        def merge_dict(a: Dict, b: Dict) -> Dict:
            for k, v in b.items():
                if isinstance(v, dict) and k in a and isinstance(a[k], dict):
                    merge_dict(a[k], v)
                else:
                    a[k] = v
            return a
        config = merge_dict(default_config, env_config)
        print(f"Loaded {env} environment config")
    else:
        config = default_config
        print(f"Using default config (no {env} config found)")
    
    return AppConfig(**config)

# 全局配置单例
config = load_config()
