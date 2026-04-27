import os
import time
import requests
import json
from datetime import datetime
from typing import Tuple
from config import config
from src.utils.logger import logger
from src.storage.es_client import es_client

class ArXivDownloader:
    """arXiv自动下载器，按ID递增探测模式"""
    
    def __init__(self):
        self.save_dir = config.download.save_dir
        self.progress_file = config.download.progress_file
        self.request_interval = config.arxiv.request_interval
        self.max_missing_count = config.arxiv.max_missing_count
        self.current_month_wait = config.arxiv.current_month_wait
        self.headers = {"User-Agent": config.arxiv.user_agent}
        
        # 确保目录存在
        os.makedirs(self.save_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
        
        # 加载进度
        self.current_month, self.current_seq = self._load_progress()
        logger.info(f"当前下载进度: 月份={self.current_month}, 序号={self.current_seq}")
    
    def _load_progress(self) -> Tuple[str, int]:
        """加载下载进度，没有则初始化"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    progress = json.load(f)
                return progress.get("month", self._get_default_month()), progress.get("seq", 1)
            except Exception as e:
                logger.warning(f"进度文件加载失败，使用默认进度: {e}")
        
        return self._get_default_month(), 1
    
    def _save_progress(self):
        """保存下载进度"""
        progress = {
            "month": self.current_month,
            "seq": self.current_seq,
            "updated_at": datetime.now().isoformat()
        }
        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(progress, f, indent=2, ensure_ascii=False)
    
    def _get_default_month(self) -> str:
        """获取默认起始月份（当前月，格式yymm）"""
        if config.download.start_month:
            return config.download.start_month
        now = datetime.now()
        return f"{now.strftime('%y%m')}"
    
    def _next_month(self, month: str) -> str:
        """计算下一个月份，例如2604→2605，2612→2701"""
        year = int(month[:2])
        mon = int(month[2:])
        mon += 1
        if mon > 12:
            mon = 1
            year += 1
        return f"{year:02d}{mon:02d}"
    
    def _is_current_month(self, month: str) -> bool:
        """判断是否是当前月份"""
        now = datetime.now()
        current_month_str = f"{now.strftime('%y%m')}"
        return month == current_month_str
    
    def _generate_paper_id(self, month: str, seq: int) -> str:
        """生成论文ID，格式yymm.xxxxx"""
        return f"{month}.{seq:05d}"
    
    def _is_downloaded(self, paper_id: str) -> bool:
        """检查论文是否已经下载过/入库过"""
        # 先查ES是否已存在，避免重复下载处理
        if es_client.paper_exists(paper_id):
            return True

        mid_dir = paper_id[:4]
        
        # 再查本地文件是否存在
        html_path = os.path.join(self.save_dir, mid_dir, f"{paper_id}.html")
        pdf_path = os.path.join(self.save_dir, mid_dir, f"{paper_id}.pdf")
        if os.path.exists(html_path) or os.path.exists(pdf_path):
            return True
        
        return False
    
    def _probe_exists(self, paper_id: str) -> Tuple[bool, str]:
        """探测论文是否存在，返回(是否存在, 文件类型html/pdf)"""
        # 先探测html
        html_url = f"https://arxiv.org/html/{paper_id}"
        try:
            resp = requests.head(html_url, headers=self.headers, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                return True, "html"
        except Exception as e:
            logger.debug(f"HTML探测失败 {paper_id}: {e}")
        
        # 再探测pdf
        pdf_url = f"https://arxiv.org/pdf/{paper_id}.pdf"
        try:
            resp = requests.head(pdf_url, headers=self.headers, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                return True, "pdf"
        except Exception as e:
            logger.debug(f"PDF探测失败 {paper_id}: {e}")
        
        return False, ""
    
    def _download_file(self, paper_id: str, file_type: str) -> bool:
        """下载文件，保存到本地（下载与解析解耦：只存文件，后续由FileScanner统一处理）"""
        url = f"https://arxiv.org/{file_type}/{paper_id}"
        if file_type == "pdf":
            url += ".pdf"
        
        save_path = os.path.join(self.save_dir, paper_id[:4], f"{paper_id}.{file_type}")
        
        try:
            logger.info(f"开始下载: {paper_id}.{file_type}")
            resp = requests.get(url, headers=self.headers, timeout=120, stream=True)
            resp.raise_for_status()

            # 文件太小 大概率是网络被封
            if 'Content-Length' not in resp.headers or int(resp.headers['Content-Length']) < 1000:
                return False
            
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"下载完成: {paper_id}.{file_type}, 大小: {os.path.getsize(save_path)//1024}KB")
            return True
        
        except Exception as e:
            logger.error(f"下载失败 {paper_id}.{file_type}: {e}", exc_info=True)
            # 下载失败删除不完整文件
            if os.path.exists(save_path):
                os.remove(save_path)
            return False
    
    def run(self):
        """启动下载循环"""
        missing_count = 0
        
        logger.info("下载器启动成功，开始自动采集...")
        
        while True:
            # 生成当前ID
            paper_id = self._generate_paper_id(self.current_month, self.current_seq)
            
            try:
                # 检查是否已下载
                if self._is_downloaded(paper_id):
                    logger.debug(f"论文已存在，跳过: {paper_id}")
                    self.current_seq += 1
                    missing_count = 0
                    # time.sleep(self.request_interval)
                    continue
                
                # 探测是否存在
                exists, file_type = self._probe_exists(paper_id)
                if not exists:
                    logger.debug(f"论文不存在: {paper_id}")
                    missing_count += 1
                    self.current_seq += 1
                    
                    # 连续不存在达到阈值，处理月份跳转
                    if missing_count >= self.max_missing_count:
                        if self._is_current_month(self.current_month):
                            # 当前月，等待后重置序号重新扫
                            logger.info(f"当前月 {self.current_month} 已扫描完成，等待 {self.current_month_wait//60} 分钟后继续扫描新论文")
                            time.sleep(self.current_month_wait)
                            self.current_seq = 1
                            missing_count = 0
                        else:
                            # 历史月，跳转到下一个月
                            next_month = self._next_month(self.current_month)
                            logger.info(f"月份 {self.current_month} 已采集完成，跳转到下一个月: {next_month}")
                            self.current_month = next_month
                            self.current_seq = 1
                            missing_count = 0
                    
                    time.sleep(self.request_interval)
                    continue
                
                # 存在则下载
                logger.info(f"发现新论文: {paper_id} (类型: {file_type})")
                missing_count = 0
                download_success = self._download_file(paper_id, file_type)
                
                if download_success:
                    self.current_seq += 1
                else:
                    # 下载失败重试一次，下次循环再试
                    logger.warning(f"下载失败，稍后重试: {paper_id}")
                
                # 保存进度
                self._save_progress()
                
                # 强制等待间隔
                time.sleep(self.request_interval)
            
            except KeyboardInterrupt:
                logger.info("下载器被中断，保存进度后退出")
                self._save_progress()
                break
            except Exception as e:
                logger.error(f"下载循环异常: {e}", exc_info=True)
                time.sleep(self.request_interval * 2)
