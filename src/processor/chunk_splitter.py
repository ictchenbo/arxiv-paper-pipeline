from typing import List, Dict
from config import config
from src.utils.logger import logger

class ChunkSplitter:
    """文本分块器"""
    
    def __init__(self):
        self.chunk_size = config.parser.chunk_size
        self.chunk_overlap = config.parser.chunk_overlap
    
    def split(self, text: str) -> List[Dict]:
        """将文本按大小分块，带重叠"""
        if not text or not text.strip():
            return []
        
        chunks = []
        chunk_id = 0
        start = 0
        text_len = len(text)
        
        while start < text_len:
            end = min(start + self.chunk_size, text_len)
            
            # 尝试在句子边界处截断
            if end < text_len:
                # 向前查找最近的句号、问号、感叹号
                boundary = text.rfind("\n", start, end)
                if boundary != -1 and boundary > start + self.chunk_size // 2:
                    end = boundary + 1
            
            chunk_text = text[start:end].strip()
            if chunk_text:
                chunks.append({
                    "chunk_id": chunk_id,
                    "text": chunk_text,
                    "start": start,
                    "end": end
                })
                chunk_id += 1
            
            # 移动起始位置
            if end >= text_len:
                break
            start = end - self.chunk_overlap
            if start < 0:
                start = 0
        
        logger.debug(f"文本分块完成，共 {len(chunks)} 个chunk，原文长度: {text_len}")
        return chunks

# 全局实例
chunk_splitter = ChunkSplitter()