import time
from typing import List, Optional
from openai import OpenAI
from config import config
from src.utils.logger import logger
from src.utils.retry import retry_on_exception

class EmbedClient:
    """向量化客户端，兼容OpenAI API接口"""
    
    def __init__(self):
        self.client = OpenAI(
            base_url=config.embed.base_url,
            api_key=config.embed.api_key,
            timeout=config.embed.timeout
        )
        self.model = config.embed.model
        self.vector_dim = config.embed.vector_dim
        self.batch_size = config.embed.batch_size
        self.max_input_length = config.embed.max_input_length
    
    @retry_on_exception(max_attempts=3, exceptions=(Exception,))
    def embed_text(self, text: str) -> Optional[List[float]]:
        """单个文本向量化"""
        try:
            if not text or not text.strip():
                return None
            
            # 截断超长文本
            if len(text) > self.max_input_length:
                text = text[:self.max_input_length]
            
            response = self.client.embeddings.create(
                model=self.model,
                input=[text],
                dimensions=self.vector_dim
            )
            
            vector = response.data[0].embedding

            # 验证向量维度是否匹配配置
            if len(vector) != self.vector_dim:
                logger.error(f"向量维度不匹配！配置: {self.vector_dim}, 实际返回: {len(vector)}")
                raise ValueError(f"向量维度不匹配，请检查配置文件中embed.vector_dim是否与模型实际输出维度一致")

            logger.debug(f"文本向量化完成，维度: {len(vector)}")
            return vector
            
        except Exception as e:
            logger.error(f"文本向量化失败: {e}", exc_info=True)
            return None
    
    @retry_on_exception(max_attempts=3, exceptions=(Exception,))
    def embed_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """批量文本向量化"""
        try:
            if not texts:
                return []
            
            results = []
            # 分批处理
            for i in range(0, len(texts), self.batch_size):
                batch = texts[i:i + self.batch_size]
                
                # 过滤空文本并截断超长文本
                processed_batch = []
                for text in batch:
                    if text and text.strip():
                        if len(text) > self.max_input_length:
                            text = text[:self.max_input_length]
                        processed_batch.append(text)
                    else:
                        processed_batch.append(" ")
                
                if not processed_batch:
                    results.extend([None] * len(batch))
                    continue
                
                response = self.client.embeddings.create(
                    model=self.model,
                    input=processed_batch,
                    dimensions=self.vector_dim
                )
                
                # 按原始顺序返回结果
                batch_results = [None] * len(batch)
                for j, embedding in enumerate(response.data):
                    vector = embedding.embedding
                    # 验证向量维度是否匹配配置
                    if len(vector) != self.vector_dim:
                        logger.error(f"向量维度不匹配！配置: {self.vector_dim}, 实际返回: {len(vector)}")
                        raise ValueError(f"向量维度不匹配，请检查配置文件中embed.vector_dim是否与模型实际输出维度一致")
                    batch_results[j] = vector

                results.extend(batch_results)
                logger.debug(f"批量向量化完成，批次 {i//self.batch_size + 1}, 数量: {len(batch)}")
                
                # 避免请求过快
                time.sleep(0.1)
            
            return results
            
        except Exception as e:
            logger.error(f"批量向量化失败: {e}", exc_info=True)
            return [None] * len(texts)

# 全局实例
embed_client = EmbedClient()