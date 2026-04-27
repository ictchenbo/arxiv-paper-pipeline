from src.models.paper import Paper
from src.processor.metadata_completer import MetadataCompleter
from src.processor.chunk_splitter import chunk_splitter
from src.processor.embed_client import embed_client
from src.storage.es_client import es_client
from src.utils.logger import logger

class PaperProcessor:
    """论文全流程处理器：元数据补全→chunk拆分→向量化→ES入库"""
    
    @classmethod
    def process(cls, paper: Paper) -> bool:
        """处理单篇论文，返回是否成功"""
        try:
            logger.info(f"开始处理论文: {paper.paper_id}")
            logger.debug(f"论文正文长度: {len(paper.content_text) if paper.content_text else 0} 字符")

            # 1. 元数据补全
            paper = MetadataCompleter.complement(paper)
            
            # 2. 摘要向量化
            abstract_vector = None
            if paper.abstract and paper.abstract.strip():
                abstract_vector = embed_client.embed_text(paper.abstract)
                if not abstract_vector:
                    logger.warning(f"摘要向量化失败: {paper.paper_id}")
            
            # 3. 正文拆分+向量化
            content_chunks = []
            if paper.content_text and paper.content_text.strip():
                # 拆分chunk
                chunks = chunk_splitter.split(paper.content_text)
                if chunks:
                    # 批量向量化所有chunk
                    chunk_texts = [c["text"] for c in chunks]
                    chunk_vectors = embed_client.embed_batch(chunk_texts)
                    
                    # 组装结果
                    for i, chunk in enumerate(chunks):
                        vector = chunk_vectors[i]
                        if vector:
                            content_chunks.append({
                                "chunk_id": chunk["chunk_id"],
                                "text": chunk["text"],
                                "vector": vector
                            })
            
            logger.info(f"论文处理完成: {paper.paper_id}, 摘要向量: {'成功' if abstract_vector else '失败'}, 有效chunk数: {len(content_chunks)}")
            
            # 4. 保存到ES
            success = es_client.save_paper(
                paper=paper,
                abstract_vector=abstract_vector,
                content_chunks=content_chunks
            )
            
            if success:
                logger.info(f"论文入库成功: {paper.paper_id} - {paper.title}")
            else:
                logger.error(f"论文入库失败: {paper.paper_id}")
            
            return success
        
        except Exception as e:
            logger.error(f"论文处理失败 {paper.paper_id}: {e}", exc_info=True)
            return False
    
    @classmethod
    def process_batch(cls, papers: list[Paper], batch_size: int = 10) -> int:
        """批量处理论文，使用ES bulk写入提升性能
        返回成功处理的论文数
        """
        if not papers:
            return 0
        
        logger.info(f"开始批量处理论文，总数: {len(papers)}, 批次大小: {batch_size}")
        
        success_count = 0
        es_batch = []
        
        for paper in papers:
            try:
                # 单篇论文处理逻辑（不含ES写入）
                paper = MetadataCompleter.complement(paper)
                
                # 摘要向量化
                abstract_vector = None
                if paper.abstract and paper.abstract.strip():
                    abstract_vector = embed_client.embed_text(paper.abstract)
                
                # 正文拆分+向量化
                content_chunks = []
                if paper.content_text and paper.content_text.strip():
                    chunks = chunk_splitter.split(paper.content_text)
                    if chunks:
                        chunk_texts = [c["text"] for c in chunks]
                        chunk_vectors = embed_client.embed_batch(chunk_texts)
                        for i, chunk in enumerate(chunks):
                            vector = chunk_vectors[i]
                            if vector:
                                content_chunks.append({
                                    "chunk_id": chunk["chunk_id"],
                                    "text": chunk["text"],
                                    "vector": vector
                                })
                
                # 加入ES批量队列
                es_batch.append({
                    "paper": paper,
                    "abstract_vector": abstract_vector,
                    "content_chunks": content_chunks
                })
                
                # 达到批次大小则写入
                if len(es_batch) >= batch_size:
                    batch_success = es_client.bulk_save_papers(es_batch)
                    success_count += batch_success
                    logger.info(f"批次写入完成，累计成功: {success_count}/{len(papers)}")
                    es_batch = []
                
            except Exception as e:
                logger.error(f"批量处理单篇论文失败 {paper.paper_id}: {e}", exc_info=True)
                continue
        
        # 处理剩余数据
        if es_batch:
            batch_success = es_client.bulk_save_papers(es_batch)
            success_count += batch_success
        
        logger.info(f"批量处理完成，总成功: {success_count}/{len(papers)}")
        return success_count
