from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime

class PaperAuthor(BaseModel):
    """作者模型"""
    name: str
    affiliation: Optional[str] = None

class PaperCategory(BaseModel):
    """分类模型"""
    primary: str
    secondary: List[str] = Field(default_factory=list)

class Paper(BaseModel):
    """论文核心模型"""
    paper_id: str = Field(description="arXiv论文ID，唯一主键")
    title: str = Field(description="论文标题")
    authors: List[PaperAuthor] = Field(description="作者列表")
    abstract: str = Field(description="摘要")
    categories: PaperCategory = Field(description="分类")
    submitted_date: Optional[datetime] = Field(description="提交日期")
    updated_date: Optional[datetime] = None
    doi: Optional[str] = None
    journal_ref: Optional[str] = None
    comment: Optional[str] = None
    
    # 解析后字段
    content_html: Optional[str] = None
    content_markdown: Optional[str] = None
    content_text: Optional[str] = Field(description="纯文本正文")
    references: List[Dict] = Field(default_factory=list, description="参考文献")
    figures: List[Dict] = Field(default_factory=list, description="图表信息")
    tables: List[Dict] = Field(default_factory=list, description="表格信息")
    
    # 元信息
    file_type: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    parsed_at: Optional[datetime] = Field(default_factory=datetime.now)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }