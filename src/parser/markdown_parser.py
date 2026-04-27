import os
import re
from typing import Optional, List, Dict
from src.models.paper import Paper, PaperCategory
from src.utils.logger import logger

class MarkdownParser:
    """MinerU输出的Markdown解析器"""

    def __init__(self):
        pass

    def parse(self, paper_id: str, file_path: str = None, md_content: str = None) -> Optional[Paper]:
        """
        解析Markdown生成Paper对象，支持两种传入方式：
        1. 传file_path：从本地文件读取内容
        2. 传md_content：直接传入Markdown字符串内容
        """
        try:
            if not file_path and not md_content:
                raise ValueError("file_path和md_content不能同时为空")

            if file_path and not md_content:
                # 从文件读取内容
                if not os.path.exists(file_path):
                    logger.error(f"Markdown文件不存在: {file_path}")
                    return None
                with open(file_path, "r", encoding="utf-8") as f:
                    md_content = f.read()

            # 清理Markdown头部（标题、作者、摘要、关键词）
            cleaned_md = self._remove_header_sections(md_content)

            # 提取纯文本正文
            content_text = self._extract_text(cleaned_md)

            # 提取参考文献
            references = self._extract_references(md_content)

            # 提取图表信息
            figures = self._extract_figures(md_content)
            tables = self._extract_tables(md_content)

            # PDF解析出来的元信息可能不全，留到元数据补全阶段填充
            paper = Paper(
                paper_id=paper_id,
                title="", # 元数据补全
                authors=[], # 元数据补全
                abstract="", # 元数据补全
                categories=PaperCategory(primary="", secondary=[]), # 元数据补全
                submitted_date=None, # 元数据补全
                # content_markdown=md_content,
                content_text=content_text,
                references=references,
                figures=figures,
                tables=tables,
                file_type="pdf",
                file_path=file_path.replace(".md", ".pdf") if file_path else ""
            )

            logger.info(f"Markdown解析成功: {paper_id}")
            return paper

        except Exception as e:
            logger.error(f"Markdown解析失败 {file_path}: {e}", exc_info=True)
            return None

    def _remove_header_sections(self, md_content: str) -> str:
        """移除Markdown头部的标题、作者、摘要、关键词等元数据区域"""
        if not md_content:
            return md_content

        lines = md_content.split('\n')
        result_lines = []
        in_header = True  # 标记是否在头部元数据区域
        skip_next_lines = 0  # 跳过后续N行（用于跳过摘要内容段落）

        # 正文开始标记模式（更宽松的匹配）
        header_end_patterns = [
            r'^#+\s*(1\.|Introduction|INTRODUCTION|1\s+Introduction)',  # 第一章开始
            r'^##\s',  # 二级标题开始（表示一级标题区域结束）
            r'^#+\s*(2\.|Related Work|RELATED WORK|Background|Methodology)',  # 也可以用第二章作为标记
        ]

        # 需要跳过的头部模式
        skip_patterns = [
            r'^#\s*Abstract',  # Abstract标题
            r'^#\s*Keywords?',  # Keywords标题
            r'^Keywords?\s*:',  # Keywords行
            r'^[A-Z][a-z]+\s+Keywords?\s*:',  # 如 "Index Terms—"
            r'^#\s*$',  # 空的#标题
            r'^!\[',  # 图片（页眉logo等）
            r'^http',  # 链接行
            r'.*\.(pdf|jpg|jpeg|png)\s*$',  # 文件名
        ]

        for i, line in enumerate(lines):
            stripped = line.strip()

            # 如果需要跳过后续行
            if skip_next_lines > 0:
                skip_next_lines -= 1
                continue

            # 检查是否到达正文开始（第一章）
            if in_header:
                for pattern in header_end_patterns:
                    if re.match(pattern, stripped, re.IGNORECASE):
                        in_header = False
                        break

                # 如果是跳过的模式，不添加到结果
                if in_header:
                    should_skip = False

                    for pattern in skip_patterns:
                        if re.match(pattern, stripped, re.IGNORECASE):
                            should_skip = True
                            # 如果是Abstract标题，还要跳过后面的摘要内容（3行）
                            if 'abstract' in pattern.lower():
                                skip_next_lines = 5
                            break

                    # 也跳过短的行（可能是作者名、机构、邮箱等）
                    if not should_skip and 0 < len(stripped) < 150:
                        # 检查是否是作者/机构/邮箱行
                        if '@' in stripped or stripped.count(',') >= 2:
                            should_skip = True
                        # 全是大写的短行（可能是机构名）
                        elif stripped.isupper() and len(stripped) < 50:
                            should_skip = True
                        # 纯数字或符号的行
                        elif all(c.isdigit() or c in '.,*†‡§' for c in stripped):
                            should_skip = True

                    if should_skip:
                        continue

            # 到达正文后，继续过滤Keywords行（防止出现在正文中）
            if not in_header:
                keywords_patterns = [r'^Keywords?\s*:', r'^Index\s+Terms\s*:']
                is_keywords_line = any(re.match(p, stripped, re.IGNORECASE) for p in keywords_patterns)
                if is_keywords_line:
                    skip_next_lines = 2  # 跳过Keywords行及其内容
                    continue

                result_lines.append(line)

        return '\n'.join(result_lines)

    def _extract_text(self, md_content: str) -> str:
        """提取纯文本，去掉markdown格式"""
        if not md_content:
            return ""

        # 移除图片标记
        text = re.sub(r"!\[.*?\]\(.*?\)", "", md_content)
        # 移除链接标记（保留文本）
        text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
        # 移除标题标记（保留标题文本，只去掉#）
        text = re.sub(r"^(#+)\s+", r"", text, flags=re.MULTILINE)
        # 移除粗体、斜体标记
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        text = re.sub(r"\*(.*?)\*", r"\1", text)
        # 移除代码标记
        text = re.sub(r"`([^`]+)`", r"\1", text)
        # 移除代码块
        text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
        # 移除表格（简单处理）
        text = re.sub(r"^\|.*?\|$", "", text, flags=re.MULTILINE)
        # 移除水平分割线
        text = re.sub(r"^\s*[-*_]{3,}\s*$", "", text, flags=re.MULTILINE)
        # 移除行首的数字标记（如 "1 " "2. " 等，可能是列表）
        text = re.sub(r"^\d+[.\)]\s*", "", text, flags=re.MULTILINE)

        # 清理多余空行和空格
        lines = [line.strip() for line in text.split('\n')]
        lines = [line for line in lines if line]  # 移除空行
        text = '\n'.join(lines)

        return text.strip()

    def _extract_references(self, md_content: str) -> List[Dict]:
        """提取参考文献"""
        references = []
        ref_pattern = re.compile(r"\[\d+\]\s*(.*?)(?=\n\[\d+\]|\Z)", re.DOTALL)
        matches = ref_pattern.findall(md_content)
        for i, match in enumerate(matches):
            references.append({"id": i+1, "text": match.strip()})
        return references

    def _extract_figures(self, md_content: str) -> List[Dict]:
        """提取图片信息"""
        figures = []
        img_pattern = re.compile(r"!\[(.*?)\]\((.*?)\)(.*?)(?=\n\n|\Z)", re.DOTALL)
        matches = img_pattern.findall(md_content)
        for i, (alt, src, caption) in enumerate(matches):
            figures.append({
                "id": i+1,
                "alt": alt,
                "src": src,
                "caption": caption.strip()
            })
        return figures

    def _extract_tables(self, md_content: str) -> List[Dict]:
        """提取表格信息（简单实现）"""
        tables = []
        table_pattern = re.compile(r"(\|.*?\|\n\|.*?[-:].*?\|\n(?:\|.*?\|\n)+)", re.DOTALL)
        matches = table_pattern.findall(md_content)
        for i, match in enumerate(matches):
            tables.append({"id": i+1, "content": match.strip()})
        return tables
