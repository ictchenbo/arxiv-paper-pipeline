from bs4 import BeautifulSoup
from typing import Optional
from src.models.paper import Paper, PaperAuthor, PaperCategory
from src.utils.logger import logger

class HTMLParser:
    """arXiv HTML页面解析器"""

    @classmethod
    def parse(cls, file_path: str, paper_id: str) -> Optional[Paper]:
        """解析HTML文件生成Paper对象"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            # 提取基本信息 - 支持arXiv两种格式：原生页面和LaTeXML渲染页面
            title_elem = soup.select_one("h1.title.mathjax") or soup.select_one(".ltx_title_document")
            title = title_elem.text.replace("Title:", "").strip() if title_elem else ""

            # 提取作者
            authors = []
            author_elems = soup.select("div.authors a") or soup.select(".ltx_authors .ltx_personname")
            for elem in author_elems:
                authors.append(PaperAuthor(name=elem.text.strip()))

            # 提取摘要 - 移除"Abstract:"前缀
            abstract_elem = soup.select_one("blockquote.abstract.mathjax") or soup.select_one(".ltx_abstract")
            abstract = ""
            if abstract_elem:
                # 移除标题元素
                for title_tag in abstract_elem.select("[class*=title]"):
                    title_tag.decompose()
                abstract = abstract_elem.text.replace("Abstract:", "").strip()

            # 提取分类
            primary_category = soup.select_one("td.subject.primary")
            primary_category = primary_category.text.strip() if primary_category else ""
            secondary_categories = []
            secondary_elems = soup.select("td.subject.secondary")
            for elem in secondary_elems:
                secondary_categories.append(elem.text.strip())
            categories = PaperCategory(primary=primary_category, secondary=secondary_categories)

            # 提取提交日期
            submitted_date = None
            date_elem = soup.select_one("div.dateline")
            if date_elem:
                import dateparser
                date_str = date_elem.text.strip().replace("Submitted ", "")
                submitted_date = dateparser.parse(date_str)

            # 提取正文内容 - 支持LaTeXML渲染的HTML（arXiv当前格式）
            content_text = ""
            content_elem = soup.select_one("div.full-text") or soup.select_one(".ltx_page_content")
            if content_elem:
                # 移除非正文元素：导航、页眉、页脚、作者信息、摘要、目录栏、标题
                unwanted_selectors = [
                    ".html-header-logo",
                    ".ltx_page_navigation",
                    ".ltx_page_footer",
                    ".ltx_page_navbar",  # 目录导航栏
                    ".modal",
                    ".infobox",
                    ".ltx_authors",
                    ".ltx_abstract",
                    ".ltx_title_document",  # 文档标题
                    ".ltx_biblist"  # 参考文献列表（单独提取）
                ]
                for selector in unwanted_selectors:
                    for unwanted in content_elem.select(selector):
                        unwanted.decompose()

                # 提取纯文本
                content_text = content_elem.get_text(separator="\n", strip=True)

                # 文本后处理 - 清理常见噪声
                content_text = cls._clean_text(content_text)

            # 构建Paper对象
            paper = Paper(
                paper_id=paper_id,
                title=title,
                authors=authors,
                abstract=abstract,
                categories=categories,
                submitted_date=submitted_date,
                # content_html=str(content_elem) if content_elem else "",
                content_text=content_text,
                file_type="html",
                file_path=file_path
            )

            logger.info(f"HTML解析成功: {paper_id}")
            return paper

        except Exception as e:
            logger.error(f"HTML解析失败 {file_path}: {e}", exc_info=True)
            return None

    @staticmethod
    def _clean_text(text: str) -> str:
        """清理文本噪声"""
        import re

        if not text:
            return ""

        # 移除多余空行（连续多个换行保留一个）
        text = re.sub(r'\n\s*\n+', '\n', text)

        # 移除行首行尾的空格
        lines = [line.strip() for line in text.split('\n')]

        # 过滤掉太短的噪声行（如单个字符、数字等）
        lines = [line for line in lines if len(line) > 1 or line in ['.', '!', '?']]

        return '\n'.join(lines).strip()
