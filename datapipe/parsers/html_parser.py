"""datapipe/parsers/html_parser.py — HTML parser (requires beautifulsoup4)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class HTMLParser(BaseParser):
    file_type = "html"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.warning("beautifulsoup4 not installed — pip install beautifulsoup4")
            return None

        try:
            html = path.read_text(encoding="utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            # Remove script and style elements
            for tag in soup(["script", "style", "nav", "footer", "head"]):
                tag.decompose()
            title = soup.title.get_text(strip=True) if soup.title else path.stem
            text = soup.get_text(separator=" ", strip=True)
            cleaned = self.clean(text)
            if not cleaned:
                return None
            records = []
            for i, chunk in enumerate(self.split_text(cleaned)):
                records.append({
                    "title": title,
                    "chunk_index": str(i),
                    "text": chunk,
                    "char_count": str(len(chunk)),
                })
            return pd.DataFrame(records) if records else None
        except Exception as exc:
            logger.error("HTMLParser: %s — %s", path.name, exc)
            return None
