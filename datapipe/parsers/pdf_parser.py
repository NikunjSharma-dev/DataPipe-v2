"""datapipe/parsers/pdf_parser.py — PDF parser (requires pypdf)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class PDFParser(BaseParser):
    file_type = "pdf"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            from pypdf import PdfReader
        except ImportError:
            logger.warning("pypdf not installed — pip install pypdf")
            return None

        try:
            reader = PdfReader(str(path))
            records = []
            for i, page in enumerate(reader.pages):
                text = page.extract_text() or ""
                cleaned = self.clean(text)
                if not cleaned:
                    continue
                for j, chunk in enumerate(self.split_text(cleaned)):
                    records.append({
                        "page": str(i + 1),
                        "chunk_index": str(j),
                        "text": chunk,
                        "char_count": str(len(chunk)),
                    })
            return pd.DataFrame(records) if records else None
        except Exception as exc:
            logger.error("PDFParser: %s — %s", path.name, exc)
            return None
