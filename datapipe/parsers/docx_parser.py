"""datapipe/parsers/docx_parser.py — DOCX parser (requires python-docx)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class DOCXParser(BaseParser):
    file_type = "docx"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            from docx import Document
        except ImportError:
            logger.warning("python-docx not installed — pip install python-docx")
            return None

        try:
            doc = Document(str(path))
            paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
            full_text = "\n\n".join(paragraphs)
            cleaned = self.clean(full_text)
            if not cleaned:
                return None
            records = []
            for i, chunk in enumerate(self.split_text(cleaned)):
                records.append({
                    "chunk_index": str(i),
                    "text": chunk,
                    "char_count": str(len(chunk)),
                })
            return pd.DataFrame(records) if records else None
        except Exception as exc:
            logger.error("DOCXParser: %s — %s", path.name, exc)
            return None
