"""datapipe/parsers/text_parser.py — Plain-text / Markdown parser with chunking."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class TextParser(BaseParser):
    file_type = "text"

    def __init__(self, chunk_size: int = 1_000, overlap: int = 100) -> None:
        self.chunk_size = chunk_size
        self.overlap = overlap

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.error("TextParser: %s — %s", path.name, exc)
            return None

        cleaned = self.clean(text)
        if not cleaned:
            return None

        chunks = self.split_text(cleaned, self.chunk_size, self.overlap)
        records = [
            {
                "chunk_index": str(i),
                "text": chunk,
                "char_count": str(len(chunk)),
            }
            for i, chunk in enumerate(chunks)
        ]
        return pd.DataFrame(records) if records else None
