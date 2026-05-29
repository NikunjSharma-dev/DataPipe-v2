"""datapipe/parsers/image_parser.py — Image OCR parser (requires pytesseract + Pillow)."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class ImageParser(BaseParser):
    file_type = "image"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            from PIL import Image
            import pytesseract
        except ImportError:
            logger.warning("pytesseract/Pillow not installed — pip install pytesseract Pillow")
            return None

        try:
            img = Image.open(path)
            text = pytesseract.image_to_string(img)
            cleaned = self.clean(text)
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
            logger.error("ImageParser: %s — %s", path.name, exc)
            return None
