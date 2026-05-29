"""datapipe/parsers/excel_parser.py — Excel (.xlsx / .xls) parser."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class ExcelParser(BaseParser):
    file_type = "excel"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            engine = "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"
            df = pd.read_excel(path, dtype=str, engine=engine)
            df = df.dropna(how="all")
            df.columns = [self.sanitize_col(c) for c in df.columns]
            return df if not df.empty else None
        except Exception as exc:
            logger.error("ExcelParser: %s — %s", path.name, exc)
            return None
