"""datapipe/parsers/csv_parser.py — CSV / TSV parser."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class CSVParser(BaseParser):
    file_type = "csv"

    def extract(self, path: Path) -> pd.DataFrame | None:
        sep = "\t" if path.suffix.lower() == ".tsv" else ","
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False)
            df = df.dropna(how="all")
            df.columns = [self.sanitize_col(c) for c in df.columns]
            return df if not df.empty else None
        except Exception as exc:
            logger.error("CSVParser: %s — %s", path.name, exc)
            return None
