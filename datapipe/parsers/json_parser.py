"""datapipe/parsers/json_parser.py — JSON / JSONL parser."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from datapipe.parsers.base import BaseParser

logger = logging.getLogger(__name__)


class JSONParser(BaseParser):
    file_type = "json"

    def extract(self, path: Path) -> pd.DataFrame | None:
        try:
            text = path.read_text(encoding="utf-8", errors="replace").strip()
            if not text:
                return None

            # JSONL
            if "\n" in text and not text.startswith("["):
                records = []
                for line in text.splitlines():
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
                data = records
            else:
                data = json.loads(text)

            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = [{"value": str(data)}]

            df = pd.json_normalize(data)
            df.columns = [self.sanitize_col(c) for c in df.columns]
            return df.astype(str) if not df.empty else None
        except Exception as exc:
            logger.error("JSONParser: %s — %s", path.name, exc)
            return None
