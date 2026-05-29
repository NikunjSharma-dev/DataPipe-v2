"""
datapipe/parsers/__init__.py — Parser plugin registry.

Every parser is a callable: Path → pd.DataFrame | None.
Register new parsers here; the Pipeline picks them by file_type tag.

Usage
-----
from datapipe.parsers import get_parser, PARSER_REGISTRY

parser = get_parser("csv")   # returns CSVParser()
df     = parser(path)        # DataFrame or None
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from datapipe.parsers.base import BaseParser
from datapipe.parsers.csv_parser import CSVParser
from datapipe.parsers.json_parser import JSONParser
from datapipe.parsers.text_parser import TextParser
from datapipe.parsers.python_parser import PythonParser
from datapipe.parsers.excel_parser import ExcelParser

# Registry: file_type string → parser instance
PARSER_REGISTRY: dict[str, BaseParser] = {
    "csv":    CSVParser(),
    "json":   JSONParser(),
    "text":   TextParser(),
    "python": PythonParser(),
    "excel":  ExcelParser(),
}

# Lazy-loaded parsers (optional heavy deps)
_OPTIONAL: dict[str, str] = {
    "pdf":   "datapipe.parsers.pdf_parser:PDFParser",
    "html":  "datapipe.parsers.html_parser:HTMLParser",
    "docx":  "datapipe.parsers.docx_parser:DOCXParser",
    "image": "datapipe.parsers.image_parser:ImageParser",
}


def get_parser(file_type: str) -> BaseParser | None:
    """Return the parser for *file_type*, loading optional ones lazily."""
    if file_type in PARSER_REGISTRY:
        return PARSER_REGISTRY[file_type]

    if file_type in _OPTIONAL:
        module_path, class_name = _OPTIONAL[file_type].split(":")
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            instance = cls()
            PARSER_REGISTRY[file_type] = instance
            return instance
        except ImportError as exc:
            import logging
            logging.getLogger("datapipe.parsers").warning(
                "Optional parser for '%s' unavailable: %s", file_type, exc
            )
            return None

    return None


def auto_transform(path: Path) -> pd.DataFrame | None:
    """Auto-detect file type and parse. Used as a default transform."""
    from datapipe.engine import EXTENSION_MAP
    ftype = EXTENSION_MAP.get(path.suffix.lower())
    if not ftype:
        return None
    parser = get_parser(ftype)
    if parser is None:
        return None
    return parser(path)


__all__ = [
    "BaseParser",
    "PARSER_REGISTRY",
    "get_parser",
    "auto_transform",
    "CSVParser",
    "JSONParser",
    "TextParser",
    "PythonParser",
    "ExcelParser",
]
