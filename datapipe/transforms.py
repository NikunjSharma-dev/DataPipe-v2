"""
datapipe/transforms.py — backward-compatibility shim.

v2 moves parsers into datapipe/parsers/. This module re-exports
the common ones so existing example configs still work unchanged.
"""

from datapipe.parsers.csv_parser import CSVParser as _C
from datapipe.parsers.json_parser import JSONParser as _J
from datapipe.parsers.text_parser import TextParser as _T
from datapipe.parsers.excel_parser import ExcelParser as _E
from datapipe.parsers import auto_transform

_csv   = _C()
_json  = _J()
_text  = _T()
_excel = _E()

csv_transform   = _csv.extract
json_transform  = _json.extract
text_transform  = _text.extract
excel_transform = _excel.extract

__all__ = [
    "auto_transform",
    "csv_transform",
    "json_transform",
    "text_transform",
    "excel_transform",
]
