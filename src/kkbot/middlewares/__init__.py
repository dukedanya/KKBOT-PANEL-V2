from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
_LEGACY_DIR = _PKG_DIR.parents[1] / "middlewares"

if str(_LEGACY_DIR) not in __path__:
    __path__.append(str(_LEGACY_DIR))
