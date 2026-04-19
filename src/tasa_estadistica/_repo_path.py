"""Priorizar el directorio `src/` del checkout sobre `site-packages` en desarrollo."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def dev_src_root_from_any_file(start: Path) -> Path | None:
    """
    Si `start` está bajo un checkout con layout `.../src/tasa_estadistica/...`,
    devuelve el `Path` de `.../src`. En wheel/instalación normal, devuelve None.
    """
    cur = start.resolve()
    if cur.is_file():
        cur = cur.parent
    for _ in range(12):
        if cur.name == "src" and (cur / "tasa_estadistica" / "web" / "app.py").is_file():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def ensure_repo_src_on_path() -> None:
    """
    Inserta `.../src` al frente de sys.path y PYTHONPATH para que `tasa_estadistica`
    se importe desde el repo (p. ej. formato $ / U$S en el panel) y no desde una
    copia vieja en site-packages.
    """
    src = dev_src_root_from_any_file(Path(__file__).resolve())
    if src is None:
        return
    s = str(src)
    if s not in sys.path:
        sys.path.insert(0, s)
    prev = os.environ.get("PYTHONPATH", "").strip()
    parts = [p for p in prev.split(os.pathsep) if p] if prev else []
    if s not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([s, *parts]) if parts else s
    logger.info("Panel: usando código desde %s (antes que site-packages)", s)
