"""Volcar estructura de carátula + detalle de 25001IC04068168A para entender claves de fecha."""

from __future__ import annotations

import json
import sqlite3

DB = "data/tasa_estadistica.db"
DI = "25001IC04068168A"


def _claves_recursivas(obj, path="", out=None):
    if out is None:
        out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            if "fecha" in k.lower() or "oficial" in k.lower():
                out.append((p, v))
            _claves_recursivas(v, p, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _claves_recursivas(v, f"{path}[{i}]", out)
    return out


def main() -> int:
    with sqlite3.connect(DB) as c:
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT raw_json FROM liquidaciones WHERE destinacion_id = ? ORDER BY id DESC LIMIT 1",
            (DI,),
        ).fetchone()
        if not row:
            print("No hay filas"); return 1
        payload = json.loads(row["raw_json"] or "{}")
        inner = payload.get("raw") or {}
        cara = inner.get("moa_detallada_caratula") or {}
        det = inner.get("moa_detallada_liquidaciones_detalle") or {}
        listado = inner.get("declaracion_listado") or {}

        print("=== Claves de primer nivel de carátula ===")
        if isinstance(cara, dict):
            for k in list(cara.keys())[:30]:
                print(f"  {k}")
        print("\n=== Fechas encontradas en carátula (recursivo) ===")
        for p, v in _claves_recursivas(cara):
            print(f"  {p} = {v!r}")
        print("\n=== Fechas encontradas en declaracion_listado ===")
        for p, v in _claves_recursivas(listado):
            print(f"  {p} = {v!r}")
        print("\n=== Fechas encontradas en detalle liquidaciones ===")
        for p, v in _claves_recursivas(det):
            print(f"  {p} = {v!r}")

        print("\n=== Muestra de carátula (primeros 1500 chars) ===")
        print(json.dumps(cara, indent=2, ensure_ascii=False)[:1500])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
