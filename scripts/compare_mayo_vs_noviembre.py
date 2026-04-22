from __future__ import annotations

import json
import sqlite3
from pathlib import Path

DB = Path("data/tasa_estadistica.db")


def _recursive_keys_with_fecha(obj, path="", out=None):
    if out is None:
        out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            if "fecha" in k.lower() or "ofici" in k.lower() or "cancel" in k.lower():
                out.append((p, v if not isinstance(v, (dict, list)) else type(v).__name__))
            _recursive_keys_with_fecha(v, p, out)
    elif isinstance(obj, list):
        for i, it in enumerate(obj[:3]):  # solo primeros 3 items
            _recursive_keys_with_fecha(it, f"{path}[{i}]", out)
    return out


def _dump_di(di_id: str) -> None:
    print("=" * 80)
    print(f"DI = {di_id}")
    print("=" * 80)
    with sqlite3.connect(str(DB)) as conn:
        rows = conn.execute(
            "SELECT id, fecha, raw_json FROM liquidaciones WHERE destinacion_id = ? ORDER BY id",
            (di_id,),
        ).fetchall()
        print(f"  total filas: {len(rows)}")
        for row_id, fecha, raw in rows[:1]:
            print(f"  fila {row_id}: fecha_col={fecha!r}")
            data = json.loads(raw)
            print("  top keys:", list(data.keys()))
            inner = data.get("raw") or data
            caratula = inner.get("moa_detallada_caratula") or {}
            detalle = inner.get("moa_detallada_liquidaciones_detalle") or {}
            listado = inner.get("declaracion_listado") or {}
            print("  caratula keys:", list(caratula.keys())[:15] if caratula else "(vacio)")
            print("  detalle keys:", list(detalle.keys())[:15] if detalle else "(vacio)")
            print("  listado keys:", list(listado.keys())[:15] if listado else "(vacio)")

            print("  --- claves con fecha/ofici/cancel ---")
            found = _recursive_keys_with_fecha(caratula, "caratula")
            found += _recursive_keys_with_fecha(detalle, "detalle")
            found += _recursive_keys_with_fecha(listado, "listado")
            for p, v in found[:30]:
                v_str = str(v)[:60]
                print(f"    {p} = {v_str}")


def main() -> int:
    _dump_di("25001IC06004713L")  # mayo 2025 CANC - SÍ tiene fecha
    print()
    _dump_di("25001IC04239657D")  # noviembre 2025 CANC - NO tiene fecha
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
