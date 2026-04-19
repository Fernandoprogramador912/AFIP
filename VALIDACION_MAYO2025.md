# Validación mayo 2025 — Excel como contrato de datos

Todo el flujo es **local** (SQLite + Excel). Solo `auth` y `fetch` en modo live requieren red hacia AFIP.

## 1. Datos frescos (opcional)

Si querés refrescar liquidaciones de mayo 2025 desde ARCA/AFIP:

1. `tasa-arca auth` (con certificado y `ARCA_MODE=live`).
2. `tasa-arca fetch --desde 2025-05-01 --hasta 2025-05-31`
3. `tasa-arca rebuild` (reaplica reglas de tasa y regenera el Excel por defecto).

Rutas de salida: las definidas en `.env` / `ARCA_SQLITE_PATH`, `ARCA_EXCEL_OUTPUT` (por defecto `data/tasa_estadistica.db` y `out/tasa_estadistica_auditoria.xlsx`).

## 2. Export solo mayo 2025 (sin mezclar otros períodos)

Si la base SQLite tiene más corridas, filtrá por **fecha de liquidación** al exportar:

```text
tasa-arca export --desde 2025-05-01 --hasta 2025-05-31
```

`rebuild` acepta los mismos flags y regenera el Excel con el mismo filtro:

```text
tasa-arca rebuild --desde 2025-05-01 --hasta 2025-05-31
```

- `--desde` y `--hasta` deben usarse **juntos** (o ninguno).
- El libro incluye la hoja **`meta_export`** con el rango aplicado (o “sin filtro”).
- La hoja **`tasa_ic_por_despacho`** usa el **mismo rango** cuando hay filtro; sin filtro sigue el rango amplio histórico en código (2019–2035).

## 3. Checklist: Excel “correcto” desde PDFs (vos)

Para cada despacho de mayo 2025 que quieras validar:

- [ ] Identificador de destinación (p. ej. `25001IC…`) coincide PDF ↔ columna en Excel generado.
- [ ] Fecha de liquidación y cabecera AFIP (`liquidaciones_cabecera`, campos `liq_*` / aplanados).
- [ ] Conceptos y montos: comparar PDF ↔ `detalle_conceptos` (importe, código, tasa estadística).
- [ ] Anotar en una copia del Excel cualquier campo que el PDF tenga y el export no (nombre de columna + ejemplo).

Compartir los **PDF** o capturas de las secciones clave permite cerrar el mapeo campo a campo.

## 4. Plantilla de análisis de gaps (después de Excel + PDFs)

| Campo / concepto | Valor en PDF (referencia) | Excel generado (`tasa-arca export …`) | Origen en SQLite / JSON | ¿Falta API o solo mapeo? |
| ---------------- | ------------------------- | ------------------------------------- | ----------------------- | ------------------------ |
|                  |                           |                                       |                         |                          |

**Criterio para ampliar meses/años:** coincidencia razonable entre PDF (columna referencia), Excel generado y raw guardado para esos despachos en mayo; lista corta de faltantes y si se resuelven con datos ya persistidos o con nuevas operaciones MOA.

## 5. Comando auxiliar (mismo criterio de fechas)

```text
tasa-arca report-ic-tasa --desde 2025-05-01 --hasta 2025-05-31
```

Lista despachos IC con tasa y escribe CSV (útil para cruzar con el Excel).
