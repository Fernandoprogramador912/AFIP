# Tasa de estadística (importaciones) — MVP local

Herramienta en Python para **extraer liquidaciones y conceptos** desde los web services oficiales (ARCA/AFIP), **clasificar la tasa de estadística** con reglas explícitas y **exportar un Excel auditable**, con el PDF solo como respaldo.

Todo corre **en tu máquina** (CLI + SQLite + Excel). No requiere servidor web.

## Requisitos

- Python 3.10+
- Certificado digital `.p12` y clave (solo para `ARCA_MODE=live` y comando `auth`)

## Instalación

```bash
cd "c:\Users\ferna\OneDrive\Escritorio\CURSOR - Tasa de Estadistica"
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev,web]"
```

Copiá `.env.example` a `.env` y ajustá valores.

### Diagnóstico AFIP/ARCA (sin soporte)

- Resumen de enlaces oficiales: [`docs/afip_arca_ws_referencia.md`](docs/afip_arca_ws_referencia.md)
- Runbook (aislamiento por capas, checklist homo/prod, matriz de fallas, panel vs CLI): [`docs/afip_runbook_diagnostico.md`](docs/afip_runbook_diagnostico.md)

Con el entorno configurado:

```bash
tasa-arca doctor
```

### Panel web local (recupero_V2)

Requiere el extra `web` (FastAPI, uvicorn, Jinja2). Con la instalación anterior ya queda incluido.

```bash
tasa-arca serve
```

Equivale a `python -m tasa_estadistica.cli serve` (puerto por defecto 8000; `TASA_ARCA_PORT` o `--port` para otro). Abrí `http://127.0.0.1:8000/` (o el puerto elegido). La grilla muestra montos con prefijos `$ ` (ARS) y `U$S ` (USD) según columna.

En **Windows**, `ejecutar_panel_web.bat` y `ejecutar_fetch.bat` activan el `.venv` y usan `python -m tasa_estadistica.cli` (evita `ModuleNotFoundError` si en el PATH quedó otro `tasa-arca`).

Si `pip install -e ".[web]"` falla al actualizar porque `tasa-arca.exe` está en uso, cerrá el proceso que lo usa y reintentá, o usá solo `python -m tasa_estadistica.cli serve` desde el repo (el código prioriza `src/` del checkout).

La API `GET /api/recupero` devuelve por defecto **filas crudas** (números); el mismo formato que el panel está en `?formatted=true` (campo `filas_formateadas`).

## Uso rápido (modo mock)

Sin red ni certificado: genera datos sintéticos coherentes para probar el flujo.

```bash
tasa-arca fetch --desde 2026-01-01 --hasta 2026-01-31 --cuit 20123456789
tasa-arca export
```

El Excel queda en `out/tasa_estadistica_auditoria.xlsx` (configurable con `ARCA_EXCEL_OUTPUT`). La base SQLite en `data/tasa_estadistica.db`.

## Homologación (live)

1. Configurá `ARCA_MODE=live`, rutas del `.p12`, `ARCA_WSAA_SERVICE` según el WS que habilitaste en ARCA, y la URL del **WSDL** en `ARCA_LIQUIDACIONES_WSDL`. Si ese WSDL es **MOA** (`wconsdeclaracion`), los listados a veces no traen todos los despachos: podés completar **`ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL`** con la URL del servicio que expone **`consultarLiquidaciones`** por CUIT y fechas (sin listar D.I. a mano). Es un dato por entorno ARCA, no por cliente.
2. Obtené el ticket WSAA:

   ```bash
   tasa-arca auth
   ```

3. Descargá liquidaciones:

   ```bash
   tasa-arca fetch --desde 2026-01-01 --hasta 2026-01-31 --cuit TU_CUIT
   ```

   **Todo un año calendario** (ej. 2025): usá el 1/1 y el 31/12 de ese año. El cliente divide el rango en tramos de 30 días para respetar límites típicos de AFIP/ARCA; puede tardar varios minutos y hace pausas entre tramos (`ARCA_MOA_CHUNK_SLEEP_SECONDS` en MOA).

   ```bash
   tasa-arca fetch --desde 2025-01-01 --hasta 2025-12-31 --cuit TU_CUIT
   ```

   Luego el mismo rango en **Recupero V2** y en el panel (fecha de liquidación):

   ```bash
   tasa-arca export-recupero --desde 2025-01-01 --hasta 2025-12-31
   ```

   En el panel web, elegí **Desde** / **Hasta** `2025-01-01` … `2025-12-31` o el atajo **Año en curso** si estás analizando el año actual.

4. Exportá Excel:

   ```bash
   tasa-arca export
   ```

Si cambiás las reglas de clasificación y querés **recalcular flags** sin volver a llamar a los servicios:

```bash
tasa-arca rebuild
```

## Tests

```bash
pytest
```

## Notebook

Ver `notebooks/auditoria_tasa_estadistica.ipynb` para inspección interactiva de la SQLite y del Excel generado.

## Modelo Excel «Recupero» (referencia)

El archivo de referencia con columnas, formato y fórmulas del modelo manual está versionado en el repo:

- [`docs/modelo/Modelo_Excel_Recupero_Tasa_Estadistica.xlsx`](docs/modelo/Modelo_Excel_Recupero_Tasa_Estadistica.xlsx)

Los datos que vienen de AFIP/liquidaciones se rellenan desde SQLite; el resto son fórmulas en el modelo. El comando `tasa-arca export-recupero` genera un `.xlsx` alineado a la hoja `recupero_V2` de ese esquema (ver también `src/tasa_estadistica/export/recupero_excel.py`).

## Notas

- El modo `live` depende del **WSDL real** del servicio de liquidaciones que defina ARCA; podés ajustar `ARCA_LIQUIDACIONES_METHOD` y `ARCA_SOAP_AUTH_NS` según el contrato del WS.
- La identificación de la tasa de estadística está en `src/tasa_estadistica/domain/tasa_estadistica_mapper.py` y es **versionable** (códigos + texto).
