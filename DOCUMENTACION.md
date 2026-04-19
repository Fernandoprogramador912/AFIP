# Documentación — Tasa de Estadística (Recupero ARCA/AFIP)

Documento único con la **documentación funcional** (qué hace, para quién, procesos de negocio) y la **documentación técnica** (arquitectura, módulos, configuración, datos, API, despliegue local). Complementa al `README.md` y al `VALIDACION_MAYO2025.md`.

Todo el sistema está diseñado para **ejecutarse de forma local** (CLI + SQLite + Excel + panel FastAPI en `127.0.0.1`). **No depende de un servidor web externo**. El único tráfico saliente es hacia los web services oficiales de ARCA/AFIP (WSAA + WS de liquidaciones) y solo en modo `live`.

---

## 1. Documentación funcional

### 1.1 Objetivo del producto

Extraer, auditar y **recuperar diferencias cobradas de más en la Tasa de Estadística** sobre despachos de importación (códigos `IC`) gestionados ante ARCA/AFIP, reemplazando el armado manual del modelo Excel «Recupero Tasa de Estadística» por un pipeline reproducible y versionable.

La herramienta:

1. **Descarga** liquidaciones y conceptos desde los web services oficiales (WSAA + WSDL `wconsdeclaracion` MOA y/o un WS complementario con `consultarLiquidaciones`).
2. **Persiste** los datos crudos en SQLite (`data/tasa_estadistica.db`), preservando los JSON/XML originales como respaldo auditable.
3. **Clasifica** automáticamente qué conceptos son Tasa de Estadística (códigos `011`, `061`, `062`, heurística de texto) mediante reglas explícitas y versionables.
4. **Exporta** un libro Excel `recupero_V2` idéntico en estructura al modelo manual histórico, con fórmulas de reconstrucción de base (FOB + FLETE + SEGURO), alícuota correcta (0,5 % / 3 %) y montos máximos 061/062.
5. **Expone** un panel web local (FastAPI + Jinja2) en `http://127.0.0.1:8000/` con grilla, filtros por período, descarga del Excel, estado del backfill mes a mes y carga manual de overrides (FOB/Flete/Seguro) para despachos CANC o con datos AFIP incompletos.

### 1.2 Usuarios y casos de uso

- **Operador de recupero (Comex / Administración):** arma el análisis mensual / anual, identifica qué despachos tienen exceso cobrado y descarga el Excel a presentar.
- **Desarrollador / IT interno:** mantiene el pipeline, actualiza reglas de clasificación, depura respuestas SOAP de AFIP, ajusta WSDL cuando ARCA rota endpoints.
- **Auditor externo:** recibe el Excel auditable (`out/tasa_estadistica_auditoria.xlsx`) con hojas de metadatos (`meta_export`, `tasa_ic_por_despacho`) que permiten reconstruir el resultado.

### 1.3 Flujo funcional de punta a punta

```
┌───────────────┐     WSAA (LoginCms)      ┌───────────────┐
│ Certificado   │ ───────────────────────> │ Ticket TA XML │
│ .p12 + clave  │                          │ (data/ta.xml) │
└───────────────┘                          └───────┬───────┘
                                                    │ Token+Sign
                                                    v
┌───────────────┐  DetalladaListaDeclaraciones /    ┌───────────────┐
│  ARCA MOA     │<──DetalladaCaratula/Detalle──────>│ liquidaciones │
│ wconsdeclar.  │  + SimiDjaiListaDeclaraciones     │ conceptos     │
│ (+compl. WSDL)│                                    │ raw_json      │
└───────────────┘                                    └───────┬───────┘
                                                              │
                                               Reglas tasa    v
                                          ┌─────────────────────────┐
                                          │ SQLite                  │
                                          │ tasa_estadistica.db     │
                                          │ (extraction_runs,       │
                                          │  liquidaciones,         │
                                          │  conceptos_liquidacion, │
                                          │  raw_payloads,          │
                                          │  recupero_overrides,    │
                                          │  backfill_meses)        │
                                          └──────────┬──────────────┘
                                                     │
                           ┌─────────────────────────┼────────────────────────┐
                           v                         v                        v
                    Panel web FastAPI          Excel auditable         Excel recupero_V2
                  (localhost:8000, grilla      out/tasa_estadistica    out/recupero_tasa.xlsx
                   + overrides + backfill)     _auditoria.xlsx         (formato modelo manual)
```

### 1.4 Reglas de negocio clave

- **Tasa de Estadística = códigos `011`, `061`, `062`** en `DetalladaLiquidacionesDetalle` (validado contra PDFs y RG 632/99 Anexo VI). El mapper también acepta códigos prefijo `TE` y matcheo por texto `tasa estadística`.
- **Alícuota máxima vigente: 3 %** (se verifica `ALICUOTA COBRADA` vs el código; si la liquidación muestra 0,5 % o 3 %, se respeta).
- **Base reconstruida = FOB + FLETE + SEGURO** en USD (CIF documental). Si difiere del CIF que AFIP usó para liquidar, la diferencia implica exceso cobrado.
- **Exceso ARS = exceso USD × TC del despacho** (tomado de `oficializacion` / carátula MOA).
- **Destinaciones alcanzadas:** por defecto se filtra por subcadena `IC` dentro del `destinacion_id` (configurable vía `TASA_RECUPERO_DESTINACION_SUBCADENAS`). Se evita `IM` por falsos positivos con `SIM`.
- **Política de fechas:** el período analítico mínimo es `2019-01-01` (configurable `TASA_ANALISIS_DESDE`) y se rechaza `hasta` posterior a hoy si `TASA_ANALISIS_HASTA_MAX_HOY=true`.
- **Overrides manuales:** cuando AFIP no devuelve FOB/Flete/Seguro (típico en despachos CANC), el operador los carga vía panel (`/api/recupero/override`) o CSV. El override pisa al JSON AFIP solo para esa fila.

### 1.5 Entregables por ejecución

| Salida                                     | Ruta por defecto                              | Uso                                         |
| ------------------------------------------ | --------------------------------------------- | ------------------------------------------- |
| Base SQLite con raw + conceptos            | `data/tasa_estadistica.db`                    | Fuente de verdad local                      |
| Excel auditable (múltiples hojas)          | `out/tasa_estadistica_auditoria.xlsx`         | Auditoría interna / externa                 |
| Excel recupero V2 (formato modelo manual)  | `out/recupero_tasa.xlsx` + `.csv`             | Presentación final tipo modelo histórico    |
| Reporte IC con tasa                        | `data/report_ic_tasa.csv`                     | Cruce rápido contra el Excel                |
| Ticket WSAA                                | `data/ta.xml`                                 | Cacheado hasta `expirationTime`             |

---

## 2. Documentación técnica

### 2.1 Stack y dependencias

- **Lenguaje:** Python 3.10+ (probado en 3.13).
- **Framework web (opcional):** FastAPI + Uvicorn + Jinja2 (extra `[web]`).
- **Cliente SOAP:** `zeep` (con `requests` para diagnóstico y ajustes de transporte).
- **Firma CMS / WSAA:** `cryptography` (PKCS#12 para `.p12`, firma SHA-256/SHA-1).
- **Persistencia:** SQLite vía `sqlite3` de la stdlib (sin ORM).
- **Excel:** `openpyxl` (lectura del modelo y escritura del resultado).
- **Configuración:** `pydantic-settings` + `python-dotenv` (archivo `.env`).
- **Testing:** `pytest` (≈24 tests).
- **Linter:** `ruff` (line-length 100, select `E,F,I,UP`).

Declaración en `pyproject.toml`; entry-point CLI: `tasa-arca = tasa_estadistica.cli:main`.

### 2.2 Estructura del repositorio

```
.
├── src/tasa_estadistica/
│   ├── cli.py                          # Entrypoint CLI (auth, doctor, fetch, export, rebuild, serve, backfill, ...)
│   ├── fetch_runner.py                 # Orquesta descarga (mock o live) y persiste en SQLite
│   ├── backfill_runner.py              # Backfill mes-a-mes con estado persistente y reanudación
│   ├── _repo_path.py                   # Bootstrap: prioriza src/ del checkout sobre instalación pip
│   ├── arca/
│   │   ├── wsaa_client.py              # Firma CMS + LoginCms → TA.xml
│   │   ├── auth_ticket_store.py        # Parseo/validez de TA (expirationTime)
│   │   ├── liquidaciones_client.py     # Cliente zeep + adaptadores SOAP
│   │   ├── moa_declaracion.py          # MOA wconsdeclaracion (listas + carátula + detalle)
│   │   └── soap_common.py              # Header Token/Sign, reintentos 6013, backoff
│   ├── config/
│   │   ├── settings.py                 # Pydantic Settings (.env → Settings)
│   │   └── date_policy.py              # Rango analítico mínimo, presets, validación
│   ├── domain/
│   │   └── tasa_estadistica_mapper.py  # Reglas de clasificación (códigos + texto + score)
│   ├── model/schemas.py                # Dataclasses/pydantic: Liquidacion, Concepto, RunParams
│   ├── storage/
│   │   ├── sqlite_repo.py              # Esquema SQLite + inserts
│   │   ├── backfill_state.py           # Estado por (cuit, anio, mes)
│   │   └── recupero_overrides.py       # FOB/Flete/Seguro manuales
│   ├── export/
│   │   ├── excel_report.py             # Libro auditable (multi-hoja)
│   │   ├── excel_raw_flat.py           # Aplana raw_json a columnas
│   │   ├── recupero_excel.py           # Hoja recupero_V2 (modelo manual)
│   │   ├── recupero_formulas.py        # Fórmulas base/CIF/exceso
│   │   ├── recupero_destinacion_sql.py # Filtro IC / subcadenas
│   │   └── recupero_compare.py         # Diff Excel modelo vs SQLite
│   ├── report/ic_tasa_report.py        # CSV despachos IC con tasa
│   └── web/
│       ├── app.py                      # FastAPI: panel + endpoints API
│       ├── fetch_jobs.py               # Jobs background (fetch / backfill)
│       ├── recupero_display.py         # Formato ARS/USD para grilla
│       └── templates/index.html        # Panel HTML (Jinja2)
├── tests/                              # pytest (dedupe MOA, date policy, export, panel, etc.)
├── scripts/debug_wsaa_once.py          # Reproducción de un LoginCms para debug
├── notebooks/auditoria_tasa_estadistica.ipynb
├── docs/
│   ├── afip_arca_ws_referencia.md
│   ├── afip_runbook_diagnostico.md
│   └── modelo/Modelo_Excel_Recupero_Tasa_Estadistica.xlsx
├── data/                               # SQLite y TA (gitignored)
├── out/                                # Excels generados (gitignored)
├── ejecutar_*.bat                      # Atajos Windows (panel, fetch, pipeline mayo 2025)
├── .env / .env.example
├── pyproject.toml
├── README.md
├── VALIDACION_MAYO2025.md
└── DOCUMENTACION.md                    # (este archivo)
```

### 2.3 Configuración (`.env`)

Variables principales (ver `.env.example` para el set completo y defaults):

| Variable                               | Default                                              | Uso                                                                          |
| -------------------------------------- | ---------------------------------------------------- | ---------------------------------------------------------------------------- |
| `ARCA_MODE`                            | `mock`                                               | `mock` usa datos sintéticos; `live` llama a WSAA + WS real                   |
| `ARCA_CUIT`                            | —                                                    | CUIT del importador (11 dígitos, sin guiones)                                |
| `ARCA_CERT_PATH` / `ARCA_CERT_PASSWORD`| —                                                    | Certificado `.p12` para WSAA (solo `live`)                                   |
| `ARCA_WSAA_URL`                        | Homologación AFIP                                    | LoginCms (homo/prod)                                                         |
| `ARCA_WSAA_SERVICE`                    | `wsaduanas`                                          | Servicio a autorizar en el TRA                                               |
| `ARCA_WSAA_HASH`                       | `sha256`                                             | Algoritmo de firma CMS                                                       |
| `ARCA_WSAA_TIME_SOURCE`                | `auto`                                               | `auto` usa `Date` HTTP AFIP si el reloj local está desfasado                 |
| `ARCA_LIQUIDACIONES_WSDL`              | —                                                    | WSDL del WS de liquidaciones (obligatorio en `live`)                         |
| `ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL`  | —                                                    | WSDL secundario con `consultarLiquidaciones` por CUIT/fechas (fusionado)     |
| `ARCA_LIQUIDACIONES_METHOD`            | `consultarLiquidaciones`                             | Nombre de la operación SOAP                                                  |
| `ARCA_SOAP_AUTH_NS`                    | `http://ar.gov.afip.dif.afip/`                       | Namespace del header Token/Sign                                              |
| `ARCA_MOA_TIPO_AGENTE` / `_ROL`        | `IMEX` / `IMEX`                                      | Código MOA (no usar texto largo: error 42075)                                |
| `ARCA_MOA_LISTA_FUENTE`                | `both`                                               | `detallada`, `simi_djai` o `both`                                            |
| `ARCA_MOA_LISTA_VARIANTES`             | `minimal`                                            | `minimal` (IC+TODOS estado) o `auto` (más variantes)                         |
| `ARCA_MOA_DESTINACION_IDS_EXTRA`       | —                                                    | D.I. a inyectar aunque no aparezcan en listados                              |
| `ARCA_MOA_CHUNK_SLEEP_SECONDS`         | `15`                                                 | Pausa entre tramos de 30 días                                                |
| `ARCA_MOA_RETRY_6013_SLEEP_SECONDS`    | `15`                                                 | Sleep base antes de reintentar un error 6013                                 |
| `ARCA_MOA_RETRY_6013_BACKOFF`          | `exponential`                                        | `fixed` o `exponential` (base·2ⁿ con jitter ±20 %)                           |
| `ARCA_MOA_RETRY_6013_MAX_RETRIES`      | `5`                                                  | Máximo de reintentos antes de propagar                                       |
| `ARCA_MOA_RETRY_6013_MAX_SLEEP_SECONDS`| `120`                                                | Tope por reintento                                                           |
| `ARCA_MOA_AUTOINJECT_SIN_CARATULA`     | `true`                                               | Antes de MOA, reinyecta D.I. sin carátula para completar montos              |
| `ARCA_DATA_DIR`                        | `data`                                               | Carpeta base                                                                 |
| `ARCA_SQLITE_PATH`                     | `data/tasa_estadistica.db`                           | SQLite local                                                                 |
| `ARCA_TICKET_PATH`                     | `data/ta.xml`                                        | Cache del TA WSAA                                                            |
| `ARCA_EXCEL_OUTPUT`                    | `out/tasa_estadistica_auditoria.xlsx`                | Excel auditable                                                              |
| `ARCA_TASA_ESTADISTICA_CODIGOS`        | `TE,011`                                             | Códigos que el mapper considera tasa estadística (coma-separados)            |
| `TASA_ANALISIS_DESDE`                  | `2019-01-01`                                         | Fecha mínima permitida                                                       |
| `TASA_ANALISIS_HASTA_MAX_HOY`          | `true`                                               | Rechaza futuro                                                               |
| `TASA_RECUPERO_DESTINACION_SUBCADENAS` | `IC`                                                 | Subcadenas que cuentan como import (OR)                                      |
| `TASA_PANEL_FETCH_ENABLED`             | `true`                                               | Permite `POST /api/fetch` desde el panel                                     |

> **Seguridad:** `.env` está en `.gitignore`. Solo se versiona `.env.example` con placeholders; nunca subir `.p12`, contraseñas ni TA real.

### 2.4 Modelo de datos (SQLite)

Esquema creado en `storage/sqlite_repo.py`:

- `extraction_runs(run_id, started_at, params_json, modo)` — una fila por ejecución de `fetch`.
- `raw_payloads(id, run_id, endpoint, body_text, created_at)` — cuerpos SOAP/JSON tal como los devolvió ARCA.
- `liquidaciones(id, run_id, cuit, id_externo, numero, fecha, destinacion_id, raw_json)` — una fila por liquidación detallada. `raw_json` contiene el dump completo del modelo Pydantic + bloques `moa_detallada_caratula`, `moa_detallada_liquidaciones_detalle`, `liquidacion_resumen`, etc.
- `conceptos_liquidacion(id, run_id, liquidacion_id, codigo, descripcion, importe, moneda, raw_json, es_tasa_estadistica, match_score, match_reason)` — un renglón por concepto con flag + score del mapper.
- `recupero_overrides(destinacion_id, fob, flete, seguro, nota, actualizado_at)` — valores manuales.
- `backfill_meses(cuit, anio, mes, estado, run_id, n_declaraciones, n_liquidaciones, primer_intento, ultimo_intento, intentos, ultimo_error)` — estado por mes (pendiente/en_proceso/ok/sin_datos/error).

Índices: `idx_liq_run`, `idx_liq_fecha`, `idx_con_run`, `idx_backfill_estado`.

### 2.5 Clasificación de la tasa de estadística

`src/tasa_estadistica/domain/tasa_estadistica_mapper.py`:

1. Códigos exactos configurables (`ARCA_TASA_ESTADISTICA_CODIGOS`, default `TE,011`). Score `1.0`, razón `código_exacto`.
2. Códigos parciales con pistas `TE`, `TASA`, `EST`, `ESTAD`, `ESTADISTICA`. Score `0.85`.
3. Patrones de texto (`tasa de estad`, `tasa estad`, `estad[ií]stica`). Score `0.75`.
4. Keywords sueltas (`TASA` + `ESTADISTICA` en la misma descripción). Score `0.65`.
5. Sin coincidencia → `matched=False`, score `0`.

La decisión queda persistida en `conceptos_liquidacion.es_tasa_estadistica` + `match_reason`, de modo que `tasa-arca rebuild` puede reaplicar nuevas reglas sin volver a llamar a AFIP.

### 2.6 CLI — subcomandos

Entrypoint `tasa-arca` (equivalente: `python -m tasa_estadistica.cli`):

| Comando                    | Propósito                                                                      |
| -------------------------- | ------------------------------------------------------------------------------ |
| `doctor`                   | Diagnóstico local (rutas, WSAA, WSDL, TA, SQLite) sin exponer contraseñas      |
| `auth`                     | Obtiene y guarda TA vía WSAA (requiere `ARCA_MODE=live` y `.p12`)              |
| `fetch --desde --hasta`    | Descarga liquidaciones/conceptos (mock o live) y persiste en SQLite            |
| `export [--desde --hasta]` | Genera Excel auditable (filtro por fecha de liquidación opcional)              |
| `rebuild [--desde --hasta]`| Reaplica el mapper sobre SQLite y regenera el Excel                            |
| `export-recupero`          | Genera el Excel `recupero_V2` + CSV gemelo                                     |
| `compare-recupero-excel`   | Diff celda a celda contra el Excel modelo (hoja `V2_Ejemplo`)                  |
| `report-ic-tasa`           | CSV de despachos IC con tasa para el período                                   |
| `refetch-caratula`         | Re-descarga solo carátula + detalle de un D.I. (recupera FOB/Flete/Seguro)     |
| `backfill`                 | Backfill mes-a-mes idempotente, reanudable, con `--reintentar-errores`         |
| `backfill-status`          | Tabla visual año/mes del estado del backfill                                   |
| `serve`                    | Panel web FastAPI en `127.0.0.1:8000` (extra `[web]`)                          |

Códigos de retorno: `0` OK, `1` comando inválido, `2` parámetros/configuración inválidos, `3` backfill sin ningún mes OK.

### 2.7 API HTTP del panel (FastAPI)

Base: `http://127.0.0.1:8000` (configurable con `TASA_ARCA_HOST` / `TASA_ARCA_PORT`).

| Método | Ruta                                         | Descripción                                                  |
| ------ | -------------------------------------------- | ------------------------------------------------------------ |
| GET    | `/`                                          | Panel HTML (grilla, filtros, backfill, overrides)            |
| GET    | `/api/resumen?desde&hasta`                   | Totales del período (JSON)                                   |
| GET    | `/api/recupero?desde&hasta&formatted=false`  | Filas Recupero V2 (crudas o formateadas)                     |
| GET    | `/export/recupero.xlsx?desde&hasta`          | Descarga directa del Excel recupero V2                       |
| POST   | `/api/fetch`                                 | Inicia descarga asíncrona (si `TASA_PANEL_FETCH_ENABLED`)    |
| GET    | `/api/fetch/status/{job_id}`                 | Polling del job de fetch                                     |
| POST   | `/api/backfill`                              | Inicia backfill mes-a-mes asíncrono                          |
| GET    | `/api/backfill/status/{job_id}`              | Polling del job de backfill                                  |
| GET    | `/api/backfill/meses?cuit&desde&hasta`       | Estado por mes (grilla del panel)                            |
| POST   | `/api/backfill/reintentar-mes`               | Reintento puntual de un mes                                  |
| GET    | `/api/recupero/override`                     | Lista overrides manuales                                     |
| POST   | `/api/recupero/override`                     | Alta/edición de override (FOB/Flete/Seguro/nota)             |
| DELETE | `/api/recupero/override/{destinacion_id}`    | Borrado de override                                          |
| POST   | `/api/recupero/override/csv`                 | Bulk upsert desde CSV `;` o `,`                              |

Validación de período: todos los endpoints aplican `validate_analysis_period` (`TASA_ANALISIS_DESDE` / `TASA_ANALISIS_HASTA_MAX_HOY`) y responden 422 si el rango es inválido.

### 2.8 Integración SOAP / MOA

Puntos clave implementados en `arca/moa_declaracion.py` y `arca/soap_common.py`:

- **Header Token+Sign** extraído del TA; namespace configurable.
- **Chunking automático** de rangos largos en tramos de 30 días (límite típico AFIP 6013). Pausa configurable entre tramos.
- **Reintentos exponenciales con jitter** ante error 6013 (`backoff` exponencial ó fijo, tope `MAX_SLEEP_SECONDS`, `MAX_RETRIES`).
- **Doble fuente de listado:** `DetalladaListaDeclaraciones` + `SimiDjaiListaDeclaraciones` (fusionadas por `destinacion_id`) + WSDL complementario opcional con `consultarLiquidaciones`.
- **Auto-inject sin carátula:** si hay D.I. en SQLite sin `moa_detallada_caratula`, se fuerzan en el siguiente fetch para completar FOB/FLETE/SEGURO.
- **Refetch carátula** (`refetch-caratula`) para actualizar un único despacho in-place sin rearmar la corrida.
- **Dedupe** a nivel de `IdentificadorLiquidacion` para evitar duplicados cuando hay overlap entre fuentes (tests: `tests/test_liquidaciones_dedupe.py`, `tests/test_moa_merge_declaraciones.py`).

### 2.9 Generación del Excel

- `export/excel_report.py` arma el libro auditable (`tasa_estadistica_auditoria.xlsx`) con hojas: `resumen_runs`, `liquidaciones_flat`, `conceptos`, `tasa_ic_por_despacho`, `meta_export` (con rango aplicado).
- `export/recupero_excel.py` genera la hoja `recupero_V2` con 25 columnas alineadas al modelo (PROVEEDOR, D.I., OFICIALIZACIÓN, TC DESPACHO, ALÍCUOTA, T.E. 011/061/062, BASE RECONSTR., FOB, FLETE, SEGURO, CIF DOCUMENTAL, DIF. BASE VS CIF, T.E. CORRECTA S/BASE, EXCESO USD, A SOLICITAR ARS, MÉTODO SUGERIDO, ESTADO REVISIÓN, ARCHIVO FUENTE, etc.).
- `export/recupero_formulas.py` calcula fila por fila: base reconstruida, T.E. correcta, exceso USD, exceso ARS (con TC despacho).
- Formato numérico diferenciado: columnas USD (10..19) y ARS (6..9, 20..21), fecha y alícuota con formatos dedicados.

### 2.10 Testing

`pytest` cubre (`tests/`):

- Mapper tasa (`test_tasa_mapper.py`).
- Dedupe y merge MOA (`test_liquidaciones_dedupe.py`, `test_moa_*`).
- Backoff 6013 (`test_moa_retry_backoff.py`).
- Export Excel + raw flat + fórmulas golden (`test_excel_*`, `test_recupero_*`).
- Panel y endpoints (`test_web_panel.py`, `test_web_fetch.py`, `test_backfill_endpoints.py`).
- WSAA parsing (`test_wsaa_parsing.py` con `fixtures/ta_sample.xml`).
- Política de fechas (`test_date_policy.py`).

Ejecución:

```bash
pytest
```

### 2.11 Instalación y puesta en marcha (local)

```powershell
cd "C:\Users\ferna\OneDrive\Escritorio\CURSOR - Tasa de Estadistica"
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev,web]"
copy .env.example .env   # completar valores reales (no commitear)
```

Atajos Windows (ya incluidos):

- `ejecutar_panel_web.bat` — activa el venv y levanta `tasa-arca serve`.
- `ejecutar_fetch.bat` — ejecuta un fetch contra ARCA usando los parámetros del `.env`.
- `ejecutar_pipeline_mayo2025.bat` — pipeline completo de validación histórica.

Acceso: `http://127.0.0.1:8000/` (ver `README.md` §«Panel web local»).

### 2.12 Operación y runbooks

- Diagnóstico sin soporte AFIP → `docs/afip_runbook_diagnostico.md`.
- Referencia de WSDL/links oficiales → `docs/afip_arca_ws_referencia.md`.
- Validación mes a mes contra PDFs → `VALIDACION_MAYO2025.md`.
- Debug puntual de WSAA → `scripts/debug_wsaa_once.py` (registra el request/response del LoginCms sin contaminar la corrida).

### 2.13 Extensión y mantenimiento

- **Agregar un código de tasa:** editar `ARCA_TASA_ESTADISTICA_CODIGOS` en `.env` y correr `tasa-arca rebuild`. No se toca código.
- **Nueva columna en el Excel recupero V2:** agregarla a `RECUPERO_V2_HEADERS` (`export/recupero_excel.py`), extender `recupero_formulas.build_recupero_v2_row_with_formulas`, actualizar formato en `recupero_v2_excel_currency_format` y sumar un test golden.
- **Nuevo endpoint AFIP:** agregar cliente en `arca/`, dependencia en `fetch_runner.execute_fetch`, persistir en `raw_payloads` o como bloque del `liquidaciones.raw_json`.
- **Cambio de WSDL:** solo actualizar `ARCA_LIQUIDACIONES_WSDL` y/o `ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL`. El flujo no requiere recompilar nada.

### 2.14 Limitaciones conocidas

- El servicio MOA a veces no devuelve algunos despachos en los listados; se mitiga con la doble fuente, WSDL complementario y `ARCA_MOA_DESTINACION_IDS_EXTRA`.
- Despachos en estado `CANC` pueden venir sin FOB/Flete/Seguro: se resuelve con overrides manuales.
- El proyecto no expone un servicio remoto. Si se desea acceso multi-usuario, requiere desplegar el panel tras un reverse proxy (no incluido en el repo por decisión de diseño: todo debe correr **local**).

---

## 3. Referencias cruzadas

- `README.md` — instalación y uso rápido.
- `VALIDACION_MAYO2025.md` — procedimiento de validación con PDFs.
- `docs/afip_arca_ws_referencia.md` — links oficiales ARCA/AFIP.
- `docs/afip_runbook_diagnostico.md` — runbook de diagnóstico.
- `ManualDesarrolladorWconsdeclaracion.pdf` — manual MOA AFIP (copia local).
- `Modelo Excel - Recupero Tasa de Estadistica.xlsx` — modelo manual de referencia (copia en `docs/modelo/`).
