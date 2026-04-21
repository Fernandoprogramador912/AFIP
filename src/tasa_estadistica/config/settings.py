"""Carga de configuración desde variables de entorno (.env)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tasa_estadistica.domain.tasa_estadistica_mapper import TasaEstadisticaMapper


def _repo_root_with_pyproject() -> Path:
    """Raíz del proyecto (carpeta con pyproject.toml). Si no hay, cwd (mismo criterio que antes)."""
    here = Path(__file__).resolve().parent
    for p in [here, *here.parents]:
        if (p / "pyproject.toml").is_file():
            return p
    return Path.cwd()


def _resolve_under_repo(p: Path | None) -> Path | None:
    if p is None:
        return None
    base = _repo_root_with_pyproject()
    return p if p.is_absolute() else (base / p).resolve()


# Siempre el .env junto a pyproject.toml (no depende del cwd al levantar uvicorn).
_ENV_FILE = _repo_root_with_pyproject() / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Entorno ARCA/AFIP
    arca_mode: str = Field(default="mock", description="mock | live")
    arca_cuit: str = Field(default="", description="CUIT sin guiones (11 dígitos)")

    # Certificado (.p12) para WSAA (solo live)
    arca_cert_path: Path | None = Field(default=None)
    arca_cert_password: str = Field(default="")

    # WSAA
    arca_wsaa_url: str = Field(
        default="https://wsaahomo.afip.gov.ar/ws/services/LoginCms",
    )
    arca_wsaa_service: str = Field(
        default="wsaduanas",
        description="Nombre del servicio a autorizar en el TRA (homologación: según ARCA)",
    )
    arca_wsaa_hash: str = Field(default="sha256", description="sha256 | sha1")
    arca_wsaa_time_source: str = Field(
        default="auto",
        description="Hora del TRA WSAA: auto (Date HTTP AFIP, fallback local) | local | http",
    )

    # Cliente de liquidaciones (SOAP / zeep)
    arca_liquidaciones_wsdl: str = Field(
        default="",
        description="URL del WSDL en homologación (obligatorio en modo live)",
    )
    arca_liquidaciones_service: str = Field(
        default="LiquidacionesService",
        description="Nombre del servicio WSDL (binding) para zeep",
    )
    arca_liquidaciones_method: str = Field(
        default="consultarLiquidaciones",
        description="Nombre de la operación SOAP a invocar",
    )
    arca_soap_auth_ns: str = Field(
        default="http://ar.gov.afip.dif.afip/",
        description="Namespace XML para elementos Token/Sign en el Header SOAP",
    )
    arca_liquidaciones_timeout: int = Field(default=120)
    arca_liquidaciones_complemento_wsdl: str = Field(
        default="",
        description=(
            "Opcional. Si ARCA_LIQUIDACIONES_WSDL es MOA (wconsdeclaracion), URL de un WSDL "
            "con consultarLiquidaciones por CUIT/fechas (sin MOA). Se fusiona con el resultado MOA "
            "para no depender solo de listados que a veces no traen todos los despachos. "
            "Misma red/TA; un valor por entorno (homologación/producción), no por cliente."
        ),
    )

    # MOA wconsdeclaracion — WSAutenticacionEmpresa (7016 si falta TipoAgente)
    arca_moa_tipo_agente: str = Field(
        default="",
        description="Ej.: según tabla AFIP MOA (importador/exportador/despachante, etc.)",
    )
    arca_moa_rol: str = Field(default="", description="Opcional; según operación en ARCA")
    arca_moa_chunk_sleep_seconds: float = Field(
        default=26.0,
        description=(
            "Pausa (segundos) entre llamadas al mismo método MOA con el mismo CUIT. AFIP "
            "exige >=25s (error 6013); 26s deja 1s de margen. Se aplica entre tramos de 30 "
            "días, entre variantes del listado y entre declaraciones. Env: ARCA_MOA_CHUNK_SLEEP_SECONDS"
        ),
    )
    arca_moa_retry_6013_sleep_seconds: float = Field(
        default=26.0,
        description=(
            "Pausa base antes de reintentar la misma llamada si AFIP devuelve 6013. AFIP "
            "exige >=25s por método/CUIT; 26s deja 1s de margen. Se combina con el backoff "
            "exponencial si ARCA_MOA_RETRY_6013_BACKOFF=exponential."
        ),
    )
    arca_moa_retry_6013_backoff: str = Field(
        default="exponential",
        description="Política de reintentos 6013: 'fixed' (sleep fijo, legacy) o 'exponential' (base*2**n, jitter ±20%). Env: ARCA_MOA_RETRY_6013_BACKOFF",
    )
    arca_moa_retry_6013_max_retries: int = Field(
        default=5,
        description="Máximo de reintentos 6013 antes de propagar el error. Env: ARCA_MOA_RETRY_6013_MAX_RETRIES",
    )
    arca_moa_retry_6013_max_sleep_seconds: float = Field(
        default=120.0,
        description="Tope por reintento en backoff exponencial (sleep = min(base*2**n, este valor)). Env: ARCA_MOA_RETRY_6013_MAX_SLEEP_SECONDS",
    )
    arca_moa_lista_fuente: str = Field(
        default="both",
        description="detallada | simi_djai | both (SIMI/DJAI + detallada)",
    )
    arca_moa_lista_variantes: str = Field(
        default="minimal",
        description="minimal | auto — minimal ahora incluye IC+TODOS estado; auto aún más variantes",
    )
    # D.I. separados por coma: se incluyen aunque no salgan en DetalladaLista/SIMI (detalle MOA igual).
    arca_moa_destinacion_ids_extra: str = Field(
        default="",
        description="Opcional. Env: ARCA_MOA_DESTINACION_IDS_EXTRA",
    )
    arca_moa_autoinject_sin_caratula: bool = Field(
        default=True,
        description=(
            "Antes de llamar a MOA, inyecta todos los D.I. de SQLite que no tengan "
            "`moa_detallada_caratula` en su raw_json. Así el fetch siguiente completa los "
            "montos faltantes sin intervención manual. Env: ARCA_MOA_AUTOINJECT_SIN_CARATULA"
        ),
    )

    # Persistencia
    arca_data_dir: Path = Field(default=Path("data"))
    arca_sqlite_path: Path = Field(default=Path("data/tasa_estadistica.db"))
    arca_ticket_path: Path = Field(default=Path("data/ta.xml"))

    # Exportación
    arca_excel_output: Path = Field(default=Path("out/tasa_estadistica_auditoria.xlsx"))

    # Período analítico mínimo (fecha de liquidación); panel/API/CLI coherentes
    tasa_analisis_desde: date = Field(
        default=date(2019, 1, 1),
        description="No consultar fechas anteriores (YYYY-MM-DD). Env: TASA_ANALISIS_DESDE",
    )
    tasa_analisis_hasta_max_hoy: bool = Field(
        default=True,
        description="Si true, rechaza «hasta» posterior a hoy. Env: TASA_ANALISIS_HASTA_MAX_HOY",
    )

    # Recupero V2 / panel: qué subcadenas en `destinacion_id` cuentan como «import» (OR).
    # Default «IC» (modelo Excel). Si AFIP usa otros códigos (p. ej. IM), añadilos separados por coma.
    tasa_recupero_destinacion_subcadenas: str = Field(
        default="IC",
        description="Ej. IC o IC,IM. Env: TASA_RECUPERO_DESTINACION_SUBCADENAS",
    )

    # Panel web: permitir iniciar descarga AFIP desde el navegador (POST /api/fetch)
    tasa_panel_fetch_enabled: bool = Field(
        default=True,
        description="Si false, desactiva descarga desde el panel. Env: TASA_PANEL_FETCH_ENABLED",
    )

    # Tasa de estadística: códigos de concepto en DetalladaLiquidacionesDetalle (separados por coma)
    arca_tasa_estadistica_codigos: str = Field(
        default="TE,011",
        description="CodigoConcepto que identifica tasa estadística (ej. 011 verificado vs PDF)",
    )

    @model_validator(mode="after")
    def _anchor_relative_paths_to_repo(self) -> Settings:
        """
        Rutas relativas (p. ej. data/tasa_estadistica.db) se resuelven contra la raíz del repo
        (donde está pyproject.toml), no contra el cwd del proceso. Así el panel y `tasa-arca
        export-recupero` leen el mismo SQLite aunque el servidor se levante desde otra carpeta.
        """
        object.__setattr__(self, "arca_data_dir", _resolve_under_repo(self.arca_data_dir))
        object.__setattr__(self, "arca_sqlite_path", _resolve_under_repo(self.arca_sqlite_path))
        object.__setattr__(self, "arca_ticket_path", _resolve_under_repo(self.arca_ticket_path))
        object.__setattr__(self, "arca_excel_output", _resolve_under_repo(self.arca_excel_output))
        object.__setattr__(self, "arca_cert_path", _resolve_under_repo(self.arca_cert_path))
        return self


def get_settings() -> Settings:
    return Settings()


def get_tasa_mapper_from_settings(settings: Settings) -> TasaEstadisticaMapper:
    """Mapper con códigos desde settings (o default TE,011)."""
    raw = (settings.arca_tasa_estadistica_codigos or "TE,011").strip()
    cods = frozenset(p.strip().upper() for p in raw.split(",") if p.strip())
    return TasaEstadisticaMapper(codigos_exactos=cods)
