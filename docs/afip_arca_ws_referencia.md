# Referencia rápida: AFIP / ARCA — Web Services (para este proyecto)

Resumen de **enlaces oficiales** y **dónde encontrar WSDL y manuales**. No reemplaza los PDF de AFIP; sirve para orientarse sin buscar de cero cada vez.

**Diagnóstico sin soporte AFIP** (checklist, matriz de fallas, panel vs CLI): ver [afip_runbook_diagnostico.md](afip_runbook_diagnostico.md).

## Portales principales

| Qué buscás | Dónde |
|------------|--------|
| Índice general Web Services SOAP | https://www.afip.gob.ar/ws/ |
| Manuales y herramientas (documentación técnica) | https://www.afip.gob.ar/ws/documentacion |
| Información para programadores | https://www.afip.gob.ar/ws/programadores |
| Arquitectura / conceptos | https://www.afip.gob.ar/ws/documentacion/ (sección según el sitio) |
| Consultas web (impositivo, aduanero, aplicativos) | https://serviciosweb.afip.gob.ar/consultas/ |

## MOA (Mis Operaciones Aduaneras)

| Qué | Dónde |
|-----|--------|
| Portal MOA | https://www.afip.gob.ar/moa/ |
| Consultas disponibles (listado de líneas / webservices) | https://www.afip.gob.ar/moa/moa/consultas-disponibles.asp |
| Manual **desarrollador** `wconsdeclaracion` (incluye URLs WSDL en el PDF) | https://www.afip.gob.ar/moa/documentos/nuevos/ManualDesarrolladorWconsdeclaracion.pdf |

Ejemplos de WSDL de **wconsdeclaracion** (verificar en el manual vigente; pueden cambiar):

- Homologación (típico): `https://wsaduhomoext.afip.gob.ar/diav2/wconsdeclaracion/wconsdeclaracion.asmx?WSDL`
- Producción (típico): `https://webservicesadu.afip.gov.ar/DIAV2/wconsdeclaracion/wconsdeclaracion.asmx?WSDL`

## Autenticación (WSAA)

Documentación general: https://www.afip.gob.ar/ws/documentacion/wsaa.asp  

Manual del desarrollador (PDF; si la URL cambia, buscar desde la página anterior): https://www.afip.gob.ar/ws/WSAA/WSAAmanualDev.pdf  

| Entorno | URL típica `LoginCms` (validar en sitio oficial) |
|---------|---------------------------------------------------|
| Homologación | `https://wsaahomo.afip.gov.ar/ws/services/LoginCms` |
| Producción | `https://wsaa.afip.gov.ar/ws/services/LoginCms` |
| WSDL homo (referencia) | `https://wsaahomo.afip.gov.ar/ws/services/LoginCms?WSDL` |
| WSDL prod (referencia) | `https://wsaa.afip.gov.ar/ws/services/LoginCms?WSDL` |

En este proyecto: `WSAA` + certificado `.p12`; el servicio de negocio en el TRA debe coincidir con lo habilitado (ej. `wsaduanas` según doc ARCA).

## Contactos (según portal AFIP)

- Producción / negocio web services: `sri@arca.gob.ar`
- Testing — solo WSAA/WSASS: `webservices-desa@arca.gob.ar`

## Relación con este repo

- **`ARCA_LIQUIDACIONES_WSDL`**: si apunta a MOA (`wconsdeclaracion`), el flujo usa listados + detalle MOA.
- **`ARCA_LIQUIDACIONES_COMPLEMENTO_WSDL`** (opcional): segundo WSDL de un servicio que exponga **`consultarLiquidaciones`** por CUIT/fechas (sin MOA), **no** el mismo `wconsdeclaracion`. La URL sale del **manual de ese servicio** en Documentación o consultando a ARCA.
- Los **WSDL no aparecen** en la pantalla de administración de certificados digitales; ahí solo se gestiona el certificado.

## ¿Descargar PDFs vos?

- **No es obligatorio** para el asistente: con los **enlaces** alcanza para buscar en el manual.
- **Conviene** tener los PDF locales si quieres leer offline o buscar por palabras (`WSDL`, `homologación`, `URL`).

Última actualización de esta nota: referencia interna del proyecto; validar URLs en el sitio oficial si algo falla.
