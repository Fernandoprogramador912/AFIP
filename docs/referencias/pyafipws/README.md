# Mirror parcial de `reingart/pyafipws` — solo referencia

Los archivos de esta carpeta (`wsaa.py`, `utils.py`) son copias **literales** del repositorio [reingart/pyafipws](https://github.com/reingart/pyafipws) (branch `main`) conservadas como material de consulta. **No se importan desde nuestro código** ni forman parte del pipeline de tasa de estadística.

- **Motivo:** usar el cliente WSAA de pyafipws como referencia de campo (años de uso en producción en Argentina) para validar nuestro propio cliente en `src/tasa_estadistica/arca/wsaa_client.py`.
- **Licencia original:** LGPL-3.0-or-later. Al no redistribuir un producto compilado ni enlazar dinámicamente con pyafipws, este mirror queda bajo "fair use" de referencia. Si alguna vez se decide tomar fragmentos de código, respetar la cabecera LGPL y documentarlo en el archivo destino.
- **Comparativa técnica:** [`../../comparativa_wsaa_pyafipws.md`](../../comparativa_wsaa_pyafipws.md).
- **Catálogo completo de módulos pyafipws:** [`docs/afip_arca_ws_referencia.md`](../../afip_arca_ws_referencia.md).

Para actualizar el mirror:

```powershell
Invoke-WebRequest -UseBasicParsing https://raw.githubusercontent.com/reingart/pyafipws/main/wsaa.py -OutFile docs\referencias\pyafipws\wsaa.py
Invoke-WebRequest -UseBasicParsing https://raw.githubusercontent.com/reingart/pyafipws/main/utils.py -OutFile docs\referencias\pyafipws\utils.py
```
