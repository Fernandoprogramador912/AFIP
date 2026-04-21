# Comparativa WSAA: nuestro cliente vs `reingart/pyafipws`

Objetivo: usar `pyafipws/wsaa.py` como **referencia de campo** (años de uso en producción en Argentina) para validar nuestro `src/tasa_estadistica/arca/wsaa_client.py` y detectar mejoras accionables sin introducir `pyafipws` como dependencia.

- Fuente comparada: [`reingart/pyafipws` @ main](https://github.com/reingart/pyafipws) — archivos `wsaa.py` y `utils.py` (copiados en `docs/referencias/pyafipws/` para consulta local; **no se importan**).
- Nuestro cliente: [`src/tasa_estadistica/arca/wsaa_client.py`](../src/tasa_estadistica/arca/wsaa_client.py) y [`src/tasa_estadistica/arca/auth_ticket_store.py`](../src/tasa_estadistica/arca/auth_ticket_store.py).

## Resumen ejecutivo

Nuestro cliente es **moderno, más limpio y más seguro** que el de pyafipws (tipado estático, `pydantic-settings`, `zoneinfo`, PKCS#12 nativo vía `cryptography`, parseo tolerante de la respuesta `loginCms`, política de hora configurable). pyafipws arrastra la mochila histórica: soporta Python 2.7, binding OpenSSL de bajo nivel, COM/OCX para VB6/VFP, salida SMIME re-parseada como email, y gestiona clave/cert como PEM por separado.

Las únicas mejoras accionables que vale la pena portar son:

1. **Leeway hacia atrás en `generationTime`** (mitiga reloj adelantado). — **Aplicado** como parámetro opcional `leeway_back_seconds` en `build_tra_xml(...)` (default 0, para no cambiar el comportamiento previo).
2. **Usar `wsaa_ticket_expired(...)` en `ensure_ticket(...)`** (más estricto que `mtime`). — **Aplicado**: `WSAAClient.ensure_ticket(...)` ahora reaprovecha el TA por `expirationTime` con leeway configurable (default 5 min).
3. **Elevar el TTL de reutilización del TA** (hoy 500 s; AFIP lo emite por ~12 h). — **Aplicado** implícitamente al pasar a `expirationTime` (TA emitido por AFIP ~12 h).

Bonus aplicado (no estaba en el plan original, pero surge del mismo análisis):

- `cmd_auth` ahora usa `ensure_ticket` por default (antes siempre llamaba a WSAA). Flag `--force` para renovar a demanda.
- `execute_fetch` / `cmd_refetch_caratula` usan `ensure_ticket` (renuevan automáticamente si el TA venció, en lugar de fallar con mensaje).
- `TicketAcceso.service` expone el `<service>` del TA; `tasa-arca doctor` lo compara contra `ARCA_WSAA_SERVICE` para detectar desajustes.

Opcionalmente, documentar un fallback con `openssl` CLI para entornos donde `cryptography` no pueda cargar el `.p12` (nunca nos pasó, pero pyafipws lo incluye).

---

## Matriz de comparación

| Aspecto                           | Nuestro cliente                                                                 | pyafipws                                                                                     | Mejor               |
| --------------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- | ------------------- |
| **Formato de credencial**         | `.p12` (PKCS#12) cargado con `cryptography.hazmat.primitives.serialization.pkcs12.load_key_and_certificates` | `.crt` + `.key` PEM por separado; requiere `openssl pkcs12 -in ... -out ...` previo          | **Nuestro**         |
| **Firma CMS**                     | `pkcs7.PKCS7SignatureBuilder().sign(Encoding.DER, [])` → bytes DER; `b64encode` al serializar | `sign(Encoding.SMIME, [PKCS7Options.Binary])` → parsea email MIME y extrae payload base64     | **Nuestro** (más directo, menos manipulación textual) |
| **Fallback sin `cryptography`**   | No hay                                                                          | `sign_tra_openssl`: invoca `openssl smime -sign` si `cryptography` no está                    | pyafipws (edge case) |
| **Hash de firma**                 | Configurable `ARCA_WSAA_HASH` (`sha1` / `sha256`). Default `sha256`.            | Fijo `SHA256` en `sign_tra_new`; variante `sign_tra_old` sin elección                         | **Nuestro**         |
| **Timezone del TRA**              | `America/Argentina/Buenos_Aires` con offset normalizado `-03:00` (XSD-safe)     | UTC (`datetime.fromtimestamp(t, tz=UTC())`); isoformat sin normalizar                         | **Nuestro** (el offset XSD-safe evita rechazos del schema) |
| **Fuente de hora del TRA**        | `ARCA_WSAA_TIME_SOURCE = auto|local|http` — `auto` lee `Date` HTTP de AFIP      | Solo reloj local                                                                              | **Nuestro**         |
| **Leeway en `generationTime`**    | **0 s** (generationTime = now)                                                  | `generationTime = now - ttl`, `expirationTime = now + ttl` (leeway simétrico)                 | pyafipws            |
| **TTL del TRA (window)**          | `ttl_seconds=600` (10 min hacia adelante)                                       | `ttl=2400` s default en `create_tra` (40 min hacia cada lado)                                 | pyafipws (margen)   |
| **Cache del TA**                  | `ensure_ticket(max_age_seconds=500)` por mtime del archivo                      | `mtime + DEFAULT_TTL(5 h) < now()`; archivo por hash MD5 de `(service,cert,key)`              | pyafipws (más tiempo), **pero** usa mtime solo |
| **Validación de vencimiento TA**  | `wsaa_ticket_expired(xml)` parseando `expirationTime` real del XML              | `Expirado(fecha)` por `expirationTime` del XML, sin leeway                                    | Empate (nosotros con leeway opcional) |
| **Parseo `loginCmsReturn`**       | Tolerante: XML plano / base64 / `itertext()` concatenado                         | Confía en WSDL: lee `results["loginCmsReturn"]` como string XML                               | **Nuestro** (robusto a variaciones AFIP) |
| **Detección de SOAP Fault**       | Busca `faultstring` en la respuesta y lanza `RuntimeError`                      | Delega a `pysimplesoap` (`LanzarExcepciones`); error queda en `self.Excepcion`                | **Nuestro** (explícito)  |
| **Stack SOAP**                    | HTTP crudo (`requests.post` con envelope literal)                               | `pysimplesoap.SimpleXMLElement` + `client.loginCms(in0=...)`                                  | Empate; nuestro es predecible, el de ellos declarativo |
| **Lenguaje / tipado**             | Python 3.10+, `from __future__ import annotations`, type hints, dataclasses     | Python 2.7 compatible, sin type hints, decoradores con capture de excepciones para COM         | **Nuestro**         |
| **Generación de CSR / claves**    | No incluido (fuera de alcance: el `.p12` viene de ARCA)                         | `CrearClavePrivada`, `CrearPedidoCertificado` con `cryptography.x509`                         | pyafipws (si se necesita crear cert)     |
| **Interfaz COM / OCX**            | No aplica                                                                        | `_reg_progid_ = "WSAA"`, registrable para VB6/VFP/Delphi                                      | N/A                 |
| **Licencia**                      | MIT-like (tu repo)                                                              | LGPL-3.0-or-later con excepción comercial                                                     | Nuestro más simple  |

## Hallazgos accionables

### Hallazgo 1 — Leeway hacia atrás en `generationTime` (prioridad media)

**Problema potencial:** si el reloj del PC está adelantado respecto al reloj de AFIP, `generationTime` puede estar en el futuro para AFIP y el TRA es rechazado con `Invalid value for generationTime` o `TRA expired`.

**Mitigación actual:** `ARCA_WSAA_TIME_SOURCE=auto` consulta la cabecera `Date` HTTP de AFIP antes de armar el TRA. Ya cubre el caso principal (reloj del PC mal configurado, como cuando Cursor reporta fechas "2026").

**Mejora portable de pyafipws:** aplicar leeway restando unos segundos a `generationTime`, dejando `expirationTime` igual o extendido:

```python
# Actual
gen = base
exp = base + timedelta(seconds=ttl_seconds)

# Propuesto (ejemplo)
gen = base - timedelta(seconds=leeway_back)   # p. ej. 120 s
exp = base + timedelta(seconds=ttl_seconds)
```

**Veredicto:** aplicar solo si encontramos rechazos de AFIP por `generationTime`. Como primera línea, `TIME_SOURCE=auto` ya nos cubre. Podemos agregarlo como parámetro opcional `leeway_back_seconds` con default `0` para no cambiar comportamiento actual.

### Hallazgo 2 — `ensure_ticket` debería usar `wsaa_ticket_expired` (prioridad alta, cambio barato)

**Problema actual:** `WSAAClient.ensure_ticket(max_age_seconds=500)` reutiliza el TA solo si el archivo fue modificado hace menos de ~8 min. Pero AFIP emite el TA con `expirationTime` ~12 horas a futuro: estamos pidiendo un nuevo TA muchísimas veces de más.

**Impacto:** más llamadas a WSAA (lentas, pueden fallar, contamos contra límites AFIP no documentados).

**Fix sugerido** (3 líneas):

```python
def ensure_ticket(self, ticket_path: Path) -> bytes:
    """Reutiliza TA si `expirationTime` aún no venció; si no, renueva."""
    if ticket_path.is_file():
        try:
            raw = ticket_path.read_bytes()
            if b"loginTicketResponse" in raw and not wsaa_ticket_expired(
                raw, leeway=timedelta(minutes=5)
            ):
                return raw
        except OSError:
            pass
    ta = self.login_cms()
    ticket_path.parent.mkdir(parents=True, exist_ok=True)
    ticket_path.write_bytes(ta)
    return ta
```

**Veredicto:** aplicar. Es estrictamente más correcto, aprovecha una función que ya tenemos y tiene tests.

### Hallazgo 3 — Documentar fallback con `openssl` CLI (prioridad baja)

pyafipws incluye `sign_tra_openssl` porque históricamente `cryptography` en Windows tenía problemas con DLLs OpenSSL. Hoy (2026) no es un problema común. No lo implementamos pero **sí documentamos en `afip_runbook_diagnostico.md`** el comando equivalente por si alguna vez hace falta reproducir manualmente:

```bash
openssl smime -sign -signer cert.crt -inkey clave.key -outform DER -nodetach \
  < tra.xml | base64
```

Útil para reproducir fuera del programa y pegar el CMS base64 al WSAA.

### Hallazgo 4 — Ampliar TTL del TRA (prioridad baja)

`build_tra_xml(ttl_seconds=600)` pide una ventana de 10 min. pyafipws pide 40 min de cada lado. Como nuestro TRA solo vive el tiempo que tarda el `POST` al WSAA, 10 min alcanza. **No cambiar salvo que AFIP empiece a rechazar por ventana corta.**

### Hallazgo 5 — Cosas a NO copiar de pyafipws

- Su manejo SMIME-→-email-→-payload para la firma CMS (innecesariamente complejo; nuestro DER directo es mejor).
- `BaseWS` + `pysimplesoap`: dependerías de su stack.
- Interfaz COM/OCX: solo tiene sentido para VB6/VFP legacy.
- Generación de CSR (`CrearPedidoCertificado`): fuera de alcance; ARCA entrega el `.p12` listo.
- Timestamp en UTC puro: nuestro AR con offset `-03:00` es igualmente válido y más claro en logs.

## Plan de aplicación — estado

1. Parche incremental en `src/tasa_estadistica/arca/wsaa_client.py`: **hecho.**
   - `ensure_ticket` usa `wsaa_ticket_expired` con leeway de 5 min (parametrizable).
   - `build_tra_xml` acepta `leeway_back_seconds` (default 0 para no romper comportamiento).
2. Tests en `tests/test_wsaa_parsing.py`: **hecho.**
   - `test_ensure_ticket_reuses_valid_ta` / `test_ensure_ticket_renews_when_expired`
   - `test_build_tra_xml_leeway_back_seconds_moves_generation_backward`
   - `test_wsaa_ticket_expired_leeway_zona_gris` (TA que vence en 3 min + leeway 5 min).
   - `test_parse_ticket_xml_extrae_service` (service del TA expuesto para diagnóstico).
3. `cmd_auth` / `execute_fetch` / `cmd_refetch_caratula` y `cmd_doctor` en `src/tasa_estadistica/cli.py` + `fetch_runner.py`: **hecho** (usan `ensure_ticket`, flag `--force`, diagnóstico de service).
4. Nota en `docs/afip_runbook_diagnostico.md` con el comando `openssl smime` como plan B manual — **pendiente** (anexo ya incluido más abajo; si querés, puedo moverlo al runbook).
5. Este documento queda como referencia viva.

Nada de lo anterior introduce dependencias nuevas ni toca el repo remoto.

## Anexo: Plan B manual con `openssl` CLI

Si alguna vez `cryptography` no puede cargar el `.p12` (entornos con OpenSSL system-wide roto, Windows con DLLs faltantes, etc.), se puede reproducir manualmente el ciclo WSAA con `openssl`. Sirve también como herramienta de debug si el `tasa-arca auth` falla y querés descartar que el problema sea la firma:

```bash
# 1) Extraer cert + clave del .p12 a PEM (una sola vez)
openssl pkcs12 -in comex.p12 -clcerts -nokeys  -out cert.crt
openssl pkcs12 -in comex.p12 -nocerts -nodes   -out clave.key

# 2) Firmar el TRA (asumiendo tra.xml ya generado por tasa-arca)
openssl smime -sign -signer cert.crt -inkey clave.key \
  -outform DER -nodetach < tra.xml | base64 > cms_b64.txt

# 3) Postear a WSAA (testing u homologación):
curl -s -X POST \
  -H 'Content-Type: text/xml; charset=utf-8' \
  -H 'SOAPAction: ""' \
  --data-binary @login_cms_envelope.xml \
  https://wsaahomo.afip.gov.ar/ws/services/LoginCms
```

Donde `login_cms_envelope.xml` es el SOAP envelope de `WSAAClient.login_cms` (envelope con `<wsaa:in0>{CMS_BASE64}</wsaa:in0>`). Este flujo es exactamente lo que hace `sign_tra_openssl()` en pyafipws.

**Nota:** nuestro pipeline **no necesita** `openssl` CLI instalado; usa `cryptography` pura. Este anexo es solo para diagnóstico manual.
