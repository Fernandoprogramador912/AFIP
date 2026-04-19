"""Endpoints `POST /api/backfill`, `GET /api/backfill/status/{id}`, `GET /api/backfill/meses`."""

from __future__ import annotations

import time

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient


def _client_mock(monkeypatch: pytest.MonkeyPatch, tmp_path) -> TestClient:
    """Cliente con SQLite temporal y modo mock (que devuelve 1 liquidación sintética)."""
    monkeypatch.setenv("ARCA_MODE", "mock")
    monkeypatch.setenv("ARCA_SQLITE_PATH", str(tmp_path / "back_endpoints.db"))
    monkeypatch.setenv("ARCA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("ARCA_CUIT", "30709572438")

    from tasa_estadistica.web.app import app

    return TestClient(app)


def _esperar_done(client: TestClient, job_id: str, timeout: float = 5.0) -> dict:
    """Polling hasta que el job termine. Devuelve el último estado."""
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        r = client.get(f"/api/backfill/status/{job_id}")
        assert r.status_code == 200
        last = r.json()
        if last["status"] in ("done", "error"):
            return last
        time.sleep(0.05)
    return last


def test_post_backfill_corre_y_actualiza_meses(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    client = _client_mock(monkeypatch, tmp_path)
    r = client.post(
        "/api/backfill",
        json={"desde": "2025-05-01", "hasta": "2025-06-30"},
    )
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]

    last = _esperar_done(client, job_id)
    assert last["status"] == "done", last
    assert last["result"]["procesados"] == 2
    assert last["result"]["ok"] == 2

    r2 = client.get("/api/backfill/meses?desde=2025-05&hasta=2025-06")
    assert r2.status_code == 200
    body = r2.json()
    assert body["total"] == 2
    estados = {(m["anio"], m["mes"]): m["estado"] for m in body["meses"]}
    assert estados == {(2025, 5): "ok", (2025, 6): "ok"}


def test_post_backfill_idempotente_si_ya_esta_ok(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    client = _client_mock(monkeypatch, tmp_path)
    job1 = client.post(
        "/api/backfill", json={"desde": "2025-05-01", "hasta": "2025-05-31"}
    ).json()["job_id"]
    _esperar_done(client, job1)

    # Segundo backfill del mismo mes: debería procesar 0 (ya está ok).
    job2 = client.post(
        "/api/backfill", json={"desde": "2025-05-01", "hasta": "2025-05-31"}
    ).json()["job_id"]
    last = _esperar_done(client, job2)
    assert last["status"] == "done"
    assert last["result"]["procesados"] == 0


def test_get_backfill_status_404(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    client = _client_mock(monkeypatch, tmp_path)
    r = client.get("/api/backfill/status/no-existe")
    assert r.status_code == 404


def test_post_backfill_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ARCA_MODE", "mock")
    monkeypatch.setenv("ARCA_SQLITE_PATH", str(tmp_path / "x.db"))
    monkeypatch.setenv("ARCA_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("ARCA_CUIT", "30709572438")
    monkeypatch.setenv("TASA_PANEL_FETCH_ENABLED", "false")

    from tasa_estadistica.web.app import app

    client = TestClient(app)
    r = client.post(
        "/api/backfill", json={"desde": "2025-05-01", "hasta": "2025-05-31"}
    )
    assert r.status_code == 403


def test_post_reintentar_mes_corre(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    client = _client_mock(monkeypatch, tmp_path)
    r = client.post(
        "/api/backfill/reintentar-mes",
        json={"anio": 2025, "mes": 5},
    )
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]
    last = _esperar_done(client, job_id)
    assert last["status"] == "done"
    assert last["result"]["procesados"] == 1
