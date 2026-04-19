"""Descarga desde panel (mock) y estado de job."""

import time

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient


def test_api_fetch_mock_completes(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ARCA_MODE", "mock")
    monkeypatch.setenv("ARCA_SQLITE_PATH", str(tmp_path / "panel_fetch.db"))
    monkeypatch.setenv("ARCA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("ARCA_CUIT", "20123456789")

    from tasa_estadistica.web.app import app

    client = TestClient(app)
    r = client.post(
        "/api/fetch",
        json={"desde": "2025-05-01", "hasta": "2025-05-31"},
    )
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    job_id = data["job_id"]

    status = "running"
    last = None
    for _ in range(50):
        s = client.get(f"/api/fetch/status/{job_id}")
        assert s.status_code == 200
        last = s.json()
        status = last["status"]
        if status in ("done", "error"):
            break
        time.sleep(0.05)

    assert status == "done"
    assert last is not None
    assert last.get("result", {}).get("n_liquidaciones") == 1


def test_api_fetch_status_404() -> None:
    from tasa_estadistica.web.app import app

    client = TestClient(app)
    r = client.get("/api/fetch/status/no-existe")
    assert r.status_code == 404


def test_api_fetch_disabled(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ARCA_MODE", "mock")
    monkeypatch.setenv("ARCA_SQLITE_PATH", str(tmp_path / "x.db"))
    monkeypatch.setenv("ARCA_DATA_DIR", str(tmp_path / "d"))
    monkeypatch.setenv("ARCA_CUIT", "20123456789")
    monkeypatch.setenv("TASA_PANEL_FETCH_ENABLED", "false")

    from tasa_estadistica.web.app import app

    client = TestClient(app)
    r = client.post(
        "/api/fetch",
        json={"desde": "2025-05-01", "hasta": "2025-05-31"},
    )
    assert r.status_code == 403
