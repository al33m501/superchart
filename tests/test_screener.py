import copy
import threading
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from streamlit.testing.v1 import AppTest

from superchart import screener_data as data


@pytest.fixture
def cache(tmp_path, monkeypatch):
    path = tmp_path / "screener.json"
    monkeypatch.setenv("SCREENER_CACHE_PATH", str(path))
    return path


@pytest.fixture
def snapshot():
    table = pd.DataFrame({
        data.RETURN: [0.05, -0.03, 0.01],
        data.TURNOVER: [500e6, 100e6, 800e6],
        data.MEDIAN: [150e6, 60e6, 10e6],
    }, index=["AAA", "BBB", "CCC"])
    return data.make_snapshot(table, "2026-10-28T15:00:00")


@pytest.fixture
def moex(tmp_path, monkeypatch):
    monkeypatch.setenv("APIMOEX_TOKEN", "test-token")
    monkeypatch.setenv("PATH_TO_DATA_FOLDER", str(tmp_path))
    pd.DataFrame({"PX_LAST": [100.0, 200.0], "MEDIAN_TURNOVER": [150e6, 20e6]},
                 index=["AAA", "BBB"]).to_pickle(tmp_path / "last_prices.p")
    pd.DataFrame({"ticker": ["AAA"], "ex_date": [pd.Timestamp("2026-10-28")],
                  "dividend_amount": [5.0]}).to_pickle(tmp_path / "div_table.p")
    payload = {"marketdata": {"columns": ["SECID", "LAST", "VALTODAY", "SYSTIME"], "data": [
        ["AAA", 95.0, 500e6, "2026-10-28 15:00:00"],
        ["BBB", 210.0, 50e6, "2026-10-28 15:00:00"],
    ]}}
    response = Mock()
    response.json.return_value = payload
    get = Mock(return_value=response)
    monkeypatch.setattr(data.requests, "get", get)
    return response, get


def test_fetch_calculates_dividends_with_single_bounded_request(moex):
    response, get = moex
    result = data.fetch_snapshot()
    table = data.snapshot_table(result)
    assert table.loc["AAA", data.RETURN] == pytest.approx(0)
    assert table.loc["BBB", data.RETURN] == pytest.approx(0.05)
    assert table.loc["AAA", data.TURNOVER] == 500e6
    get.assert_called_once()
    assert get.call_args.kwargs["timeout"] == (5, 15)
    assert get.call_args.kwargs["verify"] is False
    assert get.call_args.kwargs["headers"] == {"Authorization": "Bearer test-token"}
    response.raise_for_status.assert_called_once()


@pytest.mark.parametrize("failure", ["timeout", "http", "json", "empty", "missing", "nulls"])
def test_failure_preserves_last_successful_result(cache, snapshot, moex, failure):
    data.write_snapshot(snapshot)
    original = cache.read_bytes()
    response, get = moex
    if failure == "timeout":
        get.side_effect = requests.Timeout()
    elif failure == "http":
        response.raise_for_status.side_effect = requests.HTTPError()
    elif failure == "json":
        response.json.side_effect = ValueError("bad JSON")
    elif failure == "empty":
        response.json.return_value["marketdata"]["data"] = []
    elif failure == "missing":
        response.json.return_value = {}
    else:
        response.json.return_value["marketdata"]["data"] = [["AAA", None, None, None]]
    assert data.refresh_once() is False
    assert data.read_refresh_status()["state"] == "error"
    assert cache.read_bytes() == original


def test_cold_start_failure_does_not_invent_history(cache, moex):
    moex[1].side_effect = requests.Timeout()
    assert data.refresh_once() is False
    assert not cache.exists()
    assert data.read_snapshot() is None


def test_recovery_and_older_response(cache, snapshot, moex):
    data.write_snapshot(snapshot)
    moex[0].json.return_value["marketdata"]["data"][0][3] = "2026-10-28 15:01:00"
    assert data.refresh_once() is True
    assert data.read_refresh_status()["state"] == "success"
    assert data.read_snapshot()["market_time"] == "2026-10-28T15:01:00"
    original = cache.read_bytes()
    moex[0].json.return_value["marketdata"]["data"][0][3] = "2026-10-28 14:00:00"
    assert data.refresh_once() is False
    assert data.read_refresh_status()["state"] == "error"
    assert cache.read_bytes() == original


def test_failed_atomic_replace_preserves_cache(cache, snapshot, monkeypatch):
    data.write_snapshot(snapshot)
    original = cache.read_bytes()
    monkeypatch.setattr(data.os, "replace", Mock(side_effect=OSError("disk error")))
    with pytest.raises(OSError):
        data.write_snapshot(snapshot)
    assert cache.read_bytes() == original
    assert list(cache.parent.glob("*.tmp")) == []


@pytest.mark.parametrize("content", ["{broken", "null", '{"version": 9}', '{"rows": []}'])
def test_corrupted_cache_is_a_miss(cache, content):
    cache.write_text(content, encoding="utf-8")
    assert data.read_snapshot() is None


def test_updater_retries_without_a_visitor(cache, moex):
    stop = threading.Event()
    response, get = moex

    def fail_then_succeed(*args, **kwargs):
        if get.call_count == 1:
            raise requests.Timeout()
        stop.set()
        return response

    get.side_effect = fail_then_succeed
    data.run_updater(stop, interval=0)
    assert get.call_count == 2
    assert data.read_snapshot() is not None


def test_page_reads_cache_filters_and_never_calls_moex(cache, snapshot, monkeypatch):
    data.write_snapshot(snapshot)
    get = Mock(side_effect=AssertionError("Page must not request MOEX"))
    monkeypatch.setattr(data.requests, "get", get)
    app = AppTest.from_file("superchart/pages/screener.py", default_timeout=10).run()
    assert not app.exception
    assert len(app.table) == 4
    assert list(app.table[0].value.index) == ["AAA"]
    assert list(app.table[3].value.index) == ["CCC", "AAA", "BBB"]
    app.radio[0].set_value(app.radio[0].options[-1]).run()
    assert not app.exception
    assert len(app.table[0].value) == 3
    get.assert_not_called()


def test_page_empty_then_new_snapshot_then_stale(cache, snapshot, monkeypatch):
    monkeypatch.setattr(data.requests, "get", Mock(side_effect=AssertionError("No network in UI")))
    app = AppTest.from_file("superchart/pages/screener.py", default_timeout=10).run()
    assert not app.exception
    assert len(app.error) == 1
    assert len(app.table) == 0
    stale = copy.deepcopy(snapshot)
    stale["saved_at"] = "2026-01-01T00:00:00+00:00"
    data.write_snapshot(stale)
    app.run()
    assert not app.exception
    assert len(app.error) == 0
    assert len(app.table) == 4
    assert len(app.warning) == 1
    assert "28.10.2026 15:00:00" in app.caption[0].value


@pytest.mark.parametrize("shutdown", ["web_exit", "sigterm"])
def test_launcher_runs_updater_and_stops_children(monkeypatch, shutdown):
    from superchart import run

    handlers = {}
    started = threading.Event()
    stopped = threading.Event()

    def updater(stop, interval):
        started.set()
        if stop.wait(3):
            stopped.set()

    web = Mock()
    web.returncode = 7 if shutdown == "web_exit" else None

    def poll():
        assert started.wait(3)
        if shutdown == "sigterm":
            handlers[run.signal.SIGTERM](run.signal.SIGTERM, None)
        return web.returncode

    web.poll.side_effect = poll
    spawn = Mock(return_value=web)
    monkeypatch.setattr(run, "load_dotenv", Mock())
    monkeypatch.setattr(run, "run_updater", updater)
    monkeypatch.setattr(run.signal, "signal", lambda signum, handler: handlers.update({signum: handler}))
    monkeypatch.setattr(run.subprocess, "Popen", spawn)
    monkeypatch.setenv("PORT", "12345")
    assert run.main() == (7 if shutdown == "web_exit" else 0)
    assert stopped.is_set()
    assert "12345" in spawn.call_args.args[0]
    if shutdown == "sigterm":
        web.terminate.assert_called_once()
        web.wait.assert_called_once_with(timeout=10)
    else:
        web.terminate.assert_not_called()


def test_error_status_is_visible_without_a_cache(cache, moex):
    moex[1].side_effect = requests.exceptions.SSLError("secret-token-must-not-leak")
    assert data.refresh_once() is False
    status = data.read_refresh_status()
    assert status["state"] == "error"
    assert "SSLError" in status["message"]
    assert "secret-token" not in status["message"]
    app = AppTest.from_file("superchart/pages/screener.py", default_timeout=10).run()
    assert not app.exception
    assert len(app.error) == 1
    assert any("SSLError" in item.value for item in app.markdown)
