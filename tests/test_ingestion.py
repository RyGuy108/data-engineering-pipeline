import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from weather_pipeline import ingestion
from weather_pipeline.ingestion import IngestionError, NWSClient, ingest_batch

NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
START = "2026-09-13T09:00:00Z"
END = "2026-09-13T11:00:00Z"


def feature(timestamp="2026-09-13T10:00:00Z", **props):
    return {"type": "Feature", "id": "nws-observation", "properties": {"timestamp": timestamp, "station": "https://api.weather.gov/stations/KAUS", **props}}


def page(*features, next_url=None):
    result = {"type": "FeatureCollection", "features": list(features)}
    if next_url is not None:
        result["pagination"] = {"next": next_url}
    return result


def response(status=200, payload=None, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = payload
    return result


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(ingestion, "_utc_now", lambda: NOW)
    monkeypatch.setattr(ingestion.time, "sleep", Mock())
    result = NWSClient("weather-pipeline-tests/1.0")
    result.session.get = Mock()
    yield result
    result.close()


def test_retries_transient_status_then_timeout_then_success(client):
    client.session.get.side_effect = [response(429, headers={"Retry-After": "999"}), requests.Timeout(), response(payload={"ok": True})]
    assert client.get_json("https://api.weather.gov/stations/KAUS") == {"ok": True}
    assert client.session.get.call_count == 3
    assert [call.args[0] for call in ingestion.time.sleep.call_args_list] == [60, 2]
    assert client.session.trust_env is False
    assert client.session.get.call_args.kwargs == {"timeout": 30, "allow_redirects": False}


def test_permanent_error_and_redirect_are_not_retried(client):
    for status in (403, 404, 302):
        client.session.get.reset_mock()
        client.session.get.side_effect = None
        client.session.get.return_value = response(status)
        with pytest.raises(IngestionError, match=str(status)):
            client.get_json("https://api.weather.gov/stations/KAUS")
        assert client.session.get.call_count == 1


def test_retry_exhaustion_is_bounded(client):
    client.session.get.side_effect = requests.ConnectionError("private transport detail")
    with pytest.raises(IngestionError, match="after 4 attempts") as raised:
        client.get_json("https://api.weather.gov/stations/KAUS")
    assert "private transport detail" not in str(raised.value)
    assert client.session.get.call_count == 4


def test_pagination_preserves_bounds_filters_outside_and_keeps_invalid(client):
    client.session.get.side_effect = [
        response(payload=page(feature(), next_url="https://api.weather.gov/stations/KAUS/observations?cursor=next")),
        response(payload=page(feature(END), feature("2026-09-13T08:00:00Z"), feature("bad"))),
    ]
    records = list(client.iter_observations("KAUS", START, END))
    assert [r["properties"]["timestamp"] for r in records] == ["2026-09-13T10:00:00Z", "bad"]
    params = parse_qs(urlsplit(client.session.get.call_args.args[0]).query)
    assert params["start"] == [START]
    assert params["end"] == [END]
    assert params["cursor"] == ["next"]


@pytest.mark.parametrize("next_url", [
    "https://evil.example/steal", "http://api.weather.gov/stations/KAUS/observations",
    "https://api.weather.gov.evil.example/", "https://secret@api.weather.gov/stations/KAUS/observations",
    "https://api.weather.gov:444/stations/KAUS/observations", "https://api.weather.gov/stations/KORD/observations",
])
def test_unsafe_next_links_fail_before_second_request(client, next_url):
    client.session.get.return_value = response(payload=page(feature(), next_url=next_url))
    with pytest.raises(IngestionError):
        list(client.iter_observations("KAUS", START, END))
    assert client.session.get.call_count == 1


def test_pagination_loop_and_silent_cap_fail(client, monkeypatch):
    client.session.get.return_value = response(payload=page(feature(), next_url="https://api.weather.gov/stations/KAUS/observations?cursor=same"))
    with pytest.raises(IngestionError, match="loop"):
        list(client.iter_observations("KAUS", START, END))
    monkeypatch.setattr(ingestion, "PAGE_LIMIT", 2)
    client.session.get.return_value = response(payload=page(feature(), feature()))
    with pytest.raises(IngestionError, match="record limit"):
        list(client.iter_observations("KAUS", START, END))


@pytest.mark.parametrize("payload", [[], {"features": []}, {"type": "FeatureCollection", "features": {}}, {"type": "FeatureCollection", "features": [], "pagination": "bad"}])
def test_malformed_source_fails(client, payload):
    client.session.get.return_value = response(payload=payload)
    with pytest.raises(IngestionError):
        list(client.iter_observations("KAUS", START, END))


def test_station_metadata(client):
    client.session.get.return_value = response(payload={"properties": {"name": "Austin", "stationIdentifier": "KAUS"}, "geometry": {"type": "Point", "coordinates": [-97.67, 30.19]}})
    assert client.get_station("KAUS") == {"station_id": "KAUS", "name": "Austin", "latitude": 30.19, "longitude": -97.67}
    client.session.get.return_value = response(payload={"properties": {"name": "Wrong station", "stationIdentifier": "KORD"}})
    with pytest.raises(IngestionError, match="does not match"):
        client.get_station("KAUS")


def mock_ingestion_client():
    result = Mock(spec=NWSClient)
    result.get_station.side_effect = lambda station: {"station_id": station, "name": station, "latitude": 30, "longitude": -97}
    result.iter_observations.side_effect = lambda *args: iter([feature(temperature={"value": None})])
    return result


def test_published_batch_is_replayable_canonical_and_immutable(client, tmp_path):
    mocked = mock_ingestion_client()
    windows = [{"station_id": "KAUS", "start": START, "end": END}]
    manifest = ingest_batch(mocked, tmp_path, windows)
    raw_path = Path(manifest["raw_path"])
    manifest_path = Path(manifest["manifest_path"])
    saved_bytes = raw_path.read_bytes()
    assert json.loads(manifest_path.read_text()) == manifest
    assert manifest["raw_sha256"] == hashlib.sha256(saved_bytes).hexdigest()
    envelope = json.loads(saved_bytes)
    assert envelope["batch_id"] == manifest["batch_id"]
    assert envelope["station_id"] == "KAUS"
    assert envelope["raw_json"] == json.dumps(feature(temperature={"value": None}), sort_keys=True, separators=(",", ":"))
    assert manifest["record_count"] == 1
    second = ingest_batch(mocked, tmp_path, windows, advance_checkpoint=False)
    assert manifest["batch_id"] != second["batch_id"]
    assert raw_path.read_bytes() == saved_bytes
    assert second["advance_checkpoint"] is False
    assert not list(tmp_path.glob(".*.partial"))


def test_partial_station_failure_never_publishes_batch(client, tmp_path):
    mocked = mock_ingestion_client()
    mocked.iter_observations.side_effect = [iter([feature()]), IngestionError("page unavailable")]
    windows = [{"station_id": station, "start": START, "end": END} for station in ("KAUS", "KORD")]
    with pytest.raises(IngestionError, match="page unavailable"):
        ingest_batch(mocked, tmp_path, windows)
    assert list(tmp_path.iterdir()) == []


def test_slices_long_windows_and_fetches_station_metadata_once(client, tmp_path):
    mocked = mock_ingestion_client()
    mocked.iter_observations.side_effect = lambda *args: iter([])
    manifest = ingest_batch(mocked, tmp_path, [{"station_id": "KAUS", "start": "2026-09-13T00:00:00Z", "end": END}])
    assert mocked.iter_observations.call_count == 2
    assert mocked.iter_observations.call_args_list[0].args[1:] == ("2026-09-13T00:00:00Z", "2026-09-13T06:00:00Z")
    assert mocked.get_station.call_count == 1
    assert manifest["record_count"] == 0
    assert Path(manifest["raw_path"]).read_text() == ""


@pytest.mark.parametrize("start,end", [
    (END, START), (START, "2026-09-13T13:00:00Z"),
    ("2026-09-01T00:00:00Z", END), ("2026-09-13T09:00:00", END),
])
def test_invalid_windows_fail_before_source_requests(client, tmp_path, start, end):
    mocked = mock_ingestion_client()
    with pytest.raises(ValueError):
        ingest_batch(mocked, tmp_path, [{"station_id": "KAUS", "start": start, "end": end}])
    mocked.get_station.assert_not_called()


def test_invalid_client_settings():
    for kwargs in ({"user_agent": ""}, {"user_agent": "x\r\nsecret"}, {"user_agent": "x", "timeout": 0}, {"user_agent": "x", "max_attempts": 0}):
        with pytest.raises(ValueError):
            NWSClient(**kwargs)
