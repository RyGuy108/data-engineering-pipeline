"""Bounded NWS ingestion with replayable, atomically published raw batches."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

API_BASE = "https://api.weather.gov"
PAGE_LIMIT = 500
MAX_PAGES = 1000
MAX_HISTORY = timedelta(days=7)
REQUEST_WINDOW = timedelta(hours=6)
RETRY_STATUSES = {429, 500, 502, 503, 504}


class IngestionError(RuntimeError):
    """The source cannot be ingested completely; no batch was committed."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Invalid ISO timestamp: {value!r}") from exc
    else:
        raise ValueError("Timestamps must be timezone-aware ISO strings or datetimes")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamps must include a timezone")
    return result.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _station_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9_-]{1,16}", value):
        raise ValueError("station_id must contain 1–16 uppercase letters, digits, '_' or '-'")
    return value


def _validate_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname == "api.weather.gov"
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
        )
    except ValueError as exc:
        raise IngestionError("Malformed NWS URL") from exc
    if not valid:
        raise IngestionError("NWS requests and pagination must stay on https://api.weather.gov")
    return url


def _request_url(station_id: str, start: str, end: str, next_url: str | None = None) -> str:
    path = f"/stations/{station_id}/observations"
    base = API_BASE + path
    if next_url is None:
        query = {"start": start, "end": end, "limit": str(PAGE_LIMIT)}
        return base + "?" + urlencode(sorted(query.items()))
    candidate = _validate_url(urljoin(base, next_url))
    parsed = urlsplit(candidate)
    if parsed.path.rstrip("/") != path:
        raise IngestionError("Pagination changed the station or observation endpoint")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    # Cursor links may omit the filters. Reapply them to prevent expanded reads.
    query.update(start=start, end=end, limit=str(PAGE_LIMIT))
    return urlunsplit((parsed.scheme, parsed.netloc, path, urlencode(sorted(query.items())), ""))


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    fallback = min(2 ** (attempt - 1), 30)
    if response is None:
        return float(fallback)
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            delay = float(retry_after)
        except ValueError:
            try:
                date = parsedate_to_datetime(retry_after)
                delay = (date - _utc_now()).total_seconds()
            except (TypeError, ValueError, OverflowError):
                delay = float(fallback)
        if math.isfinite(delay):
            return max(0.0, min(delay, 60.0))
    return float(fallback)


class NWSClient:
    """NWS GeoJSON client. No API key or other credentials are required."""

    def __init__(self, user_agent: str, timeout: float = 30, max_attempts: int = 4):
        if not isinstance(user_agent, str) or not user_agent.strip() or any(c in user_agent for c in "\r\n"):
            raise ValueError("A nonempty, single-line NWS User-Agent is required")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 10:
            raise ValueError("max_attempts must be an integer from 1 to 10")
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.session = requests.Session()
        # Avoid implicitly reading ~/.netrc or sending environment credentials.
        self.session.trust_env = False
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/geo+json"})

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "NWSClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def get_json(self, url: str) -> dict[str, Any]:
        """Retrieve a JSON object, retrying only transient failures."""
        _validate_url(url)
        for attempt in range(1, self.max_attempts + 1):
            response = None
            try:
                response = self.session.get(url, timeout=self.timeout, allow_redirects=False)
                if response.status_code in RETRY_STATUSES:
                    if attempt < self.max_attempts:
                        time.sleep(_retry_delay(response, attempt))
                        continue
                    raise IngestionError(f"NWS request failed with HTTP {response.status_code} after {attempt} attempts")
                if not 200 <= response.status_code < 300:
                    raise IngestionError(f"NWS request failed with HTTP {response.status_code}")
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise IngestionError("NWS returned invalid JSON") from exc
                if not isinstance(payload, dict):
                    raise IngestionError("NWS returned a JSON value instead of an object")
                return payload
            except requests.RequestException as exc:
                if attempt == self.max_attempts:
                    # Do not include a transport exception that could echo local credentials.
                    raise IngestionError(f"NWS connection failed after {attempt} attempts ({type(exc).__name__})") from exc
                time.sleep(_retry_delay(None, attempt))
            finally:
                if response is not None:
                    response.close()
        raise IngestionError("NWS request did not complete")  # Defensive; attempts are validated.

    def get_station(self, station_id: str) -> dict[str, Any]:
        station_id = _station_id(station_id)
        payload = self.get_json(f"{API_BASE}/stations/{station_id}")
        props = payload.get("properties")
        if not isinstance(props, dict) or not isinstance(props.get("name"), str) or not props["name"].strip():
            raise IngestionError(f"Station {station_id} has invalid metadata")
        if props.get("stationIdentifier", station_id) != station_id:
            raise IngestionError("Source station metadata does not match requested station")
        geometry = payload.get("geometry") or {}
        coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
        latitude = longitude = None
        if coords is not None:
            if not isinstance(coords, list) or len(coords) < 2:
                raise IngestionError("Station coordinates are malformed")
            longitude, latitude = coords[:2]
            for value, lower, upper in ((latitude, -90, 90), (longitude, -180, 180)):
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not lower <= value <= upper:
                    raise IngestionError("Station coordinates are invalid")
        return {"station_id": station_id, "name": props["name"], "latitude": latitude, "longitude": longitude}

    def iter_observations(self, station_id: str, start: str, end: str) -> Iterator[dict[str, Any]]:
        """Read every page in one bounded interval, retaining bad records for QA."""
        station_id = _station_id(station_id)
        start_dt, end_dt = _timestamp(start), _timestamp(end)
        start, end = _iso(start_dt), _iso(end_dt)
        url = _request_url(station_id, start, end)
        visited: set[str] = set()
        for _ in range(MAX_PAGES):
            if url in visited:
                raise IngestionError("NWS pagination loop detected; refusing an incomplete batch")
            visited.add(url)
            payload = self.get_json(url)
            features = payload.get("features")
            if payload.get("type") != "FeatureCollection" or not isinstance(features, list):
                raise IngestionError("NWS observation response is not a GeoJSON FeatureCollection")
            paging = payload.get("pagination")
            if paging is not None and not isinstance(paging, dict):
                raise IngestionError("NWS pagination is malformed")
            next_url = (paging or {}).get("next")
            if next_url is not None and (not isinstance(next_url, str) or not next_url.strip()):
                raise IngestionError("NWS next-page link is malformed")
            if next_url is None and len(features) >= PAGE_LIMIT:
                raise IngestionError("NWS response reached the record limit without a next page; request shorter windows")
            validated_next = _request_url(station_id, start, end, next_url) if next_url is not None else None
            for feature in features:
                if not isinstance(feature, dict):
                    raise IngestionError("NWS features must be JSON objects")
                props = feature.get("properties")
                stamp = props.get("timestamp") if isinstance(props, dict) else None
                try:
                    observed_at = _timestamp(stamp)
                except ValueError:
                    # Required-field and timestamp validation belongs to the transform.
                    observed_at = None
                if observed_at is not None and not start_dt <= observed_at < end_dt:
                    continue
                yield feature
            if validated_next is None:
                return
            url = validated_next
        raise IngestionError(f"NWS pagination exceeded {MAX_PAGES} pages; refusing an incomplete batch")


def ingest_batch(
    client: NWSClient,
    raw_root: Path,
    windows: list[dict[str, str]],
    advance_checkpoint: bool = True,
) -> dict[str, Any]:
    """Publish one immutable raw batch after every station and page succeeds.

    All windows use [start, end) semantics. A seven-day lookback is a local
    conservative policy, not a promise about the source's historical coverage.
    Original raw batches can be replayed without calling this function.
    """
    raw_root = Path(raw_root).expanduser().resolve()
    if not isinstance(advance_checkpoint, bool):
        raise ValueError("advance_checkpoint must be boolean")
    if not isinstance(windows, list) or not windows:
        raise ValueError("At least one station window is required")
    now = _utc_now()
    normalized = []
    for window in windows:
        station_id = _station_id(window["station_id"])
        start, end = _timestamp(window["start"]), _timestamp(window["end"])
        if start >= end:
            raise ValueError("Each window start must be before its end")
        if end > now:
            raise ValueError("NWS observation windows must not end in the future")
        # Let the caller calculate an exact seven-day window just before entry.
        # This one-minute grace covers local setup, not an archival guarantee.
        if start < now - MAX_HISTORY - timedelta(minutes=1):
            raise ValueError("Live NWS ingestion is limited to the last 7 days; replay saved raw data for older history")
        normalized.append({"station_id": station_id, "start": _iso(start), "end": _iso(end)})
    normalized.sort(key=lambda w: (w["station_id"], w["start"], w["end"]))
    batch_id = now.strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex
    ingested_at = _iso(now)
    raw_root.mkdir(parents=True, exist_ok=True)
    staging = raw_root / f".{batch_id}.partial"
    destination = raw_root / batch_id
    staging.mkdir(exist_ok=False)
    try:
        stations = [client.get_station(station) for station in sorted({w["station_id"] for w in normalized})]
        count = 0
        digest = hashlib.sha256()
        with (staging / "observations.ndjson").open("w", encoding="utf-8", newline="\n") as handle:
            for window in normalized:
                slice_start, window_end = _timestamp(window["start"]), _timestamp(window["end"])
                while slice_start < window_end:
                    slice_end = min(slice_start + REQUEST_WINDOW, window_end)
                    for feature in client.iter_observations(window["station_id"], _iso(slice_start), _iso(slice_end)):
                        envelope = {
                            "batch_id": batch_id,
                            "ingested_at": ingested_at,
                            "station_id": window["station_id"],
                            "raw_json": _canonical(feature),
                        }
                        line = _canonical(envelope) + "\n"
                        handle.write(line)
                        digest.update(line.encode("utf-8"))
                        count += 1
                    slice_start = slice_end
            handle.flush()
            os.fsync(handle.fileno())
        manifest = {
            "batch_id": batch_id,
            "ingested_at": ingested_at,
            "start": _iso(min(_timestamp(w["start"]) for w in normalized)),
            "end": _iso(max(_timestamp(w["end"]) for w in normalized)),
            "raw_path": str(destination / "observations.ndjson"),
            "stations": stations,
            "windows": normalized,
            "advance_checkpoint": advance_checkpoint,
            "source": "nws",
            "manifest_path": str(destination / "manifest.json"),
            "record_count": count,
            "raw_sha256": digest.hexdigest(),
            "source_request": {"base_url": API_BASE, "max_window_hours": 6, "page_limit": PAGE_LIMIT, "window_semantics": "[start,end)"},
        }
        with (staging / "manifest.json").open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if destination.exists():
            raise IngestionError("Batch destination already exists; immutable batches cannot be overwritten")
        staging.rename(destination)
        return manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
