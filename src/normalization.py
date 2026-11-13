"""Validation and normalization for OpenSky state-vector rows."""

import math
from datetime import datetime, timezone
from typing import Any, Sequence


def epoch_to_iso(value: Any) -> str | None:
    """Convert a Unix timestamp to an ISO-8601 UTC string when possible."""
    if value is None:
        return None
    try:
        timestamp = float(value)
        if not math.isfinite(timestamp):
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def normalize_state(
    state: Sequence[Any] | None,
    snapshot_time: Any,
    ingested_at: str | None = None,
) -> dict[str, Any] | None:
    """Map a valid state vector to a JSON-ready event; skip invalid positions."""
    if not isinstance(state, (list, tuple)) or len(state) < 17:
        return None

    icao24 = state[0]
    if not isinstance(icao24, str) or not icao24.strip():
        return None
    try:
        longitude = float(state[5])
        latitude = float(state[6])
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(longitude)
        or not math.isfinite(latitude)
        or not -180 <= longitude <= 180
        or not -90 <= latitude <= 90
    ):
        return None

    ingested_at = ingested_at or datetime.now(timezone.utc).isoformat()
    source_time = state[4] if state[4] is not None else state[3]

    return {
        "icao24": icao24.strip().lower(),
        "callsign": state[1].strip() or None if isinstance(state[1], str) else None,
        "origin_country": state[2],
        "time_position": state[3],
        "last_contact": state[4],
        "longitude": longitude,
        "latitude": latitude,
        "baro_altitude_m": state[7],
        "on_ground": state[8],
        "velocity_mps": state[9],
        "true_track_deg": state[10],
        "vertical_rate_mps": state[11],
        "geo_altitude_m": state[13] if len(state) > 13 else None,
        "squawk": state[14] if len(state) > 14 else None,
        "spi": state[15] if len(state) > 15 else None,
        "position_source": state[16] if len(state) > 16 else None,
        "category": state[17] if len(state) > 17 else None,
        "source_timestamp": epoch_to_iso(source_time),
        "snapshot_timestamp": epoch_to_iso(snapshot_time),
        "ingestion_timestamp": ingested_at,
    }
