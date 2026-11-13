"""Rule-based streaming analytics for live Frankfurt aircraft observations."""
from datetime import datetime, timezone
from math import floor
from typing import Any

from src.config import settings


def _parse_time(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class FlightAnalytics:
    """Maintain a bounded event-time view and emit actionable analytical records."""

    def __init__(self, window_seconds: int | None = None) -> None:
        self.window_seconds = window_seconds or settings.window_seconds
        self.positions: dict[str, dict[str, Any]] = {}
        self.recent: dict[str, datetime] = {}
        self.alert_state: dict[str, str] = {}

    def process(self, event: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        now = _parse_time(event.get("ingestion_timestamp"))
        cutoff = now.timestamp() - self.window_seconds
        self.recent = {key: ts for key, ts in self.recent.items() if ts.timestamp() >= cutoff}
        icao = event.get("icao24")
        previous = self.positions.get(icao)
        self.positions[icao] = event
        self.recent[icao] = now
        lat, lon = float(event["latitude"]), float(event["longitude"])
        size = settings.zone_size_degrees
        zone = f"{floor(lat / size) * size:.1f},{floor(lon / size) * size:.1f}"
        zone_count = sum(
            1 for key in self.recent
            if floor(float(self.positions[key]["latitude"]) / size) * size == floor(lat / size) * size
            and floor(float(self.positions[key]["longitude"]) / size) * size == floor(lon / size) * size
        )
        alerts: list[dict[str, Any]] = []

        def emit(kind: str, severity: str, description: str) -> None:
            fingerprint = f"{kind}:{zone if kind == 'congestion' else icao}"
            if self.alert_state.get(fingerprint) == description:
                return
            self.alert_state[fingerprint] = description
            alerts.append({
                "alert_id": f"{fingerprint}:{int(now.timestamp())}",
                "alert_type": kind,
                "severity": severity,
                "icao24": icao,
                "zone_id": zone,
                "description": description,
                "event_timestamp": now.isoformat(),
            })

        if zone_count >= settings.density_threshold:
            emit("congestion", "high", f"{zone_count} aircraft observed in zone {zone} within {self.window_seconds}s")
        altitude = event.get("baro_altitude_m")
        speed = event.get("velocity_mps")
        if event.get("on_ground") is False and altitude is not None and speed is not None:
            if float(altitude) >= settings.low_speed_altitude_m and float(speed) <= settings.low_speed_threshold_mps:
                emit("low_speed", "medium", f"Airborne aircraft speed {float(speed):.1f} m/s at {float(altitude):.0f} m")
        if previous:
            old_alt = previous.get("baro_altitude_m")
            new_alt = event.get("baro_altitude_m")
            old_track, new_track = previous.get("true_track_deg"), event.get("true_track_deg")
            if old_alt is not None and new_alt is not None and abs(float(new_alt) - float(old_alt)) >= 1500:
                emit("altitude_change", "medium", f"Altitude changed by {float(new_alt) - float(old_alt):.0f} m between observations")
            if old_track is not None and new_track is not None:
                change = abs((float(new_track) - float(old_track) + 180) % 360 - 180)
                if change >= 100:
                    emit("heading_change", "low", f"Heading changed by {change:.0f} degrees between observations")
        return ({
            "airport_id": "EDDF", "zone_id": zone, "aircraft_count": zone_count,
            "window_seconds": self.window_seconds, "icao24": icao,
            "event_timestamp": now.isoformat(),
        }, alerts)
