"""Environment-backed settings for the flight streaming application."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    """Runtime configuration populated from the project .env file."""

    opensky_client_id: str = os.getenv("OPENSKY_CLIENT_ID", "")
    opensky_client_secret: str = os.getenv("OPENSKY_CLIENT_SECRET", "")
    lat_min: float = float(os.getenv("LAT_MIN", "49.85"))
    lat_max: float = float(os.getenv("LAT_MAX", "50.25"))
    lon_min: float = float(os.getenv("LON_MIN", "8.25"))
    lon_max: float = float(os.getenv("LON_MAX", "8.85"))
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_aircraft: str = os.getenv("KAFKA_TOPIC_AIRCRAFT", "aircraft-positions")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "frankfurt-flight-monitor")
    db_path: str = os.getenv("DB_PATH", "data/flight_monitor.db")
    density_threshold: int = int(os.getenv("DENSITY_THRESHOLD", "20"))
    window_seconds: int = int(os.getenv("WINDOW_SECONDS", "300"))
    zone_size_degrees: float = float(os.getenv("ZONE_SIZE_DEGREES", "0.1"))
    low_speed_threshold_mps: float = float(os.getenv("LOW_SPEED_THRESHOLD_MPS", "35"))
    low_speed_altitude_m: float = float(os.getenv("LOW_SPEED_ALTITUDE_M", "600"))

    def __post_init__(self) -> None:
        """Fail early when configuration would produce invalid API queries."""
        if bool(self.opensky_client_id) != bool(self.opensky_client_secret):
            raise ValueError(
                "Set both OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET, or leave both blank."
            )
        if not (-90 <= self.lat_min < self.lat_max <= 90):
            raise ValueError("Latitude bounds must satisfy -90 <= LAT_MIN < LAT_MAX <= 90.")
        if not (-180 <= self.lon_min < self.lon_max <= 180):
            raise ValueError("Longitude bounds must satisfy -180 <= LON_MIN < LON_MAX <= 180.")
        if self.poll_interval_seconds < 10:
            raise ValueError("POLL_INTERVAL_SECONDS must be at least 10 seconds to avoid aggressive API polling.")
        if not self.kafka_topic_aircraft.strip() or not self.kafka_consumer_group.strip():
            raise ValueError("Kafka topic and consumer group settings cannot be empty.")

    @property
    def has_opensky_credentials(self) -> bool:
        return bool(self.opensky_client_id and self.opensky_client_secret)

settings = Settings()
