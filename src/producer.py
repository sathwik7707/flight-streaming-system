import json
import logging
import signal
import time
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError
from src.config import settings
from src.opensky_client import OpenSkyClient, OpenSkyError
from src.normalization import normalize_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("flight-producer")
running = True

def stop(_signum, _frame) -> None:
    global running
    running = False

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=10,
        enable_idempotence=True,
        request_timeout_ms=30000,
        linger_ms=100,
    )

def run(once: bool = False) -> None:
    global running
    running = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    client = OpenSkyClient(
        settings.opensky_client_id, settings.opensky_client_secret
    )
    log.info("Starting Frankfurt aircraft producer; interval=%ss; auth=%s",
             settings.poll_interval_seconds, settings.has_opensky_credentials)
    try:
        producer = build_producer()
    except KafkaError:
        log.exception("Could not connect to Kafka at %s", settings.kafka_bootstrap_servers)
        return

    try:
        while running:
            started = time.monotonic()
            retry_after_seconds = 0
            try:
                payload, headers = client.get_states(
                    settings.lat_min, settings.lon_min,
                    settings.lat_max, settings.lon_max
                )
                snapshot_time = payload.get("time")
                states = payload.get("states") or []
                if not isinstance(states, list):
                    raise OpenSkyError("OpenSky response 'states' field was not a list")
                snapshot_id = str(uuid.uuid4())
                sent = 0

                for state in states:
                    event = normalize_state(state, snapshot_time)
                    if event is None:
                        continue
                    event["snapshot_id"] = snapshot_id
                    event["airport_reference"] = "EDDF"
                    future = producer.send(
                        settings.kafka_topic_aircraft,
                        key=event["icao24"],
                        value=event,
                    )
                    metadata = future.get(timeout=30)
                    log.debug("published topic=%s partition=%s offset=%s", metadata.topic, metadata.partition, metadata.offset)
                    sent += 1

                producer.flush(timeout=30)
                remaining = headers.get("X-Rate-Limit-Remaining", "not reported")
                log.info(
                    "snapshot=%s aircraft_returned=%d messages_published=%d "
                    "api_credits_remaining=%s",
                    snapshot_id, len(states), sent, remaining
                )
            except OpenSkyError as exc:
                retry_after_seconds = exc.retry_after_seconds or 0
                log.exception("OpenSky polling cycle failed: %s", exc)
            except (KafkaError, OSError, ValueError) as exc:
                log.exception("Polling/publishing cycle failed: %s", exc)
            except Exception:
                log.exception("Unexpected producer error")

            if once:
                break
            elapsed = time.monotonic() - started
            wait_seconds = max(
                1,
                settings.poll_interval_seconds - elapsed,
                retry_after_seconds,
            )
            time.sleep(wait_seconds)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=10)
        log.info("Producer stopped.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Publish live OpenSky aircraft states to Kafka.")
    parser.add_argument("--once", action="store_true", help="Run one API poll and exit.")
    args = parser.parse_args()
    run(once=args.once)
