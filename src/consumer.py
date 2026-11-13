"""Consume, analyze and persist live aircraft events from Kafka."""
import json
import logging
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from src.analytics import FlightAnalytics
from src.config import settings
from src.repository import Repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("flight-consumer")


def decode_event(raw_value: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        log.warning("Skipping malformed Kafka message: %s", exc)
        return None
    if not isinstance(value, dict):
        log.warning("Skipping Kafka message whose JSON value is not an object")
        return None
    return value


def main() -> None:
    consumer = KafkaConsumer(
        settings.kafka_topic_aircraft,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset="earliest", enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda key: key.encode(),
        value_serializer=lambda value: json.dumps(value).encode(),
        acks="all", retries=10, enable_idempotence=True,
    )
    repository = Repository()
    analytics = FlightAnalytics()
    log.info("Processing topic=%s group=%s", settings.kafka_topic_aircraft, settings.kafka_consumer_group)
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            for _partition, messages in records.items():
                for message in messages:
                    event = decode_event(message.value)
                    if event is None:
                        consumer.commit()
                        continue
                    try:
                        if not event.get("icao24") or event.get("latitude") is None or event.get("longitude") is None:
                            raise ValueError("Required aircraft identity/position fields are missing")
                        repository.save_observation(event)
                        result, alerts = analytics.process(event)
                        repository.save_analytics(result)
                        producer.send("airport-analytics", key="EDDF", value=result).get(timeout=30)
                        for alert in alerts:
                            repository.save_alert(alert)
                            producer.send("flight-alerts", key=alert.get("icao24") or "EDDF", value=alert).get(timeout=30)
                            log.warning("ALERT %s %s", alert["alert_type"], alert["description"])
                        consumer.commit()
                        log.info("aircraft=%s density=%d zone=%s alerts=%d", event["icao24"], result["aircraft_count"], result["zone_id"], len(alerts))
                    except (ValueError, KeyError, KafkaError):
                        log.exception("Could not process Kafka record partition=%d offset=%d", message.partition, message.offset)
    except KeyboardInterrupt:
        log.info("Consumer interrupted")
    finally:
        consumer.close()
        producer.flush(timeout=30)
        producer.close()
        repository.close()


if __name__ == "__main__":
    main()
