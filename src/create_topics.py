from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from src.config import settings

def main():
    admin = KafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="flight-topic-admin",
        request_timeout_ms=15000,
    )
    topics = [settings.kafka_topic_aircraft, "congestion-events", "flight-alerts", "airport-analytics"]
    try:
        try:
            response = admin.create_topics(
                [NewTopic(name=topic, num_partitions=3, replication_factor=1) for topic in topics],
                validate_only=False,
            )
        except TopicAlreadyExistsError:
            # Some kafka-python releases raise for the whole request if any
            # requested topic already exists, instead of returning per-topic
            # error results. Verify broker metadata and treat existing topics
            # as a successful, idempotent setup.
            existing = set(admin.list_topics())
            missing = [topic for topic in topics if topic not in existing]
            for topic in topics:
                print(f"{'Topic already exists' if topic in existing else 'Will retry creation'}: {topic}")
            if missing:
                response = admin.create_topics(
                    [NewTopic(name=topic, num_partitions=3, replication_factor=1) for topic in missing],
                    validate_only=False,
                )
            else:
                return
        # kafka-python versions differ: older versions return a mapping of
        # futures, newer ones return a CreateTopicsResponse with topic_errors.
        if hasattr(response, "items"):
            for topic, result in response.items():
                try:
                    result.result()
                    print(f"Created topic: {topic}")
                except TopicAlreadyExistsError:
                    print(f"Topic already exists: {topic}")
                except Exception as exc:
                    print(f"Could not create topic {topic}: {exc}")
        else:
            errors = {}
            for item in getattr(response, "topic_errors", []):
                # kafka-python response structs expose nested fields as tuples
                # in some releases and named objects in others.
                if isinstance(item, tuple) and len(item) >= 3:
                    errors[item[0]] = (item[1], item[2])
                else:
                    errors[getattr(item, "topic", None)] = (
                        getattr(item, "error_code", 0),
                        getattr(item, "error_message", ""),
                    )
            response_topics = getattr(response, "topic_errors", [])
            reported_topics = set(errors)
            for topic in topics:
                code, message = errors.get(topic, (0, ""))
                if code == 0:
                    print(f"Created topic: {topic}")
                elif code == 36:  # TOPIC_ALREADY_EXISTS
                    print(f"Topic already exists: {topic}")
                else:
                    print(f"Could not create topic {topic}: {message or code}")
    finally:
        admin.close()

if __name__ == "__main__":
    main()
