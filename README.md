# Real-Time Flight Monitoring and Congestion Detection

This project polls live OpenSky Network aircraft state vectors around Frankfurt Airport (EDDF), publishes observations to Apache Kafka, analyzes them in a separate consumer, stores results in SQLite, and displays current observations and alerts in Streamlit.

## Business objective

Provide a small, reproducible monitoring pipeline for aircraft activity near Frankfurt. It computes aircraft counts in configurable geographic grid cells, raises rule-based alerts for dense airspace and unusual observations (such as low airborne speed, large altitude changes, or abrupt heading changes), and retains a local history for review.

OpenSky provides surveillance snapshots, not guaranteed event delivery, commercial flight schedules, or official delay/cancellation data. The system does not make air traffic control or safety decisions. Holding-pattern detection and schedule-based delay estimates are not implemented; a circular trajectory heuristic alone would not establish an actual holding clearance.

## Architecture and data flow

```text
OpenSky /api/states/all -> Python producer -> Kafka aircraft-positions
                                               -> Python analytics consumer
                                                  -> SQLite observations, density, alerts
                                                  -> Kafka airport-analytics, flight-alerts
                                               -> Streamlit dashboard reads SQLite
```

Kafka topics use three partitions and replication factor one (a single local broker). Aircraft messages are keyed by ICAO24. Producer publishing uses `acks=all`, retries and Kafka's idempotent producer mode. The consumer manually commits offsets after storage and derived Kafka sends. SQLite uniqueness constraints make repeated observations and alert inserts safe to retry. This is at-least-once processing with idempotent effects, not an end-to-end Kafka exactly-once transaction across SQLite and Kafka.

## Requirements

- Python 3.10+
- Docker Desktop/Engine with Docker Compose
- Internet access to OpenSky
- Optional OpenSky OAuth2 client credentials (recommended for usable quota)

## Setup

Run commands from the repository root.

### Windows PowerShell

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

Set `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` in `.env` for OAuth2. If both are blank, the project tries anonymous access, subject to OpenSky quota and current access policy. Never share or commit `.env`.

## Run the project

1. Check OpenSky connectivity and credentials:

   ```bash
   python -m src.check_opensky
   ```

2. Start the local Kafka broker and Kafka UI:

   ```bash
   docker compose up -d
   docker compose ps
   ```

   Wait for Kafka to report healthy. Kafka UI is at <http://localhost:8080>.

3. Create all required topics:

   ```bash
   python -m src.create_topics
   ```

4. Start the analytics consumer in one terminal:

   ```bash
   python -m src.consumer
   ```

5. Start the live producer in another terminal:

   ```bash
   python -m src.producer
   ```

   For one poll only, use `python -m src.producer --once`. The producer uses `POLL_INTERVAL_SECONDS` (default 300). Do not reduce it below 10 seconds; a 2–10 second interval is generally unsuitable for API credit quotas. If a poll fails, the producer continues after the configured interval and honors a server retry delay when supplied.

6. Launch the dashboard in another terminal:

   ```bash
   streamlit run src/dashboard.py
   ```

   Open the local URL printed by Streamlit (normally <http://localhost:8501>). The dashboard reads the same SQLite database used by the consumer.

Stop Python processes with Ctrl+C. Stop containers with `docker compose down`. `docker compose down -v` also deletes the Kafka data volume. SQLite data is stored at `data/flight_monitor.db` by default and is not deleted by Compose.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `LAT_MIN`, `LAT_MAX`, `LON_MIN`, `LON_MAX` | Frankfurt area | OpenSky bounding box |
| `POLL_INTERVAL_SECONDS` | `300` | Poll interval; minimum 10 seconds |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address for host processes |
| `KAFKA_TOPIC_AIRCRAFT` | `aircraft-positions` | Raw normalized observations |
| `KAFKA_CONSUMER_GROUP` | `frankfurt-flight-monitor` | Analytics consumer group |
| `DB_PATH` | `data/flight_monitor.db` | SQLite file path; `:memory:` for temporary use |
| `DENSITY_THRESHOLD` | `20` | Aircraft count per grid zone that raises congestion |
| `WINDOW_SECONDS` | `300` | Rolling observation window |
| `ZONE_SIZE_DEGREES` | `0.1` | Latitude/longitude grid cell size |
| `LOW_SPEED_THRESHOLD_MPS` | `35` | Airborne low-speed rule threshold |
| `LOW_SPEED_ALTITUDE_M` | `600` | Minimum altitude for low-speed rule |

The local default bounding box and thresholds are demonstration settings and should be calibrated against observed data. Change both OAuth values together or leave both blank.

## Topics and processing

| Topic | Key | Content |
|---|---|---|
| `aircraft-positions` | ICAO24 | Valid, normalized OpenSky observations |
| `airport-analytics` | EDDF | Zone density result for an observation |
| `flight-alerts` | ICAO24 (or EDDF) | Congestion and aircraft behavior alerts |
| `congestion-events` | reserved | Created for the requested architecture; current consumer emits density to `airport-analytics` |

The processor uses a five-minute in-memory event window. It estimates grid-cell counts and applies configurable rules. State resets if the consumer restarts; persisted observation history remains available in SQLite. The current UI shows stored positions, recent counts and alerts. API latency, source-to-Kafka latency, throughput, API quota, and Kafka consumer lag can be measured from logs/broker tools, but a metrics dashboard/exporter is not included.

## Duplicate and outage behavior

- OpenSky snapshots are polled, so the same aircraft may occur in many snapshots. The producer publishes observations, including unchanged observations; it does not claim a Kafka broker-level de-duplication guarantee.
- `acks=all`, retries and idempotent publishing reduce duplicates caused by producer retries within a producer session. Stable observation identity is based on ICAO24 plus source timestamp in SQLite, so replaying the same source observation does not add a second stored observation.
- If a consumer crashes after storage but before committing its Kafka offset, Kafka can replay that record. SQLite insert-or-ignore and alert IDs make storage idempotent. The derived Kafka send can still be repeated across the SQLite/Kafka boundary; true atomic exactly-once effects require Kafka transactions plus Kafka-backed state/storage or a transactional outbox.
- For a short 2–10 second source/API interruption, Kafka retains already published events and the consumer continues. The producer logs the failed poll and waits before retrying; it cannot recover snapshots that OpenSky never delivered. On restart, Kafka consumer offsets resume uncommitted messages. SQLite is durable across application restarts.

## Testing

```bash
pytest -q
```

Tests use representative state-vector fixtures and do not call OpenSky or require Kafka. Live execution additionally requires Docker, network access and (for reliable API quota) OpenSky credentials.

## Project layout

```text
src/
  analytics.py       rolling density and behavior rules
  check_opensky.py   source connectivity check
  config.py          environment settings
  consumer.py        Kafka analytics, persistence and alert publishing
  create_topics.py   Kafka topic setup
  dashboard.py       Streamlit map and tables
  normalization.py   OpenSky validation and event normalization
  opensky_client.py  OAuth2 and OpenSky API client
  producer.py        live polling and Kafka publishing
  repository.py      SQLite schema and persistence
tests/               offline normalization/consumer tests
docker-compose.yml   local Kafka in KRaft mode and Kafka UI
```
