from src.consumer import decode_event
from src.normalization import normalize_state, epoch_to_iso
import json

def test_normalize_open_sky_state_vector():
    # OpenSky state-vector field ordering; representative fixture, not live data.
    state = [
        "3c675a", "DLH123 ", "Germany", 1700000000, 1700000001,
        8.5622, 50.0379, 8500.0, False, 220.5, 270.2, -2.1,
        None, 8550.0, "1234", False, 0, 3
    ]
    event = normalize_state(
        state, 1700000002, "2026-11-09T14:30:02+00:00"
    )
    assert event["icao24"] == "3c675a"
    assert event["callsign"] == "DLH123"
    assert event["latitude"] == 50.0379
    assert event["longitude"] == 8.5622
    assert event["on_ground"] is False
    assert event["category"] == 3

def test_missing_position_is_skipped():
    state = ["3c675a", "DLH123", "Germany", 1700000000, 1700000001,
             None, None, 8500.0, False, 220.5, 270.2, -2.1,
             None, 8550.0, "1234", False, 0]
    assert normalize_state(state, 1700000002) is None

def test_epoch_to_iso_none():
    assert epoch_to_iso(None) is None

def test_invalid_position_and_timestamp_are_handled():
    state = ["3c675a", "DLH123", "Germany", "bad", "bad", 181.0, 50.0,
             8500.0, False, 220.5, 270.2, -2.1, None, 8550.0, "1234", False, 0]
    assert normalize_state(state, "invalid-time") is None
    assert epoch_to_iso("invalid-time") is None

def test_consumer_decodes_json_object_and_skips_malformed(caplog):
    assert decode_event(json.dumps({"icao24": "3c675a"}).encode()) == {
        "icao24": "3c675a"
    }
    assert decode_event(b"not-json") is None
    assert "Skipping malformed Kafka message" in caplog.text
