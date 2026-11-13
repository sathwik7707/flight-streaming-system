from src.config import settings
from src.opensky_client import OpenSkyClient

def main():
    client = OpenSkyClient(
        settings.opensky_client_id, settings.opensky_client_secret
    )
    print("OpenSky mode:", "OAuth2 authenticated" if settings.has_opensky_credentials else "anonymous")
    print("Querying Frankfurt bounding box...")
    payload, headers = client.get_states(
        settings.lat_min, settings.lon_min, settings.lat_max, settings.lon_max
    )
    states = payload.get("states") or []
    print("API response time (UTC epoch):", payload.get("time"))
    print("Aircraft with state vectors in bounding box:", len(states))
    print("Remaining API credits:", headers.get("X-Rate-Limit-Remaining", "not reported"))
    if states:
        print("Sample ICAO24:", states[0][0])
        print("Sample callsign:", (states[0][1] or "").strip())
        print("Sample coordinates:", states[0][6], states[0][5])
    else:
        print("No aircraft returned in this snapshot. This can happen; retry later.")
    print("OpenSky connectivity check completed.")

if __name__ == "__main__":
    main()
