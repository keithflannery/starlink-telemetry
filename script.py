import os
import time
import requests
import logging
import sys
import h3
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

# Optional. v2 credentials are bound to a single Starlink account, so one
# instance only ever sees one account's devices. Set this when several
# instances share an Influx bucket and the account a device belongs to needs to
# be queryable. Unset writes exactly the tags this collector always has.
ACCOUNT_LABEL = (os.getenv('ACCOUNT_LABEL') or '').strip()

INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Starlink API v2. The v1 API returns 404 as of 2026-06-01 and the legacy
# web-api.starlink.com/enterprise/ host stopped serving on 2026-07-01.
TOKEN_URL = 'https://starlink.com/api/auth/connect/token'
TELEMETRY_URL = 'https://starlink.com/api/public/v2/telemetry/stream'

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
MAX_LINGER_MS = int(os.getenv('MAX_LINGER_MS', '15000'))

# 15s polling is the documented recommendation: 4 req/min against an account
# limit of 250 req/min.
POLL_INTERVAL_S = 15

# Access tokens last ~15 minutes. Renew early rather than spending a request
# discovering the expiry via a 401.
TOKEN_RENEW_MARGIN_S = 60

MAX_BACKOFF_S = 60

# Device types that predate v2 and already have series in Influx. Their field
# coercion is frozen so old and new points stay queryable together. IP
# allocations ("i") are new in v2 and have no stored history, so their
# string-only columns can be written natively rather than silently dropped.
LEGACY_DEVICE_TYPES = {'u', 'r'}

_SKIP = object()


def get_starlink_access_token():
    """Return (access_token, monotonic deadline to renew by)."""
    response = requests.post(
        TOKEN_URL,
        data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type': 'client_credentials'
        },
        timeout=30
    )
    response.raise_for_status()
    payload = response.json()
    expires_in = payload.get('expires_in') or 900
    renew_at = time.monotonic() + max(30, expires_in - TOKEN_RENEW_MARGIN_S)
    return payload['access_token'], renew_at


def poll_starlink_telemetry(access_token):
    # v2 resolves the account from the bearer token. The request schema is
    # additionalProperties:false, so sending accountNumber is a 400.
    return requests.post(
        TELEMETRY_URL,
        json={
            "batchSize": BATCH_SIZE,
            "maxLingerMs": MAX_LINGER_MS
        },
        headers={
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        },
        timeout=(MAX_LINGER_MS / 1000) + 30
    )


def coerce_field_value(value, legacy=True):
    """Coerce an API value the way this collector always has.

    Existing series in Influx were written with exactly this logic, so for
    device types with stored history the field names and types have to keep
    landing the same way, or writes that mix old and new points get rejected on
    a type conflict. That means string columns stay dropped, as they always
    were. `legacy=False` keeps strings, which is only safe for a measurement
    that has no history to conflict with.
    """
    if isinstance(value, list):
        return ','.join(str(v) for v in value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        if value.strip() == "":
            return _SKIP
        if not legacy:
            return value
    try:
        return float(value)
    except (ValueError, TypeError):
        return _SKIP


def write_telemetry_to_influx(values, column_names, write_api):
    points = []

    for entry in values:
        device_type = entry[0]
        timestamp_ns = entry[1]
        device_id = entry[2]

        columns = column_names.get(device_type)
        if not columns:
            logging.warning(f"No column names for device type {device_type!r}, skipping entry.")
            continue

        fields = {}
        tags = {'device_id': device_id}
        if ACCOUNT_LABEL:
            tags['account'] = ACCOUNT_LABEL

        lat = long = None

        for idx, value in enumerate(entry[3:], start=3):
            if idx >= len(columns):
                logging.warning(
                    f"Device type {device_type!r} returned more values than column "
                    f"names ({len(entry)} vs {len(columns)}); ignoring the extras."
                )
                break

            col_name = columns[idx]

            # Handle H3CellId conversion directly
            if col_name == "H3CellId" and value:
                try:
                    h3_cell_id_int = int(value)
                    h3_hex = hex(h3_cell_id_int)
                    if h3.is_valid_cell(h3_hex):
                        lat, long = h3.cell_to_latlng(h3_hex)
                except (ValueError, TypeError, h3.H3ValueError):
                    logging.warning(f"Invalid H3 cell ID encountered: {value}")
                continue  # Skip writing H3CellId directly to influx

            # CountryCode is a string, so it cannot go into a float field. It
            # is stored as a string field rather than a tag: tags form part of
            # the series key, so tagging would fork every user terminal away
            # from its pre-v2 history and return two tables for any range
            # spanning the changeover. The old collector always dropped this
            # column, so the field name has no stored type to conflict with.
            if col_name == "CountryCode":
                if isinstance(value, str) and value.strip():
                    fields[col_name] = value.strip()
                continue

            coerced = coerce_field_value(value, legacy=device_type in LEGACY_DEVICE_TYPES)
            if coerced is not _SKIP:
                fields[col_name] = coerced

        # Include lat/long if successfully extracted
        if lat is not None and long is not None:
            fields['latitude'] = lat
            fields['longitude'] = long

        if not fields:
            continue

        point = Point(f'starlink_{device_type}')

        # Stamp each sample with the time the device reported it. Using the
        # write time instead collapses every entry in a batch onto one instant,
        # which silently overwrites same-device samples that share a series.
        sample_time = parse_timestamp_ns(timestamp_ns)
        if sample_time is not None:
            point.time(sample_time, WritePrecision.NS)
        else:
            point.time(datetime.datetime.now(datetime.timezone.utc), WritePrecision.S)

        # Add fields
        for key, val in fields.items():
            point.field(key, val)

        # Add tags
        for tag_key, tag_val in tags.items():
            point.tag(tag_key, tag_val)

        points.append(point)

    if not points:
        return 0

    # One write for the whole batch. Writing per point costs a round trip each,
    # which at batchSize=1000 does not reliably fit inside the 15s poll window;
    # falling behind matters because the stream only retains 8 hours.
    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
    except Exception as e:
        logging.error(f"Influx write failed for {len(points)} points: {e}")
        return 0

    return len(points)


def parse_timestamp_ns(timestamp_ns):
    try:
        parsed = int(timestamp_ns)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def main():
    access_token, token_renew_at = get_starlink_access_token()
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    backoff = 0

    try:
        while True:
            if backoff:
                logging.info(f"Backing off for {backoff}s before retrying.")
                time.sleep(backoff)

            start_time = time.time()

            if time.monotonic() >= token_renew_at:
                logging.info("Access token near expiry, refreshing...")
                access_token, token_renew_at = get_starlink_access_token()

            try:
                response = poll_starlink_telemetry(access_token)
            except requests.RequestException as e:
                logging.error(f"Request failed: {e}")
                backoff = min(max(backoff * 2, 1), MAX_BACKOFF_S)
                continue

            if response.status_code == 401:
                logging.info("Access token rejected, refreshing...")
                try:
                    access_token, token_renew_at = get_starlink_access_token()
                except requests.RequestException as e:
                    logging.error(f"Token refresh failed: {e}")
                backoff = min(max(backoff * 2, 1), MAX_BACKOFF_S)
                continue

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                try:
                    backoff = min(int(retry_after), MAX_BACKOFF_S)
                except (TypeError, ValueError):
                    backoff = min(max(backoff * 2, 1), MAX_BACKOFF_S)
                logging.warning("Rate limited by the Starlink API.")
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                logging.error(f"HTTP error: {e}")
                backoff = min(max(backoff * 2, 1), MAX_BACKOFF_S)
                continue

            backoff = 0

            payload = response.json()
            data = payload.get('data', {})
            values = data.get('values', [])
            column_names = data.get('columnNamesByDeviceType', {})

            # payload['metadata']['enums'] decodes DeviceType/ActiveAlert codes.
            # Deliberately not applied: those columns are already stored as
            # numerics, and writing labels instead would be a field type
            # conflict against the existing series.

            if values:
                written = write_telemetry_to_influx(values, column_names, write_api)
                logging.info(f"Wrote {written} telemetry points to InfluxDB.")
            else:
                logging.info("No new telemetry data.")

            elapsed_time = time.time() - start_time
            sleep_duration = max(0, POLL_INTERVAL_S - elapsed_time)
            time.sleep(sleep_duration)
    except KeyboardInterrupt:
        logging.info("Terminated by user.")
    finally:
        influx_client.close()


if __name__ == '__main__':
    main()
