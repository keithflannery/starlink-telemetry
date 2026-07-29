# starlink-telemetry

Polls the Starlink Telemetry Stream API (**v2**) and writes the results to InfluxDB.

## One container per Starlink account

**A v2 service account is bound to a single Starlink account.** This is the
biggest change from v1, where one service account could reach every Starlink
account the creating user was a contact on.

So:

- **one Starlink account = one set of API credentials = one container.**
- There is no way to collect several accounts through a single instance.
- To cover N accounts, run N containers, each with its own credentials file.

v2 resolves the account from the bearer token, which is why there is no
`ACCOUNT_NUMBER` setting any more — the token decides which account you get.

### Creating credentials

In the Starlink account, create a **v2 service account** with the
**Device telemetry → View** permission. A v1 client id/secret returns
`401 Must use v2 service account`.

### Running more than one account

Add a service per account in `docker-compose.yml`, each pointing at its own env
file (see the commented example there):

```yaml
services:
  starlink-telemetry:
    image: keithflannery/starlink-telemetry:latest
    container_name: starlink-telemetry
    restart: always
    env_file:
      - .env

  starlink-telemetry-acct2:
    image: keithflannery/starlink-telemetry:latest
    container_name: starlink-telemetry-acct2
    restart: always
    env_file:
      - acct2.env
```

If several instances write to the **same Influx bucket**, set `ACCOUNT_LABEL` in
each env file. Without it nothing on `starlink_u` / `starlink_r` records which
account a device came from — the only tag is `device_id`. The alternative is a
separate bucket per account.

Rate limits are per account, so separate accounts do not compete for quota.

### Before setting ACCOUNT_LABEL on an existing deployment

No history is lost or rewritten — points already in Influx are immutable, and
turning this on does not touch them. But it is **not** simply a new column on
the existing series. Tags form part of the series key, so:

```
before:  starlink_u,device_id=ut-1               Uptime=42
after:   starlink_u,account=acct2,device_id=ut-1 Uptime=42
```

are two distinct series. New writes land in a parallel series next to the old
one rather than extending it.

What still works: history stays queryable indefinitely, `_measurement` and
`_field` filters match both sides, and anything explicitly grouping by
`device_id` continues to group old and new together.

What to watch for:

- A Flux `range()` spanning the changeover groups by the full tag set by
  default, so you get **two tables per device** instead of one.
- Because of that, `derivative()`, `difference()` and `increase()` will not span
  the boundary — expect a **one-off break in rate-style graphs** at the moment
  the variable is set. Counter fields such as `WanTxBytes` and `Uptime` are
  where this shows up.
- Series cardinality for affected devices doubles until the pre-change data
  ages out of the bucket's retention.

**Recommended:** leave `ACCOUNT_LABEL` unset on an already-running container and
set it only on containers added for new accounts. The incumbent account keeps an
unbroken series, and the data is still unambiguous — a device carrying an
`account` tag belongs to that account, one without belongs to the original.
`device_id` is unique across accounts regardless, so the tag is a grouping
convenience rather than a correctness requirement.

Backfilling the tag onto history means deleting and rewriting the range. Not
recommended: real risk for a cosmetic gain.

## Configuration

You need a .env file with the following information set:
```
CLIENT_ID=xxxx from starlink
CLIENT_SECRET=starlink client secret

INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=influx token
INFLUXDB_ORG=influx org
INFLUXDB_BUCKET=starlink
```

Optional, with defaults shown:
```
ACCOUNT_LABEL=        # adds an `account` tag; set when sharing a bucket
BATCH_SIZE=1000       # records per request, max 65000
MAX_LINGER_MS=15000   # how long the API blocks collecting records, max 65000
```

Leaving `ACCOUNT_LABEL` unset writes exactly the tags this collector has always
written. Turning it on never loses history, but it does start a new series —
read [Before setting ACCOUNT_LABEL on an existing
deployment](#before-setting-account_label-on-an-existing-deployment) first.

Polling is fixed at 15s (4 requests/minute), the documented recommendation. The
v2 rate limit is 250 requests/minute per account.

## Data written

| Measurement | Device type | Interval |
|---|---|---|
| `starlink_u` | User terminals | 15s |
| `starlink_r` | Routers | 15s |
| `starlink_i` | IP allocations | 5 min (immediate on IP change) |

Tags are `device_id`, plus `country_code` on user terminals where reported, plus
`account` if `ACCOUNT_LABEL` is set. `H3CellId` is converted to `latitude` /
`longitude` fields.

Field names come straight from the API's `columnNamesByDeviceType`. On
`starlink_u` and `starlink_r` values are coerced to floats exactly as they
always have been, so v1-era history and v2 data stay queryable together — string
columns such as `RunningSoftwareVersion` and `DishId` continue to be dropped
rather than changing an existing field's type. `starlink_i` is new in v2 and has
no history to conflict with, so its string columns are stored as-is.

Enum columns (`ActiveAlert`) are stored as raw numerics. The API returns a
`metadata.enums` map for decoding these, which is deliberately not applied —
writing labels would change the field type and Influx would reject the write.

## Operational notes

The stream has an **8 hour retention window**, and the read position advances
per credential on each successful response. A batch that is fetched but not
written is **not** re-delivered, and downtime longer than 8 hours loses data
permanently. Prefer `restart: always`.

## Building

Build context is the repo root, not `Docker/`:

```
docker build -f Docker/Dockerfile -t keithflannery/starlink-telemetry:latest .
```
