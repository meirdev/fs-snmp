# fs-snmp

SNMP polling pipeline:

```
┌──────────────┐  HTTP POST (JSON, gzip)  ┌──────────────┐  batched inserts  ┌────────────┐
│  fs-snmp     │ ───────────────────────▶ │  ingest      │ ────────────────▶ │ ClickHouse │
│  (Go poller) │                          │  (Python)    │                   │            │
└──────────────┘                          └──────────────┘                   └─────┬──────┘
   remote host                              your server                            │
                                                                            ┌──────▼──────┐
                                                                            │   Grafana   │  (dev only)
                                                                            └─────────────┘
```

- **fs-snmp** (Go) reads OID groups from `snmp.json`, polls each group of each
  device on its own interval, and ships samples to the ingest server in
  batches (or prints them to stdout for debugging).
- **ingest** (Python, FastAPI) validates batches, queues them and writes large
  inserts into ClickHouse. Returns 503 when the queue is full so the poller
  backs off and retries.
- **ClickHouse** stores numeric samples in `snmp.metrics` and text samples
  (names, MACs, descriptions) in `snmp.info`.
- **Grafana** reads ClickHouse directly; a "SNMP Device Overview" dashboard
  is provisioned automatically.

## Quick start (development)

```sh
docker compose up -d --build        # ClickHouse + ingest + Grafana
INGEST_TOKEN=dev-token ./fs-snmp    # poller on this machine (needs Go build, see below)
```

Then open Grafana at <http://localhost:3001> (anonymous admin, dev only) and
pick the device in the dashboard's "Device" dropdown.

To run the poller inside compose as well (the container must be able to reach
the SNMP devices):

```sh
docker compose --profile poller up -d --build
```

Service ports on the host: Grafana 3001, ingest 8081, ClickHouse 8123 (HTTP)
and 9000 (native). ClickHouse credentials are `snmp` / `snmp`. The ingest
token defaults to `dev-token`; set `INGEST_TOKEN` in the environment to change
it for both the compose stack and the poller.

## Poller

Requires Go 1.27 or newer.

```sh
go build -o fs-snmp .
./fs-snmp                          # use ./config.yaml
./fs-snmp -once                    # poll each group once and exit
./fs-snmp -output text             # force stdout text output (also: json, http)
./fs-snmp -config other.yaml -debug
```

Stop with Ctrl-C. Logs go to stderr.

### Configuration (`config.yaml`)

```yaml
oids_file: snmp.json

output:
  type: http                                  # stdout | http
  format: text                                # stdout only: text | json
  url: http://127.0.0.1:8081/api/v1/samples
  token: ${INGEST_TOKEN}                      # ${VAR} is expanded from the environment
  batch_size: 1000                            # samples per POST
  flush_interval: 5s                          # send partial batches at least this often
  timeout: 10s
  gzip: true

defaults:              # applied to every device unless overridden
  port: 161
  version: 2c          # 1 or 2c
  community: public
  timeout: 5s
  retries: 1
  groups:              # group name -> interval; 0 disables the group
    system: 5m
    information: 5m
    counters: 30s
    lldp: 10m
    vendor_specific: 1m

devices:
  - name: juniper-lab
    host: 172.20.1.150
    community: FlowSec_Community
    vendor: Juniper    # selects the vendor_specific objects
    groups:
      counters: 30s    # per-device override
```

Groups are the top-level keys of `snmp.json`: `system`, `information`,
`counters`, `lldp` and `vendor_specific`. The `vendor_specific` group polls
only the objects whose `vendors` list matches the device `vendor`
(case-insensitive). Known vendors: Arista, Cisco, F5 BIG-IP, Huawei, Juniper,
Nokia, Palo Alto Networks.

OIDs ending in `.0` are fetched with a single GET per group. Everything else
is walked with GETBULK (GETNEXT for SNMP v1). Objects with `"kind": "table"`
are split into one metric per column; an optional `"columns"` map in
`snmp.json` gives those columns names (see the Juniper `jnxOperatingTable`
entry, which yields `jnxOperatingCPU`, `jnxOperatingBuffer`, ...).

If the ingest server is unreachable a batch is retried four times with
backoff and then dropped with an error log line, so polling never stalls.
At most 100k samples are held in memory.

### Wire format

`POST /api/v1/samples` with `Content-Type: application/json`, optional
`Content-Encoding: gzip` and `Authorization: Bearer <token>`. The body is a
JSON array:

```json
[{"ts":1793046864226,"device":"juniper-lab","host":"172.20.1.150","group":"counters",
  "name":"ifHCInOctets","oid":"1.3.6.1.2.1.31.1.1.1.6.13","index":"13",
  "type":"counter64","value":51110329}]
```

`ts` is unix milliseconds. `type` is one of `integer`, `counter32`,
`counter64`, `gauge32`, `timeticks` (numeric) or `string`, `hex`, `oid`,
`ipaddr` (text). The same objects are printed by `-output json`.

## Ingest server

`ingest/app.py`. Environment variables:

| Variable              | Default     | Meaning                                   |
|-----------------------|-------------|-------------------------------------------|
| `CLICKHOUSE_HOST`     | `localhost` | ClickHouse host (HTTP interface)          |
| `CLICKHOUSE_PORT`     | `8123`      |                                           |
| `CLICKHOUSE_USER`     | `default`   |                                           |
| `CLICKHOUSE_PASSWORD` | empty       |                                           |
| `CLICKHOUSE_DB`       | `snmp`      |                                           |
| `INGEST_TOKEN`        | empty       | Bearer token; empty disables auth         |
| `FLUSH_ROWS`          | `50000`     | Insert when this many rows are queued     |
| `FLUSH_SECONDS`       | `2`         | ...or at least this often                 |
| `MAX_QUEUE_BATCHES`   | `500`       | Queue depth before answering 503          |

`GET /healthz` reports queue depth, rows inserted and the last insert error.

## ClickHouse schema

`clickhouse/init/01_schema.sql` runs on first start.

`snmp.metrics` (MergeTree): one row per numeric sample.
Sorted by `(device, metric, idx, ts)` so a single series is contiguous,
partitioned by day, 30-day TTL. `DoubleDelta` codec on timestamps,
`Gorilla` on values, `LowCardinality` labels.

`snmp.info` (ReplacingMergeTree keyed on `(device, metric, idx)`): only the
latest text value per object is kept. Use `argMax(value, ts)` or `FINAL` when
reading, since duplicates are collapsed lazily on merge.

`snmp.interface_rates` (view): per-second rates derived from the 64-bit
counters, with counter wraps filtered out.

Useful queries:

```sql
-- latest interface names
SELECT idx, argMax(value, ts) FROM snmp.info
WHERE device = 'juniper-lab' AND metric = 'ifDescr' GROUP BY idx;

-- inbound bits/s on ifIndex 13 for the last hour
SELECT ts, per_second * 8 AS bps FROM snmp.interface_rates
WHERE device = 'juniper-lab' AND metric = 'ifHCInOctets' AND idx = '13'
  AND ts > now() - INTERVAL 1 HOUR ORDER BY ts;
```

## Production notes

- Run the poller on the remote host with `output.type: http` pointing at the
  ingest server. Put the ingest server behind TLS (a reverse proxy) and set a
  real `INGEST_TOKEN`.
- Grafana and the host port mappings for ClickHouse are for development; drop
  them from the compose file in production.
- Only SNMP v1 and v2c are supported.

## Test device

```yaml
host: "172.20.1.150"
port: 161
community: "FlowSec_Community"
vendor: Juniper
```
