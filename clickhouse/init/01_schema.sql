-- fs-snmp schema. Executed once by the ClickHouse container on first start.

CREATE DATABASE IF NOT EXISTS snmp;

-- Numeric samples (counters, gauges, integers, timeticks).
--
-- ORDER BY (device, metric, idx, ts) keeps each time series contiguous on
-- disk, so "one interface's ifHCInOctets over the last hour" reads a few
-- granules. LowCardinality turns the repeated label strings into dictionary
-- ids. DoubleDelta on timestamps and Gorilla on values compress sampled
-- series extremely well. Daily partitions make TTL drops and backfills cheap.
CREATE TABLE IF NOT EXISTS snmp.metrics
(
    ts         DateTime64(3, 'UTC')  CODEC(DoubleDelta, ZSTD(1)),
    device     LowCardinality(String),
    host       LowCardinality(String),
    grp        LowCardinality(String),
    metric     LowCardinality(String),
    idx        String                CODEC(ZSTD(1)),
    value_type LowCardinality(String),
    value      Float64               CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (device, metric, idx, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Textual samples (names, descriptions, MAC addresses, OIDs, IPs).
--
-- These rarely change, so only the latest value per (device, metric, idx)
-- is kept: ReplacingMergeTree collapses duplicates on merge, with ts as the
-- version column. Query with FINAL or argMax(value, ts) to get the newest row
-- before a merge has happened.
CREATE TABLE IF NOT EXISTS snmp.info
(
    ts         DateTime64(3, 'UTC'),
    device     LowCardinality(String),
    host       LowCardinality(String),
    grp        LowCardinality(String),
    metric     LowCardinality(String),
    idx        String,
    value_type LowCardinality(String),
    value      String                CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (device, metric, idx);

-- Per-interface rates derived from the 64-bit counters: bytes/packets per
-- second between consecutive polls. Negative deltas (counter wrap or device
-- reboot) are discarded. Filter on device/metric/ts when selecting from it.
CREATE VIEW IF NOT EXISTS snmp.interface_rates AS
SELECT device, metric, idx, ts, (value - prev) / ((ts_ms - prev_ms) / 1000) AS per_second
FROM
(
    SELECT device, metric, idx, ts, value,
           toUnixTimestamp64Milli(ts) AS ts_ms,
           lagInFrame(value)  OVER w AS prev,
           lagInFrame(ts_ms)  OVER w AS prev_ms,
           row_number()       OVER w AS rn
    FROM snmp.metrics
    WHERE grp = 'counters' AND value_type = 'counter64'
    WINDOW w AS (PARTITION BY device, metric, idx ORDER BY ts)
)
WHERE rn > 1 AND value >= prev AND ts_ms > prev_ms;
