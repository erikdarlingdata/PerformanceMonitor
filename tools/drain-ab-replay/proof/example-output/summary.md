# Drain replay summary: proof

- Run started: 2026-10-08 20:30:49 UTC, ended: 2026-10-08 20:34:12 UTC, stop reason: fixed duration 200 s
- Server version: PostgreSQL 18.6 on x86_64-windows, compiled by msvc-19.44.35228, 64-bit; pg_stat_io: sampled
- Clients: 3, rate: 1.13 tx/s (rate scale 20), tables in the mix: 7, measured coverage: 100.0% of the store's hourly rows

## Per phase

A phase is the table the retention backend was deleting from (a pause of up to 10 s between batches stays in the phase). "no drain" is every second with no retention DELETE seen. Latency is the replayed write transaction (BEGIN to COMMIT), from pgbench --log.

| phase | start_utc | end_utc | seconds | rows_deleted | rows_per_sec | tx_per_sec | lat_p50_ms | lat_p95_ms | lat_max_ms | lag_p95_ms | lag_max_ms | active_samples | io_wait_share_pct | lock_wait_share_pct | lwlock_wait_share_pct | wal_mb_per_sec |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| no drain | 2026-10-08 20:31:21 | 2026-10-08 20:34:13 | 99 |  |  | 1.1 | 13.5 | 144.2 | 459.2 | 77.4 | 325.2 | 0 |  |  |  | 0.08 |
| wait_stats | 2026-10-08 20:30:46 | 2026-10-08 20:31:21 | 35 | 0 | 0.0 | 1.0 | 13.9 | 220.9 | 275.6 | 175.3 | 262.4 | 0 |  |  |  | 0.03 |
| perfmon_stats | 2026-10-08 20:31:45 | 2026-10-08 20:32:22 | 37 | 8000 | 216.2 | 0.8 | 17.0 | 261.8 | 275.3 | 248.1 | 252.1 | 0 |  |  |  | 0.05 |
| file_io_stats | 2026-10-08 20:32:44 | 2026-10-08 20:33:20 | 36 | 8000 | 222.2 | 1.2 | 11.4 | 254.6 | 719.5 | 94.8 | 308.9 | 0 |  |  |  | 0.10 |

lag_* is pgbench schedule lag: how late a transaction started against its slot in the rate schedule (the closest thing to a skipped collection slot).

## Latency by replayed table, per phase

| phase | script | tx | p50_ms | p95_ms | max_ms |
|---|---|---|---|---|---|
| no drain | perfmon_stats | 7 | 29.7 | 144.2 | 144.2 |
| no drain | wait_stats | 1 | 12.0 | 12.0 | 12.0 |
| no drain | file_io_stats | 5 | 16.6 | 40.0 | 40.0 |
| no drain | query_stats | 3 | 57.4 | 85.5 | 85.5 |
| no drain | collection_log | 42 | 12.0 | 49.3 | 179.9 |
| no drain | cpu_utilization_stats | 45 | 11.3 | 153.3 | 250.7 |
| no drain | query_store_stats | 5 | 30.8 | 459.2 | 459.2 |
| wait_stats | perfmon_stats | 6 | 12.8 | 17.3 | 17.3 |
| wait_stats | wait_stats | 4 | 7.8 | 16.8 | 16.8 |
| wait_stats | file_io_stats | 5 | 10.8 | 15.0 | 15.0 |
| wait_stats | query_stats | 3 | 29.1 | 39.9 | 39.9 |
| wait_stats | collection_log | 7 | 13.4 | 220.9 | 220.9 |
| wait_stats | cpu_utilization_stats | 11 | 15.8 | 275.6 | 275.6 |
| perfmon_stats | wait_stats | 3 | 27.2 | 34.5 | 34.5 |
| perfmon_stats | file_io_stats | 5 | 17.6 | 275.3 | 275.3 |
| perfmon_stats | query_stats | 1 | 254.4 | 254.4 | 254.4 |
| perfmon_stats | collection_log | 12 | 14.6 | 94.5 | 94.5 |
| perfmon_stats | cpu_utilization_stats | 10 | 15.4 | 261.8 | 261.8 |
| file_io_stats | perfmon_stats | 1 | 60.3 | 60.3 | 60.3 |
| file_io_stats | wait_stats | 4 | 15.4 | 17.2 | 17.2 |
| file_io_stats | file_io_stats | 4 | 8.8 | 187.6 | 187.6 |
| file_io_stats | query_stats | 3 | 29.9 | 719.5 | 719.5 |
| file_io_stats | collection_log | 15 | 9.5 | 334.6 | 334.6 |
| file_io_stats | cpu_utilization_stats | 15 | 11.1 | 103.3 | 103.3 |
| file_io_stats | query_store_stats | 3 | 20.2 | 21.7 | 21.7 |

## Service retention log lines

- 2026-10-08 20:31:00 [INF] Retention purge drained 20000 row(s) from wait_stats in 5 batch(es) (cap 4000), cutoff 2026-09-28 00:00Z
- 2026-10-08 20:33:00 [INF] Retention purge: 3 table(s) purged, 40000 row(s) deleted, 0 chunk(s) dropped, 0 failed, 150000ms

