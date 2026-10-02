# Backfilling the #3653 A6 successor dailies

`query_stats_interval_daily`, `procedure_stats_interval_daily` and `query_stats_db_interval_daily` are the
interval-honest successor DAILIES added by #3653 A6 lane LB. They are created `WITH NO DATA` and hierarchical
from the interval-honest successor HOURLIES (`query_stats_interval_hourly`, `procedure_stats_interval_hourly`,
`query_stats_db_interval_hourly` — #3653 Q12), not from the legacy trio their daily siblings read. Left alone
they fill forward from whichever start first refreshes them; this runbook backfills them over existing
history the same way `docs/retention-hold-runbook.md` backfills the hourly successors.

## Why only these three, and only this far back

The successor daily cannot hold more history than its own source: `query_stats_interval_daily` reads
`query_stats_interval_hourly`, so no backfill can put a row in the daily earlier than the hourly's own
materialized floor. The runbook therefore starts each daily at

```
date_trunc('day', successorHourlyFloor)
```

where `successorHourlyFloor` is that daily's successor hourly's own `min(bucket)` — a **partial first day**,
by design: the daily grid is UTC-midnight-aligned, and the hourly's floor is very unlikely to land on one.
Starting the daily's backfill any earlier would ask it to materialize a day its hourly has no rows for at
all, which reads as an empty result, not a gap — the same #1798 shape the general backfill verb's own remarks
describe for a daily whose source it under-reaches.

Do not run `--backfill-rollups` unrestricted expecting it to touch only these three: the general verb walks
every registered rollup (`RollupBackfill.Targets`, derived from `TimescaleSupport.RollupViews`) in dependency
order. That is fine — it is idempotent and no-ops over anything already converged — but it also re-touches
the legacy trio and every other rollup on the store, which is unnecessary I/O when the goal is only these
three. This runbook's SQL restricts the actual refresh calls to the three views named above.

## The sequence

From the install folder, with the service installed, on **every store**:

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"    # your install folder may differ

# 1. See the disk estimate and the plan for every rollup. Changes nothing. Read off the three
#    lines for query_stats_interval_daily, procedure_stats_interval_daily and
#    query_stats_db_interval_daily; the disk preflight applies to them like any other rollup.
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups --dry-run

# 2. Run it. Safe while the service is up, and resumable. This backfills every registered
#    rollup, not only the three successor dailies — expect the run to also converge the
#    hourlies and any other rollup not already caught up.
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups
```

Each successor daily's slice count is about **6 slices** (`RollupBackfill.SliceWidth` is one
`ChunkIntervalDays` — one day — per transaction, per `RollupBackfill.cs:61-64`), because the successor
hourlies' history reaches back to roughly the interval-honest hourlies' own creation (#3653 Q12, ~Sep 19
2026) — a handful of days at the time this runbook was written, growing by one day per day the hourlies run
uncollapsed. On the largest store this is the same disk-preflighted, per-chunk-locked operation the hourly
successors' own backfill is; nothing about the daily tier changes the preflight's shape, only the (smaller)
volume of data moved.

## Verifying it worked

For each of the three successor dailies, confirm its materialized floor reaches back to the day its
successor hourly's own floor starts:

```sql
SELECT
    (SELECT min(bucket) FROM collect.query_stats_interval_daily)      AS daily_floor,
    date_trunc('day', (SELECT min(bucket) FROM collect.query_stats_interval_hourly)) AS hourly_day_floor;
-- daily_floor should be <= hourly_day_floor once the backfill has converged.

SELECT
    (SELECT min(bucket) FROM collect.procedure_stats_interval_daily) AS daily_floor,
    date_trunc('day', (SELECT min(bucket) FROM collect.procedure_stats_interval_hourly)) AS hourly_day_floor;

SELECT
    (SELECT min(bucket) FROM collect.query_stats_db_interval_daily)  AS daily_floor,
    date_trunc('day', (SELECT min(bucket) FROM collect.query_stats_db_interval_hourly)) AS hourly_day_floor;
```

`daily_floor <= hourly_day_floor` for all three is the convergence condition; `--backfill-rollups --dry-run`
also reports "nothing to do" for each once it holds.

## What this does NOT do

- It does not change routing. Nothing reads these three successor dailies yet — that is #3653 A6 lane LA
  (the stitched reads) and lane LC (the freeze). Until those land, this backfill only fills history that sits
  unread.
- It does not touch compression. The three successor dailies are excluded from aggregate compression by
  `TimescaleSupport.CompressionDeferredUntilFreeze` until lane LC frees the daily compression band's slots (it
  is full at 23 members today; three more would overflow it to 26).
- It does not release any retention hold. The raw-purge and retention gates stay pointed at the legacy trio
  until lane LC re-points them.
