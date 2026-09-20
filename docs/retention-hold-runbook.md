# Clearing a "Retention Held" alert (Darling)

The `Retention Held` self-alert (#2813) means a retention policy has been **paused on purpose** and the
tier it governs has grown past its configured horizon. Warning fires at 2.0x that horizon by default,
Critical at 4.0x; both are settable — see [Adjusting the thresholds](#adjusting-the-thresholds).

Reported from the field in #3296, where the command sequence below came from the reporter.

## What the alert is telling you

Raw data is rolled up into continuous aggregates, and retention may only drop raw history once a rollup has
actually materialized it. If a rollup has not caught up, dropping raw would destroy the only copy — so the
coverage gate (#1680/#1877) pauses the retention policy instead.

The gate is working when this alert fires. What the alert adds is that the hold has lasted long enough to
cost real disk. "Monitor Store" in the alert is the Darling store itself, not one of your monitored servers
— or, on a store whose `peers.storeName` is set (#3500), that label is the store's self-chosen name for the
same thing.

## Do not arm the policy by hand

The history a held policy is keeping exists nowhere else. Arming it drops the only copy, which is exactly
what the gate prevents. The release is a backfill.

## The sequence

From the install folder, with the service installed:

```powershell
cd C:\PerformanceMonitorDarling    # your install folder may differ

# 1. See which rollups have gaps, and what a backfill would do. Changes nothing.
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups --dry-run

# 2. Run it. Safe while the service is up, and resumable.
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups

# 3. Wait for the next hourly retention re-evaluation (up to an hour), OR restart now if you
#    want the hold released immediately. Either one arms the policy; neither is required for
#    the other - see below.
Restart-Service -DisplayName "PerformanceMonitor Darling"    # optional

# 4. Confirm. Every rollup should now report
#    "nothing to do - coverage already reaches raw's oldest row".
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups --dry-run
```

## Why step 3 is a wait and not a restart

`TimescaleSupport.EnsureRetentionPoliciesAsync` is the only thing that arms a held policy. Until #3812 it
had a single call site, the service startup path, so a policy did not arm the moment coverage caught up —
it armed on the **next service start**, and skipping the restart left a backfill that had worked under an
alert that kept firing, which read like the backfill had failed. The running service now re-judges every
held policy on its **hourly store-maintenance tick** as well (the same tick as the compression-job health
check, at :30 past the minute), so the hold clears by itself within about an hour of the backfill
completing. A restart still runs the same gate on the start path; it is the hurry-up option, not the
remedy.

Do not let it wait a day, though: the hourly rollups carry their own retention policy, already armed,
which trims the coverage the backfill just built when it next fires (roughly daily). The hourly
re-evaluation is well inside that; if the trim ever wins the race nothing is lost — raw is still held —
and re-running the verb rebuilds it.

## How you know it worked

Three independent confirmations:

- The second dry run reports nothing to do for every rollup.
- The service log's next hourly line reads `Retention re-evaluation: 0 policies held, N armed this pass,
  K unchanged` — `armed this pass` is the release itself, and one `Retention policy for <relation> ARMED`
  line per policy names what was released. That line is written every hour whatever it found (`0 armed
  this pass` on a quiet hour), so its presence proves the re-evaluation ran without a restart marker.
- The alert resolves itself: one `Retention Hold Cleared` delivery, and the repeating Critical stops. This
  lands one tick AFTER the arm, because the alert's read runs before the re-evaluation on the same hourly
  tick — expect it on the hour following the `ARMED` line, not the same one.

`query_store_*` is the family most likely to be the one behind. It carries by far the heaviest raw volume,
so it falls behind first and its hold costs the most disk — check it first if this recurs.

## The store's background jobs, so a catalog read is not a surprise

`timescaledb_information.jobs` on a Darling store carries four families of jobs this product creates, and an
operator checking whether a hold has released will meet all of them:

- `policy_retention` — one per retained tier (raw tables, hourly history rollups, the interval-dedup layers, the
  baselines). `scheduled = false` on one of these is the hold this runbook is about.
- `policy_refresh_continuous_aggregate` — one per rollup; hourly ones on a fixed minute of the hour, daily ones on
  TimescaleDB's finish-to-start scheduling.
- `policy_compression` on a **raw hypertable** (`collect.query_stats`, `collect.wait_stats`, ...) — hourly tick,
  chunks older than 1 day, on the hourly phase grid's compression band.
- `policy_compression` on a **continuous aggregate** (`collect.query_stats_hourly`, `collect.procedure_stats_daily`,
  ...) — **once a day**, one aggregate per hour at `:35` UTC, compressing chunks older than 2 days (hourly-refreshed
  rollups) or 4 days (daily rollups). Added by #3581; before it the rollups were never compressed at all. On a store
  that had already materialized weeks of history their `next_start` values read as consecutive calendar days on
  the first pass — that is the backlog being staged one aggregate per night, largest first, not a scheduling
  fault. Every run after the first finds only the chunks that aged in since the day before. The materializations
  these jobs compress are chunked at **1 day**, the same width as every raw table (#3620; TimescaleDB's default
  would be ten times the raw width, which made a 7-day rollup hold up to 17 days and kept the newest ten days of
  every rollup out of compression's reach) — so on a store that predates #3620, `timescaledb_information.chunks`
  shows a mix of old 10-day and new 1-day chunks for a couple of weeks, and the short rollups' row counts shrink
  toward their designed window as the wide chunks age out. That tightening is convergence, not data loss.

None of the compression jobs participate in the coverage gate, and pausing or re-arming them has no effect on a
`Retention Held` alert. A `policy_compression` job on a rollup with `scheduled = false` is not a hold — the hold
mechanism only ever touches `policy_retention` rows.

## Where the rest of the verbs are

`--backfill-rollups` is one of about two dozen service verbs. Rather than copy a list here that would drift
out of date, ask the binary:

```powershell
.\PerformanceMonitor.Darling.Service.exe --help
```

That output is the authority, and it is generated from the parser that actually handles the arguments.

## Adjusting the thresholds

Both tiers live on the store's alert settings, so a change takes effect on the service's next sweep with no
restart. Three ways in, all writing the same two values:

- **Darling Viewer → Settings → Alerts**, the "Retention held past _ x its horizon; critical at _ x" row.
- **`update_alert_settings`**, under `self_alerts`:

  ```json
  { "self_alerts": { "retention_hold_warn_ratio": 3.0, "retention_hold_critical_ratio": 6.0 } }
  ```

- `config.config_alert_settings.retention_hold_warn_ratio` / `retention_hold_critical_ratio` directly.

**Both accept 2.0 or above, and that floor is deliberate.** Retention drops whole chunks and the retention
job runs on a schedule, so a tier legitimately holds more than its horizon while working perfectly — measured
on a healthy production store under 4-day horizons, that reaches **1.4x**. A threshold at or below about 1.5x
therefore fires on a store with nothing wrong with it, and the band between there and 2.0x is margin nobody
has measured, so the knob raises the tiers rather than lowering them. If the alert is too loud, raise it; to
silence one recurring signature instead, use a mute rule, which is scoped, expires, and is listed by
`get_mute_rules`.

Setting critical **below** warn is accepted and means every fire is Critical, with no Warning tier.
