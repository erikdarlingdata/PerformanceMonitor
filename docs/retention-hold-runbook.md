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
cost real disk. "Monitor Store" in the alert is the Darling store itself, not one of your monitored servers.

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

# 3. RESTART. This step is required - see below.
Restart-Service -DisplayName "PerformanceMonitor Darling"

# 4. Confirm. Every rollup should now report
#    "nothing to do - coverage already reaches raw's oldest row".
.\PerformanceMonitor.Darling.Service.exe --backfill-rollups --dry-run
```

## Why step 3 is not optional

`TimescaleSupport.EnsureRetentionPoliciesAsync` is the only thing that arms a held policy, and it has a
single call site: the service startup path. So a policy does not arm the moment coverage catches up — it
arms on the **next service start**. Skip the restart and the backfill will have worked while the alert
keeps firing, which reads like the backfill failed.

## How you know it worked

Two independent confirmations:

- The second dry run reports nothing to do for every rollup.
- The alert resolves itself: one `Retention Hold Cleared` delivery, and the repeating Critical stops.

`query_store_*` is the family most likely to be the one behind. It carries by far the heaviest raw volume,
so it falls behind first and its hold costs the most disk — check it first if this recurs.

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
