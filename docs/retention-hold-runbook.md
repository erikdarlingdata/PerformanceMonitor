# Clearing a "Retention Held" alert (Darling)

The `Retention Held` self-alert (#2813) means a retention policy has been **paused on purpose** and the
tier it governs has grown past its configured horizon. Warning fires at 2.0x that horizon, Critical at 4.0x.

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

## Known rough edges

- **The alert's email does not carry any of the above.** The alert writes a detail block naming the
  `--backfill-rollups` remedy and the do-not-arm warning; the viewer and the MCP surface show it and the
  email template never receives it. #3297.
- **The 2.0x and 4.0x thresholds are not tunable.** They are compile-time constants rather than
  `config_alert_settings` rows, which makes this the one alert that cannot be adjusted in Settings. Also
  #3297.
