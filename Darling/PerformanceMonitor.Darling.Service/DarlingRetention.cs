/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Daily retention purge for the Darling Postgres store. The extension-free baseline is
/// DELETE-based and works on any Postgres; when the worker detected TimescaleDB
/// (<c>timescaleAvailable</c> — see TimescaleSupport in Darling.Storage) the collector tables
/// purge via hypertable <c>drop_chunks</c> instead, which detaches whole expired chunks in O(1)
/// instead of scanning rows. collection_log — a hypertable since V23, though converted directly by the
/// V23 migration rather than the catalog loop — purges the SAME way (drop_chunks with a DELETE fallback for
/// a plain-PostgreSQL store). config_alert_log, config.config_command and collect.plan_force_actions stay
/// DELETE-based either way (never converted — the first two are plain config-side registry tables, and the
/// journal keeps an identity PRIMARY KEY its own rows reference), as do the analysis tables
/// (PgFindingStore.CleanupOldFindingsAsync owns those). Retention horizons are the shared
/// per-collector <see cref="CollectorScheduleDefaults"/> (identity-pinned to Lite's
/// ScheduleManager table), so both SKUs keep the same data horizons out of the box. NOTE: Lite
/// archives expired rows to parquet before deleting (ArchiveService); Darling deliberately
/// purges without archiving — with Timescale, the compression policy on old chunks IS the
/// archival tier (compressed chunks stay queryable), and the plain-PG story remains
/// purge-without-archive for now.
/// <para>Every sweep writes one AUDITABLE run-record to collection_log under a fleet-sentinel server_id
/// (<see cref="DarlingObservability.FleetServerId"/>, never a real server) — SUCCESS, WARNING (some
/// tables failed their statement, already logged + isolated), or ERROR — so a stalled or partial purge is
/// visible in the collection log, not just the service log. The DELETE path drains each table in
/// one-day time slices (<see cref="TimeSlicedDeleteSql"/> — the compressed-chunk-safe successor to the
/// Dashboard's DELETE TOP idiom); collection_log is kept 2x the base data-retention window so a
/// run-record outlives the metric rows it explains.</para>
/// </summary>
public static class DarlingRetention
{
    /// <summary>
    /// The base data-retention window the collection_log horizon is a multiple of. 30 days matches the
    /// dominant collector <see cref="CollectorScheduleDefaults"/> horizon and the Dashboard's
    /// <c>@effective_retention_days</c> default (config.data_retention).
    /// </summary>
    internal const int DataRetentionBaseDays = 30;

    /// <summary>
    /// collection_log isn't a collector, so it has no <see cref="CollectorScheduleDefaults"/> entry to carry
    /// its horizon. It is kept at 2x the base window (mirrors the Dashboard's <c>retention_date x2</c> rule)
    /// so a collector run-record survives long enough to diagnose WHY a collector failed AFTER its metric
    /// rows have aged out — a 30-day metric row and its failure log would otherwise expire together, erasing
    /// the evidence. Effectively 60 days.
    /// </summary>
    internal const int CollectionLogRetentionDays = DataRetentionBaseDays * 2;

    /// <summary>
    /// #1743 follow-up: the raw collectors whose hypertables serve baselines DIRECTLY (their
    /// retired sum/sumsq rollups could not produce a median). Their effective purge horizon is
    /// floored at <see cref="BaselineMath.BaselineWindowDays"/> regardless of the user-editable
    /// schedule — the product-controlled insulation the rollups' fixed retention used to provide.
    /// BaselineSupplyTests pins membership against the provider's raw-reading arms.
    /// </summary>
    internal static readonly IReadOnlySet<string> BaselineServingRawCollectors =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "cpu_utilization", "file_io_stats" };

    /// <summary>
    /// config_alert_log (the fired-alert history: what alerted + delivery status, read by the viewer Alert
    /// History tab and the get_alert_history MCP tool) is a plain <c>config</c>-schema registry table — NOT a
    /// collector (so it has no <see cref="CollectorScheduleDefaults"/> horizon) and NOT a hypertable (so it
    /// purges via batched DELETE, never drop_chunks). It is INSERT-only (PgAlertHistoryStore) with no other
    /// purge path, so without a horizon it grows unbounded — the same class as the #1471 findings-cleanup gap.
    /// Kept 90 days (a quarter): alert history is low-volume and a valuable audit trail, so the horizon is
    /// generous, but it is BOUNDED. No operator setting governs this today (config_alert_settings carries the
    /// cooldown / lookback knobs, not an alert-history horizon), so this constant is the single source of truth.
    /// </summary>
    internal const int AlertHistoryRetentionDays = 90;

    /// <summary>
    /// config.config_command (the imperative command queue the Viewer/MCP/CLI enqueue into and the service
    /// executes) keeps its TERMINAL rows this long. Unlike config_alert_log this is not an audit surface
    /// anyone reads — nothing in the viewer or the MCP tools queries command HISTORY; a caller polls its own
    /// command_id for a terminal result and moves on — so the retained rows exist purely for post-hoc "what did
    /// the service actually do" forensics, which is worth exactly as long as the metric data they would be
    /// correlated against (<see cref="DataRetentionBaseDays"/>). It is also higher-volume than alert history:
    /// every viewer live-plan / actual-plan / active-queries fetch enqueues a row, which is why it gets the
    /// base window rather than the alert log's generous 90 days.
    /// </summary>
    internal const int CommandHistoryRetentionDays = DataRetentionBaseDays;

    /// <summary>
    /// collect.plan_force_actions (the auto force-plan bot's decision journal) keeps its rows this long. Like
    /// config_alert_log it is neither a collector (no <see cref="CollectorScheduleDefaults"/> horizon) nor a
    /// hypertable, and it is APPEND-ONLY with no other purge path — so this horizon is the only thing bounding
    /// it. The bot's own cooldowns bound the arrival RATE, which is not a size bound: a bounded rate over
    /// unbounded time is unbounded.
    /// <para>A full year — deliberately the longest horizon in the store, because this is the audit trail of a
    /// bot WRITING to production servers and it has to outlive the metrics that motivated each decision
    /// (<see cref="DataRetentionBaseDays"/>) by enough that "why did this plan change" is still answerable
    /// releases later. That is 4x the alert log's already-generous <see cref="AlertHistoryRetentionDays"/> and
    /// 12x the metric window, and it is nearly free: volume is capped by the bot's per-query cooldown and
    /// per-server daily force budget, so the ceiling is a few decisions per server per day rather than a
    /// sample per collection cycle. It also clears, by a wide margin, the longest window the bot itself reads
    /// back when judging eligibility (a week, for the two-taken-back-forces cooldown), so retention can never
    /// make the bot forget a decision it is still bound by. Generous, but BOUNDED — a monitoring store is
    /// sized for rolling windows, and "forever" is not a horizon. No operator setting governs this, so the
    /// constant is the single source of truth.</para>
    /// </summary>
    internal const int PlanForceLedgerRetentionDays = 365;

    /// <summary>
    /// The terminal-status filter for the command purge — the two states
    /// <c>ViewerDataService.IsTerminal</c> recognizes, which are also the only two
    /// <c>DarlingCommandExecutor</c> ever writes (its report path and its stale-command reaper). A
    /// <c>pending</c> or <c>in_progress</c> row is NEVER purged no matter how old: deleting a live command
    /// would strand the caller polling it, and an ancient pending row means an operator queued something the
    /// service has not run yet — the reaper's job, not retention's.
    /// </summary>
    internal const string TerminalCommandStatuses = "status IN ('succeeded', 'failed')";

    /* Each one-day slice is bounded work (see TimeSlicedDeleteSql); the generous 300s per-slice command
       timeout (well above Npgsql's 30s default) is belt-and-suspenders for a slow disk. Slicing is also what
       keeps a large first purge from ever hitting a timeout at all — a single unbounded DELETE could roll
       back on a long backlog and never catch up (retried tomorrow with a day MORE to delete). */
    private const int DeleteTimeoutSeconds = 300;

    /// <summary>
    /// Purges every collector table past its shared <see cref="CollectorScheduleDefaults"/>
    /// RetentionDays, plus collection_log past <see cref="CollectionLogRetentionDays"/>,
    /// config_alert_log past <see cref="AlertHistoryRetentionDays"/>, terminal
    /// config.config_command rows past <see cref="CommandHistoryRetentionDays"/>, and
    /// collect.plan_force_actions past <see cref="PlanForceLedgerRetentionDays"/>.
    /// When <paramref name="timescaleAvailable"/> (the worker's startup detection), the
    /// collector tables purge via <c>drop_chunks</c> (<see cref="DropChunksSqlFor"/>) with a
    /// per-table DELETE fallback so a table that failed hypertable conversion still honors its
    /// horizon; when false, the extension-free DELETE path runs unchanged. collection_log is
    /// DELETE-based either way. Failure-isolated per table: one failed statement is logged as a
    /// warning and the sweep continues (that table is retried on the next purge). Safe on a
    /// fresh/empty store — a purge that matches nothing removes nothing. Returns a coarse
    /// activity count: rows deleted by the DELETE paths plus whole chunks dropped by
    /// drop_chunks (Timescale doesn't report per-row counts for dropped chunks).
    /// </summary>
    /// <param name="retentionDaysFor">
    /// Optional resolver for a collector's effective retention horizon (control-plane fleet-wide overrides
    /// layered on <see cref="CollectorScheduleDefaults"/>). Null (or a value it does not override) uses the
    /// shared default. A per-server override cannot apply here — the purge is per shared table, not per server.
    /// The on-demand <c>purge_now</c> command passes a <c>_ =&gt; customDays</c> resolver for its custom-N mode.
    /// </param>
    /// <returns>
    /// A <see cref="PurgeSummary"/>: how many tables were touched and the coarse activity count (DELETE rows
    /// plus dropped chunks). The daily caller discards it; the on-demand <c>purge_now</c> command reports it.
    /// </returns>
    /// <param name="planContentRetentionDays">
    /// The V75 plan-content horizon (#2316): days a payload-dimension row outlives its last sighting
    /// before the GC may take it, independent of the fact-coupled horizon. 0 (the default here, for
    /// callers and tests that predate the knob) disables it — the fact-coupled horizon stands alone.
    /// </param>
    public static async Task<PurgeSummary> PurgeAsync(
        NpgsqlDataSource postgres, bool timescaleAvailable, ILogger? logger, CancellationToken cancellationToken,
        Func<string, int>? retentionDaysFor = null, int planContentRetentionDays = 0)
    {
        /* Clamp at the destructive sink, like retentionDaysFor's clamp below (review catch): the value
           arrives pre-clamped only when a store read succeeded and ApplyToConfig ran. On a
           store-unreachable boot the worker passes darling.json's RAW value, and a file value of 1-6
           would prune plan content below the [7,365] contract — the failure direction is data loss, so
           the sink does not trust its callers. */
        planContentRetentionDays = StoreConfigProvider.ClampPlanContentRetentionDays(planContentRetentionDays);

        var sw = Stopwatch.StartNew();
        var tablesPurged = 0;
        var totalRowsDeleted = 0;
        var totalChunksDropped = 0;
        var tablesFailed = 0;

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        try
        {
            foreach (var definition in CollectorCatalog.All)
            {
                if (!CollectorScheduleDefaults.All.TryGetValue(definition.Name, out var schedule))
                {
                    /* Impossible while the retention coverage test holds — belt-and-suspenders so schedule
                       drift degrades to a loud warning (and a WARNING run-record) instead of killing the sweep. */
                    logger?.LogWarning("Retention purge: no schedule entry for '{Collector}' — {Table} was not purged",
                        definition.Name, definition.TargetTable);
                    tablesFailed++;
                    continue;
                }

                /* Clamp at the destructive sink (belt-and-suspenders with the resolver + the V17 CHECK): a
                   retention of 0/negative would flip the cutoff into the present/future and drop_chunks /
                   DELETE the entire table. Never purge with a horizon under 1 day. */
                var retentionDays = Math.Max(1, retentionDaysFor?.Invoke(definition.Name) ?? schedule.RetentionDays);

                /* #1743 follow-up: the two raw hypertables that SERVE BASELINES directly (their retired
                   sum/sumsq rollups could not produce a median) get a product-controlled floor at the
                   baseline window — restoring, at the purge itself, the insulation the rollups' own
                   fixed retention used to provide by construction. Without this, lowering either
                   collector's user-editable retention below 30 days would silently shorten the CPU or
                   I/O baseline supply (#1757's shape); with it, the operator's shorter setting still
                   applies to nothing (these two families' floors ARE the product's baseline contract). */
                if (BaselineServingRawCollectors.Contains(definition.Name))
                {
                    retentionDays = Math.Max(retentionDays, BaselineMath.BaselineWindowDays);
                }

                /* #1784: this sweep and the tiered retention POLICY drop the same chunks, but only the policy
                   was coverage-gated. On a store where the #1680 gate is deliberately holding that policy —
                   because the rollup does not reach back over raw's history — this path dropped those very
                   chunks at the catalog horizon anyway, destroying exactly the uncovered history the gate
                   exists to protect. The gate was not bypassed by a bug in the gate; it was bypassed by an
                   older path that never learned the invariant.

                   So the same predicate now guards both. It is binary rather than a clamped cutoff because
                   drop_chunks can only remove the OLDEST chunks, and when coverage lags those ARE the
                   uncovered ones — there is no cutoff that drops the covered tail while sparing the uncovered
                   head. NOT an outright exclusion of tiered tables either: the moment coverage reaches back,
                   the normal horizon applies again, so a never-armed policy cannot mean unbounded growth. */
                if (timescaleAvailable && !await IsTieredDropSafeAsync(postgres, definition.TargetTable, logger, cancellationToken))
                {
                    logger?.LogWarning(
                        "Retention purge SKIPPED for {Table}: its rollup does not yet cover the oldest rows, so dropping would delete history no aggregate holds. Resumes by itself once a backfill extends coverage.",
                        definition.TargetTable);

                    /* Those rows are still there past their horizon — which is fine for the dimension GC
                       below (#1795): its cutoff is MEASURED from the oldest surviving digest-carrying fact
                       row, so held history bounds the GC instead of deferring it. */
                    continue;
                }

                if (timescaleAvailable)
                {
                    var dropped = await DropChunksOneAsync(
                        postgres, definition.TargetTable, DropChunksSqlFor(definition, retentionDays),
                        logger, cancellationToken);
                    if (dropped is not null)
                    {
                        tablesPurged++;
                        totalChunksDropped += dropped.Value;
                        continue;
                    }

                    /* drop_chunks failed (warned) — most likely this one table failed hypertable
                       conversion and is still plain. Fall back to the extension-free DELETE so the
                       table still honors its horizon instead of growing unbounded. */
                }

                var deleted = await PurgeOneAsync(
                    postgres, definition.TargetTable, DeleteSqlFor(definition),
                    utcNow.AddDays(-retentionDays), logger, cancellationToken);
                if (deleted is not null)
                {
                    tablesPurged++;
                    totalRowsDeleted += deleted.Value;
                }
                else
                {
                    tablesFailed++;

                    /* Both paths failed for this table, so its rows are still there past their horizon.
                       The dimension GC below stays safe anyway (#1795): its cutoff is measured from the
                       oldest surviving digest-carrying fact row, which those rows ARE. */
                }
            }

            /* #1767 payload dimensions. query_text_dim / query_plan_dim are plain tables holding one copy
               of each distinct query text / plan XML; the fact rows carry only a digest. They must be
               bounded or they re-create the very problem they solve — ~23 MB/hour of distinct plans on the
               measured field instance is ~200 GB/year if nothing ever expires.

               The obvious sweep (delete dim rows no live fact references) is an anti-join against two
               hypertables per dim row and is not affordable at this size. Instead the write path stamps
               last_seen on every cycle that references a digest, so last_seen is never older than the newest
               fact row pointing at it, and the GC is an index range scan on last_seen — run through the SAME
               time-sliced DELETE every sibling purge uses, rather than one unbounded statement. That matters
               most on the FIRST sweep after an upgrade, which is the one with a whole retention window of
               expired content to clear: unsliced, that is a single transaction holding a lock and generating
               WAL proportional to the entire backlog, which is exactly what the slicing exists to bound.

               The horizon is the WIDEST effective fact retention of the two tables — resolved through the
               same resolver the fact purge above uses, so a raised per-collector override can never outlive
               the dims and orphan a reader — plus a margin covering the two ways a fact can outlive its
               nominal horizon: drop_chunks only drops a chunk once its WHOLE range is past the cutoff (up to
               one ChunkIntervalDays of extra rows), and the upsert refreshes last_seen at most hourly. */
            var widestFactRetentionDays = 1;
            foreach (var definition in CollectorCatalog.All)
            {
                if (PayloadDimensions.ForTable(definition.TargetTable).Count == 0)
                {
                    continue;
                }

                var factRetentionDays = Math.Max(1, retentionDaysFor?.Invoke(definition.Name)
                    ?? (CollectorScheduleDefaults.All.TryGetValue(definition.Name, out var dimSchedule)
                        ? dimSchedule.RetentionDays
                        : 1));
                widestFactRetentionDays = Math.Max(widestFactRetentionDays, factRetentionDays);
            }

            /* That assumed cutoff embeds one assumption: facts older than their retention are GONE. Two
               legitimate mechanisms break it — a dim-feeding table whose purge FAILED (#1782), and one the
               coverage clamp above deliberately SKIPPED (#1784). The old response deferred the whole GC
               whenever either happened, which was correct but blunt: on a coverage-lagging store the clamp
               holds every sweep until a backfill lands, so the GC deferred every sweep and a 400-day-old
               orphan survived with nothing failed anywhere (#1795).

               The true safety boundary is MEASURED instead: content older than the oldest SURVIVING
               digest-carrying fact row cannot be referenced by anything, whatever the reason those facts
               are still there. Measure each dim-feeding table's floor once per sweep, take the MINIMUM
               across tables (same reasoning as the widest retention above), and clamp the assumed cutoff
               to it. The blunt guard becomes unnecessary rather than dormant: a store whose purges are
               blocked for weeks still reclaims dimension content for queries that stopped running long
               before, and held history bounds the GC instead of stopping it.

               HOW the floor is measured differs by store shape (#1815, from the first field run):
               - On a HYPERTABLE, the floor is the oldest surviving chunk's range_start, read from the
                 chunk catalog — one instant metadata row, immune to compression. The exact per-row probe
                 shipped first and timed out in the field: compressed chunks carry NO btree indexes, so
                 the V39 partial index only serves uncompressed chunks and months of compressed history
                 turned min() into a full decompress-scan. The chunk floor is CONSERVATIVE in the safe
                 direction — every surviving fact row sits at or above its chunk's range_start, so the
                 cutoff can only land older than strictly necessary: prunes less, never dangles. The
                 ancient-orphan pathology #1795 targeted still dies (those sit far below any surviving
                 chunk), and precision improves automatically as purges/backfill advance the chunk floor.
               - On a PLAIN table (a plain-PostgreSQL store, or a table whose hypertable conversion
                 failed — the catalog answers empty either way), the exact V39-indexed probe runs as
                 designed — small, uncompressed, index-edge — now with the sweep's own generous command
                 timeout instead of the driver's 30-second default that cancelled it in the field.

               A floor that cannot be MEASURED (the table missing/renamed/unreachable) is unchanged: the
               safety boundary is then unknown, so the GC defers for the cycle — fail toward the
               recoverable side, exactly as the old guard did. Bounded growth beats silently dangled
               digests. */
            DateTime? oldestSurvivingDigestFact = null;
            var floorUnmeasurable = false;
            foreach (var (factTable, digestPredicate) in PayloadDimensions.DigestPredicateByTable)
            {
                try
                {
                    await using var floorConnection = await postgres.OpenConnectionAsync(cancellationToken);

                    DateTime? floor = null;
                    if (timescaleAvailable)
                    {
                        /* AT TIME ZONE 'UTC' collapses the catalog's timestamptz to the naive-UTC
                           discipline every stored timestamp follows (see PgCollectorRowWriter). */
                        await using var chunkProbe = new NpgsqlCommand(
                            "SELECT range_start AT TIME ZONE 'UTC' FROM timescaledb_information.chunks " +
                            "WHERE hypertable_schema = 'collect' AND hypertable_name = $1 " +
                            "ORDER BY range_start LIMIT 1", floorConnection)
                        { CommandTimeout = DeleteTimeoutSeconds };
                        chunkProbe.Parameters.AddWithValue(factTable);
                        if (await chunkProbe.ExecuteScalarAsync(cancellationToken) is DateTime chunkFloor)
                        {
                            floor = chunkFloor;
                        }
                    }

                    if (floor is null)
                    {
                        /* Plain table, failed conversion, or an empty hypertable (no chunks) — all of
                           which the exact probe answers instantly or via the V39 partial index. */
                        await using var probe = new NpgsqlCommand(
                            $"SELECT min({PrefixTimeColumn(factTable)}) FROM {factTable} WHERE {digestPredicate}", floorConnection)
                        { CommandTimeout = DeleteTimeoutSeconds };
                        floor = await probe.ExecuteScalarAsync(cancellationToken) as DateTime?;
                    }

                    if (floor is DateTime floorTime
                        && (oldestSurvivingDigestFact is null || floorTime < oldestSurvivingDigestFact.Value))
                    {
                        oldestSurvivingDigestFact = floorTime;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    floorUnmeasurable = true;
                    logger?.LogWarning("Retention purge: could not measure {Table}'s digest-carrying fact floor: {Message}",
                        factTable, ex.Message);
                }
            }

            if (floorUnmeasurable)
            {
                /* One line, deliberately a fixed string never interpolated: the field signature an operator
                   greps for when the dimensions stop shrinking. The per-table warning above names which
                   table and why. */
                logger?.LogWarning("dimension GC deferred: a dim-feeding table's fact floor was unmeasurable this cycle; dimension content is retained until it can be measured");
            }
            else
            {
                var dimensionCutoff = ComputeDimensionCutoff(utcNow, widestFactRetentionDays, oldestSurvivingDigestFact);
                var planDimensionCutoff = ComputeDimensionCutoff(utcNow, widestFactRetentionDays, oldestSurvivingDigestFact, planContentRetentionDays);
                if (dimensionCutoff < utcNow.AddDays(-(widestFactRetentionDays + TimescaleSupport.ChunkIntervalDays + 1)))
                {
                    /* Fixed string, same reasoning as the defer line: the greppable signature of a store
                       whose GC is bounded by held history (clamped or failed purges) rather than by the
                       nominal horizon. Informational — this state is the fix working, not a problem. */
                    logger?.LogInformation("dimension GC bounded by surviving facts: dimension content newer than the oldest digest-carrying fact row is retained");
                }

                /* #2210: the Query Store plan map goes FIRST, and its cutoff is deliberately LATER than the
                   dimension's — it prunes more aggressively, so the dim always outlives the map it points into.
                   The two bad end-states are not symmetric. A pruned map row whose dim row survives renders a
                   plan as "not collected" and leaves bytes unreclaimed until the dim's own horizon passes:
                   visible, self-correcting, no wrong answers. A pruned DIM row whose map row survives is a
                   reader resolving a live fact to absent content, silently, weeks after the cause. Ordering the
                   cutoffs makes the recoverable end-state the only reachable one, and
                   QueryStorePlanMap.MarginOrderingHolds is pinned against ChunkIntervalDays so shrinking that
                   constant cannot invert it unnoticed. */
                /* #2219: PostgreSQL statement text, on the same principle as the plan map below but with the
                   margin in the OPPOSITE direction, because the asymmetry is the other way round. Text is what a
                   fact row points AT, so it must OUTLIVE the statistics that reference it: text kept past its
                   facts is a few dead bytes, whereas facts kept past their text is a top-queries answer that
                   reads as a list of integers — the exact failure this table was added to fix. Hence a margin
                   ADDED to the fact horizon rather than subtracted from it. */
                var textCutoff = utcNow.AddDays(-(widestFactRetentionDays + PgStatementText.PruneMarginDays));
                var textDeleted = await PurgeOneAsync(
                    postgres, PgStatementText.TableName,
                    PgStatementText.PruneSql(TimescaleSupport.ChunkIntervalDays),
                    textCutoff, logger, cancellationToken);
                if (textDeleted is not null)
                {
                    tablesPurged++;
                    totalRowsDeleted += textDeleted.Value;
                }
                else
                {
                    tablesFailed++;
                }

                /* #2316 review catch: the map must learn the plan-content horizon too, or the dedicated
                   dim cutoff overtakes this one and a live map row can point at deleted content — the
                   silent-missing-plans failure the margin ordering exists to prevent. ComputeMapCutoff
                   keeps the map's cutoff strictly NEWER than the plan dim's under every knob value, so
                   the only reachable end-state stays the recoverable one (map row gone first, plan
                   renders as not-collected). The visible consequence is deliberate: a Query Store plan
                   fetch for an interval older than the knob misses, exactly like the dim itself. */
                var mapCutoff = ComputeMapCutoff(utcNow, widestFactRetentionDays, planContentRetentionDays);
                var mapDeleted = await PurgeOneAsync(
                    postgres, QueryStorePlanMap.TableName,
                    QueryStorePlanMap.PruneSql(TimescaleSupport.ChunkIntervalDays),
                    mapCutoff, logger, cancellationToken);
                if (mapDeleted is not null)
                {
                    tablesPurged++;
                    totalRowsDeleted += mapDeleted.Value;
                }
                else
                {
                    tablesFailed++;
                }

                /* #2150: query_store statement text, same shape and same direction of margin as the two
                   above — it must outlive the facts that reference it, because text retired early reads as a
                   statement that never had text rather than as one whose text expired. */
                var queryTextCutoff = utcNow.AddDays(-(widestFactRetentionDays + QueryStoreTextStore.PruneMarginDays));
                var queryTextDeleted = await PurgeOneAsync(
                    postgres, QueryStoreTextStore.TableName,
                    QueryStoreTextStore.PruneSql(TimescaleSupport.ChunkIntervalDays),
                    queryTextCutoff, logger, cancellationToken);
                if (queryTextDeleted is not null)
                {
                    tablesPurged++;
                    totalRowsDeleted += queryTextDeleted.Value;
                }
                else
                {
                    tablesFailed++;
                }

                foreach (var dimTable in PayloadDimensions.DimTables)
                {
                    /* #2316 review catch: the dedicated horizon applies to PLAN content only. query_text_dim
                       is ~40 MB against the plan dim's 127 GB — shortening it buys nothing and would quietly
                       break "text stays analyzable for the facts' full retention", which is half the knob's
                       own justification. The router is pure so the scoping is pinned by tests. */
                    /* #2386: the plan dim is capped by ROWS, every other dim keeps the one-day slice.
                       Only this table carries multi-kilobyte TOAST payloads, so only this one has a day
                       that cannot be deleted inside the command timeout — query_text_dim is ~40 MB
                       total and drains in a single slice. Scoped rather than global so a table that is
                       fine keeps the compressed-chunk-safe shape it needs. */
                    var isPlanDim = string.Equals(
                        dimTable, PayloadDimensions.QueryPlanDimTable, StringComparison.Ordinal);

                    var dimDeleted = await PurgeOneAsync(
                        postgres,
                        dimTable,
                        isPlanDim
                            ? RowCappedDeleteSql(dimTable, PayloadDimensions.LastSeenColumn, PlanDimDeleteRowCap)
                            : TimeSlicedDeleteSql(dimTable, PayloadDimensions.LastSeenColumn),
                        ComputeDimTableCutoff(dimTable, dimensionCutoff, planDimensionCutoff),
                        logger,
                        cancellationToken,
                        batchSize: isPlanDim ? PlanDimDeleteRowCap : 1);
                    if (dimDeleted is not null)
                    {
                        tablesPurged++;
                        totalRowsDeleted += dimDeleted.Value;
                    }
                    else
                    {
                        tablesFailed++;
                    }
                }
            }

            /* collection_log retention. Since V23 it is a TimescaleDB hypertable (converted DIRECTLY by the V23
               migration — it is NOT in CollectorCatalog.All, so the loop above skips it), so with Timescale it
               purges via drop_chunks in O(1) — no DELETE churn — at its own 2x horizon (CollectionLogRetentionDays).
               On plain PostgreSQL, or if its hypertable conversion failed, it falls back to the batched DELETE so
               the horizon is ALWAYS honored. Failure-isolated like every sibling. The FleetServerId=0 retention
               run-record sentinel is a genuine collection_log row and lives in a chunk normally. */
            var logPurged = false;
            if (timescaleAvailable)
            {
                var droppedLog = await DropChunksOneAsync(
                    postgres, "collection_log", DropChunksSqlFor("collection_log", CollectionLogRetentionDays),
                    logger, cancellationToken);
                if (droppedLog is not null)
                {
                    tablesPurged++;
                    totalChunksDropped += droppedLog.Value;
                    logPurged = true;
                }

                /* drop_chunks failed (warned) — most likely collection_log's conversion failed and it is still
                   plain. Fall through to the extension-free DELETE so it still honors its horizon. */
            }

            if (!logPurged)
            {
                var logDeleted = await PurgeOneAsync(
                    postgres, "collection_log", TimeSlicedDeleteSql("collection_log", "collection_time"),
                    utcNow.AddDays(-CollectionLogRetentionDays), logger, cancellationToken);
                if (logDeleted is not null)
                {
                    tablesPurged++;
                    totalRowsDeleted += logDeleted.Value;
                }
                else
                {
                    tablesFailed++;
                }
            }

            /* config_alert_log (the fired-alert history) is a plain config-schema registry table, never a
               hypertable, so it purges via the same batched DELETE as collection_log — on its alert_time
               column, at the AlertHistoryRetentionDays horizon. INSERT-only (PgAlertHistoryStore) with no
               other purge path, so without this it grows unbounded (the #1471 findings-cleanup class of bug).
               Failure-isolated like every sibling: a failed statement is warned + counted, the sweep goes on. */
            var alertLogDeleted = await PurgeOneAsync(
                postgres, "config_alert_log", TimeSlicedDeleteSql("config_alert_log", "alert_time"),
                utcNow.AddDays(-AlertHistoryRetentionDays), logger, cancellationToken);
            if (alertLogDeleted is not null)
            {
                tablesPurged++;
                totalRowsDeleted += alertLogDeleted.Value;
            }
            else
            {
                tablesFailed++;
            }

            /* config.config_command (the imperative command queue) — the backstop the viewer's per-command
               cleanup already ASSUMED existed ("the service-side purge is the backstop",
               ViewerDataService.RunTestConnectAsync) but which nothing implemented (#1651). The viewer deletes
               its own row for exactly four self-cleaning flows, best-effort with the exception swallowed; every
               other command type (pause/resume, snapshot_now, analyze_now, purge_now, the enable/firewall
               verbs, collector toggles, anything from MCP or the CLI) left a terminal row and its result_json
               behind forever, as did those four whenever the delete failed or the viewer died mid-poll.
               SCHEMA-QUALIFIED deliberately: unlike collection_log / config_alert_log (created bare, so they
               live in `collect` under search_path = collect, config, public), this table really is in `config`,
               and a bare name here would resolve to a nonexistent collect.config_command — a purge that fails
               every night into a warning nobody reads. Keyed on created_at (NOT NULL, so no row can slip past
               the horizon by never being stamped) and filtered to terminal rows, never a live command.
               Failure-isolated like every sibling. */
            var commandsDeleted = await PurgeOneAsync(
                postgres, "config.config_command",
                TimeSlicedDeleteSql("config.config_command", "created_at", TerminalCommandStatuses),
                utcNow.AddDays(-CommandHistoryRetentionDays), logger, cancellationToken);
            if (commandsDeleted is not null)
            {
                tablesPurged++;
                totalRowsDeleted += commandsDeleted.Value;
            }
            else
            {
                tablesFailed++;
            }

            /* collect.plan_force_actions (the force-plan bot's decision journal) purges on its own action_time
               column at PlanForceLedgerRetentionDays — the longest horizon in the store. NOT in
               CollectorCatalog.All (it is written by the service's post-analysis bot pass, not a collector), so
               the loop above skips it, and it is append-only with no other purge path, which makes this the
               only thing bounding it.
               A batched DELETE, never a hypertable: action_id is a PRIMARY KEY that related_action_id points
               back to (a self-review row references the force row it re-judges), and TimescaleSupport already
               excludes PK-bearing tables for exactly that reason — conversion would reject the key or force it
               onto the partition column, breaking the self-reference the append-only design is built on.
               The work bound is the one-day SLICE, not an index seek: idx_plan_force_actions_time leads with
               server_id and this DELETE has no server_id predicate, so that index cannot be seeked here. Same
               shape as config_alert_log's purge against idx_config_alert_log_time(server_id, metric_name,
               alert_time), and adequate for the same reason — the bot's per-query cooldown and per-server
               daily budget cap arrivals at a few rows per server per day, so there is never much to scan.
               SCHEMA-QUALIFIED to match the V107 DDL and PgPlanForceActionStore, which both name
               collect.plan_force_actions explicitly. Unlike config.config_command a bare name would also
               resolve here (search_path = collect, config, public), but naming the schema keeps the purge and
               the writer readable against each other.
               Failure-isolated like every sibling: a failed statement is warned + counted, the sweep goes on. */
            var forceLedgerDeleted = await PurgeOneAsync(
                postgres, "collect.plan_force_actions",
                TimeSlicedDeleteSql("collect.plan_force_actions", "action_time"),
                utcNow.AddDays(-PlanForceLedgerRetentionDays), logger, cancellationToken);
            if (forceLedgerDeleted is not null)
            {
                tablesPurged++;
                totalRowsDeleted += forceLedgerDeleted.Value;
            }
            else
            {
                tablesFailed++;
            }

            var summary = new PurgeSummary(tablesPurged, totalRowsDeleted, totalChunksDropped);
            logger?.LogInformation(
                "Retention purge: {Tables} table(s) purged, {Rows} row(s) deleted, {Chunks} chunk(s) dropped, {Failed} failed, {ElapsedMs}ms",
                tablesPurged, totalRowsDeleted, totalChunksDropped, tablesFailed, sw.ElapsedMilliseconds);

            /* Auditable run-record: a clean sweep writes SUCCESS, a sweep where one or more tables failed
               their statement writes WARNING (the per-table failures were already logged + isolated above).
               Fleet-wide, so it lands under the sentinel server_id (DarlingObservability.LogRetentionRunAsync),
               which is failure-isolated and never breaks the loop. */
            var (status, message) = BuildRunRecordSummary(tablesPurged, totalRowsDeleted, totalChunksDropped, tablesFailed);
            await DarlingObservability.LogRetentionRunAsync(
                postgres, status, summary.TotalPurged, sw.ElapsedMilliseconds, message, logger, cancellationToken);

            return summary;
        }
        catch (OperationCanceledException)
        {
            /* Shutdown/cancellation — propagate exactly like the per-table helpers (no ERROR run-record; the
               purge simply didn't finish and retries on the next daily tick). */
            throw;
        }
        catch (Exception ex)
        {
            /* The per-table helpers isolate their own failures, so reaching here means something unexpected
               escaped the loop. Record an ERROR run-record for the audit trail and return what we managed to
               purge — PurgeAsync must never throw at the daily caller (it is not wrapped there), so a broken
               purge surfaces as an auditable ERROR row, not a crashed collection loop. */
            logger?.LogError("Retention purge failed: {Message}", ex.Message);
            await DarlingObservability.LogRetentionRunAsync(
                postgres, "ERROR", totalRowsDeleted + totalChunksDropped, sw.ElapsedMilliseconds, ex.Message, logger, cancellationToken);
            return new PurgeSummary(tablesPurged, totalRowsDeleted, totalChunksDropped);
        }
    }

    /// <summary>
    /// The status + human message for the auditable run-record of a completed sweep (not the exception path,
    /// which writes a literal ERROR): SUCCESS when every table purged cleanly, WARNING when
    /// <paramref name="tablesFailed"/> &gt; 0 (some table's statement failed — already logged + isolated).
    /// Pure so the SUCCESS/WARNING branch and the message text are unit-testable without a live store.
    /// </summary>
    internal static (string Status, string Message) BuildRunRecordSummary(
        int tablesPurged, int totalRowsDeleted, int totalChunksDropped, int tablesFailed)
    {
        var status = tablesFailed == 0 ? "SUCCESS" : "WARNING";
        var message = tablesFailed == 0
            ? $"Purged {tablesPurged.ToString(CultureInfo.InvariantCulture)} table(s): {totalRowsDeleted.ToString(CultureInfo.InvariantCulture)} row(s) deleted, {totalChunksDropped.ToString(CultureInfo.InvariantCulture)} chunk(s) dropped"
            : $"Purged {tablesPurged.ToString(CultureInfo.InvariantCulture)} table(s), {tablesFailed.ToString(CultureInfo.InvariantCulture)} failed (see prior warnings): {totalRowsDeleted.ToString(CultureInfo.InvariantCulture)} row(s) deleted, {totalChunksDropped.ToString(CultureInfo.InvariantCulture)} chunk(s) dropped";
        return (status, message);
    }

    /// <summary>
    /// The batched purge statement for one collector table — deletes expired rows one time slice at a time
    /// on the definition's own prefix time column ("collection_time" almost everywhere; the config snapshots
    /// purge on "capture_time"), executed in a loop until the table is drained (<see cref="PurgeOneAsync"/>).
    /// Table and column names come from the shared catalog constants, never from user input, so
    /// interpolation is safe here — the same reasoning as the runner's watermark read
    /// (DarlingCollectorRunner.GetLastCollectedTimeAsync).
    /// </summary>
    internal static string DeleteSqlFor(ICollectorSchemaInfo schema)
        => TimeSlicedDeleteSql(schema.TargetTable, schema.PrefixTimeColumnName);

    /// <summary>
    /// The batched purge statement: delete the OLDEST one-day slice of expired rows per execution —
    /// <c>WHERE {col} &lt; $1 AND {col} in [min expired, min expired + 1 day)</c> — repeated until a slice
    /// deletes nothing. This replaced a <c>ctid IN (SELECT ctid … LIMIT 10000)</c> row-cap idiom (#1564):
    /// reading the <c>ctid</c> system column through TimescaleDB's transparent decompression is unsupported
    /// ("transparent decompression only supports tableoid system column" — reproduced on the pinned 2.28.1),
    /// so the moment ANY in-range chunk was compressed the whole statement errored and the table silently
    /// kept its expired rows. A plain time-range predicate instead rides TimescaleDB's DML decompression,
    /// which IS supported on compressed chunks — and is a no-op cost on plain tables.
    /// <para>The slice width doubles as the work bound: one day of arrival volume per statement — exactly
    /// what a steady-state daily purge deletes in total, so a long backlog is drained in day-sized units the
    /// store already sustains daily (the old 10k row cap served the same goal; a day is also precisely one
    /// chunk on a hypertable, <see cref="TimescaleSupport.ChunkIntervalDays"/>). Both <c>min({col})</c>
    /// subqueries evaluate against the same statement snapshot, so they are always equal; when no expired
    /// rows remain, <c>min</c> is NULL, every comparison is unknown, zero rows delete, and the drain loop
    /// stops. In production the hypertables purge via <c>drop_chunks</c>, so this path runs on plain tables
    /// (a plain-PostgreSQL store, config_alert_log) or as the fallback when a hypertable's
    /// <c>drop_chunks</c> failed — where compressed chunks are LIKELY, which is what makes the
    /// compressed-safe shape load-bearing. <c>$1</c> is bound once and referenced by all three positions.
    /// Table/column come from catalog constants (never user input), so interpolation is safe.</para>
    /// <para><paramref name="extraPredicate"/> narrows WHICH rows are eligible — currently only
    /// <see cref="TerminalCommandStatuses"/>, so the command purge cannot touch a live command. It is
    /// applied to the DELETE <b>and to both <c>min()</c> subqueries</b>, which is load-bearing, not cosmetic:
    /// with the predicate on the DELETE alone, a slice anchored on an INELIGIBLE row's timestamp would
    /// delete zero rows, and the drain loop's "a slice that clears nothing means we are done" termination
    /// would stop the purge with older eligible rows still in the table. Like the table and column it is a
    /// compile-time constant, never user input.</para>
    /// </summary>
    internal static string TimeSlicedDeleteSql(string table, string timeColumn, string? extraPredicate = null)
    {
        var and = extraPredicate is null ? string.Empty : $" AND {extraPredicate}";
        var expired = $"{timeColumn} < $1{and}";

        return $"DELETE FROM {table} WHERE {expired}"
             + $" AND {timeColumn} >= (SELECT min({timeColumn}) FROM {table} WHERE {expired})"
             + $" AND {timeColumn} < (SELECT min({timeColumn}) FROM {table} WHERE {expired}) + INTERVAL '{TimescaleSupport.ChunkIntervalDays} days'";
    }

    /// <summary>
    /// Rows per statement for the plan dimension's purge (#2386). Measured on the use2 store, whose
    /// <c>query_plan_dim</c> is 133 GB over 12.4 M rows: deletes run at <b>~1,000 rows/sec</b>, linear
    /// from 10 k to 50 k, worst observed 834 rows/sec. 50 k therefore costs ~50 s against
    /// <see cref="DeleteTimeoutSeconds"/>'s 300 s — about 5x margin, which is the point: a loaded box is
    /// slower than the idle one this was measured on, and 100 k (~97 s nominal) would sit at ~291 s under
    /// a 3x slowdown, i.e. against the wall.
    /// </summary>
    internal const int PlanDimDeleteRowCap = 50_000;

    /// <summary>
    /// The plan dimension's purge statement: capped by ROW COUNT rather than by a time slice (#2386).
    ///
    /// <para><b>Why the time slice cannot work here.</b> <see cref="TimeSlicedDeleteSql"/> bounds work at
    /// one day, justified as "exactly what a steady-state daily purge deletes in total". True, and that is
    /// the problem for this table: a day is ~755 k rows whose gzipped plan XML averages ~9.5 KB, so one
    /// statement must remove <b>~7 GB of TOAST</b>. Measured, that needs ~755 s against a 300 s command
    /// timeout — 2.5x over, not borderline. It times out, the statement rolls back, nothing is deleted,
    /// and the next sweep retries the identical doomed slice. Retention on the largest table in the store
    /// stops permanently, and the table only grows. No choice of horizon avoids it: the slice width is set
    /// by the data at the old end, not by where the cutoff sits, so stepping the horizon down one day at a
    /// time meets the same full day at the first step.</para>
    ///
    /// <para><b>Why a row cap is available here specifically.</b> The <c>ctid</c> row-cap idiom was
    /// abandoned in #1564 because reading the <c>ctid</c> system column through TimescaleDB's transparent
    /// decompression is unsupported, so it errored the moment any in-range chunk was compressed.
    /// <c>query_plan_dim</c> is a PLAIN table, never a hypertable — that constraint has never applied to
    /// it, and the fact tables that do need the compressed-safe shape keep
    /// <see cref="TimeSlicedDeleteSql"/>.</para>
    ///
    /// <para><c>ORDER BY {timeColumn}</c> keeps the delete oldest-first, which the index on that column
    /// serves directly (measured: the planner takes an index-only scan, and the sibling <c>min()</c> probe
    /// costs 0.364 ms — the scan was never the expense). Oldest-first matters because progress has to be
    /// monotonic: an unordered cap would nibble arbitrary rows and leave the floor where it was.</para>
    /// </summary>
    internal static string RowCappedDeleteSql(string table, string timeColumn, int cap) =>
        $"DELETE FROM {table} WHERE ctid IN ("
      + $"SELECT ctid FROM {table} WHERE {timeColumn} < $1 "
      + $"ORDER BY {timeColumn} LIMIT {cap})";

    /// <summary>
    /// The dimension GC's cutoff (#1795): the ASSUMED horizon (widest dim-feeding fact retention +
    /// <see cref="TimescaleSupport.ChunkIntervalDays"/> drop_chunks granularity + 1 day for the hourly
    /// <c>last_seen</c> refresh guard), CLAMPED to one day before the oldest surviving digest-carrying
    /// fact row when that measured floor reaches further back — held history bounds the GC instead of
    /// deferring it. The measured side carries the SAME one-day margin, for the same reason: a dim row's
    /// <c>last_seen</c> can trail its newest referencing fact by up to the hourly refresh guard, so
    /// pruning right AT the floor could take content the floor row still references. A null floor (no
    /// digest-carrying facts anywhere — a fresh or fully-aged store) leaves the assumed horizon alone:
    /// with no facts, nothing can dangle, and last_seen still bounds what is old enough to take.
    /// </summary>
    internal static DateTime ComputeDimensionCutoff(DateTime utcNow, int widestFactRetentionDays, DateTime? oldestSurvivingDigestFact, int planContentRetentionDays = 0)
    {
        var assumed = utcNow.AddDays(-(widestFactRetentionDays + TimescaleSupport.ChunkIntervalDays + 1));
        var coupled = assumed;
        if (oldestSurvivingDigestFact is not null)
        {
            var measured = oldestSurvivingDigestFact.Value.AddDays(-1);
            coupled = measured < assumed ? measured : assumed;
        }

        /* #2316: the dedicated plan-content horizon DELIBERATELY overrides both safeties above for
           content past its window — that is its entire point. The coupled horizon guarantees no fact
           ever references deleted content, which also means a store younger than the fact retention
           has an UNBOUNDED dimension (measured: 127 GB in the dim's first 22 days, with the coupled
           GC unable to fire until a month after projected disk-full). With the knob enabled, a fact
           older than the window keeps its metrics, hashes and text but renders a MISSING plan — the
           null every reader already handles — in exchange for a bounded store. The same one-day
           margin as the measured side covers the hourly last_seen refresh guard. Disabled (0 or
           below) returns the coupled cutoff before any dedicated value is computed, so the old
           behavior is reproduced exactly rather than approximated through a comparison. */
        if (planContentRetentionDays <= 0)
        {
            return coupled;
        }

        var dedicated = utcNow.AddDays(-(planContentRetentionDays + 1));
        return dedicated > coupled ? dedicated : coupled;
    }

    /// <summary>
    /// Routes each payload dimension to its cutoff (#2316 review catch): the dedicated plan-content
    /// horizon governs <c>query_plan_dim</c> ONLY — every other dimension (query text today) keeps the
    /// fact-coupled cutoff, so text stays resolvable for the facts' full retention. Pure so the scoping
    /// decision is pinned by tests rather than living as an inline ternary nothing exercises.
    /// </summary>
    internal static DateTime ComputeDimTableCutoff(string dimTable, DateTime coupledCutoff, DateTime planDimensionCutoff) =>
        string.Equals(dimTable, PayloadDimensions.QueryPlanDimTable, StringComparison.Ordinal)
            ? planDimensionCutoff
            : coupledCutoff;

    /// <summary>
    /// The Query Store plan map's prune cutoff, knob-aware (#2316 review catch). The invariant
    /// (<see cref="QueryStorePlanMap.MarginOrderingHolds"/>): the DIMENSION must outlive the MAP, so the
    /// only reachable end-state is the recoverable one — a map row pruned while its content survives
    /// renders "not collected" and self-corrects; content pruned while a map row survives is a live fact
    /// resolving to absent XML, silently. The coupled pair keeps that gap at ChunkIntervalDays; the
    /// dedicated pair keeps it at one day (map at knob, dim at knob + 1 — the same one-day stamp-skew
    /// margin as everywhere else, because <c>TouchAndProbeSql</c> refreshes the map's stamp eagerly while the
    /// dim's refresh is hourly-guarded, so the dim's stamp can trail). Both components are strictly
    /// ordered, so the max-of-newer composition preserves the ordering under every knob value —
    /// pinned in PlanContentRetentionTests across the full age sweep.
    /// </summary>
    internal static DateTime ComputeMapCutoff(DateTime utcNow, int widestFactRetentionDays, int planContentRetentionDays = 0)
    {
        var coupled = utcNow.AddDays(-(widestFactRetentionDays + QueryStorePlanMap.PruneMarginDays));
        if (planContentRetentionDays <= 0)
        {
            return coupled;
        }

        var dedicated = utcNow.AddDays(-planContentRetentionDays);
        return dedicated > coupled ? dedicated : coupled;
    }

    /// <summary>
    /// The prefix time column of a dim-feeding fact table, resolved from the catalog so the floor probe
    /// can never disagree with the table's actual schema (both current dim-feeding tables use
    /// <c>collection_time</c>; the resolver keeps that true by construction rather than by assertion).
    /// </summary>
    private static string PrefixTimeColumn(string factTable) =>
        CollectorCatalog.All.First(c => string.Equals(c.TargetTable, factTable, StringComparison.Ordinal)).PrefixTimeColumnName;

    /// <summary>
    /// The Timescale purge statement for one collector table — <c>drop_chunks</c> detaches every
    /// chunk wholly older than the horizon (validated live on TimescaleDB 2.28.1; the partition
    /// column is implicit in the hypertable's dimension, so no time column appears here). An
    /// accepted coarseness: drop_chunks only drops WHOLE chunks, so rows inside a
    /// partially-expired chunk survive until the entire chunk ages past the horizon (with Darling's
    /// 1-day chunk interval — <see cref="!:TimescaleSupport.ChunkIntervalDays"/> — up to ~1 day of grace) —
    /// the trade for a metadata-only purge that never scans or rewrites rows. RetentionDays comes from the shared
    /// <see cref="CollectorScheduleDefaults"/> constants, never from user input, so
    /// interpolation is safe here — the same reasoning as <see cref="DeleteSqlFor"/>.
    /// </summary>
    internal static string DropChunksSqlFor(ICollectorSchemaInfo schema, int retentionDays)
        => DropChunksSqlFor(schema.TargetTable, retentionDays);

    /// <summary>
    /// The <c>drop_chunks</c> statement for a hypertable by raw table name — the collection_log path (a
    /// hypertable since V23 but outside the collector catalog, so it has no <see cref="ICollectorSchemaInfo"/>).
    /// Same shape as the schema overload; the table name comes from a compile-time constant, never user input,
    /// so interpolation is safe.
    /// </summary>
    internal static string DropChunksSqlFor(string table, int retentionDays)
        => $"SELECT drop_chunks('{table}', older_than => make_interval(days => {retentionDays}))";

    /// <summary>
    /// One table's drop_chunks; returns the number of chunks dropped, or null when it failed
    /// (warned; the caller falls back to DELETE for that table). drop_chunks returns one row per
    /// dropped chunk, so the count comes from reading the result set.
    /// </summary>
    private static Task<int?> DropChunksOneAsync(
        NpgsqlDataSource postgres,
        string tableName,
        string dropChunksSql,
        ILogger? logger,
        CancellationToken cancellationToken)
        => ExecuteDropChunksWithDeadlockRetryAsync(async () =>
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(dropChunksSql, connection) { CommandTimeout = DeleteTimeoutSeconds };

            var chunksDropped = 0;
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                chunksDropped++;
            }

            return chunksDropped;
        }, tableName, logger);

    /// <summary>
    /// Runs one table's drop_chunks with a SINGLE immediate retry on deadlock (#2143). 40P01 is transient
    /// by definition — the deadlock partner (in the field: a TimescaleDB background job holding chunk
    /// locks, caught live by the nightly's purge e2e) commits or aborts within milliseconds of the abort,
    /// so one retry converts a wasted purge cycle into a completed one. Exactly ONE retry: a second
    /// deadlock in a row means the contention is standing, and the DELETE fallback plus next cycle's
    /// sweep — the behavior this wraps — is the right posture, not a retry loop camped on a lock queue.
    /// Any non-deadlock failure keeps the original single-shot behavior. Internal, delegate-seamed, so
    /// the retry/give-up/no-retry arms pin without a store.
    /// </summary>
    internal static async Task<int?> ExecuteDropChunksWithDeadlockRetryAsync(
        Func<Task<int>> dropChunks, string tableName, ILogger? logger)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await dropChunks();
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.DeadlockDetected && attempt == 1)
            {
                logger?.LogWarning(
                    "Retention purge (drop_chunks) deadlocked for {Table} — retrying once (the partner clears in milliseconds): {Message}",
                    tableName, ex.Message);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Failure-isolated per table — warned here, then the caller's DELETE fallback runs. */
                logger?.LogWarning("Retention purge (drop_chunks) failed for {Table} — falling back to DELETE: {Message}",
                    tableName, ex.Message);
                return null;
            }
        }
    }

    /// <summary>
    /// May this table's expired chunks be dropped, per the #1680 coverage gate (#1784)? Delegates to
    /// <see cref="TimescaleSupport.IsRawTierDropSafeAsync"/> so the sweep and the tiered policy cannot judge
    /// the same drop differently; non-tiered tables are always safe.
    ///
    /// <para>A connection failure answers "not safe" — for a DROP the fail-closed direction is to leave the
    /// data alone and re-judge next cycle, which is the same instinct the gate itself follows. That does mean
    /// a store that cannot reach its own catalogs stops purging these three tables; the skip is logged every
    /// cycle, and the alternative is deleting history on a guess.</para>
    /// </summary>
    private static async Task<bool> IsTieredDropSafeAsync(
        NpgsqlDataSource postgres,
        string tableName,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        /* Membership first: the sweep calls this for EVERY catalog table, and only the three raw tiers are
           gated. Opening a pooled connection just to have the predicate return true for the other thirty is
           avoidable work on a path that already runs per table per day. */
        if (!TimescaleSupport.IsCoverageGatedRelation(tableName))
        {
            return true;
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            return await TimescaleSupport.IsRawTierDropSafeAsync(connection, tableName, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not establish rollup coverage for {Table} ({Message}) — skipping its drop this cycle rather than deleting history on an assumption.",
                tableName, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// One table's batched DELETE: re-executes <paramref name="deleteSql"/> (a <see cref="TimeSlicedDeleteSql"/>
    /// statement clearing the oldest one-day slice of expired rows) until a slice deletes nothing — i.e. the
    /// table is drained. Returns the total rows deleted across all slices, or null when it failed (warned,
    /// sweep continues). Slicing bounds lock/WAL/dead-tuple growth on a large first purge; a small
    /// steady-state purge finishes in one slice. The connection and command (with its single bound cutoff
    /// parameter) are reused across the loop.
    /// </summary>
    private static async Task<int?> PurgeOneAsync(
        NpgsqlDataSource postgres,
        string tableName,
        string deleteSql,
        DateTime cutoff,
        ILogger? logger,
        CancellationToken cancellationToken,
        int batchSize = 1)
    {
        /* Accumulated OUTSIDE the try so the catch can report progress (#2386). Each statement
           autocommits, so a timeout on the fifth batch does not undo the first four — but the old
           catch returned null and threw the running total away, and the sweep's summary then said
           "0 row(s) deleted, 1 failed" for a purge that had removed 755k rows. That reads as total
           paralysis, which is what made this bug look worse than it was and hid that progress was
           being made one slice per sweep. */
        var deleted = 0;

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

            /* Deleting a whole expired slice from a COMPRESSED chunk (the drop_chunks-failed fallback)
               decompresses every affected segment, and TimescaleDB caps that at 100k tuples per DML
               transaction by default — a rail against accidental bulk decompression, which a retention
               purge deliberately is. Lift it for this connection only. On a store without the extension
               the qualified name is accepted as a placeholder GUC, so this is safe everywhere. */
            using (var lift = new NpgsqlCommand(
                "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
                connection) { CommandTimeout = DeleteTimeoutSeconds })
            {
                await lift.ExecuteNonQueryAsync(cancellationToken);
            }

            using var command = new NpgsqlCommand(deleteSql, connection) { CommandTimeout = DeleteTimeoutSeconds };
            command.Parameters.AddWithValue(cutoff);

            /* batchSize 1 for the TIME-SLICED statement: it has no row cap, so "fewer than the cap"
               degenerates to "deleted zero rows" — a slice that clears anything means older slices may
               remain. A ROW-capped caller passes its cap instead, which restores the drain loop's real
               contract (a full-cap batch means there may be more). */
            var batches = 0;
            var drained = await DrainBatchesAsync(
                async ct =>
                {
                    batches++;
                    var rows = await command.ExecuteNonQueryAsync(ct);
                    deleted += rows;
                    return rows;
                },
                batchSize,
                cancellationToken);

            /* A row-capped drain reports the two facts the sweep summary cannot carry, because both are
               per-table and the summary is fleet-wide.

               The CUTOFF, because it is not the retention knob and reading it as the knob is a live trap:
               ComputeDimensionCutoff subtracts the configured days PLUS a one-day margin for the hourly
               last_seen refresh, so counting rows older than the knob value overstates what is eligible by
               a full day of ingest — on this table that is hundreds of thousands of rows, which reads as a
               backlog retention is failing to clear when it is simply not due yet.

               And the BATCH COUNT, because rows-deleted alone cannot distinguish a drain from a peel. That
               is the #2386 failure mode exactly: a purge that removed one bounded slice and reported
               success looked identical in the log to one that cleared everything expired. One batch means
               the table was already inside its horizon; many means there was a backlog and it is gone. */
            if (batchSize > 1)
            {
                logger?.LogInformation(
                    "Retention purge drained {Rows} row(s) from {Table} in {Batches} batch(es) (cap {Cap}), cutoff {Cutoff:yyyy-MM-dd HH:mm}Z",
                    drained, tableName, batches, batchSize, cutoff);
            }

            return drained;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Failure-isolated per table — one stuck DELETE must not stop the sweep. Reports what DID
               land, because those batches are committed and saying otherwise sends an operator looking
               for a stall that is really a throughput limit. */
            logger?.LogWarning(
                "Retention purge failed for {Table} after removing {Rows} row(s): {Message}",
                tableName, deleted, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Repeatedly runs <paramref name="executeBatch"/> (one capped batched-DELETE execution, returning its
    /// rows-affected) and sums the total, stopping when a batch clears fewer than <paramref name="batchSize"/>
    /// rows — i.e. no expired rows remain and the table is drained. A full-cap batch means there may be more,
    /// so it goes again; an exact multiple of the cap terminates on the following empty batch. Pure over the
    /// injected executor so the loop-again + termination is unit-testable without a live store.
    /// </summary>
    internal static async Task<int> DrainBatchesAsync(
        Func<CancellationToken, Task<int>> executeBatch, int batchSize, CancellationToken cancellationToken)
    {
        var totalDeleted = 0;
        while (true)
        {
            var deleted = await executeBatch(cancellationToken);
            totalDeleted += deleted;

            if (deleted < batchSize)
            {
                break;
            }
        }

        return totalDeleted;
    }
}

/// <summary>
/// The outcome of one <see cref="DarlingRetention.PurgeAsync"/> sweep: how many tables were touched
/// (<paramref name="TablesPurged"/>) and the coarse activity count split into DELETE rows
/// (<paramref name="RowsDeleted"/>) and dropped Timescale chunks (<paramref name="ChunksDropped"/> —
/// drop_chunks doesn't report per-row counts). <see cref="TotalPurged"/> is the single headline number the
/// daily log and the on-demand <c>purge_now</c> result report.
/// </summary>
public readonly record struct PurgeSummary(int TablesPurged, int RowsDeleted, int ChunksDropped)
{
    /// <summary>Rows deleted plus whole chunks dropped — the coarse "how much did this purge remove" count.</summary>
    public int TotalPurged => RowsDeleted + ChunksDropped;
}
