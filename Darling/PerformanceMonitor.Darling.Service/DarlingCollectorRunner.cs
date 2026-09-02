/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Per-run outcome the worker logs (mirrors Lite's fetch/store phase split, #1180). <paramref name="Note"/>
/// annotates a run that SUCCEEDED but is worth explaining on its collection_log row — today only the
/// empty-enumeration case (see <see cref="EnumeratedCollectorDriver.EmptyEnumerationMessage"/>). It is the
/// Darling twin of Lite's <c>_lastCollectionNote</c>; null (the default) leaves the row's message column
/// null exactly as before.
/// </summary>
/// <param name="Fanout">
/// The per-database rollup for a run that fanned out, null for one that did not (#2472). Defaulted because
/// the great majority of construction sites here are the early returns of runs that never reached a fan-out
/// — a single query, an enumeration that yielded nothing — and null is their correct answer rather than a
/// value they forgot to supply. The one site that MUST set it is the success return.
/// </param>
public sealed record CollectorRunResult(int Rows, long SqlMs, long StorageMs, string? Note = null, FanoutCost? Fanout = null);

/// <summary>
/// Runs a shared collector definition against one monitored server and binary-COPYs the rows
/// into Postgres — the Darling counterpart of Lite's RemoteCollectorService.DefinitionRunner,
/// ported semantics-for-semantics: AppliesTo skip, host-store watermarks, the three execution
/// paths (per-database Azure connections; enumeration with the optional scalar probe; plain
/// single query with best-effort supplemental), cancellation-aware per-item catches, and the
/// separated SQL/storage phase timing. The definitions and the delta/ignored-wait/schedule
/// defaults are the shared brain; only the storage engine differs.
/// </summary>
public sealed class DarlingCollectorRunner
{
    private readonly NpgsqlDataSource _postgres;
    private readonly CollectorDeltaCalculator _deltas;
    private readonly ILogger? _logger;

    /* Feeds CollectorContext.CapturePlanXml on every cycle — the query_stats / query_store
       collectors capture the execution plan when true (darling.json "capturePlans", default true).
       Lite never sets the context flag; this is what makes Darling the plan-capturing SKU. Read
       through a provider (not a captured bool) so a control-plane store reload of config_service's
       capture_plans is honored on the NEXT cycle without reconstructing the runner. */
    private readonly Func<bool> _capturePlans;

    /* Feeds CollectorContext.CollectSchemaChangeEvents on every cycle — the default_trace_events
       collector drops its Object:Created/Altered/Deleted (schema DDL) slice when false (darling.json
       "collectSchemaChangeEvents", default true). Lite never sets the context flag, so it keeps
       collecting Object DDL. Read through a provider (not a captured bool) for symmetry with
       _capturePlans, so a future live reload is honored on the NEXT cycle without rebuilding. */
    private readonly Func<bool> _collectSchemaChanges;
    private readonly Func<bool> _compressPlanContent;

    /* Feeds CollectorContext.TextByteBudgetOverride on every cycle (#2164) — the query_store collector's
       per-database text budget in MB (config_service.query_store_text_budget_mb, V59). Provider-read for
       the same reason as the two above: a store reload takes effect on the NEXT cycle without rebuilding
       the runner. Lite has no equivalent and keeps the collector's compile-time constant. */
    private readonly Func<int> _textBudgetMb;

    /* Azure SQL DB logins without master access fall back to single-database mode, throttled per
       server so master isn't retried every cycle (#857 — mirrors Lite).

       Stores WHEN the verdict was formed, not just that it was: it expires after
       AzureMasterRecheckInterval, and OnServerReconnected drops it outright. Both escape hatches
       exist because this used to latch until the process was restarted, so a transient Azure error
       could permanently demote a healthy server to single-database collection (#1506). */
    private readonly ConcurrentDictionary<int, DateTime> _azureMasterInaccessibleSince = new();

    /// <summary>
    /// When a server's live query_store collection last failed a per-database item — the backfill
    /// worker's yield-to-live signal (#2111), read through <see cref="LastQueryStoreItemFailureUtc"/>
    /// and judged by <see cref="QueryStoreBackfillState.ShouldYieldToLive"/>. Stamped only for
    /// query_store (the one collector with a backfill worker to yield); in-memory on purpose — a
    /// service restart forgetting the stamps just means one backfill slice races one live cycle once.
    /// </summary>
    private readonly ConcurrentDictionary<int, DateTime> _lastQueryStoreItemFailureUtc = new();

    /// <summary>The #2111 yield-to-live read side: null when the server has never failed a live
    /// query_store item this process lifetime.</summary>
    public DateTime? LastQueryStoreItemFailureUtc(int serverId)
        => _lastQueryStoreItemFailureUtc.TryGetValue(serverId, out var failure) ? failure : null;

    /// <summary>
    /// Consecutive live query_store failures per DATABASE — the adaptive-shrink signal (#2111
    /// promoted from reserve): a member whose window keeps exceeding the command timeout gets a
    /// progressively narrower catch-up window (<see cref="QueryStoreBackfillState.AdaptiveSpan"/>)
    /// until one fits, and the skipped range rides the same hole records the clamp already writes.
    /// Reset on the database's next successful item; in-memory like the yield stamps and for the
    /// same reason — a restart forgetting the count costs one full-width attempt.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), int> _consecutiveQueryStoreItemFailures = new();

    private int ConsecutiveQueryStoreItemFailures(int serverId, string database)
        => _consecutiveQueryStoreItemFailures.TryGetValue((serverId, database), out var count) ? count : 0;

    private void OnQueryStoreItemFailed(int serverId, string database)
    {
        _lastQueryStoreItemFailureUtc[serverId] = DateTime.UtcNow;
        _consecutiveQueryStoreItemFailures.AddOrUpdate((serverId, database), 1, static (_, current) => current + 1);
    }

    private void OnQueryStoreItemSucceeded(int serverId, string database)
        => _consecutiveQueryStoreItemFailures.TryRemove((serverId, database), out _);

    /// <summary>
    /// Per-DATABASE observed plan-XML size estimate for the plan fetch's candidate sizing (#2312
    /// Finding 1): <see cref="QueryStorePlanXmlState.CandidatePlanCount(long?, long, bool, out bool)"/>
    /// was designed to learn each database's real average from its own shipped passes — the 11x fleet
    /// spread is the whole argument for it — and the call site passed null, so every pass on every
    /// database sized its decompression window from the 160KB first-contact seed. In-memory like the
    /// failure counters and for the same reason: a restart forgetting the estimate costs exactly one
    /// first-contact-sized pass.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), QueryStorePlanXmlState.PlanSizeEstimate> _observedPlanSize = new();

    /// <summary>
    /// Per-database ids the activity-driven fetch (#2312 Finding 2) still owes the store: probed missing in
    /// an earlier cycle but deferred by the candidate cap or the byte budget. Carried IN MEMORY because the
    /// probe's input is each cycle's batch references, and a plan referenced once — its delta rows shipped,
    /// never executed again — would otherwise never re-enter the probe and never get its XML. The honest
    /// costs of in-memory: a restart forgets the debt, and the ids re-enter only if their plans execute
    /// again — for the literal-churn plans that dominate deferrals, XML nobody can reach from a fact is the
    /// cheap thing to lose. Bounded: ids are 8 bytes and a first-contact backlog is one catalog's worth.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), long[]> _planFetchCarryover = new();

    /// <summary>Text twin of <see cref="_planFetchCarryover"/> — same deferral contract, keyed by query_id.</summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), long[]> _textFetchCarryover = new();

    /// <summary>
    /// Consecutive-failure count for the TEXT fetch (#2776), the backoff input its
    /// <see cref="QueryStorePlanXmlState.NarrowForFailures"/> call reads.
    ///
    /// <para>A dictionary of its own rather than a field on a carried estimate, because the text fetch
    /// deliberately has no estimator to hang it off — <c>DATALENGTH</c> on text is cheap, so there is no
    /// decompression to bound and no learned average to carry. The plan side keeps its counter inside
    /// <see cref="QueryStorePlanXmlState.PlanSizeEstimate"/> for exactly the opposite reason: it already has
    /// a record to live in.</para>
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), int> _textFetchFailures = new();

    /// <summary>
    /// Ids per IN-list statement for the plan fetch. Small on purpose: each id in the list is a plan the
    /// server will DECOMPRESS to run the budget's running total, so the statement size is never the real
    /// bound — the candidate cap from <see cref="QueryStorePlanXmlState.CandidatePlanCount"/> is — and 400
    /// keeps the SQL text itself a few KB.
    /// </summary>
    private const int PlanFetchIdsPerStatement = 400;

    /// <summary>Ids per IN-list statement for the text fetch. Larger than the plan side because
    /// DATALENGTH(query_sql_text) is cheap — no decompression — so the only cost is statement size.</summary>
    private const int TextFetchIdsPerStatement = 1000;

    private static readonly TimeSpan AzureMasterRecheckInterval = TimeSpan.FromMinutes(15);

    public const int CommandTimeoutSeconds = 60;

    /// <param name="capturePlans">
    /// Live provider for the plan-capture flag; null defaults to always-on (Darling's SKU default).
    /// The worker passes <c>() =&gt; config.CapturePlans</c> so a store reload takes effect next cycle;
    /// tests pass a constant lambda.
    /// </param>
    /// <param name="collectSchemaChanges">
    /// Live provider for the schema-change (Object DDL) collection flag; null defaults to on (today's
    /// behavior). The worker passes <c>() =&gt; config.CollectSchemaChangeEvents</c> so a noisy/benchmark box
    /// can suppress the default-trace Object:Created/Deleted flood; tests pass a constant lambda.
    /// </param>
    public DarlingCollectorRunner(NpgsqlDataSource postgres, CollectorDeltaCalculator deltas, ILogger? logger = null, Func<bool>? capturePlans = null, Func<bool>? collectSchemaChanges = null, Func<int>? textBudgetMb = null, Func<bool>? compressPlanContent = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _deltas = deltas ?? throw new ArgumentNullException(nameof(deltas));
        _logger = logger;
        _capturePlans = capturePlans ?? (() => true);
        /* Null provider = keep the collector's own compile-time budget (what Lite and every test does). */
        _textBudgetMb = textBudgetMb ?? (() => 0);
        _collectSchemaChanges = collectSchemaChanges ?? (() => true);
        /* #2171: plan_xml_compression provider — true = gzip into query_plan_gz (the default),
           false = 'none': plain text into query_plan_xml so direct-SQL consumers read it bare.
           The worker passes () => config.PlanXmlCompression == "gzip"; tests pass a constant. */
        _compressPlanContent = compressPlanContent ?? (() => true);
    }

    /* One ingestor for the process, so the resume marker survives between cycles - it is per-file and
       in-memory by design (#2538), and a fresh instance every cycle would silently re-read the same tail
       forever while looking like it was making progress. */
    private RdsPlanIngestor? _rdsPlans;

    /* A SEPARATE ingestor from _rdsPlans, deliberately: RdsLogSource's resume marker is consumed per
       (instance, file) on every read, so sharing one instance between plan capture and deadlock ingestion
       would starve whichever of the two runs second in a cycle. Each needs its own marker to independently
       read "the same bounded tail of the same file" - matching the pg_read_file route, where the two
       collectors' SQL queries already read that tail independently rather than sharing a cursor. */
    private RdsDeadlockIngestor? _rdsDeadlocks;

    /* Own ingestor for the same "one instance per transport" reason as the two above — see
       RdsCpuIngestor's own doc comment for why it does NOT need the marker-survival treatment
       _rdsPlans/_rdsDeadlocks get: its resume watermark lives in the store, not in this field. */
    private RdsCpuIngestor? _rdsCpu;

    /// <summary>
    /// Plan capture for Aurora and RDS, where the log is only reachable through the AWS API (#2538).
    ///
    /// <para>Reported as a normal <see cref="CollectorRunResult"/> so the cycle accounts for it exactly like
    /// a collector: same <c>collection_log</c> row, same rows-collected number, same health surface. The
    /// TRANSPORT differs; the bookkeeping should not, or an operator would have to know which route a
    /// target used before they could read its collection history.</para>
    /// </summary>
    public async Task<CollectorRunResult> IngestRdsPlansAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        _rdsPlans ??= new RdsPlanIngestor(_postgres, logger: _logger);

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        var started = Stopwatch.GetTimestamp();

        var rows = await _rdsPlans.IngestAsync(
            server.ServerId, server.StorageName, host, cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        /* Counted as STORAGE time rather than SQL time: no query ran against the monitored server, and
           filing an HTTPS round trip under sql_duration_ms would make one target's numbers mean something
           different from every other target's. */
        return new CollectorRunResult(rows, 0, elapsedMs,
            rows == 0 ? "no new auto_explain plans in the RDS log window" : null);
    }

    /// <summary>
    /// Deadlock capture for Aurora and RDS, where the log is only reachable through the AWS API — the
    /// dispatch this collector was missing (it dispatched unconditionally to the <c>pg_read_file</c> route
    /// even against a managed target, which has no filesystem to read; a 100% failure across the whole
    /// Aurora fleet, since no grant fixes what does not exist there). Mirrors
    /// <see cref="IngestRdsPlansAsync"/> exactly, including the STORAGE-time accounting rationale.
    /// </summary>
    public async Task<CollectorRunResult> IngestRdsDeadlocksAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        _rdsDeadlocks ??= new RdsDeadlockIngestor(_postgres, logger: _logger);

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        var started = Stopwatch.GetTimestamp();

        var rows = await _rdsDeadlocks.IngestAsync(
            server.ServerId, server.StorageName, host, cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        return new CollectorRunResult(rows, 0, elapsedMs,
            rows == 0 ? "no new deadlocks in the RDS log window" : null);
    }

    /// <summary>
    /// Instance CPU for Aurora and RDS Postgres, read from the AWS Performance Insights API (#2719) — the
    /// third "reach the target a different way" collector alongside <see cref="IngestRdsPlansAsync"/> and
    /// <see cref="IngestRdsDeadlocksAsync"/>, and unlike either of those, the ONLY route: PostgreSQL exposes
    /// no instance-level CPU signal at all, so there is no <c>pg_read_file</c>-style fallback for a
    /// self-hosted target to fall back to (see <see cref="PgCpuUtilizationCollector"/>'s doc comment).
    /// Reported as a normal <see cref="CollectorRunResult"/> for the same reason the other two are.
    /// </summary>
    public async Task<CollectorRunResult> IngestPgCpuAsync(
        ServerRuntime server, CancellationToken cancellationToken)
    {
        _rdsCpu ??= new RdsCpuIngestor(_postgres, logger: _logger);

        var host = new NpgsqlConnectionStringBuilder(server.ConnectionString).Host ?? string.Empty;

        var started = Stopwatch.GetTimestamp();

        var rows = await _rdsCpu.IngestAsync(
            server.ServerId, server.StorageName, host, cancellationToken);

        var elapsedMs = (long)Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        /* Counted as STORAGE time rather than SQL time, matching the other two RDS-API ingestors: no query
           ran against the monitored server, and filing an HTTPS round trip under sql_duration_ms would make
           one target's numbers mean something different from every other target's. */
        return new CollectorRunResult(rows, 0, elapsedMs,
            rows == 0 ? "no new Performance Insights CPU samples this cycle" : null);
    }

    public async Task<CollectorRunResult> RunAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        var collectionTime = DateTime.UtcNow;

        /* Some collectors don't exist on some targets (e.g. ring buffers on Azure SQL DB) —
           skip the cycle entirely, matching Lite. CollectorCatalog.AppliesTo composes the
           engine-dialect check over the definition's own gate, so a T-SQL definition can never be
           dispatched at a non-SQL-Server target. */
        if (!CollectorCatalog.AppliesTo(definition, server.Target))
        {
            return new CollectorRunResult(0, 0, 0);
        }

        /* Watermark = the newest already-collected value of the definition's time column,
           read from Postgres (Lite reads DuckDB here). */
        DateTime? watermark = definition.WatermarkColumn is null
            ? null
            : await GetLastCollectedTimeAsync(server.ServerId, definition.TargetTable, definition.WatermarkColumn, cancellationToken);

        /* Numeric (bigint) watermark = the newest already-collected value of the definition's monotonic
           identity column (job_history's instance_id), read from Postgres — the bigint twin of the timestamp
           watermark above. Null for every collector that declares no numeric watermark (the common case),
           so no extra query runs for them. */
        long? numericWatermark = definition.NumericWatermarkColumn is null
            ? null
            : await GetLastCollectedInstanceIdAsync(server.ServerId, definition.TargetTable, definition.NumericWatermarkColumn, cancellationToken);

        /* Only when the watermark came back null: tell a TRUE first run from a store merely emptied by
           retention, so default_trace_events uses a bounded window instead of re-scanning all .trc history
           (CollectorContext.HasCollectedBefore). Skipped in the common (non-null watermark) path. */
        bool hasCollectedBefore = definition.WatermarkColumn is not null
            && watermark is null
            && await HasPriorCollectorSuccessAsync(server.ServerId, definition.Name, cancellationToken);

        /* Per-server state the definition declared keys for — the watermark's sibling for facts no MAX()
           over the collected rows can produce (default_trace_events' last-seen trace FILE, #1962). No
           declared keys (every other collector) means no query runs. Mirrors Lite. */
        var collectorState = definition.StateKeys.Count == 0
            ? null
            : await GetCollectorStateAsync(server.ServerId, definition.Name, cancellationToken);

        /* #2188: retire the per-database state rows of databases that no longer exist, BEFORE the load
           below, so this cycle also works from a cleaned dictionary rather than one carrying names the
           server dropped. Runs for query_store regardless of plan capture, because the backfill worker's
           per-database keys orphan the same way and are pruned in the same pass.

           Gated on the SAME AppliesTo that decides whether database_states is collected at all, rather than
           leaning on the statement's own empty-snapshot guard to no-op: on Azure SQL DB there is no snapshot
           by design, so this would otherwise run three guaranteed-no-op deletes on every cycle forever and
           #2191's boundary would be emergent rather than stated. */
        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            && DatabaseStateCollector.Instance.AppliesTo(server.Target))
        {
            await PruneOrphanedQueryStoreDatabaseStateAsync(server.ServerId, cancellationToken);
        }
        else if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                 && server.Target.IsAzureSqlDb)
        {
            /* #2191's boundary, now crossable. Azure SQL DB has no database_states snapshot by design, which
               is why this was a stated no-op — but after #2220 a registration that names a database sweeps
               only that database, so its one legitimate key is the connection string's own catalog. No
               master read, nothing filtered, nothing that can go stale. A registration naming NO database is
               still skipped: it is a registration of the logical SERVER, and a single-name prune there would
               delete every live watermark it legitimately has. */
            var ownDatabase = new SqlConnectionStringBuilder(server.ConnectionString).InitialCatalog;
            if (AzureSweepScope.OwnDatabaseOrEmpty(ownDatabase).Count > 0)
            {
                await PruneForeignQueryStoreDatabaseStateAsync(server.ServerId, ownDatabase, cancellationToken);
            }
        }

        /* #2312: the plan and text watermark reads that used to merge in here (the #2164/#2150 host-owned
           state families) are GONE — the fetches are activity-driven against the store's own map/text
           tables now, so there is no persisted resume point to load. V77 deleted the orphaned rows. */

        /* #2312: the open-interval refresh stamps, merged into the flat State. Read unconditionally for
           query_store (the skip applies regardless of plan capture), and merged rather than replacing so a
           store predating this state keeps working: absent keys read as "include the open interval",
           which is the conservative behavior. */
        if (string.Equals(definition.Name, "query_store", StringComparison.Ordinal))
        {
            var openIntervalState = await GetCollectorStateAsync(
                server.ServerId, QueryStoreOpenIntervalState.StateCollectorName, cancellationToken);

            if (openIntervalState is { Count: > 0 })
            {
                var merged = new Dictionary<string, string>(StringComparer.Ordinal);
                if (collectorState is not null)
                {
                    foreach (var entry in collectorState)
                    {
                        merged[entry.Key] = entry.Value;
                    }
                }

                foreach (var entry in openIntervalState)
                {
                    merged[entry.Key] = entry.Value;
                }

                collectorState = merged;
            }
        }

        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = _deltas,
            Target = server.Target,
            Watermark = watermark,
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
            State = collectorState ?? CollectorContext.NoState,
            IgnoredWaitTypes = IgnoredWaitDefaults.All,
            ExcludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>(),
            PerfmonCounterOverride = null,
            CapturePlanXml = _capturePlans(),
            /* #2150: ON. query_sql_text is no longer carried on every runtime-stats row — it is fetched once
               per query_id into collect.query_store_text (FetchAndStoreQueryTextAsync, below) and resolved
               back by the readers, all six of which now prefer that table and fall back to the fact row's
               own column.
               Why this had to be one change and not two: text is immutable per query_id, but the runtime
               rows are re-collected every cycle, so the inline column re-shipped the same statement text on
               every snapshot — that is what made query_store_stats the largest table in the store. Flipping
               nulls the inline column, so a reader that had not been converted would have shown blank text
               for new rows while looking perfectly healthy. Rows collected BEFORE this flip still carry
               their text inline, which is why the readers keep the fallback instead of switching over. */
            FetchQueryTextSeparately = true,
            /* #2164: 0 from the default provider means "no override" — the collector keeps its own
               constant. Converted MB -> bytes here so the store knob stays operator-friendly. */
            TextByteBudgetOverride = _textBudgetMb() > 0 ? _textBudgetMb() * 1024 * 1024 : null,
            CollectSchemaChangeEvents = _collectSchemaChanges(),
        };

        /* Two accumulators, not one contiguous read-then-write pair: the enumeration and Azure paths now
           FLUSH each database's rows before reading the next (#1556), so SQL and storage slices interleave.
           Wall-clock (sqlMs + storageMs) and rows_collected totals stay coherent — collection_log is
           unchanged; only the split is now a sum of interleaved slices. */
        long sqlMs = 0;
        long storageMs = 0;
        var rowsWritten = 0;

        /* The per-database rollup (#2472). Both fan-out shapes feed it — the enumeration driver's
           onItemComplete hook and the Azure per-database connection loop — so a collector that fans out on
           one branch on Azure and the other on-prem reports the same shape either way. A run that never
           fans out never calls Observe and the accumulator stays empty, which is how the columns end up
           NULL on ~98 percent of collection_log rows. */
        var fanout = new FanoutCostAccumulator();

        /* The collection_log note for this run (#1837) — null on every ordinary path. Only the enumeration
           branch sets it, but it is declared here so the note reaches the single success return below when
           items WERE found and merely some of their probes failed. Lite's twin is _lastCollectionNote. */
        string? collectionNote = null;

        /* The engine's provider, resolved ONCE for both branches. It used to be resolved only inside the
           per-database branch, and the branch below opened a hardcoded SqlConnection — so every collector
           that does NOT fan out per database was handed a SQL Server connection whatever the target was.
           Six of the seven PostgreSQL collectors take that path (only pg_autovacuum_stats fans out), and
           SqlClient rejects Npgsql's keywords while parsing the connection string, before any query runs:
           "Keyword not supported: 'host'". Worse, an ArgumentException is neither SqlException nor
           PostgresException, so it missed BOTH classification arms in DarlingWorker and recorded a raw
           ERROR every sweep forever — including for all three Tier 0 outage predictors. */
        var targetProvider = TargetProviders.For(server.Target);

        if (definition.RunsPerDatabase(context.Target))
        {
            /* Azure SQL DB scopes some DMVs to the connected database — run the query once per
               database, skipping (and debug-logging) databases that error, matching Lite.

               Definitions with a database-scoped watermark (the XE ring-buffer collectors, whose
               per-database sessions dispatch independently) get the query rebuilt per database
               against that database's own newest already-collected value — the single server-wide
               watermark would let one busy database's newer event silence another database's older
               event still sitting in its ring buffer (#1535). Everything else keeps the
               build-once plan.

               Honor CommandTimeoutSecondsOverride here (#1556): this path previously passed the constant
               60s cap where Lite's twin already honored the override, a latent bug — index_object_stats
               needs 300s per database on Azure, so on a large Azure database its per-database read would
               have timed out at 60s. */
            var plan = definition.PerDatabaseWatermarkColumn is null || definition.WatermarkColumn is null
                ? definition.BuildQuery(context)
                : null;
            var perDbTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;
            var perDbProvider = targetProvider;

            /* Two enumeration paths because the FAILURE semantics genuinely differ, not the SQL. On
               Azure SQL DB an inaccessible master has a real fallback (collect the one connected
               database) and a re-probe throttle to stop hammering it; on PostgreSQL a login that
               cannot read pg_database cannot monitor the server at all, so inventing a fallback would
               turn a permissions problem into a silent one-database collection. */
            var databases = server.Target.Engine == CollectorTargetEngine.PostgreSql
                ? await GetPostgresDatabaseListAsync(server, cancellationToken)
                : await GetAzureDatabaseListAsync(server, cancellationToken);

            var attempted = 0;
            var failed = 0;
            Exception? firstFailure = null;

            /* #2623: the names, not just the count. A partial loss composes a note naming which databases
               were skipped, because the count alone does not tell an operator whether the ONE database
               that matters is in the collected set or the skipped one. */
            var failedDatabases = new List<string>();

            /* #1875: this path reads the trailing probe-failure set once PER DATABASE, so the note and the
               log cap are decided for the cycle after the loop rather than inside it — see
               CycleProbeFailures for why neither generalizes from the single-read plain path. */
            var cycleProbeFailures = new CycleProbeFailures();

            /* One pooled store connection for the whole body; one binary COPY per database on it
               (completing an importer commits that database — commit-1..N-1 semantics on abort). */
            await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

            foreach (var databaseName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();
                attempted++;

                /* #2150: THE path the field report is on — Azure SQL DB collects query_store per database
                   here, not through the enumerated driver, so the wall-clock ceiling has to be applied on
                   both. Null for every collector that declares none, in which case dbToken IS
                   cancellationToken and this loop is byte-for-byte what it was. */
                using var dbBudget = EnumeratedCollectorDriver.StartItemBudget(
                    definition.PerItemWallClockBudget, cancellationToken);
                var dbToken = dbBudget?.Token ?? cancellationToken;

                /* #2312: this database's open-interval stamp, staged at decision time and landed only
                   after its read and flush succeed — per iteration, so a fault cannot leak a stamp
                   into a sibling database's landing. */
                string? stagedOpenIntervalStamp = null;
                try
                {
                    /* The authoritative database_name for XE rows read on this path — see
                       CollectorContext.CurrentDatabaseName. */
                    context.CurrentDatabaseName = databaseName;

                    var dbPlan = plan;
                    if (dbPlan is null)
                    {
                        /* Null (no rows for this database yet) falls back to the definition's
                           documented first-run window, per database. No clamp is applied HERE because
                           this branch also serves the XE ring-buffer collectors (deadlocks / BPR),
                           where flooring a stale watermark would WRONGLY truncate legitimate catch-up
                           — those sources roll past the catch-up horizon on their own. query_store also
                           branch on Azure SQL DB (#1836) and does need the bound, so it applies
                           WatermarkPolicy.ClampCatchup inside its own cutoff computation: the clamp
                           travels with the collector that needs it instead of with the path. */
                    /* dbToken throughout this branch (#2150 review catch): the interface contract says the
                       budget covers "the watermark refresh, the command, and the whole drain", and the
                       enumerated path's perItemWatermark delegate already honours that. Leaving these three
                       store round-trips on cancellationToken made THIS loop — the one the field report is
                       actually on — the only place the promise was not kept, and a store that has stopped
                       answering is exactly the stall the budget exists to bound. Safe for the hole records
                       specifically: a budget expiry abandons the whole pass, so the watermark does not
                       advance, the clamp is re-derived next cycle, and the hole is re-recorded (merged wider
                       with any already pending) rather than lost. */
                        /* #2344: same bound as the enumerated arm. Safe here for the same reason and
                           by a different route — this branch does not clamp itself, but query_store's own
                           BuildCutoffParameters does (the #1836 double-clamp the policy documents), so the
                           value this read returns is clamped before anything uses it. */
                        var azureReadFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                            ? WatermarkPolicy.ReadFloor(collectionTime)
                            : null;
                        context.Watermark = await GetLastCollectedTimeForDatabaseAsync(
                            server.ServerId, definition.TargetTable, definition.WatermarkColumn!,
                            definition.PerDatabaseWatermarkColumn!, databaseName, dbToken, azureReadFloor);

                        /* #2111 adaptive shrink, Azure arm — tighten BEFORE BuildQuery: the
                           definition's own clamp only floors OLDER watermarks, so a tighter one
                           passes through untouched. The skipped range is recorded as a hole here
                           (wider than the clamp's own record would be, so the block below firing
                           too would merge, not conflict). */
                        var azureFailures = ConsecutiveQueryStoreItemFailures(server.ServerId, databaseName);
                        if (azureFailures > 0
                            && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            var adaptiveSpan = QueryStoreBackfillState.AdaptiveSpan(WatermarkPolicy.MaxCatchup, azureFailures);
                            var tighterFloor = collectionTime - adaptiveSpan;
                            if (context.Watermark is DateTime azureRaw)
                            {
                                if (azureRaw < tighterFloor)
                                {
                                    _logger?.LogWarning(
                                        "query_store on '{Server}' database [{Database}] adaptive catch-up shrink: {Failures} consecutive failed cycles — window narrowed to {Minutes:F0}m; the skipped range rides the backfill hole.",
                                        server.Config.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                    await RecordQueryStoreBackfillHoleAsync(server.ServerId, databaseName, azureRaw, tighterFloor, dbToken);
                                    context.Watermark = tighterFloor;
                                }
                            }
                            else
                            {
                                /* Never-succeeded database: tighten the first-run fallback too (the
                                   review catch); no hole — pre-watermark history is the tail's job. */
                                _logger?.LogWarning(
                                    "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                                    server.Config.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                context.Watermark = tighterFloor;
                            }
                        }

                        /* #2312, Azure arm: same per-database open-interval decision as the enumerated
                           delegate, BEFORE BuildQuery bakes the predicate. Staged into the local, landed
                           only in the post-flush success block below — a per-database fault this loop
                           tolerates must re-include next cycle, not spend the refresh window. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            var includeOpen = QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
                                context.State, databaseName, collectionTime);
                            context.IncludeOpenInterval = includeOpen;
                            if (includeOpen)
                            {
                                stagedOpenIntervalStamp = QueryStoreOpenIntervalState.Format(collectionTime);
                            }
                        }

                        dbPlan = definition.BuildQuery(context);

                        /* The definition clamped its own cutoff — surface the same WARNING the
                           enumeration path emits, so the bounded history hole stays LOGGED and does
                           not become the one silent hole in a policy whose whole premise is that it
                           is visible. Mirrors Lite. */
                        if (context.CatchupClampApplied)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                                definition.Name, server.Config.DisplayName, databaseName, WatermarkPolicy.MaxCatchup.TotalHours, context.Watermark);

                            /* #2058 (the Azure arm of #2022's hole recording): context.Watermark still
                               holds the RAW value here — the definition clamped only its own cutoff
                               parameter — so the hole is (raw, re-derived clamp floor), same merge
                               semantics as the enumerated site. Only query_store both clamps AND has a
                               backfill worker; the name guard keeps the XE collectors that share this
                               branch from growing backfill state they have no worker for. */
                            if (context.Watermark.HasValue
                                && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                && WatermarkPolicy.ClampCatchup(context.Watermark, collectionTime) is DateTime azureClampedFloor)
                            {
                                await RecordQueryStoreBackfillHoleAsync(
                                    server.ServerId, databaseName, context.Watermark.Value, azureClampedFloor, dbToken);
                            }
                        }
                    }

                    var sqlSlice = Stopwatch.StartNew();
                    List<TRow> batch;
                    /* dbToken, not cancellationToken (#2150): connect, execute and drain are the phases the
                       budget bounds. The FLUSH below deliberately stays on cancellationToken — abandoning a
                       write already in flight would trade a slow cycle for a partially-written one. */
                    using (var dbConnection = await OpenDatabaseConnectionAsync(perDbProvider, server, databaseName, dbToken))
                    using (var dbCommand = CreateCollectorCommand(perDbProvider, dbPlan, dbConnection, perDbTimeout))
                    using (var dbReader = await dbCommand.ExecuteReaderAsync(dbToken))
                    {
                        batch = await definition.ReadAsync(dbReader, context, dbToken);

                        /* #1875: the payload path's probe-failure contract, on the path that used to
                           ignore it. blocked_process_report is the declaring collector that also runs per
                           database (Azure SQL DB, #1535), so before this its batch produced the trailing
                           set and the loop simply never advanced the reader to it — the rows were built
                           and dropped. Read HERE, still inside the reader and inside the per-database
                           try, so a diagnostics fault stays a one-database skip like any other. */
                        if (definition.EmitsProbeFailures)
                        {
                            cycleProbeFailures.Add(
                                await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(dbReader, dbToken));
                        }
                    }
                    /* Read ONCE. The stopwatch is still running, so a second read a few statements later
                       returns a larger number, and the per-item total would then exceed the blended total
                       it is a ratio against — a dominance a hair above the truth, on every Azure run
                       (#2472). Small, and wrong in the direction that matters. */
                    var dbSqlMs = sqlSlice.ElapsedMilliseconds;
                    sqlMs += dbSqlMs;

                    /* Flush this database before reading the next — peak memory is one database's rows. */
                    long dbStorageMs = 0;
                    if (batch.Count > 0)
                    {
                        var storageSlice = Stopwatch.StartNew();
                        rowsWritten += await WriteBatchAsync(pgConnection, definition, batch, server, collectionTime, context, cancellationToken);
                        dbStorageMs = storageSlice.ElapsedMilliseconds;
                        storageMs += dbStorageMs;
                    }

                    /* #2472: this database's slice, counted even when its batch was empty — an empty batch
                       still paid for its read, and that read is in the blended total the rollup is a ratio
                       against. Observed here rather than beside the log line below for the same reason the
                       completion hook fires after the flush: both slices are only known once the write is
                       done. */
                    fanout.Observe(databaseName, dbSqlMs + dbStorageMs);

                    /* Same per-database bounded-cycle WARNING the enumeration path emits from
                       onItemComplete, mirroring Lite. Reachable here since #1836 put query_store — the
                       only collector that declares either bound — on this branch for Azure SQL DB;
                       without it a database whose cycle was cut at the bound would look like a clean
                       collection. Since #1960 a bound DEFERS the backlog to the next cycle's resume
                       from the shipped boundary rather than dropping it — this log is how a long
                       catch-up stays observable. Read after the flush, as on the other path: the
                       context signal stays this database's until the next read resets it. */
                    var capHit = definition.PerItemRowCountWarnThreshold is int cap && batch.Count >= cap;
                    if (capHit || context.PerItemTextBudgetExceeded)
                    {
                        _logger?.LogWarning(
                            "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                            definition.Name, server.Config.DisplayName, databaseName,
                            capHit ? $"row cap {definition.PerItemRowCountWarnThreshold}" : "text byte budget",
                            context.PerItemTextBytesShipped / (1024.0 * 1024.0),
                            context.PerItemShippedBoundary?.ToString("o") ?? "n/a");
                    }

                    /* #2111: success resets the adaptive-shrink count on the Azure arm too. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemSucceeded(server.ServerId, databaseName);

                        /* #2312: read and flush both landed — the staged open-interval stamp may too. */
                        if (stagedOpenIntervalStamp is not null)
                        {
                            context.PendingState[QueryStoreOpenIntervalState.KeyFor(databaseName)] = stagedOpenIntervalStamp;
                        }
                    }
                }
                catch (OutOfMemoryException)
                {
                    /* AHEAD of the budget arm, because ItemBudgetExpired classifies on the TOKENS and never
                       looks at the exception type (review catch). Without this, an OOM thrown while the
                       budget's timer had already fired — materializing a large batch, or inside the store
                       write — would be caught by that arm and logged as a routine per-database timeout,
                       silently breaking the invariant the generic catch below states outright. The shared
                       EnumeratedCollectorDriver already orders it this way; these two loops did not. */
                    throw;
                }
                catch (Exception ex) when (EnumeratedCollectorDriver.ItemBudgetExpired(dbBudget, cancellationToken))
                {
                    /* #2150: this database ran out of wall clock. Counted as a per-database failure so the
                       cycle moves on — one database must not be able to starve the rest, which is the harm
                       the field report describes. Ahead of the generic catch because a cancelled command
                       does not reliably arrive as an OperationCanceledException, so that filter cannot be
                       trusted to claim it; the token check is what keeps a real shutdown out of this arm.
                       The provider's own cancellation exception is dropped in favour of the budget message:
                       it describes HOW the read was stopped, not why. */
                    _ = ex;
                    var budgetFailure = EnumeratedCollectorDriver.ItemBudgetException(
                        definition.PerItemWallClockBudget!.Value);
                    failed++;
                    failedDatabases.Add(databaseName);
                    firstFailure ??= budgetFailure;

                    /* Same #2111 stamp the generic arm makes, and it MATTERS more here: this is what turns
                       the bound from a cut that repeats forever into one that converges. The consecutive
                       count narrows this database's next catch-up window, so a database that cannot finish
                       in the budget keeps halving until it can. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(server.ServerId, databaseName);
                    }

                    /* WARNING, not Debug, unlike the routine per-database skip beside it: an offline
                       database is ordinary and this is a collector that could not finish its work. */
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' database [{Database}] {Message}",
                        definition.Name, server.Config.DisplayName, databaseName, budgetFailure.Message);
                }
                catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
                {
                    /* OOM is filtered OUT of this per-database skip and propagates: it is fatal, not a
                       routine one-database miss. */
                    failed++;
                    failedDatabases.Add(databaseName);
                    firstFailure ??= ex;

                    /* #2111: the yield-to-live stamp + adaptive-shrink count for the Azure SQL DB
                       arm — query_store reaches THIS per-database loop there, not the enumeration
                       path's onItemError, and without the stamp the backfill worker would never
                       yield on an Azure target (the review catch on #2112). Same query_store-only
                       guard as the hole recording above. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(server.ServerId, databaseName);
                    }

                    _logger?.LogDebug("Skipping database '{Database}' for {Collector}: {Error}", databaseName, definition.Name, ex.Message);
                }
            }

            context.CurrentDatabaseName = null;

            /* #1875: ONE note for the cycle and ONE capped log burst, composed from every database's
               failures together. Assigned unconditionally — a cycle where nothing failed composes null,
               which is exactly what this path carried before. */
            collectionNote = EnumeratedCollectorDriver.MergeNotes(
                cycleProbeFailures.Note,
                EnumeratedCollectorDriver.BuildPartialFailureNote(
                    failed, attempted, failedDatabases, firstFailure?.Message));
            LogEnumerationProbeFailures(definition, server, cycleProbeFailures.Failures);

            /* One database failing is routine (offline, mid-restore, a permissions oddity) and stays a
               debug-logged skip. EVERY database failing is a systemic fault — before this check the run
               recorded SUCCESS with zero rows, which on the XE collectors also made the SESSION_MISSING
               classification (RunXeTolerantAsync → the Capture Down self-alert) unreachable on Azure.
               Rethrow the first failure so RunOneAsync classifies it (SESSION_MISSING / PERMISSIONS /
               ERROR) instead. Mirrors Lite's definition runner. */
            if (attempted > 0 && failed == attempted && firstFailure is not null)
            {
                _logger?.LogWarning("{Collector} failed in all {Count} database(s) on '{Server}'; surfacing the first failure",
                    definition.Name, attempted, server.Config.DisplayName);
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();
            }
        }
        else
        {
            using var targetConnection = CreateTargetConnection(server);
            await targetConnection.OpenAsync(cancellationToken);

            var enumerationPlan = definition.BuildEnumerationQuery(context);
            if (enumerationPlan is not null)
            {
                /* Enumeration shape (the [db].sys.sp_executesql idiom): list items first, then
                   run one query per item ON THE SAME CONNECTION; an item that fails is skipped
                   with a warning, matching Lite. */
                var listSlice = Stopwatch.StartNew();
                EnumerationOutcome enumeration;
                using (var enumerationCommand = CreateCollectorCommand(targetProvider, enumerationPlan, targetConnection, CommandTimeoutSeconds))
                using (var enumerationReader = await enumerationCommand.ExecuteReaderAsync(cancellationToken))
                {
                    /* Shared read (#1837): the item list, then the OPTIONAL second result set of items the
                       enumeration could not probe. Both hosts route through it so the item read, the
                       failure read, and the note wording cannot drift. */
                    enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(enumerationReader, cancellationToken);
                }
                sqlMs += listSlice.ElapsedMilliseconds;

                var items = enumeration.Items;
                collectionNote = enumeration.Note;
                LogEnumerationProbeFailures(definition, server, enumeration.ProbeFailures);

                if (items.Count == 0)
                {
                    /* Nothing failed outright, so this stays SUCCESS/0 rows — but the note (the
                       empty-enumeration breadcrumb, the probe-failure summary, or both) rides onto the
                       collection_log row so it is distinguishable from a healthy collector whose databases
                       were simply quiet (#1837). Mirrors Lite's _lastCollectionNote. */
                    return new CollectorRunResult(0, sqlMs, 0, enumeration.Note);
                }

                /* Optional quick scalar probe (query_store's live PRODUCTVERSION check) —
                   best-effort on a 10-second budget; failure leaves the documented default. */
                var probeSlice = Stopwatch.StartNew();
                var probePlan = definition.BuildEnumerationProbe(context);
                if (probePlan is not null)
                {
                    try
                    {
                        using var probeCommand = CreateCollectorCommand(targetProvider, probePlan, targetConnection, 10);
                        var probeResult = await probeCommand.ExecuteScalarAsync(cancellationToken);
                        if (probeResult is not null && probeResult != DBNull.Value)
                        {
                            context.EnumerationProbeResult = probeResult;
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger?.LogDebug("Enumeration probe for {Collector} failed; using defaults: {Error}",
                            definition.Name, ex.Message);
                    }
                }
                sqlMs += probeSlice.ElapsedMilliseconds;

                var itemTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;

                /* One pooled store connection for the whole body; the driver writes one binary COPY per
                   database on it, flushing each before reading the next. */
                await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

                /* #2312: open-interval stamps STAGED at decision time (perItemWatermark, below), landed
                   into PendingState only from onItemComplete — which the driver invokes solely after the
                   item's read AND flush succeeded. Staging them straight into PendingState would let a
                   per-item fault the driver tolerates still "spend" the 15-minute refresh window for a
                   cycle that captured nothing (the review catch): PendingState flushes as long as the
                   whole run survives. Keyed per item, so one database's decision cannot land on another. */
                var stagedOpenIntervalStamps = new Dictionary<string, string>(StringComparer.Ordinal);

                var driverResult = await EnumeratedCollectorDriver.RunAsync<TRow>(
                    items,
                    /* Per-database watermark refresh + the catch-up clamp, computed INSIDE the loop —
                       this is the per-item cutoff site the plan's LOUD FLAG requires the clamp to live at.
                       Only query_store (the sole enumeration collector with a per-database timestamp
                       watermark) reaches this; the two snapshot collectors are watermark-less. */
                    perItemWatermark: definition.PerDatabaseWatermarkColumn is null || definition.WatermarkColumn is null
                        ? null
                        : async (item, ct) =>
                        {
                            /* #2164: the driver's per-item stopwatch starts BEFORE this delegate, so the
                               watermark refresh — a STORE read, plus a store write on the clamp path below —
                               would otherwise be silently counted as row-streaming time. Measured here so
                               DrainMsFrom can subtract it; the whole point of the split is that each number
                               names one real phase. */
                            var watermarkWatch = Stopwatch.StartNew();
                            /* #2344: bound the read for the ONE collector whose value is clamped right
                               below. Name-guarded rather than applied to every enumerating definition,
                               for the reason WatermarkPolicy's remarks give: a ring-buffer source whose
                               legitimate catch-up spans days must keep reading its whole history, and the
                               floor would silently truncate it. The clamp and the bound travel together. */
                            var readFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                ? WatermarkPolicy.ReadFloor(collectionTime)
                                : null;
                            var raw = await GetLastCollectedTimeForDatabaseAsync(
                                server.ServerId, definition.TargetTable, definition.WatermarkColumn!,
                                definition.PerDatabaseWatermarkColumn!, item, ct, readFloor);
                            var clamped = WatermarkPolicy.ClampCatchup(raw, collectionTime);
                            if (raw.HasValue && clamped != raw)
                            {
                                _logger?.LogWarning(
                                    "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                                    definition.Name, server.Config.DisplayName, item, WatermarkPolicy.MaxCatchup.TotalHours, raw.Value);

                                /* #2022: the clamp opens a hole (raw, clamped) the live path will never
                                   revisit — its next cutoff IS the clamped floor. Record it for the
                                   backfill worker, merged wider with any hole already pending for this
                                   database. Only query_store reaches this lambda today; the name guard
                                   keeps a future enumeration collector from inheriting backfill state
                                   it has no worker for. */
                                if (clamped.HasValue
                                    && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                                {
                                    await RecordQueryStoreBackfillHoleAsync(server.ServerId, item, raw.Value, clamped.Value, ct);
                                }
                            }

                            /* #2111 adaptive shrink (promoted from reserve on field evidence — a member
                               whose 1h window intermittently exceeds the command timeout stays stuck for
                               hours): after N consecutive live failures the window halves per failure
                               toward 15 minutes, and the range the tighter floor skips rides the SAME
                               hole records the clamp writes — deferred to the trickle, never dropped.
                               Success resets the count, so a recovered member is back at full width
                               next cycle. */
                            var failures = ConsecutiveQueryStoreItemFailures(server.ServerId, item);
                            if (failures > 0
                                && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                            {
                                var span = QueryStoreBackfillState.AdaptiveSpan(WatermarkPolicy.MaxCatchup, failures);
                                var tighterFloor = collectionTime - span;
                                if (clamped is DateTime current)
                                {
                                    if (current < tighterFloor)
                                    {
                                        _logger?.LogWarning(
                                            "query_store on '{Server}' database [{Database}] adaptive catch-up shrink: {Failures} consecutive failed cycles — window narrowed to {Minutes:F0}m; the skipped range rides the backfill hole.",
                                            server.Config.DisplayName, item, failures, span.TotalMinutes);
                                        await RecordQueryStoreBackfillHoleAsync(server.ServerId, item, current, tighterFloor, ct);
                                        clamped = tighterFloor;
                                    }
                                }
                                else
                                {
                                    /* Never-succeeded database (null watermark): the definition's 60-minute
                                       first-run fallback is MaxCatchup-sized, so it can be exactly the window
                                       that cannot fit — tighten it the same way (the review catch on the first
                                       cut, which gated shrink on a non-null watermark and left first contact
                                       retrying the full width forever). No hole record: pre-watermark history
                                       is the backfill TAIL's job by design. */
                                    _logger?.LogWarning(
                                        "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                                        server.Config.DisplayName, item, failures, span.TotalMinutes);
                                    clamped = tighterFloor;
                                }
                            }

                            context.Watermark = clamped;

                            /* #2312: decide per database whether this cycle reads the OPEN interval. The
                               stamp is only STAGED here — it lands in PendingState from onItemComplete,
                               after this item's read and flush actually succeeded, so a per-item fault
                               (which this driver swallows by design) re-includes next time instead of
                               spending the refresh window on a cycle that captured nothing. Name-guarded
                               like the hole records: only query_store's payload reads the flag. */
                            if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                            {
                                var includeOpen = QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
                                    context.State, item, collectionTime);
                                context.IncludeOpenInterval = includeOpen;
                                if (includeOpen)
                                {
                                    stagedOpenIntervalStamps[QueryStoreOpenIntervalState.KeyFor(item)] =
                                        QueryStoreOpenIntervalState.Format(collectionTime);
                                }
                            }

                            context.PerItemWatermarkMs = watermarkWatch.ElapsedMilliseconds;
                        },
                    readItem: async (item, ct) =>
                    {
                        var batch = new List<TRow>();
                        using var itemCommand = CreateCollectorCommand(targetProvider, definition.BuildPerItemQuery(item, context), targetConnection, itemTimeout);
                        /* #2164: time the OPEN separately from the drain. ExecuteReaderAsync returns only
                           when the first rowset is available, so for query_store's staged batch this is the
                           #pm_qs_slice aggregate plus time-to-first-row — the part no client-side budget can
                           shorten. Everything after is streaming, which the budget does govern. The blended
                           sql: number could not tell those apart, which is why a 5x payload cut looked like
                           it did nothing. */
                        /* Cleared BEFORE the open so an item whose open faults cannot log the previous
                           item's split as its own — a stale timing is worse than no timing. The watermark
                           phase is NOT cleared here: it ran already, for THIS item, and clearing it would
                           hand its milliseconds to drain. The fetch phases clear on the same rule. */
                        context.PerItemOpenMs = 0;
                        context.PerItemPlanFetchMs = 0;
                        context.PerItemTextFetchMs = 0;
                        var openWatch = Stopwatch.StartNew();
                        using var itemReader = await itemCommand.ExecuteReaderAsync(ct);
                        context.PerItemOpenMs = openWatch.ElapsedMilliseconds;
                        await definition.ReadItemAsync(item, itemReader, batch, context, ct);
                        /* #2210: this database's plan-XML fetch, right after its runtime-stats read. A separate
                           query on purpose — it ships in plan_id order, so a budget cut truncates a SUFFIX,
                           which is the only reason the watermark can advance from a cut pass at all. */
                        /* `is SqlConnection` rather than a bare cast, and it does two jobs (merge resolution
                           against #2213's provider seam): the connection here is a provider-neutral
                           DbConnection now, and this fetch is Query-Store-only, so the pattern narrows the
                           type the signature needs AND gates the engine in one expression that cannot drift
                           from either. The enumerated path serves PostgreSQL targets since #2213; query_store
                           declares TargetEngine = SqlServer so it never reaches here for one, but relying on
                           the catalog for that would be an invariant held somewhere else. */
                        if (context.CapturePlanXml && targetConnection is SqlConnection planFetchConnection)
                        {
                            /* #2312 investigation: timed so the log split can say whether the invariant
                               per-cycle cost lives HERE rather than in the payload — a 0-row cycle's
                               blended sql: could not distinguish them. */
                            var planFetchWatch = Stopwatch.StartNew();
                            await FetchAndStorePlansAsync(planFetchConnection,
                                server, item, context, itemTimeout, ExtractPlanReferences(batch), ct);
                            context.PerItemPlanFetchMs = planFetchWatch.ElapsedMilliseconds;
                        }

                        /* #2150: and this database's statement-text fetch, for the same reason and with the
                           same shape — the payload no longer carries query_sql_text, because selecting it
                           inside the shipping TOP/ORDER BY made a Top-N Sort materialize nvarchar(max) text
                           for the whole qualifying set (measured 4.67s vs 0.45s time-to-first-row). Ships in
                           query_id order so a budget cut is a suffix, which is what lets the watermark
                           advance from a cut pass.

                           Gated on the same flag the payload branches on, so the two can never disagree
                           about who owns the text: if the column is nulled, this runs. */
                        if (context.FetchQueryTextSeparately && targetConnection is SqlConnection textFetchConnection)
                        {
                            /* #2312 investigation: same split as the plan fetch above. */
                            var textFetchWatch = Stopwatch.StartNew();
                            await FetchAndStoreQueryTextAsync(textFetchConnection,
                                server, item, context, itemTimeout, ExtractTextReferences(batch), ct);
                            context.PerItemTextFetchMs = textFetchWatch.ElapsedMilliseconds;
                        }

                        return batch;
                    },
                    writeBatch: (batch, ct) => WriteBatchAsync(pgConnection, definition, batch, server, collectionTime, context, ct),
                    onItemComplete: (item, batchCount, itemSqlMs, itemStorageMs) =>
                    {
                        /* #2472: the per-database cost the blended collection_log row cannot carry. Counted
                           for every completed item, including the quiet ones the log line below skips —
                           their read time is in the blended total, so leaving them out would inflate the
                           dominance ratio of whichever database happened to have rows. */
                        fanout.Observe(item, itemSqlMs + itemStorageMs);

                        /* #2111: a completed item resets the adaptive-shrink count — recovery returns
                           the member to the full catch-up width on its next cycle. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            OnQueryStoreItemSucceeded(server.ServerId, item);

                            /* #2312: NOW the open-interval stamp may land — this hook only fires after
                               the item's read and flush both succeeded. Remove, not read: a stamp left
                               staged (read faulted) must not leak into a later run's landing. */
                            if (stagedOpenIntervalStamps.Remove(QueryStoreOpenIntervalState.KeyFor(item), out var landedStamp))
                            {
                                context.PendingState[QueryStoreOpenIntervalState.KeyFor(item)] = landedStamp;
                            }
                        }

                        /* Per-DATABASE line for non-empty batches (#1565): the per-server summary blends
                           every database into one number, which hid a single busy database's 50s burst
                           behind four quiet siblings. Quiet databases (0 rows — the 2-of-3 cycles between
                           Query Store's 900s flushes) stay silent. */
                        if (batchCount > 0)
                        {
                            /* #2164: open vs drain, because they have different fixes. A pass that is nearly
                               all OPEN is bound by server-side work before the first row (for query_store,
                               the #pm_qs_slice aggregate) and no client-side budget or payload trimming will
                               touch it; a pass that is mostly drain is bound by moving rows, where the byte
                               budget and the link are the levers. Only emitted when the host measured it. */
                            if (context.PerItemOpenMs > 0)
                            {
                                /* #2312: the fetch phases print only when a separate fetch actually ran,
                                   so every other collector's line is byte-identical to before. */
                                if (context.PerItemPlanFetchMs > 0 || context.PerItemTextFetchMs > 0)
                                {
                                    _logger?.LogInformation("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms = wm:{WatermarkMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + plan_fetch:{PlanFetchMs}ms + text_fetch:{TextFetchMs}ms, pg:{PgMs}ms)",
                                        server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs,
                                        context.PerItemWatermarkMs, context.PerItemOpenMs, context.DrainMsFrom(itemSqlMs),
                                        context.PerItemPlanFetchMs, context.PerItemTextFetchMs, itemStorageMs);
                                }
                                else
                                {
                                    _logger?.LogInformation("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms = wm:{WatermarkMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms, pg:{PgMs}ms)",
                                        server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs,
                                        context.PerItemWatermarkMs, context.PerItemOpenMs, context.DrainMsFrom(itemSqlMs), itemStorageMs);
                                }
                            }
                            else
                            {
                                _logger?.LogInformation("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                                    server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs, itemStorageMs);
                            }
                        }

                        var capHit = definition.PerItemRowCountWarnThreshold is int cap && batchCount >= cap;
                        if (capHit || context.PerItemTextBudgetExceeded)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                                definition.Name, server.Config.DisplayName, item,
                                capHit ? $"row cap {definition.PerItemRowCountWarnThreshold}" : "text byte budget",
                                context.PerItemTextBytesShipped / (1024.0 * 1024.0),
                                context.PerItemShippedBoundary?.ToString("o") ?? "n/a");
                        }
                    },
                    onItemError: (item, ex) =>
                    {
                        /* #2111: stamp the yield-to-live signal (any database's live failure vouches
                           for the whole replica being contended) + the per-database adaptive-shrink
                           count. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            OnQueryStoreItemFailed(server.ServerId, item);
                        }

                        _logger?.LogWarning("Failed to collect {Collector} from [{Database}] on '{Server}': {Message}",
                            definition.Name, item, server.Config.DisplayName, ex.Message);
                    },
                    cancellationToken,
                    /* #2150: the per-database wall-clock ceiling. Null for every collector but
                       query_store, so this argument leaves every other cycle untouched. */
                    perItemBudget: definition.PerItemWallClockBudget);

                rowsWritten = driverResult.Rows;
                sqlMs += driverResult.SqlMs;
                storageMs += driverResult.StorageMs;
            }
            else
            {
                /* Plain single-query path (server-scoped): read all rows, then write them in one batch
                   (supplemental never runs for per-database collectors). Routed through WriteBatchAsync
                   so all three paths share one writer.

                   #2673: the primary read + DRAIN is bounded by the collector's PerItemWallClockBudget
                   (one item = the whole server here). The 60s per-command timeout covers only EXECUTION,
                   not the drain of a large result set, so a heavy server-scoped collector (procedure_stats,
                   query_stats) could occupy a monitored server for minutes — the exact profile we must never
                   present. Null budget = itemToken IS cancellationToken and this block is byte-for-byte what
                   it was. */
                var sqlSlice = Stopwatch.StartNew();
                var plan = definition.BuildQuery(context);
                List<TRow> rows;
                using var itemBudget = EnumeratedCollectorDriver.StartItemBudget(definition.PerItemWallClockBudget, cancellationToken);
                var itemToken = itemBudget?.Token ?? cancellationToken;
                try
                {
                    using var command = CreateCollectorCommand(targetProvider, plan, targetConnection, definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds);
                    using var reader = await command.ExecuteReaderAsync(itemToken);
                    rows = await definition.ReadAsync(reader, context, itemToken);

                    /* #1851: a definition that declares it may hand back an OPTIONAL trailing
                       (item_name, error_text) result set naming items its own server-side cursor
                       reached but could not probe — database_size_stats' mid-restore / inaccessible
                       databases, which used to vanish into an empty CATCH. Read through the SAME
                       shared machinery as the enumeration path's failures (#1837), so the note wording
                       and the log cap cannot drift between the two channels or between the two hosts.
                       Read HERE, still inside the reader, and before the storage phase below: it
                       touches only the note, never `rows`, so the payload and its delta ordering are
                       exactly what they were. */
                    if (definition.EmitsProbeFailures)
                    {
                        var probes = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, itemToken);
                        collectionNote = probes.Note;
                        LogEnumerationProbeFailures(definition, server, probes.ProbeFailures);
                    }
                }
                catch (Exception ex) when (EnumeratedCollectorDriver.ItemBudgetExpired(itemBudget, cancellationToken))
                {
                    /* #2673: this server-scoped collector blew its wall-clock budget mid read/drain. Abandon
                       the whole cycle WITHOUT advancing any watermark — ship nothing, retry next cycle with
                       the same cutoff — so no single collector runs minutes on a target. Returning here skips
                       the storage phase AND the state-persistence block below, which is what keeps the
                       watermark from moving. The provider's cancellation artifact (ex) is dropped in favour
                       of the budget message, the same as the per-item arm. */
                    _ = ex;
                    var budgetSeconds = (int)definition.PerItemWallClockBudget!.Value.TotalSeconds;
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' reached its {Budget}s wall-clock budget mid-collection — abandoned this cycle, will retry next (#2673).",
                        definition.Name, server.Config.DisplayName, budgetSeconds);
                    return new CollectorRunResult(0, sqlSlice.ElapsedMilliseconds, 0, $"wall-clock budget ({budgetSeconds}s) reached; cycle abandoned");
                }

                /* Optional best-effort second query on the same connection (server_properties'
                   health probe). Failure-isolated; skipped on an empty primary, matching Lite. */
                var supplementalPlan = definition.BuildSupplementalQuery(context);
                if (supplementalPlan is not null && rows.Count > 0)
                {
                    try
                    {
                        using var supplementalCommand = CreateCollectorCommand(targetProvider, supplementalPlan, targetConnection, CommandTimeoutSeconds);
                        using var supplementalReader = await supplementalCommand.ExecuteReaderAsync(cancellationToken);
                        await definition.ApplySupplementalAsync(rows, supplementalReader, context, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogDebug(ex, "Supplemental query for {Collector} failed; continuing without it", definition.Name);
                    }
                }
                sqlMs += sqlSlice.ElapsedMilliseconds;

                var storageSlice = Stopwatch.StartNew();
                await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);
                rowsWritten = await WriteBatchAsync(pgConnection, definition, rows, server, collectionTime, context, cancellationToken);
                storageMs += storageSlice.ElapsedMilliseconds;
            }
        }

        /* Persist what the definition observed, AFTER the cycle completed — including a cycle that wrote
           zero rows, which is exactly the case a row-derived watermark cannot cover (#1962). A cycle that
           threw never reaches here, so the older state survives and the next run takes its conservative
           path. Outside the storage-phase timer: this is host bookkeeping, not collected data. */
        if (context.PendingState.Count > 0)
        {
            /* #2312: query_store's pending state is down to ONE family — the open-interval refresh stamps
               (qsowm:), which belong to the host's own state owner rather than the definition's name (the
               definition declares no state keys, so a row written under "query_store" would never be read
               back). The plan/text watermark families that used to be split out here retired with the
               watermarks themselves; the split-by-prefix survives only as the qsowm: extraction, so a
               future fourth family cannot silently land under the wrong owner and become unprunable. */
            var openIntervalKeys = context.PendingState
                .Where(entry => entry.Key.StartsWith(QueryStoreOpenIntervalState.WatermarkKeyPrefix, StringComparison.Ordinal))
                .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);

            if (openIntervalKeys.Count > 0)
            {
                await SaveCollectorStateAsync(
                    server.ServerId, QueryStoreOpenIntervalState.StateCollectorName, openIntervalKeys, cancellationToken);

                var others = context.PendingState
                    .Where(entry => !openIntervalKeys.ContainsKey(entry.Key))
                    .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);
                if (others.Count > 0)
                {
                    await SaveCollectorStateAsync(server.ServerId, definition.Name, others, cancellationToken);
                }
            }
            else
            {
                await SaveCollectorStateAsync(server.ServerId, definition.Name, context.PendingState, cancellationToken);
            }
        }

        _logger?.LogDebug("Collected {RowCount} {Collector} rows for server '{Server}'",
            rowsWritten, definition.Name, server.Config.DisplayName);
        return new CollectorRunResult(rowsWritten, sqlMs, storageMs, collectionNote, fanout.Result);
    }

    /// <summary>
    /// Writes the per-item app-log lines for probe failures, capped at
    /// <see cref="EnumeratedCollectorDriver.MaxLoggedProbeFailures"/> with the suppressed remainder
    /// reported as a count. The collection_log row already carries the summary note; this is where the
    /// actual per-database error text lands, and it is why that note says "see the app log". Lite's twin
    /// is <c>RemoteCollectorService.LogEnumerationProbeFailures</c> — same shared templates.
    ///
    /// <para>Serves BOTH channels: an enumeration's second result set (#1837) and a payload collector's
    /// trailing one (#1851). Named for the shared template it writes, which reports the failing step as
    /// an enumeration probe — accurate for both, since a payload collector reaches this only by
    /// enumerating and probing databases inside its own server-side cursor.</para>
    /// </summary>
    private void LogEnumerationProbeFailures<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        IReadOnlyList<EnumerationProbeFailure> probeFailures)
    {
        if (probeFailures.Count == 0)
        {
            return;
        }

        var shown = Math.Min(probeFailures.Count, EnumeratedCollectorDriver.MaxLoggedProbeFailures);
        for (var i = 0; i < shown; i++)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureLogTemplate,
                definition.Name, server.Config.DisplayName, probeFailures[i].Item, probeFailures[i].Error);
        }

        if (probeFailures.Count > shown)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureOverflowLogTemplate,
                definition.Name, server.Config.DisplayName, probeFailures.Count, probeFailures.Count - shown, shown);
        }
    }

    /// <summary>
    /// Writes ONE batch (one enumerated item / one database, or the whole result set for a plain
    /// collector) to Postgres as a single binary COPY on the caller's already-open connection (#1556).
    /// The three collection paths route through here so the storage logic — the prefix columns, the
    /// naive-UTC stamp, the positional payload — lives once. A batch is atomic and independent: on a
    /// mid-run abort the batches already written stay committed (commit-1..N-1). An empty batch opens
    /// no COPY and returns 0 (rows_collected = Σ non-empty batch counts).
    ///
    /// <para>Collectors that divert large text payloads into the hash-keyed dimension tables (#1767 —
    /// query_stats, procedure_stats) wrap the COPY and the dimension upsert in ONE explicit
    /// transaction, so no reader can observe a fact row whose digest has no dimension row. Every
    /// other collector keeps the pre-#1767 path exactly, where completing the importer is itself the
    /// commit.</para>
    /// </summary>
    private async Task<int> WriteBatchAsync<TRow>(
        NpgsqlConnection pgConnection,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        DateTime collectionTime,
        CollectorContext context,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var rowsWritten = 0;
        var writer = new PgCollectorRowWriter();

        /* #1767: which payload columns (if any) store a content digest and send their text to a
           dimension table instead of inline onto every row. Derived from the same schema
           CopyCommandFor derives its column list from, so the two cannot disagree. */
        var diversionPlan = PayloadDimensions.DiversionPlanFor(definition);
        var dimensions = new PayloadDimensionBatch();
        if (diversionPlan.Count > 0)
        {
            writer.UseDimensions(diversionPlan, dimensions);
        }

        /* Only the diverting collectors need a transaction; everything else keeps the pre-#1767
           single-COPY commit and pays nothing. */
        await using var transaction = diversionPlan.Count > 0
            ? await pgConnection.BeginTransactionAsync(cancellationToken)
            : null;

        /* Naive-UTC storage — see PgCollectorRowWriter. */
        var storedCollectionTime = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

        using (var importer = await pgConnection.BeginBinaryImportAsync(
            PgCollectorRowWriter.CopyCommandFor(definition), cancellationToken))
        {
            writer.Importer = importer;

            foreach (var row in rows)
            {
                await importer.StartRowAsync(cancellationToken);

                if (definition.IncludesCollectionId)
                {
                    writer.Value(CollectionIdGenerator.Next());
                }

                writer.Value(storedCollectionTime)
                      .Value(server.ServerId)
                      .Value(server.StorageName);

                writer.BeginPayload();
                definition.WritePayload(row, writer, context);
                writer.EndPayload(definition.PayloadColumns.Count);
                rowsWritten++;
            }

            await importer.CompleteAsync(cancellationToken);
        }

        if (transaction is not null)
        {
            await PayloadDimensionWriter.FlushAsync(
                pgConnection, transaction, dimensions, storedCollectionTime, cancellationToken,
                compressPlanContent: _compressPlanContent());
            await transaction.CommitAsync(cancellationToken);
        }

        return rowsWritten;
    }

    /// <summary>
    /// Runs a collector definition against one monitored server's LIVE connection and RETURNS the shredded
    /// rows WITHOUT writing them to the store — the read-only "fetch phase only" twin of <see cref="RunAsync"/>,
    /// for an on-demand read (the live Current Active Queries snapshot the <c>fetch_active_queries</c> command
    /// serves). It builds the SAME <see cref="CollectorContext"/> the scheduled sweep builds (the shared delta
    /// calculator, the live capture-plan / schema-change providers, the server's excluded databases, the
    /// ignored-wait defaults), so the live query is byte-identical to the collector's, then opens ONE SqlClient
    /// connection and runs the definition's single query and shredder. It deliberately supports ONLY the
    /// single-statement path (no per-database enumeration, no per-item enumeration, no supplemental query): it
    /// exists for <see cref="QuerySnapshotsCollector"/>, whose Azure variant already reads what it needs from one
    /// connection. A collector that does not apply to the target yields an empty list (mirrors
    /// <see cref="RunAsync"/>). Cancellation is honored; a <c>SqlException</c> propagates to the caller, which
    /// maps it to a legible command outcome (timeout / permission / error) exactly as the actual-plan handler does.
    /// </summary>
    public async Task<List<TRow>> FetchRowsAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        if (!CollectorCatalog.AppliesTo(definition, server.Target))
        {
            return new List<TRow>();
        }

        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = _deltas,
            Target = server.Target,
            Watermark = null,
            NumericWatermark = null,
            HasCollectedBefore = false,
            IgnoredWaitTypes = IgnoredWaitDefaults.All,
            ExcludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>(),
            PerfmonCounterOverride = null,
            CapturePlanXml = _capturePlans(),
            /* #2150: stays FALSE here, and NOT because the readers are behind — this is FetchRowsAsync, the
               on-demand live fetch. It returns the rows straight to the caller and writes nothing: no store
               insert, no text fetch, no watermark. Turning it on would null query_sql_text in rows that have
               no side table to be resolved from, so text would simply be gone. Today's only caller is the
               active-queries snapshot, which has no Query Store text at all, so this is a guard for the next
               caller rather than a live behaviour. */
            FetchQueryTextSeparately = false,
            /* #2164: 0 from the default provider means "no override" — the collector keeps its own
               constant. Converted MB -> bytes here so the store knob stays operator-friendly. */
            TextByteBudgetOverride = _textBudgetMb() > 0 ? _textBudgetMb() * 1024 * 1024 : null,
            CollectSchemaChangeEvents = _collectSchemaChanges(),
        };

        var plan = definition.BuildQuery(context);

        /* Engine-neutral: a Postgres target gets an NpgsqlConnection here and the definition's
           ReadAsync never knows the difference — it reads a DbDataReader either way. */
        var provider = TargetProviders.For(server.Target);
        using var connection = provider.CreateConnection(server.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = CreateCollectorCommand(provider, plan, connection, commandTimeoutSeconds);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await definition.ReadAsync(reader, context, cancellationToken);
    }

    /// <summary>
    /// Gets the most recent value of a timestamp column from Postgres for incremental collection.
    /// Returns null on first run or if the query fails (caller uses a fallback window) — the
    /// Postgres twin of Lite's GetLastCollectedTimeAsync.
    /// </summary>
    public async Task<DateTime?> GetLastCollectedTimeAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(serverId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
            {
                return dt;
            }
        }
        catch
        {
            /* If the Postgres query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// The stored per-server state for one collector's declared keys (#1962) — the sibling of
    /// <see cref="GetLastCollectedTimeAsync"/> for state no MAX() over the collected rows can produce.
    /// Read only for the collectors that declare keys, so it costs the rest nothing. An empty result on
    /// failure is the SAFE direction: every definition treats absent state as its conservative path
    /// (default_trace_events re-reads the whole rollover set), so a broken read costs time, never events.
    /// Lite's twin is <c>RemoteCollectorService.GetCollectorStateAsync</c> — same table, same columns.
    /// </summary>
    public async Task<Dictionary<string, string>> GetCollectorStateAsync(
        int serverId, string collectorName, CancellationToken cancellationToken)
    {
        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT state_key, state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2", connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(1))
                {
                    state[reader.GetString(0)] = reader.GetString(1);
                }
            }
        }
        catch (Exception ex)
        {
            /* Fail toward "no state" — the definition's conservative path, never a wrong-but-plausible one. */
            _logger?.LogDebug(ex, "Reading collector state for {Collector} failed; using the no-state path", collectorName);
        }
        return state;
    }

    /// <summary>
    /// The cycle's distinct referenced plans with their live hashes — the probe's whole input (#2312).
    /// Generic because the dispatch loop is; any batch that is not query_store rows extracts nothing, and
    /// the caller's <c>CapturePlanXml</c> gate means that never actually happens. When one plan appears in
    /// several rows (several intervals), a non-null hash wins over a null one — the probe compares against
    /// whatever the engine reported, and null only means the payload row predated the hash column.
    /// </summary>
    private static IReadOnlyList<(long PlanId, string? PlanHash)> ExtractPlanReferences<TRow>(List<TRow> batch)
    {
        if (batch is not List<QueryStoreCollector.Row> rows || rows.Count == 0)
        {
            return Array.Empty<(long, string?)>();
        }

        var seen = new Dictionary<long, string?>();
        foreach (var row in rows)
        {
            if (row.PlanId <= 0)
            {
                continue;
            }

            if (!seen.TryGetValue(row.PlanId, out var hash) || (hash is null && row.QueryPlanHash is not null))
            {
                seen[row.PlanId] = row.QueryPlanHash;
            }
        }

        var references = new List<(long PlanId, string? PlanHash)>(seen.Count);
        foreach (var entry in seen)
        {
            references.Add((entry.Key, entry.Value));
        }

        references.Sort((a, b) => a.PlanId.CompareTo(b.PlanId));
        return references;
    }

    /// <summary>Text twin of <see cref="ExtractPlanReferences"/>, keyed by query_id with query_hash.</summary>
    private static IReadOnlyList<(long QueryId, string? QueryHash)> ExtractTextReferences<TRow>(List<TRow> batch)
    {
        if (batch is not List<QueryStoreCollector.Row> rows || rows.Count == 0)
        {
            return Array.Empty<(long, string?)>();
        }

        var seen = new Dictionary<long, string?>();
        foreach (var row in rows)
        {
            if (row.QueryId <= 0)
            {
                continue;
            }

            if (!seen.TryGetValue(row.QueryId, out var hash) || (hash is null && row.QueryHash is not null))
            {
                seen[row.QueryId] = row.QueryHash;
            }
        }

        var references = new List<(long QueryId, string? QueryHash)>(seen.Count);
        foreach (var entry in seen)
        {
            references.Add((entry.Key, entry.Value));
        }

        references.Sort((a, b) => a.QueryId.CompareTo(b.QueryId));
        return references;
    }

    /// <summary>
    /// Clears this database's plan-fetch backoff (#2776) without disturbing the size it learned.
    /// </summary>
    /// <remarks>
    /// Called from the "nothing to fetch this cycle" early returns, which are the two ways a pass can end
    /// well without reaching the success line at the bottom of the fetch. Without this a database that
    /// failed once and then went quiet would keep the count pinned — nothing resets it, because nothing
    /// runs — and the first pass after the work came back, possibly hours later and against a completely
    /// different store, would be narrowed for a reason that expired long ago. Only the counter is cleared;
    /// the learned average is the expensive part and it stays. Advisory, so a lost race is fine: the next
    /// idle cycle clears it again.
    /// </remarks>
    private void ClearPlanFetchBackoff((int ServerId, string Database) carryKey)
    {
        if (_observedPlanSize.TryGetValue(carryKey, out var estimate) && estimate.ConsecutiveFetchFailures > 0)
        {
            _observedPlanSize.TryUpdate(carryKey, QueryStorePlanXmlState.RecordFetchSuccess(estimate), estimate);
        }
    }

    /// <summary>
    /// The activity-driven plan-XML fetch for one database (#2312 Finding 2): touch-and-probe the store for
    /// the cycle's referenced plans — which refreshes map/dim liveness (Finding 3's unwired TouchSql, now
    /// the same round trip) and answers which plans are missing or hash-stale — then fetch exactly those by
    /// id, budget-bounded, and land them into the shared dimension plus the map. The store is the
    /// watermark: a caught-up database's missing set is EMPTY and no target query runs at all, which is the
    /// property the retired catalog walk lacked (measured 23s per cycle to discover "nothing new").
    ///
    /// <para>Failure-isolated, and that is load-bearing rather than defensive: plan XML is an enrichment on
    /// top of runtime statistics, so a fetch that throws must not cost the database its runtime stats. It
    /// logs and returns; whatever did not land is still missing from the store, so the next cycle that
    /// references it re-selects it by construction.</para>
    ///
    /// <para>Budget-deferred and capped ids go to <see cref="_planFetchCarryover"/>, because the probe's
    /// input is each cycle's batch references: a plan referenced ONCE whose fetch was deferred would
    /// otherwise never re-enter the probe. Ids the target no longer has (Query Store cleanup took the plan
    /// between reference and fetch) are dropped from the debt — but only on a pass that provably completed
    /// uncut, because inside a cut pass "absent from the result" and "excluded by the budget predicate" are
    /// indistinguishable from the client.</para>
    /// </summary>
    private async Task FetchAndStorePlansAsync(
        SqlConnection sqlConnection,
        ServerRuntime server,
        string databaseName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long PlanId, string? PlanHash)> references,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776) so the catch can advance this database's backoff counter — the
           handler needs the same key the body uses. */
        var carryKey = (server.ServerId, databaseName);

        try
        {
            var hasCarryover = _planFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* The steady quiet cycle: nothing referenced, nothing owed. Zero store reads, zero target
                   queries — the whole point of the reshape. */
                ClearPlanFetchBackoff(carryKey);
                return;
            }

            await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

            var missing = new SortedSet<long>();
            if (hasCarryover)
            {
                foreach (var id in carriedIds!)
                {
                    missing.Add(id);
                }
            }

            if (references.Count > 0)
            {
                var verdicts = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                    pgConnection, server.ServerId, databaseName, references, context.CollectionTime, itemTimeout, cancellationToken);
                foreach (var verdict in verdicts)
                {
                    if (!verdict.Resolved || verdict.HashStale)
                    {
                        missing.Add(verdict.Id);
                    }
                    else
                    {
                        /* Resolved and current: if it was carried debt, it is paid. */
                        missing.Remove(verdict.Id);
                    }
                }
            }

            if (missing.Count == 0)
            {
                /* The probe round-tripped the store and came back with nothing owed — a stronger proof of
                   store health than the quiet cycle above, since this one actually wrote touch timestamps.
                   Treat it as the completed pass it is (#2776). */
                _planFetchCarryover.TryRemove(carryKey, out _);
                ClearPlanFetchBackoff(carryKey);
                return;
            }

            var budget = context.TextByteBudgetOverride ?? 12 * 1024 * 1024;

            /* #2312 Finding 1 (#2322): cap the attempt from THIS database's learned average instead of the
               160KB seed every pass — zero AvgBytes means never learned, which is the seed's job. The cap
               bounds server-side DECOMPRESSION (the running total materializes every plan it measures), so
               it stays load-bearing even though the walk it originally sized is gone. */
            var estimate = _observedPlanSize.TryGetValue(carryKey, out var carriedEstimate)
                ? carriedEstimate
                : default;
            var cap = QueryStorePlanXmlState.CandidatePlanCount(
                estimate.AvgBytes > 0 ? estimate.AvgBytes : null, budget, estimate.CatchUpInProgress, out var clamped);
            if (clamped)
            {
                _logger?.LogInformation(
                    "query_store plan fetch on '{Server}' database [{Database}]: candidate cap clamped to {K} — a bound sized this pass, not a measurement.",
                    server.Config.DisplayName, databaseName, cap);
            }

            /* #2776: narrow the width by the consecutive-failure count before it is used. A database whose
               store write keeps timing out re-paid FULL decompression every cycle and re-attempted a write
               the store had already proven it could not commit; halving per failure converges on a width
               that fits. Inert at zero failures, floored so the database never stops. */
            var backedOff = QueryStorePlanXmlState.NarrowForFailures(cap, estimate.ConsecutiveFetchFailures);
            if (backedOff != cap)
            {
                _logger?.LogInformation(
                    "query_store plan fetch on '{Server}' database [{Database}]: width narrowed {Cap} -> {Narrowed} after {Failures} consecutive failure(s) — backing off, not giving up; a completed pass restores full width.",
                    server.Config.DisplayName, databaseName, cap, backedOff, estimate.ConsecutiveFetchFailures);
                cap = backedOff;
            }

            /* Ascending ids (SortedSet order) so the budget's in-SQL cut and the cross-chunk break are
               deterministic — the same debt is retried in the same order until paid. */
            var attempt = missing.Take(cap).ToList();
            var attempted = new List<long>(attempt.Count);
            var fetched = new List<FetchedPlan>();
            var shippedBytes = 0L;
            var brokeOnBudget = false;

            foreach (var chunk in attempt.Chunk(PlanFetchIdsPerStatement))
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var query = QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                using var command = CreateCollectorCommand(query, sqlConnection, itemTimeout);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                attempted.AddRange(chunk);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var planXml = reader.IsDBNull(2) ? null : reader.GetString(2);
                    fetched.Add(new FetchedPlan(
                        reader.GetInt64(0),
                        planXml,
                        reader.IsDBNull(1) ? null : reader.GetString(1)));
                    if (planXml is not null)
                    {
                        /* nvarchar length * 2 is DATALENGTH exactly — no server round-trip needed. */
                        shippedBytes += (long)planXml.Length * 2;
                    }
                }
            }

            /* NULL-XML rows count for the cap/catch-up comparison (they shipped, and the writer records
               their content-less marker) but not for the average's divisor (they carried no bytes). */
            var plansMeasured = 0;
            foreach (var plan in fetched)
            {
                if (plan.PlanXml is not null)
                {
                    plansMeasured++;
                }
            }
            _observedPlanSize[carryKey] =
                QueryStorePlanXmlState.Learn(estimate, shippedBytes, fetched.Count, plansMeasured, cap, budget);

            var returned = new HashSet<long>(fetched.Count);
            if (fetched.Count > 0)
            {
                var landed = await QueryStorePlanWriter.WriteAsync(
                    pgConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, cancellationToken);
                foreach (var id in landed)
                {
                    missing.Remove(id);
                }

                foreach (var plan in fetched)
                {
                    returned.Add(plan.PlanId);
                }
            }

            /* Target-side-gone cleanup, only when the pass provably completed UNCUT: every chunk issued
               and the in-SQL predicate never fired (a fired cut leaves shipped at or past the remaining
               budget by the oversized-admission arithmetic). On such a pass an attempted id with no
               returned row does not exist in sys.query_store_plan any more — Query Store cleanup took it
               between reference and fetch — and carrying it forever would be the content-less stall
               wearing a new hat. */
            if (!brokeOnBudget && shippedBytes < budget && attempted.Count == attempt.Count)
            {
                foreach (var id in attempted)
                {
                    if (!returned.Contains(id))
                    {
                        missing.Remove(id);
                    }
                }
            }

            if (missing.Count > 0)
            {
                var owed = new long[missing.Count];
                missing.CopyTo(owed);
                _planFetchCarryover[carryKey] = owed;
            }
            else
            {
                _planFetchCarryover.TryRemove(carryKey, out _);
            }

            /* #2776: the pass completed — restore full width. Recorded HERE rather than inside Learn
               because Learn runs before the store write, so a pass that threw would otherwise clear its own
               backoff on the way down. Reaching this line is the only proof the write actually committed. */
            _observedPlanSize.AddOrUpdate(
                carryKey,
                static (_, _) => default,
                static (_, current, _) => QueryStorePlanXmlState.RecordFetchSuccess(current),
                0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2776: advance the backoff so the next pass attempts a narrower width. Read-modify-write
               through the dictionary rather than the local `estimate`, because Learn may already have
               written a newer record for this key and clobbering it would discard this pass's size
               learning. */
            var failed = _observedPlanSize.AddOrUpdate(
                carryKey,
                static (_, _) => QueryStorePlanXmlState.RecordFetchFailure(default),
                static (_, current, _) => QueryStorePlanXmlState.RecordFetchFailure(current),
                0);

            _logger?.LogWarning(ex,
                "query_store plan fetch failed on '{Server}' database [{Database}] ({Failures} consecutive) — runtime statistics are unaffected, and whatever did not land is still missing from the store, so the next cycle that references it re-selects it at a narrower width.",
                server.Config.DisplayName, databaseName, failed.ConsecutiveFetchFailures);
        }
    }

    /// <summary>
    /// One database's statement-text fetch, the sibling of <see cref="FetchAndStorePlansAsync"/> — the
    /// #2150 split (text out of the runtime stream) driven the #2312 way (activity, not a watermark walk):
    /// touch-and-probe <c>query_store_text</c> for the cycle's referenced query_ids, fetch exactly the
    /// missing or hash-stale ones by id, land them. The hash-stale arm is the Query Store RESET detector —
    /// ids renumber on a reset, so a stored hash differing from the live one means the id names a
    /// different statement now, and its text is refetched within one cycle instead of waiting on the
    /// retired daily re-walk.
    ///
    /// <para><b>A failure here is text-only.</b> Runtime statistics are already written by the time this
    /// runs, and whatever did not land is still missing from the store — so a throw leaves the rows in
    /// place with their text unresolved and the next cycle that references them re-selects them. That is
    /// why this is a warning rather than a failure of the collector.</para>
    ///
    /// <para>Simpler than the plan fetch on purpose, in the same two ways the builders differ: no
    /// candidate-cap estimator (DATALENGTH on text is cheap — no decompression to bound) and larger id
    /// chunks. The budget, the carry-over debt, and the uncut-pass target-side-gone cleanup all work
    /// exactly as the plan side documents.</para>
    /// </summary>
    private async Task FetchAndStoreQueryTextAsync(
        SqlConnection sqlConnection,
        ServerRuntime server,
        string databaseName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long QueryId, string? QueryHash)> references,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776), same reason as the plan side: the catch advances the backoff. */
        var carryKey = (server.ServerId, databaseName);

        try
        {
            var hasCarryover = _textFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* Nothing referenced, nothing owed — so any carried failure count is stale (#2776). */
                _textFetchFailures.TryRemove(carryKey, out _);
                return;
            }

            await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

            var missing = new SortedSet<long>();
            if (hasCarryover)
            {
                foreach (var id in carriedIds!)
                {
                    missing.Add(id);
                }
            }

            if (references.Count > 0)
            {
                var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                    pgConnection, server.ServerId, databaseName, references, context.CollectionTime, itemTimeout, cancellationToken);
                foreach (var verdict in verdicts)
                {
                    if (!verdict.Resolved || verdict.HashStale)
                    {
                        missing.Add(verdict.Id);
                    }
                    else
                    {
                        missing.Remove(verdict.Id);
                    }
                }
            }

            if (missing.Count == 0)
            {
                /* Probed the store, nothing owed: the same end-well-without-fetching case as the plan side. */
                _textFetchCarryover.TryRemove(carryKey, out _);
                _textFetchFailures.TryRemove(carryKey, out _);
                return;
            }

            var budget = context.TextByteBudgetOverride ?? 12 * 1024 * 1024;

            /* #2776: the text fetch has no candidate cap by design — DATALENGTH on text is cheap, so only
               the byte budget bounds it and the whole missing set is normally attempted. That stays true
               while the fetch is healthy. Once it starts throwing, the unbounded set is the problem: the
               store write is what times out, and re-attempting the identical width guarantees the identical
               timeout. Narrowing by consecutive failures converges on a width the store can commit, and is
               inert (full set) at zero failures. */
            var textFailures = _textFetchFailures.TryGetValue(carryKey, out var carriedFailures)
                ? carriedFailures
                : 0;
            var textWidth = QueryStorePlanXmlState.NarrowForFailures(missing.Count, textFailures);
            if (textWidth != missing.Count)
            {
                _logger?.LogInformation(
                    "query_store text fetch on '{Server}' database [{Database}]: width narrowed {Full} -> {Narrowed} after {Failures} consecutive failure(s) — backing off, not giving up; a completed pass restores full width.",
                    server.Config.DisplayName, databaseName, missing.Count, textWidth, textFailures);
            }

            var attempt = new List<long>(textWidth);
            foreach (var id in missing)
            {
                if (attempt.Count >= textWidth)
                {
                    break;
                }

                attempt.Add(id);
            }

            var attempted = new List<long>(attempt.Count);
            var fetched = new List<FetchedQueryText>();
            var shippedBytes = 0L;
            var brokeOnBudget = false;

            foreach (var chunk in attempt.Chunk(TextFetchIdsPerStatement))
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var query = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                using var command = CreateCollectorCommand(query, sqlConnection, itemTimeout);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                attempted.AddRange(chunk);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var text = reader.IsDBNull(2) ? null : reader.GetString(2);
                    fetched.Add(new FetchedQueryText(
                        reader.GetInt64(0),
                        text,
                        reader.IsDBNull(1) ? null : reader.GetString(1)));
                    if (text is not null)
                    {
                        shippedBytes += (long)text.Length * 2;
                    }
                }
            }

            var returned = new HashSet<long>(fetched.Count);
            if (fetched.Count > 0)
            {
                var landed = await QueryStoreTextWriter.WriteAsync(
                    pgConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, cancellationToken);
                foreach (var id in landed)
                {
                    missing.Remove(id);
                }

                foreach (var text in fetched)
                {
                    returned.Add(text.QueryId);
                }
            }

            /* Same uncut-pass cleanup as the plan side: an id the target no longer serves must not become
               permanent debt. */
            if (!brokeOnBudget && shippedBytes < budget && attempted.Count == attempt.Count)
            {
                foreach (var id in attempted)
                {
                    if (!returned.Contains(id))
                    {
                        missing.Remove(id);
                    }
                }
            }

            if (missing.Count > 0)
            {
                var owed = new long[missing.Count];
                missing.CopyTo(owed);
                _textFetchCarryover[carryKey] = owed;
            }
            else
            {
                _textFetchCarryover.TryRemove(carryKey, out _);
            }

            /* #2776: the pass completed — restore full width. Removing the key rather than zeroing it keeps
               the dictionary to just the databases currently backing off. */
            _textFetchFailures.TryRemove(carryKey, out _);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2776: advance the backoff so the next pass attempts a narrower width. Saturates at the same
               halving count the plan side uses, so the counter stays meaningful rather than unbounded. */
            var failures = _textFetchFailures.AddOrUpdate(
                carryKey,
                1,
                static (_, current) => current >= QueryStorePlanXmlState.MaxBackoffHalvings
                    ? QueryStorePlanXmlState.MaxBackoffHalvings
                    : current + 1);

            _logger?.LogWarning(ex,
                "query_store text fetch failed on '{Server}' database [{Database}] ({Failures} consecutive) — runtime statistics are already written, and whatever did not land is still missing from the store, so the next cycle that references those statements re-selects them at a narrower width.",
                server.Config.DisplayName, databaseName, failures);
        }
    }

    /// <summary>
    /// Upserts what the definition observed this cycle (<see cref="CollectorContext.PendingState"/>),
    /// after the cycle completed — so a cycle that collected zero rows still records what it saw, which is
    /// the whole point of keeping this state off the payload. Best-effort: a failed write leaves the older
    /// value, and the next cycle re-derives from it or falls back.
    /// </summary>
    public async Task SaveCollectorStateAsync(
        int serverId, string collectorName, IReadOnlyDictionary<string, string> state, CancellationToken cancellationToken)
    {
        if (state.Count == 0)
        {
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            foreach (var entry in state)
            {
                /* One statement per key: Npgsql's positional parameters cannot span a multi-statement
                   batch (they bind to the FIRST statement and the rest fail silently), and this loop
                   runs over a single declared key today. */
                using var command = new NpgsqlCommand(@"
INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at", connection);
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(collectorName);
                command.Parameters.AddWithValue(entry.Key);
                command.Parameters.AddWithValue(entry.Value);
                /* Naive UTC, Kind-Unspecified — the product-wide PG timestamp discipline
                   (PgAlertStateStore.NaiveUtcNow, DarlingObservability, the storedCollectionTime below).
                   updated_at is `timestamp` WITHOUT time zone, and binding a Kind=Utc DateTime does not
                   fail: Npgsql infers `timestamptz` from the Kind, PostgreSQL casts it into the column,
                   and the cast renders it in the SERVER's zone — so the row lands silently offset by the
                   server's UTC offset (measured at exactly 4h on an America/New_York store) while every
                   other timestamp in the store is UTC. Nothing throws and nothing logs; the column simply
                   disagrees with the rest of the store. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Storing collector state for {Collector} failed; next cycle uses the older value", collectorName);
        }
    }

    /// <summary>
    /// Records a clamp-opened Query Store hole for the #2022 backfill worker, under the WORKER's
    /// collector_state name (not the definition's — query_store still declares no state keys).
    /// Merged wider with any pending hole so a repeat outage cannot overwrite an unserviced one.
    /// Best-effort: a lost record means a lost backfill opportunity, never wrong data — the live
    /// path's own WARNING already disclosed the hole.
    /// </summary>
    private async Task RecordQueryStoreBackfillHoleAsync(
        int serverId, string databaseName, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken)
    {
        try
        {
            var key = QueryStoreBackfillState.HoleKeyPrefix + databaseName;
            var existing = await GetCollectorStateAsync(serverId, QueryStoreBackfillState.StateCollectorName, cancellationToken);
            var merged = QueryStoreBackfillState.MergeHole(existing.TryGetValue(key, out var encoded) ? encoded : null, fromUtc, toUtc);
            await SaveCollectorStateAsync(
                serverId, QueryStoreBackfillState.StateCollectorName,
                new Dictionary<string, string>(StringComparer.Ordinal) { [key] = QueryStoreBackfillState.EncodeHole(merged.FromUtc, merged.ToUtc) },
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Recording query_store backfill hole for [{Database}] failed; the live WARNING remains the disclosure", databaseName);
        }
    }

    /// <summary>
    /// Deletes ONE collector_state key — the backfill worker's retirement path for a serviced or
    /// expired hole record (#2022). Best-effort like its siblings: a failed delete leaves the row,
    /// and the worker's scan re-derives the same verdict next tick.
    /// </summary>
    public async Task DeleteCollectorStateKeyAsync(
        int serverId, string collectorName, string stateKey, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "DELETE FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            command.Parameters.AddWithValue(stateKey);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Deleting collector state {Key} for {Collector} failed; next tick re-derives", stateKey, collectorName);
        }
    }

    /// <summary>
    /// Retires one collector's per-database <c>collector_state</c> rows for databases the server no longer
    /// has (#2188). One statement per (owner, prefix) pair — <c>$1</c> server_id, <c>$2</c> collector_name,
    /// <c>$3</c> the key prefix, which is also what reconstructs each live database's key for the anti-join.
    ///
    /// <para><b>The existence list is <c>database_states</c>, not the collector's enumeration.</b> That is
    /// the whole design. query_store's enumeration is a heavily FILTERED list — ONLINE only, AG primaries
    /// only, the excluded-database filter, the vendor-name screen, <c>HAS_DBACCESS</c>, and a per-database
    /// probe that can fail — so a database missing from one cycle's items is far more often offline,
    /// excluded or unprobeable than dropped, and pruning on that absence would delete live watermarks on
    /// exactly the servers that have such databases. <c>database_states</c> is an unfiltered
    /// <c>SELECT ... FROM sys.databases</c>, so it answers the only question being asked here: does this
    /// name still exist on the instance.</para>
    ///
    /// <para><b>Guarded on the snapshot existing</b> (<c>newest IS NOT NULL</c>): a server that has never
    /// collected database_states, or whose rows have aged out, produces an empty snapshot, and an unguarded
    /// anti-join against nothing deletes EVERY row. The subselect always yields one row (MAX over zero rows
    /// is NULL), so the guard is what turns "no snapshot" into "prune nothing" instead of "prune all".</para>
    ///
    /// <para><b>And guarded on the snapshot being NEWER than the state row</b>
    /// (<c>s.updated_at &lt; snapshot.newest</c>), which is the stronger of the two and the one that makes
    /// this correct rather than merely usually-correct. Existing is not the same as CURRENT: if
    /// database_states stops collecting for a server — a per-server schedule change, a failing collector,
    /// anything — the newest snapshot freezes, and every database created after that instant is missing from
    /// it while being perfectly alive. Presence alone would prune such a database's watermark on EVERY cycle
    /// forever, silently paying a full plan-XML refetch each time: the exact cost #2164 exists to remove,
    /// with a log line confidently calling a live database dropped. A snapshot cannot judge a row written
    /// after it was taken. This holds regardless of the two collectors' relative cadences, so nothing here
    /// depends on database_states being scheduled more often than query_store; both stamps are the SERVICE
    /// clock's naive UTC (<c>collectionTime</c> and <see cref="SaveCollectorStateAsync"/> both read
    /// <c>DateTime.UtcNow</c>), never the monitored server's. A genuinely dropped database is still pruned:
    /// its last state write necessarily precedes any snapshot taken after the drop.</para>
    ///
    /// <para>The two guards overlap — <c>&lt;</c> against a NULL newest is already NULL, so the freshness
    /// test alone would cover the empty snapshot. The explicit NULL check stays anyway: "no snapshot prunes
    /// nothing" is a promise worth reading off the statement instead of deriving from three-valued
    /// logic.</para>
    ///
    /// <para><b>Bounded consequence, either way.</b> Deleting a watermark that should have stayed costs one
    /// full plan-XML refetch for that database and nothing else — the same conservative path an absent or
    /// expired watermark already takes (<see cref="QueryStorePlanXmlState.Resolve"/>), which is why racing
    /// an in-flight cycle is safe: the write-back is an upsert, so a cycle that had already loaded the state
    /// simply restores the row it is still using, and a row deleted for a genuinely dropped database has no
    /// cycle to race.</para>
    ///
    /// <para><b>Not reached on Azure SQL DB</b>, where <c>DatabaseStateCollector.AppliesTo</c> is false and
    /// there is therefore no snapshot to check — the guard makes it a no-op rather than a mass delete. Those
    /// orphans stay (#2191 tracks the Azure arm, including why that path's own database list cannot be used
    /// as the existence check); the accumulation is bounded to one ~100-byte row per database name ever
    /// seen.</para>
    ///
    /// <para>Best-effort like every sibling here: a failed prune leaves the rows and the next cycle retries.
    /// Nothing downstream reads them — an orphan is a row nobody asks about, which is why this is hygiene
    /// rather than a correctness fix.</para>
    /// </summary>
    internal const string PruneOrphanedDatabaseStateKeysSql = @"
DELETE FROM collector_state s
USING (SELECT MAX(collection_time) AS newest FROM database_states WHERE server_id = $1) snapshot
WHERE s.server_id = $1
AND   s.collector_name = $2
AND   starts_with(s.state_key, $3)
AND   snapshot.newest IS NOT NULL
AND   s.updated_at < snapshot.newest
AND   NOT EXISTS
      (
          SELECT 1
          FROM database_states ds
          WHERE ds.server_id = $1
          AND   ds.collection_time = snapshot.newest
          AND   s.state_key = $3 || ds.database_name
      )
RETURNING s.state_key";

    /// <summary>
    /// The Azure SQL DB variant (#2191): prune every per-database state key that is not the ONE database this
    /// registration names. <c>$1</c> server_id, <c>$2</c> collector_name, <c>$3</c> key prefix, <c>$4</c> the
    /// registration's own database.
    ///
    /// <para><b>Why this needs no snapshot, and no freshness guard.</b> The on-prem statement anti-joins
    /// <c>database_states</c> because the question there is "does this name still exist on the instance", and
    /// it needs the two guards because a SNAPSHOT can be empty or stale. Here there is no snapshot: after
    /// #2220 a registration that names a database sweeps only that database, so its one legitimate key is
    /// derivable from the connection string's own catalog — which is current by construction and cannot go
    /// stale, be empty, or be filtered. That is why #2191 looked unfixable when it was filed and is not now:
    /// it asked for "an authoritative unfiltered sys.databases read from master, used only on the success
    /// path", and #2220 removed the need for any master read on this path at all.</para>
    ///
    /// <para><b>What it actually deletes, today.</b> Mostly #2220's residue. Before that fix each Azure
    /// registration swept every sibling database on the logical server and wrote a watermark for each, all
    /// under its own server_id — so these keys are the state half of that contamination. <c>collector_state</c>
    /// carries no retention (it is state, not facts), so unlike the collected rows those orphans would
    /// otherwise persist forever rather than ageing out.</para>
    ///
    /// <para>Bounded consequence, exactly as on the on-prem path: deleting a watermark that should have
    /// stayed costs one full plan-XML refetch for that database and nothing else.</para>
    /// </summary>
    internal const string PruneForeignDatabaseStateKeysSql = @"
DELETE FROM collector_state s
WHERE s.server_id = $1
AND   s.collector_name = $2
AND   starts_with(s.state_key, $3)
AND   s.state_key <> $3 || $4
RETURNING s.state_key";

    /// <summary>
    /// Runs <see cref="PruneOrphanedDatabaseStateKeysSql"/> for every owner/prefix in the SHARED
    /// <see cref="QueryStorePerDatabaseState.PrunableKeys"/> — the same set Lite's DuckDB twin
    /// (<c>RemoteCollectorService.PruneOrphanedQueryStoreDatabaseStateAsync</c>) iterates, so a prefix
    /// cannot end up pruned on one SKU and orphaning on the other. Once per query_store cycle for one
    /// server. Separate statements rather than one combined predicate because Npgsql's positional
    /// parameters cannot span a multi-statement batch, and three narrow deletes down the primary key are
    /// easier to read than one that ORs three prefixes together.
    /// </summary>
    internal async Task PruneOrphanedQueryStoreDatabaseStateAsync(int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var pruned = new List<string>();

            foreach (var (owner, prefix) in QueryStorePerDatabaseState.PrunableKeys)
            {
                using var command = new NpgsqlCommand(PruneOrphanedDatabaseStateKeysSql, connection);
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(owner);
                command.Parameters.AddWithValue(prefix);

                /* RETURNING rather than a rows-affected count: the only symptom of a WRONG delete here is a
                   silent refetch, so a bare number would leave nothing to diagnose it with. The keys name
                   the databases, which is what makes a mistaken prune visible in the log. */
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    pruned.Add(reader.GetString(0));
                }
            }

            if (pruned.Count > 0)
            {
                /* Information, like DarlingObservability's orphaned-server sweep: rare, and it names a
                   database lifecycle event the operator may not know the monitor noticed. */
                _logger?.LogInformation(
                    "[server_id {ServerId}] pruned {Count} query_store state row(s) for database(s) no longer on the server: {Keys}",
                    serverId, pruned.Count, string.Join(", ", pruned));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Pruning orphaned query_store database state failed; next cycle retries");
        }
    }

    /// <summary>
    /// The Azure SQL DB arm of the #2188 prune (#2191): retire every per-database state key that does not
    /// belong to the one database this registration names.
    ///
    /// <para>Runs the same shared <see cref="QueryStorePerDatabaseState.PrunableKeys"/> set as the on-prem
    /// path, so a prefix cannot be pruned on one target type and left orphaning on the other.</para>
    /// </summary>
    /// <param name="ownDatabase">
    /// The registration's own database — the connection string's initial catalog. Callers must only reach
    /// here when that is non-empty: a registration naming no database (or naming <c>master</c>) is a
    /// registration of the logical SERVER, whose legitimate database set is everything on it, and pruning
    /// against a single name there would delete every live watermark it has.
    /// </param>
    internal async Task PruneForeignQueryStoreDatabaseStateAsync(
        int serverId, string ownDatabase, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(ownDatabase))
        {
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var pruned = new List<string>();

            foreach (var (owner, prefix) in QueryStorePerDatabaseState.PrunableKeys)
            {
                using var command = new NpgsqlCommand(PruneForeignDatabaseStateKeysSql, connection);
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(owner);
                command.Parameters.AddWithValue(prefix);
                command.Parameters.AddWithValue(ownDatabase);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    pruned.Add(reader.GetString(0));
                }
            }

            if (pruned.Count > 0)
            {
                /* Names the keys rather than counting them, like the on-prem twin: the only symptom of a
                   wrong delete here is a silent refetch, so a bare number would leave nothing to diagnose
                   with. On an Azure server upgraded past #2220 this fires ONCE and clears that fix's state
                   residue, which is worth saying plainly rather than looking like a recurring anomaly. */
                _logger?.LogInformation(
                    "[server_id {ServerId}] pruned {Count} query_store state row(s) belonging to databases other than this registration's [{Database}]: {Keys}",
                    serverId, pruned.Count, ownDatabase, string.Join(", ", pruned));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Pruning foreign query_store database state failed; next cycle retries");
        }
    }

    /// <summary>
    /// The #2022 backfill write entry: the SAME private COPY writer every live path routes through
    /// (dimension diversion, positional contract, naive-UTC stamp), on its own store connection.
    /// <paramref name="collectionTime"/> is the slice's BACKDATED ceiling — see QueryStoreBackfill's
    /// horizon contract for why that is safe only inside the raw tier's window.
    /// </summary>
    public async Task<int> WriteBackfillBatchAsync<TRow>(
        ICollectorDefinition<TRow> definition, List<TRow> rows, ServerRuntime server,
        DateTime collectionTime, CollectorContext context, CancellationToken cancellationToken)
    {
        await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);
        return await WriteBatchAsync(pgConnection, definition, rows, server, collectionTime, context, cancellationToken);
    }

    /// <summary>
    /// Postgres twin of Lite's GetLastCollectedTimeForDatabaseAsync: the newest already-collected
    /// value for ONE database, for definitions with a PerDatabaseWatermarkColumn (Azure SQL DB
    /// per-database XE capture, #1535). Null on first run for that database or on failure — the
    /// caller falls back to the definition's documented window.
    ///
    /// <para><paramref name="collectedSince"/> bounds the read on <c>collection_time</c> — the
    /// PARTITIONING column, so the bound actually prunes chunks (#2344). Null keeps the unbounded
    /// behaviour, which is correct for any reader whose watermark is NOT clamped; pass
    /// <see cref="WatermarkPolicy.ReadFloor"/> only from a caller whose value is, and read that method's
    /// remarks for why the bound provably changes no answer. Unbounded, this is a <c>MAX</c> over a
    /// non-partitioning column with no time predicate — every chunk in retention, per database, per
    /// cycle, at a cost that grows with the store rather than the workload.</para>
    /// </summary>
    public async Task<DateTime?> GetLastCollectedTimeForDatabaseAsync(
        int serverId, string tableName, string columnName, string databaseColumnName, string databaseName,
        CancellationToken cancellationToken, DateTime? collectedSince = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var sql = collectedSince is null
                ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2"
                : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2 AND collection_time > $3";
            using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(databaseName);
            if (collectedSince is DateTime floor)
            {
                /* Naive like every other timestamp bound in this store (#1969): a Utc Kind infers
                   timestamptz and Postgres would convert it into the session zone on the way in. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(floor, DateTimeKind.Unspecified));
            }

            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
            {
                return dt;
            }
        }
        catch
        {
            /* If the Postgres query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// Gets the most recent value of a monotonic bigint identity column from Postgres for incremental
    /// collection — the numeric twin of <see cref="GetLastCollectedTimeAsync"/> (job_history dedups on
    /// <c>instance_id</c>, sysjobhistory's IDENTITY bigint). Returns null on first run or if the query
    /// fails (caller uses its documented first-run/fallback path).
    /// </summary>
    public async Task<long?> GetLastCollectedInstanceIdAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(serverId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is not null && result != DBNull.Value)
            {
                return Convert.ToInt64(result);
            }
        }
        catch
        {
            /* If the Postgres query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// Whether a prior SUCCESS row exists in collection_log for this collector+server — the "has collected
    /// before" signal (<see cref="CollectorContext.HasCollectedBefore"/>), consulted only when the watermark
    /// is null. Returns false on any failure, which errs toward the all-history first run (correct for a
    /// genuinely fresh store).
    /// </summary>
    public async Task<bool> HasPriorCollectorSuccessAsync(int serverId, string collectorName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT EXISTS(SELECT 1 FROM collection_log WHERE server_id = $1 AND collector_name = $2 AND status = 'SUCCESS')", connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is bool b && b;
        }
        catch
        {
            /* Fail toward first-run (all-history) — matches a fresh store with no log yet. */
            return false;
        }
    }

    /// <summary>
    /// Drops any cached master-inaccessible verdict for a server that has just reconnected.
    ///
    /// Azure SQL DB reports "this login may not read master" and "you cannot reach this server right
    /// now" with overlapping error numbers, so a verdict formed while a server was failing is not
    /// trustworthy. The moment it answers again is the moment to discard that verdict and re-probe.
    /// Without this, a transient outage permanently misfiles a login that CAN read master, and
    /// database-scoped collection stays degraded until the service restarts (#1506).
    /// </summary>
    public void OnServerReconnected(int serverId)
    {
        if (_azureMasterInaccessibleSince.TryRemove(serverId, out _))
        {
            _logger?.LogInformation("[server_id {ServerId}] reconnected — re-probing master for database-scoped collectors.", serverId);
        }
    }

    /// <summary>
    /// The databases one Azure SQL DB registration's per-database sweep covers.
    ///
    /// <para><b>A registration that names a database sweeps that database, and nothing else</b> (#2220) —
    /// which is the common case, since <c>server_id</c> hashes <c>host[:database][:RO]</c> and registering
    /// each database separately is how you get separate identities. That path returns immediately and never
    /// touches <c>master</c>.</para>
    ///
    /// <para>Only a registration naming NO database — or naming <c>master</c>, where a catalog-less Azure
    /// connection lands — is a registration of the logical SERVER, and only that one enumerates: master
    /// first with the per-server exclusion filter, and on a master-access error a fallback that has nothing
    /// to fall back to and therefore throws (#857's shape, now the exceptional path rather than the default).
    /// The re-probe throttle is deliberately NOT consulted there; see the comment at the call site.</para>
    ///
    /// <para>It read master unconditionally before #2220, sweeping every online database on the logical
    /// server into whichever registration ran the sweep — N registrations of N databases meant N² collection
    /// with every registration's history contaminated by its siblings'.</para>
    /// </summary>
    internal async Task<List<string>> GetAzureDatabaseListAsync(ServerRuntime server, CancellationToken cancellationToken)
    {
        var targetDb = new SqlConnectionStringBuilder(server.ConnectionString).InitialCatalog;

        /* #2220: a registration that NAMES a database is a registration OF that database, so its sweep
           covers exactly that one and never touches master. Before this, EVERY database-scoped collector
           enumerated master and swept every online database on the logical server, storing all of it under
           the one server_id of whichever registration ran the sweep — N registrations of N databases on one
           server meant N² collection with every registration's history contaminated by its siblings'.

           This also subsumes the #857 case it looks like it bypasses, and improves on it: a login granted
           access to one user database but not to master HAS a named database, so it now returns here without
           probing master at all, rather than probing, failing, forming a verdict and falling back. Master is
           reached only by a registration that names no database — the logical-server registration, which has
           nothing else to enumerate from. */
        var ownDatabase = AzureSweepScope.OwnDatabaseOrEmpty(targetDb);
        if (ownDatabase.Count > 0)
        {
            return ownDatabase;
        }

        /* NO throttle check here, and that is deliberate rather than an omission — restoring what the
           `hasFallback &&` guard used to achieve. This branch is reached ONLY when the registration names no
           database, so there is nothing to fall back TO: honouring the throttle would return
           FallbackDatabaseList, which throws immediately without probing, and would keep throwing for the
           whole recheck interval while never attempting the one thing that could recover. Probing master
           every cycle is the cheaper failure. (Review caught me reintroducing exactly this: I read
           `hasFallback &&` as a redundant condition when it was there to DISABLE the throttle.)

           The throttle machinery itself is left alone. It is tested behaviour from #857/#1506, and it is now
           unreachable in production for a different reason than this one: its whole purpose was to stop
           re-probing master for a registration that HAS a fallback, and such a registration no longer probes
           master at all. Retiring it is its own change, with those tests. */

        /* The query and the hop to master both come from the provider, so the enumeration set is defined
           in exactly one place per engine. What stays here is the failure policy below, which is the
           part that is genuinely Azure-specific. */
        var (masterConnectionString, enumerationQuery) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan(
            server.ConnectionString, server.Config.ExcludedDatabases);

        var databases = new List<string>();
        try
        {
            using var connection = new SqlConnection(masterConnectionString);
            await connection.OpenAsync(cancellationToken);
            /* Azure master enumeration is SQL-Server-only, but it goes through the same parameter
               mapping as every other command so a type cannot be mapped two ways. */
            using var command = CreateCollectorCommand(enumerationQuery, connection, CommandTimeoutSeconds);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                databases.Add(reader.GetString(0));
            }

            _azureMasterInaccessibleSince.TryRemove(server.ServerId, out _);
            return databases;
        }
        catch (SqlException ex) when (ShouldFallBackToSingleDatabaseError(ex.Number))
        {
            _azureMasterInaccessibleSince[server.ServerId] = DateTime.UtcNow;

            return FallbackDatabaseList(server, targetDb, reason: $"master DB inaccessible (SQL error {ex.Number})");
        }
    }

    /// <summary>
    /// True while a recent master-inaccessible verdict still stands. It expires so a server whose
    /// access was restored recovers on its own rather than staying degraded until restart (#1506).
    /// </summary>
    private bool IsMasterProbeThrottled(int serverId)
    {
        if (!_azureMasterInaccessibleSince.TryGetValue(serverId, out var deniedAt))
        {
            return false;
        }

        if (DateTime.UtcNow - deniedAt < AzureMasterRecheckInterval)
        {
            return true;
        }

        _azureMasterInaccessibleSince.TryRemove(serverId, out _);
        return false;
    }

    /// <summary>
    /// The database list to use when master cannot be enumerated: the connection's own catalog.
    ///
    /// When there isn't one, database-scoped collectors have nowhere to read from. That used to be a
    /// warning and an empty list, which made every one of them report success having collected zero
    /// rows. Throwing puts the failure where it can actually be seen (#1506).
    /// </summary>
    /// <param name="quiet">
    /// Set on the throttled path, which runs for every database-scoped collector on every cycle. Only
    /// forming the verdict is worth an Information line; re-reading it is not.
    /// </param>
    private List<string> FallbackDatabaseList(ServerRuntime server, string? targetDb, string reason, bool quiet = false)
    {
        var fallback = SingleDbOrEmpty(targetDb);

        if (fallback.Count == 0)
        {
            throw new InvalidOperationException(
                $"{reason}, and this connection has no target database to fall back to (it resolves to " +
                $"master). Set a database for '{server.Config.DisplayName}' so database-scoped collectors " +
                $"have something to read.");
        }

        if (quiet)
        {
            _logger?.LogDebug("[{Server}] {Reason} — collecting from '{Database}' only.",
                server.Config.DisplayName, reason, targetDb);
        }
        else
        {
            _logger?.LogInformation("[{Server}] {Reason} — collecting from '{Database}' only.",
                server.Config.DisplayName, reason, targetDb);
        }

        return fallback;
    }

    internal async Task<SqlConnection> OpenAzureDatabaseConnectionAsync(ServerRuntime server, string databaseName, CancellationToken cancellationToken)
        => (SqlConnection)await OpenDatabaseConnectionAsync(
            SqlServerTargetProvider.Instance, server, databaseName, cancellationToken);

    /// <summary>
    /// The connection for a collector that reads the server as a whole — engine-resolved from the probed
    /// target, never constructed directly.
    /// <para>Extracted so it can be PINNED by test. This is the exact seam that broke: the non-per-database
    /// branch built a <c>SqlConnection</c> literally, so six of the seven PostgreSQL collectors got a SQL
    /// Server connection and failed in the connection-string parser before running a query. Both engines'
    /// providers were already correct and individually tested — nothing asserted that the RUNNER asked them.
    /// A test that opens nothing and only checks the returned TYPE is enough to catch it, which is why it is
    /// worth having.</para>
    /// </summary>
    internal static DbConnection CreateTargetConnection(ServerRuntime server)
    {
        ArgumentNullException.ThrowIfNull(server);

        return TargetProviders.For(server.Target).CreateConnection(server.ConnectionString);
    }

    /// <summary>
    /// The engine-neutral per-database connection: same monitored server, one specific database.
    /// <para>PostgreSQL has no alternative to this. A SQL Server collector can reach another database
    /// without reconnecting (<c>EXECUTE [db].sys.sp_executesql</c>), but a PostgreSQL connection is
    /// bound to one database for its lifetime, so a per-database collector there is necessarily one
    /// connection per database per cycle. That is the cost of reading <c>pg_stat_user_tables</c> and
    /// friends at all, and it is why per-database PostgreSQL collectors get slow cadences.</para>
    /// </summary>
    internal static async Task<DbConnection> OpenDatabaseConnectionAsync(
        ITargetProvider provider, ServerRuntime server, string databaseName, CancellationToken cancellationToken)
    {
        var connection = provider.CreateConnection(
            provider.WithDatabase(server.ConnectionString, databaseName));

        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            /* The caller only disposes what it receives, so a connection that fails to open must be
               disposed HERE or it leaks — once per database per cycle, on exactly the unreachable
               database the per-database loop is designed to skip and keep going past. */
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Lists the databases to fan out over on a PostgreSQL target.
    /// <para>No master-inaccessible fallback and no re-probe throttle, unlike the Azure twin, because
    /// neither has a meaning here: <c>pg_database</c> is a shared catalog readable from the connected
    /// database, so a failure means the login or the server is broken rather than that one catalog is
    /// out of reach. Falling back to the connected database would convert a permissions problem into a
    /// quiet partial collection, which is the failure mode that fallback exists to avoid elsewhere.</para>
    /// </summary>
    internal async Task<List<string>> GetPostgresDatabaseListAsync(ServerRuntime server, CancellationToken cancellationToken)
    {
        var provider = TargetProviders.For(server.Target);
        var (connectionString, query) = provider.BuildDatabaseListPlan(
            server.ConnectionString, server.Config.ExcludedDatabases);

        var databases = new List<string>();

        using var connection = provider.CreateConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        using var command = CreateCollectorCommand(provider, query, connection, CommandTimeoutSeconds);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    /* #2220: delegates to the shared rule. Both runners carried their own copy of this predicate, and a
       sweep-scoping rule that disagrees between Lite and Darling is the same class of defect as the one
       #2220 fixes. */
    private static List<string> SingleDbOrEmpty(string? targetDb) =>
        AzureSweepScope.OwnDatabaseOrEmpty(targetDb);

    /// <summary>
    /// Whether master enumeration failed in a way that means database-scoped collectors should fall back
    /// to the connection's own catalog (#857). Deliberately broader than "this login cannot read master":
    /// a 40615 firewall rejection at the logical server says nothing about the login's rights, but the
    /// fallback still works, because Azure evaluates DATABASE-level firewall rules first and a user
    /// database can be reachable while master is not (#1631). The list — and the reason a reachability
    /// error must never be read as a rights verdict (#1506) — is owned by
    /// <see cref="SqlErrorClassification"/>, shared with Lite so the two cannot drift. This bug reached
    /// Darling because the list was duplicated here.
    /// </summary>
    internal static bool ShouldFallBackToSingleDatabaseError(int errorNumber) =>
        SqlErrorClassification.ShouldFallBackToSingleDatabase(errorNumber);

    /* Internal, not private: QueryStoreBackfill (#2022) builds its slice commands through the same
       parameter mapping so the two paths cannot drift on a type.

       Still SqlCommand-typed and still SQL-Server-only, because every caller of THIS overload is:
       Query Store backfill and the Azure per-database/master paths are SQL Server features by
       definition. The engine-neutral path goes through CreateCollectorCommand(ITargetProvider, ...)
       below, and both end up in the same parameter mapping inside SqlServerTargetProvider, so a
       parameter type cannot be mapped two ways. */
    internal static SqlCommand CreateCollectorCommand(CollectorQuery plan, SqlConnection connection, int commandTimeoutSeconds)
        => (SqlCommand)SqlServerTargetProvider.Instance.CreateCommand(plan, connection, commandTimeoutSeconds);

    /// <summary>
    /// The engine-neutral command factory: same collector query, whichever engine the target is.
    /// </summary>
    private static DbCommand CreateCollectorCommand(
        ITargetProvider provider, CollectorQuery plan, DbConnection connection, int commandTimeoutSeconds)
        => provider.CreateCommand(plan, connection, commandTimeoutSeconds);
}
