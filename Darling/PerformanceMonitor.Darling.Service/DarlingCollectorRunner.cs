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
/// <param name="Abandoned">
/// True only for a cycle the #2673 whole-server wall-clock budget gave up on: nothing was stored and no
/// watermark advanced. Carried as its own field rather than inferred from <paramref name="Note"/>, because
/// matching on the note's TEXT would make the collection_log status depend on a human-readable string that
/// exists to be reworded — the classification and the wording have to move independently. Defaulted false so
/// the seven ordinary construction sites are unchanged; the abandonment return is the only site that sets it.
/// </param>
/// <param name="ServerPhasesMeasured">
/// True when the SERVER-SCOPED path stamped <paramref name="ServerOpenMs"/>/<paramref name="ServerDrainMs"/>
/// (#2851). The flag exists because the enumerated path gates its own split on <c>PerItemOpenMs &gt; 0</c>,
/// which conflates "we measured it" with "the number was non-zero" — a genuinely instant open would suppress
/// the whole split, and a reader could not tell that from a collector that emits none. Gate on the flag, so a
/// measured zero prints as a zero.
/// </param>
/// <param name="ServerOpenMs">
/// Milliseconds the server-scoped <c>ExecuteReaderAsync</c> took — the part no client-side budget can shorten.
/// Stamped from a <c>finally</c> rather than after the await (#2816): a throwing open must still report how
/// long it ran, or its time silently lands in the residual and the residual is the one term nobody can
/// attribute.
/// </param>
/// <param name="ServerDrainMs">
/// Milliseconds the server-scoped <c>ReadAsync</c> took — row streaming, which the read loop and any byte
/// budget do govern. Measured rather than inferred so <paramref name="ServerOtherMs"/> is a real residual
/// instead of "everything we did not time".
/// </param>
/// <param name="ServerWatermarkMs">
/// Milliseconds the server-scoped watermark read took. **Deliberately NOT part of the <see cref="SqlMs"/>
/// decomposition**, because on this path it is not part of <see cref="SqlMs"/> at all: it runs before the
/// <c>sqlSlice</c> stopwatch is even started, so folding it into the sum would print a permanent <c>wm:0ms</c>
/// and invite exactly the wrong conclusion — that a store read which #2796 measured at 50s cold is free. It is
/// reported alongside as its own figure, and #2851's own framing (that <c>sql:</c> is wm+open+drain here) is
/// the misreading this parameter exists to prevent.
/// </param>
public sealed record CollectorRunResult(
    int Rows,
    long SqlMs,
    long StorageMs,
    string? Note = null,
    FanoutCost? Fanout = null,
    bool Abandoned = false,
    bool ServerPhasesMeasured = false,
    long ServerOpenMs = 0,
    long ServerDrainMs = 0,
    long ServerWatermarkMs = 0,
    long ServerRowsRead = -1,
    long ServerBytesRead = -1,
    long ServerLastReadMs = -1,
    int? TargetSessionId = null)
{
    /// <summary>
    /// The part of <see cref="SqlMs"/> that is neither the open nor the drain — query building, command
    /// construction, the optional probe-failure rowset and the supplemental query. Computed as the residual so
    /// the printed terms SUM to <see cref="SqlMs"/> by construction rather than approximately, which is the
    /// whole point of splitting it (#2811's argument, one seam over). A large value here is itself the finding:
    /// it would mean the cost sits in our own code between the phases, not in the target.
    /// Clamped at zero — the phases run on separate stopwatches, so tiny skew must never print negative.
    /// </summary>
    public long ServerOtherMs => Math.Max(0, SqlMs - ServerOpenMs - ServerDrainMs);

    /// <summary>
    /// The three phase figures as one value for the collection_log write (V108), or NULL when this path did
    /// not measure them. Gated on <see cref="ServerPhasesMeasured"/> rather than on any figure being
    /// non-zero, for the reason that flag exists: a genuinely instant open must record as 0, not vanish.
    ///
    /// <para>ONE value rather than three loose nullables so a caller cannot persist half a split - the
    /// FanoutCost discipline, where a slowest item without its count is worse than no answer at all.
    /// <see cref="ServerOtherMs"/> is deliberately absent: it is a residual defined against
    /// <see cref="SqlMs"/>, and a stored copy could drift from the column it is supposed to complete.
    /// Readers subtract, exactly as the property above does.</para>
    ///
    /// <para>Narrowing to int matches the collection_log columns, which are integer like sql_duration_ms
    /// itself; a phase would have to exceed 24 days to overflow, and the #2673 wall-clock budget abandons
    /// the cycle at 120 seconds.</para>
    /// </summary>
    public ServerPhaseCost? ServerPhases => ServerPhasesMeasured
        ? new ServerPhaseCost((int)ServerOpenMs, (int)ServerDrainMs, (int)ServerWatermarkMs)
        : null;

    /// <summary>
    /// What the drain actually delivered (V109, #2864), or NULL when this path did not measure it. Gated on
    /// <see cref="ServerPhasesMeasured"/> — the same flag and the same path, since the counting reader is
    /// installed exactly where the phase stopwatches are.
    ///
    /// <para>One value rather than four loose fields, the <see cref="ServerPhases"/> discipline: a row count
    /// without its last-read reading is the half-answer this issue was filed about. The two are only
    /// meaningful together — a positive count says rows arrived, and only the elapsed reading says whether
    /// they were still arriving when the budget fired.</para>
    /// </summary>
    public DrainForensics? Drain => ServerPhasesMeasured
        ? new DrainForensics(ServerRowsRead, ServerBytesRead, ServerLastReadMs, TargetSessionId)
        : null;
}

/// <summary>
/// What a server-scoped drain delivered, as persisted by V109 (#2864).
///
/// <para><c>rows_collected</c> answers what a run STORED; an abandoned cycle stores nothing, so it is 0
/// whether the target sent no rows at all or sent 149 and then went silent. These four say what actually
/// arrived, so that distinction — a target that could not execute versus a stream that stalled — survives
/// into the store instead of being lost with the cycle.</para>
///
/// <para><paramref name="LastReadMs"/> is the one that carries the diagnosis: subtracted from the drain it
/// gives the time the reader spent with nothing arriving, which is what separates a slow stream from a
/// stalled one when both end at the wall-clock budget. -1 on any figure means unmeasured or never-happened,
/// never a real measurement, so an absence cannot read as a fast zero.</para>
/// </summary>
public readonly record struct DrainForensics(long RowsRead, long BytesRead, long LastReadMs, int? TargetSessionId);

/// <summary>
/// One server-scoped collector run's phase split, as persisted by V108: the open and the drain that
/// decompose <c>sql_duration_ms</c>, and the watermark read that deliberately does NOT (it runs before that
/// stopwatch starts - see <see cref="CollectorRunResult.ServerWatermarkMs"/>). All three or none, which is
/// what makes the stored triple readable: a row with an open but no drain would be un-interpretable.
/// </summary>
public readonly record struct ServerPhaseCost(int OpenMs, int DrainMs, int WatermarkMs);

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

    /* Feeds the procedure_stats plan-capture cadence (#2862) — how many collection cycles pass between
       plan renders for that ONE collector. Provider-read for the same reason as the knobs above: the
       value is re-read on every cycle, so whichever source ends up feeding it is honored on the NEXT
       cycle without rebuilding the runner. 1 (and anything below) means capture on every cycle, which is
       byte-identical to the pre-#2862 collector. Lite has no equivalent and never sets CapturePlanXml at
       all, so it is unaffected either way. */
    private readonly Func<int> _procedureStatsPlanCycleInterval;

    /// <summary>
    /// Per-(server, collector) cycle counter for the #2862 plan-capture cadence. In-memory, and lost on a
    /// service restart — deliberately, and harmlessly, which is the whole reason this needs no stored
    /// state: the fleet STAGGER comes from the server id (see <see cref="ShouldCapturePlanThisCycle"/>),
    /// not from accumulated drift, so a fleet-wide restart cannot bunch every server onto the same capture
    /// cycle. Persisting the counter in collect.collector_state would buy nothing for that and would cost a
    /// store write per server per cycle on the hot path.
    ///
    /// <para>The only cost of the reset is that each server's first post-restart capture lands within one
    /// interval of the restart rather than continuing its old phase.</para>
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Collector), long> _planCadenceCycles = new();

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
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), QueryStorePlanXmlState.PlanSizeEstimate> _observedPlanSize = new();

    /// <summary>
    /// Per-database, per-COLLECTOR ids the activity-driven fetch (#2312 Finding 2) still owes the store
    /// — see <see cref="FetchStateKey"/> for why the collector belongs in that key (#2902). Probed
    /// missing in an earlier cycle but deferred by the candidate cap or the byte budget. Carried IN MEMORY
    /// because the probe's input is each cycle's batch references, and a plan referenced once — its delta
    /// rows shipped, never executed again — would otherwise never re-enter the probe and never get its
    /// XML. The honest costs of in-memory: a restart forgets the debt, and the ids re-enter only if their
    /// plans execute again — for the literal-churn plans that dominate deferrals, XML nobody can reach
    /// from a fact is the cheap thing to lose. Bounded: ids are 8 bytes and a first-contact backlog is one
    /// catalog's worth.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), long[]> _planFetchCarryover = new();

    /// <summary>Text twin of <see cref="_planFetchCarryover"/> — same deferral contract, keyed by query_id.</summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), long[]> _textFetchCarryover = new();

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
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), int> _textFetchFailures = new();

    /// <summary>
    /// The key every one of the four fetch-state dictionaries above is read and written under. The COLLECTOR
    /// is part of it, and that is the fix rather than a detail (#2902).
    ///
    /// <para><b>Why it was wrong without it.</b> The fetch's gate is a SKU flag —
    /// <see cref="CollectorContext.CapturePlanXml"/> for plans and
    /// <see cref="CollectorContext.FetchQueryTextSeparately"/> for text — not a collector identity, so
    /// every collector that takes the enumerated path reaches it: five do off Azure, and only one of them
    /// (query_store) can ever CREATE this state, because <see cref="ExtractPlanReferences"/> and its text
    /// twin extract nothing from any other collector's batch. Keyed by database alone, the other four found
    /// query_store's deferred ids waiting under the key they happened to share, drained them, and had the
    /// fetch's wall clock recorded against their own <c>collection_log</c> row. Measured over 38 h on one
    /// monitoring host: ~346 s of Query Store fetch time billed to plan_correction, query_store_health and
    /// index_object_stats, with the signature that admits no other reading — <c>probed = 0</c> with
    /// <c>ids &gt; 0</c>, a collector that fetched ids it never probed for.</para>
    ///
    /// <para>The key shape came from <see cref="_consecutiveQueryStoreItemFailures"/>, which is per-database
    /// for the same reason and is safe that way only because every one of ITS call sites sits behind an
    /// explicit <c>definition.Name == query_store</c> check. The shape was inherited; the guard was not.
    /// Holding the collector in the key rather than re-testing the name at the fetch's call site keeps the
    /// invariant in the data structure the state lives in, which is the one place it cannot drift from.</para>
    ///
    /// <para>A method rather than an inline tuple literal so the #2902 pin drives the SHIPPED construction
    /// instead of a copy of it — an inline literal at each site is exactly how the collector went missing.
    /// Ordinal comparison (the tuple's default for strings) matches the <c>StringComparison.Ordinal</c>
    /// every <c>definition.Name</c> comparison in this file already uses.</para>
    /// </summary>
    internal static (int ServerId, string Database, string Collector) FetchStateKey(
        int serverId,
        string databaseName,
        string collectorName)
        => (serverId, databaseName, collectorName);

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

    /// <summary>The one collector the #2862 cadence gate applies to. Named rather than inferred from
    /// &quot;does it capture plans&quot;, because query_stats, query_store, deadlocks and
    /// blocked_process_report all capture plans too and are deliberately NOT gated: this is a targeted
    /// response to one collector's measured cost, not a fleet-wide policy change.</summary>
    internal const string PlanCadenceGatedCollector = "procedure_stats";

    /// <summary>
    /// Whether a given cycle captures plan XML under the #2862 cadence. Pure — no clock, no I/O, no state —
    /// so the policy is unit-testable without a host, per the house rule for scheduling decisions.
    ///
    /// <para><paramref name="interval"/> at or below 1 captures on EVERY cycle, which is byte-identical to
    /// the pre-#2862 collector; that is what makes the knob safely reversible.</para>
    ///
    /// <para><b>The phase is derived from the server id, and that is the whole point.</b> A bare
    /// <c>ordinal % interval</c> would put every server in the fleet on the SAME capture cycle: 42 servers
    /// would each skip three cycles and then all pay full plan-render cost together, converting a steady
    /// load into a 4x spike every fourth cycle — worse for the monitored servers than collecting every
    /// time, because peak is what produces the 120 s wall-clock abandonments. Offsetting by
    /// <c>serverId % interval</c> spreads the fleet evenly across the interval instead, and does so
    /// deterministically, so it survives a restart that resets the counters.</para>
    /// </summary>
    internal static bool ShouldCapturePlanThisCycle(long cycleOrdinal, int serverId, int interval)
    {
        if (interval <= 1)
        {
            return true;
        }

        /* Unsigned so a negative server id (none exist today, but the column is a signed integer and this
           must not throw or bias if one ever does) still lands in [0, interval). */
        var phase = (long)((uint)serverId % (uint)interval);
        return (cycleOrdinal + phase) % interval == 0;
    }

    /// <summary>
    /// The complete plan-capture decision for one collector on one server on this cycle: the SKU flag
    /// AND the #2862 cadence gate. This is what <see cref="CollectorContext.CapturePlanXml"/> is set from,
    /// and it exists as ONE named seam rather than as a <c>&amp;&amp;</c> inside the context initializer so the
    /// decision is reachable from a test — a bare conjunction there is droppable by a refactor with every
    /// pin still green, which is the failure mode this shape removes.
    /// </summary>
    internal bool ShouldCapturePlanXmlFor(string collectorName, int serverId) =>
        _capturePlans() && ShouldCapturePlanForCollector(collectorName, serverId);

    /// <summary>
    /// The instance side of the #2862 cadence: advances this (server, collector) cycle counter and asks the
    /// pure policy. Returns true unconditionally for every collector except
    /// <see cref="PlanCadenceGatedCollector"/>, so no other collector's behaviour changes and no other
    /// collector's counter is even allocated.
    /// </summary>
    private bool ShouldCapturePlanForCollector(string collectorName, int serverId)
    {
        if (!string.Equals(collectorName, PlanCadenceGatedCollector, StringComparison.Ordinal))
        {
            return true;
        }

        var interval = _procedureStatsPlanCycleInterval();
        if (interval <= 1)
        {
            return true;
        }

        /* AddOrUpdate returns the STORED value, so the first cycle for a (server, collector) is ordinal 0
           and each later cycle is one more. Concurrent sweeps of the same server do not overlap for one
           collector, but the dictionary is concurrent because different servers are swept in parallel. */
        var ordinal = _planCadenceCycles.AddOrUpdate((serverId, collectorName), 0L, static (_, previous) => previous + 1);
        return ShouldCapturePlanThisCycle(ordinal, serverId, interval);
    }

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
    /// <param name="procedureStatsPlanCycleInterval">
    /// Live provider for the #2862 procedure_stats plan-capture cadence; null defaults to 1, which is
    /// every cycle and therefore the pre-#2862 collector. Every existing caller and test keeps the
    /// collector it already had without naming the knob.
    /// </param>
    public DarlingCollectorRunner(NpgsqlDataSource postgres, CollectorDeltaCalculator deltas, ILogger? logger = null, Func<bool>? capturePlans = null, Func<bool>? collectSchemaChanges = null, Func<int>? textBudgetMb = null, Func<bool>? compressPlanContent = null, Func<int>? procedureStatsPlanCycleInterval = null)
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
        /* Null provider = 1 = capture a plan on every cycle, i.e. the pre-#2862 behaviour. */
        _procedureStatsPlanCycleInterval = procedureStatsPlanCycleInterval ?? (() => 1);
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
           read from Postgres (Lite reads DuckDB here).

           #2344's read floor, applied to the SERVER-scoped read the way it already is to the
           per-database one below. Name-guarded on the same collector and for the same reason
           WatermarkPolicy's remarks give: the bound is only sound where the caller CLAMPS, and a
           ring-buffer source whose legitimate catch-up spans days must keep reading its whole history.
           The clamp and the bound travel together. query_store declares both watermark columns, so
           before this guard it paid the unbounded server-scoped cost on top of the bounded
           per-database reads — the 2,092-cancellations-a-day the method's remarks record. */
        var serverReadFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            ? WatermarkPolicy.ReadFloor(collectionTime)
            : null;

        var excludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>();

        /* #2797: on the two FAN-OUT paths the answer to this read is thrown away — both of them overwrite
           context.Watermark with a per-database value before any query is built — so skip the round trip
           there, and only there. ServerWatermarkIsDiscarded holds the predicate and the argument for why it
           is a DISPATCH-PATH question rather than a watermark-column one.

           Note what this does to the read floor just above: query_store is the only collector that guard
           names, and the gate is true for query_store on EVERY target (it runs per database on Azure and
           enumerates everywhere else), so the floor it computes is now unused. Kept rather than deleted
           because it is the BOUND on this read, and removing it would make #2796's correctness depend on
           #2797's gate continuing to fire — two unrelated conditions that must not become one.

           The probe context exists because the real one cannot be built yet: hasCollectedBefore below is
           computed FROM this read, and CollectorContext.HasCollectedBefore/NumericWatermark/State are all
           init-only, so constructing the cycle's context ahead of the read would mean widening three
           deliberately-immutable members on a type both SKUs and every definition share. It carries the
           real Target and the real ExcludedDatabases — everything the five BuildEnumerationQuery
           implementations actually read — and nothing this read produces. */
        var dispatchProbe = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = _deltas,
            Target = server.Target,
            ExcludedDatabases = excludedDatabases,
        };
        var serverWatermarkDiscarded = ServerWatermarkIsDiscarded(definition, dispatchProbe);

        /* #2851: timed because this is a STORE round trip that the server-scoped path's sql: stopwatch does
           not cover — it runs before that stopwatch starts. #2796 measured a sibling store read at 50s cold
           on a bounded-only-by-luck predicate, so "the watermark read is free" is an assumption worth
           holding a number against rather than believing. finally, not a trailing assignment (#2816): a
           throwing read must still report how long it ran. Zero when the definition declares no watermark
           column, or when #2797's gate skipped the read, which is honest either way — no read happened. */
        var serverWatermarkWatch = Stopwatch.StartNew();
        long serverWatermarkMs;
        DateTime? watermark;
        try
        {
            watermark = definition.WatermarkColumn is null || serverWatermarkDiscarded
                ? null
                : await GetLastCollectedTimeAsync(server.ServerId, definition.TargetTable, definition.WatermarkColumn, cancellationToken, serverReadFloor);
        }
        finally
        {
            serverWatermarkMs = serverWatermarkWatch.ElapsedMilliseconds;
        }

        /* Numeric (bigint) watermark = the newest already-collected value of the definition's monotonic
           identity column (job_history's instance_id), read from Postgres — the bigint twin of the timestamp
           watermark above. Null for every collector that declares no numeric watermark (the common case),
           so no extra query runs for them. */
        long? numericWatermark = definition.NumericWatermarkColumn is null
            ? null
            : await GetLastCollectedInstanceIdAsync(server.ServerId, definition.TargetTable, definition.NumericWatermarkColumn, cancellationToken);

        /* Only when the watermark came back null: tell a TRUE first run from a store merely emptied by
           retention, so default_trace_events uses a bounded window instead of re-scanning all .trc history
           (CollectorContext.HasCollectedBefore). Skipped in the common (non-null watermark) path.

           #2797 gates this on the SAME flag, and it is not incidental: this branch keys off `watermark is
           null`, so skipping the read above would otherwise READ AS A FIRST RUN and fire this store query
           on precisely the cycles the gate exists to make cheaper — trading one round trip for another and
           netting nothing. The two consumers of HasCollectedBefore (default_trace_events, job_history)
           declare no PerDatabaseWatermarkColumn, so the flag is false for both on every target and neither
           loses the signal. */
        bool hasCollectedBefore = definition.WatermarkColumn is not null
            && !serverWatermarkDiscarded
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
            /* The same array the #2797 dispatch probe above was built with, deliberately shared rather than
               re-derived: the probe's answer is only sound if it saw the exclusions this cycle will actually
               use, and two independent reads of server.Config could disagree. */
            ExcludedDatabases = excludedDatabases,
            PerfmonCounterOverride = null,
            /* #2862: plan capture is additionally cadence-gated for procedure_stats — see
               ShouldCapturePlanForCollector. Every other collector reads exactly _capturePlans().
               Only this path is gated: FetchRowsAsync below is the on-demand live fetch, which an
               operator asked for by name and which stores nothing, so it always renders. */
            CapturePlanXml = ShouldCapturePlanXmlFor(definition.Name, server.ServerId),
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

        /* #2851: whether the server-scoped branch ran at all. The phase VALUES live on the context (next
           to PerItemOpenMs, the enumerated twin) so the stamps are property setters a reachability pin can
           see; only this flag needs method scope, because the success return sits outside the branch that
           sets it. Left false on the enumerated and Azure branches, which have their own per-item split
           (#2164) and no server-scoped line to hang this one off — the flag, not the values, is what tells
           a reader which of those two situations produced a zero. */
        bool serverPhasesMeasured = false;

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

                    /* #2855: cleared BEFORE the connect, once per iteration. This loop reuses ONE context
                       across every database, so without the reset a database whose connect faults would
                       print the PREVIOUS database's split as its own — and a stale timing that looks
                       precise is worse than no timing at all. Same rule, and the same reason, as the
                       enumerated readItem's clear of its own stamps. */
                    context.PerDatabaseConnectMs = 0;
                    context.PerDatabaseOpenMs = 0;
                    context.PerDatabaseDrainMs = 0;
                    context.PerDatabasePhasesMeasured = false;

                    /* dbToken, not cancellationToken (#2150): connect, execute and drain are the phases the
                       budget bounds. The FLUSH below deliberately stays on cancellationToken — abandoning a
                       write already in flight would trade a slow cycle for a partially-written one. */
                    /* #2855: the CONNECT is a phase in its own right here, and only here. This branch opens a
                       connection per database, so on Azure SQL DB every cycle pays a fresh login per
                       database — a cost that had nowhere to appear and therefore sat inside the blended
                       number with everything else. Timed around OpenDatabaseConnectionAsync alone, so
                       command construction lands in the residual rather than in a phase that is supposed to
                       mean "reaching the database".

                       Stamped from a finally, with the flag, for the #2854 reason: a login that times out or
                       a per-database budget that fires during the handshake is exactly the case this exists
                       to attribute, and a trailing assignment is skipped on precisely that path. The stamp
                       would then read connect:0ms and hand its whole cost to other: — the one term whose job
                       is to be small and unattributed. The flag rides the same finally so the two can never
                       disagree about whether a measurement happened. */
                    var connectWatch = Stopwatch.StartNew();
                    DbConnection openedConnection;
                    try
                    {
                        openedConnection = await OpenDatabaseConnectionAsync(perDbProvider, server, databaseName, dbToken);
                    }
                    finally
                    {
                        context.PerDatabaseConnectMs = connectWatch.ElapsedMilliseconds;
                        context.PerDatabasePhasesMeasured = true;
                    }

                    using (var dbConnection = openedConnection)
                    using (var dbCommand = CreateCollectorCommand(perDbProvider, dbPlan, dbConnection, perDbTimeout))
                    {
                        /* #2855: the open, same contract as the other two paths — ExecuteReaderAsync returns
                           only when the first rowset is available, so this is server-side work before the
                           first row, which no client-side budget shortens. Hoisted out of the `using` header
                           only so the reader below keeps its original disposal scope. */
                        var openWatch = Stopwatch.StartNew();
                        DbDataReader openedReader;
                        try
                        {
                            openedReader = await dbCommand.ExecuteReaderAsync(dbToken);
                        }
                        finally
                        {
                            context.PerDatabaseOpenMs = openWatch.ElapsedMilliseconds;
                        }

                        using var dbReader = openedReader;

                        /* #2855: the drain, measured rather than inferred — the ServerScopeDrainMs argument.
                           A residual drain would silently absorb command construction and the trailing
                           probe-failure rowset below, and then a large other: would say nothing about our
                           own code. From a finally so a budget expiry mid-stream reports how far it got. */
                        var drainWatch = Stopwatch.StartNew();
                        try
                        {
                            batch = await definition.ReadAsync(dbReader, context, dbToken);
                        }
                        finally
                        {
                            context.PerDatabaseDrainMs = drainWatch.ElapsedMilliseconds;
                        }

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

                    /* #2855: this branch's per-database phase line — the line it did not emit at all. Read
                       after the flush, like the other two paths, so pg: is this database's own figure.

                       Gated on the split being present rather than on a figure being non-zero, and the
                       null-or-value decision lives on the context so "no split" and "no line" are one
                       decision made once. A `connect: > 0` gate would suppress the whole line for a pooled
                       connect that really did cost nothing — the case where the rest of the line is most
                       worth reading, and the mistake #2854 had to undo on the enumerated path.

                       NOT gated on batch.Count > 0, unlike the enumerated path's #1565 quiet-on-zero rule.
                       That rule guards a ROW-COUNT line, whose payload is the row count, and a zero-row one
                       carries nothing. This is a PHASE line, and its payload — the connect above all — is
                       paid on a quiet database exactly as on a busy one. The server-scoped phase line is the
                       closer sibling and prints for every measured run regardless of rows; suppressing quiet
                       databases here would leave them emitting nothing whatsoever, which is the state this
                       issue is about.

                       Its OWN line rather than folded into anything, the #2811/#2851 rule: these lines are
                       parsed by tooling outside this repo, so "don't break the parser" outranks "one line to
                       grep". Nothing here is persisted — a per-database split is N:1 against collection_log
                       and shaping that is the open decision in #2860.

                       KNOWN LATENCY, stated rather than implied: this site is on the SUCCESS path, so a
                       database whose connect times out stamps its phases and sets the flag (both from the
                       finally above) and then reaches a catch arm that prints no split. Same standing as the
                       enumerated path's #2854 note — the stamp is worth having regardless, because it is
                       correct and it survives on the context into the catch, so emitting from the fault path
                       is a later addition rather than a re-instrumentation. It is NOT done here because the
                       parent this line decomposes is not available in the catch: sqlSlice is declared inside
                       the try, and hoisting it above the watermark read and BuildQuery would silently widen
                       dbSqlMs — which feeds sqlMs, the fan-out rollup and collection_log's sql_duration_ms.
                       So the choices are a second stopwatch or a second line shape, and neither belongs in a
                       change whose scope is the split itself. */
                    if (context.PerDatabasePhasesFrom(dbSqlMs) is { } dbPhases)
                    {
                        _logger?.LogInformation(
                            "  [{Server}] {Collector} [{Database}] sql:{SqlMs}ms = connect:{ConnectMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms ({Rows} rows, pg:{PgMs}ms)",
                            server.Config.DisplayName, definition.Name, databaseName, dbSqlMs,
                            dbPhases.ConnectMs, dbPhases.OpenMs, dbPhases.DrainMs, dbPhases.OtherMs,
                            batch.Count, dbStorageMs);
                    }

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
                   database on it, flushing each before reading the next.

                   #2819: "for the whole body" is now literally true. The Query Store plan and text fetches
                   used to open their own on every item — a third and a fourth connection per database, ~228
                   acquisitions per cycle against MaxPoolSize=24, at a measured 673-893ms floor each — and
                   they now borrow this one. Safe precisely because of the flushing order named above: the
                   driver reads an item and then awaits its write, so this connection is idle for the whole
                   read, which is when those fetches run. */
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
                            /* #2854: hoisted and stamped from finally, not assigned after the awaits below.
                               A trailing assignment is SKIPPED when an await throws, and this delegate's
                               awaits are store reads and a store write — precisely the ones #2796 found being
                               cancelled.

                               LATENT rather than live, and the distinction is worth writing down: a throwing
                               watermark refresh propagates out of the driver's per-item try, which routes to
                               onItemError, leaves `batch` null and `continue`s past onItemComplete
                               (EnumeratedCollectorDriver.cs:635-702). onItemComplete is the ONLY reader of
                               these fields, so that item prints no split line at all and the bad number never
                               reaches a log. Stamped from finally anyway, because the field's honesty should
                               not rest on a caller two files away continuing to discard the item, and because
                               the pin derives its set from CollectorContext and requires every phase stamp to
                               be handler-reachable. */
                            var watermarkWatch = Stopwatch.StartNew();
                            try
                            {
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

                            }
                            finally
                            {
                                context.PerItemWatermarkMs = watermarkWatch.ElapsedMilliseconds;
                            }
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
                        /* #2811: the sub-phases clear on the SAME rule as their parents, and for the same
                           reason — an item whose fetch faults before setting them must not print the previous
                           database's split as its own. A stale sub-split is worse than a stale total, because
                           it looks precise. */
                        context.PerItemPlanProbeMs = 0;
                        context.PerItemPlanTargetMs = 0;
                        context.PerItemPlanWriteMs = 0;
                        context.PerItemPlanChunks = 0;
                        context.PerItemPlanIdsAttempted = 0;
                        context.PerItemPlanProbeIds = 0;
                        context.PerItemTextProbeMs = 0;
                        context.PerItemTextTargetMs = 0;
                        context.PerItemTextWriteMs = 0;
                        context.PerItemTextChunks = 0;
                        context.PerItemTextIdsAttempted = 0;
                        context.PerItemTextProbeIds = 0;
                        context.PerItemPhasesMeasured = false;
                        /* #2854: stamped from finally, and the reader is hoisted out of the try only so the
                           `using` below keeps its original disposal scope. A trailing assignment here was
                           skipped whenever the open threw — a command timeout, a cancelled budget.

                           Latent for the same reason as the watermark above: a throwing open never reaches
                           onItemComplete, so the reading it would have produced cannot currently print. What
                           it WOULD have produced is why the stamp is worth fixing regardless — DrainMsFrom
                           subtracts open from the item total, so an unstamped 500s open hands its whole cost
                           to drain: and blames row streaming for a statement that never returned a row. The
                           flag is set in the same finally so the two can never disagree. */
                        var openWatch = Stopwatch.StartNew();
                        DbDataReader openedReader;
                        try
                        {
                            openedReader = await itemCommand.ExecuteReaderAsync(ct);
                        }
                        finally
                        {
                            context.PerItemOpenMs = openWatch.ElapsedMilliseconds;
                            context.PerItemPhasesMeasured = true;
                        }
                        using var itemReader = openedReader;
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
                            /* #2854: stamped from finally. This one is the PARENT of the sub-split #2816
                               already fixed, which makes a bare stamp here worse than the defect it fixed:
                               probe/target/write stamp from their own handlers and report real values, so a
                               throwing fetch printed plan_fetch:0ms above non-zero children. PlanFetchOtherMs
                               then clamps a negative residual to zero and the line reads as precise while
                               being arithmetically impossible. */
                            var planFetchWatch = Stopwatch.StartNew();
                            try
                            {
                                await FetchAndStorePlansAsync(planFetchConnection, pgConnection,
                                    server, item, definition.Name, context, itemTimeout, ExtractPlanReferences(batch), ct);
                            }
                            finally
                            {
                                context.PerItemPlanFetchMs = planFetchWatch.ElapsedMilliseconds;
                            }
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
                            /* #2854: stamped from finally, same parent/child inconsistency as the plan fetch. */
                            var textFetchWatch = Stopwatch.StartNew();
                            try
                            {
                                await FetchAndStoreQueryTextAsync(textFetchConnection, pgConnection,
                                    server, item, definition.Name, context, itemTimeout, ExtractTextReferences(batch), ct);
                            }
                            finally
                            {
                                context.PerItemTextFetchMs = textFetchWatch.ElapsedMilliseconds;
                            }
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
                               budget and the link are the levers. Only emitted when the host measured it.

                               #2854: the gate is the FLAG, not `PerItemOpenMs > 0`. That form conflated a
                               genuinely instant open with a path that measured nothing, and suppressed the
                               whole split for the former — a real loss, because a fast open is exactly the
                               item whose drain: number is worth reading. No value alone can separate those
                               two states; a flag set beside the stamp can. */
                            if (context.PerItemPhasesMeasured)
                            {
                                /* #2312: the fetch phases print only when a separate fetch actually ran,
                                   so every other collector's line is byte-identical to before. */
                                if (context.PerItemPlanFetchMs > 0 || context.PerItemTextFetchMs > 0)
                                {
                                    _logger?.LogInformation("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms = wm:{WatermarkMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + plan_fetch:{PlanFetchMs}ms + text_fetch:{TextFetchMs}ms, pg:{PgMs}ms)",
                                        server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs,
                                        context.PerItemWatermarkMs, context.PerItemOpenMs, context.DrainMsFrom(itemSqlMs),
                                        context.PerItemPlanFetchMs, context.PerItemTextFetchMs, itemStorageMs);

                                    /* #2811: the sub-split rides its OWN line rather than nesting inside the
                                       one above, because that line is parsed by tooling outside this repo and
                                       "don't break the parser" outranks "one line to grep". Emitted only when
                                       the corresponding fetch actually ran, so a text-only pass prints one
                                       line and a fetchless collector prints none. */
                                    if (context.PerItemPlanFetchMs > 0)
                                    {
                                        _logger?.LogInformation("  [{Server}] {Collector} [{Database}] plan_fetch:{PlanFetchMs}ms = probe:{ProbeMs}ms + target:{TargetMs}ms + write:{WriteMs}ms + other:{OtherMs}ms ({Chunks} chunk(s), {Ids} ids, {ProbeIds} probed)",
                                            server.Config.DisplayName, definition.Name, item, context.PerItemPlanFetchMs,
                                            context.PerItemPlanProbeMs, context.PerItemPlanTargetMs, context.PerItemPlanWriteMs,
                                            context.PlanFetchOtherMs, context.PerItemPlanChunks, context.PerItemPlanIdsAttempted,
                                            context.PerItemPlanProbeIds);
                                    }

                                    if (context.PerItemTextFetchMs > 0)
                                    {
                                        _logger?.LogInformation("  [{Server}] {Collector} [{Database}] text_fetch:{TextFetchMs}ms = probe:{ProbeMs}ms + target:{TargetMs}ms + write:{WriteMs}ms + other:{OtherMs}ms ({Chunks} chunk(s), {Ids} ids, {ProbeIds} probed)",
                                            server.Config.DisplayName, definition.Name, item, context.PerItemTextFetchMs,
                                            context.PerItemTextProbeMs, context.PerItemTextTargetMs, context.PerItemTextWriteMs,
                                            context.TextFetchOtherMs, context.PerItemTextChunks, context.PerItemTextIdsAttempted,
                                            context.PerItemTextProbeIds);
                                    }
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
                /* #2851: this branch IS the server-scoped path, so its phases are measured from here on.
                   Set before the read rather than after it so the wall-clock-budget catch below reports the
                   phases of the cycle it abandoned — that is the case where "where did the time go" matters
                   most, and the one a trailing assignment would leave at zero. */
                serverPhasesMeasured = true;

                using var itemBudget = EnumeratedCollectorDriver.StartItemBudget(definition.PerItemWallClockBudget, cancellationToken);
                var itemToken = itemBudget?.Token ?? cancellationToken;
                try
                {
                    using var command = CreateCollectorCommand(targetProvider, plan, targetConnection, definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds);

                    /* Both phases stamp from finally rather than after the await (#2816). A throwing open or
                       a drain cut short by the budget must still report the time it burned; the alternative
                       is that its milliseconds land in the residual, and the residual is the one term whose
                       whole job is to be small and unattributed. 97% of a day's residual budget was one such
                       misattribution the last time this was got wrong. */
                    var openWatch = Stopwatch.StartNew();
                    DbDataReader opened;
                    try
                    {
                        opened = await command.ExecuteReaderAsync(itemToken);
                    }
                    finally
                    {
                        context.ServerScopeOpenMs = openWatch.ElapsedMilliseconds;
                    }

                    using var reader = opened;

                    /* #2864: the target's own session id, read off the connection as a client property rather
                       than asked for with SELECT @@SPID — a round trip would be ~25,000 extra queries an hour
                       across the fleet to fetch a number the client already holds. It is what makes a stalled
                       run joinable to waiting_tasks / dmv_blocking_snapshot / query_snapshots, all of which
                       record a session id; without it "what was OUR session waiting on" cannot be asked
                       retrospectively even for a window where the answering snapshot was captured.

                       Captured HERE, after ExecuteReaderAsync has returned, and not beside the phase flag
                       above (#2884): SqlConnection.ServerProcessId is not reliably populated until the
                       connection has round-tripped a command, so a read placed before the open recorded 0 —
                       a value no real session has — on exactly the abandoned cycles the id exists to explain.
                       Here the round trip has provably happened, so a drain-stall abandon (open completes in
                       ~100-200ms; the budget fires minutes into the read) records its REAL id, which is the
                       load-bearing case. An abandon that fires INSIDE ExecuteReaderAsync leaves this null,
                       which the store reads as NOT RECORDED — the honest answer for a connection that never
                       finished its first exchange, and one of the reachable-NULL cases the V109 write-side
                       comment already documents. */
                    context.TargetSessionId = TryReadTargetSessionId(targetConnection);
                    var drainWatch = Stopwatch.StartNew();

                    /* #2864: the collector reads through a counting decorator rather than the provider reader
                       directly, so what the drain DELIVERED is recorded whether or not the cycle survives to
                       store it. An abandoned cycle ships nothing, so rows_collected is 0 either way and could
                       never separate "the target never sent row 1" from "it sent 149 and stopped" — the first
                       production capture with V108's phases read open:104ms drain:119,945ms rows=0 and could
                       go no further. Decorating rather than editing 66 collectors' read loops means a
                       collector cannot forget to count, and the counters ride the SAME drainWatch so the
                       last-read reading and the drain figure it is subtracted from share one clock. */
                    var counting = new DrainCountingDataReader(reader, drainWatch);
                    try
                    {
                        rows = await definition.ReadAsync(counting, context, itemToken);
                    }
                    finally
                    {
                        context.ServerScopeDrainMs = drainWatch.ElapsedMilliseconds;

                        /* Stamped from finally with the drain itself, and for the same #2816 reason: the run
                           that matters most here is the one cut short, so a count only recorded on the
                           success path would be absent from exactly the cycles it exists to explain. */
                        context.ServerScopeRowsRead = counting.RowsRead;
                        context.ServerScopeBytesRead = counting.PayloadBytes;
                        context.ServerScopeLastReadMs = counting.LastReadElapsedMs;
                    }

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
                    return new CollectorRunResult(
                        0,
                        sqlSlice.ElapsedMilliseconds,
                        0,
                        EnumeratedCollectorDriver.WholeCycleBudgetNote(budgetSeconds),
                        Abandoned: true,
                        /* #2851: the abandoned cycle reports its phases too. A collector that blew a
                           wall-clock budget is precisely the one worth asking "on what" — and because the
                           stamps come from finally above, the phase it died IN is reported rather than lost. */
                        ServerPhasesMeasured: true,
                        ServerOpenMs: context.ServerScopeOpenMs,
                        ServerDrainMs: context.ServerScopeDrainMs,
                        ServerWatermarkMs: serverWatermarkMs,
                        /* #2864, and THIS is the arm the counters were added for: an abandoned cycle stores
                           nothing, so rows_collected is 0 either way and cannot say whether the target sent
                           nothing or sent rows and then stopped. The counting reader stamps from finally, so
                           what arrived before the budget fired survives the abandon. */
                        ServerRowsRead: context.ServerScopeRowsRead,
                        ServerBytesRead: context.ServerScopeBytesRead,
                        ServerLastReadMs: context.ServerScopeLastReadMs,
                        TargetSessionId: context.TargetSessionId);
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
        /* #2864: the drain forensics are recorded on the SUCCESS path too, not only on abandon. The
           abandoned row is the one being explained, but a ratio needs a denominator - "149 rows, last read
           at 500 ms" only reads as pathological against what this collector on this server normally
           delivers, and that baseline has to come from the same column on ordinary rows. Storing it only on
           failures would reproduce exactly the cross-referencing this change exists to remove.

           Kept ABOVE the return rather than inside the argument list: DarlingEmptyEnumerationNoteTests pins
           this call's arguments as one whitespace-collapsed run, so a comment between them breaks a pin
           whose actual job is to catch a DROPPED argument. */
        return new CollectorRunResult(
            rowsWritten, sqlMs, storageMs, collectionNote, fanout.Result,
            ServerPhasesMeasured: serverPhasesMeasured,
            ServerOpenMs: context.ServerScopeOpenMs,
            ServerDrainMs: context.ServerScopeDrainMs,
            ServerWatermarkMs: serverWatermarkMs,
            ServerRowsRead: context.ServerScopeRowsRead,
            ServerBytesRead: context.ServerScopeBytesRead,
            ServerLastReadMs: context.ServerScopeLastReadMs,
            TargetSessionId: context.TargetSessionId);
    }

    /// <summary>
    /// The monitored target's own session id, off the OPEN connection as a property (#2864). SQL Server's
    /// SPID via <c>SqlConnection.ServerProcessId</c>, PostgreSQL's backend pid via
    /// <c>NpgsqlConnection.ProcessID</c>; null for any other provider.
    ///
    /// <para>A property read rather than <c>SELECT @@SPID</c> deliberately: the value is already on the
    /// client after the handshake, and asking for it would add a round trip per collector per server per
    /// cycle — on this fleet roughly 25,000 extra queries an hour to learn something already known. The
    /// point of the number is joinability: waiting_tasks, dmv_blocking_snapshot and query_snapshots all
    /// carry a session id, so recording ours turns "what was our own stalled session waiting on" from an
    /// unanswerable question into a join.</para>
    ///
    /// <para>Best-effort by design. A provider that exposes no such property returns null, which reads as
    /// "not available" — never as a session id of 0, which is a real SPID.</para>
    /// </summary>
    private static int? TryReadTargetSessionId(DbConnection connection)
    {
        /* A session id of 0 is never real — SQL Server assigns no SPID 0 to a user session, and both
           providers report 0 from this property for a connection that has not completed the exchange
           that populates it — so 0 is the provider's "not available" state, not a measurement. #2884
           found it written to the store as though it were data, on exactly the abandoned cycles where
           the id is the join key. Normalized to null here, at the source, so every consumer sees the
           declared NOT-RECORDED convention instead of a real-looking placeholder. */
        var raw = connection switch
        {
            SqlConnection sql => sql.ServerProcessId,
            NpgsqlConnection npgsql => npgsql.ProcessID,
            _ => 0,
        };
        return raw > 0 ? raw : null;
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
    /// The server-scoped watermark SQL, exposed so a pin can assert the SHIPPED string rather than a
    /// retyped copy of it. <paramref name="bounded"/> adds the <c>collection_time</c> predicate — the
    /// partitioning column, and the only thing here that prunes a chunk.
    /// </summary>
    internal static string BuildServerWatermarkSql(string tableName, string columnName, bool bounded) =>
        bounded
            ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND collection_time > $2"
            : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1";

    /// <summary>
    /// True when this cycle will OVERWRITE the server-scoped watermark before any query is built, so
    /// <see cref="GetLastCollectedTimeAsync"/>'s answer would be read by nothing and the round trip can be
    /// skipped (#2797).
    ///
    /// <para><b>This is a DISPATCH-PATH question, not a watermark-column one, and that is the whole issue.</b>
    /// #2797 proposed gating on <c>PerDatabaseWatermarkColumn is not null</c> alone — the signal the per-item
    /// paths already key on. That reads plausibly and would ship a data-correctness bug. Four collectors
    /// declare BOTH watermark columns (query_store, deadlocks, blocked_process_report,
    /// long_query_completions) and all four declare <c>RunsPerDatabase =&gt; target.IsAzureSqlDb</c>, but only
    /// query_store overrides <see cref="ICollectorDefinition{TRow}.BuildEnumerationQuery"/>; the other three
    /// inherit <c>CollectorDefinitionBase</c>'s <c>=&gt; null</c>. So OFF Azure — which is the entire on-prem
    /// and RDS fleet — three of the four fall through to the plain server-scoped path and genuinely CONSUME
    /// this value. Gating on the column alone would hand them a null watermark on every cycle and make them
    /// re-collect their whole first-run window forever: #2795's defect, arrived at from the other side.</para>
    ///
    /// <para><b>The three paths.</b> <see cref="RunAsync"/> dispatches down exactly one of them.
    /// <see cref="ICollectorDefinition{TRow}.RunsPerDatabase"/> opens a connection per database and assigns
    /// <c>context.Watermark</c> from <see cref="GetLastCollectedTimeForDatabaseAsync"/> inside the loop; a
    /// non-null <see cref="ICollectorDefinition{TRow}.BuildEnumerationQuery"/> drives the enumerated loop,
    /// which assigns <c>context.Watermark</c> inside its <c>perItemWatermark</c> delegate; anything else
    /// reads the server-scoped value this method guards. Both fan-out paths wire their per-item refresh only
    /// when BOTH watermark columns are declared, which is why that conjunct belongs here too: a collector
    /// that enumerates with no <c>PerDatabaseWatermarkColumn</c> keeps the server-wide value and still
    /// consumes it.</para>
    ///
    /// <para><b>Derived, not declared, so it cannot drift.</b> The enumeration half asks the definition's own
    /// <c>BuildEnumerationQuery</c> rather than a second flag mirroring it — a hand-maintained "enumerates"
    /// signal would just relocate the bug the moment the two disagreed. The price is that the question is
    /// asked against a probe context rather than the cycle's own (which cannot exist yet — see the call
    /// site), and that is sound only while null-ness is a function of
    /// <see cref="CollectorContext.Target"/> alone. That is not assumed: <c>ServerWatermarkDispatchGateTests</c>
    /// varies Watermark, NumericWatermark, HasCollectedBefore, State and ExcludedDatabases across every
    /// definition in <see cref="CollectorCatalog.All"/> and fails if any of them moves the answer.</para>
    /// </summary>
    internal static bool ServerWatermarkIsDiscarded<TRow>(
        ICollectorDefinition<TRow> definition, CollectorContext dispatchProbe) =>
        definition.WatermarkColumn is not null
        && definition.PerDatabaseWatermarkColumn is not null
        && (definition.RunsPerDatabase(dispatchProbe.Target)
            || definition.BuildEnumerationQuery(dispatchProbe) is not null);

    /// <summary>
    /// Gets the most recent value of a timestamp column from Postgres for incremental collection.
    /// Returns null on first run or if the query fails (caller uses a fallback window) — the
    /// Postgres twin of Lite's GetLastCollectedTimeAsync.
    ///
    /// <para><paramref name="collectedSince"/> bounds the read on <c>collection_time</c> — the
    /// PARTITIONING column, so the bound actually prunes chunks. This is #2344's bound applied to the
    /// SERVER-scoped read; #2344 fixed only the per-database sibling
    /// (<see cref="GetLastCollectedTimeForDatabaseAsync"/>), and query_store declares BOTH watermark
    /// columns, so it kept paying the unbounded cost here on every cycle. Pass
    /// <see cref="WatermarkPolicy.ReadFloor"/> only from a caller whose value is clamped, and read that
    /// method's remarks for why the bound provably changes no answer. Null keeps the unbounded
    /// behaviour, which stays correct for any reader whose watermark is NOT clamped.</para>
    ///
    /// <para>Measured on use1 before the bound (query_store_stats, 62.5 GB, 19 chunks): 40.7 s and
    /// 50.6 s cold, 9.3 s warm, against Npgsql's 30 s default CommandTimeout — so the read was being
    /// cancelled mid-flight, which the store's own log recorded 2,092 times in one day while the
    /// per-database bounded sibling was cancelled 17 times. Every one of those cancellations returned
    /// null here, and null is indistinguishable from a first run: the caller fell back to
    /// query_store's 60-minute window instead of the ~5-minute incremental one, re-collected what it
    /// already had, and grew the table that made the next read slower.</para>
    /// </summary>
    public async Task<DateTime?> GetLastCollectedTimeAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken,
        DateTime? collectedSince = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var sql = BuildServerWatermarkSql(tableName, columnName, collectedSince is not null);
            using var command = new NpgsqlCommand(sql, connection);
            /* Explicit rather than Npgsql's 30 s default: that default was never a decision anyone made
               here, and it silently governed a read whose measured cost exceeded it. */
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Caller still uses its fallback window — but SAY SO. Swallowing this silently is what let
               a permanently-failing watermark read look identical to a healthy first run for an unknown
               length of time: nothing reached collection_log, nothing reached this log, and the only
               trace was a cancellation line in the store's own Postgres log.

               The OperationCanceledException guard is the shape this file already uses at its
               collector-run catches: a cancelled read is a service shutdown, not a watermark failure,
               and logging it as one would put a misleading warning per in-flight collector on every
               normal stop. */
            _logger?.LogWarning(
                "Watermark read failed for server {ServerId} on {Table}.{Column} — falling back to the "
                + "collector's default window, which re-collects data already stored: {Message}",
                serverId, tableName, columnName, ex.Message);
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
    /// Generic because the dispatch loop is; any batch that is not query_store rows extracts nothing, which
    /// #2902 measured happening on every cycle — <c>CapturePlanXml</c> is a SKU flag, so it gates the SKU
    /// and not the collector, and all five enumerating collectors reach this. Extracting nothing is the
    /// right answer for them; what was wrong is that the fetch then read a carryover key they shared.
    ///
    /// <para>When one plan appears in several rows (several intervals), a non-null hash wins over a null
    /// one — the probe compares against whatever the engine reported, and null only means the payload row
    /// predated the hash column.</para>
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
    private void ClearPlanFetchBackoff((int ServerId, string Database, string Collector) carryKey)
    {
        if (_observedPlanSize.TryGetValue(carryKey, out var estimate) && estimate.ConsecutiveFetchFailures > 0)
        {
            _observedPlanSize.TryUpdate(carryKey, QueryStorePlanXmlState.RecordFetchSuccess(estimate), estimate);
        }
    }

    /// <summary>
    /// Puts the BORROWED store connection back into a usable state after a fetch faulted on it (#2819).
    ///
    /// <para><b>Why this is required rather than tidy.</b> The fetches swallow their own exceptions and
    /// return normally, so <c>readItem</c> still hands the driver a non-null batch built from the target rows
    /// it already read. <c>EnumeratedCollectorDriver.RunAsync</c> then calls <c>writeBatch</c> OUTSIDE its
    /// per-item try/catch, and that path is documented to propagate — "a flush failure PROPAGATES, storage
    /// failure is systemic". So a fetch that broke the connection would not cost one database its plan XML;
    /// it would fail the runtime-stats write for that item and abort the REST OF THE SWEEP, every remaining
    /// database for that collector this cycle. While the fetches held private connections that was
    /// structurally impossible, and borrowing must not buy pool headroom at the price of a whole-cycle
    /// abort.</para>
    ///
    /// <para><b>Takes no CancellationToken on purpose.</b> Every call site has one — query_store's per-item
    /// wall-clock budget — and an expired budget is the likeliest reason this runs at all, so reopening
    /// under it would fail on a token check before attempting a connection. Recovery of a connection SHARED
    /// by every remaining database must not be abandoned because one slow database ran out of time.</para>
    ///
    /// <para>A transient fault — a cancelled command, a dropped socket — leaves the pooled connection
    /// unusable but the store perfectly reachable, and that is the case this recovers: reopen, and the
    /// caller's write proceeds as if the fetch had its own connection. If the reopen ALSO fails the store is
    /// genuinely unreachable, which is exactly the systemic condition the driver's propagate-on-flush
    /// behaviour exists for, so the failure is left to travel — swallowed here, surfaced there, unchanged
    /// from before this borrowing.</para>
    /// </summary>
    private async Task<bool> RestoreBorrowedStoreConnectionAsync(
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName)
    {
        if (storeConnection.State == ConnectionState.Open)
        {
            return true;
        }

        try
        {
            /* Close first: Npgsql will not reopen a Broken connection in place, and Close on an already
               closed one is a no-op rather than a fault. */
            await storeConnection.CloseAsync();

            /* CancellationToken.None, deliberately, and this method takes no token so a caller cannot pass
               a cancelled one by reflex. The token available at every call site is query_store's per-item
               wall-clock budget, and an EXPIRED budget is the single most likely reason we are here — so
               reopening under it would make OpenAsync throw OperationCanceledException off a token check
               before it ever attempted a connection. That is recovery failing closed at exactly the moment
               it is needed, and worse than not trying: the OCE would escape this method's own non-OCE catch
               and skip the caller's remaining backoff bookkeeping too.

               One slow database's budget is not a reason to abandon a connection SHARED by every database
               still to come in this sweep. Real shutdown is still respected — it tears the process down
               regardless, and this is one short reconnect, not a loop. */
            await storeConnection.OpenAsync(CancellationToken.None);

            _logger?.LogWarning(
                "Reopened the shared store connection after a Query Store fetch fault on '{Server}' database [{Database}] — the fetch borrows the collector body's connection, and leaving it broken would fail this item's runtime-stats write and abort the rest of the sweep.",
                server.Config.DisplayName, databaseName);

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The store itself is unreachable. Deliberately swallowed: the write that follows will fail on
               the same connection and PROPAGATE, which is the correct handling of a systemic store failure
               and the behaviour that predates this borrowing. Logged so the cause is visible there. */
            _logger?.LogWarning(ex,
                "Could not reopen the shared store connection after a Query Store fetch fault on '{Server}' database [{Database}] — the store looks unreachable, so this cycle's remaining writes will fail systemically.",
                server.Config.DisplayName, databaseName);

            return false;
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
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        string collectorName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long PlanId, string? PlanHash)> references,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776) so the catch can advance this database's backoff counter — the
           handler needs the same key the body uses. Carries the collector since #2902 — see
           FetchStateKey for why the database alone let four other collectors drain this one's debt. */
        var carryKey = FetchStateKey(server.ServerId, databaseName, collectorName);

        /* Hoisted for the same shape of reason (#2816): the probe stamp sits at the END of the probe phase,
           so a probe that THROWS jumps clean over it and reports probe:0ms. The residual then absorbs the
           whole cost into other:, which is documented as this method's own bookkeeping — "the cost would be
           in our own code, not in either database" — and that is the exact INVERSE of the truth for a store
           round trip that timed out. The target and write phases already stamp from finally blocks for
           precisely this reason (#2811); the probe was the one phase that did not. Measured on 2026-09-03:
           one failed probe put 43,053ms into other:, 97% of the whole fleet's other: budget that day. */
        var probeWatch = new Stopwatch();
        var probeStamped = false;

        /* #2819: set by the catch, acted on AFTER it. The restore needs an await, and an await inside a
           catch makes the compiler lift the handler's body out of the exception region — which would break
           #2816's pin that the probe stamp is reachable from inside a handler, and with it the guarantee
           that a throwing probe reports its own time instead of donating it to other:. Flag here, await
           below: the property #2816 holds and the recovery #2819 needs are not in tension, they just cannot
           share a block. */
        var storeConnectionSuspect = false;

        try
        {
            /* FIRST statement in the try, ahead of the quiet-cycle return below, and that ordering is
               the whole point. The catch further down filters out OperationCanceledException, and OCE is
               the fault most likely here: this method's token is the per-item wall-clock budget, not host
               shutdown, and Npgsql throws OCE when a caller-supplied token fires. So a database that simply
               ran slow mid-probe cancels, propagates past that catch, and never reaches the restore at the
               bottom — leaving the BORROWED connection broken for whoever runs next.

               Putting the repair after the early return would have missed exactly the case that matters.
               "Nothing referenced, nothing owed" is derived from this database's own just-read rows and is
               completely independent of whether it has runtime stats to WRITE, so a busy-but-stable database
               routinely returns early here while still handing the driver a non-empty batch. writeBatch then
               runs on the still-broken connection, outside any per-item try/catch, and propagates — aborting
               the rest of the sweep. That is precisely the whole-cycle abort this borrowing must not buy.

               Classifying on connection STATE rather than exception type is the discipline
               EnumeratedCollectorDriver.ItemBudgetExpired already applies to the budget question: the tokens
               and the connection know, the exception type does not. Free when nothing is wrong — the helper
               returns immediately on an Open connection — and outside the probe measurement on purpose,
               since a reopen is recovery rather than store work. */
            /* Timed into probe:, not left to other:. A reconnect is store-connection time — the very cost
               this PR removed from the steady-state path — and other: is documented as work in NEITHER
               database, so letting a reopen land there would recreate the misattribution #2816 fixed for
               the probe stamp. Zero on the ordinary pass: the helper returns on a state check. */
            probeWatch.Restart();
            if (!await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName))
            {
                /* The store is still down. Returning beats falling through: the probe below would fault on
                   the same Broken connection, log a second "could not reopen" for one underlying outage,
                   and the text sibling would double it again per item. This codebase caps repetitive
                   failure logging deliberately (MaxLoggedProbeFailures), and attempting store work already
                   known to be doomed is not diagnosis. The carryover is untouched, so nothing is forgotten
                   and the next cycle re-selects it.

                   Stamped before returning, like every other exit from this method: the failed reconnect
                   still took real wall clock, the caller's fetch watch still counted it, and leaving it
                   unstamped would hand the one measurable part of an outage to the other: residual. */
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
                return;
            }

            var hasCarryover = _planFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* The steady quiet cycle: nothing referenced, nothing owed. Zero store reads, zero target
                   queries — the whole point of the reshape.

                   Stamped anyway, matching the missing.Count == 0 return below. Normally this is 0ms, but
                   the repair above CAN have reconnected a connection a previous item broke, and that is
                   real store time: leaving it unstamped would drop it into the other: residual, which is
                   documented as work in neither database. A quiet cycle should read as free because it WAS
                   free, not because its one real cost went unattributed. */
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
                ClearPlanFetchBackoff(carryKey);
                return;
            }

            /* #2811 named this phase "the store connection open PLUS the touch/probe round trip", and
               declined to split them because doing so "would name a connection pool rather than a cost".
               #2819 measured it and it WAS the connection pool: the probe SQL runs in 0.4ms (nothing stale)
               to 37ms (40 stale ids), while the phase measured 673-6,663ms — and a cycle with zero ids,
               which issues no SQL at all, still cost 673ms. That is acquisition, not work.

               So this method no longer acquires. It borrows the caller's connection — the one opened once
               per collector body at the top of RunEnumeratedAsync and described there as "one pooled store
               connection for the whole body", a promise this method and its text sibling were quietly
               breaking by opening a second and third. The driver is strictly sequential (read an item, then
               await its write), so that connection is provably IDLE across exactly the window this fetch
               occupies; there is no concurrent use to collide with.

               What that buys: ~228 acquisitions per cycle (114 (server, database) pairs x two fetches)
               against a MaxPoolSize of 24 collapse to zero, and the pool slot that used to be held across
               the SQL Server target fetch — measured at 104,799ms on one database — is no longer held at
               all, because it was already held by the caller regardless.

               The trade, stated at its real size rather than a flattering one: the borrowed connection is
               the BODY's, opened once per collector run and reused for every database in the sweep — so a
               store fault in here does not just cost this item its write, it breaks the connection every
               SUBSEQUENT item in the cycle will probe and write on. That is a larger blast radius than a
               private connection had, and worth naming plainly.

               It is still the right trade, because the write path already had exactly this shape: writeBatch
               has always run on this same shared connection, so those later writes were going to fail on it
               regardless. What changes is that their probes fail alongside, and a broken store connection
               means the store is unreachable anyway. The driver's per-item catch skips each affected item
               and the next cycle re-selects it, which is the recovery either arm takes.

               probeWatch now times the probe ROUND TRIP only, which is what the phase name always claimed.
               Started HERE rather than at the declaration so the quiet-cycle return above stays outside the
               measurement, while a reconnect above it does not vanish into the residual. */

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
                /* #2823: the probe's INPUT size, stamped where the input is known. probe: scales with
                   this (~0.61ms/reference), not with PerItemPlanIdsAttempted, which counts only what came
                   back missing — so a pass probing hundreds of references and owing nothing logs 0 ids
                   while doing real store work. Logging one and dividing by the other produced a phantom
                   140x gap twice (#2819, #2822). */
                context.PerItemPlanProbeIds = references.Count;
                var verdicts = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                    storeConnection, server.ServerId, databaseName, references, context.CollectionTime, itemTimeout, cancellationToken);
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

            /* Stamped BEFORE the nothing-owed return below, so a pass that is pure store round trip still
               reports where its milliseconds went. That pass is the interesting one: it issues no target
               query at all, so anything it costs is unambiguously the store. */
            context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
            probeStamped = true;

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

            /* #2811: the TARGET half, and only the target half — the statement plus its read loop, summed
               across chunks. Query building and the budget check sit outside deliberately: they are our
               arithmetic, and folding them in here would let this number quietly absorb the thing the
               residual is meant to expose. */
            var targetMs = 0L;

            foreach (var chunk in attempt.Chunk(PlanFetchIdsPerStatement))
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var query = QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                var chunkWatch = Stopwatch.StartNew();
                try
                {
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
                finally
                {
                    /* finally, not after the block: a chunk that TIMES OUT is the case this instrumentation
                       most needs to describe, and stamping only on success would report target:0ms for it —
                       "the target was free" is the exact misreading this change exists to end. The ids and
                       chunk count are stamped here too so the per-id cost stays computable on a failed pass. */
                    targetMs += chunkWatch.ElapsedMilliseconds;
                    context.PerItemPlanTargetMs = targetMs;
                    context.PerItemPlanChunks++;
                    context.PerItemPlanIdsAttempted += chunk.Length;
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
                /* #2811: the store WRITE, on its own stopwatch. #2777 raised this command's timeout from
                   Npgsql's unchosen 30s default to 500s because it was being cancelled mid-write; that fixed
                   the failure but left the duration unmeasured, so "the write is slow" stayed an assertion. */
                var writeWatch = Stopwatch.StartNew();
                IReadOnlyList<long> landed;
                try
                {
                    landed = await QueryStorePlanWriter.WriteAsync(
                        storeConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, cancellationToken);
                }
                finally
                {
                    /* Same finally-not-after reasoning as the target chunks: a cancelled write is precisely
                       the event #2777 chased, and it must not report write:0ms. */
                    context.PerItemPlanWriteMs = writeWatch.ElapsedMilliseconds;
                }

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
            /* #2816: the probe stamp is unreachable once its own await throws, so stamp it here instead.
               Guarded by the flag rather than by `== 0` because a throw in the LATER target or write phases
               must not overwrite an already-honest probe reading with the whole elapsed span — and a probe
               that genuinely measured 0ms is not distinguishable from an unstamped one by value alone. */
            if (!probeStamped)
            {
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
            }

            storeConnectionSuspect = true;

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

        /* Outside the catch on purpose — see the flag's declaration. Runs before this method returns, so
           the caller's writeBatch (which propagates, and would abort the whole sweep) never meets a broken
           borrowed connection that a reopen could have saved. */
        if (storeConnectionSuspect)
        {
            /* Charged to probe: for the same reason as the top-of-try repair — a reconnect is store time,
               and other: is documented as work in neither database. */
            var reopenWatch = Stopwatch.StartNew();
            await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName);
            context.PerItemPlanProbeMs += reopenWatch.ElapsedMilliseconds;
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
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        string collectorName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long QueryId, string? QueryHash)> references,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776), same reason as the plan side: the catch advances the backoff.
           Collector-scoped since #2902, same as the plan side and for the same measured reason. */
        var carryKey = FetchStateKey(server.ServerId, databaseName, collectorName);

        /* #2816: hoisted for the same reason as the plan side — a probe that throws would otherwise report
           probe:0ms and hand its whole cost to the other: residual, inverting where the time was spent. */
        var probeWatch = new Stopwatch();
        var probeStamped = false;

        /* #2819: set by the catch, acted on AFTER it. The restore needs an await, and an await inside a
           catch makes the compiler lift the handler's body out of the exception region — which would break
           #2816's pin that the probe stamp is reachable from inside a handler, and with it the guarantee
           that a throwing probe reports its own time instead of donating it to other:. Flag here, await
           below: the property #2816 holds and the recovery #2819 needs are not in tension, they just cannot
           share a block. */
        var storeConnectionSuspect = false;

        try
        {
            /* FIRST statement in the try, ahead of the quiet-cycle return below, and that ordering is
               the whole point. The catch further down filters out OperationCanceledException, and OCE is
               the fault most likely here: this method's token is the per-item wall-clock budget, not host
               shutdown, and Npgsql throws OCE when a caller-supplied token fires. So a database that simply
               ran slow mid-probe cancels, propagates past that catch, and never reaches the restore at the
               bottom — leaving the BORROWED connection broken for whoever runs next.

               Putting the repair after the early return would have missed exactly the case that matters.
               "Nothing referenced, nothing owed" is derived from this database's own just-read rows and is
               completely independent of whether it has runtime stats to WRITE, so a busy-but-stable database
               routinely returns early here while still handing the driver a non-empty batch. writeBatch then
               runs on the still-broken connection, outside any per-item try/catch, and propagates — aborting
               the rest of the sweep. That is precisely the whole-cycle abort this borrowing must not buy.

               Classifying on connection STATE rather than exception type is the discipline
               EnumeratedCollectorDriver.ItemBudgetExpired already applies to the budget question: the tokens
               and the connection know, the exception type does not. Free when nothing is wrong — the helper
               returns immediately on an Open connection — and outside the probe measurement on purpose,
               since a reopen is recovery rather than store work. */
            /* Timed into probe:, not left to other:. A reconnect is store-connection time — the very cost
               this PR removed from the steady-state path — and other: is documented as work in NEITHER
               database, so letting a reopen land there would recreate the misattribution #2816 fixed for
               the probe stamp. Zero on the ordinary pass: the helper returns on a state check. */
            probeWatch.Restart();
            if (!await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName))
            {
                /* The store is still down. Returning beats falling through: the probe below would fault on
                   the same Broken connection, log a second "could not reopen" for one underlying outage,
                   and the text sibling would double it again per item. This codebase caps repetitive
                   failure logging deliberately (MaxLoggedProbeFailures), and attempting store work already
                   known to be doomed is not diagnosis. The carryover is untouched, so nothing is forgotten
                   and the next cycle re-selects it.

                   Stamped before returning, like every other exit from this method: the failed reconnect
                   still took real wall clock, the caller's fetch watch still counted it, and leaving it
                   unstamped would hand the one measurable part of an outage to the other: residual. */
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
                return;
            }

            var hasCarryover = _textFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* Nothing referenced, nothing owed — so any carried failure count is stale (#2776).
                   Probe stamped for the same reason as the plan side: 0ms normally, but a reconnect
                   performed by the repair above is store time and must not fall into other:. */
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
                _textFetchFailures.TryRemove(carryKey, out _);
                return;
            }

            /* #2819: same borrowed connection as the plan side, for the same measured reason — this method
               was the SECOND of the two per-item acquisitions, and its zero-id probes measured 893ms, the
               worst floor of either. See the plan side for why borrowing is safe (the driver reads an item
               and then awaits its write, so the caller's connection is idle across this window) and what it
               trades (a store fault here breaks the caller's connection rather than a private one).

               probeWatch covers the probe round trip and, on a pass that needed one, the recovery reconnect
               above — both are store time, and neither belongs in the other: residual. */

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
                /* #2823: probe input size — see the plan-side comment. */
                context.PerItemTextProbeIds = references.Count;
                var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                    storeConnection, server.ServerId, databaseName, references, context.CollectionTime, itemTimeout, cancellationToken);
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

            context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
            probeStamped = true;

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

            /* #2811: target half only, same contract as the plan side. */
            var targetMs = 0L;

            foreach (var chunk in attempt.Chunk(TextFetchIdsPerStatement))
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var query = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                var chunkWatch = Stopwatch.StartNew();
                try
                {
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
                finally
                {
                    targetMs += chunkWatch.ElapsedMilliseconds;
                    context.PerItemTextTargetMs = targetMs;
                    context.PerItemTextChunks++;
                    context.PerItemTextIdsAttempted += chunk.Length;
                }
            }

            var returned = new HashSet<long>(fetched.Count);
            if (fetched.Count > 0)
            {
                var writeWatch = Stopwatch.StartNew();
                IReadOnlyList<long> landed;
                try
                {
                    landed = await QueryStoreTextWriter.WriteAsync(
                        storeConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, cancellationToken);
                }
                finally
                {
                    context.PerItemTextWriteMs = writeWatch.ElapsedMilliseconds;
                }
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
            /* #2816: same as the plan side — an unstamped probe would otherwise report probe:0ms and push
               its cost into the other: residual. */
            if (!probeStamped)
            {
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
            }

            storeConnectionSuspect = true;

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

        /* Outside the catch on purpose — see the flag's declaration. Runs before this method returns, so
           the caller's writeBatch (which propagates, and would abort the whole sweep) never meets a broken
           borrowed connection that a reopen could have saved. */
        if (storeConnectionSuspect)
        {
            /* Charged to probe: for the same reason as the top-of-try repair — a reconnect is store time,
               and other: is documented as work in neither database. */
            var reopenWatch = Stopwatch.StartNew();
            await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName);
            context.PerItemTextProbeMs += reopenWatch.ElapsedMilliseconds;
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
            /* Parity with the server-scoped twin (#2795): explicit, not Npgsql's inherited 30 s. */
            command.CommandTimeout = CommandTimeoutSeconds;
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Parity with the server-scoped twin (#2795). #2344's bound makes a TIMEOUT unlikely here,
               but every other failure — dropped connection, bad SQL — still returned a null that reads
               as a first run, and silence is the property that let the twin's version of this survive
               for months under a green suite. */
            _logger?.LogWarning(
                "Per-database watermark read failed for server {ServerId} database {Database} on "
                + "{Table}.{Column} — falling back to the collector's default window, which re-collects "
                + "data already stored: {Message}",
                serverId, databaseName, tableName, columnName, ex.Message);
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
            /* Parity with both timestamp twins (#2795): explicit, not Npgsql's inherited 30 s. */
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is not null && result != DBNull.Value)
            {
                return Convert.ToInt64(result);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Parity with both timestamp twins (#2795). job_history is small enough that this is
               unlikely to time out, but a swallowed failure here sets HasCollectedBefore down the
               first-run path with no trace anywhere — the same silence, one table over. */
            _logger?.LogWarning(
                "Numeric watermark read failed for server {ServerId} on {Table}.{Column} — falling back "
                + "to the collector's first-run path: {Message}",
                serverId, tableName, columnName, ex.Message);
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
