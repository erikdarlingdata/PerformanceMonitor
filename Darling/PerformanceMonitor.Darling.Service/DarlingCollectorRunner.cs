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
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Per-run outcome the worker logs (mirrors Lite's fetch/store phase split, #1180).
/// <see cref="Note"/> — what reaches <c>collection_log.error_message</c> — is COMPUTED from the two halves
/// below and cannot be assigned, so no caller can pick up one half and drop the other.
/// </summary>
/// <param name="Measurements">
/// The labelled counts the DEFINITION measured on the target (#3161), straight off
/// <see cref="CollectorContext.Measurements"/>. REQUIRED rather than defaulted, and for the reason
/// <c>DarlingObservability.LogCollectionAsync</c>'s <c>fanout</c> is: most construction sites here are the
/// early returns of runs that never reached a definition's read — an <c>AppliesTo</c> miss, an enumeration
/// that listed nothing, a cycle the wall-clock budget abandoned — and
/// <see cref="CollectorContext.NoMeasurements"/> is their correct answer rather than a value they forgot. A
/// default would let those sites stand in for the ONE success site that must pass the real list, which is
/// the site whose silence #3161 was filed about. The compiler names every site instead of a grep.
/// </param>
/// <param name="SqlMs">
/// What lands in <c>collection_log.sql_duration_ms</c>: the driver's SQL slice for this run.
///
/// <para><b>Not a purely target-side figure, and on the heaviest collector mostly not one (#3192).</b> On
/// the enumerated path this is the per-item stopwatch around the watermark refresh plus the whole
/// <c>readItem</c> closure, and for <c>query_store</c> that closure calls
/// <c>DarlingCollectorRunner.FetchAndStorePlansAsync</c> /
/// <c>FetchAndStoreQueryTextAsync</c> — each of which probes the STORE for
/// what content is already held and writes back what came off the target. One measured production run put
/// 107,334 ms of a 124,972 ms <c>SqlMs</c> in the store against 6,494 ms of plan-plus-text target time, and
/// the store probe is the largest single term in both fetches fleet-wide (55.4% / 80.6%, V110).</para>
///
/// <para><b>Deliberately left blended rather than re-based</b>, and the reason is downstream: this value is
/// also what <see cref="CollectorCostAccumulator"/> sums into <c>collect.collector_cost</c>, a 90-day hourly
/// series that carries NO phase split and is built in memory rather than re-aggregated from
/// <c>collection_log</c>. Subtracting the store terms here would leave 90 days of rows meaning one thing and
/// every row after meaning another, with nothing in that series able to reconcile them — under a
/// Collector Cost Regression self-alert whose baseline window is 14 days. The attribution is published
/// instead of applied: <see cref="Mcp.DarlingDataReader.CollectionLogEntry.SqlStoreMs"/> derives the store
/// share from the V110 columns, which makes it retroactive to every row that has them.</para>
/// </param>
/// <param name="HostNote">
/// The note the RUNNER authored for a run worth explaining on its collection_log row: the RDS ingest
/// outcome, the whole-cycle budget, the probe-failure summary, the fan-out bookkeeping. Null (the default)
/// on an ordinary run. Named <c>HostNote</c> rather than <c>Note</c> because it is only one half of what
/// the column receives — the rename is what made the compiler point at every site that used to assume
/// otherwise.
/// </param>
/// <param name="Fanout">
/// The per-database rollup for a run that fanned out, null for one that did not (#2472). Defaulted because
/// the great majority of construction sites here are the early returns of runs that never reached a fan-out
/// — a single query, an enumeration that yielded nothing — and null is their correct answer rather than a
/// value they forgot to supply. The one site that MUST set it is the success return.
/// </param>
/// <param name="Abandoned">
/// True only for a cycle the #2673 whole-server wall-clock budget gave up on: nothing was stored and no
/// watermark advanced. Carried as its own field rather than inferred from <see cref="Note"/>, because
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
    IReadOnlyList<CollectorMeasurement> Measurements,
    string? HostNote = null,
    FanoutCost? Fanout = null,
    bool Abandoned = false,
    bool ServerPhasesMeasured = false,
    long ServerOpenMs = 0,
    long ServerDrainMs = 0,
    long ServerWatermarkMs = 0,
    long ServerRowsRead = -1,
    long ServerBytesRead = -1,
    long ServerLastReadMs = -1,
    int? TargetSessionId = null,
    /* V110 (#2860): the per-database fetch split, summed across the fan-out. Passed in already ROLLED UP
       rather than as loose figures plus a flag, unlike the Server* members above, because the rollup is a
       cross-item accumulation the enumerated branch performs and this record cannot: the Server* pattern
       works only because those are single-shot stamps on one path. Same reasoning as FanoutCost, which
       arrives the same way for the same reason. */
    FetchPhaseCost? FetchPhases = null,
    /* #6254: the run's Query Store read-source summary (QueryStoreReplicaRouting.Summarize), written to collection_log.read_source;
       null for every other run and whenever no replica is set. */
    string? ReadSource = null)
{
    /// <summary>
    /// What this run puts in <c>collection_log.error_message</c>: the runner's own
    /// <see cref="HostNote"/> and then the definition's <see cref="Measurements"/>, composed through the
    /// shared <see cref="CollectorMeasurementNote.Compose"/> that Lite's <c>RunTelemetry.Note</c> also
    /// computes through — so the two SKUs cannot come to disagree about what a run note contains.
    ///
    /// <para><b>Computed rather than stored, and that is the parity guarantee.</b> There is no spelling of
    /// "the host note alone" for a caller to reach for, so a definition-supplied count cannot be dropped by
    /// a host that simply never learned about it. A shared member wired into one runner reads as a
    /// permanently-empty value in the other SKU and nothing fails to build, which is the failure mode
    /// CONTRIBUTING's two-store parity rules name.</para>
    /// </summary>
    public string? Note => CollectorMeasurementNote.Compose(HostNote, Measurements);

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
/// One fetch half's phase split, SUMMED across a run's fan-out, as persisted by V110 (#2860).
///
/// <para>Five figures rather than three, because the counts are what turn the durations into rates and the
/// rates are where the phase question stops being descriptive: <see cref="TargetMs"/> ÷
/// <see cref="IdsAttempted"/> measured 31.65 ms/id against production's cold plans where #2806's controlled
/// A/B saw ~1.6 ms/id on hot ones, and <see cref="ProbeMs"/> ÷ <see cref="ProbeIds"/> reproduces the
/// ~0.61 ms/reference already documented on <c>PerItemPlanProbeIds</c>. #2902 wants them for a second reason:
/// the fetch carryover has no eviction of any kind, so <see cref="IdsAttempted"/> beside
/// <see cref="ProbeIds"/> is the only stored signal that would show a backlog growing.</para>
///
/// <para><c>other:</c> is deliberately absent, #2859's rule — it is a residual, and a stored copy could drift
/// from the parent it completes. Unlike V108 the parent is not itself a column here, so the honest statement
/// is that the residual is not recorded at all: it measured 0.1% of both fetches fleet-wide, which makes the
/// three durations the parent to within a rounding error. <c>chunks</c> is absent too, as a batching artifact
/// no question has asked for.</para>
/// </summary>
/// <param name="ProbeMs">Milliseconds in the store touch/probe round trip that decides which ids are missing.</param>
/// <param name="TargetMs">Milliseconds inside the TARGET statements only, across every chunk.</param>
/// <param name="WriteMs">Milliseconds writing what came back into the store.</param>
/// <param name="IdsAttempted">Ids the fetch actually attempted — those the probe found MISSING.</param>
/// <param name="ProbeIds">References the probe EXAMINED, which is what <paramref name="ProbeMs"/> scales with.</param>
public readonly record struct FetchPhaseSums(int ProbeMs, int TargetMs, int WriteMs, int IdsAttempted, int ProbeIds);

/// <summary>
/// A run's plan-fetch and text-fetch phase sums (V110, #2860), each half null when THAT fetch never ran.
///
/// <para>Two independently-nullable halves rather than ten loose fields, and the nullability is per half on
/// purpose: the log line emits its two sub-lines separately, so a run that fetched text but no plans has to
/// store the text figures and leave the plan ones NULL rather than claiming five measured zeros. Within a
/// half it is all-or-nothing — the <see cref="FetchPhaseSums"/> and <see cref="FanoutCost"/> discipline, where
/// a target time without its id count is the half-answer that cannot be turned into the rate it exists for.</para>
/// </summary>
public readonly record struct FetchPhaseCost(FetchPhaseSums? Plan, FetchPhaseSums? Text);

/// <summary>
/// Accumulates a fan-out's PER-DATABASE fetch splits into one <see cref="FetchPhaseCost"/> of sums (V110,
/// #2860).
///
/// <para><b>Why sums are the right rollup</b> is argued at length on V110's own doc comment; the short of it
/// is that the split is N:1 against this row (N &gt; 1 on 68.8% of <c>query_store</c> runs) and the two
/// questions are already split across two mechanisms — V80's rollup answers WHICH DATABASE, and a sum answers
/// WHICH PHASE exactly rather than at 92.8% with a ~6.5 : 1 bias toward blaming the target for what was
/// actually the store probe. Sums also compose against a row whose <c>duration_ms</c> and
/// <c>sql_duration_ms</c> are already cross-item totals, so nothing new has to be decided about cardinality.</para>
///
/// <para><b>Observed for EVERY completed item, not only the ones the log line prints.</b> The per-database
/// line is gated on <c>batchCount &gt; 0</c> and stays silent on a quiet database — but a quiet database can
/// still pay real fetch time, because #2902's carryover means a pass with an empty batch may drain ids
/// deferred from an earlier one. Counting only the noisy items would drop that time, which is the same
/// reasoning that made V80's <see cref="FanoutCostAccumulator"/> count quiet items too. So this deliberately
/// records MORE than the log line reports, and the log line is untouched.</para>
/// </summary>
public sealed class FetchPhaseCostAccumulator
{
    /* long, then clamped on the way out. Each per-item figure is an int, but a 7-database fan-out summing
       them is arithmetic the accumulator does rather than the column, and a silent int overflow here would
       surface as a negative duration in the store - the one thing a duration column must never hold. */
    private long _planProbeMs, _planTargetMs, _planWriteMs, _planIds, _planProbeIds;
    private long _textProbeMs, _textTargetMs, _textWriteMs, _textIds, _textProbeIds;

    /* Per half, and set from the SAME condition the log line gates its two sub-lines on, so the stored
       population and the logged population are the same one. See V110's doc comment for why that is the
       right call here and the opposite of V108's MEASURED flag: a 0 ms open is a real event measured at
       zero, while a 0 ms fetch is a fetch that found nothing to do. */
    private bool _planRan, _textRan;

    /// <summary>
    /// Folds one completed item's per-database split into the run's totals. Reads the context's
    /// <c>PerItem*</c> members, which are live at this point: they are cleared at the START of the next
    /// item's read, not at the end of this one.
    /// </summary>
    public void Observe(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.PerItemPlanFetchMs > 0)
        {
            _planRan = true;
            _planProbeMs += context.PerItemPlanProbeMs;
            _planTargetMs += context.PerItemPlanTargetMs;
            _planWriteMs += context.PerItemPlanWriteMs;
            _planIds += context.PerItemPlanIdsAttempted;
            _planProbeIds += context.PerItemPlanProbeIds;
        }

        if (context.PerItemTextFetchMs > 0)
        {
            _textRan = true;
            _textProbeMs += context.PerItemTextProbeMs;
            _textTargetMs += context.PerItemTextTargetMs;
            _textWriteMs += context.PerItemTextWriteMs;
            _textIds += context.PerItemTextIdsAttempted;
            _textProbeIds += context.PerItemTextProbeIds;
        }
    }

    /// <summary>
    /// The two halves, or null when NEITHER fetch ran anywhere in this run — which is every collector except
    /// the plan/text-fetching ones, and ~78% of even those runs. Null is the honest answer there: the columns
    /// say "this run performed no deferred fetch", which is not the claim "its fetch was free".
    /// </summary>
    public FetchPhaseCost? Result =>
        _planRan || _textRan
            ? new FetchPhaseCost(
                _planRan ? Narrow(_planProbeMs, _planTargetMs, _planWriteMs, _planIds, _planProbeIds) : null,
                _textRan ? Narrow(_textProbeMs, _textTargetMs, _textWriteMs, _textIds, _textProbeIds) : null)
            : null;

    private static FetchPhaseSums Narrow(long probeMs, long targetMs, long writeMs, long ids, long probeIds) =>
        new((int)Math.Min(probeMs, int.MaxValue), (int)Math.Min(targetMs, int.MaxValue),
            (int)Math.Min(writeMs, int.MaxValue), (int)Math.Min(ids, int.MaxValue),
            (int)Math.Min(probeIds, int.MaxValue));
}

/// <summary>
/// Runs a shared collector definition against one monitored server and binary-COPYs the rows
/// into Postgres — the Darling counterpart of Lite's RemoteCollectorService.DefinitionRunner,
/// ported semantics-for-semantics: AppliesTo skip, host-store watermarks, the three execution
/// paths (per-database Azure connections; enumeration with the optional scalar probe; plain
/// single query with best-effort supplemental), cancellation-aware per-item catches, and the
/// separated SQL/storage phase timing. The definitions and the delta/ignored-wait/schedule
/// defaults are the shared brain; only the storage engine differs.
/// </summary>
public sealed partial class DarlingCollectorRunner
{
    private readonly NpgsqlDataSource _postgres;

    /// <summary>
    /// The query-tag sightings the collectors saw since the last map pass (#5985): the per-batch write path appends here, in
    /// memory, and the map step flushes it (<see cref="QueryTagMapBuilder.RunMapPassAsync"/>). One per runner, which is one per
    /// service.
    /// </summary>
    public QueryTagSightingBuffer QueryTagSightings { get; }
    private readonly CollectorDeltaCalculator _deltas;
    private readonly ILogger? _logger;

    /* Feeds CollectorContext.CapturePlanXml on every cycle — the query_stats / query_store
       collectors capture the execution plan when true (darling.json "capturePlans", default true).
       Lite never sets the context flag; this is what makes Darling the plan-capturing SKU. Read
       through a provider (not a captured bool) so a control-plane store reload of config_service's
       capture_plans is honored on the NEXT cycle without reconstructing the runner. #6771: this is the service-wide
       value; a server's own capture_plans (ServerRuntime.CapturePlans) wins over it when not null (CapturePlansFor). */
    private readonly Func<bool> _capturePlans;

    /* Feeds CollectorContext.CollectSchemaChangeEvents on every cycle — the default_trace_events
       collector drops its Object:Created/Altered/Deleted (schema DDL) slice when false (darling.json
       "collectSchemaChangeEvents", default true). Lite never sets the context flag, so it keeps
       collecting Object DDL. Read through a provider (not a captured bool) for symmetry with
       _capturePlans, so a future live reload is honored on the NEXT cycle without rebuilding. */
    private readonly Func<bool> _collectSchemaChanges;
    private readonly Func<bool> _compressPlanContent;

    /* #3953: the latest-snapshot-per-interval table's writer. One per runner, because it carries the per-server
       "coverage ensured" and "gap checked at" state that must outlive a cycle. */
    private readonly QueryStoreIntervalLatest _queryStoreIntervalLatest;
    private readonly QueryStoreIntervalWide _queryStoreIntervalWide;

    /* #4659: told before and after every Query Store batch write (see CopyBatchOnceAsync). Null = no reader is caching. */
    private readonly QueryStoreWriteFence? _queryStoreWriteFence;

    /* Feeds CollectorContext.TextByteBudgetOverride on every cycle (#2164) — the query_store collector's
       per-database text budget in MB (config_service.query_store_text_budget_mb, V59). Provider-read for
       the same reason as the two above: a store reload takes effect on the NEXT cycle without rebuilding
       the runner. Lite has no equivalent and keeps the collector's compile-time constant. */
    private readonly Func<int> _textBudgetMb;

    /* #6211: plans per database-hour and metric whose text and plan XML the query_store fetch pulls (config_service.query_store_fetch_top_n,
       V209). Provider-read at each fetch, so a store change takes effect on the next cycle with no restart. Zero or less, which is also what a
       caller that passes no provider gets, fetches every new plan: the behaviour before the setting. */
    private readonly Func<int> _queryStoreFetchTopN;

    /* Feeds the procedure_stats plan-capture cadence (#2862) — how many collection cycles pass between
       plan renders for that ONE collector. Provider-read for the same reason as the knobs above: the
       value is re-read on every cycle, so whichever source ends up feeding it is honored on the NEXT
       cycle without rebuilding the runner. 1 (and anything below) means capture on every cycle, which is
       byte-identical to the pre-#2862 collector. Lite has no equivalent and never sets CapturePlanXml at
       all, so it is unaffected either way. */
    private readonly Func<int> _procedureStatsPlanCycleInterval;

    /* #5158: whether query_stats defers its plan fetch (darling.json "queryStatsDeferredPlanFetch", default true).
       Off gives the inline plan capture exactly as before. Read through a provider on every cycle, but a file-only
       knob: darling.json is loaded once at start and nothing re-reads it, so an edit takes effect on the next
       RESTART. */
    private readonly Func<bool> _queryStatsDeferredPlanFetch;

    /* #5158: what this host knows of the statement plans it has already committed, one cache per server and per
       service lifetime. A restart starts empty (each server renders its plans once more); nothing seeds it from
       the store, because a probe costs more than the render it would save (#2831). */
    private readonly ConcurrentDictionary<int, PlanDigestCache<QueryStatsPlanKey>> _queryStatsPlanCaches = new();

    /// <summary>How long a plan identity may go unseen before the host forgets it (#5158).</summary>
    internal static readonly TimeSpan QueryStatsPlanCacheMaxAge = TimeSpan.FromHours(1);

    /// <summary>
    /// How long a procedure_stats module-plan identity may go unseen before the host forgets it (#5158). The same
    /// value as <see cref="QueryStatsPlanCacheMaxAge"/>, named for the collector that uses it so the two can move apart.
    /// </summary>
    internal static readonly TimeSpan ProcedureStatsPlanCacheMaxAge = QueryStatsPlanCacheMaxAge;

    /* #5158: procedure_stats' knob (darling.json "procedureStatsDeferredPlanFetch": off | shadow | on), read through a
       provider like its siblings. Null provider = off, the shipped default. */
    private readonly Func<string?> _procedureStatsDeferredPlanFetch;

    /* #5158: what this host knows of the module plans it has committed, per server; empty after a restart. */
    private readonly ConcurrentDictionary<int, PlanDigestCache<ProcedureStatsPlanKey>> _procedureStatsPlanCaches = new();

    /* #5367 review, A-L1: the mode each server's procedure_stats cache was last filled under. A shadow pass digests the
       plans its rows carry inline, which can be the whole-plan marker; an on pass would answer a hit on that entry with the
       marker's digest, and the row would store the marker until the entry expired. A switch clears the cache. */
    private readonly ConcurrentDictionary<int, ProcedureStatsPlanFetchMode> _procedureStatsPlanCacheModes = new();

    /* #5158: how many capture cycles each server has run with the knob on or shadow. The cache's age limit counts these,
       so a cycle the cadence gate skips does not age an entry. */
    private readonly ConcurrentDictionary<int, long> _procedureStatsCaptureOrdinals = new();

    /* The unrecognized knob value already warned about, so the warning appears once per value rather than per cycle. */
    private string? _warnedProcedureStatsPlanFetchValue;

    /// <summary>
    /// Runs between query_stats' main query and its plan fetch, on the same target connection. A test uses it to
    /// age a plan out of the cache in that window. Null in production.
    /// </summary>
    internal Func<CancellationToken, Task>? BetweenPlanPhasesForTests { get; set; }

    /* Resolves the per-collector database scope (#3477) for one (collector, server) pair — the worker
       passes StoreConfigProvider.ResolveDatabaseScope over its live schedule overrides, so a store
       write to config_collector_schedules.databases is honored on the collector's NEXT run through the
       same reload beacon every other schedule column rides. Provider-shaped like its knob siblings:
       resolved ONCE per run, at dispatch, so one run cannot see two different scopes. Empty = unscoped. */
    private readonly Func<string, int, IReadOnlyList<string>> _databaseScope;

    /* The databases monitored as their own servers, for a runtime on an Azure SQL Database logical server
       (AzureMasterScope.SeparatelyMonitoredDatabases). Empty for every other runtime. */
    private readonly Func<ServerRuntime, IReadOnlyList<string>> _separatelyMonitoredDatabases;

    /// <summary>
    /// The #3477 database scope for one collector on one server: per-server row, then fleet row, then unscoped (empty).
    /// The same delegate the read loop resolves at dispatch, so the long-query trace's lifecycle and its read cannot
    /// disagree about which databases are in scope.
    /// </summary>
    internal IReadOnlyList<string> DatabaseScopeFor(string collectorName, int serverId) => _databaseScope(collectorName, serverId);

    /// <summary>The databases monitored as their own servers, for this runtime. Empty unless it is an Azure SQL Database logical server.</summary>
    internal IReadOnlyList<string> SeparatelyMonitoredDatabasesFor(ServerRuntime server) => _separatelyMonitoredDatabases(server);

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
    /// In-memory server-scoped watermark cache (#4197 part b): a hit here skips the store's watermark
    /// reads entirely for a (server, collector) that has one cached. Only definitions that declare a
    /// per-row watermark accessor (<see cref="ICollectorDefinition{TRow}.WatermarkValueAccessor"/> /
    /// <see cref="ICollectorDefinition{TRow}.NumericWatermarkValueAccessor"/>) participate — a null
    /// accessor means "don't cache this collector", so <c>pg_cpu_utilization</c> (a second writer,
    /// <c>Targets/RdsCpuIngestor.cs</c>, can move its watermark without this runner's knowledge) always
    /// reads fresh, exactly as before. See <c>ServerWatermarkCache</c>'s own remarks for seed/advance/
    /// invalidate. Lives here rather than on a per-cycle type because the runner itself is constructed
    /// once per service process (mirrors <see cref="_azureMasterInaccessibleSince"/>), so the cache
    /// outlives every cycle it needs to.
    /// </summary>
    private readonly ServerWatermarkCache _watermarkCache = new();

    /// <summary>
    /// Exact per-(server, database) Query Store watermark cache for the enumerated per-item path and the
    /// Azure SQL Database per-database loop (#5514). The
    /// runner is constructed once per service process and the backfill holds this same runner, so the
    /// cache outlives every cycle and sees every writer. See <see cref="DatabaseWatermarkCache"/> for the
    /// hit rule and the single-writer assumption.
    /// </summary>
    private readonly DatabaseWatermarkCache _databaseWatermarkCache = new();

    /// <summary>The Query Store seed read's side-table reader (#5684); it owns the fallback counter and log throttle.</summary>
    private readonly QueryStoreWatermarkReader _queryStoreWatermarkReader;

    internal QueryStoreWatermarkReader QueryStoreWatermarkSeedReader => _queryStoreWatermarkReader;

    /// <summary>The once-per-database-per-UTC-day throttle on the Query Store read's timed-out warning (#5813).</summary>
    private readonly QueryStoreReadTimeoutLog _queryStoreReadTimeouts = new();

    /// <summary>
    /// The per-database budget on the live Query Store runtime-stats read (#5813), <see cref="QueryStoreReadBudget.Budget"/>
    /// in production. Settable so a test can plant a slow read without waiting 30 seconds per database.
    /// </summary>
    internal TimeSpan QueryStoreReadBudgetSpan { get; set; } = QueryStoreReadBudget.Budget;

    /// <summary>The clock the timed-out warning's UTC day is read from (#5813); a test moves it across midnight.</summary>
    internal Func<DateTime> QueryStoreReadTimeoutClockForTests { get; set; } = static () => DateTime.UtcNow;

    /// <summary>The same once-per-database-per-UTC-day throttle, on the plan fetch's budget line (#5813).</summary>
    private readonly QueryStoreReadTimeoutLog _queryStorePlanFetchBudgetLog = new();

    /// <summary>
    /// The per-database, per-pass plan-XML fetch budget (#5813), <see cref="QueryStoreReadBudget.PlanFetchBudget"/> in
    /// production. Settable so a test can plant slow plans without waiting 30 seconds.
    /// </summary>
    internal TimeSpan QueryStorePlanFetchBudgetSpan { get; set; } = QueryStoreReadBudget.PlanFetchBudget;

    /// <summary>
    /// Each database's last measured plan-fetch time per plan, in milliseconds (#5813), keyed like
    /// <see cref="_planFetchCarryover"/>. <see cref="QueryStoreReadBudget.PlanFetchWidth"/> sizes the next pass's chunks
    /// from it, so a database at 2 s a plan asks for about 15 plans a pass instead of being cut at the budget. In memory
    /// like the size estimate: a restart costs one pass at the full width, which the budget still bounds.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), double> _planFetchMsPerPlan = new();

    /// <summary>The time per plan the next plan-fetch pass of one database will size its chunks from (#5813); null before
    /// its first measured pass. For tests.</summary>
    internal double? PlanFetchMsPerPlanForTests(int serverId, string database, string collector) =>
        _planFetchMsPerPlan.TryGetValue(FetchStateKey(serverId, database, collector), out var ms) ? ms : null;

    /// <summary>The same once-per-database-per-UTC-day throttle, on the text fetch's budget line (#6102).</summary>
    private readonly QueryStoreReadTimeoutLog _queryStoreTextFetchBudgetLog = new();

    /// <summary>
    /// The per-database, per-pass statement-text fetch budget (#6102), <see cref="QueryStoreReadBudget.TextFetchBudget"/> in
    /// production, separate from <see cref="QueryStorePlanFetchBudgetSpan"/>. Settable so a test can plant slow texts
    /// without waiting 30 seconds.
    /// </summary>
    internal TimeSpan QueryStoreTextFetchBudgetSpan { get; set; } = QueryStoreReadBudget.TextFetchBudget;

    /// <summary>
    /// Each database's last measured text-fetch time per id, in milliseconds (#6102), the text twin of
    /// <see cref="_planFetchMsPerPlan"/>: <see cref="QueryStoreReadBudget.TextFetchWidth"/> sizes the next pass's chunks
    /// from it. In memory like the plan side: a restart costs one pass at the full width, which the budget still bounds.
    /// </summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database, string Collector), double> _textFetchMsPerText = new();

    /// <summary>The time per text id the next text-fetch pass of one database will size its chunks from (#6102); null before
    /// its first measured pass. For tests.</summary>
    internal double? TextFetchMsPerTextForTests(int serverId, string database, string collector) =>
        _textFetchMsPerText.TryGetValue(FetchStateKey(serverId, database, collector), out var ms) ? ms : null;

    /// <summary>The consecutive text-fetch failures a database carries into its next pass (#2776, #6102); 0 when none. For tests.</summary>
    internal int TextFetchFailuresForTests(int serverId, string database, string collector) =>
        _textFetchFailures.TryGetValue(FetchStateKey(serverId, database, collector), out var failures) ? failures : 0;

    /// <summary>
    /// #5813 review: adds a failed database's SQL slice to the run's blended SQL time, on the Azure per-database loop,
    /// when the fault came before the straight path folded it. Stopped first, so the fault line printed next shows the
    /// same figure that was added. Null means the fault came before the timed region began: nothing to add.
    /// </summary>
    private static void FoldFailedSlice(Stopwatch? sqlSlice, bool sliceFolded, ref long sqlMs)
    {
        if (sqlSlice is null || sliceFolded)
        {
            return;
        }

        sqlSlice.Stop();
        sqlMs += sqlSlice.ElapsedMilliseconds;
    }

    /// <summary>The plan-size estimate the next plan-fetch pass of one database will size its candidate cap from
    /// (#5813 review); null before its first pass. For tests.</summary>
    internal QueryStorePlanXmlState.PlanSizeEstimate? PlanSizeEstimateForTests(int serverId, string database, string collector) =>
        _observedPlanSize.TryGetValue(FetchStateKey(serverId, database, collector), out var estimate) ? estimate : null;

    /// <summary>
    /// #5813: one database's plan-XML fetch was bounded by <see cref="QueryStorePlanFetchBudgetSpan"/>, by a cut or by a
    /// predicted width that left ids for the next pass. Information once per database per UTC day, Debug for the rest
    /// of that day: it is the budget working, not a fault, and nothing is lost (the ids not received stay in the
    /// backlog). Nothing new in collection_log: the fetch-phase milliseconds are already in the run's row.
    /// </summary>
    private void NoteQueryStorePlanFetchBudget(
        ServerRuntime server, string databaseName, bool cut, int received, int deferred, int nextWidth, double? msPerPlan)
    {
        var how = cut ? "cut at" : "held to";
        var perPlan = msPerPlan is double ms ? ms.ToString("0", System.Globalization.CultureInfo.InvariantCulture) : "?";
        if (_queryStorePlanFetchBudgetLog.ShouldWarn(server.ServerId, databaseName, QueryStoreReadTimeoutClockForTests()))
        {
            _logger?.LogInformation(
                "query_store plan fetch on '{Server}' database [{Database}] {How} its {Budget} budget: {Received} plan(s) received, {Deferred} id(s) deferred to the next pass, next pass asks for {NextWidth} ({MsPerPlan} ms a plan). Logged once per database per UTC day.",
                server.Config.DisplayName, databaseName, how, EnumeratedCollectorDriver.DescribeBudget(QueryStorePlanFetchBudgetSpan),
                received, deferred, nextWidth, perPlan);
        }
        else
        {
            _logger?.LogDebug(
                "query_store plan fetch on '{Server}' database [{Database}] {How} its budget again today: {Received} received, {Deferred} deferred, next {NextWidth}.",
                server.Config.DisplayName, databaseName, how, received, deferred, nextWidth);
        }
    }

    /// <summary>
    /// #6102: one database's statement-text fetch was bounded by <see cref="QueryStoreTextFetchBudgetSpan"/>, by a cut or by
    /// a predicted width that left ids for the next pass. The text twin of <see cref="NoteQueryStorePlanFetchBudget"/>:
    /// Information once per database per UTC day, Debug for the rest of that day, because it is the budget working, not a
    /// fault, and the ids not received stay in the backlog.
    /// </summary>
    private void NoteQueryStoreTextFetchBudget(
        ServerRuntime server, string databaseName, bool cut, int received, int deferred, int nextWidth, double? msPerText)
    {
        var how = cut ? "cut at" : "held to";
        var perText = msPerText is double ms ? ms.ToString("0", System.Globalization.CultureInfo.InvariantCulture) : "?";
        if (_queryStoreTextFetchBudgetLog.ShouldWarn(server.ServerId, databaseName, QueryStoreReadTimeoutClockForTests()))
        {
            _logger?.LogInformation(
                "query_store text fetch on '{Server}' database [{Database}] {How} its {Budget} budget: {Received} text(s) received, {Deferred} id(s) deferred to the next pass, next pass asks for {NextWidth} ({MsPerText} ms an id). Logged once per database per UTC day.",
                server.Config.DisplayName, databaseName, how, EnumeratedCollectorDriver.DescribeBudget(QueryStoreTextFetchBudgetSpan),
                received, deferred, nextWidth, perText);
        }
        else
        {
            _logger?.LogDebug(
                "query_store text fetch on '{Server}' database [{Database}] {How} its budget again today: {Received} received, {Deferred} deferred, next {NextWidth}.",
                server.Config.DisplayName, databaseName, how, received, deferred, nextWidth);
        }
    }

    /// <summary>
    /// #5813: one database's live Query Store read ran out of <see cref="QueryStoreReadBudgetSpan"/>. Counts it in the run's
    /// fan-out cost under its plain name, so <c>slowest_item</c> names it, and logs it at Warning once per database per UTC
    /// day (Debug for the rest of that day). Returns false, doing nothing, for every other exception, so the caller's own
    /// log line stands.
    /// </summary>
    private bool NoteQueryStoreReadTimeout(ServerRuntime server, Exception exception, FanoutCostAccumulator fanout)
    {
        if (!QueryStoreReadBudget.ObserveTimeout(exception, fanout))
        {
            return false;
        }

        var timeout = (QueryStoreReadTimeoutException)exception;
        if (_queryStoreReadTimeouts.ShouldWarn(server.ServerId, timeout.Database, QueryStoreReadTimeoutClockForTests()))
        {
            _logger?.LogWarning(
                "query_store on '{Server}' timed out: {Message} Logged once per database per UTC day; every timeout is in that run's collection_log row.",
                server.Config.DisplayName, timeout.Message);
        }
        else
        {
            _logger?.LogDebug("query_store on '{Server}' timed out again today: {Message}", server.Config.DisplayName, timeout.Message);
        }

        return true;
    }

    /// <summary>
    /// When a server's live query_store collection last failed a per-database item — the backfill
    /// worker's yield-to-live signal (#2111), read through <see cref="LastQueryStoreItemFailureUtc"/>
    /// and judged by <see cref="QueryStoreBackfillState.ShouldYieldToLive"/>. Stamped only for
    /// query_store (the one collector with a backfill worker to yield); in-memory on purpose — a
    /// service restart forgetting the stamps just means one backfill slice races one live cycle once.
    /// </summary>
    private readonly ConcurrentDictionary<int, DateTime> _lastQueryStoreItemFailureUtc = new();

    /// <summary>#4660: when the orphaned per-database state prune last succeeded, per server.</summary>
    private readonly ConcurrentDictionary<int, DateTime> _lastOrphanStatePruneUtc = new();

    /* When each server's failed log_timezone read last logged at Warning (#4051 round-2 review, L-2). See
       LogTimezoneReadFailureLevel. In memory on purpose: a restart that warns once more is the right answer. */
    private readonly ConcurrentDictionary<int, DateTime> _logTimezoneReadWarnedUtc = new();

    /* #4053 part c1: the same throttle as _logTimezoneReadWarnedUtc, for TryReadPgLogUsesCsvlogAsync's own
       probe — its own dictionary because a target can fail one read and not the other (they run on separate
       throwaway connections), and LogTimezoneReadFailureLevel already takes the dictionary as a parameter
       for exactly this reuse (see LogTimezoneReadFailureLevelTests). */
    private readonly ConcurrentDictionary<int, DateTime> _pgLogUsesCsvlogReadWarnedUtc = new();

    /* #4053 review round 1 (item 5): the last good pgLogUsesCsvlog verdict this runner has seen for a
       server, kept so a probe failure can return IT rather than false. False is not a neutral fallback
       here the way it is for a target this route has never successfully read: on a target the route was
       already reading csvlog for, a transient connection or probe failure flipping the verdict to false
       would make the ingestor read the STDERR file instead, from that file's own (stale or absent) marker,
       and re-emit rows the csvlog route already stored under different text — the very duplication
       CommitResume's discipline exists to avoid. Runner-local and in-memory for the same reason every other
       per-server cache on this type is: a restart simply re-probes from nothing, which is the existing
       first-contact behaviour, not a regression. */
    private readonly ConcurrentDictionary<int, bool> _lastPgLogUsesCsvlogVerdict = new();

    /* #6875: where a per-database pass that spent its PassWallClockBudget stopped, per (server, collector): the last database
       it started. The next pass reads the databases after it first and wraps (PassBudget.ResumeOrder). A pass that covered every
       database removes its entry, so the next one starts at the first. In memory on purpose, like the caches around it: a restart
       begins at the first database, which only repeats the databases the pass before it already read. */
    private readonly ConcurrentDictionary<(int ServerId, string Collector), string> _passResumeAfter = new();

    /// <summary>The pass budget for one run of <paramref name="definition"/> on <paramref name="server"/>, and the databases in the order it
    /// reads them (#6875). A definition with no pass budget gets a null budget and the list as it came.</summary>
    internal (PassBudget? Budget, IReadOnlyList<string> Items) BeginPass(ICollectorSchemaInfo definition, int serverId, IReadOnlyList<string> items)
    {
        if (definition.PassWallClockBudget is not { } span)
        {
            return (null, items);
        }

        _passResumeAfter.TryGetValue((serverId, definition.Name), out var resumeAfter);
        return (new PassBudget(span), PassBudget.ResumeOrder(items, resumeAfter));
    }

    /// <summary>Keeps (or clears) the resume point after a pass and returns the stop note, null when the pass covered every database (#6875).</summary>
    internal string? EndPass(PassBudget? budget, ICollectorSchemaInfo definition, int serverId, string? stoppedAfter, int reached, int total)
    {
        if (budget is null)
        {
            return null;
        }

        if (stoppedAfter is null)
        {
            _passResumeAfter.TryRemove((serverId, definition.Name), out _);
            return null;
        }

        _passResumeAfter[(serverId, definition.Name)] = stoppedAfter;
        return PassBudget.StopNote(budget.BudgetSeconds, reached, total);
    }

    /* #4735: the (target, log-tail collector) pairs whose last cycle used every read start and was refused at each.
       A start inside a character is cured by one of the later starts. A byte that is invalid in the database
       encoding, anywhere in the 4 MB window, is refused at all four and would be again on every cycle, so while a
       pair is here its cycle makes one read at shift 0 and no retries. Only a read that succeeds removes it. Keyed
       as the binary-file grant is (ReadBinaryFileCacheKey), plus the collector. In memory on purpose, like the
       caches around it: a restart pays one four-read cycle and sets it again, the safe direction. */
    private readonly ConcurrentDictionary<(string Target, string Collector), byte> _pgLogRefusedAtEveryShift = new();

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

    /// <summary>#6254: the per-server ship guard and capturing-secondary tracker, kept in memory for the life of the service.</summary>
    private readonly ConcurrentDictionary<int, QueryStoreServerReplicaState> _queryStoreReplicaStates = new();

    /// <summary>Test seam: the replica connection string for a server (null = none); the production builder needs a resolved secret.</summary>
    internal Func<ServerRuntime, string?>? QueryStoreReplicaConnectionStringOverrideForTests { get; set; }

    /// <summary>
    /// #6254: this run's replica state: the server's guard and tracker (the tracker seeded from the store on first use, #6279) and
    /// whether the server reads Query Store from a replica. Sets <see cref="CollectorContext.QueryStoreReplicaConfigured"/> so the
    /// primary enumeration returns the flush intervals the routing needs. Azure SQL Database never reads a replica.
    /// </summary>
    private async Task<QueryStoreReplicaRun> PrepareQueryStoreReplicaRunAsync(
        ServerRuntime server, CollectorContext context, CancellationToken cancellationToken)
    {
        var state = _queryStoreReplicaStates.GetOrAdd(server.ServerId, static _ => new QueryStoreServerReplicaState());
        if (!state.Seeded)
        {
            state.Seeded = true;
            try
            {
                /* The databases the store saw a non-primary replica_role for in the last 24 hours, with their newest collection_time. */
                await using var seedConnection = await _postgres.OpenConnectionAsync(cancellationToken);
                await using var seedCommand = new NpgsqlCommand(
                    "SELECT database_name, max(collection_time) FROM collect.query_store_stats WHERE server_id = $1 "
                    + "AND collection_time > (now() AT TIME ZONE 'UTC') - interval '24 hours' "
                    + "AND replica_role IS NOT NULL AND lower(replica_role) <> 'primary' GROUP BY database_name", seedConnection);
                seedCommand.CommandTimeout = CommandTimeoutSeconds;
                seedCommand.Parameters.AddWithValue(server.ServerId);
                var seeds = new List<(string, DateTime)>();
                await using (var seedReader = await seedCommand.ExecuteReaderAsync(cancellationToken))
                {
                    while (await seedReader.ReadAsync(cancellationToken))
                    {
                        seeds.Add((seedReader.GetString(0), DateTime.SpecifyKind(seedReader.GetDateTime(1), DateTimeKind.Utc)));
                    }
                }

                state.Tracker.Seed(seeds);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger?.LogDebug("query_store capturing-secondary seed for '{Server}' failed; detection starts at the first read: {Error}",
                    server.Config.DisplayName, ex.Message);
            }
        }

        var configured = !server.Target.IsAzureSqlDb && QueryStoreReplicaSetting.Normalize(server.Config.QueryStoreReplica) is not null;
        context.QueryStoreReplicaConfigured = configured;
        return new QueryStoreReplicaRun(state, configured, server.Target.IsAzureSqlDb);
    }

    /// <summary>#6254: one connection's AG redo rows (this server's own row per availability-group database), keyed by database.</summary>
    private static async Task ReadQueryStoreRedoAsync(
        ITargetProvider targetProvider, DbConnection connection, Dictionary<string, QueryStoreAgRedoState> into, CancellationToken cancellationToken)
    {
        using var redoCommand = CreateCollectorCommand(targetProvider, QueryStoreCollector.BuildRedoStateQuery(), connection, CommandTimeoutSeconds);
        using var redoReader = await redoCommand.ExecuteReaderAsync(cancellationToken);
        foreach (var (name, redo) in await QueryStoreCollector.ReadRedoStatesAsync(redoReader, cancellationToken))
        {
            into[name] = redo;
        }
    }

    /// <summary>
    /// #6254: opens the replica connection and reads its facts: the major version, the databases it can serve and the redo
    /// state on both sides. Any failure leaves the run with <see cref="QueryStoreReplicaRun.ReplicaUnavailable"/> set, so every
    /// database reads the primary and says why; nothing here fails the run.
    /// </summary>
    private async Task OpenQueryStoreReplicaAsync(
        QueryStoreReplicaRun run, ServerRuntime server, CollectorContext context, ITargetProvider targetProvider,
        DbConnection primaryConnection, CancellationToken cancellationToken)
    {
        try
        {
            run.PrimaryMajorVersion = context.EnumerationProbeResult is null
                ? 0
                : Convert.ToInt32(context.EnumerationProbeResult, CultureInfo.InvariantCulture);

            /* The primary's own AG rows first: they say which databases are in an availability group (the look-back and read_source apply
               to those), and a replica that cannot be reached must not hide that. */
            await ReadQueryStoreRedoAsync(targetProvider, primaryConnection, run.PrimaryRedo, cancellationToken);

            var replicaConnectionString = QueryStoreReplicaConnectionStringOverrideForTests?.Invoke(server)
                ?? DarlingServerConnector.ResolveQueryStoreReplicaConnectionString(server.Config, server.Target.IsAzureSqlDb, _logger);
            var replicaPlan = QueryStoreCollector.Instance.BuildReplicaEnumerationQuery(context);
            if (replicaConnectionString is null || replicaPlan is null)
            {
                run.ReplicaUnavailable = true;
                return;
            }

            run.Connection = targetProvider.CreateConnection(replicaConnectionString);
            await run.Connection.OpenAsync(cancellationToken);

            using (var versionCommand = CreateCollectorCommand(
                targetProvider, new CollectorQuery(QueryStoreCollector.ProductVersionProbeText), run.Connection, 10))
            {
                var version = await versionCommand.ExecuteScalarAsync(cancellationToken);
                run.ReplicaMajorVersion = version is null || version == DBNull.Value ? 0 : Convert.ToInt32(version, CultureInfo.InvariantCulture);
            }

            using (var listCommand = CreateCollectorCommand(targetProvider, replicaPlan, run.Connection, CommandTimeoutSeconds))
            using (var listReader = await listCommand.ExecuteReaderAsync(cancellationToken))
            {
                var listed = await QueryStoreCollector.ReadReplicaEnumerationAsync(listReader, cancellationToken);
                foreach (var database in listed.Databases)
                {
                    run.ReplicaFacts[database.Name] = database;
                }
            }

            await ReadQueryStoreRedoAsync(targetProvider, run.Connection, run.ReplicaRedo, cancellationToken);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            _logger?.LogWarning(
                "query_store on '{Server}': the replica could not be read ({Message}); every database reads the primary this run.",
                server.Config.DisplayName, ex.Message);
            run.ReplicaUnavailable = true;
        }
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
    /// Memoizes <see cref="QueryStoreFetchProbe.TouchAndProbePlansAsync"/>'s verdict so a plan id
    /// already confirmed resolved-and-current recently skips the store round trip entirely (#3189,
    /// #3216 — see <see cref="QueryStoreProbeCache"/> for the measurement and why this differs from
    /// the carryover dictionaries' collector-scoped key). Not per-collector, on purpose.
    /// </summary>
    private readonly QueryStoreProbeCache _planProbeCache = new();

    /// <summary>Text twin of <see cref="_planProbeCache"/> — same memoization, keyed by query_id.</summary>
    private readonly QueryStoreProbeCache _textProbeCache = new();

    /// <summary>
    /// How long <see cref="_planProbeCache"/> / <see cref="_textProbeCache"/> trust a confirmed verdict.
    /// Deliberately the SAME width as <see cref="QueryStoreLivenessTouchGuard.GuardHours"/> rather than
    /// an independently chosen number: that is the staleness the store's own touch guard already
    /// accepts for these rows ("a row that stays referenced is re-stamped at least once per guard
    /// window"), so reusing it means this cache changes the CADENCE a live reference pays the round trip
    /// at, not the liveness contract itself.
    /// </summary>
    private static readonly TimeSpan s_probeCacheTtl = TimeSpan.FromHours(QueryStoreLivenessTouchGuard.GuardHours);

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
    internal bool ShouldCapturePlanXmlFor(string collectorName, int serverId, bool? serverCapturePlans) =>
        CapturePlansFor(serverCapturePlans) && ShouldCapturePlanForCollector(collectorName, serverId);

    /// <summary>
    /// #6771: whether plan XML is fetched for a server at all: the server's own <c>capture_plans</c> when it has one, else the
    /// service-wide setting, read live. An explicit false stops the plan fetch for that server with the service-wide setting on, and
    /// an explicit true keeps it with the service-wide setting off. Every decision below that used to read the service-wide
    /// provider directly goes through here.
    /// </summary>
    internal bool CapturePlansFor(bool? serverCapturePlans) => serverCapturePlans ?? _capturePlans();

    /// <summary>
    /// #5158: whether this run of this collector leaves plan XML out of its main query and fetches it in a
    /// second target query for only the plans the host has not committed. True for query_stats on a server-scoped
    /// target that captures plans, with the knob on. Azure SQL Database reads per database through a path with no
    /// second query, so it keeps the inline capture.
    /// </summary>
    internal bool ShouldDeferPlanFetchFor(
        string collectorName, bool capturePlanXml, CollectorTargetInfo target, bool? serverCapturePlans) =>
        ShouldDeferPlanFetchFor(
            collectorName, capturePlanXml, target, ProcedureStatsPlanFetchModeFor(collectorName, target, serverCapturePlans));

    /// <summary>
    /// The same decision with procedure_stats' mode already resolved, so one run reads the knob and the capture setting
    /// once (see <see cref="StampPlanFetchFlags"/>).
    /// </summary>
    internal bool ShouldDeferPlanFetchFor(
        string collectorName, bool capturePlanXml, CollectorTargetInfo target, ProcedureStatsPlanFetchMode procedureStatsMode) =>
        capturePlanXml
        && ((string.Equals(collectorName, QueryStatsCollector.Instance.Name, StringComparison.Ordinal)
                && !target.IsAzureSqlDb
                && _queryStatsDeferredPlanFetch())
            || procedureStatsMode == ProcedureStatsPlanFetchMode.On);

    /// <summary>
    /// #5158: the two context flags that describe a run's plan fetch, resolved from ONE read of the knob and of the
    /// capture setting. The store reloads the capture setting live, so deciding the stamp, the reader close and the apply
    /// branch from separate reads could disagree inside one run (shadow rows carrying inline plans, then an on-mode fetch
    /// run on them over an open reader). Everything after the stamp reads the mode back from the context with
    /// <see cref="ProcedureStatsPlanFetchModes.OfRun"/>.
    /// </summary>
    internal (bool DeferPlanXmlFetch, bool PlanIdentityColumns) StampPlanFetchFlags(
        string collectorName, bool capturePlanXml, CollectorTargetInfo target, bool? serverCapturePlans)
    {
        var mode = ProcedureStatsPlanFetchModeFor(collectorName, target, serverCapturePlans);
        return (
            ShouldDeferPlanFetchFor(collectorName, capturePlanXml, target, mode),
            /* Shadow adds the identity columns after the inline plan on a capture cycle; on sends them every cycle, a
               gated one included, where they stand alone. */
            mode == ProcedureStatsPlanFetchMode.On
                || (mode == ProcedureStatsPlanFetchMode.Shadow && capturePlanXml));
    }

    /// <summary>
    /// #5158: the procedure_stats plan fetch mode for this run. Off for every other collector, while the host captures
    /// no plans, and on Azure SQL Database, where whether <c>sys.dm_exec_query_stats</c> lists a module's statements
    /// is unverified, so the identity could not be trusted there. An unrecognized knob value is off and warns once.
    /// </summary>
    internal ProcedureStatsPlanFetchMode ProcedureStatsPlanFetchModeFor(
        string collectorName, CollectorTargetInfo target, bool? serverCapturePlans)
    {
        if (!string.Equals(collectorName, PlanCadenceGatedCollector, StringComparison.Ordinal)
            || target.IsAzureSqlDb
            || !CapturePlansFor(serverCapturePlans))
        {
            return ProcedureStatsPlanFetchMode.Off;
        }

        var value = _procedureStatsDeferredPlanFetch();
        if (!ProcedureStatsPlanFetchModes.TryParse(value, out var mode))
        {
            if (!string.Equals(_warnedProcedureStatsPlanFetchValue, value, StringComparison.Ordinal))
            {
                _warnedProcedureStatsPlanFetchValue = value;
                _logger?.LogWarning(
                    "procedureStatsDeferredPlanFetch is '{Value}', which is not off, shadow or on; procedure_stats keeps its inline plan capture (#5158).",
                    value);
            }

            return ProcedureStatsPlanFetchMode.Off;
        }

        return mode;
    }

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

    private RepeatLogGate? _repeatLog;

    /// <summary>#7067: the gate for warnings repeated every cycle. The worker gives the runner its own, so both share one
    /// clock and one memory of what was logged; created on first use otherwise.</summary>
    internal RepeatLogGate RepeatLog
    {
        get => LazyInitializer.EnsureInitialized(ref _repeatLog, static () => new RepeatLogGate());
        set => _repeatLog = value;
    }

    /// <summary>
    /// #7067: the "failed in all N database(s)" line that precedes the surfaced first failure. It follows the same decision as
    /// the line it explains. When the surfaced fault is one of the PostgreSQL faults the worker's warning gates (an extension
    /// that is not created, a grant that is not given), this line is gated the same way: logged when first seen and when it
    /// changes, then a reminder once a day, and the worker clears the gate when the collector succeeds. Any other fault
    /// (every SQL Server fault, and a PostgreSQL fault that lands in the general ERROR arm) is logged by the worker on every
    /// cycle, so this line is too and keeps the "every database failed" context beside it.
    /// </summary>
    internal void LogFailedInAllDatabases(ServerRuntime server, string collector, int count, Exception? firstFailure)
    {
        var name = server.Config.DisplayName;
        if (_logger is null
            || (IsGatedPostgresFault(server, collector, firstFailure) && !RepeatLog.ShouldLog(name, collector, $"failed in all {count} database(s)")))
        {
            return;
        }

        _logger.LogWarning("{Collector} failed in all {Count} database(s) on '{Server}'; surfacing the first failure",
            collector, count, name);
    }

    /// <summary>True when the worker logs this fault through <see cref="DarlingWorker.LogPostgresFault"/> (a classified
    /// PostgreSQL fault other than a lock-timeout yield), the one warning that repeats only once a day.</summary>
    private static bool IsGatedPostgresFault(ServerRuntime server, string collector, Exception? fault) =>
        fault is PostgresException pg
        && DarlingWorker.PostgresFaultOutcome(
            pg, collector, CollectorFaultDatabase.For(pg, server.ConnectedDatabase), server.PgStatStatementsExtension, server.Target.IsAurora)
            .Status is not ("ERROR" or "YIELDED");

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
    public DarlingCollectorRunner(NpgsqlDataSource postgres, CollectorDeltaCalculator deltas, ILogger? logger = null, Func<bool>? capturePlans = null, Func<bool>? collectSchemaChanges = null, Func<int>? textBudgetMb = null, Func<bool>? compressPlanContent = null, Func<int>? procedureStatsPlanCycleInterval = null, Func<string, int, IReadOnlyList<string>>? databaseScope = null, PgLogHashKey? logHashKey = null, QueryStoreWriteFence? queryStoreWriteFence = null, Func<ServerRuntime, IReadOnlyList<string>>? separatelyMonitoredDatabases = null, Func<string?>? installId = null, Func<bool>? queryStatsDeferredPlanFetch = null, Func<string?>? procedureStatsDeferredPlanFetch = null, Func<int>? queryStoreFetchTopN = null)
    {
        _queryStoreFetchTopN = queryStoreFetchTopN ?? (() => 0);
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        QueryTagSightings = new QueryTagSightingBuffer(postgres, logger);
        _queryStoreWriteFence = queryStoreWriteFence;
        _deltas = deltas ?? throw new ArgumentNullException(nameof(deltas));
        _logger = logger;
        _queryStoreWatermarkReader = new QueryStoreWatermarkReader(logger);
        _capturePlans = capturePlans ?? (() => true);
        /* Null provider = keep the collector's own compile-time budget (what Lite and every test does). */
        _textBudgetMb = textBudgetMb ?? (() => 0);
        _collectSchemaChanges = collectSchemaChanges ?? (() => true);
        /* #2171: plan_xml_compression provider — true = gzip into query_plan_gz (the default),
           false = 'none': plain text into query_plan_xml so direct-SQL consumers read it bare.
           The worker passes () => config.PlanXmlCompression == "gzip"; tests pass a constant. */
        _compressPlanContent = compressPlanContent ?? (() => true);
        _queryStoreIntervalLatest = new QueryStoreIntervalLatest(logger);
        _queryStoreIntervalWide = new QueryStoreIntervalWide(logger);
        /* Null provider = 1 = capture a plan on every cycle, i.e. the pre-#2862 behaviour. */
        _procedureStatsPlanCycleInterval = procedureStatsPlanCycleInterval ?? (() => 1);
        /* Null provider = the deferred plan fetch is on, the shipped default (#5158). */
        _queryStatsDeferredPlanFetch = queryStatsDeferredPlanFetch ?? (() => true);
        /* Null provider = procedure_stats keeps its inline plan capture, the shipped default (#5158). */
        _procedureStatsDeferredPlanFetch = procedureStatsDeferredPlanFetch ?? (() => ProcedureStatsPlanFetchModes.DefaultValue);
        /* Null provider = no scope for any collector = every database the server enumerates, which is
           what Lite's twin and every pre-#3477 test constructs. */
        _databaseScope = databaseScope ?? ((_, _) => Array.Empty<string>());
        _separatelyMonitoredDatabases = separatelyMonitoredDatabases ?? (_ => Array.Empty<string>());
        /* #4961: null provider = no install id, so no long-query session can be named (what a test that builds a runner
           without one gets). The worker passes the id it made at start. */
        _installId = installId ?? (() => null);
        _awsRoles = new Targets.AwsRoleCredentialCache(installId: () => _installId(), logger: logger);
        /* #4004: the store's log-hash key, loaded once by the worker at start and shared by every run that hashes log
           text (pg_log_events on both transports). Null = none could be used: those runs refuse, with the reason. */
        _logHashKey = logHashKey;
    }
}
