/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Orchestrates the full analysis pipeline: collect → score → traverse → persist.
/// Can be run on-demand or on a timer. Each run analyzes a single server's data
/// for a given time window and persists the findings.
///
/// <para>
/// Port of Lite's AnalysisService (Phase-5 analysis slice AN3) over the PG pieces — Lite's
/// pipeline ORDER (24h-data-span gate → CollectFacts → DetectAnomalies → ScoreAll →
/// BuildStories → PopulateStoryText → ClusterIntoIncidents + StampClusters → persist →
/// enrich) with the DASHBOARD twin's richer SAVE phase adopted (recommendations rebuild
/// D2/P2): mute-filter WITHOUT inserting → enrich the survivors → build + attach each
/// finding's RemediationAction from the drill-down → insert with
/// <c>remediation_action_json</c> persisted. The V4 schema carries that column and
/// <see cref="PgFindingStore"/> implements the two-phase surface precisely for this.
/// </para>
///
/// <para>
/// Clock semantics are LITE's, not the Dashboard's: the analysis window is built from host
/// UTC and the context times are used as-is (no server-clock probe, no offset math) because
/// Darling's collectors stamp <c>collection_time</c> with the service host's
/// <c>DateTime.UtcNow</c> exactly like Lite's — the windowed reads and the collected rows
/// share one clock already. <c>ServerUtcOffset</c> stays <see cref="TimeSpan.Zero"/>, which
/// <see cref="PgFindingStore.FilterMutedFindingsAsync"/> treats as an identity conversion.
/// </para>
///
/// <para>
/// /* PG port deviations from the twins, the PgFactCollector conventions: the DuckDB
/// initializer / SQL connection string becomes an <see cref="NpgsqlDataSource"/>; Lite's
/// static AppLogger / Dashboard's static Logger become an injected optional
/// <see cref="ILogger"/>; the data-span SQL moves to a <c>public const</c> for the ungated
/// dialect pins (text is Lite's verbatim — <c>EXTRACT(EPOCH FROM ...)</c> is shared-dialect
/// and runs unchanged on Postgres, reading the raw <c>wait_stats</c> table for a SQL Server target
/// with Lite's multi-server <c>server_id = $1</c> filter, which the single-server Dashboard twin
/// drops). Dashboard's <c>GetServerClockAsync</c>/<c>GetServerLocalNowAsync</c> are
/// deliberately NOT ported — they exist only for its server-local collection clock. */
/// </para>
///
/// <para>
/// <b>Two engines, one pipeline (#3542).</b> Since the PostgreSQL-target analysis engine landed, this
/// service holds TWO component sets — <see cref="AnalysisEngineSet"/>: the SQL Server one (the five
/// objects above, unchanged) and the PostgreSQL-target one (<see cref="PgTargetFactCollector"/>,
/// <see cref="PgTargetAnomalyDetector"/>, <see cref="PgTargetRelationshipGraph"/>,
/// <see cref="PgTargetDrillDownCollector"/>, <see cref="PgTargetBaselineProvider"/>, and a data-span
/// gate over <c>pg_database_stats</c>) — and picks one PER CALL by reading the registry's
/// <c>servers.engine_kind</c> for the server being analyzed (<see cref="ResolveEngineAsync"/>). Per call,
/// not per constructor, because two of this class's three construction sites are process-wide singletons
/// shared by every server (the MCP host and the web endpoints), and the MCP tools resolve a server by
/// NAME and never see an engine; only the worker constructs a fresh service per pass and could have
/// passed one. The pipeline body below is engine-blind by construction — gate, collect, detect, score,
/// story, cluster, reconcile, mute-filter, enrich, persist, notify run once, on whichever set was
/// resolved — so a PostgreSQL finding persists, mutes, recurs and notifies through exactly the code a
/// SQL Server finding does (design decision D4, realised at the call rather than the constructor).
/// </para>
/// </summary>
public sealed class DarlingAnalysisService
{
    /// <summary>
    /// One engine's worth of analysis components (#3542): the fact collector, the anomaly detector, the
    /// inference engine over that engine's relationship graph, the drill-down, the baseline provider the
    /// comparison banding reads dispersion from, and the SQL that measures the server's total collected
    /// history for the data-span gate. Two instances live for the life of the service; the pipeline body
    /// never names a concrete type.
    /// </summary>
    internal sealed record AnalysisEngineSet(
        IFactCollector Collector,
        IAnomalyDetector Detector,
        InferenceEngine Engine,
        IDrillDownCollector DrillDown,
        PgBaselineProvider Baselines,
        string DataSpanSql);

    /// <summary>
    /// The deadline every command in the analysis pass runs under (#2871), and the reason it is a
    /// number rather than an inheritance.
    ///
    /// <para><b>Why not the default.</b> An <see cref="NpgsqlCommand"/> with no
    /// <c>CommandTimeout</c> inherits Npgsql's undocumented 30 s, which nobody chose. #2810 found
    /// that in <see cref="PgFactCollector"/>; the rest of the pass had it too. On the dogfood box
    /// the <c>io_latency</c> baseline failed nineteen times over two days and EVERY sample landed
    /// between 30.1 s and 31.4 s — a fixed wall, not a variable-duration fault, and not the 15 s
    /// connection timeout. The same read completes in ~1.6 s normally (measured against the live
    /// store on the three busiest servers), so it only crosses the ceiling when the store stalls,
    /// which is why it looked intermittent. #2820 had already made that query 5.6x faster and #2826
    /// had made the failure visible; neither touched what it was failing against.</para>
    ///
    /// <para><b>Lower bound.</b> The failure record is RIGHT-CENSORED — every run was killed at
    /// 30 s, so nothing says whether it wanted 35 s or 300 s. 60 s is therefore chosen as twice
    /// what it had rather than as a fitted value: it doubles the headroom without claiming a number
    /// the data does not contain, and clears the measured ~1.6 s steady state by a wide margin.</para>
    ///
    /// <para><b>Upper bound, which is the one that binds.</b> The whole pass gets 120 s
    /// (<c>DarlingWorker.s_analysisTimeout</c>), and the fact collector's thirty-one reads, up to
    /// eleven baseline computations per server, the anomaly detector and the drill-down all share
    /// it. A per-command deadline at or near 120 s would let ONE stalled read consume the pass and
    /// cost the server every other fact — strictly worse than today's failure, which loses one
    /// metric. At half the budget a stalled command still leaves half the pass for everything
    /// else. It matches <c>PgFactCollector.FactCommandTimeoutSeconds</c> deliberately: two numbers
    /// in one budget would have to be reasoned about together every time either moved.</para>
    ///
    /// <para>A generous ceiling is inert on a query that returns in milliseconds, so applying it
    /// uniformly costs nothing and removes the trap. When a store genuinely outgrows it the read
    /// starts warning (#2826) instead of failing silently — that is the signal to look at the
    /// window being scanned, not at this constant.</para>
    /// </summary>
    internal const int AnalysisCommandTimeoutSeconds = 60;

    private readonly NpgsqlDataSource _postgres;
    private readonly PgFindingStore _findingStore;
    private readonly FactScorer _scorer;
    private readonly AnalysisEngineSet _sqlServerEngine;
    private readonly AnalysisEngineSet _pgTargetEngine;
    private readonly ILogger? _logger;

    /// <summary>
    /// Minimum hours of collected data required before analysis will run.
    /// Short collection windows distort fraction-of-period calculations —
    /// 5 seconds of THREADPOOL looks alarming in a 16-minute window.
    /// 24 hours has been validated empirically as sufficient.
    /// </summary>
    internal double MinimumDataHours { get; set; } = 24;

    /// <summary>
    /// Raised after each analysis run completes, providing the findings — the twins' UI
    /// hook, kept so the surface stays twin-shaped (the worker awaits AnalyzeAsync's return
    /// directly, like Lite's scheduler does).
    /// </summary>
    public event EventHandler<AnalysisCompletedEventArgs>? AnalysisCompleted;

    /// <summary>
    /// Whether an analysis is currently running.
    /// </summary>
    public bool IsAnalyzing { get; private set; }

    /// <summary>
    /// Time of the last completed analysis run.
    /// </summary>
    public DateTime? LastAnalysisTime { get; private set; }

    /// <summary>
    /// Set after AnalyzeAsync if insufficient data was found. Null if enough data exists.
    /// </summary>
    public string? InsufficientDataMessage { get; private set; }

    /// <summary>
    /// Set after AnalyzeAsync when the server PASSED the data-span gate but the collector observed NONE
    /// of the analysis window (#3524, #3538 A2): the coverage witness found no collection interval inside
    /// it. Null otherwise. The gate measures TOTAL history, so a server whose collection died still sails
    /// through it and lands on an unobserved window — which is a dead collector or an unreachable target,
    /// not a healthy server. Callers must not render an empty findings list as an all-clear while this is
    /// set; nothing was measured.
    ///
    /// <para>#3653: this is the UNOBSERVED-window message and nothing else. An observed window over which
    /// the collector emitted no fact leaves it null and runs the pass — that is a measurement with nothing
    /// in it, and <see cref="LastWindowCoverage"/> says how much of the window the all-clear rests on.</para>
    /// </summary>
    public string? WindowEmptyMessage { get; private set; }

    /// <summary>
    /// How much of the last pass's window the collector actually observed (#3538 A2), stamped by the
    /// fact collector and carried out here the way <see cref="WindowEmptyMessage"/> is, because the
    /// findings list cannot say it: a pass over a window with a three-hour hole returns the SAME shape as
    /// a pass over a fully collected one, and only this tells the caller that the rates were divided by
    /// one hour rather than four and that the caveat is owed. Null when the pass never reached
    /// collection (the data-span gate, or a fault before it).
    /// </summary>
    public WindowCoverage? LastWindowCoverage { get; private set; }

    /// <summary>
    /// The fact families whose read FAILED in the last pass (#3691), carried out the way
    /// <see cref="LastWindowCoverage"/> is and for the same reason: the findings list cannot say it. A pass
    /// in which every family failed returns the same empty list as a quiet server, and until now the tools
    /// rendered it as the <c>empty</c> all-clear. Empty on a clean pass and on a pass that never reached
    /// collection; <see cref="LastCollectionFamilyCount"/> is the total the count is stated against.
    /// </summary>
    public IReadOnlyList<CollectionFailure> LastCollectionFailures { get; private set; } = [];

    /// <summary>How many family reads the collector ran in the last pass — the caveat's denominator, stamped
    /// by the collector from its own type (#3691). 0 when the pass never reached collection.</summary>
    public int LastCollectionFamilyCount { get; private set; }

    /// <summary>
    /// How many facts the last pass handed to the scorer — the collector's plus the anomaly detector's
    /// (#3691) — and how many of those came out with a non-zero severity. Both null when the pass never
    /// reached scoring (the data-span gate, the unobserved window, a fault). They exist so an <c>empty</c>
    /// envelope can say which kind of nothing it is: "facts were scored and none fired" (both &gt; 0),
    /// "facts were read and none graded above zero" (count &gt; 0, scored 0), or "no fact was emitted"
    /// (count 0) — which, beside <see cref="LastCollectionFailures"/>, is the difference between a quiet
    /// server and a blind pass.
    /// </summary>
    public int? LastFactCount { get; private set; }

    /// <summary>See <see cref="LastFactCount"/>: of those facts, how many scored above zero.</summary>
    public int? LastFactsScored { get; private set; }

    /// <summary>
    /// How the last pass ended EARLY, or null when it ran through (#2430). Set inside the pass's own
    /// catch, so <see cref="AnalysisAbandonKind.None"/> here means a genuine fault: the pass reached the
    /// catch and the classifier said it was not an abandonment.
    ///
    /// <para>Carried out to the caller because the caller cannot re-derive it. "No findings and the
    /// budget token has fired" is true of a fault as well as of a timeout, and inferring a timeout from
    /// it buries the fault's ERROR under a Warning that says the pass merely ran out of time. The pass
    /// has already classified this once and logged the one line for it; this is how the scheduler reads
    /// that answer instead of guessing at a second one.</para>
    /// </summary>
    public AnalysisAbandonKind? EndedEarlyAs { get; private set; }

    /// <param name="postgres">The store, read as whatever role this data source connects as.</param>
    /// <param name="planFetcher">Optional; the SQL Server drill-down's cached-plan fetch.</param>
    /// <param name="logger">Optional.</param>
    /// <param name="baselineCache">#3941: the process's shared baseline tier. The worker builds a fresh service per pass
    /// (see <see cref="IsAnalyzing"/>), so without it every pass recomputed every 30-day baseline and the MCP and web
    /// hosts paid for them again; with it, all of them share one compute per series per analysis hour. Null keeps each
    /// provider's cache private to this instance, as before.</param>
    public DarlingAnalysisService(
        NpgsqlDataSource postgres, IPlanFetcher? planFetcher = null, ILogger? logger = null, BaselineCache? baselineCache = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        _findingStore = new PgFindingStore(postgres, logger);
        _scorer = new FactScorer();

        /* The SQL Server set: the five objects this service always composed, in the same order. */
        var sqlServerBaselines = new PgBaselineProvider(postgres, logger, baselineCache);
        _sqlServerEngine = new AnalysisEngineSet(
            new PgFactCollector(postgres, logger),
            new PgAnomalyDetector(postgres, sqlServerBaselines, logger),
            new InferenceEngine(new RelationshipGraph()),
            new PgDrillDownCollector(postgres, planFetcher, logger),
            sqlServerBaselines,
            TotalDataSpanSql);

        /* The PostgreSQL-target set (#3542). No plan fetcher: that object connects to a monitored SQL Server
           to fetch a cached plan, and a PostgreSQL target's plans arrive through the collectors
           (pg_plan_capture) — there is nothing for it to fetch. */
        var pgTargetBaselines = new PgTargetBaselineProvider(postgres, logger, baselineCache);
        _pgTargetEngine = new AnalysisEngineSet(
            new PgTargetFactCollector(postgres, logger),
            new PgTargetAnomalyDetector(postgres, pgTargetBaselines, logger),
            new InferenceEngine(new PgTargetRelationshipGraph()),
            new PgTargetDrillDownCollector(postgres, logger),
            pgTargetBaselines,
            PgTargetDataSpanSql);
    }

    /// <summary>
    /// Runs the full analysis pipeline for a server.
    /// Default time range is the last 4 hours. Host-UTC window (Lite's clock semantics —
    /// Darling's collectors stamp rows with the service host's UTC clock).
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> moves the END of that window off "now" while
    /// <paramref name="hoursBack"/> stays its LENGTH, so an incident can be analyzed where it happened.
    /// Null — every caller but the anchored MCP tool — is the pre-#2506 behaviour exactly. Anchoring
    /// reaches the whole pipeline through the context, including the anomaly detector's hour-of-day ×
    /// day-of-week baseline, which is keyed off the window rather than off the clock; that is what makes
    /// the answer for a past window the same KIND of answer, and not merely a differently-filtered one.
    /// An anchored pass does not persist — see <see cref="AnalysisContext.PersistFindings"/>.</para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1068:CancellationToken parameters must come last",
        Justification = "The two tokens are at positions 4 and 5 and the Darling worker passes them POSITIONALLY. " +
                        "Moving asOfUtc ahead of them to satisfy the rule would silently rebind that call site's " +
                        "arguments — a compiling change of meaning on the one caller that matters. Appending is the " +
                        "only edit that cannot do that, and Lite's twin keeps the same order so the two stay transplantable.")]
    public async Task<List<AnalysisFinding>> AnalyzeAsync(
        int serverId, string serverName, int hoursBack = 4, CancellationToken cancellationToken = default,
        CancellationToken shutdownToken = default, DateTime? asOfUtc = null)
    {
        var timeRangeEnd = asOfUtc ?? DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            AsOfUtc = asOfUtc,
            CancellationToken = cancellationToken,

            /* #2430. The fifth argument is what keeps the abandon classification truthful once
               cancellationToken is a BUDGET rather than the stopping token. Defaulting it to None is
               deliberate rather than lazy: an on-demand caller (the MCP analyze_server tool, the
               Viewer) has no service stop to distinguish, so its cancellations are timeouts and
               should read as timeouts. The scheduled worker is the one caller that has both, and it
               is the only one that passes both. */
            ShutdownToken = shutdownToken
        };

        return await AnalyzeAsync(context);
    }

    /// <summary>
    /// Runs the full analysis pipeline with a specific context.
    /// </summary>
    public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)
    {
        if (IsAnalyzing)
            return [];

        IsAnalyzing = true;
        InsufficientDataMessage = null;
        WindowEmptyMessage = null;
        LastWindowCoverage = null;
        LastCollectionFailures = [];
        LastCollectionFamilyCount = 0;
        LastFactCount = null;
        LastFactsScored = null;
        EndedEarlyAs = null;

        try
        {
            /* #3542: which engine's components this pass runs on, decided from the registry for THIS server
               and this call. Ahead of the data-span gate because the gate itself is engine-specific: a
               PostgreSQL target has no wait_stats rows and would otherwise sit at "0 hours of history" forever,
               which is the exact lie the deleted worker-side tombstone existed to paper over. */
            var (engine, engineKind) = await ResolveEngineAsync(context.ServerId, context.CancellationToken);

            // 0. Check minimum data span — total history, not the analysis window.
            // A server with 100h of total history can be analyzed over a 4h window.
            var dataSpanHours = await GetTotalDataSpanHoursAsync(engine, context.ServerId, context.CancellationToken);
            if (dataSpanHours < MinimumDataHours)
            {
                var needed = MinimumDataHours >= 24
                    ? $"{MinimumDataHours / 24:F1} days"
                    : $"{MinimumDataHours:F0} hours";
                var have = dataSpanHours >= 24
                    ? $"{dataSpanHours / 24:F1} days"
                    : $"{dataSpanHours:F1} hours";

                InsufficientDataMessage =
                    $"Not enough data for reliable analysis. Need {needed} of collected data, " +
                    $"have {have}. Keep the collector running and try again later.";

                /* #3542: an UNSTAMPED registry row took the SQL Server set above (a NULL makes no claim,
                   #2530). For a SQL Server target that is today's answer exactly; for a PostgreSQL target
                   whose connect has not yet recorded engine_kind it is the wrong series, and "0 hours"
                   would read as "still collecting" forever. So the message names the missing fact instead
                   of guessing from the data — the stamp arrives on the next successful connect and the
                   next pass measures the right series without anyone doing anything. Stamped rows get the
                   sentence above, byte for byte. */
                if (engineKind is null)
                {
                    InsufficientDataMessage +=
                        " This server's registry row carries no engine stamp yet (no connect has recorded engine_kind), " +
                        "so the history was measured on the SQL Server series; if this is a PostgreSQL target, the next " +
                        "successful connect stamps it and the next pass measures pg_database_stats instead.";
                }

                _logger?.LogInformation(
                    "[DarlingAnalysisService] Skipping analysis for {Server}: {Have:F1}h data, need {Need}h",
                    context.ServerName, dataSpanHours, MinimumDataHours);

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            /* #2299: abandon BETWEEN the expensive store stages when the host is stopping. The
               fact collector's per-query catches are deliberately silent, so a stop mid-collect
               cannot unwind from inside it — these boundary checks are what turn the token into
               an exit. The post-enrichment tail (action build + insert) carries no check: by
               then the expensive work is done, and finishing preserves it when the store is
               still up, while a store already gone classifies quietly. */
            context.CancellationToken.ThrowIfCancellationRequested();

            // 1. Collect facts from the Postgres store
            var facts = await engine.Collector.CollectFactsAsync(context);
            LastWindowCoverage = context.Coverage;
            LastCollectionFailures = context.CollectionFailures;
            LastCollectionFamilyCount = context.CollectionFamilyCount;

            /* #3691 lane 45: the same two facts into the PROCESS-WIDE ledger, because the two properties
               above are on THIS INSTANCE and the worker builds a fresh analysis service for every scheduled
               pass — so on a production install, where every pass is scheduled, the singleton the MCP tools
               were injected with is permanently empty and a scheduled pass's caveats reached nothing but the
               log line below. Recorded on EVERY pass, clean ones included: a clean pass is the only evidence
               that a family recovered. Failure-free by construction (a bounded in-memory ring), so it needs
               no isolation wrap of its own. */
            CollectionCaveatLedger.Shared.Record(
                context.ServerId, context.ServerName, DateTime.UtcNow,
                context.CollectionFailures, context.CollectionFamilyCount);

            /* #3691 part a1: the store side, so a separate Viewer process reading only the store learns what
               only the in-process ledger above knew. Best-effort by construction (ApplyPassAsync never
               throws), and awaited: a store failure is logged once at Warning and never fails or blocks the
               pass, but the pass still finishes writing it before moving on, matching the ledger call above.
               Only the failed families are named; "every other family this server had a row for" is read as
               "read successfully" and cleared — see ApplyPassAsync for why there is no separate read list. */
            var unreadFamilies = context.CollectionFailures
                .GroupBy(f => f.Family, StringComparer.Ordinal)
                .Select(g => new CollectionCaveatStore.UnreadFamily(g.Key, CollectionFailure.Label(g.Last().Outcome)))
                .ToArray();

            await CollectionCaveatStore.ApplyPassAsync(
                _postgres, context.ServerId, unreadFamilies, DateTime.UtcNow, _logger, context.CancellationToken);

            if (context.CollectionFailures.Count > 0)
            {
                /* #3691: the per-site log lines above say each failure as it happened; this one line says
                   what the PASS is missing, so a scheduled pass — which has no payload to carry
                   collection_caveats — still leaves the summary beside the findings it persists. Every
                   envelope the tools render off this pass carries the same sentence. */
                _logger?.LogWarning("[DarlingAnalysisService] Collection caveat for {ServerName}: {Caveat}",
                    context.ServerName, CollectionCaveats.Describe(context.CollectionFailures, context.CollectionFamilyCount));
            }

            if (context.ObservedDurationMs <= 0)
            {
                /* #3524: the span gate above passed on LIFETIME history, so an empty WINDOW here means
                   collection stopped producing rows for it — not that the server is healthy. Say so,
                   instead of returning a bare [] that reads exactly like "analyzed and found nothing".

                   #3538 A2 widens the branch to a window with NO OBSERVED COLLECTION TIME even when some
                   facts exist. The facts that survive an unobserved window are the point-in-time ones
                   (server config, trace flags, hardware) — read from the latest row regardless of
                   window — and scoring those alone would produce a pass whose every windowed rate is
                   absent and whose all-clear (or config-only findings) still reads as "analyzed this
                   window". Nothing was measured over the window; the same envelope says so.

                   #3653: the branch is gated on the coverage witness ALONE. Until then it read
                   `facts.Count == 0 || context.ObservedDurationMs <= 0`, and the first half — the original
                   #3524 test, written before any collector stamped coverage — made an OBSERVED window that
                   happened to produce no fact wear this envelope too: "collection appears to have stopped"
                   over a window the witness proves the collector was up for, rendered `unavailable` by both
                   analyze_server tools with a pointer at collection health, and persisted by the worker as
                   the dead-collector marker the Viewer renders. That case falls through to the branch below.
                   This envelope is now for exactly the case its prose is true of: no collection interval
                   landed inside the window. Every collector stamps the witness before its first windowed
                   read (wait_stats on a SQL Server target, pg_database_stats on a PostgreSQL one, #3665), so
                   "unstamped" is not a third state here — Coverage is null only when collection threw, and
                   that unwinds through the catch below, never through this test. */
                /* True when the WINDOW went unobserved but point-in-time facts (config, trace flags,
                   hardware) still read — the case that used to slip past the facts.Count == 0 check. */
                var hasPointInTimeFactsOnly = facts.Count > 0;
                WindowEmptyMessage = hasPointInTimeFactsOnly
                    ? $"The collector observed none of the analysis window " +
                      $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC): " +
                      $"no collection interval landed inside it, even though this server has {dataSpanHours:F1} hours " +
                      $"of total collected history. The {facts.Count} fact(s) that could still be read are point-in-time " +
                      "configuration and state, not measurements of this window. Collection appears to have stopped or " +
                      "broken for this window, so nothing was measured — this is NOT an all-clear."
                    : $"No facts were collected in the analysis window " +
                      $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC) " +
                      $"even though this server has {dataSpanHours:F1} hours of total collected history. " +
                      "Collection appears to have stopped or broken for this window, so nothing was measured " +
                      "— this is NOT an all-clear.";

                _logger?.LogWarning(
                    "[DarlingAnalysisService] No observed collection in the analysis window for {Server} ({Start} to {End}) " +
                    "despite {Span:F1}h of total history — collection may be down ({FactCount} point-in-time fact(s) only)",
                    context.ServerName, context.TimeRangeStart, context.TimeRangeEnd, dataSpanHours, facts.Count);

                LastAnalysisTime = DateTime.UtcNow;
                return [];
            }

            if (facts.Count == 0)
            {
                /* #3653: the collector OBSERVED the window — the witness found collection intervals inside
                   it — and emitted no fact over it. Nothing rose to a fact: no wait accrued time, no event
                   table has a row in the window, and no point-in-time family had a latest row to read. That
                   is not the dead-collector shape above, and it must not wear its envelope: WindowEmptyMessage
                   stays null, so both analyze_server tools render whatever this pass returns as `empty` with
                   the coverage block rather than `unavailable`, the worker clears the analysis_state marker
                   instead of writing "collection appears broken", and neither Viewer points at collection
                   health for a collector that is up.

                   The pass CONTINUES rather than returning [] here, because "the collector emitted no fact"
                   is a statement about the collector's reads, not the detector's: the anomaly detector reads
                   the store on its own and still gets its say, and whatever survives scoring is the answer.
                   A zero-finding pass out of here is an all-clear AT THE STATED COVERAGE — the coverage block
                   every tool payload carries says which fraction of the window it rests on, and the line
                   below says the same for the scheduled pass, which has no payload to put it in.

                   Partial coverage cannot arrive here. The collector adds the COLLECTION_GAP context fact
                   whenever the observed fraction is under WindowCoverage.PartialThreshold, so zero facts with
                   observed time means coverage at or above the bar; the partial caveat below is therefore
                   unreachable from this branch by construction, not by luck. Information rather than Warning:
                   nothing is wrong with the collector, and a Warning here would be the old lie at a lower
                   volume. */
                _logger?.LogInformation(
                    "[DarlingAnalysisService] The collector observed the analysis window for {Server} ({Start} to {End}) " +
                    "but emitted no fact over it — {Coverage}; nothing rose to a fact, and an all-clear from this pass rests on that coverage",
                    context.ServerName, context.TimeRangeStart, context.TimeRangeEnd, context.Coverage!.Describe());
            }

            if (context.Coverage is { IsPartial: true } partial)
            {
                /* Logged, not just carried: a scheduled pass has no payload to put the caveat in, and
                   the persisted findings from this pass were rated against the observed hours. */
                _logger?.LogWarning(
                    "[DarlingAnalysisService] Partial collection coverage for {Server}: {Coverage} — rates in this pass are per observed time, and the COLLECTION_GAP fact records the hole",
                    context.ServerName, partial.Describe());
            }

            context.CancellationToken.ThrowIfCancellationRequested();

            // 1.5. Detect anomalies (compare analysis window against baseline)
            var anomalies = await engine.Detector.DetectAnomaliesAsync(context);
            facts.AddRange(anomalies);

            // 2. Score facts (base severity + amplifiers)
            _scorer.ScoreAll(facts);

            /* #3691: what the scorer saw and what it graded, for the `empty` envelope's two counts. Read
               here, after scoring and before attribution adds its own cards, so "facts_scored" means the
               collector's and detector's facts and nothing composed later. */
            LastFactCount = facts.Count;
            LastFactsScored = facts.Count(f => f.Severity > 0);

            // 2.5. Config → outcome attribution (#3653 A10, Q2). AFTER scoring, never before: the fact
            // is appended with its Information severity preset (ConfigChangeAttribution.InformationSeverity),
            // because FactScorer.ScoreConfigFact returns 0 for a "config" key it does not know and ScoreAll
            // would zero it out of the working set. SQL Server engine only — the PostgreSQL-target config
            // family is #3691's. Its own try inside: a failed snapshot read or compare costs the pass this
            // one card, never the pass.
            context.CancellationToken.ThrowIfCancellationRequested();
            await AttributeConfigChangesAsync(engine, context, facts);

            // 3. Build stories via graph traversal
            var stories = engine.Engine.BuildStories(facts);

            // 3.5. Freeze value-stated advice (current MAXDOP/CTFP/etc.) into each story's StoryText
            // from the FULL fact set, BEFORE the store copies StoryText onto the finding. This is the
            // only place the raw fact VALUES are in scope; read-back cards then state the numbers
            // (FactAdvice.GetComposedForFinding) instead of generic folklore. No schema change.
            FactAdvice.PopulateStoryText(stories, facts);

            // 3.6. Cluster the run's stories into causally-related incidents (graph-connectivity) and
            // stamp each with its own trackable id, BEFORE the store copies it onto the finding. The
            // grouped surface renders one report per incident; the id fingerprints the incident's
            // primary so the same recurring incident is trackable across runs.
            var incidents = engine.Engine.ClusterIntoIncidents(stories, facts);
            IncidentId.StampClusters(context.ServerName, incidents);

            // 3.7. Fold each ANOMALY_* story into the REGULAR finding that describes the same symptom
            // (same run, same database) by rewriting its stamped incident id onto that parent's — so
            // the anomaly stops rendering as its own card / its own email. No-parent anomalies stay
            // solo; db-scoped object anomalies never cross databases. Presentation-only: nothing is
            // dropped, only the incident tag is reconciled. The facts ride along (#3704) so a
            // maintenance-family anomaly folded onto a fired RUNNING_JOBS' incident can name the job
            // in its frozen StoryText — the name is on the fact (#3693), not on any story.
            AnomalyIncidentReconciler.Reconcile(stories, facts);

            // 3.8. Label the chains that keep a weekly schedule (#3653 item 3, ruling Q3: LABEL, not discount).
            // One store read of the prior three weeks — every chain that fired in this pass's hour×weekday
            // slot on the target's clock, plus every RUNNING_JOBS card in any slot — then RecurrenceLabeler
            // appends one sentence to the frozen StoryText of each story whose chain fired in this slot for
            // three consecutive weeks ("recurring at this hour"), and one to each story tied to a fired job
            // whose slot MOVED since last week ("maintenance window moved"). Severity is untouched, by ruling.
            // Here — after the fold has finished rewriting StoryText and before the store copies it onto the
            // finding — so the label is on the persisted row every card, e-mail and MCP read renders. The
            // reference instant is the window's end: "now" for a scheduled pass, the anchor for an as-of one,
            // so an exploratory pass is labelled relative to the instant it explores. A read the store could
            // not make labels nothing and costs the pass nothing (PgFindingStore.GetPriorOccurrencesAsync).
            var priorOccurrences = await _findingStore.GetPriorOccurrencesAsync(context, context.TimeRangeEnd);
            RecurrenceLabeler.Label(stories, facts, context.TimeRangeEnd, priorOccurrences);

            // 4. Mute-filter the stories into the surviving findings (the Dashboard twin's D2/P2
            //    reorder) — WITHOUT inserting yet, so enrichment + action-build happen on the
            //    survivors first and the BUILT RemediationAction is persisted on each row. Muted/
            //    absolution findings are dropped here and never enriched.
            var findings = await _findingStore.FilterMutedFindingsAsync(stories, context);

            // 5. Enrich the survivors with drill-down data (ephemeral except through the built
            //    action; the cheap config drill-downs run below the 0.5 gate inside the collector).
            await engine.DrillDown.EnrichFindingsAsync(findings, context);

            // 6. Build + attach each finding's RemediationAction from the now drill-down-
            //    populated finding (D2). The builders REQUIRE finding.DrillDown, which the
            //    store read-back does not return — so the BUILT action is persisted, exactly
            //    the artifact the alert path serializes into ContextJson. Try the always-safe/
            //    db-config force action first, then the destructive entry points (each gates
            //    internally on RootFactKey + drill-down and returns null when N/A); attach the
            //    first non-null.
            foreach (var finding in findings)
            {
                finding.Remediation =
                    FactRemediation.BuildAction(finding)
                    ?? FactRemediation.BuildRcsiAction(finding)
                    ?? FactRemediation.BuildClearPlanAction(finding)
                    ?? FactRemediation.BuildFileAutogrowthAction(finding) // WS3: advisory only (no handler -> no Apply); carried for the read-time copy-paste
                    ?? FactRemediation.BuildServerConfigAction(finding) // WS3: server-level config — MAXDOP/CTFP/memory
                    ?? FactRemediation.BuildMissingIndexAction(finding); // WS4: missing-index CREATE — copy-paste only
            }

            // 7. Insert the survivors in one batched pass, persisting remediation_action_json —
            //    UNLESS the window was anchored at a past instant (#2506), in which case the pass is
            //    exploratory and writes nothing. The findings are still built, enriched and returned in
            //    full; only the row is withheld, because the row would claim to be a current
            //    observation. AnalysisContext.PersistFindings carries the whole argument.
            if (context.PersistFindings)
            {
                await _findingStore.InsertFindingsAsync(findings, context);
            }

            LastAnalysisTime = DateTime.UtcNow;

            // 8. Notify listeners — the returned/enriched findings (now action-bearing) also
            //    flow back to the caller (the worker), which routes them to the shared
            //    AnalysisNotificationService. Gated with the insert for the same reason and not a
            //    weaker one: this event is how findings reach notification, and an alert about last
            //    Tuesday delivered today is the persistence problem with a shorter fuse.
            if (context.PersistFindings)
            {
                AnalysisCompleted?.Invoke(this, new AnalysisCompletedEventArgs
                {
                    ServerId = context.ServerId,
                    ServerName = context.ServerName,
                    Findings = findings,
                    AnalysisTime = LastAnalysisTime.Value
                });
            }

            _logger?.LogInformation(
                "[DarlingAnalysisService] Analysis complete for {Server}: {Count} finding(s), highest severity {Severity:F2}{Exploratory}",
                context.ServerName, findings.Count, findings.Count > 0 ? findings.Max(f => f.Severity) : 0,
                context.PersistFindings ? string.Empty : " (anchored window — exploratory, not persisted)");

            return findings;
        }
        catch (Exception ex)
        {
            /* #2299: the ONE line an abandonment is allowed to cost. The component catches let the
               residue propagate instead of logging it per-metric, so seven ERRORs collapse to a single
               line here — and it states the loss honestly: whatever this pass would have written is
               gone, and the next scheduled pass recomputes it from the store.

               #2430 split that line in two, because the pass token now fires for two very different
               reasons and only one of them is fine. Getting this wrong is the reason the Lite fix could
               not simply be ported: arm the token with a budget while the classifier still asks "are we
               stopping?", and every ordinary overrun on a healthy service reports itself at Information
               as a clean stop — a wrong answer wearing a calm one's clothes, on exactly the signal
               someone would use to decide the budget needs raising.

               Classified ONCE, in the catch body rather than across two exception filters, because the
               three outcomes are one decision and splitting it would mean evaluating it twice and
               letting the halves drift. */
            EndedEarlyAs = AnalysisShutdown.Classify(ex, context.ShutdownToken, context.CancellationToken);

            switch (EndedEarlyAs)
            {
                case AnalysisAbandonKind.Shutdown:
                    _logger?.LogInformation(
                        "[DarlingAnalysisService] Analysis abandoned at shutdown for {Server} — this pass's findings are lost by design; the next pass recomputes them ({Detail})",
                        context.ServerName, ex.Message);
                    break;

                case AnalysisAbandonKind.Timeout:
                    _logger?.LogWarning(
                        "[DarlingAnalysisService] Analysis for {Server} was cancelled at its per-pass budget and unwound as asked — this cycle produces no findings and the next one recomputes them. A pass that keeps hitting this is not finishing inside its budget, which is a server whose analysis is quietly getting less complete, not a stop ({Detail})",
                        context.ServerName, ex.Message);
                    break;

                default:
                    _logger?.LogError("[DarlingAnalysisService] Analysis failed for {Server}: {Message}",
                        context.ServerName, ex.Message);
                    break;
            }

            return [];
        }
        finally
        {
            IsAnalyzing = false;
        }
    }

    /// <summary>
    /// Runs the collect + detect + score pipeline without graph traversal.
    /// Returns raw scored facts with amplifier details for direct inspection, together with the
    /// window's observed coverage (#3538 A2) — returned rather than parked on a property because this
    /// path has no <see cref="IsAnalyzing"/> guard and two on-demand callers can overlap; a shared
    /// property would let one read the other's window. Coverage is null only when collection threw.
    /// The third element (#3691) is the same context's collection failures and family total, for the
    /// callers' <c>collection_caveats</c> — returned for the same overlap reason, and populated even when
    /// collection threw, so a read that recorded failures before the throw still reports them.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the END of the window; null is "now", which is
    /// every caller but the anchored MCP tool. Nothing here persists, so the anchor carries no
    /// write-side question — this is a read that happens to score what it read.</para>
    ///
    /// <para>#3691: the anomaly detector runs here too, over the same context, between collection and
    /// scoring — the order the full pass uses. Until then this read was collector + scorer only, so the
    /// fact set <c>get_analysis_facts</c> sold as "every observation the engine sees" never held an
    /// <c>ANOMALY_*</c> fact, and the gate metadata the detector stamps (deviation_sigma, fire_threshold,
    /// sample_count, baseline_confidence, threshold_lineage) was reachable only through a finding that
    /// had already survived the severity floor; an anomaly that fired and scored under 0.5 was invisible
    /// everywhere. The cost is the detector's baseline reads (one per baselined metric) on top of the
    /// collector's, which is what the full pass already pays for the same answer. The detector is gated
    /// the way the pass gates it: it does not run over a window the collector never observed, because a
    /// deviation is measured against the window's own rate, and an unobserved window has none — the
    /// callers' unobserved envelope keeps describing exactly the point-in-time facts it names. The
    /// detector is the RESOLVED engine's (<see cref="AnalysisEngineSet.Detector"/>): a PostgreSQL target
    /// gets <see cref="PgTargetAnomalyDetector"/>'s <c>ANOMALY_PG_*</c> facts and a SQL Server target
    /// gets <see cref="PgAnomalyDetector"/>'s, off the one resolution this read already performs.</para>
    /// </summary>
    public async Task<(List<Fact> Facts, WindowCoverage? Coverage, CollectionCaveatState Caveats)> CollectAndScoreFactsAsync(
        int serverId, string serverName, int hoursBack = 4, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        var timeRangeEnd = asOfUtc ?? DateTime.UtcNow;
        var timeRangeStart = timeRangeEnd.AddHours(-hoursBack);

        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeStart,
            TimeRangeEnd = timeRangeEnd,
            AsOfUtc = asOfUtc,
            CancellationToken = cancellationToken
        };

        try
        {
            var (engine, _) = await ResolveEngineAsync(context.ServerId, context.CancellationToken);
            var facts = await engine.Collector.CollectFactsAsync(context);
            if (context.ObservedDurationMs > 0)
            {
                /* Same detector, same context, same position as the pass (#3691). A collector that
                   observed the window but emitted no fact does not short-circuit this: the detector reads
                   the store on its own and its facts are part of what this read shows. */
                var anomalies = await engine.Detector.DetectAnomaliesAsync(context);
                facts.AddRange(anomalies);
            }
            if (facts.Count == 0) return (facts, context.Coverage, CollectionCaveatState.From(context));
            _scorer.ScoreAll(facts);
            return (facts, context.Coverage, CollectionCaveatState.From(context));
        }
        catch (Exception ex)
        {
            _logger?.LogError("[DarlingAnalysisService] Fact collection or anomaly detection failed for {Server}: {Message}",
                serverName, ex.Message);
            return ([], null, CollectionCaveatState.From(context));
        }
    }

    /// <summary>
    /// audit_config's own read (#4192): the resolved engine's narrow family collection, never the full
    /// collect + detect + score pass <see cref="CollectAndScoreFactsAsync"/> runs. No coverage witness, no
    /// anomaly detector — audit_config projects 8 point-in-time facts (SQL Server) or the
    /// CONFIG_PG_*/host-memory facts (PostgreSQL) and discards WindowCoverage already, so the full pass was
    /// paying for, and this skips, every other family plus the detector's baseline reads. Falls back to the
    /// full <see cref="IFactCollector.CollectFactsAsync"/> for an engine that is neither of Darling's two
    /// concrete collectors (there is none today; the fallback keeps this correct rather than throwing if one
    /// is ever added without a narrow read of its own).
    ///
    /// <para>#4206: scorer IS run, on the narrow fact set. The PostgreSQL arm's AuditConfig projection maps
    /// <see cref="Fact.Severity"/> to the ok/review/warning vocabulary, so facts must be scored before they
    /// are returned. Config-family scoring is self-contained (pure threshold checks, no amplifiers that need
    /// wait-stats or blocking), so running <see cref="FactScorer.ScoreAll"/> over the narrow set is correct
    /// and cheap.</para>
    /// </summary>
    public async Task<List<Fact>> CollectConfigAuditFactsAsync(
        int serverId, string serverName, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        var timeRangeEnd = asOfUtc ?? DateTime.UtcNow;
        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = timeRangeEnd.AddHours(-1),
            TimeRangeEnd = timeRangeEnd,
            AsOfUtc = asOfUtc,
            CancellationToken = cancellationToken
        };

        try
        {
            var (engine, _) = await ResolveEngineAsync(serverId, context.CancellationToken);
            var facts = engine.Collector switch
            {
                PgFactCollector sql => await sql.CollectConfigAuditFactsAsync(context),
                PgTargetFactCollector pg => await pg.CollectConfigAuditFactsAsync(context),
                _ => await engine.Collector.CollectFactsAsync(context),
            };
            /* #4206: Score so AuditConfig can map fact.Severity to ok/review/warning. The narrow
               collector runs only the config-family partials, so ScoreAll sees only those facts;
               no amplifiers that need wait-stats or blocking fire, and the config scorer's threshold
               checks (shared_buffers ≤ 128 MB → 0.4, max_wal_size ≤ 1 GB → 0.4) are self-contained. */
            _scorer.ScoreAll(facts);
            return facts;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #4203: cancellation (an abandoned web/MCP read) must reach the caller as
               OperationCanceledException, not be swallowed into an empty result and logged as a fault —
               this method has no per-pass budget of its own, so the only source of a cancelled
               context.CancellationToken is the caller's own token. */
            _logger?.LogError("[DarlingAnalysisService] Config-audit fact collection failed for {Server}: {Message}",
                serverName, ex.Message);
            return [];
        }
    }

    /// <summary>
    /// Compares analysis of two time periods, returning facts from both for comparison, each with its
    /// window's observed coverage (#3538 A2) so the caller can say when one side was only partly
    /// collected — the case the empty-window caveats never reached, where a half-collected window
    /// produces confident numbers with nothing to flag them.
    ///
    /// <para>#3538 A3: also returns the stored per-server dispersion for the baselined metrics some
    /// compared key is measured in (<see cref="ComparisonBanding.DispersionMetricsFor"/>), keyed by metric
    /// name, so <c>compare_analysis</c> can band a CPU or read-latency delta in the server's own robust
    /// sigma instead of on a flat severity dead-band. The bucket is the comparison window's START hour
    /// × day-of-week — the same coordinate the anomaly detectors read for a pass over that window
    /// (<c>PgAnomalyDetector</c> passes <c>context.TimeRangeStart</c>), so the default same-hour-yesterday
    /// call reuses the pass's cached buckets and an anchored one recomputes them the way an anchored
    /// <c>analyze_server</c> does. The lookups are fenced separately from collection: a baseline read
    /// that fails must not cost the caller the comparison it was only meant to refine, so it degrades
    /// to an empty map and every key takes the absolute rule — the never-blind fallback the anomaly
    /// gate follows.</para>
    /// </summary>
    public async Task<(List<Fact> BaselineFacts, List<Fact> ComparisonFacts, WindowCoverage? BaselineCoverage, WindowCoverage? ComparisonCoverage, IReadOnlyDictionary<string, BaselineBucket> Dispersion)> ComparePeriodsAsync(
        int serverId, string serverName,
        DateTime baselineStart, DateTime baselineEnd,
        DateTime comparisonStart, DateTime comparisonEnd,
        CancellationToken cancellationToken = default)
    {
        var baselineContext = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = baselineStart,
            TimeRangeEnd = baselineEnd,
            CancellationToken = cancellationToken
        };

        var comparisonContext = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = comparisonStart,
            TimeRangeEnd = comparisonEnd,
            CancellationToken = cancellationToken
        };

        try
        {
            var (engine, _) = await ResolveEngineAsync(serverId, comparisonContext.CancellationToken);
            var baselineFacts = await engine.Collector.CollectFactsAsync(baselineContext);
            var comparisonFacts = await engine.Collector.CollectFactsAsync(comparisonContext);

            _scorer.ScoreAll(baselineFacts);
            _scorer.ScoreAll(comparisonFacts);

            var dispersion = await LookUpDispersionAsync(engine, serverId, serverName, baselineFacts, comparisonFacts, comparisonStart);

            return (baselineFacts, comparisonFacts, baselineContext.Coverage, comparisonContext.Coverage, dispersion);
        }
        catch (Exception ex)
        {
            _logger?.LogError("[DarlingAnalysisService] Period comparison failed for {Server}: {Message}",
                serverName, ex.Message);
            return ([], [], null, null, new Dictionary<string, BaselineBucket>());
        }
    }

    /// <summary>
    /// The baseline buckets <see cref="ComparePeriodsAsync"/> hands to the comparison, one per metric some
    /// compared key is measured in. Its own try: see the summary above for why a failed baseline read
    /// degrades to "no dispersion" rather than failing the comparison.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, BaselineBucket>> LookUpDispersionAsync(
        AnalysisEngineSet engine, int serverId, string serverName, List<Fact> baselineFacts, List<Fact> comparisonFacts, DateTime comparisonStart)
    {
        var dispersion = new Dictionary<string, BaselineBucket>(StringComparer.Ordinal);
        try
        {
            foreach (var metric in ComparisonBanding.DispersionMetricsFor(baselineFacts, comparisonFacts))
                dispersion[metric] = await engine.Baselines.GetBaselineAsync(serverId, metric, comparisonStart);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning("[DarlingAnalysisService] Baseline dispersion lookup failed for {Server}; compare_analysis bands every key by the absolute rule: {Message}",
                serverName, ex.Message);
            dispersion.Clear();
        }
        return dispersion;
    }

    /// <summary>
    /// Gets the latest findings for a server without running a new analysis.
    /// </summary>
    public async Task<List<AnalysisFinding>> GetLatestFindingsAsync(int serverId)
    {
        return await _findingStore.GetLatestFindingsAsync(serverId);
    }

    /// <summary>
    /// Gets recent findings for a server within the given time range. The MCP findings read
    /// passes <see cref="FindingOccurrences.WindowCoveringLimit"/> so its occurrence stats cover
    /// the whole window; the store's default 100 stays for everyone else.
    ///
    /// <para>#2506: <paramref name="asOfUtc"/> anchors the window's END. This one is a pure read of
    /// rows the SCHEDULED passes already wrote, so anchoring it asks "what did analysis say about this
    /// server at the time" — the only way to see findings the retention sweep has not yet reached but
    /// the default 24-hour window has scrolled past.</para>
    /// </summary>
    public async Task<List<AnalysisFinding>> GetRecentFindingsAsync(
        int serverId, int hoursBack = 24, int limit = 100, DateTime? asOfUtc = null, CancellationToken cancellationToken = default)
    {
        return await _findingStore.GetRecentFindingsAsync(serverId, hoursBack, limit, asOfUtc, cancellationToken);
    }

    /// <summary>
    /// Mutes a finding pattern so it won't appear in future runs and returns what the write did — a new row,
    /// an existing one, or a failure (<see cref="PgFindingStore.MuteStoryAsync"/> logs and returns
    /// <see cref="MuteRegistration.Failed"/> on a store failure) — so the MCP mute verb reports what happened
    /// rather than what it asked for (#3541 A14, widened to the three-way answer by #3653 A15/A16). The Lite
    /// twin returns the same type by name and never <c>Failed</c>, because its store throws instead of
    /// swallowing; the caller-visible contract is the same — a mute that did not land is never reported as
    /// one that did, and a mute that was already in force is never reported as newly registered.
    ///
    /// <para>An empty <see cref="AnalysisFinding.StoryPath"/> means the caller knows only the hash (the MCP entry
    /// point does) and the store resolves the path from the retained findings; see the store note for the
    /// placeholder it writes when none carries the hash.</para>
    /// </summary>
    public async Task<MuteWriteResult> MuteFindingAsync(AnalysisFinding finding, string? reason = null)
    {
        return await _findingStore.MuteStoryAsync(
            finding.ServerId,
            finding.StoryPathHash,
            string.IsNullOrEmpty(finding.StoryPath) ? null : finding.StoryPath,
            reason);
    }

    /// <summary>
    /// How many stored findings carry <paramref name="storyPathHash"/> for one server (or fleet-wide when
    /// <paramref name="serverId"/> is null / the all-servers sentinel 0) — the mute verb's <c>matched_now</c>
    /// disclosure (#3541 A14). A pass-through to <see cref="PgFindingStore.CountStoredFindingsAsync"/>; see its
    /// note for why this is reported beside the mute and not used to refuse it.
    /// </summary>
    public Task<long> CountStoredFindingsAsync(int? serverId, string storyPathHash, CancellationToken cancellationToken = default) =>
        _findingStore.CountStoredFindingsAsync(serverId, storyPathHash, cancellationToken);

    /// <summary>
    /// Cleans up old findings beyond the retention period.
    ///
    /// <para>The default names <see cref="AnalysisRetentionDefaults.FindingsRetentionDays"/> rather than
    /// repeating its value. Nothing in the service calls this — it carries the twins' surface, and the
    /// worker sweeps findings through <see cref="PgFindingStore.CleanupOldFindingsAsync"/> directly — so
    /// for any future caller this default IS the horizon, with no call site above it to correct a stale
    /// literal.</para>
    /// </summary>
    public async Task CleanupAsync(int retentionDays = AnalysisRetentionDefaults.FindingsRetentionDays)
    {
        await _findingStore.CleanupOldFindingsAsync(retentionDays);
    }

    /// <summary>
    /// Lite's data-span query VERBATIM — EXTRACT(EPOCH FROM ...) is shared dialect and runs
    /// unchanged on Postgres. Reads the raw wait_stats table (not the view) like Lite, with
    /// Lite's multi-server server_id filter (the Dashboard twin is single-server and drops it).
    /// Exposed const so Darling.Tests can pin the dialect ungated. The SQL Server engine set's gate.
    /// </summary>
    public const string TotalDataSpanSql = @"
SELECT EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0
FROM wait_stats
WHERE server_id = $1";

    /// <summary>
    /// The PostgreSQL-target engine set's data-span gate (#3542, D3): the same measurement over
    /// <c>pg_database_stats</c> — the one-minute series every PostgreSQL flavour writes with no extension,
    /// and the same table the pass's coverage witness and baseline gate read, so "how long has this server
    /// been monitored" and "how much of this window was observed" cannot disagree about which series counts.
    /// <see cref="MinimumDataHours"/> is shared with the SQL Server gate: the fraction-of-period distortion it
    /// guards against is a property of short windows, not of either engine.
    /// </summary>
    public const string PgTargetDataSpanSql = @"
SELECT EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0
FROM pg_database_stats
WHERE server_id = $1";

    /// <summary>
    /// The <c>server_config</c> snapshots the config-change attribution diffs (#3653 A10, Q2): every capture
    /// inside the pass window PLUS the last capture before it — the diff baseline, without which a change
    /// first seen on the first in-window capture is invisible (the WINDOWING rule every history reader
    /// follows; see <c>ConfigChangeDiff</c>). Reads the collector table directly like the SQL Server fact
    /// collector's <c>ServerConfigSql</c> does (the MCP history tool reads the <c>v_server_config</c>
    /// passthrough; same rows). <c>$1</c> server_id, <c>$2</c> window start, <c>$3</c> window end — naive
    /// timestamps, the column being <c>timestamp</c> without time zone. Exposed const for the dialect pins.
    /// The subquery's COALESCE covers a server with no capture before the window: the bound then becomes the
    /// window start and the in-window captures are read alone, which the diff answers with "no change"
    /// (it needs two captures) rather than a fabricated one.
    /// </summary>
    public const string ServerConfigSnapshotsForAttributionSql = @"
SELECT capture_time, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced
FROM server_config
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM server_config WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY configuration_name, capture_time";

    /// <summary>
    /// The <c>database_config</c> snapshots the attribution diffs (#3653 A10, slice two): the same window
    /// rule as <see cref="ServerConfigSnapshotsForAttributionSql"/> — every capture inside the pass window
    /// plus the last capture before it — over the WIDE <c>sys.databases</c> row, with the 27 option columns
    /// cast to text in <c>ConfigChangeDiff.DatabaseConfigChangeSettingNames</c> ORDER, which the diff walks
    /// positionally (the MCP history reader's projection, <c>DarlingConfigHistoryReader.DatabaseConfigSnapshotsSql</c>,
    /// bounded; <c>ConfigChangeFamilyReadTests</c> pins the order against the diff's list). Reads the base
    /// table like the PostgreSQL fact collector's database-config read does. <c>$1</c> server_id, <c>$2</c>
    /// window start, <c>$3</c> window end. Cost: rows = databases × captures in the window plus one baseline
    /// capture — two captures' worth on a stable connection — on the <c>(server_id, capture_time)</c> index.
    /// </summary>
    public const string DatabaseConfigSnapshotsForAttributionSql = @"
SELECT
    capture_time,
    database_name,
    state_desc,
    compatibility_level::text,
    collation_name,
    recovery_model,
    is_read_only::text,
    is_auto_close_on::text,
    is_auto_shrink_on::text,
    is_auto_create_stats_on::text,
    is_auto_update_stats_on::text,
    is_auto_update_stats_async_on::text,
    is_read_committed_snapshot_on::text,
    snapshot_isolation_state,
    is_parameterization_forced::text,
    is_query_store_on::text,
    is_encrypted::text,
    is_trustworthy_on::text,
    is_db_chaining_on::text,
    is_broker_enabled::text,
    is_cdc_enabled::text,
    is_mixed_page_allocation_on::text,
    log_reuse_wait_desc,
    page_verify_option,
    target_recovery_time_seconds::text,
    delayed_durability,
    is_accelerated_database_recovery_on::text,
    is_memory_optimized_enabled::text,
    is_optimized_locking_on::text
FROM database_config
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM database_config WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY database_name, capture_time";

    /// <summary>
    /// The <c>trace_flags</c> snapshots the attribution set-diffs (#3653 A10, slice two): the same window
    /// rule again, over the per-enabled-flag rows <c>DBCC TRACESTATUS(-1)</c> yields. The baseline subquery
    /// finds the last capture WITH rows before the window — the collector writes none when no flag is enabled,
    /// so a zero-flag capture is invisible to this read and to the diff alike (the blind spot
    /// <c>ConfigChangeAttribution</c>'s remarks state). <c>$1</c> server_id, <c>$2</c> window start, <c>$3</c>
    /// window end.
    /// </summary>
    public const string TraceFlagSnapshotsForAttributionSql = @"
SELECT capture_time, trace_flag, status, is_global, is_session
FROM trace_flags
WHERE server_id = $1
AND   capture_time <= $3
AND   capture_time >= COALESCE(
        (SELECT MAX(capture_time) FROM trace_flags WHERE server_id = $1 AND capture_time < $2),
        $2)
ORDER BY capture_time, trace_flag";

    /// <summary>
    /// The default trace's sp_configure lines for the attribution's trace anchor (#3740): every stored
    /// <c>ErrorLog</c> row carrying msg 15457 (<see cref="ConfigChangeAttribution.ReconfigureMessageNumber"/>)
    /// whose event time falls in <c>($2, $3]</c> — the span between the config capture that saw the old value
    /// and the one that saw the new, bound by the caller as naive UTC. Selected on <c>error_number</c>, not on
    /// the text: the number is populated on the row and does not change with the instance's language, and
    /// the attribution parses the text afterwards (<see cref="ConfigChangeAttribution.ParseReconfigureLine"/>).
    /// Reads the base table like <c>DarlingDefaultTraceReader</c> does (it has no <c>v_*</c> view).
    ///
    /// <para><b>The stored <c>event_time</c> is the monitored server's LOCAL wall clock</b> —
    /// <c>fn_trace_gettable</c>'s <c>StartTime</c>, stored raw so the collector's watermark compares like with
    /// like — while the capture times this is bounded by are naive UTC, so the column is de-skewed by the
    /// collected <c>server_properties.utc_offset_minutes</c> on BOTH the projection and both bounds, in the
    /// exact spelling <c>DarlingDefaultTraceReader.EventsByWindowSql</c> and the viewer's System Events read
    /// use (<c>ServerLocalReadFrameDisciplineTests</c> counts the three sites and forbids an un-de-skewed
    /// read of the aliased column). Getting this wrong is not a cosmetic skew: at UTC−4 an un-de-skewed line would
    /// sit four hours later than its capture and fall OUT of the span, so the anchor would silently never
    /// resolve on the very fleet it was built for. A server with no collected offset yet falls back to 0
    /// (local == UTC) through the single-row COALESCE CTE, which also keeps the cross join from dropping the
    /// events; one offset covers the span, so a span straddling a DST transition is off by an hour on its
    /// far side — the same single-snapshot approximation every reader of this column makes, stated here
    /// rather than implied.</para>
    ///
    /// <para><b>Cost.</b> No <c>event_time</c> index exists (the table is indexed <c>(server_id,
    /// collection_time)</c>), so this is a scan of the server's rows in a curated, low-volume table with a
    /// 30-day retention; and it runs only when the snapshot diff found a change in the pass window, which
    /// the connect cadence makes rare. Exposed const for the dialect pins.</para>
    /// </summary>
    public const string ReconfigureTraceLinesForAttributionSql = @"
WITH svr AS (
    SELECT COALESCE((
        SELECT sp.utc_offset_minutes
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1), 0) AS offset_minutes
)
SELECT
    dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc,
    dte.text_data
FROM default_trace_events AS dte, svr
WHERE dte.server_id = $1
AND   dte.error_number = 15457
AND   dte.event_time - make_interval(mins => svr.offset_minutes) > $2
AND   dte.event_time - make_interval(mins => svr.offset_minutes) <= $3
ORDER BY event_time_utc";

    /// <summary>
    /// Step 2.5 of the pass (#3653 A10, Q2 and slice two): if a configuration value was first observed
    /// changed inside the pass window — a <c>sys.configurations</c> setting, a <c>sys.databases</c> option on
    /// one database, or a trace flag — run <see cref="ComparePeriodsAsync"/> over the four hours before the
    /// change and the (clamped) four hours after it, band the result with
    /// <see cref="ComparisonBanding.Compare"/> exactly as <c>compare_analysis</c> would, and append ONE
    /// <c>CONFIG_CHANGED</c> fact carrying the verdict. <see cref="ConfigChangeAttribution"/> holds the
    /// design — why one fact, why Information, which clock "the change" is on, why changes of different
    /// families observed at one connect are one event; this method is the store reads, the engine gate and
    /// the wiring.
    ///
    /// <para><b>Three families, three reads, one event list.</b> Each family's snapshots are read with the
    /// same window rule (in-window captures plus the last capture before the window), diffed through its
    /// <c>ConfigChangeDiff</c> arm, mapped onto <see cref="ConfigChangeAttribution.SettingChange"/> in one LINQ
    /// line per family (the database family dropping <c>log_reuse_wait_desc</c> through
    /// <see cref="ConfigChangeAttribution.IsAttributableDatabaseSetting"/> — a status, not an option), and
    /// grouped into events against ITS OWN capture times (the three collectors stamp their own
    /// <c>capture_time</c>, seconds apart at one connect, so a shared list would put a same-connect sibling
    /// capture where the previous connect belongs). <see cref="ConfigChangeAttribution.MergeSameConnectEvents"/>
    /// then folds same-connect events across families, and the most recent event is the card's subject.</para>
    ///
    /// <para><b>Two clocks, in order (#3740).</b> The snapshot diff says WHAT changed and when it was first
    /// observed; the default trace's sp_configure line, when the store holds one for the same option in the
    /// span between the two captures, says WHEN it changed. <see cref="ReconfigureTraceLinesForAttributionSql"/>
    /// reads those lines and <see cref="ConfigChangeAttribution.ResolveServerConfigTraceAnchor"/> joins
    /// them; the compare and the fact anchor on the trace's time when the join resolves and on the
    /// observation otherwise, and the fact says which. The trace read is the server family's alone — the
    /// other two have no trace subject the store holds (measured; see the attribution's remarks) — so it runs
    /// only when the subject event holds a server setting. It has its own catch INSIDE the method's: a fault
    /// there costs the pass the trace anchor, not the card — the observation anchor is exactly what #3720
    /// shipped and is still true.</para>
    ///
    /// <para><b>Engine gate by identity, not by token.</b> <c>ReferenceEquals(engine, _sqlServerEngine)</c>
    /// rather than re-reading <c>engine_kind</c>: the pass already resolved the set once, and an unstamped
    /// row (NULL kind → SQL Server set) must attribute exactly as a stamped SQL Server row does.</para>
    ///
    /// <para><b>Cost.</b> A compare is two full fact collections (the collector's thirty-one reads, twice) plus
    /// the baseline lookups, inside the pass's 120 s budget. It runs only when a change is in-window — the
    /// snapshot read alone is the steady-state cost, one small indexed query — and at most once per pass
    /// (the most recent event). The compare's contexts are not wired to the pass token
    /// (<see cref="ComparePeriodsAsync"/>'s public shape is <c>compare_analysis</c>'s); each of its commands
    /// carries the 60 s per-command deadline, and the pass's boundary checks bracket the call.</para>
    ///
    /// <para><b>Its own catch.</b> Like every collector read: a store fault here degrades to "no card" with
    /// one Warning naming why, and shutdown residue propagates to the pass's single classified line (#2299).
    /// <see cref="ComparePeriodsAsync"/> swallows its own faults and returns empties with null coverage;
    /// that shape is passed through as <c>compare_unavailable</c> on the fact, so a change is still recorded
    /// when its compare could not run.</para>
    /// </summary>
    private async Task AttributeConfigChangesAsync(AnalysisEngineSet engine, AnalysisContext context, List<Fact> facts)
    {
        if (!ReferenceEquals(engine, _sqlServerEngine))
            return;

        try
        {
            var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>();
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = new NpgsqlCommand(ServerConfigSnapshotsForAttributionSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeStart, DateTimeKind.Unspecified));
                cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeEnd, DateTimeKind.Unspecified));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    snapshots.Add(new ConfigChangeDiff.ServerConfigSnapshot(
                        reader.GetDateTime(0),
                        reader.IsDBNull(1) ? "" : reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetInt64(2),
                        reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        reader.IsDBNull(4) ? null : reader.GetBoolean(4),
                        reader.IsDBNull(5) ? null : reader.GetBoolean(5)));
                }
            }

            var databaseSnapshots = await ReadDatabaseConfigSnapshotsAsync(context);
            var traceFlagSnapshots = await ReadTraceFlagSnapshotsAsync(context);

            var serverEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffServerConfigChanges(snapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Select(c => (c.ChangeTime, new ConfigChangeAttribution.SettingChange(
                        c.ConfigurationName, c.OldValueConfigured, c.NewValueConfigured, c.OldValueInUse, c.NewValueInUse, c.RequiresRestart))),
                snapshots.Select(s => s.CaptureTime));
            var databaseEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffDatabaseConfigChanges(databaseSnapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Where(c => ConfigChangeAttribution.IsAttributableDatabaseSetting(c.SettingName))
                    .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForDatabase(c.DatabaseName, c.SettingName, c.OldValue, c.NewValue))),
                databaseSnapshots.Select(s => s.CaptureTime));
            var traceFlagEvents = ConfigChangeAttribution.GroupIntoEvents(
                ConfigChangeDiff.DiffTraceFlagChanges(traceFlagSnapshots, context.TimeRangeStart, context.TimeRangeEnd)
                    .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForTraceFlag(c.TraceFlag, c.ChangeType, c.Scope, c.PreviousStatus, c.NewStatus))),
                traceFlagSnapshots.Select(s => s.CaptureTime));
            var events = ConfigChangeAttribution.MergeSameConnectEvents(serverEvents.Concat(databaseEvents).Concat(traceFlagEvents));
            if (events.Count == 0)
                return;

            var latest = events[0];
            var anchor = (latest.Families & ConfigChangeAttribution.ChangeFamily.ServerConfig) != 0
                ? await ResolveTraceAnchorAsync(context, latest)
                : null;
            var anchorTime = ConfigChangeAttribution.AnchorTime(latest, anchor);
            var windows = ConfigChangeAttribution.WindowsFor(anchorTime, context.TimeRangeEnd);

            context.CancellationToken.ThrowIfCancellationRequested();
            var (before, after, beforeCoverage, afterCoverage, dispersion) = await ComparePeriodsAsync(
                context.ServerId, context.ServerName,
                windows.BeforeStart, windows.BeforeEnd,
                windows.AfterStart, windows.AfterEnd);

            /* Both coverages null is ComparePeriodsAsync's own catch (collection threw); an empty compare
               over OBSERVED windows is the "nothing moved" answer and is banded like any other. */
            var compare = beforeCoverage is null && afterCoverage is null
                ? null
                : ComparisonBanding.Compare(before, after, dispersion, ConfigChangeAttribution.CoverageCaveatFor(beforeCoverage, afterCoverage));

            facts.Add(ConfigChangeAttribution.BuildFact(
                context.ServerId, latest, events.Count - 1, windows, compare, beforeCoverage, afterCoverage, anchor));

            _logger?.LogInformation(
                "[DarlingAnalysisService] Configuration change attributed for {Server} ({Families}): {Settings} {Verb} {AnchorAt:u} ({AnchorSource}), compare over ±{Hours} h ({AfterHours:0.#} h after so far) — {Worse} worse, {Better} better, {Stable} stable{Unavailable}",
                context.ServerName, latest.Families, string.Join(ConfigChangeAttribution.SettingSeparator, latest.Changes.Select(c => c.Name)),
                anchor is null ? "first observed at" : "changed at", anchorTime,
                anchor is null ? "configuration snapshot" : "default trace, msg 15457",
                ConfigChangeAttribution.CompareWindowHours, windows.AfterHoursObserved,
                compare?.Worse ?? 0, compare?.Better ?? 0, compare?.Stable ?? 0,
                compare is null ? " (compare unavailable this pass)" : string.Empty);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogWarning(
                "[DarlingAnalysisService] Configuration-change attribution failed for {Server}; the pass continues without the CONFIG_CHANGED card: {Message}",
                context.ServerName, ex.Message);
        }
    }

    /// <summary>
    /// The database family's half of step 2.5's snapshot read (slice two): <see cref="DatabaseConfigSnapshotsForAttributionSql"/>
    /// into the diff's WIDE record, the 27 option columns read positionally after <c>capture_time</c> and
    /// <c>database_name</c> — the same walk <c>DarlingConfigHistoryReader.GetDatabaseConfigSnapshotsAsync</c>
    /// makes over its unbounded read. A NULL column is a null value the diff compares ordinally against the
    /// other capture's, so a column that appeared or emptied is a change like any other. No catch of its own:
    /// a fault here is the caller's, and costs the pass the whole card rather than a family of it, because a
    /// card that silently dropped one family would read as "nothing changed there".
    /// </summary>
    private async Task<List<ConfigChangeDiff.DatabaseConfigSnapshot>> ReadDatabaseConfigSnapshotsAsync(AnalysisContext context)
    {
        var rows = new List<ConfigChangeDiff.DatabaseConfigSnapshot>();
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
        using var cmd = new NpgsqlCommand(DatabaseConfigSnapshotsForAttributionSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeStart, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeEnd, DateTimeKind.Unspecified));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
            for (var i = 0; i < values.Length; i++)
            {
                var ordinal = i + 2; /* capture_time, database_name precede the settings */
                values[i] = reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
            }

            rows.Add(new ConfigChangeDiff.DatabaseConfigSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                values));
        }

        return rows;
    }

    /// <summary>
    /// The trace-flag family's half of step 2.5's snapshot read (slice two): <see cref="TraceFlagSnapshotsForAttributionSql"/>
    /// into the diff's per-flag record, the same mapping <c>DarlingConfigHistoryReader.GetTraceFlagSnapshotsAsync</c>
    /// makes. No catch of its own, for the reason <see cref="ReadDatabaseConfigSnapshotsAsync"/> states.
    /// </summary>
    private async Task<List<ConfigChangeDiff.TraceFlagSnapshot>> ReadTraceFlagSnapshotsAsync(AnalysisContext context)
    {
        var rows = new List<ConfigChangeDiff.TraceFlagSnapshot>();
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
        using var cmd = new NpgsqlCommand(TraceFlagSnapshotsForAttributionSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeStart, DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeEnd, DateTimeKind.Unspecified));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            rows.Add(new ConfigChangeDiff.TraceFlagSnapshot(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetBoolean(4)));
        }

        return rows;
    }

    /// <summary>
    /// The trace half of step 2.5 (#3740): the stored sp_configure lines in the span between the two config
    /// captures of <paramref name="change"/>, joined to its settings by
    /// <see cref="ConfigChangeAttribution.ResolveServerConfigTraceAnchor"/>. Null — the observation anchor —
    /// when no line matches (trace off, Azure SQL Database, the row aged out before collection, a
    /// non-English message) AND when the read itself faults: the anchor is an improvement on a card that is
    /// already true without it, so a store fault here is logged at Warning and costs only the anchor.
    /// Shutdown residue still propagates to the caller's classified line (#2299). Bounds are bound as naive
    /// timestamps like the snapshot read's, the column being <c>timestamp</c> without time zone.
    /// </summary>
    private async Task<ConfigChangeAttribution.TraceAnchor?> ResolveTraceAnchorAsync(
        AnalysisContext context, ConfigChangeAttribution.ChangeEvent change)
    {
        try
        {
            var lines = new List<ConfigChangeAttribution.TraceLine>();
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = new NpgsqlCommand(ReconfigureTraceLinesForAttributionSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(change.PreviousCaptureTime, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(change.ChangeTime, DateTimeKind.Unspecified));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                if (reader.IsDBNull(0))
                    continue;
                lines.Add(new ConfigChangeAttribution.TraceLine(
                    reader.GetDateTime(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1)));
            }

            return ConfigChangeAttribution.ResolveServerConfigTraceAnchor(change, lines);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogWarning(
                "[DarlingAnalysisService] Default-trace anchor lookup failed for {Server}; the CONFIG_CHANGED card keeps its observation anchor: {Message}",
                context.ServerName, ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The registry read behind <see cref="ResolveEngineAsync"/>: the server's engine KIND token, stamped on
    /// every connect since V82 (#2530). One indexed row by primary key. The same column the MCP capability
    /// helper reads for its <c>not_collected</c> envelopes, so the analysis pass and the reads that explain
    /// its NULLs answer the engine question from one fact.
    /// </summary>
    public const string ServerEngineKindSql = @"
SELECT engine_kind
FROM servers
WHERE server_id = $1";

    /// <summary>
    /// Which engine set analyzes <paramref name="serverId"/> — decided from the registry on EVERY call (#3542).
    ///
    /// <para><b>The mapping.</b> <see cref="MonitoredEngineKind.IsPostgres"/> (the <c>postgres</c> and
    /// <c>aurora-postgres</c> tokens) selects the PostgreSQL-target set; anything else selects the SQL Server
    /// set — including a NULL, an unknown token, and a server with no registry row. A NULL kind is a row no
    /// connect has stamped since V82 landed and "makes no claim" (#2530); taking the SQL Server set for it is
    /// exactly what every pass did before this seam existed, so an unstamped SQL Server row behaves
    /// identically, and an unstamped PostgreSQL row lands on the honest data-span message ("0 hours of
    /// wait_stats history") until its next connect stamps it — the message names the engine stamp as the
    /// missing fact rather than guessing from the data.</para>
    ///
    /// <para><b>No cache, deliberately.</b> <c>engine_kind</c> is written on BOTH connect arms precisely so a
    /// re-pointed registration corrects it, and a cached answer here would keep analyzing the old engine's
    /// tables for the life of the process. One primary-key read per pass is cheaper than the first fact read
    /// that follows it.</para>
    ///
    /// <para><b>No catch, deliberately.</b> A registry that cannot answer this is a store that cannot answer
    /// anything; the caller's catch classifies it (shutdown / budget / fault) exactly as it would the first
    /// collector read. Catching here and defaulting to SQL Server would run a SQL Server pass against a
    /// PostgreSQL target on a transient fault and persist its "0 hours" verdict.</para>
    /// </summary>
    internal async Task<(AnalysisEngineSet Engine, string? EngineKind)> ResolveEngineAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        using var cmd = new NpgsqlCommand(ServerEngineKindSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(serverId);

        /* A missing row and a NULL column both come back as null here (ExecuteScalar returns null for no
           rows and DBNull for a NULL, and DBNull is not a string) — the two cases the mapping treats alike. */
        var kind = await cmd.ExecuteScalarAsync(cancellationToken) as string;
        return (EngineFor(kind), kind);
    }

    /// <summary>The set <see cref="ResolveEngineAsync"/> answers for a token, without a store — the mapping
    /// itself, exposed so the engine-routing pins can assert it on every token in <see cref="MonitoredEngineKind"/>.</summary>
    internal AnalysisEngineSet EngineFor(string? engineKind) =>
        MonitoredEngineKind.IsPostgres(engineKind) ? _pgTargetEngine : _sqlServerEngine;

    /// <summary>
    /// Returns the total span of collected data for a server (no time range filter), measured on the
    /// resolved engine's own series (<see cref="AnalysisEngineSet.DataSpanSql"/>).
    /// This answers "has this server been monitored long enough?" — separate from
    /// the analysis window. A server with 100 hours of total history can safely
    /// be analyzed over a 4-hour window without dilution.
    /// </summary>
    private async Task<double> GetTotalDataSpanHoursAsync(AnalysisEngineSet engine, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(engine.DataSpanSql, connection) { CommandTimeout = AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull)
                return 0;

            return Convert.ToDouble(result);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* Probe failure reads as "no data yet" — EXCEPT shutdown residue, which must not be
               allowed to masquerade as a 0-hour history (#2299): it propagates to the pass's
               shutdown catch instead of producing a bogus insufficient-data skip. */
            return 0;
        }
    }
}

/// <summary>
/// Event args for when an analysis run completes.
/// </summary>
public class AnalysisCompletedEventArgs : EventArgs
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public List<AnalysisFinding> Findings { get; set; } = [];
    public DateTime AnalysisTime { get; set; }
}
