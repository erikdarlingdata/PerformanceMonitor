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
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Detects a SAME-STATEMENT PILEUP in one active-query snapshot: N concurrent sessions on one
/// statement, all past a floor of elapsed time, at least one suspended on an IO-class wait, where the
/// same statement's recent snapshots show sub-second in-flight observations. That combination is the
/// live signature of a parameterized plan flipping to an IO-heavy shape — every caller shares the
/// plan, so every tenant inherits it at once and the copies pile up on PAGEIOLATCH while the
/// statement's own recent history says it has no business running for seconds (#3467).
///
/// <para><b>Why this exists as its own detector instead of a scheduled-analysis fact.</b> The measured
/// incident (#3467, 2026-09-15, one production store class on a multi-tenant server) ran ~90 seconds
/// end to end and self-cleared: three concurrent copies of one statement — 29.1 s / 28.3 s / 27.7 s
/// elapsed, two suspended on PAGEIOLATCH_SH, 287K logical reads on the leader — while the same
/// statement's snapshot 3.5 minutes earlier showed 54 ms / 8.2K reads. Sixteen executions across ten
/// tenants died at the client's 30-second command timeout. The scheduled pass produced correct
/// anomalies 21 minutes later, every one capped below notify_severity; the finding that could notify
/// (PLAN_REGRESSION) keys off query_store data that landed ~28 minutes after the damage. This detector
/// runs against the snapshot the collectors had already written WHILE the incident was still alive, so
/// it is evaluated per collection sweep (Darling: the 30-second alert-sweep cadence; Lite: the
/// one-minute collection loop), not on the 5–360-minute analysis interval.</para>
///
/// <para><b>Query Store independence is a requirement, not a preference.</b> On at least one production
/// store class the Query Store readers are deliberately disabled (#2296), so anything keyed on
/// query_store identity is blind exactly where this finding was needed. Everything here — the trigger,
/// the baseline, the statement identity, the incident fingerprint — is computed from the active-query
/// snapshot collector alone (<c>query_snapshots</c>, sourced from sys.dm_exec_requests), whose
/// <c>query_hash</c> is the DMV's own statement hash, not a Query Store artifact. Query-store-derived
/// findings (PLAN_REGRESSION, the #2138 force-plan bot) remain follow-on enrichment where QS happens
/// to be readable — never the trigger. <c>SameStatementPileupSourceCensusTests</c> pins this from
/// source for the detector and both SKUs' readers.</para>
///
/// <para><b>Where the thresholds come from.</b> Fleet distributions pulled 2026-09-15 from the Darling
/// store's active-query snapshots: four production servers × 24 hours, ~4,160 snapshot instants.
/// Same-statement concurrent groups: 484 at n ≥ 2, 103 at n ≥ 3 — but only FOUR n ≥ 3 groups had every
/// session past 10 s, and three of the four were routine batch shapes (a sync job at n = 5 / 13 s, a
/// permission rebuild at n = 3, a nightly catalog scan at n = 3 / 24 s) that fail BOTH the IO-wait gate
/// (zero IO-class waits) and the baseline gate (no in-window observation, or priors already slow). The
/// fourth was the measured incident. Each constant's derivation is stated on the constant, in the unit
/// the gate compares (#3463's lesson), and the calibration is pinned by
/// <c>SameStatementPileupDetectorTests</c>.</para>
/// </summary>
public static class SameStatementPileupDetector
{
    /// <summary>
    /// One active-query snapshot row, as both SKUs' readers project it from <c>query_snapshots</c>.
    /// <paramref name="ElapsedMs"/> is the row's <c>total_elapsed_time_ms</c> — the in-flight elapsed
    /// at the instant the snapshot was taken, which is what both the pileup floor and the sub-second
    /// baseline compare (an observation, not a completed-execution duration).
    /// </summary>
    public sealed record SnapshotRow(
        DateTime CollectionTime,
        int SessionId,
        string? QueryHash,
        string? QueryText,
        string? DatabaseName,
        string? Status,
        string? WaitType,
        long? WaitTimeMs,
        long ElapsedMs,
        long CpuTimeMs,
        long LogicalReads,
        long PhysicalReads);

    /// <summary>One fired pileup: the story (severity, fingerprint, frozen advice) plus the evidence
    /// drill-down the caller attaches to the materialized finding before persisting it.</summary>
    public sealed record Detection(AnalysisStory Story, Dictionary<string, object> DrillDown);

    /// <summary>The finding's root fact key — the statement-independent family name. The latest-batch
    /// carve-out in both finding stores and the advice table key both spell this string; it is public
    /// so they reference the constant instead of drifting copies.</summary>
    public const string RootFactKey = "SAME_STATEMENT_PILEUP";

    /// <summary>
    /// Concurrent sessions on ONE statement, at one snapshot instant, before a pileup exists: 3.
    /// From the fleet pull (four servers × 24 h): n ≥ 2 same-statement groups number 484 and include
    /// routine paired workloads — scheduled-job pairs, two-lane ETL, an index-maintenance pair — while
    /// n ≥ 3 past the elapsed floor numbered four in four server-days. Three concurrent copies is also
    /// where the incident's mechanism starts to show: a flipped SHARED plan drags every caller at once,
    /// which is precisely what two coincidentally-slow sessions do not prove. A two-session pileup
    /// therefore produces NO finding at any severity — the issue's "two-session brief pileup must not
    /// notify" holds by construction, not by arithmetic.
    /// </summary>
    public const int MinConcurrentSessions = 3;

    /// <summary>
    /// Elapsed-time floor, in milliseconds, that EVERY session in the group must have passed: 10 000.
    /// The measured incident's victims died at the client's 30-second command timeout; a 10 s floor
    /// pages while they are still alive. Below it the fleet data is noisy: the 5–10 s bucket at n ≥ 3
    /// holds routine permission-cache rebuilds (observed at 6.7 s), and the measured incident cleared
    /// this floor with 2.7× headroom (min elapsed 27 703 ms).
    /// </summary>
    public const long MinElapsedMs = 10_000;

    /// <summary>
    /// Sessions in the group that must currently show an IO-class wait: 1. The mechanism gate — the
    /// pileup shape this rung prices is an IO-saturating plan, so the pack piles up suspended on
    /// PAGEIOLATCH. One, not two, because page-IO waits are millisecond-scale (the incident's two
    /// suspended sessions showed wait_time_ms = 5): sessions bounce between suspended and runnable, so
    /// demanding two SIMULTANEOUSLY suspended makes the gate a coin flip on sampling phase. The
    /// measured incident showed two; requiring one keeps the mechanism evidence without betting the
    /// page on which instant the collector sampled. Specificity is carried by the conjunction — on the
    /// fleet data the full predicate fired exactly once in four server-days, and that firing was the
    /// incident.
    /// </summary>
    public const int MinIoWaitSessions = 1;

    /// <summary>
    /// Wait types that count as IO-class for <see cref="MinIoWaitSessions"/>. PAGEIOLATCH covers the
    /// measured shape (data pages read under a plan doing ~35× the reads); the completion and log
    /// waits cover the same mechanism surfacing on other IO paths. Deliberately NOT included:
    /// BACKUPIO/BACKUPTHREAD (a backup convoy is not a plan flip, and long-elapsed backup statements
    /// are ordinary).
    /// </summary>
    public static readonly string[] IoClassWaitPrefixes =
    {
        "PAGEIOLATCH_", "IO_COMPLETION", "ASYNC_IO_COMPLETION", "WRITELOG", "WRITE_COMPLETION",
    };

    /// <summary>
    /// How far back, in minutes, the sub-second baseline looks: 45. The window has to be long enough
    /// to catch a fast statement in flight at the ~1-minute snapshot cadence — the incident statement
    /// was observed at 54 ms three and a half minutes before the pileup — and short enough that the
    /// window self-clears after an episode: a fired pileup leaves 27+ second observations in the
    /// window, so the same statement cannot re-fire until they age out. That quiet period is
    /// deliberate and roughly matches the analysis-notify cooldown's floor (30 minutes); the measured
    /// recurrence class (#3467's episode two, ~2h40m later per the issue) sits well outside it.
    /// </summary>
    public const int BaselineLookbackMinutes = 45;

    /// <summary>
    /// The baseline ceiling, in milliseconds of observed in-flight elapsed: 1 000. "Recent snapshots
    /// show sub-second executions" is the issue's own wording, and the measured statement's in-window
    /// observation was 54 ms — 18× under the ceiling. Every prior observation in the lookback must sit
    /// under it: a single recent multi-second observation means the statement was ALREADY slow, and
    /// that is long_running_query's beat (or an ordinary slow query's), not a flip.
    /// </summary>
    public const long SubSecondBaselineCeilingMs = 1_000;

    /// <summary>
    /// Prior in-window observations required before the baseline claim means anything: 1. A statement
    /// never seen before the pileup has no "normally sub-second" to betray — the fleet pull's loudest
    /// counterexample (a sync job at n = 5, all past 13 s) had ZERO prior observations and is excluded
    /// by exactly this gate. One observation is thin evidence on its own; it is load-bearing only in
    /// conjunction with the session, elapsed, and IO gates, and the conjunction measured clean.
    /// </summary>
    public const int MinBaselineObservations = 1;

    /// <summary>
    /// Severity divisor, in SESSION-SECONDS per severity point: 40. Severity is
    /// (sessions × min-elapsed-seconds) / 40, capped at <see cref="SeverityCap"/> — the #3462
    /// principle (magnitude, not presence) priced in the unit the notify gate compares (severity,
    /// 0–2 band; the gate is ≥ 1.5). Calibration, stated in that unit and to the figures the
    /// arithmetic actually produces (#3463 — a calibration claim stated in a rounded unit is not the
    /// claim the gate evaluates):
    /// <list type="bullet">
    /// <item>the measured incident — 3 sessions × 27.703 s = 83.109 session-seconds — scores
    /// <b>2.0777</b> before the cap: 0.578 severity points of headroom above the 1.5 gate, and it
    /// clears the gate even if two of its three sessions are discounted to the 10-second floor
    /// (3 × 20 s would be exactly 1.5);</item>
    /// <item>the minimum firing shape — 3 × 10 s = 30 session-seconds — scores <b>0.75</b>, half the
    /// notify gate: a visible, persisted, drill-downable finding that does NOT page;</item>
    /// <item>notify-eligibility begins at exactly <b>60 session-seconds</b> (3 × 20 s, 4 × 15 s,
    /// 6 × 10 s) — three concurrent copies each past two-thirds of the measured victims' 30-second
    /// client timeout;</item>
    /// <item>the cap is reached at 80 session-seconds, so the measured incident saturates it — which
    /// is deliberate: past "three tenants' worth of callers stuck for half a minute each" the
    /// operator response does not change with the arithmetic.</item>
    /// </list>
    /// <c>SameStatementPileupDetectorTests</c> pins every point above.
    /// </summary>
    public const double SessionSecondsPerSeverityPoint = 40.0;

    /// <summary>The severity band's ceiling (the shared 0–2 scale every reader bands on).</summary>
    public const double SeverityCap = 2.0;

    /// <summary>
    /// A snapshot older than this, in minutes, is not evaluated: 10 — the same staleness rule the
    /// long-running-query alert applies to the same table (#1812's shape: a collector outage must not
    /// hold a level-triggered evaluation on frozen rows).
    /// </summary>
    public const int StaleSnapshotCutoffMinutes = 10;

    /// <summary>How much statement text the drill-down's preview carries. Bounds one finding row's
    /// payload only — it filters nothing and takes no part in identity (the readers cap the projected
    /// text well above this, and the identity key is the DMV hash).</summary>
    private const int StatementPreviewChars = 300;

    /// <summary>
    /// Evaluates the newest snapshot instant in <paramref name="rows"/> against the pileup predicate.
    /// <paramref name="rows"/> must cover the newest instant PLUS the baseline lookback behind it —
    /// both SKUs' readers pull exactly that window. Returns one detection per piled-up statement
    /// (almost always zero or one). Pure and clock-driven by <paramref name="utcNow"/> so the
    /// measured incident replays verbatim in tests.
    ///
    /// <para>Rows are grouped by statement identity — <c>query_hash</c> (the DMV's statement hash,
    /// shared across sessions running the same parameterized statement) with a text-hash surrogate for
    /// the rare null-hash row. Two sessions on the same statement are two rows with one identity and
    /// two session_ids at one collection_time.</para>
    ///
    /// <para><b>Clock basis</b>: <c>CollectionTime</c> arrives as NAIVE UTC — both SKUs' collector
    /// runners stamp a batch with <c>DateTime.UtcNow</c> and store it in an offset-less column — so it
    /// is compared directly against <paramref name="utcNow"/> here, which is the same convention the
    /// long-running-query alert applies to this table. Every row in one collector batch carries the
    /// IDENTICAL stamp (it is taken once per sweep, not per row), which is what makes equality on
    /// <c>CollectionTime</c> a sound definition of "one snapshot instant" rather than a tolerance
    /// window.</para>
    /// </summary>
    public static List<Detection> Evaluate(
        string serverName, IReadOnlyList<SnapshotRow> rows, DateTime utcNow)
    {
        var detections = new List<Detection>();
        if (rows is null || rows.Count == 0)
        {
            return detections;
        }

        var usable = rows.Where(r => r is not null && !IsNoise(r)).ToList();
        if (usable.Count == 0)
        {
            return detections;
        }

        var latest = usable.Max(r => r.CollectionTime);
        if (latest < utcNow.AddMinutes(-StaleSnapshotCutoffMinutes))
        {
            /* The newest snapshot predates the staleness cutoff — the collector is down or the
               server is unreachable, and a level check against frozen rows would hold a "live"
               pileup forever (#1812's rule, borrowed from the long-running alert on this table). */
            return detections;
        }

        var baselineFloor = latest.AddMinutes(-BaselineLookbackMinutes);

        /* The candidate groups: the newest instant's rows, by statement identity. */
        foreach (var group in usable.Where(r => r.CollectionTime == latest).GroupBy(StatementIdentity))
        {
            var pack = group.ToList();
            var sessions = pack.Select(r => r.SessionId).Distinct().Count();
            if (sessions < MinConcurrentSessions)
            {
                continue;
            }

            /* EVERY session past the floor — the pack's minimum is the conservative magnitude: each
               concurrent caller has burned at least this long on a statement that should be done in
               milliseconds. A single long-running session with fast siblings is long_running_query's
               job, and the min-elapsed condition is what keeps it there. */
            var minElapsedMs = pack.Min(r => r.ElapsedMs);
            if (minElapsedMs < MinElapsedMs)
            {
                continue;
            }

            /* DISTINCT sessions on an IO-class wait, not rows: a parallel request can appear as
               several rows for one session_id, and counting rows would let one waiting session satisfy
               a floor written in sessions — the same distinctness the session gate above applies. */
            var ioWaiters = pack
                .Where(r => IsIoClassWait(r.WaitType))
                .Select(r => r.SessionId)
                .Distinct()
                .Count();
            if (ioWaiters < MinIoWaitSessions)
            {
                continue;
            }

            /* The baseline: the same statement's in-flight observations across the lookback, strictly
               BEFORE the pileup instant (the pileup's own rows are the anomaly, not the norm). All of
               them sub-second, and at least one of them present — see the two constants for why both
               halves are load-bearing. */
            var identity = group.Key;
            var baseline = usable
                .Where(r => r.CollectionTime < latest
                            && r.CollectionTime >= baselineFloor
                            && StatementIdentity(r) == identity)
                .ToList();
            if (baseline.Count < MinBaselineObservations
                || baseline.Max(r => r.ElapsedMs) >= SubSecondBaselineCeilingMs)
            {
                continue;
            }

            detections.Add(BuildDetection(
                serverName, identity, pack, baseline, minElapsedMs, sessions, ioWaiters, latest));
        }

        return detections;
    }

    /// <summary>
    /// The severity arithmetic, exposed for the calibration pins: sessions × the pack's minimum
    /// elapsed (seconds), divided by <see cref="SessionSecondsPerSeverityPoint"/>, capped at
    /// <see cref="SeverityCap"/>. #3462's magnitude principle: three sessions stuck for 30 seconds
    /// each is a different event from three stuck for ten, and the severity says so.
    /// </summary>
    public static double ComputeSeverity(int sessions, long minElapsedMs) =>
        Math.Min(SeverityCap, sessions * (minElapsedMs / 1000.0) / SessionSecondsPerSeverityPoint);

    /// <summary>
    /// The statement-scoped incident fingerprint: the <see cref="IncidentId"/> recipe (server | root
    /// key | database, SHA256, first 16 hex chars) extended with the statement identity, so episode
    /// two of the SAME statement folds onto episode one's incident trail instead of reading as a
    /// fresh surprise — the treatment every alert family already has, and the stable trigger token
    /// #2138's force-plan bot will want. Statement identity is the DMV query_hash (never Query Store
    /// identity — see the class remarks), so the fingerprint survives plan churn, restarts, and QS
    /// resets by construction.
    /// </summary>
    public static string ComputeIncidentId(string serverName, string? databaseName, string statementIdentity)
    {
        var seed = (serverName ?? string.Empty) + "|" + RootFactKey + "|" + (databaseName ?? string.Empty) + "|" + statementIdentity;
        return ShortHash(seed);
    }

    /// <summary>
    /// The statement identity a pileup groups and fingerprints on: the row's <c>query_hash</c> when
    /// present (the sys.dm_exec_requests statement hash — identical across sessions running the same
    /// parameterized statement, and independent of Query Store), else a text-hash surrogate over the
    /// whitespace-normalized statement prefix. The surrogate exists for the rare null-hash row; it is
    /// honest about its reach (readers cap the projected text, so two distinct giant statements
    /// sharing a prefix could collide) and the conjunction of gates keeps that from mattering: a
    /// collision still has to pass the elapsed floor, the IO gate, and the sub-second baseline.
    /// </summary>
    public static string StatementIdentity(SnapshotRow row)
    {
        if (!string.IsNullOrWhiteSpace(row.QueryHash))
        {
            return row.QueryHash.Trim().ToLowerInvariant();
        }

        var text = NormalizeText(row.QueryText);
        return text.Length == 0 ? "text:empty" : "text:" + ShortHash(text);
    }

    private static Detection BuildDetection(
        string serverName,
        string identity,
        List<SnapshotRow> pack,
        List<SnapshotRow> baseline,
        long minElapsedMs,
        int sessions,
        int ioWaiters,
        DateTime snapshotTime)
    {
        var severity = ComputeSeverity(sessions, minElapsedMs);
        var databaseName = pack.Select(r => r.DatabaseName).FirstOrDefault(d => !string.IsNullOrEmpty(d));
        var baselineMaxMs = baseline.Max(r => r.ElapsedMs);
        var leader = pack.OrderByDescending(r => r.ElapsedMs).First();

        /* The wait types actually observed on the pack, named in the advice rather than described as
           "IO-class": an operator reading the e-mail should not have to open the drill-down to learn
           WHICH wait the convoy is on, and PAGEIOLATCH_SH vs WRITELOG point at different storage
           conversations. */
        var observedWaits = pack
            .Select(r => r.WaitType)
            .Where(IsIoClassWait)
            .Select(w => w!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(w => w, StringComparer.Ordinal)
            .ToList();

        /* The story path doubles as the mute/cooldown key (its hash), so it carries the statement
           identity: muting one statement's pileup must not silence every other statement's, and the
           notification cooldown must answer this statement's question with this statement's history
           (#3459's rule). */
        var storyPath = RootFactKey + " → " + identity;

        var story = new AnalysisStory
        {
            RootFactKey = RootFactKey,
            /* The RAW magnitude, not the severity (the MCP root_fact.value contract): the count of
               concurrent sessions in the pack. The pack's minimum elapsed rides as the leaf value. */
            RootFactValue = sessions,
            Severity = severity,
            /* Single-observation story — the symptom is directly measured, no traversal to dilute
               (the InferenceEngine convention: single-node paths carry confidence 1.0). */
            Confidence = 1.0,
            Category = "queries",
            Path = [RootFactKey],
            StoryPath = storyPath,
            StoryPathHash = ShortHash(storyPath),
            StoryText = FreezeAdvice(
                sessions, minElapsedMs, ioWaiters, baselineMaxMs, baseline.Count, databaseName, observedWaits),
            LeafFactKey = "min_elapsed_ms",
            LeafFactValue = minElapsedMs,
            FactCount = 1,
            IsAbsolution = false,
            IncidentId = ComputeIncidentId(serverName, databaseName, identity),
            DatabaseName = databaseName,
            RootFactMetadata = new Dictionary<string, double>
            {
                ["concurrent_sessions"] = sessions,
                ["min_elapsed_ms"] = minElapsedMs,
                ["max_elapsed_ms"] = pack.Max(r => r.ElapsedMs),
                ["io_wait_sessions"] = ioWaiters,
                ["baseline_observations"] = baseline.Count,
                ["baseline_max_elapsed_ms"] = baselineMaxMs,
                ["session_seconds"] = sessions * (minElapsedMs / 1000.0),
            },
        };

        /* The evidence, ordered for the DrillDownSerializer's per-section head cap (it keeps the first
           rows and names anything it drops): the pack WORST-first, so the leader an operator will chase
           survives; the baseline NEWEST-first, because the freshest sub-second observation is the one
           that makes "this statement was fine minutes ago" a measurement rather than a claim. A
           frequently-executed statement can have more baseline rows in 45 minutes than the cap keeps,
           and the rows worth keeping are the recent ones. */
        var drillDown = new Dictionary<string, object>
        {
            ["pileup_sessions"] = pack
                .OrderByDescending(r => r.ElapsedMs)
                .Select(r => new
                {
                    session_id = r.SessionId,
                    status = r.Status,
                    wait_type = r.WaitType,
                    wait_time_ms = r.WaitTimeMs,
                    elapsed_ms = r.ElapsedMs,
                    cpu_time_ms = r.CpuTimeMs,
                    logical_reads = r.LogicalReads,
                    physical_reads = r.PhysicalReads,
                })
                .Cast<object>()
                .ToList(),
            ["recent_baseline"] = baseline
                .OrderByDescending(r => r.CollectionTime)
                .Select(r => new
                {
                    collection_time = r.CollectionTime,
                    session_id = r.SessionId,
                    elapsed_ms = r.ElapsedMs,
                    logical_reads = r.LogicalReads,
                })
                .Cast<object>()
                .ToList(),
            ["statement"] = new
            {
                identity,
                database_name = databaseName,
                snapshot_time = snapshotTime,
                query_text = Preview(leader.QueryText),
            },
        };

        return new Detection(story, drillDown);
    }

    /// <summary>
    /// Freezes the value-stated advice into StoryText, so every surface — e-mail, webhook, MCP, both
    /// viewers' cards — quotes the numbers the detector measured rather than folklore. Serialized
    /// through <see cref="FactAdvice.SerializeForStoryText"/> rather than a local JSON shape, so this
    /// finding cannot drift from the {h,i,r} contract <see cref="FactAdvice.TryReadStoryText"/> reads
    /// back; the static <see cref="FactAdvice"/> block for <see cref="RootFactKey"/> remains the
    /// fallback for rows persisted without it.
    /// </summary>
    private static string FreezeAdvice(
        int sessions,
        long minElapsedMs,
        int ioWaiters,
        long baselineMaxMs,
        int baselineObservations,
        string? databaseName,
        List<string> observedWaits)
    {
        var db = string.IsNullOrEmpty(databaseName) ? "one database" : databaseName;
        var waits = observedWaits.Count > 0 ? string.Join(", ", observedWaits) : "IO-class";

        return FactAdvice.SerializeForStoryText(new AdviceBlock(
            Headline:
                $"{sessions} concurrent sessions piled up on one statement in {db} — every copy past "
                + $"{minElapsedMs / 1000.0:F0} seconds on a statement observed at {baselineMaxMs} ms minutes earlier",
            Investigation:
                $"The active-query snapshot caught {sessions} sessions executing the SAME statement at one instant, "
                + $"all past {minElapsedMs / 1000.0:F1} s elapsed, {ioWaiters} suspended on {waits} — while the same "
                + $"statement's {baselineObservations} in-flight observation(s) over the prior {BaselineLookbackMinutes} minutes "
                + $"topped out at {baselineMaxMs} ms. That is the live signature of a shared parameterized plan flipping to an "
                + "IO-heavy shape: every caller inherits the flip at once, the copies convoy on page reads, and client "
                + "timeouts follow within seconds. The drill-down quotes the piled-up sessions (elapsed, waits, reads) and "
                + "the sub-second baseline rows this conclusion is computed from.",
            Remediation:
                "Confirm and clear the plan: find the statement's current plan in the plan cache and evict it "
                + "(DBCC FREEPROCCACHE with the plan handle, or sp_recompile on the object) — the incident class "
                + "self-clears when the plan churns out, and eviction forces that churn now. Where Query Store is "
                + "readable on this server, the PLAN_REGRESSION finding and its force-plan drill-down will surface the "
                + "flip on the next analysis pass as confirmation and offer the durable fix (forcing the historical "
                + "plan); this finding deliberately does not wait for it (#2296 deployments have no Query Store to "
                + "read). If the pileup recurs — episodes fold onto one incident id — the statement is re-drawing its "
                + "bad plan on plan-cache churn, and the parameter-sensitivity playbook applies to the statement itself."));
    }

    /// <summary>
    /// Noise rows the pileup must never count: the system health session (sp_server_diagnostics runs
    /// for days and is always "one statement"), deliberate waits (WAITFOR convoys are scheduling, not
    /// pileups), and backup workers (long-elapsed by design; BACKUPIO is likewise excluded from the
    /// IO-class set). Mirrors the long-running-query alert's opt-out filters on the same table, applied
    /// here in shared code so both SKUs' readers stay thin and one test pins the behavior.
    /// </summary>
    private static bool IsNoise(SnapshotRow row)
    {
        if (row.WaitType is { } wait)
        {
            if (wait.Contains("SP_SERVER_DIAGNOSTICS", StringComparison.OrdinalIgnoreCase)
                || wait.Equals("WAITFOR", StringComparison.OrdinalIgnoreCase)
                || wait.Equals("BROKER_RECEIVE_WAITFOR", StringComparison.OrdinalIgnoreCase)
                || wait.Equals("BACKUPTHREAD", StringComparison.OrdinalIgnoreCase)
                || wait.Equals("BACKUPIO", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return row.QueryText is { } text
            && text.Contains("sp_server_diagnostics", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsIoClassWait(string? waitType)
    {
        if (string.IsNullOrEmpty(waitType))
        {
            return false;
        }

        foreach (var prefix in IoClassWaitPrefixes)
        {
            if (waitType.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return string.Empty;
        }

        var sb = new StringBuilder(text.Length);
        var lastWasSpace = false;
        foreach (var c in text.Trim())
        {
            if (char.IsWhiteSpace(c))
            {
                if (!lastWasSpace)
                {
                    sb.Append(' ');
                    lastWasSpace = true;
                }
            }
            else
            {
                sb.Append(char.ToLowerInvariant(c));
                lastWasSpace = false;
            }
        }

        return sb.ToString();
    }

    private static string Preview(string? text)
    {
        var normalized = NormalizeText(text);
        return normalized.Length <= StatementPreviewChars ? normalized : normalized[..StatementPreviewChars];
    }

    /// <summary>SHA256 → first 16 lowercase hex chars — the same shape <see cref="IncidentId"/> and
    /// the InferenceEngine's StoryPathHash use, so this fingerprint lives in the same keyspace the
    /// mute registry and the occurrence folding already handle.</summary>
    private static string ShortHash(string seed)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }
}
