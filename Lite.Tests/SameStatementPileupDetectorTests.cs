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
using System.Text.Json;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3467: the SAME-STATEMENT PILEUP detector, held to the measured incident and to the fleet shapes
/// that must NOT fire.
///
/// <para>The fixtures are the incident verbatim (2026-09-15, one production store class on a
/// multi-tenant server, reconstructed from the store's own <c>query_snapshots</c> rows): the
/// 20:10:31Z pileup — session 649 running at 29 124 ms / 1 351 ms CPU / 287 049 logical reads /
/// 29 505 physical, sessions 311 and 847 suspended on PAGEIOLATCH_SH at 28 343 ms and 27 703 ms —
/// and the same statement's 20:07:01Z observation at 54 ms / 8 200 reads. Sixteen executions across
/// ten tenants died at the client's 30-second command timeout during this episode; the scheduled
/// analysis pass 21 minutes later produced three correct anomalies, all capped below the 1.5 notify
/// gate, and the notify-eligible PLAN_REGRESSION could not exist until query_store landed ~28
/// minutes in. This detector's job is to have said it at 20:10:31Z.</para>
///
/// <para>The counterexamples are real too — every non-firing shape below was pulled from the same
/// fleet's active-query snapshots (four production servers × 24 hours, ~4 160 snapshot instants,
/// 484 same-statement concurrent groups at n ≥ 2 and 103 at n ≥ 3) and is the reason its gate's
/// constant reads the way it does.</para>
/// </summary>
public class SameStatementPileupDetectorTests
{
    /* The incident's clock. The pileup instant and the baseline observation are the store's own
       collection_time values; everything else is derived from them so the fixture cannot drift. */
    private static readonly DateTime PileupAt = new(2026, 9, 15, 20, 10, 31, 643, DateTimeKind.Utc);
    private static readonly DateTime BaselineAt = new(2026, 9, 15, 20, 7, 1, 201, DateTimeKind.Utc);
    private const string IncidentServer = "one-production-store-class";
    private const string IncidentHash = "0x8a2f19c4be77d301";
    private const string IncidentDb = "tenant_shared";

    private static SameStatementPileupDetector.SnapshotRow Row(
        DateTime at,
        int sessionId,
        long elapsedMs,
        string? hash = IncidentHash,
        string? wait = null,
        string status = "running",
        long cpuMs = 100,
        long logicalReads = 1_000,
        long physicalReads = 0,
        long? waitMs = null,
        string? db = IncidentDb,
        /* Shape-faithful and deliberately synthetic: the incident class is a parameterized select
           joining a #temp table, which is all the fixture needs the text to say. The detector's
           identity key is the DMV hash above, so the text is load-bearing only in the null-hash
           surrogate tests, which supply their own. */
        string? text = "select p.id, p.name, t.qty from dbo.product as p join #scope as t on t.id = p.id where p.org_id = @org_id") =>
        new(at, sessionId, hash, text, db, status, wait, waitMs, elapsedMs, cpuMs, logicalReads, physicalReads);

    /// <summary>The 20:10:31Z snapshot verbatim, plus the 20:07:01Z baseline observation.</summary>
    private static List<SameStatementPileupDetector.SnapshotRow> MeasuredIncidentWindow() =>
    [
        Row(PileupAt, 649, 29_124, wait: null, status: "running", cpuMs: 1_351, logicalReads: 287_049, physicalReads: 29_505),
        Row(PileupAt, 311, 28_343, wait: "PAGEIOLATCH_SH", status: "suspended", cpuMs: 203, logicalReads: 89_822, physicalReads: 2_102, waitMs: 5),
        Row(PileupAt, 847, 27_703, wait: "PAGEIOLATCH_SH", status: "suspended", cpuMs: 149, logicalReads: 47_023, physicalReads: 2_055, waitMs: 5),
        Row(BaselineAt, 300, 54, cpuMs: 47, logicalReads: 8_200),
    ];

    /// <summary>"Now" a few seconds after the pileup instant — the sweep evaluating a fresh snapshot.</summary>
    private static DateTime JustAfterPileup => PileupAt.AddSeconds(9);

    /* ─────────────────────────── the measured incident ─────────────────────────── */

    /// <summary>
    /// The lane's reason to exist: the 20:10:31Z snapshot produces ONE notify-eligible finding, with
    /// the statement identity, the session/elapsed evidence, and the incident fingerprint on it.
    /// </summary>
    [Fact]
    public void TheMeasuredIncident_Fires_NotifyEligible_WithItsEvidence()
    {
        var detections = SameStatementPileupDetector.Evaluate(
            IncidentServer, MeasuredIncidentWindow(), JustAfterPileup);

        var detection = Assert.Single(detections);
        var story = detection.Story;

        /* Notify-eligible against the shipped default (AnalysisNotifySeverity 1.5, compared with >=). */
        Assert.True(story.Severity >= 1.5,
            $"#3467: the measured incident must clear notify_severity 1.5; scored {story.Severity:F2}");
        Assert.Equal(SameStatementPileupDetector.SeverityCap, story.Severity, precision: 4);

        /* Identity and scope. */
        Assert.Equal(SameStatementPileupDetector.RootFactKey, story.RootFactKey);
        Assert.Equal(IncidentDb, story.DatabaseName);
        Assert.Contains(IncidentHash, story.StoryPath, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("queries", story.Category);
        Assert.False(story.IsAbsolution);
        Assert.Equal(1.0, story.Confidence, precision: 4);

        /* The magnitude fields the MCP root_fact/leaf_fact contract publishes: RAW values, not
           severity — three concurrent sessions, and the pack's minimum elapsed. */
        Assert.Equal(3.0, story.RootFactValue, precision: 4);
        Assert.Equal("min_elapsed_ms", story.LeafFactKey);
        Assert.Equal(27_703.0, story.LeafFactValue!.Value, precision: 4);

        /* The metadata the notification headline reads, including the baseline that makes
           "normally sub-second" a measurement. */
        var metadata = Assert.IsType<Dictionary<string, double>>(story.RootFactMetadata);
        Assert.Equal(3.0, metadata["concurrent_sessions"], precision: 4);
        Assert.Equal(27_703.0, metadata["min_elapsed_ms"], precision: 4);
        Assert.Equal(29_124.0, metadata["max_elapsed_ms"], precision: 4);
        Assert.Equal(2.0, metadata["io_wait_sessions"], precision: 4);
        Assert.Equal(1.0, metadata["baseline_observations"], precision: 4);
        Assert.Equal(54.0, metadata["baseline_max_elapsed_ms"], precision: 4);

        /* The fingerprint is populated and statement-scoped (folding is pinned separately). */
        Assert.Equal(
            SameStatementPileupDetector.ComputeIncidentId(IncidentServer, IncidentDb, IncidentHash),
            story.IncidentId);
        Assert.Equal(16, story.IncidentId.Length);
    }

    /// <summary>
    /// The drill-down carries the three piled-up sessions worst-first with their waits and reads, and
    /// the baseline rows the sub-second claim rests on — the evidence an operator needs to decide
    /// whether the plan deserves forcing (#2138's trigger-input point).
    /// </summary>
    [Fact]
    public void TheMeasuredIncident_DrillDown_QuotesTheSessionsAndTheBaseline()
    {
        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, MeasuredIncidentWindow(), JustAfterPileup));

        var sessions = Assert.IsType<List<object>>(detection.DrillDown["pileup_sessions"]);
        Assert.Equal(3, sessions.Count);

        /* Worst-first: the 29 124 ms leader with its 287 049 logical reads leads the section (the
           DrillDownSerializer keeps section HEADS, so ordering is load-bearing). */
        var leader = JsonSerializer.SerializeToElement(sessions[0]);
        Assert.Equal(649, leader.GetProperty("session_id").GetInt32());
        Assert.Equal(29_124, leader.GetProperty("elapsed_ms").GetInt64());
        Assert.Equal(287_049, leader.GetProperty("logical_reads").GetInt64());

        /* The two suspended copies carry the mechanism: PAGEIOLATCH_SH. */
        var suspended = sessions.Skip(1)
            .Select(s => JsonSerializer.SerializeToElement(s))
            .Select(e => e.GetProperty("wait_type").GetString())
            .ToList();
        Assert.All(suspended, w => Assert.Equal("PAGEIOLATCH_SH", w));

        var baseline = Assert.IsType<List<object>>(detection.DrillDown["recent_baseline"]);
        var baselineRow = JsonSerializer.SerializeToElement(Assert.Single(baseline));
        Assert.Equal(54, baselineRow.GetProperty("elapsed_ms").GetInt64());

        var statement = JsonSerializer.SerializeToElement(detection.DrillDown["statement"]);
        Assert.Equal(IncidentHash, statement.GetProperty("identity").GetString());
        Assert.Equal(IncidentDb, statement.GetProperty("database_name").GetString());
    }

    /// <summary>
    /// The frozen advice (#3463's lesson, and FactAdvice's value-stated path): StoryText round-trips
    /// as an AdviceBlock whose headline quotes the MEASURED numbers, so every surface — e-mail,
    /// webhook, MCP, viewer card — reads the same figures the detector observed.
    /// </summary>
    [Fact]
    public void TheMeasuredIncident_FreezesValueStatedAdvice_IntoStoryText()
    {
        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, MeasuredIncidentWindow(), JustAfterPileup));

        var advice = FactAdvice.TryReadStoryText(detection.Story.StoryText);
        Assert.NotNull(advice);
        Assert.Contains("3 concurrent sessions", advice!.Headline, StringComparison.Ordinal);
        Assert.Contains(IncidentDb, advice.Headline, StringComparison.Ordinal);
        Assert.Contains("54 ms", advice.Headline, StringComparison.Ordinal);

        /* The MEASURED wait type, not the phrase "IO-class": an operator reading the e-mail should not
           have to open the drill-down to learn which wait the convoy is on, and PAGEIOLATCH_SH vs
           WRITELOG point at different storage conversations. */
        Assert.Contains("PAGEIOLATCH_SH", advice.Investigation, StringComparison.Ordinal);

        /* And the static fallback exists for rows persisted without the frozen text. */
        var fallback = FactAdvice.GetForFactKey(SameStatementPileupDetector.RootFactKey);
        Assert.NotNull(fallback);
        Assert.Contains("pileup", fallback!.Headline, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── the shapes that must not fire ─────────────────────────── */

    /// <summary>
    /// The pre-incident snapshot — the same statement, 3.5 minutes earlier, 54 ms in flight. The
    /// store held this and it is NOT an incident; a detector that fires here would page on every
    /// healthy multi-caller statement in the fleet.
    /// </summary>
    [Fact]
    public void ThePreIncidentSnapshot_DoesNotFire()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(BaselineAt, 300, 54, cpuMs: 47, logicalReads: 8_200),
            Row(BaselineAt, 301, 61, cpuMs: 50, logicalReads: 8_400),
            Row(BaselineAt, 302, 48, cpuMs: 44, logicalReads: 7_900),
            Row(BaselineAt.AddMinutes(-1), 299, 52, logicalReads: 8_100),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(
            IncidentServer, window, BaselineAt.AddSeconds(5)));
    }

    /// <summary>
    /// Busy-but-healthy parallel workload: many concurrent sessions, all slow, all on DIFFERENT
    /// statements. This is an ordinary loaded server — the shape the finding must not mistake for a
    /// shared-plan flip, and the reason the predicate groups by statement identity rather than
    /// counting concurrency.
    /// </summary>
    [Fact]
    public void BusyButHealthy_DifferentStatements_DoesNotFire()
    {
        var at = PileupAt;
        var window = new List<SameStatementPileupDetector.SnapshotRow>();
        for (var i = 0; i < 8; i++)
        {
            /* Eight distinct statements, every one past the elapsed floor and on a page-IO wait. */
            window.Add(Row(at, 700 + i, 25_000 + i, hash: $"0xdistinct{i:D8}", wait: "PAGEIOLATCH_SH", status: "suspended"));
            /* Each with its own sub-second baseline, so ONLY the shared-statement condition is missing. */
            window.Add(Row(at.AddMinutes(-3), 600 + i, 40, hash: $"0xdistinct{i:D8}"));
        }

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// A long-running single session, hours deep on a normally-fast statement. That is
    /// long_running_query's beat (and its 30-minute threshold); this finding is about a CONVOY, and
    /// its minimum-session gate is what keeps the two from fighting over the same event.
    /// </summary>
    [Fact]
    public void ALongRunningSingleSession_DoesNotFire()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, 3_600_000, wait: "PAGEIOLATCH_SH", status: "suspended", logicalReads: 9_000_000),
            Row(PileupAt.AddMinutes(-4), 300, 54),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// Two sessions, both deep — the issue's "two-session brief pileup must not notify", held one
    /// step stronger than asked: it produces no finding at ANY elapsed time, because the session
    /// floor is a predicate and not a severity input. A coincidentally-slow pair is not a shared-plan
    /// flip, and the fleet data's 484 n ≥ 2 groups are full of ordinary paired workloads.
    /// </summary>
    [Theory]
    [InlineData(12_000)]
    [InlineData(29_000)]
    [InlineData(600_000)]
    public void TwoSessions_NeverFire_AtAnyElapsed(long elapsedMs)
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, elapsedMs, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 311, elapsedMs - 500, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt.AddMinutes(-4), 300, 54),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// The four fleet shapes that reached n ≥ 3 with every session past ten seconds in four
    /// server-days, minus the incident itself. Each fails a DIFFERENT gate, which is the argument
    /// that the conjunction — not any single threshold — is what makes the finding specific:
    /// <list type="bullet">
    /// <item>the inventory-sync job (n = 5, 13 s): no IO-class wait, and no in-window observation;</item>
    /// <item>the permission rebuild (n = 3, 6.7 s): priors already at 11.9 s — never sub-second;</item>
    /// <item>the nightly catalog scan (n = 3, 24 s): first observation of the night, no baseline;</item>
    /// <item>the slow shipment pair (2 s baseline, deep now): the baseline ceiling is 1 000 ms.</item>
    /// </list>
    /// </summary>
    [Fact]
    public void TheFleetsOtherDeepGroups_DoNotFire()
    {
        var at = PileupAt;

        /* The sync job: five concurrent copies, all past 13 s, ZERO on IO waits, never seen before. */
        var syncJob = new List<SameStatementPileupDetector.SnapshotRow>();
        for (var i = 0; i < 5; i++)
        {
            syncJob.Add(Row(at, 800 + i, 13_000 + i * 100, hash: "0xsyncjob00000001", wait: null, status: "running"));
        }

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, syncJob, JustAfterPileup));

        /* The permission rebuild: three copies past the floor, but its priors were already slow. */
        List<SameStatementPileupDetector.SnapshotRow> permissions =
        [
            Row(at, 810, 10_500, hash: "0xpermrebuild0001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 811, 10_400, hash: "0xpermrebuild0001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 812, 10_300, hash: "0xpermrebuild0001", wait: null, status: "running"),
            Row(at.AddMinutes(-6), 700, 11_900, hash: "0xpermrebuild0001"),
            Row(at.AddMinutes(-12), 701, 6_700, hash: "0xpermrebuild0001"),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, permissions, JustAfterPileup));

        /* The nightly catalog scan: three copies, 24 s, on page IO — and no prior observation at all,
           so "normally sub-second" is unproven and the finding declines to claim it. */
        List<SameStatementPileupDetector.SnapshotRow> nightly =
        [
            Row(at, 820, 24_000, hash: "0xnightlyscan0001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 821, 24_100, hash: "0xnightlyscan0001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 822, 24_200, hash: "0xnightlyscan0001", wait: null, status: "running"),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, nightly, JustAfterPileup));

        /* The slow shipment statement: three deep copies with a 2-second prior — over the ceiling, so
           this is an already-slow statement getting slower, not a flip from sub-second. */
        List<SameStatementPileupDetector.SnapshotRow> shipment =
        [
            Row(at, 830, 89_000, hash: "0xshipmentread001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 831, 88_000, hash: "0xshipmentread001", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(at, 832, 87_000, hash: "0xshipmentread001", wait: null, status: "running"),
            Row(at.AddMinutes(-5), 702, 2_000, hash: "0xshipmentread001"),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, shipment, JustAfterPileup));
    }

    /// <summary>
    /// The mechanism gate on its own: the incident's pack with every IO wait removed does not fire.
    /// A CPU-bound convoy on a normally-fast statement is a different event with a different fix, and
    /// this rung prices the IO-saturating one the issue measured.
    /// </summary>
    [Fact]
    public void NoIoClassWait_DoesNotFire()
    {
        var window = MeasuredIncidentWindow()
            .Select(r => r with { WaitType = r.WaitType is null ? null : "SOS_SCHEDULER_YIELD" })
            .ToList();

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// The staleness rule (#1812's shape, borrowed from the long-running alert on this table): a
    /// snapshot older than the cutoff is not evaluated, so a collector outage cannot hold a
    /// level-triggered finding firing forever on frozen rows.
    /// </summary>
    [Fact]
    public void AStaleSnapshot_DoesNotFire()
    {
        var window = MeasuredIncidentWindow();

        /* One minute inside the cutoff: still live. */
        Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, window,
            PileupAt.AddMinutes(SameStatementPileupDetector.StaleSnapshotCutoffMinutes - 1)));

        /* One minute past it: the store's newest word on this server is too old to act on. */
        Assert.Empty(SameStatementPileupDetector.Evaluate(
            IncidentServer, window,
            PileupAt.AddMinutes(SameStatementPileupDetector.StaleSnapshotCutoffMinutes + 1)));
    }

    /// <summary>
    /// The noise filters, shared so both SKUs' readers stay thin: the health session
    /// (sp_server_diagnostics runs for days and is always "one statement"), deliberate waits, and
    /// backup workers cannot form a pileup.
    /// </summary>
    [Theory]
    [InlineData("SP_SERVER_DIAGNOSTICS_SLEEP", "sp_server_diagnostics")]
    [InlineData("WAITFOR", "waitfor delay '00:01:00'")]
    [InlineData("BACKUPIO", "backup database [x] to disk = 'y'")]
    public void NoiseShapes_NeverFormAPileup(string wait, string text)
    {
        var at = PileupAt;
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(at, 900, 136_000_000, hash: "0xnoise0000000001", wait: wait, status: "suspended", text: text),
            Row(at, 901, 136_000_000, hash: "0xnoise0000000001", wait: wait, status: "suspended", text: text),
            Row(at, 902, 136_000_000, hash: "0xnoise0000000001", wait: wait, status: "suspended", text: text),
            Row(at.AddMinutes(-3), 903, 40, hash: "0xnoise0000000001", text: text),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /* ─────────────────────────── severity calibration ─────────────────────────── */

    /// <summary>
    /// The severity arithmetic, pinned in the unit the gate compares (#3463): severity on the shared
    /// 0–2 band, against notify_severity's default 1.5. The three calibration points the constant's
    /// doc comment claims:
    /// <list type="bullet">
    /// <item>the measured incident (3 × 27.703 s = 83.109 session-seconds) → 2.0777 uncapped, 2.0
    /// capped — 0.578 severity points of headroom over the gate;</item>
    /// <item>the minimum firing shape (3 × 10 s = 30 session-seconds) → 0.75, half the gate: a
    /// visible finding that does not page;</item>
    /// <item>notify-eligibility begins at 60 session-seconds — 3 × 20 s, 4 × 15 s, 6 × 10 s.</item>
    /// </list>
    /// </summary>
    [Theory]
    /* The measured incident: uncapped 2.078, capped at the band ceiling. */
    [InlineData(3, 27_703, 2.0, true)]
    /* The floor shape: fires, does not notify. */
    [InlineData(3, 10_000, 0.75, false)]
    /* The notify boundary, three ways in — all exactly 1.5, and >= is the gate's comparison. */
    [InlineData(3, 20_000, 1.5, true)]
    [InlineData(4, 15_000, 1.5, true)]
    [InlineData(6, 10_000, 1.5, true)]
    /* Just under the boundary: 3 × 19 s = 57 session-seconds. */
    [InlineData(3, 19_000, 1.425, false)]
    public void SeverityArithmetic_IsCalibratedAgainstTheNotifyGate(
        int sessions, long minElapsedMs, double expectedSeverity, bool notifyEligible)
    {
        var severity = SameStatementPileupDetector.ComputeSeverity(sessions, minElapsedMs);

        Assert.Equal(expectedSeverity, severity, precision: 4);
        Assert.Equal(notifyEligible, severity >= 1.5);
        Assert.True(severity <= SameStatementPileupDetector.SeverityCap);
    }

    /// <summary>
    /// The formula's own identity, stated in session-seconds so the divisor's unit cannot drift from
    /// its doc comment: severity × the divisor == sessions × min-elapsed-seconds, below the cap.
    /// </summary>
    [Fact]
    public void SeverityIsSessionSecondsOverTheNamedDivisor()
    {
        const int sessions = 3;
        const long minElapsedMs = 15_000;
        var sessionSeconds = sessions * (minElapsedMs / 1000.0);

        var severity = SameStatementPileupDetector.ComputeSeverity(sessions, minElapsedMs);

        Assert.Equal(
            sessionSeconds,
            severity * SameStatementPileupDetector.SessionSecondsPerSeverityPoint,
            precision: 6);

        /* And the notify gate's own boundary, expressed in the detector's unit. */
        Assert.Equal(60.0, 1.5 * SameStatementPileupDetector.SessionSecondsPerSeverityPoint, precision: 6);
    }

    /// <summary>
    /// The detector's severity IS the calibrated formula — a story that scored some other way would
    /// make every calibration claim above decorative.
    /// </summary>
    [Fact]
    public void TheDetectorScoresWithTheCalibratedFormula()
    {
        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, MeasuredIncidentWindow(), JustAfterPileup));

        Assert.Equal(
            SameStatementPileupDetector.ComputeSeverity(3, 27_703),
            detection.Story.Severity,
            precision: 6);
    }

    /* ─────────────────────────── incident folding ─────────────────────────── */

    /// <summary>
    /// Episode two folds onto episode one. The measured statement re-drew its regressed plan later
    /// the same evening after fully self-clearing, which is the recurrence class the issue's first
    /// comment describes: an operator deciding whether the plan deserves forcing wants ONE incident
    /// trail with two occurrences, not two unrelated surprises. Both the incident id (the folding key
    /// the notification service and the occurrence reader group on) and the story path hash (the mute
    /// key) are identical across episodes.
    /// </summary>
    [Fact]
    public void EpisodeTwo_FoldsOntoEpisodeOnesIncident()
    {
        var episodeOne = Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, MeasuredIncidentWindow(), JustAfterPileup));

        /* ~2h40m later: same statement, same server, same database — a fresh pileup after a full
           self-clear, with its own sub-second baseline re-established in the meantime. */
        var laterPileup = PileupAt.AddMinutes(160);
        List<SameStatementPileupDetector.SnapshotRow> episodeTwoWindow =
        [
            Row(laterPileup, 259, 28_000, wait: "PAGEIOLATCH_SH", status: "suspended", logicalReads: 121_359, physicalReads: 7_388),
            Row(laterPileup, 260, 27_500, wait: "PAGEIOLATCH_SH", status: "suspended", logicalReads: 98_000, physicalReads: 6_100),
            Row(laterPileup, 261, 27_100, wait: null, status: "running", logicalReads: 140_000, physicalReads: 9_200),
            Row(laterPileup.AddMinutes(-4), 258, 61, logicalReads: 8_400),
        ];

        var episodeTwo = Assert.Single(SameStatementPileupDetector.Evaluate(
            IncidentServer, episodeTwoWindow, laterPileup.AddSeconds(7)));

        Assert.Equal(episodeOne.Story.IncidentId, episodeTwo.Story.IncidentId);
        Assert.Equal(episodeOne.Story.StoryPathHash, episodeTwo.Story.StoryPathHash);
    }

    /// <summary>
    /// The fingerprint discriminates where it must: a different statement, database, or server is a
    /// different incident. Without this, folding would be collapsing — one statement's pileup would
    /// mute and cool down every other statement's (#3459's rule: never answer one cooldown key's
    /// question with another key's history).
    /// </summary>
    [Fact]
    public void TheFingerprint_SeparatesStatements_Databases_AndServers()
    {
        var baseline = SameStatementPileupDetector.ComputeIncidentId(IncidentServer, IncidentDb, IncidentHash);

        Assert.NotEqual(baseline, SameStatementPileupDetector.ComputeIncidentId(IncidentServer, IncidentDb, "0xotherstatement"));
        Assert.NotEqual(baseline, SameStatementPileupDetector.ComputeIncidentId(IncidentServer, "other_db", IncidentHash));
        Assert.NotEqual(baseline, SameStatementPileupDetector.ComputeIncidentId("another-server", IncidentDb, IncidentHash));

        /* Stable across calls — a fingerprint that moved would break the trail it exists to keep. */
        Assert.Equal(baseline, SameStatementPileupDetector.ComputeIncidentId(IncidentServer, IncidentDb, IncidentHash));
    }

    /// <summary>
    /// Two DIFFERENT statements piling up in the same snapshot produce two findings with two
    /// fingerprints — the grouping is per statement, and the story path (hence the mute key) carries
    /// the identity.
    /// </summary>
    [Fact]
    public void TwoStatementsPilingUp_ProduceTwoDistinctFindings()
    {
        var at = PileupAt;
        var window = new List<SameStatementPileupDetector.SnapshotRow>();
        foreach (var hash in new[] { "0xfirststatement1", "0xsecondstatement" })
        {
            window.Add(Row(at, 910 + hash.Length, 22_000, hash: hash, wait: "PAGEIOLATCH_SH", status: "suspended"));
            window.Add(Row(at, 920 + hash.Length, 21_500, hash: hash, wait: "PAGEIOLATCH_SH", status: "suspended"));
            window.Add(Row(at, 930 + hash.Length, 21_000, hash: hash, wait: null, status: "running"));
            window.Add(Row(at.AddMinutes(-3), 940 + hash.Length, 45, hash: hash));
        }

        var detections = SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup);

        Assert.Equal(2, detections.Count);
        Assert.Equal(2, detections.Select(d => d.Story.IncidentId).Distinct().Count());
        Assert.Equal(2, detections.Select(d => d.Story.StoryPathHash).Distinct().Count());
    }

    /* ─────────────────────────── database scoping (#3474) ─────────────────────────── */

    /// <summary>
    /// The conflation kill-shot (#3469's review, then measured on the evidence day: within the same
    /// hour as the incident, the same statement text ran 9.7 s in a DIFFERENT tenant database on the
    /// same server — one snapshot's timing from joining the pack under a server-wide key). Three
    /// same-hash sessions in three databases are three one-session groups, not a pileup: a
    /// compiled-plan cache entry is keyed per database, so these sessions never shared the plan the
    /// finding would have blamed. Each database carries its own sub-second history, so ONLY the
    /// shared-database condition is missing — under the old server-wide key this window produced one
    /// notify-eligible "pileup" whose members shared nothing but text.
    /// </summary>
    [Fact]
    public void ThreeDatabases_SharingAHash_AreThreeGroups_NotAPileup()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, 29_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tenant_a"),
            Row(PileupAt, 311, 28_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tenant_b"),
            Row(PileupAt, 847, 27_000, wait: null, status: "running", db: "tenant_c"),
            Row(PileupAt.AddMinutes(-3), 300, 54, db: "tenant_a"),
            Row(PileupAt.AddMinutes(-3), 301, 61, db: "tenant_b"),
            Row(PileupAt.AddMinutes(-3), 302, 48, db: "tenant_c"),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// Baseline isolation, the vouching direction: "normally sub-second" is a claim about THIS
    /// database's plan. A real pileup in one database with no in-window history of its own is not
    /// vouched for by another database's fast history of the same hash — and once the pileup
    /// database's own observation exists, the same window fires, attributed to and counted from that
    /// database alone: the concurrent same-hash copy in the other database joins neither the session
    /// count nor the pack minimum the severity is priced on.
    /// </summary>
    [Fact]
    public void AnotherDatabasesFastHistory_NeitherVouches_NorJoinsThePack()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, 29_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tenant_a"),
            Row(PileupAt, 311, 28_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tenant_a"),
            Row(PileupAt, 847, 27_000, wait: null, status: "running", db: "tenant_a"),
            /* A concurrent same-hash copy in ANOTHER database, slower than the pack's own minimum —
               the measured near-miss shape. It must not become a fourth session or the new minimum. */
            Row(PileupAt, 512, 26_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tenant_b"),
            /* And the other database's fast history for the same hash, in the window. Under the
               server-wide baseline this row vouched for tenant_a's pack. */
            Row(PileupAt.AddMinutes(-3), 300, 54, db: "tenant_b"),
        ];

        /* Unproven where it actually runs: no finding. */
        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));

        /* With tenant_a's own sub-second observation present, the identical window fires — the
           foreign rows' presence changes nothing in either direction. */
        window.Add(Row(PileupAt.AddMinutes(-4), 299, 61, db: "tenant_a"));

        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
        Assert.Equal("tenant_a", detection.Story.DatabaseName);
        Assert.Equal(3.0, detection.Story.RootFactValue, precision: 4);
        Assert.Equal(27_000.0, detection.Story.LeafFactValue!.Value, precision: 4);
    }

    /// <summary>
    /// Baseline isolation, the suppression direction — the one that fails QUIET. The measured
    /// near-miss row verbatim: the same statement text at 9.7 s / 7.4M reads in a different tenant
    /// database, within the same hour as the incident. Under the server-wide baseline that row
    /// breaks the sub-second ceiling for EVERY database's pack and silences the real pileup; scoped,
    /// it is the other database's story and the incident fires exactly as measured.
    /// </summary>
    [Fact]
    public void AnotherDatabasesSlowRow_DoesNotSuppressARealPileup()
    {
        var window = MeasuredIncidentWindow();
        window.Add(Row(PileupAt.AddMinutes(-8), 512, 9_700, logicalReads: 7_400_000, db: "tenant_other"));

        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
        Assert.Equal(IncidentDb, detection.Story.DatabaseName);
        Assert.Equal(SameStatementPileupDetector.SeverityCap, detection.Story.Severity, precision: 4);
    }

    /// <summary>
    /// The tempdb rule (#3474, measured: the day before the incident, a 16.5 s copy of the incident
    /// statement was recorded with tempdb as its database context — the session's execution context
    /// WAS tempdb, reaching the tenant tables by three-part names; a #temp join moves nothing). A
    /// tempdb-attributed row scopes to tempdb: it neither joins a tenant database's pack nor
    /// rewrites its arithmetic, tempdb-attributed sessions can form their own pileup, and that
    /// pileup's baseline is drawn only from tempdb-attributed history — correct plan-cache grouping
    /// (a compiled plan is keyed by its context database's dbid, so the tempdb-context copies share
    /// one among themselves), not reattribution guessing (DatabaseScope's remarks carry why a
    /// reattributed row would blame the wrong plan and poison the baselines this scoping cleans).
    /// </summary>
    [Fact]
    public void TempdbAttributedRows_GroupAmongThemselves()
    {
        /* The measured wrinkle beside the measured incident: the 16.5 s tempdb-attributed copy at
           the pileup instant. Under a database-blind key it would have become the pack's fourth
           session AND its new minimum (16.5 s, not 27.7 s), repricing the severity with a row from a
           context the plan does not live in. */
        var window = MeasuredIncidentWindow();
        window.Add(Row(PileupAt, 950, 16_500, wait: "PAGEIOLATCH_SH", status: "suspended", waitMs: 5, db: "tempdb"));

        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
        Assert.Equal(IncidentDb, detection.Story.DatabaseName);
        Assert.Equal(3.0, detection.Story.RootFactValue, precision: 4);
        Assert.Equal(27_703.0, detection.Story.LeafFactValue!.Value, precision: 4);

        /* Tempdb-attributed sessions form their own (tempdb, statement) group, vouched by
           tempdb-attributed history. */
        List<SameStatementPileupDetector.SnapshotRow> tempdbPileup =
        [
            Row(PileupAt, 960, 22_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tempdb"),
            Row(PileupAt, 961, 21_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "tempdb"),
            Row(PileupAt, 962, 20_000, wait: null, status: "running", db: "tempdb"),
            Row(PileupAt.AddMinutes(-3), 963, 70, db: "tempdb"),
        ];

        var tempdbDetection = Assert.Single(
            SameStatementPileupDetector.Evaluate(IncidentServer, tempdbPileup, JustAfterPileup));
        Assert.Equal("tempdb", tempdbDetection.Story.DatabaseName);

        /* And a tenant database's history cannot vouch for a tempdb-scoped pack: the same pileup
           with its only prior observation attributed to the tenant database stays quiet. */
        var crossVouched = tempdbPileup
            .Select(r => r.CollectionTime == PileupAt ? r : r with { DatabaseName = IncidentDb })
            .ToList();

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, crossVouched, JustAfterPileup));
    }

    /// <summary>
    /// The fold-attribution fix, pinned as behavior: the finding's database IS the group key's, so
    /// row order cannot move it. The previous shape — a first-non-empty pick over the pack — could
    /// hand the same recurring pileup different database names across evaluations when row order
    /// shifted (rows tie on collection_time), and ComputeIncidentId folds on databaseName, so the
    /// fold key itself wobbled (#3469's review, first-listed consequence). Permuting the window's
    /// row order must never move the database or the fingerprint.
    /// </summary>
    [Fact]
    public void TheFindingsDatabase_IsTheGroupKeys_UnderAnyRowOrder()
    {
        var window = MeasuredIncidentWindow();

        List<List<SameStatementPileupDetector.SnapshotRow>> permutations =
        [
            window,
            Enumerable.Reverse(window).ToList(),
            window.OrderBy(r => r.SessionId).ToList(),
            window.OrderByDescending(r => r.ElapsedMs).ToList(),
        ];

        foreach (var permutation in permutations)
        {
            var detection = Assert.Single(
                SameStatementPileupDetector.Evaluate(IncidentServer, permutation, JustAfterPileup));

            Assert.Equal(IncidentDb, detection.Story.DatabaseName);
            Assert.Equal(
                SameStatementPileupDetector.ComputeIncidentId(IncidentServer, IncidentDb, IncidentHash),
                detection.Story.IncidentId);
        }
    }

    /// <summary>
    /// Rows the DMV reported with no database name fold to one empty scope: they still group with
    /// their identity-mates (fragmenting per row would make an unattributed pileup undetectable),
    /// and the finding carries a null database — the same null the pick-based shape produced for an
    /// all-null pack, so nothing downstream of the story changes.
    /// </summary>
    [Fact]
    public void RowsWithoutADatabaseName_ShareOneScope_AndCarryANullDatabase()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 970, 25_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: null),
            Row(PileupAt, 971, 24_000, wait: "PAGEIOLATCH_SH", status: "suspended", db: "  "),
            Row(PileupAt, 972, 23_000, wait: null, status: "running", db: null),
            Row(PileupAt.AddMinutes(-3), 973, 45, db: null),
        ];

        var detection = Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
        Assert.Null(detection.Story.DatabaseName);
    }

    /* ─────────────────────────── Query Store independence ─────────────────────────── */

    /// <summary>
    /// The QS-independence requirement, pinned behaviorally (the source census pins it structurally):
    /// the detector's whole input is active-query snapshot rows, and the incident fires from a fixture
    /// whose rows carry NO query_store-derived field — no plan id, no query_store query id, no
    /// runtime-stats row. On the deployments where the Query Store readers are deliberately disabled
    /// (#2296), this is the difference between a finding and a blind spot.
    /// </summary>
    [Fact]
    public void TheFinding_IsComputedFromSnapshotRowsAlone()
    {
        /* The row type IS the contract: every member is a query_snapshots column. A field added here
           from a query_store source would break this list and have to be justified. */
        var members = typeof(SameStatementPileupDetector.SnapshotRow)
            .GetProperties()
            .Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            new[]
            {
                "CollectionTime", "CpuTimeMs", "DatabaseName", "ElapsedMs", "LogicalReads",
                "PhysicalReads", "QueryHash", "QueryText", "SessionId", "Status", "WaitTimeMs", "WaitType",
            },
            members);

        /* And the identity the fingerprint is built on is the DMV's own statement hash — present on
           the snapshot row, not resolved through any Query Store lookup. */
        var row = Row(PileupAt, 649, 29_124);
        Assert.Equal(IncidentHash, SameStatementPileupDetector.StatementIdentity(row));
    }

    /// <summary>
    /// The null-hash surrogate: a row whose <c>query_hash</c> is absent still groups and fingerprints,
    /// by normalized statement text — so a statement shape the DMV did not hash cannot silently
    /// disable the finding. Whitespace and case normalize; different text is a different identity.
    /// </summary>
    [Fact]
    public void ANullQueryHash_FallsBackToTheTextSurrogate()
    {
        var a = Row(PileupAt, 1, 20_000, hash: null, text: "SELECT  A\r\n FROM  T");
        var b = Row(PileupAt, 2, 20_000, hash: null, text: "select a from t");
        var c = Row(PileupAt, 3, 20_000, hash: null, text: "select b from t");

        var identityA = SameStatementPileupDetector.StatementIdentity(a);
        Assert.StartsWith("text:", identityA, StringComparison.Ordinal);
        Assert.Equal(identityA, SameStatementPileupDetector.StatementIdentity(b));
        Assert.NotEqual(identityA, SameStatementPileupDetector.StatementIdentity(c));

        /* And such a group fires like any other — the surrogate is a real identity, not a bypass. */
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 11, 25_000, hash: null, text: "select a from t", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 12, 24_000, hash: null, text: "SELECT A FROM T", wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 13, 23_000, hash: null, text: "select   a   from   t", wait: null, status: "running"),
            Row(PileupAt.AddMinutes(-3), 14, 40, hash: null, text: "select a from t"),
        ];

        Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /* ─────────────────────────── the baseline window's own behavior ─────────────────────────── */

    /// <summary>
    /// The baseline reads strictly BEHIND the pileup instant and strictly WITHIN the lookback. The
    /// pileup's own rows are the anomaly, not the norm (including them would make every pileup its
    /// own counter-evidence), and an observation older than the window cannot vouch for "recent".
    /// </summary>
    [Fact]
    public void TheBaselineWindow_ExcludesThePileupInstant_AndAgesOut()
    {
        /* Baseline one minute outside the lookback: unproven, so no fire. */
        var tooOld = MeasuredIncidentWindow()
            .Select(r => r.CollectionTime == BaselineAt
                ? r with { CollectionTime = PileupAt.AddMinutes(-(SameStatementPileupDetector.BaselineLookbackMinutes + 1)) }
                : r)
            .ToList();

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, tooOld, JustAfterPileup));

        /* Just inside it: fires. */
        var justInside = MeasuredIncidentWindow()
            .Select(r => r.CollectionTime == BaselineAt
                ? r with { CollectionTime = PileupAt.AddMinutes(-(SameStatementPileupDetector.BaselineLookbackMinutes - 1)) }
                : r)
            .ToList();

        Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, justInside, JustAfterPileup));

        /* With NO prior observation at all, the pileup rows alone must not vouch for themselves. */
        var pileupOnly = MeasuredIncidentWindow().Where(r => r.CollectionTime == PileupAt).ToList();

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, pileupOnly, JustAfterPileup));
    }

    /// <summary>
    /// The quiet period the lookback buys, stated as behavior: once an episode's deep rows are in the
    /// window, the same statement cannot re-fire until they age out. That is what keeps a 90-second
    /// episode spanning several snapshot instants from paging once per instant, and it is why the
    /// lookback is tuned near the notify cooldown's floor rather than to hours.
    /// </summary>
    [Fact]
    public void AnEpisodesOwnDeepRows_SuppressARefireWhileTheyAreInTheWindow()
    {
        /* The next snapshot instant, 70 seconds on: the pileup persists, and episode one's 27-second
           rows are now the "baseline" — over the ceiling, so no second finding for the same episode. */
        var nextInstant = PileupAt.AddSeconds(70);
        var window = MeasuredIncidentWindow();
        window.AddRange(
        [
            Row(nextInstant, 649, 29_124 + 70_000, wait: null, status: "running", logicalReads: 320_000),
            Row(nextInstant, 311, 28_343 + 70_000, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(nextInstant, 847, 27_703 + 70_000, wait: "PAGEIOLATCH_SH", status: "suspended"),
        ]);

        Assert.Empty(SameStatementPileupDetector.Evaluate(
            IncidentServer, window, nextInstant.AddSeconds(5)));
    }

    /// <summary>
    /// The ACCEPTED onset blind spot, pinned as behavior so a change to it is a decision and not an
    /// accident (the detector's class remarks carry the full "what this cannot see" statement). If
    /// the collector's FIRST sample of an episode catches the pack between the sub-second ceiling
    /// and the elapsed floor — here ~5 s in — that instant correctly produces nothing (the floor
    /// gate), and its rows then poison the statement's own baseline: the next instant, every session
    /// past the floor and the mechanism on display, STILL produces nothing, because the ceiling gate
    /// reads the onset rows as multi-second priors. Roughly a 9-in-60 sampling-phase exposure at the
    /// one-minute cadence on the measured 90-second shape, and it fails QUIET — a missed page, never
    /// a false one. Accepted until the recurrence watch produces a measured miss; the counterfactual
    /// half below is what either repair (onset-exclusion, qualifying-subset pricing) would have to
    /// preserve.
    /// </summary>
    [Fact]
    public void AnOnsetSampledEpisode_PoisonsItsOwnBaseline_AndStaysQuiet_TheAcceptedBlindSpot()
    {
        var onsetAt = PileupAt;               /* first sample lands ~5 s into the episode */
        var deepAt = PileupAt.AddSeconds(60); /* the next collection: same sessions, past the floor */

        List<SameStatementPileupDetector.SnapshotRow> onsetInstant =
        [
            Row(onsetAt, 649, 5_200, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(onsetAt, 311, 5_000, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(onsetAt, 847, 4_800, wait: null, status: "running"),
            /* The genuine history is there — the statement really is normally sub-second. */
            Row(onsetAt.AddMinutes(-3), 300, 54),
        ];

        /* Instant one: no finding — the floor gate, correctly (nothing is past 10 s yet). */
        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, onsetInstant, onsetAt.AddSeconds(5)));

        /* Instant two: every session past the floor, the IO wait present, the 54 ms prior still in
           the window — and STILL no finding, because the baseline gate now reads instant one's
           5-second rows and the sub-second ceiling fails. The accepted behavior, stated. */
        var window = new List<SameStatementPileupDetector.SnapshotRow>(onsetInstant)
        {
            Row(deepAt, 649, 65_200, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(deepAt, 311, 65_000, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(deepAt, 847, 64_800, wait: null, status: "running"),
        };

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, deepAt.AddSeconds(5)));

        /* The counterfactual that isolates the cause: the identical second instant with the onset
           sample absent — the collector's phase landing 10 s later — fires. The miss is priced
           entirely by where the first sample landed, which is the phase arithmetic the doc states. */
        var withoutOnset = window.Where(r => r.CollectionTime != onsetAt).ToList();

        Assert.Single(SameStatementPileupDetector.Evaluate(IncidentServer, withoutOnset, deepAt.AddSeconds(5)));
    }

    /// <summary>
    /// Every session must be past the floor, not just the leader: one deep copy alongside two fast
    /// ones is the long-running-single-session shape wearing a crowd, and the pack's MINIMUM is what
    /// the severity prices, so the gate has to agree with the arithmetic.
    /// </summary>
    [Fact]
    public void OneDeepCopyAmongFastSiblings_DoesNotFire()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, 29_124, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 311, 120, wait: null, status: "running"),
            Row(PileupAt, 847, 95, wait: null, status: "running"),
            Row(PileupAt.AddMinutes(-3), 300, 54),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>
    /// The same session id appearing twice at one instant (a parallel request's rows, or a
    /// re-projected row) is ONE session — the pileup counts distinct sessions, because three rows
    /// from one session is a parallel plan, not a convoy of callers.
    /// </summary>
    [Fact]
    public void DuplicateSessionRowsAtOneInstant_CountAsOneSession()
    {
        List<SameStatementPileupDetector.SnapshotRow> window =
        [
            Row(PileupAt, 649, 29_124, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 649, 29_124, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt, 649, 29_124, wait: "PAGEIOLATCH_SH", status: "suspended"),
            Row(PileupAt.AddMinutes(-3), 300, 54),
        ];

        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, window, JustAfterPileup));
    }

    /// <summary>Empty and null inputs are non-events, not exceptions — the sweep calls this every
    /// cycle for every server, most of them idle.</summary>
    [Fact]
    public void EmptyInput_IsANonEvent()
    {
        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, [], JustAfterPileup));
        Assert.Empty(SameStatementPileupDetector.Evaluate(IncidentServer, null!, JustAfterPileup));
    }
}
