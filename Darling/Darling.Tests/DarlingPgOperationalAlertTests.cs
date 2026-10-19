/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2711: the pure, testable seams behind the Postgres Deadlocks/Blocking alerts —
/// <see cref="DarlingWorker.BuildPgDeadlockIncident"/>, <see cref="DarlingWorker.WorstPgBlockingChainPerRoot"/>,
/// and <see cref="DarlingWorker.BuildPgBlockingIncident"/>. The gating itself (fire/clear, edge-triggering,
/// immunity to re-firing on an unrefreshed data point) is <c>RollingCountAlertGate</c>'s job and is already
/// exhaustively pinned in <c>Lite.Tests/RollingCountAlertGateTests.cs</c>; these tests cover the mapping and
/// dedup logic layered on top of it that is genuinely new here.
///
/// <para>#3653 (A8e, the PostgreSQL host): the tier each of the four host arms fires at — the pure graders
/// (<see cref="DarlingWorker.GradePgCpuFire"/>, the shared <c>AlertEngine.GradeDeadlockFire</c> on the store's
/// tiers, the two explicit Warning constants), the source pin that no arm still passes <c>Severity: null</c>,
/// and one drive of the real <see cref="DarlingAlertDeliverer"/> proving the tier lands on the persisted row
/// where #3635's grids and <c>get_alert_history</c> read it. The arms themselves need a live store; the
/// graders and the deliverer do not.</para>
/// </summary>
public sealed class DarlingPgOperationalAlertTests
{
    private static DarlingPgDeadlockReader.PgDeadlockRow DeadlockRow(
        string hash, string? victimStatement = "SELECT 1", int victimPid = 111, int participantCount = 2) =>
        new(
            OccurredAtUtc: new DateTime(2026, 8, 31, 0, 0, 0, DateTimeKind.Utc),
            VictimPid: victimPid,
            ParticipantCount: participantCount,
            DeadlockHash: hash,
            LockModes: null,
            Resources: null,
            VictimStatement: victimStatement,
            TimesSeen: 1);

    private static DarlingPgBlockingReader.PgBlockingChainRow BlockingRow(
        long rootBackendId,
        int rootPid = 200,
        int totalVictims = 3,
        string[]? databases = null,
        string? rootQuery = "SELECT * FROM t",
        DateTime? capturedAt = null) =>
        new(
            CapturedAt: capturedAt ?? new DateTime(2026, 8, 31, 0, 0, 0, DateTimeKind.Utc),
            RootBackendId: rootBackendId,
            RootPid: rootPid,
            Databases: databases ?? new[] { "sales" },
            RootUsername: "app",
            RootApplicationName: "webapi",
            RootState: "active",
            RootQuery: rootQuery,
            RootIsIdleInTransaction: false,
            RootXactDurationMs: 5000,
            RootQueryDurationMs: 5000,
            TotalVictims: totalVictims,
            DirectVictims: totalVictims,
            MaxDepth: 1,
            WorstVictimWaitMs: 4000,
            WorstVictimQuery: "SELECT 2",
            SamplesAsRoot: 1,
            QueryTextMayBeTruncated: false,
            ChainMayBeTruncated: false);

    [Fact]
    public void BuildPgDeadlockIncident_UsesDeadlockHashAsDedupKey_AndVictimStatementAsDetail()
    {
        var incident = DarlingWorker.BuildPgDeadlockIncident(DeadlockRow("hash-1", victimStatement: "DELETE FROM t"));

        Assert.Equal("hash-1", incident.DedupKey);
        Assert.Single(incident.InvolvedObjects);
        Assert.Equal("DELETE FROM t", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgDeadlockIncident_FallsBackToPidAndParticipantCount_WhenVictimStatementIsMissing()
    {
        var incident = DarlingWorker.BuildPgDeadlockIncident(
            DeadlockRow("hash-2", victimStatement: null, victimPid: 555, participantCount: 3));

        Assert.Equal("victim pid 555, 3 participant(s)", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgDeadlockIncident_FallsBackOnWhitespaceStatement_NotJustNull()
    {
        var incident = DarlingWorker.BuildPgDeadlockIncident(
            DeadlockRow("hash-3", victimStatement: "   ", victimPid: 7, participantCount: 2));

        Assert.StartsWith("victim pid 7,", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void WorstPgBlockingChainPerRoot_CollapsesRepeatedSamplesOfTheSameRoot_ToOneEntry()
    {
        /* The defensive case this exists for (mirroring tonight's #2704/#2708 lesson): the SAME persistent
           blocker sampled across several sweep cycles within the rolling window must not inflate the count
           past "one blocking situation" just because it was still there the next time anyone looked. */
        var rows = new[]
        {
            BlockingRow(rootBackendId: 100, capturedAt: new DateTime(2026, 8, 31, 0, 5, 0, DateTimeKind.Utc)),
            BlockingRow(rootBackendId: 100, capturedAt: new DateTime(2026, 8, 31, 0, 10, 0, DateTimeKind.Utc)),
            BlockingRow(rootBackendId: 100, capturedAt: new DateTime(2026, 8, 31, 0, 15, 0, DateTimeKind.Utc)),
        };

        var worst = DarlingWorker.WorstPgBlockingChainPerRoot(rows);

        Assert.Single(worst);
        Assert.Equal(100, worst[0].RootBackendId);
    }

    [Fact]
    public void WorstPgBlockingChainPerRoot_KeepsDistinctRootsSeparate()
    {
        var rows = new[]
        {
            BlockingRow(rootBackendId: 100),
            BlockingRow(rootBackendId: 200),
            BlockingRow(rootBackendId: 100), // a repeat sample of the first root
        };

        var worst = DarlingWorker.WorstPgBlockingChainPerRoot(rows);

        Assert.Equal(2, worst.Count);
        Assert.Contains(worst, r => r.RootBackendId == 100);
        Assert.Contains(worst, r => r.RootBackendId == 200);
    }

    [Fact]
    public void WorstPgBlockingChainPerRoot_CollapsesRepeatedSamplesOfTheSameSentinelRoot_ToOneEntry()
    {
        /* The re-fire-class regression review caught: deduping the vanished-blocker sentinel (RootBackendId
           == 0) by RootPid must still collapse repeated samples of the SAME persisting vanished-root block
           — otherwise RollingCountAlertGate's watermark climbs every sweep and the alert re-fires every
           cooldown for one ongoing incident (the #1091/#2704/#2708 class this design exists to avoid). */
        var rows = new[]
        {
            BlockingRow(rootBackendId: 0, rootPid: 777, capturedAt: new DateTime(2026, 8, 31, 0, 5, 0, DateTimeKind.Utc)),
            BlockingRow(rootBackendId: 0, rootPid: 777, capturedAt: new DateTime(2026, 8, 31, 0, 10, 0, DateTimeKind.Utc)),
            BlockingRow(rootBackendId: 0, rootPid: 777, capturedAt: new DateTime(2026, 8, 31, 0, 15, 0, DateTimeKind.Utc)),
        };

        var worst = DarlingWorker.WorstPgBlockingChainPerRoot(rows);

        Assert.Single(worst);
        Assert.Equal(777, worst[0].RootPid);
    }

    [Fact]
    public void WorstPgBlockingChainPerRoot_KeepsDifferentSentinelPidsSeparate()
    {
        /* RootBackendId == 0 is PgBlockingCollector's coalesce(blocker.backend_id, 0) sentinel — the root's
           own row had already left pg_stat_activity by capture time. Two GENUINELY DIFFERENT blocking
           situations that both happen to hit this case in the same window (different pids) must both
           survive; a plain GroupBy-by-RootBackendId would wrongly merge them (the review finding this test
           pins) — dedup for the sentinel case is by RootPid instead, so different pids stay distinct. */
        var unrelatedIncidentOne = BlockingRow(rootBackendId: 0, rootPid: 111, databases: new[] { "sales" });
        var unrelatedIncidentTwo = BlockingRow(rootBackendId: 0, rootPid: 222, databases: new[] { "billing" });
        var rows = new[] { unrelatedIncidentOne, unrelatedIncidentTwo };

        var worst = DarlingWorker.WorstPgBlockingChainPerRoot(rows);

        Assert.Equal(2, worst.Count);
        Assert.Contains(worst, r => r.RootPid == 111);
        Assert.Contains(worst, r => r.RootPid == 222);
    }

    [Fact]
    public void WorstPgBlockingChainPerRoot_KeepsTheFirstSampleAsTheReaderAlreadyOrderedItWorstFirst()
    {
        /* GetPgBlockingChainsAsync orders worst-first (widest chain, then deepest, then most recent) — this
           helper must not re-sort, only dedupe, or it would silently discard that ordering. */
        var worstSample = BlockingRow(rootBackendId: 100, totalVictims: 9);
        var laterButSmallerSample = BlockingRow(rootBackendId: 100, totalVictims: 1);
        var rows = new[] { worstSample, laterButSmallerSample };

        var worst = DarlingWorker.WorstPgBlockingChainPerRoot(rows);

        Assert.Single(worst);
        Assert.Equal(9, worst[0].TotalVictims);
    }

    [Fact]
    public void BuildPgBlockingIncident_DedupKeyIsRootBackendId_NotPid()
    {
        /* Backend id, not pid, is the dedup key on purpose — pids are reused, backend id is stable for the
           life of a backend (see the collector's own doc comment). */
        var incident = DarlingWorker.BuildPgBlockingIncident(BlockingRow(rootBackendId: 424242, rootPid: 99));

        Assert.Equal("424242", incident.DedupKey);
    }

    [Fact]
    public void BuildPgBlockingIncident_GivesTheVanishedBlockerSentinelAUniqueDedupKey_NotABareZero()
    {
        /* IncidentCooldown.BuildKeys does incidents.Select(i => i.DedupKey).Distinct() to build one
           cooldown key per fingerprint — two genuinely distinct sentinel (RootBackendId == 0) incidents
           sharing the literal key "0" would collapse into one cooldown slot downstream, even though
           WorstPgBlockingChainPerRoot correctly kept both as separate list entries (review finding this
           test pins). */
        var capturedAt = new DateTime(2026, 8, 31, 1, 2, 3, DateTimeKind.Utc);
        var incidentA = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 0, rootPid: 111, capturedAt: capturedAt));
        var incidentB = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 0, rootPid: 222, capturedAt: capturedAt));

        Assert.NotEqual(incidentA.DedupKey, incidentB.DedupKey);
        Assert.NotEqual("0", incidentA.DedupKey);
        Assert.NotEqual("0", incidentB.DedupKey);
    }

    [Fact]
    public void BuildPgBlockingIncident_IncludesDatabaseRootPidVictimCountAndQuery()
    {
        var incident = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 5, databases: new[] { "sales" }, rootQuery: "SELECT * FROM x"));

        Assert.Equal("root pid 42 blocking 5 session(s) in [sales]: SELECT * FROM x", incident.InvolvedObjects[0]);
        Assert.Equal("sales", incident.Database);
    }

    [Fact]
    public void BuildPgBlockingIncident_OmitsTrailingQueryClause_WhenRootQueryIsMissing()
    {
        var incident = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 2, databases: new[] { "sales" }, rootQuery: null));

        Assert.Equal("root pid 42 blocking 2 session(s) in [sales]", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgBlockingIncident_CollapsesMultiLineRootQuery_ToASingleLinePreview()
    {
        /* Every other query-text field this codebase puts on an AlertIncident goes through
           AlertContextBuilders.TruncateText first (review finding this test pins) — Postgres root queries
           are commonly multi-line formatted DML with no length cap of their own. */
        var incident = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 1, databases: Array.Empty<string>(),
                rootQuery: "UPDATE t\nSET x = 1\nWHERE y = 2"));

        Assert.DoesNotContain('\n', incident.InvolvedObjects[0]);
        Assert.Contains("UPDATE t SET x = 1 WHERE y = 2", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgDeadlockIncident_CollapsesMultiLineVictimStatement_ToASingleLinePreview()
    {
        var incident = DarlingWorker.BuildPgDeadlockIncident(
            DeadlockRow("hash-4", victimStatement: "DELETE FROM t\nWHERE id = 1"));

        Assert.DoesNotContain('\n', incident.InvolvedObjects[0]);
        Assert.Equal("DELETE FROM t WHERE id = 1", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgBlockingIncident_OmitsDatabaseClause_WhenNoDatabasesResolved()
    {
        var incident = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 2, databases: Array.Empty<string>(), rootQuery: null));

        Assert.Equal("root pid 42 blocking 2 session(s)", incident.InvolvedObjects[0]);
        Assert.Null(incident.Database);
    }

    // ── Long-Running Query (#2711) ───────────────────────────────────────────────────────────────

    private static DarlingPgSessionStatesReader.LongRunningSessionRow LongRunningRow(
        long backendId = 1001, int pid = 100, string? databaseName = "sales", string? commandTag = "SELECT",
        long queryDurationMs = 2_100_000) =>
        new(backendId, pid, databaseName, "appuser", "myapp", commandTag, queryDurationMs);

    [Fact]
    public void BuildPgLongRunningQueryIncident_DedupKeyIsBackendId_NotPid()
    {
        /* Same reasoning as BuildPgBlockingIncident's dedup key: pids are reused, the synthetic backend id
           is stable for the life of a backend. */
        var incident = DarlingWorker.BuildPgLongRunningQueryIncident(LongRunningRow(backendId: 424242, pid: 99));

        Assert.Equal("424242", incident.DedupKey);
    }

    [Fact]
    public void BuildPgLongRunningQueryIncident_IncludesPidDurationAndCommandTag()
    {
        var incident = DarlingWorker.BuildPgLongRunningQueryIncident(
            LongRunningRow(pid: 55, commandTag: "UPDATE", queryDurationMs: 42 * 60_000L));

        Assert.Equal("pid 55 running 42m (UPDATE)", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgLongRunningQueryIncident_FallsBackToUnknown_WhenCommandTagIsMissing()
    {
        /* pg_session_states substitutes NULL for command_tag under redaction (no pg_monitor grant) — the
           incident text must not go blank or throw in that case. */
        var incident = DarlingWorker.BuildPgLongRunningQueryIncident(LongRunningRow(commandTag: null));

        Assert.Contains("(unknown)", incident.InvolvedObjects[0]);
    }

    [Fact]
    public void BuildPgLongRunningQueryIncident_CarriesTheDatabaseName()
    {
        var incident = DarlingWorker.BuildPgLongRunningQueryIncident(LongRunningRow(databaseName: "billing"));

        Assert.Equal("billing", incident.Database);
    }

    // ── #3653 (A8e): the tier each PostgreSQL host arm fires at ──────────────────────────────────

    /// <summary>
    /// WARNING below the CPU health band's Critical bar, CRITICAL at or above it — on the CAPACITY percent
    /// (percent of the configured ACU ceiling), which is the figure the gate thresholded (#3281). The raw
    /// Performance Insights CPU is passed too, exactly as the card passes it, and must not decide: a
    /// one-vCPU serverless instance reads 100% raw whenever one core is busy, and grading that red is the
    /// defect #3281 removed from the gate.
    /// </summary>
    [Theory]
    [InlineData(100.0, 80.0, AlertSeverityLevel.Warning)]
    [InlineData(100.0, 94.9, AlertSeverityLevel.Warning)]
    [InlineData(100.0, 95.0, AlertSeverityLevel.Critical)]
    [InlineData(100.0, 100.0, AlertSeverityLevel.Critical)]
    /* The raw CPU is NOT the input: pinned at 100% while the ceiling is idle, and near-idle while the
       ceiling is reached. */
    [InlineData(100.0, 30.0, AlertSeverityLevel.Warning)]
    [InlineData(20.0, 96.0, AlertSeverityLevel.Critical)]
    public void GradePgCpuFire_IsTheCardsClassifierOnTheCapacityPercent_CriticalAtTheBandsBar(
        double rawCpuPercent, double acuUtilizationPercent, AlertSeverityLevel expected)
    {
        Assert.Equal(expected, DarlingWorker.GradePgCpuFire(rawCpuPercent, acuUtilizationPercent));

        /* The same function the fleet card calls, on the same two figures, on the Performance Insights arm —
           so the alert row and the card cannot disagree about the colour of the same minute. */
        var band = ServerHealthClassifier.CpuSeverity(rawCpuPercent, acuUtilizationPercent, FleetCpuSource.PerformanceInsights);
        Assert.Equal(expected == AlertSeverityLevel.Critical, band == HealthSeverity.Critical);
        Assert.Equal(95d, ServerHealthThresholds.CpuCriticalPercent);
    }

    /// <summary>
    /// No capacity reading bands Unknown on the card and maps to Warning here rather than throwing or
    /// falling back to the raw CPU. Unreachable at the fire site (the gate never fires without a capacity
    /// percent), pinned so a future caller cannot make it reachable and get a red row off a percent-of-allocated.
    /// </summary>
    [Fact]
    public void GradePgCpuFire_WithNoCapacityReading_IsWarning_NeverGradedOnTheRawCpu()
    {
        Assert.Equal(AlertSeverityLevel.Warning, DarlingWorker.GradePgCpuFire(100.0, null));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.CpuSeverity(100.0, null, FleetCpuSource.PerformanceInsights));
    }

    /// <summary>
    /// The PostgreSQL deadlock fire is graded by the SAME grader #3660 gave the SQL Server twin, on the same
    /// one-hour count, so the boundaries are the band's: Warning through 19/hr, Critical at 20/hr on the
    /// shipped pair. And on the STORE's pair, not the shipped one — <see cref="DarlingAlertSettings.DeadlockRateThresholds"/>
    /// is what the arm reads, and it is the V120 knobs the PostgreSQL fleet card bands the same server on
    /// (#3638), so an operator who raised the card's Critical tier to 31 sees the alert grade Warning at 25.
    /// </summary>
    [Fact]
    public void PgDeadlockFire_IsTheSharedGrader_OnTheStoresTiers()
    {
        Assert.Equal(AlertSeverityLevel.Warning, AlertEngine.GradeDeadlockFire(1, DeadlockRateThresholds.Default));
        Assert.Equal(AlertSeverityLevel.Warning, AlertEngine.GradeDeadlockFire(5, DeadlockRateThresholds.Default));
        Assert.Equal(AlertSeverityLevel.Warning, AlertEngine.GradeDeadlockFire(19, DeadlockRateThresholds.Default));
        Assert.Equal(AlertSeverityLevel.Critical, AlertEngine.GradeDeadlockFire(20, DeadlockRateThresholds.Default));
        /* The read caps at 50 distinct reports; the cap is above the shipped Critical tier, so the shipped
           pair is fully reachable on this host. */
        Assert.Equal(AlertSeverityLevel.Critical, AlertEngine.GradeDeadlockFire(50, DeadlockRateThresholds.Default));
        Assert.True(50 > ServerHealthThresholds.DeadlockCriticalPerHourDefault);

        var config = new DarlingConfig();
        config.Alerts.DeadlockWarnPerHour = 7;
        config.Alerts.DeadlockCriticalPerHour = 31;
        var tiers = new DarlingAlertSettings(config).DeadlockRateThresholds;
        Assert.Equal(new DeadlockRateThresholds(7, 31), tiers);
        Assert.Equal(AlertSeverityLevel.Warning, AlertEngine.GradeDeadlockFire(25, tiers));
        Assert.Equal(AlertSeverityLevel.Critical, AlertEngine.GradeDeadlockFire(31, tiers));

        /* The window the arm counts over is the constant the grader divides by — the count IS the rate. */
        Assert.Equal(1, AlertEngine.RollingCountWindowHours);
    }

    /// <summary>
    /// Blocking Detected and Long-Running Query on the PostgreSQL host fire an explicit WARNING and never
    /// Critical: no measured bar exists for either (the constants' docs say which were considered), and an
    /// explicit Warning is what moves the row from "the name implies amber" to "the fire said amber".
    /// </summary>
    [Fact]
    public void PgBlockingAndLongRunningQuery_FireAnExplicitWarning_AndNeverCritical()
    {
        Assert.Equal(AlertSeverityLevel.Warning, DarlingWorker.PgBlockingFireSeverity);
        Assert.Equal(AlertSeverityLevel.Warning, DarlingWorker.PgLongRunningQueryFireSeverity);
    }

    /// <summary>
    /// Source pin: each of the four host arms passes its grade at its fire site and none still passes
    /// <c>Severity: null</c>. The arms are private and need a live store to drive, so the pin reads the
    /// fire sites out of the source, sliced per arm so a grade on one cannot vouch for another. The
    /// deliverer's #2090 fold — what carries an outcome-only tier into the persisted context — is pinned
    /// beside them, because every arm relies on it.
    /// </summary>
    [Fact]
    public void PgHostAlertArms_PassTheirGradeAtTheFireSite_NoneFiresSeverityNull()
    {
        var worker = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var cpu = Slice(worker, "private async Task EvaluatePgCpuAsync(", "private async Task EvaluatePgDeadlocksAsync(");
        Assert.Contains("var grade = GradePgCpuFire(reading.CpuPercent, reading.AcuUtilizationPercent);", cpu, StringComparison.Ordinal);
        Assert.Contains("Severity: grade,", cpu, StringComparison.Ordinal);
        Assert.DoesNotContain("Severity: null,\n", cpu, StringComparison.Ordinal);

        var deadlocks = Slice(worker, "private async Task EvaluatePgDeadlocksAsync(", "private async Task EvaluatePgBlockingAsync(");
        Assert.Contains("var rateTiers = alertSettings.DeadlockRateThresholds;", deadlocks, StringComparison.Ordinal);
        Assert.Contains("Severity: AlertEngine.GradeDeadlockFire(count, rateTiers),", deadlocks, StringComparison.Ordinal);
        /* The store's tiers, never the shipped pair — the operator-tier disagreement #3660 names. */
        Assert.DoesNotContain("DeadlockRateThresholds.Default", deadlocks, StringComparison.Ordinal);
        Assert.DoesNotContain("Severity: null,\n", deadlocks, StringComparison.Ordinal);

        var blocking = Slice(worker, "private async Task EvaluatePgBlockingAsync(", "private const int PgLongRunningQueryRecencyMinutes");
        Assert.Contains("Severity: PgBlockingFireSeverity,", blocking, StringComparison.Ordinal);
        Assert.DoesNotContain("Severity: null,\n", blocking, StringComparison.Ordinal);

        var longRunning = Slice(worker, "private async Task EvaluatePgLongRunningQueryAsync(", "internal static AlertIncident BuildPgLongRunningQueryIncident(");
        Assert.Contains("Severity: PgLongRunningQueryFireSeverity,", longRunning, StringComparison.Ordinal);
        Assert.DoesNotContain("Severity: null,\n", longRunning, StringComparison.Ordinal);

        /* Positive control for the DoesNotContain form: the exact code spelling IS present elsewhere in the
           tree (the SQL engine's two ungraded arms), so its absence above is a real absence. */
        var engine = RepoFile.ReadRepoFileLf("PerformanceMonitor.Alerting", "AlertEngine.cs");
        Assert.Contains("Severity: null,\n", engine, StringComparison.Ordinal);

        var deliverer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertDeliverer.cs");
        Assert.Contains("context ??= new AlertContext();", deliverer, StringComparison.Ordinal);
        Assert.Contains("context.SeverityOverride ??= outcome.Severity;", deliverer, StringComparison.Ordinal);
    }

    private static string Slice(string source, string startMarker, string endMarker)
    {
        var start = source.IndexOf(startMarker, StringComparison.Ordinal);
        Assert.True(start >= 0, $"marker not found: {startMarker}");
        var end = source.IndexOf(endMarker, start + startMarker.Length, StringComparison.Ordinal);
        Assert.True(end > start, $"end marker not found after start: {endMarker}");
        return source[start..end];
    }

    /// <summary>
    /// The whole point, measured at the row: a fire shaped exactly as the PostgreSQL host shapes it —
    /// Incidents-only context (deadlocks/blocking/LRQ) or NO context (CPU), the tier on the outcome alone —
    /// goes through the real <see cref="DarlingAlertDeliverer"/> and comes out as a history row whose
    /// <c>context_json</c> carries the tier, so <see cref="AlertHistoryRowSeverity"/> (the grids' and
    /// <c>get_alert_history</c>'s one decision, #3635) reads <c>fired</c>, not <c>metric_name</c>. The two
    /// cases chosen are the two where the grade CHANGES the colour: a Warning-graded Deadlocks Detected row
    /// is amber where the name alone says red; a Critical-graded High CPU row is red where the name says
    /// amber. No channel is configured (DarlingConfig defaults), so the row is the only output.
    /// </summary>
    [Fact]
    public async Task APgHostFire_PersistsTheTierItFiredAt_SoTheRowRendersItInsteadOfTheName()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        /* The deadlock arm's shape: three reports in the hour, Warning on the shipped tiers. */
        await deliverer.DeliverAsync(
            new AlertOutcome(
                "pg:7", "pg-target", "Deadlocks Detected", "3", "1",
                Context: new AlertContext
                {
                    Incidents = new List<AlertIncident>
                    {
                        DarlingWorker.BuildPgDeadlockIncident(DeadlockRow("hash-a")),
                        DarlingWorker.BuildPgDeadlockIncident(DeadlockRow("hash-b")),
                        DarlingWorker.BuildPgDeadlockIncident(DeadlockRow("hash-c")),
                    },
                },
                DetailText: null, NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: false,
                Severity: AlertEngine.GradeDeadlockFire(3, settings.DeadlockRateThresholds),
                ShortMessage: "3 deadlock(s) in the last hour"),
            TestContext.Current.CancellationToken);

        /* The CPU arm's shape: no context at all, the grade on the outcome. */
        await deliverer.DeliverAsync(
            new AlertOutcome(
                "pg:7", "pg-target", AlertEngine.CpuPersistenceMetric, "97%", "80%",
                Context: null, DetailText: "  Capacity: 97% of configured ACU ceiling\n  Threshold: 80%",
                NumericCurrentValue: 97, NumericThresholdValue: 80, Muted: false,
                Severity: DarlingWorker.GradePgCpuFire(100.0, 97.0),
                ShortMessage: "Capacity at 97%"),
            TestContext.Current.CancellationToken);

        Assert.Equal(2, history.Records.Count);

        var deadlockRow = history.Records[0];
        Assert.Equal("Deadlocks Detected", deadlockRow.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, AlertHistoryRowSeverity.FiredAt(deadlockRow.ContextJson));
        Assert.Equal(("warning", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe(deadlockRow.MetricName, deadlockRow.ContextJson));
        Assert.False(AlertHistoryRowSeverity.IsCritical(deadlockRow.MetricName, deadlockRow.ContextJson));
        Assert.True(AlertHistoryRowSeverity.IsWarning(deadlockRow.MetricName, deadlockRow.ContextJson));
        /* The by-name replay arm WOULD have said red — that is the colour this row no longer wears. */
        Assert.True(AlertMetricClassifier.IsCritical(deadlockRow.MetricName));
        /* The incidents rode through beside the tier: the grade was folded into the existing context, not
           written over it. */
        Assert.True(AlertContextSerializer.TryDeserialize(deadlockRow.ContextJson, out var rehydrated));
        Assert.Equal(3, rehydrated.Incidents!.Count);

        var cpuRow = history.Records[1];
        Assert.Equal("High CPU", cpuRow.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, AlertHistoryRowSeverity.FiredAt(cpuRow.ContextJson));
        Assert.Equal(("critical", AlertHistoryRowSeverity.SourceFired), AlertHistoryRowSeverity.Describe(cpuRow.MetricName, cpuRow.ContextJson));
        Assert.True(AlertHistoryRowSeverity.IsCritical(cpuRow.MetricName, cpuRow.ContextJson));
        Assert.False(AlertMetricClassifier.IsCritical(cpuRow.MetricName));
        /* A pre-#3653 PostgreSQL row of either name carried no tier and keeps its by-name colour. */
        Assert.Equal(("critical", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("Deadlocks Detected", "{\"Details\":[],\"Incidents\":[]}"));
        Assert.Equal(("warning", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe("High CPU", null));
    }
}
