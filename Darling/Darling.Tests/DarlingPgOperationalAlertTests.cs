/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2711: the pure, testable seams behind the Postgres Deadlocks/Blocking alerts —
/// <see cref="DarlingWorker.BuildPgDeadlockIncident"/>, <see cref="DarlingWorker.WorstPgBlockingChainPerRoot"/>,
/// and <see cref="DarlingWorker.BuildPgBlockingIncident"/>. The gating itself (fire/clear, edge-triggering,
/// immunity to re-firing on an unrefreshed data point) is <c>RollingCountAlertGate</c>'s job and is already
/// exhaustively pinned in <c>Lite.Tests/RollingCountAlertGateTests.cs</c>; these tests cover the mapping and
/// dedup logic layered on top of it that is genuinely new here.
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
            Databases: databases ?? new[] { "eden" },
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
        var unrelatedIncidentOne = BlockingRow(rootBackendId: 0, rootPid: 111, databases: new[] { "eden" });
        var unrelatedIncidentTwo = BlockingRow(rootBackendId: 0, rootPid: 222, databases: new[] { "sky" });
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
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 5, databases: new[] { "eden" }, rootQuery: "SELECT * FROM x"));

        Assert.Equal("root pid 42 blocking 5 session(s) in [eden]: SELECT * FROM x", incident.InvolvedObjects[0]);
        Assert.Equal("eden", incident.Database);
    }

    [Fact]
    public void BuildPgBlockingIncident_OmitsTrailingQueryClause_WhenRootQueryIsMissing()
    {
        var incident = DarlingWorker.BuildPgBlockingIncident(
            BlockingRow(rootBackendId: 1, rootPid: 42, totalVictims: 2, databases: new[] { "eden" }, rootQuery: null));

        Assert.Equal("root pid 42 blocking 2 session(s) in [eden]", incident.InvolvedObjects[0]);
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
}
