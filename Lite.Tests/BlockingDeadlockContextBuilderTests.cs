/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Identity pins for Phase-5 slice B: the LAST two alert-context builders — blocking and deadlock —
/// moved verbatim from this app's <c>MainWindow.AlertEngine.cs</c> async wrappers into the shared
/// <see cref="AlertContextBuilders"/> (Lite's grouped rendering is canonical), with the store fetch
/// behind <see cref="IAlertReadAdapter"/>. Every expectation here is derived from the
/// pre-extraction code, so a behavioral drift in the shared copy fails a pin: the #1140/#1141
/// chain grouping with occurrence counts and wait ranges, the 10-group cap with the "+N more"
/// trailer (gotqn's report), the deadlock victim rendering with the parsed process summary, the
/// XML attachments, and the excluded-database semantics (including
/// <see cref="AlertContextBuilders.IsDeadlockExcluded"/>'s all-processes rule). Also pins the
/// XE→DMV fallback merge (<see cref="BlockedProcessReportMerge"/>) both store adapters share, and
/// that Lite's row types ARE the shared alert row shapes (inheritance, no mapping copy).
/// </summary>
public class BlockingDeadlockContextBuilderTests
{
    private const string Server = "SQL2022";
    private static readonly List<string> NoExclusions = new();

    /* ---------------- blocking builder ---------------- */

    private static BlockedProcessAlertRow Blocked(
        string db = "StackOverflow", string obj = "", string blockedSql = "", string blockingSql = "",
        long waitMs = 5000, string lockMode = "", string xml = "", DateTime? eventTime = null) => new()
    {
        EventTime = eventTime ?? new DateTime(2026, 6, 19, 12, 0, 0),
        DatabaseName = db,
        ContentiousObject = obj,
        BlockedSqlText = blockedSql,
        BlockingSqlText = blockingSql,
        WaitTimeMs = waitMs,
        LockMode = lockMode,
        BlockedProcessReportXml = xml
    };

    [Fact]
    public void BuildBlockingContext_EmptyOrNull_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.BuildBlockingContext(Server, null, NoExclusions));
        Assert.Null(AlertContextBuilders.BuildBlockingContext(Server, new List<BlockedProcessAlertRow>(), NoExclusions));
    }

    [Fact]
    public void BuildBlockingContext_GroupsSamplesOfTheSameChain_WithCountAndWaitRange()
    {
        /* Two samples of the same chain (same contentious object) differing only by wait time —
           pre-extraction behavior: ONE group with the true occurrence count and wait range. */
        var rows = new List<BlockedProcessAlertRow>
        {
            Blocked(obj: "StackOverflow.dbo.Users", blockedSql: "UPDATE Users SET x = 1",
                blockingSql: "BEGIN TRAN UPDATE Users", waitMs: 5000, lockMode: "X"),
            Blocked(obj: "StackOverflow.dbo.Users", blockedSql: "UPDATE Users SET x = 1",
                blockingSql: "BEGIN TRAN UPDATE Users", waitMs: 12000, lockMode: "X")
        };

        var context = AlertContextBuilders.BuildBlockingContext(Server, rows, NoExclusions);

        Assert.NotNull(context);
        /* 1 grouped chain item + 1 appended incident item. */
        Assert.Equal(2, context!.Details.Count);
        Assert.Equal("Blocking chain (x2)", context.Details[0].Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Database", "StackOverflow"),
                ("Blocked Query", "UPDATE Users SET x = 1"),
                ("Blocking Query", "BEGIN TRAN UPDATE Users"),
                ("Wait Range", "5.0s-12.0s")
            },
            context.Details[0].Fields);

        /* #1140: object-identity fingerprint (stable across index rebuilds). */
        var incident = Assert.Single(context.Incidents!);
        Assert.Equal(2, incident.OccurrenceCount);
        var expected = AlertFingerprint.ForObjects(Server, AlertFingerprint.Blocking, new[] { "StackOverflow.dbo.Users" });
        Assert.Equal(expected!.DedupKey, incident.DedupKey);
    }

    [Fact]
    public void BuildBlockingContext_SingleSample_HeadingHasNoCount()
    {
        var context = AlertContextBuilders.BuildBlockingContext(
            Server, new List<BlockedProcessAlertRow> { Blocked(obj: "db.dbo.T") }, NoExclusions);

        Assert.Equal("Blocking chain", context!.Details[0].Heading);
    }

    [Fact]
    public void BuildBlockingContext_CapsAtTenGroups_WithTrueTotalTrailer()
    {
        /* 12 distinct chains -> 10 shown + "+2 more" trailer (gotqn's report: never silently drop). */
        var rows = new List<BlockedProcessAlertRow>();
        for (int i = 0; i < 12; i++)
            rows.Add(Blocked(obj: $"db.dbo.Table{i}", waitMs: 1000 + i));

        var context = AlertContextBuilders.BuildBlockingContext(Server, rows, NoExclusions);

        Assert.NotNull(context);
        /* 10 chain items + 1 trailer + 10 appended incident items. */
        Assert.Equal(21, context!.Details.Count);
        Assert.Equal("+2 more distinct blocking incident(s)", context.Details[10].Heading);
        Assert.Empty(context.Details[10].Fields);
        Assert.Equal(10, context.Incidents!.Count);
    }

    [Fact]
    public void BuildBlockingContext_AttachesFirstReportXml()
    {
        var rows = new List<BlockedProcessAlertRow>
        {
            Blocked(obj: "db.dbo.A"),                                       /* DMV-style row: no XML */
            Blocked(obj: "db.dbo.B", xml: "<blocked-process-report first/>"),
            Blocked(obj: "db.dbo.C", xml: "<blocked-process-report second/>")
        };

        var context = AlertContextBuilders.BuildBlockingContext(Server, rows, NoExclusions);

        Assert.Equal("<blocked-process-report first/>", context!.AttachmentXml);
        Assert.Equal("blocked_process_report.xml", context.AttachmentFileName);
    }

    [Fact]
    public void BuildBlockingContext_NoReportXmlAnywhere_NoAttachment()
    {
        var context = AlertContextBuilders.BuildBlockingContext(
            Server, new List<BlockedProcessAlertRow> { Blocked(obj: "db.dbo.A") }, NoExclusions);

        Assert.Null(context!.AttachmentXml);
        Assert.Null(context.AttachmentFileName);
    }

    [Fact]
    public void BuildBlockingContext_ExcludedDatabases_DropRows_CaseInsensitive_NoDbAlwaysPasses()
    {
        var rows = new List<BlockedProcessAlertRow>
        {
            Blocked(db: "ExcludedDb", obj: "ExcludedDb.dbo.T"),
            Blocked(db: "", obj: "unknown.dbo.T")
        };

        var context = AlertContextBuilders.BuildBlockingContext(Server, rows, new List<string> { "excludeddb" });

        Assert.NotNull(context);
        /* Only the database-less row survives (1 chain + 1 incident item). */
        Assert.Equal(2, context!.Details.Count);

        /* All rows excluded -> null, exactly like the pre-extraction early return. */
        Assert.Null(AlertContextBuilders.BuildBlockingContext(
            Server, new List<BlockedProcessAlertRow> { Blocked(db: "ExcludedDb") }, new List<string> { "ExcludedDb" }));
    }

    /* ---------------- deadlock builder ---------------- */

    private const string DeadlockGraph = @"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>
    <process id=""process1"" spid=""55"" currentdbname=""StackOverflow""><inputbuf>UPDATE Users SET Reputation = 1</inputbuf></process>
    <process id=""process2"" spid=""60"" currentdbname=""StackOverflow""><inputbuf>UPDATE Badges SET Name = 'x'</inputbuf></process>
  </process-list>
  <resource-list>
    <keylock objectname=""StackOverflow.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock>
  </resource-list>
</deadlock>";

    private static DeadlockAlertRow Deadlock(string xml = DeadlockGraph, string victim = "process1", string victimSql = "UPDATE Users SET Reputation = 1") =>
        new() { VictimProcessId = victim, VictimSqlText = victimSql, DeadlockGraphXml = xml };

    [Fact]
    public void BuildDeadlockContext_EmptyOrNull_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.BuildDeadlockContext(Server, null, NoExclusions));
        Assert.Null(AlertContextBuilders.BuildDeadlockContext(Server, new List<DeadlockAlertRow>(), NoExclusions));
    }

    [Fact]
    public void BuildDeadlockContext_FingerprintedDeadlock_IsOneSelfContainedItem()
    {
        var context = AlertContextBuilders.BuildDeadlockContext(
            Server, new List<DeadlockAlertRow> { Deadlock() }, NoExclusions);

        Assert.NotNull(context);
        /* #2108: a fingerprinted deadlock renders as ONE self-contained item — its Database
           (#2109), forensic fields, and dedup metadata together — no separate victim item whose
           association with the fingerprint a multi-incident card would lose. */
        var item = Assert.Single(context!.Details);
        Assert.Equal("Deadlock", item.Heading);
        var expected = AlertFingerprint.ForObjects(Server, AlertFingerprint.Deadlock, new[] { "StackOverflow.dbo.Users" });
        Assert.Equal(
            new List<(string, string)>
            {
                ("Database", "StackOverflow"),
                ("Victim SQL", "UPDATE Users SET Reputation = 1"),
                ("Processes", "SPID 55 (victim) vs SPID 60"),
                ("Dedup Key", expected!.DedupKey),
                ("Involved Objects", "StackOverflow.dbo.Users")
            },
            item.Fields);

        Assert.Equal(DeadlockGraph, context.AttachmentXml);
        Assert.Equal("deadlock_graph.xml", context.AttachmentFileName);

        /* #1140: involved-object fingerprint from the graph's resource list. */
        var incident = Assert.Single(context.Incidents!);
        Assert.Equal(expected.DedupKey, incident.DedupKey);
    }

    [Fact]
    public void BuildDeadlockContext_RecurrencesCollapse_ToOneItemWithTheCount()
    {
        /* 4 deadlocks over the same object set: ONE self-contained incident item carrying the
           occurrence count across ALL of them — the #1140 collapse, now without the three raw
           victim repeats beside it (#2108). */
        var rows = new List<DeadlockAlertRow> { Deadlock(), Deadlock(), Deadlock(), Deadlock() };

        var context = AlertContextBuilders.BuildDeadlockContext(Server, rows, NoExclusions);

        Assert.NotNull(context);
        var item = Assert.Single(context!.Details);
        Assert.Equal("Deadlock", item.Heading);
        Assert.Contains(("Occurrences", "4"), item.Fields);
        var incident = Assert.Single(context.Incidents!);
        Assert.Equal(4, incident.OccurrenceCount);
    }

    [Fact]
    public void BuildDeadlockContext_DistinctFingerprints_EachSelfContained_AndIndexed()
    {
        /* #2108's core: two different deadlocks on one alert must read as two labeled units, each
           carrying its OWN victim + database + dedup metadata — the victim→fingerprint association
           the flat list lost. */
        var otherGraph = DeadlockGraph
            .Replace("StackOverflow.dbo.Users", "OtherDb.dbo.T1")
            .Replace(@"currentdbname=""StackOverflow""", @"currentdbname=""OtherDb""");
        var rows = new List<DeadlockAlertRow>
        {
            Deadlock(),
            Deadlock(xml: otherGraph, victimSql: "UPDATE T1 SET x = 1")
        };

        var context = AlertContextBuilders.BuildDeadlockContext(Server, rows, NoExclusions);

        Assert.NotNull(context);
        Assert.Equal(2, context!.Details.Count);
        Assert.Equal("Deadlock 1 of 2", context.Details[0].Heading);
        Assert.Equal("Deadlock 2 of 2", context.Details[1].Heading);
        Assert.Contains(("Database", "StackOverflow"), context.Details[0].Fields);
        Assert.Contains(("Victim SQL", "UPDATE Users SET Reputation = 1"), context.Details[0].Fields);
        Assert.Contains(("Database", "OtherDb"), context.Details[1].Fields);
        Assert.Contains(("Victim SQL", "UPDATE T1 SET x = 1"), context.Details[1].Fields);
        Assert.Equal(2, context.Incidents!.Count);
    }

    [Fact]
    public void BuildDeadlockContext_UnfingerprintableDeadlock_KeepsTheStandaloneVictimItem()
    {
        /* A graph with no parseable lock objects has no fingerprint identity — under incident-only
           rendering it would vanish, so it keeps the classic victim item (#1140's "the builder
           still displays them", scoped to exactly these). */
        var noObjects = DeadlockGraph.Replace(
            @"<keylock objectname=""StackOverflow.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock>",
            "");

        var context = AlertContextBuilders.BuildDeadlockContext(
            Server, new List<DeadlockAlertRow> { Deadlock(xml: noObjects) }, NoExclusions);

        Assert.NotNull(context);
        var item = Assert.Single(context!.Details);
        Assert.Equal("Deadlock Victim", item.Heading);
        /* #2109: the Database fact comes from the processes' currentdbname, so even the
           unfingerprintable form names where it happened. */
        Assert.Contains(("Database", "StackOverflow"), item.Fields);
        Assert.Null(context.Incidents);
    }

    [Fact]
    public void BuildDeadlockContext_AttachmentComesFromFirstRowWithXml()
    {
        var noXml = new DeadlockAlertRow { VictimSqlText = "SELECT 1", DeadlockGraphXml = "" };
        var context = AlertContextBuilders.BuildDeadlockContext(
            Server, new List<DeadlockAlertRow> { noXml, Deadlock() }, NoExclusions);

        Assert.Equal(DeadlockGraph, context!.AttachmentXml);
        Assert.Equal("deadlock_graph.xml", context.AttachmentFileName);
    }

    [Fact]
    public void IsDeadlockExcluded_TrueOnlyWhenEveryProcessDatabaseIsExcluded()
    {
        var excluded = new List<string> { "stackoverflow" };

        /* All processes in StackOverflow (case-insensitive) -> excluded. */
        Assert.True(AlertContextBuilders.IsDeadlockExcluded(Deadlock(), excluded));

        /* One process in another database -> NOT excluded. */
        var mixed = DeadlockGraph.Replace(@"spid=""60"" currentdbname=""StackOverflow""", @"spid=""60"" currentdbname=""OtherDb""");
        Assert.False(AlertContextBuilders.IsDeadlockExcluded(Deadlock(xml: mixed), excluded));

        /* No graph, unparseable graph, or no currentdbname attributes -> never excluded. */
        Assert.False(AlertContextBuilders.IsDeadlockExcluded(Deadlock(xml: ""), excluded));
        Assert.False(AlertContextBuilders.IsDeadlockExcluded(Deadlock(xml: "<not-xml"), excluded));
        Assert.False(AlertContextBuilders.IsDeadlockExcluded(Deadlock(xml: "<deadlock><process-list><process id=\"p1\" spid=\"5\"/></process-list></deadlock>"), excluded));
    }

    [Fact]
    public void BuildDeadlockContext_AllDeadlocksExcluded_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.BuildDeadlockContext(
            Server, new List<DeadlockAlertRow> { Deadlock() }, new List<string> { "StackOverflow" }));
    }

    [Fact]
    public void DeadlockGraphSummary_UnparseableOrEmpty_ReturnsEmpty()
    {
        Assert.Equal("", DeadlockGraphSummary.Summarize("", "process1"));
        Assert.Equal("", DeadlockGraphSummary.Summarize(null, "process1"));
        Assert.Equal("", DeadlockGraphSummary.Summarize("<not-xml", "process1"));
    }

    /* ---------------- the shared XE→DMV fallback merge ---------------- */

    [Fact]
    public void BlockedProcessMerge_DmvRowDropped_WhenBprCoversSameSpidPairInSameMinute()
    {
        var bpr = Blocked(obj: "db.dbo.T", xml: "<bpr/>", eventTime: new DateTime(2026, 6, 19, 12, 0, 10));
        bpr.BlockedSpid = 55; bpr.BlockingSpid = 66;
        var dmvDupe = Blocked(obj: "db.dbo.T", eventTime: new DateTime(2026, 6, 19, 12, 0, 40));
        dmvDupe.BlockedSpid = 55; dmvDupe.BlockingSpid = 66; dmvDupe.Source = BlockedProcessAlertRow.DmvSnapshotSource;
        var dmvNew = Blocked(obj: "db.dbo.U", eventTime: new DateTime(2026, 6, 19, 12, 2, 0));
        dmvNew.BlockedSpid = 77; dmvNew.BlockingSpid = 88; dmvNew.Source = BlockedProcessAlertRow.DmvSnapshotSource;

        var items = new List<BlockedProcessAlertRow> { bpr };
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, new List<BlockedProcessAlertRow> { dmvDupe, dmvNew });

        /* The same-minute same-SPID-pair DMV row is dropped (BPR preferred); the other appended;
           result re-sorted newest first. */
        Assert.Equal(2, items.Count);
        Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, items[0].Source); /* 12:02 DMV row */
        Assert.Equal(BlockedProcessAlertRow.XeReportSource, items[1].Source);    /* 12:00 BPR row */
    }

    [Fact]
    public void BlockedProcessMerge_RecapsToCap_NewestFirst_BprWinsTies()
    {
        var items = new List<BlockedProcessAlertRow>();
        var dmv = new List<BlockedProcessAlertRow>();
        var t = new DateTime(2026, 6, 19, 12, 0, 0);
        for (int i = 0; i < 3; i++)
        {
            var b = Blocked(eventTime: t); b.BlockedSpid = i; b.BlockingSpid = 100; items.Add(b);
            /* Same event time, different SPID pair -> not deduped; competes on the cap. */
            var d = Blocked(eventTime: t); d.BlockedSpid = i + 50; d.BlockingSpid = 200; d.Source = BlockedProcessAlertRow.DmvSnapshotSource; dmv.Add(d);
        }

        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmv, cap: 4);

        /* Cap re-applied after the merge; the stable sort keeps all BPR rows (added first) ahead
           of DMV rows at the same event time — so the one surviving DMV row is last. */
        Assert.Equal(4, items.Count);
        Assert.Equal(3, items.Count(i => i.Source == BlockedProcessAlertRow.XeReportSource));
        Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, items[3].Source);
    }

    /* ---------------- slice B seams: Lite's rows ARE the shared shapes ---------------- */

    [Fact]
    public void LiteRowTypes_DeriveFromSharedAlertRows_SoNoMappingCopyCanDrift()
    {
        Assert.True(typeof(BlockedProcessReportRow).IsSubclassOf(typeof(BlockedProcessAlertRow)));
        Assert.True(typeof(DeadlockRow).IsSubclassOf(typeof(DeadlockAlertRow)));
        Assert.True(typeof(IAlertReadAdapter).IsAssignableFrom(typeof(LiteAlertReadAdapter)));
    }

    [Fact]
    public void LiteAlertReadAdapter_ExposesAllSevenCollectedFeeds()
    {
        /* The engine (slice D) consumes exactly this surface; failed jobs stay out (live msdb via
           FailedJobsQuery — not a collected feed). */
        Assert.Equal(typeof(Task<List<BlockedProcessAlertRow>>),
            typeof(LiteAlertReadAdapter).GetMethod("GetRecentBlockedProcessReportsAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<DeadlockAlertRow>>),
            typeof(LiteAlertReadAdapter).GetMethod("GetRecentDeadlocksAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<PoisonWaitDelta>>),
            typeof(LiteAlertReadAdapter).GetMethod("GetPoisonWaitDeltasAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<LongRunningQueryInfo>>),
            typeof(LiteAlertReadAdapter).GetMethod("GetLongRunningQueriesAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<VolumeFreeSpaceInfo>>),
            typeof(LiteAlertReadAdapter).GetMethod("GetVolumeFreeSpaceAsync")!.ReturnType);
        Assert.Equal(typeof(Task<TempDbSpaceInfo?>),
            typeof(LiteAlertReadAdapter).GetMethod("GetTempDbSpaceAsync")!.ReturnType);
        /* #1812: the jobs feed carries its evidence quality — a stale snapshot must be
           distinguishable from genuinely-no-anomalous-jobs, or the engine either re-fires a
           historical run forever or fabricates a resolution. */
        Assert.Equal(typeof(Task<AnomalousJobsResult>),
            typeof(LiteAlertReadAdapter).GetMethod("GetAnomalousJobsAsync")!.ReturnType);
        Assert.Null(typeof(IAlertReadAdapter).GetMethod("GetRecentlyFailedJobsAsync"));
    }

    [Fact]
    public async Task LiteBuilderWrappers_PassLiteRowsCovariantly_IntoTheSharedBuilders()
    {
        /* Lite's store returns List<BlockedProcessReportRow>/List<DeadlockRow>; IReadOnlyList<T>
           covariance feeds them to the shared builders with zero mapping — pin that the derived
           lists ARE accepted and render identically to base-typed rows. */
        var liteRows = new List<BlockedProcessReportRow>
        {
            new() { DatabaseName = "db", ContentiousObject = "db.dbo.T", WaitTimeMs = 1000, BlockedProcessReportXml = "<bpr/>" }
        };
        var context = AlertContextBuilders.BuildBlockingContext(Server, liteRows, NoExclusions);
        Assert.NotNull(context);
        Assert.Equal("<bpr/>", context!.AttachmentXml);

        var liteDeadlocks = new List<DeadlockRow> { new() { VictimProcessId = "process1", VictimSqlText = "SELECT 1", DeadlockGraphXml = DeadlockGraph } };
        var deadlockContext = AlertContextBuilders.BuildDeadlockContext(Server, liteDeadlocks, NoExclusions);
        Assert.NotNull(deadlockContext);
        Assert.Equal("SPID 55 (victim) vs SPID 60", liteDeadlocks[0].ProcessSummary);

        await Task.CompletedTask;
    }
}
