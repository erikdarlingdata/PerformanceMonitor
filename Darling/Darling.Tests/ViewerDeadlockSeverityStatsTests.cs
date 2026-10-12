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
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Blocking Stats sub-tab's deadlock-SEVERITY aggregate — the on-the-fly Darling equivalent of the
/// Dashboard's <c>collect.blocking_deadlock_stats</c> victim_count / total_deadlock_wait_time_ms, computed by
/// parsing the stored <c>deadlock_graph_xml</c> (no collector, no migration). Two halves: (1) the read SQL,
/// which hands back raw (deadlock_time, xml) rows over the SAME <c>v_deadlocks</c> / <c>collection_time</c>
/// window as the deadlock COUNT trend so the two reconcile in period; (2) the pure parse→aggregate logic that
/// buckets by minute and sums the shared <see cref="DeadlockGraphParser"/>'s per-process IsVictim / WaitTimeMs.
/// No live Postgres (the live round-trip is the gated sibling class below).
/// </summary>
public sealed class ViewerDeadlockSeverityStatsSqlTests
{
    [Fact]
    public void DeadlockSeverityGraphsSql_ReadsGraphAndTime_FromDeadlocksView()
    {
        /* The read pulls only what the C# aggregate needs: the bucket key (deadlock_time) and the payload
           the shared parser reads (deadlock_graph_xml) — nothing is aggregated in SQL. */
        Assert.Contains("FROM v_deadlocks", ViewerDataService.DeadlockSeverityGraphsSql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time", ViewerDataService.DeadlockSeverityGraphsSql, StringComparison.Ordinal);
        Assert.Contains("deadlock_graph_xml", ViewerDataService.DeadlockSeverityGraphsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeadlockSeverityGraphsSql_WindowsOnDeadlockTime_ReconcilesWithCountTrend()
    {
        /* Reconciliation: the deadlock COUNT trend and this read both window on deadlock_time (when it
           happened), so the count on the summary strip and the victim/wait aggregate draw from the identical
           row set. collection_time is only the lower-bound floor ($4) - never an upper bound, so a deadlock
           collected after the window end still counts. */
        foreach (var sql in new[] { ViewerDataService.DeadlockSeverityGraphsSql, ViewerDataService.DeadlockTrendSql })
        {
            Assert.Contains("deadlock_time >= $2", sql, StringComparison.Ordinal);
            Assert.Contains("deadlock_time <= $3", sql, StringComparison.Ordinal);
            Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("collection_time <=", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DeadlockSeverityGraphsSql_HasNoCap_SoItCannotDropRowsTheCountTrendKeeps()
    {
        /* No LIMIT: deadlocks are rare (low volume), and any cap would silently drop rows the un-capped count
           trend keeps, breaking reconciliation. If a cap is ever needed it must be surfaced, not silent. */
        Assert.DoesNotContain("LIMIT", ViewerDataService.DeadlockSeverityGraphsSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DeadlockSeverityGraphsSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        var sql = ViewerDataService.DeadlockSeverityGraphsSql;
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        Assert.Contains("$3", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeadlockSeverityGraphXml_IsAnOriginalDeadlocksColumn_SoTheViewExposesIt()
    {
        /* Unlike victim_query_plan_xml (V7, why RecentDeadlocksSql reads the base table), deadlock_graph_xml is
           an original deadlocks column, so the SELECT * view (v_deadlocks) projects it. The generated DDL pins
           that the column exists; the gated live test proves the view actually returns it. */
        var ddl = PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance);
        Assert.Contains("deadlock_graph_xml", ddl, StringComparison.Ordinal);
        Assert.Contains("deadlock_time", ddl, StringComparison.Ordinal);
    }
}

/// <summary>
/// Pins the pure parse→aggregate core (<see cref="ViewerDataService.AggregateDeadlockSeverity"/>): buckets the
/// deadlock graphs by minute and sums the shared parser's per-process IsVictim / WaitTimeMs into victim_count
/// and total / max / avg deadlock wait — the Dashboard analyzer's exact semantics
/// (26_blocking_deadlock_analyzer.sql: victim_count = SUM(IsVictim); total = SUM(every process's wait)).
/// Driven by the REAL sql2025 deadlock fixtures (documented victim counts) plus synthetic graphs with known
/// wait times for exact total/max/avg. No database, no WPF.
/// </summary>
public sealed class DeadlockSeverityAggregationTests
{
    /// <summary>Builds a bare &lt;deadlock&gt; graph (what the store keeps) from known processes, so a test can
    /// assert exact victim_count / total / max / avg without depending on a captured fixture's wait times.</summary>
    private static string BuildGraph(params (string Id, int Spid, long WaitTime, bool IsVictim)[] processes)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list>");
        foreach (var p in processes.Where(p => p.IsVictim))
            sb.Append($"<victimProcess id=\"{p.Id}\"/>");
        sb.Append("</victim-list><process-list>");
        foreach (var p in processes)
            sb.Append($"<process id=\"{p.Id}\" spid=\"{p.Spid}\" waittime=\"{p.WaitTime}\"><inputbuf>x</inputbuf></process>");
        sb.Append("</process-list></deadlock>");
        return sb.ToString();
    }

    private static readonly DateTime MinuteA = new(2026, 6, 21, 6, 51, 0, DateTimeKind.Unspecified);
    private static readonly DateTime MinuteB = new(2026, 6, 21, 6, 53, 0, DateTimeKind.Unspecified);

    [Fact]
    public void SingleGraph_SumsVictimsAndWaits_AvgIsProcessWeighted()
    {
        var graph = BuildGraph(
            ("p1", 55, 1000, true),
            ("p2", 66, 3000, false));

        var points = ViewerDataService.AggregateDeadlockSeverity(
            new (DateTime?, string?)[] { (MinuteA.AddSeconds(30), graph) });

        var point = Assert.Single(points);
        Assert.Equal(MinuteA, point.Time);               // bucketed to the minute
        Assert.Equal(1, point.VictimCount);              // p1 only
        Assert.Equal(4000, point.TotalWaitMs);           // 1000 + 3000
        Assert.Equal(3000, point.MaxWaitMs);             // max process wait
        Assert.Equal(2000.0, point.AvgWaitMs, precision: 3); // 4000 / 2 processes
    }

    [Fact]
    public void MultipleGraphs_SameMinute_CollapseIntoOneBucket()
    {
        /* Two deadlocks in the same minute → one bucket summing victims + waits across BOTH graphs. */
        var graphs = new (DateTime?, string?)[]
        {
            (MinuteA.AddSeconds(10), BuildGraph(("a1", 55, 1000, true), ("a2", 66, 3000, false))),
            (MinuteA.AddSeconds(50), BuildGraph(("b1", 77, 2000, true))),
        };

        var point = Assert.Single(ViewerDataService.AggregateDeadlockSeverity(graphs));
        Assert.Equal(MinuteA, point.Time);
        Assert.Equal(2, point.VictimCount);              // one victim per graph
        Assert.Equal(6000, point.TotalWaitMs);           // 1000 + 3000 + 2000
        Assert.Equal(3000, point.MaxWaitMs);
        Assert.Equal(2000.0, point.AvgWaitMs, precision: 3); // 6000 / 3 processes
    }

    [Fact]
    public void MultipleGraphs_DifferentMinutes_ProduceOrderedBuckets()
    {
        /* Fed out of order; the aggregate must return buckets ordered by time (parity with the SQL ORDER BY). */
        var graphs = new (DateTime?, string?)[]
        {
            (MinuteB.AddSeconds(5), BuildGraph(("b1", 77, 500, true), ("b2", 88, 700, false))),
            (MinuteA.AddSeconds(5), BuildGraph(("a1", 55, 1000, true))),
        };

        var points = ViewerDataService.AggregateDeadlockSeverity(graphs);
        Assert.Equal(2, points.Count);
        Assert.Equal(MinuteA, points[0].Time);
        Assert.Equal(MinuteB, points[1].Time);
        Assert.Equal(1, points[0].VictimCount);
        Assert.Equal(1200, points[1].TotalWaitMs);       // 500 + 700
        Assert.Equal(700, points[1].MaxWaitMs);
    }

    [Fact]
    public void NullDeadlockTime_IsSkipped_CannotBeBucketed()
    {
        var graphs = new (DateTime?, string?)[]
        {
            (null, BuildGraph(("p1", 55, 1000, true))),
            (MinuteA.AddSeconds(5), BuildGraph(("p2", 66, 2000, true))),
        };

        var point = Assert.Single(ViewerDataService.AggregateDeadlockSeverity(graphs));
        Assert.Equal(MinuteA, point.Time);
        Assert.Equal(2000, point.TotalWaitMs);           // only the placeable graph
    }

    [Fact]
    public void EmptyOrMalformedXml_ContributesNothing()
    {
        var graphs = new (DateTime?, string?)[]
        {
            (MinuteA.AddSeconds(1), null),
            (MinuteA.AddSeconds(2), ""),
            (MinuteA.AddSeconds(3), "<not-a-deadlock/>"),
            (MinuteA.AddSeconds(4), "<deadlock><oops"),   // unparseable
        };

        Assert.Empty(ViewerDataService.AggregateDeadlockSeverity(graphs));
    }

    [Fact]
    public void Real2ProcFixture_OneVictim_ExactWaitSums()
    {
        /* deadlock_2proc_real_sql2025.xml: spid 103 (VICTIM, waittime 630) + spid 85 (waittime 626). */
        var xml = DeadlockGraphParserTests.LoadFixture("deadlock_2proc_real_sql2025.xml");
        var point = Assert.Single(ViewerDataService.AggregateDeadlockSeverity(
            new (DateTime?, string?)[] { (MinuteA.AddSeconds(30), xml) }));

        Assert.Equal(1, point.VictimCount);
        Assert.Equal(1256, point.TotalWaitMs);           // 630 + 626
        Assert.Equal(630, point.MaxWaitMs);
        Assert.Equal(628.0, point.AvgWaitMs, precision: 3); // 1256 / 2
    }

    [Theory]
    [InlineData("deadlock_2proc_real_sql2025.xml", 1)]
    [InlineData("deadlock_5proc_real_sql2025.xml", 2)]
    [InlineData("deadlock_8proc_multivictim_real_sql2025.xml", 4)]
    public void RealFixtures_VictimCountAndWaitSum_MatchTheSharedParser(string fixture, int expectedVictims)
    {
        /* The documented victim counts (README) are absolute ground truth; the wait sums are cross-checked
           against the shared parser so this pins that the aggregate faithfully SUMS what the parser produces
           (the parser itself is covered by DeadlockGraphParserTests). */
        var xml = DeadlockGraphParserTests.LoadFixture(fixture);
        var model = DeadlockGraphParser.Parse(xml);
        long expectedTotal = model.Processes.Sum(p => p.WaitTimeMs);
        long expectedMax = model.Processes.Max(p => p.WaitTimeMs);

        var point = Assert.Single(ViewerDataService.AggregateDeadlockSeverity(
            new (DateTime?, string?)[] { (MinuteA.AddSeconds(15), xml) }));

        Assert.Equal(expectedVictims, point.VictimCount);
        Assert.Equal(expectedTotal, point.TotalWaitMs);
        Assert.Equal(expectedMax, point.MaxWaitMs);
    }

    [Fact]
    public void BucketVictimCount_NeverExceedsProcesses_ButAtLeastOnePerRealDeadlock()
    {
        /* Reconciliation sanity: each real deadlock contributes >= 1 victim to its bucket, so the summed
           victim_count is a coherent companion to the deadlock COUNT drawn from the same window. */
        var xml = DeadlockGraphParserTests.LoadFixture("deadlock_5proc_real_sql2025.xml");
        var point = Assert.Single(ViewerDataService.AggregateDeadlockSeverity(
            new (DateTime?, string?)[] { (MinuteA.AddSeconds(15), xml) }));

        Assert.True(point.VictimCount >= 1);
        Assert.True(point.VictimCount <= DeadlockGraphParser.Parse(xml).Processes.Count);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the deadlock-severity aggregate: proves the read executes
/// against the real migrated schema (in particular that <c>v_deadlocks</c> exposes <c>deadlock_graph_xml</c>),
/// windows on collection_time, and that the C# parse→bucket produces the right per-minute victim / total /
/// max / avg. Shares the serialized "live-postgres" collection; uses a negative sentinel server_id + finally
/// cleanup so it can't race or leak.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerDeadlockSeverityLivePostgresTests
{
    private const int SeverityServerId = -972101;
    private const string ServerName = "viewer-deadlock-severity-e2e";

    [Fact]
    public async Task DeadlockSeverity_BucketsPerMinute_VictimTotalMaxAvg_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock-severity-stats test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, SeverityServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var baseTime = TruncateToMinute(DateTime.UtcNow.AddMinutes(-10));
            var minuteA = baseTime;
            var minuteB = baseTime.AddMinutes(3);

            /* minute A: two deadlocks (2 victims total, waits 1000+3000 then 2000 → total 6000, max 3000,
               3 processes → avg 2000); minute B: one deadlock (1 victim, waits 500+700 → total 1200, max 700,
               2 processes → avg 600). */
            await InsertDeadlockAsync(connection, minuteA, minuteA.AddSeconds(10),
                BuildGraph(("a1", 55, 1000, true), ("a2", 66, 3000, false)));
            await InsertDeadlockAsync(connection, minuteA, minuteA.AddSeconds(40),
                BuildGraph(("b1", 77, 2000, true)));
            await InsertDeadlockAsync(connection, minuteB, minuteB.AddSeconds(10),
                BuildGraph(("c1", 88, 500, true), ("c2", 99, 700, false)));

            var points = await viewer.GetDeadlockSeverityStatsAsync(
                SeverityServerId, baseTime.AddMinutes(-1), baseTime.AddMinutes(10));

            Assert.Equal(2, points.Count);
            Assert.True(points[0].Time < points[1].Time);

            Assert.Equal(2, points[0].VictimCount);
            Assert.Equal(6000, points[0].TotalWaitMs);
            Assert.Equal(3000, points[0].MaxWaitMs);
            Assert.Equal(2000.0, points[0].AvgWaitMs, precision: 3);

            Assert.Equal(1, points[1].VictimCount);
            Assert.Equal(1200, points[1].TotalWaitMs);
            Assert.Equal(700, points[1].MaxWaitMs);
            Assert.Equal(600.0, points[1].AvgWaitMs, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, SeverityServerId, cleanupCt));
        }
    }

    private static string BuildGraph(params (string Id, int Spid, long WaitTime, bool IsVictim)[] processes)
    {
        var sb = new StringBuilder();
        sb.Append("<deadlock><victim-list>");
        foreach (var p in processes.Where(p => p.IsVictim))
            sb.Append($"<victimProcess id=\"{p.Id}\"/>");
        sb.Append("</victim-list><process-list>");
        foreach (var p in processes)
            sb.Append($"<process id=\"{p.Id}\" spid=\"{p.Spid}\" waittime=\"{p.WaitTime}\"><inputbuf>x</inputbuf></process>");
        sb.Append("</process-list></deadlock>");
        return sb.ToString();
    }

    private static async Task InsertDeadlockAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, DateTime deadlockTimeUtc, string graphXml)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time,
     victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(SeverityServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("a1");
        command.Parameters.AddWithValue("UPDATE t SET c = 1");
        command.Parameters.AddWithValue(graphXml);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToMinute(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM deadlocks WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
