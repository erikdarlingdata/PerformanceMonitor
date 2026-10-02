/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// "Last 4 hours" counts the blocked-process reports and deadlocks that HAPPENED in the last 4 hours, in every
/// Darling read, as Lite does. One server is seeded with three rows per table:
/// <list type="bullet">
/// <item><b>a</b> — happened 2 h BEFORE the window, collected inside it (a late pick-up);</item>
/// <item><b>b</b> — happened and was collected inside it;</item>
/// <item><b>c</b> — happened inside it, collected 30 minutes AFTER its end (the catch-up after an outage).</item>
/// </list>
/// Every windowed read returns b and c and not a. The alert reads are the opposite on purpose: they are a
/// delivery cursor over "rows collected since the last sweep", so they still return a.
/// </summary>
[Collection("live-postgres")]
public sealed class EventTimeWindowReadsLiveTests
{
    private const string ServerName = "darling-event-time-reads-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const int SpidA = 111;
    private const int SpidB = 222;
    private const int SpidC = 333;

    private static string Graph(string id) =>
        $"<deadlock><victim-list><victimProcess id=\"{id}\"/></victim-list><process-list>" +
        $"<process id=\"{id}\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process></process-list></deadlock>";

    private sealed record Seeded(DateTime Start, DateTime End)
    {
        /// <summary>Where b and c happened (a never belongs to the window).</summary>
        public DateTime EventB => Start.AddHours(1);
        public DateTime EventC => End.AddMinutes(-10);

        public DateTime[] Hours => [Hour(EventB), Hour(EventC)];
        public DateTime[] Minutes => [Minute(EventB), Minute(EventC)];

        private static DateTime Hour(DateTime t) => new(t.Year, t.Month, t.Day, t.Hour, 0, 0, DateTimeKind.Unspecified);
        private static DateTime Minute(DateTime t) => new(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, DateTimeKind.Unspecified);
    }

    private static async Task<Seeded> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var end = DarlingMcpTestData.TruncateToSeconds(DarlingMcpTestData.Naive(DateTime.UtcNow));
        var start = end.AddHours(-4);

        var rows = new[]
        {
            (Spid: SpidA, Event: start.AddHours(-2), Collected: start.AddHours(1)),
            (Spid: SpidB, Event: start.AddHours(1), Collected: start.AddHours(1).AddMinutes(1)),
            (Spid: SpidC, Event: end.AddMinutes(-10), Collected: end.AddMinutes(30)),
        };
        foreach (var r in rows)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, database_name, blocked_process_report_xml)
VALUES ($1,$2,$3,$4,$5,12000,60,$6,'AppDb','<blocked-process-report/>')",
                CollectionIdGenerator.Next(), r.Collected, ServerId, ServerName, r.Event, r.Spid);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,'select 1',$7)",
                CollectionIdGenerator.Next(), r.Collected, ServerId, ServerName, r.Event, $"process{r.Spid}", Graph($"process{r.Spid}"));
        }

        return new Seeded(start, end);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, string, Seeded, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live event-time window read tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var seeded = await SeedAsync(connection, ct);
            await body(connection, postgres, cs!, seeded, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static readonly int[] BAndC = [SpidB, SpidC];
    private static readonly string[] BAndCVictims = [$"process{SpidB}", $"process{SpidC}"];

    [Fact]
    public Task ViewerGrids_ListTheEventsThatHappenedInTheWindow() => RunAsync(async (_, _, cs, s, ct) =>
    {
        var viewer = new ViewerDataService(cs);

        var bpr = await viewer.GetRecentBlockedProcessReportsAsync(ServerId, s.Start, s.End, cancellationToken: ct);
        Assert.Equal(BAndC, bpr.Select(r => r.BlockedSpid).OrderBy(x => x).ToArray());

        var deadlocks = await viewer.GetRecentDeadlocksAsync(ServerId, s.Start, s.End, ct);
        Assert.Equal(BAndCVictims, deadlocks.Select(r => r.VictimProcessId).OrderBy(x => x, StringComparer.Ordinal).ToArray());
    });

    [Fact]
    public Task ViewerSlicersTrendAndSeverity_CountTheEventsThatHappenedInTheWindow() => RunAsync(async (_, _, cs, s, ct) =>
    {
        var viewer = new ViewerDataService(cs);

        var blockingSlicer = await viewer.GetBlockingSlicerDataAsync(ServerId, s.Start, s.End, cancellationToken: ct);
        Assert.Equal(s.Hours, blockingSlicer.Select(b => b.BucketTime).ToArray());
        Assert.All(blockingSlicer, b => Assert.Equal(1, b.SessionCount));

        var deadlockSlicer = await viewer.GetDeadlockSlicerDataAsync(ServerId, s.Start, s.End, ct);
        Assert.Equal(s.Hours, deadlockSlicer.Select(b => b.BucketTime).ToArray());
        Assert.All(deadlockSlicer, b => Assert.Equal(1, b.SessionCount));

        var trend = await viewer.GetDeadlockTrendAsync(ServerId, s.Start, s.End, ct);
        Assert.Equal(s.Minutes, trend.Select(p => p.Time).ToArray());

        var severity = await viewer.GetDeadlockSeverityStatsAsync(ServerId, s.Start, s.End, ct);
        Assert.Equal(s.Minutes, severity.Select(p => p.Time).ToArray());
    });

    [Fact]
    public Task McpBlockedProcessReads_ListTheEventsThatHappenedInTheWindow() => RunAsync(async (_, postgres, _, s, ct) =>
    {
        var plain = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, s.Start, s.End, 50, ct);
        Assert.Equal(BAndC, plain.Select(r => r.BlockedSpid).OrderBy(x => x).ToArray());

        var withXml = await DarlingBlockingReader.GetRecentBlockedProcessReportsWithXmlAsync(postgres, ServerId, s.Start, s.End, 50, ct);
        Assert.Equal(BAndC, withXml.Select(r => r.BlockedSpid).OrderBy(x => x).ToArray());
    });

    [Fact]
    public Task McpDeadlockReadsTrendAndSeverity_CountTheEventsThatHappenedInTheWindow() => RunAsync(async (_, postgres, _, s, ct) =>
    {
        var all = await DarlingBlockingReader.GetRecentDeadlocksAsync(postgres, ServerId, s.Start, s.End, 50, cancellationToken: ct);
        Assert.Equal(BAndCVictims, all.Select(r => r.VictimProcessId).OrderBy(x => x, StringComparer.Ordinal).ToArray());

        var graphs = await DarlingBlockingReader.GetRecentDeadlocksAsync(postgres, ServerId, s.Start, s.End, 50, graphOnly: true, cancellationToken: ct);
        Assert.Equal(BAndCVictims, graphs.Select(r => r.VictimProcessId).OrderBy(x => x, StringComparer.Ordinal).ToArray());

        var trend = await DarlingBlockingTrendReader.GetDeadlockTrendAsync(postgres, ServerId, s.Start, s.End, ct);
        Assert.Equal(s.Minutes, trend.Select(p => p.Time).ToArray());

        var severityGraphs = await DarlingDataReader.GetDeadlockGraphsAsync(postgres, ServerId, s.Start, s.End, ct);
        Assert.Equal(2, severityGraphs.Count);
        Assert.DoesNotContain(severityGraphs, g => g.Xml!.Contains($"process{SpidA}", StringComparison.Ordinal));
    });

    /// <summary>A deadlock that happened at 23:50 on day D-1 and was collected at 00:10 on day D belongs to day D-1.</summary>
    [Fact]
    public Task DailySummary_CountsADeadlockAndABlockedReportOnTheDayTheyHappened() => RunAsync(async (connection, _, _, _, ct) =>
    {
        var day = DarlingMcpTestData.Naive(DateTime.UtcNow.Date.AddDays(-6));
        var happened = day.AddMinutes(-10);   // 23:50 on D-1
        var collected = day.AddMinutes(10);   // 00:10 on D
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,'processX','select 1','<deadlock/>')",
            CollectionIdGenerator.Next(), collected, ServerId, ServerName, happened);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, database_name)
VALUES ($1,$2,$3,$4,$5,7000,60,444,'AppDb')",
            CollectionIdGenerator.Next(), collected, ServerId, ServerName, happened);

        var from = day.AddDays(-1);
        var to = day.AddDays(1);
        await using var read = new NpgsqlCommand(DailySummarySql.RangeSql, connection);
        read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = from });
        read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = to });
        // The event-window floor rides along as $4 once the statement carries one.
        if (DailySummarySql.RangeSql.Contains("$4", StringComparison.Ordinal))
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(from) });

        var byDay = new System.Collections.Generic.Dictionary<DateTime, (long Deadlocks, long Blocking)>();
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            var dayOrd = reader.GetOrdinal("day");
            var dlOrd = reader.GetOrdinal("deadlock_count");
            var blOrd = reader.GetOrdinal("blocking_events");
            while (await reader.ReadAsync(ct))
                byDay[reader.GetDateTime(dayOrd)] = (reader.GetInt64(dlOrd), reader.GetInt64(blOrd));
        }

        Assert.True(byDay.TryGetValue(day.AddDays(-1), out var onHappenedDay), "the day the events happened on must count them");
        Assert.Equal((1L, 1L), onHappenedDay);
        Assert.True(!byDay.ContainsKey(day) || byDay[day] == (0L, 0L), "the day the rows were COLLECTED on must not count them");
    });

    /// <summary>The alert reads are a delivery cursor: a report collected inside the window whose event is older
    /// than the window must still alert, or an outage's catch-up batch would never page.</summary>
    [Fact]
    public Task AlertReads_StayOnCollectionTime_SoALateCollectedEventStillAlerts() => RunAsync(async (_, postgres, _, _, ct) =>
    {
        var adapter = new DarlingAlertReadAdapter(postgres);
        var key = ServerId.ToString(CultureInfo.InvariantCulture);

        var bpr = await adapter.GetRecentBlockedProcessReportsAsync(key, 4, ct);
        Assert.Contains(SpidA, bpr.Select(r => r.BlockedSpid));
        Assert.Contains(SpidB, bpr.Select(r => r.BlockedSpid));

        var deadlocks = await adapter.GetRecentDeadlocksAsync(key, 4, ct);
        Assert.Contains($"process{SpidA}", deadlocks.Select(r => r.VictimProcessId));
        Assert.Contains($"process{SpidB}", deadlocks.Select(r => r.VictimProcessId));
    });
}
