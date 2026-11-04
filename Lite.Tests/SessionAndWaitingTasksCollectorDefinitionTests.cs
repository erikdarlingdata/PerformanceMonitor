/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>Pins the parity contracts of the batch-9 definitions.</summary>
public sealed class SessionStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void NamesColumnsAndQuery_MatchSchema()
    {
        Assert.Equal("session_stats", SessionStatsCollector.Instance.Name);
        var query = SessionStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("sys.dm_exec_sessions", query, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE N'PerformanceMonitor%'", query, StringComparison.Ordinal);
        Assert.Equal(
            new[]
            {
                "program_name", "connection_count", "running_count", "sleeping_count", "dormant_count",
                "total_cpu_time_ms", "total_reads", "total_writes", "total_logical_reads",
            },
            SessionStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAndWrite_RoundTrip_WithNullableSums()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "HammerDB", 25L, 5, 20, 0, 123456L, 9L, 8L, DBNull.Value });

        var rows = await SessionStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.TotalLogicalReads);

        var writer = new RecordingCollectorRowWriter();
        SessionStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { "HammerDB", 25L, 5, 20, 0, 123456L, 9L, 8L, null }, writer.Values);
    }
}

public sealed class WaitingTasksCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_SplicesExclusions_AndTargetsWaitingTasks()
    {
        var plan = WaitingTasksCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "SO" },
        });

        Assert.Contains("sys.dm_os_waiting_tasks", plan.Text, StringComparison.Ordinal);
        Assert.Contains("AND d.name NOT IN (@excl_db_0)", plan.Text, StringComparison.Ordinal);
        Assert.Equal("SO", Assert.Single(plan.Parameters).Value);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        Assert.Equal(
            new[] { "session_id", "wait_type", "wait_duration_ms", "blocking_session_id", "resource_description", "database_name", "program_name" },
            WaitingTasksCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task ReadAndWrite_WidensSmallints_AndKeepsNullPlaceholder()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { (short)71, "LCK_M_X", 4500L, (short)55, "StackOverflow", "PerformanceMonitor Darling" },
            new object[] { (short)72, "PAGEIOLATCH_SH", 20L, DBNull.Value, DBNull.Value, DBNull.Value });

        var rows = await WaitingTasksCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Equal(2, rows.Count);

        var writer = new RecordingCollectorRowWriter();
        WaitingTasksCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { 71, "LCK_M_X", 4500L, 55, null, "StackOverflow", "PerformanceMonitor Darling" }, writer.Values);

        writer.Values.Clear();
        WaitingTasksCollector.Instance.WritePayload(rows[1], writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(72, writer.Values[0]);          /* smallint widened to int */
        Assert.Null(writer.Values[3]);               /* null blocking session survives */
        Assert.Null(writer.Values[4]);               /* resource_description placeholder */
        Assert.Null(writer.Values[6]);               /* #7573: a session with no program name stores NULL, never a placeholder */
    }

    [Fact]
    public void BuildQuery_ReadsTheSessionsProgramName()
    {
        /* #7573: the program name comes from sys.dm_exec_sessions on session_id, LEFT-joined so a task whose session ended is kept. */
        var text = WaitingTasksCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas)).Text;
        Assert.Contains("program_name = es.program_name", text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN sys.dm_exec_sessions AS es", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_FiltersIgnoredWaitTypes()
    {
        /* WAITFOR is a benign wait that (unlike most of the ignore list) surfaces on user sessions
           (session_id >= 50), so it is the poster child for the "Current Waits" leak this filter closes:
           the collector now skips ignored types just like WaitStatsCollector, so both wait surfaces honor
           the same list (Lite: ignored_wait_types.json, Darling: IgnoredWaitDefaults.All). */
        using var reader = new FakeCollectorDataReader(
            new object[] { (short)57, "LCK_M_X", 200L, (short)58, "SalesDb", "App" },
            new object[] { (short)60, "WAITFOR", 5000L, DBNull.Value, "SalesDb", "App" },
            new object[] { (short)62, "SOS_SCHEDULER_YIELD", 15L, DBNull.Value, "SalesDb", DBNull.Value });

        var context = CollectorTestContext.Make(s_deltas, ignored: new[] { "WAITFOR" });

        var rows = await WaitingTasksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new WaitingTasksCollector.Row(57, "LCK_M_X", 200L, 58, "SalesDb", "App"), rows[0]);
        Assert.Equal(new WaitingTasksCollector.Row(62, "SOS_SCHEDULER_YIELD", 15L, null, "SalesDb", null), rows[1]);
    }
}
