/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The window-floor notice of the PostgreSQL window reads (#4966): top queries, database stats, I/O stats, captured plans and
/// CPU utilization. Each tool writes <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c> right after
/// <c>hours_back</c> on a data answer, carries them under <c>hints</c> on an empty one, and leaves a not-collected answer bare.
/// </summary>
public sealed class DarlingMcpPgSourceTests
{
    public static IEnumerable<object[]> WebListed() =>
        [["get_pg_top_queries"], ["get_pg_database_stats"], ["get_pg_io_stats"], ["get_pg_plans"],
         ["get_pg_blocking"], ["get_pg_session_states"], ["get_pg_replication_stats"],
         ["get_pg_wait_stats"], ["get_pg_wait_sampling"], ["get_pg_kernel_stats"], ["get_pg_lock_stats"], ["get_pg_predicate_stats"],
         ["get_pg_server_config_changes"], ["get_pg_xmin_horizon"]];

    /// <summary>A tool the web lists probes the very source the web's note probes, so the tool and the page never name two starts.</summary>
    [Theory]
    [MemberData(nameof(WebListed))]
    public void AWebListedTool_ProbesTheSourceTheWebProbes(string read)
    {
        Assert.True(WebDataStartNote.TryGetReadSource(read, out var web));
        var tool = DarlingMcpWindowNotice.SourceFor(read);

        Assert.Equal(web.Relation, tool.Relation);
        Assert.Equal(web.TimeColumn, tool.TimeColumn);
        Assert.Equal(web.EndExclusive, tool.EndExclusive);
        Assert.Equal(web.CollectorName, tool.CollectorName);
        Assert.Equal(web.LogCollectorName, tool.LogCollectorName);
        Assert.Equal(WebDataStartNote.CollectorRunsByRead.ContainsKey(read), tool.LogCollectorName is not null);
    }

    /// <summary>The one read the web does not list is probed on its collector's own table.</summary>
    [Theory]
    [InlineData("get_pg_cpu_utilization", "pg_cpu_utilization")]
    public void AnUnlistedTool_ProbesItsCollectorTable(string read, string table)
    {
        Assert.False(WebDataStartNote.TableByRead.ContainsKey(read));
        var source = DarlingMcpWindowNotice.SourceFor(read);
        Assert.Equal(table, source.Relation);
        Assert.Equal("collection_time", source.TimeColumn);
        Assert.Equal(table, DarlingMcpWindowNotice.TableFor(read));
    }
}

[Collection("live-postgres")]
public sealed class DarlingMcpPgWindowD8aLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Tool(string Read, string Table, string Collector, Func<NpgsqlDataSource, string, int, DateTime, Task<string>> Call, Func<NpgsqlConnection, string, DateTime, Task> SeedRow);

    private static string Name(string read, string window) => "pgwin-" + read.Replace("get_pg_", "").Replace('_', '-') + "-" + window;

    private static Task Exec(NpgsqlConnection c, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, sql, values);

    private static int Id(string name) => ServerIdHelper.GetDeterministicHashCode(name);

    private static string AsOf(DateTime end) => WebDataStartNote.FormatWindowEnd(end);

    /// <summary>Two cumulative samples a minute apart at <paramref name="at"/>, so the differencing reads have an interval.</summary>
    private static readonly Tool[] Tools =
    [
        new("get_pg_top_queries", "pg_statement_stats", "pg_statement_stats",
            (ds, n, h, end) => DarlingMcpPgStatementTools.GetPgTopQueries(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, 1, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, 5, 50.0, 50, 60)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
        new("get_pg_database_stats", "pg_database_stats", "pg_database_stats",
            (ds, n, h, end) => DarlingMcpPgDatabaseTools.GetPgDatabaseStats(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) =>
            {
                foreach (var (snap, commits) in new[] { (at, 1000L), (at.AddMinutes(1), 1100L) })
                {
                    await Exec(c,
                        "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'appdb', $5, 0, 100, 9000, 0, 0, 0, NULL)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(snap), Id(n), n, commits);
                }
            }),
        new("get_pg_io_stats", "pg_io_stats", "pg_io_stats",
            (ds, n, h, end) => DarlingMcpPgIoTools.GetPgIoStats(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) =>
            {
                foreach (var (snap, reads) in new[] { (at, 10L), (at.AddMinutes(1), 500L) })
                {
                    await Exec(c,
                        "INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context, reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes, hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes) VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal', $5, 0.0, 0, 0.0, 0, 0, 0, 0, 8192, 0, 0, 0, 0, 0, NULL, 0, 0, 0)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(snap), Id(n), n, reads);
                }
            }),
        new("get_pg_plans", "pg_plan_capture", "pg_plan_capture",
            (ds, n, h, end) => DarlingMcpPgPlanTools.GetPgPlans(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_plan_capture (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms, node_count, top_node_type, plan_json) VALUES ($1, $2, $3, $4, 7, 'h1', 1200.0, 3, 'Seeded', '{\"Plan\":{\"Node Type\":\"Seq Scan\"}}')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
        new("get_pg_cpu_utilization", "pg_cpu_utilization", "pg_cpu_utilization",
            (ds, n, h, end) => DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(ds, n, h, AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu) VALUES ($1, $2, $3, $4, $2, 30.0, 25.0, 3.8, 12.0)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
    ];

    public static IEnumerable<object[]> ToolNames() => Tools.Select(t => new object[] { t.Read });

    private static Tool Of(string read) => Tools.Single(t => t.Read == read);

    private static Task RunAsync(Tool t, string window, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, string, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [t.Collector], [Name(t.Read, window)], [t.Table],
            (c, ds, end) => body(c, ds, end, Name(t.Read, window)));

    private static Task SeedServerAsync(NpgsqlConnection c, Tool t, string name, DateTime created, DateTime? runsFrom, DateTime end) =>
        WindowFloorLiveHarness.SeedServerAsync(c, name, created, t.Collector, runsFrom, 30, end, [t.Table], TestContext.Current.CancellationToken);

    private static Task SetEngineAsync(NpgsqlConnection c, string name, int edition, string? kind) =>
        Exec(c, "UPDATE servers SET sql_engine_edition = $2, engine_kind = $3 WHERE server_id = $1", Id(name), edition, kind);

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task ADataAnswer_ForAServerAddedTwoDaysAgo_NamesWhereCoverageStarts_RightAfterHoursBack_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "added", async (c, ds, end, name) =>
        {
            var added = end.AddDays(-2);
            await SeedServerAsync(c, t, name, added, added, end);
            /* A dense collector's edge is its oldest row, so the first row lands at the start of collection. */
            await t.SeedRow(c, name, added);
            await t.SeedRow(c, name, end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.False(root.TryGetProperty("status", out var status) && status.GetString() != "database_activity" && status.GetString() != "io_activity", root.ToString());
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(added, end.AddHours(-168), DarlingMcpWindowNotice.TableFor(read)).TruncationNote, root.GetProperty("truncation_note").GetString());

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });
    }

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task ADataAnswer_WhoseCollectionStartsBeforeTheWindow_IsCovered_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "quiet", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), end.AddDays(-9), end);
            await t.SeedRow(c, name, end.AddDays(-8));
            await t.SeedRow(c, name, end.AddDays(-5));

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean(), root.ToString());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });
    }

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "short", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), end.AddDays(-2), end);
            await t.SeedRow(c, name, end.AddMinutes(-40));
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 1, end));

            Assert.Equal(0, calls);
            Assert.False(root.GetProperty("window_truncated").GetBoolean(), root.ToString());
        });
    }

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "probefail", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-2), end.AddDays(-2), end);
            await t.SeedRow(c, name, end.AddDays(-1));
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.False(McpHelpers.IsErrorEnvelope(root.ToString()), root.ToString());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));
            Assert.True(root.EnumerateObject().Count() > 5, "the rows must survive the failed probe");
        });
    }

    /// <summary>Two quiet snapshots inside a window that begins before collection did: the all-clear names where the data starts instead (#4966).</summary>
    [Fact]
    public async Task ADatabaseStatsAllClear_OverACutWindow_NamesWhereTheDataStarts_AgainstDevPostgres()
    {
        var t = Of("get_pg_database_stats");
        await RunAsync(t, "emptycut", async (c, ds, end, name) =>
        {
            var began = end.AddMinutes(-40);
            await SeedServerAsync(c, t, name, began, began, end);
            foreach (var snap in new[] { end.AddMinutes(-30), end.AddMinutes(-20) })
            {
                await Exec(c,
                    "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'quietdb', 1000, 10, 100, 9900, 0, 0, 0, NULL)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(snap), Id(name), name);
            }

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 3, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.True(root.GetProperty("hints").GetProperty("window_truncated").GetBoolean(), root.ToString());
            var message = root.GetProperty("message").GetString()!;
            Assert.EndsWith(". " + McpHelpers.CutWindowNothingMessage, message, StringComparison.Ordinal);
            Assert.DoesNotContain("genuine all-clear", message, StringComparison.Ordinal);
        });
    }

    [Theory]
    [InlineData("get_pg_io_stats")]
    [InlineData("get_pg_plans")]
    [InlineData("get_pg_cpu_utilization")]
    public async Task AnEmptyAnswer_PastCoverage_CarriesHints_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "empty", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), null, end);

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 1, end));

            Assert.False(root.TryGetProperty("window_truncated", out _), "an empty answer carries the keys under hints, not at the top");
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
            Assert.Contains("no collection of " + t.Table, hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ADatabaseStatsEmptyAnswer_WithTwoQuietSamples_CarriesHintsBesideItsOwn_AgainstDevPostgres()
    {
        var t = Of("get_pg_database_stats");
        await RunAsync(t, "empty", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), end.AddDays(-30), end);
            foreach (var snap in new[] { end.AddMinutes(-30), end.AddMinutes(-20) })
            {
                await Exec(c,
                    "INSERT INTO pg_database_stats (collection_id, collection_time, server_id, server_name, database_name, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset) VALUES ($1, $2, $3, $4, 'appdb', 1000, 0, 100, 9000, 0, 0, 0, NULL)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(snap), Id(name), name);
            }

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 1, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.Equal("2+", hints.GetProperty("samples_in_window").GetString());
            Assert.False(hints.GetProperty("window_truncated").GetBoolean());
            Assert.True(hints.TryGetProperty("effective_start", out _));
        });
    }

    [Theory]
    [InlineData("get_pg_top_queries", 3, "sqlserver")]
    [InlineData("get_pg_database_stats", 3, "sqlserver")]
    [InlineData("get_pg_io_stats", 3, "sqlserver")]
    [InlineData("get_pg_plans", 3, "sqlserver")]
    [InlineData("get_pg_cpu_utilization", 0, "postgres")]
    public async Task ANotCollectedAnswer_StaysBare_AndStartsNoProbe_AgainstDevPostgres(string read, int edition, string? kind)
    {
        var t = Of(read);
        await RunAsync(t, "notcollected", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), null, end);
            await SetEngineAsync(c, name, edition, kind);
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("hints", out _));
            Assert.Equal(0, calls);
        });
    }
}
