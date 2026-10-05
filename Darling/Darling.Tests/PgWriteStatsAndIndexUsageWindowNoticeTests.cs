/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4966: where the data starts, on <c>get_pg_write_stats</c> and <c>get_pg_index_usage</c>. Write stats answers ONE row whose
/// figures are first-to-last differences, so its start is the row's own first sample and no probe is needed on a data answer.
/// Index usage reads per-index differences off a daily collector, so its start comes from coverage. The two newest-per-key reads
/// (<c>get_pg_index_bloat</c>, <c>get_pg_column_stats</c>) carry no notice, and a test pins that.
/// </summary>
[Collection("live-postgres")]
public sealed class PgWriteStatsAndIndexUsageWindowNoticeTests
{
    private const string WriteTable = "pg_write_stats";
    private const string UsageTable = "pg_index_usage_stats";
    private static readonly string[] WriteTables = [WriteTable];
    private static readonly string[] UsageTables = [UsageTable];

    private static string Name(string scenario) => "darling-mcp-pgnotice-" + scenario;

    private static Task RunAsync(string table, string[] tables, string scenario, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG"), [table], [Name(scenario)], tables, body);

    private static Task<string> WriteAsync(NpgsqlDataSource ds, string scenario, int hours, DateTime end) =>
        DarlingMcpPgServerStateTools.GetPgWriteStats(ds, Name(scenario), hours, WebDataStartNote.FormatWindowEnd(end));

    private static Task<string> UsageAsync(NpgsqlDataSource ds, string scenario, int hours, DateTime end) =>
        DarlingMcpPgIndexUsageTools.GetPgIndexUsage(ds, Name(scenario), hours, as_of: WebDataStartNote.FormatWindowEnd(end));

    private static Task SeedWriteSamplesAsync(NpgsqlConnection c, string scenario, DateTime first, int minutes) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO pg_write_stats (collection_id, collection_time, server_id, server_name, num_timed, num_requested, num_done, wal_bytes)
SELECT $1 + n, $2::timestamp + n * interval '1 minute', $3, $4, n, 0, n, n * 1048576
FROM generate_series(0, $5) AS n",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(first), ServerIdHelper.GetDeterministicHashCode(Name(scenario)), Name(scenario), minutes);

    private static Task SeedUsageSampleAsync(NpgsqlConnection c, string scenario, DateTime at, long scans) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition)
VALUES ($1,$2,$3,$4,'db1','public','t1','ix_t1',$5, 0,0,0,0, 10485760, 20971520, false,false,true,true, false,false,false,false, 'btree', 1, 'CREATE INDEX ix_t1 ON t1 (a)')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(Name(scenario)), Name(scenario), scans);

    private static Task MarkSqlServerAsync(NpgsqlConnection c, string scenario) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerIdHelper.GetDeterministicHashCode(Name(scenario)), MonitoredEngineKind.SqlServer);

    [Theory]
    [InlineData("wlate", 120, true)]
    [InlineData("wsoon", 30, false)]
    public async Task WriteStats_DerivesTheKeysFromItsOwnSpan_WithNoProbe_AgainstDevPostgres(string scenario, int firstSampleAfterStartMinutes, bool truncated) =>
        await RunAsync(WriteTable, WriteTables, scenario, async (c, ds, end) =>
        {
            var start = end.AddHours(-24);
            var first = start.AddMinutes(firstSampleAfterStartMinutes);
            await WindowFloorLiveHarness.SeedServerAsync(c, Name(scenario), end.AddDays(-30), WriteTable, null, 1, end, WriteTables, TestContext.Current.CancellationToken);
            await SeedWriteSamplesAsync(c, scenario, first, 60);

            var probes = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { probes++; throw new TimeoutException("a data answer must not probe"); };
            var root = WindowFloorLiveHarness.Parse(await WriteAsync(ds, scenario, 24, end));

            Assert.Equal(0, probes);
            Assert.Equal(truncated, root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(first), root.GetProperty("effective_start").GetString());
            Assert.EndsWith("Z", root.GetProperty("effective_start").GetString(), StringComparison.Ordinal);
            if (truncated)
            {
                Assert.Contains("effective_start to window_end", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            }
            else
            {
                Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            }

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });

    [Fact]
    public async Task WriteStats_AGapAcrossTheWindowStart_IsTruncated_WithTheFirstSampleAfterTheGap_AgainstDevPostgres() =>
        await RunAsync(WriteTable, WriteTables, "wgap", async (c, ds, end) =>
        {
            var start = end.AddHours(-24);
            var afterGap = start.AddHours(3);
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("wgap"), end.AddDays(-30), WriteTable, null, 1, end, WriteTables, TestContext.Current.CancellationToken);
            await SeedWriteSamplesAsync(c, "wgap", start.AddHours(-4), 60);
            await SeedWriteSamplesAsync(c, "wgap", afterGap, 60);

            var root = WindowFloorLiveHarness.Parse(await WriteAsync(ds, "wgap", 24, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(afterGap), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task WriteStats_EmptyAnswer_CarriesTheKeysUnderHints_AndNotCollectedStaysBare_AgainstDevPostgres()
    {
        await RunAsync(WriteTable, WriteTables, "wempty", async (c, ds, end) =>
        {
            var added = end.AddDays(-1);
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("wempty"), added, WriteTable, added, 1, end, WriteTables, TestContext.Current.CancellationToken);

            var root = WindowFloorLiveHarness.Parse(await WriteAsync(ds, "wempty", 168, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), hints.GetProperty("effective_start").GetString());
            Assert.False(root.TryGetProperty("window_truncated", out _));
        });

        await RunAsync(WriteTable, WriteTables, "wbare", async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("wbare"), end.AddDays(-1), WriteTable, null, 1, end, WriteTables, TestContext.Current.CancellationToken);
            await MarkSqlServerAsync(c, "wbare");

            var root = WindowFloorLiveHarness.Parse(await WriteAsync(ds, "wbare", 168, end));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("hints", out _));
        });
    }

    [Fact]
    public async Task IndexUsage_ADailyCollectorWithAShortHistory_IsTruncatedOn168Hours_AgainstDevPostgres() =>
        await RunAsync(UsageTable, UsageTables, "ushort", async (c, ds, end) =>
        {
            var floor = end.AddDays(-2);
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("ushort"), floor, UsageTable, floor, 1440, end, UsageTables, TestContext.Current.CancellationToken);
            await SeedUsageSampleAsync(c, "ushort", end.AddDays(-2), 10);
            await SeedUsageSampleAsync(c, "ushort", end.AddDays(-1), 12);

            var root = WindowFloorLiveHarness.Parse(await UsageAsync(ds, "ushort", 168, end));

            Assert.Equal(1, root.GetProperty("indexes_returned").GetInt32());
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(floor), root.GetProperty("effective_start").GetString());
            var note = root.GetProperty("truncation_note").GetString();
            Assert.Contains("scans_in_window", note, StringComparison.Ordinal);
            Assert.Contains("undercounts", note, StringComparison.Ordinal);
            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });

    [Fact]
    public async Task IndexUsage_AFailedProbe_CostsTheKeysNotTheRows_AgainstDevPostgres() =>
        await RunAsync(UsageTable, UsageTables, "uprobe", async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("uprobe"), end.AddDays(-30), UsageTable, end.AddDays(-30), 1440, end, UsageTables, TestContext.Current.CancellationToken);
            await SeedUsageSampleAsync(c, "uprobe", end.AddDays(-2), 10);
            await SeedUsageSampleAsync(c, "uprobe", end.AddDays(-1), 12);
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");

            var root = WindowFloorLiveHarness.Parse(await UsageAsync(ds, "uprobe", 168, end));

            Assert.Equal(1, root.GetProperty("indexes_returned").GetInt32());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));
        });

    [Fact]
    public async Task IndexUsage_NothingToRead_CarriesTheKeysUnderHints_AndNotCollectedStaysBare_AgainstDevPostgres()
    {
        await RunAsync(UsageTable, UsageTables, "uempty", async (c, ds, end) =>
        {
            var added = end.AddDays(-1);
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("uempty"), added, UsageTable, added, 1440, end, UsageTables, TestContext.Current.CancellationToken);

            var root = WindowFloorLiveHarness.Parse(await UsageAsync(ds, "uempty", 168, end));

            Assert.Equal("unavailable", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), hints.GetProperty("effective_start").GetString());
            Assert.Equal(0, hints.GetProperty("snapshots_in_window").GetInt32());
            Assert.False(root.TryGetProperty("window_truncated", out _));
        });

        await RunAsync(UsageTable, UsageTables, "ubare", async (c, ds, end) =>
        {
            await WindowFloorLiveHarness.SeedServerAsync(c, Name("ubare"), end.AddDays(-1), UsageTable, null, 1440, end, UsageTables, TestContext.Current.CancellationToken);
            await MarkSqlServerAsync(c, "ubare");

            var root = WindowFloorLiveHarness.Parse(await UsageAsync(ds, "ubare", 168, end));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("hints", out _));
        });
    }

    /// <summary>The two newest-per-key reads answer a verdict of their own coverage, so neither carries the window notice (#4966).
    /// Read from the source text, so it holds without a database.</summary>
    [Theory]
    [InlineData("get_pg_index_bloat")]
    [InlineData("get_pg_column_stats")]
    public void TheNewestPerKeyReads_EmitNoWindowNotice(string tool)
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgIndexTools.cs");
        var signature = tool switch
        {
            "get_pg_index_bloat" => "public static async Task<string> GetPgIndexBloat(",
            _ => "public static async Task<string> GetPgColumnStats(",
        };
        var at = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at > 0, tool + " is not in DarlingMcpPgIndexTools.cs");
        /* The body runs from the method declaration to the next tool's attribute (or the end of the file). */
        var nextAttribute = source.IndexOf("[McpServerTool(", at + signature.Length, StringComparison.Ordinal);
        var body = source[at..(nextAttribute < 0 ? source.Length : nextAttribute)];

        Assert.DoesNotContain("window_truncated", body, StringComparison.Ordinal);
        Assert.DoesNotContain("effective_start", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DarlingMcpWindowNotice", body, StringComparison.Ordinal);
    }
}
