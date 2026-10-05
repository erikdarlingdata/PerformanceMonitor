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
/// #4966: the window-floor keys on six PostgreSQL MCP reads (wait stats, wait sampling, kernel stats, lock stats, predicate stats and
/// the server config changes), probed on the web's own source for each read (<see cref="WebDataStartNote.TryGetReadSource"/>).
/// </summary>
public sealed class PgGridsMcpDataStartLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Tables =
        ["pg_wait_stats", "pg_wait_sampling", "pg_kernel_stats", "pg_lock_stats", "pg_predicate_stats", "pg_server_config"];

    public static TheoryData<string> Reads => new()
    {
        "get_pg_wait_stats", "get_pg_wait_sampling", "get_pg_kernel_stats", "get_pg_lock_stats", "get_pg_predicate_stats", "get_pg_server_config_changes",
    };

    private static string Name(string tool, string scenario) => $"pgg-{tool.Replace("get_pg_", "").Replace('_', '-')}-{scenario}";

    private static Task Run(string tool, string scenario, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, string, Task> body)
    {
        var name = Name(tool, scenario);
        return WindowFloorLiveHarness.RunAsync(ConnectionString, Tables, [name], Tables, (c, ds, end) => body(c, ds, end, name));
    }

    private static Task<string> Call(string tool, NpgsqlDataSource ds, string name, int hours, DateTime end, int? limit = null)
    {
        var asOf = WebDataStartNote.FormatWindowEnd(end);
        return tool switch
        {
            "get_pg_wait_stats" => DarlingMcpPgWaitTools.GetPgWaitStats(ds, name, hours, limit ?? 20, asOf),
            "get_pg_wait_sampling" => DarlingMcpPgWaitSamplingTools.GetPgWaitSampling(ds, name, hours, limit ?? 20, asOf),
            "get_pg_kernel_stats" => DarlingMcpPgKernelStatsTools.GetPgKernelStats(ds, name, hours, limit ?? 20, asOf),
            "get_pg_lock_stats" => DarlingMcpPgServerStateTools.GetPgLockStats(ds, name, hours, limit ?? 25, asOf),
            "get_pg_predicate_stats" => DarlingMcpPgPredicateTools.GetPgPredicateStats(ds, name, hours, limit ?? 25, asOf),
            _ => DarlingMcpPgServerStateTools.GetPgServerConfigChanges(ds, name, hours, limit ?? 100, asOf),
        };
    }

    private static string RowsKey(string tool) => tool switch
    {
        "get_pg_wait_stats" or "get_pg_wait_sampling" => "waits",
        "get_pg_kernel_stats" => "queries",
        "get_pg_lock_stats" => "locks",
        "get_pg_predicate_stats" => "predicates",
        _ => "changes",
    };

    private static string TableOf(string tool) => WebDataStartNote.TableByRead[tool];

    /// <summary>Seeds the read's rows: two collections ten minutes apart from <paramref name="first"/> (the cumulative families need two to difference).</summary>
    private static async Task SeedAsync(NpgsqlConnection c, string tool, string name, DateTime first)
    {
        var ct = TestContext.Current.CancellationToken;
        var id = ServerIdHelper.GetDeterministicHashCode(name);
        for (var i = 0; i < 2; i++)
        {
            var t = DarlingMcpTestData.Naive(first.AddMinutes(10 * i));
            switch (tool)
            {
                case "get_pg_wait_stats":
                    await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_wait_stats (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event, waits, wait_time_us, delta_waits, delta_wait_time_us)
VALUES ($1, $2, $3, $4, 1, 100, 'IO', 'DataFileRead', 100, 9999999, 10, 5000000)", CollectionIdGenerator.Next(), t, id, name);
                    break;
                case "get_pg_wait_sampling":
                    await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count)
VALUES ($1, $2, $3, $4, 'IO', 'DataFileRead', 7, $5, 10, 1)", CollectionIdGenerator.Next(), t, id, name, 100L + 50 * i);
                    break;
                case "get_pg_kernel_stats":
                    await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_kernel_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since)
VALUES ($1, $2, $3, $4, 'app', 7, $5, $6, 0, 8192, 0, 0, 0, $7)", CollectionIdGenerator.Next(), t, id, name, 100.0 + 20 * i, 10.0 * i, DarlingMcpTestData.Naive(first.AddDays(-1)));
                    break;
                case "get_pg_lock_stats":
                    await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_lock_stats (collection_id, collection_time, server_id, server_name, database_name, lock_type, mode, granted, relation_name, backend_count, oldest_wait_ms)
VALUES ($1, $2, $3, $4, 'app', 'relation', 'AccessShareLock', false, 'public.t', 2, 1500)", CollectionIdGenerator.Next(), t, id, name);
                    break;
                case "get_pg_predicate_stats":
                    await DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_predicate_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, operator, query_id, sample_count, rows_evaluated, rows_filtered, worst_estimate_error_ratio, sample_rate)
VALUES ($1, $2, $3, $4, 'app', 'public', 't', 'c', '=', 7, 120, 1000, 900, 1.5, 0.01)", CollectionIdGenerator.Next(), t, id, name);
                    break;
                default:
                    await SeedConfigAsync(c, name, t, i == 0 ? "128MB" : "256MB");
                    break;
            }
        }
    }

    private static Task SeedConfigAsync(NpgsqlConnection c, string name, DateTime at, string setting) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, @"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
     source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, 'work_mem', $5, NULL, 'Resource Usage / Memory', 'user', 'integer', 'configuration file', '4MB', $5, NULL, 0, false, NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(name), name, setting);

    private static Task SeedServerAsync(NpgsqlConnection c, string name, DateTime created) =>
        WindowFloorLiveHarness.SeedServerAsync(c, name, created, "pg_wait_stats", null, 30, DateTime.UtcNow, Tables, TestContext.Current.CancellationToken);

    /// <summary>What the web's own source says coverage is for the tool's window.</summary>
    private static async Task<DateTime?> WebFloorAsync(NpgsqlDataSource ds, string tool, string name, int hours, DateTime end)
    {
        Assert.True(WebDataStartNote.TryGetReadSource(tool, out var source));
        return await DataWindowFloor.GetAsync(ds, [source], [name], end.AddHours(-hours), end, StorageCommandDeadlines.McpReadSeconds, TestContext.Current.CancellationToken);
    }

    [Theory]
    [MemberData(nameof(Reads))]
    public Task ARowsAnswer_ForAServerAddedTwoDaysAgo_NamesWhereCoverageStarts_OnTheWebsSource_AgainstDevPostgres(string tool) =>
        Run(tool, "added", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-2));
            await SeedAsync(c, tool, name, end.AddHours(-3));

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 168, end));
            var floor = await WebFloorAsync(ds, tool, name, 168, end);

            Assert.NotNull(floor);
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(floor!.Value), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(floor, end.AddHours(-168), TableOf(tool)).TruncationNote, root.GetProperty("truncation_note").GetString());

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
            Assert.True(root.GetProperty(RowsKey(tool)).GetArrayLength() > 0);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public Task AQuietStart_GivesNoNotice_AgainstDevPostgres(string tool) =>
        Run(tool, "quiet", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-30));
            await SeedAsync(c, tool, name, end.AddDays(-8));
            await SeedAsync(c, tool, name, end.AddHours(-3));

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public Task AnEmptyAnswer_PastCoverage_CarriesHints_WhereTheReadHasAnEmpty_AgainstDevPostgres(string tool) =>
        Run(tool, "empty", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-2));

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 1, end.AddDays(-9)));

            if (tool == "get_pg_wait_stats")
            {
                /* The read's empty answer is "unavailable", which stays bare. */
                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                Assert.False(root.TryGetProperty("hints", out _));
                return;
            }

            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.NotEqual(JsonValueKind.Null, hints.GetProperty("truncation_note").ValueKind);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public Task NotCollected_StaysBare_AgainstDevPostgres(string tool) =>
        Run(tool, "notcollected", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-2));
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                "UPDATE servers SET sql_engine_edition = 3, engine_kind = 'sqlserver' WHERE server_id = $1", ServerIdHelper.GetDeterministicHashCode(name));
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 168, end.AddDays(-9)));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("hints", out _));
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.Equal(0, calls);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres(string tool) =>
        Run(tool, "short", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-30));
            await SeedAsync(c, tool, name, end.AddMinutes(-40));
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 1, end));

            Assert.Equal(0, calls);
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.True(root.GetProperty(RowsKey(tool)).GetArrayLength() > 0);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres(string tool) =>
        Run(tool, "probefail", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-2));
            await SeedAsync(c, tool, name, end.AddHours(-3));
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");

            var root = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 168, end));

            Assert.False(root.TryGetProperty("status", out var status) && status.GetString() != "config_changes");
            Assert.True(root.GetProperty(RowsKey(tool)).GetArrayLength() > 0);
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));

            if (tool != "get_pg_wait_stats")
            {
                var empty = WindowFloorLiveHarness.Parse(await Call(tool, ds, name, 1, end.AddDays(-9)));
                Assert.False(empty.TryGetProperty("hints", out _));
            }
        });

    [Fact]
    public Task ACappedConfigChangesPage_NamesItsOldestChange_AndStartsNoProbe_AgainstDevPostgres() =>
        Run("get_pg_server_config_changes", "capped", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, name, end.AddDays(-30));
            await SeedConfigAsync(c, name, end.AddHours(-5), "1MB");
            await SeedConfigAsync(c, name, end.AddHours(-4), "2MB");
            await SeedConfigAsync(c, name, end.AddHours(-3), "3MB");
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await Call("get_pg_server_config_changes", ds, name, 168, end, limit: 1));

            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(1, root.GetProperty("changes").GetArrayLength());
            var shown = DateTime.Parse(root.GetProperty("changes")[0].GetProperty("changed_at").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal);
            Assert.Equal(McpHelpers.FormatEffectiveStart(shown), root.GetProperty("effective_start").GetString());
            Assert.Equal(0, calls);
        });

    [Theory]
    [MemberData(nameof(Reads))]
    public void TheToolProbesTheWebsSourceForItsRead(string tool)
    {
        Assert.True(WebDataStartNote.TryGetReadSource(tool, out _), $"{tool} has no web probe source");
        var file = tool switch
        {
            "get_pg_wait_stats" => "DarlingMcpPgWaitTools.cs",
            "get_pg_wait_sampling" => "DarlingMcpPgWaitSamplingTools.cs",
            "get_pg_kernel_stats" => "DarlingMcpPgKernelStatsTools.cs",
            "get_pg_predicate_stats" => "DarlingMcpPgPredicateTools.cs",
            _ => "DarlingMcpPgServerStateTools.cs",
        };
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
        /* Every notice read of the tool names its read and the web's table for it, and none builds its own source. */
        Assert.Contains($"\"{tool}\", \"{TableOf(tool)}\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ForCollectorTable(", source, StringComparison.Ordinal);
        Assert.Contains(tool, WebDataStartNote.TableByRead.Keys);
        Assert.DoesNotContain(tool, WebDataStartNote.CollectorRunsByRead.Keys);
    }
}
