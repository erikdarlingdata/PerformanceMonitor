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
using System.Threading;
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
/// #4966: where the data starts on Darling's two PostgreSQL event lists, <c>get_pg_deadlocks</c> and <c>get_pg_log_events</c>.
/// Both are sparse, newest first and capped, and both window on the event's own time, so the notice is the earlier of the
/// coverage floor and the oldest event shown, and a full page names its oldest row with no probe. Every instant is an offset
/// from one anchor minute, so the probe's purge edge never lands inside a window.
/// </summary>
public sealed class PgEventToolsWindowFloorLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record PgTool(string Name, string Table, string ArrayKey);

    private static readonly PgTool[] Tools =
    [
        new("get_pg_deadlocks", "pg_deadlocks", "deadlocks"),
        new("get_pg_log_events", "pg_log_events", "events"),
    ];

    private static string ServerOf(PgTool tool, string window) => "darling-mcp-pgevt-" + window + "-" + tool.Name.Replace('_', '-');

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static Task<string> CallAsync(NpgsqlDataSource postgres, PgTool tool, string server, int hours, DateTime end, int limit = 15)
    {
        var asOf = WebDataStartNote.FormatWindowEnd(end);
        return tool.Name == "get_pg_deadlocks"
            ? DarlingMcpPgDeadlockTools.GetPgDeadlocks(postgres, server, hours, limit, asOf)
            : DarlingMcpPgLogEventTools.GetPgLogEvents(postgres, server, hours, null, null, limit, asOf);
    }

    /// <summary>A server registered at <paramref name="created"/> with three events an hour apart from <paramref name="firstEvent"/> (none when null).</summary>
    private static async Task SeedAsync(
        NpgsqlConnection connection, PgTool tool, string name, DateTime created, DateTime? firstEvent, CancellationToken ct, DateTime? collectedAt = null)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DeleteAsync(connection, name, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, name, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", serverId, DarlingMcpTestData.Naive(created));
        if (firstEvent is not DateTime first) return;

        for (var i = 0; i < 3; i++)
        {
            var at = DarlingMcpTestData.Naive(first.AddHours(i));
            var collected = collectedAt is DateTime c ? DarlingMcpTestData.Naive(c) : at;
            if (tool.Table == "pg_deadlocks")
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO pg_deadlocks (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1,$2,$3,$4,$5,4242,2,$6,'ShareLock','relation','UPDATE t SET x = ?','graph')",
                    CollectionIdGenerator.Next(), collected, serverId, name, at, "pgevt-hash-" + i);
            }
            else
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO pg_log_events (collection_id, collection_time, server_id, server_name, occurred_at, family, severity, message, raw_line_hash)
VALUES ($1,$2,$3,$4,$5,'error','ERROR',$6,$7)",
                    CollectionIdGenerator.Next(), collected, serverId, name, at, "boom " + i, "pgevt-line-" + i);
            }
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, string name, CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        foreach (var table in new[] { "pg_deadlocks", "pg_log_events", "collection_log", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", serverId);
        }
    }

    private static async Task ForEachToolAsync(string window, Func<NpgsqlConnection, NpgsqlDataSource, PgTool, string, DateTime, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL event-tool test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var end = WindowFloorLiveHarness.AnchorNow();
            foreach (var tool in Tools)
            {
                await body(connection, postgres, tool, ServerOf(tool, window), end);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var tool in Tools)
                {
                    await DeleteAsync(cleanup, ServerOf(tool, window), cleanupCt);
                }
            });
        }
    }

    /// <summary>Runs <paramref name="act"/> with the probe stand-in <paramref name="probe"/> set, and clears it after.</summary>
    private static async Task WithProbeAsync(Func<Task<DateTime?>> probe, Func<Task> act)
    {
        var bodySucceeded = false;
        DarlingMcpWindowNotice.TestOnlyProbe = probe;
        try
        {
            await act();
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
            {
                DarlingMcpWindowNotice.TestOnlyProbe = null;
                return Task.CompletedTask;
            });
        }
    }

    [Fact]
    public async Task ServerAddedTwoDaysAgo_NamesWhereCoverageStarts_AndKeepsTheKeyOrder_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("added", async (connection, postgres, tool, name, end) =>
        {
            var added = end.AddDays(-2);
            await SeedAsync(connection, tool, name, added, end.AddDays(-1), ct);

            var root = Parse(await CallAsync(postgres, tool, name, 168, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(
                DarlingMcpWindowNotice.Build(added, end.AddHours(-168), tool.Table).TruncationNote,
                root.GetProperty("truncation_note").GetString());
            Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            var at = names.IndexOf("hours_back");
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(at + 1).Take(3));
        });
    }

    [Fact]
    public async Task QuietStart_FirstEventLate_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("quiet", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddDays(-30), end.AddDays(-5), ct);

            var root = Parse(await CallAsync(postgres, tool, name, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });
    }

    [Fact]
    public async Task AnEvent_OlderThanTheFirstCollection_MovesTheStartBackToIt_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("backfill", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddHours(-1), end.AddHours(-23.5), ct, collectedAt: end.AddMinutes(-30));

            var root = Parse(await CallAsync(postgres, tool, name, 24, end));

            Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());
            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            Assert.Equal(McpHelpers.FormatEffectiveStart(end.AddHours(-23.5)), root.GetProperty("effective_start").GetString());
        });
    }

    [Fact]
    public async Task AFullCappedPage_NamesItsOldestRowShown_WithNoProbe_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("capped", async (connection, postgres, tool, name, end) =>
        {
            /* Events at 10, 9 and 8 hours back; a page of 2 shows 8 and 9 hours back, so the answer starts 9 hours back. */
            await SeedAsync(connection, tool, name, end.AddDays(-30), end.AddHours(-10), ct);

            var probes = 0;
            await WithProbeAsync(() => { probes++; throw new TimeoutException("the probe must not run"); }, async () =>
            {
                var root = Parse(await CallAsync(postgres, tool, name, 24, end, limit: 2));

                Assert.Equal(0, probes);
                Assert.True(root.GetProperty("truncated").GetBoolean(), tool.Name);
                Assert.Equal(2, root.GetProperty(tool.ArrayKey).GetArrayLength());
                Assert.True(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
                Assert.Equal(McpHelpers.FormatEffectiveStart(end.AddHours(-9)), root.GetProperty("effective_start").GetString());
                Assert.Contains("page is full", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            });
        });
    }

    [Fact]
    public async Task ACappedPage_ReachingTheWindowStart_GivesNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("cappedstart", async (connection, postgres, tool, name, end) =>
        {
            /* The page's oldest row is 22h48m back, 72 minutes past the window's start: inside the 90-minute slack. */
            await SeedAsync(connection, tool, name, end.AddDays(-30), end.AddHours(-23.8), ct);

            var root = Parse(await CallAsync(postgres, tool, name, 24, end, limit: 2));

            Assert.True(root.GetProperty("truncated").GetBoolean(), tool.Name);
            Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });
    }

    [Fact]
    public async Task AnEmptyAnswer_PastCoverage_CarriesTheNoticeUnderHints_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("empty", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddDays(-2), null, ct);

            var root = Parse(await CallAsync(postgres, tool, name, 1, end.AddDays(-5)));

            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean(), tool.Name);
            Assert.Equal(JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
            Assert.Contains("no collection of " + tool.Table, hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
            Assert.False(root.TryGetProperty("window_truncated", out _), tool.Name);
        });
    }

    [Fact]
    public async Task NotCollected_StaysBare_AndStartsNoProbe_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("notcollected", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddDays(-2), null, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerIdHelper.GetDeterministicHashCode(name), MonitoredEngineKind.SqlServer);

            var probes = 0;
            await WithProbeAsync(() => { probes++; throw new TimeoutException("the probe must not run"); }, async () =>
            {
                var root = Parse(await CallAsync(postgres, tool, name, 1, end.AddDays(-5)));

                Assert.Equal("not_collected", root.GetProperty("status").GetString());
                Assert.False(root.TryGetProperty("hints", out _), tool.Name);
                Assert.Equal(0, probes);
            });
        });
    }

    [Fact]
    public async Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("short", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddDays(-30), end.AddMinutes(-40), ct);

            var probes = 0;
            await WithProbeAsync(() => { probes++; throw new TimeoutException("the probe must not run"); }, async () =>
            {
                var root = Parse(await CallAsync(postgres, tool, name, 1, end));

                Assert.Equal(0, probes);
                Assert.True(root.GetProperty(tool.ArrayKey).GetArrayLength() > 0, tool.Name);
                Assert.False(root.GetProperty("window_truncated").GetBoolean(), tool.Name);
            });
        });
    }

    [Fact]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await ForEachToolAsync("probefail", async (connection, postgres, tool, name, end) =>
        {
            await SeedAsync(connection, tool, name, end.AddDays(-2), end.AddDays(-1), ct);

            await WithProbeAsync(() => throw new TimeoutException("the probe's deadline passed"), async () =>
            {
                var root = Parse(await CallAsync(postgres, tool, name, 168, end));

                Assert.False(root.TryGetProperty("error", out _), tool.Name);
                Assert.Equal(3, root.GetProperty(tool.ArrayKey).GetArrayLength());
                Assert.False(root.TryGetProperty("effective_start", out _), tool.Name);
                Assert.False(root.TryGetProperty("window_truncated", out _), tool.Name);
                Assert.False(root.TryGetProperty("truncation_note", out _), tool.Name);

                var empty = Parse(await CallAsync(postgres, tool, name, 1, end.AddDays(-5)));
                Assert.False(empty.TryGetProperty("hints", out _), tool.Name);
            });
        });
    }
}
