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
/// The window-floor notice of the sparse PostgreSQL reads (blocking, session states, replication stats) and the xmin horizon
/// (#4966). The three sparse reads are probed on their collector's logged runs, the xmin horizon on its table's schedule edge.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPgWindowD8bLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Tool(string Read, string Table, string Collector, bool RunsProbed, Func<NpgsqlDataSource, string, int, DateTime, Task<string>> Call, Func<NpgsqlConnection, string, DateTime, Task> SeedRow);

    private static string Name(string read, string window) => "pgwin-" + read.Replace("get_pg_", "").Replace('_', '-') + "-" + window;

    private static Task Exec(NpgsqlConnection c, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, sql, values);

    private static int Id(string name) => ServerIdHelper.GetDeterministicHashCode(name);

    private static string AsOf(DateTime end) => WebDataStartNote.FormatWindowEnd(end);

    private static readonly Tool[] Tools =
    [
        new("get_pg_blocking", "pg_blocking_edges", "pg_blocking", true,
            (ds, n, h, end) => DarlingMcpPgBlockingTools.GetPgBlocking(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_blocking_edges (collection_id, collection_time, server_id, server_name, blocked_backend_id, blocked_pid, blocking_backend_id, blocking_pid, database_name, blocked_username, blocked_application_name, blocked_client_addr, blocked_state, blocked_wait_event_type, blocked_wait_event, blocked_query, blocked_xact_duration_ms, blocked_query_duration_ms, blocking_username, blocking_application_name, blocking_client_addr, blocking_state, blocking_wait_event_type, blocking_wait_event, blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms, blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated) VALUES ($1, $2, $3, $4, 1, 100, 2, 200, 'appdb', 'user1', 'app', '10.0.0.1', 'active', 'Lock', 'relation', 'select 1', 100, 100, 'user2', 'app', '10.0.0.2', 'idle in transaction', NULL, NULL, 'select 2', 200, 200, 1, TRUE, FALSE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
        new("get_pg_session_states", "pg_session_states", "pg_session_states", true,
            (ds, n, h, end) => DarlingMcpPgSessionStatesTools.GetPgSessionStates(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c, @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, 5, 500, 'appdb', 'u', 'app', NULL,
        'client backend', 'idle in transaction', NULL, NULL, 'SELECT', NULL,
        900000, 900000, 900000, 900000, 100, 100, 100,
        true, true, false,
        3, 1, 1, 3)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
        new("get_pg_replication_stats", "pg_replication_stats", "pg_replication_stats", true,
            (ds, n, h, end) => DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_replication_stats (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state, sync_priority, sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start) VALUES ($1, $2, $3, $4, 'replica1', '10.0.0.2', 'streaming', 'async', 0, 0, 0, 0, 0, 1.0, 1.0, 1.0, $2)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
        new("get_pg_xmin_horizon", "pg_xmin_horizon", "pg_xmin_horizon", false,
            (ds, n, h, end) => DarlingMcpPgXminTools.GetPgXminHorizon(ds, n, h, as_of: AsOf(end)),
            async (c, n, at) => await Exec(c,
                "INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner) VALUES ($1, $2, $3, $4, 'session', 5000, 'pid 42', NULL, TRUE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), Id(n), n)),
    ];

    public static IEnumerable<object[]> ToolNames() => Tools.Select(t => new object[] { t.Read });

    private static Tool Of(string read) => Tools.Single(t => t.Read == read);

    private static Task RunAsync(Tool t, string window, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, string, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [t.Collector], [Name(t.Read, window)], [t.Table],
            (c, ds, end) => body(c, ds, end, Name(t.Read, window)));

    private static Task SeedServerAsync(NpgsqlConnection c, Tool t, string name, DateTime created, DateTime? runsFrom, DateTime end) =>
        WindowFloorLiveHarness.SeedServerAsync(c, name, created, t.Collector, runsFrom, 30, end, [t.Table], TestContext.Current.CancellationToken);

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task ADataAnswer_ForAServerAddedTwoDaysAgo_NamesWhereCoverageStarts_RightAfterHoursBack_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "added", async (c, ds, end, name) =>
        {
            var added = end.AddDays(-2);
            await SeedServerAsync(c, t, name, added, added, end);
            await t.SeedRow(c, name, end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean(), root.ToString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(added, end.AddHours(-168), DarlingMcpWindowNotice.TableFor(read)).TruncationNote, root.GetProperty("truncation_note").GetString());

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });
    }

    /// <summary>The collector ran all week and stored nothing until a day back: its first row says nothing about when collection began.</summary>
    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task AQuietStart_WhereTheCollectorRanButStoredNoRowsEarly_GivesNoNotice_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "quiet", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), end.AddDays(-9), end);
            await t.SeedRow(c, name, end.AddDays(-1));

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
            Assert.True(root.TryGetProperty("replicas", out _) || root.GetProperty("status").GetString() is "blocking_sampled" or "session_states" or "holder_present", root.ToString());
        });
    }

    /// <summary>An all-clear over a window that begins before collection did: nothing was stored, but the collector's runs show where it started.</summary>
    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task AnAllClear_PastCoverage_CarriesHints_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "empty", async (c, ds, end, name) =>
        {
            var began = end.AddMinutes(-30);
            await SeedServerAsync(c, t, name, began, began, end);

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 3, end));

            Assert.False(root.TryGetProperty("window_truncated", out _), "an empty answer carries the keys under hints, not at the top: " + root);
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean(), root.ToString());
            Assert.Equal(McpHelpers.FormatEffectiveStart(began), hints.GetProperty("effective_start").GetString());
            Assert.False(string.IsNullOrEmpty(hints.GetProperty("truncation_note").GetString()));
        });
    }

    /// <summary>The quiet claim (#4966): over a window the collector began inside, the answer names where the data starts instead of calling the stretch before it quiet; over a covered window it keeps its words.</summary>
    [Theory]
    [InlineData("get_pg_replication_stats", "that is the expected answer", false)]
    [InlineData("get_pg_xmin_horizon", "Vacuum is free to reclaim dead rows", true)]
    [InlineData("get_pg_session_states", "a real all-clear rather than missing data", false)]
    public async Task AnAllClear_ClaimsQuiet_OnlyOverACoveredWindow_AgainstDevPostgres(string read, string coveredClaim, bool inFinding)
    {
        var t = Of(read);
        string Text(JsonElement root) => inFinding ? root.GetProperty("finding").GetString()! : root.GetProperty("message").GetString()!;

        await RunAsync(t, "quietclaim", async (c, ds, end, name) =>
        {
            var began = end.AddMinutes(-30);
            await SeedServerAsync(c, t, name, began, began, end);
            var cut = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 3, end));
            Assert.True(cut.GetProperty("hints").GetProperty("window_truncated").GetBoolean(), cut.ToString());
            Assert.Contains(". " + McpHelpers.CutWindowNothingMessage, Text(cut), StringComparison.Ordinal);
            Assert.DoesNotContain(coveredClaim, Text(cut), StringComparison.Ordinal);
            if (read == "get_pg_xmin_horizon")
            {
                /* The cut factual part claims only the captures the store holds, never the whole window. */
                Assert.Contains("in the captures the store holds", Text(cut), StringComparison.Ordinal);
                Assert.DoesNotContain("in this window", Text(cut), StringComparison.Ordinal);
                Assert.EndsWith(". " + McpHelpers.CutWindowNothingMessage, Text(cut), StringComparison.Ordinal);
            }

            await SeedServerAsync(c, t, name, end.AddDays(-30), end.AddDays(-30), end);
            var covered = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 3, end));
            Assert.False(covered.GetProperty("hints").GetProperty("window_truncated").GetBoolean(), covered.ToString());
            Assert.Contains(coveredClaim, Text(covered), StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(McpHelpers.CutWindowNothingMessage, Text(covered), StringComparison.Ordinal);
        });
    }

    [Theory]
    [MemberData(nameof(ToolNames))]
    public async Task ANotCollectedAnswer_StaysBare_AndStartsNoProbe_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "notcollected", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), null, end);
            await Exec(c, "UPDATE servers SET sql_engine_edition = $2, engine_kind = $3 WHERE server_id = $1", Id(name), 3, "sqlserver");
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 168, end));

            Assert.Equal("not_collected", root.GetProperty("status").GetString());
            Assert.False(root.TryGetProperty("hints", out _));
            Assert.Equal(0, calls);
        });
    }

    /// <summary>No capture at all in the window is not an all-clear and carries no notice: blocking's not_sampled, session states' and xmin's unavailable.</summary>
    [Theory]
    [InlineData("get_pg_blocking")]
    [InlineData("get_pg_session_states")]
    [InlineData("get_pg_xmin_horizon")]
    public async Task ANoCaptureAnswer_StaysBare_AgainstDevPostgres(string read)
    {
        var t = Of(read);
        await RunAsync(t, "nocapture", async (c, ds, end, name) =>
        {
            await SeedServerAsync(c, t, name, end.AddDays(-30), null, end);

            var root = WindowFloorLiveHarness.Parse(await t.Call(ds, name, 1, end));

            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("hints", out var hints) && hints.TryGetProperty("window_truncated", out _), root.ToString());
            Assert.NotEqual("empty", root.GetProperty("status").GetString());
        });
    }
}
