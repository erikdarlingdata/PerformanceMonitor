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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6292: the web's <c>get_block_chain</c> read returns the lead-blocker tree of one blocked-process row, as the Viewer's
/// Block Chain window does. Seeded on a scratch store: scan 1 holds a three-level chain (52 blocks 55, 55 blocks 58), scan 2,
/// at the same minute, holds an unrelated pair (70 blocks 71). The row for 58 must come back rooted at 52 with 55 under it and 58
/// under 55, with each node's own wait and session text; the row for 71 must come back as its own two-node tree, never merged
/// into scan 1's. A session in no chain answers one plain sentence, and an unreadable event_time is refused.
/// </summary>
[Collection("live-postgres")]
public sealed class BlockChainWebReadLiveTests
{
    private const string ServerName = "darling-block-chain-web-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live block-chain web read test (it mints its own scratch database).";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<JsonElement> ReadAsync(NpgsqlDataSource postgres, string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        var json = await DarlingWebEndpoints.BuildReadDispatch()["get_block_chain"](context, postgres, null!);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    private const int ServerOffsetMinutes = -420;

    /// <summary>A session's transaction start on the server's own clock: the session's number of minutes before the event, then seven hours behind UTC.</summary>
    private static DateTime ServerLocalTranStart(DateTime eventTimeUtc, int spid) =>
        eventTimeUtc.AddMinutes(-spid).AddMinutes(ServerOffsetMinutes);

    private static string Query(DateTime eventTime, int spid, int ecid = 0) =>
        "?server=" + ServerName + "&event_time=" + Uri.EscapeDataString(eventTime.ToString("o")) + "&blocked_spid=" + spid + "&blocked_ecid=" + ecid;

    [Fact]
    public async Task GetBlockChain_ThreeLevelChain_IsRootedAtTheLeadBlocker_WithEachSessionUnderItsBlocker()
    {
        await RunAsync(async (postgres, eventTime, ct) =>
        {
            /* The clicked row is the deepest victim: the tree still starts at the lead blocker. */
            var answer = await ReadAsync(postgres, Query(eventTime, 58));
            Assert.Equal("ok", answer.GetProperty("status").GetString());

            var apex = answer.GetProperty("chain");
            Assert.Equal(52, apex.GetProperty("spid").GetInt32());
            Assert.True(apex.GetProperty("is_apex").GetBoolean());
            Assert.Equal("alice<b>", apex.GetProperty("login_name").GetString());

            var middle = Assert.Single(apex.GetProperty("children").EnumerateArray());
            Assert.Equal(55, middle.GetProperty("spid").GetInt32());
            Assert.False(middle.GetProperty("is_apex").GetBoolean());
            Assert.Equal(2000, middle.GetProperty("wait_time_ms").GetInt64());
            Assert.Equal("X", middle.GetProperty("lock_mode").GetString());
            Assert.Equal("SalesDb", middle.GetProperty("database_name").GetString());
            Assert.Equal("SELECT 1", middle.GetProperty("sql_text").GetString());
            Assert.Equal("UPDATE t SET c = 1", apex.GetProperty("sql_text").GetString());

            var leaf = Assert.Single(middle.GetProperty("children").EnumerateArray());
            Assert.Equal(58, leaf.GetProperty("spid").GetInt32());
            Assert.Equal(3000, leaf.GetProperty("wait_time_ms").GetInt64());
            Assert.Empty(leaf.GetProperty("children").EnumerateArray());

            /* The same tree whichever session of the chain is asked for. */
            var fromMiddle = await ReadAsync(postgres, Query(eventTime, 55));
            Assert.Equal(52, fromMiddle.GetProperty("chain").GetProperty("spid").GetInt32());

            /* The other scan's pair is its own tree: scan 1's chain is not merged into it, nor it into scan 1's. */
            var other = await ReadAsync(postgres, Query(eventTime, 71));
            var otherApex = other.GetProperty("chain");
            Assert.Equal(70, otherApex.GetProperty("spid").GetInt32());
            Assert.Equal(71, Assert.Single(otherApex.GetProperty("children").EnumerateArray()).GetProperty("spid").GetInt32());
        });
    }

    [Fact]
    public async Task GetBlockChain_TransactionStarts_AreServedInUtc_NotAsTheServersLocalClock()
    {
        await RunAsync(async (postgres, eventTime, ct) =>
        {
            var answer = await ReadAsync(postgres, Query(eventTime, 58));
            var apex = answer.GetProperty("chain");
            var middle = Assert.Single(apex.GetProperty("children").EnumerateArray());
            var leaf = Assert.Single(middle.GetProperty("children").EnumerateArray());

            /* The page reads each stamp as UTC, so each must be the instant the transaction began: its session's number of minutes
               before the event, whatever the server's own clock said. Stored as seen, these read seven hours early. */
            foreach (var (node, spid) in new[] { (apex, 52), (middle, 55), (leaf, 58) })
            {
                var started = DateTime.Parse(node.GetProperty("tran_started").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                Assert.Equal(eventTime.AddMinutes(-spid), started);
            }
        });
    }

    [Fact]
    public async Task GetBlockChain_ASessionInNoChain_SaysSo_AndABadEventTimeIsRefused()
    {
        await RunAsync(async (postgres, eventTime, ct) =>
        {
            var none = await ReadAsync(postgres, Query(eventTime, 999));
            Assert.Equal("empty", none.GetProperty("status").GetString());
            Assert.Contains("999", none.GetProperty("message").GetString(), StringComparison.Ordinal);

            var farAway = await ReadAsync(postgres, Query(eventTime.AddHours(-3), 58));
            Assert.Equal("empty", farAway.GetProperty("status").GetString());

            var refused = await ReadAsync(postgres, "?server=" + ServerName + "&event_time=yesterday&blocked_spid=58");
            Assert.Equal("invalid", refused.GetProperty("status").GetString());
            Assert.Equal("event_time", refused.GetProperty("hints").GetProperty("parameter").GetString());
        });
    }

    /// <summary>Seeds the two scans, then runs the body with the scans' shared event time.</summary>
    private static async Task RunAsync(Func<NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database, so no other class's chunks shape the store under test (#4650).
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        var cs = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(cs, connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var eventTime = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10));

            /* The monitored server runs on UTC-7 (a fixed offset, no zone id): the transaction stamps in its reports are its own
               wall clock, seven hours behind the UTC event_time they sit beside. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES ($1,$2,$3,$4,$5)",
                CollectionIdGenerator.Next(), eventTime.AddMinutes(-5), ServerId, ServerName, ServerOffsetMinutes);

            /* (monitor_loop, blocked, blocking, wait ms, blocking login) */
            var pairs = new (int Loop, int Blocked, int Blocking, int Wait, string BlockingLogin)[]
            {
                (1, 55, 52, 2000, "alice<b>"),
                (1, 58, 55, 3000, "bob"),
                (2, 71, 70, 1500, "carol"),
            };
            foreach (var (loop, blocked, blocking, wait, login) in pairs)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, lock_mode,
     blocked_sql_text, blocking_sql_text, blocked_process_report_xml, monitor_loop,
     blocking_login_name, blocked_login_name, blocked_last_tran_started, blocking_last_tran_started)
VALUES ($1,$2,$3,$4,$5,'SalesDb',$6,0,$7,0,$8,'X','SELECT 1','UPDATE t SET c = 1','<blocked-process-report/>',$9,$10,'victim',$11,$12)",
                    CollectionIdGenerator.Next(), eventTime.AddSeconds(30), ServerId, ServerName, eventTime, blocked, blocking, wait, loop, login,
                    ServerLocalTranStart(eventTime, blocked), ServerLocalTranStart(eventTime, blocking));
            }

            await body(postgres, eventTime, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PrepareStoreAsync(string connectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; DELETE FROM server_properties WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
