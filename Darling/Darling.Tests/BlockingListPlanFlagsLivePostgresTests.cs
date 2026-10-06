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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5236: the plan-presence flags on <c>get_blocking</c> and <c>get_deadlocks</c> come from one keyed statement over the PAGE's
/// rows, not from the list read, so a compressed chunk decompresses a plan column only for the batches holding the page's own
/// rows. The list statements must stay free of every plan column (their planner output names none), and the flags must still be
/// right for a page whose rows carry a plan on both sides, one side, an empty string, or NULL, and must not leak onto rows
/// past the page.
/// </summary>
[Collection("live-postgres")]
public sealed class BlockingListPlanFlagsLivePostgresTests
{
    private const string ServerName = "darling-list-plan-flags-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Plan = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence/></ShowPlanXML>";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetBlocking_FlagsThePagesRows_ByTheirOwnStoredPlans()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live list plan flag test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* Newest first by event_time: spid 101 (both plans), 102 (blocked only), 103 (blocking only), 104 (empty strings),
               105 (NULLs), 106 (both plans; the oldest, so a page of five leaves it out). */
            var seeded = new (int Spid, string? Blocked, string? Blocking)[]
            {
                (101, Plan, Plan), (102, Plan, null), (103, null, Plan), (104, "", ""), (105, null, null), (106, Plan, Plan),
            };
            for (var i = 0; i < seeded.Length; i++)
            {
                var eventTime = now.AddMinutes(-10 - i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, lock_mode,
     blocked_sql_text, blocking_sql_text, blocked_process_report_xml, blocked_query_plan_xml, blocking_query_plan_xml)
VALUES ($1,$2,$3,$4,$5,'PlanFlagsDb',$6,0,$7,0,1000,'X','SELECT 1','UPDATE t SET c = 1','<blocked-process-report/>',$8,$9)",
                    CollectionIdGenerator.Next(), eventTime.AddMinutes(1), ServerId, ServerName, eventTime,
                    seeded[i].Spid, 900 + i, (object?)seeded[i].Blocked ?? DBNull.Value, (object?)seeded[i].Blocking ?? DBNull.Value);
            }

            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, limit: 5)))
            {
                var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();
                Assert.Equal(new[] { 101, 102, 103, 104, 105 }, events.Select(e => e.GetProperty("blocked_spid").GetInt32()).ToArray());
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());

                /* Written only when true: a row with no plan carries neither key. */
                Assert.Equal(new[] { (true, true), (true, false), (false, true), (false, false), (false, false) },
                    events.Select(e => (Flag(e, "has_blocked_plan"), Flag(e, "has_blocking_plan"))).ToArray());
                Assert.False(events[3].TryGetProperty("has_blocked_plan", out _));
                Assert.False(events[4].TryGetProperty("has_blocking_plan", out _));
            }

            /* A page of two: the flags are for those two rows, and the rest of the window is not read for them. */
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, limit: 2)))
            {
                var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();
                Assert.Equal(2, events.Count);
                Assert.Equal(new[] { (true, true), (true, false) },
                    events.Select(e => (Flag(e, "has_blocked_plan"), Flag(e, "has_blocking_plan"))).ToArray());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task GetDeadlocks_FlagsThePagesRows_ByTheirOwnStoredPlans()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live list plan flag test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* Newest first by deadlock_time: a plan, an empty string, NULL, and an older plan the page of three leaves out. */
            var seeded = new (string Victim, string? Plan)[] { ("processA", Plan), ("processB", ""), ("processC", null), ("processD", Plan) };
            for (var i = 0; i < seeded.Length; i++)
            {
                var deadlockTime = now.AddMinutes(-10 - i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO deadlocks
    (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml, victim_query_plan_xml, database_name)
VALUES ($1,$2,$3,$4,$5,$6,'UPDATE t SET c = 1','<deadlock/>',$7,'PlanFlagsDb')",
                    CollectionIdGenerator.Next(), deadlockTime.AddMinutes(1), ServerId, ServerName, deadlockTime,
                    seeded[i].Victim, (object?)seeded[i].Plan ?? DBNull.Value);
            }

            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetDeadlocks(postgres, ServerName, limit: 3)))
            {
                var rows = doc.RootElement.GetProperty("deadlocks").EnumerateArray().ToList();
                Assert.Equal(new[] { "processA", "processB", "processC" }, rows.Select(r => r.GetProperty("victim_process_id").GetString()).ToArray());
                Assert.Equal(new[] { true, false, false }, rows.Select(r => Flag(r, "has_victim_plan")).ToArray());
                Assert.False(rows[1].TryGetProperty("has_victim_plan", out _));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The planner's own output for each list statement names no plan column, on the store as it is built here (a
    /// hypertable where TimescaleDB is present). A flag computed in the list is what put the plan text through its sorts.</summary>
    [Fact]
    public async Task TheListStatements_PlannerOutput_NamesNoPlanColumn()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live list plan flag test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        foreach (var sql in new[]
        {
            DarlingBlockingReader.BlockedProcessReportsSql,
            DarlingBlockingReader.BlockedProcessReportsWithXmlSql,
            DarlingBlockingReader.RecentDeadlocksSql,
            DarlingBlockingReader.RecentDeadlocksWithGraphSql,
        })
        {
            using var explain = new NpgsqlCommand("EXPLAIN (VERBOSE, COSTS OFF) " + sql, connection);
            DarlingMcpReadParameters.AddWindow(explain, ServerId, now.AddHours(-24), now);
            DarlingMcpReadParameters.AddInt(explain, 21);
            DarlingMcpReadParameters.AddTimestamp(explain, EventWindowFloor.For(now.AddHours(-24)));
            var lines = new List<string>();
            await using (var reader = await explain.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    lines.Add(reader.GetString(0));
                }
            }

            Assert.DoesNotContain(lines, line => line.Contains("query_plan_xml", StringComparison.Ordinal));
        }
    }

    private static bool Flag(JsonElement row, string name) =>
        row.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.True;

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
