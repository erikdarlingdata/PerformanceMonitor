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
using Darling.Tests;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR3 (lane W3): the six blocking and waits reads the Blocking and Current Waits panels drive, called the way the page calls
/// them, through the web read dispatch with two REPEATED <c>database_name</c> keys over a seed of databases A, B and C. Each read
/// must answer with A's and B's rows and none of C's. On the base the dispatch ignored the key, so each read returned all three.
/// </summary>
[Collection("live-postgres")]
public sealed class BlockingWaitsWebDispatchLiveTests
{
    private const string ServerName = "darling-w3-blocking-waits-web";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static readonly string[] Reads =
    [
        "get_waiting_tasks", "get_blocking", "get_blocked_process_xml",
        "get_blocking_trend", "get_current_waits_trend", "get_blocking_stats",
    ];

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", ServerName), new("hours_back", "2") };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    /// <summary>The databases (or, for the two count series, the total count) a read's answer covers.</summary>
    private static (List<string> Databases, long Count) CoverageOf(string read, string json)
    {
        var root = JsonDocument.Parse(json).RootElement;
        List<string> Databases(string property)
        {
            Assert.True(root.TryGetProperty(property, out var rows), $"{read}: not the success shape: {json}");
            return rows.EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();
        }
        long Total(string property, string field)
        {
            Assert.True(root.TryGetProperty(property, out var rows), $"{read}: not the success shape: {json}");
            return rows.EnumerateArray().Sum(p => p.GetProperty(field).GetInt64());
        }
        return read switch
        {
            "get_waiting_tasks" => (Databases("tasks"), 0),
            "get_blocking" => (Databases("events"), 0),
            "get_blocked_process_xml" => (Databases("reports"), 0),
            "get_current_waits_trend" => (Databases("blocked_sessions"), 0),
            "get_blocking_trend" => ([], Total("trend", "count")),
            _ => ([], Total("blocking_duration", "event_count")),
        };
    }

    [Fact]
    public async Task EachOfTheSixReads_WithTwoChosenDatabases_ReturnsOnlyThoseDatabasesRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live blocking and waits web dispatch tests.");
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database, so no other class's chunks shape the store under test (#4650).
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var cs = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(cs, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(cs);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            var spid = 100;
            foreach (var db in new[] { "A", "B", "C" })
            {
                spid++;
                var eventTime = DarlingMcpTestData.Naive(now.AddMinutes(-10));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, lock_mode,
     blocked_sql_text, blocking_sql_text, blocked_process_report_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,0,$8,0,1000,'X','SELECT 1','UPDATE t SET c = 1','<blocked-process-report/>')",
                    CollectionIdGenerator.Next(), eventTime.AddSeconds(30), ServerId, ServerName, eventTime, db, spid, 900 + spid);

                /* A blocked waiting task per database, so the blocked-session series and the waiting-task list both have a row each. */
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,'LCK_M_S',3000,90,NULL,$6)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-10)), ServerId, ServerName, spid, db);
            }

            /* The collectors ran, so a trend's empty answer is an all-clear and not a gap. */
            foreach (var collector in new[] { "blocked_process_report", "waiting_tasks" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1,$2,$3,$4,$5,100,'SUCCESS',0)",
                    CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(now.AddMinutes(-30)));
            }

            foreach (var read in Reads)
            {
                var all = CoverageOf(read, await WebReadAsync(postgres, read));
                var chosen = CoverageOf(read, await WebReadAsync(postgres, read, "A", "B"));
                if (read is "get_blocking_trend" or "get_blocking_stats")
                {
                    Assert.True(all.Count == 3, $"{read}: the control (no database_name) counted {all.Count}, not 3");
                    Assert.True(chosen.Count == 2, $"{read} over A and B counted {chosen.Count}, not 2");
                }
                else
                {
                    Assert.True(new[] { "A", "B", "C" }.SequenceEqual(all.Databases), $"{read}: the control covered [{string.Join(",", all.Databases)}]");
                    Assert.True(new[] { "A", "B" }.SequenceEqual(chosen.Databases), $"{read} over A and B covered [{string.Join(",", chosen.Databases)}]");
                }

                /* A blank-only list is refused, never widened to "all databases". */
                var refused = JsonDocument.Parse(await WebReadAsync(postgres, read, "  ", "")).RootElement;
                Assert.True(refused.GetProperty("status").GetString() == "invalid", $"{read}: a blank-only database_name must be refused");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt,
                    $"DELETE FROM waiting_tasks WHERE server_id = {ServerId}; DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};"));
        }
    }
}
