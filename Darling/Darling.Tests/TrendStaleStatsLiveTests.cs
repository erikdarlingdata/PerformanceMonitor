/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5425: every trend read <see cref="TrendPayloadBudgetLiveTests"/> covers, run against the planner state that
/// made <c>get_pg_io_trend</c> answer "Exception while reading from stream" (its 30 s read deadline).
///
/// <para>One store holds many servers, so a table's statistics routinely describe other rows: an autoanalyze that
/// ran while the table held another server's data, or none. The planner then estimates one row for this server's
/// window, picks a nested loop for any join, and re-runs whatever sits on the inner side once per outer row.
/// Two reads had such a join over a windowed pass: <c>get_pg_io_trend</c> (the interval to the previous
/// collection; 64.7 s over 72 hours, 60 ms after the fix) and <c>get_file_io_trend</c> (every file I/O row joined
/// to the ranking of the series; 84.6 s over a week of the 609-series census shape, 2 s after the fix). Every
/// other read is a single pass over its table and measured under 1.5 s on the same state.</para>
///
/// <para>This fixture builds that state on purpose, for all twelve tables the census seeds: statistics taken over
/// another server's rows from well before the window, then those rows deleted, then the real server's rows
/// planted. Autovacuum is switched off on the tables for the duration (best effort: a hypertable's chunks keep
/// their own setting), so an autoanalyze cannot heal the state between planting and reading and turn this into a
/// test of nothing. The reads go through the tools, with the real 30 s deadline: a read that goes quadratic is
/// cancelled at the deadline and the tool answers an error envelope, which fails the status check below.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TrendStaleStatsLiveTests
{
    private const string ServerName = "trend-stale-stats";
    private const string OtherServerName = "trend-stale-stats-other";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int OtherServerId = ServerIdHelper.GetDeterministicHashCode(OtherServerName);

    /// <summary>The same shape <c>TrendPayloadBudgetLiveTests.SeedAsync</c> plants.</summary>
    private const string PgQueryIdText = "987654321";

    private static readonly string[] Tables =
    {
        "file_io_stats", "wait_stats", "query_stats", "pg_io_stats", "pg_database_stats",
        "cpu_utilization_stats", "tempdb_stats", "memory_stats", "memory_grant_stats", "perfmon_stats", "pg_statement_stats",
        "pg_cpu_utilization",
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EveryTrendRead_AnswersWithinTheDeadline_WhenThePlannersRowEstimatesAreStale()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live stale-statistics trend reads.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await SetAutovacuumAsync(connection, "autovacuum_enabled = false", ct);

            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0).AddMinutes(-1);

            /* Statistics over ANOTHER server's rows, from well before this window, then those rows gone: the
               planner's picture of every table is now "nothing near this server_id or these times". */
            await TrendPayloadBudgetLiveTests.SeedAsync(connection, end.AddDays(-40), ct, OtherServerId, OtherServerName);
            foreach (var table in Tables)
            {
                using var analyze = new NpgsqlCommand($"ANALYZE {table}", connection) { CommandTimeout = 300 };
                await analyze.ExecuteNonQueryAsync(ct);
            }

            await DeleteRowsAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await TrendPayloadBudgetLiveTests.SeedAsync(connection, end, ct, ServerId, ServerName);

            var output = TestContext.Current.TestOutputHelper;
            var failures = new List<string>();
            foreach (var hours in new[] { 72, 168 })
            {
                var reads = new (string Tool, string PointsKey, Func<Task<string>> Run)[]
                {
                    ("get_file_io_trend", "trend", () => DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, hours)),
                    ("get_lock_wait_trend", "trend", () => DarlingMcpBlockingTools.GetLockWaitTrend(postgres, ServerName, hours)),
                    ("get_pg_io_trend", "points", () => DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, hours_back: hours)),
                    ("get_pg_database_trend", "points", () => DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, hours_back: hours)),
                    ("get_pg_cpu_utilization", "samples", () => DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(postgres, ServerName, hours_back: hours)),
                    ("get_wait_trend", "trend", () => DarlingMcpDataTools.GetWaitTrend(postgres, "LCK_M_S", ServerName, hours)),
                    ("get_cpu_utilization", "samples", () => DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName, hours)),
                    ("get_tempdb_trend", "trend", () => DarlingMcpDataTools.GetTempDbTrend(postgres, ServerName, hours)),
                    ("get_memory_trend", "trend", () => DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, hours)),
                    ("get_perfmon_trend", "trend", () => DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName, hours)),
                    ("get_pg_query_duration_trend", "points", () => DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, PgQueryIdText, hours)),
                    /* The raw query-stats tier holds four days: get_query_duration_trend is rostered to 72 h. */
                    ("get_query_duration_trend", "trend", () => DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, hours_back: Math.Min(hours, 72))),
                };

                foreach (var (tool, pointsKey, run) in reads)
                {
                    var clock = Stopwatch.StartNew();
                    var answer = await run();
                    clock.Stop();
                    output?.WriteLine($"{tool} {hours}h: {clock.ElapsedMilliseconds} ms");

                    var root = JsonDocument.Parse(answer).RootElement;
                    var status = root.TryGetProperty("status", out var s) ? s.GetString() : null;
                    if (status is not (null or "io_trend" or "database_trend" or "query_duration_trend"))
                    {
                        failures.Add($"{tool} over {hours}h answered {status} after {clock.ElapsedMilliseconds} ms: {answer[..Math.Min(answer.Length, 300)]}");
                    }
                    else if (root.GetProperty(pointsKey).GetArrayLength() == 0)
                    {
                        failures.Add($"{tool} over {hours}h returned no points");
                    }
                }
            }

            Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));

            /* #5425 first pass: the interval join of get_pg_io_trend, at the width and window it failed at.
               72 hours at 30-minute buckets is 144 buckets; the first snapshot has nothing to difference against
               and the last bucket may be partial. */
            var points = await DarlingPgTrendReader.GetIoTrendAsync(
                postgres, ServerId, "client backend", "normal", end.AddHours(-72), end, 30, ct);
            Assert.InRange(points.Count, 140, 146);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, token) =>
            {
                await DeleteRowsAsync(cleanup, token);
                await SetAutovacuumAsync(cleanup, null, token);
            });
        }
    }

    /// <summary>Turns autovacuum off (or, with <c>null</c>, back to the table's default) on every table the
    /// census seeds.</summary>
    private static async Task SetAutovacuumAsync(NpgsqlConnection connection, string? setting, CancellationToken ct)
    {
        foreach (var table in Tables)
        {
            var sql = setting is null ? $"ALTER TABLE {table} RESET (autovacuum_enabled)" : $"ALTER TABLE {table} SET ({setting})";
            using var alter = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
            await alter.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { ServerId, OtherServerId })
        {
            foreach (var table in Tables.Append("servers"))
            {
                using var delete = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection) { CommandTimeout = 300 };
                delete.Parameters.AddWithValue(id);
                await delete.ExecuteNonQueryAsync(ct);
            }
        }
    }
}
