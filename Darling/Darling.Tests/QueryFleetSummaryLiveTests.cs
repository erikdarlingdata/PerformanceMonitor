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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7609: <c>get_query_fleet_summary</c> answers "where does this statement run, and what does it cost on each server" in one
/// read. Three servers, two databases on one: the old hours sit in the hourly rollup (their raw rows purged, as retention
/// does), the newest hours are raw rows the rollup has not built. The sums come from both sides of the rollup's ceiling and
/// no hour is counted twice.
/// </summary>
public sealed class QueryFleetSummaryLiveTests
{
    private const string Hash = "0xQ7609";
    private static readonly (int Id, string Name)[] Servers = [(-760901, "a7609-one"), (-760902, "a7609-two"), (-760903, "a7609-three")];

    private static async Task InsertAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, string serverName,
        string db, DateTime at, string handle, long executions, long cpuUs, long elapsedUs, string hash = Hash)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 100, 1000, 3600)",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, hash, handle, executions, cpuUs, elapsedUs);
    }

    private static JsonElement[] Servers_(JsonDocument doc) => doc.RootElement.GetProperty("servers").EnumerateArray().ToArray();

    [Fact]
    public async Task OneRead_SumsEveryServerAndDatabase_AcrossTheRollupAndTheRawTail()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live fleet summary test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live fleet summary test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        foreach (var (id, name) in Servers)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var thisHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
            var oldHour = thisHour.AddDays(-10);
            var olderHour = oldHour.AddHours(-5);

            /* Rollup side, ten days back. Server one runs it in two databases (the first under two handles in one hour, and
               again five hours earlier), server two in one, server three in one. CPU order: two (9 s), one/Alpha (4 s),
               three (3 s), one/Beta (1 s). Another hash on server one must stay out. */
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Alpha", oldHour.AddMinutes(10), "0xH1", 10, 3_000_000, 5_000_000);
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Alpha", oldHour.AddMinutes(20), "0xH2", 5, 1_000_000, 2_000_000);
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Beta", olderHour.AddMinutes(10), "0xH1", 2, 1_000_000, 1_000_000);
            await InsertAsync(connection, ct, Servers[1].Id, Servers[1].Name, "Alpha", oldHour.AddMinutes(10), "0xH1", 40, 9_000_000, 12_000_000);
            await InsertAsync(connection, ct, Servers[2].Id, Servers[2].Name, "Gamma", olderHour.AddMinutes(10), "0xH1", 7, 3_000_000, 3_500_000);
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Alpha", oldHour.AddMinutes(10), "0xH1", 1000, 900_000_000, 900_000_000, "0xOTHER");

            foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(olderHour.AddHours(-1));
                refresh.Parameters.AddWithValue(oldHour.AddHours(2));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await using (var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time < $1", connection))
            {
                purge.Parameters.AddWithValue(thisHour.AddDays(-1));
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* A raw row left in a rolled-up hour (the rollup read already counted that hour): it must not be counted again. */
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Alpha", oldHour.AddMinutes(40), "0xH1", 999, 999_000_000, 999_000_000);

            /* Raw tail, past the rollup's end: server one/Alpha 3 more executions an hour ago, and a brand new pair, server three/Delta. */
            var recent = thisHour.AddHours(-1).AddMinutes(5);
            await InsertAsync(connection, ct, Servers[0].Id, Servers[0].Name, "Alpha", recent, "0xH1", 3, 500_000, 600_000);
            await InsertAsync(connection, ct, Servers[2].Id, Servers[2].Name, "Delta", recent, "0xH1", 1, 100_000, 100_000);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* 90 days: the rollup and the raw tail together. */
            var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 2160, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(json))
            {
                Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
                Assert.Equal(5, doc.RootElement.GetProperty("server_database_pairs").GetInt32());
                Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
                Assert.Equal(3, doc.RootElement.GetProperty("servers_returned").GetInt32());
                var rows = Servers_(doc);
                Assert.Equal(5, rows.Length);

                /* Ranked by CPU: two/Alpha 9000, one/Alpha 4500 (rollup 4000 + raw tail 500), three/Gamma 3000, one/Beta 1000, three/Delta 100. */
                Assert.Equal(new[] { "a7609-two", "a7609-one", "a7609-three", "a7609-one", "a7609-three" }, rows.Select(r => r.GetProperty("server_name").GetString()).ToArray());
                Assert.Equal(new[] { "Alpha", "Alpha", "Gamma", "Beta", "Delta" }, rows.Select(r => r.GetProperty("database_name").GetString()).ToArray());
                Assert.Equal(new[] { 9000.0, 4500.0, 3000.0, 1000.0, 100.0 }, rows.Select(r => r.GetProperty("cpu_ms").GetDouble()).ToArray());

                /* One/Alpha: 10 + 5 from the rollup (two handles, one hour), 3 from raw; the 999 raw row in a rolled-up hour is not added. */
                var oneAlpha = rows[1];
                Assert.Equal(18, oneAlpha.GetProperty("execution_count").GetInt64());
                Assert.Equal(7600.0, oneAlpha.GetProperty("elapsed_ms").GetDouble());
                Assert.StartsWith(oldHour.ToString("yyyy-MM-ddTHH:00:00"), oneAlpha.GetProperty("first_hour").GetString());
                Assert.StartsWith(thisHour.AddHours(-1).ToString("yyyy-MM-ddTHH:00:00"), oneAlpha.GetProperty("last_hour").GetString());
                Assert.Equal("plan_cache", oneAlpha.GetProperty("source").GetString());

                /* A pair that exists only in the rollup, and a pair that exists only in the raw tail. */
                Assert.Equal(7, rows[2].GetProperty("execution_count").GetInt64());
                Assert.Equal(1, rows[4].GetProperty("execution_count").GetInt64());
                Assert.StartsWith(olderHour.ToString("yyyy-MM-ddTHH:00:00"), rows[3].GetProperty("first_hour").GetString());
            }

            /* The cap: two rows, and the answer says it cut three of five. */
            var capped = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 2160, limit: 2, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(capped))
            {
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
                Assert.Equal(5, doc.RootElement.GetProperty("server_database_pairs").GetInt32());
                Assert.Equal(2, doc.RootElement.GetProperty("rows_returned").GetInt32());
                Assert.Equal(2, Servers_(doc).Length);
                Assert.Contains("2 of 5", doc.RootElement.GetProperty("note").GetString());
            }

            /* The default window is seven days: the rollup's ten-day-old hours are out, the raw tail is in. */
            var week = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(week))
            {
                var rows = Servers_(doc);
                Assert.Equal(2, rows.Length);
                Assert.All(rows, r => Assert.Equal("Alpha" == r.GetProperty("database_name").GetString() ? 3 : 1, r.GetProperty("execution_count").GetInt64()));
            }

            /* as_of puts the window's end back: ending 9 days ago, the raw tail is out and only the rollup's hours are summed. */
            var past = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 240,
                as_of: thisHour.AddDays(-9).ToString("yyyy-MM-ddTHH:mm:ss") + "Z", cancellationToken: ct);
            using (var doc = JsonDocument.Parse(past))
            {
                Assert.Equal(4, doc.RootElement.GetProperty("server_database_pairs").GetInt32());
                var oneAlpha = Servers_(doc).Single(r => r.GetProperty("server_name").GetString() == "a7609-one" && r.GetProperty("database_name").GetString() == "Alpha");
                Assert.Equal(15, oneAlpha.GetProperty("execution_count").GetInt64());
            }

            /* A hash that ran nowhere: empty, never an error. */
            var none = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, "0xNOWHERE", 2160, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(none))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }

            /* Past the reach and out-of-range caps are refused, never clamped. */
            Assert.Contains("exceeds maximum of 2160 hours", McpHelpers.ErrorMessageOf(await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 2161, cancellationToken: ct)));
            Assert.Contains("Must be 1-200", await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 168, limit: 201, cancellationToken: ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
