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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6073: get_query_store_aborts over seeded Query Store rows. The seed holds a cheap query that aborts often, an expensive query that
/// aborted once, a Regular-only query, and an exception-only query in a second database. as_of is fixed against an anchor a few hours
/// back, so no clock decides what the window holds.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreAbortsLiveTests
{
    private const string ServerName = "darling-qs-aborts-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "AbortsDb";
    private const string OtherDb = "AbortsOtherDb";

    private static readonly DateTime Anchor = DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-3), DateTimeKind.Unspecified);
    private static readonly DateTime End = Anchor.AddHours(2);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string AsOf => End.ToString("o", CultureInfo.InvariantCulture);

    /// <summary>
    /// Query 1 (cheap, aborts often): 10 Regular runs at 5 ms; aborted runs in two intervals, the first collected twice (2, then 4 runs at 400 ms:
    /// only the 4 counts), the second 4 runs at 600 ms. Aborted 8, average 500 ms, total 18.
    /// Query 2 (expensive, aborted once): 100,000 Regular runs at 5 s, one Aborted run at 30 s. Query 3: 500 Regular runs only.
    /// Query 4, in the other database: 7 Regular runs and 3 Exception runs.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await RegisterServerAsync(c, ct);
        var first = Anchor.AddMinutes(-60);
        await InsertAsync(c, Db, 1, 11, 1, Anchor.AddMinutes(1), Anchor, 10, 5_000, "Regular", ct);
        await InsertAsync(c, Db, 1, 11, 2, Anchor.AddMinutes(1), first, 2, 400_000, "Aborted", ct);
        await InsertAsync(c, Db, 1, 11, 2, Anchor.AddMinutes(2), first, 4, 400_000, "Aborted", ct);
        await InsertAsync(c, Db, 1, 11, 3, Anchor.AddMinutes(3), Anchor, 4, 600_000, "Aborted", ct);
        await InsertAsync(c, Db, 2, 21, 1, Anchor.AddMinutes(1), Anchor, 100_000, 5_000_000, "Regular", ct);
        await InsertAsync(c, Db, 2, 21, 2, Anchor.AddMinutes(1), Anchor, 1, 30_000_000, "Aborted", ct);
        await InsertAsync(c, Db, 3, 31, 1, Anchor.AddMinutes(1), Anchor, 500, 1_000_000, "Regular", ct);
        await InsertAsync(c, OtherDb, 4, 41, 1, Anchor.AddMinutes(1), Anchor, 7, 2_000, "Regular", ct);
        await InsertAsync(c, OtherDb, 4, 41, 2, Anchor.AddMinutes(1), Anchor, 3, 2_000, "Exception", ct);
    }

    private static async Task<JsonDocument> ReadAsync(NpgsqlDataSource postgres, CancellationToken ct, int top = 20, string? database = null, int hours = 24) =>
        JsonDocument.Parse(await DarlingMcpQueryStoreAbortsTools.GetQueryStoreAborts(
            postgres, ServerName, hours, top, database, AsOf, cancellationToken: ct));

    private static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live aborts test.");
        var ct = TestContext.Current.CancellationToken;
        /* A scratch database of its own, as ForcedPlanStateLiveTests does: on the shared DARLING_TEST_PG database, CI
           saw this class's server insert fail with 42P01 (relation "servers" does not exist) after another class ran in the same shard. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        cs = scratch.ConnectionString;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(cs, ct);
        await DeleteRowsAsync(connection, ct);
        /* The body's seed connections come from this data source too, so seed and read share the scratch database. */
        await using var postgres = NpgsqlDataSource.Create(cs);
        var ok = false;
        try
        {
            await body(postgres, ct);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public Task ACheapQueryThatAbortsOften_RanksAboveAnExpensiveQueryThatAbortedOnce_AndRegularOnlyQueriesAreAbsent() => RunAsync(async (postgres, ct) =>
    {
        await using (var seed = await postgres.OpenConnectionAsync(ct)) { await SeedAsync(seed, ct); }
        using var doc = await ReadAsync(postgres, ct);
        var ids = doc.RootElement.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("query_id").GetInt64()).ToArray();
        /* 8 failed runs, then 3, then 1; query 3 has none and is not listed. */
        Assert.Equal(new long[] { 1, 4, 2 }, ids);
        Assert.DoesNotContain(3L, ids);
    });

    [Fact]
    public Task TheShare_IsTheQuerysOwnFailedRunsOverItsOwnTotalRuns_AndTheCountsFoldEachIntervalOnce() => RunAsync(async (postgres, ct) =>
    {
        await using (var seed = await postgres.OpenConnectionAsync(ct)) { await SeedAsync(seed, ct); }
        using var doc = await ReadAsync(postgres, ct);
        var rows = doc.RootElement.GetProperty("queries").EnumerateArray().ToDictionary(q => q.GetProperty("query_id").GetInt64());
        var cheap = rows[1];
        Assert.Equal(8, cheap.GetProperty("aborted_runs").GetInt64());
        Assert.Equal(0, cheap.GetProperty("exception_runs").GetInt64());
        Assert.Equal(10, cheap.GetProperty("regular_runs").GetInt64());
        Assert.Equal(18, cheap.GetProperty("total_runs").GetInt64());
        Assert.Equal(Math.Round(8.0 / 18, 4), cheap.GetProperty("failed_share").GetDouble(), 6);
        var expensive = rows[2];
        Assert.Equal(1, expensive.GetProperty("failed_runs").GetInt64());
        Assert.Equal(100_001, expensive.GetProperty("total_runs").GetInt64());
        Assert.Equal(Math.Round(1.0 / 100_001, 4), expensive.GetProperty("failed_share").GetDouble(), 6);
        var exception = rows[4];
        Assert.Equal(3, exception.GetProperty("exception_runs").GetInt64());
        Assert.Equal(0.3, exception.GetProperty("failed_share").GetDouble(), 6);
    });

    [Fact]
    public Task TheAverageDuration_CountsAbortedRunsOnly() => RunAsync(async (postgres, ct) =>
    {
        await using (var seed = await postgres.OpenConnectionAsync(ct)) { await SeedAsync(seed, ct); }
        using var doc = await ReadAsync(postgres, ct);
        var rows = doc.RootElement.GetProperty("queries").EnumerateArray().ToDictionary(q => q.GetProperty("query_id").GetInt64());
        /* (4 x 400 ms + 4 x 600 ms) / 8 = 500 ms. The 10 Regular runs at 5 ms would pull a mixed average to about 280 ms. */
        Assert.Equal(500.0, rows[1].GetProperty("avg_aborted_duration_ms").GetDouble(), 1);
        Assert.Equal(30_000.0, rows[2].GetProperty("avg_aborted_duration_ms").GetDouble(), 1);
        /* No aborted run, so no aborted average: an exception-only query has none. */
        Assert.Equal(JsonValueKind.Null, rows[4].GetProperty("avg_aborted_duration_ms").ValueKind);
        /* The newest time a run aborted or failed. */
        Assert.StartsWith(Anchor.ToString("yyyy-MM-dd'T'HH:mm", CultureInfo.InvariantCulture), rows[1].GetProperty("last_failed_time").GetString());
    });

    [Fact]
    public Task TheDatabaseFilter_AndTheRowLimit_Hold() => RunAsync(async (postgres, ct) =>
    {
        await using (var seed = await postgres.OpenConnectionAsync(ct)) { await SeedAsync(seed, ct); }
        using (var one = await ReadAsync(postgres, ct, database: OtherDb))
        {
            Assert.Equal(new long[] { 4 }, one.RootElement.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("query_id").GetInt64()));
            Assert.Equal(OtherDb, one.RootElement.GetProperty("database_name").GetString());
        }

        using var capped = await ReadAsync(postgres, ct, top: 2);
        Assert.Equal(new long[] { 1, 4 }, capped.RootElement.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("query_id").GetInt64()));
        Assert.True(capped.RootElement.GetProperty("truncated").GetBoolean());
        using var all = await ReadAsync(postgres, ct, top: 3);
        Assert.False(all.RootElement.GetProperty("truncated").GetBoolean());
    });

    [Fact]
    public Task AnEmptyAnswer_SaysWhy_ForNoAbortedRuns_AndForNoData() => RunAsync(async (postgres, ct) =>
    {
        /* Runs, but none aborted: a measured zero. */
        await using (var seed = await postgres.OpenConnectionAsync(ct))
        {
            await RegisterServerAsync(seed, ct);
            await InsertAsync(seed, Db, 3, 31, 1, Anchor.AddMinutes(1), Anchor, 500, 1_000_000, "Regular", ct);
        }

        using (var zero = await ReadAsync(postgres, ct))
        {
            Assert.Equal("empty", zero.RootElement.GetProperty("status").GetString());
            Assert.Contains("aborted or exception run", zero.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        /* No Query Store rows at all in a window the rows are not in. */
        using var none = await ReadAsync(postgres, ct, hours: 1);
        var status = none.RootElement.GetProperty("status").GetString();
        Assert.NotEqual("ok", status);
        Assert.DoesNotContain("aborted or exception run", none.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    });

    [Fact]
    public Task BothStatements_RunAgainstTheStore_AndTheTopLimitIsRefusedAboveTheCap() => RunAsync(async (postgres, ct) =>
    {
        /* The interval table's statement, run directly: the table holds nothing for this server, so it answers no rows, and a bad column or parameter would throw. */
        await using (var command = postgres.CreateCommand(DarlingQueryStoreAbortsReader.TableSql))
        {
            command.CommandTimeout = 60;
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, Anchor.AddHours(-24));
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, End);
            command.Parameters.AddWithValue(5);
            command.Parameters.Add(DatabaseFilter.All.Parameter());
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.False(await reader.ReadAsync(ct));
        }

        using var refused = JsonDocument.Parse(await DarlingMcpQueryStoreAbortsTools.GetQueryStoreAborts(
            postgres, ServerName, 24, DarlingMcpQueryStoreAbortsTools.MaxTop + 1, null, AsOf, cancellationToken: ct));
        Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
    });

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
    is_enabled = TRUE, modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, string database, long queryId, long planId, long intervalId, DateTime collectionTime, DateTime firstExecution,
        long executions, double avgDurationUs, string outcome, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, query_hash, plan_id, runtime_stats_interval_id,
     first_execution_time, last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, is_forced_plan, execution_type_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11, $12, $12, FALSE, $13)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue("0xABORT" + queryId.ToString(CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(intervalId);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(firstExecution, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(avgDurationUs);
        command.Parameters.AddWithValue(outcome);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        cleanup.CommandTimeout = 60;
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
