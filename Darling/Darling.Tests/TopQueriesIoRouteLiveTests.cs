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
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: each test mints a scratch database, because it materializes continuous aggregates (and one drops
   one), which no other class sharing the store may see. */
/// <summary>
/// #5329 lane B1, live: the Top Queries reader on an Hourly-tier window with the io hourly rollup absent, empty, partial
/// and covering. The oracle seed is lane A's <see cref="IoRollupOracleSeed"/> (a NULL-reads group, a reads tie, two
/// sql_handles under one query_hash, two databases, a zero-interval row the rollups drop and a NULL-interval row they keep).
///
/// <para>The oracle: over the SAME window, the reads ranking answered from <c>query_stats_io_hourly</c> equals the raw route
/// (keys, order and every total, the reads, physical reads and writes included), and the CPU, duration and executions rankings
/// answered from it equal the same ranking over the interval-honest sibling. Raw is read FIRST (its rows still exist), then
/// the raw rows are deleted so the router must read a rollup. Each read goes through a FRESH data source, because coverage
/// is cached per data source.</para>
/// </summary>
public sealed class TopQueriesIoRouteLiveTests
{
    private const int ServerId = -943861;
    private const string ServerName = "a5329-top-queries-io-route";
    private const int Top = 10;

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(IoRollupOracleSeed.Hours + 1);
    private static readonly DateTime SurvivorAt = WindowStart.AddHours(IoRollupOracleSeed.Hours).AddMinutes(30);

    private sealed record Total(string Db, string Hash, long Executions, long CpuUs, long ElapsedUs, long LogicalReads, long PhysicalReads, long LogicalWrites);

    private static Total[] Totals(IEnumerable<DarlingDataReader.TopQueryRow> rows, bool withIo = true) =>
        rows.Select(r => new Total(
            r.DatabaseName, r.QueryHash, r.TotalExecutions, r.TotalCpuUs, r.TotalElapsedUs,
            withIo ? r.TotalLogicalReads : 0, withIo ? r.TotalPhysicalReads : 0, withIo ? r.TotalLogicalWrites : 0)).ToArray();

    [Fact]
    public Task Covering_EveryRankingReadsIo_ReadsEqualsTheRawRoute_AndTheOtherRankingsEqualTheIntervalSibling() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, WindowStart);
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, SurvivorAt, "QSURV", 1_000, 1_000, 7, 1);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, ct);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIoHourlyView, WindowStart, ct);

            /* The raw route, read FIRST while raw still holds the rows: the raw reads statement itself over the same window. The
               router cannot supply it: a window this old is Hourly-tier by age, and the reader's raw arm runs only on a
               recent one. */
            var raw = await RunRawReadsAsync(connection, ct);
            Assert.True(raw.Length >= 5, "the seed should rank at least five groups, or the comparison below proves nothing");
            Assert.DoesNotContain(raw, r => r.Hash == "0xQHD");   /* the zero-interval row's 9,999,999 reads are in neither route */

            /* Now the raw rows go, so the window can only be answered by a rollup. */
            await PurgeRawBeforeAsync(connection, WindowStart.AddHours(IoRollupOracleSeed.Hours), ct);

            await using var ioSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var io = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                ioSource, ServerId, WindowStart, WindowEnd, Top, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);
            Assert.Equal(RetentionTier.Hourly, io.Tier);
            Assert.True(io.IoRoute);
            Assert.False(io.RawForced);
            Assert.Null(io.RetentionNotice);   /* the io rollup reaches the start: nothing partial to disclose */
            Assert.Equal(raw, Totals(io.Rows));
            Assert.Contains(io.Rows, r => r.TotalLogicalReads > 0 && r.TotalPhysicalReads > 0 && r.TotalLogicalWrites > 0);
            Assert.Contains(io.Rows, r => r.QueryHash == "0xQHC" && r.TotalLogicalReads == 0);   /* the NULL-reads group stays 0, never dropped */

            /* Every other ranking reads the io relation too, and equals the interval sibling over the same window. */
            foreach (var ranking in new[] { TopRanking.Cpu, TopRanking.Duration, TopRanking.Executions })
            {
                var label = TopRankings.WireName(ranking);
                var viaIo = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                    ioSource, ServerId, WindowStart, WindowEnd, Top, databaseName: null, ranking: ranking, cancellationToken: ct);
                Assert.True(viaIo.IoRoute, $"{label}: covering io takes every ranking");
                Assert.Equal(RetentionTier.Hourly, viaIo.Tier);
                var sibling = await RunHourlyAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, ranking, ct);
                Assert.Equal(sibling, Totals(viaIo.Rows, withIo: false).Select(t => (t.Db, t.Hash, t.Executions, t.CpuUs, t.ElapsedUs)).ToArray());
            }

            /* The tool: the rows carry the sums, and the note names the relation instead of saying only raw has reads. */
            var query = "?server=" + ServerName + "&hours=24&as_of=" + Uri.EscapeDataString(WindowEnd.ToString("o")) + "&order_by=reads";
            using var doc = JsonDocument.Parse(await TopRankingLiveTests.WebReadAsync(ioSource, "get_top_queries_by_cpu", query));
            Assert.Equal("hourly", doc.RootElement.GetProperty("tier_used").GetString());
            Assert.Contains("query_stats_io_hourly", doc.RootElement.GetProperty("precision_note").GetString(), StringComparison.Ordinal);
            Assert.NotEqual(JsonValueKind.Null, doc.RootElement.GetProperty("queries")[0].GetProperty("total_logical_reads").ValueKind);
        });

    [Fact]
    public Task EmptyThenPartial_RoutesByWhichRelationReachesFurtherBack() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, WindowStart);
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, SurvivorAt, "QSURV", 1_000, 1_000, 7, 1);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, ct);
            await PurgeRawBeforeAsync(connection, WindowStart.AddHours(1), ct);   /* raw now starts at hour 1 */

            /* io EMPTY: reads is raw (forced, with raw's retention notice); the other rankings keep the stitched rollup. */
            var empty = await ReadAsync(scratch, TopRanking.Reads, ct);
            Assert.Equal(RetentionTier.Raw, empty.Tier);
            Assert.True(empty.RawForced);
            Assert.False(empty.IoRoute);
            Assert.NotNull(empty.RetentionNotice);
            var cpuEmpty = await ReadAsync(scratch, TopRanking.Cpu, ct);
            Assert.Equal(RetentionTier.Hourly, cpuEmpty.Tier);
            Assert.False(cpuEmpty.IoRoute);
            Assert.All(cpuEmpty.Rows, r => Assert.Equal(0, r.TotalLogicalReads));

            /* Covering is the other test: the floor cache trusts a floor while the rollup's oldest chunk is unchanged, so a refresh
               that only moves the floor earlier inside that chunk is not seen by the next read of the same store. */
            /* io PARTIAL (starts at hour 2) while raw reaches back to hour 1: raw is deeper, so reads stays raw. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIoHourlyView, WindowStart.AddHours(2), ct);
            var rawDeeper = await ReadAsync(scratch, TopRanking.Reads, ct);
            Assert.Equal(RetentionTier.Raw, rawDeeper.Tier);
            Assert.False(rawDeeper.IoRoute);
            Assert.NotNull(rawDeeper.RetentionNotice);
            Assert.False((await ReadAsync(scratch, TopRanking.Cpu, ct)).IoRoute);

            /* raw is purged to hour 4: now the partial io reaches further back than raw, and reads takes it, disclosed as
               a hourly window that starts at io's first bucket rather than with raw's retention notice. */
            await PurgeRawBeforeAsync(connection, WindowStart.AddHours(4), ct);
            var partial = await ReadAsync(scratch, TopRanking.Reads, ct);
            Assert.Equal(RetentionTier.Hourly, partial.Tier);
            Assert.True(partial.IoRoute);
            Assert.Null(partial.RetentionNotice);
            Assert.Equal(WindowStart.AddHours(2), partial.HourlyFirstBucket);
            Assert.DoesNotContain(partial.Rows, r => r.QueryHash == "0xQHA" && r.TotalLogicalReads == 0);
            Assert.False((await ReadAsync(scratch, TopRanking.Cpu, ct)).IoRoute);   /* partial io never takes a non-reads ranking */
        });

    [Fact]
    public Task Absent_AStoreWithoutTheIoRollup_ReadsRawForReads_AndNeverNamesTheMissingRelation() =>
        WithStoreAsync(async (scratch, connection, ct) =>
        {
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, WindowStart);
            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, SurvivorAt, "QSURV", 1_000, 1_000, 7, 1);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, ct);
            await PurgeRawBeforeAsync(connection, WindowStart.AddHours(IoRollupOracleSeed.Hours), ct);
            await using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStatsIoHourlyView}", connection))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            var reads = await ReadAsync(scratch, TopRanking.Reads, ct);
            Assert.Equal(RetentionTier.Raw, reads.Tier);
            Assert.True(reads.RawForced);
            Assert.Equal("0xQSURV", Assert.Single(reads.Rows).QueryHash);

            var cpu = await ReadAsync(scratch, TopRanking.Cpu, ct);
            Assert.Equal(RetentionTier.Hourly, cpu.Tier);
            Assert.False(cpu.IoRoute);
            Assert.NotEmpty(cpu.Rows);
        });

    // ---------------------------------------------------------------- plumbing

    private static async Task<DarlingDataReader.TopQueriesReadResult> ReadAsync(ScratchPostgres scratch, TopRanking ranking, CancellationToken ct)
    {
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
            source, ServerId, WindowStart, WindowEnd, Top, databaseName: null, ranking: ranking, cancellationToken: ct);
    }

    /// <summary>The hourly statement over <paramref name="view"/> directly (the sibling the io rollup must equal), as
    /// (database, hash, executions, cpu, elapsed) in page order.</summary>
    private static async Task<(string Db, string Hash, long Executions, long CpuUs, long ElapsedUs)[]> RunHourlyAsync(
        NpgsqlConnection connection, string view, TopRanking ranking, CancellationToken ct)
    {
        var sql = TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, ranking, hourly: true)
            .Replace(DarlingDataReader.TopQueriesHourlyFromPlaceholder, $"collect.{view} AS f", StringComparison.Ordinal)
            .Replace(DarlingDataReader.TopQueriesHourlyIoSumsPlaceholder, TopRankings.HourlyNoIoSums, StringComparison.Ordinal)
            .Replace("$CEIL$", "", StringComparison.Ordinal);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = WindowStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = WindowEnd });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top + 5 });
        var rows = new List<(string, string, long, long, long)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(10))
            {
                continue;   /* the candidate-count row */
            }

            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4)));
        }

        return rows.ToArray();
    }

    /// <summary>The raw reads statement (<see cref="DarlingDataReader.TopQueriesSql"/> ranked by reads) over the window, as totals in page order.</summary>
    private static async Task<Total[]> RunRawReadsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = TopRankings.Apply(DarlingDataReader.TopQueriesSql, TopRanking.Reads, hourly: false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = WindowStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = WindowEnd });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 0 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top + 5 });
        var rows = new List<Total>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(44))
            {
                continue;   /* the candidate-count row */
            }

            rows.Add(new Total(
                reader.GetString(0), reader.GetString(1), reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9), reader.IsDBNull(11) ? 0 : reader.GetInt64(11), reader.IsDBNull(10) ? 0 : reader.GetInt64(10)));
        }

        return rows.ToArray();
    }

    private static async Task PurgeRawBeforeAsync(NpgsqlConnection connection, DateTime before, CancellationToken ct)
    {
        await using var purge = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time < $2", connection);
        purge.Parameters.AddWithValue(ServerId);
        purge.Parameters.AddWithValue(before);
        await purge.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(WindowEnd.AddHours(1));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Mints a scratch TimescaleDB store with the continuous aggregates and this class's server registered, and runs the body on it.</summary>
    private static async Task WithStoreAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5329 io route tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #5329 io route tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }
}
