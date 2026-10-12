/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5329 lane A: the two hourly rollups that keep logical reads (<c>query_stats_io_hourly</c> and
/// <c>procedure_stats_io_hourly</c>), pinned without a database. The text is the interval sibling's plus three
/// sums; the registrations follow the ruling (leaf retention rule, NOT in the raw purge gate, NOT in the
/// stitch list).
/// </summary>
public sealed class IoHourlyRollupRegistrationTests
{
    private static readonly (string View, string Sibling, string Create, string SiblingCreate)[] Pairs =
    [
        (TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView,
            TimescaleSupport.CreateQueryStatsIoHourlySql, TimescaleSupport.CreateQueryStatsIntervalHourlySql),
        (TimescaleSupport.ProcedureStatsIoHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView,
            TimescaleSupport.CreateProcedureStatsIoHourlySql, TimescaleSupport.CreateProcedureStatsIntervalHourlySql),
    ];

    [Fact]
    public void EachIoRollup_IsItsIntervalSiblingsTextPlusTheThreeIoSums()
    {
        foreach (var (view, sibling, create, siblingCreate) in Pairs)
        {
            Assert.StartsWith($"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.{view}", create, StringComparison.Ordinal);
            Assert.Contains("WITH NO DATA", create, StringComparison.Ordinal);
            Assert.DoesNotContain("materialized_only", create, StringComparison.Ordinal);
            Assert.Contains("WHERE sample_interval_seconds IS DISTINCT FROM 0", create, StringComparison.Ordinal);
            Assert.Equal(TimescaleSupport.RefreshGroupingTermsFor(siblingCreate), TimescaleSupport.RefreshGroupingTermsFor(create));
            Assert.Equal(
                Regex.Match(siblingCreate, @"\bFROM\s+collect\.(\w+)").Groups[1].Value,
                Regex.Match(create, @"\bFROM\s+collect\.(\w+)").Groups[1].Value);

            /* every sibling select item survives under the same alias */
            foreach (Match item in Regex.Matches(siblingCreate, @"\b(\w+\([^)]*\)) AS (\w+)"))
            {
                Assert.Contains($"{item.Groups[1].Value} AS {item.Groups[2].Value}", create, StringComparison.Ordinal);
            }

            Assert.Contains("sum(delta_logical_reads) AS logical_reads_sum", create, StringComparison.Ordinal);
            Assert.Contains("sum(delta_physical_reads) AS physical_reads_sum", create, StringComparison.Ordinal);
            Assert.Contains("sum(delta_logical_writes) AS logical_writes_sum", create, StringComparison.Ordinal);
            Assert.DoesNotContain("logical_reads_sum", siblingCreate, StringComparison.Ordinal);
            Assert.NotEqual(sibling, view);
        }
    }

    [Fact]
    public void EachIoRollup_IsRegisteredWhereTheRulingSays_AndNowhereElse()
    {
        foreach (var (view, _, create, _) in Pairs)
        {
            Assert.Contains(TimescaleSupport.HourlyAggregates, a => a.View == view && a.CreateSql == create);
            Assert.DoesNotContain(TimescaleSupport.DailyAggregates, a => a.View == view);
            Assert.Contains($"to_regclass('collect.{view}') IS NOT NULL", TimescaleSupport.RollupProbeSql, StringComparison.Ordinal);
            Assert.True(RollupAvailability.All.Has(view));
            Assert.False(RollupAvailability.WithoutIoHourlies.Has(view));
            Assert.True(RollupAvailability.WithoutIoHourlies.Has(TimescaleSupport.QueryStatsIntervalDailyView));

            var row = TimescaleSupport.RollupViews.Single(r => r.View == view);
            Assert.Equal("collection_time", row.SourceTimeColumn);
            Assert.Equal(TimescaleSupport.HourlyBucket, row.BucketWidth);
            Assert.False(RollupBackfill.Targets.Single(t => t.View == view).IsHierarchical);

            /* THE LEAF RULE: 90 days, Coverage is itself; not in the raw purge gate, not stitched. */
            var policy = TimescaleSupport.RetentionPolicies.Single(p => p.Relation == view);
            Assert.Equal(TimescaleSupport.HourlyRetentionInterval, policy.DropAfter);
            Assert.Equal("bucket", policy.TimeColumn);
            Assert.Equal(new[] { view }, policy.Coverage);
            Assert.All(TimescaleSupport.RawTierCoverage, tier => Assert.DoesNotContain(view, tier.Coverage));
            Assert.DoesNotContain(TimescaleSupport.SupersededHourlyRollups, s => s.Legacy == view || s.Successor == view);
            Assert.True(TimescaleSupport.IsAggregateCompressionTarget(view));
        }

        Assert.False(RollupAvailability.WithoutIoHourlies.AllPresent);
        Assert.True(RollupAvailability.All.AllPresent);
    }

    [Fact]
    public void TheGrid_GivesTheQueryIoViewTheFourthUnboundedPosition_AndTheBandHoldsNoFifth()
    {
        Assert.True(TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.QueryStatsIoHourlyView));
        Assert.False(TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.ProcedureStatsIoHourlyView));

        /* band index 12 of 0-13: the 4th unbounded member takes 3 * 4 */
        Assert.Equal(12, TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.QueryStatsIoHourlyView));
        Assert.Equal(
            4,
            TimescaleSupport.HourlyRefreshPhaseOrder.Count(v =>
                v != TimescaleSupport.HeaviestHourlyRefreshView && TimescaleSupport.IsUnboundedCardinalityRefresh(v)));
        Assert.True(TimescaleSupport.LightBandHoldsUnboundedRefreshCount(4));
        Assert.False(TimescaleSupport.LightBandHoldsUnboundedRefreshCount(5));

        /* compression: 22 of the 23 hours; appending moved the daily and baseline members by +2 */
        Assert.Equal(22, TimescaleSupport.AggregateCompressionTargets.Count);
        Assert.Equal(9, TimescaleSupport.AggregateCompressionBandHourFor(TimescaleSupport.QueryStoreStatsDailyView));
        Assert.Equal(7, TimescaleSupport.AggregateCompressionBandHourFor(TimescaleSupport.QueryStatsIoHourlyView));
        Assert.Equal(8, TimescaleSupport.AggregateCompressionBandHourFor(TimescaleSupport.ProcedureStatsIoHourlyView));
        Assert.Equal(22, TimescaleSupport.AggregateCompressionBandHourFor(TimescaleSupport.MemoryBaselineView));
    }
}

/// <summary>
/// The deterministic seed lane B's oracle reuses: rows in <c>collect.query_stats</c> and
/// <c>collect.procedure_stats</c> over <see cref="Hours"/> hourly buckets from a window start, with a NULL-reads
/// row, a reads tie, a zero-interval row (the rollups' WHERE drops it), two sql_handles under one query_hash and
/// two databases. Refresh from the window start to <see cref="Hours"/> + 1 hours after it.
/// </summary>
internal static class IoRollupOracleSeed
{
    internal const int Hours = 4;

    internal static async Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime windowStart)
    {
        for (var hour = 0; hour < Hours; hour++)
        {
            var at = windowStart.AddHours(hour).AddMinutes(10);
            // name, db, handle, reads (null = unknown), interval, cpu
            (string Name, string Db, string Handle, long? Reads, int? Interval, long Cpu)[] rows =
            [
                ("A", "SeedDb1", "h1", 5_000 + hour, 3600, 100 + hour),
                ("A", "SeedDb1", "h2", 5_000, 3600, 50),       // same query_hash, second handle
                ("B", "SeedDb1", "h3", 5_000 + hour, 3600, 70), // ties A's h1 at hour 0
                ("C", "SeedDb2", "h4", null, 3600, 90),         // NULL reads
                ("D", "SeedDb2", "h5", 9_999_999, 0, 1_000),    // zero interval: excluded by the rollups
                ("E", "SeedDb2", "h6", 12, null, 5),            // NULL interval: IS DISTINCT FROM 0 keeps it
            ];
            foreach (var row in rows)
            {
                await InsertAsync(connection, ct, "collect.query_stats",
                    "query_hash, sql_handle", "$6, $7", row.Name, serverId, serverName, at, row.Db, row.Handle, row.Reads, row.Interval, row.Cpu, "'dbo'");
                await InsertAsync(connection, ct, "collect.procedure_stats",
                    "schema_name, object_name, sql_handle", "'dbo', $6, $7", row.Name, serverId, serverName, at, row.Db, row.Handle, row.Reads, row.Interval, row.Cpu, "");
            }
        }
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, CancellationToken ct, string table, string keyColumns, string keyValues,
        string name, int serverId, string serverName, DateTime at, string db, string handle, long? reads, int? interval, long cpu, string unused)
    {
        _ = unused;
        var key = table.EndsWith("query_stats", StringComparison.Ordinal) ? "0xQH" + name : "usp_" + name;
        await using var command = new NpgsqlCommand(
            $@"INSERT INTO {table}
                   (collection_id, collection_time, server_id, server_name, database_name, {keyColumns},
                    delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_physical_reads,
                    delta_logical_writes, sample_interval_seconds)
               VALUES ($1, $2, $3, $4, $5, {keyValues}, $8, $9, $10, $11, $12, $13, $14)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(db);
        command.Parameters.AddWithValue(key);
        command.Parameters.AddWithValue(handle);
        command.Parameters.AddWithValue(3L);
        command.Parameters.AddWithValue(cpu);
        command.Parameters.AddWithValue(cpu * 2);
        command.Parameters.AddWithValue((object?)reads ?? DBNull.Value);
        command.Parameters.AddWithValue(reads is { } r ? r / 10 : (object)DBNull.Value);
        command.Parameters.AddWithValue(cpu / 5);
        command.Parameters.AddWithValue((object?)interval ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }
}

/* #1776 own-store: each test mints a scratch database, because it materializes continuous aggregates and drops one,
   which no other class sharing the store may see. */
/// <summary>
/// #5329 lane A, live: after a refresh the io rollups' shared columns equal the interval siblings' for the same
/// seed, and the three sums equal raw's sums per bucket; a store missing one view reports exactly that view
/// missing, the next ensure sweep builds it, and a rerun builds nothing.
/// </summary>
public sealed class IoHourlyRollupLiveTests
{
    private const int ServerId = -953290;
    private const string ServerName = "a5329-io-rollups";
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly string[] SharedQueryColumns =
    [
        "server_id", "server_name", "database_name", "query_hash", "sql_handle", "bucket",
        "worker_time_sum", "worker_time_min", "worker_time_max", "elapsed_time_sum", "elapsed_time_min", "elapsed_time_max",
        "execution_count_sum", "execution_count_min", "execution_count_max", "sample_interval_seconds_sum", "sample_count",
    ];

    private static readonly string[] SharedProcedureColumns =
    [
        "server_id", "server_name", "database_name", "schema_name", "object_name", "bucket",
        "worker_time_sum", "worker_time_min", "worker_time_max", "elapsed_time_sum", "elapsed_time_min", "elapsed_time_max",
        "execution_count_sum", "execution_count_min", "execution_count_max", "sample_interval_seconds_sum", "sample_count",
    ];

    [Fact]
    public Task AfterARefresh_IoSharedColumnsEqualTheIntervalSibling_AndTheSumsEqualRawsPerBucket() =>
        WithStoreAsync(async (connection, ct) =>
        {
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, WindowStart);
            var to = WindowStart.AddHours(IoRollupOracleSeed.Hours + 1);
            foreach (var view in new[]
            {
                TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView,
                TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
            })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(WindowStart);
                refresh.Parameters.AddWithValue(to);
                await refresh.ExecuteNonQueryAsync(ct);
            }

            foreach (var (io, sibling, columns, raw, keys) in new[]
            {
                (TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, SharedQueryColumns, "collect.query_stats", "query_hash, sql_handle"),
                (TimescaleSupport.ProcedureStatsIoHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView, SharedProcedureColumns, "collect.procedure_stats", "schema_name, object_name"),
            })
            {
                var list = string.Join(", ", columns);
                var siblingRows = await ScalarAsync(connection, ct,
                    $"SELECT count(*) FROM collect.{sibling} WHERE server_id = {ServerId}");
                Assert.True(siblingRows > 0, $"{sibling} is empty, so the equality below proves nothing");
                var diff = await ScalarAsync(connection, ct,
                    $@"SELECT count(*) FROM (
                          (SELECT {list} FROM collect.{io} WHERE server_id = {ServerId} EXCEPT SELECT {list} FROM collect.{sibling} WHERE server_id = {ServerId})
                          UNION ALL
                          (SELECT {list} FROM collect.{sibling} WHERE server_id = {ServerId} EXCEPT SELECT {list} FROM collect.{io} WHERE server_id = {ServerId})) d");
                Assert.Equal(0L, diff);
                Assert.Equal(siblingRows, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.{io} WHERE server_id = {ServerId}"));

                /* the three sums equal raw's, per group and bucket, under the rollup's own WHERE; the NULL-reads
                   group stays NULL and the zero-interval row's 9,999,999 reads are in neither */
                var sumDiff = await ScalarAsync(connection, ct,
                    $@"SELECT count(*) FROM (
                          SELECT server_id, server_name, database_name, {keys}, time_bucket('1 hour', collection_time) AS bucket,
                                 sum(delta_logical_reads) AS l, sum(delta_physical_reads) AS p, sum(delta_logical_writes) AS w
                          FROM {raw} WHERE server_id = {ServerId} AND sample_interval_seconds IS DISTINCT FROM 0
                          GROUP BY server_id, server_name, database_name, {keys}, bucket) r
                       FULL JOIN (SELECT server_id, server_name, database_name, {keys}, bucket,
                                         logical_reads_sum AS l, physical_reads_sum AS p, logical_writes_sum AS w
                                  FROM collect.{io} WHERE server_id = {ServerId}) v
                         USING (server_id, server_name, database_name, {keys.Replace(", ", ", ")}, bucket)
                       WHERE r.l IS DISTINCT FROM v.l OR r.p IS DISTINCT FROM v.p OR r.w IS DISTINCT FROM v.w");
                Assert.Equal(0L, sumDiff);
                Assert.True(await ScalarAsync(connection, ct,
                    $"SELECT count(*) FROM collect.{io} WHERE server_id = {ServerId} AND logical_reads_sum IS NULL") > 0, "the NULL-reads group should stay NULL");
                Assert.Equal(0L, await ScalarAsync(connection, ct,
                    $"SELECT count(*) FROM collect.{io} WHERE server_id = {ServerId} AND logical_reads_sum >= 9999999"));
            }
        });

    [Fact]
    public Task AStoreMissingOneIoView_ReportsItMissingPerView_TheNextSweepBuildsIt_AndARerunBuildsNothing() =>
        WithStoreAsync(async (connection, ct) =>
        {
            Assert.True((await DetectAsync(connection, ct)).AllPresent);

            await using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.ProcedureStatsIoHourlyView}", connection))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            var missing = await DetectAsync(connection, ct);
            Assert.True(missing.QueryGrainIoHourly);
            Assert.False(missing.ProcedureGrainIoHourly);
            Assert.False(missing.AllPresent);

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.True((await DetectAsync(connection, ct)).AllPresent);

            var before = await ScalarAsync(connection, ct,
                "SELECT count(*) FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'");
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.Equal(before, await ScalarAsync(connection, ct,
                "SELECT count(*) FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect'"));
        });

    private static async Task<RollupAvailability> DetectAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var source = NpgsqlDataSource.Create(connection.ConnectionString);
        return await TimescaleSupport.DetectRollupsAsync(source, ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5329 io rollup tests.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: a scratch database of its own, so the scheduler stop below cannot race another fixture */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #5329 io rollup tests need TimescaleDB.");
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
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
