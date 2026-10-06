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
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5226 live pins for the ranking choice where it needs rollups: the hourly path answers CPU, duration and executions
/// from the rollup on a window raw no longer holds, reads stays on raw (the rollups carry no logical reads) and says
/// so, and a reads ranking over a window that starts before the raw floor carries the retention notice while one inside
/// the floor, and every other ranking, carries none. The raw path and the endpoint's refusal are in
/// <see cref="TopRankingLiveTests"/>, which runs on the shared store.
///
/// <para><b>#1776 own-store</b> — each test mints a scratch database, because it materializes continuous aggregates the
/// shared fixture must never inherit, so this class is deliberately NOT in the <c>live-postgres</c> collection.</para>
///
/// <para><b>One test per grid, not per choice.</b> Every test builds a TimescaleDB store, which is most of its cost, so the
/// choices are looped inside one test per grid and each assertion names the choice it checks. The hourly tests reuse the
/// seed of <see cref="TopRankingLiveTests"/>; the raw floor and the rollup floor are made to differ by planting the seed
/// in the first hours of an aged window, refreshing the hourly view, then deleting those raw rows and keeping one later
/// survivor, so the router sends the CPU, duration and executions reads to the rollup and the reads read finds only the
/// survivor in raw. The reads are read through a FRESH data source, because coverage is cached per data source.</para>
/// </summary>
public sealed class TopRankingHourlyLiveTests
{
    private const int ServerId = -943851;
    private const string ServerName = "a5226-top-ranking-hourly";

    /// <summary>Fixed anchor, never wall-clock relative: the window ages past raw's floor once the seed's raw rows are deleted,
    /// which is what drives the router, not calendar time (the same shape the #4231 routing tests use).</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly TimeSpan[] SeedOffsets =
    [
        TimeSpan.FromHours(1), TimeSpan.FromMinutes(80), TimeSpan.FromHours(2), TimeSpan.FromMinutes(140),
    ];

    private static readonly DateTime SurvivorAt = WindowStart.AddHours(5);

    [Fact]
    public Task Queries_OnTheHourlyPath_EachChoiceButReadsRanksItsOwnMetric_AndReadsStaysOnRaw() =>
        WithHourlyStoreAsync(async (scratch, connection, ct) =>
        {
            var windowEnd = WindowStart.AddDays(1);
            for (var i = 0; i < TopRankingLiveTests.Seeds.Length; i++)
            {
                var seed = TopRankingLiveTests.Seeds[i];
                await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, WindowStart + SeedOffsets[i],
                    seed.Name, seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
            }

            await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, SurvivorAt, "QSURV", 1_000, 1_000, 7, 1);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await PurgeSeedAsync(connection, "collect.query_stats", ct);

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            foreach (var (ranking, expectedOrder) in new[]
            {
                (TopRanking.Cpu, "0xQCPU,0xQREADS,0xQEXEC,0xQDUR,0xQSURV"),
                (TopRanking.Duration, "0xQDUR,0xQCPU,0xQREADS,0xQEXEC,0xQSURV"),
                (TopRanking.Executions, "0xQEXEC,0xQREADS,0xQCPU,0xQDUR,0xQSURV"),
            })
            {
                var label = "queries by " + TopRankings.WireName(ranking);
                var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                    hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, ranking: ranking, cancellationToken: ct);
                Assert.True(result.Tier == RetentionTier.Hourly, $"{label}: expected the hourly tier but read {result.Tier}");
                TopRankingLiveTests.AssertOrder(result.Rows.Select(r => r.QueryHash), expectedOrder, label + " (hourly)");
                Assert.Null(result.RetentionNotice);
            }

            /* Reads: the same window, which the router would send to the rollup, is read from raw, where only the survivor is. */
            var reads = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, reads.Tier);
            Assert.True(reads.RawForced);
            Assert.Equal("0xQSURV", Assert.Single(reads.Rows).QueryHash);
            Assert.NotNull(reads.RetentionNotice);

            /* The page sees it as the tier it read, the reason, and how far back raw reached. */
            var query = "?server=" + ServerName + "&hours=24&as_of=" + Uri.EscapeDataString(windowEnd.ToString("o"));
            using (var readsDoc = JsonDocument.Parse(await TopRankingLiveTests.WebReadAsync(hourlyDataSource, "get_top_queries_by_cpu", query + "&order_by=reads")))
            {
                var root = readsDoc.RootElement;
                Assert.Equal("raw", root.GetProperty("tier_used").GetString());
                Assert.Contains("order_by=reads needs per-query logical reads", root.GetProperty("precision_note").GetString(), StringComparison.Ordinal);
                Assert.True(root.GetProperty("window_truncated").GetBoolean());
                Assert.Equal(DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(SurvivorAt), DateTimeKind.Utc).ToString("o"), root.GetProperty("effective_start").GetString());
                Assert.Equal(JsonValueKind.String, root.GetProperty("retention_notice").ValueKind);
            }

            using var durationDoc = JsonDocument.Parse(await TopRankingLiveTests.WebReadAsync(hourlyDataSource, "get_top_queries_by_cpu", query + "&order_by=duration"));
            Assert.Equal("hourly", durationDoc.RootElement.GetProperty("tier_used").GetString());
            Assert.Equal("0xQDUR", durationDoc.RootElement.GetProperty("queries")[0].GetProperty("query_hash").GetString());
        });

    [Fact]
    public Task Procedures_OnTheHourlyPath_EachChoiceButReadsRanksItsOwnMetric_AndReadsStaysOnRaw() =>
        WithHourlyStoreAsync(async (scratch, connection, ct) =>
        {
            var windowEnd = WindowStart.AddDays(1);
            for (var i = 0; i < TopRankingLiveTests.Seeds.Length; i++)
            {
                var seed = TopRankingLiveTests.Seeds[i];
                await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, WindowStart + SeedOffsets[i],
                    seed.Name, seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
            }

            await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, SurvivorAt, "QSURV", 1_000, 1_000, 7, 1);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await PurgeSeedAsync(connection, "collect.procedure_stats", ct);

            await using var hourlyDataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            foreach (var (ranking, expectedOrder) in new[]
            {
                (TopRanking.Cpu, "usp_QCPU,usp_QREADS,usp_QEXEC,usp_QDUR,usp_QSURV"),
                (TopRanking.Duration, "usp_QDUR,usp_QCPU,usp_QREADS,usp_QEXEC,usp_QSURV"),
                (TopRanking.Executions, "usp_QEXEC,usp_QREADS,usp_QCPU,usp_QDUR,usp_QSURV"),
            })
            {
                var label = "procedures by " + TopRankings.WireName(ranking);
                var result = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                    hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, ranking: ranking, cancellationToken: ct);
                Assert.True(result.Tier == RetentionTier.Hourly, $"{label}: expected the hourly tier but read {result.Tier}");
                TopRankingLiveTests.AssertOrder(result.Rows.Select(r => r.ObjectName), expectedOrder, label + " (hourly)");
                Assert.Null(result.RetentionNotice);
            }

            var reads = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                hourlyDataSource, ServerId, WindowStart, windowEnd, top: 10, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, reads.Tier);
            Assert.True(reads.RawForced);
            Assert.Equal("usp_QSURV", Assert.Single(reads.Rows).ObjectName);
            Assert.NotNull(reads.RetentionNotice);

            var query = "?server=" + ServerName + "&hours=24&as_of=" + Uri.EscapeDataString(windowEnd.ToString("o"));
            using (var readsDoc = JsonDocument.Parse(await TopRankingLiveTests.WebReadAsync(hourlyDataSource, "get_top_procedures_by_cpu", query + "&order_by=reads")))
            {
                var root = readsDoc.RootElement;
                Assert.Equal("raw", root.GetProperty("tier_used").GetString());
                Assert.Contains("order_by=reads needs per-procedure logical reads", root.GetProperty("precision_note").GetString(), StringComparison.Ordinal);
                Assert.Equal(JsonValueKind.String, root.GetProperty("retention_notice").ValueKind);
            }

            using var durationDoc = JsonDocument.Parse(await TopRankingLiveTests.WebReadAsync(hourlyDataSource, "get_top_procedures_by_cpu", query + "&order_by=duration"));
            Assert.Equal("hourly", durationDoc.RootElement.GetProperty("tier_used").GetString());
            Assert.Equal("dbo.usp_QDUR", durationDoc.RootElement.GetProperty("procedures")[0].GetProperty("full_name").GetString());
        });

    /// <summary>
    /// The notice, both sides, through the web read dispatch. Raw holds only the last six hours, so a reads ranking over seven days
    /// starts before the store's measured raw floor and says the list is partial, while one over three hours starts inside the
    /// floor and says nothing, and no other ranking says anything over either window (they read the rollup, which can answer).
    /// </summary>
    [Fact]
    public Task RetentionNotice_AReadsRankingStartingBeforeTheRawFloorCarriesIt_AndOneInsideTheFloorDoesNot() =>
        WithHourlyStoreAsync(async (scratch, connection, ct) =>
        {
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            foreach (var seed in TopRankingLiveTests.Seeds)
            {
                await TopRankingLiveTests.PlantQueryAsync(connection, ct, "collect.query_stats", ServerId, ServerName, now.AddHours(-6),
                    seed.Name, seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
                await TopRankingLiveTests.PlantProcedureAsync(connection, ct, "collect.procedure_stats", ServerId, ServerName, now.AddHours(-6),
                    seed.Name, seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            foreach (var read in new[] { "get_top_queries_by_cpu", "get_top_procedures_by_cpu" })
            {
                var before = NoticeOf(await TopRankingLiveTests.WebReadAsync(postgres, read, $"?server={ServerName}&hours=168&order_by=reads"));
                Assert.True(before is not null, $"{read}: a reads ranking over seven days starts before raw's floor and must carry the notice");
                Assert.Contains("partial window", before, StringComparison.Ordinal);
                Assert.Contains("raw tier", before, StringComparison.Ordinal);

                var inside = NoticeOf(await TopRankingLiveTests.WebReadAsync(postgres, read, $"?server={ServerName}&hours=3&order_by=reads"));
                Assert.True(inside is null, $"{read}: a reads ranking that starts inside raw's floor carries no notice, but read: {inside}");

                foreach (var other in new[] { "", "&order_by=cpu", "&order_by=duration", "&order_by=executions" })
                {
                    var notice = NoticeOf(await TopRankingLiveTests.WebReadAsync(postgres, read, $"?server={ServerName}&hours=168{other}"));
                    Assert.True(notice is null, $"{read}{other}: only the reads ranking carries the notice, but read: {notice}");
                }
            }
        });

    // ---------------------------------------------------------------- plumbing

    /// <summary>The notice the payload carries, or null when it carries none (a JSON null, or the property left out).</summary>
    private static string? NoticeOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("retention_notice", out var notice) && notice.ValueKind == JsonValueKind.String
            ? notice.GetString()
            : null;
    }

    /// <summary>Deletes the seed's raw rows (the first three hours of the window) and leaves the survivor.</summary>
    private static async Task PurgeSeedAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var purge = new NpgsqlCommand(
            $"DELETE FROM {table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        purge.Parameters.AddWithValue(ServerId);
        purge.Parameters.AddWithValue(WindowStart);
        purge.Parameters.AddWithValue(WindowStart.AddHours(3));
        await purge.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Mints a scratch TimescaleDB store with the continuous aggregates and this class's server registered, and runs the body on it.</summary>
    private static async Task WithHourlyStoreAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5226 hourly ranking tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #5226 hourly ranking tests need TimescaleDB.");
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
