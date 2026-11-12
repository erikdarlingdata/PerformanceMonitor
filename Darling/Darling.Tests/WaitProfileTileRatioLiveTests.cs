using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7177: the wait-profile card prints "peaked at {current_ms_per_sec} ms/sec, roughly {ratio}x the
/// {baseline_mean} ms/sec normal". When the worst tile is not the window-start hour, current and baseline_mean
/// come from that tile, so the ratio must divide them and not the start bucket's mean.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitProfileTileRatioLiveTests
{
    /* #1776 own-store: this fact seeds and removes rows under its own server id in the shared dev store. */
    private const int ServerId = -717700;
    private const string ServerName = "wait-profile-tile-ratio";
    private const string InsertWait =
        "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, 'PAGEIOLATCH_SH', $5, $6)";

    [Fact]
    public async Task WorstTileInLaterHour_RatioDividesTilePeakByTileBaselineMean()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live wait-profile tests.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var lastHistoryMonday = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);
            var analysisTime = lastHistoryMonday.AddDays(7);

            // Three Mondays of history. Hour 10 (the window-start bucket) runs about 200 ms/sec, hour 11 about 100.
            foreach (var weeksBack in new[] { 2, 1, 0 })
            {
                var monday = lastHistoryMonday.AddDays(-7 * weeksBack);
                for (var i = 0; i < 24; i++)
                {
                    var hourTen = i < 12;
                    var totalMs = (i % 3) switch
                    {
                        0 => hourTen ? 30000L : 15000L,
                        1 => hourTen ? 60000L : 30000L,
                        _ => hourTen ? 90000L : 45000L
                    };
                    await InsertAsync(connection, monday.AddMinutes(5 * i), totalMs, ct);
                }
            }

            // The window: hour 10 stays at its normal 200 ms/sec, hour 11 runs hot with one 3,200 ms/sec peak.
            for (var i = 1; i <= 17; i++)
            {
                var at = analysisTime.AddMinutes(5 * i);
                var totalMs = at.Hour == 10 ? 60000L : (at.Minute == 15 ? 960000L : 300000L);
                await InsertAsync(connection, at, totalMs, ct);
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var startBucket = await provider.GetBaselineAsync(ServerId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.True(startBucket.IsTrustworthy, "three Mondays make the baseline trusted");
            Assert.True(startBucket.EffectiveRobustSigma > 0, "the fixture must land on the robust tile arm");

            var detector = new PgAnomalyDetector(postgres, provider);
            var facts = await detector.DetectAnomaliesAsync(new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(90),
                ServerUtcOffset = TimeSpan.Zero
            });
            var fact = Assert.Single(facts, f => f.Key == "ANOMALY_WAIT_PROFILE");
            var current = fact.Metadata["current_ms_per_sec"];
            var baselineMean = fact.Metadata["baseline_mean"];
            Assert.Equal(3200.0, current, 0.001);
            Assert.True(Math.Abs(baselineMean - startBucket.Mean) > 1.0, "the worst tile must score against a different bucket than the window start");
            Assert.Equal(current / baselineMean, fact.Metadata["ratio"], 0.001);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanAsync(cleanup, cleanupCt);
                foreach (var (_, view) in TimescaleSupport.BaselineAggregates.Concat(TimescaleSupport.PlainBaselineViews))
                {
                    using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), cleanup);
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
    }

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand($"DELETE FROM wait_stats WHERE server_id = {ServerId};", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, DateTime at, long totalMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(InsertWait, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(totalMs);
        await command.ExecuteNonQueryAsync(ct);
    }
}
