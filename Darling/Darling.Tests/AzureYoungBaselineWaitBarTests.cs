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
/// On an Azure SQL Database (engine edition 5) an idle Hyperscale database carries a steady ~1,000 ms/s of
/// REMOTE_BLOCK_IO. While the wait baseline is untrusted, the absolute bar must not count that wait; every
/// other edition, and the trusted arms, keep the all-types behaviour.
/// </summary>
[Collection("live-postgres")]
public sealed class AzureYoungBaselineWaitBarTests
{
    /* #1776 own-store: each fact seeds and removes rows under its own server id in the shared dev store. */
    private const int BaseServerId = -626300;
    private const string RemoteBlockIo = "REMOTE_BLOCK_IO";
    private const string InsertWait =
        "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";
    private const string InsertProps =
        "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition) VALUES ($1, $2, $3, $4, 'Edition', '12.0.2000.8', 'RTM', $5)";

    [Fact]
    public async Task EditionFive_UntrustedBaseline_OnlyRemoteBlockIo_DoesNotFire()
    {
        var facts = await RunUntrustedAsync(BaseServerId + 1, ServerHardwareScope.AzureSqlDatabaseEngineEdition, extraWaitMsPerSec: 0);
        Assert.DoesNotContain(facts, f => f.Key == "ANOMALY_WAIT_PROFILE");
    }

    [Fact]
    public async Task EditionFive_UntrustedBaseline_OtherWaitOverBar_FiresWithAllTypesPeak()
    {
        var facts = await RunUntrustedAsync(BaseServerId + 2, ServerHardwareScope.AzureSqlDatabaseEngineEdition, extraWaitMsPerSec: 300);
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_WAIT_PROFILE");
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(1300.0, fact.Metadata["current_ms_per_sec"], 0.01);
        Assert.Equal(1.0, fact.Metadata["bar_excluded_REMOTE_BLOCK_IO"]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task OtherEditions_UntrustedBaseline_OnlyRemoteBlockIo_StillFires(int edition)
    {
        var facts = await RunUntrustedAsync(BaseServerId + 10 + edition, edition, extraWaitMsPerSec: 0);
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_WAIT_PROFILE");
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(1000.0, fact.Metadata["current_ms_per_sec"], 0.01);
        Assert.DoesNotContain(fact.Metadata.Keys, k => k.StartsWith("bar_excluded_", StringComparison.Ordinal));
    }

    [Fact]
    public async Task EditionFive_TrustedBaseline_RemoteBlockIoSurge_StillFires()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live wait-bar tests.");
        var ct = TestContext.Current.CancellationToken;
        const int serverId = BaseServerId + 20;
        const string serverName = "azure-trusted-wait-bar";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var lastHistoryMonday = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);
            var analysisTime = lastHistoryMonday.AddDays(7);

            foreach (var weeksBack in new[] { 2, 1, 0 })
            {
                var monday = lastHistoryMonday.AddDays(-7 * weeksBack);
                for (var i = 0; i < 12; i++)
                {
                    var totalMs = (i % 3) switch { 0 => 30000L, 1 => 60000L, _ => 90000L };
                    await InsertAsync(connection, InsertWait, CollectionIdGenerator.Next(), monday.AddMinutes(5 * i), serverId, serverName, RemoteBlockIo, 10L, totalMs);
                }
            }
            await InsertAsync(connection, InsertProps, CollectionIdGenerator.Next(), analysisTime.AddDays(-1), serverId, serverName, ServerHardwareScope.AzureSqlDatabaseEngineEdition);
            for (var i = 0; i < 17; i++)
                await InsertAsync(connection, InsertWait, CollectionIdGenerator.Next(), analysisTime.AddMinutes(5 * (i + 1)), serverId, serverName, RemoteBlockIo, 50L, i == 8 ? 960000L : 450000L);

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.True(baseline.IsTrustworthy, "three Mondays make the baseline trusted");

            var detector = new PgAnomalyDetector(postgres, provider);
            var facts = await detector.DetectAnomaliesAsync(new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(90),
                ServerUtcOffset = TimeSpan.Zero
            });
            var fact = Assert.Single(facts, f => f.Key == "ANOMALY_WAIT_PROFILE");
            Assert.Equal(0.0, fact.Metadata["is_new"]);
            Assert.Equal(3200.0, fact.Metadata["current_ms_per_sec"], 0.001);
            Assert.DoesNotContain(fact.Metadata.Keys, k => k.StartsWith("bar_excluded_", StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanAsync(cleanup, serverId, cleanupCt);
                foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
                {
                    using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), cleanup);
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
    }

    /// <summary>
    /// Three 5-minute-spaced collections with no history, so the baseline is untrusted. The first is
    /// unrated; the other two carry REMOTE_BLOCK_IO at 1,000 ms/s plus an optional second wait.
    /// </summary>
    private static async Task<System.Collections.Generic.IReadOnlyList<Fact>> RunUntrustedAsync(int serverId, int edition, int extraWaitMsPerSec)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live wait-bar tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverName = $"azure-wait-bar-{serverId}";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, serverId, ct);
        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        System.Collections.Generic.IReadOnlyList<Fact> facts;
        try
        {
            var analysisTime = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3).AddHours(10), DateTimeKind.Unspecified);
            await InsertAsync(connection, InsertProps, CollectionIdGenerator.Next(), analysisTime.AddDays(-1), serverId, serverName, edition);
            for (var i = 1; i <= 3; i++)
            {
                var at = analysisTime.AddMinutes(5 * i);
                await InsertAsync(connection, InsertWait, CollectionIdGenerator.Next(), at, serverId, serverName, RemoteBlockIo, 50L, 300000L);
                if (extraWaitMsPerSec > 0)
                    await InsertAsync(connection, InsertWait, CollectionIdGenerator.Next(), at, serverId, serverName, "PAGEIOLATCH_SH", 50L, extraWaitMsPerSec * 300L);
            }

            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.False(baseline.IsTrustworthy, "the fixture must land on the absolute-bar arm");

            var detector = new PgAnomalyDetector(postgres, provider);
            facts = await detector.DetectAnomaliesAsync(new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(30),
                ServerUtcOffset = TimeSpan.Zero
            });
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded,
                (cleanup, cleanupCt) => CleanAsync(cleanup, serverId, cleanupCt));
        }
        return facts;
    }

    private static async Task CleanAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {serverId}; DELETE FROM server_properties WHERE server_id = {serverId};", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
