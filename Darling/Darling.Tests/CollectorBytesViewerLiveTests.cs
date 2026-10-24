/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres. */
/// <summary>
/// #6629: the Viewer's three byte reads (the Collection Health grid's <see cref="ViewerDataService.CollectionBytesSql"/> and the two
/// Collection Log statements built by <see cref="ViewerDataService.CollectionLogWithBytesSql"/>) against a migrated V219 store with
/// planted collection_log rows. Until now they were only checked as text. Each case plants known bytes and one NULL, and asserts the
/// sums, the per-run values, and that a NULL stays NULL (not measured), never 0.
/// </summary>
public sealed class CollectorBytesViewerLiveTests : IAsyncLifetime
{
    private const int ServerId = -662901;
    private const string ServerName = "cbv-662901";

    private ScratchPostgres? _scratch;
    private ViewerDataService? _viewer;
    private DateTime _now;

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var utc = DateTime.UtcNow;
        _now = new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0, DateTimeKind.Unspecified);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, CancellationToken.None);

            /* alpha: two measured runs and one NULL run. beta: a collector that was never measured (all NULL). */
            await PlantAsync(connection, "alpha", 30, 1_000, 100);
            await PlantAsync(connection, "alpha", 20, 2_000, 200);
            await PlantAsync(connection, "alpha", 10, null, null);
            await PlantAsync(connection, "beta", 25, null, null);
            await PlantAsync(connection, "beta", 15, null, null);
        }

        _viewer = new ViewerDataService(_scratch.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (_viewer is not null)
        {
            await _viewer.DisposeAsync();
        }

        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }

    private static int s_nextLogId = 662_900_000;

    private async Task PlantAsync(NpgsqlConnection connection, string collector, int minutesAgo, long? received, long? sent) =>
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, bytes_received, bytes_sent) "
            + "VALUES ($1, $2, $3, $4, $5, 10, 'SUCCESS', 0, $6, $7)",
            (long)Interlocked.Increment(ref s_nextLogId), ServerId, ServerName, collector, DarlingMcpTestData.Naive(_now.AddMinutes(-minutesAgo)), received, sent);

    private void RequireStore() =>
        Assert.SkipWhen(_viewer is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live collector bytes Viewer test.");

    [Fact]
    public async Task CollectionHealth_SumsTheMeasuredRuns_AndLeavesAnUnmeasuredCollectorNull()
    {
        RequireStore();

        var rows = await _viewer!.GetCollectionHealthAsync(ServerId, TestContext.Current.CancellationToken);

        var alpha = rows.Single(r => r.CollectorName == "alpha");
        var beta = rows.Single(r => r.CollectorName == "beta");

        /* #6629: 3,000 received and 300 sent over the measured runs, the NULL run adding nothing. The first measured run is 30 minutes old,
           under the one-hour floor, so the figure divides by 1 hour: 3,000 x 24 and 300 x 24, not 3,000 / 7. */
        Assert.NotNull(alpha.BytesReceivedPerDay);
        Assert.NotNull(alpha.BytesSentPerDay);
        Assert.InRange(alpha.BytesReceivedPerDay!.Value, 3_000 * 24 - 1, 3_000 * 24 + 1);
        Assert.InRange(alpha.BytesSentPerDay!.Value, 300 * 24 - 1, 300 * 24 + 1);
        Assert.Equal(1.0, alpha.BytesMeasuredHours);
        Assert.Null(beta.BytesReceivedPerDay);
        Assert.Null(beta.BytesSentPerDay);
    }

    [Fact]
    public async Task CollectionHealth_ADayIsTheSumOverTheTimeMeasured_ForAStoreYoungerThanTheWindow()
    {
        RequireStore();

        await using (var connection = new NpgsqlConnection(_scratch!.ConnectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            /* gamma: measured only in the last two hours (119 and 60 minutes ago), 1,000 received and 100 sent each run. */
            await PlantAsync(connection, "gamma", 119, 1_000, 100);
            await PlantAsync(connection, "gamma", 60, 1_000, 100);
        }

        var rows = await _viewer!.GetCollectionHealthAsync(ServerId, TestContext.Current.CancellationToken);
        var gamma = rows.Single(r => r.CollectorName == "gamma");

        /* About 2 hours measured (a little over: the read runs after the plant), so a day is the 2,000 sum x about 12, not 2,000 / 7. */
        Assert.NotNull(gamma.BytesMeasuredHours);
        Assert.InRange(gamma.BytesMeasuredHours!.Value, 1.9, 2.2);
        Assert.InRange(gamma.BytesReceivedPerDay!.Value, 2_000 * 24 / 2.2 - 1, 2_000 * 24 / 1.9 + 1);
        Assert.InRange(gamma.BytesReceivedPerDay!.Value / 2_000.0, 24 / (gamma.BytesMeasuredHours!.Value + 0.06), 24 / (gamma.BytesMeasuredHours!.Value - 0.06));
        Assert.InRange(gamma.BytesSentPerDay!.Value, 200 * 24 / 2.2 - 1, 200 * 24 / 1.9 + 1);
    }

    [Fact]
    public async Task CollectionLog_ReturnsEachRunsBytes_AndANullRunStaysNull()
    {
        RequireStore();

        var ct = TestContext.Current.CancellationToken;
        var recent = await _viewer!.GetRecentCollectionLogAsync(ServerId, _now.AddDays(-1), _now.AddMinutes(1), cancellationToken: ct);

        Assert.Equal(5, recent.Count);
        var alpha = recent.Where(r => r.CollectorName == "alpha").OrderBy(r => r.CollectionTime).ToList();
        Assert.Equal([1_000L, 2_000L, (long?)null], alpha.Select(r => r.BytesReceived).ToArray());
        Assert.Equal([100L, 200L, (long?)null], alpha.Select(r => r.BytesSent).ToArray());
        Assert.All(recent.Where(r => r.CollectorName == "beta"), r =>
        {
            Assert.Null(r.BytesReceived);
            Assert.Null(r.BytesSent);
        });

        var drill = await _viewer.GetCollectionLogByCollectorAsync(ServerId, "alpha", _now.AddDays(-1), _now.AddMinutes(1), ct);

        Assert.Equal(3, drill.Count);
        var ordered = drill.OrderBy(r => r.CollectionTime).ToList();
        Assert.Equal([1_000L, 2_000L, (long?)null], ordered.Select(r => r.BytesReceived).ToArray());
        Assert.Equal([100L, 200L, (long?)null], ordered.Select(r => r.BytesSent).ToArray());
    }
}
