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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7644: the Overview's I/O band is in the unit of the line, and the band steps by the server's local hour. Both run against a real
/// store: the chart-unit arm is SQL, and the hourly steps read the server's own clock from the store.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <c>ScratchPostgres</c>, dropped
/// when the test ends, so nothing is left in the shared <c>live-postgres</c> store.</para>
/// </summary>
public sealed class OverviewIoBandLiveTests
{
    private const int IoServer = -764401;
    private const int ZoneServer = -764402;

    /// <summary>The metric name is spelled out so the test reads the same on a build that has no constant for it.</summary>
    private const string ChartMetric = "io_latency_chart";

    /// <summary>
    /// Every collection holds two files: a small one with 10 slow reads (100 ms each) and the busy one with 10,000 fast reads (1 ms each).
    /// The strip draws 11000 / 10010 = 1.0989 ms per collection. The per-file arm averages the two files' own ratios (100 and 1), 50.5 ms.
    /// </summary>
    [Fact]
    public async Task TheChartUnitArm_MeansThePerCollectionLine_AndThePerFileArmDoesNot()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live overview I/O band test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, IoServer, "overview-io-band", ct);

        var start = DateTime.SpecifyKind(new DateTime(2026, 10, 1, 0, 0, 0), DateTimeKind.Unspecified);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, delta_reads, delta_writes, delta_stall_read_ms) " +
            "SELECT 764400000 + n * 2 + f, ts, $1, $2, 'D', CASE WHEN f = 0 THEN 'small' ELSE 'busy' END, " +
            "CASE WHEN f = 0 THEN 10 ELSE 10000 END, 0, CASE WHEN f = 0 THEN 1000 ELSE 10000 END " +
            "FROM (SELECT row_number() OVER (ORDER BY ts) AS n, ts FROM generate_series($3::timestamp, $4::timestamp, interval '15 minutes') AS ts) AS x " +
            "CROSS JOIN generate_series(0, 1) AS f",
            IoServer, "overview-io-band", start, start.AddDays(10));

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new PgBaselineProvider(postgres);
        var at = start.AddDays(8);

        var chart = await provider.GetBaselineAsync(IoServer, ChartMetric, at, ct);
        var perFile = await provider.GetBaselineAsync(IoServer, MetricNames.IoLatency, at, ct);

        Assert.InRange(chart.Mean, 1.09, 1.11);
        Assert.InRange(perFile.Mean, 50, 51);
    }

    /// <summary>
    /// A server at UTC+5:30 whose CPU is 20 plus twice its LOCAL hour, so each local hour has its own mean. A window of three hours
    /// answers one step per local hour it crosses, each starting at :30 UTC, each carrying the mean of its own local hour.
    /// </summary>
    [Fact]
    public async Task AZoneAtFiveThirty_StepsOnItsLocalHours_EachWithItsOwnHoursBucket()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live overview I/O band test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ZoneServer, "overview-zone-band", ct);

        var now = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES (764402, $1, $2, $3, 330)",
            now, ZoneServer, "overview-zone-band");
        /* Local hour h reads 20 + 2h, alternating 10 above and below, so its mean is 20 + 2h and its deviation is 10. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) " +
            "SELECT 764410000 + n, ts, $1, $2, ts, 20 + 2 * EXTRACT(HOUR FROM ts + interval '330 minutes') + CASE WHEN n % 2 = 0 THEN -10 ELSE 10 END, 0 " +
            "FROM (SELECT row_number() OVER (ORDER BY ts) AS n, ts FROM generate_series($3::timestamp, $4::timestamp, interval '10 minutes') AS ts) AS x",
            ZoneServer, "overview-zone-band", now.AddDays(-12), now);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var windowStart = now.AddHours(-3);
        using var reply = JsonDocument.Parse(await DarlingWebOverviewBaselines.GetOverviewBaselines(
            postgres, null, "overview-zone-band", windowStart.ToString("o", CultureInfo.InvariantCulture), ct));

        var steps = reply.RootElement.GetProperty("baselines").GetProperty("cpu").GetProperty("steps").EnumerateArray().ToArray();
        DateTime At(JsonElement step, string name) => DateTime.Parse(step.GetProperty(name).GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

        /* Three hours back from now crosses three or four local hours, whichever side of :30 UTC now falls. */
        var firstHourStart = new DateTime(windowStart.Year, windowStart.Month, windowStart.Day, windowStart.Hour, 30, 0, DateTimeKind.Utc);
        if (firstHourStart > windowStart) firstHourStart = firstHourStart.AddHours(-1);
        var expected = (int)Math.Ceiling((now - firstHourStart).TotalHours);
        Assert.Equal(expected, steps.Length);
        foreach (var step in steps)
        {
            var from = At(step, "from");
            Assert.Equal(30, from.Minute);
            Assert.Equal(TimeSpan.FromHours(1), At(step, "to") - from);
            var localHour = from.AddMinutes(330).Hour;
            Assert.InRange(step.GetProperty("mean").GetDouble(), 20 + 2 * localHour - 1, 20 + 2 * localHour + 1);
        }
    }
}
