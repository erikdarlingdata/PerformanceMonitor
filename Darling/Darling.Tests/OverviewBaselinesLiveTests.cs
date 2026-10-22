/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6291: <c>get_overview_baselines</c>, the web-only read the Overview strips draw their baseline band and anomaly markers from,
/// against a real store. It asks the same <c>PgBaselineProvider</c> the desktop Viewer's lanes ask, so a server with a history of CPU
/// samples answers a CPU baseline of that history's mean and dispersion, and a server with none answers "empty" (the page then draws no
/// band and no markers, never a band at 0).
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): the class mints its own scratch database through <c>ScratchPostgres</c>, which is
/// dropped when the test ends (nothing for <c>LiveStoreCleanup</c> to delete), so it leaves nothing in the shared <c>live-postgres</c> store and needs no serialization against it.</para>
/// </summary>
public sealed class OverviewBaselinesLiveTests
{
    private const int WithHistory = -629101;
    private const int NoHistory = -629102;

    [Fact]
    public async Task AServerWithCpuHistory_AnswersItsCpuBaseline_AndAServerWithNoneAnswersEmpty()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live overview-baselines test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, WithHistory, "overview-baselines-history", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, NoHistory, "overview-baselines-none", ct);

            /* Twelve days of CPU samples every ten minutes, alternating 10 and 30 percent: mean 20, standard deviation 10. */
            var now = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) " +
                "SELECT 6291000 + n, ts, $1, $2, ts, CASE WHEN n % 2 = 0 THEN 10 ELSE 30 END, 0 " +
                "FROM (SELECT row_number() OVER (ORDER BY ts) AS n, ts FROM generate_series($3::timestamp, $4::timestamp, interval '10 minutes') AS ts) AS x",
                WithHistory, "overview-baselines-history", now.AddDays(-12), now.AddHours(-2));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var windowStart = now.AddHours(-1).ToString("o", System.Globalization.CultureInfo.InvariantCulture);

            using var withHistory = JsonDocument.Parse(
                await DarlingWebOverviewBaselines.GetOverviewBaselines(postgres, null, "overview-baselines-history", windowStart, ct));
            Assert.Equal("ok", withHistory.RootElement.GetProperty("status").GetString());
            var baselines = withHistory.RootElement.GetProperty("baselines");
            var cpu = baselines.GetProperty("cpu");
            Assert.InRange(cpu.GetProperty("mean").GetDouble(), 19, 21);
            Assert.InRange(cpu.GetProperty("effective_std_dev").GetDouble(), 9, 11);
            Assert.True(cpu.GetProperty("sample_count").GetInt64() > 5);
            /* The store holds no waits, I/O or blocking: those three lanes are null, never an object with a zero mean. */
            foreach (var lane in new[] { "waits", "io", "blocking" })
                Assert.Equal(JsonValueKind.Null, baselines.GetProperty(lane).ValueKind);

            using var none = JsonDocument.Parse(
                await DarlingWebOverviewBaselines.GetOverviewBaselines(postgres, null, "overview-baselines-none", windowStart, ct));
            Assert.Equal("empty", none.RootElement.GetProperty("status").GetString());
            Assert.False(none.RootElement.TryGetProperty("baselines", out _));

            var refused = await DarlingWebOverviewBaselines.GetOverviewBaselines(postgres, null, "overview-baselines-history", "last tuesday", ct);
            Assert.Contains("window_start", refused);
        }
    }
}
