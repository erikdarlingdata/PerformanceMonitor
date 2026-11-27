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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9000: the Store Growth self-alert, end to end against the hourly sizes in <c>collect.store_metrics</c>. Each fact seeds hourly rows into a
/// scratch store of its own, reads them back through the service's read, and applies the verdict to a recording deliverer.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database through
   ScratchPostgres and never touches the shared store's tables. */
public sealed class StoreGrowthAlertLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9000 Store Growth tests (they mint their own scratch databases).";
    private const double Gb = 1024.0 * 1024.0 * 1024.0;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One series: its name, kind, and the growth in GB of each day, newest day first (index 0 is the last 24 hours). A day without a
    /// value ends the history: no hourly rows are written before it. <paramref name="SkipHoursAgo"/> leaves out the hourly rows it names (a
    /// restart, a skipped sweep).</summary>
    private sealed record Series(string Name, string Kind, double[] DailyGrowthGb, Func<int, bool>? SkipHoursAgo = null);

    private static async Task<StoreGrowthReading> SeedAndReadAsync(string connectionString, CancellationToken ct, params Series[] series)
    {
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var newest = DateTime.UtcNow;
        newest = new DateTime(newest.Year, newest.Month, newest.Day, newest.Hour, 0, 0, DateTimeKind.Unspecified);

        foreach (var s in series)
        {
            var times = new List<DateTime>();
            var sizes = new List<long>();
            const double nowSizeGb = 1000.0;
            for (var hoursAgo = 0; hoursAgo < s.DailyGrowthGb.Length * 24; hoursAgo++)
            {
                var day = hoursAgo / 24;
                var back = 0.0;
                for (var d = 0; d < day; d++)
                {
                    back += s.DailyGrowthGb[d];
                }

                back += (hoursAgo % 24) / 24.0 * s.DailyGrowthGb[day];
                if (s.SkipHoursAgo?.Invoke(hoursAgo) == true)
                {
                    continue;
                }

                times.Add(newest.AddHours(-hoursAgo));
                sizes.Add((long)((nowSizeGb - back) * Gb));
            }

            await using var insert = new NpgsqlCommand(
                "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes) " +
                "SELECT t, $1, $2, b FROM unnest($3::timestamp[], $4::bigint[]) AS u(t, b)", connection);
            insert.Parameters.AddWithValue(s.Name);
            insert.Parameters.AddWithValue(s.Kind);
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times.ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = sizes.ToArray() });
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await StoreGrowthReader.ReadAsync(dataSource, ct);
    }

    private static double[] Days(int count, double gb, double lastDayGb)
    {
        var days = Enumerable.Repeat(gb, count).ToArray();
        days[0] = lastDayGb;
        return days;
    }

    private static Series Store(double[] days, Func<int, bool>? skip = null) => new("store", StoreSelfMetrics.StoreObjectKind, days, skip);

    private static Series Dimension(string name, double[] days, Func<int, bool>? skip = null) =>
        new(name, StoreSelfMetrics.DimensionObjectKind, days, skip);

    [Fact]
    public async Task TheFieldCase_AStoreOnTwoGbADay_ThenATwentyGbDayInOneObject_FiresAndNamesThatObject()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(16, 2.0, 20.0)),
            Dimension("query_plan_dim", Days(16, 1.2, 19.0)),
            Dimension("query_text_dim", Days(16, 0.5, 0.6)));

        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await e.ApplyStoreGrowthAsync(reading, (long)(500 * Gb), ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Growth", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("query_plan_dim", fired.DetailText);
        Assert.DoesNotContain("query_text_dim", fired.DetailText);
        Assert.Contains("19 GB", fired.DetailText);
        Assert.Contains("1.2 GB", fired.DetailText);
        Assert.Contains("20 GB", fired.DetailText);
        Assert.Contains("2 GB", fired.DetailText);
        Assert.Contains("about 25 days", fired.DetailText);
    }

    [Fact]
    public async Task WhenTheVolumeIsUnknown_TheTextHasNoDaysUntilFull()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(16, 2.0, 20.0)), Dimension("query_plan_dim", Days(16, 1.2, 19.0)));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, null, ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("query_plan_dim", fired.DetailText);
        Assert.DoesNotContain("full in about", fired.DetailText);
    }

    [Fact]
    public async Task ASteadyHighGrowthStore_BaselineIsHigh_NoAlert()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(21, 10.0, 10.0)), Dimension("query_plan_dim", Days(21, 8.0, 8.0)));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, (long)(500 * Gb), ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ASpikeUnderTheFloors_NoAlert_AndAZeroFloorLetsTheSameSpikeThrough()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(16, 0.5, 4.0)), Dimension("query_plan_dim", Days(16, 0.1, 1.5)));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, null, ct);
        Assert.Empty(h.Deliverer.Outcomes);

        var noFloors = new DarlingSelfAlertTests.Harness { StoreGrowth = new StoreGrowthSettings(true, 0, 0) };
        await noFloors.Build().ApplyStoreGrowthAsync(reading, null, ct);
        Assert.Single(noFloors.Deliverer.Outcomes);
    }

    [Fact]
    public async Task SixDaysOfHistory_NoBaseline_NoAlert()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(7, 1.0, 30.0)), Dimension("query_plan_dim", Days(7, 0.5, 29.0)));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, null, ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ADayOfNegativeGrowth_CountsAsZero_NoAlertAndNoError()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var shrinkingLastDay = Days(16, 2.0, -15.0);
        var shrinkingEarlierDay = Days(16, 2.0, 2.0);
        shrinkingEarlierDay[3] = -50.0;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(shrinkingLastDay), Dimension("query_plan_dim", shrinkingEarlierDay));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, null, ct);

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AStandingBreachAlertsOnce_AndClearsWithOneResolution()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var spike = await SeedAndReadAsync(ConnectionString!, ct, Store(Days(16, 2.0, 20.0)), Dimension("query_plan_dim", Days(16, 1.2, 19.0)));
        var calm = await SeedAndReadAsync(ConnectionString!, ct, Store(Days(16, 2.0, 2.0)), Dimension("query_plan_dim", Days(16, 1.2, 1.2)));

        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await e.ApplyStoreGrowthAsync(spike, null, ct);
        h.Now = h.Now.AddHours(1);
        await e.ApplyStoreGrowthAsync(spike, null, ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyStoreGrowthAsync(calm, null, ct);
        var resolution = Assert.Single(h.History.Records);
        Assert.Equal("Store Growth Resolved", resolution.MetricName);
    }

    [Fact]
    public async Task AMissingSampleAtTheDayAnchor_IsNotARecovery_NoResolutionAndNoSecondAlert()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        var spike = await SeedAndReadAsync(ConnectionString!, ct, Store(Days(16, 2.0, 20.0)), Dimension("query_plan_dim", Days(16, 1.2, 19.0)));
        /* The same spike, but nothing was written from 21 to 27 hours back, so no sample sits within 3 hours of the 24-hour anchor. */
        static bool Gap(int hoursAgo) => hoursAgo is >= 21 and <= 27;
        var missing = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(16, 2.0, 20.0), Gap), Dimension("query_plan_dim", Days(16, 1.2, 19.0), Gap));

        var h = new DarlingSelfAlertTests.Harness();
        var e = h.Build();
        await e.ApplyStoreGrowthAsync(spike, null, ct);
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddHours(1);
        await e.ApplyStoreGrowthAsync(missing, null, ct);
        Assert.Empty(h.History.Records);
        Assert.Single(h.Deliverer.Outcomes);

        /* The sample is back and the same spike still stands: the alert is still the one that fired, not a new one. */
        h.Now = h.Now.AddHours(1);
        await e.ApplyStoreGrowthAsync(spike, null, ct);
        Assert.Empty(h.History.Records);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task ASampleTwoHoursOffTheDayAnchor_IsUsed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        /* Nothing from 23 to 25 hours back, so the nearest rows to the 24-hour anchor sit 2 hours off it. */
        static bool Gap(int hoursAgo) => hoursAgo is >= 23 and <= 25;
        var reading = await SeedAndReadAsync(ConnectionString!, ct,
            Store(Days(16, 2.0, 20.0), Gap), Dimension("query_plan_dim", Days(16, 1.2, 19.0), Gap));

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyStoreGrowthAsync(reading, null, ct);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Store Growth", fired.MetricName);
        Assert.Contains("query_plan_dim", fired.DetailText);
    }

    [Fact]
    public async Task TheStoredSwitchAndFloors_SurviveTheServiceReload_AndStartOnForAFreshStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await new StoreConfigProvider(dataSource).SeedIfEmptyAsync(new DarlingConfig(), ct);
        var fresh = new DarlingConfig();
        StoreConfigProvider.ApplyToConfig(fresh, (await new StoreConfigProvider(dataSource).LoadViewAsync(fresh, ct))!);
        var freshSettings = new DarlingAlertSettings(fresh);
        Assert.True(freshSettings.SelfStoreGrowthEnabled);
        Assert.Equal(5, freshSettings.SelfStoreGrowthFloorGb);
        Assert.Equal(2, freshSettings.SelfStoreGrowthObjectFloorGb);

        await using (var update = new NpgsqlCommand(
            "UPDATE config.config_alert_settings SET self_store_growth_enabled = FALSE, self_store_growth_floor_gb = 40, self_store_growth_object_floor_gb = 0 WHERE id = 1", connection))
        {
            await update.ExecuteNonQueryAsync(ct);
        }

        var reloaded = new DarlingConfig();
        StoreConfigProvider.ApplyToConfig(reloaded, (await new StoreConfigProvider(dataSource).LoadViewAsync(reloaded, ct))!);
        var settings = new DarlingAlertSettings(reloaded);
        Assert.False(settings.SelfStoreGrowthEnabled);
        Assert.Equal(40, settings.SelfStoreGrowthFloorGb);
        Assert.Equal(0, settings.SelfStoreGrowthObjectFloorGb);
    }
}
