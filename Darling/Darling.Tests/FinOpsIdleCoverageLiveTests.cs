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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The idle-database coverage rule read through <c>HasIdleCoverageAtAsync(now)</c> against a real store: the oldest query-stats
/// sample at or before now minus 7 days AND a sample on each of the 7 complete UTC days before today (today is not required).
/// Every scenario gets its own server and asks at several times of day, because the rule's day boundaries move with the clock.
/// </summary>
/* #1776 own-store: the scenarios share one scratch database, and each server's rows are its own, so nothing here shares rows with another test. */
public sealed class FinOpsIdleCoverageLiveTests
{
    private const int TimeoutSeconds = 30;
    private static readonly DateTime Day = new(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);

    /* 00:30, 06:00, 12:00, 18:00 and 23:30: either side of a day's start, middle and end. */
    private static readonly double[] HoursOfDay = { 0.5, 6, 12, 18, 23.5 };

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(int Id, string Name)> RegisterAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        var id = ServerIdHelper.GetDeterministicHashCode(name);
        await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
        return (id, name);
    }

    /// <summary>One zero-execution sample every half day from <paramref name="days"/> back up to <paramref name="now"/>, skipping those <paramref name="skip"/> accepts.</summary>
    private static async Task SeedHalfDaysAsync(
        NpgsqlConnection c, int id, string name, DateTime now, double days, Func<DateTime, bool> skip, CancellationToken ct)
    {
        var steps = (int)Math.Round(days * 2);
        for (var k = 0; k <= steps; k++)
        {
            var at = now.AddHours(-12.0 * k);
            if (skip(at)) continue;
            await FinOpsIdleCoverageSeed.InsertAsync(c, ct, id, name, at, "h" + k);
        }
    }

    [Fact]
    public async Task SixAndAHalfDaysOfHistory_GivesNoCoverage_AtAnyTimeOfDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live idle-coverage test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var hour in HoursOfDay)
        {
            var now = Day.AddHours(hour);
            /* 6.5 days sampled every half day: every complete day D-6 .. D-1 holds samples, but nothing is old enough to
               claim 7 days were watched, however many UTC dates 6.5 days touch (it touches 8 at some hours). */
            var (shortId, shortName) = await RegisterAsync(c, $"darling-idle-cov-short-{hour}", ct);
            await SeedHalfDaysAsync(c, shortId, shortName, now, 6.5, _ => false, ct);
            Assert.False(await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, shortId, now, TimeoutSeconds, ct), $"6.5 days at hour {hour}");

            /* Control: 7.5 days sampled the same way is covered at the same moment, so the scenario above is not passing for
               some unrelated reason. */
            var (longId, longName) = await RegisterAsync(c, $"darling-idle-cov-long-{hour}", ct);
            await SeedHalfDaysAsync(c, longId, longName, now, 7.5, _ => false, ct);
            Assert.True(await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, longId, now, TimeoutSeconds, ct), $"7.5 days at hour {hour}");
        }
    }

    [Fact]
    public async Task SevenAndAHalfDaysWithDMinus3Missing_GivesNoCoverage_AtAnyTimeOfDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live idle-coverage test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var hour in HoursOfDay)
        {
            var now = Day.AddHours(hour);
            var dMinus3 = now.Date.AddDays(-3);
            /* 7.5 days of history, long enough for the oldest-sample test, but no sample at all on D-3: the gap is
               inside the window, so nothing was watching that day and every database would read as idle by default. */
            var (gapId, gapName) = await RegisterAsync(c, $"darling-idle-cov-gap-{hour}", ct);
            await SeedHalfDaysAsync(c, gapId, gapName, now, 7.5, at => at.Date == dMinus3, ct);
            Assert.False(await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, gapId, now, TimeoutSeconds, ct), $"D-3 missing at hour {hour}");
        }
    }

    [Fact]
    public async Task FullCoverage_JustAfterMidnightUtc_WithNoSampleYetToday_IsCovered()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live idle-coverage test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An old sample and a noon sample on each of D-7 .. D-1; nothing yet today. Five minutes after 00:00 UTC the
           rule still holds (today is not required), and it keeps holding through the day with no new sample. */
        var seeded = Day.AddMinutes(5);
        var (id, name) = await RegisterAsync(c, "darling-idle-cov-midnight", ct);
        await FinOpsIdleCoverageSeed.SeedAsync(c, ct, id, name, seeded);

        var asked = new List<string>();
        foreach (var at in new[] { seeded, Day.AddHours(6), Day.AddHours(23.5) })
        {
            if (!await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, id, at, TimeoutSeconds, ct))
                asked.Add(at.ToString("HH:mm"));
        }
        Assert.True(asked.Count == 0, "not covered at " + string.Join(", ", asked));

        /* The same data asked a day later: the window slid one day on, and D-1 (yesterday, which holds no sample) breaks
           it, so coverage ends there rather than lasting forever. */
        Assert.False(await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, id, Day.AddDays(1).AddMinutes(5), TimeoutSeconds, ct));
    }
}
