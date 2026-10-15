/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Review round 1 on #5812, pinned without a store: what the PLAN_REGRESSION candidate cache evicts (L1), why a pass
/// reused or read live (L3), that an exact-bound read's shape carries its exact window start (L3), and that every
/// PLAN_REGRESSION read, the exact-bound fallbacks included, runs under the one work memory setting (M2).
/// </summary>
public sealed class PlanRegressionCandidateCacheEvictionTests
{
    private static readonly DateTime Now = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    private static readonly PgFactCollector.PlanRegressionCandidate[] None = [];

    private static PgFactCollector.PlanRegressionCandidateCache Cache(TimeSpan refreshAfter) => new() { RefreshAfter = refreshAfter };

    [Fact]
    public void AStore_EvictsTheEntriesReadOneRefreshPeriodOrMoreBeforeIt_AndKeepsTheYoungerOnes()
    {
        var cache = Cache(TimeSpan.FromMinutes(30));
        cache.Store("store", 1, "s", Now.AddMinutes(-45), None);
        cache.Store("store", 2, "s", Now.AddMinutes(-31), None);
        cache.Store("store", 3, "s", Now.AddMinutes(-29), None);
        Assert.Equal(3, cache.Count);

        cache.Store("store", 4, "s", Now, None);

        /* Server 1 (45 minutes) and server 2 (31 minutes) are gone; server 3 (29 minutes) still serves. */
        Assert.Equal(2, cache.Count);
        Assert.Null(cache.TryGet("store", 1, "s", Now, Now, out var first));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.NoEntry, first);
        Assert.Null(cache.TryGet("store", 2, "s", Now, Now));
        Assert.NotNull(cache.TryGet("store", 3, "s", Now, Now));
        Assert.NotNull(cache.TryGet("store", 4, "s", Now, Now));
    }

    [Fact]
    public void AStore_EvictsTheEntriesOfServersNoLongerMonitored_OnlyOnceASetIsPublished()
    {
        var cache = Cache(TimeSpan.FromMinutes(30));
        cache.Store("store", 1, "s", Now, None);
        cache.Store("store", 2, "s", Now, None);
        cache.Store("other", 2, "s", Now, None);
        Assert.Equal(3, cache.Count);

        /* Server 2 is removed from the monitored set: the next store drops its entries, for every store identity. */
        cache.RetainServers([1, 3]);
        Assert.Equal(3, cache.Count);
        cache.Store("store", 3, "s", Now, None);
        Assert.Equal(2, cache.Count);
        Assert.NotNull(cache.TryGet("store", 1, "s", Now, Now));
        Assert.Null(cache.TryGet("store", 2, "s", Now, Now));
        Assert.Null(cache.TryGet("other", 2, "s", Now, Now));
    }

    [Fact]
    public void TheEntryJustStored_IsNeverEvictedByItsOwnStore()
    {
        var cache = Cache(TimeSpan.FromMinutes(30));
        cache.RetainServers([1]);
        cache.Store("store", 9, "s", Now, None);
        Assert.Equal(1, cache.Count);
        Assert.NotNull(cache.TryGet("store", 9, "s", Now, Now));
    }

    [Fact]
    public void EveryDecision_NamesItsReason()
    {
        var window = TimeSpan.FromMinutes(30);
        var entry = new PgFactCollector.PlanRegressionCandidateCache.Entry("s", Now.AddMinutes(-10), None);
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.Hit,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, "s", Now, Now, window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.NoEntry,
            PgFactCollector.PlanRegressionCandidateCache.Decide(null, "s", Now, Now, window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.Stale,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, "s", Now.AddMinutes(25), Now.AddMinutes(25), window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.Stale,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, "s", Now.AddMinutes(-11), Now.AddMinutes(-11), window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.NotCurrent,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, "s", Now.AddHours(-1), Now, window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.ShapeDiffers,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, "t", Now, Now, window));
    }

    /// <summary>The daily read's shape: <c>$6</c> (the watermarks) goes last, so a builder tick that moves a watermark is
    /// a miss reported as such, and a change elsewhere is not.</summary>
    [Fact]
    public void AMovedWatermark_IsAMissReportedAsOne()
    {
        static NpgsqlCommand Daily(DateTime watermark, string scope)
        {
            var cmd = new NpgsqlCommand("SELECT daily");
            cmd.Parameters.AddWithValue(7);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, new DateTime(2026, 9, 30));
            cmd.Parameters.AddWithValue(new[] { new DateOnly(2026, 10, 13), new DateOnly(2026, 10, 14) });
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, new DateTime(2026, 10, 14));
            cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, new DateTime(2026, 9, 29));
            cmd.Parameters.AddWithValue(new[] { new DateTime(2026, 10, 14), watermark });
            cmd.Parameters.AddWithValue(new[] { scope });
            return cmd;
        }

        var floor = new DateTime(2026, 9, 30);
        var days = new List<DateOnly> { new(2026, 10, 13), new(2026, 10, 14) };
        string Shape(DateTime watermark, string scope) =>
            PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Daily(watermark, scope), 5, floor, days, watermarkParameter: 5);

        var tenOClock = new DateTime(2026, 10, 14, 10, 0, 0);
        var entry = new PgFactCollector.PlanRegressionCandidateCache.Entry(Shape(tenOClock, "db1"), Now, None);
        var window = TimeSpan.FromMinutes(30);
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.Hit,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, Shape(tenOClock, "db1"), Now, Now, window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.WatermarkMoved,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, Shape(tenOClock.AddHours(1), "db1"), Now, Now, window));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.ShapeDiffers,
            PgFactCollector.PlanRegressionCandidateCache.Decide(entry, Shape(tenOClock, "db2"), Now, Now, window));
    }

    /// <summary>L3: the table and raw reads are bound to the exact 14-day edge, so a one-hour call does not take a four-hour
    /// pass's read, whose edge sits three hours earlier.</summary>
    [Fact]
    public void AnExactBoundRead_WithADifferentWindowStart_IsADifferentShape()
    {
        static NpgsqlCommand Table(DateTime rawWindowStart)
        {
            var cmd = new NpgsqlCommand("SELECT table");
            cmd.Parameters.AddWithValue(7);
            cmd.Parameters.AddWithValue(rawWindowStart);
            cmd.Parameters.AddWithValue(rawWindowStart.AddDays(-1));
            cmd.Parameters.AddWithValue(rawWindowStart.AddDays(-1));
            return cmd;
        }

        var end = new DateTime(2026, 10, 14, 12, 0, 0);
        var fourHours = end.AddHours(-4).AddDays(-14);
        var oneHour = end.AddHours(-1).AddDays(-14);
        var floor = fourHours.Date;
        var pass = PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Table(fourHours), 4, floor, [], exactWindowStart: fourHours);
        var call = PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Table(oneHour), 4, floor, [], exactWindowStart: oneHour);
        Assert.Equal(floor, oneHour.Date);
        Assert.NotEqual(pass, call);
        Assert.Equal(pass, PgFactCollector.PlanRegressionCandidateCache.ShapeOf(Table(fourHours), 4, floor, [], exactWindowStart: fourHours));

        var cache = Cache(TimeSpan.FromMinutes(30));
        cache.Store("store", 7, pass, Now, None);
        Assert.Null(cache.TryGet("store", 7, call, Now, Now, out var decision));
        Assert.Equal(PgFactCollector.PlanRegressionCandidateCache.ReuseDecision.ShapeDiffers, decision);

        /* The collector binds it: the exact-bound shapes take rawWindowStart, the daily shape its watermarks. */
        var fact = FactSource();
        Assert.Contains("exactWindowStart: readsDays ? null : rawWindowStart, watermarkParameter: readsDays ? 5 : null", fact, StringComparison.Ordinal);
        Assert.Contains("out decision);", fact, StringComparison.Ordinal);
        Assert.Contains("_logger?.LogDebug(\"PLAN_REGRESSION candidates for server {ServerId}: {Decision}", fact, StringComparison.Ordinal);
    }

    /// <summary>M2: one constant sets the work memory, and both the daily read's snapshot and the exact-bound reads' own
    /// short transaction run it before the read.</summary>
    [Fact]
    public void EveryPlanRegressionRead_RunsUnderTheOneWorkMemSetting()
    {
        Assert.Equal("SET LOCAL work_mem = '64MB'", PgFactCollector.PlanRegressionWorkMemSql);

        var fact = FactSource();
        Assert.Equal(1, Count(fact, "new NpgsqlCommand(PlanRegressionWorkMemSql"));
        Assert.DoesNotContain("work_mem =", fact, StringComparison.Ordinal);
        Assert.Equal(2, Count(fact, "await BoundTheReadAsync();"));

        var fallback = fact.IndexOf("if (snapshot is null)", StringComparison.Ordinal);
        var fallbackBegin = fact.IndexOf("snapshot = await connection.BeginTransactionAsync(context.CancellationToken);", StringComparison.Ordinal);
        var read = fact.IndexOf("readsDays ? PlanRegressionDailySql : readsTable ? PlanRegressionTableSql : PlanRegressionSql, connection, snapshot)", StringComparison.Ordinal);
        Assert.True(fallback > 0 && fallback < fallbackBegin && fallbackBegin < read,
            "the exact-bound read must begin its own transaction, and bound it, before the read command");
        Assert.True(fact.IndexOf("await BoundTheReadAsync();", fallbackBegin, StringComparison.Ordinal) < read);
    }

    [Fact]
    public void TheCacheHasNoForget_AndTheServicePublishesItsServerSet()
    {
        var root = Root();
        var cache = File.ReadAllText(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.PlanRegressionCache.cs"));
        Assert.DoesNotContain("Forget", cache, StringComparison.Ordinal);

        var worker = File.ReadAllText(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        Assert.Contains("PlanRegressionCandidateCache.Shared.RetainServers(initialServers.Select(s => s.ServerId));", worker, StringComparison.Ordinal);
        Assert.Contains("PlanRegressionCandidateCache.Shared.RetainServers(view.EnabledServers.Select(s => s.ServerId));", worker, StringComparison.Ordinal);
    }

    private static int Count(string text, string part)
    {
        var count = 0;
        for (var at = text.IndexOf(part, StringComparison.Ordinal); at >= 0; at = text.IndexOf(part, at + part.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string FactSource()
    {
        var source = File.ReadAllText(Path.Combine(Root(), "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs"));
        var start = source.IndexOf("private async Task CollectPlanRegressionFactsAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        return source[start..source.IndexOf("internal const int PlanRegressionMaxOffenders", start, StringComparison.Ordinal)];
    }

    private static string Root()
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
