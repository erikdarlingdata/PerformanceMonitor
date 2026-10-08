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
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5562 (lane L4): the per-read reach the web picker reads. <see cref="WebReadReach"/> is the one table; these tests
/// pin that it covers every windowed web read (a census that fails when a read is added without a row), that the
/// catalog serves it as <c>max_hours</c> on the read's <c>hours</c> param, that only bucketed trends reach past
/// <see cref="McpHelpers.MaxHoursBack"/>, that the validator of each such read takes its ceiling from the table, and
/// that the <c>ValidateWindow</c> overload refuses (never clamps) beyond the ceiling it is handed.
/// </summary>
public sealed class WebReadReachTests
{
    private static string[] WindowedReads() =>
        DarlingWebEndpoints.CatalogDescriptors
            .Where(kv => kv.Value.Params.Any(p => p.Name == "hours"))
            .Select(kv => kv.Key)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    private static JsonObject HoursParam(JsonObject catalog, string read) =>
        catalog["reads"]!.AsArray()
            .Single(r => r!["name"]!.GetValue<string>() == read)!["params"]!.AsArray()
            .Single(p => p!["name"]!.GetValue<string>() == "hours")!.AsObject();

    /* ───────────── the census ───────────── */

    [Fact]
    public void EveryWindowedWebRead_HasARowInTheReachTable()
    {
        var missing = WindowedReads().Where(n => !WebReadReach.All.ContainsKey(n)).ToArray();
        Assert.True(
            missing.Length == 0,
            "These web reads take an hours param but have no row in WebReadReach.All (#5562: add the read's reach and main collector): "
            + string.Join(", ", missing));
    }

    [Fact]
    public void EveryRowInTheReachTable_NamesAWindowedWebRead()
    {
        var windowed = WindowedReads().ToHashSet(StringComparer.Ordinal);
        var orphans = WebReadReach.All.Keys.Where(n => !windowed.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.True(
            orphans.Length == 0,
            "These WebReadReach rows name no windowed web read (renamed or removed read, or a read with no hours param): "
            + string.Join(", ", orphans));
    }

    [Fact]
    public void EveryRow_NamesACollectorTheDefaultsKnow_OrNone()
    {
        foreach (var (read, reach) in WebReadReach.All)
        {
            if (reach.Collector is not null)
            {
                Assert.True(
                    CollectorScheduleDefaults.All.ContainsKey(reach.Collector),
                    $"{read} names collector '{reach.Collector}', which CollectorScheduleDefaults does not know.");
            }
        }
    }

    /* ───────────── what reaches past 168 hours ───────────── */

    [Fact]
    public void OnlyBucketedTrends_ReachPastTheDefault_AndNoneReachesPastTheirRollup()
    {
        foreach (var (read, reach) in WebReadReach.All)
        {
            if (reach.MaxHours > McpHelpers.MaxHoursBack)
            {
                Assert.True(reach.Shape == ReadShape.BucketedTrend, $"{read} reaches {reach.MaxHours} h but is a {reach.Shape}; lists and rankings stay at {McpHelpers.MaxHoursBack}.");
                Assert.True(reach.MaxHours <= WebReadReach.RollupTrendHours, $"{read} reaches past the hourly rollup's 90 days.");
            }
            else
            {
                Assert.Equal(McpHelpers.MaxHoursBack, reach.MaxHours);
            }
        }

        Assert.Equal(
            new[] { "get_procedure_duration_trend", "get_query_duration_trend", "get_query_store_duration_trend" },
            WebReadReach.All.Where(kv => kv.Value.MaxHours > McpHelpers.MaxHoursBack).Select(kv => kv.Key).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(2160, WebReadReach.RollupTrendHours);
    }

    /// <summary>
    /// The validator of each opted-in read takes its ceiling from the table, and no other tool does: a row that says
    /// 2,160 with a validator still on the 168-hour overload would advertise a window the read then refuses, and a
    /// validator that opts in with no row would reach further than the picker was told.
    /// </summary>
    [Fact]
    public void EveryOptedInValidator_NamesAReadWhoseRowReachesPast168_AndEveryRowThatDoesIsOptedIn()
    {
        var serviceDir = PathTo("Darling", "PerformanceMonitor.Darling.Service");
        var optedIn = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var file in System.IO.Directory.EnumerateFiles(serviceDir, "*.cs", System.IO.SearchOption.AllDirectories))
        {
            if (file.EndsWith("WebReadReach.cs", StringComparison.Ordinal)
                || file.Contains($"{System.IO.Path.DirectorySeparatorChar}obj{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                || file.Contains($"{System.IO.Path.DirectorySeparatorChar}bin{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (Match m in Regex.Matches(System.IO.File.ReadAllText(file), @"WebReadReach\.MaxHoursFor\(""(?<read>[a-z_0-9]+)""\)"))
            {
                var read = m.Groups["read"].Value;
                optedIn[read] = optedIn.GetValueOrDefault(read) + 1;
            }
        }

        foreach (var read in optedIn.Keys)
        {
            Assert.True(
                WebReadReach.All.TryGetValue(read, out var reach) && reach.MaxHours > McpHelpers.MaxHoursBack,
                $"{read}'s validator takes its ceiling from the table, but its row does not reach past {McpHelpers.MaxHoursBack}.");
        }

        foreach (var (read, reach) in WebReadReach.All.Where(kv => kv.Value.MaxHours > McpHelpers.MaxHoursBack))
        {
            Assert.True(optedIn.GetValueOrDefault(read) == 1, $"{read} reaches {reach.MaxHours} h in the table but {optedIn.GetValueOrDefault(read)} validator(s) read that ceiling (want exactly one).");
        }
    }

    [Fact]
    public void MaxHoursFor_ThrowsForAReadWithNoRow_AndAnswersTheTableForOne()
    {
        Assert.Equal(2160, WebReadReach.MaxHoursFor("get_query_duration_trend"));
        Assert.Equal(168, WebReadReach.MaxHoursFor("get_top_queries_by_cpu"));
        Assert.Throws<InvalidOperationException>(() => WebReadReach.MaxHoursFor("get_no_such_read"));
    }

    /* ───────────── the catalog ───────────── */

    [Fact]
    public void Catalog_EveryHoursParam_CarriesMaxHoursFromTheTable()
    {
        var catalog = DarlingWebEndpoints.BuildCatalogNode();
        foreach (var read in WindowedReads())
        {
            var hours = HoursParam(catalog, read);
            var reach = WebReadReach.All[read];
            Assert.Equal(reach.MaxHours, hours["max_hours"]!.GetValue<int>());
            Assert.Equal(reach.Collector, hours["collector"]?.GetValue<string>());
            Assert.NotNull(hours["shape"]);
        }

        Assert.Equal(2160, HoursParam(catalog, "get_query_store_duration_trend")["max_hours"]!.GetValue<int>());
        Assert.Equal(168, HoursParam(catalog, "get_wait_trend")["max_hours"]!.GetValue<int>());
        Assert.Equal("bucketed_trend", HoursParam(catalog, "get_wait_trend")["shape"]!.GetValue<string>());
        Assert.Equal("latest_snapshot", HoursParam(catalog, "get_active_queries")["shape"]!.GetValue<string>());
    }

    [Fact]
    public void Catalog_OnlyTheHoursParamCarriesReach()
    {
        var catalog = DarlingWebEndpoints.BuildCatalogNode();
        foreach (var read in catalog["reads"]!.AsArray())
        {
            foreach (var p in read!["params"]!.AsArray())
            {
                var isHours = p!["name"]!.GetValue<string>() == "hours";
                Assert.Equal(isHours, p.AsObject().ContainsKey("max_hours"));
            }
        }
    }

    [Fact]
    public void Catalog_CollectorInterval_IsTheShippedDefaultUntilTheStoreHoldsASchedule()
    {
        var plain = HoursParam(DarlingWebEndpoints.BuildCatalogNode(), "get_query_store_duration_trend");
        Assert.Equal("query_store", plain["collector"]!.GetValue<string>());
        Assert.Equal(5, plain["collector_interval_minutes"]!.GetValue<int>());
        Assert.Equal(5, plain["collector_default_interval_minutes"]!.GetValue<int>());

        var oneMinute = HoursParam(DarlingWebEndpoints.BuildCatalogNode(), "get_wait_trend");
        Assert.Equal(1, oneMinute["collector_interval_minutes"]!.GetValue<int>());

        var noCollector = HoursParam(DarlingWebEndpoints.BuildCatalogNode(), "get_analysis_findings");
        Assert.Null(noCollector["collector"]);
        Assert.Null(noCollector["collector_interval_minutes"]);
        Assert.Null(noCollector["collector_default_interval_minutes"]);
    }

    [Fact]
    public void Catalog_CollectorInterval_FollowsTheScheduleInForce_PerServerOverFleetOverDefault()
    {
        var schedules = new[]
        {
            new ScheduleOverride(null, "query_store", 15, null, null),
            new ScheduleOverride(7, "query_store", 30, null, null),
            new ScheduleOverride(7, "wait_stats", 2, null, null),
        };

        /* No server: the fleet-wide row, and only it. */
        var fleet = DarlingWebEndpoints.BuildCatalogNode(null, schedules);
        Assert.Equal(15, HoursParam(fleet, "get_query_store_duration_trend")["collector_interval_minutes"]!.GetValue<int>());
        Assert.Equal(1, HoursParam(fleet, "get_wait_trend")["collector_interval_minutes"]!.GetValue<int>());

        /* Server 7: its own row wins over the fleet's, a column it does not set falls through, the default stays visible. */
        var server = DarlingWebEndpoints.BuildCatalogNode(7, schedules);
        var qs = HoursParam(server, "get_query_store_duration_trend");
        Assert.Equal(30, qs["collector_interval_minutes"]!.GetValue<int>());
        Assert.Equal(5, qs["collector_default_interval_minutes"]!.GetValue<int>());
        Assert.Equal(2, HoursParam(server, "get_wait_trend")["collector_interval_minutes"]!.GetValue<int>());

        /* Another server sees the fleet's row, not server 7's. */
        Assert.Equal(15, HoursParam(DarlingWebEndpoints.BuildCatalogNode(8, schedules), "get_query_store_duration_trend")["collector_interval_minutes"]!.GetValue<int>());
    }

    /* ───────────── the McpHelpers overload ───────────── */

    [Fact]
    public void ValidateWindow_WithACeiling_AcceptsUpToItAndRefusesBeyondWithoutClamping()
    {
        Assert.Null(McpHelpers.ValidateWindow(2160, null, 2160, out _));
        Assert.Null(McpHelpers.ValidateWindow(1, null, 2160, out _));

        var over = McpHelpers.ValidateWindow(2161, null, 2160, out _);
        Assert.NotNull(over);
        var message = McpHelpers.ErrorMessageOf(over!);
        Assert.Contains("2161", message);
        Assert.Contains("maximum of 2160 hours (90 days)", message);

        var zero = McpHelpers.ErrorMessageOf(McpHelpers.ValidateWindow(0, null, 2160, out _)!);
        Assert.Contains("(1-2160)", zero);
    }

    [Fact]
    public void ValidateWindow_WithACeiling_ChecksTheSpanBeforeTheAnchor_AndStillResolvesAsOf()
    {
        var spanFirst = McpHelpers.ValidateWindow(3000, "not a date", 2160, out _);
        Assert.Contains("hours_back", spanFirst!);

        var anchorBad = McpHelpers.ValidateWindow(2000, "not a date", 2160, out _);
        Assert.NotNull(anchorBad);
        Assert.Contains("as_of", anchorBad!);

        var asOf = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.Null(McpHelpers.ValidateWindow(2000, asOf.ToString("o"), 2160, out var end));
        Assert.Equal(asOf, end);
    }

    [Fact]
    public void ValidateWindow_WithoutACeiling_StaysAt168_AndSaysTheSameWords()
    {
        Assert.Equal(168, McpHelpers.MaxHoursBack);
        Assert.Null(McpHelpers.ValidateWindow(168, null, out _));
        Assert.NotNull(McpHelpers.ValidateWindow(169, null, out _));
        Assert.Equal(McpHelpers.ValidateHoursBack(169), McpHelpers.ValidateHoursBack(169, 168));
        Assert.Equal(McpHelpers.ValidateWindow(169, null, out _), McpHelpers.ValidateWindow(169, null, 168, out _));
        Assert.Contains("exceeds maximum of 168 hours (7 days)", McpHelpers.ErrorMessageOf(McpHelpers.ValidateHoursBack(169)!));
        Assert.Contains("(1-168)", McpHelpers.ErrorMessageOf(McpHelpers.ValidateHoursBack(0)!));
    }

    [Fact]
    public void ACeilingThatIsNotWholeDays_IsSpelledInHours()
    {
        Assert.Contains("maximum of 100 hours. Use", McpHelpers.ErrorMessageOf(McpHelpers.ValidateHoursBack(101, 100)!));
    }
}
