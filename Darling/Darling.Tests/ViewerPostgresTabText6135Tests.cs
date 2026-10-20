/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6135: the text a PostgreSQL server shows in the viewer. Each test calls the code the screen calls and fails on the
/// defect it names: a size in raw bytes, the connection key in a sentence, no offline note, and a recommendation card
/// that carries MCP tool names.
/// </summary>
public sealed class ViewerPostgresTabText6135Tests
{
    private static readonly DateTime Now = new(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TheOverviewGrid_NamesTheServerByItsDisplayName_NotTheConnectionKey()
    {
        var server = new DarlingServer(1, "alpha-pg-01:appdb:pg", "alpha", isEnabled: true, sqlMajorVersion: null,
            monthlyCostUsd: 0m, engineKind: MonitoredEngineKind.Postgres,
            engineEdition: CollectorEngineCapability.UnknownEngineEdition);

        var rows = ViewerDataService.BuildPostgresCollectorHealth(
            server, ViewerPostgresTabs.PostgresCollectors(), Array.Empty<ViewerDataService.PostgresCollectorLogFacts>());

        var gap = rows.First(r => r.IsPermanentGap);
        Assert.Contains("alpha", gap.Explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("alpha-pg-01:appdb:pg", gap.Explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerTabHeader_SaysOffline_WithTheOverviewCardsSentence_ForAnOfflineServer()
    {
        var registered = Now.AddDays(-60);

        var offline = ViewerServerTab.FreshnessLabel(Now.AddDays(-9), registered, Now);
        Assert.StartsWith("collected ", offline, StringComparison.Ordinal);
        Assert.EndsWith(FleetRollup.OfflineNote, offline, StringComparison.Ordinal);

        var fresh = ViewerServerTab.FreshnessLabel(Now.AddMinutes(-1), registered, Now);
        Assert.StartsWith("collected ", fresh, StringComparison.Ordinal);
        Assert.DoesNotContain("Offline", fresh, StringComparison.Ordinal);

        Assert.Equal("no data collected yet", ViewerServerTab.FreshnessLabel(null, Now.AddMinutes(-5), Now));
    }

    [Fact]
    public void TheRecommendationsStatusLine_SaysOffline_OnlyForAnOfflineServer()
    {
        Assert.Equal("Last analyzed 10:00", RecommendationsViewModel.WithOfflineNote("Last analyzed 10:00", offline: false));
        Assert.Equal(FleetRollup.OfflineNote, RecommendationsViewModel.WithOfflineNote("", offline: true));
        Assert.Equal(
            "Last analyzed 10:00. " + FleetRollup.OfflineNote,
            RecommendationsViewModel.WithOfflineNote("Last analyzed 10:00", offline: true));
    }

    [Fact]
    public void ThePostgresCard_KeepsWhatWasSeenAndWhatToDo_AndDropsMcpToolsAndInternalTerms()
    {
        var advice = new AdviceBlock(
            "Head",
            "The peak was well above normal. It was judged against the hour-of-week baseline. This is the FALLBACK reading.",
            "Call get_pg_top_queries first. Cap the pool size. Review the retry policy. Then check a fleet read of fifty clusters.");

        var text = RecommendationsViewModel.ComposeShortPostgresAdvice(advice);

        Assert.Equal("The peak was well above normal.\nCap the pool size. Review the retry policy.", text);
        Assert.Null(RecommendationsViewModel.ComposeShortPostgresAdvice(new AdviceBlock("Head", "See get_pg_plans.", "Ask the agent.")));
        Assert.Null(RecommendationsViewModel.ComposeShortPostgresAdvice(null));
    }

    [Fact]
    public void EveryPostgresAdviceBlock_ReadsShortOnTheCard_WithNoToolName()
    {
        var keys = typeof(PgTargetFactKeys).GetFields()
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(k => PgTargetAdvice.Static(k) is not null)
            .ToList();
        Assert.NotEmpty(keys);

        var someBlockNamesATool = false;
        foreach (var key in keys)
        {
            var block = PgTargetAdvice.Static(key)!;
            var full = RecommendationsViewModel.ComposeAdvice(block) ?? "";
            someBlockNamesATool |= full.Contains("get_", StringComparison.Ordinal);

            var card = RecommendationsViewModel.ComposeShortPostgresAdvice(block);
            if (card is null)
                continue;

            Assert.DoesNotContain("get_", card, StringComparison.Ordinal);
            Assert.DoesNotContain("FALLBACK", card, StringComparison.Ordinal);
            Assert.True(card.Length <= full.Length, key);
            Assert.True(card.Split('\n').Length <= 2, key);
        }

        /* The loop above proves something only while some block still names a tool. */
        Assert.True(someBlockNamesATool);
    }
}
