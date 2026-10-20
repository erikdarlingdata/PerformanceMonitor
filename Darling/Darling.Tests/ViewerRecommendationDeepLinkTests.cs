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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6134: a recommendation card's deep link follows the engine of the server it belongs to. A PostgreSQL finding's
/// link used to open the SQL Server Queries tabs, which a PostgreSQL server's tab set hides.
/// </summary>
public sealed class ViewerRecommendationDeepLinkTests
{
    private static readonly DateTime Start = new(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime End = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

    private static RecommendationCardViewModel CardFor(string rootFactKey, bool isPostgres)
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = End,
            ServerId = 7,
            ServerName = "srv",
            Severity = 1.0,
            Confidence = 0.9,
            Category = "cpu",
            StoryPath = "a -> b",
            StoryPathHash = "hash",
            StoryText = "",
            RootFactKey = rootFactKey,
            IncidentId = "",
            FactCount = 1,
            TimeRangeStart = Start,
            TimeRangeEnd = End,
        };
        var row = new ViewerFindingRow
        {
            SeverityBand = ViewerDataService.SeverityBand(1.0),
            RawSeverity = 1.0,
            Title = "Finding",
            Category = "cpu",
            DatabaseName = "",
            Finding = finding,
        };

        var vm = RecommendationsViewModel.FromFindings(new[] { row }, "srv", isPostgres: isPostgres);
        return vm.Sections.Single().Cards.Single();
    }

    [Theory]
    [InlineData("PLAN_REGRESSION")]
    [InlineData("CPU_SPIKE")]
    [InlineData("ANOMALY_QUERY_DURATION")]
    public void PostgresFinding_LinkOpensTheActivityTab_NotActiveQueries(string rootFactKey)
    {
        var card = CardFor(rootFactKey, isPostgres: true);

        Assert.True(card.ShowOpenInActiveQueries);
        Assert.Equal(RecommendationLinkTarget.PgQueryShapes, card.LinkTarget);
        Assert.Equal("Open in Activity", card.OpenLinkLabel);
    }

    [Theory]
    [InlineData("BLOCKING_EVENTS")]
    [InlineData("BLOCKING_CHAIN")]
    [InlineData("DEADLOCKS")]
    public void PostgresBlockingFinding_LinkOpensTheBlockingSubTab(string rootFactKey)
    {
        var card = CardFor(rootFactKey, isPostgres: true);

        Assert.True(card.ShowOpenInActiveQueries);
        Assert.Equal(RecommendationLinkTarget.PgBlocking, card.LinkTarget);
    }

    [Theory]
    [InlineData("RUNNING_JOBS")]
    [InlineData("TRACE_FLAGS")]
    [InlineData("PLAN_CACHE_BLOAT")]
    [InlineData("MEMORY_GRANT_PENDING")]
    public void PostgresFindingWithNoPostgresView_OffersNoLink(string rootFactKey)
    {
        var card = CardFor(rootFactKey, isPostgres: true);

        Assert.Equal(RecommendationLinkTarget.None, card.LinkTarget);
        Assert.False(card.ShowOpenInActiveQueries);
    }

    [Theory]
    [InlineData("PLAN_REGRESSION")]
    [InlineData("BLOCKING_EVENTS")]
    [InlineData("RUNNING_JOBS")]
    public void SqlServerFinding_StillOpensActiveQueries(string rootFactKey)
    {
        var card = CardFor(rootFactKey, isPostgres: false);

        Assert.True(card.ShowOpenInActiveQueries);
        Assert.Equal(RecommendationLinkTarget.SqlServerActiveQueries, card.LinkTarget);
        Assert.Equal("Open in Active Queries", card.OpenLinkLabel);
    }
}
