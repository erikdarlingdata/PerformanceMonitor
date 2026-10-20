/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis.Recommendations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>#6159: a recommendation card that is the only finding in its section does not repeat the title its expander header already shows.</summary>
public sealed class LiteRecommendationCardTitleTests
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -240);

    private static LiteRecommendationItem Finding(string title, string incidentId) => new()
    {
        Severity = LiteRecommendationSeverity.Warning,
        RawSeverity = 1.0,
        Title = title,
        ServerName = "PRODSQL",
        IncidentId = incidentId,
    };

    [Fact]
    public void ALoneFinding_HidesTheCardTitle_BecauseTheHeaderCarriesIt()
    {
        var vm = LiteRecommendationsViewModel.FromItems(new[] { Finding("High CPU", "") }, Eastern);

        var card = Assert.Single(Assert.Single(vm.Sections).Cards);
        Assert.False(card.ShowTitle);
        Assert.Contains("High CPU", vm.Sections[0].Header);
    }

    [Fact]
    public void AnIncidentOfTwoFindings_KeepsEachCardTitle()
    {
        var vm = LiteRecommendationsViewModel.FromItems(new[] { Finding("High CPU", "inc-1"), Finding("Blocking", "inc-1") }, Eastern);

        Assert.All(Assert.Single(vm.Sections).Cards, c => Assert.True(c.ShowTitle));
    }
}
