/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4546: <see cref="PlanWarningDisplay"/> pulls the viewer's warning header text and ordering out
/// of <c>PlanViewerControl.Properties.cs</c> (WPF, can't run in a unit test on macOS) so this suite
/// can pin them directly. New members, so every assertion here is a compile-only RED against dev:
/// <c>PlanWarningDisplay</c> doesn't exist on dev.
/// </summary>
public sealed class PlanViewerBenefitDisplayTests
{
    [Fact]
    public void PlanWarningHeader_WithBenefit_HasSuffix()
    {
        var warning = new PlanWarning { WarningType = "Serial Plan", MaxBenefitPercent = 67.5 };

        Assert.Equal("\u26A0 Serial Plan \u2014 up to 67.5% benefit", PlanWarningDisplay.PlanWarningHeader(warning));
    }

    [Fact]
    public void PlanWarningHeader_WithBenefit_SqlServerSource_HasTagAndSuffix()
    {
        var warning = new PlanWarning
        {
            WarningType = "Serial Plan",
            MaxBenefitPercent = 67.5,
            Source = PlanWarningSource.SqlServer
        };

        Assert.Equal("\u26A0 Serial Plan [SQL Server] \u2014 up to 67.5% benefit", PlanWarningDisplay.PlanWarningHeader(warning));
    }

    [Fact]
    public void PlanWarningHeader_WithoutBenefit_HasNoSuffix()
    {
        var warning = new PlanWarning { WarningType = "Local Variables", MaxBenefitPercent = null };

        Assert.Equal("\u26A0 Local Variables", PlanWarningDisplay.PlanWarningHeader(warning));
    }

    [Fact]
    public void OrderByBenefit_OrdersDescending_NullsLast()
    {
        var low = new PlanWarning { WarningType = "Low", MaxBenefitPercent = 20 };
        var high = new PlanWarning { WarningType = "High", MaxBenefitPercent = 80 };
        var unscored = new PlanWarning { WarningType = "Unscored", MaxBenefitPercent = null };

        var ordered = PlanWarningDisplay.OrderByBenefit(new[] { low, unscored, high }).ToList();

        Assert.Equal(new[] { "High", "Low", "Unscored" }, ordered.Select(w => w.WarningType));
    }
}
