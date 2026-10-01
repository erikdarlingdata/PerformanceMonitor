/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An Azure SQL Database logical server's <c>master</c> has no service objective to resize, so FinOps gives it no
/// right-sizing advice and no provisioning verdict.
/// </summary>
public sealed class NoServiceObjectiveToResizeTests
{
    [Theory]
    [InlineData(5, "Azure SQL Database (System)", true)]
    [InlineData(5, "Azure SQL Database (General Purpose)", false)]
    [InlineData(5, "Azure SQL Database (Hyperscale)", false)]
    [InlineData(5, null, false)]
    [InlineData(5, "System", false)]
    [InlineData(2, "Azure SQL Database (System)", false)]
    [InlineData(null, "Azure SQL Database (System)", false)]
    [InlineData(2, "Enterprise Edition (System)", false)]
    public void Predicate_IsTrueOnlyForEdition5WithTheSystemDatabaseEdition(int? engineEdition, string? edition, bool expected) =>
        Assert.Equal(expected, ServerHardwareScope.HasNoServiceObjectiveToResize(engineEdition, edition));

    [Fact]
    public void RecommendationRules_StandDownOnTheSharedPredicate()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs"));
        Assert.Contains("ProvisioningVerdict.NotApplicable", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Utilization.cs")]
    [InlineData("ViewerDataService.FinOps.Inventory.cs")]
    public void ProvisioningVerdictCallers_PassTheEdition(string file)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file));
        Assert.Matches(new Regex(@"ProvisioningVerdict\s*\.\s*Evaluate\s*\([^;]*edition", RegexOptions.Singleline), source);
    }
}
