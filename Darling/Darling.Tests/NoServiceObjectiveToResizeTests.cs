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
    [InlineData(5, "System", true)]
    [InlineData(5, "system", true)]
    [InlineData(5, "GP_Gen5_2", false)]
    [InlineData(5, null, false)]
    [InlineData(2, "System", false)]
    [InlineData(3, "System", false)]
    [InlineData(null, "System", false)]
    public void Predicate_IsTrueOnlyForEdition5WithTheSystemObjective(int? edition, string? objective, bool expected) =>
        Assert.Equal(expected, ServerHardwareScope.HasNoServiceObjectiveToResize(edition, objective));

    [Fact]
    public void RecommendationRules_StandDownOnTheSharedPredicate()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Recommendations.cs"));
        Assert.Contains("HasNoServiceObjectiveToResize", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ViewerDataService.FinOps.Utilization.cs")]
    [InlineData("ViewerDataService.FinOps.Inventory.cs")]
    public void ProvisioningVerdictCallers_PassTheServiceObjective(string file)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file));
        Assert.Matches(new Regex(@"ProvisioningVerdict\s*\.\s*Evaluate\s*\([^;]*serviceObjective", RegexOptions.Singleline), source);
    }
}
