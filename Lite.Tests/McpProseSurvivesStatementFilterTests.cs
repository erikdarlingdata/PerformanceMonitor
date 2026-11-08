/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8320 on Lite: the statement filter (#4348) sweeps every tool answer, and a value it names is replaced whole by
/// the withheld marker. A tool guide is prose the product wrote, so a guide the judge names is a wording defect
/// caught here, not a leak caught on a store. Darling's twin is <c>Darling.Tests/McpProseSurvivesStatementFilterTests</c>.
/// </summary>
public sealed class McpProseSurvivesStatementFilterTests
{
    /// <summary>Every served tool's guide tail, discovered from the served list and not a hand-kept roster, comes
    /// back from the judge as the same text. A tail that comes back as the marker names the tool.</summary>
    [Fact]
    public void EveryServedGuideTail_ComesBackReadableFromTheStatementFilter()
    {
        var withheld = McpToolsListBudgetTests.Measure().Tools
            .Where(t => t.Tail is not null && !ReferenceEquals(SensitiveStatements.Text(t.Tail), t.Tail))
            .Select(t => t.Name)
            .ToList();
        Assert.True(withheld.Count == 0, "guide tail(s) the statement filter withholds: " + string.Join(", ", withheld));
    }
}
