/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR3 (W3 wording carry): a filtered empty answer reads the same in Darling and Lite. These are the words Lite's
/// <c>McpDatabaseSelectionTests</c> pins with the same literals: the name for one database, "the chosen databases" for two or more.
/// </summary>
public sealed class BlockingWaitsFilteredEmptyWordingTests
{
    [Fact]
    public void Describe_IsTheNameForOne_AndTheChosenDatabasesForTwoOrMore_AndNothingForAll()
    {
        Assert.Null(DatabaseFilter.All.Describe());
        Assert.Equal("A", DatabaseFilter.One("A").Describe());
        Assert.Equal("the chosen databases", DatabaseFilter.Of(["A", "B"]).Describe());
        Assert.Equal("the chosen databases", DatabaseFilter.ManyDatabasesDescription);
    }

    [Fact]
    public void TheEmptyAnswerSentence_SaysTheDatabaseForOne_AndTheChosenDatabasesForTwoOrMore()
    {
        Assert.Equal(string.Empty, DarlingMcpBlockingTools.ForChosenDatabases(DatabaseFilter.All));
        Assert.Equal(" for the database A", DarlingMcpBlockingTools.ForChosenDatabases(DatabaseFilter.One("A")));
        Assert.Equal(" for the chosen databases", DarlingMcpBlockingTools.ForChosenDatabases(DatabaseFilter.Of(["A", "B"])));
    }
}
