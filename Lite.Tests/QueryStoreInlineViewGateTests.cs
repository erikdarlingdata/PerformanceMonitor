/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Query Store grid's inline View button is enabled by <see cref="QueryStoreRow.HasQueryPlan"/>. Lite keeps no plan text
/// for a Query Store row: the plan is fetched from the monitored server's Query Store by plan id when it is asked for, as
/// the row's context menu and the history window do. So a row can be viewed when it names a plan, and only then.
/// </summary>
public sealed class QueryStoreInlineViewGateTests
{
    [Fact]
    public void ARowThatNamesAPlan_CanBeViewed_WithNoPlanTextOnTheRow()
    {
        var row = new QueryStoreRow { QueryId = 3 };

        Assert.False(row.HasQueryPlan, "a Query Store row with no plan id can be viewed: there is no plan to fetch for it");
        row.PlanId = 7;
        Assert.True(row.HasQueryPlan, "a Query Store row with plan id 7 and no plan text cannot be viewed: the View button is disabled on every row the grid shows");
    }

    [Fact]
    public void ARowThatNamesNoPlan_CannotBeViewed()
    {
        Assert.False(new QueryStoreRow { QueryId = 3, PlanId = 0 }.HasQueryPlan);
    }
}
