/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5809: the table form of the Query Store regressions read, without a database. The first form shipped with its two
/// table arms declared below the field that concatenated them, and static field initializers run in textual order, so
/// both copies (service and viewer) began at <c>baseline_performance</c>: every call failed with 42601 and quietly fell
/// back to raw. These pins hold the composed text, and the fault log that would have shown it.
/// </summary>
public sealed class QueryStoreRegressionsTableReadTests
{
    [Fact]
    public void ServiceTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
    }

    [Fact]
    public void ViewerTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(ViewerDataService.QueryStoreRegressionsTableSql);
    }

    private static void AssertTableShape(string sql)
    {
        Assert.StartsWith("WITH deduped_baseline AS (", sql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("deduped_recent AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_interval_wide", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_stats", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATableReadFault_WarnsOncePerProcessWithItsMessage_ThenLogsAtDebug()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("first fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("second fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("third fault text"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Debug));
            Assert.Contains(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("first fault text", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("second fault text", StringComparison.Ordinal));
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }

    [Fact]
    public void ATableReadFault_WithNoLogger_LeavesTheOnceFlagForTheFirstLoggedFault()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(null, new InvalidOperationException("unscoped fault"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("scoped fault"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("scoped fault", logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }
}
