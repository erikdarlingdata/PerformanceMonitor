/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Database Resources reads live in <see cref="DarlingFinOpsDatabaseResourcesReader"/>; the viewer keeps its public
/// constants as aliases and delegates both reads to it, so the SQL has one copy.
/// </summary>
public sealed class ViewerFinOpsDatabaseResourcesDelegatesTests
{
    private static string Workload([CallerFilePath] string testFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(testFile)!, "..", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Workload.cs"))
            .ReplaceLineEndings("\n");

    [Fact]
    public void ViewerWorkloadPartial_NoLongerHoldsTheMovedSqlText()
    {
        var source = Workload();

        Assert.DoesNotContain("WITH workload AS", source);
        Assert.DoesNotContain("FULL JOIN io i", source);
        Assert.DoesNotContain("CAST(c.cpu_time_ms * 100.0", source);
        Assert.DoesNotContain("WorkloadCteRaw", source);
        Assert.DoesNotContain("ConsumerCteRaw", source);
    }

    [Fact]
    public void ViewerReads_DelegateToTheStorageReader()
    {
        var source = Workload();

        Assert.Contains("DarlingFinOpsDatabaseResourcesReader.GetDatabaseResourceUsageAsync(", source);
        Assert.Contains("DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync(", source);
        Assert.Contains("DatabaseResourceUsageRow.From", source);
        Assert.Contains("TopResourceConsumerRow.From", source);
    }

    [Fact]
    public void ViewerConstants_EqualTheStorageConstants()
    {
        Assert.Equal(DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSql, ViewerDataService.DatabaseResourceUsageSql);
        Assert.Equal(DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSql, ViewerDataService.TopResourceConsumersSql);
    }
}
