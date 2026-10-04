/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The FinOps recommendation SQL lives once, in <see cref="DarlingFinOpsRecommendationsReader"/>. The viewer's partial
/// keeps each <c>RecommendationsXSql</c> constant as an alias of the Storage constant so the viewer's SQL pins read the
/// same text, and holds no SQL literal of its own for them.
/// </summary>
public sealed class ViewerFinOpsRecommendationsDelegatesTests
{
    private static string ViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer",
            "ViewerDataService.FinOps.Recommendations.cs")).ReplaceLineEndings("\n");

    [Fact]
    public void TheRecommendationsPartial_HoldsNoSqlLiteralForTheMovedConstants()
    {
        var source = ViewerSource();
        Assert.DoesNotContain("@\"", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EachViewerSqlConstant_EqualsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsRecommendationsReader.EditionFactsSql, ViewerDataService.RecommendationsEditionFactsSql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.MemoryP95Sql, ViewerDataService.RecommendationsMemoryP95Sql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.CpuP95Sql, ViewerDataService.RecommendationsCpuP95Sql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.MaintenanceWindowSql, ViewerDataService.RecommendationsMaintenanceWindowSql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.StorageTierSql, ViewerDataService.RecommendationsStorageTierSql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.QueryStatsFirstSampleSql, ViewerDataService.RecommendationsQueryStatsFirstSampleSql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.ReservedCapacitySql, ViewerDataService.RecommendationsReservedCapacitySql);
        Assert.Equal(DarlingFinOpsRecommendationsReader.EngineEditionSql, ViewerDataService.RecommendationsEngineEditionSql);
    }
}
