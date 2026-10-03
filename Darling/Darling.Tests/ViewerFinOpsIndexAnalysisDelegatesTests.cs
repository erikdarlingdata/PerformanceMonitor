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
/// The FinOps Index Analysis reads live once, in <see cref="DarlingFinOpsIndexAnalysisReader"/>. The viewer's partial
/// holds no SQL text, calls the reader, and keeps each <c>XSql</c> constant as an alias of the Storage constant so
/// the viewer's SQL pins read the same text.
/// </summary>
public sealed class ViewerFinOpsIndexAnalysisDelegatesTests
{
    private static string ViewerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer",
            "ViewerDataService.FinOps.IndexAnalysis.cs")).ReplaceLineEndings("\n");

    [Fact]
    public void TheIndexAnalysisPartial_HoldsNoSqlText()
    {
        var source = ViewerSource();
        Assert.DoesNotContain("@\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("CreateCommand(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("new NpgsqlCommand(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexAnalysisPartial_CallsTheStorageReader()
    {
        var source = ViewerSource();
        Assert.Contains("DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisAsync(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void EachViewerSqlConstant_EqualsTheStorageConstant()
    {
        Assert.Equal(DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql, ViewerDataService.IndexObjectStatsLatestSql);
        Assert.Equal(DarlingFinOpsIndexAnalysisReader.ServerCompressionInfoSql, ViewerDataService.ServerCompressionInfoSql);
    }
}
