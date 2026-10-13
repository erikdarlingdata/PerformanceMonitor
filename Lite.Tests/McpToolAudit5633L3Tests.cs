/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5633 items 10 and 12 on Lite (the Darling twins, and item 23, are pinned in <c>Darling.Tests</c>):
/// audit_config called a Developer Edition server "Enterprise"; compare_analysis accepted overlapping windows.
/// </summary>
public sealed class McpToolAudit5633L3Tests
{
    [Theory]
    [InlineData("Developer Edition (64-bit)", 3)]
    [InlineData("Enterprise Evaluation Edition (64-bit)", 3)]
    public void AuditEditionLabel_ReportsTheStoredEditionString_NotTheEngineEditionWord(string stored, int engineEdition)
    {
        var (name, note) = McpAnalysisTools.AuditEditionLabel(stored, engineEdition);

        Assert.Equal(stored, name);
        Assert.Null(note);
    }

    [Fact]
    public void AuditEditionLabel_FallsBackToTheEngineLabel_AndSaysSo_WhenTheStoreHoldsNoEditionString()
    {
        var (name, note) = McpAnalysisTools.AuditEditionLabel("", 3);

        Assert.Equal("Enterprise", name);
        Assert.Contains("engine edition", note, StringComparison.Ordinal);
    }

    [Fact]
    public void AuditConfig_ReadsTheStoredEditionProperty()
    {
        var body = ToolBody("audit_config");

        Assert.Contains("dataService.GetLatestServerPropertiesAsync(resolved.ServerId)", body, StringComparison.Ordinal);
        Assert.Contains("AuditEditionLabel(propertiesRow?.Edition, edition)", body, StringComparison.Ordinal);
        Assert.Contains("edition_note = editionNote", body, StringComparison.Ordinal);
    }

    [Fact]
    public void CompareAnalysis_UsesTheSharedBaselineWindowCheck_AndNoLongerDefaultsTo28()
    {
        var body = ToolBody("compare_analysis");

        Assert.Contains("int? baseline_hours_back = null", body, StringComparison.Ordinal);
        Assert.Contains("McpHelpers.ValidateBaselineWindow(hours_back, baseline_hours_back", body, StringComparison.Ordinal);
        Assert.DoesNotContain("baseline_hours_back = 28", body, StringComparison.Ordinal);
        Assert.DoesNotContain("baseline_hours_back <= hours_back", body, StringComparison.Ordinal);

        Assert.NotNull(McpHelpers.ValidateBaselineWindow(4, 7, out _));
        Assert.Null(McpHelpers.ValidateBaselineWindow(4, null, out var resolved));
        Assert.Equal(28, resolved);
    }

    private static string ToolBody(string tool)
    {
        var source = File.ReadAllText(Path.GetFullPath(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpAnalysisTools.cs")));
        var start = source.IndexOf($"Name = \"{tool}\"", StringComparison.Ordinal);
        Assert.True(start > 0);
        return source[start..source.IndexOf($"FormatError(\"{tool}\"", StringComparison.Ordinal)];
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
