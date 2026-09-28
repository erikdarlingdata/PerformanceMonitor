/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4572: <see cref="PlanWarningDisplay.WarningSeverityColorHex"/> is the one place a warning's
/// severity becomes a colour, replacing three duplicated ternaries in
/// <c>PlanViewerControl.Properties.cs</c> and <c>PlanViewerControl.Tooltips.cs</c>. Also pins that
/// the properties panel now renders <see cref="PlanWarning.ActionableFix"/> under each warning
/// message. The colour-helper assertions are compile-only RED on dev (the member doesn't exist);
/// the source-census assertion is a runtime RED on dev (the old ternaries are still there).
/// </summary>
public sealed class PlanViewerWarningColorAndFixTests
{
    [Fact]
    public void WarningSeverityColorHex_Critical_IsRed() =>
        Assert.Equal("#E57373", PlanWarningDisplay.WarningSeverityColorHex(PlanWarningSeverity.Critical));

    [Fact]
    public void WarningSeverityColorHex_Warning_IsAmber() =>
        Assert.Equal("#FFB347", PlanWarningDisplay.WarningSeverityColorHex(PlanWarningSeverity.Warning));

    [Fact]
    public void WarningSeverityColorHex_Info_IsBlue() =>
        Assert.Equal("#6BB5FF", PlanWarningDisplay.WarningSeverityColorHex(PlanWarningSeverity.Info));

    [Fact]
    public void PlanWarning_WithActionableFix_CarriesTheFixText()
    {
        var warning = new PlanWarning
        {
            WarningType = "Serial Plan",
            Message = "This operator ran single-threaded.",
            ActionableFix = "Consider raising cost threshold for parallelism."
        };

        Assert.Equal("Consider raising cost threshold for parallelism.", warning.ActionableFix);
    }

    [Fact]
    public void PlanWarning_WithoutActionableFix_IsNull()
    {
        var warning = new PlanWarning { WarningType = "Local Variables", Message = "Uses a local variable." };

        Assert.Null(warning.ActionableFix);
    }

    /// <summary>
    /// No inline severity-colour hex literal is left in the WPF viewer control: every site resolves
    /// through <see cref="PlanWarningDisplay.WarningSeverityColorHex"/> instead of repeating the
    /// ternary. RED on dev, where the three sites (plan warnings, node warnings, tooltip warnings)
    /// each still spell out "#E57373"/"#FFB347"/"#6BB5FF" directly.
    /// </summary>
    [Fact]
    public void PlanViewerControl_HasNoInlineSeverityColorLiterals()
    {
        var repoRoot = FindRepoRoot();
        var uiDir = Path.Combine(repoRoot, "PerformanceMonitor.Ui");
        Assert.True(Directory.Exists(uiDir), $"Expected {uiDir} to exist.");

        foreach (var file in Directory.EnumerateFiles(uiDir, "PlanViewerControl.*.cs"))
        {
            var text = File.ReadAllText(file);
            Assert.DoesNotContain("#E57373", text, StringComparison.Ordinal);
            Assert.DoesNotContain("#FFB347", text, StringComparison.Ordinal);
            Assert.DoesNotContain("#6BB5FF", text, StringComparison.Ordinal);
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
            dir = dir.Parent;

        Assert.NotNull(dir);
        return dir!.FullName;
    }
}
