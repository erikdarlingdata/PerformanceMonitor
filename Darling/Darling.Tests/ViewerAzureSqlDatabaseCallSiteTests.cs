/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The code that USES the Azure SQL Database helpers: the viewer tabs, get_memory_stats and the web Memory tiles.
/// <see cref="ViewerAzureSqlDatabaseEmptyStateTests"/> holds each helper's answer, but a tab that stopped calling one
/// would still pass there, so each call site is pinned here by its source text, inside the method that runs it. In C#
/// the comments and string literals are blanked before the search, so a call left only in a comment does not count.
/// </summary>
public sealed class ViewerAzureSqlDatabaseCallSiteTests
{
    private static readonly string[] SystemEventsFile =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.SystemEvents.cs"];

    private static readonly string[] SystemHealthChartsFile =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.SystemHealthCharts.cs"];

    private const string GapNoteArguments = "(_server.ServerName, _server.EngineEdition, _server.EngineKind)";

    /// <summary>The eight grids the system_health session feeds, by the prefix of their loader and message block.</summary>
    public static TheoryData<string> SystemHealthGrids => new()
    {
        "SchedulerIssues", "SevereErrors", "MemoryConditions", "MemoryBroker", "MemoryNodeOom", "SignificantWaits", "CpuTasks", "IoIssues",
    };

    [Theory]
    [MemberData(nameof(SystemHealthGrids))]
    public void EachSystemHealthGrid_ShowsItsEmptyStateThroughTheGapNote(string grid)
    {
        var body = MethodBody(SystemEventsFile, $"Task Load{grid}Async(");

        Assert.Contains($"ShowSystemHealthEmptyState({grid}NoDataMessage, data.Count);", body, StringComparison.Ordinal);
        Assert.DoesNotContain($"{grid}NoDataMessage.Visibility =", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSystemHealthEmptyState_UsesTheGapNote()
    {
        var body = MethodBody(SystemEventsFile, "void ShowSystemHealthEmptyState(");

        Assert.Contains($"SystemHealthGapNote{GapNoteArguments} is {{ }} gap", body, StringComparison.Ordinal);
        Assert.Contains("message.Text = gap;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void BothChartSubTabs_ShowTheGapNote()
    {
        Assert.Contains("ShowSystemHealthChartsNote();", MethodBody(SystemHealthChartsFile, "Task LoadSystemHealthChartsAsync("), StringComparison.Ordinal);

        var body = MethodBody(SystemHealthChartsFile, "void ShowSystemHealthChartsNote(");
        Assert.Contains($"SystemHealthGapNote{GapNoteArguments}", body, StringComparison.Ordinal);
        Assert.Contains("(CorruptionEventsNoDataMessage, CorruptionEventsCharts)", body, StringComparison.Ordinal);
        Assert.Contains("(ContentionEventsNoDataMessage, ContentionEventsCharts)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultTraceGrid_ShowsTheGapNote()
    {
        var body = MethodBody(SystemEventsFile, "Task LoadDefaultTraceEventsAsync(");

        Assert.Contains($"DefaultTraceGapNote{GapNoteArguments} is {{ }} gap", body, StringComparison.Ordinal);
        Assert.Contains("DefaultTraceNoDataMessage.Text = gap;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCpuSchedulerGrid_ShowsTheGapNote()
    {
        var body = MethodBody(["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.CpuScheduler.cs"], "Task LoadCpuSchedulerAsync(");

        Assert.Contains($"var gap = CpuSchedulerGapNote{GapNoteArguments};", body, StringComparison.Ordinal);
        Assert.Contains("CpuSchedulerNoDataMessage.Text = gap ?? ", body, StringComparison.Ordinal);
        Assert.Contains("CpuSchedulerNoDataMessage.Visibility = gap is null ? Visibility.Collapsed : Visibility.Visible;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMemoryOverview_ReadsNotApplicable_ThroughTheHelpers()
    {
        var body = MethodBody(["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Memory.cs"], "void RenderMemorySummary(");

        Assert.Contains("TotalPageFileText.Text = PageFileText(stats.TotalPageFileMb, _server.EngineEdition);", body, StringComparison.Ordinal);
        Assert.Contains("AvailablePageFileText.Text = PageFileText(stats.AvailablePageFileMb, _server.EngineEdition);", body, StringComparison.Ordinal);
        Assert.Contains("MemoryStateText.Text = SystemMemoryStateText(stats.SystemMemoryState, _server.EngineEdition);", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// get_memory_stats reads the edition on its success path and publishes the state through the shared rule. The tool
    /// needs a live store to run, so its use of the rule is pinned here and the rule itself in
    /// <see cref="ViewerAzureSqlDatabaseEmptyStateTests.TheMcpMemoryState_IsNullWithItsNote_OnAzureSqlDatabaseOnly"/>.
    /// </summary>
    [Fact]
    public void GetMemoryStats_PublishesTheStateThroughTheSharedRule()
    {
        var body = MethodBody(["Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"], "Task<string> GetMemoryStats(");

        Assert.Contains("var engineEdition = await DarlingEngineCapability.EngineEditionAsync(postgres, resolved.ServerId, cancellationToken);", body, StringComparison.Ordinal);
        Assert.Contains("system_memory_state = ServerHardwareScope.MemoryStateOrNull(engineEdition, stats.SystemMemoryState),", body, StringComparison.Ordinal);
        Assert.Contains("system_memory_state_note = ServerHardwareScope.MemoryStateNoteFor(engineEdition),", body, StringComparison.Ordinal);
        Assert.DoesNotContain("system_memory_state = stats.SystemMemoryState", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The web Memory tiles (the server page and the Memory pressure custom-view template) show the server's note where
    /// the state is null, through the stat tile's nullKey, the table cell's rule. The page writes no sentence of its
    /// own: the text arrives in the payload.
    /// </summary>
    [Fact]
    public void TheWebMemoryTiles_ShowTheServersNote_WhereTheStateIsNull()
    {
        const string Tile = "{ key: \"system_memory_state\", label: \"System state\", format: \"text\", small: true, nullKey: \"system_memory_state_note\" }";

        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains(Tile, Slice(tabs, "const MEMORY_STATS = [", "];"), StringComparison.Ordinal);

        var templates = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js");
        Assert.Contains(Tile, Slice(templates, "read: \"get_memory_stats\"", "]"), StringComparison.Ordinal);

        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        var stat = Slice(panels, "function vizStat(", "function vizLine(");
        Assert.Contains("const why = raw == null && s.nullKey ? getPath(data, s.nullKey) : null;", stat, StringComparison.Ordinal);
        Assert.Contains("text: why != null && why !== \"\" ? String(why) : applyFormat(s.format, raw)", stat, StringComparison.Ordinal);

        Assert.DoesNotContain("memory-state source", tabs + templates + panels, StringComparison.Ordinal);
    }

    private static string MethodBody(string[] file, string anchor)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(file));
        var at = code.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{anchor} not found in {string.Join('/', file)}");

        return CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));
    }

    private static string Slice(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from + start.Length, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }
}
