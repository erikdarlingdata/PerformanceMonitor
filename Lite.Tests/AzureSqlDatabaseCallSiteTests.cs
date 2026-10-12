/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Darling.Tests;
using Lite.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The tab code that USES the Azure SQL Database helpers. <see cref="AzureSqlDatabaseEmptyStateTests"/> holds each
/// helper's answer, but a tab that stopped calling one would still pass there, so each call site is pinned here by its
/// source text, inside the method that runs it. Comments and string literals are blanked before the search, so a call
/// left only in a comment does not count.
/// </summary>
public sealed class AzureSqlDatabaseCallSiteTests
{
    private const string SystemEventsFile = "Lite/Controls/ServerTab.SystemEvents.cs";
    private const string SystemHealthChartsFile = "Lite/Controls/ServerTab.SystemHealthCharts.cs";

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

        Assert.Contains("SystemHealthGapNote(_server.DisplayName, _isAzureSqlDatabase) is { } gap", body, StringComparison.Ordinal);
        Assert.Contains("message.Text = gap;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void BothChartSubTabs_ShowTheGapNote()
    {
        Assert.Contains("ShowSystemHealthChartsNote();", MethodBody(SystemHealthChartsFile, "Task RefreshSystemHealthChartsAsync("), StringComparison.Ordinal);

        var body = MethodBody(SystemHealthChartsFile, "void ShowSystemHealthChartsNote(");
        Assert.Contains("SystemHealthGapNote(_server.DisplayName, _isAzureSqlDatabase)", body, StringComparison.Ordinal);
        Assert.Contains("(CorruptionEventsNoDataMessage, CorruptionEventsCharts)", body, StringComparison.Ordinal);
        Assert.Contains("(ContentionEventsNoDataMessage, ContentionEventsCharts)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultTraceGrid_ShowsTheGapNote()
    {
        var body = MethodBody(SystemEventsFile, "Task LoadDefaultTraceEventsAsync(");

        Assert.Contains("DefaultTraceGapNote(_server.DisplayName, _isAzureSqlDatabase) is { } gap", body, StringComparison.Ordinal);
        Assert.Contains("DefaultTraceNoDataMessage.Text = gap;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCpuSchedulerGrid_ShowsTheGapNote()
    {
        var body = MethodBody("Lite/Controls/ServerTab.CpuScheduler.cs", "Task RefreshCpuSchedulerAsync(");

        Assert.Contains("var gap = CpuSchedulerGapNote(_server.DisplayName, _isAzureSqlDatabase);", body, StringComparison.Ordinal);
        Assert.Contains("CpuSchedulerNoDataMessage.Text = gap ?? ", body, StringComparison.Ordinal);
        Assert.Contains("CpuSchedulerNoDataMessage.Visibility = gap is null ? Visibility.Collapsed : Visibility.Visible;", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMemoryOverview_ReadsNotApplicable_ThroughTheHelpers()
    {
        var body = MethodBody("Lite/Controls/ServerTab.Charts.cs", "void UpdateMemorySummary(");

        Assert.Contains("TotalPageFileText.Text = PageFileText(stats.TotalPageFileMb, _isAzureSqlDatabase);", body, StringComparison.Ordinal);
        Assert.Contains("AvailablePageFileText.Text = PageFileText(stats.AvailablePageFileMb, _isAzureSqlDatabase);", body, StringComparison.Ordinal);
        Assert.Contains("MemoryStateText.Text = SystemMemoryStateText(stats.SystemMemoryState, _isAzureSqlDatabase);", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// One edition for the whole Memory Overview panel. The two captions over its first figures take <c>_engineEdition</c>, and the
    /// page-file and memory-state lines beside them take <c>_isAzureSqlDatabase</c>, which is that same field compared with edition
    /// 5. So the panel cannot name a figure "Physical Memory" above a page file of "n/a". Nothing in the method reads an edition
    /// off the memory row.
    /// </summary>
    [Fact]
    public void TheMemoryOverview_ReadsTheTabsOwnEdition_ForItsCaptionsAndForItsOtherLines()
    {
        var body = MethodBody("Lite/Controls/ServerTab.Charts.cs", "void UpdateMemorySummary(");

        Assert.Contains("PhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabTotalLabel(_engineEdition);", body, StringComparison.Ordinal);
        Assert.Contains("AvailablePhysicalMemoryLabel.Text = ServerHardwareScope.MemoryTabAvailableLabel(_engineEdition);", body, StringComparison.Ordinal);
        Assert.Contains("private bool _isAzureSqlDatabase => _engineEdition == ServerHardwareScope.AzureSqlDatabaseEngineEdition;", Code("Lite/Controls/ServerTab.xaml.cs"), StringComparison.Ordinal);

        /* Five lines read an edition: two spell it _engineEdition and three spell it _isAzureSqlDatabase. No other source is
           read in the method, and a property read off the row (stats.EngineEdition and the like) would show up as EngineEdition. */
        Assert.Equal(2, CountOf(body, "_engineEdition"));
        Assert.Equal(3, CountOf(body, "_isAzureSqlDatabase"));
        Assert.DoesNotContain("EngineEdition", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The tab holds the connection check's edition as a number, so a failed check (0) is told apart from a box, and
    /// every tab load fills it from the store before any loader reads it.
    /// </summary>
    [Fact]
    public void TheTab_FallsBackToTheStoredEdition_BeforeEveryLoad()
    {
        var tab = Code("Lite/Controls/ServerTab.xaml.cs");
        Assert.Contains("_engineEdition = sqlEngineEdition;", tab, StringComparison.Ordinal);
        Assert.Contains("private bool _isAzureSqlDatabase => _engineEdition == ServerHardwareScope.AzureSqlDatabaseEngineEdition;", tab, StringComparison.Ordinal);

        var mainWindow = Code("Lite/MainWindow.xaml.cs");
        Assert.Contains("status.HasMsdbAccess, status.SqlEngineEdition,", mainWindow, StringComparison.Ordinal);

        var load = MethodBody("Lite/Controls/ServerTab.Refresh.cs", "Task RefreshVisibleTabAsync(");
        var fallback = load.IndexOf("await RefreshEngineEditionAsync();", StringComparison.Ordinal);
        Assert.True(fallback >= 0 && fallback < load.IndexOf("switch (MainTabControl.SelectedIndex)", StringComparison.Ordinal),
            "RefreshVisibleTabAsync must fill the edition in before it dispatches to any loader");

        var refresh = MethodBody("Lite/Controls/ServerTab.Refresh.cs", "Task RefreshEngineEditionAsync(");
        Assert.Contains("ResolveEngineEditionAsync(_engineEdition, () => Task.Run(() => _dataService.GetSqlEngineEditionAsync(_serverId)))", refresh, StringComparison.Ordinal);
    }

    /// <summary>
    /// get_memory_stats has ONE edition, read ONCE, through <c>McpEngineCapability.EngineEditionAsync</c>: the newest collected
    /// <c>server_properties</c> row, which is also what <c>NotCollectedStatusAsync</c> and every other Lite MCP gate read. The
    /// payload's <c>engine_edition</c>, <c>memory_note</c> and memory-state pair are all built from that value, so the tool cannot
    /// disagree with its own gate. The memory row carries no edition at all. The payload's answers are pinned in
    /// <c>AzureSqlDatabaseMemoryScopeTests</c>; this pins where the value comes from.
    /// </summary>
    [Fact]
    public void GetMemoryStats_ReadsTheEditionOnce_FromTheEngineCapabilitySource_AndEverythingEditionDependentFollowsIt()
    {
        const string ToolsFile = "Lite/Mcp/McpMemoryTools.cs";
        const string CapabilityFile = "Lite/Mcp/McpEngineCapability.cs";

        var body = MethodBody(ToolsFile, "Task<string> GetMemoryStats(");
        var payload = MethodBody(ToolsFile, "string MemoryStatsPayload(");

        /* One read in the tool, and no edition off the row (it has none). */
        Assert.Equal(1, CountOf(body, "EngineEditionAsync("));
        Assert.Contains("var engineEdition = await McpEngineCapability.EngineEditionAsync(dataService, resolved.ServerId);", body, StringComparison.Ordinal);
        Assert.Contains("return MemoryStatsPayload(resolved.ServerName, stats, engineEdition);", body, StringComparison.Ordinal);
        Assert.DoesNotContain("stats.EngineEdition", body + payload, StringComparison.Ordinal);

        /* Every edition-dependent line of the payload reads the one value it was handed. */
        Assert.Contains("system_memory_state = ServerHardwareScope.MemoryStateOrNull(engineEdition, stats.SystemMemoryState),", payload, StringComparison.Ordinal);
        Assert.Contains("system_memory_state_note = ServerHardwareScope.MemoryStateNoteFor(engineEdition),", payload, StringComparison.Ordinal);
        Assert.Contains("engine_edition = engineEdition == CollectorEngineCapability.UnknownEngineEdition ? (int?)null : engineEdition", payload, StringComparison.Ordinal);
        Assert.Contains("if (!ServerHardwareScope.HardwareIsTheHosts(engineEdition))", payload, StringComparison.Ordinal);

        /* That source is the one the not_collected gate reads: the newest collected server_properties row. */
        Assert.Contains("return await dataService.GetSqlEngineEditionAsync(serverId);", MethodBody(CapabilityFile, "Task<int> EngineEditionAsync("), StringComparison.Ordinal);
        Assert.Contains("var engineEdition = await EngineEditionAsync(dataService, serverId);", MethodBody(CapabilityFile, "Task<string?> NotCollectedStatusAsync("), StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    /// <summary>The body of the method whose declaration contains <paramref name="anchor"/>, from its opening brace.</summary>
    private static string MethodBody(string file, string anchor)
    {
        var code = Code(file);
        var at = code.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{anchor} not found in {file}");

        return CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));
    }

    private static string Code(string file) => CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(file));
}
