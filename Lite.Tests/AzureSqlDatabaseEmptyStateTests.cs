/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What the server-tab surfaces show on an Azure SQL Database, where the data behind them does not exist: the Default
/// Trace, CPU Scheduler and system_health grids and charts, whose collectors' own AppliesTo gates skip that engine, and
/// the Memory Overview's page-file figures and memory state, which the memory collector stores as 0 and "Available"
/// there. Before, the grids said there were no events in the window or nothing, and the Overview read "0 MB" and
/// "Available".
///
/// <para>Each pin runs both ways: a rule that answered "does not apply" or "n/a" for every server would fail here
/// as surely as the old text does.</para>
/// </summary>
public sealed class AzureSqlDatabaseEmptyStateTests
{
    private const string ServerName = "LiteAzureEmptyState";

    [Fact]
    public void DefaultTrace_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ServerTab.DefaultTraceGapNote(ServerName, isAzureSqlDatabase: true);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("default_trace_events", note, StringComparison.Ordinal);

        Assert.Null(ServerTab.DefaultTraceGapNote(ServerName, isAzureSqlDatabase: false));
    }

    [Fact]
    public void CpuScheduler_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ServerTab.CpuSchedulerGapNote(ServerName, isAzureSqlDatabase: true);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("cpu_scheduler_stats", note, StringComparison.Ordinal);

        Assert.Null(ServerTab.CpuSchedulerGapNote(ServerName, isAzureSqlDatabase: false));
    }

    [Fact]
    public void PageFile_ReadsNotApplicable_OnAzureSqlDatabase_AndTheStoredFigureElsewhere()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.PageFileText(0, isAzureSqlDatabase: true));
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.PageFileText(512, isAzureSqlDatabase: true));

        Assert.Equal("0 MB", ServerTab.PageFileText(0, isAzureSqlDatabase: false));
        Assert.Equal("512 MB", ServerTab.PageFileText(512, isAzureSqlDatabase: false));
    }

    /// <summary>
    /// The ten System Events sub-tabs the system_health session feeds (eight grids, two chart sub-tabs) say that the
    /// system_health_events collector does not run on an Azure SQL Database, the sentence the health-parser MCP tools
    /// return as not_collected there. Before, each grid said there were no events in the window.
    /// </summary>
    [Fact]
    public void SystemHealth_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ServerTab.SystemHealthGapNote(ServerName, isAzureSqlDatabase: true);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("system_health_events", note, StringComparison.Ordinal);

        Assert.Null(ServerTab.SystemHealthGapNote(ServerName, isAzureSqlDatabase: false));
    }

    /// <summary>
    /// Each "does not apply" note agrees with its collector's own AppliesTo gate on every engine edition, so the notes
    /// can never become a second, hand-kept list of what an Azure SQL Database lacks. The tab maps an edition to "is
    /// an Azure SQL Database" the way <c>ServerTab</c> does; if a gate ever excluded another edition, the two would
    /// disagree here and this fails.
    /// </summary>
    [Fact]
    public void EveryGapNote_AgreesWithItsCollectorsOwnGate_OnEveryEdition()
    {
        var notes = new (string Collector, Func<bool, string?> Note)[]
        {
            ("system_health_events", isAzure => ServerTab.SystemHealthGapNote(ServerName, isAzure)),
            ("default_trace_events", isAzure => ServerTab.DefaultTraceGapNote(ServerName, isAzure)),
            ("cpu_scheduler_stats", isAzure => ServerTab.CpuSchedulerGapNote(ServerName, isAzure)),
        };

        foreach (var (collector, note) in notes)
        {
            foreach (var edition in new[] { 0, 1, 2, 3, 4, 5, 6, 8, 9, 11 })
            {
                var gatedOff = !CollectorEngineCapability.IsCollectedOnEngineEdition(collector, edition);
                var isAzure = edition == ServerHardwareScope.AzureSqlDatabaseEngineEdition;

                Assert.True(gatedOff == (note(isAzure) is not null),
                    $"{collector} on edition {edition}: the gate says {(gatedOff ? "not collected" : "collected")}, the note disagrees");
            }
        }
    }

    [Fact]
    public void MemoryState_ReadsNotApplicable_OnAzureSqlDatabase_AndTheStoredStateElsewhere()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.SystemMemoryStateText("Available", isAzureSqlDatabase: true));

        Assert.Equal("Available", ServerTab.SystemMemoryStateText("Available", isAzureSqlDatabase: false));
        Assert.Equal("Available physical memory is high", ServerTab.SystemMemoryStateText("Available physical memory is high", isAzureSqlDatabase: false));
    }

    /// <summary>
    /// A connection check that failed when the tab opened (edition 0) no longer leaves the tab blind: the newest
    /// collected server_properties row decides, so the Azure SQL Database texts still show. A check that did read an
    /// edition wins and never asks the store, and a store read that fails stays unknown, which makes no claim.
    /// </summary>
    [Fact]
    public async Task AFailedConnectionCheck_FallsBackToTheStoredEdition_AndKeepsTheAzureTexts()
    {
        var edition = await ServerTab.ResolveEngineEditionAsync(CollectorEngineCapability.UnknownEngineEdition, () => Task.FromResult(5));
        var isAzure = edition == ServerHardwareScope.AzureSqlDatabaseEngineEdition;

        Assert.Equal(5, edition);
        Assert.NotNull(ServerTab.DefaultTraceGapNote(ServerName, isAzure));
        Assert.NotNull(ServerTab.CpuSchedulerGapNote(ServerName, isAzure));
        Assert.NotNull(ServerTab.SystemHealthGapNote(ServerName, isAzure));
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.PageFileText(0, isAzure));
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.SystemMemoryStateText("Available", isAzure));

        var storeAsked = false;
        Assert.Equal(3, await ServerTab.ResolveEngineEditionAsync(3, () => { storeAsked = true; return Task.FromResult(5); }));
        Assert.False(storeAsked);

        Assert.Equal(CollectorEngineCapability.UnknownEngineEdition,
            await ServerTab.ResolveEngineEditionAsync(CollectorEngineCapability.UnknownEngineEdition, () => Task.FromResult(CollectorEngineCapability.UnknownEngineEdition)));
        Assert.Equal(CollectorEngineCapability.UnknownEngineEdition,
            await ServerTab.ResolveEngineEditionAsync(CollectorEngineCapability.UnknownEngineEdition, () => throw new InvalidOperationException("store unavailable")));
    }
}
