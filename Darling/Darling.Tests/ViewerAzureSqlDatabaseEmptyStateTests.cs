/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the viewer surfaces show on an Azure SQL Database, where the data behind them does not exist: the Default Trace,
/// CPU Scheduler and system_health grids and charts, whose collectors' own AppliesTo gates skip that engine, and the
/// Memory Overview's page-file figures and memory state, which the memory collector stores as 0 and "Available" there.
/// Before, the grids said there were no events in the window or nothing, and the Overview read "0 MB" and "Available".
///
/// <para>Each pin runs both ways, and Managed Instance (EngineEdition 8) is on the "does run" side: these collectors
/// collect there, so a rule keyed on "any Azure" would be as wrong as the old text.</para>
/// </summary>
public sealed class ViewerAzureSqlDatabaseEmptyStateTests
{
    private const string ServerName = "DarlingAzureEmptyState";
    private const int OnPremEngineEdition = 3;

    [Fact]
    public void DefaultTrace_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ViewerServerTab.DefaultTraceGapNote(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("default_trace_events", note, StringComparison.Ordinal);

        Assert.Null(ViewerServerTab.DefaultTraceGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer));
        Assert.Null(ViewerServerTab.DefaultTraceGapNote(
            ServerName, CollectorEngineCapability.AzureManagedInstanceEngineEdition, MonitoredEngineKind.SqlServer));
    }

    [Fact]
    public void CpuScheduler_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ViewerServerTab.CpuSchedulerGapNote(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("cpu_scheduler_stats", note, StringComparison.Ordinal);

        Assert.Null(ViewerServerTab.CpuSchedulerGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer));
        Assert.Null(ViewerServerTab.CpuSchedulerGapNote(
            ServerName, CollectorEngineCapability.AzureManagedInstanceEngineEdition, MonitoredEngineKind.SqlServer));
    }

    [Fact]
    public void PageFile_ReadsNotApplicable_OnAzureSqlDatabase_AndTheStoredFigureElsewhere()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable,
            ViewerServerTab.PageFileText(0, CollectorEngineCapability.AzureSqlDatabaseEngineEdition));
        Assert.Equal(ServerHardwareScope.NotApplicable,
            ViewerServerTab.PageFileText(512, CollectorEngineCapability.AzureSqlDatabaseEngineEdition));

        Assert.Equal("0 MB", ViewerServerTab.PageFileText(0, OnPremEngineEdition));
        Assert.Equal("512 MB", ViewerServerTab.PageFileText(512, OnPremEngineEdition));
        Assert.Equal("512 MB", ViewerServerTab.PageFileText(512, CollectorEngineCapability.AzureManagedInstanceEngineEdition));
    }

    /// <summary>
    /// The ten System Events sub-tabs the system_health session feeds (eight grids, two chart sub-tabs) say that the
    /// system_health_events collector does not run on an Azure SQL Database, the sentence the health-parser MCP tools
    /// return as not_collected there. Before, each grid said there were no events in the window.
    /// </summary>
    [Fact]
    public void SystemHealth_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ViewerServerTab.SystemHealthGapNote(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer);

        Assert.NotNull(note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("system_health_events", note, StringComparison.Ordinal);

        Assert.Null(ViewerServerTab.SystemHealthGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer));
        Assert.Null(ViewerServerTab.SystemHealthGapNote(
            ServerName, CollectorEngineCapability.AzureManagedInstanceEngineEdition, MonitoredEngineKind.SqlServer));
        Assert.Null(ViewerServerTab.SystemHealthGapNote(ServerName, CollectorEngineCapability.UnknownEngineEdition, engineKind: null));
    }

    /// <summary>
    /// Each "does not apply" note agrees with its collector's own AppliesTo gate on every engine edition, so the notes
    /// can never become a second, hand-kept list of what an Azure SQL Database lacks. If a note were rewritten as its
    /// own edition check, or named the wrong collector, the two would disagree here and this fails.
    /// </summary>
    [Fact]
    public void EveryGapNote_AgreesWithItsCollectorsOwnGate_OnEveryEdition()
    {
        var notes = new (string Collector, Func<int, string?> Note)[]
        {
            ("system_health_events", edition => ViewerServerTab.SystemHealthGapNote(ServerName, edition, MonitoredEngineKind.SqlServer)),
            ("default_trace_events", edition => ViewerServerTab.DefaultTraceGapNote(ServerName, edition, MonitoredEngineKind.SqlServer)),
            ("cpu_scheduler_stats", edition => ViewerServerTab.CpuSchedulerGapNote(ServerName, edition, MonitoredEngineKind.SqlServer)),
        };

        foreach (var (collector, note) in notes)
        {
            foreach (var edition in new[] { 0, 1, 2, 3, 4, 5, 6, 8, 9, 11 })
            {
                var gatedOff = !CollectorEngineCapability.IsCollectedOnEngineEdition(collector, edition);

                Assert.True(gatedOff == (note(edition) is not null),
                    $"{collector} on edition {edition}: the gate says {(gatedOff ? "not collected" : "collected")}, the note disagrees");
            }
        }
    }

    [Fact]
    public void MemoryState_ReadsNotApplicable_OnAzureSqlDatabase_AndTheStoredStateElsewhere()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable,
            ViewerServerTab.SystemMemoryStateText("Available", CollectorEngineCapability.AzureSqlDatabaseEngineEdition));

        Assert.Equal("Available physical memory is high",
            ViewerServerTab.SystemMemoryStateText("Available physical memory is high", OnPremEngineEdition));
        Assert.Equal("Available physical memory is high",
            ViewerServerTab.SystemMemoryStateText("Available physical memory is high", CollectorEngineCapability.AzureManagedInstanceEngineEdition));
        Assert.Equal("Available", ViewerServerTab.SystemMemoryStateText("Available", CollectorEngineCapability.UnknownEngineEdition));
    }

    /// <summary>
    /// The rule both apps' get_memory_stats publish through. On an Azure SQL Database the stored "Available" is not a
    /// reading, so the state is null and the note says why; the note starts with n/a, so the web tile that shows it
    /// reads n/a too. Every other edition, unknown included, keeps the stored state and has no note.
    /// </summary>
    [Fact]
    public void TheMcpMemoryState_IsNullWithItsNote_OnAzureSqlDatabaseOnly()
    {
        Assert.Null(ServerHardwareScope.MemoryStateOrNull(CollectorEngineCapability.AzureSqlDatabaseEngineEdition, "Available"));
        Assert.Equal(ServerHardwareScope.MemoryStateNote, ServerHardwareScope.MemoryStateNoteFor(CollectorEngineCapability.AzureSqlDatabaseEngineEdition));
        Assert.StartsWith(ServerHardwareScope.NotApplicable + " (", ServerHardwareScope.MemoryStateNote, StringComparison.Ordinal);

        foreach (int? edition in new int?[] { null, CollectorEngineCapability.UnknownEngineEdition, OnPremEngineEdition, CollectorEngineCapability.AzureManagedInstanceEngineEdition })
        {
            Assert.Equal("Available physical memory is high", ServerHardwareScope.MemoryStateOrNull(edition, "Available physical memory is high"));
            Assert.Null(ServerHardwareScope.MemoryStateNoteFor(edition));
        }
    }
}
