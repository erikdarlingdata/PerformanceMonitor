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
/// What three viewer surfaces show on an Azure SQL Database, where the data behind them does not exist: the Default
/// Trace and CPU Scheduler grids, whose collectors' own AppliesTo gates skip that engine, and the Memory Overview's
/// page-file figures, which the memory collector stores as 0 there. Before, the Default Trace grid said there were no
/// events in the window, the CPU Scheduler grid said nothing, and the page file read "0 MB".
///
/// <para>Each pin runs both ways, and Managed Instance (EngineEdition 8) is on the "does run" side: both collectors
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
}
