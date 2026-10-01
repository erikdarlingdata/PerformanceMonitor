/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What three server-tab surfaces show on an Azure SQL Database, where the data behind them does not exist: the
/// Default Trace and CPU Scheduler grids, whose collectors' own AppliesTo gates skip that engine, and the Memory
/// Overview's page-file figures, which the memory collector stores as 0 there. Before, the Default Trace grid said
/// there were no events in the window, the CPU Scheduler grid said nothing, and the page file read "0 MB".
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
}
