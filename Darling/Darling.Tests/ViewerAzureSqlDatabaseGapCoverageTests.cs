/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every collector that does not run on an Azure SQL Database either has a viewer surface that says so, or a stated reason
/// it needs none. The collectors are read from the catalog (the ones <see cref="CollectorEngineCapability"/> says no
/// engine-edition-5 server runs), so a collector added later with no entry here fails this class, and so does an entry for
/// a collector that now runs there.
///
/// <para>Before, only the Default Trace, CPU Scheduler and system_health surfaces said so. Running Jobs, Server
/// Configuration, Trace Flags, the two config-change grids, Memory Pressure Events and the database-state editor showed an
/// empty grid, chart or "0 databases" with no message.</para>
/// </summary>
public sealed class ViewerAzureSqlDatabaseGapCoverageTests
{
    private const string ServerName = "EngineGapServer";
    private const int OnPremEngineEdition = 3;

    /// <summary>One surface: the code file that loads it, the XAML that declares its message element, and that element's name.</summary>
    private sealed record Surface(string LoaderFile, string XamlFile, string Message);

    /// <summary>What a collector that does not run on an Azure SQL Database has: surfaces that say so, or the reason it needs none.</summary>
    private sealed record Coverage(Surface[] Surfaces, string Reason);

    private static Surface OnServerTab(string loaderFile, string message) => new(loaderFile, "ViewerServerTab.xaml", message);

    private static Coverage Says(params Surface[] surfaces) => new(surfaces, "");

    private static Coverage Needs(string reason) => new([], reason);

    private const string JobsReason =
        "Fleet-wide Job History tab; an Azure SQL Database server has no SQL Server Agent, so it has no rows and no filter entry.";

    private const string AvailabilityGroupsReason =
        "Fleet-wide Availability Groups tab, hidden until rows exist; its empty state already says these collectors do not run on Azure SQL Database.";

    private static readonly Dictionary<string, Coverage> Map = new(StringComparer.Ordinal)
    {
        ["running_jobs"] = Says(OnServerTab("ViewerServerTab.RunningJobs.cs", "RunningJobsNoDataMessage")),
        ["server_config"] = Says(
            OnServerTab("ViewerServerTab.Filters.cs", "ServerConfigNoDataMessage"),
            OnServerTab("ViewerServerTab.ConfigChanges.cs", "ServerConfigChangesNoDataMessage")),
        ["trace_flags"] = Says(
            OnServerTab("ViewerServerTab.Filters.cs", "TraceFlagsNoDataMessage"),
            OnServerTab("ViewerServerTab.ConfigChanges.cs", "TraceFlagChangesNoDataMessage")),
        ["memory_pressure_events"] = Says(OnServerTab("ViewerServerTab.Memory.cs", "MemoryPressureEventsNoDataMessage")),
        ["database_states"] = Says(new Surface("DatabaseStateOverridesWindow.xaml.cs", "DatabaseStateOverridesWindow.xaml", "StatusText")),
        ["cpu_scheduler_stats"] = Says(OnServerTab("ViewerServerTab.CpuScheduler.cs", "CpuSchedulerNoDataMessage")),
        ["system_health_events"] = Says(OnServerTab("ViewerServerTab.SystemEvents.cs", "SchedulerIssuesNoDataMessage")),
        ["default_trace_events"] = Says(OnServerTab("ViewerServerTab.SystemEvents.cs", "DefaultTraceNoDataMessage")),
        ["job_history"] = Needs(JobsReason),
        ["agent_status"] = Needs(JobsReason),
        ["ag_replica_states"] = Needs(AvailabilityGroupsReason),
        ["ag_database_replica_states"] = Needs(AvailabilityGroupsReason),
    };

    private static string ViewerFile(string file) => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    /// <summary>The collectors in the catalog that no Azure SQL Database server runs, by name.</summary>
    private static List<string> CollectorsThatDoNotRunOnAzureSqlDatabase() =>
        CollectorCatalog.All
            .Where(d => !CollectorEngineCapability.IsCollectedOnEngineEdition(d, CollectorEngineCapability.AzureSqlDatabaseEngineEdition))
            .Select(d => d.Name)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// The map's keys are exactly the catalog's collectors that do not run on an Azure SQL Database. A collector added to
    /// the catalog with such a gap and no entry here fails with its name; an entry whose collector now runs there, or no
    /// longer exists, fails as stale.
    /// </summary>
    [Fact]
    public void TheMap_NamesEveryCollectorThatDoesNotRunOnAzureSqlDatabase_AndNoOther()
    {
        var gaps = CollectorsThatDoNotRunOnAzureSqlDatabase();

        var missing = gaps.Where(n => !Map.ContainsKey(n)).ToList();
        var stale = Map.Keys.Where(n => !gaps.Contains(n, StringComparer.Ordinal)).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(missing.Count == 0, "Collectors with no surface and no reason: " + string.Join(", ", missing));
        Assert.True(stale.Count == 0, "Entries for collectors that run on an Azure SQL Database or do not exist: " + string.Join(", ", stale));
    }

    /// <summary>Each entry has surfaces or a reason, not neither, and a reason is a sentence and not a blank.</summary>
    [Fact]
    public void EveryEntry_HasASurfaceOrAReason()
    {
        foreach (var (collector, coverage) in Map)
        {
            if (coverage.Surfaces.Length == 0)
            {
                Assert.False(string.IsNullOrWhiteSpace(coverage.Reason), collector + " has neither a surface nor a reason.");
            }
        }
    }

    /// <summary>
    /// Each surface's message element is declared in its XAML, and its loader names the collector as a quoted string, the
    /// way a call to the gap helper or the capability sentence spells it. A comment that mentions the collector does not
    /// count, so removing the call that shows the note fails here. The loader also refers to the message element.
    /// </summary>
    [Fact]
    public void EverySurface_DeclaresItsMessage_AndItsLoaderNamesTheCollector()
    {
        foreach (var (collector, coverage) in Map)
        {
            foreach (var surface in coverage.Surfaces)
            {
                var xaml = ViewerFile(surface.XamlFile);
                var loader = ViewerFile(surface.LoaderFile);

                Assert.True(
                    xaml.Contains("x:Name=\"" + surface.Message + "\"", StringComparison.Ordinal),
                    $"{surface.XamlFile} does not declare x:Name=\"{surface.Message}\" for {collector}.");
                Assert.True(
                    loader.Contains("\"" + collector + "\"", StringComparison.Ordinal),
                    $"{surface.LoaderFile} does not name \"{collector}\" as a string, so it does not show the note for {collector}.");
                Assert.True(
                    loader.Contains(surface.Message, StringComparison.Ordinal),
                    $"{surface.LoaderFile} does not refer to {surface.Message} for {collector}.");
            }
        }
    }

    /// <summary>
    /// The helper every new surface calls: the sentence the Default Trace grid, the PostgreSQL panels and the MCP tools'
    /// not_collected answer give, for an Azure SQL Database. Null on an on-premises server and on Managed Instance, which
    /// run the collector, so those surfaces change nothing.
    /// </summary>
    [Fact]
    public void EngineGapNote_SaysTheCollectorDoesNotRun_OnAzureSqlDatabaseOnly()
    {
        var note = ViewerServerTab.EngineGapNote(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs");

        Assert.NotNull(note);
        Assert.Equal(
            CollectorEngineCapability.NotCollectedMessage(
                ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs"),
            note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("running_jobs", note, StringComparison.Ordinal);

        Assert.Null(ViewerServerTab.EngineGapNote(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs"));
        Assert.Null(ViewerServerTab.EngineGapNote(
            ServerName, CollectorEngineCapability.AzureManagedInstanceEngineEdition, MonitoredEngineKind.SqlServer, "running_jobs"));
    }

    /// <summary>
    /// The Memory Overview shows the memory model the collector stored, except that its "N/A" reads n/a, the same word the
    /// Overview's other figures that do not apply use.
    /// </summary>
    [Fact]
    public void MemoryModelText_ReadsNotApplicable_ForNA_AndLeavesEveryOtherValueAlone()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable, ViewerServerTab.MemoryModelText("N/A"));
        Assert.Equal(ServerHardwareScope.NotApplicable, ViewerServerTab.MemoryModelText("n/a"));

        Assert.Equal("n/a", ViewerServerTab.MemoryModelText("N/A"));
        Assert.Equal("CONVENTIONAL", ViewerServerTab.MemoryModelText("CONVENTIONAL"));
        Assert.Equal("LOCK_PAGES", ViewerServerTab.MemoryModelText("LOCK_PAGES"));
        Assert.Equal("", ViewerServerTab.MemoryModelText(""));
    }
}
