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
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Visibility = System.Windows.Visibility;

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

    /// <summary>The edition a PostgreSQL target stores, and so does a SQL Server whose engine has not been read yet.</summary>
    private const int NoEngineEdition = 0;

    /// <summary>How a surface's loader ties its message element to the collector.</summary>
    private enum Wiring
    {
        /// <summary>One call names both: <c>ShowEngineGap(Message, "collector", ...)</c>.</summary>
        ShowEngineGapCall,

        /// <summary>One statement names both: a test of <c>EngineGapNote(..., "collector")</c> that sets the message's text.</summary>
        OneStatement,

        /// <summary>The surface has its own gap-note helper, which holds the collector name apart from the message, so the loader's file names both.</summary>
        OwnHelper,
    }

    /// <summary>One surface: the code file that loads it, the XAML that declares its message element, that element's name, and how the loader wires them.</summary>
    private sealed record Surface(string LoaderFile, string XamlFile, string Message, Wiring Shape);

    /// <summary>What a collector that does not run on an Azure SQL Database has: surfaces that say so, or the reason it needs none.</summary>
    private sealed record Coverage(Surface[] Surfaces, string Reason);

    private static Surface OnServerTab(string loaderFile, string message, Wiring shape = Wiring.ShowEngineGapCall) =>
        new(loaderFile, "ViewerServerTab.xaml", message, shape);

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
            OnServerTab("ViewerServerTab.ConfigChanges.cs", "ServerConfigChangesNoDataMessage", Wiring.OneStatement)),
        ["trace_flags"] = Says(
            OnServerTab("ViewerServerTab.Filters.cs", "TraceFlagsNoDataMessage"),
            OnServerTab("ViewerServerTab.ConfigChanges.cs", "TraceFlagChangesNoDataMessage", Wiring.OneStatement)),
        ["memory_pressure_events"] = Says(OnServerTab("ViewerServerTab.Memory.cs", "MemoryPressureEventsNoDataMessage")),
        ["database_states"] = Says(
            new Surface("DatabaseStateOverridesWindow.xaml.cs", "DatabaseStateOverridesWindow.xaml", "StatusText", Wiring.OwnHelper)),
        ["cpu_scheduler_stats"] = Says(OnServerTab("ViewerServerTab.CpuScheduler.cs", "CpuSchedulerNoDataMessage", Wiring.OwnHelper)),
        ["system_health_events"] = Says(OnServerTab("ViewerServerTab.SystemEvents.cs", "SchedulerIssuesNoDataMessage", Wiring.OwnHelper)),
        ["default_trace_events"] = Says(OnServerTab("ViewerServerTab.SystemEvents.cs", "DefaultTraceNoDataMessage", Wiring.OwnHelper)),
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

    /// <summary>The source with its block and line comments removed, so a comment cannot stand in for the code it describes.</summary>
    private static string WithoutComments(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline | RegexOptions.CultureInvariant);

    /// <summary>
    /// Whether the loader's code ties the message element to the collector the way the surface's <c>Shape</c> says. A call
    /// that names the message with another surface's collector does not tie it to this one.
    /// </summary>
    private static bool LoaderTiesMessageToCollector(Surface surface, string collector, string code)
    {
        var quoted = "\"" + collector + "\"";
        var message = new Regex(@"\b" + Regex.Escape(surface.Message) + @"\b", RegexOptions.CultureInvariant);

        return surface.Shape switch
        {
            Wiring.ShowEngineGapCall => code.Contains("ShowEngineGap(" + surface.Message + ", " + quoted, StringComparison.Ordinal),
            Wiring.OneStatement => code.Split(';').Any(s => s.Contains(quoted, StringComparison.Ordinal) && message.IsMatch(s)),
            _ => code.Contains(quoted, StringComparison.Ordinal) && message.IsMatch(code),
        };
    }

    /// <summary>
    /// Each surface's message element is declared in its XAML, and its loader ties that element to the collector the way the
    /// surface is wired, so two surfaces with their collectors swapped fail. The shared helper's surfaces have one call
    /// naming both, <c>ShowEngineGap(Message, "collector", ...)</c>. The two config-change grids set the message's text from
    /// <c>EngineGapNote(..., "collector")</c> in one statement. A surface with its own gap-note helper (Default Trace, CPU
    /// Scheduler, system_health, the database-state editor) keeps the collector name in that helper, so there the file names
    /// the collector as a quoted string and refers to the message. Comments are removed first, so a comment that mentions a
    /// call does not count, and removing the call that shows the note fails here.
    /// </summary>
    [Fact]
    public void EverySurface_DeclaresItsMessage_AndItsLoaderTiesItToTheCollector()
    {
        foreach (var (collector, coverage) in Map)
        {
            foreach (var surface in coverage.Surfaces)
            {
                var xaml = ViewerFile(surface.XamlFile);
                var code = WithoutComments(ViewerFile(surface.LoaderFile));

                Assert.True(
                    xaml.Contains("x:Name=\"" + surface.Message + "\"", StringComparison.Ordinal),
                    $"{surface.XamlFile} does not declare x:Name=\"{surface.Message}\" for {collector}.");
                Assert.True(
                    LoaderTiesMessageToCollector(surface, collector, code),
                    $"{surface.LoaderFile} does not tie {surface.Message} to \"{collector}\" ({surface.Shape}), so it does not show the note for {collector}.");
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
    /// What a surface's message element shows: the sentence, visible, only where the collector cannot run (an Azure SQL
    /// Database) and the surface has no rows. With rows it is collapsed. On an on-premises server and on Managed Instance,
    /// which run the collector, and on a server whose engine has not been read yet (edition 0, no kind), it is collapsed and
    /// empty, so those surfaces change nothing.
    /// </summary>
    [Fact]
    public void EngineGapState_ShowsTheSentence_OnlyOnAnAzureSqlDatabaseWithNoRows()
    {
        var azure = CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
        var sentence = CollectorEngineCapability.NotCollectedMessage(ServerName, azure, MonitoredEngineKind.SqlServer, "running_jobs");
        Assert.NotNull(sentence);

        var noRows = ViewerServerTab.EngineGapState(ServerName, azure, MonitoredEngineKind.SqlServer, "running_jobs", 0);
        Assert.Equal(Visibility.Visible, noRows.Visibility);
        Assert.Equal(sentence, noRows.Text);

        var withRows = ViewerServerTab.EngineGapState(ServerName, azure, MonitoredEngineKind.SqlServer, "running_jobs", 3);
        Assert.Equal(Visibility.Collapsed, withRows.Visibility);

        foreach (var edition in new[] { OnPremEngineEdition, CollectorEngineCapability.AzureManagedInstanceEngineEdition })
        {
            var runs = ViewerServerTab.EngineGapState(ServerName, edition, MonitoredEngineKind.SqlServer, "running_jobs", 0);
            Assert.Equal(Visibility.Collapsed, runs.Visibility);
            Assert.Equal("", runs.Text);
        }

        var unread = ViewerServerTab.EngineGapState(ServerName, NoEngineEdition, null, "running_jobs", 0);
        Assert.Equal(Visibility.Collapsed, unread.Visibility);
        Assert.Equal("", unread.Text);
    }

    /// <summary>
    /// The database-state editor lists every server in the fleet, so a PostgreSQL target there shows the database_states
    /// collector's not-collected sentence (the collector reads sys.databases, which PostgreSQL does not have) in place of a
    /// count of zero databases. An Azure SQL Database shows it too. A server that runs the collector, and a server whose
    /// engine has not been read yet, show none.
    /// </summary>
    [Fact]
    public void DatabaseStateEditor_GapNote_SaysNotCollected_ForPostgresAndAzureSqlDatabase_AndNothingElse()
    {
        var postgres = DatabaseStateOverridesWindow.GapNoteFor(ServerName, NoEngineEdition, MonitoredEngineKind.Postgres);
        Assert.NotNull(postgres);
        Assert.Contains("database_states", postgres, StringComparison.Ordinal);

        var azure = DatabaseStateOverridesWindow.GapNoteFor(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition, MonitoredEngineKind.SqlServer);
        Assert.NotNull(azure);
        Assert.Contains("database_states", azure, StringComparison.Ordinal);

        Assert.Null(DatabaseStateOverridesWindow.GapNoteFor(ServerName, NoEngineEdition, null));
        Assert.Null(DatabaseStateOverridesWindow.GapNoteFor(ServerName, OnPremEngineEdition, MonitoredEngineKind.SqlServer));
    }

    /// <summary>
    /// The database-state editor's status line carries the not-collected sentence, a long one, in a 720 px window. A
    /// TextBlock does not wrap by default, so without TextWrapping="Wrap" the sentence runs off the right edge and is cut.
    /// </summary>
    [Fact]
    public void DatabaseStateEditor_StatusText_Wraps()
    {
        var xaml = ViewerFile("DatabaseStateOverridesWindow.xaml");
        var element = Regex.Match(xaml, @"<TextBlock\b[^>]*\bx:Name=""StatusText""[^>]*>", RegexOptions.CultureInvariant);

        Assert.True(element.Success, "DatabaseStateOverridesWindow.xaml has no TextBlock named StatusText.");
        Assert.Contains("TextWrapping=\"Wrap\"", element.Value, StringComparison.Ordinal);
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
