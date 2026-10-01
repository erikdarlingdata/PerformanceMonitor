/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows;
using Darling.Tests;
using Lite.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Every surface fed by a collector that never runs on an Azure SQL Database says so, instead of showing an empty grid
/// or chart. The Default Trace, CPU Scheduler and system_health surfaces already did. Running Jobs, Server
/// Configuration, Trace Flags, the two configuration-change grids, Memory Pressure Events and the Database State window
/// now do too. The collectors the fleet-wide tabs read have a reason here instead, because those tabs cannot show a
/// message for a server that has no rows.
///
/// <para>The coverage pin is driven by the collector catalog, so a collector that later stops running on Azure SQL Database
/// fails here until someone gives it a surface or a reason, and a stale entry fails too.</para>
/// </summary>
public sealed class AzureSqlDatabaseNotCollectedSurfaceTests
{
    private const string ServerName = "LiteAzureNotCollected";
    private const int AzureSqlDatabase = ServerHardwareScope.AzureSqlDatabaseEngineEdition;

    private const string ServerTabXaml = "Lite/Controls/ServerTab.xaml";
    private const string RefreshFile = "Lite/Controls/ServerTab.Refresh.cs";
    private const string ChartsFile = "Lite/Controls/ServerTab.Charts.cs";
    private const string ConfigChangesFile = "Lite/Controls/ServerTab.ConfigChanges.cs";
    private const string CpuSchedulerFile = "Lite/Controls/ServerTab.CpuScheduler.cs";
    private const string SystemEventsFile = "Lite/Controls/ServerTab.SystemEvents.cs";
    private const string StateWindowXaml = "Lite/Windows/DatabaseStateOverridesWindow.xaml";
    private const string StateWindowCode = "Lite/Windows/DatabaseStateOverridesWindow.xaml.cs";

    /// <summary>
    /// One message element: where its loader sets it, which XAML declares it, and its <c>x:Name</c>. A loader names the
    /// collector in the same statement as the element. The three surfaces that came first reach the collector through a
    /// one-line helper that other call sites share, so they name that helper in <paramref name="Via"/> instead.
    /// </summary>
    private sealed record Surface(string LoaderFile, string XamlFile, string Message, string? Via = null);

    /// <summary>What a collector that skips Azure SQL Database has: surfaces that say it is not collected, or a reason it has none.</summary>
    private sealed record Entry(IReadOnlyList<Surface> Surfaces, string? Reason)
    {
        public static Entry Shown(params Surface[] surfaces) => new(surfaces, null);

        public static Entry Because(string reason) => new(Array.Empty<Surface>(), reason);
    }

    private const string JobsReason =
        "Fleet-wide Job History tab; an Azure SQL Database server has no SQL Server Agent, so it has no rows and no filter entry.";

    private const string AvailabilityGroupsReason =
        "Fleet-wide Availability Groups tab, hidden until rows exist; its empty state already says these collectors do not run on Azure SQL Database.";

    private static readonly Dictionary<string, Entry> Map = new(StringComparer.Ordinal)
    {
        ["running_jobs"] = Entry.Shown(new Surface(RefreshFile, ServerTabXaml, "RunningJobsNoDataMessage")),
        ["server_config"] = Entry.Shown(
            new Surface(RefreshFile, ServerTabXaml, "ServerConfigNoDataMessage"),
            new Surface(ConfigChangesFile, ServerTabXaml, "ServerConfigChangesNoDataMessage")),
        ["trace_flags"] = Entry.Shown(
            new Surface(RefreshFile, ServerTabXaml, "TraceFlagsNoDataMessage"),
            new Surface(ConfigChangesFile, ServerTabXaml, "TraceFlagChangesNoDataMessage")),
        ["memory_pressure_events"] = Entry.Shown(new Surface(ChartsFile, ServerTabXaml, "MemoryPressureEventsNoDataMessage")),
        ["database_states"] = Entry.Shown(new Surface(StateWindowCode, StateWindowXaml, "StatusText")),
        ["cpu_scheduler_stats"] = Entry.Shown(new Surface(CpuSchedulerFile, ServerTabXaml, "CpuSchedulerNoDataMessage", Via: "CpuSchedulerGapNote")),
        ["system_health_events"] = Entry.Shown(new Surface(SystemEventsFile, ServerTabXaml, "SchedulerIssuesNoDataMessage", Via: "SystemHealthGapNote")),
        ["default_trace_events"] = Entry.Shown(new Surface(SystemEventsFile, ServerTabXaml, "DefaultTraceNoDataMessage", Via: "DefaultTraceGapNote")),
        ["job_history"] = Entry.Because(JobsReason),
        ["agent_status"] = Entry.Because(JobsReason),
        ["ag_replica_states"] = Entry.Because(AvailabilityGroupsReason),
        ["ag_database_replica_states"] = Entry.Because(AvailabilityGroupsReason),
    };

    /// <summary>The collectors the catalog says do not run on an Azure SQL Database, by name.</summary>
    private static string[] CollectorsThatSkipAzureSqlDatabase() =>
        CollectorCatalog.All
            .Where(d => !CollectorEngineCapability.IsCollectedOnEngineEdition(d, AzureSqlDatabase))
            .Select(d => d.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void TheMap_HasAnEntryForEveryCollectorTheCatalogSkipsOnAzureSqlDatabase_AndNoOther()
    {
        var skipped = CollectorsThatSkipAzureSqlDatabase();
        var mapped = Map.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray();

        var missing = skipped.Except(mapped, StringComparer.Ordinal).ToArray();
        var stale = mapped.Except(skipped, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0 && stale.Length == 0,
            $"The catalog skips these on Azure SQL Database but the map has no entry: [{string.Join(", ", missing)}]. " +
            $"The map has entries for collectors the catalog no longer skips there: [{string.Join(", ", stale)}].");
    }

    [Fact]
    public void EveryEntry_HasASurfaceOrAReason_NotBoth_AndNeverAnEmptyReason()
    {
        var problems = new List<string>();

        foreach (var (collector, entry) in Map)
        {
            var hasSurfaces = entry.Surfaces.Count > 0;
            var hasReason = !string.IsNullOrWhiteSpace(entry.Reason);

            if (hasSurfaces == hasReason)
                problems.Add($"{collector}: needs either surfaces or a reason, and has {(hasSurfaces ? "both" : "neither")}");
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
    }

    /// <summary>
    /// Each surface is declared in its XAML, and its loader puts the message element and the collector's name, as a
    /// string literal in code (a comment does not count), in ONE statement. Matching the two anywhere in the file would
    /// let two loaders swap their collectors (Server Configuration and Trace Flags share a file) and still pass.
    /// A loader that stopped asking, or a message that was deleted, fails here too.
    /// </summary>
    [Fact]
    public void EverySurface_IsDeclaredInItsXaml_AndItsLoaderPairsTheMessageWithTheCollectorInOneStatement()
    {
        var problems = new List<string>();

        foreach (var (collector, entry) in Map)
        {
            foreach (var surface in entry.Surfaces)
            {
                var xaml = ParitySource.ReadFile(surface.XamlFile);
                if (!xaml.Contains($"x:Name=\"{surface.Message}\"", StringComparison.Ordinal))
                    problems.Add($"{collector}: {surface.XamlFile} does not declare x:Name=\"{surface.Message}\"");

                var loader = ParitySource.ReadFile(surface.LoaderFile);
                var statements = CodeOfStatementsNaming(loader, collector).ToArray();
                var message = new Regex($@"\b{Regex.Escape(surface.Message)}\b");

                if (surface.Via is null)
                {
                    if (!statements.Any(statement => message.IsMatch(statement)))
                        problems.Add($"{collector}: no statement in {surface.LoaderFile} holds both {surface.Message} and the \"{collector}\" string literal");

                    continue;
                }

                var via = new Regex($@"\b{Regex.Escape(surface.Via)}\b");
                if (!statements.Any(statement => via.IsMatch(statement)))
                    problems.Add($"{collector}: no statement in {surface.LoaderFile} holds both {surface.Via} and the \"{collector}\" string literal");

                if (!message.IsMatch(CSharpSourceWalker.StripCommentsAndStrings(loader)))
                    problems.Add($"{collector}: {surface.LoaderFile} never uses {surface.Message}");
            }
        }

        Assert.True(problems.Count == 0, string.Join("; ", problems));
    }

    /// <summary>
    /// The code (comments and literal text blanked) of each statement that holds a <paramref name="collector"/> string
    /// literal. A statement runs from the nearest <c>;</c>, <c>{</c> or <c>}</c> before the literal to the next <c>;</c>,
    /// and a parenthesised group is never a boundary, so an <c>if (...)</c> header and its embedded statement are one.
    /// </summary>
    private static IEnumerable<string> CodeOfStatementsNaming(string source, string collector)
    {
        var isCode = CSharpSourceWalker.CodeMask(source);

        foreach (var literal in CSharpSourceWalker.StringLiteralBodies(source).Where(l => string.Equals(l.Text, collector, StringComparison.Ordinal)))
        {
            var start = literal.Start;
            var depth = 0;

            for (; start > 0; start--)
            {
                var c = source[start - 1];
                if (!isCode[start - 1])
                    continue;

                if (c == ')')
                    depth++;
                else if (c == '(')
                    depth--;
                else if (depth <= 0 && (c == ';' || c == '{' || c == '}'))
                    break;
            }

            var end = literal.Start;
            while (end < source.Length && !(isCode[end] && source[end] == ';'))
                end++;

            yield return new string(Enumerable.Range(start, end - start).Select(i => isCode[i] ? source[i] : ' ').ToArray());
        }
    }

    /// <summary>
    /// The gap helper gives the sentence the MCP tools return as not_collected for a collector that does not run on an
    /// Azure SQL Database, and nothing for an on-premises server, so every other server's surface is unchanged.
    /// </summary>
    [Fact]
    public void TheGapHelper_GivesTheNotCollectedSentenceOnAzureSqlDatabase_AndNothingOnPremises()
    {
        var note = ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "running_jobs");

        Assert.NotNull(note);
        Assert.Equal(CollectorEngineCapability.NotCollectedMessage(ServerName, AzureSqlDatabase, engineKind: null, "running_jobs"), note);
        Assert.Contains(ServerName, note, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", note, StringComparison.Ordinal);
        Assert.Contains("running_jobs", note, StringComparison.Ordinal);

        Assert.Null(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: false, "running_jobs"));
    }

    /// <summary>Every collector on the map answers the same way, so a surface cannot say "not collected" on a server that does collect it.</summary>
    [Fact]
    public void TheGapHelper_AgreesWithTheCatalog_ForEveryMappedCollector()
    {
        foreach (var collector in Map.Keys)
        {
            Assert.NotNull(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, collector));
            Assert.Null(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: false, collector));
        }
    }

    /// <summary>
    /// The Database State window knows the stored engine edition, and gives the gap helper the one fact it uses, whether
    /// the server is an Azure SQL Database. On every edition its note follows the collector's own AppliesTo rule.
    /// </summary>
    [Fact]
    public void TheDatabaseStateWindow_SaysNotCollectedOnAzureSqlDatabase_AndMakesNoClaimOnAnyEditionTheCollectorRuns()
    {
        foreach (var edition in new[] { 0, 1, 2, 3, 4, 5, 6, 8, 9, 11 })
        {
            var notCollected = !CollectorEngineCapability.IsCollectedOnEngineEdition("database_states", edition);
            var note = ServerTab.EngineGapNote(ServerName, edition == AzureSqlDatabase, "database_states");

            Assert.True(notCollected == (note is not null),
                $"database_states on edition {edition}: AppliesTo says {(notCollected ? "not collected" : "collected")}, the note disagrees");
        }

        var azure = ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "database_states");
        Assert.NotNull(azure);
        Assert.Contains("Azure SQL Database", azure, StringComparison.Ordinal);
        Assert.Contains("database_states", azure, StringComparison.Ordinal);
    }

    /// <summary>The collector stores "N/A" where the engine has no memory model. The tab shows it as n/a and leaves every real model alone.</summary>
    [Fact]
    public void TheMemoryModel_ReadsNotApplicable_WhereTheCollectorStoredNA_AndTheStoredModelElsewhere()
    {
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.MemoryModelText("N/A"));
        Assert.Equal(ServerHardwareScope.NotApplicable, ServerTab.MemoryModelText("n/a"));

        Assert.Equal("CONVENTIONAL", ServerTab.MemoryModelText("CONVENTIONAL"));
        Assert.Equal("LOCK_PAGES", ServerTab.MemoryModelText("LOCK_PAGES"));
        Assert.Equal("", ServerTab.MemoryModelText(""));
        Assert.Null(ServerTab.MemoryModelText(null));
    }

    /// <summary>
    /// The "grant the login access to msdb" warning shows only where the running_jobs collector can run. On an Azure
    /// SQL Database no grant could help, so the not-collected note stands alone there.
    /// </summary>
    [Fact]
    public void TheMsdbWarning_IsHiddenOnAzureSqlDatabase_AndShownOnPremisesWithoutMsdbAccess()
    {
        Assert.Equal(Visibility.Collapsed, ServerTab.RunningJobsMsdbWarningVisibility(hasMsdbAccess: false, isAzureSqlDatabase: true));
        Assert.Equal(Visibility.Visible, ServerTab.RunningJobsMsdbWarningVisibility(hasMsdbAccess: false, isAzureSqlDatabase: false));

        Assert.Equal(Visibility.Collapsed, ServerTab.RunningJobsMsdbWarningVisibility(hasMsdbAccess: true, isAzureSqlDatabase: true));
        Assert.Equal(Visibility.Collapsed, ServerTab.RunningJobsMsdbWarningVisibility(hasMsdbAccess: true, isAzureSqlDatabase: false));
    }

    [Fact]
    public void TheRunningJobsLoader_ShowsTheGapNote()
    {
        var body = MethodBody(RefreshFile, "Task RefreshRunningJobsAsync(");

        Assert.Contains("ShowEngineGap(RunningJobsNoDataMessage,", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMsdbWarning_ComesFromTheHelper_WhenTheTabOpens_AndAgainOnceTheEditionIsKnown()
    {
        const string call = "RunningJobsMsdbWarning.Visibility = RunningJobsMsdbWarningVisibility(_hasMsdbAccess, _isAzureSqlDatabase);";

        Assert.Contains(call, CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile("Lite/Controls/ServerTab.xaml.cs")), StringComparison.Ordinal);
        Assert.Contains(call, MethodBody(RefreshFile, "Task RefreshEngineEditionAsync("), StringComparison.Ordinal);
    }

    /// <summary>
    /// The two sub-tab Refresh buttons run their loader without going through RefreshVisibleTabAsync, which is what
    /// learns the engine edition. Each learns it first, or a manual refresh could show the old empty-state text.
    /// </summary>
    [Theory]
    [InlineData(ConfigChangesFile, "void ConfigChangesRefresh_Click(", "RefreshConfigChangesAsync(")]
    [InlineData(SystemEventsFile, "void SystemEventsRefresh_Click(", "RefreshSystemEventsAsync(")]
    public void TheSubTabRefreshButtons_LearnTheEngineEdition_BeforeTheirLoaderRuns(string file, string handler, string loader)
    {
        var body = MethodBody(file, handler);
        var edition = body.IndexOf("RefreshEngineEditionAsync(", StringComparison.Ordinal);
        var load = body.IndexOf(loader, StringComparison.Ordinal);

        Assert.True(edition >= 0 && load >= 0 && edition < load, $"{handler} must call RefreshEngineEditionAsync before {loader}");
    }

    [Fact]
    public void TheMemoryOverview_ShowsTheModelThroughTheHelper()
    {
        var body = MethodBody(ChartsFile, "void UpdateMemorySummary(");

        Assert.Contains("SqlMemoryModelText.Text = MemoryModelText(stats.SqlMemoryModel);", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLogicalCpusHeader_ExplainsWhatTheCountMeansOnAzureSqlDatabase()
    {
        var xaml = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml");
        var header = xaml.IndexOf("Text=\"Logical CPUs\"", StringComparison.Ordinal);

        Assert.True(header >= 0, "the Logical CPUs header is gone");
        Assert.Contains(
            "ToolTip=\"The CPU count the engine reports. On Azure SQL Database this is the scheduler count, so it can differ from the vCores you pay for.\"",
            xaml.Substring(header, Math.Min(400, xaml.Length - header)),
            StringComparison.Ordinal);
    }

    /// <summary>The body of the method whose declaration contains <paramref name="anchor"/>, with comments and string literals blanked.</summary>
    private static string MethodBody(string file, string anchor)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(file));
        var at = code.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{anchor} not found in {file}");

        return CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));
    }
}
