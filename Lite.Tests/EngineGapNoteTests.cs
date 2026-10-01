/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using Darling.Tests;
using DuckDB.NET.Data;
using Lite.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The rule that decides when an empty surface says its collector does not collect for this server. The note shows with
/// no rows when the engine rules the collector out (an Azure SQL Database), or when the store has no run of the collector
/// for the server in all retained history. The Running Jobs tab uses the shared skipped-collector wording the MCP tool
/// uses, so the tab and the tool say the same thing.
/// </summary>
public sealed class EngineGapNoteTests
{
    private const string ServerName = "LiteNeverRan";

    [Fact]
    public void AzureWithNoRows_ShowsTheEditionSentence()
    {
        var (text, visibility) = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: true, collectorNeverRan: false, "running_jobs", rowCount: 0);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "running_jobs"), text);
        Assert.Contains("Azure SQL Database", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureWithRows_IsHidden()
    {
        var (_, visibility) = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: true, collectorNeverRan: false, "running_jobs", rowCount: 3);

        Assert.Equal(Visibility.Collapsed, visibility);
    }

    [Fact]
    public void OnPremisesThatRanWithNoRows_IsHidden_SoTheNormalEmptyStateStands()
    {
        var (_, visibility) = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: false, "server_config", rowCount: 0);

        Assert.Equal(Visibility.Collapsed, visibility);
    }

    [Fact]
    public void OnPremisesThatNeverRan_ShowsTheGenericSentence_WithNoRows()
    {
        var (text, visibility) = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: true, "server_config", rowCount: 0);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(ServerTab.NeverCollectedNote(ServerName, "server_config"), text);
        Assert.Contains(ServerName, text, StringComparison.Ordinal);
        Assert.Contains("server_config", text, StringComparison.Ordinal);

        Assert.Equal(Visibility.Collapsed, ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: true, "server_config", rowCount: 2).Visibility);
    }

    [Fact]
    public void ANeverRanNoteReplacesTheGenericSentence_ButTheEditionSentenceStillComesFirst()
    {
        var onPremises = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: true, "running_jobs", rowCount: 0, neverRanNote: "custom");
        var azure = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: true, collectorNeverRan: true, "running_jobs", rowCount: 0, neverRanNote: "custom");

        Assert.Equal("custom", onPremises.Text);
        Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "running_jobs"), azure.Text);
    }

    [Fact]
    public void AnUnknownEditionWithNoCollectionLogRowAtAll_IsHidden()
    {
        var neverRan = CollectorRunHistory.Empty.NeverRan("running_jobs");
        var (_, visibility) = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, neverRan, "running_jobs", rowCount: 0);

        Assert.False(neverRan);
        Assert.Equal(Visibility.Collapsed, visibility);
    }

    /* A history with no first or last collection makes no first-run grace claim, so these read as they always did. */
    private static CollectorRunHistory HistoryThatSawOnly(params string[] loggedCollectors) =>
        new(new HashSet<string>(loggedCollectors, StringComparer.Ordinal), new HashSet<string>(StringComparer.Ordinal), ServerHasAnyLogRow: true,
            ServerLastCollectedUtc: null, ServerFirstCollectedUtc: null);

    private static CollectorRunHistory HistoryCollectingFrom(DateTime firstCollected, DateTime lastCollected, params string[] loggedCollectors) =>
        new(new HashSet<string>(loggedCollectors, StringComparer.Ordinal), new HashSet<string>(StringComparer.Ordinal), ServerHasAnyLogRow: true,
            ServerLastCollectedUtc: lastCollected, ServerFirstCollectedUtc: firstCollected);

    private static readonly string[] SurfaceCollectors = ["server_config", "trace_flags", "memory_pressure_events", "database_states"];

    /* When the grace pins' server first collected. Fixed and far from the wall clock. */
    private static readonly DateTime FirstCollected = new(2026, 1, 5, 9, 0, 0, DateTimeKind.Utc);

    /* The tab loads running_jobs on tab 10, which does not refresh the collector history. A history read on the Memory tab
       before running_jobs first ran still says it never ran, after it has run and found no jobs. */
    [Fact]
    public void RunningJobsWithNoSkippedNote_IsNotNeverRan_EvenWhenTheCollectorHistoryIsStale()
    {
        var staleHistory = HistoryThatSawOnly("wait_stats");
        Assert.True(staleHistory.NeverRan("running_jobs"));

        var note = ServerTab.NoRunNote(ServerName, "running_jobs", skipped: null, staleHistory);
        var (_, visibility) = ServerTab.EngineGapStateFromRuns(ServerName, isAzureSqlDatabase: false, "running_jobs", rowCount: 0, skipped: null, staleHistory);

        Assert.Null(note);
        Assert.Equal(Visibility.Collapsed, visibility);
    }

    [Fact]
    public void RunningJobsWithASkippedNote_IsNeverRan_SoTheNoteShows()
    {
        var history = HistoryThatSawOnly("wait_stats", "running_jobs");

        Assert.Equal("the skipped-collector note", ServerTab.NoRunNote(ServerName, "running_jobs", skipped: "the skipped-collector note", history));
    }

    [Fact]
    public void AnyOtherCollector_TakesNeverRanFromTheCollectorHistory()
    {
        var history = HistoryThatSawOnly("wait_stats");

        Assert.Equal(ServerTab.NeverCollectedNote(ServerName, "server_config"), ServerTab.NoRunNote(ServerName, "server_config", skipped: null, history));
        Assert.Null(ServerTab.NoRunNote(ServerName, "wait_stats", skipped: null, history));
    }

    /* The first-run grace on every surface that reads the history, by the shared rule. One second before a collector is due it
       says it has not run yet, and nothing says it never ran. Once it is due and still has no run, the never-ran note comes
       back. A collector that ran, or a surface with rows, shows nothing inside the grace either. */
    [Fact]
    public void EverySurfaceCollector_SaysNotRunYetInsideItsGrace_AndNeverRanAgainOnceItIsDue()
    {
        foreach (var collector in SurfaceCollectors)
        {
            var due = FirstCollected.AddMinutes(
                CollectorScheduleDefaults.All[collector].FrequencyMinutes + CollectorRuntimePrecondition.FirstRunSlackMinutes);

            var inside = HistoryCollectingFrom(FirstCollected, due.AddSeconds(-1), "wait_stats");
            var (notYet, notYetVisibility) = ServerTab.EngineGapStateFromRuns(ServerName, isAzureSqlDatabase: false, collector, rowCount: 0, skipped: null, inside);
            Assert.Equal(Visibility.Visible, notYetVisibility);
            Assert.Equal(CollectorRuntimePrecondition.NotYetRunMessage(ServerName, collector, due.AddSeconds(-1), FirstCollected), notYet);
            Assert.Contains("has not run against", notYet, StringComparison.Ordinal);
            Assert.False(inside.NeverRan(collector));
            Assert.Equal(Visibility.Collapsed, ServerTab.EngineGapStateFromRuns(ServerName, isAzureSqlDatabase: false, collector, rowCount: 3, skipped: null, inside).Visibility);

            var over = HistoryCollectingFrom(FirstCollected, due, "wait_stats");
            var (overdue, overdueVisibility) = ServerTab.EngineGapStateFromRuns(ServerName, isAzureSqlDatabase: false, collector, rowCount: 0, skipped: null, over);
            Assert.Equal(Visibility.Visible, overdueVisibility);
            Assert.Equal(ServerTab.NeverCollectedNote(ServerName, collector), overdue);
            Assert.True(over.NeverRan(collector));
        }

        var ran = HistoryCollectingFrom(FirstCollected, FirstCollected.AddMinutes(1), "wait_stats", "server_config");
        Assert.Null(ServerTab.NoRunNote(ServerName, "server_config", skipped: null, ran));
    }

    /* The database state editor shows its status line through the tabs' rule, so it says not yet inside the grace too. */
    [Fact]
    public void TheDatabaseStateEditor_TakesItsNoteFromTheTabsRule()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile("Lite/Windows/DatabaseStateOverridesWindow.xaml.cs"));

        Assert.Matches(@"StatusText\.Text\s*=\s*ServerTab\.EngineGapStateFromRuns\(", code);
        Assert.DoesNotContain(".NeverRan(", code, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AfterOneReadThatSawEverySurfaceCollector_TheNextRefreshIssuesNoRead()
    {
        var reads = 0;
        Task<CollectorRunHistory> Read()
        {
            reads++;
            return Task.FromResult(HistoryThatSawOnly(SurfaceCollectors));
        }

        var first = await CollectorRunHistory.ReadAsync(CollectorRunHistory.Empty, Read);
        var second = await CollectorRunHistory.ReadAsync(first, Read);

        Assert.Equal(1, reads);
        Assert.Same(first, second);
        Assert.False(second.NeverRan("trace_flags"));
    }

    [Fact]
    public async Task WhileASurfaceCollectorIsUnseen_EveryRefreshReads()
    {
        var reads = 0;
        Task<CollectorRunHistory> Read()
        {
            reads++;
            return Task.FromResult(HistoryThatSawOnly("wait_stats", "server_config"));
        }

        var known = await CollectorRunHistory.ReadAsync(CollectorRunHistory.Empty, Read);
        await CollectorRunHistory.ReadAsync(known, Read);

        Assert.Equal(2, reads);
    }

    [Fact]
    public async Task ACollectorSeenOnce_StaysRan_AfterALaterReadThatDoesNotListIt()
    {
        var first = await CollectorRunHistory.ReadAsync(CollectorRunHistory.Empty, () => Task.FromResult(HistoryThatSawOnly("wait_stats", "trace_flags")));
        var later = await CollectorRunHistory.ReadAsync(first, () => Task.FromResult(HistoryThatSawOnly("wait_stats")));

        Assert.False(later.NeverRan("trace_flags"));
        Assert.True(later.NeverRan("server_config"));
    }

    /* The tab keeps what it has seen across refreshes, so the merge keeps the server's earliest first collection and its latest
       last collection. A later read that no longer holds the oldest row does not restart the grace, and the grace ends on the
       refresh after the server has collected past it. */
    [Fact]
    public async Task TheMergedHistory_KeepsTheEarliestFirstAndTheLatestLastCollection()
    {
        var fresh = await CollectorRunHistory.ReadAsync(
            CollectorRunHistory.Empty, () => Task.FromResult(HistoryCollectingFrom(FirstCollected, FirstCollected.AddMinutes(2), "wait_stats")));

        Assert.Equal(FirstCollected, fresh.ServerFirstCollectedUtc);
        Assert.Equal(FirstCollected.AddMinutes(2), fresh.ServerLastCollectedUtc);
        Assert.NotNull(fresh.NotYetRunNote(ServerName, "server_config"));

        var later = await CollectorRunHistory.ReadAsync(
            fresh, () => Task.FromResult(HistoryCollectingFrom(FirstCollected.AddMinutes(1), FirstCollected.AddMinutes(30), "wait_stats")));

        Assert.Equal(FirstCollected, later.ServerFirstCollectedUtc);
        Assert.Equal(FirstCollected.AddMinutes(30), later.ServerLastCollectedUtc);
        Assert.Null(later.NotYetRunNote(ServerName, "server_config"));
        Assert.True(later.NeverRan("server_config"));
    }

    [Fact]
    public async Task AFailedRead_MakesNoClaim_AndTheNextRefreshReadsAgain()
    {
        var seen = HistoryThatSawOnly("wait_stats", "trace_flags");

        var afterFailure = await CollectorRunHistory.ReadAsync(seen, () => throw new InvalidOperationException("the store is busy"));

        Assert.Same(CollectorRunHistory.Empty, afterFailure);
        Assert.False(afterFailure.NeverRan("server_config"));
    }

    /* On an Azure SQL Database the engine note answers every surface that reads the history, so a refresh reads nothing. */
    [Fact]
    public async Task OnAnAzureSqlDatabase_TheRefreshIssuesNoRead_AndEverySurfaceStillShowsTheEditionSentence()
    {
        var reads = 0;
        Task<CollectorRunHistory> Read()
        {
            reads++;
            return Task.FromResult(HistoryThatSawOnly("wait_stats"));
        }

        var azure = await CollectorRunHistory.ReadAsync(CollectorRunHistory.Empty, Read, isAzureSqlDatabase: true);
        Assert.Equal(0, reads);
        Assert.Same(CollectorRunHistory.Empty, azure);

        foreach (var collector in SurfaceCollectors)
        {
            var (text, visibility) = ServerTab.EngineGapStateFromRuns(
                ServerName, isAzureSqlDatabase: true, collector, rowCount: 0, skipped: null, azure);
            Assert.Equal(Visibility.Visible, visibility);
            Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, collector), text);
        }

        await CollectorRunHistory.ReadAsync(CollectorRunHistory.Empty, Read, isAzureSqlDatabase: false);
        Assert.Equal(1, reads);
    }

    [Fact]
    public void TheTabRefresh_PassesTheEditionToTheHistoryRead()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile("Lite/Controls/ServerTab.EngineGaps.cs"));
        var at = code.IndexOf("Task RefreshCollectorRunsAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0, "RefreshCollectorRunsAsync not found");

        var statement = code[at..code.IndexOf(';', at)];

        /* The edition itself, as the last argument. A negation would skip the read on every server but an Azure SQL Database. */
        Assert.Matches(@",\s*_isAzureSqlDatabase\s*\)\s*$", statement);
        Assert.DoesNotMatch(@"!\s*_isAzureSqlDatabase", statement);
    }

    /* The Running Jobs loader passes the edition to the read, so an Azure SQL Database makes no read and every other server does. */
    [Fact]
    public void TheRunningJobsLoader_PassesTheEditionToItsRead()
    {
        var loader = MethodBody("Lite/Controls/ServerTab.EngineGaps.cs", "Task RefreshRunningJobsSkippedNoteAsync(");

        Assert.Matches(@"ReadRunningJobsSkippedNoteAsync\(\s*_server\.DisplayName\s*,\s*_isAzureSqlDatabase\s*,", loader);
        Assert.DoesNotMatch(@"!\s*_isAzureSqlDatabase", loader);
    }

    /* On an Azure SQL Database the engine note answers the Running Jobs tab first, so the loader makes no read there. Every other
       server reads, and the read carries the server's first collection to the note, so the tab gets the tool's first-run grace. */
    [Fact]
    public async Task TheRunningJobsRead_IsSkippedOnAnAzureSqlDatabase_AndCarriesTheFirstCollectionEverywhereElse()
    {
        var reads = 0;
        Task<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)> Read()
        {
            reads++;
            return Task.FromResult<(DateTime?, DateTime?, DateTime?)>((null, FirstCollected.AddMinutes(2), FirstCollected));
        }

        Assert.Null(await ServerTab.ReadRunningJobsSkippedNoteAsync(ServerName, isAzureSqlDatabase: true, Read));
        Assert.Equal(0, reads);

        var note = await ServerTab.ReadRunningJobsSkippedNoteAsync(ServerName, isAzureSqlDatabase: false, Read);
        Assert.Equal(1, reads);
        Assert.Equal(ServerTab.RunningJobsSkippedNote(ServerName, null, FirstCollected.AddMinutes(2), FirstCollected), note);
        Assert.Contains("has not run against", note, StringComparison.Ordinal);

        Assert.Null(await ServerTab.ReadRunningJobsSkippedNoteAsync(
            ServerName, isAzureSqlDatabase: false, () => throw new InvalidOperationException("the store is busy")));
    }

    /* The loader reads the skipped-collector note before it shows the gap: the note is the only never-ran input for running_jobs. */
    [Fact]
    public void TheRunningJobsLoader_ReadsTheSkippedNoteBeforeItShowsTheGap()
    {
        var loader = MethodBody("Lite/Controls/ServerTab.Refresh.cs", "Task RefreshRunningJobsAsync(");
        var note = loader.IndexOf("RefreshRunningJobsSkippedNoteAsync(", StringComparison.Ordinal);
        var show = loader.IndexOf("ShowEngineGap(RunningJobsNoDataMessage,", StringComparison.Ordinal);
        Assert.True(note >= 0 && show > note, "the Running Jobs loader must read the skipped-collector note before it shows the gap");
    }

    [Fact]
    public void TheCollectorHistory_IsReadOncePerTabRefresh_AndByTheConfigurationChangesRefreshButton()
    {
        var refresh = MethodBody("Lite/Controls/ServerTab.Refresh.cs", "Task RefreshVisibleTabAsync(");
        var edition = refresh.IndexOf("RefreshEngineEditionAsync(", StringComparison.Ordinal);
        var history = refresh.IndexOf("RefreshCollectorRunsAsync(", StringComparison.Ordinal);
        Assert.True(edition >= 0 && history > edition, "RefreshVisibleTabAsync must read the collector history after the edition");

        Assert.Contains("RefreshCollectorRunsAsync(", MethodBody("Lite/Controls/ServerTab.ConfigChanges.cs", "void ConfigChangesRefresh_Click("), StringComparison.Ordinal);
    }

    private static string MethodBody(string file, string anchor)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ParitySource.ReadFile(file));
        var at = code.IndexOf(anchor, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{anchor} not found in {file}");

        return CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', at));
    }
}

/// <summary>
/// The store read behind the never-ran rule, against a real DuckDB. The read has no time window: the server_config and
/// trace_flags collectors run once at load, so a window would put a false note on every server once that row aged out.
/// </summary>
public sealed class CollectorRunHistoryReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "LiteNeverRanStore";
    private const int ServerId = 4903;
    private const int OtherServerId = 4904;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectorRunHistoryReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private Task LogAsync(int serverId, string collector, TimeSpan ago) =>
        LogAtAsync(serverId, collector, Truncate(DateTime.UtcNow - ago));

    private async Task LogAtAsync(int serverId, string collector, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Inserts one row into <paramref name="table"/> for the server, giving every required column a value of its type.</summary>
    private async Task InsertDataRowAsync(string table, int serverId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        var columns = new List<string>();
        var values = new List<string>();
        using (var info = connection.CreateCommand())
        {
            info.CommandText = $"SELECT name, type, \"notnull\" FROM pragma_table_info('{table}')";
            using var reader = await info.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                var type = reader.GetString(1).ToUpperInvariant();
                if (!name.Equals("server_id", StringComparison.OrdinalIgnoreCase) && !Convert.ToBoolean(reader.GetValue(2)))
                    continue;

                columns.Add(name);
                values.Add(name.Equals("server_id", StringComparison.OrdinalIgnoreCase) ? serverId.ToString()
                    : type.StartsWith("TIMESTAMP", StringComparison.Ordinal) ? "now()::TIMESTAMP"
                    : type.Contains("CHAR", StringComparison.Ordinal) || type == "TEXT" ? "''"
                    : type == "BOOLEAN" ? "false"
                    : "0");
            }
        }

        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {table} ({string.Join(", ", columns)}) VALUES ({string.Join(", ", values)})";
        await insert.ExecuteNonQueryAsync();
    }

    [Theory]
    [InlineData(6)]
    [InlineData(120)]
    public async Task AServerConfigThatRanOnceHoursOrDaysBeforeTheServersLastCollection_ReadsAsRan_AndShowsNoNote(int hoursAgo)
    {
        await LogAsync(ServerId, "server_config", TimeSpan.FromHours(hoursAgo));
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromMinutes(1));

        var history = await new LocalDataService(_duckDb).GetCollectorRunHistoryAsync(ServerId);

        Assert.False(history.NeverRan("server_config"));
        Assert.Equal(Visibility.Collapsed, ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, history.NeverRan("server_config"), "server_config", rowCount: 0).Visibility);
    }

    /* When running_jobs is due on a server: its first collection, plus the collector's default interval, plus the slack. */
    private static int RunningJobsDueMinutes =>
        CollectorScheduleDefaults.All["running_jobs"].FrequencyMinutes + CollectorRuntimePrecondition.FirstRunSlackMinutes;

    /// <summary>
    /// The tab's running_jobs note, read from the store the way the Running Jobs loader reads it, and the get_running_jobs tool's
    /// message for the same server. The tool's side is the static builder McpJobTools calls, with the cause it passes.
    /// </summary>
    private async Task<(string? TabNote, string? ToolStatus)> RunningJobsNotesAsync(int serverId)
    {
        var dataService = new LocalDataService(_duckDb);
        var (lastRun, serverLastCollected, serverFirstCollected) = await dataService.GetCollectorLastRunAsync(serverId, "running_jobs");
        var tabNote = ServerTab.RunningJobsSkippedNote(ServerName, lastRun, serverLastCollected, serverFirstCollected);
        var toolStatus = await McpRuntimePrecondition.GatedOffStatusAsync(
            dataService, serverId, ServerName, "running_jobs", CollectorRuntimePrecondition.RunningJobsPossibleCauses);

        return (tabNote, toolStatus);
    }

    private static void AssertTheToolSays(string? toolStatus, string? tabNote)
    {
        Assert.NotNull(toolStatus);
        Assert.NotNull(tabNote);
        using var tool = JsonDocument.Parse(toolStatus!);
        Assert.Contains(tool.RootElement.EnumerateObject(), p => p.Value.ValueKind == JsonValueKind.String && p.Value.GetString() == tabNote);
    }

    [Fact]
    public async Task TheRunningJobsToolAndTheTab_SayTheSameThing_ForAServerWhoseRunningJobsNeverRan()
    {
        /* Collecting for two hours, so running_jobs is long past due. */
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromHours(2));
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromMinutes(1));

        var (tabNote, toolStatus) = await RunningJobsNotesAsync(ServerId);

        AssertTheToolSays(toolStatus, tabNote);
        Assert.Contains("has never run against", tabNote, StringComparison.Ordinal);
        Assert.Contains(CollectorRuntimePrecondition.RunningJobsPossibleCauses, tabNote, StringComparison.Ordinal);

        /* A server that has collected nothing gets no note from either. */
        var (nothingTab, nothingTool) = await RunningJobsNotesAsync(OtherServerId);
        Assert.Null(nothingTool);
        Assert.Null(nothingTab);
    }

    /* Both sides of the due time, through the store read: one second before, the tab says not yet and names no cause. At the
       due time it gives the hedged note with the possible cause. The tool says the same thing at each. */
    [Fact]
    public async Task TheTab_JustBeforeRunningJobsIsDue_SaysNotYet_AndOnceDue_NamesThePossibleCause_AsTheToolDoes()
    {
        var first = Truncate(DateTime.UtcNow.AddHours(-2));
        await LogAtAsync(ServerId, "wait_stats", first);
        await LogAtAsync(ServerId, "wait_stats", first.AddMinutes(RunningJobsDueMinutes).AddSeconds(-1));

        var (notYet, notYetTool) = await RunningJobsNotesAsync(ServerId);

        AssertTheToolSays(notYetTool, notYet);
        Assert.Contains("has not run against", notYet, StringComparison.Ordinal);
        Assert.DoesNotContain("Possible cause", notYet, StringComparison.Ordinal);
        Assert.DoesNotContain("switched off", notYet, StringComparison.Ordinal);

        await LogAtAsync(ServerId, "wait_stats", first.AddMinutes(RunningJobsDueMinutes));

        var (due, dueTool) = await RunningJobsNotesAsync(ServerId);

        AssertTheToolSays(dueTool, due);
        Assert.Contains("has never run against", due, StringComparison.Ordinal);
        Assert.Contains("usually means the collector is switched off", due, StringComparison.Ordinal);
        Assert.Contains(CollectorRuntimePrecondition.RunningJobsPossibleCauses, due, StringComparison.Ordinal);
    }

    /* The grace runs on the server's last collection, not the clock: a server that collected for five minutes three days ago
       and then stopped is still inside it. */
    [Fact]
    public async Task TheTab_ForAServerWhoseLastCollectionIsStale_StaysInsideTheGrace_AsTheToolDoes()
    {
        var first = Truncate(DateTime.UtcNow.AddDays(-3));
        await LogAtAsync(ServerId, "wait_stats", first);
        await LogAtAsync(ServerId, "wait_stats", first.AddMinutes(5));

        var (tabNote, toolStatus) = await RunningJobsNotesAsync(ServerId);

        AssertTheToolSays(toolStatus, tabNote);
        Assert.Contains("has not run against", tabNote, StringComparison.Ordinal);
        Assert.DoesNotContain("AWS RDS", tabNote, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWithOnlyOtherCollectorsRows_ReadsRunningJobsAsNeverRan()
    {
        /* Collecting for two hours, so every first-run grace is over. */
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromHours(2));
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromMinutes(1));
        await LogAsync(OtherServerId, "running_jobs", TimeSpan.FromMinutes(1));

        var history = await new LocalDataService(_duckDb).GetCollectorRunHistoryAsync(ServerId);

        Assert.True(history.NeverRan("running_jobs"));
        Assert.False(history.NeverRan("wait_stats"));
    }

    [Fact]
    public async Task AServerWithNoRowsAtAll_ReadsNothingAsNeverRan()
    {
        await LogAsync(OtherServerId, "wait_stats", TimeSpan.FromMinutes(1));

        var history = await new LocalDataService(_duckDb).GetCollectorRunHistoryAsync(ServerId);

        Assert.False(history.ServerHasAnyLogRow);
        Assert.Empty(history.LoggedCollectors);
        Assert.False(history.NeverRan("running_jobs"));
        Assert.False(history.NeverRan("server_config"));
    }

    [Fact]
    public async Task AnyDataRowForTheServer_ProvesItsCollectorRan_EvenWithNoLogRow()
    {
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromHours(2));
        await LogAsync(ServerId, "wait_stats", TimeSpan.FromMinutes(1));
        await InsertDataRowAsync("database_states", ServerId);

        var history = await new LocalDataService(_duckDb).GetCollectorRunHistoryAsync(ServerId);

        Assert.DoesNotContain("database_states", history.LoggedCollectors);
        Assert.Contains("database_states", history.CollectorsWithData);
        Assert.False(history.NeverRan("database_states"));
        Assert.True(history.NeverRan("trace_flags"));
    }

    /* A new server through the real read. The read returns the server's first and last collection, so every surface collector
       with no run says it has not run yet. Once the server has collected past every grace, the never-ran note is back. */
    [Fact]
    public async Task ANewServer_ReadsItsFirstAndLastCollection_AndItsSurfacesSayNotRunYet_UntilTheGraceIsOver()
    {
        var first = Truncate(DateTime.UtcNow.AddHours(-1));
        await LogAtAsync(ServerId, "wait_stats", first);
        await LogAtAsync(ServerId, "cpu_utilization", first.AddMinutes(2));
        await LogAtAsync(OtherServerId, "wait_stats", first.AddMinutes(-30));

        var dataService = new LocalDataService(_duckDb);
        var fresh = await dataService.GetCollectorRunHistoryAsync(ServerId);

        Assert.Equal(first, fresh.ServerFirstCollectedUtc);
        Assert.Equal(first.AddMinutes(2), fresh.ServerLastCollectedUtc);
        Assert.Equal(DateTimeKind.Utc, fresh.ServerFirstCollectedUtc!.Value.Kind);

        foreach (var collector in new[] { "server_config", "trace_flags", "memory_pressure_events", "database_states" })
        {
            Assert.Equal(
                CollectorRuntimePrecondition.NotYetRunMessage(ServerName, collector, first.AddMinutes(2), first),
                ServerTab.NoRunNote(ServerName, collector, skipped: null, fresh));
            Assert.False(fresh.NeverRan(collector));
        }

        await LogAtAsync(ServerId, "wait_stats", first.AddMinutes(30));
        var later = await dataService.GetCollectorRunHistoryAsync(ServerId);

        foreach (var collector in new[] { "server_config", "trace_flags", "memory_pressure_events", "database_states" })
        {
            Assert.Equal(ServerTab.NeverCollectedNote(ServerName, collector), ServerTab.NoRunNote(ServerName, collector, skipped: null, later));
        }
    }
}

/// <summary>
/// The never-ran read sees archived log rows. Archival moves a collection_log row older than seven days out of the hot table
/// into a Parquet file, and a collector that runs only at load, such as trace_flags, logs once. A read of the hot table alone
/// calls that collector never run once its one row is archived.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class CollectorRunHistoryArchiveTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public CollectorRunHistoryArchiveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ALoadTimeCollectorsLogRow_ThatArchivalMovedOutOfTheHotTable_StillReadsAsRan()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var tenDaysAgo = DateTime.UtcNow.AddDays(-10).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var oneMinuteAgo = DateTime.UtcNow.AddMinutes(-1).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        await ExecAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES (1, 1, 'S1', 'trace_flags', TIMESTAMP '{tenDaysAgo}', 'SUCCESS'),
       (2, 1, 'S1', 'wait_stats', TIMESTAMP '{oneMinuteAgo}', 'SUCCESS')");

        await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        /* The archive pass took the old row out of the hot table and left the recent one. */
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log WHERE collector_name = 'trace_flags'"));
        Assert.Equal(1, await ScalarAsync("SELECT COUNT(*) FROM collection_log WHERE collector_name = 'wait_stats'"));

        var history = await new LocalDataService(initializer).GetCollectorRunHistoryAsync(1);

        Assert.False(history.NeverRan("trace_flags"));
        Assert.True(history.NeverRan("running_jobs"));

        /* The server's first collection is the archived row, so its first-run grace ended long ago. */
        Assert.Equal(tenDaysAgo, history.ServerFirstCollectedUtc?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
    }
}
