using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A master target's own Blocking and Deadlocks lists keep its server-wide rows while the card counts skip the
/// separately monitored databases' events, so those lists carry one explanatory note, only when the same list the
/// counts use is non-empty.
/// </summary>
public class SeparatelyMonitoredListNoteTests : IDisposable
{
    private const int MasterWithSiblings = -4925_01;
    private const int MasterAlone = -4925_02;
    private const int SqlServerTarget = -4925_03;

    public SeparatelyMonitoredListNoteTests()
    {
        AnalysisService.SeparatelyMonitoredDatabasesProvider = id =>
            id == MasterWithSiblings ? new[] { "GP", "HS" } : null;
    }

    public void Dispose() => AnalysisService.SeparatelyMonitoredDatabasesProvider = null;

    [Fact]
    public void Note_Text_Is_The_Assigned_Sentence()
    {
        Assert.Equal(
            "Events from databases monitored as their own servers are listed here and counted under those servers.",
            AzureMasterScope.SeparatelyMonitoredListNote);
    }

    [Fact]
    public void Note_Shown_For_A_Master_With_Siblings()
    {
        Assert.Equal(AzureMasterScope.SeparatelyMonitoredListNote, SeparatelyMonitoredScope.ListNote(MasterWithSiblings));
    }

    [Theory]
    [InlineData(MasterAlone)]
    [InlineData(SqlServerTarget)]
    public void Note_Absent_For_A_Master_Without_Siblings_And_For_SqlServer(int serverId)
    {
        Assert.Null(SeparatelyMonitoredScope.ListNote(serverId));
    }

    [Fact]
    public void Note_Absent_When_The_Provider_Throws()
    {
        AnalysisService.SeparatelyMonitoredDatabasesProvider = _ => throw new InvalidOperationException("lookup failed");
        Assert.Null(SeparatelyMonitoredScope.ListNote(MasterWithSiblings));
    }

    [Fact]
    public void Mcp_Reply_Field_Carries_The_Note_Only_When_Listed()
    {
        var with = JsonSerializer.Serialize(new { separately_monitored_note = SeparatelyMonitoredScope.ListNote(MasterWithSiblings) });
        var without = JsonSerializer.Serialize(new { separately_monitored_note = SeparatelyMonitoredScope.ListNote(MasterAlone) });
        Assert.Contains(AzureMasterScope.SeparatelyMonitoredListNote, with);
        Assert.Contains("\"separately_monitored_note\":null", without);
    }

    [Fact]
    public void Both_Mcp_Replies_And_Both_Grids_Use_The_Note()
    {
        var mcp = ReadRepoFile("Lite/Mcp/McpBlockingTools.cs");
        Assert.Equal(2, CountOf(mcp, "separately_monitored_note = SeparatelyMonitoredScope.ListNote(resolved.ServerId)"));

        var xaml = ReadRepoFile("Lite/Controls/ServerTab.xaml");
        Assert.Contains("x:Name=\"BlockedProcessReportNoteText\"", xaml);
        Assert.Contains("x:Name=\"DeadlockNoteText\"", xaml);

        var refresh = ReadRepoFile("Lite/Controls/ServerTab.Refresh.cs");
        Assert.Contains("ApplySeparatelyMonitoredListNote(BlockedProcessReportNoteText)", refresh);
        Assert.Contains("ApplySeparatelyMonitoredListNote(DeadlockNoteText)", refresh);
    }

    private static int CountOf(string haystack, string needle)
    {
        var n = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + 1, StringComparison.Ordinal)) n++;
        return n;
    }

    private static string ReadRepoFile(string relative)
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, relative))) dir = dir.Parent;
        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!.FullName, relative));
    }
}
