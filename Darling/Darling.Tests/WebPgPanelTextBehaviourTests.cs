/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7548: on a healthy PostgreSQL server the Deadlocks, Deadlock Graphs, Column Statistics and Index Bloat (answered ranking) panels say one line a
/// person can act on, and the text the MCP tool sends sits behind "More detail". These run the shipped <c>server-tabs.js</c> under Node
/// (<c>web-pg-panel-text-harness.mjs</c>) against replies that carry the tool's real text; skipped when Node is not installed.
/// </summary>
public sealed class WebPgPanelTextBehaviourTests
{
    private static readonly Regex IssueNumber = new(@"#\d", RegexOptions.Compiled);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-panel-text-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the panel text harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the panel text harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static void AssertPlainLine(string line)
    {
        Assert.DoesNotMatch(IssueNumber, line);
        foreach (var internalWord in new[] { "facet", "message_locale", "log_line_prefix_readable", "lc_messages", "collector judges" })
        {
            Assert.DoesNotContain(internalWord, line, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("deadlocks")]
    [InlineData("graphs")]
    public void ADeadlockPanelOnAHealthyServer_SaysOneLine_AndNamesThePanelThatHoldsTheAnswer(string panel)
    {
        var p = Run().GetProperty(panel);
        var line = p.GetProperty("line").GetString()!;
        Assert.Contains("If you expected one, Plan Capture Readiness above shows whether the server log can be read.", line, StringComparison.Ordinal);
        Assert.True(line.Length < 160, "the line should be one short sentence pair, not the tool's paragraph: " + line);
        AssertPlainLine(line);
        Assert.DoesNotContain("the Activity tab", line, StringComparison.Ordinal);

        // The tool's own text is kept, one click away, and no issue number reaches it either.
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
        Assert.Equal("More detail", p.GetProperty("summary").GetString());
        var detail = p.GetProperty("detail").GetString()!;
        Assert.Contains("healthy answer", detail, StringComparison.Ordinal);
        Assert.DoesNotMatch(IssueNumber, detail);
    }

    [Fact]
    public void ColumnStatisticsWithoutPgStatsAccess_SaysOneSentenceAndTheGrant_AndKeepsTheRestBehindMoreDetail()
    {
        var p = Run().GetProperty("columns");
        var line = p.GetProperty("line").GetString()!;
        Assert.Contains("the monitoring login cannot read pg_stats", line, StringComparison.Ordinal);
        Assert.Contains("GRANT pg_read_all_data TO <login> on PostgreSQL 14 and above, or GRANT SELECT on the tables on 13.", line, StringComparison.Ordinal);
        Assert.DoesNotContain("CAUSE:", line, StringComparison.Ordinal);
        Assert.DoesNotContain("Remedy:", line, StringComparison.Ordinal);
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
        Assert.Contains("CAUSE: the privilege filter", p.GetProperty("detail").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void ColumnStatisticsEmptyForAnotherCause_KeepsTheToolsText()
    {
        var p = Run().GetProperty("columnsFloor");
        Assert.Contains("CAUSE: the size floor", p.GetProperty("line").GetString()!, StringComparison.Ordinal);
        Assert.Equal(0, p.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void IndexBloatRankingWithoutPgStatsAccess_SaysOneSentenceAndTheGrant_AndKeepsTheRestBehindMoreDetail()
    {
        var p = Run().GetProperty("bloat");
        var line = p.GetProperty("line").GetString()!;
        Assert.Contains("the monitoring login cannot read pg_stats", line, StringComparison.Ordinal);
        Assert.Contains("GRANT pg_read_all_data TO <login>", line, StringComparison.Ordinal);
        foreach (var shouted in new[] { "NOT ONE", "NO COVERAGE AT ALL", "index(es)" })
        {
            Assert.DoesNotContain(shouted, line, StringComparison.Ordinal);
        }

        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
        Assert.Contains("NOT ONE", p.GetProperty("detail").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void IndexBloatRankingEmptyForAnotherCause_PointsAtTheTableThatListsTheReasons()
    {
        var p = Run().GetProperty("bloatOther");
        var line = p.GetProperty("line").GetString()!;
        Assert.Contains("Index Bloat (estimated)", line, StringComparison.Ordinal);
        // The table above lists at most 25 indexes, so the line names the column that holds each reason and does not promise a reason for every index.
        Assert.Contains("Not Measured", line, StringComparison.Ordinal);
        Assert.DoesNotContain("each one", line, StringComparison.Ordinal);
        Assert.DoesNotContain("GRANT", line, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT ONE", line, StringComparison.Ordinal);
        Assert.Equal(1, p.GetProperty("hasDetails").GetInt32());
    }

    [Fact]
    public void AParenthesizedIssueReference_IsDroppedFromTextThePageShows()
    {
        var refs = Run().GetProperty("refs").EnumerateArray();
        var said = System.Linq.Enumerable.ToArray(System.Linq.Enumerable.Select(refs, x => x.GetString()!));
        Assert.Equal("The read went on without them. The next sentence stays.", said[0]);
        Assert.Equal("Older ones were not collected.", said[1]);
        Assert.Equal("It reads the facet (the setting of the Activity tab). It stays.", said[2]);
        // A hex colour is not an issue reference.
        Assert.Equal("A real color #416FA6 and a count of 3 stay.", said[3]);
    }

    [Fact]
    public void TheStoredWithheldMarker_ReadsAsPlainWordsInAStatementCell_AndNoOtherTextIsTouched()
    {
        var w = Run().GetProperty("withheld");
        Assert.Equal("Statement text withheld by the statement filter.", w.GetProperty("marker").GetString());
        Assert.Equal("Statement text withheld by the statement filter.", w.GetProperty("padded").GetString());
        // A statement that merely holds the marker, and a SQL comment that holds issue references, show exactly as stored.
        Assert.Equal("SELECT 1 /* see -- statement text withheld (#4348) */", w.GetProperty("containing").GetString());
        Assert.Equal("SELECT 1 -- fixed in (#123), then (#4046, #6866)", w.GetProperty("comment").GetString());
    }

    [Fact]
    public void AWithheldPlan_ReadsAsPlainWords_AndARealPlanIsStillAPlan()
    {
        var w = Run().GetProperty("withheld");
        Assert.Equal("none", w.GetProperty("plan").GetProperty("kind").GetString());
        Assert.Equal("This plan was withheld by the statement filter.", w.GetProperty("plan").GetProperty("message").GetString());
        Assert.Equal("plan", w.GetProperty("realPlan").GetProperty("kind").GetString());
    }
}
