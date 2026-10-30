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
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A long text cell (a collector's note, error or output; a job's message) shows on one line and opens the rest on a click
/// (#5644 W11): the rows were hundreds of pixels tall. These run the shipped <c>panels.js</c> and <c>util.js</c> under Node
/// (<c>web-clamp-cell-harness.mjs</c>), plus pins on the column lists and the stylesheet. Skipped when Node is not installed,
/// except the source pins.
/// </summary>
public sealed class WebClampCellBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clamp-cell-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped table script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the clamp cell harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the clamp cell harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string Wwwroot(params string[] parts) =>
        System.IO.File.ReadAllText(PathTo(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }.Concat(parts).ToArray()));

    [Fact]
    public void AClampColumn_KeepsTheFullText_AndHasTheClampClass()
    {
        var r = Run();
        Assert.Equal("clamp", r.GetProperty("noteTdClass").GetString());
        Assert.Equal("wrap", r.GetProperty("plainTdClass").GetString());
        Assert.True(r.GetProperty("longHasFullText").GetBoolean());
        Assert.True(r.GetProperty("longTitleFull").GetBoolean());
    }

    [Fact]
    public void ALongCell_OpensAndClosesOnAClick_WithoutActivatingTheRow()
    {
        var r = Run();
        Assert.Equal("button", r.GetProperty("longRole").GetString());
        Assert.Equal("false", r.GetProperty("longExpandedBefore").GetString());
        var first = r.GetProperty("afterClick");
        Assert.Equal("true", first.GetProperty("expanded").GetString());
        Assert.True(first.GetProperty("open").GetBoolean());
        Assert.True(first.GetProperty("stopped").GetBoolean());
        Assert.Equal(0, first.GetProperty("rowClicks").GetInt32());
        var second = r.GetProperty("afterSecondClick");
        Assert.Equal("false", second.GetProperty("expanded").GetString());
        Assert.False(second.GetProperty("open").GetBoolean());
    }

    [Fact]
    public void OnlyAnOptedInMessageColumn_LocalizesAnIsoInstant_SqlTextAndDetailsPrintAsWritten()
    {
        // #5644 W20: a query_text of WHERE d >= '2026-01-01T00:00:00' showed as a local date in the browser.
        var l = Run().GetProperty("localize");
        Assert.True(l.GetProperty("sqlUnchanged").GetBoolean());
        Assert.True(l.GetProperty("detailsUnchanged").GetBoolean());
        var expected = l.GetProperty("expected").GetString();
        Assert.Equal(expected, l.GetProperty("messageLocal").GetString());
        Assert.Equal(expected, l.GetProperty("clampedLocal").GetString());
        Assert.DoesNotContain("T14:37", expected);
    }

    [Fact]
    public void EachAlertSweepAndRegressionShape_IsLocalizedInAnOptedInCell_AndPrintedAsWrittenElsewhere()
    {
        // #5644 W20 (H2): "until 2026-10-11 12:48:52Z", "service started 2026-10-12T23:01:13.7792072Z", "Failed At: 2026-10-12 07:00:00Z".
        var rows = Run().GetProperty("shapes").EnumerateArray().ToList();
        Assert.Equal(6, rows.Count);
        var sqlZ = "SELECT * FROM t WHERE d = '2026-01-01 00:00:00Z'";
        foreach (var r in rows)
        {
            Assert.Equal(sqlZ, r.GetProperty("sql").GetString());
            Assert.Equal(r.GetProperty("raw").GetString(), r.GetProperty("notOpted").GetString());
        }

        foreach (var r in rows.Take(5))
        {
            var expected = r.GetProperty("expected").GetString();
            Assert.Equal(expected, r.GetProperty("opted").GetString());
            Assert.Equal(expected, r.GetProperty("clamped").GetString());
            Assert.NotEqual(r.GetProperty("raw").GetString(), r.GetProperty("opted").GetString());
        }

        // A server's own wall clock (no Z, no UTC) is never shifted, even in an opted-in cell.
        Assert.Equal("start time 2026-09-27 21:32:43 (server clock)", rows[5].GetProperty("opted").GetString());
    }

    [Fact]
    public void TheFleetSweepReasonsAndNotes_AreOptedIn_AndTheAlertPagesLocalizeTheirBodies()
    {
        var sweeps = Wwwroot("js", "pages", "sweeps.js");
        Assert.Equal(2, Regex.Matches(sweeps, "key: \"reason\", label: \"Reason\", wrap: true, localizeText: true").Count);
        Assert.Contains("el(\"li\", { text: localizeIsoText(String(n)) })", sweeps, StringComparison.Ordinal);
        foreach (var page in new[] { "alerts.js", "triage.js", "views.js" })
        {
            Assert.Contains("localizeAlertDetail(", Wwwroot("js", "pages", page), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheCollectionHealthMessageColumns_AreOptedIn_AndTheCustomViewAndDefaultTraceTextColumnsAreNot()
    {
        var tabs = Wwwroot("js", "pages", "server-tabs.js");
        var health = tabs[tabs.IndexOf("key: \"run_time_note\"", StringComparison.Ordinal)..tabs.IndexOf("const HEAVIEST_COLUMNS", StringComparison.Ordinal)];
        Assert.Equal(7, Regex.Matches(health, "clamp: true, localizeText: true").Count);
        foreach (var f in new[] { "js/read-fields.js", "js/pages/server-tabs.js" })
        {
            var src = Wwwroot(f.Split('/'));
            Assert.DoesNotMatch(new Regex("key: \"(query_text|statement_text|query_sql_text|sql_text|details)\"[^\n]*localizeText"), src);
        }
    }

    [Fact]
    public void TheAlertHistoryDetailColumn_LocalizesItsTimeLines_AndNeverTheSqlLines()
    {
        // #5644 W20 (H1): get_alert_history's detail_text opts in with localizeAlertDetail; localizeText is never used on a column of SQL.
        var d = Run().GetProperty("alertDetail");
        var raw = d.GetProperty("raw").GetString()!;
        Assert.Equal(raw, d.GetProperty("notOpted").GetString());
        var opted = d.GetProperty("opted").GetString()!;
        Assert.Contains(d.GetProperty("sinceLocal").GetString()!, opted);
        Assert.Contains(d.GetProperty("sampleLocal").GetString()!, opted);
        Assert.Contains(d.GetProperty("sqlLine").GetString()!, opted);
        Assert.DoesNotContain("23:13:00Z", opted);

        var fields = Wwwroot("js", "read-fields.js");
        Assert.Contains("{ key: \"detail_text\", label: \"Detail\", wrap: true, localizeAlertDetail: true },", fields, StringComparison.Ordinal);
    }

    [Fact]
    public void ACellsPlainMode_PicksItsOwnPass_CountsKeepsAToolNameInARealError()
    {
        // #6649: the Collection Log's Error column is `plain: "counts"`; a mistyped key would fall through to the full pass, which
        // turns a real error's get_ name into a tab name.
        var p = Run().GetProperty("plainModes");
        var raw = p.GetProperty("raw").GetString()!;
        var counts = p.GetProperty("counts").GetString()!;
        Assert.Equal("Connection failed (timeout=30): ring buffer truncated: 1, ring buffer events hidden: 167, newest visible event age s: 4300; ask get_collection_health for the time range", counts);
        var full = p.GetProperty("full").GetString()!;
        Assert.Contains("ask Collection Health for the time range", full);
        Assert.DoesNotContain("get_collection_health", full);
        var named = p.GetProperty("named").GetString()!;
        Assert.Contains("ring_buffer_truncated=1", named);
        Assert.Contains("get_collection_health for the time range", named);
        Assert.Equal(raw, p.GetProperty("asStored").GetString());
    }

    [Fact]
    public void EnterOpensTheCell_AndOtherKeysDoNot()
    {
        var r = Run();
        var enter = r.GetProperty("afterEnter");
        Assert.Equal("true", enter.GetProperty("expanded").GetString());
        Assert.True(enter.GetProperty("prevented").GetBoolean());
        Assert.True(enter.GetProperty("stopped").GetBoolean());
        var other = r.GetProperty("otherKey");
        Assert.Equal("true", other.GetProperty("expanded").GetString());
        Assert.False(other.GetProperty("stopped").GetBoolean());
    }

    [Fact]
    public void AShortCell_AnEmptyCell_AndANonExpandableCell_AreInert()
    {
        var r = Run();
        var s = r.GetProperty("short");
        Assert.Equal(JsonValueKind.Null, s.GetProperty("role").ValueKind);
        Assert.Equal("short", s.GetProperty("text").GetString());
        var n = r.GetProperty("notExpandable");
        Assert.Equal(JsonValueKind.Null, n.GetProperty("role").ValueKind);
        Assert.True(n.GetProperty("text").GetBoolean());
        Assert.True(n.GetProperty("title").GetBoolean());
        Assert.Equal("", r.GetProperty("empty").GetProperty("text").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("empty").GetProperty("title").ValueKind);
        Assert.Equal("button", r.GetProperty("multiline").GetString());
    }

    [Fact]
    public void TheCollectorsLongTextColumns_AndBothJobMessageColumns_AreClamped()
    {
        var tabs = Wwwroot("js", "pages", "server-tabs.js").ReplaceLineEndings("\n");
        var columns = tabs[tabs.IndexOf("const COLLECTOR_COLUMNS = [", StringComparison.Ordinal)..];
        columns = columns[..columns.IndexOf("\n];", StringComparison.Ordinal)];
        foreach (var key in new[] { "run_time_note", "last_error", "note_summary", "recovered_note", "output_finding", "regression_finding", "message" })
            Assert.Matches("key: \"" + key + "\"[^\\n]*clamp: true", columns);
        Assert.DoesNotMatch("key: \"(last_error|note_summary|output_finding)\"[^\\n]*wrap: true", columns);

        Assert.Matches("label: \"Message\", clamp: true, render: jobMessageCell", tabs);
        var page = Wwwroot("js", "pages", "job-history.js");
        Assert.Matches("label: \"Message\", clamp: true, render: messageCell", page);
    }

    [Fact]
    public void TheStylesheet_ClipsAClampCellToOneLine_AndLetsAnOpenOneWrap()
    {
        var css = Wwwroot("css", "app.css");
        Assert.Matches(@"\.clamp-text \{[^}]*overflow: hidden;[^}]*text-overflow: ellipsis;[^}]*white-space: nowrap;", css);
        Assert.Matches(@"\.clamp-text\.open \{[^}]*white-space: pre-wrap;", css);
        Assert.DoesNotMatch(@"\.jh-message \{[^}]*line-clamp", css);
    }
}
