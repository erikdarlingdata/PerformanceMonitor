/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// What the Save XML buttons on the web Blocking tab do, from the shipped <c>server-tabs.js</c> and <c>grid-tools.js</c> run under
/// Node (<c>web-blocking-save-xml-harness.mjs</c>) against scripted reads: the Blob's type, text and file name for a report (.xml)
/// and a deadlock graph (.xdl), a cut graph disabling its button, and a row with no XML. Node is skipped when it is not installed.
/// </summary>
public sealed class WebBlockingSaveXmlBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-blocking-save-xml-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the save-xml harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the save-xml harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void SaveXml_DownloadsABlobWithTheRightNameAndType_ForAReportAndADeadlockGraph()
    {
        var r = Run("save");
        var downloads = r.GetProperty("downloads").EnumerateArray().ToArray();
        Assert.Equal(2, downloads.Length);
        Assert.Equal("blocked_process_20260701_110203.xml", downloads[0].GetProperty("name").GetString());
        Assert.Equal("<blocked-process-report/>", downloads[0].GetProperty("text").GetString());
        Assert.Equal("deadlock_20260701_100005.xdl", downloads[1].GetProperty("name").GetString());
        Assert.Equal("<deadlock-list><deadlock/></deadlock-list>", downloads[1].GetProperty("text").GetString());
        Assert.All(downloads, d => Assert.StartsWith("application/xml", d.GetProperty("type").GetString()));
        // The rows already carry the XML: saving makes no extra read.
        Assert.Equal(2, r.GetProperty("fetches").GetInt32());
        Assert.False(r.GetProperty("bprDisabledBefore").GetBoolean());
    }

    [Fact]
    public void TheGridsShowTheDesktopHeaders_WithASaveXmlColumn()
    {
        var r = Run("save");
        Assert.Equal(new[] { "Event Time", "XML", "Database", "Blocked SPID", "Blocking SPID", "Wait Time", "Report" }, Strings(r.GetProperty("bprHeaders")));
        Assert.Contains("XML", Strings(r.GetProperty("deadlockHeaders")));
    }

    [Fact]
    public void ACutDeadlockGraph_DisablesSave_AndSaysWhy_AndDownloadsNothing()
    {
        var r = Run("truncated");
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.False(r.GetProperty("hasClick").GetBoolean());
        Assert.Contains("sent only a preview of the XML", r.GetProperty("title").GetString());
        Assert.Equal(0, r.GetProperty("downloads").GetInt32());
    }

    [Fact]
    public void ARowWithNoXml_HasADisabledSaveButton()
    {
        var r = Run("noxml");
        Assert.True(r.GetProperty("disabled").GetBoolean());
        Assert.False(r.GetProperty("hasClick").GetBoolean());
    }

    /* #7545: a report the read left out for size is a grid row with a Load XML button, the note says why, and loading it reads that
       report alone (as_of at its event time, a few rows, the matching one taken) and fills the Report cell and the Save XML button. */
    [Fact]
    public void AReportLeftOutForSize_IsARowThatLoadsItsXmlOnClick()
    {
        var r = Run("leftout");
        Assert.Equal(2, r.GetProperty("rows").GetInt32());
        Assert.True(r.GetProperty("noteShown").GetBoolean());
        Assert.Equal(1, r.GetProperty("loadButtons").GetInt32());
        Assert.Equal(1, r.GetProperty("loadFetches").GetInt32());
        var url = r.GetProperty("loadUrl").GetString()!;
        Assert.Contains("limit=5", url);
        Assert.Contains("as_of=2026-07-01T10%3A40%3A00", url);
        Assert.True(r.GetProperty("reportShown").GetBoolean());
        var downloads = r.GetProperty("downloads").EnumerateArray().Select(d => d.GetProperty("text").GetString()).ToArray();
        Assert.Contains("<loaded-report/>", downloads);
    }

    /* #7545: two reports share one event_time and the read answers with the OTHER one first. The row gets its own report, matched on
       event time, database and both session ids, never the first row of the answer. Fails on a page that takes reports[0]. */
    [Fact]
    public void LoadXml_WhenTwoReportsShareAnEventTime_AttachesOnlyTheMatchingReport()
    {
        var r = Run("sametime");
        Assert.Equal(0, r.GetProperty("loadButtonsAfter").GetInt32());
        Assert.False(r.GetProperty("otherShown").GetBoolean());
        var downloads = r.GetProperty("downloads").EnumerateArray().Select(d => d.GetProperty("text").GetString()).ToArray();
        Assert.Contains("<loaded-report/>", downloads);
        Assert.DoesNotContain("<other-report/>", downloads);
    }

    /* #7545: only another report comes back (the row's own is gone). The row says it could not be read and to try again, attaches
       nothing and Save XML writes no file for it. */
    [Fact]
    public void LoadXml_WhenOnlyAnotherReportComesBack_SaysSoAndAttachesNothing()
    {
        var r = Run("nomatch");
        Assert.False(r.GetProperty("otherShown").GetBoolean());
        Assert.True(r.GetProperty("errorShown").GetBoolean());
        Assert.True(r.GetProperty("tryAgainShown").GetBoolean());
        Assert.False(r.GetProperty("shareTimeShown").GetBoolean());
        Assert.Equal(1, r.GetProperty("loadButtonsAfter").GetInt32());
        /* The first row has its own XML and saves it; the row with none writes no file, and the other report is nowhere. */
        var downloads = r.GetProperty("downloads").EnumerateArray().Select(d => d.GetProperty("text").GetString()).ToArray();
        Assert.Equal(new[] { "<blocked-process-report/>" }, downloads);
    }

    /* #7545: the row's own report is left out again. The page gets the web fold's output, not the tool's: the other report with its XML
       and the row's own with xml_left_out true and no XML, and no left_out_reports key. The row says other reports share its time (a retry
       cannot help) and not "Try again"; Save XML writes no file for it. Fails on a page that looks for left_out_reports. */
    [Fact]
    public void LoadXml_WhenTheRowsOwnReportIsLeftOutAgain_SaysOtherReportsShareItsTime()
    {
        var r = Run("leftagain");
        Assert.False(r.GetProperty("otherShown").GetBoolean());
        Assert.True(r.GetProperty("shareTimeShown").GetBoolean());
        Assert.False(r.GetProperty("tryAgainShown").GetBoolean());
        Assert.Equal(1, r.GetProperty("loadButtonsAfter").GetInt32());
        var downloads = r.GetProperty("downloads").EnumerateArray().Select(d => d.GetProperty("text").GetString()).ToArray();
        Assert.Equal(new[] { "<blocked-process-report/>" }, downloads);
    }
}
