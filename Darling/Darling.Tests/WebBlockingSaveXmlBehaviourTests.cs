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
}
