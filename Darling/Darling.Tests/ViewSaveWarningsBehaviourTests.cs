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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7565: the view editors show the warnings the service sent back with a saved view, as text, and a link to the saved view. Run as
/// the shipped page script under Node (<c>view-save-warnings-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class ViewSaveWarningsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(RepoFile.PathTo("Darling", "Darling.Tests", "view-save-warnings-harness.mjs"));
        psi.ArgumentList.Add(RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the view save warnings harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the view save warnings harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheStrip_ShowsEachWarningAsText_WithALinkToTheSavedView()
    {
        var run = Run();

        Assert.Equal("strip notice", run.GetProperty("stripClass").GetString());
        var lines = run.GetProperty("lines").EnumerateArray().Select(x => x.GetString()!).ToArray();
        Assert.Equal("Saved. The service has a warning about this view:", lines[0]);
        /* The text is the validator's, shown as written: markup in it stays text. */
        Assert.Equal("panel 0: use perfmon_value_per_second <b>x</b>", lines[1]);
        Assert.Equal("cell 2: second", lines[2]);
        Assert.Equal("a", run.GetProperty("linkTag").GetString());
        Assert.Equal("#/view/9", run.GetProperty("linkHref").GetString());
    }

    [Fact]
    public void ASavedViewWithNoWarnings_YieldsNone_SoTheEditorRedirectsAsBefore()
    {
        var run = Run();

        Assert.Empty(run.GetProperty("none").EnumerateArray());
        Assert.Empty(run.GetProperty("nullView").EnumerateArray());
        Assert.Equal(new[] { "a", "b" }, run.GetProperty("mixed").EnumerateArray().Select(x => x.GetString()!).ToArray());
    }
}
