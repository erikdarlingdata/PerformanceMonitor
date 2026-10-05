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
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5241: an expanded alert on the web Alert History page shows the advice and the fix script, as the desktop's
/// Alert Detail does. The shipped <c>alerts.js</c> runs under Node (<c>alert-history-advice-harness.mjs</c>) against
/// a recording fetch: a row with <c>details</c> shows the advice as a paragraph and the fix script in a
/// <c>&lt;pre&gt;</c> with a Copy button; a row without them shows its <c>detail_text</c> as before; and every value
/// reaches the DOM as text. Node is skipped when it is not installed.
/// </summary>
public sealed class AlertHistoryAdviceTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-history-advice-harness.mjs"));
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
                Assert.Fail("the Alert History advice harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the Alert History advice harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ThePageAsksForTheDetails()
    {
        var r = Run("withDetails");
        Assert.Equal("true", r.GetProperty("requested").GetProperty("include_details").GetString());
    }

    [Fact]
    public void AnExpandedAlertWithDetails_ShowsTheAdviceAsAParagraphAndTheFixScriptInAPreWithCopy()
    {
        var r = Run("withDetails");
        Assert.Equal(new[] { "Diagnosis", "Advice", "Remediation T-SQL" }, Strings(r.GetProperty("headings")));
        Assert.Equal(new[] { "StoryPLAN_REGRESSION" }, Strings(r.GetProperty("fields")));

        var paragraphs = Strings(r.GetProperty("paragraphs"));
        Assert.Single(paragraphs);
        Assert.Contains("force the better plan", paragraphs[0], StringComparison.Ordinal);

        Assert.Equal(new[] { "EXEC sys.sp_query_store_force_plan 1, 2;" }, Strings(r.GetProperty("pres")));
        Assert.Equal(new[] { "Copy" }, Strings(r.GetProperty("buttons")));

        /* Copy puts the script, and only the script, on the clipboard, and says so. */
        Assert.Equal(new[] { "EXEC sys.sp_query_store_force_plan 1, 2;" }, Strings(r.GetProperty("copied")));
        Assert.Equal("Copied", r.GetProperty("buttonAfter").GetString());
    }

    [Fact]
    public void AnExpandedAlertWithoutDetails_KeepsTheDetailTextRendering()
    {
        var r = Run("withoutDetails");
        Assert.Equal(0, r.GetProperty("paragraphs").GetInt32());
        Assert.Equal(0, r.GetProperty("pres").GetInt32());
        Assert.Equal(0, r.GetProperty("buttons").GetInt32());
        Assert.Equal(new[] { "Storyx", "Severity1.20" }, Strings(r.GetProperty("fields")));
        Assert.Equal(new[] { "Diagnosis" }, Strings(r.GetProperty("headings")));
    }

    [Fact]
    public void EveryValueReachesTheDomAsText()
    {
        var r = Run("hostile");
        Assert.Equal(new[] { "<script>alert(1)</script>" }, Strings(r.GetProperty("pre")));
        Assert.Equal(new[] { "<img src=x onerror=alert(1)>" }, Strings(r.GetProperty("headings")));
        Assert.Equal(0, r.GetProperty("imgs").GetInt32());
    }
}
