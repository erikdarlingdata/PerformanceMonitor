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
/// The Window picker on the FinOps Optimization, High Impact and Application Connections tabs, run from the shipped
/// page scripts under Node (<c>finops-window-picker-harness.mjs</c>): the first build reads 24 hours, a pick re-reads
/// with the chosen hours, a rebuild for the same server (the 60 s poll) keeps the pick, and another server starts at
/// 24 hours again. Node is skipped when it is not installed.
/// </summary>
public sealed class FinOpsWindowPickerBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-window-picker-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the window-picker harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the window-picker harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Hours(JsonElement step) => step.GetProperty("hours").EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Theory]
    [InlineData("optimization")]
    [InlineData("high-impact")]
    [InlineData("application-connections")]
    public void ThePickKeepsAcrossARebuildForTheSameServerAndResetsForAnother(string tab)
    {
        var t = Run().GetProperty(tab);

        var first = t.GetProperty("first");
        Assert.Equal(new[] { "24" }, Hours(first));
        Assert.Equal("1,4,12,24,168", string.Join(",", first.GetProperty("options").EnumerateArray().Select(e => e.GetString())));

        // A pick reads again with the chosen hours.
        var picked = t.GetProperty("picked");
        Assert.Equal(new[] { "24", "4" }, Hours(picked));
        Assert.Equal("4", picked.GetProperty("select").GetString());

        // The poll rebuilds the tab: same server, same window, one read with it.
        var rebuilt = t.GetProperty("rebuilt");
        Assert.Equal(new[] { "4" }, Hours(rebuilt));
        Assert.Equal("4", rebuilt.GetProperty("select").GetString());

        // Another server starts at the default.
        var other = t.GetProperty("other");
        Assert.Equal(new[] { "24" }, Hours(other));
        Assert.Equal("24", other.GetProperty("select").GetString());
    }
}
