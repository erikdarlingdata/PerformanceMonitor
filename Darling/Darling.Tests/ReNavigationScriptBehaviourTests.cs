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
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The re-navigation page's script (#6286), the exact text the service sends, run under Node
/// (<c>renavigation-script-harness.mjs</c>): it reloads only for an ordinary navigation, and in every other case
/// (a reload, a missing entry, no <c>performance</c>, a failing call) it shows the "open the address again" message and
/// does not reload, so a browser that keeps the request cross-site cannot loop. Skipped when Node is not installed.
/// </summary>
public sealed class ReNavigationScriptBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var script = DarlingWebHostService.ReNavigationScriptBlock;
        Assert.StartsWith("<script>", script, StringComparison.Ordinal);
        Assert.EndsWith("</script>", script, StringComparison.Ordinal);
        var body = script["<script>".Length..^"</script>".Length];

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "renavigation-script-harness.mjs"));
        psi.ArgumentList.Add(body);
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the re-navigation script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the re-navigation harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the re-navigation harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void AnOrdinaryNavigation_ReloadsOnce_AndShowsNoMessage()
    {
        if (!TryRun("navigate", out var r)) return;

        Assert.Equal(1, r.GetProperty("reload").GetInt32());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("message").ValueKind);
    }

    [Theory]
    [InlineData("reload")]
    [InlineData("backForward")]
    [InlineData("prerender")]
    [InlineData("noEntry")]
    [InlineData("noPerformance")]
    [InlineData("throwing")]
    public void EveryOtherCase_ShowsTheMessage_AndNeverReloads(string scenario)
    {
        if (!TryRun(scenario, out var r)) return;

        Assert.Equal(0, r.GetProperty("reload").GetInt32());
        Assert.Equal("Open the address again to continue.", r.GetProperty("message").GetString());
    }
}
