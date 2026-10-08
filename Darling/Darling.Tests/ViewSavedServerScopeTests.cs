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
/// W4 of the morning walk: a custom view saved with a server (its server-dimension variable's default) opened on
/// "All servers (fleet)" and read the whole fleet, because the scope control always seeded "All" and the run body's
/// explicit server "All" beat the variable on the service side. The shipped <c>views.js</c> seeding is run under Node
/// (<c>view-scope-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class ViewSavedServerScopeTests
{
    private static bool TryRun(out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "view-scope-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped view-scope code cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the view-scope harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the view-scope harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Names(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void AViewSavedWithAServer_OpensOnThatServer()
    {
        if (!TryRun(out var r)) return;
        Assert.Equal(new[] { "sql2025" }, Names(r.GetProperty("byName")));
        Assert.Equal(new[] { "sql2022" }, Names(r.GetProperty("byDisplayName")));
        Assert.Equal(new[] { "sql2025", "sql2022" }, Names(r.GetProperty("list")));
    }

    [Theory]
    [InlineData("none")]
    [InlineData("all")]
    [InlineData("blank")]
    [InlineData("reference")]
    [InlineData("unknown")]
    [InlineData("twoServerVariables")]
    [InlineData("otherDimension")]
    public void AViewWithNoUsableSavedServer_StaysOnTheWholeFleet(string scenario)
    {
        if (!TryRun(out var r)) return;
        Assert.Equal("All", r.GetProperty(scenario).GetString());
    }
}
