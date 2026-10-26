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
/// Web pages that stated two different things at once. The page script runs under Node
/// (<c>web-status-texts-harness.mjs</c>): the Queries tab's one banner beside the live-read notice, the Overview CPU
/// tile's label, and the Running Jobs panel's empty text on a server whose collection is stale. The sign-in page is
/// built in C#, so it is read directly. Node is skipped when it is not installed.
/// </summary>
public sealed class WebStatusTextsTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-status-texts-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the status-texts harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the status-texts harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheQueriesTab_ShowsOneBanner_AndTheLiveReadAdviceLivesOnlyInTheLiveReadNotice()
    {
        var r = Run();
        var banner = r.GetProperty("queriesNoteText").GetString()!;
        var notice = r.GetProperty("liveNotice").GetString()!;

        /* The tab banner is about plans; the Run live now advice used to repeat in it beside the notice. */
        Assert.Contains("desktop viewer", banner, StringComparison.Ordinal);
        Assert.DoesNotContain("Run live now", banner, StringComparison.Ordinal);
        Assert.DoesNotContain("Current Active Queries", banner, StringComparison.Ordinal);

        /* The notice says why a seat that can edit still cannot do this, and where the script is. */
        Assert.Contains("This seat can edit", notice, StringComparison.Ordinal);
        Assert.Contains("postgres.webConnectionString", notice, StringComparison.Ordinal);
        Assert.Contains("provision-roles.sql", notice, StringComparison.Ordinal);
        Assert.Contains("GitHub repository", notice, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOverviewCpuTile_SaysItIsTheSqlProcess_SoItMatchesTheHeaderBreakdown()
    {
        var r = Run();
        Assert.Equal("SQL CPU", r.GetProperty("tileLabel").GetString());
        Assert.Equal("SQL CPU", r.GetProperty("columnLabel").GetString());
    }

    [Fact]
    public void RunningJobs_OnAServerWithStaleCollection_NeverReadsAsNormal_AndACollectingServerKeepsTheReadsOwnWords()
    {
        var r = Run();
        var stale = r.GetProperty("jobsStale").GetString()!;
        var online = r.GetProperty("jobsOnline").GetString()!;

        Assert.Contains("collection for this server is stale", stale, StringComparison.Ordinal);
        Assert.DoesNotContain("normal", stale, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("No running SQL Agent jobs found", stale, StringComparison.Ordinal);

        Assert.Contains("No running SQL Agent jobs found", online, StringComparison.Ordinal);
        Assert.DoesNotContain("stale", online, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheSignInPage_NamesBothEnginesLikeTheSignedInSidebar(bool oidcEnabled)
    {
        var html = DarlingWebHostService.BuildLoginPageHtml(oidcEnabled);

        Assert.Contains("SQL Server and PostgreSQL monitor", html, StringComparison.Ordinal);
        Assert.DoesNotContain("SQL Server fleet monitor", html, StringComparison.Ordinal);
    }
}
