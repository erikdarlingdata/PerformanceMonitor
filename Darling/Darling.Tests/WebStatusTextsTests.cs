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
    private static JsonElement Run(string seat = "edit")
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-status-texts-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(seat);

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
        /* The starter dashboard's Overview tile spreads the same stat list, so a dashboard made from it names the number the same way. */
        Assert.Equal("SQL CPU", r.GetProperty("starterTileLabel").GetString());
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

        /* The same through the server page, which hands the tab ctx.offline from the server's fleet card (is_online false). */
        var pageStale = r.GetProperty("pageJobsStale").GetString()!;
        var pageOnline = r.GetProperty("pageJobsOnline").GetString()!;
        Assert.Contains("collection for this server is stale", pageStale, StringComparison.Ordinal);
        Assert.DoesNotContain("No running SQL Agent jobs found", pageStale, StringComparison.Ordinal);
        Assert.Contains("No running SQL Agent jobs found", pageOnline, StringComparison.Ordinal);
        Assert.DoesNotContain("stale", pageOnline, StringComparison.Ordinal);
    }

    [Fact]
    public void RunningJobs_OnThePollWhereAServerFirstGoesStale_IsRepaintedFromTheFreshCard()
    {
        /* The page already held an online card, painted from it, and the fresh card (is_online false) came after. */
        var flip = Run().GetProperty("pageJobsFlip").GetString()!;
        Assert.Contains("collection for this server is stale", flip, StringComparison.Ordinal);
        Assert.DoesNotContain("No running SQL Agent jobs found", flip, StringComparison.Ordinal);
    }

    [Fact]
    public void TheActiveQueriesLiveBar_ShowsAReadOnlySeatTheListOnlyNoteAndNoButton_AndASeatThatCanEditItsButton()
    {
        var readOnly = Run("readonly").GetProperty("liveBar");
        Assert.Equal(0, readOnly.GetProperty("buttons").GetArrayLength());
        Assert.Equal("read-only", readOnly.GetProperty("status").GetString());
        var text = readOnly.GetProperty("text").GetString()!;
        Assert.Contains("To read the active queries live", text, StringComparison.Ordinal);
        Assert.Contains("Viewer", text, StringComparison.Ordinal);

        var edit = Run("edit").GetProperty("liveBar");
        Assert.Equal("ready", edit.GetProperty("status").GetString());
        Assert.Equal("Run live now", Assert.Single(edit.GetProperty("buttons").EnumerateArray()).GetString());
        Assert.DoesNotContain("To read the active queries live", edit.GetProperty("text").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void TheActiveQueriesLiveBar_DrawsNothingWhenTheSessionProbeFailed()
    {
        /* The session answer carries no can_edit, so the client falls back to its fail-closed { can_edit: false, probe_failed: true }: not a read-only seat, so no note, and no button either. */
        var failed = Run("failed").GetProperty("liveBar");
        Assert.Equal(0, failed.GetProperty("buttons").GetArrayLength());
        Assert.Equal(string.Empty, failed.GetProperty("text").GetString());
        Assert.Equal(JsonValueKind.Null, failed.GetProperty("status").ValueKind);
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
