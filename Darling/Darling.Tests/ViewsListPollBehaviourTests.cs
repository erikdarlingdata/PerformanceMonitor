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
/// The web Custom Views list under its 60 s poll (#7925), run in Node against a recording fetch: the import JSON a reader has
/// pasted, the open Import and template menus and the server picked for a template stay through the poll, the poll reads a
/// view's definition again only when its version changed, a failed poll keeps the list with a notice, and a fresh navigation
/// draws the list anew. Behavior only; the spelling of the page's source is not pinned.
/// </summary>
public sealed class ViewsListPollBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "views-list-poll-harness.mjs"));
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
                Assert.Fail("the views list poll harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the views list poll harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheFirstPaint_DrawsTheCards_AndReadsEveryDefinitionOnce()
    {
        var first = Run().GetProperty("first");
        Assert.Equal(new[] { "Waits by server", "CPU trend", "Blocking" }, Strings(first.GetProperty("names")));
        Assert.Equal(new[] { "2 panels,line,bar", "1 panel,line", "3 panels,pie,stat" }, Strings(first.GetProperty("chips")));
        Assert.Equal(new[] { "1", "2", "3" }, Strings(first.GetProperty("viewReads")));
        Assert.True(first.GetProperty("hasImportBox").GetBoolean());
        Assert.True(first.GetProperty("hasPicker").GetBoolean());
    }

    [Fact]
    public void ThePoll_KeepsThePastedImportText_AndTheOpenMenus_WhileTheCardsAreRedrawn()
    {
        var poll = Run().GetProperty("poll");
        Assert.False(poll.GetProperty("loadingDuringPoll").GetBoolean());
        Assert.True(poll.GetProperty("boxKept").GetBoolean());
        Assert.Equal("{\"definition\": {\"panels\": [{\"viz\": \"line\"}]}}", poll.GetProperty("boxValue").GetString());
        Assert.True(poll.GetProperty("importOpen").GetBoolean());
        Assert.True(poll.GetProperty("menuOpen").GetBoolean());
        Assert.Equal("SQL02", poll.GetProperty("serverPick").GetString());
        Assert.Equal(new[] { "Waits by server", "CPU trend, renamed", "Blocking" }, Strings(poll.GetProperty("names")));
        Assert.Equal(0, poll.GetProperty("refreshError").GetInt32());
    }

    [Fact]
    public void ThePoll_ReadsADefinitionAgain_OnlyWhenTheViewsVersionChanged()
    {
        var poll = Run().GetProperty("poll");
        Assert.Equal(new[] { "3" }, Strings(poll.GetProperty("viewReads")));
        Assert.Equal(new[] { "2 panels,line,bar", "1 panel,line", "2 panels,pie,stat" }, Strings(poll.GetProperty("chips")));
    }

    [Fact]
    public void AFailedPoll_KeepsTheListAndTheImportText_AndSaysSo_UntilTheNextPollSucceeds()
    {
        var run = Run();
        var failed = run.GetProperty("failedPoll");
        Assert.Equal(new[] { "Waits by server", "CPU trend, renamed", "Blocking" }, Strings(failed.GetProperty("names")));
        Assert.Equal("{\"definition\": {\"panels\": [{\"viz\": \"line\"}]}}", failed.GetProperty("boxValue").GetString());
        var notice = Assert.Single(Strings(failed.GetProperty("refreshError")));
        Assert.Contains("Could not refresh", notice);
        Assert.Contains("the store is down", notice);
        Assert.Equal(0, failed.GetProperty("viewReads").GetInt32());
        var recovered = run.GetProperty("recoveredPoll");
        Assert.Equal(0, recovered.GetProperty("refreshError").GetInt32());
        Assert.Equal("{\"definition\": {\"panels\": [{\"viz\": \"line\"}]}}", recovered.GetProperty("boxValue").GetString());
    }

    [Fact]
    public void AFreshNavigation_DrawsTheListAnew_WithAnEmptyImportBox()
    {
        var navigated = Run().GetProperty("navigated");
        Assert.Equal(new[] { "Waits by server", "CPU trend, renamed", "Blocking" }, Strings(navigated.GetProperty("names")));
        Assert.Equal("", navigated.GetProperty("boxValue").GetString());
    }

    [Fact]
    public void APollWhoseFleetReadAddsAServer_ShowsItInThePicker_AndKeepsTheOpenMenuAndThePick()
    {
        var run = Run();
        var grows = run.GetProperty("fleetGrows");
        Assert.Equal(1, grows.GetProperty("fleetReads").GetInt32());
        Assert.Equal(new[] { "SQL01", "SQL02", "SQL03" }, Strings(grows.GetProperty("servers")));
        Assert.Equal("SQL02", grows.GetProperty("serverPick").GetString());
        Assert.True(grows.GetProperty("menuOpen").GetBoolean());
        Assert.Equal("{\"definition\": {\"panels\": [{\"viz\": \"line\"}]}}", grows.GetProperty("boxValue").GetString());
        Assert.Equal(new[] { "Waits by server", "CPU trend, renamed", "Blocking" }, Strings(grows.GetProperty("names")));

        var heroGrows = run.GetProperty("heroFleetGrows");
        Assert.True(heroGrows.GetProperty("heroKept").GetBoolean());
        Assert.True(heroGrows.GetProperty("heroMenuOpen").GetBoolean());
        Assert.Equal(new[] { "SQL01" }, Strings(heroGrows.GetProperty("heroMenuServers")));
        Assert.Equal(new[] { "SQL01" }, Strings(heroGrows.GetProperty("heroServers")));
        Assert.Equal(new[] { "SQL01" }, Strings(heroGrows.GetProperty("headServers")));
        Assert.True(heroGrows.GetProperty("starterChips").GetInt32() > 0);
    }

    [Fact]
    public void APollAfterAFailedFirstPaint_DrawsTheErrorAgain_OrTheEmptyStoreHero()
    {
        var run = Run();
        var first = run.GetProperty("failedFirstPaint");
        Assert.False(first.GetProperty("hero").GetBoolean());
        Assert.Contains("the store is down", Assert.Single(Strings(first.GetProperty("errors"))));

        var again = run.GetProperty("failedAgainPoll");
        Assert.Contains("the store is down", Assert.Single(Strings(again.GetProperty("errors"))));
        Assert.Equal(0, again.GetProperty("refreshError").GetInt32());
        Assert.False(again.GetProperty("hero").GetBoolean());

        var empty = run.GetProperty("emptyAfterFailure");
        Assert.True(empty.GetProperty("hero").GetBoolean());
        Assert.Equal("Compose your own dashboards & notebooks", empty.GetProperty("heroTitle").GetString());
        Assert.Empty(Strings(empty.GetProperty("errors")));
        Assert.Equal(0, empty.GetProperty("refreshError").GetInt32());
        Assert.Empty(Strings(empty.GetProperty("heroServers")));
    }

    [Fact]
    public void APollAfterTheSessionProbeRecovers_DrawsTheEditingAffordances()
    {
        var run = Run();
        var readOnly = run.GetProperty("readOnlyFirstPaint");
        Assert.Equal(new[] { "Waits by server", "CPU trend", "Blocking" }, Strings(readOnly.GetProperty("names")));
        Assert.Equal(0, readOnly.GetProperty("templateMenus").GetInt32());
        Assert.Equal(0, readOnly.GetProperty("newView").GetInt32());
        Assert.False(readOnly.GetProperty("hasImportBox").GetBoolean());

        var edit = run.GetProperty("canEditPoll");
        Assert.Equal(new[] { "Waits by server", "CPU trend", "Blocking" }, Strings(edit.GetProperty("names")));
        Assert.Equal(1, edit.GetProperty("templateMenus").GetInt32());
        Assert.Equal(1, edit.GetProperty("newView").GetInt32());
        Assert.True(edit.GetProperty("hasImportBox").GetBoolean());
        Assert.Equal(new[] { "SQL01", "SQL02" }, Strings(edit.GetProperty("servers")));
    }
}
