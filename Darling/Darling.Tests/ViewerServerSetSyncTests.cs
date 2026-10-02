/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's server list follows servers added or removed OUTSIDE this window — another viewer, the web
/// viewer, the MCP add and remove tools — on the fleet refresh tick, instead of only at the next start.
///
/// <para><b>What was wrong.</b> The sidebar, the status bar's "Servers: N", the Overview's total and the
/// server list Settings hands the database state editor all read the one fleet that
/// <c>LoadServersAsync</c> fills, and nothing filled it again after start except an add, edit or remove made
/// in this window. A server added elsewhere showed in the FinOps picker (which re-reads the registry when its
/// tab opens) and nowhere else until a restart.</para>
///
/// <para><b>Two halves.</b> The source pins hold the wiring in the shell: the tick fires the sync above its
/// tab early-return, the sync reloads through the same <c>LoadServersAsync(preserveSelection: true)</c> a
/// local add or remove ends with, a server removed elsewhere gets the local remove's own cleanup, and every
/// surface that lists servers reads the fleet that reload rebuilds. The behavior tests drive the comparer
/// that decides whether to reload at all, against a real <c>FleetView</c>, and count the reloads.</para>
/// </summary>
public sealed class ViewerServerSetSyncTests
{
    // ── Source pins: the wiring in the viewer shell ──────────────────────────────────────────────

    [Fact]
    public void RefreshTick_FiresTheServerSetSync_AboveTheTabEarlyReturn_AndOnlyOnThatTimer()
    {
        var tick = MethodBody("MainWindow.xaml.cs", "OnRefreshTimerTick");
        var sync = Regex.Match(tick, @"\b_\s*=\s*SyncServerSetAsync\s*\(");

        Assert.True(
            sync.Success,
            "OnRefreshTimerTick does not fire SyncServerSetAsync, so a server added or removed outside this " +
            "window never reaches the sidebar, the status bar or the Overview until the viewer restarts");

        /* Below the tab early-return the sync would stop whenever the Overview or a server tab is up, which
           is most of the time: the Overview is the tab that ships selected. */
        var tabReturn = tick.IndexOf("ReferenceEquals(MainTabs.SelectedItem", StringComparison.Ordinal);

        Assert.True(tabReturn > 0, "the tab early-return this pin measures against is gone from OnRefreshTimerTick");
        Assert.True(
            sync.Index < tabReturn,
            $"SyncServerSetAsync is fired at {sync.Index}, below the tab early-return at {tabReturn}");

        /* One timer, not two: the Overview timer firing it as well would read the registry twice a cycle. */
        Assert.DoesNotMatch(@"\bSyncServerSetAsync\b", MethodBody("MainWindow.xaml.cs", "OnOverviewTimerTick"));
    }

    [Fact]
    public void ServerSetSync_ReloadsOnlyThroughTheComparer_KeepingTheSelection()
    {
        var sync = MethodBody("MainWindow.ServerManagement.cs", "SyncServerSetAsync");
        var load = MethodBody("MainWindow.xaml.cs", "LoadServersAsync");

        /* The load reads the managed list, which on a seeded store is the config list the sync compares
           (ServerSetSync_ActsOnlyOnTheConfigList_NeverOnTheObservedFallback), so an id that differs is a real
           add or remove rather than two sources disagreeing on every tick. */
        Assert.Matches(@"_dataService\s*\.\s*GetManagedServersAsync\s*\(", load);

        /* Exactly one reload, it keeps the selection, and it is handed to the comparer rather than called on
           every pass. The behavior tests below prove the comparer calls it only when the id set changed. */
        var apply = Regex.Match(sync, @"\bViewerServerSetSync\s*\.\s*ApplyAsync\s*\(");

        Assert.True(apply.Success, "SyncServerSetAsync does not go through ViewerServerSetSync.ApplyAsync");
        Assert.Single(Regex.Matches(sync, @"\bLoadServersAsync\s*\("));

        var reload = Regex.Match(sync, @"\bLoadServersAsync\s*\(\s*preserveSelection\s*:\s*true\s*\)");
        var applyArgsEnd = ClosingParen(sync, apply.Index + apply.Length - 1);

        Assert.True(reload.Success, "the sync's reload does not pass preserveSelection: true, so it would move the selection");
        Assert.True(
            reload.Index > apply.Index && reload.Index < applyArgsEnd,
            "the sync's LoadServersAsync call is not inside the ViewerServerSetSync.ApplyAsync arguments, so " +
            "nothing stops it reloading on an unchanged server set");

        /* preserveSelection restores by server id, the same way after a local add or remove. */
        Assert.Matches(@"ResolveSelection\s*\(\s*preserveSelection\s*\?\s*previousSelection\s*:\s*null\s*\)", load);
    }

    [Fact]
    public void ServerSetSync_ActsOnlyOnTheConfigList_NeverOnTheObservedFallback()
    {
        var sync = MethodBody("MainWindow.ServerManagement.cs", "SyncServerSetAsync");

        /* GetManagedServersAsync answers from the OBSERVED list whenever the seeded check returns false, and
           that check returns false on any error. After a store restart the check can fail on a dead pooled
           connection while the next read succeeds, and the observed list lacks every configured server that
           has never collected: compared as the registry, each one would read as removed and lose its pin. So
           the sync reads the config list or nothing, and hands the comparer what it got. */
        Assert.Matches(@"\bregistered\s*=\s*await\s+_dataService\s*\.\s*GetConfigManagedServersAsync\s*\(", sync);
        Assert.DoesNotMatch(@"\bGetManagedServersAsync\b|\bGetServersAsync\b", sync);
        Assert.Matches(@"\bViewerServerSetSync\s*\.\s*ApplyAsync\s*\(\s*_fleet\s*\.\s*All\s*,\s*registered\s*,", sync);

        /* The config read has nothing (null) unless the seeded check said yes; the load keeps its fallback. */
        var configRead = MethodBody("ViewerDataService.MonitoredServers.cs", "GetConfigManagedServersAsync");
        var nothing = Regex.Match(
            configRead,
            @"if\s*\(\s*!\s*await\s+IsConfigSeededAsync\s*\([^)]*\)\s*\)\s*\{\s*return\s+null\s*;\s*\}");

        Assert.True(nothing.Success, "GetConfigManagedServersAsync no longer returns null when the seeded check says no");
        Assert.True(
            nothing.Index < configRead.IndexOf("ManagedServersSql", StringComparison.Ordinal),
            "GetConfigManagedServersAsync reads the list before it checks the store is seeded");
        Assert.Matches(
            @"await\s+GetConfigManagedServersAsync\s*\([^)]*\)\s*\?\?\s*await\s+GetServersAsync\s*\(",
            MethodBody("ViewerDataService.MonitoredServers.cs", "GetManagedServersAsync"));
    }

    [Fact]
    public void ServerRemovedElsewhere_GetsTheLocalRemoveCleanup_AndItsOpenTabIsLeftAsALocalRemoveLeavesIt()
    {
        var forget = MethodBody("MainWindow.ServerManagement.cs", "ForgetRemovedServer");

        Assert.Matches(@"_serverStore\s*\.\s*SetFavorite\s*\(\s*server\s*\.\s*ServerId\s*,\s*false\s*,", forget);
        Assert.Matches(@"_alertStateService\s*\.\s*RemoveServerState\s*\(\s*server\s*\.\s*ServerId\s*\)", forget);

        /* One cleanup, two callers: the context-menu remove calls it instead of carrying its own copy. */
        var localRemove = MethodBody("MainWindow.ServerManagement.cs", "ServerContextMenu_Remove_Click");

        Assert.Matches(@"\bForgetRemovedServer\s*\(\s*server\s*\)", localRemove);
        Assert.DoesNotMatch(@"\bSetFavorite\b|\bRemoveServerState\b", localRemove);

        var sync = MethodBody("MainWindow.ServerManagement.cs", "SyncServerSetAsync");

        Assert.Matches(@"\bForgetRemovedServer\b", sync);

        /* A local remove leaves the server's open tab where it is, still showing the collected history; a
           remove made elsewhere is handled the same way, so neither closes it. */
        Assert.DoesNotMatch(@"\bCloseServerTab\b", localRemove);
        Assert.DoesNotMatch(@"\bCloseServerTab\b", sync);
    }

    [Fact]
    public void EverySurfaceThatListsServers_ReadsTheFleetTheReloadRebuilds()
    {
        var load = MethodBody("MainWindow.xaml.cs", "LoadServersAsync");

        /* The sidebar and the status bar's "Servers: N". */
        Assert.Matches(@"_fleet\s*\.\s*SetAll\s*\(", load);
        Assert.Matches(@"ServerList\s*\.\s*ItemsSource\s*=\s*_fleet\s*\.\s*Visible\b", load);
        Assert.Matches(@"\bUpdateServerCountText\s*\(\s*\)", load);
        Assert.Matches(
            new Regex(@"\bUpdateServerCountText\s*\(\s*\)\s*=>[^;]*_fleet\s*\.\s*TotalCount\b", RegexOptions.Singleline),
            Stripped("MainWindow.xaml.cs"));

        /* The Overview's total: the fleet's own count, not the cards that happened to load. */
        var overview = MethodBody("MainWindow.xaml.cs", "LoadOverviewAsync");

        Assert.Matches(@"\bservers\s*=\s*_fleet\s*\.\s*All\b", overview);
        Assert.Matches(@"\blist\s*=\s*servers\s*\.\s*ToList\s*\(", overview);
        Assert.Matches(@"totalServerCount\s*:\s*list\s*\.\s*Count\b", overview);

        /* The database state editor lists the servers Settings was handed, and Settings is handed the fleet
           when it opens, so an editor opened after the reload lists the reloaded set. */
        Assert.Matches(
            new Regex(@"new\s+SettingsWindow\s*\([^;]*_fleet\s*\.\s*All\b", RegexOptions.Singleline),
            Stripped("MainWindow.xaml.cs"));

        var settings = Stripped("SettingsWindow.xaml.cs");

        Assert.Matches(@"_servers\s*=\s*servers\b", settings);
        Assert.Matches(@"new\s+DatabaseStateOverridesWindow\s*\(\s*_dataService\s*,\s*_servers\s*\)", settings);
    }

    // ── Behavior: the comparer against a real FleetView, counting the reloads ──────────────────────

    [Fact]
    public async Task UnchangedIdSet_DoesNotReload_InAnyOrder()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));
        var tick = new TickRecorder(fleet, selectedId: 2);

        Assert.False(await tick.RunAsync(Server(2, "SQL02"), Server(1, "SQL01")));
        Assert.False(await tick.RunAsync(Server(1, "SQL01"), Server(2, "SQL02")));

        Assert.Equal(0, tick.Reloads);
        Assert.Empty(tick.Forgotten);
        Assert.Equal(2, tick.SelectedId);

        /* An empty fleet against an empty registry is unchanged too, not a reload on every tick. */
        var empty = new TickRecorder(FleetOf(), selectedId: null);

        Assert.False(await empty.RunAsync());
        Assert.Equal(0, empty.Reloads);
    }

    [Fact]
    public async Task ServerAddedElsewhere_ReloadsOnce_AndShowsInTheSidebarTheStatusBarAndTheOverview_KeepingTheSelection()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));
        var tick = new TickRecorder(fleet, selectedId: 2);
        var registered = new[] { Server(1, "SQL01"), Server(2, "SQL02"), Server(3, "SQL03") };

        Assert.True(await tick.RunAsync(registered));
        Assert.Equal(1, tick.Reloads);
        Assert.Empty(tick.Forgotten);

        /* The sidebar binds Visible; the status bar's "Servers: N" reads TotalCount; the Overview's total and
           the list Settings hands the database state editor read All. */
        Assert.Contains(fleet.Visible.OfType<FleetServerRow>(), row => row.Server.ServerId == 3);
        Assert.Equal(3, fleet.TotalCount);
        Assert.Equal(new[] { 1, 2, 3 }, fleet.All.Select(server => server.ServerId).OrderBy(id => id));

        /* The selection stays on the server it was on, not the first row. */
        Assert.Equal(2, tick.SelectedId);

        /* The next tick sees the set it just loaded and does not reload again. */
        Assert.False(await tick.RunAsync(registered));
        Assert.Equal(1, tick.Reloads);
    }

    [Fact]
    public async Task ServerRemovedElsewhere_IsForgottenLikeALocalRemove_BeforeTheOneReload()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"), Server(3, "SQL03"));
        var tick = new TickRecorder(fleet, selectedId: 1);

        Assert.True(await tick.RunAsync(Server(1, "SQL01"), Server(2, "SQL02")));

        /* The local remove's cleanup runs for the removed server only, and before the reload, as it does in
           the context-menu remove. */
        Assert.Equal(new[] { "forget 3", "reload" }, tick.Steps);
        Assert.Equal(new[] { 3 }, tick.Forgotten);

        Assert.Equal(2, fleet.TotalCount);
        Assert.DoesNotContain(fleet.Visible.OfType<FleetServerRow>(), row => row.Server.ServerId == 3);
        Assert.Equal(1, tick.SelectedId);
    }

    [Fact]
    public async Task NoConfigRead_NoForgetAndNoReload_EvenWhenTheObservedListLacksAConfiguredServer()
    {
        /* Server 3 is configured but has never collected, so the observed list is 1 and 2 only. When the
           seeded check fails (a dead pooled connection after a store restart) the config read reports that
           it has nothing, and the pass does nothing: 3 keeps its pin, its alert state, its sidebar row and
           the selection. The next tick with a working check compares again. */
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"), Server(3, "SQL03"));
        var tick = new TickRecorder(fleet, selectedId: 3);

        Assert.False(await tick.RunReadAsync(null));

        Assert.Equal(0, tick.Reloads);
        Assert.Empty(tick.Forgotten);
        Assert.Equal(3, fleet.TotalCount);
        Assert.Equal(3, tick.SelectedId);
    }

    [Fact]
    public async Task OneServerSwappedForAnother_SameCount_StillReloads()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));
        var tick = new TickRecorder(fleet, selectedId: 1);

        Assert.True(await tick.RunAsync(Server(1, "SQL01"), Server(3, "SQL03")));

        Assert.Equal(1, tick.Reloads);
        Assert.Equal(new[] { 2 }, tick.Forgotten);
        Assert.Equal(new[] { 1, 3 }, fleet.All.Select(server => server.ServerId).OrderBy(id => id));
        Assert.Equal(1, tick.SelectedId);
    }

    private static DarlingServer Server(int id, string name) =>
        new(id, name, name, isEnabled: true, sqlMajorVersion: 16);

    private static FleetView FleetOf(params DarlingServer[] servers)
    {
        var fleet = new FleetView();
        fleet.SetAll(servers);
        return fleet;
    }

    /// <summary>
    /// One <c>SyncServerSetAsync</c> pass without the window: the comparer on the fleet's loaded servers,
    /// with a reload that does what <c>LoadServersAsync(preserveSelection: true)</c> does to the fleet — set
    /// the registry's servers, then restore the selection by server id. Counts every reload and forget.
    /// </summary>
    private sealed class TickRecorder(FleetView fleet, int? selectedId)
    {
        public int? SelectedId { get; private set; } = fleet.ResolveSelection(selectedId)?.Server.ServerId;

        public List<string> Steps { get; } = new();

        public List<int> Forgotten { get; } = new();

        public int Reloads => Steps.Count(step => step == "reload");

        public Task<bool> RunAsync(params DarlingServer[] registered) => RunReadAsync(registered);

        /// <summary>A pass over one config read; null is the read that had nothing to report.</summary>
        public Task<bool> RunReadAsync(IReadOnlyList<DarlingServer>? registered) =>
            ViewerServerSetSync.ApplyAsync(
                fleet.All,
                registered,
                server =>
                {
                    Forgotten.Add(server.ServerId);
                    Steps.Add($"forget {server.ServerId}");
                },
                () =>
                {
                    Steps.Add("reload");
                    fleet.SetAll(registered!);
                    SelectedId = fleet.ResolveSelection(SelectedId)?.Server.ServerId;
                    return Task.CompletedTask;
                });
    }

    // ── Source helpers ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// One named method's brace-balanced body from a viewer source file, cut AFTER
    /// <see cref="CSharpSourceWalker"/> has blanked comments and literals, so prose can neither unbalance
    /// the cut nor read as a call.
    /// </summary>
    private static string MethodBody(string file, string name)
    {
        var code = Stripped(file);
        var signature = Regex.Match(code, @"\b(?:void|Task(?:<[^;{}()=]*?>)?)\s+" + Regex.Escape(name) + @"\s*\(");

        Assert.True(signature.Success, $"{name} is not declared in {file}");

        /* Past the parameter list before looking for the body brace: a parameter default can carry one. */
        var paramsEnd = ClosingParen(code, signature.Index + signature.Length - 1);
        var open = code.IndexOf('{', paramsEnd);

        Assert.True(open >= 0, $"{name} has no body in {file}");

        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    /// <summary>The index of the parenthesis that closes the one at <paramref name="open"/>.</summary>
    private static int ClosingParen(string code, int open)
    {
        var depth = 0;

        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')' && --depth == 0)
            {
                return i;
            }
        }

        return code.Length;
    }

    private static string Stripped(string file) =>
        CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(ViewerPath(file)));

    private static string ViewerPath(string file)
    {
        var path = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", file);

        Assert.True(File.Exists(path), $"the viewer source is not where this pin looks: {path}");

        return path;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }
}
