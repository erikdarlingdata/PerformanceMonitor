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
/// The web's Custom Views folders, personal views and owner fields (#6214): the shipped <c>view-folders.js</c>,
/// <c>view-sidebar.js</c>, <c>view-meta-fields.js</c> and the per-browser collapse state in <c>viewer-local.js</c>, run under
/// Node (<c>view-folders-harness.mjs</c>) against a fake DOM and a stubbed <c>localStorage</c>. Skipped when Node is not installed.
/// </summary>
public sealed class CustomViewFoldersWebTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "view-folders-harness.mjs"));
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
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the custom view folders harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the custom view folders harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void TheSidebar_ListsMyViewsFirst_ThenFoldersAToZ_ThenTopLevelViews_AndHidesEmptyGroups()
    {
        if (!TryRun("order", out var r)) return;

        var folderAndTopLevel = new[]
        {
            "G:Dev(1)", "V:Blocking@1", "G:Ops(2)", "V:CPU@1", "V:Waits@1", "G:ops(1)", "V:apple@1",
            "V:Alpha top level@0", "V:Old team view@0", "V:Zeta top level@0",
        };

        /* Alice's own personal views sit in a group of their own, whatever folder they carry; Bob's is not hers to see. */
        Assert.Equal(new[] { "G:My views(2)", "V:Another scratch@1", "V:My scratch@1" }.Concat(folderAndTopLevel), Strings(r.GetProperty("alice")));
        Assert.Equal(new[] { "G:My views(1)", "V:Bob private@1" }.Concat(folderAndTopLevel), Strings(r.GetProperty("bob")));

        /* The shared-token session has no identity, so no My views group, and never shows a personal view it was handed. */
        Assert.Equal(folderAndTopLevel, Strings(r.GetProperty("noIdentity")));

        /* The page never compares names: a personal view that carries my display name but is not `mine` is not in My views. */
        Assert.Empty(Strings(r.GetProperty("sameNameNotMine")));

        /* A collapsed group keeps its header (with its count) and hides its views. */
        Assert.Contains("G:Ops(2,c)", Strings(r.GetProperty("aliceOpsCollapsed")));
        Assert.DoesNotContain("V:CPU@1", Strings(r.GetProperty("aliceOpsCollapsed")));
        Assert.Contains("V:Blocking@1", Strings(r.GetProperty("aliceOpsCollapsed")));
        Assert.Equal("G:My views(2,c)", Strings(r.GetProperty("aliceMyCollapsed"))[0]);
        Assert.DoesNotContain("V:My scratch@1", Strings(r.GetProperty("aliceMyCollapsed")));

        /* No folders and no personal views: just the top level. Nothing at all: no rows. */
        Assert.Equal(new[] { "V:Alpha top level@0", "V:Zeta top level@0" }, Strings(r.GetProperty("onlyTopLevel")));
        Assert.Empty(Strings(r.GetProperty("empty")));

        /* The editor's folder suggestions are the shared folders only, A-Z: "Ignored" belongs to a personal view. */
        Assert.Equal(new[] { "Dev", "Ops", "ops" }, Strings(r.GetProperty("folders")));
    }

    [Fact]
    public void TheCollapsedFolders_ArePerBrowser_SurviveAReload_AndIgnoreIdsThatAreNotGroups()
    {
        if (!TryRun("collapse", out var r)) return;

        Assert.Equal(new[] { "folder:Ops", "my" }, Strings(r.GetProperty("storedAfterTwo").GetProperty("collapsed")));
        Assert.True(r.GetProperty("opsCollapsed").GetBoolean());
        Assert.False(r.GetProperty("devCollapsed").GetBoolean());
        Assert.True(r.GetProperty("opsAfterReload").GetBoolean());
        Assert.False(r.GetProperty("opsAfterToggleBack").GetBoolean());

        /* A tag id, an empty or over-long folder, a control character and a number are never stored. */
        Assert.Equal(new[] { "my" }, Strings(r.GetProperty("storedAfterJunk").GetProperty("collapsed")));

        /* A hand-edited value is cleaned when read, and the server-tag groups' own key is not touched. */
        Assert.Equal(new[] { true, true, false }, r.GetProperty("cleaned").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.True(r.GetProperty("serverGroupsKey").GetBoolean());

        /* The collapse set sits in sessionStorage (it goes with the tab): localStorage never holds it. */
        Assert.Empty(r.GetProperty("localStorageKeys").EnumerateArray());
    }

    [Fact]
    public void ThePaintedSidebar_TogglesAFolderFromItsHeader_RemembersIt_AndKeepsNamesAsText()
    {
        if (!TryRun("paint", out var r)) return;

        var first = Strings(r.GetProperty("first"));
        Assert.Equal("G:My views(2)true", first[0]);
        Assert.Contains("V:Waits*#/view/4", first);
        Assert.Contains("V:Blocking#/notebook/5", first);
        Assert.Equal("V:＋ New view#/view/new", first[^2]);
        Assert.Equal("V:＋ New notebook#/notebook/new", first[^1]);

        /* Clicking the Ops header collapses it (aria-expanded false, its two views gone) and stores the choice by folder name. */
        var after = Strings(r.GetProperty("afterCollapse"));
        Assert.Contains("G:Ops(2)false", after);
        Assert.DoesNotContain(after, line => line.StartsWith("V:CPU", StringComparison.Ordinal) || line.StartsWith("V:Waits", StringComparison.Ordinal));
        Assert.Equal(new[] { "folder:Ops" }, Strings(r.GetProperty("stored").GetProperty("collapsed")));
        Assert.Equal(4, r.GetProperty("headerCount").GetInt32());

        /* Read-only shared-token session: the shared view only, the personal one is not listed and there are no New links. */
        Assert.Equal(new[] { "V:Zeta top level#/view/1" }, Strings(r.GetProperty("tokenReadOnly")));
        Assert.Equal("No views yet", r.GetProperty("emptyText").GetString());

        /* Markup in a name or folder is text, never elements. */
        Assert.Contains("<img src=x onerror=alert(1)>", Strings(r.GetProperty("hostile")));
    }

    [Fact]
    public void TheEditorFields_OfferFolderAndPersonalOnlyWhereTheStoreWouldAllowThem_AndSayWhy()
    {
        if (!TryRun("fields", out var r)) return;

        /* A new view with an identity: both fields open, the datalist holds the existing shared folders. */
        var fresh = r.GetProperty("newWithIdentity");
        Assert.False(fresh.GetProperty("personalDisabled").GetBoolean());
        Assert.Equal(new[] { "Dev", "Ops", "ops" }, Strings(fresh.GetProperty("options")));
        Assert.True(fresh.GetProperty("listLinked").GetBoolean());
        Assert.Equal("100", fresh.GetProperty("maxlength").GetString());

        /* No identity: the box is disabled and the page says why. */
        var noIdentity = r.GetProperty("newNoIdentity");
        Assert.True(noIdentity.GetProperty("personalDisabled").GetBoolean());
        Assert.Contains("Personal views need a sign-in that can own them", noIdentity.GetProperty("text").GetString());
        Assert.Contains("open /auth/logout, then sign in again", noIdentity.GetProperty("text").GetString());

        /* A shared view: only its creator can make it personal; one with no recorded creator can be made personal by nobody. */
        Assert.False(r.GetProperty("ownShared").GetProperty("personalDisabled").GetBoolean());
        Assert.Contains("Created by alice · changed by alice", r.GetProperty("ownShared").GetProperty("text").GetString());
        Assert.True(r.GetProperty("otherShared").GetProperty("personalDisabled").GetBoolean());
        Assert.Contains("Only the person who created this view can make it personal", r.GetProperty("otherShared").GetProperty("text").GetString());
        var legacy = r.GetProperty("legacyShared");
        Assert.True(legacy.GetProperty("personalDisabled").GetBoolean());
        Assert.Contains("has no recorded creator", legacy.GetProperty("text").GetString());
        Assert.Contains("Created by unknown", legacy.GetProperty("text").GetString());
        Assert.DoesNotContain("changed by", legacy.GetProperty("text").GetString());

        /* My own personal view: editable, ticked, and its folder box is locked while it is personal. */
        var own = r.GetProperty("ownPersonal");
        Assert.True(own.GetProperty("personalChecked").GetBoolean());
        Assert.False(own.GetProperty("personalDisabled").GetBoolean());
        Assert.False(own.GetProperty("readOnly").GetBoolean());
        Assert.True(own.GetProperty("folderDisabled").GetBoolean());

        /* Somebody else's personal view: read-only, whoever the viewer is (an identity or not), and the page names the owner. */
        foreach (var name in new[] { "othersPersonal", "othersPersonalNoIdentity" })
        {
            var others = r.GetProperty(name);
            Assert.True(others.GetProperty("readOnly").GetBoolean());
            Assert.True(others.GetProperty("personalDisabled").GetBoolean());
            Assert.True(others.GetProperty("folderDisabled").GetBoolean());
            Assert.Contains("Personal view of bob. It is read-only for you.", others.GetProperty("text").GetString());
        }

        /* Ticking Personal updates the model, locks the folder box, and tells the editor to recompute Save. Unticking undoes it. */
        var ticked = r.GetProperty("afterTicking");
        Assert.True(ticked.GetProperty("meta").GetProperty("personal").GetBoolean());
        Assert.Equal("Dev", ticked.GetProperty("meta").GetProperty("folder").GetString());
        Assert.True(ticked.GetProperty("folderDisabled").GetBoolean());
        Assert.Equal(2, ticked.GetProperty("changed").GetInt32());
        Assert.False(r.GetProperty("afterUnticking").GetProperty("meta").GetProperty("personal").GetBoolean());
        Assert.False(r.GetProperty("afterUnticking").GetProperty("folderDisabled").GetBoolean());
    }

    [Fact]
    public void TheFolderBox_RefusesInvisibleCharactersAndTheReservedName_LikeTheStore_AndBlocksSave()
    {
        if (!TryRun("fields", out var r)) return;

        var block = r.GetProperty("folderBlock");
        foreach (var name in new[] { "hidden", "zeroWidth", "reserved", "tooLong" })
        {
            Assert.True(block.GetProperty(name).GetBoolean(), name);
        }

        Assert.False(block.GetProperty("fine").GetBoolean());
        Assert.Contains("reserved", block.GetProperty("note").GetString());
        Assert.False(block.GetProperty("personalIgnoresFolder").GetBoolean());

        /* blank, blank, Ops, then the refusals (the control character is the last of them before the 101-character name), then a
           100-character name that is fine, then the lookalikes (no-break space, em space, variation selector, hangul filler,
           combining grapheme joiner, a tag variation selector), which are refused, then a tab and runs of spaces, which are only
           normalized */
        Assert.Equal(new[] { false, false, false, true, true, true, true, true, true, true, false, true, true, true, true, true, true, false, false },
            r.GetProperty("folderProblems").EnumerateArray().Select(e => e.GetBoolean()).ToArray());

        /* The page sends the name the store keeps: every kind of space becomes one plain space, the ends are trimmed. */
        Assert.Equal(new[] { "Ops Team", "", "x", "" }, r.GetProperty("normalized").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal("My Ops Team", r.GetProperty("saved").GetProperty("folder").GetString());
    }

    [Fact]
    public void SigningOut_ClearsTheCollapsedFolders_FromThisBrowser()
    {
        if (!TryRun("signout", out var r)) return;

        Assert.True(r.GetProperty("before").GetBoolean());
        Assert.False(r.GetProperty("after").GetBoolean());
        Assert.True(r.GetProperty("keyGone").GetBoolean());
        Assert.False(r.GetProperty("afterReload").GetBoolean());
    }

    [Fact]
    public void ASignedOutPage_NeverRepaintsTheOldSessionsLists_AndDropsTheCachedViewList()
    {
        if (!TryRun("expired", out var r)) return;

        /* Before the session ends the list (with the personal views) and the fleet sidebar paint. */
        var before = r.GetProperty("beforeExpiry");
        Assert.Equal(new[] { 2 }, before.GetProperty("views").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Equal(1, before.GetProperty("servers").GetInt32());
        Assert.True(before.GetProperty("cached").GetBoolean());

        /* After: the takeover drops the cache and the folder state, and a later repaint (a poll, a collapse) paints nothing. */
        var after = r.GetProperty("afterExpiry");
        Assert.Equal(1, after.GetProperty("views").GetArrayLength());
        Assert.Equal(1, after.GetProperty("servers").GetInt32());
        Assert.False(after.GetProperty("cached").GetBoolean());
        Assert.Equal(1, after.GetProperty("cleared").GetInt32());

        /* A list read that was already in flight and lands after the sign-out keeps nothing. */
        var late = r.GetProperty("lateRead");
        Assert.Equal(1, late.GetProperty("views").GetArrayLength());
        Assert.False(late.GetProperty("cached").GetBoolean());
    }

    [Fact]
    public void TheSaveBody_TrimsTheFolder_SendsNullForTheTopLevel_AndNeverSendsPersonalTheStoreWouldRefuse()
    {
        if (!TryRun("fields", out var r)) return;

        var save = r.GetProperty("save");
        Assert.Equal("Ops", save.GetProperty("typed").GetProperty("folder").GetString());
        Assert.Equal(JsonValueKind.Null, save.GetProperty("blank").GetProperty("folder").ValueKind);
        Assert.True(save.GetProperty("personal").GetProperty("personal").GetBoolean());
        Assert.False(save.GetProperty("personalNoIdentity").GetProperty("personal").GetBoolean());
        Assert.False(save.GetProperty("personalLegacy").GetProperty("personal").GetBoolean());
        Assert.False(save.GetProperty("personalOthers").GetProperty("personal").GetBoolean());
        Assert.True(save.GetProperty("ownPersonalStays").GetProperty("personal").GetBoolean());

        var owner = Strings(r.GetProperty("owner"));
        Assert.Equal("Created by alice · changed by alice", owner[0]);
        Assert.Equal("Created by unknown", owner[1]);
        Assert.Equal("Created by a · changed by b", owner[2]);
        Assert.Equal("", owner[3]);
    }

    [Fact]
    public void APersonalView_SaysWhoseItIs_AndIsReadOnlyForEveryoneElse()
    {
        if (!TryRun("access", out var r)) return;

        Assert.False(r.GetProperty("shared").GetProperty("readOnly").GetBoolean());
        Assert.Equal("", r.GetProperty("shared").GetProperty("note").GetString());
        Assert.False(r.GetProperty("own").GetProperty("readOnly").GetBoolean());
        Assert.Equal("Personal view of bob", r.GetProperty("others").GetProperty("note").GetString());
        Assert.True(r.GetProperty("others").GetProperty("readOnly").GetBoolean());
        Assert.True(r.GetProperty("noIdentity").GetProperty("readOnly").GetBoolean());
        Assert.Equal("Personal view of unknown", r.GetProperty("legacyPersonal").GetProperty("note").GetString());
    }

    [Fact]
    public void ACreate_AnEdit_AndADelete_EachRefreshTheSidebarList_AndARefusedWriteDoesNot()
    {
        if (!TryRun("write-refresh", out var r)) return;

        Assert.Equal(new[] { "First" }, Strings(r.GetProperty("start")));
        Assert.Equal(new[] { "Brand new", "First" }, Strings(r.GetProperty("afterCreate")));
        Assert.Equal("data", r.GetProperty("createdKind").GetString());
        Assert.Equal("Moved", r.GetProperty("afterEdit").GetProperty("folder").GetString());
        Assert.Equal(new[] { "Brand new" }, Strings(r.GetProperty("afterDelete")));

        /* One refresh for each accepted write, none for the one the service refused. */
        Assert.Equal("error", r.GetProperty("refusedKind").GetString());
        Assert.Equal(3, r.GetProperty("refreshesFromWrites").GetInt32());
    }

    [Fact]
    public void TheViewList_DrawsOnceTheSessionAndListAnswer_WithoutWaitingForTheFleetRead()
    {
        if (!TryRun("direct-load", out var r)) return;

        var pending = r.GetProperty("whileFleetPending");
        Assert.False(pending.GetProperty("loading").GetBoolean());
        Assert.True(pending.GetProperty("hasViews").GetBoolean());
        Assert.False(r.GetProperty("pickerBefore").GetBoolean());

        /* The server picker for the dashboard templates arrives when the fleet answers, and the list stays. */
        Assert.True(r.GetProperty("afterFleet").GetProperty("hasViews").GetBoolean());
        Assert.True(r.GetProperty("afterFleet").GetProperty("picker").GetBoolean());

        /* A fleet answer that lands after the user went to another page draws nothing over it. */
        Assert.Equal("another page", r.GetProperty("leftPage").GetString());
    }
}
