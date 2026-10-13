/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5750, the Lite Notification Routes window and edit dialog. The logic that decides what they show and save lives in
/// <see cref="NotificationRouteUi"/>, so it is tested here without a window: the scope picker, the tag path, the status and
/// marker text, which metric choices a scope allows, and the keep, replace or remove rule for a saved destination. Source
/// pins hold the pending-restore refusal on every write path. In the <c>app-alert-statics</c> collection because the one
/// round-trip test writes through the store, which invalidates the static route cache.
/// </summary>
[Collection("app-alert-statics")]
public sealed class NotificationRouteUiTests : IDisposable
{
    private const string TeamsUrl = "https://teams.r5-secret.test/hook/abc123";
    private const string SlackUrl = "https://hooks.slack.r5-secret.test/T0/B0/xyz";
    private const string GenericUrl = "https://generic.r5-secret.test/in";
    private const string PagerKey = "R5PAGERDUTYROUTINGKEY0123456789ab";

    private const string Window = "Lite/Windows/NotificationRoutesWindow.xaml.cs";
    private const string Dialog = "Lite/Windows/NotificationRouteEditDialog.xaml.cs";

    private readonly List<string> _tempDirs = new();

    public void Dispose()
    {
        NotificationRouteCache.Configure(null);
        foreach (var dir in _tempDirs)
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
            catch (IOException) { /* best-effort cleanup */ }
            catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
        }
    }

    private static readonly IReadOnlyDictionary<int, string> NoNames = new Dictionary<int, string>();

    private static NotificationRoute Route(string match = "performance", int? server = null, int? tag = null,
        string teams = "", string slack = "", string generic = "", string pager = "", string smtp = "", bool enabled = true) =>
        new(1, match, teams, slack, generic, pager, smtp, enabled) { ScopeServerId = server, ScopeTagId = tag };

    private static List<ServerTag> Tags() => new()
    {
        new ServerTag(1, "Production", null, 0, null),
        new ServerTag(2, "East", 1, 0, null),
        new ServerTag(3, "Dev", null, 0, null),
    };

    // ---- scope picker -------------------------------------------------------------------------

    [Fact]
    public void ScopeOptions_AreAllServers_ThenServersByDisplayName_ThenTagPaths()
    {
        var options = NotificationRouteUi.ScopeOptions(
            new[] { (Id: 7102, Name: "zeta"), (Id: 7101, Name: "Alpha") }, Tags());

        Assert.Equal(
            new[] { "All servers", "Server: Alpha", "Server: zeta", "Tag: Dev", "Tag: Production", "Tag: Production / East" },
            options.Select(o => o.Label).ToArray());
        Assert.Equal(RouteScopeKind.AllServers, options[0].Kind);
        Assert.Null(options[0].Id);
        Assert.Equal(7101, options[1].Id);
        Assert.Equal(RouteScopeKind.Tag, options[^1].Kind);
        Assert.Equal(2, options[^1].Id);
    }

    [Fact]
    public void TagPaths_WalkToTheRoot_AndStopAtACycleOrAMissingParent()
    {
        var paths = NotificationRouteUi.TagPaths(new[]
        {
            new ServerTag(1, "A", 2, 0, null),   // 1 -> 2 -> 1: a cycle
            new ServerTag(2, "B", 1, 0, null),
            new ServerTag(3, "Orphan", 99, 0, null), // parent gone
        });

        Assert.Equal("B / A", paths[1]);
        Assert.Equal("A / B", paths[2]);
        Assert.Equal("Orphan", paths[3]);
    }

    [Fact]
    public void StarIsOffered_OnlyWhenAServerOrATagIsChosen()
    {
        var options = NotificationRouteUi.ScopeOptions(new[] { (Id: 7101, Name: "a") }, Tags());
        Assert.False(NotificationRouteUi.WildcardOffered(null));
        Assert.False(NotificationRouteUi.WildcardOffered(options.First(o => o.Kind == RouteScopeKind.AllServers)));
        Assert.True(NotificationRouteUi.WildcardOffered(options.First(o => o.Kind == RouteScopeKind.Server)));
        Assert.True(NotificationRouteUi.WildcardOffered(options.First(o => o.Kind == RouteScopeKind.Tag)));
    }

    [Fact]
    public void ScopeIds_NeverSetBoth()
    {
        Assert.Equal((null, null), NotificationRouteUi.ScopeIds(null));
        Assert.Equal((null, null), NotificationRouteUi.ScopeIds(new RouteScopeOption(RouteScopeKind.AllServers, null, "x")));
        Assert.Equal((7101, null), NotificationRouteUi.ScopeIds(new RouteScopeOption(RouteScopeKind.Server, 7101, "x")));
        Assert.Equal((null, 2), NotificationRouteUi.ScopeIds(new RouteScopeOption(RouteScopeKind.Tag, 2, "x")));
    }

    [Fact]
    public void EditingAnOrphan_KeepsShowingWhatItNames()
    {
        var options = NotificationRouteUi.ScopeOptions(new[] { (Id: 7101, Name: "a") }, Tags()).ToList();
        var count = options.Count;

        var server = NotificationRouteUi.OptionFor(Route(server: 7101), options);
        Assert.Equal("Server: a", server.Label);
        Assert.Equal(count, options.Count);

        var gone = NotificationRouteUi.OptionFor(Route(server: 4242), options);
        Assert.Equal(RouteScopeKind.Server, gone.Kind);
        Assert.Equal(4242, gone.Id);
        Assert.Contains("removed", gone.Label, StringComparison.Ordinal);

        var tagGone = NotificationRouteUi.OptionFor(Route(tag: 77), options);
        Assert.Contains("deleted", tagGone.Label, StringComparison.Ordinal);

        Assert.Equal(RouteScopeKind.AllServers, NotificationRouteUi.OptionFor(Route(), options).Kind);
    }

    // ---- list text ----------------------------------------------------------------------------

    [Fact]
    public void ScopeDisplay_IsAllServers_TheDisplayName_OrTheTagPath()
    {
        var names = new Dictionary<int, string> { [7101] = "Prod SQL 01" };
        var paths = NotificationRouteUi.TagPaths(Tags());
        Assert.Equal("All servers", NotificationRouteUi.ScopeDisplay(Route(), names, paths));
        Assert.Equal("Prod SQL 01", NotificationRouteUi.ScopeDisplay(Route(server: 7101), names, paths));
        Assert.Equal("Production / East", NotificationRouteUi.ScopeDisplay(Route(tag: 2), names, paths));
        Assert.Equal("(server id 9)", NotificationRouteUi.ScopeDisplay(Route(server: 9), names, paths));
        Assert.Equal("(tag id 9)", NotificationRouteUi.ScopeDisplay(Route(tag: 9), names, paths));
    }

    [Fact]
    public void Status_NamesAnOrphan_ByWhatWasRemoved()
    {
        /* Through the real expander, so the Orphaned flag is the one the store's loader sets. */
        var expanded = NotificationRouteScopeExpander.Expand(
            new[] { Route(server: 4242), Route(tag: 77), Route(server: 7101) },
            new[] { 7101 },
            new Dictionary<int, int?>(),
            new Dictionary<int, IReadOnlyCollection<int>>());

        Assert.Equal("Orphaned: server removed", NotificationRouteUi.StatusText(expanded[0]));
        Assert.Equal("Orphaned: tag deleted", NotificationRouteUi.StatusText(expanded[1]));
        Assert.Equal("OK", NotificationRouteUi.StatusText(expanded[2]));
    }

    [Fact]
    public void Status_FlagsASavedButUnreadableChannel_AndDisabled()
    {
        var unreadable = Route(slack: "") with { OffChannels = new[] { NotificationRouter.SlackChannel, NotificationRouter.PagerDutyChannel } };
        Assert.Equal("Saved, unreadable: Slack, PagerDuty", NotificationRouteUi.StatusText(unreadable));
        Assert.Equal("Disabled", NotificationRouteUi.StatusText(Route(enabled: false)));
        Assert.Equal("OK", NotificationRouteUi.StatusText(Route()));

        var both = NotificationRouteScopeExpander.Expand(
            new[] { Route(server: 4242) with { OffChannels = new[] { NotificationRouter.TeamsChannel } } },
            new[] { 7101 }, new Dictionary<int, int?>(), new Dictionary<int, IReadOnlyCollection<int>>())[0];
        Assert.Equal("Orphaned: server removed; Saved, unreadable: Teams", NotificationRouteUi.StatusText(both));
    }

    [Fact]
    public void ChannelMarkers_SaySetNotSetOrUnreadable_AndNeverTheValue()
    {
        var route = Route(teams: TeamsUrl, pager: PagerKey, smtp: "ops@example.test")
            with { OffChannels = new[] { NotificationRouter.SlackChannel } };

        Assert.Equal("Set", NotificationRouteUi.ChannelMarker(route, NotificationRouter.TeamsChannel));
        Assert.Equal("Unreadable", NotificationRouteUi.ChannelMarker(route, NotificationRouter.SlackChannel));
        Assert.Equal("Not set", NotificationRouteUi.ChannelMarker(route, NotificationRouter.GenericChannel));
        Assert.Equal("Set", NotificationRouteUi.ChannelMarker(route, NotificationRouter.PagerDutyChannel));
        Assert.Equal("Set", NotificationRouteUi.ChannelMarker(route, NotificationRouter.EmailChannel));

        var row = new NotificationRouteRow(route, NoNames, NoNames);
        var text = string.Join("|", row.ScopeDisplay, row.TeamsMarker, row.SlackMarker, row.GenericMarker, row.PagerDutyMarker, row.EmailMarker, row.Status, row.MetricMatch);
        Assert.DoesNotContain("r5-secret", text, StringComparison.Ordinal);
        Assert.DoesNotContain(PagerKey, text, StringComparison.Ordinal);
    }

    // ---- keep, replace, remove ----------------------------------------------------------------

    [Fact]
    public void ASavedDestination_IsNeverPutInTheBox_OnlyTheMask()
    {
        Assert.Equal(NotificationRouteUi.KeepMask, NotificationRouteUi.SecretBoxInitialText(TeamsUrl));
        Assert.DoesNotContain("teams", NotificationRouteUi.SecretBoxInitialText(TeamsUrl), StringComparison.OrdinalIgnoreCase);
        Assert.Equal("", NotificationRouteUi.SecretBoxInitialText(""));
        Assert.Equal("", NotificationRouteUi.SecretBoxInitialText(null));
        Assert.Equal("", NotificationRouteUi.SecretBoxInitialText("   "));
    }

    [Theory]
    [InlineData(null, TeamsUrl, TeamsUrl)]            // the mask, untouched: keep
    [InlineData("MASK", TeamsUrl, TeamsUrl)]
    [InlineData("", TeamsUrl, "")]                    // cleared: remove
    [InlineData("   ", TeamsUrl, "")]
    [InlineData("https://new.test/x", TeamsUrl, "https://new.test/x")]       // typed over: replace
    [InlineData(" https://new.test/x ", TeamsUrl, "https://new.test/x")]
    [InlineData("MASKhttps://new.test/x", TeamsUrl, "https://new.test/x")]   // typed after the mask: only what was typed
    [InlineData("MASK", "", "")]                      // the mask on a channel with nothing readable: nothing to keep
    [InlineData("", "", "")]
    [InlineData("https://new.test/x", "", "https://new.test/x")]
    public void ResolveSecret_KeepsReplacesOrRemoves(string? box, string stored, string expected)
    {
        var text = box == null ? NotificationRouteUi.KeepMask : box.Replace("MASK", NotificationRouteUi.KeepMask, StringComparison.Ordinal);
        Assert.Equal(expected, NotificationRouteUi.ResolveSecret(text, stored));
    }

    [Fact]
    public async Task EditingARoute_KeepsTheUntouchedSecret_ReplacesOne_AndClearsOne()
    {
        var dir = Path.Combine(Path.GetTempPath(), "NotificationRouteUiTests_" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        Directory.CreateDirectory(dir);
        using var initializer = new DuckDbInitializer(Path.Combine(dir, "lite.duckdb"));
        await initializer.InitializeAsync();
        var secrets = new Dictionary<string, string>();
        var store = new DuckDbNotificationRouteStore(
            initializer, () => new[] { 7101 },
            key => secrets.TryGetValue(key, out var v) ? v : "",
            (key, value) => { if (string.IsNullOrWhiteSpace(value)) secrets.Remove(key); else secrets[key] = value; });

        var id = await store.InsertAsync(Route(server: 7101, teams: TeamsUrl, slack: SlackUrl, generic: GenericUrl, pager: PagerKey));
        var stored = (await store.GetAsync(id))!;

        /* What the dialog saves: Teams untouched (mask), Slack typed over, Generic cleared, PagerDuty untouched. */
        var edited = stored with
        {
            TeamsUrl = NotificationRouteUi.ResolveSecret(NotificationRouteUi.SecretBoxInitialText(stored.TeamsUrl), stored.TeamsUrl),
            SlackUrl = NotificationRouteUi.ResolveSecret("https://hooks.slack.r5-secret.test/NEW", stored.SlackUrl),
            GenericUrl = NotificationRouteUi.ResolveSecret("", stored.GenericUrl),
            PagerDutyRoutingKey = NotificationRouteUi.ResolveSecret(NotificationRouteUi.SecretBoxInitialText(stored.PagerDutyRoutingKey), stored.PagerDutyRoutingKey)
        };
        Assert.True(await store.UpdateAsync(edited));

        var after = (await store.GetAsync(id))!;
        Assert.Equal(TeamsUrl, after.TeamsUrl);
        Assert.Equal("https://hooks.slack.r5-secret.test/NEW", after.SlackUrl);
        Assert.Equal("", after.GenericUrl);
        Assert.Equal(PagerKey, after.PagerDutyRoutingKey);
        Assert.False(secrets.ContainsKey(DuckDbNotificationRouteStore.SecretKey(id, NotificationRouter.GenericChannel)));
        Assert.Equal("Not set", NotificationRouteUi.ChannelMarker(after, NotificationRouter.GenericChannel));
    }

    [Fact]
    public void TheStoresRefusals_ShowWithoutTheParameterSuffix()
    {
        var both = Assert.Throws<ArgumentException>(() =>
            new DuckDbNotificationRouteStore(null!, () => Array.Empty<int>(), _ => "", (_, _) => { })
                .InsertAsync(Route(server: 1, tag: 2)).GetAwaiter().GetResult());
        Assert.Equal("A notification route is scoped to a server or to a tag, never both.", NotificationRouteUi.InlineMessage(both));

        var star = Assert.Throws<ArgumentException>(() =>
            new DuckDbNotificationRouteStore(null!, () => Array.Empty<int>(), _ => "", (_, _) => { })
                .InsertAsync(Route(match: "*")).GetAwaiter().GetResult());
        Assert.StartsWith("The every-metric match ('*') needs a server or tag scope", NotificationRouteUi.InlineMessage(star), StringComparison.Ordinal);
        Assert.DoesNotContain("(Parameter", NotificationRouteUi.InlineMessage(star), StringComparison.Ordinal);
    }

    // ---- pending-restore refusal and wiring (source pins) --------------------------------------

    [Theory]
    [InlineData("AddRoute_Click", "ShowDialog")]
    [InlineData("EditRoute_Click", "ShowDialog")]
    [InlineData("ToggleRoute_Click", "_store.SetEnabledAsync")]
    [InlineData("DeleteRoute_Click", "_store.DeleteAsync")]
    [InlineData("EnabledCheckBox_Click", "_store.SetEnabledAsync")]
    public void EveryWindowWrite_RefusesWhileARestoreIsPending_BeforeTheStoreCall(string method, string call)
    {
        var body = MethodBody(Window, method);
        var refuse = body.IndexOf("RefuseWhileRestorePending()", StringComparison.Ordinal);
        var at = body.IndexOf(call, StringComparison.Ordinal);
        Assert.True(refuse >= 0 && at > refuse, $"{method}: refuse while a restore is pending before {call}.");
    }

    [Fact]
    public void Delete_ChecksAgainAfterTheConfirmation_AndTheToggleBoxIsPutBackWhenRefused()
    {
        var delete = MethodBody(Window, "DeleteRoute_Click");
        var confirm = delete.IndexOf("MessageBox.Show", StringComparison.Ordinal);
        var call = delete.IndexOf("_store.DeleteAsync", StringComparison.Ordinal);
        Assert.True(confirm >= 0 && call > confirm && delete[confirm..call].Contains("RefuseWhileRestorePending()", StringComparison.Ordinal),
            "a restore can start while the confirmation is open; check again between the confirm and the delete.");

        var click = MethodBody(Window, "EnabledCheckBox_Click");
        var refuse = click.IndexOf("RefuseWhileRestorePending()", StringComparison.Ordinal);
        var ret = click.IndexOf("return", refuse, StringComparison.Ordinal);
        Assert.True(refuse >= 0 && ret > refuse, "the refusal branch must return.");
        Assert.Contains("row.Enabled =", click[refuse..ret], StringComparison.Ordinal);
        Assert.Contains("cb.IsChecked =", click[refuse..ret], StringComparison.Ordinal);
    }

    [Fact]
    public void TheWindowsRefusal_IsTheStoresOwnTable()
    {
        var source = Lite.Tests.ParitySource.ReadFile(Window);
        Assert.Contains("RestoreTable = \"notification_routes\"", source, StringComparison.Ordinal);
        Assert.Contains("PendingRestoreNotice.Refuse(RestoreTable, this)", source, StringComparison.Ordinal);
        Assert.Equal("notification_routes", DuckDbNotificationRouteStore.TableName);
    }

    [Fact]
    public void TheDialogRefuses_BeforeItSaves_AndShowsTheStoresMessageInline()
    {
        var body = MethodBody(Dialog, "Save_Click");
        var refuse = body.IndexOf("PendingRestoreNotice.Refuse(RestoreTable", StringComparison.Ordinal);
        var save = body.IndexOf("_save(", StringComparison.Ordinal);
        Assert.True(refuse >= 0 && save > refuse, "Save_Click must refuse while a restore is pending before it saves.");
        Assert.Contains("catch (ArgumentException", body, StringComparison.Ordinal);
        Assert.Contains("ShowInline(NotificationRouteUi.InlineMessage(ex))", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDialogPutsNoSavedDestinationInABox()
    {
        var source = Lite.Tests.ParitySource.ReadFile(Dialog);
        Assert.DoesNotMatch(new Regex(@"UrlBox\.(Password|Text)\s*=\s*existing\.", RegexOptions.None), source);
        Assert.DoesNotMatch(new Regex(@"KeyBox\.(Password|Text)\s*=\s*existing\.", RegexOptions.None), source);
        var xaml = Lite.Tests.ParitySource.ReadFile("Lite/Windows/NotificationRouteEditDialog.xaml");
        foreach (var box in new[] { "TeamsUrlBox", "SlackUrlBox", "GenericUrlBox", "PagerDutyKeyBox" })
        {
            Assert.Matches(new Regex(@"<PasswordBox[^>]*x:Name=""" + box + @""""), xaml);
        }
    }

    [Fact]
    public void TheSettingsWindow_HasTheButton_AndMainWindowPassesTheStore()
    {
        Assert.Contains("ManageNotificationRoutesButton_Click", Lite.Tests.ParitySource.ReadFile("Lite/Windows/SettingsWindow.xaml"), StringComparison.Ordinal);
        Assert.Contains("new NotificationRoutesWindow(", Lite.Tests.ParitySource.ReadFile("Lite/Windows/SettingsWindow.xaml.cs"), StringComparison.Ordinal);
        Assert.Contains("NotificationRouteStore, _dataService", Lite.Tests.ParitySource.ReadFile("Lite/MainWindow.xaml.cs"), StringComparison.Ordinal);
    }

    private static string MethodBody(string file, string method)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(Lite.Tests.ParitySource.ReadFile(file));
        var found = Regex.Matches(source, @"\b(?:void|Task(?:<[^>]*>)?|bool|string|int)\s+" + Regex.Escape(method) + @"\s*\(");
        Assert.True(found.Count == 1, $"{file}: {found.Count} declarations of {method}; the pin's anchor moved.");
        var open = source.IndexOf('{', found[0].Index);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}' && --depth == 0) return source[open..i];
        }

        throw new InvalidOperationException($"{file}: unbalanced braces in {method}.");
    }
}
