/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>What a notification route is scoped to (#5750).</summary>
internal enum RouteScopeKind
{
    AllServers,
    Server,
    Tag
}

/// <summary>One entry of the edit dialog's scope picker. <see cref="Id"/> is the server's hash id (the id the alert path
/// and <c>server_tag_map</c> use) or the tag id; null for "All servers".</summary>
internal sealed record RouteScopeOption(RouteScopeKind Kind, int? Id, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// The window-free logic behind the Notification Routes window and its edit dialog (#5750), kept here so tests can drive it
/// without a window: the scope picker's options, the scope and status text, the per-channel set markers, which metric
/// choices a scope allows, and the keep-or-clear rule for a saved destination.
/// </summary>
internal static class NotificationRouteUi
{
    internal const string AllServersLabel = "All servers";
    internal const string OrphanedServerStatus = NotificationRouteStatusText.OrphanedServer;
    internal const string OrphanedTagStatus = NotificationRouteStatusText.OrphanedTag;
    internal const string UnreadableStatus = "Saved, unreadable";

    /// <summary>The status of a tag-scoped route the loader could not expand because the tag tables could not be read (#5750):
    /// the route matches nothing until a reload succeeds, so it is never shown as "OK". The text is the shared
    /// <see cref="NotificationRouteStatusText.TagDataUnreadable"/>, which the Darling viewer's
    /// <c>ViewerNotificationRouteScope.TagDataUnreadableStatus</c> aliases too.</summary>
    internal const string TagsUnreadableStatus = NotificationRouteStatusText.TagDataUnreadable;

    /// <summary>The same for a server-scoped route the loader could not expand. A failed tag read does not cause it (a server
    /// route's ids come from the server list, as in Darling); only a server list that could not be built does. The text is the
    /// shared <see cref="NotificationRouteStatusText.ScopeDataUnreadable"/>.</summary>
    internal const string ScopeUnreadableStatus = NotificationRouteStatusText.ScopeDataUnreadable;
    internal const string SetMarker = "Set";
    internal const string NotSetMarker = "Not set";
    internal const string UnreadableMarker = "Unreadable";

    /// <summary>The tag separator in a tag path ("Production / East").</summary>
    internal const string PathSeparator = " / ";

    /// <summary>Every tag's path from the root, by tag id. A cycle in the parent map, or a parent that no longer exists,
    /// stops the walk instead of looping.</summary>
    internal static IReadOnlyDictionary<int, string> TagPaths(IEnumerable<ServerTag> tags)
    {
        var byId = new Dictionary<int, ServerTag>();
        foreach (var tag in tags)
        {
            byId[tag.Id] = tag;
        }

        var paths = new Dictionary<int, string>();
        foreach (var tag in byId.Values)
        {
            var parts = new List<string> { tag.Name };
            var seen = new HashSet<int> { tag.Id };
            var parent = tag.ParentId;
            while (parent.HasValue && byId.TryGetValue(parent.Value, out var up) && seen.Add(up.Id))
            {
                parts.Add(up.Name);
                parent = up.ParentId;
            }

            parts.Reverse();
            paths[tag.Id] = string.Join(PathSeparator, parts);
        }

        return paths;
    }

    /// <summary>The scope picker: All servers first, then each configured server (by hash id, showing its display name),
    /// then each tag (showing its path), each group in name order.</summary>
    internal static IReadOnlyList<RouteScopeOption> ScopeOptions(
        IEnumerable<(int Id, string Name)> servers, IEnumerable<ServerTag> tags)
    {
        var options = new List<RouteScopeOption> { new(RouteScopeKind.AllServers, null, AllServersLabel) };
        foreach (var server in servers.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase).ThenBy(s => s.Id))
        {
            options.Add(new RouteScopeOption(RouteScopeKind.Server, server.Id, $"Server: {server.Name}"));
        }

        foreach (var tag in TagPaths(tags).OrderBy(kv => kv.Value, StringComparer.OrdinalIgnoreCase).ThenBy(kv => kv.Key))
        {
            options.Add(new RouteScopeOption(RouteScopeKind.Tag, tag.Key, $"Tag: {tag.Value}"));
        }

        return options;
    }

    /// <summary>The option for a stored route's scope. A scope whose server or tag no longer exists is not in the list, so a
    /// placeholder option is added at the end and returned: editing an orphaned route keeps showing what it names.</summary>
    internal static RouteScopeOption OptionFor(NotificationRoute route, List<RouteScopeOption> options)
    {
        if (route.ScopeServerId is { } serverId)
        {
            return Find(options, RouteScopeKind.Server, serverId, $"Server: (removed, id {serverId})");
        }

        if (route.ScopeTagId is { } tagId)
        {
            return Find(options, RouteScopeKind.Tag, tagId, $"Tag: (deleted, id {tagId})");
        }

        return options[0];
    }

    private static RouteScopeOption Find(List<RouteScopeOption> options, RouteScopeKind kind, int id, string missingLabel)
    {
        var found = options.FirstOrDefault(o => o.Kind == kind && o.Id == id);
        if (found is null)
        {
            found = new RouteScopeOption(kind, id, missingLabel);
            options.Add(found);
        }

        return found;
    }

    /// <summary>The list's Scope column: "All servers", the server's display name, or the tag path. An orphaned route shows
    /// what it still names (the id when the name is gone).</summary>
    internal static string ScopeDisplay(
        NotificationRoute route, IReadOnlyDictionary<int, string> serverNames, IReadOnlyDictionary<int, string> tagPaths)
    {
        if (route.ScopeServerId is { } serverId)
        {
            return serverNames.TryGetValue(serverId, out var name) ? name : $"(server id {serverId})";
        }

        if (route.ScopeTagId is { } tagId)
        {
            return tagPaths.TryGetValue(tagId, out var path) ? path : $"(tag id {tagId})";
        }

        return AllServersLabel;
    }

    /// <summary>The list's Status column. An orphaned route matches nothing, and so does a scoped route the loader could not expand (<see cref="NotificationRoute.ScopeUnexpanded"/>); a channel in <see cref="NotificationRoute.OffChannels"/>
    /// has a saved destination that Credential Manager could not give back, so it is off for the route (#5366).</summary>
    internal static string StatusText(NotificationRoute route)
    {
        var parts = new List<string>();
        if (route.Orphaned)
        {
            parts.Add(route.ScopeServerId.HasValue ? OrphanedServerStatus : OrphanedTagStatus);
        }

        if (route.ScopeUnexpanded)
        {
            parts.Add(route.ScopeTagId.HasValue ? TagsUnreadableStatus : ScopeUnreadableStatus);
        }

        if (route.OffChannels is { Count: > 0 } off)
        {
            parts.Add($"{UnreadableStatus}: {string.Join(", ", off)}");
        }

        if (parts.Count > 0)
        {
            return string.Join("; ", parts);
        }

        return route.Enabled ? "OK" : "Disabled";
    }

    /// <summary>The choice shown for a stored Level 2 route (#5912), which Lite reads but never writes.</summary>
    internal const string AgentActChoice = "Act (set in SQL)";

    /// <summary>The agent-level combo's item for a stored level (#5912). A level the combo does not offer (a Level 2 row, set by raw
    /// SQL) gets a DISABLED item showing it, added to the combo and returned: the dialog neither throws on it (it used to
    /// <c>.First(...)</c>) nor saves it as another level. Lite's store then refuses to save a 2 (Lite is advise-only), so the user
    /// moves the row to a level Lite writes.</summary>
    internal static System.Windows.Controls.ComboBoxItem AgentLevelItemFor(System.Windows.Controls.ItemCollection items, int? level)
    {
        var item = items.Cast<System.Windows.Controls.ComboBoxItem>().FirstOrDefault(i => Equals(i.Tag, level));
        if (item is null)
        {
            item = new System.Windows.Controls.ComboBoxItem
            {
                Content = level == NotificationRoute.AgentLevelAct ? AgentActChoice : $"Level {level}",
                Tag = level,
                IsEnabled = false,
            };
            items.Add(item);
        }

        return item;
    }

    /// <summary>The list's Agent column (#5820): Inherit, Off, Investigate, or Act (#5912, a Level 2 row Lite cannot write but can read) with its hourly and daily limits. The same words as
    /// Darling's viewer.</summary>
    internal static string AgentDisplay(NotificationRoute route) => route.AgentLevel switch
    {
        null => "Inherit",
        NotificationRoute.AgentLevelInvestigate => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"Investigate ({route.AgentMaxPerHour}/h, {route.AgentMaxPerDay}/day)"),
        NotificationRoute.AgentLevelAct => string.Create(System.Globalization.CultureInfo.InvariantCulture, $"Act ({route.AgentMaxPerHour}/h, {route.AgentMaxPerDay}/day)"),
        _ => "Off",
    };

    /// <summary>The set/not-set marker for one channel (a <see cref="NotificationRouter"/> channel name). Never the value.</summary>
    internal static string ChannelMarker(NotificationRoute route, string channel)
    {
        if (route.IsOffFor(channel))
        {
            return UnreadableMarker;
        }

        var set = channel switch
        {
            NotificationRouter.TeamsChannel => !string.IsNullOrWhiteSpace(route.TeamsUrl),
            NotificationRouter.SlackChannel => !string.IsNullOrWhiteSpace(route.SlackUrl),
            NotificationRouter.GenericChannel => !string.IsNullOrWhiteSpace(route.GenericUrl),
            NotificationRouter.PagerDutyChannel => !string.IsNullOrWhiteSpace(route.PagerDutyRoutingKey),
            NotificationRouter.EmailChannel => !string.IsNullOrWhiteSpace(route.SmtpRecipients),
            _ => false
        };
        return set ? SetMarker : NotSetMarker;
    }

    /// <summary>The every-metric match is offered only when a scope is chosen (ruling 4): a fleet-wide route cannot use it.</summary>
    internal static bool WildcardOffered(RouteScopeOption? scope) => scope is { Kind: not RouteScopeKind.AllServers };

    /// <summary>The route's scope, from the picked option. Never both ids.</summary>
    internal static (int? ServerId, int? TagId) ScopeIds(RouteScopeOption? scope) => scope?.Kind switch
    {
        RouteScopeKind.Server => (scope.Id, null),
        RouteScopeKind.Tag => (null, scope.Id),
        _ => (null, null)
    };

    /// <summary>The text a secret box starts with: nothing for a channel with no saved destination, else a fixed mask. The
    /// saved value is never put in a box.</summary>
    internal const string KeepMask = "••••••••";

    internal static string SecretBoxInitialText(string? stored, bool unreadable = false) =>
        string.IsNullOrWhiteSpace(stored) && !unreadable ? "" : KeepMask;

    /// <summary>True when a channel's saved destination could not be read (<paramref name="unreadable"/>, empty
    /// <paramref name="stored"/>) and its box still holds the mask: the user left it alone. The route then names the channel in
    /// <see cref="NotificationRoute.OffChannels"/> and the store keeps the has-value flag, so the channel stays off for the
    /// route (#5366) rather than falling through to the fleet-wide route or the app-wide URL. Clearing the box is the explicit
    /// remove; typing replaces it (#5750).</summary>
    internal static bool KeepsUnreadable(string? boxText, string? stored, bool unreadable) =>
        unreadable && string.IsNullOrWhiteSpace(stored) && (boxText ?? "").Trim() == KeepMask;

    /// <summary>
    /// What to save for one destination box. <c>UpdateAsync</c> replaces a route's destinations whole, and an empty value
    /// removes that channel's credential, so "keep" means passing the saved value back in:
    /// <list type="bullet">
    /// <item>the box still holds the mask: keep <paramref name="stored"/>;</item>
    /// <item>the box is empty: remove the destination;</item>
    /// <item>anything else: that is the new value (a mask the user typed over keeps only what follows it).</item>
    /// </list>
    /// A channel whose saved destination was unreadable has an empty <paramref name="stored"/>, so this returns empty for it;
    /// <see cref="KeepsUnreadable"/> is what tells the store to keep that channel's flag instead of removing it.
    /// </summary>
    internal static string ResolveSecret(string? boxText, string? stored)
    {
        var text = (boxText ?? "").Trim();
        if (text.Length == 0)
        {
            return "";
        }

        if (text == KeepMask)
        {
            return stored?.Trim() ?? "";
        }

        if (text.StartsWith(KeepMask, StringComparison.Ordinal))
        {
            text = text[KeepMask.Length..].Trim();
        }

        return text;
    }

    /// <summary>The store's <see cref="ArgumentException"/> message without the "(Parameter 'route')" suffix the runtime appends.</summary>
    internal static string InlineMessage(ArgumentException ex)
    {
        var message = ex.Message;
        if (!string.IsNullOrEmpty(ex.ParamName))
        {
            message = message.Replace($" (Parameter '{ex.ParamName}')", "", StringComparison.Ordinal);
        }

        return message;
    }
}

/// <summary>One line of the Notification Routes list. Holds the route (with its destinations, in memory only) and the display
/// text built from it; the grid binds the text, never a destination.</summary>
internal sealed class NotificationRouteRow
{
    public NotificationRouteRow(
        NotificationRoute route, IReadOnlyDictionary<int, string> serverNames, IReadOnlyDictionary<int, string> tagPaths)
    {
        Route = route;
        Enabled = route.Enabled;
        ScopeDisplay = NotificationRouteUi.ScopeDisplay(route, serverNames, tagPaths);
        TeamsMarker = NotificationRouteUi.ChannelMarker(route, NotificationRouter.TeamsChannel);
        SlackMarker = NotificationRouteUi.ChannelMarker(route, NotificationRouter.SlackChannel);
        GenericMarker = NotificationRouteUi.ChannelMarker(route, NotificationRouter.GenericChannel);
        PagerDutyMarker = NotificationRouteUi.ChannelMarker(route, NotificationRouter.PagerDutyChannel);
        EmailMarker = NotificationRouteUi.ChannelMarker(route, NotificationRouter.EmailChannel);
        AgentDisplay = NotificationRouteUi.AgentDisplay(route);
        Status = NotificationRouteUi.StatusText(route);
    }

    public NotificationRoute Route { get; }
    public int RouteId => Route.RouteId;
    public string MetricMatch => Route.MetricMatch;
    public string ScopeDisplay { get; }
    public string TeamsMarker { get; }
    public string SlackMarker { get; }
    public string GenericMarker { get; }
    public string PagerDutyMarker { get; }
    public string EmailMarker { get; }
    public string AgentDisplay { get; }
    public string Status { get; }

    /// <summary>Two-way bound to the grid's checkbox; put back by the window when a write is refused or fails.</summary>
    public bool Enabled { get; set; }
}
