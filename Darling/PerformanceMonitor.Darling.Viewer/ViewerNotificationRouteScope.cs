/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What a notification route is scoped to (#5750).</summary>
public enum RouteScopeKind
{
    AllServers,
    Server,
    Tag,
}

/// <summary>One entry of the edit dialog's scope picker. <see cref="Id"/> is the registry <c>server_id</c> or the tag id; null
/// for "All servers".</summary>
public sealed record RouteScopeOption(RouteScopeKind Kind, int? Id, string Label)
{
    public override string ToString() => Label;
}

/// <summary>
/// The three inputs the shared <see cref="NotificationRouteScopeExpander"/> needs, as the viewer reads them through its own
/// connection (#5750): the registry servers (id and display name), the tag tree and the server-to-tag map. The service reads the same
/// three tables (<c>StoreConfigProvider.ReadRouteScopeInputsAsync</c>); the viewer cannot call that, because it does not reference
/// the service project. <see cref="TagsReadable"/> is false when the tag tables could not be read, and the status column then says
/// so for a tag route instead of calling it orphaned or OK.
/// </summary>
public sealed record ViewerRouteScopeInputs(
    IReadOnlyList<(int Id, string Name)> Servers,
    IReadOnlyList<DarlingTag> Tags,
    IReadOnlyDictionary<int, IReadOnlyCollection<int>> ServerTags,
    bool TagsReadable)
{
    public IReadOnlyDictionary<int, int?> TagParents => Tags.ToDictionary(t => t.Id, t => t.ParentId);
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Reads the expander's three inputs through the viewer's connection (#5750). A failed server read propagates, as the
    /// service's does. A failed tag read is caught and reported as <see cref="ViewerRouteScopeInputs.TagsReadable"/> = false, so a
    /// tag route is shown as not active instead of being called orphaned for a transient failure.
    /// </summary>
    public async Task<ViewerRouteScopeInputs> GetNotificationRouteScopeInputsAsync(CancellationToken cancellationToken = default)
    {
        var servers = (await GetMonitoredServersAsync(cancellationToken)).Select(s => (s.ServerId, s.Name)).ToList();
        try
        {
            var tags = await GetServerTagsAsync(cancellationToken);
            var assignments = await GetServerTagAssignmentsAsync(cancellationToken);
            var serverTags = assignments
                .GroupBy(a => a.ServerId)
                .ToDictionary(g => g.Key, g => (IReadOnlyCollection<int>)g.Select(a => a.TagId).Distinct().ToList());
            return new ViewerRouteScopeInputs(servers, tags, serverTags, true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new ViewerRouteScopeInputs(servers, Array.Empty<DarlingTag>(), new Dictionary<int, IReadOnlyCollection<int>>(), false);
        }
    }
}

/// <summary>
/// The window-free logic behind the viewer's Notification Routes window and edit dialog (#5750), the twin of Lite's
/// <c>NotificationRouteUi</c>: the scope picker's options, the Scope and Status text, and the rule that the every-metric match needs a
/// scope. The words match Lite's, so the two products read the same.
/// </summary>
public static class ViewerNotificationRouteScope
{
    public const string AllServersLabel = "All servers";
    public const string OrphanedServerStatus = "Orphaned: server removed";
    public const string OrphanedTagStatus = "Orphaned: tag deleted";
    public const string TagDataUnreadableStatus = "Not active: tag data unreadable";
    public const string ScopeDataUnreadableStatus = "Not active: scope data unreadable";
    public const string DisabledStatus = "Disabled";
    public const string OkStatus = "OK";

    /// <summary>The label of the every-metric choice in the dialog's Matches list.</summary>
    public const string WildcardChoiceLabel = "Every metric (*)";

    /// <summary>The tag separator in a tag path ("Production / East").</summary>
    public const string PathSeparator = " / ";

    /// <summary>Every tag's path from the root, by tag id. A cycle in the parent map, or a parent that no longer exists, stops the
    /// walk instead of looping.</summary>
    public static IReadOnlyDictionary<int, string> TagPaths(IEnumerable<DarlingTag> tags)
    {
        var byId = new Dictionary<int, DarlingTag>();
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

    /// <summary>The scope picker: All servers first, then each registry server (by <c>server_id</c>, showing its display name), then
    /// each tag (showing its path), each group in name order.</summary>
    public static List<RouteScopeOption> ScopeOptions(ViewerRouteScopeInputs? inputs)
    {
        var options = new List<RouteScopeOption> { new(RouteScopeKind.AllServers, null, AllServersLabel) };
        if (inputs is null)
        {
            return options;
        }

        foreach (var server in inputs.Servers.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase).ThenBy(s => s.Id))
        {
            options.Add(new RouteScopeOption(RouteScopeKind.Server, server.Id, $"Server: {server.Name}"));
        }

        foreach (var tag in TagPaths(inputs.Tags).OrderBy(kv => kv.Value, StringComparer.OrdinalIgnoreCase).ThenBy(kv => kv.Key))
        {
            options.Add(new RouteScopeOption(RouteScopeKind.Tag, tag.Key, $"Tag: {tag.Value}"));
        }

        return options;
    }

    /// <summary>The option for a stored route's scope. A scope whose server or tag no longer exists is not in the list, so a
    /// placeholder option is added at the end and returned: editing an orphaned route keeps showing what it names.</summary>
    public static RouteScopeOption OptionFor(NotificationRouteRow route, List<RouteScopeOption> options)
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

    /// <summary>The every-metric match is offered only while a scope is chosen: a fleet-wide route cannot use it (the V181 CHECK).</summary>
    public static bool WildcardOffered(RouteScopeOption? scope) => scope is { Kind: not RouteScopeKind.AllServers };

    /// <summary>The route's scope ids from the picked option. Never both.</summary>
    public static (int? ServerId, int? TagId) ScopeIds(RouteScopeOption? scope) => scope?.Kind switch
    {
        RouteScopeKind.Server => (scope.Id, null),
        RouteScopeKind.Tag => (null, scope.Id),
        _ => (null, null),
    };

    /// <summary>The Scope column: "All servers", the server's display name or the tag path. A route whose server or tag is gone shows
    /// the id it still names.</summary>
    public static string ScopeText(NotificationRouteRow route, ViewerRouteScopeInputs? inputs)
    {
        if (route.ScopeServerId is { } serverId)
        {
            var name = inputs?.Servers.Where(s => s.Id == serverId).Select(s => s.Name).FirstOrDefault();
            return name ?? $"(server id {serverId})";
        }

        if (route.ScopeTagId is { } tagId)
        {
            return inputs is not null && TagPaths(inputs.Tags).TryGetValue(tagId, out var path) ? path : $"(tag id {tagId})";
        }

        return AllServersLabel;
    }

    /// <summary>
    /// The Status column. Orphan status comes from the shared <see cref="NotificationRouteScopeExpander"/> on the inputs dispatch
    /// uses: a route whose server or tag no longer exists matches nothing. A tag route whose tag data could not be read, or any
    /// scoped route when the inputs could not be read at all, is not active and never reads "OK" (dispatch matches nothing for it
    /// either, and no one can tell it is orphaned). <paramref name="expanded"/> is the expander's output for this route.
    /// </summary>
    public static string StatusText(NotificationRouteRow route, NotificationRoute? expanded, ViewerRouteScopeInputs? inputs)
    {
        var scoped = route.ScopeServerId.HasValue || route.ScopeTagId.HasValue;
        if (scoped && inputs is null)
        {
            return ScopeDataUnreadableStatus;
        }

        if (route.ScopeTagId.HasValue && !route.ScopeServerId.HasValue && inputs is { TagsReadable: false })
        {
            return TagDataUnreadableStatus;
        }

        if (expanded is { Orphaned: true })
        {
            return route.ScopeServerId.HasValue ? OrphanedServerStatus : OrphanedTagStatus;
        }

        return route.Enabled ? OkStatus : DisabledStatus;
    }

    /// <summary>Fills the Scope and Status display members of every row, running the shared expander once over all of them.
    /// <paramref name="inputs"/> null means the scope inputs could not be read.</summary>
    public static void Annotate(IReadOnlyList<NotificationRouteRow> rows, ViewerRouteScopeInputs? inputs)
    {
        IReadOnlyList<NotificationRoute>? expanded = null;
        if (inputs is not null)
        {
            var routes = rows.Select(r => new NotificationRoute(r.RouteId, r.MetricMatch, "", "", "", "", r.SmtpRecipients, r.Enabled)
            {
                ScopeServerId = r.ScopeServerId,
                ScopeTagId = r.ScopeTagId,
            }).ToList();
            var known = inputs.Servers.Select(s => s.Id).ToList();
            expanded = NotificationRouteScopeExpander.Expand(routes, known, inputs.TagParents, inputs.ServerTags);
        }

        for (var i = 0; i < rows.Count; i++)
        {
            rows[i].ScopeDisplay = ScopeText(rows[i], inputs);
            rows[i].StatusDisplay = StatusText(rows[i], expanded?[i], inputs);
        }
    }
}
