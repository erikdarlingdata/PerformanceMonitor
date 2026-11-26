/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Viewer's web access group administration as MCP tools (#9642): the advanced web access switch, and creating, changing,
/// rotating and deleting a group. The writes run <see cref="WebAccessGroupAdmin"/>, the code the Viewer runs, on the service's own
/// store connection (the <c>mcp</c> role holds no grant on these tables or functions, as with <c>set_server_action</c>). A create and
/// a rotate answer with the new token once: the store keeps only the sealed copy, and no log line or journal row holds the value.
/// The writes are served as <c>add_servers</c> is, only while <c>mcp.allowWrites</c> and <c>mcp.allowServerAdmin</c> are both on, and
/// so is the group list: it reads on the store owner connection, which no reader or agent caller may use.
/// </summary>
[McpServerToolType]
public sealed partial class DarlingMcpWebAccessTools
{
    /// <summary>A test's own ring, for the one test that calls these tools through a built server. Production never sets it, so every call seals with the service's ring.</summary>
    internal static volatile IPasswordKeyRing? RingForTests;

    private static IPasswordKeyRing Ring() => RingForTests ?? DarlingPasswordKey.Current;

    /// <summary>The journal actor of an MCP change: <c>mcp:</c> plus the caller the request filter worked out.</summary>
    internal static string ActorOf() => DarlingMcpServerActionTools.ActorOf();

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_web_access_groups"), Description(
        "Lists the web access groups and whether advanced web access is on: for each group its id, name, scope (tags or all_servers), " +
        "tag_ids, enabled, revision and activation state. No token is ever returned: a token is shown once, by create_web_access_group " +
        "or rotate_web_access_group_token. groups_returned 0 means none exists. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> GetWebAccessGroups(StoreOwnerSource? ownerSource = null, CancellationToken cancellationToken = default) =>
        ListAsync(ownerSource?.Read(), cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "set_advanced_web_access"), Description(
        "Turns advanced web access on or off, the switch the Viewer shows as Advanced web access. While it is off, no web access group " +
        "signs anyone in. Turning it off or on ends every existing group session. Turning it on takes confirm=true, because it lets every " +
        "enabled group sign people in again. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> SetAdvancedWebAccess(
        [Description("True to turn advanced web access on, false to turn it off.")] bool enabled,
        [Description("Must be true to turn it on. Without it nothing changes and the answer names the groups it would bring back.")] bool confirm = false,
        StoreOwnerSource? ownerSource = null, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        SetAdvancedAsync(ownerSource?.Read(), enabled, confirm, ActorOf(), logger, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "create_web_access_group"), Description(
        "Creates a web access group: a name, a scope (tags with one or more tag_ids, or all_servers) and a new token. The ANSWER HOLDS THE " +
        "TOKEN, ONCE: it is sealed to the service's password key and cannot be read back, so keep it where the answer is not logged, " +
        "and use rotate_web_access_group_token if it is lost. The group is enabled and starts signing people in after advanced web " +
        "access is on (see set_advanced_web_access) and the service has activated it. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> CreateWebAccessGroup(
        [Description("The group's name, unique across groups.")] string name,
        [Description("tags or all_servers. An empty tag grant never means all servers.")] string scope,
        [Description("For scope tags, one or more server tag ids (from get_fleet_overview tags). Leave out for all_servers.")] int[]? tag_ids = null,
        StoreOwnerSource? ownerSource = null, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        CreateAsync(ownerSource?.Read(), Ring(), name, scope, tag_ids, ActorOf(), logger, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "update_web_access_group"), Description(
        "Changes a group's scope, tag grants or enabled flag, as the Viewer's group editor does. A group's name does not change. " +
        "Anything left out keeps its current value. Disabling a group ends its sessions. Enabling a group, widening it to all_servers, or adding a tag it did not have takes " +
        "confirm=true. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> UpdateWebAccessGroup(
        [Description("The group's id, or its exact name, as get_web_access_groups shows.")] string group,
        [Description("tags or all_servers. Leave out to keep the current scope.")] string? scope = null,
        [Description("For scope tags, the full list of server tag ids the group may read (replaces the current list). Leave out to keep it.")] int[]? tag_ids = null,
        [Description("True to enable the group, false to disable it. Leave out to keep it.")] bool? enabled = null,
        [Description("Must be true when the change enables a disabled group, widens a group to all_servers or adds a tag it did not have. Without it nothing changes and the answer says what the change gives.")] bool confirm = false,
        StoreOwnerSource? ownerSource = null, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        UpdateAsync(ownerSource?.Read(), group, scope, tag_ids, enabled, confirm, ActorOf(), logger, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "rotate_web_access_group_token"), Description(
        "Replaces a group's token. The group's old token and its existing sessions stop working immediately. The ANSWER HOLDS THE NEW " +
        "TOKEN, ONCE: it cannot be read back. Takes confirm=true, as the Viewer asks before it rotates. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> RotateWebAccessGroupToken(
        [Description("The group's id, or its exact name, as get_web_access_groups shows.")] string group,
        [Description("Must be true. Without it nothing changes and the answer says what a rotate does.")] bool confirm = false,
        StoreOwnerSource? ownerSource = null, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        RotateAsync(ownerSource?.Read(), Ring(), group, confirm, ActorOf(), logger, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "delete_web_access_group"), Description(
        "Deletes a group. Its token and its existing sessions stop working immediately, and the deletion cannot be undone. Takes confirm=true, " +
        "as the Viewer asks before it deletes. Needs mcp.allowWrites and mcp.allowServerAdmin.")]
    public static Task<string> DeleteWebAccessGroup(
        [Description("The group's id, or its exact name, as get_web_access_groups shows.")] string group,
        [Description("Must be true. Without it nothing changes and the answer says what a delete does.")] bool confirm = false,
        StoreOwnerSource? ownerSource = null, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        DeleteAsync(ownerSource?.Read(), group, confirm, ActorOf(), logger, cancellationToken);

    internal static async Task<string> ListAsync(NpgsqlDataSource? owner, CancellationToken ct)
    {
        try
        {
            if (owner is null) return NoOwner();
            var store = new WebAccessGroupStore(owner);
            var groups = await store.ListAsync(ct);
            var advanced = await store.ReadAdvancedSessionEpochAsync(ct) is not null;
            return new JsonObject
            {
                ["advanced_web_access_enabled"] = advanced,
                ["groups_returned"] = groups.Count,
                ["groups"] = new JsonArray(groups.Select(g => (JsonNode)GroupJson(g)).ToArray()),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_web_access_groups", ex);
        }
    }

    internal static async Task<string> SetAdvancedAsync(NpgsqlDataSource? owner, bool enabled, bool confirm, string actor, ILogger? logger, CancellationToken ct)
    {
        const string tool = "set_advanced_web_access";
        try
        {
            if (owner is null) return NoOwner();
            var store = new WebAccessGroupStore(owner);
            /* The store decides the confirm under its row lock and returns the value it held then (#9662). Reading the switch here first would let a
               switch turned off in between be turned back on with no confirm, and the answer would say unchanged with no undo. */
            var prior = await store.SetAdvancedEnabledAsync(enabled, actor, confirm, ct);
            /* No switch row: the store changed nothing, so the answer must not read as a flip and must not offer an undo. */
            if (prior is not { } before) return Refuse("switch_missing", "The advanced web access switch row is missing from the store, so nothing was changed.");
            if (enabled && !before && !confirm)
            {
                /* The store changed nothing. Turning the switch on is what lets a group sign in: every enabled group starts again, so the answer names them. */
                var coming = (await store.ListAsync(ct)).Where(g => g.Enabled).ToList();
                var names = coming.Count == 0
                    ? "no group is enabled, so nobody signs in until one is"
                    : $"{coming.Count} enabled group(s) sign people in again: " +
                      string.Join(", ", coming.Take(10).Select(g => $"'{Clip(g.Name)}' ({ScopeName(g.Scope)})")) + (coming.Count > 10 ? ", and more" : string.Empty);
                return Refuse("confirm_required", $"Turning advanced web access on lets web access groups sign in: {names}. Call again with confirm=true to go ahead. Nothing was changed.");
            }
            if (before != enabled) logger?.LogWarning("MCP advanced web access {Before} -> {After}, caller {Caller}", before, enabled, actor);
            return new JsonObject
            {
                ["status"] = before == enabled ? "unchanged" : "set",
                ["advanced_web_access_enabled"] = enabled,
                ["undo"] = before == enabled ? null : Undo("set_advanced_web_access", before ? new JsonObject { ["enabled"] = true, ["confirm"] = true } : new JsonObject { ["enabled"] = false }),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    internal static async Task<string> CreateAsync(
        NpgsqlDataSource? owner, IPasswordKeyRing? ring, string? name, string? scope, int[]? tagIds, string actor, ILogger? logger, CancellationToken ct,
        NpgsqlDataSource? followUpOwner = null)
    {
        const string tool = "create_web_access_group";
        try
        {
            if (owner is null) return NoOwner();
            if (name is null || !GroupName().IsMatch(name.Trim()))
                return Refuse("invalid", "name must be 1 to 64 letters, digits, dots, underscores, colons or hyphens. Nothing was changed.");
            if (!TryScope(scope, out var parsed)) return Refuse("invalid", "scope must be tags or all_servers. Nothing was changed.");
            var tags = tagIds ?? [];
            if (parsed == WebAccessGroupScope.Tags && tags.Length == 0)
                return Refuse("invalid", "scope tags needs one or more tag_ids. Nothing was changed.");
            if (parsed == WebAccessGroupScope.AllServers && tags.Length > 0)
                return Refuse("invalid", "scope all_servers takes no tag_ids. Nothing was changed.");
            if (SealRefusal(ring) is { } refusal) return refusal;

            var store = new WebAccessGroupStore(owner);
            var token = await Admin(store, ring!, actor).CreateAsync(name, parsed, tags, ct);
            logger?.LogWarning("MCP web access group {GroupId} created, caller {Caller}", token.GroupId, actor);

            /* The group is committed and the token is known only here, so nothing after this line may lose the answer: the group's own
               row and the advanced switch are read best effort, and the answer falls back to what the write itself said. */
            var after = followUpOwner is null ? store : new WebAccessGroupStore(followUpOwner);
            var group = await TryReadGroupAsync(after, token.GroupId);
            var advanced = await TryReadAdvancedAsync(after);
            return new JsonObject
            {
                ["status"] = "created",
                ["group"] = group is null
                    ? new JsonObject { ["group_id"] = token.GroupId.ToString(), ["name"] = name.Trim(), ["scope"] = ScopeName(parsed), ["enabled"] = true }
                    : GroupJson(group),
                ["token"] = token.Plain,
                ["token_note"] = "Shown once. The store keeps only a sealed copy. Rotate the token if it is lost.",
                ["advanced_web_access_enabled"] = advanced,
                ["undo"] = Undo("delete_web_access_group", new JsonObject { ["group"] = token.GroupId.ToString(), ["confirm"] = true }),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    internal static async Task<string> UpdateAsync(
        NpgsqlDataSource? owner, string? groupText, string? scope, int[]? tagIds, bool? enabled, bool confirm, string actor, ILogger? logger, CancellationToken ct,
        NpgsqlDataSource? followUpOwner = null)
    {
        try
        {
            if (owner is null) return NoOwner();
            var store = new WebAccessGroupStore(owner);
            var (group, refusal) = await ResolveAsync(store, groupText, ct);
            if (group is null) return refusal!;

            var newScope = group.Scope;
            if (scope is not null)
            {
                if (!TryScope(scope, out newScope)) return Refuse("invalid", "scope must be tags or all_servers. Nothing was changed.");
            }

            int[] tags;
            if (newScope == WebAccessGroupScope.AllServers)
            {
                if (tagIds is { Length: > 0 }) return Refuse("invalid", "scope all_servers takes no tag_ids. Nothing was changed.");
                tags = [];
            }
            else
            {
                tags = tagIds ?? group.TagIds.ToArray();
                if (tags.Length == 0) return Refuse("invalid", "scope tags needs one or more tag_ids. Nothing was changed.");
            }

            var newEnabled = enabled ?? group.Enabled;
            var widensScope = newScope == WebAccessGroupScope.AllServers && group.Scope != WebAccessGroupScope.AllServers;
            var addedTags = AddedTags(group.Scope, group.TagIds, newScope, tags);
            var enables = newEnabled && !group.Enabled;
            if ((widensScope || addedTags.Length > 0 || enables) && !confirm)
            {
                var gives = new List<string>();
                if (widensScope) gives.Add("gives it every server, not only the servers its tags reach");
                if (addedTags.Length > 0) gives.Add($"adds server tag(s) {string.Join(", ", addedTags)}, so its token reaches more servers");
                if (enables) gives.Add("lets its token sign people in again while advanced web access is on");
                return Refuse("confirm_required", $"This change to group '{Clip(group.Name)}' {string.Join(" and ", gives)}. Call again with confirm=true to go ahead. Nothing was changed.");
            }

            await Admin(store, null, actor).UpdateAsync(group, newScope, tags, newEnabled, ct);
            logger?.LogWarning("MCP web access group {GroupId} changed, caller {Caller}", group.GroupId, actor);
            var after = await TryReadGroupAsync(followUpOwner is null ? store : new WebAccessGroupStore(followUpOwner), group.GroupId);

            /* The undo restores the old grant, so it needs the confirm exactly when going back gives more than the group has now. */
            var undoArguments = new JsonObject
            {
                ["group"] = group.GroupId.ToString(),
                ["scope"] = ScopeName(group.Scope),
                ["tag_ids"] = new JsonArray(group.TagIds.Select(t => (JsonNode)JsonValue.Create(t)!).ToArray()),
                ["enabled"] = group.Enabled,
            };
            var undoWidens = (group.Scope == WebAccessGroupScope.AllServers && newScope != WebAccessGroupScope.AllServers)
                || AddedTags(newScope, tags, group.Scope, group.TagIds).Length > 0
                || (group.Enabled && !newEnabled);
            if (undoWidens) undoArguments["confirm"] = true;
            return new JsonObject
            {
                ["status"] = "updated",
                ["group"] = after is null ? null : GroupJson(after),
                ["undo"] = Undo("update_web_access_group", undoArguments),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("update_web_access_group", ex);
        }
    }

    /// <summary>The tag ids the new grant holds that the old one did not, when both name servers by tag. A move to all_servers is the scope widening, and a move off it adds no tag.</summary>
    private static int[] AddedTags(WebAccessGroupScope fromScope, IEnumerable<int> fromTags, WebAccessGroupScope toScope, IEnumerable<int> toTags) =>
        fromScope == WebAccessGroupScope.Tags && toScope == WebAccessGroupScope.Tags ? toTags.Except(fromTags).Order().ToArray() : [];

    internal static async Task<string> RotateAsync(
        NpgsqlDataSource? owner, IPasswordKeyRing? ring, string? groupText, bool confirm, string actor, ILogger? logger, CancellationToken ct,
        NpgsqlDataSource? followUpOwner = null)
    {
        try
        {
            if (owner is null) return NoOwner();
            var store = new WebAccessGroupStore(owner);
            var (group, refusal) = await ResolveAsync(store, groupText, ct);
            if (group is null) return refusal!;
            if (!confirm)
                return Refuse("confirm_required", $"Rotating replaces the token of group '{Clip(group.Name)}'. Its old token and its existing sessions stop working immediately. Call again with confirm=true to go ahead. Nothing was changed.");
            if (SealRefusal(ring) is { } sealRefusal) return sealRefusal;

            var token = await Admin(store, ring!, actor).RotateAsync(group, ct);
            logger?.LogWarning("MCP web access group {GroupId} token rotated, caller {Caller}", group.GroupId, actor);
            var after = await TryReadGroupAsync(followUpOwner is null ? store : new WebAccessGroupStore(followUpOwner), group.GroupId);
            return new JsonObject
            {
                ["status"] = "rotated",
                ["group"] = GroupJson(after ?? group),
                ["token"] = token.Plain,
                ["token_note"] = "Shown once. The store keeps only a sealed copy. The old token no longer works. Rotate again if this one is lost.",
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("rotate_web_access_group_token", ex);
        }
    }

    internal static async Task<string> DeleteAsync(
        NpgsqlDataSource? owner, string? groupText, bool confirm, string actor, ILogger? logger, CancellationToken ct)
    {
        try
        {
            if (owner is null) return NoOwner();
            var store = new WebAccessGroupStore(owner);
            var (group, refusal) = await ResolveAsync(store, groupText, ct);
            if (group is null) return refusal!;
            if (!confirm)
                return Refuse("confirm_required", $"Deleting removes group '{Clip(group.Name)}'. Its token and its existing sessions stop working immediately and the deletion cannot be undone. Call again with confirm=true to go ahead. Nothing was changed.");

            await Admin(store, null, actor).DeleteAsync(group, ct);
            logger?.LogWarning("MCP web access group {GroupId} deleted, caller {Caller}", group.GroupId, actor);
            return new JsonObject { ["status"] = "deleted", ["group_id"] = group.GroupId.ToString(), ["name"] = group.Name }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("delete_web_access_group", ex);
        }
    }

    private static WebAccessGroupAdmin Admin(WebAccessGroupStore store, IPasswordKeyRing? ring, string actor) =>
        new(store, (token, groupId, generation) => ring!.Seal(token, PasswordBinding.ForWebAccessToken(groupId, generation)), actor);

    /// <summary>The refusal that says why the service cannot seal a token right now, or null when it can.</summary>
    private static string? SealRefusal(IPasswordKeyRing? ring) =>
        ring is { Status.CanSeal: true } ? null
            : Refuse("key_unavailable", (ring?.Status.Reason ?? DarlingPasswordKey.NotReadyReason) + " Nothing was changed.");

    private static async Task<(WebAccessGroup? Group, string? Refusal)> ResolveAsync(WebAccessGroupStore store, string? text, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(text)) return (null, Refuse("invalid", "group is required. Nothing was changed."));
        var trimmed = text.Trim();
        var groups = await store.ListAsync(ct);
        /* An id and a name are both asked about together: a group may be named like another group's id, and a write must not pick one silently. */
        var isId = Guid.TryParse(trimmed, out var id);
        var match = groups.Where(g => (isId && g.GroupId == id) || string.Equals(g.Name, trimmed, StringComparison.Ordinal)).ToList();
        if (match.Count > 1)
            return (null, Refuse("ambiguous", $"'{Clip(trimmed)}' is the id of one web access group and the name of another, so it cannot say which one you mean. " +
                "Nothing was changed."));
        return match.Count == 1
            ? (match[0], null)
            : (null, Refuse("not_found", $"No web access group has the id or exact name '{Clip(trimmed)}'. Nothing was changed."));
    }

    private static string ScopeName(WebAccessGroupScope scope) => scope == WebAccessGroupScope.AllServers ? "all_servers" : "tags";

    /// <summary>A read after a committed write: it never throws and never takes the caller's cancellation, so the answer that holds a
    /// one-time token is not lost to a failed or cancelled follow-up.</summary>
    private static async Task<WebAccessGroup?> TryReadGroupAsync(WebAccessGroupStore store, Guid groupId)
    {
        try { return await store.ReadAsync(groupId, CancellationToken.None); }
        catch (Exception) { return null; }
    }

    private static async Task<bool?> TryReadAdvancedAsync(WebAccessGroupStore store)
    {
        try { return await store.ReadAdvancedSessionEpochAsync(CancellationToken.None) is not null; }
        catch (Exception) { return null; }
    }

    private static bool TryScope(string? text, out WebAccessGroupScope scope)
    {
        scope = WebAccessGroupScope.Tags;
        switch (text?.Trim().ToLowerInvariant())
        {
            case "tags": return true;
            case "all_servers": scope = WebAccessGroupScope.AllServers; return true;
            default: return false;
        }
    }

    private static JsonObject GroupJson(WebAccessGroup g) => new()
    {
        ["group_id"] = g.GroupId.ToString(),
        ["name"] = g.Name,
        ["scope"] = ScopeName(g.Scope),
        ["tag_ids"] = new JsonArray(g.TagIds.Select(t => (JsonNode)JsonValue.Create(t)!).ToArray()),
        ["enabled"] = g.Enabled,
        ["revision"] = g.Revision,
        ["activation_state"] = g.ActivationState,
        ["refusal_code"] = g.RefusalCode,
    };

    private static JsonObject Undo(string tool, JsonObject arguments) => new() { ["tool"] = tool, ["arguments"] = arguments };

    private static string NoOwner() =>
        Refuse("no_owner_source", "The service has not published its store owner connection yet, so web access cannot be managed. Nothing was changed.");

    /* The store's own rule for a group name (a CHECK on config.web_access_groups), said before the write so the refusal names it. */
    [System.Text.RegularExpressions.GeneratedRegex("^[A-Za-z0-9._:-]{1,64}$")]
    private static partial System.Text.RegularExpressions.Regex GroupName();

    private static string Clip(string text) => text.Length <= 80 ? text : text[..80];

    private static string Refuse(string code, string message) =>
        new JsonObject { ["status"] = "refused", ["code"] = code, ["message"] = message }.ToJsonString();
}
