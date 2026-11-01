/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The people directory read (#7107): every person an administrator named, their identities and their grants. A read, served with writes off,
/// on the <c>mcp</c> role's own pool (it holds SELECT on the three tables and nothing more). There is no MCP write of a person, an identity or a
/// grant: only an administrator changes them, in the Viewer or on a signed-in admin web seat.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPeopleTools
{
    internal const string ToolName = "get_people";

    [McpServerTool(Name = "get_people"), Description(
        "Lists the people an administrator has named, with each person's identities and grants. A person has a display name and is enabled or disabled; a disabled person's grants do not count. An identity maps a sign-in to the person: kind web is a signed-in web sign-in, kind slack is a Slack user. Only the kind is shown here, never the sign-in key or the Slack ids. A grant names a capability (ask, request_level_2, request_level_3, approve, undo, decide_note, feedback), a scope (one server, one tag, or the fleet) and, for request_level_3 and approve, the server action kinds it covers. Removing a server or tag removes its grants. Grants are not yet checked by any gate. Read-only: only an administrator changes people, in the Viewer or the web dashboard.")]
    public static async Task<string> GetPeople(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            var people = await PersonDirectory.ListAsync(postgres, cancellationToken);
            return ToJson(people, includeExternalIds: false).ToJsonString();
        }
        catch (System.Exception ex) when (ex is not System.OperationCanceledException)
        {
            return McpHelpers.FormatError(ToolName, ex);
        }
    }

    /// <summary>The list as the MCP read and the web page's admin read answer it: <c>{people_returned, people: [{person_id, display_name, enabled,
    /// created_by, created_at_utc, updated_by, updated_at_utc, identities: [...], grants: [...]}]}</c>. An identity's <c>external_id</c> (a web
    /// sign-in key, or Slack ids) is included only when <paramref name="includeExternalIds"/> is set, which only an administrator's read does:
    /// MCP tokens, agent tokens included, see the kind and never the id (#7107).</summary>
    internal static JsonObject ToJson(IReadOnlyList<PersonRow> people, bool includeExternalIds) => new()
    {
        ["people_returned"] = people.Count,
        ["people"] = new JsonArray(people.Select(p => (JsonNode)new JsonObject
        {
            ["person_id"] = p.PersonId,
            ["display_name"] = p.DisplayName,
            ["enabled"] = p.Enabled,
            ["created_by"] = p.CreatedBy,
            ["created_at_utc"] = Utc(p.CreatedAt),
            ["updated_by"] = p.UpdatedBy,
            ["updated_at_utc"] = Utc(p.UpdatedAt),
            ["identities"] = new JsonArray(p.Identities.Select(i => (JsonNode)Identity(i, includeExternalIds)).ToArray()),
            ["grants"] = new JsonArray(p.Grants.Select(g => (JsonNode)new JsonObject
            {
                ["grant_id"] = g.GrantId,
                ["capability"] = g.Capability,
                ["kinds"] = new JsonArray(g.Kinds.Select(k => (JsonNode)JsonValue.Create(k)!).ToArray()),
                ["scope"] = g.ScopeServerId is not null ? "server" : g.ScopeTagId is not null ? "tag" : "fleet",
                ["scope_server_id"] = g.ScopeServerId,
                ["scope_server_name"] = g.ScopeServerName,
                ["scope_tag_id"] = g.ScopeTagId,
                ["scope_tag_name"] = g.ScopeTagName,
                ["created_by"] = g.CreatedBy,
                ["created_at_utc"] = Utc(g.CreatedAt),
            }).ToArray()),
        }).ToArray()),
    };

    private static JsonObject Identity(PersonIdentityRow i, bool includeExternalId)
    {
        var o = new JsonObject { ["identity_id"] = i.IdentityId, ["kind"] = i.Kind };
        if (includeExternalId)
            o["external_id"] = i.ExternalId;
        o["created_by"] = i.CreatedBy;
        o["created_at_utc"] = Utc(i.CreatedAt);
        return o;
    }

    private static string Utc(System.DateTime value) =>
        System.DateTime.SpecifyKind(value, System.DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ss.ffffffZ", CultureInfo.InvariantCulture);
}
