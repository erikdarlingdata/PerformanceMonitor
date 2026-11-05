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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The people directory writes an administrator makes (#7107), one command type each, shared by the two administrator surfaces: the
/// Viewer through the command plane (<see cref="DarlingCommandExecutor"/>, journaled as <c>person:&lt;requested_by&gt;</c>, surface
/// <c>viewer</c>) and a signed-in admin web seat (<see cref="RunForWebSeatAsync"/>, surface <c>web</c>). Both run
/// <see cref="PersonDirectory"/> on the store owner's data source, the only role that can write the tables. No MCP tool, chat message or
/// agent token reaches this: the mcp role's command function takes five other command types only.
/// <para>Each command's <c>args_json</c>:
/// <c>add_person {display_name}</c>, <c>rename_person {person_id, display_name}</c>, <c>set_person_enabled {person_id, enabled}</c>,
/// <c>add_person_identity {person_id, kind, external_id}</c>, <c>remove_person_identity {identity_id}</c>,
/// <c>add_person_grant {person_id, capability, kinds?, scope_server_id?, scope_tag_id?}</c> and <c>remove_person_grant {grant_id}</c>.</para>
/// </summary>
internal static class PersonDirectoryCommands
{
    public const string AddPerson = "add_person";
    public const string RenamePerson = "rename_person";
    public const string SetPersonEnabled = "set_person_enabled";
    public const string AddIdentity = "add_person_identity";
    public const string RemoveIdentity = "remove_person_identity";
    public const string AddGrant = "add_person_grant";
    public const string RemoveGrant = "remove_person_grant";

    /// <summary>Every command type above.</summary>
    public static IReadOnlySet<string> Types { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        AddPerson, RenamePerson, SetPersonEnabled, AddIdentity, RemoveIdentity, AddGrant, RemoveGrant,
    };

    /// <summary>One parsed write: run it with the owner source, the journal actor and the surface.</summary>
    internal delegate Task<PersonWriteResult> PersonWrite(NpgsqlDataSource owner, string actor, string surface, CancellationToken cancellationToken);

    /// <summary>
    /// Parses one command's <c>args_json</c> into the write it names. False, with <paramref name="usage"/> saying what the command needs, for an unknown
    /// type, JSON that is not an object, or a required argument that is missing or of the wrong JSON type. The values themselves (a name, a kind, a
    /// capability) are checked by <see cref="PersonDirectory"/> when the write runs.
    /// </summary>
    internal static bool TryParse(string? commandType, string? argsJson, out PersonWrite write, out string usage)
    {
        write = static (_, _, _, _) => Task.FromResult(new PersonWriteResult(false, "refused", "invalid_args", "invalid args_json"));
        var type = (commandType ?? string.Empty).Trim().ToLowerInvariant();
        usage = type switch
        {
            AddPerson => "add_person requires args_json {display_name}",
            RenamePerson => "rename_person requires args_json {person_id, display_name}",
            SetPersonEnabled => "set_person_enabled requires args_json {person_id, enabled} with a true or false enabled",
            AddIdentity => "add_person_identity requires args_json {person_id, kind, external_id}",
            RemoveIdentity => "remove_person_identity requires args_json {identity_id}",
            AddGrant => "add_person_grant requires args_json {person_id, capability, kinds?, scope_server_id?, scope_tag_id?}",
            RemoveGrant => "remove_person_grant requires args_json {grant_id}",
            _ => "unknown people directory command",
        };
        if (!Types.Contains(type) || string.IsNullOrWhiteSpace(argsJson))
            return false;

        try
        {
            using var document = JsonDocument.Parse(argsJson);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
                return false;

            switch (type)
            {
                case AddPerson when String(root, "display_name") is { } name:
                    write = (o, a, s, ct) => PersonDirectory.AddPersonAsync(o, name, a, s, ct);
                    return true;
                case RenamePerson when Id(root, "person_id") is { } person && String(root, "display_name") is { } name:
                    write = (o, a, s, ct) => PersonDirectory.RenamePersonAsync(o, person, name, a, s, ct);
                    return true;
                case SetPersonEnabled when Id(root, "person_id") is { } person && Flag(root, "enabled") is { } enabled:
                    write = (o, a, s, ct) => PersonDirectory.SetPersonEnabledAsync(o, person, enabled, a, s, ct);
                    return true;
                case AddIdentity when Id(root, "person_id") is { } person && String(root, "kind") is { } kind && String(root, "external_id") is { } externalId:
                    write = (o, a, s, ct) => PersonDirectory.AddIdentityAsync(o, person, kind, externalId, a, s, ct);
                    return true;
                case RemoveIdentity when Id(root, "identity_id") is { } identity:
                    write = (o, a, s, ct) => PersonDirectory.RemoveIdentityAsync(o, identity, a, s, ct);
                    return true;
                case AddGrant when Id(root, "person_id") is { } person && String(root, "capability") is { } capability
                                   && OptionalInt(root, "scope_server_id", out var server) && OptionalInt(root, "scope_tag_id", out var tag)
                                   && OptionalKinds(root, out var kinds):
                    write = (o, a, s, ct) => PersonDirectory.AddGrantAsync(o, person, server, tag, capability, kinds, a, s, ct);
                    return true;
                case RemoveGrant when Id(root, "grant_id") is { } grant:
                    write = (o, a, s, ct) => PersonDirectory.RemoveGrantAsync(o, grant, a, s, ct);
                    return true;
                default:
                    return false;
            }
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// The web entry point (#7107): one people write for a signed-in admin web seat, on the store owner's data source, journaled as the seat's
    /// person (<c>person:&lt;signed-in name&gt;</c>, the actor every web person write records) with surface <c>web</c>. A request that is not a
    /// signed-in admin is refused with <c>sign_in_required</c> before anything is parsed or read. The route that calls this runs the same
    /// admin-writes check the server action route runs (<c>CanDecideAsync</c>) before it, and answers 503 while the owner source is not published.
    /// </summary>
    internal static async Task<PersonWriteResult> RunForWebSeatAsync(HttpContext context, NpgsqlDataSource owner, string? commandType, string? argsJson,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (!DarlingWebSeat.IsSignedInAdmin(context))
            return new PersonWriteResult(false, "refused", "sign_in_required", "Sign in as an administrator to change people, identities or grants.");

        var actor = DarlingWebEndpoints.NoteDecider(DarlingWebSeat.FromContext(context));
        if (InvestigationPersonWrites.ValidateActor(actor) is not null)
            return new PersonWriteResult(false, "refused", "invalid_actor", "The signed-in name cannot be recorded as the person who made this change.");

        if (!TryParse(commandType, argsJson, out var write, out var usage))
            return new PersonWriteResult(false, "refused", "invalid_args", usage);

        return await write(owner, actor, ServerActionSurfaces.Web, cancellationToken);
    }

    /// <summary>
    /// The web identity of a seat (#7107): its owner key, the hash of the issuer and <c>sub</c>, and never its display subject. Null for a seat with
    /// no owner key (the shared web token, a reads-only seat, an older cookie): such a seat names nobody and has no identity to map.
    /// </summary>
    internal static string? WebIdentityOf(DarlingWebSeat seat) => seat?.OwnerKey;

    private static string? String(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static long? Id(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var id) && id > 0 ? id : null;

    private static bool? Flag(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind is JsonValueKind.True or JsonValueKind.False ? value.ValueKind == JsonValueKind.True : null;

    private static bool OptionalInt(JsonElement root, string name, out int? result)
    {
        result = null;
        if (!root.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
            return true;
        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number))
        {
            result = number;
            return true;
        }

        return false;
    }

    private static bool OptionalKinds(JsonElement root, out IReadOnlyCollection<string> kinds)
    {
        kinds = Array.Empty<string>();
        if (!root.TryGetProperty("kinds", out var value) || value.ValueKind == JsonValueKind.Null)
            return true;
        if (value.ValueKind != JsonValueKind.Array || value.EnumerateArray().Any(k => k.ValueKind != JsonValueKind.String))
            return false;

        kinds = value.EnumerateArray().Select(k => k.GetString() ?? string.Empty).ToArray();
        return true;
    }
}
