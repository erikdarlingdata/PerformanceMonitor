/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The journal events of the people directory (#7107). Only the store owner journals them: <see cref="PersonDirectory"/> writes each in the
/// transaction of its change, on the owner connection, and the journal's owner-only trigger refuses every one of them from the <c>mcp</c>,
/// <c>admin</c>, <c>viewer</c> and <c>web</c> logins, so no other login can write a row that says an administrator mapped or granted somebody.
/// </summary>
public static class PersonEvents
{
    public const string PersonAdded = "person_added";
    public const string PersonRenamed = "person_renamed";
    public const string PersonEnabled = "person_enabled";
    public const string PersonDisabled = "person_disabled";
    public const string IdentityAdded = "person_identity_added";
    public const string IdentityRemoved = "person_identity_removed";
    public const string GrantAdded = "person_grant_added";
    public const string GrantRemoved = "person_grant_removed";

    /// <summary>Every event above, for the journal trigger's owner-only list.</summary>
    public static IReadOnlyList<string> OwnerOnly { get; } =
        [PersonAdded, PersonRenamed, PersonEnabled, PersonDisabled, IdentityAdded, IdentityRemoved, GrantAdded, GrantRemoved];
}

/// <summary>The identity kinds of a person (#7107).</summary>
public static class PersonIdentityKinds
{
    /// <summary>A Slack user: <c>team id:user id</c>.</summary>
    public const string Slack = "slack";

    /// <summary>A signed-in web seat: its owner key, the 43-character base64url SHA-256 of the issuer and <c>sub</c>. Never the display
    /// subject, which is <c>preferred_username</c> first and which some identity providers reuse.</summary>
    public const string Web = "web";

    public static IReadOnlyList<string> All { get; } = [Slack, Web];
}

/// <summary>The capabilities a grant gives (#7107, design section 2 of #6980). Only <see cref="RequestLevel3"/> and <see cref="Approve"/>
/// carry a list of server action kinds (<see cref="ServerActionNames.All"/>).</summary>
public static class PersonCapabilities
{
    public const string Ask = "ask";
    public const string RequestLevel2 = "request_level_2";
    public const string RequestLevel3 = "request_level_3";
    public const string Approve = "approve";
    public const string Undo = "undo";
    public const string DecideNote = "decide_note";
    public const string Feedback = "feedback";

    public static IReadOnlyList<string> All { get; } = [Ask, RequestLevel2, RequestLevel3, Approve, Undo, DecideNote, Feedback];

    /// <summary>Whether the capability carries a list of kinds (at least one), rather than none.</summary>
    public static bool TakesKinds(string capability) => capability is RequestLevel3 or Approve;
}

/// <summary>The answer to one people directory write (#7107). A refusal changes nothing and has a <see cref="Code"/>.</summary>
public sealed record PersonWriteResult(bool Succeeded, string Status, string? Code, string Message, long? PersonId = null, long? IdentityId = null,
    long? GrantId = null, long? JournalId = null)
{
    public JsonObject ToJson()
    {
        var node = new JsonObject { ["status"] = Status, ["message"] = Message };
        if (Code is not null)
            node["code"] = Code;
        if (PersonId is not null)
            node["person_id"] = PersonId;
        if (IdentityId is not null)
            node["identity_id"] = IdentityId;
        if (GrantId is not null)
            node["grant_id"] = GrantId;
        if (JournalId is not null)
            node["journal_id"] = JournalId;
        return node;
    }
}

public sealed record PersonIdentityRow(long IdentityId, string Kind, string ExternalId, DateTime CreatedAt, string CreatedBy);

public sealed record PersonGrantRow(long GrantId, int? ScopeServerId, string? ScopeServerName, int? ScopeTagId, string? ScopeTagName, string Capability,
    IReadOnlyList<string> Kinds, DateTime CreatedAt, string CreatedBy);

public sealed record PersonRow(long PersonId, string DisplayName, bool Enabled, DateTime CreatedAt, string CreatedBy, DateTime UpdatedAt, string UpdatedBy,
    IReadOnlyList<PersonIdentityRow> Identities, IReadOnlyList<PersonGrantRow> Grants);

/// <summary>
/// The one writer of <c>config.person</c>, <c>config.person_identity</c> and <c>config.person_grant</c> (#7107). Every surface (the Viewer's
/// people commands through the command plane, the signed-in admin web seat) calls it with the store OWNER's data source: no other role holds
/// a write on the three tables, and none is given one. No MCP tool, chat message or agent token reaches it. Each write checks its input,
/// changes the row and journals its event (<see cref="PersonEvents"/>) in ONE transaction, with the administrator as the actor
/// (<c>person:&lt;login&gt;</c>) and the surface in the detail. A refusal changes nothing and never throws.
/// <para>No gate reads a grant yet (child (d1) of #6980 adds that), so nothing here changes what any current path may do.</para>
/// </summary>
public static class PersonDirectory
{
    /// <summary>The longest display name (the table's CHECK holds the same bound).</summary>
    public const int MaxDisplayNameLength = 128;

    private static readonly Regex s_webKey = new("^[A-Za-z0-9_-]{43}$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
    private static readonly Regex s_slackKey = new("^[A-Z0-9]{1,32}:[A-Z0-9]{1,32}$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>Every statement waits at most this long, so a write never hangs the command plane or a web request.</summary>
    internal const int CommandSeconds = 30;

    internal const string JournalSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
VALUES (now(), NULL, $1, $2, $3, NULL, $4::jsonb)
RETURNING journal_id";

    /// <summary>Whether <paramref name="externalId"/> is a valid external id for <paramref name="kind"/>, as the table's CHECK reads it.</summary>
    public static bool IsValidExternalId(string kind, string? externalId) => kind switch
    {
        PersonIdentityKinds.Web => externalId is not null && s_webKey.IsMatch(externalId),
        PersonIdentityKinds.Slack => externalId is not null && s_slackKey.IsMatch(externalId),
        _ => false,
    };

    public static Task<PersonWriteResult> AddPersonAsync(NpgsqlDataSource owner, string? displayName, string actor, string surface,
        CancellationToken cancellationToken = default)
    {
        var name = (displayName ?? string.Empty).Trim();
        if (NameRefusal(name) is { } refused)
            return Task.FromResult(refused);

        return RunAsync(owner, actor, async (connection, transaction) =>
        {
            long personId;
            await using (var command = Command(connection, transaction,
                "INSERT INTO config.person (display_name, created_by, updated_by) VALUES ($1, $2, $2) RETURNING person_id", Text(name), Text(actor)))
            {
                personId = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            var journalId = await JournalAsync(connection, transaction, null, PersonEvents.PersonAdded, actor,
                new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["display_name"] = name }, cancellationToken);
            return new PersonWriteResult(true, "added", null, $"Added {name}.", personId, JournalId: journalId);
        }, cancellationToken);
    }

    public static Task<PersonWriteResult> RenamePersonAsync(NpgsqlDataSource owner, long personId, string? displayName, string actor, string surface,
        CancellationToken cancellationToken = default)
    {
        var name = (displayName ?? string.Empty).Trim();
        if (NameRefusal(name) is { } refused)
            return Task.FromResult(refused);

        return RunAsync(owner, actor, async (connection, transaction) =>
        {
            if (await LockPersonAsync(connection, transaction, personId, cancellationToken) is not { } person)
                return UnknownPerson(personId);
            if (string.Equals(person.Name, name, StringComparison.Ordinal))
                return new PersonWriteResult(true, "unchanged", null, $"{name} already has that name. Nothing was changed.", personId);

            await using (var command = Command(connection, transaction,
                "UPDATE config.person SET display_name = $2, updated_at = now(), updated_by = $3 WHERE person_id = $1", Long(personId), Text(name), Text(actor)))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            var journalId = await JournalAsync(connection, transaction, null, PersonEvents.PersonRenamed, actor,
                new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["before"] = person.Name, ["after"] = name }, cancellationToken);
            return new PersonWriteResult(true, "renamed", null, $"Renamed {person.Name} to {name}.", personId, JournalId: journalId);
        }, cancellationToken);
    }

    /// <summary>Turns a person on or off. A disabled person keeps their identities and grants, and no grant of theirs counts while they are off.</summary>
    public static Task<PersonWriteResult> SetPersonEnabledAsync(NpgsqlDataSource owner, long personId, bool enabled, string actor, string surface,
        CancellationToken cancellationToken = default) =>
        RunAsync(owner, actor, async (connection, transaction) =>
        {
            if (await LockPersonAsync(connection, transaction, personId, cancellationToken) is not { } person)
                return UnknownPerson(personId);
            if (person.Enabled == enabled)
                return new PersonWriteResult(true, "unchanged", null, $"{person.Name} was already {(enabled ? "enabled" : "disabled")}. Nothing was changed.", personId);

            await using (var command = Command(connection, transaction,
                "UPDATE config.person SET enabled = $2, updated_at = now(), updated_by = $3 WHERE person_id = $1", Long(personId), Bool(enabled), Text(actor)))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            var journalId = await JournalAsync(connection, transaction, null, enabled ? PersonEvents.PersonEnabled : PersonEvents.PersonDisabled, actor,
                new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["display_name"] = person.Name }, cancellationToken);
            return new PersonWriteResult(true, enabled ? "enabled" : "disabled", null, $"{person.Name} is now {(enabled ? "enabled" : "disabled")}.", personId,
                JournalId: journalId);
        }, cancellationToken);

    /// <summary>
    /// Maps one identity to a person. <paramref name="externalId"/> is, for <c>web</c>, a seat's owner key (<c>DarlingWebSeat.OwnerKey</c>), and for
    /// <c>slack</c>, <c>team id:user id</c>. An identity belongs to one person at most: one already mapped is refused with <c>identity_taken</c>.
    /// </summary>
    public static Task<PersonWriteResult> AddIdentityAsync(NpgsqlDataSource owner, long personId, string? kind, string? externalId, string actor,
        string surface, CancellationToken cancellationToken = default)
    {
        var k = (kind ?? string.Empty).Trim();
        var id = (externalId ?? string.Empty).Trim();
        if (!PersonIdentityKinds.All.Contains(k, StringComparer.Ordinal))
            return Task.FromResult(Refused("unknown_kind", $"The identity kinds are {string.Join(", ", PersonIdentityKinds.All)}."));
        if (!IsValidExternalId(k, id))
        {
            return Task.FromResult(Refused("invalid_identity", k == PersonIdentityKinds.Web
                ? "A web identity is a signed-in seat's owner key: 43 letters, digits, '-' or '_'."
                : "A Slack identity is the team id and the user id, joined by ':' (for example T0123ABCD:U0456EFGH)."));
        }

        return RunAsync(owner, actor, async (connection, transaction) =>
        {
            if (await LockPersonAsync(connection, transaction, personId, cancellationToken) is not { } person)
                return UnknownPerson(personId);

            await using (var command = Command(connection, transaction,
                "SELECT person_id FROM config.person_identity WHERE kind = $1 AND external_id = $2", Text(k), Text(id)))
            {
                if (await command.ExecuteScalarAsync(cancellationToken) is long holder)
                {
                    return holder == personId
                        ? new PersonWriteResult(true, "unchanged", null, $"{person.Name} already has that identity. Nothing was changed.", personId)
                        : Refused("identity_taken", "That identity is already mapped to another person. Remove it there first.");
                }
            }

            long identityId;
            await using (var command = Command(connection, transaction,
                "INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ($1, $2, $3, $4) RETURNING identity_id",
                Long(personId), Text(k), Text(id), Text(actor)))
            {
                identityId = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            var journalId = await JournalAsync(connection, transaction, null, PersonEvents.IdentityAdded, actor,
                new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["display_name"] = person.Name, ["identity_id"] = identityId, ["kind"] = k, ["external_id"] = id },
                cancellationToken);
            return new PersonWriteResult(true, "added", null, $"Mapped a {k} identity to {person.Name}.", personId, identityId, JournalId: journalId);
        }, cancellationToken);
    }

    public static Task<PersonWriteResult> RemoveIdentityAsync(NpgsqlDataSource owner, long identityId, string actor, string surface,
        CancellationToken cancellationToken = default) =>
        RunAsync(owner, actor, async (connection, transaction) =>
        {
            long personId;
            string kind, externalId;
            await using (var command = Command(connection, transaction,
                "DELETE FROM config.person_identity WHERE identity_id = $1 RETURNING person_id, kind, external_id", Long(identityId)))
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (!await reader.ReadAsync(cancellationToken))
                    return Refused("unknown_identity", "That identity is not mapped to anybody.");
                personId = reader.GetInt64(0);
                kind = reader.GetString(1);
                externalId = reader.GetString(2);
            }

            var journalId = await JournalAsync(connection, transaction, null, PersonEvents.IdentityRemoved, actor,
                new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["identity_id"] = identityId, ["kind"] = kind, ["external_id"] = externalId },
                cancellationToken);
            return new PersonWriteResult(true, "removed", null, $"Removed the {kind} identity.", personId, identityId, JournalId: journalId);
        }, cancellationToken);

    /// <summary>
    /// Grants a person one capability for one scope: one server (<paramref name="scopeServerId"/>), one tag (<paramref name="scopeTagId"/>) or the
    /// fleet (neither). <paramref name="kinds"/> is required for <c>request_level_3</c> and <c>approve</c> and refused for the rest. One grant per
    /// person, capability and scope: a second is refused with <c>grant_exists</c> (remove the first to change its kinds).
    /// </summary>
    public static Task<PersonWriteResult> AddGrantAsync(NpgsqlDataSource owner, long personId, int? scopeServerId, int? scopeTagId, string? capability,
        IReadOnlyCollection<string>? kinds, string actor, string surface, CancellationToken cancellationToken = default)
    {
        var cap = (capability ?? string.Empty).Trim();
        if (!PersonCapabilities.All.Contains(cap, StringComparer.Ordinal))
            return Task.FromResult(Refused("unknown_capability", $"The capabilities are {string.Join(", ", PersonCapabilities.All)}."));
        if (scopeServerId is not null && scopeTagId is not null)
            return Task.FromResult(Refused("invalid_scope", "A grant is for one server, one tag, or the whole fleet, not a server and a tag."));

        var given = (kinds ?? Array.Empty<string>()).Select(x => (x ?? string.Empty).Trim()).Where(x => x.Length > 0).Distinct(StringComparer.Ordinal).ToList();
        if (given.FirstOrDefault(x => !ServerActionNames.All.Contains(x, StringComparer.Ordinal)) is { } bad)
            return Task.FromResult(Refused("unknown_kind", $"'{(bad.Length <= 40 ? bad : bad[..40])}' is not a server action. The kinds are {string.Join(", ", ServerActionNames.All)}."));
        if (PersonCapabilities.TakesKinds(cap) && given.Count == 0)
            return Task.FromResult(Refused("kinds_required", $"{cap} needs at least one kind: {string.Join(", ", ServerActionNames.All)}."));
        if (!PersonCapabilities.TakesKinds(cap) && given.Count > 0)
            return Task.FromResult(Refused("kinds_not_taken", $"{cap} takes no kinds. Only request_level_3 and approve do."));
        var ordered = ServerActionNames.All.Where(given.Contains).ToArray();

        return RunAsync(owner, actor, async (connection, transaction) =>
        {
            if (await LockPersonAsync(connection, transaction, personId, cancellationToken) is not { } person)
                return UnknownPerson(personId);

            string scope = "the fleet";
            if (scopeServerId is int serverId)
            {
                await using var command = Command(connection, transaction, "SELECT name FROM config.config_monitored_servers WHERE server_id = $1", Int(serverId));
                if (await command.ExecuteScalarAsync(cancellationToken) is not string serverName)
                    return Refused("unknown_server", "That server is not in the registry.");
                scope = serverName;
            }
            else if (scopeTagId is int tagId)
            {
                await using var command = Command(connection, transaction, "SELECT name FROM config.server_tags WHERE id = $1", Int(tagId));
                if (await command.ExecuteScalarAsync(cancellationToken) is not string tagName)
                    return Refused("unknown_tag", "That tag does not exist.");
                scope = "tag " + tagName;
            }

            await using (var command = Command(connection, transaction, @"
SELECT grant_id FROM config.person_grant
WHERE person_id = $1 AND capability = $2
  AND scope_server_id IS NOT DISTINCT FROM $3 AND scope_tag_id IS NOT DISTINCT FROM $4", Long(personId), Text(cap), NullableInt(scopeServerId), NullableInt(scopeTagId)))
            {
                if (await command.ExecuteScalarAsync(cancellationToken) is long existing)
                    return new PersonWriteResult(false, "refused", "grant_exists", $"{person.Name} already holds {cap} for {scope}. Remove that grant first to change it.", personId, GrantId: existing);
            }

            long grantId;
            await using (var command = Command(connection, transaction, @"
INSERT INTO config.person_grant (person_id, scope_server_id, scope_tag_id, capability, kinds, created_by)
VALUES ($1, $3, $4, $2, $5, $6) RETURNING grant_id",
                Long(personId), Text(cap), NullableInt(scopeServerId), NullableInt(scopeTagId),
                new NpgsqlParameter<string[]> { TypedValue = ordered, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text }, Text(actor)))
            {
                grantId = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            }

            var journalId = await JournalAsync(connection, transaction, scopeServerId, PersonEvents.GrantAdded, actor, GrantDetail(surface, personId, person.Name, grantId,
                scopeServerId, scopeTagId, cap, ordered), cancellationToken);
            return new PersonWriteResult(true, "added", null, $"{person.Name} now holds {cap}{KindsText(ordered)} for {scope}.", personId, GrantId: grantId, JournalId: journalId);
        }, cancellationToken);
    }

    public static Task<PersonWriteResult> RemoveGrantAsync(NpgsqlDataSource owner, long grantId, string actor, string surface,
        CancellationToken cancellationToken = default) =>
        RunAsync(owner, actor, async (connection, transaction) =>
        {
            long personId;
            int? serverId, tagId;
            string capability;
            string[] kinds;
            await using (var command = Command(connection, transaction,
                "DELETE FROM config.person_grant WHERE grant_id = $1 RETURNING person_id, scope_server_id, scope_tag_id, capability, kinds", Long(grantId)))
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (!await reader.ReadAsync(cancellationToken))
                    return Refused("unknown_grant", "That grant does not exist.");
                personId = reader.GetInt64(0);
                serverId = reader.IsDBNull(1) ? null : reader.GetInt32(1);
                tagId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                capability = reader.GetString(3);
                kinds = reader.GetFieldValue<string[]>(4);
            }

            var journalId = await JournalAsync(connection, transaction, serverId, PersonEvents.GrantRemoved, actor,
                GrantDetail(surface, personId, null, grantId, serverId, tagId, capability, kinds), cancellationToken);
            return new PersonWriteResult(true, "removed", null, $"Removed the {capability} grant.", personId, GrantId: grantId, JournalId: journalId);
        }, cancellationToken);

    internal const string ListPeopleSql = @"
SELECT person_id, display_name, enabled, created_at, created_by, updated_at, updated_by
FROM config.person
ORDER BY lower(display_name), person_id";

    internal const string ListIdentitiesSql = @"
SELECT person_id, identity_id, kind, external_id, created_at, created_by
FROM config.person_identity
ORDER BY person_id, kind, identity_id";

    internal const string ListGrantsSql = @"
SELECT g.person_id, g.grant_id, g.scope_server_id, s.name, g.scope_tag_id, t.name, g.capability, g.kinds, g.created_at, g.created_by
FROM config.person_grant AS g
LEFT JOIN config.config_monitored_servers AS s ON s.server_id = g.scope_server_id
LEFT JOIN config.server_tags AS t ON t.id = g.scope_tag_id
ORDER BY g.person_id, g.capability, g.grant_id";

    /// <summary>Every person with their identities and grants. Runs on any role that reads the three tables (all four do). A grant whose
    /// server or tag was removed reads with a null name: it grants nothing.</summary>
    public static async Task<IReadOnlyList<PersonRow>> ListAsync(NpgsqlDataSource source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        var people = new List<(long Id, string Name, bool Enabled, DateTime CreatedAt, string CreatedBy, DateTime UpdatedAt, string UpdatedBy)>();
        var identities = new Dictionary<long, List<PersonIdentityRow>>();
        var grants = new Dictionary<long, List<PersonGrantRow>>();
        await using (var command = new NpgsqlCommand(ListPeopleSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                people.Add((reader.GetInt64(0), reader.GetString(1), reader.GetBoolean(2), reader.GetDateTime(3), reader.GetString(4), reader.GetDateTime(5), reader.GetString(6)));
        }

        await using (var command = new NpgsqlCommand(ListIdentitiesSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var personId = reader.GetInt64(0);
                if (!identities.TryGetValue(personId, out var list))
                    identities[personId] = list = [];
                list.Add(new PersonIdentityRow(reader.GetInt64(1), reader.GetString(2), reader.GetString(3), reader.GetDateTime(4), reader.GetString(5)));
            }
        }

        await using (var command = new NpgsqlCommand(ListGrantsSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var personId = reader.GetInt64(0);
                if (!grants.TryGetValue(personId, out var list))
                    grants[personId] = list = [];
                list.Add(new PersonGrantRow(reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetInt32(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetString(6),
                    reader.GetFieldValue<string[]>(7), reader.GetDateTime(8), reader.GetString(9)));
            }
        }

        return people.Select(p => new PersonRow(p.Id, p.Name, p.Enabled, p.CreatedAt, p.CreatedBy, p.UpdatedAt, p.UpdatedBy,
            identities.TryGetValue(p.Id, out var i) ? i : [], grants.TryGetValue(p.Id, out var g) ? g : [])).ToList();
    }

    private static async Task<PersonWriteResult> RunAsync(NpgsqlDataSource owner, string actor,
        Func<NpgsqlConnection, NpgsqlTransaction, Task<PersonWriteResult>> body, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(owner);
        if (string.IsNullOrWhiteSpace(actor))
            throw new ArgumentException("A people directory change needs the name of who made it.", nameof(actor));

        await using var connection = await owner.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            var result = await body(connection, transaction);
            if (result.Succeeded && result.Status != "unchanged")
                await transaction.CommitAsync(cancellationToken);
            return result;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            /* Two administrators added the same identity or grant at once: the second one's insert meets the unique index. Rolled back on dispose. */
            return Refused("conflict", "Somebody else made the same change at the same time. Reload and try again.");
        }
    }

    private static async Task<(string Name, bool Enabled)?> LockPersonAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, long personId,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, "SELECT display_name, enabled FROM config.person WHERE person_id = $1 FOR UPDATE", Long(personId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? (reader.GetString(0), reader.GetBoolean(1)) : null;
    }

    private static async Task<long> JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int? serverId, string journalEvent, string actor,
        JsonObject detail, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, JournalSql, NullableInt(serverId), Text(journalEvent), Text(actor), Text(detail.ToJsonString()));
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static JsonObject GrantDetail(string surface, long personId, string? personName, long grantId, int? serverId, int? tagId, string capability,
        string[] kinds)
    {
        var detail = new JsonObject { ["surface"] = surface, ["person_id"] = personId, ["grant_id"] = grantId, ["capability"] = capability };
        if (personName is not null)
            detail["display_name"] = personName;
        detail["scope"] = serverId is not null ? "server" : tagId is not null ? "tag" : "fleet";
        if (serverId is not null)
            detail["scope_server_id"] = serverId;
        if (tagId is not null)
            detail["scope_tag_id"] = tagId;
        detail["kinds"] = new JsonArray(kinds.Select(k => (JsonNode)JsonValue.Create(k)!).ToArray());
        return detail;
    }

    private static string KindsText(string[] kinds) => kinds.Length == 0 ? string.Empty : " (" + string.Join(", ", kinds) + ")";

    private static PersonWriteResult? NameRefusal(string name) =>
        name.Length == 0 ? Refused("invalid_name", "A person needs a display name.")
        : name.Length > MaxDisplayNameLength ? Refused("invalid_name", $"A display name is at most {MaxDisplayNameLength} characters.")
        : null;

    private static PersonWriteResult UnknownPerson(long personId) =>
        new(false, "refused", "unknown_person", "That person does not exist.", personId);

    private static PersonWriteResult Refused(string code, string message) => new(false, "refused", code, message);

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static NpgsqlParameter<string> Text(string value) => new NpgsqlParameter<string> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
    private static NpgsqlParameter<long> Long(long value) => new NpgsqlParameter<long> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Bigint };
    private static NpgsqlParameter<int> Int(int value) => new NpgsqlParameter<int> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Integer };
    private static NpgsqlParameter<bool> Bool(bool value) => new NpgsqlParameter<bool> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Boolean };
    private static NpgsqlParameter<int?> NullableInt(int? value) => new NpgsqlParameter<int?> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Integer };
}
