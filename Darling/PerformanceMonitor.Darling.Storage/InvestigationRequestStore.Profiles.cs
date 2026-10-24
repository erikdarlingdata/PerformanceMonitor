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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One environment profile as it is listed (#6264): the text agents are told about a scope.</summary>
public sealed class InvestigationProfileRow
{
    public int ProfileId { get; init; }
    public int? ScopeServerId { get; init; }
    public int? ScopeTagId { get; init; }
    public string ProfileText { get; init; } = "";
    public DateTime ModifiedAtUtc { get; init; }
    public string? ModifiedBy { get; init; }
}

/// <summary>
/// The environment profile rules and statements (#6264): the one copy the desktop Viewer's Investigations window and the web
/// dashboard's profile page both use, so a profile saved in either place is checked, written and journaled the same way.
/// </summary>
public static class InvestigationProfiles
{
    /// <summary>The longest profile text, in characters (the V184 CHECK's twin).</summary>
    public const int MaxLength = 4000;

    /// <summary>Every environment profile: the fleet's (both scopes NULL), then per server, then per tag.</summary>
    public const string SelectSql = @"
SELECT profile_id, scope_server_id, scope_tag_id, profile_text, modified_at, modified_by
FROM config.agent_environment_profiles
ORDER BY (scope_server_id IS NULL AND scope_tag_id IS NULL) DESC, scope_server_id NULLS LAST, scope_tag_id NULLS LAST";

    /// <summary>Writes the one profile of a scope. $1 server, $2 tag (both NULL = fleet), $3 text, $4 actor.</summary>
    public const string UpsertSql = @"
INSERT INTO config.agent_environment_profiles (scope_server_id, scope_tag_id, profile_text, modified_at, modified_by)
VALUES ($1, $2, $3, now(), $4)
ON CONFLICT ((coalesce(scope_server_id, 0)), (coalesce(scope_tag_id, 0)))
DO UPDATE SET profile_text = EXCLUDED.profile_text, modified_at = EXCLUDED.modified_at, modified_by = EXCLUDED.modified_by";

    /// <summary>Writes the first profile of a scope and nothing when one already exists (#6274). $1 server, $2 tag, $3 text, $4 actor.</summary>
    public const string InsertNewSql = @"
INSERT INTO config.agent_environment_profiles (scope_server_id, scope_tag_id, profile_text, modified_at, modified_by)
VALUES ($1, $2, $3, now(), $4)
ON CONFLICT DO NOTHING";

    /// <summary>Locks the profile of a scope and returns its <c>modified_at</c> (#6274). $1 server, $2 tag (both NULL = fleet).</summary>
    public const string LockSql = @"
SELECT modified_at FROM config.agent_environment_profiles
WHERE coalesce(scope_server_id, 0) = coalesce($1::integer, 0) AND coalesce(scope_tag_id, 0) = coalesce($2::integer, 0)
FOR UPDATE";

    /// <summary>The sentence a save answers when the scope's profile was changed or removed since the screen read it, or a first save
    /// finds one already there.</summary>
    public const string ConflictText = "This profile was changed or removed since you opened it. Reload it and make the change again.";

    /// <summary>Removes the profile of a scope (a profile saved blank). $1 server, $2 tag (both NULL = fleet).</summary>
    public const string DeleteSql = @"
DELETE FROM config.agent_environment_profiles
WHERE coalesce(scope_server_id, 0) = coalesce($1::integer, 0) AND coalesce(scope_tag_id, 0) = coalesce($2::integer, 0)";

    /// <summary>Whether a tag exists (#6400). $1 tag id. No foreign key ties a profile to its tag, so a save checks it.</summary>
    public const string TagExistsSql = "SELECT 1 FROM config.server_tags WHERE id = $1";

    /// <summary>Whether a server is registered (#6400): defined in <c>config.config_monitored_servers</c>, the set every picker and
    /// <c>list_servers</c> offer. Not <c>collect.servers</c>: that is the observed registry, written on a server's first connection (so a server
    /// just added has no row there yet) and never pruned (so a removed server keeps its row). $1 server id. No foreign key ties a profile to its
    /// server, so a save checks it.</summary>
    public const string ServerExistsSql = "SELECT 1 FROM config.config_monitored_servers WHERE server_id = $1";

    /// <summary>A profile: one scope at most (fleet, a server or a tag), at most <see cref="MaxLength"/> characters. A tag id is 1 or more
    /// (#6400): the store matches a scope on <c>coalesce(scope_tag_id, 0)</c>, so a profile saved for tag 0 would stand in the fleet profile's place.</summary>
    public static string? Validate(int? scopeServerId, int? scopeTagId, string? profileText) =>
        scopeServerId.HasValue && scopeTagId.HasValue ? "A profile is for the fleet, one server or one tag, not a server and a tag."
        : scopeServerId is <= 0 ? string.Create(CultureInfo.InvariantCulture, $"Server id {scopeServerId} is not a server; server ids start at 1.")
        : scopeTagId is <= 0 ? string.Create(CultureInfo.InvariantCulture, $"Tag id {scopeTagId} is not a tag; tag ids start at 1.")
        : (profileText?.Trim().Length ?? 0) > MaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"A profile must be {MaxLength:N0} characters or fewer.")
        : null;

    /// <summary>The journal reason of a profile change, naming its scope: <c>fleet profile</c>, <c>server 7 profile</c> or
    /// <c>tag 3 profile</c>.</summary>
    public static string ScopeReason(int? scopeServerId, int? scopeTagId) =>
        scopeServerId is { } server ? string.Create(CultureInfo.InvariantCulture, $"server {server} profile")
        : scopeTagId is { } tag ? string.Create(CultureInfo.InvariantCulture, $"tag {tag} profile")
        : "fleet profile";
}

/// <summary>A profile save found the scope's profile changed or removed since the screen read it (#6274). The message is the
/// sentence to show.</summary>
public sealed class InvestigationProfileConflictException : Exception
{
    public InvestigationProfileConflictException() : base(InvestigationProfiles.ConflictText)
    {
    }
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>Every environment profile: fleet first, then per server, then per tag (#6264).</summary>
    public async Task<IReadOnlyList<InvestigationProfileRow>> ListProfilesAsync(CancellationToken cancellationToken = default)
    {
        var rows = new List<InvestigationProfileRow>();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, InvestigationProfiles.SelectSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new InvestigationProfileRow
            {
                ProfileId = reader.GetInt32(0),
                ScopeServerId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                ScopeTagId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                ProfileText = reader.GetString(3),
                ModifiedAtUtc = reader.GetDateTime(4),
                ModifiedBy = reader.IsDBNull(5) ? null : reader.GetString(5),
            });
        }

        return rows;
    }

    /// <summary>
    /// Saves the environment profile of one scope: the fleet (both null), one server or one tag. One row per scope; a blank text removes
    /// the scope's profile. <c>modified_by</c> is <c>person:&lt;login&gt;</c>. The write and its journal row (<c>profile_saved</c>, or
    /// <c>profile_removed</c> when a blank save removed a profile; request NULL, server = the scope's server or NULL for the fleet and tag
    /// scopes, reason naming the scope) run in one transaction: the profile goes into every brief of its scope, so a change to what agents
    /// are told leaves a trace (#5632 review L2). A blank save on a scope with no profile changes nothing and journals nothing. Returns
    /// whether a row was written or removed. Throws <see cref="ArgumentException"/> with the rule's sentence when the login or the profile
    /// is refused; a store error (including SQLSTATE 42501 for a login with no write grant) propagates.
    /// <para>#6274: <paramref name="expectedModifiedAt"/> is the <c>modified_at</c> the writer's screen read for the scope, or null when it
    /// showed no profile. The scope's row is locked first; a row that is gone, or whose <c>modified_at</c> is not the expected one, or a
    /// first save that finds a profile already there, throws <see cref="InvestigationProfileConflictException"/> and writes nothing: two
    /// admins' edits never silently replace one another. A blank save of a scope that has no profile, expecting none, changes nothing.</para>
    /// </summary>
    public async Task<bool> SaveProfileAsync(
        int? scopeServerId, int? scopeTagId, string? profileText, string login, DateTime? expectedModifiedAt,
        CancellationToken cancellationToken = default)
    {
        var error = InvestigationPersonWrites.ValidateLogin(login) ?? InvestigationProfiles.Validate(scopeServerId, scopeTagId, profileText);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(profileText));
        }

        var blank = string.IsNullOrWhiteSpace(profileText);
        var actor = InvestigationPersonWrites.PersonActor(login);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* #6400: no foreign key ties a profile to its server or its tag, so an id that names none is refused here, in the save's transaction.
           A blank save is let through: it only removes, and a profile left behind by a deleted server or tag must still be removable. */
        if (!blank && scopeServerId is { } serverId)
        {
            await using var serverCommand = Command(connection, transaction, InvestigationProfiles.ServerExistsSql, P(serverId));
            if (await serverCommand.ExecuteScalarAsync(cancellationToken) is null)
            {
                throw new ArgumentException(string.Create(CultureInfo.InvariantCulture, $"Server {serverId} is not a registered server, so no profile was saved."), nameof(profileText));
            }
        }

        if (!blank && scopeTagId is { } tagId)
        {
            await using var tagCommand = Command(connection, transaction, InvestigationProfiles.TagExistsSql, P(tagId));
            if (await tagCommand.ExecuteScalarAsync(cancellationToken) is null)
            {
                throw new ArgumentException(string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} does not exist, so no profile was saved."), nameof(profileText));
            }
        }

        DateTime? current;
        await using (var lockCommand = Command(connection, transaction, InvestigationProfiles.LockSql, NullableInt(scopeServerId), NullableInt(scopeTagId)))
        {
            current = await lockCommand.ExecuteScalarAsync(cancellationToken) is DateTime stamp ? stamp : null;
        }

        /* The version the screen read must be the row's now: a changed, removed or newly created profile is the other admin's work. */
        var expectedNone = expectedModifiedAt is null;
        if (expectedNone ? current is not null : current is null || expectedModifiedAt!.Value.Ticks != current.Value.Ticks)
        {
            throw new InvestigationProfileConflictException();
        }

        int changed;
        if (blank)
        {
            changed = current is null ? 0 : await ExecAsync(connection, transaction, InvestigationProfiles.DeleteSql, cancellationToken,
                NullableInt(scopeServerId), NullableInt(scopeTagId));
        }
        else
        {
            changed = await ExecAsync(connection, transaction, expectedNone ? InvestigationProfiles.InsertNewSql : InvestigationProfiles.UpsertSql,
                cancellationToken, NullableInt(scopeServerId), NullableInt(scopeTagId), P(profileText!.Trim()), P(actor));
            if (changed == 0)
            {
                /* A first save that raced another first save: the row is theirs. */
                throw new InvestigationProfileConflictException();
            }
        }

        if (changed > 0)
        {
            /* The store's clock, not this process's (#6274): the row is dated by now() in SQL, as the profile row is. */
            await InvestigationPersonJournal.InsertAsync(connection, transaction, null, null, scopeServerId, blank ? "profile_removed" : "profile_saved",
                actor, InvestigationProfiles.ScopeReason(scopeServerId, scopeTagId), null, _commandTimeoutSeconds, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return changed > 0;
    }

    private static NpgsqlParameter<int?> NullableInt(int? value) =>
        new NpgsqlParameter<int?> { TypedValue = value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer };
}
