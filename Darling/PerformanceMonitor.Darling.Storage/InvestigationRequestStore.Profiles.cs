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

    /// <summary>The longest login the journal actor names: room for a signed-in web person's subject (207 characters at most).</summary>
    public const int MaxLoginLength = 256;

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

    /// <summary>Removes the profile of a scope (a profile saved blank). $1 server, $2 tag (both NULL = fleet).</summary>
    public const string DeleteSql = @"
DELETE FROM config.agent_environment_profiles
WHERE coalesce(scope_server_id, 0) = coalesce($1::integer, 0) AND coalesce(scope_tag_id, 0) = coalesce($2::integer, 0)";

    /// <summary>A profile: one scope at most (fleet, a server or a tag), at most <see cref="MaxLength"/> characters.</summary>
    public static string? Validate(int? scopeServerId, int? scopeTagId, string? profileText) =>
        scopeServerId.HasValue && scopeTagId.HasValue ? "A profile is for the fleet, one server or one tag, not a server and a tag."
        : (profileText?.Trim().Length ?? 0) > MaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"A profile must be {MaxLength:N0} characters or fewer.")
        : null;

    /// <summary>The login the actor names: not blank, at most <see cref="MaxLoginLength"/> characters.</summary>
    public static string? ValidateLogin(string? login) =>
        string.IsNullOrWhiteSpace(login) ? "The viewer could not tell who you are, so it cannot record the change."
        : login.Trim().Length > MaxLoginLength ? string.Create(CultureInfo.InvariantCulture, $"The login is longer than {MaxLoginLength} characters.")
        : null;

    /// <summary>The journal actor for a person: <c>person:&lt;login&gt;</c>, as the V184 journal records people.</summary>
    public static string PersonActor(string login) => "person:" + login.Trim();

    /// <summary>The journal reason of a profile change, naming its scope: <c>fleet profile</c>, <c>server 7 profile</c> or
    /// <c>tag 3 profile</c>.</summary>
    public static string ScopeReason(int? scopeServerId, int? scopeTagId) =>
        scopeServerId is { } server ? string.Create(CultureInfo.InvariantCulture, $"server {server} profile")
        : scopeTagId is { } tag ? string.Create(CultureInfo.InvariantCulture, $"tag {tag} profile")
        : "fleet profile";
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
    /// </summary>
    public async Task<bool> SaveProfileAsync(
        int? scopeServerId, int? scopeTagId, string? profileText, string login, CancellationToken cancellationToken = default)
    {
        var error = InvestigationProfiles.ValidateLogin(login) ?? InvestigationProfiles.Validate(scopeServerId, scopeTagId, profileText);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(profileText));
        }

        var blank = string.IsNullOrWhiteSpace(profileText);
        var actor = InvestigationProfiles.PersonActor(login);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var changed = blank
            ? await ExecAsync(connection, transaction, InvestigationProfiles.DeleteSql, cancellationToken,
                NullableInt(scopeServerId), NullableInt(scopeTagId))
            : await ExecAsync(connection, transaction, InvestigationProfiles.UpsertSql, cancellationToken,
                NullableInt(scopeServerId), NullableInt(scopeTagId), P(profileText!.Trim()), P(actor));
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
