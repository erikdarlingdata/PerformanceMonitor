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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One personal MCP token as the lists show it: never its secret and never its hash.</summary>
public sealed record PersonalTokenRow(string TokenId, string OwnerKey, string Subject, string TokenName, string Role,
    DateTimeOffset CreatedAt, DateTimeOffset ExpiresAt, DateTimeOffset? LastUsedAt, DateTimeOffset? RevokedAt,
    string? RevokedBy, string? RevokeReason);

/// <summary>The store has no <c>config.mcp_personal_token</c> yet (it is below the rung), found by a catalog lookup before any statement names it.</summary>
public sealed class PersonalTokenTableMissingException : Exception
{
    public PersonalTokenTableMissingException()
        : base("The store has no config.mcp_personal_token table yet.")
    {
    }
}

/// <summary>A token with the sha256 of its secret, for the MCP snapshot only.</summary>
public sealed record PersonalTokenSecretRow(PersonalTokenRow Row, byte[] SecretSha256);

public enum PersonalTokenCreateOutcome { Created, NameTaken, AtCap, TooManyRecent, SubjectTaken }

/// <summary>A list of tokens, <see cref="PersonalTokenStore.ListLimit"/> at most, oldest first; <see cref="Truncated"/> when rows were left out: dead rows first, then the oldest live ones.</summary>
public sealed record PersonalTokenPage(IReadOnlyList<PersonalTokenRow> Rows, bool Truncated);

/// <summary>
/// The one reader and writer of <c>config.mcp_personal_token</c> (#8737). Every call takes the store OWNER's data source: the table grants nothing
/// to the admin, viewer, web and mcp roles, and owner-only triggers refuse every other login's write. Each change journals its event
/// (<see cref="PersonalTokenEvents"/>) in the transaction of the change. A token is live while it is not revoked and has not expired. The callers
/// check the token's name and role; the store trusts them.
/// </summary>
public static class PersonalTokenStore
{
    private const int CommandSeconds = 30;

    private const string LockSql = "SELECT pg_advisory_xact_lock(hashtextextended('mcp_personal_token:' || $1, 0))";

    /* #9643: two sign-ins never share a subject. The web page makes subjects from the sign-in principal and the MCP tool from the directory display name,
       so a mint is checked against every other owner key's rows, in any case, dead rows included (the journal and the logs still name them). */
    private const string SubjectLockSql = "SELECT pg_advisory_xact_lock(hashtextextended('mcp_personal_token_subject:' || lower($1), 0))";

    private const string SubjectTakenSql = "SELECT EXISTS (SELECT 1 FROM config.mcp_personal_token WHERE lower(subject) = lower($1) AND owner_key <> $2)";

    private const string TableExistsSql = "SELECT to_regclass('config.mcp_personal_token') IS NOT NULL";

    private const string CountSql = @"
SELECT count(*) FILTER (WHERE token_name = $2 AND revoked_at IS NULL AND expires_at > $3),
       count(*) FILTER (WHERE revoked_at IS NULL AND expires_at > $3),
       count(*) FILTER (WHERE created_at > $4)
FROM config.mcp_personal_token
WHERE owner_key = $1";

    private const string InsertSql = @"
INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

    /* The first $2 rows (the caller asks for one more than it answers), live ones first and then newest first, in that order: a live token
       (not revoked, not expired at $3) is never the row that is left out while dead ones are kept. The caller sorts the page. */
    private const string ListSql = @"
SELECT token_id, owner_key, subject, token_name, role, created_at, expires_at, last_used_at, revoked_at, revoked_by, revoke_reason
FROM config.mcp_personal_token
WHERE $1::text IS NULL OR owner_key = $1
ORDER BY (revoked_at IS NULL AND expires_at > $3) DESC, created_at DESC, token_id DESC
LIMIT $2";

    private const string LoadAllSql = @"
SELECT token_id, owner_key, subject, token_name, role, created_at, expires_at, last_used_at, revoked_at, revoked_by, revoke_reason, secret_sha256
FROM config.mcp_personal_token
ORDER BY created_at, token_id";

    /* Whose row it is decides who revoked it and why, not the caller's seat: a row whose owner_key is the caller's ($4) is the owner revoking
       it (revoked_by $5, reason $7), anyone else's an administrator (revoked_by $6, reason $8). A null $4 matches no row, so it is always the
       administrator's arm. */
    private const string RevokeSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $2,
    revoked_by = CASE WHEN owner_key = $4 THEN $5 ELSE $6 END,
    revoke_reason = CASE WHEN owner_key = $4 THEN $7 ELSE $8 END
WHERE token_id = $1 AND revoked_at IS NULL AND expires_at > $2 AND ($3::text IS NULL OR owner_key = $3)
RETURNING token_name, role, revoke_reason, subject, owner_key";

    private const string RevokeForOwnerSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $2, revoked_by = $3, revoke_reason = $4
WHERE owner_key = $1 AND revoked_at IS NULL AND expires_at > $2 AND ($5 = false OR role = 'operator')
RETURNING token_id, token_name, role, subject, owner_key";

    private const string RevokeOperatorsSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $1, revoked_by = $2, revoke_reason = $3
WHERE revoked_at IS NULL AND expires_at > $1 AND role = 'operator'
RETURNING token_id, token_name, role, subject, owner_key";

    private const string CutExpirySql = @"
UPDATE config.mcp_personal_token
SET expires_at = created_at + make_interval(hours => 24 * $2)
WHERE revoked_at IS NULL AND expires_at > $1 AND expires_at > created_at + make_interval(hours => 24 * $2)
RETURNING token_id, token_name, role, subject, owner_key";

    private const string TouchSql = @"
UPDATE config.mcp_personal_token AS t
SET last_used_at = greatest(t.last_used_at, v.used_at)
FROM unnest($1::text[], $2::timestamptz[]) AS v(token_id, used_at)
WHERE t.token_id = v.token_id";

    /// <summary>How many tokens one owner key may create in <see cref="RecentWindow"/>, per slot of the live cap, revoked ones included.</summary>
    public const int RecentCreatesPerSlot = 4;

    /// <summary>The window <see cref="RecentCreatesPerSlot"/> counts creates over.</summary>
    public static readonly TimeSpan RecentWindow = TimeSpan.FromHours(24);

    /// <summary>The most rows one list answers.</summary>
    public const int ListLimit = 1000;

    /// <summary>The reason stored and journaled when a token's owner revokes it, from any seat.</summary>
    public const string RevokedByOwnerReason = "revoked_by_owner";

    /// <summary>The reason stored and journaled when an administrator revokes another person's token.</summary>
    public const string RevokedByAdminReason = "revoked_by_admin";

    /// <summary>The reason stored and journaled when the role mapping refuses the person's sign-in.</summary>
    public const string SignInRefusedReason = "sign_in_refused";

    /// <summary>The reason stored and journaled when the person signs in with a read-only seat.</summary>
    public const string SeatLoweredReason = "seat_lowered_to_read_only";

    /// <summary>The reason stored and journaled when <c>adminRole: reader</c> turns operator tokens off, at a sign-in or the start sweep.</summary>
    public const string OperatorsDisabledReason = "operator_tokens_disabled";

    /// <summary>
    /// Makes a token, unless the owner key already holds <paramref name="maxPerPerson"/> live ones (<see cref="PersonalTokenCreateOutcome.AtCap"/>)
    /// or a live one with the same name (<see cref="PersonalTokenCreateOutcome.NameTaken"/>), or has created <see cref="RecentCreatesPerSlot"/> times
    /// <paramref name="maxPerPerson"/> tokens in the last <see cref="RecentWindow"/>, revoked ones included
    /// (<see cref="PersonalTokenCreateOutcome.TooManyRecent"/>): a revoke frees a live slot at once, and the dead row stays for the retention, so
    /// without that ceiling a create-and-revoke loop grows the table without bound. A subject that another owner key's token already carries, compared
    /// without case and against every such row, is <see cref="PersonalTokenCreateOutcome.SubjectTaken"/>, checked behind a second advisory lock on the subject. The counts and the insert sit in one transaction behind a
    /// transaction-scoped advisory lock on the owner key, so two creates for the same owner at the last slot cannot both pass the count. The name of a
    /// revoked or expired token is free.
    /// </summary>
    public static async Task<PersonalTokenCreateOutcome> InsertAsync(NpgsqlDataSource owner, string tokenId, byte[] secretSha256,
        string ownerKey, string subject, string tokenName, string role, DateTimeOffset now, int lifetimeDays,
        int maxPerPerson, string actor, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(secretSha256);
        var utcNow = now.ToUniversalTime();
        return await RunAsync(owner, actor, async (connection, transaction) =>
        {
            await using (var command = Command(connection, transaction, LockSql, Text(ownerKey)))
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            await using (var command = Command(connection, transaction, SubjectLockSql, Text(subject)))
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            await using (var command = Command(connection, transaction, SubjectTakenSql, Text(subject), Text(ownerKey)))
            {
                if (await command.ExecuteScalarAsync(ct) is true)
                {
                    return (PersonalTokenCreateOutcome.SubjectTaken, false);
                }
            }

            long named, live, recent;
            await using (var command = Command(connection, transaction, CountSql, Text(ownerKey), Text(tokenName), Time(utcNow), Time(utcNow - RecentWindow)))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                await reader.ReadAsync(ct);
                named = reader.GetInt64(0);
                live = reader.GetInt64(1);
                recent = reader.GetInt64(2);
            }

            if (named > 0)
            {
                return (PersonalTokenCreateOutcome.NameTaken, false);
            }

            if (live >= maxPerPerson)
            {
                return (PersonalTokenCreateOutcome.AtCap, false);
            }

            if (recent >= (long)maxPerPerson * RecentCreatesPerSlot)
            {
                return (PersonalTokenCreateOutcome.TooManyRecent, false);
            }

            await using (var command = Command(connection, transaction, InsertSql, Text(tokenId),
                new NpgsqlParameter<byte[]> { TypedValue = secretSha256, NpgsqlDbType = NpgsqlDbType.Bytea },
                Text(ownerKey), Text(subject), Text(tokenName), Text(role), Time(utcNow), Time(utcNow.AddDays(lifetimeDays))))
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            await JournalAsync(connection, transaction, PersonalTokenEvents.Created, actor, tokenId, tokenName, role, null, subject, ownerKey, ct);
            return (PersonalTokenCreateOutcome.Created, true);
        }, ct);
    }

    /// <summary>
    /// The tokens of one owner key, or of everyone when <paramref name="ownerKey"/> is null: <see cref="ListLimit"/> rows, live ones first and then
    /// the newest, handed back oldest first, and whether rows were left out (dead rows first, then the oldest live ones) (read with one more row than it answers, #3699). A live token at
    /// <paramref name="now"/> is never the one left out while a dead one is kept. Never selects the hash.
    /// </summary>
    public static async Task<PersonalTokenPage> ListAsync(NpgsqlDataSource owner, string? ownerKey, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        await using var connection = await owner.OpenConnectionAsync(ct);
        await using var command = Command(connection, null, ListSql, NullableText(ownerKey),
            new NpgsqlParameter<int> { TypedValue = ListLimit + 1, NpgsqlDbType = NpgsqlDbType.Integer }, Time(now.ToUniversalTime()));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<PersonalTokenRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRow(reader));
        }

        /* The extra row is the last one read (a dead one, the oldest, unless every row is live); it is left out. */
        var truncated = rows.Count > ListLimit;
        if (truncated)
        {
            rows.RemoveAt(rows.Count - 1);
        }

        rows.Sort((x, y) => x.CreatedAt != y.CreatedAt ? x.CreatedAt.CompareTo(y.CreatedAt) : string.CompareOrdinal(x.TokenId, y.TokenId));
        return new PersonalTokenPage(rows, truncated);
    }

    /// <summary>Every token with the sha256 of its secret: the MCP snapshot's read, and nothing else's. Runs every few seconds, so on a store below
    /// the rung it throws <see cref="PersonalTokenTableMissingException"/> without sending the read (no 42P01 in the PostgreSQL log).</summary>
    public static async Task<IReadOnlyList<PersonalTokenSecretRow>> LoadAllAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        await using var connection = await owner.OpenConnectionAsync(ct);
        await EnsureTableAsync(connection, null, ct);
        await using var command = Command(connection, null, LoadAllSql);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<PersonalTokenSecretRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new PersonalTokenSecretRow(ReadRow(reader), reader.GetFieldValue<byte[]>(11)));
        }

        return rows;
    }

    /// <summary>
    /// Revokes one live token. <paramref name="requiredOwnerKey"/> limits it to that owner's token (null lets an administrator revoke anyone's).
    /// Whose row it is decides what is stored: the row of <paramref name="callerOwnerKey"/> is revoked by its owner
    /// (<see cref="RevokedByOwnerReason"/>, <paramref name="ownerRevokedBy"/>), any other row by an administrator (<see cref="RevokedByAdminReason"/>,
    /// <paramref name="adminRevokedBy"/>). False when no live row matched: the token is absent, already dead, or not this owner's.
    /// </summary>
    public static async Task<bool> RevokeAsync(NpgsqlDataSource owner, string tokenId, string? requiredOwnerKey, string? callerOwnerKey,
        string ownerRevokedBy, string adminRevokedBy, string actor, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return await RunAsync(owner, actor, async (connection, transaction) =>
        {
            string name, role, reason, subject, ownerKey;
            await using (var command = Command(connection, transaction, RevokeSql, Text(tokenId), Time(now.ToUniversalTime()), NullableText(requiredOwnerKey),
                NullableText(callerOwnerKey), Text(ownerRevokedBy), Text(adminRevokedBy), Text(RevokedByOwnerReason), Text(RevokedByAdminReason)))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                if (!await reader.ReadAsync(ct))
                {
                    return (false, false);
                }

                name = reader.GetString(0);
                role = reader.GetString(1);
                reason = reader.GetString(2);
                subject = reader.GetString(3);
                ownerKey = reader.GetString(4);
            }

            await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, actor, tokenId, name, role, reason, subject, ownerKey, ct);
            return (true, true);
        }, ct);
    }

    /// <summary>
    /// Revokes the live tokens of one owner key: only the operator ones when <paramref name="operatorOnly"/> (the person lost the operator
    /// grant), else every one (the person was removed or disabled). Journals each with <paramref name="revokedBy"/> as the actor. Returns how many it revoked.
    /// </summary>
    public static async Task<int> RevokeForOwnerAsync(NpgsqlDataSource owner, string ownerKey, bool operatorOnly, string revokedBy,
        string reason, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return await RunAsync(owner, revokedBy, async (connection, transaction) =>
        {
            var revoked = new List<(string Id, string Name, string Role, string Subject, string OwnerKey)>();
            await using (var command = Command(connection, transaction, RevokeForOwnerSql, Text(ownerKey), Time(now.ToUniversalTime()), Text(revokedBy), Text(reason),
                new NpgsqlParameter<bool> { TypedValue = operatorOnly, NpgsqlDbType = NpgsqlDbType.Boolean }))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    revoked.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4)));
                }
            }

            foreach (var (id, name, role, subject, ownerKey) in revoked)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, revokedBy, id, name, role, reason, subject, ownerKey, ct);
            }

            return (revoked.Count, revoked.Count > 0);
        }, ct);
    }

    /// <summary>
    /// Applies the settings to the live tokens, journaled under the actor <c>config</c>: with operator tokens off, revokes every live operator token
    /// (<see cref="OperatorsDisabledReason"/>); then cuts every live token that outlives <paramref name="maxLifetimeDays"/> from its creation back to it.
    /// Returns how many tokens it revoked and how many it cut. On a store below the rung it throws <see cref="PersonalTokenTableMissingException"/>
    /// without sending a statement that names the table.
    /// </summary>
    public static async Task<(int Revoked, int Cut)> ApplySettingsAsync(NpgsqlDataSource owner, int maxLifetimeDays,
        bool operatorAllowed, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        const string actor = "config";
        return await RunAsync(owner, actor, async (connection, transaction) =>
        {
            await EnsureTableAsync(connection, transaction, ct);
            var utcNow = now.ToUniversalTime();
            var revoked = new List<(string Id, string Name, string Role, string Subject, string OwnerKey)>();
            if (!operatorAllowed)
            {
                await using var command = Command(connection, transaction, RevokeOperatorsSql, Time(utcNow), Text(actor), Text(OperatorsDisabledReason));
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    revoked.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4)));
                }
            }

            foreach (var (id, name, role, subject, ownerKey) in revoked)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, actor, id, name, role, OperatorsDisabledReason, subject, ownerKey, ct);
            }

            var cut = new List<(string Id, string Name, string Role, string Subject, string OwnerKey)>();
            await using (var command = Command(connection, transaction, CutExpirySql, Time(utcNow),
                new NpgsqlParameter<int> { TypedValue = maxLifetimeDays, NpgsqlDbType = NpgsqlDbType.Integer }))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    cut.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4)));
                }
            }

            var cutReason = "max_lifetime_days=" + maxLifetimeDays.ToString(CultureInfo.InvariantCulture);
            foreach (var (id, name, role, subject, ownerKey) in cut)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.ExpiryCut, actor, id, name, role, cutReason, subject, ownerKey, ct);
            }

            return ((revoked.Count, cut.Count), revoked.Count + cut.Count > 0);
        }, ct);
    }

    /// <summary>Stamps the last use of each token in one statement. A stamp never moves a token's last use backwards.</summary>
    public static async Task TouchLastUsedAsync(NpgsqlDataSource owner, IReadOnlyDictionary<string, DateTimeOffset> lastUsed, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(lastUsed);
        if (lastUsed.Count == 0)
        {
            return;
        }

        var ids = new string[lastUsed.Count];
        var times = new DateTime[lastUsed.Count];
        var i = 0;
        foreach (var (id, time) in lastUsed)
        {
            ids[i] = id;
            times[i] = time.UtcDateTime;
            i++;
        }

        await using var connection = await owner.OpenConnectionAsync(ct);
        await using var command = Command(connection, null, TouchSql,
            new NpgsqlParameter<string[]> { TypedValue = ids, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
            new NpgsqlParameter<DateTime[]> { TypedValue = times, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.TimestampTz });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Whether the exception says the table does not exist: a store below V275, as the store raised it (42P01) or as the catalog check of
    /// <see cref="LoadAllAsync"/> and <see cref="ApplySettingsAsync"/> found it. Callers branch on this one test.</summary>
    public static bool IsMissingTable(Exception ex) =>
        ex is PersonalTokenTableMissingException or PostgresException { SqlState: PostgresErrorCodes.UndefinedTable };

    private static async Task EnsureTableAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        await using var command = Command(connection, transaction, TableExistsSql);
        if (await command.ExecuteScalarAsync(ct) is not true)
        {
            throw new PersonalTokenTableMissingException();
        }
    }

    private static async Task<T> RunAsync<T>(NpgsqlDataSource owner, string actor,
        Func<NpgsqlConnection, NpgsqlTransaction, Task<(T Result, bool Commit)>> body, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(actor))
        {
            throw new ArgumentException("A personal token change needs the name of who made it.", nameof(actor));
        }

        await using var connection = await owner.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var (result, commit) = await body(connection, transaction);
        if (commit)
        {
            /* #9643: the change has run, so the commit does not take the caller's token. A cancel during the commit could let it land while the
               caller is told the call was cancelled. On None the outcome is definite and journalled, though a caller that has gone still does
               not see the answer. */
            await transaction.CommitAsync(CancellationToken.None);
        }

        return result;
    }

    private static async Task JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string journalEvent, string actor,
        string tokenId, string tokenName, string role, string? reason, string subject, string ownerKey, CancellationToken ct)
    {
        /* #9643: the row says whose token it is. The subject (the name the person is shown by) and the owner key (the sign-in the token acts as)
           are not secrets, and without them a token name alone cannot be traced to a person. */
        var detail = new JsonObject
        {
            ["token_id"] = tokenId, ["token_name"] = tokenName, ["role"] = role, ["reason"] = reason, ["subject"] = subject, ["owner_key"] = ownerKey,
        };
        await using var command = Command(connection, transaction, PersonDirectory.JournalSql,
            new NpgsqlParameter<int?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Integer }, Text(journalEvent), Text(actor), Text(detail.ToJsonString()));
        await command.ExecuteScalarAsync(ct);
    }

    private static PersonalTokenRow ReadRow(NpgsqlDataReader reader) => new(
        reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
        reader.GetFieldValue<DateTimeOffset>(5), reader.GetFieldValue<DateTimeOffset>(6),
        reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
        reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
        reader.IsDBNull(9) ? null : reader.GetString(9),
        reader.IsDBNull(10) ? null : reader.GetString(10));

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static NpgsqlParameter<string> Text(string value) => new NpgsqlParameter<string> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
    private static NpgsqlParameter<string?> NullableText(string? value) => new NpgsqlParameter<string?> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
    private static NpgsqlParameter<DateTime> Time(DateTimeOffset value) =>
        new NpgsqlParameter<DateTime> { TypedValue = value.UtcDateTime, NpgsqlDbType = NpgsqlDbType.TimestampTz };
}
