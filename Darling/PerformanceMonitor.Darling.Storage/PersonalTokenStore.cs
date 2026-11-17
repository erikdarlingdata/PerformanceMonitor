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

/// <summary>A token with the sha256 of its secret, for the MCP snapshot only.</summary>
public sealed record PersonalTokenSecretRow(PersonalTokenRow Row, byte[] SecretSha256);

public enum PersonalTokenCreateOutcome { Created, NameTaken, AtCap }

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

    private const string CountSql = @"
SELECT count(*) FILTER (WHERE token_name = $2), count(*)
FROM config.mcp_personal_token
WHERE owner_key = $1 AND revoked_at IS NULL AND expires_at > $3";

    private const string InsertSql = @"
INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

    private const string ListSql = @"
SELECT token_id, owner_key, subject, token_name, role, created_at, expires_at, last_used_at, revoked_at, revoked_by, revoke_reason
FROM config.mcp_personal_token
WHERE $1::text IS NULL OR owner_key = $1
ORDER BY created_at, token_id";

    private const string LoadAllSql = @"
SELECT token_id, owner_key, subject, token_name, role, created_at, expires_at, last_used_at, revoked_at, revoked_by, revoke_reason, secret_sha256
FROM config.mcp_personal_token
ORDER BY created_at, token_id";

    private const string RevokeSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $2, revoked_by = $3, revoke_reason = $4
WHERE token_id = $1 AND revoked_at IS NULL AND expires_at > $2 AND ($5::text IS NULL OR owner_key = $5)
RETURNING token_name, role";

    private const string RevokeForOwnerSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $2, revoked_by = $3, revoke_reason = $4
WHERE owner_key = $1 AND revoked_at IS NULL AND expires_at > $2 AND ($5 = false OR role = 'operator')
RETURNING token_id, token_name, role";

    private const string RevokeOperatorsSql = @"
UPDATE config.mcp_personal_token
SET revoked_at = $1, revoked_by = $2, revoke_reason = $3
WHERE revoked_at IS NULL AND expires_at > $1 AND role = 'operator'
RETURNING token_id, token_name, role";

    private const string CutExpirySql = @"
UPDATE config.mcp_personal_token
SET expires_at = created_at + make_interval(days => $2)
WHERE revoked_at IS NULL AND expires_at > $1 AND expires_at > created_at + make_interval(days => $2)
RETURNING token_id, token_name, role";

    private const string TouchSql = @"
UPDATE config.mcp_personal_token AS t
SET last_used_at = greatest(t.last_used_at, v.used_at)
FROM unnest($1::text[], $2::timestamptz[]) AS v(token_id, used_at)
WHERE t.token_id = v.token_id";

    /// <summary>The reason journaled and stored when the settings turn operator tokens off.</summary>
    public const string OperatorsDisabledReason = "operator_tokens_disabled";

    /// <summary>
    /// Makes a token, unless the owner key already holds <paramref name="maxPerPerson"/> live ones (<see cref="PersonalTokenCreateOutcome.AtCap"/>)
    /// or a live one with the same name (<see cref="PersonalTokenCreateOutcome.NameTaken"/>). The count and the insert sit in one transaction behind a
    /// transaction-scoped advisory lock on the owner key, so two creates for the same owner at the last slot cannot both pass the count. The name of a
    /// revoked or expired token is free.
    /// </summary>
    public static async Task<PersonalTokenCreateOutcome> CreateAsync(NpgsqlDataSource owner, string tokenId, byte[] secretSha256,
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

            long named, live;
            await using (var command = Command(connection, transaction, CountSql, Text(ownerKey), Text(tokenName), Time(utcNow)))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                await reader.ReadAsync(ct);
                named = reader.GetInt64(0);
                live = reader.GetInt64(1);
            }

            if (named > 0)
            {
                return (PersonalTokenCreateOutcome.NameTaken, false);
            }

            if (live >= maxPerPerson)
            {
                return (PersonalTokenCreateOutcome.AtCap, false);
            }

            await using (var command = Command(connection, transaction, InsertSql, Text(tokenId),
                new NpgsqlParameter<byte[]> { TypedValue = secretSha256, NpgsqlDbType = NpgsqlDbType.Bytea },
                Text(ownerKey), Text(subject), Text(tokenName), Text(role), Time(utcNow), Time(utcNow.AddDays(lifetimeDays))))
            {
                await command.ExecuteNonQueryAsync(ct);
            }

            await JournalAsync(connection, transaction, PersonalTokenEvents.Created, actor, tokenId, tokenName, role, null, ct);
            return (PersonalTokenCreateOutcome.Created, true);
        }, ct);
    }

    /// <summary>The tokens of one owner key, or of everyone when <paramref name="ownerKey"/> is null. Never selects the hash.</summary>
    public static async Task<IReadOnlyList<PersonalTokenRow>> ListAsync(NpgsqlDataSource owner, string? ownerKey, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        await using var connection = await owner.OpenConnectionAsync(ct);
        await using var command = Command(connection, null, ListSql, NullableText(ownerKey));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<PersonalTokenRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRow(reader));
        }

        return rows;
    }

    /// <summary>Every token with the sha256 of its secret: the MCP snapshot's read, and nothing else's.</summary>
    public static async Task<IReadOnlyList<PersonalTokenSecretRow>> LoadAllAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        await using var connection = await owner.OpenConnectionAsync(ct);
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
    /// False when no live row matched: the token is absent, already dead, or not this owner's.
    /// </summary>
    public static async Task<bool> RevokeAsync(NpgsqlDataSource owner, string tokenId, string? requiredOwnerKey, string revokedBy,
        string reason, string actor, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return await RunAsync(owner, actor, async (connection, transaction) =>
        {
            string name, role;
            await using (var command = Command(connection, transaction, RevokeSql, Text(tokenId), Time(now.ToUniversalTime()), Text(revokedBy), Text(reason),
                NullableText(requiredOwnerKey)))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                if (!await reader.ReadAsync(ct))
                {
                    return (false, false);
                }

                name = reader.GetString(0);
                role = reader.GetString(1);
            }

            await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, actor, tokenId, name, role, reason, ct);
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
            var revoked = new List<(string Id, string Name, string Role)>();
            await using (var command = Command(connection, transaction, RevokeForOwnerSql, Text(ownerKey), Time(now.ToUniversalTime()), Text(revokedBy), Text(reason),
                new NpgsqlParameter<bool> { TypedValue = operatorOnly, NpgsqlDbType = NpgsqlDbType.Boolean }))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    revoked.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }
            }

            foreach (var (id, name, role) in revoked)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, revokedBy, id, name, role, reason, ct);
            }

            return (revoked.Count, revoked.Count > 0);
        }, ct);
    }

    /// <summary>
    /// Applies the settings to the live tokens, journaled under the actor <c>config</c>: with operator tokens off, revokes every live operator token
    /// (<see cref="OperatorsDisabledReason"/>); then cuts every live token that outlives <paramref name="maxLifetimeDays"/> from its creation back to it.
    /// Returns how many tokens it revoked and how many it cut.
    /// </summary>
    public static async Task<(int Revoked, int Cut)> ApplySettingsAsync(NpgsqlDataSource owner, int maxLifetimeDays,
        bool operatorAllowed, DateTimeOffset now, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(owner);
        const string actor = "config";
        return await RunAsync(owner, actor, async (connection, transaction) =>
        {
            var utcNow = now.ToUniversalTime();
            var revoked = new List<(string Id, string Name, string Role)>();
            if (!operatorAllowed)
            {
                await using var command = Command(connection, transaction, RevokeOperatorsSql, Time(utcNow), Text(actor), Text(OperatorsDisabledReason));
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    revoked.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }
            }

            foreach (var (id, name, role) in revoked)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.Revoked, actor, id, name, role, OperatorsDisabledReason, ct);
            }

            var cut = new List<(string Id, string Name, string Role)>();
            await using (var command = Command(connection, transaction, CutExpirySql, Time(utcNow),
                new NpgsqlParameter<int> { TypedValue = maxLifetimeDays, NpgsqlDbType = NpgsqlDbType.Integer }))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    cut.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }
            }

            var cutReason = "max_lifetime_days=" + maxLifetimeDays.ToString(CultureInfo.InvariantCulture);
            foreach (var (id, name, role) in cut)
            {
                await JournalAsync(connection, transaction, PersonalTokenEvents.ExpiryCut, actor, id, name, role, cutReason, ct);
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

    /// <summary>Whether the exception is the store saying the table does not exist: a store below V269. Callers branch on this one code.</summary>
    public static bool IsMissingTable(Exception ex) => ex is PostgresException { SqlState: PostgresErrorCodes.UndefinedTable };

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
            await transaction.CommitAsync(ct);
        }

        return result;
    }

    private static async Task JournalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string journalEvent, string actor,
        string tokenId, string tokenName, string role, string? reason, CancellationToken ct)
    {
        var detail = new JsonObject { ["token_id"] = tokenId, ["token_name"] = tokenName, ["role"] = role, ["reason"] = reason };
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
