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
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One signature's day rows around <c>today</c>: today's count, yesterday's, and the sum of the 7 days before today.</summary>
public sealed record SignatureDayStats(string Signature, int Today, int Yesterday, int PriorWeek);

/// <summary>
/// #6005 (D13): the store half of the deadlock rollup, gate and digest. It reads and writes
/// <c>config.investigation_signature_days</c> (V193, purged at 30 days) and journals the <c>deadlock_digest</c> event. It runs as
/// the store owner in the service, so it needs no grant. Counts are read from the day rows only, never from graph XML.
/// </summary>
public sealed class DeadlockSignatureStore
{
    /* #6006: a signature merged into another is counted as that other (one level: a merge is refused when either side is already
       part of a live merge). CanonicalSql is the one place that rule is written; the three reads below use it. */
    internal const string CanonicalSql = @"
SELECT signature, merged_into FROM config.investigation_signature_merges
WHERE server_id = $1 AND undone_at IS NULL AND signature = ANY ($2)";

    /* A signature is known when any day row counts under the same canonical signature: its own, or a merged-in sibling's. */
    internal const string ExistingSql = @"
SELECT s.sig FROM unnest($2::text[]) AS s(sig)
WHERE EXISTS (
    SELECT 1 FROM config.investigation_signature_days AS d
    WHERE d.server_id = $1
      AND COALESCE((SELECT m.merged_into FROM config.investigation_signature_merges AS m
                    WHERE m.server_id = d.server_id AND m.signature = d.signature AND m.undone_at IS NULL), d.signature)
        = COALESCE((SELECT m.merged_into FROM config.investigation_signature_merges AS m
                    WHERE m.server_id = $1 AND m.signature = s.sig AND m.undone_at IS NULL), s.sig))";

    internal const string UpsertSql = @"
INSERT INTO config.investigation_signature_days (server_id, signature, day, count)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, signature, day) DO UPDATE SET count = config.investigation_signature_days.count + EXCLUDED.count";

    internal const string StatsSql = @"
SELECT COALESCE(m.merged_into, d.signature) AS signature,
       COALESCE(sum(d.count) FILTER (WHERE d.day = $2), 0)::integer,
       COALESCE(sum(d.count) FILTER (WHERE d.day = $2 - 1), 0)::integer,
       COALESCE(sum(d.count) FILTER (WHERE d.day >= $2 - 7 AND d.day < $2), 0)::integer
FROM config.investigation_signature_days AS d
LEFT JOIN config.investigation_signature_merges AS m
       ON m.server_id = d.server_id AND m.signature = d.signature AND m.undone_at IS NULL
WHERE d.server_id = $1 AND d.day >= $2 - 7
GROUP BY COALESCE(m.merged_into, d.signature)";

    /* The digest has to name a request: the journal's readers join an event to its request for the route. It attaches to the
       server's newest live-family request on an agent route (any state, never a practice one). */
    internal const string DigestRequestSql = @"
SELECT request_id, route_id FROM config.investigation_requests
WHERE server_id = $1 AND family = 'blocking' AND practice_of IS NULL
ORDER BY opened_at DESC LIMIT 1";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    public DeadlockSignatureStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <summary>
    /// Adds <paramref name="counts"/> to <paramref name="day"/>'s rows for the server, and answers the signatures that had no
    /// row at all before (a new signature), in one transaction.
    /// </summary>
    public async Task<IReadOnlySet<string>> AddAsync(
        int serverId, IReadOnlyDictionary<string, int> counts, DateOnly day, CancellationToken cancellationToken)
    {
        var fresh = new HashSet<string>(StringComparer.Ordinal);
        if (counts.Count == 0)
        {
            return fresh;
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var existing = new HashSet<string>(StringComparer.Ordinal);
        await using (var find = Command(connection, transaction, ExistingSql,
                         new NpgsqlParameter<int> { TypedValue = serverId },
                         new NpgsqlParameter<string[]> { TypedValue = counts.Keys.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text }))
        await using (var reader = await find.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                existing.Add(reader.GetString(0));
            }
        }

        var canonical = await CanonicalAsync(connection, transaction, serverId, counts.Keys, cancellationToken);
        foreach (var (signature, count) in counts)
        {
            await using var upsert = Command(connection, transaction, UpsertSql,
                new NpgsqlParameter<int> { TypedValue = serverId },
                new NpgsqlParameter<string> { TypedValue = signature },
                new NpgsqlParameter<DateOnly> { TypedValue = day, NpgsqlDbType = NpgsqlDbType.Date },
                new NpgsqlParameter<int> { TypedValue = count });
            await upsert.ExecuteNonQueryAsync(cancellationToken);
            if (!existing.Contains(signature))
            {
                fresh.Add(canonical.GetValueOrDefault(signature, signature));
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return fresh;
    }

    /// <summary>
    /// Each signature in <paramref name="signatures"/> with a live merge, mapped to the signature it is merged into. A signature
    /// with no live merge is not in the answer.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, string>> CanonicalAsync(
        int serverId, IEnumerable<string> signatures, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await CanonicalAsync(connection, null, serverId, signatures, cancellationToken);
    }

    private async Task<Dictionary<string, string>> CanonicalAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, IEnumerable<string> signatures, CancellationToken cancellationToken)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = Command(connection, transaction, CanonicalSql,
            new NpgsqlParameter<int> { TypedValue = serverId },
            new NpgsqlParameter<string[]> { TypedValue = signatures.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetString(0)] = reader.GetString(1);
        }

        return map;
    }

    /// <summary>The day rows of every signature the server has counted in the 7 days up to <paramref name="today"/>.</summary>
    public async Task<IReadOnlyList<SignatureDayStats>> StatsAsync(int serverId, DateOnly today, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, StatsSql,
            new NpgsqlParameter<int> { TypedValue = serverId },
            new NpgsqlParameter<DateOnly> { TypedValue = today, NpgsqlDbType = NpgsqlDbType.Date });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<SignatureDayStats>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SignatureDayStats(reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3)));
        }

        return rows;
    }

    /// <summary>
    /// Journals one <c>deadlock_digest</c> event for the server, attached to its newest blocking-family request (the request
    /// gives the event its route; a journal event is read through its request). Answers 1 when written, 0 when
    /// <paramref name="onceKey"/> already exists, and -1 when the server has no such request, in which case nothing is written.
    /// </summary>
    public async Task<int> WriteDigestAsync(
        int serverId, DateTime nowUtc, string onceKey, string detailJson, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        Guid requestId;
        int routeId;
        await using (var find = Command(connection, transaction, DigestRequestSql, new NpgsqlParameter<int> { TypedValue = serverId }))
        await using (var reader = await find.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return -1;
            }

            requestId = reader.GetGuid(0);
            routeId = reader.GetInt32(1);
        }

        await using var insert = Command(connection, transaction, InvestigationRequestStore.JournalSql,
            new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz },
            new NpgsqlParameter<Guid?> { TypedValue = requestId, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter<int?> { TypedValue = routeId, NpgsqlDbType = NpgsqlDbType.Integer },
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            new NpgsqlParameter<string> { TypedValue = "deadlock_digest" },
            new NpgsqlParameter<string> { TypedValue = "service" },
            new NpgsqlParameter<string?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Text },
            new NpgsqlParameter<string?> { TypedValue = detailJson, NpgsqlDbType = NpgsqlDbType.Text },
            new NpgsqlParameter<string?> { TypedValue = onceKey, NpgsqlDbType = NpgsqlDbType.Text });
        var written = await insert.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return written > 0 ? 1 : 0;
    }

    /// <summary>The refusal word for a merge that cannot be made (<c>invalid</c>, <c>unknown_signature</c>, <c>already_merged</c>, <c>merge_chain</c>).</summary>
    public sealed record MergeProblem(string Code, string Message);

    internal const string MergeStateSql = @"
SELECT
  EXISTS (SELECT 1 FROM config.investigation_signature_days WHERE server_id = $1 AND signature = $2),
  EXISTS (SELECT 1 FROM config.investigation_signature_days WHERE server_id = $1 AND signature = $3),
  EXISTS (SELECT 1 FROM config.investigation_signature_merges WHERE server_id = $1 AND signature = $2 AND undone_at IS NULL),
  EXISTS (SELECT 1 FROM config.investigation_signature_merges WHERE server_id = $1 AND signature = $3 AND undone_at IS NULL),
  EXISTS (SELECT 1 FROM config.investigation_signature_merges WHERE server_id = $1 AND merged_into = $2 AND undone_at IS NULL)";

    internal const string InsertMergeSql = @"
INSERT INTO config.investigation_signature_merges (server_id, signature, merged_into, reason, merged_by)
VALUES ($1, $2, $3, $4, $5)
RETURNING merge_id";

    /// <summary>
    /// Why a merge of <paramref name="signature"/> into <paramref name="mergedInto"/> on the server is refused, or null when it may
    /// go ahead. Merges are one level deep: the target must not itself be merged away, and the merged signature must not be a
    /// target. Both signatures must have been counted (a day row inside the 30 days the rollup keeps).
    /// </summary>
    public static async Task<MergeProblem?> CheckMergeAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, string signature, string mergedInto,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(signature) || string.IsNullOrWhiteSpace(mergedInto))
        {
            return new MergeProblem("invalid", "signature and merged_into are both required.");
        }

        if (string.Equals(signature, mergedInto, StringComparison.Ordinal))
        {
            return new MergeProblem("invalid", "A signature cannot be merged into itself.");
        }

        if (!signature.StartsWith(DeadlockSignaturePrefix, StringComparison.Ordinal)
            || !mergedInto.StartsWith(DeadlockSignaturePrefix, StringComparison.Ordinal))
        {
            return new MergeProblem("invalid", $"A deadlock signature starts with {DeadlockSignaturePrefix}. Use the signatures from the deadlock_digest event.");
        }

        await using var command = new NpgsqlCommand(MergeStateSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = signature });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = mergedInto });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var (knownFrom, knownTo, fromMerged, toMerged, fromIsTarget) =
            (reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetBoolean(4));
        if (!knownFrom || !knownTo)
        {
            return new MergeProblem("unknown_signature", "Both signatures must be ones this server has counted in the last 30 days (see the deadlock_digest event).");
        }

        if (fromMerged)
        {
            return new MergeProblem("already_merged", "The first signature is already merged into another. Undo that merge first.");
        }

        if (toMerged || fromIsTarget)
        {
            return new MergeProblem("merge_chain", "Merges are one level deep: merge into the signature the others already merge into, not into one that is merged away, and not away from one that others merge into.");
        }

        return null;
    }

    /// <summary>Writes the merge row inside the caller's transaction and answers its id. Takes the server's merge lock first, so two merges cannot build a chain between them.</summary>
    public static async Task<long> InsertMergeAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string signature, string mergedInto,
        string reason, string mergedBy, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(InsertMergeSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = signature });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = mergedInto });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reason });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = mergedBy });
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>Serialises merges on one server for the rest of the transaction.</summary>
    public static async Task LockMergesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(6006, $1)", connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private const string DeadlockSignaturePrefix = "dl1-";

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }
}
