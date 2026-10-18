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
    internal const string ExistingSql = @"
SELECT DISTINCT signature FROM config.investigation_signature_days WHERE server_id = $1 AND signature = ANY ($2)";

    internal const string UpsertSql = @"
INSERT INTO config.investigation_signature_days (server_id, signature, day, count)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, signature, day) DO UPDATE SET count = config.investigation_signature_days.count + EXCLUDED.count";

    internal const string StatsSql = @"
SELECT signature,
       COALESCE(sum(count) FILTER (WHERE day = $2), 0)::integer,
       COALESCE(sum(count) FILTER (WHERE day = $2 - 1), 0)::integer,
       COALESCE(sum(count) FILTER (WHERE day >= $2 - 7 AND day < $2), 0)::integer
FROM config.investigation_signature_days
WHERE server_id = $1 AND day >= $2 - 7
GROUP BY signature";

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
                fresh.Add(signature);
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return fresh;
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

    private NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.AddRange(parameters);
        return command;
    }
}
