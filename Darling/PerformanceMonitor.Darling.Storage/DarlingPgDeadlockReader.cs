// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the stored PostgreSQL deadlock reports (#2661).
///
/// <para><b>Once per deadlock, not once per sighting.</b> The collector re-reads an overlapping tail of the
/// server log every cycle on purpose, so a report inside the window arrives again and again until it falls
/// out. Every read here groups on <c>deadlock_hash</c> and takes the earliest sighting, which is also the
/// closest to when it actually happened.</para>
/// </summary>
public static class DarlingPgDeadlockReader
{
    public readonly record struct PgDeadlockRow(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string DeadlockHash,
        string? LockModes,
        string? Resources,
        string? VictimStatement,
        int TimesSeen);

    public readonly record struct PgDeadlockDetailRow(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string? LockModes,
        string? Resources,
        string? GraphText);

    /// <summary>
    /// Deadlocks in the window, newest first, one row per distinct report.
    ///
    /// <para>Windowed on <c>occurred_at</c> rather than <c>collection_time</c>, and the difference is not
    /// cosmetic: a report is collected some minutes AFTER it happened, and can be collected repeatedly for
    /// as long as it stays in the log tail. Filtering on collection time would put a deadlock in the wrong
    /// window and would move it every cycle.</para>
    /// </summary>
    public const string DeadlocksSql = """
        SELECT
            MIN(d.occurred_at)          AS occurred_at,
            MIN(d.victim_pid)           AS victim_pid,
            MAX(d.participant_count)    AS participant_count,
            d.deadlock_hash,
            MIN(d.lock_modes)           AS lock_modes,
            MIN(d.resources)            AS resources,
            MIN(d.victim_statement)     AS victim_statement,
            /* How many times the collector saw this same report. Not a count of deadlocks — it is a
               property of the overlapping read window, and it is surfaced so a reader can tell it apart
               from a deadlock that genuinely recurred, which would carry a DIFFERENT hash each time
               because the pids differ. */
            COUNT(*)::int               AS times_seen
        FROM pg_deadlocks AS d
        WHERE d.server_id = $1
        AND   d.occurred_at >= $2
        AND   d.occurred_at <= $3
        AND   d.deadlock_hash IS NOT NULL
        GROUP BY d.deadlock_hash
        ORDER BY MIN(d.occurred_at) DESC
        LIMIT $4
        """;

    /// <summary>
    /// One report in full, including the graph text the collector stored verbatim. Keyed by hash because
    /// that is what the summary read returns and what identifies a report across sightings.
    /// </summary>
    public const string DeadlockDetailSql = """
        SELECT
            d.occurred_at,
            d.victim_pid,
            d.participant_count,
            d.lock_modes,
            d.resources,
            d.graph_text
        FROM pg_deadlocks AS d
        WHERE d.server_id = $1
        AND   d.deadlock_hash = $2
        ORDER BY d.collection_time
        LIMIT 1
        """;

    public static async Task<List<PgDeadlockRow>> GetDeadlocksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgDeadlockRow>();
        await using var command = postgres.CreateCommand(DeadlocksSql);
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then converts these naive columns at the store session's
           TimeZone, which silently empties the window east of UTC. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgDeadlockRow(
                reader.IsDBNull(0) ? default : reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7)));
        }

        return rows;
    }

    public static async Task<PgDeadlockDetailRow?> GetDeadlockDetailAsync(
        NpgsqlDataSource postgres, int serverId, string deadlockHash,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(DeadlockDetailSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(deadlockHash);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new PgDeadlockDetailRow(
            reader.IsDBNull(0) ? default : reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5));
    }
}
