/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the PostgreSQL wait store (<c>pg_wait_stats</c>) — the counterpart of
/// <see cref="DarlingDataReader.WaitStatsSql"/> for an Aurora target.
/// <para>Deliberately a separate reader and a separate MCP tool rather than a widened
/// <c>get_wait_stats</c>: the two engines' wait models do not line up column for column. Postgres has
/// a two-level type/event taxonomy where SQL Server has one flat name, carries no signal-wait concept
/// at all, and reports microseconds where SQL Server reports milliseconds. Folding them into one
/// result would mean either lying about a unit or emitting mostly-null columns.</para>
/// </summary>
public static class DarlingPgWaitReader
{
    /// <summary>
    /// One row per wait event over the window, heaviest first.
    /// <para><c>WaitTimeMs</c> is converted from the stored microseconds so the field means the same
    /// thing it does everywhere else in this product. The store keeps microseconds because that is
    /// what Aurora reports; the read layer converts once, here, rather than leaving every consumer to
    /// remember.</para>
    /// </summary>
    public sealed record PgWaitRow(
        string WaitType,
        string WaitEvent,
        long TotalWaits,
        double TotalWaitTimeMs,
        double AvgWaitTimeMs);

    /// <summary>
    /// Aggregated over the window from the delta columns, not the raw cumulative counters — summing
    /// cumulative values across snapshots would multiply the whole history by the snapshot count.
    /// <para>Unnamed events are surfaced rather than filtered: <c>wait_type</c> and <c>wait_event</c>
    /// are nullable because their lookups are LEFT JOINed, and an event Aurora reports but does not
    /// name is exactly the new-wait-type case an operator should see. They get a synthetic label built
    /// from the numeric ids so the row is still identifiable.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC, matching every other read in this store), $4 row cap.</para>
    /// <para>The cap is a PARAMETER, not a literal. It was <c>LIMIT 50</c> while the tool advertised a
    /// caller-supplied limit and then applied it with <c>Take(limit)</c> — so a caller asking for more than 50
    /// silently got 50, and every request below that fetched rows only to discard them. Same shape as every
    /// other read in this store.</para>
    /// </summary>
    public const string PgWaitStatsSql = """
        SELECT
            COALESCE(wait_type, 'unknown_type_' || wait_type_id::text) AS wait_type,
            COALESCE(wait_event, 'unknown_event_' || wait_event_id::text) AS wait_event,
            CAST(SUM(delta_waits) AS bigint) AS total_waits,
            SUM(delta_wait_time_us) / 1000.0 AS total_wait_time_ms,
            CASE
                WHEN SUM(delta_waits) > 0
                THEN (SUM(delta_wait_time_us) / 1000.0) / SUM(delta_waits)
                ELSE 0
            END AS avg_wait_time_ms
        FROM pg_wait_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY
            COALESCE(wait_type, 'unknown_type_' || wait_type_id::text),
            COALESCE(wait_event, 'unknown_event_' || wait_event_id::text)
        HAVING SUM(delta_wait_time_us) > 0
        ORDER BY SUM(delta_wait_time_us) DESC
        LIMIT $4
        """;

    public static async Task<List<PgWaitRow>> GetPgWaitStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgWaitRow>();
        await using var command = postgres.CreateCommand(PgWaitStatsSql);
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgWaitRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4))));
        }

        return rows;
    }
}
