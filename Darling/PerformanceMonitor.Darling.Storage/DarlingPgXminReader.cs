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
/// Reads what is holding back the xmin horizon (<c>pg_xmin_horizon</c>), by cause.
/// </summary>
public static class DarlingPgXminReader
{
    public sealed record PgXminRow(
        string Source,
        DateTime MeasuredAt,
        long XminAge,
        string? Holder,
        string? Detail,
        bool IsWinner,
        long PeakXminAge,
        long SamplesAsWinner,
        long Samples);

    /// <summary>
    /// The current holder per source, joined to that source's behaviour across the window.
    /// <para>The window columns are what separate a chronic holder from a transient one, and that
    /// distinction changes the response. A slot that won 58 of 60 samples is a standing problem someone
    /// needs to own; a session that won twice is a query that ran long and finished. Reporting only the
    /// current state would make those two look the same, and reporting only the window would hide which
    /// one is holding the horizon right now.</para>
    /// <para>Both branches read the same window, so a source present in one is present in the other and
    /// the join cannot drop a row.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string PgXminHorizonSql = """
        WITH latest AS (
            SELECT DISTINCT ON (source)
                source, collection_time, xmin_age, holder, detail, is_winner
            FROM pg_xmin_horizon
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY source, collection_time DESC
        ),
        window_stats AS (
            SELECT
                source,
                MAX(xmin_age) AS peak_xmin_age,
                COUNT(*) FILTER (WHERE is_winner) AS samples_as_winner,
                COUNT(*) AS samples
            FROM pg_xmin_horizon
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY source
        )
        SELECT
            l.source,
            l.collection_time,
            l.xmin_age,
            l.holder,
            l.detail,
            l.is_winner,
            w.peak_xmin_age,
            w.samples_as_winner,
            w.samples
        FROM latest AS l
        JOIN window_stats AS w ON w.source = l.source
        ORDER BY l.xmin_age DESC
        """;

    public static async Task<List<PgXminRow>> GetPgXminHorizonAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<PgXminRow>();
        await using var command = postgres.CreateCommand(PgXminHorizonSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the BIND, per the store's naive-UTC discipline: a Kind=Utc DateTime makes
           Npgsql infer timestamptz, and PostgreSQL then resolves the comparison against these naive
           timestamp columns by converting THEM at the store session's TimeZone - east of UTC every fresh
           row falls out of the window and the read silently returns nothing. Hidden by UTC-hosted test
           stores; found by the round-2 review. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgXminRow(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                !reader.IsDBNull(5) && reader.GetBoolean(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8)));
        }

        return rows;
    }
}
