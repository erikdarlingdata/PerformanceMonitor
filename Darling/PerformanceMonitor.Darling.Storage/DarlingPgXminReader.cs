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

    /// <summary>
    /// How many times the xmin collector actually CAPTURED in the window — the honest denominator for
    /// "what share of the window was this source winning" (#3541 A12, contract rule 5).
    /// <para><see cref="PgXminHorizonSql"/>'s <c>samples</c> counts a source's OWN rows, and the collector
    /// writes a row only when something holds the horizon — an unheld capture stores nothing. So a source
    /// that held the horizon in 2 of the window's 288 captures had <c>samples = 2</c>,
    /// <c>samples_as_winner = 2</c>, and read as winning 100% of the window: a two-minute query rendered as
    /// a chronic holder. The denominator has to be every time the collector LOOKED, and only
    /// <c>collection_log</c> has that: one row per run INCLUDING the zero-row (healthy, unheld) runs, behind
    /// its <c>(server_id, collection_time)</c> index, counting the exact collector whose captures are being
    /// fractioned. Runs that stored nothing because they could not look (ERROR / ABANDONED / PERMISSIONS /
    /// YIELDED) are excluded: a cycle that did not look is not evidence the horizon was clear.</para>
    /// <para>This is the SAME denominator the alert evaluator's horizon arm uses
    /// (<c>DarlingPostgresAlertReadAdapter.XminSql</c>'s <c>captures</c> CTE, #3537): same table, same
    /// collector name, same SUCCESS filter — pinned equal by DarlingPgXminReaderTests so the MCP payload and
    /// the alert can never fraction the same window over different denominators. A separate statement rather
    /// than a CROSS JOIN onto the holder rows because the viewer renders <see cref="PgXminRow"/> field for
    /// field and this is a window fact, not a row fact. The log write is failure-isolated and can skip a
    /// row, so the count may UNDERCOUNT — the payload says so rather than clamping the share.</para>
    /// <para>$1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string XminCapturesInWindowSql = """
        SELECT COUNT(*) AS captures_in_window
        FROM collection_log
        WHERE server_id = $1
        AND   collector_name = 'pg_xmin_horizon'
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   status = 'SUCCESS'
        """;

    /// <summary>Runs <see cref="XminCapturesInWindowSql"/>.</summary>
    public static async Task<long> GetXminCapturesInWindowAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(XminCapturesInWindowSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* Kind-Unspecified at the bind, for the reason GetPgXminHorizonAsync states. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is long count ? count : Convert.ToInt64(value);
    }

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
