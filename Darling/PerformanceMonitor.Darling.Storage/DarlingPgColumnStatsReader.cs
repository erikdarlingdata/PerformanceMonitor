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
/// Reads the stored per-column planner statistics (<c>pg_column_stats</c>, #2543) — the LATEST row per
/// column, ranked by how likely that column is to be producing a bad estimate.
///
/// <para><b>Latest per column, not the history.</b> These change only when <c>ANALYZE</c> runs, so a window
/// holds the same answer repeated daily. The history exists so somebody can see that <c>n_distinct</c> moved
/// on the day a plan changed shape, which is a different question and a different read.</para>
///
/// <para><b>Ranked by suspicion, not alphabetically.</b> A schema has thousands of columns and almost all of
/// them are fine. Two shapes cause most misestimates, so they sort first:</para>
///
/// <list type="number">
/// <item><b>Heavy skew</b> — <c>top_value_frequency</c> high means one value dominates, so a plan that suits
/// most parameter values is catastrophic for that one. This is the PostgreSQL analogue of parameter
/// sniffing, and it is the reason the frequency is collected at all.</item>
/// <item><b>Low correlation on a wide column</b> — near-zero correlation is why an index scan was rejected
/// on a column that "obviously" has an index.</item>
/// </list>
///
/// <para><b><c>NDistinct</c> must be read with its sign.</b> Negative is a RATIO of the row count, not a
/// quantity: <c>-1</c> means distinct ≈ every row. A caller that formats it as a count will print
/// "-1 distinct values" on the commonest possible column, a unique key.</para>
///
/// <para><b>Zero rows has two causes and they are not the same.</b> <c>pg_stats</c> filters on
/// <c>has_column_privilege</c>, so a monitoring role without SELECT on a table sees nothing for it — and
/// row-level security empties the view too. Neither is an absence of problems, and a caller must say so
/// rather than reporting healthy statistics.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgColumnStatsReader
{
    /// <param name="NDistinct">NEGATIVE IS A RATIO of row count, not a count. See the type header.</param>
    /// <param name="TopValueFrequency">Share of the table held by the single most common value. The
    /// parameter-sensitivity signal; carries no value itself, by design.</param>
    public sealed record PgColumnStatRow(
        string? SchemaName,
        string? TableName,
        string? ColumnName,
        double? NDistinct,
        double? NullFrac,
        int? AvgWidth,
        double? Correlation,
        double? TopValueFrequency,
        int? CommonValueCount,
        DateTime CaptureTime);

    /* DISTINCT ON the column identity ordered by collection_time DESC gives the newest row per column in one
       pass - the standard PostgreSQL idiom, cheaper than a correlated MAX per column on a hypertable.

       The outer ORDER BY re-sorts by suspicion, which the inner one cannot do: DISTINCT ON requires its
       ORDER BY to lead with the distinct key, so picking the newest and ranking by interest are two
       different sorts and need the subquery. Same shape as the readiness and extension readers.

       NULLS LAST on both ranking keys: a column with no MCV list has no skew to report, and sorting NULL
       first would put the least informative rows at the top of a grid whose whole job is to rank. */
    public const string PgColumnStatsSql = """
        SELECT schema_name, table_name, column_name,
               n_distinct, null_frac, avg_width, correlation,
               top_value_frequency, common_value_count, collection_time
        FROM (
            SELECT DISTINCT ON (schema_name, table_name, column_name)
                   schema_name, table_name, column_name,
                   n_distinct, null_frac, avg_width, correlation,
                   top_value_frequency, common_value_count, collection_time
            FROM pg_column_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY schema_name, table_name, column_name, collection_time DESC
        ) AS latest
        ORDER BY top_value_frequency DESC NULLS LAST,
                 abs(correlation) ASC NULLS LAST,
                 schema_name, table_name, column_name
        LIMIT $4
        """;

    public static async Task<List<PgColumnStatRow>> GetPgColumnStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgColumnStatRow>();
        await using var command = postgres.CreateCommand(PgColumnStatsSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgColumnStatRow(
                SchemaName: reader.IsDBNull(0) ? null : reader.GetString(0),
                TableName: reader.IsDBNull(1) ? null : reader.GetString(1),
                ColumnName: reader.IsDBNull(2) ? null : reader.GetString(2),
                NDistinct: reader.IsDBNull(3) ? null : reader.GetDouble(3),
                NullFrac: reader.IsDBNull(4) ? null : reader.GetDouble(4),
                AvgWidth: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                Correlation: reader.IsDBNull(6) ? null : reader.GetDouble(6),
                TopValueFrequency: reader.IsDBNull(7) ? null : reader.GetDouble(7),
                CommonValueCount: reader.IsDBNull(8) ? null : reader.GetInt32(8),
                CaptureTime: reader.IsDBNull(9)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(9), DateTimeKind.Utc)));
        }

        return rows;
    }
}
