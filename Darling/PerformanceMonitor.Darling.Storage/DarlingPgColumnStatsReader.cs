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
using PerformanceMonitor.Collectors;

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
/// <para><b>Zero rows has several causes and they are not the same.</b> <c>pg_stats</c> filters on
/// <c>has_column_privilege</c>, so a monitoring role without SELECT on a table sees nothing for it — and
/// row-level security empties the view too, while a target with no table above the collector's size floor
/// has nothing to read in the first place. Neither is an absence of problems. Callers do not have to say
/// which: <see cref="GetCoverageEvidenceAsync"/> reads the evidence and
/// <see cref="PgColumnStatsCoverage.Classify"/> selects the arm, so the WPF panel and the MCP tool cannot
/// disagree about what an empty means (#3154).</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgColumnStatsReader
{
    /// <param name="NDistinct">NEGATIVE IS A RATIO of row count, not a count. See the type header.</param>
    /// <param name="TopValueFrequency">Share of the table held by the single most common value. The
    /// parameter-sensitivity signal; carries no value itself, by design.</param>
    public sealed record PgColumnStatRow(
        string? DatabaseName,
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

       database_name LEADS the distinct key (#2599). It is not decorative: this collector runs once per
       database, so without it two databases sharing a schema collapse to one row here and the newest
       collection_time silently decides which database the grid is describing.

       NULLS LAST on both ranking keys: a column with no MCV list has no skew to report, and sorting NULL
       first would put the least informative rows at the top of a grid whose whole job is to rank. */
    public const string PgColumnStatsSql = """
        SELECT database_name, schema_name, table_name, column_name,
               n_distinct, null_frac, avg_width, correlation,
               top_value_frequency, common_value_count, collection_time
        FROM (
            SELECT DISTINCT ON (database_name, schema_name, table_name, column_name)
                   database_name, schema_name, table_name, column_name,
                   n_distinct, null_frac, avg_width, correlation,
                   top_value_frequency, common_value_count, collection_time
            FROM pg_column_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, schema_name, table_name, column_name, collection_time DESC
        ) AS latest
        ORDER BY top_value_frequency DESC NULLS LAST,
                 abs(correlation) ASC NULLS LAST,
                 database_name, schema_name, table_name, column_name
        LIMIT $4
        """;

    /// <summary>
    /// What <c>pg_table_bloat_stats</c> recorded about this server in this window: whether it ran at all,
    /// how many tables clear <see cref="PgColumnStatsCollector.MinimumRelPages"/>, and how many of those had
    /// every column's <c>pg_stats</c> row readable by the monitoring login. The three inputs
    /// <see cref="PgColumnStatsCoverage.Classify"/> needs, and nothing else.
    /// </summary>
    /// <param name="EvidenceRuns">
    /// <c>pg_table_bloat_stats</c> runs logged for this server in the window — a RUN count, not a row count,
    /// and that distinction is the whole reason this field exists. A run that stored no rows is the
    /// measurement establishing that nothing on the target clears the floor; collapsing the two would make
    /// "measured, and everything is small" indistinguishable from "never measured here".
    /// </param>
    public readonly record struct PgColumnStatsCoverageEvidence(
        int EvidenceRuns,
        int CandidateTables,
        int TablesWithVisibleStatistics);

    /* Both counts come from the LATEST measurement of each table in the window, which is why the derived
       table exists at all: pg_table_bloat_stats is hourly and per-database, so a plain aggregate over the
       window would count one table once per hour and report a fleet-sized candidate count for a schema
       holding twelve tables.

       estimate_unavailable is the pg_stats visibility signal, and it is read in its SOUND direction only:
       false cannot happen unless every column of that table had a pg_stats row this login could read, so
       count(... NOT estimate_unavailable) is a floor on visibility rather than an estimate of it. True has
       three other causes (a name-typed column, reltuples < 0, a null page estimate), which is why the arm
       built on zero-visible names the privilege filter as the cause and says what else the reading admits
       instead of asserting a denied grant.

       heap_pages is pg_class.relpages - the same column, on the same catalog, that the collector's own
       WHERE filters on - so the candidate count is the collector's own predicate re-evaluated, not a
       size-based approximation of it. The 1 MB floor pg_table_bloat_stats collects at is a different
       measure (pg_relation_size), so the page filter here is not redundant with it: measured on the fleet,
       1,263 of 1,264 tables past the byte floor also clear the page floor, and the one that does not is a
       table this collector would genuinely skip.

       The run count is a scalar subquery rather than a join, so a server whose bloat collector ran and
       stored NOTHING still reports its run - a join would drop exactly the row that distinguishes the
       size-floor arm from the no-evidence one. $1 server_id, $2 window start, $3 window end. */
    public static readonly string CoverageEvidenceSql = @"
SELECT
    (
        SELECT count(*)
        FROM collection_log
        WHERE server_id = $1
        AND   collector_name = 'pg_table_bloat_stats'
        AND   collection_time >= $2
        AND   collection_time <= $3
    )::int                                                              AS evidence_runs,
    count(*) FILTER (WHERE latest.heap_pages >= " + PgColumnStatsCollector.MinimumRelPages + @")::int
                                                                        AS candidate_tables,
    count(*) FILTER (WHERE latest.heap_pages >= " + PgColumnStatsCollector.MinimumRelPages + @"
                     AND   NOT latest.estimate_unavailable)::int        AS visible_tables
FROM (
    SELECT DISTINCT ON (database_name, schema_name, table_name)
           heap_pages, estimate_unavailable
    FROM pg_table_bloat_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    ORDER BY database_name, schema_name, table_name, collection_time DESC
) AS latest";

    /// <summary>
    /// The coverage verdict for a <c>pg_column_stats</c> read — which arm produced
    /// <paramref name="storedColumnRows"/>, and the sentence saying so.
    ///
    /// <para>A failed evidence read answers <see cref="PgColumnStatsCoverageArm.Undetermined"/> with its own
    /// wording rather than throwing, the same rule <c>DarlingRuntimePrecondition</c> follows: this runs to
    /// EXPLAIN a result the caller already has, and turning that into a read error would replace an
    /// under-described answer with no answer.</para>
    /// </summary>
    public static async Task<PgColumnStatsCoverageVerdict> GetCoverageVerdictAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int storedColumnRows,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        try
        {
            var evidence = await GetCoverageEvidenceAsync(
                postgres, serverId, EvidenceStart(startUtc, endUtc), endUtc, cancellationToken);

            return PgColumnStatsCoverage.Classify(
                evidence.EvidenceRuns,
                evidence.CandidateTables,
                evidence.TablesWithVisibleStatistics,
                storedColumnRows);
        }
        catch (Exception)
        {
            return PgColumnStatsCoverage.EvidenceUnreadable(storedColumnRows);
        }
    }

    /// <summary>
    /// The shortest evidence lookback that can answer the coverage question, whatever window the CALLER
    /// asked its data over.
    ///
    /// <para><b>The read's own window is the wrong window for this, and using it would manufacture
    /// <see cref="PgColumnStatsCoverageArm.Undetermined"/> on servers whose answer is known.</b> Whether the
    /// monitoring login can read <c>pg_stats</c> is a CURRENT state of the target, not a property of the
    /// interval somebody happened to select in the viewer. <c>pg_table_bloat_stats</c> collects hourly, so a
    /// one-hour panel window straddles zero or one of its runs, and a zero would report "no evidence" for a
    /// target measured 167 times in the past week. Same reasoning as
    /// <c>CollectorRuntimePrecondition</c>, which consults the LATEST run rather than a window for exactly
    /// this: a precondition is a state, and a window is a question about history.</para>
    ///
    /// <para>A day rather than no lower bound at all, because the store table is a hypertable and an
    /// unbounded scan would read every chunk of a 90-day retention to answer a diagnostic. A day is the
    /// SUBJECT collector's own cadence — <c>pg_column_stats</c> runs daily, so evidence older than its last
    /// run could describe a grant that has since changed, and evidence newer than a day is guaranteed to
    /// exist wherever the hourly collector is running at all.</para>
    ///
    /// <para>Anchored on <paramref name="endUtc"/>, so an <c>as_of</c> read gets the evidence contemporary
    /// with the data it is explaining rather than today's.</para>
    /// </summary>
    internal static DateTime EvidenceStart(DateTime startUtc, DateTime endUtc)
    {
        var floor = endUtc.AddHours(-MinimumEvidenceHours);

        /* The WIDER of the two. A caller asking about a month gets a month - narrowing to a day there would
           throw away measurements of tables the read's own rows come from. */
        return startUtc < floor ? startUtc : floor;
    }

    /// <summary>
    /// How far back <see cref="EvidenceStart"/> looks when the caller's window is shorter. Named so the
    /// relationship to the subject collector's cadence is assertable rather than a number in a call.
    /// </summary>
    internal const int MinimumEvidenceHours = 24;

    /// <summary>The raw evidence, for callers that want the counts rather than the sentence.</summary>
    public static async Task<PgColumnStatsCoverageEvidence> GetCoverageEvidenceAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        await using var command = postgres.CreateCommand(CoverageEvidenceSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the bind, for the reason the row read below documents: Kind=Utc
           infers timestamptz and the comparison against these naive columns then resolves at the store
           session's TimeZone, which east of UTC slides the window off the data. An evidence read that
           silently returned nothing would answer Undetermined on a server that has the evidence. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return new PgColumnStatsCoverageEvidence(0, 0, 0);
        }

        return new PgColumnStatsCoverageEvidence(
            EvidenceRuns: reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
            CandidateTables: reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            TablesWithVisibleStatistics: reader.IsDBNull(2) ? 0 : reader.GetInt32(2));
    }

    public static async Task<List<PgColumnStatRow>> GetPgColumnStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgColumnStatRow>();
        await using var command = postgres.CreateCommand(PgColumnStatsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
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
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                ColumnName: reader.IsDBNull(3) ? null : reader.GetString(3),
                NDistinct: reader.IsDBNull(4) ? null : reader.GetDouble(4),
                NullFrac: reader.IsDBNull(5) ? null : reader.GetDouble(5),
                AvgWidth: reader.IsDBNull(6) ? null : reader.GetInt32(6),
                Correlation: reader.IsDBNull(7) ? null : reader.GetDouble(7),
                TopValueFrequency: reader.IsDBNull(8) ? null : reader.GetDouble(8),
                CommonValueCount: reader.IsDBNull(9) ? null : reader.GetInt32(9),
                CaptureTime: reader.IsDBNull(10)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(10), DateTimeKind.Utc)));
        }

        return rows;
    }
}
