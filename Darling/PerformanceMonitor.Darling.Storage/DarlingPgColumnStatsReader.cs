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
    /// <param name="EvidenceCollectorRan">
    /// Whether <c>pg_table_bloat_stats</c> RAN for this server in the evidence window — a run, not a row,
    /// and that distinction is the whole reason this field exists. A run that stored no rows is the
    /// measurement establishing that nothing on the target clears the floor; collapsing the two would make
    /// "measured, and everything is small" indistinguishable from "never measured here".
    ///
    /// <para>A boolean rather than a count because a count is what the decision costs, not what it needs:
    /// nothing reads how MANY times it ran, and measured against the production store an
    /// <c>EXISTS</c> probe answers in 6 ms where <c>count(*)</c> over the same predicate takes 152 —
    /// <c>collection_log</c> has no index on (server_id, collector_name), so counting means a bitmap heap
    /// scan that discarded 14,060 rows to keep 22. Proving ABSENCE is the worst case for a short-circuit and
    /// it still measured 4.5 ms.</para>
    /// </param>
    public readonly record struct PgColumnStatsCoverageEvidence(
        bool EvidenceCollectorRan,
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

       The run probe is a scalar subquery rather than a join, so a server whose bloat collector ran and
       stored NOTHING still reports its run - a join would drop exactly the row that distinguishes the
       size-floor arm from the no-evidence one. EXISTS rather than count(*) because only the zero test is
       read, and it is 25x cheaper on this store.

       And it counts only runs that SUCCEEDED, which is not a detail: a bloat collector erroring on a
       target still writes a collection_log row, so an unfiltered probe would say "it ran" while the
       collector stored nothing - candidate_tables 0, visible 0 - and the classifier would answer
       BelowSizeFloor, "nothing to fix". That is the exact false-innocence this whole issue closes,
       reintroduced one level down in the EVIDENCE collector's own health. The status set is read from
       EnumeratedCollectorDriver.FreshnessSuccessStatuses rather than retyped, so it is the same bar the
       freshness reads and the self-alert evaluator apply, and a status added there propagates here.

       $1 server_id, $2 window start, $3 window end. */
    /// <summary>
    /// <see cref="EnumeratedCollectorDriver.FreshnessSuccessStatuses"/> as a SQL <c>IN</c> list. Built from
    /// the shared list rather than retyped so the bar for "this collector produced valid evidence" is the
    /// one the freshness reads and the self-alert evaluator already use, and so a status added there reaches
    /// this probe without anybody remembering to come here.
    ///
    /// <para>Literal-safe by construction: every element is a compile-time constant in this repo's own
    /// source, never operator input.</para>
    /// </summary>
    private static readonly string EvidenceStatusList = string.Join(
        ", ",
        EnumeratedCollectorDriver.FreshnessSuccessStatuses.Select(status => "'" + status + "'"));

    public static readonly string CoverageEvidenceSql = @"
SELECT
    (
        SELECT EXISTS (
            SELECT 1
            FROM collection_log
            WHERE server_id = $1
            AND   collector_name = 'pg_table_bloat_stats'
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   status IN (" + EvidenceStatusList + @")
        )
    )                                                                   AS evidence_collector_ran,
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
    ///
    /// <para><b>No window START, deliberately, and the signature says so by not having one.</b> The evidence
    /// span is <see cref="EvidenceStart"/> to <paramref name="endUtc"/> whatever the caller read its rows
    /// over — see that method for why. An accepted-but-ignored <c>startUtc</c> is worse than an absent one:
    /// the caller passes a window, reasonably believes it is honoured, and neither the compiler nor the
    /// answer tells them otherwise. Callers that want the raw counts over a span of their own choosing have
    /// <see cref="GetCoverageEvidenceAsync"/>, which takes both ends and uses both.</para>
    /// </summary>
    /// <param name="endUtc">The instant the caller's read ENDS at, which the evidence window is anchored
    /// on so an <c>as_of</c> read is explained by contemporary evidence rather than by today's.</param>
    public static async Task<PgColumnStatsCoverageVerdict> GetCoverageVerdictAsync(
        NpgsqlDataSource postgres, int serverId, DateTime endUtc, int storedColumnRows,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        try
        {
            var evidence = await GetCoverageEvidenceAsync(
                postgres, serverId, EvidenceStart(endUtc), endUtc, cancellationToken);

            return PgColumnStatsCoverage.Classify(
                evidence.EvidenceCollectorRan,
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
    /// The evidence lookback: a FIXED span ending where the caller's read ends, independent of how wide a
    /// window the caller asked its data over.
    ///
    /// <para><b>The read's own window is the wrong window for this, in both directions.</b> A one-hour panel
    /// window straddles zero or one run of the hourly evidence collector, so it would manufacture
    /// <see cref="PgColumnStatsCoverageArm.Undetermined"/> for a target measured 167 times in the past week.
    /// And a wide one is no better: the MCP tool defaults to 168 hours, where the census measured 242 ms
    /// against 41 ms over a day — a diagnostic that runs on every call, empty or not, paying six times over
    /// for history it does not use.</para>
    ///
    /// <para><b>Because it is not a question about history.</b> Whether the monitoring login can read
    /// <c>pg_stats</c>, and whether anything on the target clears the size floor, are CURRENT states — and
    /// <c>CollectorRuntimePrecondition</c> settled this shape already: it consults the LATEST run rather than
    /// a window, on the reasoning that a precondition somebody has since satisfied must not keep being
    /// reported. Averaging a month of candidate counts would answer a question nobody asked and cost more to
    /// do it.</para>
    ///
    /// <para>A day rather than the latest row alone, because the store table is a hypertable and the count
    /// is over DISTINCT tables — so it needs a span, and a day both bounds the chunks scanned and is the
    /// SUBJECT collector's own cadence: <c>pg_column_stats</c> runs daily, so this is the evidence
    /// contemporary with the run being explained, and it is guaranteed to exist wherever the hourly
    /// collector is running at all.</para>
    ///
    /// <para>Anchored on <paramref name="endUtc"/>, so an <c>as_of</c> read gets the evidence contemporary
    /// with the data it is explaining rather than today's.</para>
    /// </summary>
    /// <para>The span itself lives on <see cref="PgColumnStatsCoverage.EvidenceHours"/>, not here: it
    /// appears in the census an operator reads, so the label and the query have to be the same figure and a
    /// copy in this file is how they stop being.</para>
    internal static DateTime EvidenceStart(DateTime endUtc) =>
        endUtc.AddHours(-PgColumnStatsCoverage.EvidenceHours);

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
            return new PgColumnStatsCoverageEvidence(false, 0, 0);
        }

        return new PgColumnStatsCoverageEvidence(
            EvidenceCollectorRan: !reader.IsDBNull(0) && reader.GetBoolean(0),
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
