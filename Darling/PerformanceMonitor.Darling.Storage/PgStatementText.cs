/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The <c>(server_id, queryid) → statement text</c> store for PostgreSQL targets (#2219), so
/// <c>get_pg_top_queries</c> returns something a human can read.
///
/// <para><b>The gap.</b> <c>pg_statement_stats</c> identifies queries by <c>queryid</c> and stores no text —
/// <c>aurora_stat_statements</c>'s <c>showtext</c> is a real per-collection cost and normalized text is highly
/// repetitive, so storing it per snapshot would be almost entirely duplication. But <c>queryid</c> is NOT stable
/// across a major version upgrade, so afterwards the stored history joins to nothing readable: a list of integers
/// that used to be your slowest queries. Keying text on <c>(server_id, queryid)</c> preserves the OLD ids' text
/// when the live view re-keys, which no on-demand fetch can recover — the live view no longer has the entry, and
/// anything else reading <c>pg_stat_statements</c> on the instance may have reset it (pganalyze's collector calls
/// <c>pg_stat_statements_reset()</c> on a size budget).</para>
///
/// <para><b>Written here rather than by a collector, on purpose.</b> The collector framework writes append-only
/// binary COPY into a hypertable, once per collection — which is exactly the duplication being avoided. This is
/// an UPSERT of one row per statement, so it takes the shape <see cref="QueryStorePlanMap"/> established for
/// content keyed to facts rather than collected as facts: a plain table, a bespoke write path, pruned on
/// <c>last_seen</c>. Being outside the catalog also keeps it out of the generated-schema ladder diff, which
/// compares rungs against <c>PgSchemaGenerator</c> output for collector tables only.</para>
///
/// <para><b>Idempotent by construction, which is what makes the cadence a free choice.</b> Every fetch upserts
/// the same rows, so re-fetching costs one statement and no growth — there is no "which queryids do I already
/// have" bookkeeping to get wrong, and no watermark to corrupt. The cadence therefore only trades freshness
/// against the <c>showtext</c> cost, and <see cref="IsDueSql"/> asks the STORE when it last wrote rather than
/// keeping state in the service, so a restart cannot re-fetch the fleet.</para>
/// </summary>
public static class PgStatementText
{
    /// <summary>The table. Not a hypertable: one row per statement per server, near-static once a workload is
    /// warm, so it is dimension-shaped and pruned on <see cref="LastSeenColumn"/> rather than by drop_chunks.</summary>
    public const string TableName = "collect.pg_statement_text";

    /// <summary>The liveness column the prune sweeps and every upsert refreshes.</summary>
    public const string LastSeenColumn = "last_seen";

    /// <summary>
    /// How often text is re-fetched for a server. One hour, and the reasoning is a cost trade rather than a
    /// preference: <c>showtext</c> is paid for the WHOLE <c>aurora_stat_statements</c> call regardless of how few
    /// rows want text, so the only lever is cadence — and the thing being collected barely changes, because a
    /// statement's normalized text is a property of its <c>queryid</c>. Hourly means a newly-appeared statement
    /// is unreadable for at most an hour, against 24 calls per server per day.
    /// </summary>
    public static readonly TimeSpan RefreshInterval = TimeSpan.FromHours(1);

    public const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.pg_statement_text (
    server_id integer NOT NULL,
    queryid bigint NOT NULL,
    query_text text NOT NULL,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, queryid)
);
CREATE INDEX IF NOT EXISTS idx_pg_statement_text_last_seen ON collect.pg_statement_text(last_seen);";

    /// <summary>
    /// The text fetch, against the monitored PostgreSQL server. <c>$1</c> caps the rows.
    ///
    /// <para><c>showtext = true</c> is the entire point of this query and the reason it is separate from
    /// <c>pg_statement_stats</c>'s: that one passes <c>false</c> every minute and must keep doing so. Ordered by
    /// <c>total_exec_time</c> descending and capped, so if a catalog holds more statements than the cap the text
    /// that lands is the text for the queries anyone would look at — a truncation that keeps the useful half
    /// rather than an arbitrary one.</para>
    ///
    /// <para>Aliased explicitly, per the house rule: an unaliased expression comes back named after the function
    /// and the query stops being debuggable in psql, which is the one tool anyone reaches for. <c>toplevel</c> is
    /// filtered rather than grouped — a nested statement shares its parent's text and would only duplicate the
    /// row it upserts into.</para>
    /// </summary>
    public const string FetchSql = AuroraFetchSql;

    /// <summary>
    /// Aurora's extended function. Kept as its own constant now that there are two sources (#2651).
    /// </summary>
    public const string AuroraFetchSql = @"
SELECT
    s.queryid AS queryid,
    s.query AS query_text
FROM aurora_stat_statements(true) AS s
WHERE s.queryid IS NOT NULL
AND   s.query IS NOT NULL
AND   s.toplevel
ORDER BY s.total_exec_time DESC
LIMIT $1";

    /// <summary>
    /// The vanilla view, for every PostgreSQL that is not Aurora (#2651).
    ///
    /// <para>Without this the table is never populated off Aurora, and two things fail SILENTLY:
    /// <c>get_pg_top_queries</c> returns <c>query_text: null</c> on every row forever — while the field's
    /// own documentation says null means "not captured YET", which is true on Aurora and a lie here — and
    /// <c>test_hypothetical_index</c> (#2612) cannot resolve a statement at all, so it answers "no
    /// statement text is stored" and blames a refresh cadence for a missing source.</para>
    ///
    /// <para>Strictly simpler than the Aurora arm: the two columns this needs are the two both sources
    /// have, so there is no NULL-filling and no ordinal to keep in step.</para>
    /// </summary>
    public const string VanillaFetchSql = @"
SELECT
    queryid,
    query_text
FROM
(
    /* DISTINCT ON, and it is not tidiness. pg_stat_statements keys on
       (queryid, userid, dbid, toplevel), so ONE queryid comes back once per user and database that ran
       it - and the upsert below keys on (server_id, queryid), which meets those duplicates as
       '21000: ON CONFLICT DO UPDATE command cannot affect row a second time' and abandons the whole
       batch. Measured: the first cut of this shipped without it and the refresh failed every cycle,
       logging and storing nothing, which is a quieter failure than it sounds because the caller
       deliberately treats a text-refresh error as non-fatal.

       Any of the duplicates would do - the text is the normalized statement and is identical across
       them - so the costliest is taken, which is the same thing the outer ORDER BY is choosing for. */
    SELECT DISTINCT ON (s.queryid)
        s.queryid AS queryid,
        s.query AS query_text,
        s.total_exec_time AS total_exec_time
    FROM public.pg_stat_statements AS s
    WHERE s.queryid IS NOT NULL
    AND   s.query IS NOT NULL
    AND   {TOPLEVEL}
    ORDER BY s.queryid, s.total_exec_time DESC
) AS d
ORDER BY d.total_exec_time DESC
LIMIT $1";

    /// <summary>
    /// The fetch for a target, by flavor.
    ///
    /// <para><c>toplevel</c> arrived in <c>pg_stat_statements</c> 1.9 (PostgreSQL 14). Before that nested
    /// tracking did not exist, so every row IS top level and <c>true</c> is the correct predicate rather
    /// than a fallback — the same guard <c>PgStatementStatsCollector</c> already applies, and without it
    /// the query fails on a 13 target instead of degrading.</para>
    /// </summary>
    public static string FetchSqlFor(bool isAurora, int postgresMajorVersion)
        => isAurora
            ? AuroraFetchSql
            : VanillaFetchSql.Replace(
                "{TOPLEVEL}",
                postgresMajorVersion >= 14 ? "s.toplevel" : "true",
                System.StringComparison.Ordinal);

    /// <summary>
    /// Whether this server is due a text fetch — asked of the STORE rather than remembered in the service, so a
    /// restart does not re-fetch the whole fleet and two hosts cannot disagree about when they last wrote.
    ///
    /// <para>Returns true when the server has no rows at all (the first fetch) or its newest row is older than
    /// the interval. <c>$2</c> is the caller's naive-UTC now, passed in rather than read from <c>now()</c> so the
    /// decision uses the same clock as the timestamps it writes — mixing the store's clock with the service's is
    /// how a cadence check drifts by exactly the offset nobody measures.</para>
    /// </summary>
    public const string IsDueSql = @"SELECT COALESCE(
    (SELECT max(last_seen) FROM collect.pg_statement_text WHERE server_id = $1) < $2::timestamp,
    TRUE) AS is_due";

    /// <summary>
    /// Upserts a batch. <c>first_seen</c> is preserved on conflict — it records when this statement shape was
    /// first seen on this server, which is the one fact that survives a major-version re-key and cannot be
    /// recovered afterwards; overwriting it would quietly turn every row's age into "since the last fetch".
    ///
    /// <para><c>query_text</c> IS advanced on conflict. A <c>queryid</c> is derived from the parse tree so its
    /// text is stable in practice, but not by guarantee across versions — and if it ever differs, the newer text
    /// is the one that matches the stats being collected now.</para>
    ///
    /// <para>Ordered by the conflict key for the #1801 reason <see cref="QueryStorePlanMap.UpsertSql"/> gives:
    /// concurrent batch upserts taking row locks in different relative orders deadlock, and this runs per server
    /// across a fleet. Cheap to keep, expensive to rediscover.</para>
    /// </summary>
    public const string UpsertSql = @"INSERT INTO collect.pg_statement_text
    (server_id, queryid, query_text, first_seen, last_seen)
SELECT server_id, queryid, query_text, stamped, stamped
FROM unnest($1::integer[], $2::bigint[], $3::text[], $4::timestamp[])
     AS batch(server_id, queryid, query_text, stamped)
ORDER BY server_id, queryid
ON CONFLICT (server_id, queryid) DO UPDATE SET
    query_text = EXCLUDED.query_text,
    last_seen = EXCLUDED.last_seen
WHERE EXCLUDED.last_seen >= pg_statement_text.last_seen";

    /// <summary>
    /// Strips the <see cref="DateTimeKind"/> before binding to any <c>::timestamp</c> parameter here — the #1969
    /// trap, and it is silent: Npgsql infers <c>timestamptz</c> from a Utc or Local Kind, PostgreSQL then
    /// converts into the session zone on the way into a naive column, and the row lands at the wrong hour with no
    /// error. For <c>last_seen</c> that means text ageing out ahead of the facts that reference it, which is the
    /// silently-missing-text outcome this design exists to prevent, arrived at through a timezone.
    /// </summary>
    public static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>
    /// Days of margin the prune adds past the fact-retention horizon, so text outlives the statistics rows that
    /// reference it. Strictly greater than zero for the reason <see cref="QueryStorePlanMap.MarginOrderingHolds"/>
    /// explains for its own pair: the two bad end-states are not symmetric. Text kept past its facts is some dead
    /// bytes; facts kept past their text is a reader resolving a live row to nothing, which is the failure this
    /// whole table exists to prevent.
    /// </summary>
    public const int PruneMarginDays = 2;

    /// <summary>
    /// Retires text whose statements have all aged out: an index range scan on <see cref="LastSeenColumn"/>,
    /// time-sliced like every sibling purge so one sweep cannot take an unbounded lock.
    ///
    /// <para>Timestamp-driven, NOT an anti-join against <c>pg_statement_stats</c>. The anti-join is the cost this
    /// shape avoids, and it is unnecessary because every fetch refreshes <c>last_seen</c> for everything still in
    /// the target's <c>pg_stat_statements</c> — so a statement that falls out of the view stops being touched and
    /// ages out on its own.</para>
    /// </summary>
    public static string PruneSql(int chunkIntervalDays) =>
        "DELETE FROM collect.pg_statement_text WHERE " + LastSeenColumn + " < $1" +
        " AND " + LastSeenColumn + " >= (SELECT min(" + LastSeenColumn + ") FROM collect.pg_statement_text WHERE " +
        LastSeenColumn + " < $1)" +
        " AND " + LastSeenColumn + " < (SELECT min(" + LastSeenColumn + ") FROM collect.pg_statement_text WHERE " +
        LastSeenColumn + " < $1) + INTERVAL '" +
        chunkIntervalDays.ToString(CultureInfo.InvariantCulture) + " days'";
}
