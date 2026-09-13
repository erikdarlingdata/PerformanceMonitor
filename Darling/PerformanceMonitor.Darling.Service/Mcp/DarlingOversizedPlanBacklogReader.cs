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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over <c>collect.oversized_plan_backlog</c> (#3398) — the V121 worklist of cached plans
/// <see cref="QueryPlanXmlCaptureLimits"/> declined to capture, and what the out-of-band sweep has since
/// done about each one.
///
/// <para><b>A worklist, not a series, so nothing here takes a window.</b> Every other self-monitoring read
/// on this surface bounds itself by time because its table is append-only telemetry. This table holds one
/// row per plan identity for as long as that plan keeps recurring, and the sweep MUTATES those rows in
/// place, so "the last 24 hours of it" is not a thing that exists. The verdict timestamps carry the time
/// axis instead: <c>last_captured_at</c> and <c>last_expired_at</c> answer "has the fetch half moved since
/// T" by comparison, which is the one question the recording half shipped without any client able to
/// ask.</para>
///
/// <para><b>The three verdicts are a strict partition of the total, and the read is written so they cannot
/// be anything else.</b> <c>captured</c> is <c>captured_at IS NOT NULL</c>; <c>expired</c> excludes a row
/// that also carries content, because a capture retires a standing expiry
/// (<see cref="OversizedPlanBacklog.RecordCaptureSql"/>) and so does a re-sighting
/// (<see cref="OversizedPlanBacklog.UpsertSightingSql"/>); <c>pending</c> is the sweep's own claim
/// predicate. Every row therefore lands in exactly one bucket and <c>pending + captured + expired</c>
/// equals <c>total</c> — asserted against a live store rather than argued, because a rollup whose buckets
/// silently overlap reads as a bigger backlog than exists, and one whose buckets leave a gap reads as a
/// smaller one.</para>
///
/// <para><b><c>pending</c> is the claim's predicate, spelled the same way on purpose.</b> A read that
/// defined "still to do" independently of <see cref="OversizedPlanBacklog.ClaimSql"/> could report an empty
/// backlog while the sweep still had work, or the reverse — which is the whole class of divergence this
/// read exists to remove, reintroduced one layer up. <c>OversizedPlanBacklogReadPins</c> holds the two
/// predicates against each other and then executes both.</para>
///
/// <para><b>No size is ever derived from the stored content, and that is a cost decision as well as a
/// correctness one.</b> <c>observed_bytes</c> is the monitored server's own <c>DATALENGTH</c> over
/// <c>nvarchar(max)</c> — UTF-16 BYTES, the same unit as
/// <see cref="QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes"/>, so it is the only size figure that can
/// be compared to the cap. PostgreSQL's <c>length(plan_xml)</c> is CHARACTERS and runs exactly half of it
/// on this content, so reporting one under a byte-shaped name would put every reader a factor of two below
/// the cap they were checking against. It is also the expensive read on this table: the captured column is
/// TOASTed and compresses about 16x, measured on one production store at 525 MB logical against 32 MB of
/// <c>pg_total_relation_size</c>, so a length over the whole table detoasts half a gigabyte to produce a
/// number nobody can use. <c>plan_xml IS NOT NULL</c> reads the varlena header and detoasts nothing, which
/// is why presence is reported and size is not.</para>
///
/// <para><b><c>rows_with_content</c> is beside <c>captured</c> rather than folded into it.</b> The capture
/// statement writes the stamp and the content together, so the two counts agree on a healthy store and the
/// second column looks redundant. It is the integrity check that is otherwise unavailable: the fallback
/// reads key on <c>plan_xml IS NOT NULL</c>
/// (<see cref="OversizedPlanBacklog.QueryStatsFallbackSql"/>), so a row stamped captured with no content is
/// invisible to <c>get_plan_xml</c> while counting as done here — the same shape of silent hole the whole
/// issue is about.</para>
///
/// <para><b>LEFT JOIN to the registry, and no <c>is_enabled</c> filter.</b> Retention prunes these rows on
/// <c>last_seen_at</c> and nothing prunes them when a server leaves the registry, so an inner join or an
/// enabled-only predicate would drop rows that are really there and hand back a total that is quietly
/// short. A row whose <c>server_id</c> no longer resolves reports a null <c>server_name</c> and still
/// counts.</para>
/// </summary>
internal static class DarlingOversizedPlanBacklogReader
{
    /// <summary>The verdict a row with content carries.</summary>
    public const string VerdictCaptured = "captured";

    /// <summary>The verdict a row carries once a fetch established that its handle no longer renders a
    /// plan.</summary>
    public const string VerdictExpired = "expired";

    /// <summary>The verdict a row the sweep will still go back for carries.</summary>
    public const string VerdictPending = "pending";

    /// <summary>
    /// The sweep's claim predicate, qualified to this read's alias — the definition of "still to do" that
    /// <see cref="OversizedPlanBacklog.ClaimSql"/> selects on, restated once here and reused by every
    /// pending-shaped aggregate below so the three of them cannot drift from each other either.
    /// </summary>
    private const string PendingPredicate = "b.captured_at IS NULL AND b.expired_at IS NULL";

    /// <summary>
    /// One row per server that has any backlog at all ($1 = an optional server_id, or NULL for every
    /// server), most pending work first.
    ///
    /// <para>Ordered by pending descending because the first question of any read of this table is which
    /// server the sweep has not caught up on. Ties break on total and then on name, so the ordering is
    /// total rather than partial — a page that reshuffles between two calls on equal counts reads as data
    /// changing when nothing did.</para>
    ///
    /// <para><c>percentile_disc</c> rather than <c>percentile_cont</c>, deliberately. The discrete
    /// percentile returns a value that is actually IN the column and keeps its type, so the median is a
    /// size some plan really measured and comes back as a <c>bigint</c> in the same unit as its neighbours.
    /// The continuous one interpolates between two rows and returns <c>double precision</c>, which would
    /// need a cast to survive a two-argument <c>round</c> — PostgreSQL has no
    /// <c>round(double precision, integer)</c> — and would report a byte count no plan ever had.</para>
    /// </summary>
    public const string PerServerRollupSql = @"
SELECT
    b.server_id                                                              AS server_id,
    s.server_name                                                            AS server_name,
    count(*)                                                                 AS total_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @")                        AS pending_rows,
    count(*) FILTER (WHERE b.captured_at IS NOT NULL)                        AS captured_rows,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at IS NOT NULL)                         AS expired_rows,
    count(*) FILTER (WHERE b.plan_xml IS NOT NULL)                           AS rows_with_content,
    count(*) FILTER (WHERE b.collector_name = '" + OversizedPlanBacklog.QueryStatsCollectorName + @"')
                                                                             AS query_stats_rows,
    count(*) FILTER (WHERE b.collector_name = '" + OversizedPlanBacklog.ProcedureStatsCollectorName + @"')
                                                                             AS procedure_stats_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @"
                     AND   b.attempt_count > 0)                              AS pending_rows_attempted,
    coalesce(sum(b.attempt_count) FILTER (WHERE " + PendingPredicate + @"), 0)
                                                                             AS pending_attempts,
    coalesce(max(b.attempt_count) FILTER (WHERE " + PendingPredicate + @"), 0)
                                                                             AS max_attempts_on_a_pending_row,
    min(b.first_seen_at)                                                     AS oldest_first_seen_at,
    max(b.last_seen_at)                                                      AS last_seen_at,
    max(b.last_attempt_at)                                                   AS last_attempt_at,
    max(b.captured_at)                                                       AS last_captured_at,
    max(b.expired_at)                                                        AS last_expired_at,
    min(b.observed_bytes)                                                    AS min_observed_bytes,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY b.observed_bytes)            AS median_observed_bytes,
    max(b.observed_bytes)                                                    AS max_observed_bytes
FROM " + OversizedPlanBacklog.TableName + @" AS b
LEFT JOIN servers AS s
  ON s.server_id = b.server_id
WHERE ($1::integer IS NULL OR b.server_id = $1)
GROUP BY
    b.server_id,
    s.server_name
ORDER BY
    pending_rows DESC,
    total_rows DESC,
    b.server_id";

    /// <summary>
    /// The same scope ($1 = an optional server_id, or NULL for every server) split by COLLECTOR rather
    /// than by server — the census, and the first thing to read.
    ///
    /// <para>It exists because the two collectors' shares of this table are not predictable from each
    /// other and not stable across stores: measured the day after the V121 fleet install,
    /// <c>procedure_stats</c> held the larger half on one production store (1,111 rows against 1,095) and
    /// 3.6% of the other (199 against 5,306). A rollup that only totalled per server would let a reader
    /// carry an assumption about which collector's plans they were looking at, and that assumption is
    /// wrong on one of the two stores whichever way they guess.</para>
    /// </summary>
    public const string CollectorCensusSql = @"
SELECT
    b.collector_name                                                         AS collector_name,
    count(DISTINCT b.server_id)                                              AS servers,
    count(*)                                                                 AS total_rows,
    count(*) FILTER (WHERE " + PendingPredicate + @")                        AS pending_rows,
    count(*) FILTER (WHERE b.captured_at IS NOT NULL)                        AS captured_rows,
    count(*) FILTER (WHERE b.captured_at IS NULL
                     AND   b.expired_at IS NOT NULL)                         AS expired_rows,
    max(b.captured_at)                                                       AS last_captured_at,
    max(b.expired_at)                                                        AS last_expired_at,
    min(b.observed_bytes)                                                    AS min_observed_bytes,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY b.observed_bytes)            AS median_observed_bytes,
    max(b.observed_bytes)                                                    AS max_observed_bytes
FROM " + OversizedPlanBacklog.TableName + @" AS b
WHERE ($1::integer IS NULL OR b.server_id = $1)
GROUP BY
    b.collector_name
ORDER BY
    total_rows DESC,
    b.collector_name";

    /// <summary>
    /// One server's rows ($1 = server_id, $2 = row cap), largest plan first — the claim key, the plan's
    /// identity, its measured size and all four timestamps, which is everything needed to take a
    /// <c>query_hash</c> from here into <c>get_plan_xml</c> and exercise
    /// <see cref="OversizedPlanBacklog.QueryStatsFallbackSql"/> end to end.
    ///
    /// <para>Server-scoped rather than fleet-wide because the claim key is only meaningful within one
    /// server and the two handles are long hex strings — a fleet listing would be mostly identifiers for
    /// servers the caller did not ask about.</para>
    ///
    /// <para><b>Largest first, which is deliberately NOT the claim's order.</b> The claim takes oldest
    /// attempt first so nothing starves, and only breaks ties by size; ordering this listing the same way
    /// would make it look like a prediction of the sweep's next three picks while omitting the rows it
    /// will skip. Size descending answers the question a reader actually has — which plans the cap cost
    /// the most visibility on — and the key columns in the tiebreak keep the page stable.</para>
    /// </summary>
    public const string RowsSql = @"
SELECT
    b.collector_name                                                         AS collector_name,
    b.plan_handle                                                            AS plan_handle,
    b.sql_handle                                                             AS sql_handle,
    b.statement_start_offset                                                 AS statement_start_offset,
    b.statement_end_offset                                                   AS statement_end_offset,
    b.database_name                                                          AS database_name,
    b.query_hash                                                             AS query_hash,
    b.observed_bytes                                                         AS observed_bytes,
    b.first_seen_at                                                          AS first_seen_at,
    b.last_seen_at                                                           AS last_seen_at,
    b.captured_at                                                            AS captured_at,
    b.expired_at                                                             AS expired_at,
    b.last_attempt_at                                                        AS last_attempt_at,
    b.attempt_count                                                          AS attempt_count,
    (b.plan_xml IS NOT NULL)                                                 AS has_plan_xml
FROM " + OversizedPlanBacklog.TableName + @" AS b
WHERE b.server_id = $1
ORDER BY
    b.observed_bytes DESC,
    b.first_seen_at,
    b.plan_handle,
    b.statement_start_offset
LIMIT $2";

    /// <summary>
    /// Which bucket a row falls in, from its two verdict stamps — the row-level twin of the rollup's three
    /// FILTER predicates, so a listed row and the count it contributed to can never disagree about what it
    /// is.
    ///
    /// <para>Content wins over an expiry, matching the store: a capture clears any standing
    /// <c>expired_at</c>, so a row carrying both stamps is a row that was retired and later fetched
    /// anyway, and the useful thing to say about it is that the plan is available.</para>
    /// </summary>
    public static string VerdictOf(DateTime? capturedAt, DateTime? expiredAt) =>
        capturedAt.HasValue ? VerdictCaptured
        : expiredAt.HasValue ? VerdictExpired
        : VerdictPending;

    /// <summary>One server's backlog, rolled up.</summary>
    public sealed record BacklogServerRollup(
        int ServerId,
        string? ServerName,
        long TotalRows,
        long PendingRows,
        long CapturedRows,
        long ExpiredRows,
        long RowsWithContent,
        long QueryStatsRows,
        long ProcedureStatsRows,
        long PendingRowsAttempted,
        long PendingAttempts,
        int MaxAttemptsOnAPendingRow,
        DateTime OldestFirstSeenAt,
        DateTime LastSeenAt,
        DateTime? LastAttemptAt,
        DateTime? LastCapturedAt,
        DateTime? LastExpiredAt,
        long MinObservedBytes,
        long MedianObservedBytes,
        long MaxObservedBytes);

    /// <summary>One collector's share of the scope.</summary>
    public sealed record BacklogCollectorCensus(
        string CollectorName,
        int Servers,
        long TotalRows,
        long PendingRows,
        long CapturedRows,
        long ExpiredRows,
        DateTime? LastCapturedAt,
        DateTime? LastExpiredAt,
        long MinObservedBytes,
        long MedianObservedBytes,
        long MaxObservedBytes);

    /// <summary>One backlog row: the claim key, the plan's identity, and every stamp.</summary>
    public sealed record BacklogRow(
        string CollectorName,
        string PlanHandle,
        string SqlHandle,
        int StatementStartOffset,
        int StatementEndOffset,
        string? DatabaseName,
        string? QueryHash,
        long ObservedBytes,
        DateTime FirstSeenAt,
        DateTime LastSeenAt,
        DateTime? CapturedAt,
        DateTime? ExpiredAt,
        DateTime? LastAttemptAt,
        int AttemptCount,
        bool HasPlanXml)
    {
        /// <summary>This row's bucket — see <see cref="VerdictOf"/>.</summary>
        public string Verdict => VerdictOf(CapturedAt, ExpiredAt);
    }

    public static async Task<List<BacklogServerRollup>> GetPerServerRollupAsync(
        NpgsqlDataSource postgres, int? serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogServerRollup>();

        await using var command = postgres.CreateCommand(PerServerRollupSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId.HasValue ? serverId.Value : (object)DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogServerRollup(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetInt64(9),
                reader.GetInt64(10),
                reader.GetInt32(11),
                reader.GetDateTime(12),
                reader.GetDateTime(13),
                reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                reader.GetInt64(17),
                reader.GetInt64(18),
                reader.GetInt64(19)));
        }

        return rows;
    }

    public static async Task<List<BacklogCollectorCensus>> GetCollectorCensusAsync(
        NpgsqlDataSource postgres, int? serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogCollectorCensus>();

        await using var command = postgres.CreateCommand(CollectorCensusSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId.HasValue ? serverId.Value : (object)DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogCollectorCensus(
                reader.GetString(0),
                reader.GetInt32(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4),
                reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                reader.GetInt64(8),
                reader.GetInt64(9),
                reader.GetInt64(10)));
        }

        return rows;
    }

    public static async Task<List<BacklogRow>> GetRowsAsync(
        NpgsqlDataSource postgres, int serverId, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<BacklogRow>();

        await using var command = postgres.CreateCommand(RowsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BacklogRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt32(3),
                reader.GetInt32(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.GetInt64(7),
                reader.GetDateTime(8),
                reader.GetDateTime(9),
                reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.GetInt32(13),
                reader.GetBoolean(14)));
        }

        return rows;
    }
}
