/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// <c>collect.oversized_plan_backlog</c> — the store side of #3392: which cached plans
/// <see cref="QueryPlanXmlCaptureLimits"/> declined to capture, and the content once a later
/// out-of-band fetch has retrieved it.
///
/// <para><b>Why a table and not a retry flag.</b> The cap is a per-row CONTENT decision, so the same plan
/// ships NULL every cycle it recurs — nothing about the next cycle makes it smaller. And a capped row
/// produces no plan-dimension row at all, so there is no content, no digest and nothing in the store to
/// point at. The only way back is the cache coordinates, which is what these rows hold.</para>
///
/// <para><b>Identity is the row identity, not the plan's.</b> The key is
/// <c>(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset)</c>
/// — the full <c>sys.dm_exec_query_stats</c> row identity, for the reason that collector's own delta key
/// records: keying on <c>plan_handle</c> alone cross-contaminates multi-statement plans, where one handle
/// carries many statements at different offsets. A re-sighting touches <c>last_seen_at</c>; it does not
/// insert a second row and does not re-fetch content already held.</para>
///
/// <para><b>Growth is bounded by the horizon, not by a queue design.</b> Retention prunes on
/// <c>last_seen_at</c> (<c>DarlingRetention</c>), so a plan that stops recurring leaves with the fact rows
/// it explained rather than accumulating forever — and that horizon is the only bound there is, because
/// arrival is not uniformly a trickle. One production store class presents a handful of long-lived cache
/// residents per server: 13 plans over the cap at once on its outlier server, compile ages from 13.6 hours
/// to 144.6 days. Another presents ~1,400 new over-cap identities an hour fleet-wide and never saturates,
/// because plan-cache churn re-keys the same logical queries under fresh <c>plan_handle</c> and offsets. The
/// drain rate is sized for the second (<c>OversizedPlanBacklogSweep.MaxPlansPerServerPerTick</c>); this
/// table holds either population without a different shape.</para>
///
/// <para><b>One index, deliberately: the primary key.</b> Its leading column is <c>server_id</c>, which is
/// the leading predicate of all three access paths — the sweep's claim, the read-surface fallback and the
/// sighting upsert — so at this table's size every one of them is served or is a scan of a few thousand
/// narrow rows. A second index chosen for an ORDER BY nobody has measured would be a guess; the ORDER BYs
/// below name the index to add if it ever stops being one.</para>
///
/// <para><b>Not a collector table.</b> It is absent from <see cref="CollectorCatalog"/>, so
/// <see cref="TimescaleSupport"/>'s catalog-driven hypertable conversion and
/// <c>DarlingRetention</c>'s catalog purge never reach it — the <c>collect.store_metrics</c> precedent. It
/// needs neither chunks nor compression, and a hypertable could not carry this primary key anyway (a
/// hypertable's unique constraint must include the partitioning column).</para>
/// </summary>
public static class OversizedPlanBacklog
{
    /// <summary>
    /// The command deadline for this table's own store statements — its own constant rather than
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/>, whose regime is an interactive MCP read, and
    /// rather than an inherited Npgsql default, which is the defect class that constant exists to close.
    ///
    /// <para>Every statement here is a primary-key-matched single row against a table bounded at a few
    /// thousand narrow rows, so 30 s is orders of magnitude above the work. The one with real work is the
    /// capture UPDATE, which writes the fetched plan XML — megabytes of TOAST for the tail this exists to
    /// reach — and nothing encloses it: the sweep's per-plan budget bounds the TARGET fetch, and abandoning
    /// the store write afterwards would discard content already paid for. So the deadline is set low enough
    /// that a stalled write is reported rather than held, and high enough that the largest plan measured on
    /// the fleet (8.7 MB) is not close to it.</para>
    /// </summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>The table, schema-qualified — the migrate session's <c>search_path</c> puts <c>collect</c>
    /// first, but every site here is explicit so a read from a differently-configured session cannot land
    /// somewhere else.</summary>
    public const string TableName = "collect.oversized_plan_backlog";

    /// <summary>
    /// The two <c>collector_name</c> values this table ever holds — the collectors that capture cached-plan
    /// XML and therefore have a cap to exceed. Named rather than left implicit so the read-surface fallbacks
    /// can filter on the right one without a literal per call site, and so a pin can check the set against
    /// the collectors that actually describe observations.
    /// </summary>
    public const string QueryStatsCollectorName = "query_stats";

    /// <summary>See <see cref="QueryStatsCollectorName"/>.</summary>
    public const string ProcedureStatsCollectorName = "procedure_stats";

    /// <summary>
    /// The DDL, owned here rather than written into the migration rung as a second copy: <c>PgMigrations</c>
    /// concatenates this constant into its rung, the V38 idiom, so the shape the store gets and the shape the
    /// statements below address cannot drift.
    ///
    /// <para><c>attempt_count</c> and <c>last_attempt_at</c> are not bookkeeping. Ranking the claim by
    /// <c>last_attempt_at</c> makes the tried rows a round-robin: a row that was just tried goes to the back of
    /// the tried rows, and the tried rows hold at most <see cref="TriedRowsPerClaim"/> of a claim's slots, so
    /// one plan that can never be fetched cannot keep every never-tried row out (it does keep a slot's turn,
    /// every <c>ceil(tried rows / share)</c> passes, for as long as it fails: a failure is not counted). The
    /// count is the sweep's retirement limit: it grows only when a plan that had the whole judging budget to
    /// itself overran it (#5367 review, A-M1), so a connect failure or an expiry does not move it.</para>
    /// </summary>
    public const string CreateTableSql = @"
CREATE TABLE IF NOT EXISTS collect.oversized_plan_backlog
(
    server_id integer NOT NULL,
    collector_name text NOT NULL,
    plan_handle text NOT NULL,
    sql_handle text NOT NULL,
    statement_start_offset integer NOT NULL,
    statement_end_offset integer NOT NULL,
    database_name text,
    query_hash text,
    observed_bytes bigint NOT NULL,
    first_seen_at timestamp NOT NULL,
    last_seen_at timestamp NOT NULL,
    plan_xml text,
    captured_at timestamp,
    expired_at timestamp,
    last_attempt_at timestamp,
    attempt_count integer NOT NULL DEFAULT 0,
    CONSTRAINT pk_oversized_plan_backlog PRIMARY KEY
    (
        server_id,
        collector_name,
        plan_handle,
        sql_handle,
        statement_start_offset,
        statement_end_offset
    )
);";

    /// <summary>
    /// The primary key as a bound predicate, written once so the three statements that stamp one row's
    /// outcome address exactly the row the claim handed back.
    /// </summary>
    private const string KeyPredicate = @"
WHERE server_id = $1
AND   collector_name = $2
AND   plan_handle = $3
AND   sql_handle = $4
AND   statement_start_offset = $5
AND   statement_end_offset = $6";

    /// <summary>
    /// One sighting. <c>first_seen_at</c> and <c>last_seen_at</c> both take <c>$10</c> on the insert, so a
    /// first sighting reads as seen-once rather than as a row with an unknown age.
    ///
    /// <para><b><c>expired_at</c> is CLEARED on a re-sighting, and that is load-bearing.</b> A collector only
    /// sees a row while its plan is in cache, so a fresh sighting is positive evidence the handle resolves —
    /// which retires whatever made the previous fetch come back empty. Without the clear, one transient miss
    /// would retire the row permanently, recreating the blind spot this table exists to close.</para>
    ///
    /// <para><c>captured_at</c> and <c>plan_xml</c> are deliberately left alone: the key pins the handle AND
    /// the offsets, so a row already captured describes the same document and there is nothing to go back
    /// for.</para>
    /// </summary>
    public const string UpsertSightingSql = @"
INSERT INTO collect.oversized_plan_backlog AS b
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
ON CONFLICT ON CONSTRAINT pk_oversized_plan_backlog DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    observed_bytes = EXCLUDED.observed_bytes,
    database_name = COALESCE(EXCLUDED.database_name, b.database_name),
    query_hash = COALESCE(EXCLUDED.query_hash, b.query_hash),
    expired_at = NULL;";

    /// <summary>
    /// How many already-tried rows one claim of <paramref name="limit"/> rows may take while there are never-tried
    /// rows to take instead: half, rounded down (5 of 10). The other half is reserved for never-tried rows, so a
    /// backlog of tried rows whose fetch keeps failing cannot fill every claim and keep a new row out for good
    /// (#5367 review round 2, N2). When one side has fewer rows than its share, the other side takes the rest.
    /// </summary>
    public static int TriedRowsPerClaim(int limit) => limit / 2;

    /// <summary>
    /// The sweep's claim for one server: the rows with no content yet and no standing expiry, at most
    /// <paramref name="limit"/> of them, in the order the sweep works them: rows already tried first, oldest ATTEMPT
    /// first, then the rows never attempted, largest plan first. A never-tried row's size is what ranks it, because
    /// those are the plans the cap cost the most visibility on, which is the whole reason to go back for them.
    ///
    /// <para><b>Which rows make the cut (#5367).</b> Tried rows get at most <see cref="TriedRowsPerClaim"/> of the
    /// slots (oldest attempt first) and never-tried rows get the rest (largest first); a side that is short leaves
    /// its unused slots to the other. A row tried once used to sit behind every row never tried, so with new rows
    /// arriving each pass it could wait without end, and the sweep judges a tried row on a session of its own
    /// (<c>OversizedPlanBacklogSweep.SessionFor</c>) so that each claim of it makes progress. Each tried row whose
    /// fetch succeeds leaves in at most two claims after the one that tried it: judged, or a judge timeout counted
    /// and then the marker. A tried row whose fetch FAILS is not counted and never retires, so it stays in the
    /// tried queue: that queue is served round-robin by attempt age, <see cref="TriedRowsPerClaim"/> rows a pass,
    /// and the cap is what stops it from taking every slot.</para>
    ///
    /// <para>The limit is interpolated from the caller's own constant rather than written here, so the
    /// statement and the policy cannot disagree about how many plans one tick may fetch. It is never user
    /// input.</para>
    /// </summary>
    public static string ClaimSql(int limit)
    {
        var triedShare = TriedRowsPerClaim(limit).ToString(CultureInfo.InvariantCulture);
        var newShare = (limit - TriedRowsPerClaim(limit)).ToString(CultureInfo.InvariantCulture);
        var all = limit.ToString(CultureInfo.InvariantCulture);
        return @"
SELECT
    collector_name,
    plan_handle,
    sql_handle,
    statement_start_offset,
    statement_end_offset,
    database_name,
    observed_bytes,
    attempt_count,
    tried
FROM
(
    SELECT
        collector_name,
        plan_handle,
        sql_handle,
        statement_start_offset,
        statement_end_offset,
        database_name,
        observed_bytes,
        attempt_count,
        last_attempt_at,
        tried
    FROM
    (
        SELECT
            collector_name,
            plan_handle,
            sql_handle,
            statement_start_offset,
            statement_end_offset,
            database_name,
            observed_bytes,
            attempt_count,
            last_attempt_at,
            last_attempt_at IS NOT NULL AS tried,
            row_number() OVER
            (
                PARTITION BY (last_attempt_at IS NOT NULL)
                ORDER BY last_attempt_at ASC, observed_bytes DESC
            ) AS share_rank
        FROM collect.oversized_plan_backlog
        WHERE server_id = $1
        AND   captured_at IS NULL
        AND   expired_at IS NULL
    ) AS ranked
    ORDER BY
        (share_rank <= CASE WHEN tried THEN " + triedShare + " ELSE " + newShare + @" END) DESC,
        tried DESC,
        last_attempt_at ASC,
        observed_bytes DESC
    LIMIT " + all + @"
) AS claimed
ORDER BY
    tried DESC,
    last_attempt_at ASC,
    observed_bytes DESC;";
    }

    /// <summary>A fetch that returned the plan: store it, and retire any standing expiry.</summary>
    public const string RecordCaptureSql = @"
UPDATE collect.oversized_plan_backlog
SET plan_xml = $8,
    captured_at = $7,
    last_attempt_at = $7,
    attempt_count = attempt_count + 1,
    expired_at = NULL" + KeyPredicate + ";";

    /// <summary>
    /// A fetch that came back with nothing: the handle no longer renders a plan. Stamped rather than deleted
    /// — the row is the record that this plan was seen, measured and then lost, and a delete would let the
    /// next sighting present it as new.
    /// </summary>
    public const string RecordExpirySql = @"
UPDATE collect.oversized_plan_backlog
SET expired_at = $7,
    last_attempt_at = $7" + KeyPredicate + ";";

    /// <summary>
    /// A fetch that could not complete — connect failure, driver fault, its own budget, or a judging budget an
    /// earlier plan had already used. NOT an expiry: nothing was learned about the handle, so the row stays
    /// claimable and simply goes to the back of the tried rows. Not counted in <c>attempt_count</c>, which is the
    /// retirement limit's count of judge timeouts and nothing else (#5367 review, A-M1).
    /// </summary>
    public const string RecordFailureSql = @"
UPDATE collect.oversized_plan_backlog
SET last_attempt_at = $7" + KeyPredicate + ";";

    /// <summary>
    /// A plan that had the whole judging budget to itself and overran it: the one outcome that adds to
    /// <c>attempt_count</c>. The row stays claimable and goes to the back of the tried rows, until the count reaches
    /// the sweep's limit and the whole-plan marker is stored as the capture.
    /// </summary>
    public const string RecordAttemptSql = @"
UPDATE collect.oversized_plan_backlog
SET last_attempt_at = $7,
    attempt_count = attempt_count + 1" + KeyPredicate + ";";

    /// <summary>
    /// The read-surface fallback for <c>query_stats</c>: the most recently captured over-cap plan for a
    /// (server, query_hash) — optionally narrowed to one database, the shape both the MCP read (database
    /// optional) and the viewer's (database required) need from one statement.
    ///
    /// <para><b>Read from the backlog, not joined to the fact table.</b> When this was written
    /// <c>query_stats</c> did not store the statement offsets — they were read for the delta key and never
    /// persisted — so a join from a stored fact row could only match on <c>plan_handle</c> + <c>sql_handle</c>,
    /// which for a multi-statement plan is several backlog rows describing DIFFERENT statements' plans.
    /// Serving one of those as "the plan for this query" is worse than serving nothing. V128 (#3540) stores
    /// the offsets, so an exact join is POSSIBLE for rows written since; it is not taken here because the
    /// readers ask at the <c>query_hash</c> grain (the Dashboard's and viewer's key), which
    /// <c>query_hash</c> on the backlog row already serves, and because every row written before V128
    /// carries NULL offsets, so an exact join would go dark on an upgraded store for a raw retention's
    /// worth of history. If a reader ever asks at the statement grain, the join is now available to
    /// it.</para>
    ///
    /// <para>A non-null <c>plan_xml</c> is itself the "this row was capped" test the caller would otherwise
    /// make against <c>query_plan_xml_bytes</c>: the row exists only because the measurement exceeded the
    /// cap.</para>
    /// </summary>
    public const string QueryStatsFallbackSql = @"
SELECT plan_xml
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   collector_name = 'query_stats'
AND   query_hash = $2
AND   ($3::text IS NULL OR database_name = $3)
AND   plan_xml IS NOT NULL
ORDER BY captured_at DESC
LIMIT 1;";

    /// <summary>
    /// The read-surface fallback for <c>procedure_stats</c> keyed on <c>sql_handle</c>, matching that
    /// collector's MCP read.
    /// </summary>
    public const string ProcedureStatsFallbackBySqlHandleSql = @"
SELECT plan_xml
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   collector_name = 'procedure_stats'
AND   sql_handle = $2
AND   plan_xml IS NOT NULL
ORDER BY captured_at DESC
LIMIT 1;";

    /// <summary>
    /// The read-surface fallback for <c>procedure_stats</c> keyed on the OBJECT, matching the viewer's Top
    /// Procedures grid, which groups by (database, schema, object) and never sees a handle.
    ///
    /// <para>This one CAN join the fact table exactly, unlike its <c>query_stats</c> sibling: the three
    /// module-level DMVs aggregate at the whole-object grain and expose no offsets, so this collector's plan
    /// fetch passes fixed literals and every backlog row for it carries that same pair. Joining on
    /// <c>plan_handle</c> + <c>sql_handle</c> + those offsets is therefore the full key, not a prefix of
    /// it.</para>
    /// </summary>
    public const string ProcedureStatsFallbackByObjectSql = @"
SELECT b.plan_xml
FROM procedure_stats AS ps
JOIN collect.oversized_plan_backlog AS b
  ON  b.server_id = ps.server_id
  AND b.collector_name = 'procedure_stats'
  AND b.plan_handle = ps.plan_handle
  AND b.sql_handle = ps.sql_handle
WHERE ps.server_id = $1
AND   ps.database_name = $2
AND   ps.schema_name = $3
AND   ps.object_name = $4
AND   b.plan_xml IS NOT NULL
ORDER BY ps.collection_time DESC
LIMIT 1;";

    /// <summary>One claimed backlog row: its identity, and what the cap measured.</summary>
    /// <param name="CollectorName">Which collector saw it — decides nothing about the fetch, but rides
    /// along because the outcome statements address the full key.</param>
    /// <param name="Observation">The cache coordinates and the measured size, in the same shape the
    /// collector described it.</param>
    /// <param name="AttemptCount">The row's <c>attempt_count</c> at claim time: how many times the plan has had the
    /// whole judging budget to itself and overrun it. The sweep retires a plan the judging budget keeps failing to
    /// cover once this reaches its limit (#5320); a connect failure or an expiry does not add to it.</param>
    /// <param name="Tried">Whether the row had been attempted before this claim (<c>last_attempt_at</c> is set), whatever
    /// the outcome was. The sweep judges a tried row on a judging session of its own (#5367).</param>
    public sealed record PendingPlan(string CollectorName, OversizedPlanObservation Observation, int AttemptCount = 0, bool Tried = false);

    /// <summary>
    /// Records this cycle's over-cap sightings for one server. Opens NO connection when there are none,
    /// which is the overwhelmingly common case — a fleet with nothing over the cap pays nothing.
    ///
    /// <para>Never throws. A best-effort backlog that cannot be written is a lost opportunity to go back for
    /// a plan, not a reason to fail a collection cycle that stored every row it read.</para>
    /// </summary>
    /// <returns>How many sightings were written, or 0 when none were attempted or the write failed.</returns>
    public static async Task<int> RecordSightingsAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        IReadOnlyList<OversizedPlanObservation> observations,
        DateTime sightingUtc,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(observations);

        if (observations.Count == 0)
        {
            return 0;
        }

        /* Naive-UTC storage: Npgsql 6+ infers timestamptz from Kind=Utc and silently zone-shifts — see
           PgCollectorRowWriter. */
        var seenAt = DateTime.SpecifyKind(sightingUtc, DateTimeKind.Unspecified);
        var written = 0;

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(UpsertSightingSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };

            /* $1..$10 in statement order; the per-server values are set once, the per-row ones below. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = collectorName });
            var planHandle = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var sqlHandle = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var startOffset = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var endOffset = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer });
            var databaseName = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var queryHash = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
            var observedBytes = command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = seenAt });

            foreach (var observation in observations)
            {
                planHandle.Value = observation.PlanHandle;
                sqlHandle.Value = observation.SqlHandle;
                startOffset.Value = observation.StatementStartOffset;
                endOffset.Value = observation.StatementEndOffset;
                databaseName.Value = (object?)observation.DatabaseName ?? DBNull.Value;
                queryHash.Value = (object?)observation.QueryHash ?? DBNull.Value;
                observedBytes.Value = observation.ObservedBytes;

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                written++;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(
                "Oversized-plan backlog: {Collector} on server {ServerId} recorded {Written} of {Total} sighting(s) before {Message}",
                collectorName, serverId, written, observations.Count, ex.Message);
        }

        return written;
    }
}
