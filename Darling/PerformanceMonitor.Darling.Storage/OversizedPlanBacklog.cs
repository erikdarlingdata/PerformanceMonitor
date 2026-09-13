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
/// <para><b>Growth is bounded by the horizon, not by a queue design.</b> Measured on the fleet's outlier
/// server for this collector, 13 plans were over the cap at once, with compile ages from 13.6 hours to
/// 144.6 days — the arrival rate is a handful of long-lived cache residents per server, not a stream.
/// Retention prunes on <c>last_seen_at</c> (<c>DarlingRetention</c>), so a plan that stops recurring leaves
/// with the fact rows it explained rather than accumulating forever.</para>
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
    /// <para><c>attempt_count</c> and <c>last_attempt_at</c> are not bookkeeping. Ordering the claim by
    /// <c>last_attempt_at</c> is what makes head-of-line starvation impossible: a row that was just tried
    /// goes to the back, so one plan that can never be fetched cannot occupy a slot every tick forever. The
    /// count then makes a chronic failure legible in the TABLE rather than only in a log line nobody
    /// greps.</para>
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
    /// The sweep's claim for one server: the rows with no content yet and no standing expiry, oldest ATTEMPT
    /// first so nothing can starve, then largest plan first among rows never attempted — those are the plans
    /// the cap cost the most visibility on, which is the whole reason to go back for them.
    ///
    /// <para>The limit is interpolated from the caller's own constant rather than written here, so the
    /// statement and the policy cannot disagree about how many plans one tick may fetch. It is never user
    /// input.</para>
    /// </summary>
    public static string ClaimSql(int limit) => @"
SELECT
    collector_name,
    plan_handle,
    sql_handle,
    statement_start_offset,
    statement_end_offset,
    database_name,
    observed_bytes
FROM collect.oversized_plan_backlog
WHERE server_id = $1
AND   captured_at IS NULL
AND   expired_at IS NULL
ORDER BY
    last_attempt_at ASC NULLS FIRST,
    observed_bytes DESC
LIMIT " + limit.ToString(CultureInfo.InvariantCulture) + ";";

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
    last_attempt_at = $7,
    attempt_count = attempt_count + 1" + KeyPredicate + ";";

    /// <summary>
    /// A fetch that could not complete — connect failure, driver fault, or its own budget. NOT an expiry:
    /// nothing was learned about the handle, so the row stays claimable and simply goes to the back of the
    /// queue.
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
    /// <para><b>Read from the backlog, not joined to the fact table.</b> <c>query_stats</c> does not store
    /// the statement offsets — they are read for the delta key and never persisted — so a join from a stored
    /// fact row could only match on <c>plan_handle</c> + <c>sql_handle</c>, which for a multi-statement plan
    /// is several backlog rows describing DIFFERENT statements' plans. Serving one of those as "the plan for
    /// this query" is worse than serving nothing. <c>query_hash</c> on the row keys the fallback at exactly
    /// the grain the readers already ask at.</para>
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
    public sealed record PendingPlan(string CollectorName, OversizedPlanObservation Observation);

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
