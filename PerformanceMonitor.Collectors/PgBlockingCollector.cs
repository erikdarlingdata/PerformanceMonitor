/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Who is blocked, by whom, on what — the condition people actually call about.
///
/// <para><b>This is a SAMPLE, and that has to be said out loud.</b> SQL Server has a blocked-process report:
/// the engine itself materialises a record when blocking exceeds a threshold, so a 6-second block leaves
/// evidence in a ring buffer whether or not anyone was looking. PostgreSQL has no equivalent — nothing is
/// recorded unless something asks. So this captures the state at the moment it runs, and
/// <b>blocking shorter than the cadence is invisible</b>. A reader who mistakes this for a
/// blocked-process-report equivalent will conclude "no blocking happened" from "no blocking was sampled".</para>
///
/// <para><b>Cost control: <c>pg_blocking_pids()</c> is not free.</b> It takes ShareLock on the lock manager
/// partitions per call, so calling it for every row on a 5,000-connection instance is exactly the monitoring
/// query that becomes the incident. It is evaluated only where <c>wait_event_type = 'Lock'</c> — the only
/// population that can have blockers — as a CASE in the select list rather than a WHERE filter, so one query
/// returns the whole picture and the expensive call is still bounded to the actually-blocked set.</para>
///
/// <para><b>An edge list, not a rendered tree.</b> <c>pg_blocking_pids()</c> returns an array; it is unnested
/// to one row per (blocked, blocker) pair. Rendering a chain here would bake in one view of it, and every
/// interesting question — root blocker, chain depth, fan-out — is cheap over edges and expensive to recover
/// from a string. The reader assembles the tree, the same division of labour as the other PostgreSQL reads
/// computing ratios this layer deliberately does not.</para>
///
/// <para><b>Both sides of every edge carry their own state.</b> A chain rooted in <c>idle in transaction</c> is
/// a different problem from one rooted in a long-running query, and the pid alone does not say which — the
/// remedy is "fix the application" versus "tune the query". Capturing only pids is the most common gap in
/// homegrown PostgreSQL blocking monitoring: you get a number, go looking, and by then it is gone.</para>
///
/// <para>Runs on ANY PostgreSQL target INCLUDING standbys, deliberately unlike
/// <see cref="PgAutovacuumStatsCollector"/>. That collector gates off replicas because
/// <c>pg_stat_user_tables</c> reports zeros there; <c>pg_stat_activity</c> reports the standby's own backends,
/// and recovery conflicts are real blocking that only happens on a standby.</para>
///
/// <para><b>Permissions degrade quietly.</b> <c>pg_monitor</c> is required to see other backends'
/// <c>query</c> text; without it PostgreSQL substitutes <c>&lt;insufficient privilege&gt;</c> for backends the
/// login does not own. That is not an error and will not fail the collection — it produces a capture with
/// pids and no queries, which is nearly useless. The text is stored as returned so the condition is visible
/// in the data rather than hidden.</para>
/// </summary>
public sealed class PgBlockingCollector : PostgresCollectorDefinitionBase<PgBlockingCollector.Row>
{
    public static PgBlockingCollector Instance { get; } = new();

    private PgBlockingCollector()
    {
    }

    public readonly record struct Row(
        long BlockedBackendId,
        int BlockedPid,
        long BlockingBackendId,
        int BlockingPid,
        string? DatabaseName,
        string? BlockedUsername,
        string? BlockedApplicationName,
        string? BlockedClientAddr,
        string? BlockedState,
        string? BlockedWaitEventType,
        string? BlockedWaitEvent,
        string? BlockedQuery,
        long BlockedXactDurationMs,
        long BlockedQueryDurationMs,
        string? BlockingUsername,
        string? BlockingApplicationName,
        string? BlockingClientAddr,
        string? BlockingState,
        string? BlockingWaitEventType,
        string? BlockingWaitEvent,
        string? BlockingQuery,
        long BlockingXactDurationMs,
        long BlockingQueryDurationMs,
        int BlockedPidCount,
        bool BlockingIsIdleInTransaction,
        bool QueryTextMayBeTruncated);

    /* The synthetic backend identity is worth explaining because it looks like noise. A pid is reused: on a
       busy instance the same number can be two different backends within one retention window, so a history
       keyed on pid alone silently merges them. backend_start disambiguates, and packing the two into one
       bigint (epoch seconds, then the zero-padded pid) gives a value that is stable for the life of a backend
       and comparable across samples. Borrowed from pganalyze's collector, which solves the same problem the
       same way — worth adopting rather than reinventing.

       track_activity_query_size caps pg_stat_activity.query (1 KB by default), so a long statement arrives
       clipped with no marker. The length is compared against the setting to record WHETHER it may be clipped,
       rather than joining out to pg_stat_statements for the full text: queryid is not on pg_stat_activity
       before PG14, and even after, matching a live backend to a normalised entry is a different claim than
       "this is what it ran".

       Durations are computed server-side in milliseconds rather than shipping timestamps, which sidesteps the
       timestamptz-render trap entirely — there is no timestamp column here to get wrong. */
    private const string QueryText = @"
WITH activity AS
(
    SELECT
        (extract(epoch FROM coalesce(a.backend_start, pg_postmaster_start_time()))::bigint::text
            || to_char(a.pid, 'FM0000000'))::bigint             AS backend_id,
        a.pid,
        a.datname,
        a.usename,
        a.application_name,
        a.client_addr,
        a.state,
        a.wait_event_type,
        a.wait_event,
        a.query,
        (extract(epoch FROM (clock_timestamp() - a.xact_start)) * 1000)::bigint  AS xact_duration_ms,
        (extract(epoch FROM (clock_timestamp() - a.query_start)) * 1000)::bigint AS query_duration_ms,
        /* The expensive call, bounded to backends already waiting on a lock. */
        CASE
            WHEN coalesce(a.wait_event_type, '') = 'Lock' THEN pg_blocking_pids(a.pid)
        END                                                     AS blocker_pids
    FROM pg_stat_activity AS a
    WHERE a.pid IS NOT NULL
    AND   a.pid <> pg_backend_pid()
),
edges AS
(
    SELECT
        blocked.backend_id                                      AS blocked_backend_id,
        blocked.pid                                             AS blocked_pid,
        unnest(blocked.blocker_pids)                            AS blocking_pid
    FROM activity AS blocked
    WHERE blocked.blocker_pids IS NOT NULL
    AND   cardinality(blocked.blocker_pids) > 0
),
fan_out AS
(
    SELECT blocking_pid, count(*)::int AS blocked_pid_count
    FROM edges
    GROUP BY blocking_pid
)
SELECT
    e.blocked_backend_id,
    e.blocked_pid,
    /* A blocker that has since gone (or that pg_stat_activity does not show) still leaves a real edge, so
       the join is LEFT and the blocker's own columns come back NULL rather than dropping the row. Losing the
       edge would understate the chain, which is the opposite of useful. */
    coalesce(blocker.backend_id, 0)                             AS blocking_backend_id,
    e.blocking_pid,
    coalesce(blocked.datname, blocker.datname)                  AS database_name,
    blocked.usename                                             AS blocked_username,
    blocked.application_name                                    AS blocked_application_name,
    blocked.client_addr::text                                   AS blocked_client_addr,
    blocked.state                                               AS blocked_state,
    blocked.wait_event_type                                     AS blocked_wait_event_type,
    blocked.wait_event                                          AS blocked_wait_event,
    blocked.query                                               AS blocked_query,
    coalesce(blocked.xact_duration_ms, -1)                      AS blocked_xact_duration_ms,
    coalesce(blocked.query_duration_ms, -1)                     AS blocked_query_duration_ms,
    blocker.usename                                             AS blocking_username,
    blocker.application_name                                    AS blocking_application_name,
    blocker.client_addr::text                                   AS blocking_client_addr,
    blocker.state                                               AS blocking_state,
    blocker.wait_event_type                                     AS blocking_wait_event_type,
    blocker.wait_event                                          AS blocking_wait_event,
    blocker.query                                               AS blocking_query,
    coalesce(blocker.xact_duration_ms, -1)                      AS blocking_xact_duration_ms,
    coalesce(blocker.query_duration_ms, -1)                     AS blocking_query_duration_ms,
    /* No coalesce: fan_out is grouped from the same edges CTE and joined INNER, so every blocking_pid
       here necessarily has a row and the fallback was unreachable. */
    f.blocked_pid_count                                         AS blocked_pid_count,
    /* Stamped here rather than derived on read: the remedy for this root differs from every other root, and
       a reader filtering rows must not be able to lose the distinction. */
    coalesce(blocker.state, '') = 'idle in transaction'          AS blocking_is_idle_in_transaction,
    /* Whether EITHER text may be clipped by track_activity_query_size.

       pg_size_bytes(), NOT ::int. current_setting() renders a memory GUC WITH ITS UNIT — this one comes
       back as '8kB' on Aurora 17.7 and '4kB' on 16.11 (pg_settings.unit is 'B', and current_setting
       scales to the largest whole unit), so current_setting(...)::int raises an
       invalid-input-syntax-for-integer error and takes the WHOLE collection down with it, every cycle.
       Verified against both majors rather than reasoned about. pg_size_bytes parses every unit form, so
       this stays right if the value is ever set in MB or left at the 1kB default.

       octet_length(), NOT length() — the same unit mistake as above wearing a different disguise, and it
       shipped in the first draft one line under a comment about getting units right. PostgreSQL truncates
       query text at a BYTE boundary while length() counts CHARACTERS in the session encoding, so on
       multi-byte text the comparison undercounts and the flag comes back false for a query that really was
       clipped. Measured on live Aurora: repeat('あ',100) is length 100, octet_length 300 — a 3x undercount
       against an 8192-byte limit. */
    (
        octet_length(coalesce(blocked.query, ''))
            >= pg_size_bytes(current_setting('track_activity_query_size'))
     OR octet_length(coalesce(blocker.query, ''))
            >= pg_size_bytes(current_setting('track_activity_query_size'))
    )                                                           AS query_text_may_be_truncated
FROM edges AS e
JOIN activity AS blocked
  ON  blocked.pid = e.blocked_pid
LEFT JOIN activity AS blocker
  ON  blocker.pid = e.blocking_pid
JOIN fan_out AS f
  ON  f.blocking_pid = e.blocking_pid
ORDER BY f.blocked_pid_count DESC, e.blocking_pid, e.blocked_pid";

    public override string Name => "pg_blocking";

    public override string TargetTable => "pg_blocking_edges";

    /// <summary>
    /// Any PostgreSQL target, standbys included — recovery conflicts are real blocking and a standby is where
    /// they happen.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("blocked_backend_id", CollectorColumnType.BigInt),
        new CollectorColumn("blocked_pid", CollectorColumnType.Integer),
        new CollectorColumn("blocking_backend_id", CollectorColumnType.BigInt),
        new CollectorColumn("blocking_pid", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_username", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_application_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_client_addr", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_state", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_wait_event_type", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_wait_event", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_query", CollectorColumnType.Varchar),
        /* -1, not NULL and not 0: a backend with no open transaction has no duration to report, and 0 would
           read as "started this instant". Same not-applicable sentinel the other PostgreSQL level columns use.
           pg_stat_io is the documented exception, where NULL means something different. */
        new CollectorColumn("blocked_xact_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("blocked_query_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("blocking_username", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_application_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_client_addr", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_state", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_wait_event_type", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_wait_event", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_query", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_xact_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("blocking_query_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("blocked_pid_count", CollectorColumnType.Integer),
        new CollectorColumn("blocking_is_idle_in_transaction", CollectorColumnType.Boolean),
        new CollectorColumn("query_text_may_be_truncated", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var edges = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            edges.Add(new Row(
                BlockedBackendId: reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                BlockedPid: reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                BlockingBackendId: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                BlockingPid: reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                DatabaseName: Text(reader, 4),
                BlockedUsername: Text(reader, 5),
                BlockedApplicationName: Text(reader, 6),
                BlockedClientAddr: Text(reader, 7),
                BlockedState: Text(reader, 8),
                BlockedWaitEventType: Text(reader, 9),
                BlockedWaitEvent: Text(reader, 10),
                BlockedQuery: Text(reader, 11),
                BlockedXactDurationMs: reader.IsDBNull(12) ? -1 : reader.GetInt64(12),
                BlockedQueryDurationMs: reader.IsDBNull(13) ? -1 : reader.GetInt64(13),
                BlockingUsername: Text(reader, 14),
                BlockingApplicationName: Text(reader, 15),
                BlockingClientAddr: Text(reader, 16),
                BlockingState: Text(reader, 17),
                BlockingWaitEventType: Text(reader, 18),
                BlockingWaitEvent: Text(reader, 19),
                BlockingQuery: Text(reader, 20),
                BlockingXactDurationMs: reader.IsDBNull(21) ? -1 : reader.GetInt64(21),
                BlockingQueryDurationMs: reader.IsDBNull(22) ? -1 : reader.GetInt64(22),
                BlockedPidCount: reader.IsDBNull(23) ? 1 : reader.GetInt32(23),
                BlockingIsIdleInTransaction: !reader.IsDBNull(24) && reader.GetBoolean(24),
                QueryTextMayBeTruncated: !reader.IsDBNull(25) && reader.GetBoolean(25)));
        }

        /* Zero rows is the overwhelmingly common case and the healthy one. It must never read as a failure —
           and it does not, because the runner records a SUCCESS with 0 rows for a collector that legitimately
           found nothing. */
        return edges;
    }

    private static string? Text(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.BlockedBackendId)
            .Value(row.BlockedPid)
            .Value(row.BlockingBackendId)
            .Value(row.BlockingPid)
            .Value(row.DatabaseName)
            .Value(row.BlockedUsername)
            .Value(row.BlockedApplicationName)
            .Value(row.BlockedClientAddr)
            .Value(row.BlockedState)
            .Value(row.BlockedWaitEventType)
            .Value(row.BlockedWaitEvent)
            .Value(row.BlockedQuery)
            .Value(row.BlockedXactDurationMs)
            .Value(row.BlockedQueryDurationMs)
            .Value(row.BlockingUsername)
            .Value(row.BlockingApplicationName)
            .Value(row.BlockingClientAddr)
            .Value(row.BlockingState)
            .Value(row.BlockingWaitEventType)
            .Value(row.BlockingWaitEvent)
            .Value(row.BlockingQuery)
            .Value(row.BlockingXactDurationMs)
            .Value(row.BlockingQueryDurationMs)
            .Value(row.BlockedPidCount)
            .Value(row.BlockingIsIdleInTransaction)
            .Value(row.QueryTextMayBeTruncated);
    }
}
