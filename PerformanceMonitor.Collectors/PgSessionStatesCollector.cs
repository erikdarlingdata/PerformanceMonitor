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
/// Which sessions are holding a transaction open, for how long, and whether that transaction is what is
/// pinning the xmin horizon.
///
/// <para><b>This closes a chain the product previously told backwards.</b>
/// <see cref="PgAutovacuumStatsCollector"/> shows vacuum falling behind,
/// <see cref="PgTableBloatStatsCollector"/> shows the bloat that produces, and
/// <see cref="PgWraparoundStatsCollector"/> shows where that ends. <see cref="PgXminHorizonCollector"/>
/// names the CLASS of thing holding the horizon back — and when that class is <c>session</c>, it emits one
/// row: the single oldest holder, as a pid and a formatted detail string. That is enough to know a session
/// is the cause and not enough to do anything about it. This collector is the session-side counterpart:
/// every session with an open transaction old enough to matter, with the columns needed to decide whether
/// it is a stuck application, a slow query, or nothing at all.</para>
///
/// <para><b>Idle in transaction is NOT automatically a horizon holder, and this is the single most important
/// thing this collector exists to get right.</b> Four idle-in-transaction shapes were measured on a live
/// PostgreSQL 16.15 instance, and only two of them pin anything:</para>
/// <list type="table">
///   <item><description><c>BEGIN; SELECT ...;</c> under READ COMMITTED — <c>backend_xmin</c> NULL,
///   <c>backend_xid</c> NULL. Pins <b>nothing</b>: the snapshot was released when the statement ended.</description></item>
///   <item><description><c>BEGIN; UPDATE ...;</c> that matched <b>zero</b> rows — also both NULL. Pins
///   <b>nothing</b>: no rows changed, so no transaction id was ever assigned.</description></item>
///   <item><description><c>BEGIN; UPDATE ...;</c> that matched a row — <c>backend_xmin</c> NULL but
///   <c>backend_xid</c> set. <b>Pins</b> the horizon, via its xid.</description></item>
///   <item><description><c>BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT ...;</c> — <c>backend_xmin</c> set,
///   <c>backend_xid</c> NULL. <b>Pins</b> the horizon, via its snapshot.</description></item>
/// </list>
/// <para>So a read that concludes "this session has been idle in transaction for 40 minutes, therefore it is
/// starving vacuum" is wrong half the time, and the half it is wrong on is the half where killing the session
/// accomplishes nothing. <c>horizon_age</c> is stamped here from the GREATER of the two ages precisely so the
/// causal claim is only ever made where the data supports it — and it is <c>-1</c>, not <c>0</c>, when the
/// session holds neither, because "holds nothing" and "holds the newest possible xid" are opposite findings.
/// The same GREATEST reasoning already lives in <see cref="PgXminHorizonCollector"/>; this is the other end
/// of the same wire, and <c>is_horizon_holder</c> marks the row that should agree with that collector's
/// <c>session</c> row in the same capture window.</para>
///
/// <para><b>This is a SAMPLE, and the caveat is the same one <see cref="PgBlockingCollector"/> carries.</b>
/// <c>pg_stat_activity</c> is a point-in-time view and PostgreSQL records nothing about session state unless
/// something asks, so a transaction that opened and closed between two samples is invisible — not rare, and
/// not a defect. What a one-minute sample reliably catches is the CHRONIC case, which is also the only case
/// that can pin the horizon long enough to matter. The read carries its own capture counts from
/// <c>collection_log</c> so "no long transactions in this window" is distinguishable from "nobody looked",
/// which the stored rows alone can never say.</para>
///
/// <para><b>No raw query text is stored, deliberately, and this is a departure from
/// <see cref="PgBlockingCollector"/>.</b> <c>pg_stat_activity.query</c> is the statement as submitted, with
/// literal parameter values inline — verified on the live rig, where a probe session's row came back carrying
/// its literal argument verbatim. <c>pg_blocking</c> stores that text because blocking is an exceptional
/// condition: the rows are rare and the text is the finding. This collector fires on a DURATION FLOOR that a
/// perfectly ordinary application can cross, so storing the same text here would mean routinely accumulating
/// user data in the store to answer a question that does not need it. What it stores instead is
/// <c>query_id</c> — PostgreSQL's own normalised statement fingerprint, the same value
/// <c>pg_stat_statements</c> keys on, so the read can join to <see cref="PgStatementStatsCollector"/> and
/// show the statement text with <c>$1</c> placeholders where the literals were — plus <c>command_tag</c>, the
/// leading SQL keyword mapped through a fixed whitelist. Full actionability, no literals. See the
/// <c>command_tag</c> comment for why a whitelist and not a substring.</para>
///
/// <para><b><c>pg_monitor</c> is load-bearing here in a way that fails silently.</b> Measured against a
/// least-privileged role on the live rig: without <c>pg_monitor</c>, PostgreSQL does not deny the read — it
/// returns every row, for every backend the login does not own, with all but SIX columns NULL. Measured
/// column by column on PostgreSQL 16.15 rather than enumerated from memory: what survives is
/// <c>pid</c>, <c>application_name</c>, <c>datname</c>, <c>usename</c>, <c>backend_xid</c> and
/// <c>backend_xmin</c>. Everything else — <c>state</c>, <c>state_change</c>, <c>xact_start</c>,
/// <c>query_start</c>, <c>backend_start</c>, <c>wait_event_type</c>, <c>wait_event</c>,
/// <c>backend_type</c>, <c>client_addr</c>, <c>leader_pid</c> and <c>query_id</c> — comes back NULL, and
/// <c>query</c> is replaced by the literal <c>&lt;insufficient privilege&gt;</c>. That two of the six
/// survivors are <c>backend_xid</c> and <c>backend_xmin</c> is the cruel part: the horizon still reads as
/// pinned and nothing can say by what. Every discriminating column this collector needs is in the
/// redacted set, so without the grant the feature is gutted while looking healthy. That is why
/// <c>state_is_redacted</c> is stamped per row off the <c>&lt;insufficient privilege&gt;</c> literal rather
/// than off <c>state IS NULL</c> — background workers legitimately have a NULL state under full privilege
/// (measured: <c>checkpointer</c>, <c>walwriter</c>, <c>autovacuum launcher</c> all do), so NULL alone cannot
/// tell a permission problem from a background process.</para>
///
/// <para><b>Runs on standbys, deliberately</b>, for the same reason <see cref="PgBlockingCollector"/> does
/// and unlike <see cref="PgAutovacuumStatsCollector"/>: <c>pg_stat_activity</c> reports the standby's own
/// backends, a standby can hold transactions open exactly like a primary, and a long-running query on a
/// standby with <c>hot_standby_feedback</c> on propagates its xmin to the PRIMARY — which is one of the four
/// causes <see cref="PgXminHorizonCollector"/> attributes. Gating this off replicas would blind it to the
/// place where that cause is created.</para>
///
/// <para><b>Version floor.</b> Every column read here except <c>query_id</c> exists in PostgreSQL 13, the
/// product's documented floor — confirmed by reading <c>pg_attribute</c> for the view on a live 13.23
/// instance, which has 21 columns including <c>leader_pid</c>, <c>backend_type</c>, <c>backend_xid</c> and
/// <c>backend_xmin</c>, and no <c>query_id</c>. <c>query_id</c> arrived in 14, so below that it is
/// substituted with a typed NULL rather than gating the whole collector off — the same substitution
/// <see cref="PgIndexUsageStatsCollector"/> makes for <c>last_idx_scan</c>, and for the same reason: the row
/// shape must not change across a mixed-version fleet.</para>
/// </summary>
public sealed class PgSessionStatesCollector : PostgresCollectorDefinitionBase<PgSessionStatesCollector.Row>
{
    public static PgSessionStatesCollector Instance { get; } = new();

    private PgSessionStatesCollector()
    {
    }

    /// <summary>
    /// How long a session must have been sitting in <c>idle in transaction</c> before it is worth storing.
    /// <para>Ten seconds, and the number is a bound on NOISE rather than a claim about harm. A routine OLTP
    /// transaction completes in single-digit milliseconds to low hundreds, so ten seconds is two to three
    /// orders of magnitude past normal turnaround and excludes every healthy application pattern; at the same
    /// time it is far below any threshold at which something would already have intervened
    /// (<c>idle_in_transaction_session_timeout</c> ships disabled, and PgBouncer's
    /// <c>idle_transaction_timeout</c> defaults to 0 as well). Nothing about crossing it means the session is
    /// a problem — that judgement needs <c>horizon_age</c> and the duration, and belongs in the read.</para>
    /// </summary>
    private const int IdleInTransactionFloorSeconds = 10;

    /// <summary>
    /// How long ANY transaction — active or idle — must have been open before it is worth storing.
    /// <para>Thirty seconds. A slow transaction pins the horizon exactly as hard as an idle one, which the
    /// issue behind this collector calls out and which the four measured shapes above confirm: what pins is
    /// the xid or the snapshot, not the state string. Thirty seconds is long by any OLTP standard while still
    /// being well inside the window where a reporting query or a batch job is legitimate, so crossing it is
    /// an observation and not a verdict.</para>
    /// </summary>
    private const int OpenTransactionFloorSeconds = 30;

    /// <summary>
    /// Hard cap on rows stored per capture.
    /// <para>An instance whose connection pool routinely parks sessions in <c>idle in transaction</c> can put
    /// thousands of backends over the floor at once, every minute, forever. The cap bounds that; the
    /// pre-limit count travels on every row as <c>reportable_sessions</c>, so a truncated capture is
    /// self-evident rather than silently under-reported — stored rows below <c>reportable_sessions</c> IS the
    /// truncation flag, and it also happens to be the number you actually want when the answer is "four
    /// thousand of them".</para>
    /// </summary>
    private const int MaxRowsPerCapture = 100;

    public readonly record struct Row(
        long BackendId,
        int Pid,
        string? DatabaseName,
        string? Username,
        string? ApplicationName,
        string? ClientAddr,
        string? BackendType,
        string? State,
        string? WaitEventType,
        string? WaitEvent,
        string? CommandTag,
        long? QueryId,
        long StateDurationMs,
        long XactDurationMs,
        long QueryDurationMs,
        long BackendDurationMs,
        long XminAge,
        long XidAge,
        long HorizonAge,
        bool IsIdleInTransaction,
        bool IsHorizonHolder,
        bool StateIsRedacted,
        int TotalSessions,
        int ActiveSessions,
        int IdleInTransactionSessions,
        int ReportableSessions);

    /* The synthetic backend identity is the same construction pg_blocking uses, for the same reason: a pid is
       reused, so on a busy instance a history keyed on pid alone silently merges two different backends
       inside one retention window. backend_start disambiguates. Sharing the construction is the point — the
       ids are comparable ACROSS the two collectors, so a session that shows up here as a long idle-in-
       transaction and there as a blocking root is provably the same backend rather than probably.

       One measured caveat on that comparability: backend_start is itself in the privileged column set, so
       without pg_monitor it comes back NULL and the coalesce falls through to pg_postmaster_start_time().
       The id is still stable within a capture but it no longer disambiguates a reused pid, and it does not
       match the id the same backend would get under full privilege. state_is_redacted is what tells a
       reader they are in that regime.

       Durations are computed server-side in milliseconds. There is no timestamp column in this table at all,
       which is deliberate: timestamptz::text renders in the SESSION TimeZone and is byte-identical to UTC on
       any instance running UTC, so the mistake survives every probe. Shipping a duration removes the class.

       age() rather than arithmetic on the raw xid, because modular wrap makes naive subtraction wrong
       exactly at the boundary where it matters most. */
    private static string BuildQueryText(int postgresMajorVersion)
    {
        /* query_id arrived in PostgreSQL 14. Below that the column does not exist and naming it is a parse
           error, so it is substituted with a typed NULL: the row shape stays constant across a mixed-version
           fleet and the read distinguishes "this version cannot report it" from "compute_query_id is off"
           by the target's version, not by guessing from a NULL. Confirmed absent by reading pg_attribute for
           the view on a live 13.23 instance. */
        var queryId = postgresMajorVersion >= 14 ? "a.query_id" : "NULL::bigint";

        return $@"
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
        a.backend_type,
        a.state,
        a.wait_event_type,
        a.wait_event,
        {queryId}                                              AS query_id,
        /* The redaction tell, and it has to be this rather than 'state IS NULL'. Without pg_monitor
           PostgreSQL does not refuse the read: it returns the row with every privileged column NULL and
           substitutes this exact literal for the query text. But a NULL state is ALSO the honest answer for
           a background worker under full privilege — checkpointer, walwriter and the autovacuum launcher all
           report NULL state and NULL query — so testing state alone would flag a healthy instance as
           unprivileged. The literal is unambiguous. */
        coalesce(a.query, '') = '<insufficient privilege>'      AS state_is_redacted,
        /* The command keyword, whitelisted rather than substringed.

           A substring of query text is a data-leak vector wearing a formatting decision: plenty of ORMs
           prepend a SQL comment block, so the first token can be a comment opener followed by anything the
           application chose to put in it, and a leading-N-characters rule would carry literals directly out
           of a WHERE clause. Matching the leading word against a closed set of SQL command keywords cannot
           emit anything that was not already in this list. Everything unrecognised collapses to '(other)',
           which is the honest answer and not a lossy one - the statement identity is query_id's job, not
           this column's.

           And a note for whoever edits this text: PostgreSQL block comments NEST, so a literal comment
           opener written inside this comment opens a nested one that never closes and the whole collection
           fails to parse, every cycle. The first draft did exactly that and was caught only by running the
           shipped string against a live instance.

           NULL query with a NULL state is a background worker, which has no command to report; NULL query
           with a live state is a session that has run nothing yet on this connection. Both are '(idle)'. */
        CASE
            WHEN coalesce(a.query, '') = '<insufficient privilege>' THEN '(redacted)'
            WHEN coalesce(btrim(a.query), '') = '' THEN '(idle)'
            WHEN upper(split_part(btrim(a.query), ' ', 1)) IN
                 ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'BEGIN', 'START', 'COMMIT', 'ROLLBACK',
                  'SAVEPOINT', 'RELEASE', 'DECLARE', 'FETCH', 'MOVE', 'CLOSE', 'COPY', 'CREATE', 'ALTER',
                  'DROP', 'TRUNCATE', 'GRANT', 'REVOKE', 'VACUUM', 'ANALYZE', 'REINDEX', 'CLUSTER',
                  'REFRESH', 'LOCK', 'CALL', 'DO', 'EXECUTE', 'PREPARE', 'EXPLAIN', 'SET', 'RESET', 'SHOW',
                  'LISTEN', 'NOTIFY', 'UNLISTEN', 'CHECKPOINT', 'WITH', 'VALUES', 'TABLE')
                THEN upper(split_part(btrim(a.query), ' ', 1))
            ELSE '(other)'
        END                                                     AS command_tag,
        (extract(epoch FROM (clock_timestamp() - a.state_change)) * 1000)::bigint  AS state_duration_ms,
        (extract(epoch FROM (clock_timestamp() - a.xact_start)) * 1000)::bigint    AS xact_duration_ms,
        (extract(epoch FROM (clock_timestamp() - a.query_start)) * 1000)::bigint   AS query_duration_ms,
        (extract(epoch FROM (clock_timestamp() - a.backend_start)) * 1000)::bigint AS backend_duration_ms,
        age(a.backend_xmin)::bigint                             AS xmin_age,
        age(a.backend_xid)::bigint                              AS xid_age,
        /* The causal link to the xmin horizon, and the reason it is stamped here rather than derived on
           read. A session pins the horizon through EITHER its snapshot (backend_xmin, which is what a
           REPEATABLE READ transaction holds) or its transaction id (backend_xid, which is what a READ
           COMMITTED transaction that has written holds after releasing its snapshot). Neither column alone
           sees both cases, and the greater of the two ages is what actually sets the floor. -1, not 0, when
           the session holds neither: 0 would read as 'holding the newest possible xid', which is the
           opposite finding from 'holding nothing', and an idle-in-transaction session holding nothing is
           the common healthy case rather than an edge one. */
        CASE
            WHEN a.backend_xmin IS NULL
             AND a.backend_xid IS NULL
                THEN -1::bigint
            ELSE GREATEST(
                     coalesce(age(a.backend_xmin), 0),
                     coalesce(age(a.backend_xid), 0))::bigint
        END                                                     AS horizon_age,
        coalesce(a.state, '') LIKE 'idle in transaction%'       AS is_idle_in_transaction
    FROM pg_stat_activity AS a
    WHERE a.pid IS NOT NULL
    /* Never report the collector's own session. Darling's read sits in pg_stat_activity with an open
       transaction and a backend_xmin like anything else, so without this it is a PERMANENT row: the
       zero-rows-when-healthy state becomes unreachable and every denominator is padded by one. Same
       exclusion, same reason, as pg_blocking and pg_xmin_horizon. */
    AND   a.pid <> pg_backend_pid()
    /* Parallel workers are not independent sessions. They share the leader's transaction, so including them
       would report one long transaction as N rows with identical durations and inflate every count here by
       the instance's parallelism. The leader represents them. The '<> pid' guard rather than a bare NULL
       check because leader_pid is documented NULL for a plain leader but is set to the process's OWN pid for
       a leader that participates in its parallel group on newer majors — excluding on NULL alone would drop
       those leaders entirely. leader_pid exists from PostgreSQL 13, the floor.

       leader_pid is itself in the privileged column set (measured), so without pg_monitor it is NULL for
       every backend the login does not own and this guard stops excluding anything - a parallel worker
       would come through as its own row. That errs toward INCLUDING rather than dropping, which is the safe
       direction for a guard whose only job is to avoid double-counting, and state_is_redacted already tells
       the reader the whole row is untrustworthy. */
    AND   (a.leader_pid IS NULL OR a.leader_pid = a.pid)
),
/* Capture-wide context, computed once and carried on every row.

   Repeating four integers per row is a deliberate denormalisation. A read that filters to
   is_idle_in_transaction still has to answer 'out of how many', and 'two sessions idle in transaction' is a
   different finding on a six-connection instance than on a four-thousand-connection one. Carrying it on the
   row means any single stored row is self-describing, and no read can accidentally drop the denominator by
   filtering. It does NOT survive a capture with zero reportable sessions — nothing does — which is what the
   read's collection_log capture counts are for. */
totals AS
(
    SELECT
        /* EVERY backend the view reports, background workers included — checkpointer, walwriter, the
           autovacuum launcher and any autovacuum workers, minus this collector's own session and minus
           parallel workers. Not filtered to backend_type = 'client backend', deliberately: backend_type is
           in the privileged column set, so that filter would come back NULL for every row on a target
           without pg_monitor and collapse the denominator to zero — losing it precisely where the numerator
           is already unreliable. A handful of background rows inflates the count on a quiet instance and
           changes nothing on a busy one, which is the cheaper error. */
        count(*)::int                                                       AS total_sessions,
        /* Both are derived from state, which redaction nulls - so under redaction they count only the
           backends the monitoring login OWNS. Redaction is per-BACKEND and ownership-based rather than
           per-role-wide (measured: an unprivileged role sees its OWN backend with a live state and a real
           query while every other row is redacted), and this collector already excludes its own session,
           so in practice that leaves 0 unless the login holds several connections at capture time.

           Either way these two UNDER-report rather than reporting nothing, which is the more dangerous
           shape of the two: state_is_redacted on the row is what says the numbers cannot be read at face
           value. */
        count(*) FILTER (WHERE state = 'active')::int                       AS active_sessions,
        count(*) FILTER (WHERE is_idle_in_transaction)::int                 AS idle_in_transaction_sessions,
        /* The oldest holder on the instance, or NULL when nothing holds the horizon at all. Computed over
           the FULL activity set rather than over the reportable subset, because the horizon is a property of
           the instance: a young transaction can be the oldest holder, and calling a filtered survivor 'the
           holder' would name the wrong session precisely when the real one fell below the floor. */
        max(horizon_age) FILTER (WHERE horizon_age >= 0)                    AS oldest_horizon_age
    FROM activity
),
reportable AS
(
    SELECT
        a.*,
        t.total_sessions,
        t.active_sessions,
        t.idle_in_transaction_sessions,
        /* One row is force-included regardless of duration: whichever session actually holds the oldest
           xmin or xid. That is the entire causal claim this collector makes, and it must not be lost to a
           duration floor — a horizon holder below the floor is the answer to 'what is pinning it', and
           reporting nothing there would leave the reader exactly where pg_xmin_horizon already left them. */
        (t.oldest_horizon_age IS NOT NULL AND a.horizon_age = t.oldest_horizon_age) AS is_horizon_holder
    FROM activity AS a
    CROSS JOIN totals AS t
),
filtered AS
(
    SELECT
        r.*,
        count(*) OVER ()::int                                   AS reportable_sessions
    FROM reportable AS r
    WHERE r.is_horizon_holder
    OR    (r.is_idle_in_transaction AND r.state_duration_ms >= {IdleInTransactionFloorSeconds * 1000})
    OR    (r.xact_duration_ms IS NOT NULL AND r.xact_duration_ms >= {OpenTransactionFloorSeconds * 1000})
)
SELECT
    f.backend_id,
    f.pid,
    f.datname                                                   AS database_name,
    f.usename                                                   AS username,
    f.application_name,
    f.client_addr::text                                         AS client_addr,
    f.backend_type,
    f.state,
    f.wait_event_type,
    f.wait_event,
    f.command_tag,
    f.query_id,
    /* -1 rather than NULL for every duration, matching pg_blocking's sentinel convention. A backend with no
       open transaction has no transaction duration to report and 0 would read as 'started this instant'. */
    coalesce(f.state_duration_ms, -1)                           AS state_duration_ms,
    coalesce(f.xact_duration_ms, -1)                            AS xact_duration_ms,
    coalesce(f.query_duration_ms, -1)                           AS query_duration_ms,
    coalesce(f.backend_duration_ms, -1)                         AS backend_duration_ms,
    coalesce(f.xmin_age, -1)                                    AS xmin_age,
    coalesce(f.xid_age, -1)                                     AS xid_age,
    f.horizon_age,
    f.is_idle_in_transaction,
    f.is_horizon_holder,
    f.state_is_redacted,
    f.total_sessions,
    f.active_sessions,
    f.idle_in_transaction_sessions,
    f.reportable_sessions
FROM filtered AS f
/* Worst first, so the row cap keeps the findings and discards the routine. Horizon holders lead because they
   are the only rows carrying a proven causal claim; then the longest-held transactions. */
ORDER BY f.is_horizon_holder DESC, f.horizon_age DESC, f.xact_duration_ms DESC NULLS LAST, f.pid
LIMIT {MaxRowsPerCapture}";
    }

    public override string Name => "pg_session_states";

    /* Name and TargetTable agree here, and that is safe rather than lucky: pg_catalog is searched implicitly
       and FIRST, so a collector table sharing a name with a catalog object breaks CREATE INDEX and makes
       readers silently read the catalog. Checked against a live instance —
       to_regclass('pg_catalog.pg_session_states') is NULL, so nothing is shadowed. */
    public override string TargetTable => "pg_session_states";

    /// <summary>
    /// Any PostgreSQL target, standbys included. See the class remarks: a standby holds its own transactions,
    /// and with <c>hot_standby_feedback</c> on its xmin is propagated to the primary, which is one of the
    /// causes <see cref="PgXminHorizonCollector"/> attributes. <c>query_id</c> is version-substituted rather
    /// than gated, so PostgreSQL 13 targets are collected too.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Synthetic (backend_start, pid) identity — comparable to pg_blocking_edges' backend ids by
           construction, so the same backend can be followed across both collectors. */
        new CollectorColumn("backend_id", CollectorColumnType.BigInt),
        new CollectorColumn("pid", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("username", CollectorColumnType.Varchar),
        new CollectorColumn("application_name", CollectorColumnType.Varchar),
        new CollectorColumn("client_addr", CollectorColumnType.Varchar),
        new CollectorColumn("backend_type", CollectorColumnType.Varchar),
        new CollectorColumn("state", CollectorColumnType.Varchar),
        new CollectorColumn("wait_event_type", CollectorColumnType.Varchar),
        new CollectorColumn("wait_event", CollectorColumnType.Varchar),
        /* The leading SQL keyword, whitelisted. Not a substring of the statement — see the query comment. */
        new CollectorColumn("command_tag", CollectorColumnType.Varchar),
        /* Nullable on purpose, with three distinct meanings the read must keep apart: PostgreSQL 13 has no
           such column, PostgreSQL 14+ reports NULL when compute_query_id is off, and a redacted row reports
           NULL because the whole privileged column set is redacted. state_is_redacted separates the third;
           the target's major version separates the first. */
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("state_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("xact_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("query_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("backend_duration_ms", CollectorColumnType.BigInt),
        new CollectorColumn("xmin_age", CollectorColumnType.BigInt),
        new CollectorColumn("xid_age", CollectorColumnType.BigInt),
        /* The greater of the two ages, stamped at collection. This is the column that carries the causal
           claim, and -1 means the session pins nothing at all. */
        new CollectorColumn("horizon_age", CollectorColumnType.BigInt),
        new CollectorColumn("is_idle_in_transaction", CollectorColumnType.Boolean),
        new CollectorColumn("is_horizon_holder", CollectorColumnType.Boolean),
        new CollectorColumn("state_is_redacted", CollectorColumnType.Boolean),
        new CollectorColumn("total_sessions", CollectorColumnType.Integer),
        new CollectorColumn("active_sessions", CollectorColumnType.Integer),
        new CollectorColumn("idle_in_transaction_sessions", CollectorColumnType.Integer),
        /* Pre-LIMIT count. Stored rows below this value is what a truncated capture looks like. */
        new CollectorColumn("reportable_sessions", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var sessions = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            sessions.Add(new Row(
                BackendId: reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                Pid: reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                DatabaseName: Text(reader, 2),
                Username: Text(reader, 3),
                ApplicationName: Text(reader, 4),
                ClientAddr: Text(reader, 5),
                BackendType: Text(reader, 6),
                State: Text(reader, 7),
                WaitEventType: Text(reader, 8),
                WaitEvent: Text(reader, 9),
                CommandTag: Text(reader, 10),
                /* Stays NULL rather than collapsing to a sentinel: 0 is a legal query_id and the three
                   reasons this can be absent are all "not measured", which is exactly what NULL means. */
                QueryId: reader.IsDBNull(11) ? null : reader.GetInt64(11),
                StateDurationMs: reader.IsDBNull(12) ? -1 : reader.GetInt64(12),
                XactDurationMs: reader.IsDBNull(13) ? -1 : reader.GetInt64(13),
                QueryDurationMs: reader.IsDBNull(14) ? -1 : reader.GetInt64(14),
                BackendDurationMs: reader.IsDBNull(15) ? -1 : reader.GetInt64(15),
                XminAge: reader.IsDBNull(16) ? -1 : reader.GetInt64(16),
                XidAge: reader.IsDBNull(17) ? -1 : reader.GetInt64(17),
                HorizonAge: reader.IsDBNull(18) ? -1 : reader.GetInt64(18),
                IsIdleInTransaction: !reader.IsDBNull(19) && reader.GetBoolean(19),
                IsHorizonHolder: !reader.IsDBNull(20) && reader.GetBoolean(20),
                StateIsRedacted: !reader.IsDBNull(21) && reader.GetBoolean(21),
                TotalSessions: reader.IsDBNull(22) ? 0 : reader.GetInt32(22),
                ActiveSessions: reader.IsDBNull(23) ? 0 : reader.GetInt32(23),
                IdleInTransactionSessions: reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                ReportableSessions: reader.IsDBNull(25) ? 0 : reader.GetInt32(25)));
        }

        /* Zero rows is the healthy state and the common one: no session had a transaction open past the
           floor and nothing held the horizon. It must never read as a failure, and it does not — the runner
           records SUCCESS with 0 rows, which is precisely what lets the read's capture counts tell "looked
           and found nothing" from "never looked". */
        return sessions;
    }

    private static string? Text(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every column here is a LEVEL measured at the instant of the sample — how long this
           session has been in this state, how old the xid it holds is. Differencing two samples would
           produce the elapsed time between samples, which is a property of the schedule and not of the
           session. */
        writer
            .Value(row.BackendId)
            .Value(row.Pid)
            .Value(row.DatabaseName)
            .Value(row.Username)
            .Value(row.ApplicationName)
            .Value(row.ClientAddr)
            .Value(row.BackendType)
            .Value(row.State)
            .Value(row.WaitEventType)
            .Value(row.WaitEvent)
            .Value(row.CommandTag)
            .Value(row.QueryId)
            .Value(row.StateDurationMs)
            .Value(row.XactDurationMs)
            .Value(row.QueryDurationMs)
            .Value(row.BackendDurationMs)
            .Value(row.XminAge)
            .Value(row.XidAge)
            .Value(row.HorizonAge)
            .Value(row.IsIdleInTransaction)
            .Value(row.IsHorizonHolder)
            .Value(row.StateIsRedacted)
            .Value(row.TotalSessions)
            .Value(row.ActiveSessions)
            .Value(row.IdleInTransactionSessions)
            .Value(row.ReportableSessions);
    }
}
