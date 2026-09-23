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
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /* filled by lane 17 of #3691 — the blocking / active-query family (design §2a). The marker stays, as v1's did. */

    /// <summary>
    /// Every blocked → blocking edge the window sampled, one row per (capture, pair), without either side's statement
    /// text: the reconstruction needs pids, states, durations and the blocker's identity, and the text is the one
    /// column this table stores that a doubles-only fact could never carry — <c>get_pg_blocking</c> is where the
    /// operator reads it, and the fact's <c>next_tools</c> says so. Durations keep the collector's <c>-1</c> sentinel
    /// through <c>coalesce</c> so a NULL and a "no transaction" read alike as "not known"; the reconstructor ignores
    /// negatives. Ordered by capture so the reader groups on a change of <c>collection_time</c> without a sort of its
    /// own. Not capped: a cap that cut a capture in half would reconstruct a chain that never existed, and the rows
    /// are narrow (a dozen columns, no text) — the window is hours, the cadence a minute, and a server with hundreds
    /// of blocked sessions every minute for hours is the finding, not a reason to read less of it.
    /// <para><c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).</para>
    /// </summary>
    public const string PgTargetBlockingEdgesSql = @"
SELECT
    collection_time,
    blocked_pid,
    blocking_pid,
    coalesce(blocked_query_duration_ms, -1)          AS blocked_query_ms,
    coalesce(blocked_xact_duration_ms, -1)           AS blocked_xact_ms,
    coalesce(blocking_is_idle_in_transaction, false) AS blocking_is_idle_in_transaction,
    coalesce(database_name, '')                      AS database_name,
    coalesce(blocking_application_name, '')          AS blocking_application_name,
    coalesce(blocking_username, '')                  AS blocking_username,
    coalesce(blocking_query_duration_ms, -1)         AS blocking_query_ms,
    coalesce(blocking_xact_duration_ms, -1)          AS blocking_xact_ms,
    coalesce(query_text_may_be_truncated, false)     AS query_text_may_be_truncated
FROM pg_blocking_edges
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   blocked_pid IS NOT NULL
AND   blocking_pid IS NOT NULL
ORDER BY collection_time, blocking_pid, blocked_pid";

    /// <summary>
    /// The sample's honest denominator — how many times the <c>pg_blocking</c> collector LOOKED in the window and how
    /// many of those looks recorded any edge — BY ALIAS to <see cref="DarlingPgBlockingReader.PgBlockingCaptureCountsSql"/>,
    /// the read <c>get_pg_blocking</c> reports <c>captures_total</c> from, so the fact and the tool cannot disagree on
    /// the count. <c>collection_log</c> is the one non-collector table an analysis read may name (the FROM/JOIN census
    /// admits it by name, with the xmin family's argument: only the log knows how often a collector ran and found
    /// nothing, and for a table that is EMPTY on a healthy instance that is the whole difference between "no
    /// blocking" and "not sampled"). <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window.
    /// </summary>
    public const string PgTargetBlockingCaptureCountsSql = DarlingPgBlockingReader.PgBlockingCaptureCountsSql;

    /// <summary>
    /// The two engine settings this family states: <c>deadlock_timeout</c> (the engine's own "a lock wait this long
    /// is logged" line — the one engine-defined number here, carried as context and never a bar) and
    /// <c>log_lock_waits</c> (whether the engine writes the <c>lock_wait</c> lines at all — off, and the event fact is
    /// <c>unavailable</c> rather than a false zero). From the latest <c>pg_server_config</c> snapshot at or before the
    /// window's end, the config family's own read shape (<c>PgTargetFactCollector.Config.cs</c>), including its
    /// three-value <c>source</c> exclusion — copied, not narrowed, for the reason that file gives. The config family
    /// emits neither name as a context fact (its list is the knobs and conventions it grades), so this family reads
    /// them itself. <c>$1</c> server_id, <c>$2</c> window end, <c>$3</c> the lower bound
    /// <see cref="ConfigSnapshotLowerBounds"/> hands it (#3928).
    /// </summary>
    public const string PgTargetBlockingSettingsSql = @"
SELECT
    c.name,
    c.setting,
    c.unit
FROM pg_server_config AS c
WHERE c.server_id = $1
AND   c.collection_time >= $3
AND   c.collection_time = (
          SELECT MAX(collection_time)
          FROM pg_server_config
          WHERE server_id = $1
          AND   collection_time >= $3
          AND   collection_time <= $2)
AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
/* V138 (#3691): server-wide rows only. pg_server_config now also holds the per-database and per-role
   overrides, which repeat a setting's NAME under a different scope; this read is keyed on the name, so an
   override row would shadow the server-wide value. The inner MAX(collection_time) is per SERVER and needs
   no predicate. */
AND   c.database_name IS NULL
AND   c.role_name IS NULL
AND   c.name IN ('deadlock_timeout', 'log_lock_waits')";

    /// <summary>
    /// The window's <c>lock_wait</c> family of <c>pg_log_events</c> (#3601) — the engine's own record of every lock
    /// wait that outlived <c>deadlock_timeout</c>, EVENT grain, four line shapes: <c>still waiting for … after N ms</c>
    /// (one per wait, written when the wait crosses the timeout — the COUNT of waits), <c>acquired … after N ms</c>
    /// (the same wait ending — the wait's true end-to-end length), <c>avoided deadlock … after N ms</c> and
    /// <c>detected deadlock … after N ms</c>.
    ///
    /// <para><b>The duration is read off the message, not the <c>duration_ms</c> column.</b> Verified at source
    /// (<c>PgLockWaitEventParser</c>): the lock_wait parser stores the line with NO metrics lifted — <c>duration_ms</c>
    /// and <c>relation_name</c> are NULL on every lock_wait row today (the autovacuum family fills them). The number
    /// is in <c>message</c>, which is stored as PostgreSQL wrote it (#3944), so <c>after ([0-9.]+) ms</c> is the
    /// engine's own figure; <c>coalesce</c> prefers the column the day a parser fills it. The relation, likewise,
    /// comes from the stored <c>CONTEXT</c> (<c>while updating tuple (0,7) in relation "orders"</c>) when the column
    /// is NULL.</para>
    ///
    /// <para><b>The window is on <c>collection_time</c></b> (the indexed column; the log collector runs every five
    /// minutes and stamps the batch), so an event is "in the window" when it was COLLECTED in it — a line written in
    /// the window's last minutes and collected after its end is the next pass's, one collected in its first minutes
    /// and written just before it is this pass's. Stated, bounded by one collector cadence, and the same rule every
    /// PostgreSQL family applies to its table.</para>
    ///
    /// <para><b>The two denominators ride the row.</b> <c>log_captures</c> — SUCCESS runs of the <c>pg_log_events</c>
    /// collector in the window, from <c>collection_log</c> — tells "no lines" apart from "nobody was reading the log"
    /// (the collector is optional and needs the RDS log API or file access); without it a silent collector would
    /// read as a quiet server. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window.</para>
    /// </summary>
    public const string PgTargetLockWaitEventsSql = @"
WITH events AS (
    SELECT
        message,
        occurred_at,
        statement_fingerprint,
        coalesce(relation_name, substring(coalesce(context, '') from 'in relation ""([^""]+)""')) AS relation_name,
        coalesce(duration_ms::DOUBLE PRECISION,
                 NULLIF(substring(message from 'after ([0-9]+(?:\.[0-9]+)?) ms'), '')::DOUBLE PRECISION) AS wait_ms
    FROM pg_log_events
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   family = 'lock_wait'
),
shape AS (
    SELECT
        COUNT(*) FILTER (WHERE message LIKE 'process % still waiting for %')                 AS still_waiting,
        COUNT(*) FILTER (WHERE message LIKE 'process % acquired %')                          AS acquired,
        COUNT(*) FILTER (WHERE message LIKE 'process % detected deadlock %')                 AS deadlocks,
        COUNT(*)                                                                             AS lines,
        MAX(wait_ms)                                                                         AS max_wait_ms,
        coalesce(SUM(wait_ms) FILTER (WHERE message LIKE 'process % acquired %'), 0)         AS acquired_wait_ms,
        MAX(occurred_at)                                                                     AS last_event_at
    FROM events
),
top_relation AS (
    SELECT relation_name, COUNT(*) AS waits
    FROM events
    WHERE relation_name IS NOT NULL
    AND   message LIKE 'process % still waiting for %'
    GROUP BY relation_name
    ORDER BY waits DESC, relation_name
    LIMIT 1
),
top_fingerprint AS (
    SELECT statement_fingerprint, COUNT(*) AS waits
    FROM events
    WHERE statement_fingerprint IS NOT NULL
    AND   message LIKE 'process % still waiting for %'
    GROUP BY statement_fingerprint
    ORDER BY waits DESC, statement_fingerprint
    LIMIT 1
),
looked AS (
    SELECT COUNT(*) AS log_captures
    FROM collection_log
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   collector_name = 'pg_log_events'
    AND   status = 'SUCCESS'
)
SELECT
    s.still_waiting,
    s.acquired,
    s.deadlocks,
    s.lines,
    s.max_wait_ms,
    s.acquired_wait_ms,
    s.last_event_at,
    (SELECT relation_name FROM top_relation)        AS top_relation,
    (SELECT waits FROM top_relation)                AS top_relation_waits,
    (SELECT statement_fingerprint FROM top_fingerprint) AS top_fingerprint,
    (SELECT waits FROM top_fingerprint)             AS top_fingerprint_waits,
    l.log_captures
FROM shape AS s
CROSS JOIN looked AS l";

    /// <summary>
    /// The window's long-running ACTIVE statements from <c>pg_session_states</c>: every stored row in the
    /// <c>active</c> state on a <c>client backend</c> whose <c>query_duration_ms</c> is at or over <c>$4</c> (the
    /// scorer's WARNING bar, passed so the SQL carries no second copy of it), grouped by RUNNER (pid + <c>query_id</c>
    /// + application + role + database), with recurrence as distinct captures, the longest sighting, and how many
    /// sightings carried a wait event. Longest first, 25 runners cap (<c>runners</c> states the count seen).
    ///
    /// <para><b>Three exclusions, each said.</b> <c>backend_type &lt;&gt; 'client backend'</c> drops autovacuum
    /// workers, the checkpointer, walsenders and every other engine process — a long autovacuum is the vacuum
    /// family's, and a walsender is "active" for the life of the standby. <c>wait_event_type = 'Lock'</c> rows are
    /// the BLOCKING CHAIN's: a statement that has run for an hour because it is queued behind a lock is a waiter,
    /// not a runner, and grading it here would count one wait twice — the chain fact names the head it is behind.
    /// Redacted rows are excluded by the flag: under redaction <c>state</c> and the durations are NULL for every
    /// backend the login does not own, so they could not qualify anyway, and the flag says so.</para>
    ///
    /// <para>The table stores a row only past the collector's own floors (thirty seconds of open transaction), so
    /// every ten-minute statement has rows; a NULL <c>query_id</c> (pre-14, <c>compute_query_id</c> off) groups as 0
    /// so the runner is still identified by pid. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window, <c>$4</c> floor ms.</para>
    /// </summary>
    public const string PgTargetLongRunningQuerySql = @"
WITH runners AS (
    SELECT
        collection_time,
        pid,
        coalesce(query_id, 0)               AS query_id,
        query_duration_ms,
        coalesce(application_name, '')      AS application_name,
        coalesce(username, '')              AS username,
        coalesce(database_name, '')         AS database_name,
        wait_event_type
    FROM pg_session_states
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   state = 'active'
    AND   coalesce(backend_type, '') = 'client backend'
    AND   NOT coalesce(state_is_redacted, false)
    AND   coalesce(wait_event_type, '') <> 'Lock'
    AND   query_duration_ms >= $4
),
identities AS (
    SELECT
        pid,
        query_id,
        application_name,
        username,
        database_name,
        COUNT(DISTINCT collection_time)                              AS captures_seen,
        MAX(query_duration_ms)                                       AS max_query_ms,
        MAX(collection_time)                                         AS last_seen_at,
        COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL)          AS waiting_sightings
    FROM runners
    GROUP BY pid, query_id, application_name, username, database_name
),
shape AS (
    SELECT
        COUNT(DISTINCT collection_time)       AS captures_with_runners,
        COUNT(*)                              AS runner_rows,
        COUNT(DISTINCT (pid, query_id))       AS runners
    FROM runners
)
SELECT
    i.pid,
    i.query_id,
    i.application_name,
    i.username,
    i.database_name,
    i.captures_seen,
    i.max_query_ms,
    i.last_seen_at,
    i.waiting_sightings,
    s.captures_with_runners,
    s.runner_rows,
    s.runners
FROM identities AS i
CROSS JOIN shape AS s
ORDER BY i.max_query_ms DESC, i.captures_seen DESC, i.pid
LIMIT 25";

    /// <summary>
    /// <c>PG_BLOCKING_CHAIN</c> from the window's <c>pg_blocking_edges</c> (reconstructed per capture — the <c>BlockingChainReconstructor</c> port from the <c>get_pg_blocking</c> reader, root attributed, recurrence counted across captures), <c>PG_LOCK_WAIT_EVENTS</c> from <c>pg_log_events</c> where <c>family = 'lock_wait'</c> (event grain, rated over observed time), and <c>PG_LONG_RUNNING_QUERY</c> from <c>pg_session_states</c>' active rows. The pattern copied is <c>PgTargetFactCollector.Sessions.cs</c> — one connection, one read per fact, the exception table's honest empty (no rows over the floor, no fact); the chain fact states its capture count so a sample is never read as an event log.
    ///
    /// <para><b>Emission order inside the family: settings, chain, events, long runner.</b> The settings read comes
    /// first because both the chain fact (it states <c>deadlock_timeout</c>) and the event fact (it needs
    /// <c>log_lock_waits</c> to tell a zero from a blindness) carry them; the chain before the events so the event
    /// fact's advice can name the root the chain attributed; the long runner last — it is the ACTIVE-query half of
    /// §2a and shares only the pid seam with the chain. The family runs after every other family (root partial), so
    /// the idle-in-transaction and Lock-wait facts are already in the list for the graph to join by key.</para>
    ///
    /// <para><b>Every read is inside one <c>try</c></b> with the shared three-outcome degrade: <c>pg_log_events</c>
    /// arrived in V129 and <c>pg_blocking_edges</c> in V71, so a pre-migration store raises 42P01, which the reporter
    /// classifies quiet; one unavailable table costs this server the whole family this pass and nothing else, and WHY
    /// is logged, not assumed (#2826). An abandonment is NOT swallowed (#2443). Every rate divides by
    /// <see cref="AnalysisContext.ObservedDurationMs"/>; every age is measured from the window's end the caller
    /// asked for — no clock of its own.</para>
    /// </summary>
    private async partial Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* The event fact's per-hour rate divides by observed time; an unobserved window has nothing to rate and the
           pass is "unavailable" on the coverage witness regardless. */
        if (context.ObservedDurationMs <= 0)
            return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            var windowEnd = AsNaive(context.TimeRangeEnd);

            var settings = await ReadBlockingSettingsAsync(connection, context);
            var chainEmitted = await ReadBlockingChainAsync(connection, context, facts, settings, windowEnd);
            await ReadLockWaitEventsAsync(connection, context, facts, settings, windowEnd, chainEmitted);
            await ReadLongRunningQueryAsync(connection, context, facts, windowEnd);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The two settings as the snapshot has them: <c>DeadlockTimeoutMs</c> is the engine default when the
    /// snapshot lacks the name; <c>LogLockWaitsOn</c> is null when the snapshot lacks it (unknown, not off).</summary>
    internal readonly record struct PgBlockingSettings(double DeadlockTimeoutMs, bool DeadlockTimeoutFromSnapshot, bool? LogLockWaitsOn);

    private async Task<PgBlockingSettings> ReadBlockingSettingsAsync(NpgsqlConnection connection, AnalysisContext context)
    {
        var deadlockTimeoutMs = PgTargetScorer.DeadlockTimeoutDefaultMs;
        var fromSnapshot = false;
        bool? logLockWaitsOn = null;

        /* #3928: the day first, and every retained snapshot only when that found nothing. */
        foreach (var lowerBound in ConfigSnapshotLowerBounds(context.TimeRangeEnd))
        {
            using var cmd = new NpgsqlCommand(PgTargetBlockingSettingsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(lowerBound);

            var found = false;
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                found = true;
                var name = reader.GetString(0);
                var setting = reader.IsDBNull(1) ? null : reader.GetString(1);
                var unit = reader.IsDBNull(2) ? null : reader.GetString(2);
                if (name == "deadlock_timeout" && ParseDurationMs(setting, unit) is { } ms)
                {
                    deadlockTimeoutMs = ms;
                    fromSnapshot = true;
                }
                else if (name == "log_lock_waits" && setting is not null)
                {
                    /* pg_settings spells booleans "on" / "off" (never true/false) — the same rule the config family's
                       autovacuum convention check reads by. */
                    logLockWaitsOn = string.Equals(setting, "on", StringComparison.OrdinalIgnoreCase);
                }
            }

            if (found) break;
        }

        return new PgBlockingSettings(deadlockTimeoutMs, fromSnapshot, logLockWaitsOn);
    }

    /// <summary><c>pg_settings</c> stores a duration as a number in the setting's own unit (<c>deadlock_timeout</c>:
    /// <c>1000</c> with unit <c>ms</c>); the unit is applied, a unitless value is taken as milliseconds.</summary>
    internal static double? ParseDurationMs(string? setting, string? unit)
    {
        if (!double.TryParse(setting, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || value < 0)
            return null;
        return unit switch
        {
            "us" => value / 1_000.0,
            "s" => value * 1_000.0,
            "min" => value * 60_000.0,
            "h" => value * 3_600_000.0,
            "d" => value * 86_400_000.0,
            _ => value,
        };
    }

    /* ── the chain: reconstruction over the window's sampled edges ── */

    /// <summary>One sampled edge, as <see cref="PgTargetBlockingEdgesSql"/> returns it. Durations are the collector's
    /// own (<c>-1</c> = not known).</summary>
    internal readonly record struct PgBlockingEdgeSample(
        DateTime CapturedAt,
        int BlockedPid,
        int BlockingPid,
        long BlockedQueryMs,
        long BlockedXactMs,
        bool BlockingIsIdleInTransaction,
        string DatabaseName,
        string BlockingApplicationName,
        string BlockingUsername,
        long BlockingQueryMs,
        long BlockingXactMs,
        bool QueryTextTruncated);

    /// <summary>One head blocker in one capture: a blocker that is not itself blocked in that capture, with the
    /// sessions behind it (every waiter reachable through the blocked → blocking edges), the depth of the longest
    /// path, and the head's own state as the edge rows carried it.</summary>
    internal sealed record PgBlockingHead(
        int Pid,
        string ApplicationName,
        string Username,
        string DatabaseName,
        bool IsIdleInTransaction,
        int Depth,
        int BlockedSessions,
        long MaxBlockedQueryMs,
        long MaxBlockedXactMs,
        long QueryMs,
        long XactMs,
        bool QueryTextTruncated);

    /// <summary>One capture reconstructed: its heads, how many distinct sessions were blocked in it at all, and
    /// whether some blocked session was reachable from NO head (a cycle in flight — a deadlock the detector had not
    /// yet broken when the sample landed; counted, never attributed).</summary>
    internal sealed record PgBlockingCapture(DateTime CapturedAt, IReadOnlyList<PgBlockingHead> Heads, int BlockedSessions, bool HasCycle);

    /// <summary>
    /// The <c>BlockingChainReconstructor</c> IDEA, ported and not its code: SQL Server's reconstructor walks a
    /// blocked-process report's (spid, ecid, monitor_loop) triples with a 1900-01-01 sentinel and dedupes repeated
    /// reports of one episode; none of that exists here. A PostgreSQL capture is one moment's edge list, so a HEAD
    /// is a <c>blocking_pid</c> that is not any row's <c>blocked_pid</c> in the same capture, the sessions behind it
    /// are the waiters reachable by following "who is blocked by X" edges, and depth is the longest such path. A
    /// waiter blocked by two pids (parallel workers, multiple lock holders) is reached from both heads and counted
    /// behind each — the sample says both hold it up. Cycles are tolerated by a visited set on every walk (a waiter
    /// reached twice is counted once and not descended again), and a blocked pid no head reaches is a cycle in
    /// flight, reported on the capture.
    /// </summary>
    internal static PgBlockingCapture ReconstructBlockingCapture(DateTime capturedAt, IReadOnlyList<PgBlockingEdgeSample> edges)
    {
        ArgumentNullException.ThrowIfNull(edges);

        var waitersBehind = new Dictionary<int, List<PgBlockingEdgeSample>>();
        var blocked = new HashSet<int>();
        foreach (var edge in edges)
        {
            blocked.Add(edge.BlockedPid);
            if (!waitersBehind.TryGetValue(edge.BlockingPid, out var list))
                waitersBehind[edge.BlockingPid] = list = [];
            list.Add(edge);
        }

        var reached = new HashSet<int>();
        var heads = new List<PgBlockingHead>();
        foreach (var (headPid, direct) in waitersBehind.Where(kv => !blocked.Contains(kv.Key)).OrderBy(kv => kv.Key))
        {
            var visited = new HashSet<int> { headPid };
            var depth = 0;
            var maxBlockedQueryMs = -1L;
            var maxBlockedXactMs = -1L;
            var frontier = new Queue<(int Pid, int Level)>();
            frontier.Enqueue((headPid, 0));
            while (frontier.Count > 0)
            {
                var (pid, level) = frontier.Dequeue();
                if (!waitersBehind.TryGetValue(pid, out var behind))
                    continue;
                foreach (var edge in behind)
                {
                    maxBlockedQueryMs = Math.Max(maxBlockedQueryMs, edge.BlockedQueryMs);
                    maxBlockedXactMs = Math.Max(maxBlockedXactMs, edge.BlockedXactMs);
                    if (!visited.Add(edge.BlockedPid))
                        continue;
                    reached.Add(edge.BlockedPid);
                    depth = Math.Max(depth, level + 1);
                    frontier.Enqueue((edge.BlockedPid, level + 1));
                }
            }

            /* The head's own state and identity ride every direct edge alike (one backend, one capture); the idle flag
               is OR-ed so a row that carried it wins over a row that did not. */
            var first = direct[0];
            heads.Add(new PgBlockingHead(
                headPid,
                first.BlockingApplicationName,
                first.BlockingUsername,
                first.DatabaseName,
                direct.Any(e => e.BlockingIsIdleInTransaction),
                depth,
                visited.Count - 1,
                maxBlockedQueryMs,
                maxBlockedXactMs,
                direct.Max(e => e.BlockingQueryMs),
                direct.Max(e => e.BlockingXactMs),
                direct.Any(e => e.QueryTextTruncated)));
        }

        return new PgBlockingCapture(capturedAt, heads, blocked.Count, blocked.Any(pid => !reached.Contains(pid)));
    }

    /// <summary>The identity a head persists under across captures: a pid is one backend's lifetime, and the
    /// application and role guard the rare recycled pid inside one window.</summary>
    private readonly record struct HeadKey(int Pid, string ApplicationName, string Username);

    private sealed class HeadTally
    {
        public int Captures;
        public long BlockedSessions;
        public int MaxDepth;
        public PgBlockingHead Worst = null!;
        public DateTime LastSeenAt;
    }

    /// <summary>
    /// <c>PG_BLOCKING_CHAIN</c>: every edge in the window, grouped by capture and reconstructed; across the window the
    /// deepest chain, the peak blocked count, the head with the most sessions behind it summed over its captures (the
    /// NAMED head — <see cref="Fact.ObjectName"/> <c>application as role</c>, <see cref="Fact.DatabaseName"/> its
    /// database), and the most captures ANY head was the root of (the persistence witness — the chronic head need not
    /// be the most-blocked one, lane 14's recurrence rule). <see cref="Fact.Value"/> is the longest blocked statement
    /// in seconds; <see cref="PgTargetScorer.BlockingMaxBlockedQueryMsKey"/> is what grades. No edge in the window,
    /// no fact — and the SAMPLE caveat is on the fact that does exist: <c>captures_total</c> from
    /// <c>collection_log</c> beside <c>captures_with_blocking</c>, so "blocking in 3 captures" reads as 3 of 240
    /// looks, never as three events.
    /// </summary>
    private async Task<bool> ReadBlockingChainAsync(
        NpgsqlConnection connection, AnalysisContext context, List<Fact> facts, PgBlockingSettings settings, DateTime windowEnd)
    {
        var captures = new List<PgBlockingCapture>();
        using (var cmd = new NpgsqlCommand(PgTargetBlockingEdgesSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
        {
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            DateTime? current = null;
            var edges = new List<PgBlockingEdgeSample>();
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var capturedAt = reader.GetDateTime(0);
                if (current is { } open && open != capturedAt)
                {
                    captures.Add(ReconstructBlockingCapture(open, edges));
                    edges = [];
                }
                current = capturedAt;
                edges.Add(new PgBlockingEdgeSample(
                    capturedAt,
                    reader.GetInt32(1),
                    reader.GetInt32(2),
                    ToInt64(reader.GetValue(3)),
                    ToInt64(reader.GetValue(4)),
                    reader.GetBoolean(5),
                    reader.GetString(6),
                    reader.GetString(7),
                    reader.GetString(8),
                    ToInt64(reader.GetValue(9)),
                    ToInt64(reader.GetValue(10)),
                    reader.GetBoolean(11)));
            }
            if (current is { } last)
                captures.Add(ReconstructBlockingCapture(last, edges));
        }

        if (captures.Count == 0)
            return false;

        /* The denominator, by alias to the reader get_pg_blocking uses. */
        long capturesTotal = 0, capturesWithBlockingLogged = 0;
        using (var cmd = new NpgsqlCommand(PgTargetBlockingCaptureCountsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
        {
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (await reader.ReadAsync(context.CancellationToken))
            {
                capturesWithBlockingLogged = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
                capturesTotal = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            }
        }

        var tallies = new Dictionary<HeadKey, HeadTally>();
        long maxBlockedQueryMs = -1, maxBlockedXactMs = -1;
        var longestDepth = 0;
        var peakBlocked = 0;
        DateTime? peakAt = null;
        var cycleCaptures = 0;
        foreach (var capture in captures)
        {
            if (capture.HasCycle) cycleCaptures++;
            if (capture.BlockedSessions > peakBlocked || (capture.BlockedSessions == peakBlocked && peakAt is { } p && capture.CapturedAt > p))
            {
                peakBlocked = capture.BlockedSessions;
                peakAt = capture.CapturedAt;
            }
            foreach (var seen in capture.Heads)
            {
                longestDepth = Math.Max(longestDepth, seen.Depth);
                maxBlockedQueryMs = Math.Max(maxBlockedQueryMs, seen.MaxBlockedQueryMs);
                maxBlockedXactMs = Math.Max(maxBlockedXactMs, seen.MaxBlockedXactMs);
                var key = new HeadKey(seen.Pid, seen.ApplicationName, seen.Username);
                if (!tallies.TryGetValue(key, out var tally))
                    tallies[key] = tally = new HeadTally { Worst = seen };
                tally.Captures++;
                tally.BlockedSessions += seen.BlockedSessions;
                tally.MaxDepth = Math.Max(tally.MaxDepth, seen.Depth);
                tally.LastSeenAt = capture.CapturedAt;
                /* The worst sighting names the head: most sessions behind it, then deepest, then the latest. */
                if (seen.BlockedSessions > tally.Worst.BlockedSessions
                    || (seen.BlockedSessions == tally.Worst.BlockedSessions && seen.Depth >= tally.Worst.Depth))
                    tally.Worst = seen;
            }
        }

        if (tallies.Count == 0)
        {
            /* Every capture was a pure cycle (deadlocks in flight, no head anywhere): the deadlock family owns that
               story and the chain fact would have no root to name. Logged, no fact. */
            _logger?.LogDebug(
                "[PgTargetFactCollector] CollectBlockingFactsAsync on server {ServerId} ({ServerName}) saw {Captures} capture(s) of blocking edges, all of them cycles with no head; no chain fact this pass.",
                context.ServerId, context.ServerName, captures.Count);
            return false;
        }

        var named = tallies
            .OrderByDescending(kv => kv.Value.BlockedSessions)
            .ThenByDescending(kv => kv.Value.Captures)
            .ThenBy(kv => kv.Key.Pid)
            .First();
        var recurringHeadCaptures = tallies.Values.Max(t => t.Captures);
        var head = named.Value.Worst;

        var fact = new Fact
        {
            Source = PgTargetSources.BlockingSource,
            Key = PgTargetFactKeys.BlockingChain,
            Value = Math.Max(0, maxBlockedQueryMs) / 1_000.0,
            ServerId = context.ServerId,
            DatabaseName = string.IsNullOrEmpty(head.DatabaseName) ? null : head.DatabaseName,
            ObjectName = $"{(string.IsNullOrEmpty(head.ApplicationName) ? "(no application_name)" : head.ApplicationName)} as {(string.IsNullOrEmpty(head.Username) ? "(unknown role)" : head.Username)}",
            Metadata =
            {
                [PgTargetScorer.BlockingMaxBlockedQueryMsKey] = Math.Max(0, maxBlockedQueryMs),
                [PgTargetScorer.BlockingMaxBlockedXactMsKey] = Math.Max(0, maxBlockedXactMs),
                [PgTargetScorer.BlockingHeadPidKey] = head.Pid,
                [PgTargetScorer.BlockingHeadIsIdleInTransactionKey] = head.IsIdleInTransaction ? 1 : 0,
                [PgTargetScorer.BlockingHeadBlockedSessionsKey] = named.Value.BlockedSessions,
                [PgTargetScorer.BlockingHeadCapturesKey] = named.Value.Captures,
                [PgTargetScorer.BlockingRecurringHeadCapturesKey] = recurringHeadCaptures,
                ["head_depth"] = named.Value.MaxDepth,
                ["head_query_ms"] = Math.Max(0, head.QueryMs),
                ["head_xact_ms"] = Math.Max(0, head.XactMs),
                [PgTargetScorer.BlockingHeadQueryTruncatedKey] = head.QueryTextTruncated ? 1 : 0,
                ["distinct_heads"] = tallies.Count,
                [PgTargetScorer.BlockingLongestChainDepthKey] = longestDepth,
                [PgTargetScorer.BlockingPeakBlockedSessionsKey] = peakBlocked,
                [PgTargetScorer.BlockingCapturesWithBlockingKey] = captures.Count,
                [PgTargetScorer.BlockingCapturesTotalKey] = capturesTotal,
                ["captures_with_blocking_logged"] = capturesWithBlockingLogged,
                [PgTargetScorer.BlockingCycleCapturesKey] = cycleCaptures,
                [PgTargetScorer.BlockingDeadlockTimeoutMsKey] = settings.DeadlockTimeoutMs,
                [PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey] = settings.DeadlockTimeoutFromSnapshot ? 1 : 0,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        };
        fact.Metadata["head_last_seen_age_s"] = Math.Max(0, (windowEnd - AsNaive(named.Value.LastSeenAt)).TotalSeconds);
        if (peakAt is { } at)
            fact.Metadata["peak_age_s"] = Math.Max(0, (windowEnd - AsNaive(at)).TotalSeconds);

        facts.Add(fact);
        return true;
    }

    /* ── the events: the engine's own record ── */

    /// <summary>
    /// <c>PG_LOCK_WAIT_EVENTS</c> from <see cref="PgTargetLockWaitEventsSql"/>. The honest answer has FOUR shapes and
    /// three of them are not "nothing": events present (a graded fact — count, longest wait, waits per observed hour,
    /// the most-waited relation as <see cref="Fact.ObjectName"/>); no events with <c>log_lock_waits</c> on and the log
    /// collector running (<c>no_events = 1</c>, a measured zero, base 0); no events with the setting off
    /// (<c>unavailable</c>, <c>reason_log_lock_waits_off</c> — the engine wrote nothing, so silence says nothing); no
    /// events with the setting unknown (no snapshot names it — <c>reason_log_lock_waits_unknown</c>) or with the
    /// setting on but no SUCCESS run of the log collector in the window (<c>reason_log_collector_silent</c> — nobody
    /// was reading the log). An event that DID arrive settles the question whatever the snapshot says: the engine was
    /// writing.
    ///
    /// <para><b>When the three silent shapes are emitted at all.</b> Only beside a chain fact from the same window
    /// (<paramref name="chainEmitted"/>): that is where "the log has nothing" needs saying — the operator reading the
    /// chain's card should know whether the engine's own record corroborates the sample, could not (off), or was not
    /// read (silent). On a server with no sampled blocking the silence is the ordinary state of a healthy instance,
    /// and a fact stating it on every pass would be a row on every quiet PostgreSQL target's <c>get_analysis_facts</c>
    /// saying nothing about that server — the "quiet server, no facts" contract every other family keeps (and three
    /// families' e2e pins hold). With events present the fact is emitted regardless: the engine wrote waits down
    /// whether or not the one-minute sample caught them, and that IS the finding.</para>
    /// </summary>
    private async Task ReadLockWaitEventsAsync(
        NpgsqlConnection connection, AnalysisContext context, List<Fact> facts, PgBlockingSettings settings, DateTime windowEnd, bool chainEmitted)
    {
        using var cmd = new NpgsqlCommand(PgTargetLockWaitEventsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
            return;

        var stillWaiting = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        var acquired = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
        var deadlocks = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
        var lines = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
        var maxWaitMs = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4), CultureInfo.InvariantCulture);
        var acquiredWaitMs = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5), CultureInfo.InvariantCulture);
        var lastEventAt = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6);
        var topRelation = reader.IsDBNull(7) ? null : reader.GetString(7);
        var topRelationWaits = reader.IsDBNull(8) ? 0L : ToInt64(reader.GetValue(8));
        var topFingerprintWaits = reader.IsDBNull(10) ? 0L : ToInt64(reader.GetValue(10));
        var logCaptures = reader.IsDBNull(11) ? 0L : ToInt64(reader.GetValue(11));

        if (lines == 0 && !chainEmitted)
            return;

        var fact = new Fact
        {
            Source = PgTargetSources.BlockingSource,
            Key = PgTargetFactKeys.LockWaitEvents,
            Value = maxWaitMs / 1_000.0,
            ServerId = context.ServerId,
            ObjectName = string.IsNullOrEmpty(topRelation) ? null : topRelation,
            Metadata =
            {
                [PgTargetScorer.LockWaitEventsCountKey] = stillWaiting,
                ["acquired_events"] = acquired,
                [PgTargetScorer.LockWaitEventsDeadlocksKey] = deadlocks,
                ["lines"] = lines,
                [PgTargetScorer.LockWaitEventsMaxMsKey] = maxWaitMs,
                [PgTargetScorer.LockWaitEventsAcquiredMsKey] = acquiredWaitMs,
                [PgTargetScorer.LockWaitEventsPerHourKey] = stillWaiting / (context.ObservedDurationMs / 3_600_000.0),
                ["top_relation_waits"] = topRelationWaits,
                ["top_fingerprint_waits"] = topFingerprintWaits,
                ["log_captures"] = logCaptures,
                [PgTargetScorer.BlockingDeadlockTimeoutMsKey] = settings.DeadlockTimeoutMs,
                [PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey] = settings.DeadlockTimeoutFromSnapshot ? 1 : 0,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        };
        if (settings.LogLockWaitsOn is { } on)
            fact.Metadata[PgTargetScorer.LockWaitLogLockWaitsOnKey] = on ? 1 : 0;
        if (lastEventAt is { } last)
            fact.Metadata["last_event_age_s"] = Math.Max(0, (windowEnd - AsNaive(last)).TotalSeconds);

        if (lines == 0)
        {
            /* The three silences, told apart. Precedence: the setting first (off means the engine wrote nothing, whether
               or not anyone read the log), then the reader (on, but nobody collected), then the measured zero. */
            if (settings.LogLockWaitsOn == false)
            {
                fact.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
                fact.Metadata[PgTargetScorer.LockWaitReasonLogLockWaitsOffKey] = 1;
            }
            else if (settings.LogLockWaitsOn is null)
            {
                fact.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
                fact.Metadata[PgTargetScorer.LockWaitReasonSettingUnknownKey] = 1;
            }
            else if (logCaptures == 0)
            {
                fact.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
                fact.Metadata[PgTargetScorer.LockWaitReasonLogCollectorSilentKey] = 1;
            }
            else
            {
                fact.Metadata[PgTargetScorer.LockWaitNoEventsKey] = 1;
            }
        }

        facts.Add(fact);
    }

    /* ── the long runner: the active-query half of §2a ── */

    /// <summary>
    /// <c>PG_LONG_RUNNING_QUERY</c> from <see cref="PgTargetLongRunningQuerySql"/>: no row over the floor, no fact
    /// (the exception table's honest empty). Otherwise ONE fact for the window named for the LONGEST runner
    /// (<see cref="Fact.ObjectName"/> <c>application as role</c>, <see cref="Fact.DatabaseName"/>), <see cref="Fact.Value"/>
    /// its longest sighting in seconds, and the rest as metadata: the milliseconds the scorer grades, the runner's pid
    /// (the seam to the chain's head) and <c>query_id</c>, the most captures ANY runner recurred in (the amplifier's
    /// witness), how many runners were over the floor, the window's shape. Runs inside the caller's <c>try</c>.
    /// </summary>
    private async Task ReadLongRunningQueryAsync(NpgsqlConnection connection, AnalysisContext context, List<Fact> facts, DateTime windowEnd)
    {
        using var cmd = new NpgsqlCommand(PgTargetLongRunningQuerySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        /* The scorer's WARNING bar IS the read floor (unmeasured, stated on the constant): a row under it is not a
           runner this fact speaks of, and the constant is passed so the SQL cannot carry a second copy. */
        cmd.Parameters.AddWithValue((long)PgTargetScorer.LongRunningQueryWarningMs);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
            return;

        /* Row 1 is the longest runner (ORDER BY max_query_ms DESC); the window shape repeats on every row. */
        var pid = reader.GetInt32(0);
        var queryId = ToInt64(reader.GetValue(1));
        var applicationName = reader.GetString(2);
        var username = reader.GetString(3);
        var databaseName = reader.GetString(4);
        var runnerCaptures = ToInt64(reader.GetValue(5));
        var maxQueryMs = ToInt64(reader.GetValue(6));
        var lastSeenAt = reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7);
        var waitingSightings = ToInt64(reader.GetValue(8));
        var capturesWithRunners = ToInt64(reader.GetValue(9));
        var runnerRows = ToInt64(reader.GetValue(10));
        var runners = ToInt64(reader.GetValue(11));

        if (maxQueryMs <= 0)
            return;

        var recurringCaptures = runnerCaptures;
        while (await reader.ReadAsync(context.CancellationToken))
            recurringCaptures = Math.Max(recurringCaptures, ToInt64(reader.GetValue(5)));

        var fact = new Fact
        {
            Source = PgTargetSources.BlockingSource,
            Key = PgTargetFactKeys.LongRunningQuery,
            Value = maxQueryMs / 1_000.0,
            ServerId = context.ServerId,
            DatabaseName = string.IsNullOrEmpty(databaseName) ? null : databaseName,
            ObjectName = $"{(string.IsNullOrEmpty(applicationName) ? "(no application_name)" : applicationName)} as {(string.IsNullOrEmpty(username) ? "(unknown role)" : username)}",
            Metadata =
            {
                [PgTargetScorer.LongRunningQueryMaxMsKey] = maxQueryMs,
                [PgTargetScorer.LongRunningQueryPidKey] = pid,
                /* A bigint fingerprint stored in a double: exact to 2^53, which is every real query_id's low bits'
                   worth of identity for a join by eye against get_pg_session_states; the pid is the exact seam. */
                [PgTargetScorer.LongRunningQueryIdKey] = queryId,
                ["runner_captures_seen"] = runnerCaptures,
                [PgTargetScorer.LongRunningQueryCapturesKey] = recurringCaptures,
                [PgTargetScorer.LongRunningQueryWaitingKey] = waitingSightings > 0 ? 1 : 0,
                ["runner_waiting_sightings"] = waitingSightings,
                ["runners"] = runners,
                ["runner_rows"] = runnerRows,
                ["captures_with_runners"] = capturesWithRunners,
                ["floor_ms"] = PgTargetScorer.LongRunningQueryWarningMs,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        };
        if (lastSeenAt is { } last)
            fact.Metadata["runner_last_seen_age_s"] = Math.Max(0, (windowEnd - AsNaive(last)).TotalSeconds);

        facts.Add(fact);
    }
}
