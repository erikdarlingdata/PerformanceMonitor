/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.ChangeWatch;

/// <summary>
/// #6234: the change watch's worker pass, run on the investigation sweep's tick. It does two things for SQL Server targets.
/// <para><b>Detect, at most once an hour per target.</b> It reads the recorded changes since the target's last detect (the same snapshot readers and diff the
/// <c>get_server_config_changes</c>, <c>get_database_config_changes</c> and <c>get_trace_flag_changes</c> tools use; the
/// database-scoped configuration snapshots; plan forcing by an agent, from the plan-force ledger, and by a person, from the forced-plan
/// snapshots) and records them: each change opens a watch for its server or extends the open one. A configuration change is rare and the
/// read is the costliest statement the service sends to the store (the database-scoped configuration diff), so a target is read once per
/// <see cref="DetectInterval"/> (#6234), not on every sweep tick. The time of a target's last detect is kept in memory only: a new
/// instance (a restart) detects every target on its first run, and the marks and the change keys in the store, not that time, are what
/// keep a change from being missed or recorded twice (see <see cref="DetectLookback"/>).</para>
/// <para><b>Evaluate, on every run.</b> Every open watch whose window has ended (including one that ended while the service was down: the pass
/// runs at start) is compared with the hour before and the same hour a week earlier. When a measure moved, ONE request opens through
/// the dispatcher's own gate and route (<c>change_watch</c> trigger kind); otherwise the watch closes quietly. Finding the due watches is one
/// indexed read of the small watch table, and the measure reads happen only for a watch that is due, so this part stays on the sweep's tick and a
/// watch is still evaluated within a tick of its window's end.</para>
/// </summary>
public sealed partial class ChangeWatchPass
{
    /// <summary>
    /// #6234: the least time between two detects of one target. The database-scoped configuration diff was the store's costliest statement at a
    /// tick of 60 seconds, and a setting changes far less often than that. A target detects at once on a new <see cref="ChangeWatchPass"/> (the
    /// first run after a start) and then once this long after its last detect finished reading. A watch is evaluated at the end of its window, which
    /// is <see cref="ChangeWatchStore.Window"/> after the latest change, so a change is recorded up to this long (and one sweep tick) after it
    /// happened. The watch's window is still measured from the change's own time, so the measures are the same, and the watch is evaluated at the end of that window,
    /// or on the run after the one that records it when that end is already past.
    /// </summary>
    public static readonly TimeSpan DetectInterval = TimeSpan.FromMinutes(60);

    /// <summary>
    /// How far back a detect looks for a change: twice <see cref="DetectInterval"/>. The marks, not this, decide what is read: a detect reads from each
    /// source's mark, which sits <see cref="MarkSlack"/> before the end of the target's last detect, so an hourly cadence reads about an hour and five
    /// minutes. This is only the cap on a stretch with no detect at all (the service paused or down). It is twice the interval, and not the 70 minutes it
    /// was at a tick of 60 seconds, so that a detect that comes late by a long sweep, a pause or a slow store (more than five minutes at 70, now up to
    /// about an hour) still reads from its mark and loses nothing. A change is recorded once under its key, so a longer look only costs reads.
    /// </summary>
    public static readonly TimeSpan DetectLookback = DetectInterval + DetectInterval;

    /// <summary>
    /// #6234: how long a target whose detect failed waits before it is tried again. A detect that fails on every run (a read that always throws)
    /// would otherwise cost a full detect and a warning on every sweep tick, which is the cost <see cref="DetectInterval"/> removes.
    /// </summary>
    public static readonly TimeSpan FailedDetectBackoff = TimeSpan.FromMinutes(5);

    /* #6234 PostgreSQL pass: every enabled target, with its engine kind: a PostgreSQL target reads its own change sources
       (ChangeWatchPass.Postgres.cs) and is evaluated by its own readers. */
    private const string ServersSql = "SELECT s.server_id, s.server_name, s.engine_kind FROM servers AS s WHERE s.is_enabled";

    /// <summary>How far a watch window's own mark may lag the present: the marks move to this far behind the pass, so a row the store makes visible a little late is still read next tick (a change is recorded once under its key).</summary>
    public static readonly TimeSpan MarkSlack = TimeSpan.FromMinutes(5);

    /* The baseline is the newest capture at or before the mark ($2): the first capture after the mark is diffed against it. */
    private static readonly string ScopedConfigChangesSql = $@"
SELECT capture_time, database_name, configuration_name, value, prev_value, value_for_secondary, prev_secondary
FROM (
    SELECT capture_time, database_name, configuration_name, value, value_for_secondary,
           LAG(value) OVER w AS prev_value, LAG(value_for_secondary) OVER w AS prev_secondary, row_number() OVER w AS rn
    FROM v_database_scoped_config
    WHERE server_id = $1 AND {PerformanceMonitor.Darling.Service.Mcp.DarlingConfigHistoryReader.BaselineBound("v_database_scoped_config", "$1", "$2", perColumn: "database_name")}
    WINDOW w AS (PARTITION BY database_name, configuration_name ORDER BY capture_time)
) AS x
WHERE rn > 1 AND capture_time > $2 AND capture_time <= $3
  AND (value IS DISTINCT FROM prev_value OR value_for_secondary IS DISTINCT FROM prev_secondary)";

    private const string PlanActionsSql = @"
SELECT action_id, action_time, database_name, query_id, plan_id, action
FROM collect.plan_force_actions
WHERE server_id = $1 AND outcome = 'succeeded' AND action_time > $2 AND action_time <= $3";

    /* #6234, #6239 round 3 (H1, H2, L1): plan changes by a person, read the way DarlingForcedPlanStateReader reads them. The forced-plan collector writes
       ONE marker row per CAPTURED database per pass (database_name set, plan_id and query_id NULL), so a database's captures are the distinct
       (collection_time, database_name) pairs. For each capture after the mark ($2) the plans are compared with the SAME database's previous capture: a plan in
       the newer one and not the older was forced, the reverse was unforced. A database with no previous capture is a baseline, and a database that has no
       capture in a run (failover, Query Store off, the collector failed for it) has nothing to compare, so it is never read as an unforce.
       #6239 round 3, L5: the read covers only the captures after the mark ($2) up to $3. Each one's previous capture is the newest marker row (plan_id IS NULL) of
       the same database in the day before it, found by a LATERAL seek on idx_query_store_forced_plans_markers (V207), so a pass no longer sorts a day of forced-plan
       rows per server. #6239 round 3, L4: a database whose previous capture is more than $4 minutes before this one (it came back after a failback, a
       Query Store that was read-only, a collector that failed for hours) has this capture treated as a NEW BASELINE. #6234: $4 is CaptureGapLimit of the
       forced-plans collector's frequency on this server (the larger of 30 minutes and twice the frequency plus 5 minutes), not a fixed 30 minutes: at a 60-minute
       frequency every capture is more than 30 minutes after the last and no plan change would ever be read, and at the 5-minute default twice the frequency plus
       5 minutes is only 15, so the 30-minute floor keeps a capture delayed by load from dropping a real plan change. A skipped gap is one of more than a missed capture or two: what was forced or unforced during the gap
       cannot be dated, and reporting it at the return capture would open a watch bracketing the failback and not the change. #6239 round 3, L3: a change the
       plan-force ledger explains (a succeeded action on the plan AFTER the previous capture and up to this one) is skipped; a fixed 30 minutes missed an action
       early in a longer gap. The other capture's plans are probed with a LATERAL LIMIT 1 per plan row (an index probe on the capture's time), because as an
       anti-join the planner hashes the whole day's rows on the other side.
       #6234: the captures are a GROUP BY and not a SELECT DISTINCT. On TimescaleDB 2.30 the planner turns the DISTINCT over idx_query_store_forced_plans_markers into a
       Custom Scan (SkipScan) (skip qual database_name > NULL), and when the mark is old or the markers are few next to the plan rows it fails at run time with XX000
       "ScanKey for skip qual not found", so no plan change was read on a SQL Server fleet. SkipScan only rewrites DISTINCT; the GROUP BY keeps the same index scan. */
    internal const string PlanSnapshotChangesSql = @"
WITH fresh AS (
    SELECT m.t, m.database_name, p.prev_t
    FROM (SELECT collection_time AS t, database_name
          FROM collect.query_store_forced_plans
          WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3 AND database_name IS NOT NULL AND plan_id IS NULL
          GROUP BY collection_time, database_name) AS m
    CROSS JOIN LATERAL (SELECT max(q.collection_time) AS prev_t
                        FROM collect.query_store_forced_plans AS q
                        WHERE q.server_id = $1 AND q.database_name = m.database_name AND q.plan_id IS NULL
                          AND q.collection_time < m.t AND q.collection_time >= m.t - interval '1 day') AS p
    WHERE p.prev_t IS NOT NULL AND m.t - p.prev_t <= $4 * interval '1 minute'
)
SELECT 'forced' AS change_kind, n.t, n.database_name, f.query_id, f.plan_id, f.plan_forcing_type
FROM fresh AS n
JOIN collect.query_store_forced_plans AS f
  ON f.server_id = $1 AND f.collection_time = n.t AND f.database_name = n.database_name AND f.plan_id IS NOT NULL AND f.query_id IS NOT NULL
LEFT JOIN LATERAL (SELECT 1 AS hit FROM collect.query_store_forced_plans AS p
                   WHERE p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name
                     AND p.query_id = f.query_id AND p.plan_id = f.plan_id LIMIT 1) AS before ON true
WHERE before.hit IS NULL
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = $1 AND a.database_name = f.database_name AND a.query_id = f.query_id AND a.plan_id = f.plan_id
                    AND a.outcome = 'succeeded' AND a.action_time > n.prev_t AND a.action_time <= n.t)
UNION ALL
SELECT 'unforced', n.t, n.database_name, p.query_id, p.plan_id, p.plan_forcing_type
FROM fresh AS n
JOIN collect.query_store_forced_plans AS p
  ON p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name AND p.plan_id IS NOT NULL AND p.query_id IS NOT NULL
LEFT JOIN LATERAL (SELECT 1 AS hit FROM collect.query_store_forced_plans AS f
                   WHERE f.server_id = $1 AND f.collection_time = n.t AND f.database_name = n.database_name
                     AND f.query_id = p.query_id AND f.plan_id = p.plan_id LIMIT 1) AS after ON true
WHERE after.hit IS NULL
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = $1 AND a.database_name = p.database_name AND a.query_id = p.query_id AND a.plan_id = p.plan_id
                    AND a.outcome = 'succeeded' AND a.action_time > n.prev_t AND a.action_time <= n.t)";

    /* #6581 slice 2: Query Store hint changes, read the way PlanSnapshotChangesSql reads forced plans, from collect.query_store_hints (V217). The collector
       writes ONE all-NULL marker row (query_hint_id IS NULL) per captured database per pass, then one row per hint. A capture is a marker row, and its previous
       capture is the newest marker row of the same database in the day before it (a LATERAL seek on idx_query_store_hints_markers), so a database with no
       previous capture is a baseline, one that has no capture in a run (Query Store off, the collector failed or skipped it, SQL Server before 2022) has nothing
       to compare and is never read as a clear, and a database whose previous capture is more than $4 minutes before this one is a NEW BASELINE (the same gap
       rule as the plan diff, from the hints collector's own frequency). A hint id new in this capture is 'set'; the same id with different text is
       'changed'; an id in the previous capture and not in this one is 'cleared'. The captures are a GROUP BY and not a SELECT DISTINCT for the reason the plan
       diff's are (TimescaleDB's SkipScan rewrite of DISTINCT, #6234). Darling sets no hints, so no ledger explains a change away. (database, query_id, query_hint_id) identifies a hint, the key the reader
       uses, and a row without a query_id is not a hint to compare (it would also fail the GetInt64 read of query_id), as in the plan diff. */
    internal const string HintSnapshotChangesSql = @"
WITH fresh AS (
    SELECT m.t, m.database_name, p.prev_t
    FROM (SELECT collection_time AS t, database_name
          FROM collect.query_store_hints
          WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3 AND database_name IS NOT NULL AND query_hint_id IS NULL
          GROUP BY collection_time, database_name) AS m
    CROSS JOIN LATERAL (SELECT max(q.collection_time) AS prev_t
                        FROM collect.query_store_hints AS q
                        WHERE q.server_id = $1 AND q.database_name = m.database_name AND q.query_hint_id IS NULL
                          AND q.collection_time < m.t AND q.collection_time >= m.t - interval '1 day') AS p
    WHERE p.prev_t IS NOT NULL AND m.t - p.prev_t <= $4 * interval '1 minute'
)
SELECT CASE WHEN before.query_hint_id IS NULL THEN 'set' ELSE 'changed' END AS change_kind, n.t, n.database_name, h.query_id, h.query_hint_id,
       h.query_hint_text, before.query_hint_text AS previous_text
FROM fresh AS n
JOIN collect.query_store_hints AS h
  ON h.server_id = $1 AND h.collection_time = n.t AND h.database_name = n.database_name AND h.query_hint_id IS NOT NULL AND h.query_id IS NOT NULL
LEFT JOIN LATERAL (SELECT p.query_hint_id, p.query_hint_text FROM collect.query_store_hints AS p
                   WHERE p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name
                     AND p.query_id = h.query_id AND p.query_hint_id = h.query_hint_id LIMIT 1) AS before ON true
WHERE before.query_hint_id IS NULL OR before.query_hint_text IS DISTINCT FROM h.query_hint_text
UNION ALL
SELECT 'cleared', n.t, n.database_name, p.query_id, p.query_hint_id, p.query_hint_text, NULL
FROM fresh AS n
JOIN collect.query_store_hints AS p
  ON p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name AND p.query_hint_id IS NOT NULL AND p.query_id IS NOT NULL
LEFT JOIN LATERAL (SELECT 1 AS hit FROM collect.query_store_hints AS h
                   WHERE h.server_id = $1 AND h.collection_time = n.t AND h.database_name = n.database_name
                     AND h.query_id = p.query_id AND h.query_hint_id = p.query_hint_id LIMIT 1) AS after ON true
WHERE after.hit IS NULL";

    /// <summary>#6551: the databases of <paramref name="serverId"/> that are readable-secondary copies, whose plan-snapshot changes are the primary's.
    /// The rule is the shared one every other read of the store uses (<see cref="PgSecondaryReplicaScope"/>): the newest availability-group snapshots at or
    /// before <paramref name="end"/>, both within <see cref="AgReplicaScope.SnapshotFreshness"/>, and nothing is left out when they are stale or unreadable.
    /// A snapshot older than that is not trusted to say this node is still a secondary: when the collectors stopped at or before a failover, a day-old
    /// SECONDARY row would drop this node's real plan changes.</summary>
    internal async Task<IReadOnlySet<string>> ReadReplicaDatabasesAsync(int serverId, DateTime end, CancellationToken cancellationToken)
        => await PgSecondaryReplicaScope.ReadAsync(_readSource, serverId, end, _logger, cancellationToken);

    private readonly NpgsqlDataSource _readSource;
    private readonly ChangeWatchStore _store;
    private readonly ChangeWatchEvaluator _evaluator;
    private readonly InvestigationDispatchCore _dispatch;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly int _commandTimeoutSeconds;

    /* #6234: when each target's last detect was read (the pass clock at its start), by server id. In memory on purpose: a restart detects every target at once, which costs one read per target and cannot miss a change. */
    private readonly ConcurrentDictionary<int, DateTime> _lastDetected = new();

    /* #6234: when each target's last detect failed (the pass clock at its start), by server id. A failed target waits FailedDetectBackoff before it is tried again. */
    private readonly ConcurrentDictionary<int, DateTime> _lastFailed = new();

    /// <param name="readSource">The store's read connection (the collected tables and the registry).</param>
    /// <param name="store">The watch tables.</param>
    /// <param name="dispatch">The shared dispatch core the alert path uses: its route, agent gate and store call open the request.</param>
    public ChangeWatchPass(
        NpgsqlDataSource readSource, ChangeWatchStore store, InvestigationDispatchCore dispatch, ILogger logger, int commandTimeoutSeconds,
        Func<DateTime>? utcNow = null)
    {
        _readSource = readSource ?? throw new ArgumentNullException(nameof(readSource));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _dispatch = dispatch ?? throw new ArgumentNullException(nameof(dispatch));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _commandTimeoutSeconds = commandTimeoutSeconds;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _evaluator = new ChangeWatchEvaluator(readSource, commandTimeoutSeconds);
    }

    /// <summary>The forced-plans collector, whose captures the plan-change diff reads (the frequency it runs at on a server is settable).</summary>
    internal const string ForcedPlansCollector = "query_store_forced_plans";

    /// <summary>
    /// #6234: how far apart two captures of one database may be and still be diffed: the larger of 30 minutes and twice the collector's frequency plus
    /// <see cref="MarkSlack"/>. Twice the frequency lets one missed capture still bridge and the slack covers the capture's own run time; the 30-minute floor is
    /// what the limit was before it followed the frequency, and keeps a capture delayed by load from dropping a real plan change at the 5-minute default (where
    /// twice the frequency plus the slack is only 15 minutes). A longer gap is a return from an outage, whose change cannot be dated.
    /// </summary>
    public static TimeSpan CaptureGapLimit(int frequencyMinutes) => TimeSpan.FromMinutes(Math.Max(MinimumCaptureGap.TotalMinutes, (2L * Math.Max(1, CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(frequencyMinutes))) + MarkSlack.TotalMinutes));

    /// <summary>#6234: the floor of <see cref="CaptureGapLimit"/>: the fixed gap limit before it followed the collector's frequency.</summary>
    public static readonly TimeSpan MinimumCaptureGap = TimeSpan.FromMinutes(30);

    /// <summary>
    /// #6234: the frequency, in minutes, the forced-plans collector runs at on one server, resolved the way the worker resolves it: the server's own
    /// override row, else the fleet-wide row, else the shipped default (<see cref="CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes"/>).
    /// </summary>
    internal Task<int> ForcedPlansFrequencyMinutesAsync(int serverId, CancellationToken cancellationToken) =>
        CollectorFrequencyMinutesAsync(ForcedPlansCollector, serverId, cancellationToken);

    /// <summary>#6581 slice 2: the Query Store hints collector, whose captures the hint-change diff reads (its frequency is settable on its own).</summary>
    internal const string HintsCollector = "query_store_hints";

    /// <summary>The frequency, in minutes, a collector runs at on one server, resolved the way the worker resolves it.</summary>
    internal async Task<int> CollectorFrequencyMinutesAsync(string collector, int serverId, CancellationToken cancellationToken)
    {
        var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(_readSource, serverId, cancellationToken);
        return CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(collector, serverId, overrides)
               ?? CollectorScheduleDefaults.All[collector].FrequencyMinutes;
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private static string Stamp(DateTime time) => Utc(time).ToString("yyyyMMddTHHmmssfff", CultureInfo.InvariantCulture);

    /// <summary>One detect pass and one evaluate pass. A failure on one server is logged and the others go on.</summary>
    public async Task RunOnceAsync(CancellationToken cancellationToken = default)
    {
        /* #6234: a target whose open watch is due is detected first, whatever its hourly gate says. A change is recorded up to DetectInterval
           after it happened, so a second change inside the window may not be recorded yet when the window ends. This read records it and extends
           the watch, as the detect on every tick did. A change after the window is deferred by RecordAsync and opens a new watch below. */
        await DetectAsync(await DueOpenWatchServersAsync(cancellationToken), cancellationToken);

        /* #6239 round 3, M4: a due watch is evaluated BEFORE detection, so a change found after its window ended opens a new watch and never stretches the old one. */
        await EvaluateDueAsync(cancellationToken);
        await DetectAsync(null, cancellationToken);
    }

    /* #6234: the servers with an open watch whose window has ended, with that window's end. Only open ones: a watch already claimed for evaluation can no longer be extended. */
    private async Task<IReadOnlyDictionary<int, DateTime>> DueOpenWatchServersAsync(CancellationToken cancellationToken)
    {
        try
        {
            return (await _store.DueAsync(cancellationToken)).Where(w => w.State == "open")
                .GroupBy(w => w.ServerId).ToDictionary(g => g.Key, g => g.Max(w => w.WindowEndsUtc));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Change watch: the due watches could not be read before evaluation: {Message}", ex.Message);
            return new Dictionary<int, DateTime>();
        }
    }

    /// <summary>
    /// Records the changes of every target (SQL Server and PostgreSQL) that has not been detected for <see cref="DetectInterval"/>, opening or extending its
    /// watch. A target detected less than that long ago is skipped without a read of its marks or its sources. A target whose detect failed, or whose changes
    /// were deferred behind an open watch, keeps its marks. A deferred target is not counted as detected and is read again on the next call (the watch that
    /// held it back is due, so it is evaluated first). A failed target is tried again once <see cref="FailedDetectBackoff"/> has passed.
    /// </summary>
    public Task DetectAsync(CancellationToken cancellationToken = default) => DetectAsync(null, cancellationToken);

    /// <summary>
    /// <see cref="DetectAsync(CancellationToken)"/> for every target, or, when <paramref name="only"/> is set, only for those targets and without the
    /// <see cref="DetectInterval"/> gate (#6234: a target whose open watch is due, by the end of that watch's window). Such a target is skipped when its last
    /// detect was at or after that end, as every change up to the end has then been read, so the read happens once per due watch. An empty set reads nothing.
    /// </summary>
    private async Task DetectAsync(IReadOnlyDictionary<int, DateTime>? only, CancellationToken cancellationToken)
    {
        if (only is { Count: 0 })
        {
            return;
        }

        var servers = new List<(int Id, string Name, bool Postgres)>();
        await using (var command = _readSource.CreateCommand(ServersSql))
        {
            command.CommandTimeout = _commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0), reader.GetString(1), MonitoredEngineKind.IsPostgres(reader.IsDBNull(2) ? null : reader.GetString(2))));
            }
        }

        var end = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        var floor = end - DetectLookback;
        var enabled = servers.Select(s => s.Id).ToHashSet();
        foreach (var known in _lastDetected.Keys.Where(k => !enabled.Contains(k)).ToList())
        {
            _lastDetected.TryRemove(known, out _);
        }

        foreach (var known in _lastFailed.Keys.Where(k => !enabled.Contains(k)).ToList())
        {
            _lastFailed.TryRemove(known, out _);
        }

        foreach (var (id, name, postgres) in servers)
        {
            var hasLast = _lastDetected.TryGetValue(id, out var last);
            if (_lastFailed.TryGetValue(id, out var failed) && end - failed < FailedDetectBackoff)
            {
                continue;
            }

            if (only is not null)
            {
                if (!only.TryGetValue(id, out var windowEnds) || (hasLast && last >= windowEnds))
                {
                    continue;
                }
            }
            else if (hasLast && end - last < DetectInterval)
            {
                continue;
            }

            try
            {
                var marks = await _store.GetMarksAsync(id, cancellationToken);
                var sources = postgres ? ChangeWatchStore.PostgresMarkSources : ChangeWatchStore.MarkSources;
                var unmarked = sources.Where(s => !marks.ContainsKey(s)).ToDictionary(s => s, _ => end, StringComparer.Ordinal);
                if (unmarked.Count > 0)
                {
                    /* The first pass on a store that has history for this server: its marks start at now, and nothing already stored is a change. */
                    await _store.SetMarksAsync(id, unmarked, cancellationToken);
                    foreach (var (source, mark) in unmarked)
                    {
                        marks[source] = mark;
                    }
                }

                /* A restart after a long stop still looks no further back than the lookback: a watch is an hour, not a catch-up. */
                var oldest = marks.Where(m => sources.Contains(m.Key)).Select(m => m.Value).DefaultIfEmpty(end).Min();
                if (oldest < floor)
                {
                    /* #6239 round 3, L2: say what the lookback skips. */
                    _logger.LogWarning(
                        "Change detection for {Server} had no pass from {From:u} to {To:u}; changes in that span are not read (the lookback is {Minutes} minutes)",
                        name, oldest, floor, (int)DetectLookback.TotalMinutes);
                }

                var bounds = marks.ToDictionary(m => m.Key, m => m.Value > floor ? m.Value : floor, StringComparer.Ordinal);
                var changes = postgres
                    ? await ReadPostgresChangesAsync(id, bounds, end, cancellationToken)
                    : await ReadChangesAsync(id, bounds, end, cancellationToken);
                var deferred = false;
                if (changes.Count > 0)
                {
                    var recorded = await _store.RecordAsync(id, name, changes, cancellationToken);
                    deferred = recorded.Deferred;
                    if (recorded.WatchId is { } watchId)
                    {
                        _logger.LogInformation("Change watch {WatchId} on {Server} now names {Count} change(s) seen in the last hour", watchId, name, changes.Count);
                    }
                }

                if (deferred)
                {
                    /* A change after the open watch's window waits for that watch to be evaluated: the marks stay, and a later pass reads it again. */
                    _logger.LogDebug("A change on {Server} is after its open watch's window; it is recorded once that watch has been evaluated", name);
                    /* #6234: a deferred change is after a window end and at or before now, so the watch holding it back is already due and is evaluated
                       on this run or the next. The target is not counted as detected, so the next detect reads the change again at once and opens its watch. */
                    _lastDetected.TryRemove(id, out _);
                    continue;
                }

                /* Only after the changes are recorded: a failed read or record leaves the marks, and the next pass reads the same rows again. */
                await _store.SetMarksAsync(id, sources.ToDictionary(s => s, _ => end - MarkSlack, StringComparer.Ordinal), cancellationToken);
                _lastDetected[id] = end;
                _lastFailed.TryRemove(id, out _);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _lastFailed[id] = end;
                _logger.LogWarning("Change detection failed for {Server}; it is tried again in {Minutes} minutes: {Message}", name, (int)FailedDetectBackoff.TotalMinutes, ex.Message);
            }
        }
    }

    /// <summary>Every source after the same time. For a caller (a test) that has one lower bound.</summary>
    internal Task<List<ChangeWatchChange>> ReadChangesAsync(int serverId, DateTime after, DateTime end, CancellationToken cancellationToken) =>
        ReadChangesAsync(serverId, ChangeWatchStore.MarkSources.ToDictionary(s => s, _ => after, StringComparer.Ordinal), end, cancellationToken);

    /// <summary>
    /// The changes of one server NEWER than each source's mark (strictly: a change at the mark was already seen) and no later than
    /// <paramref name="end"/>. A snapshot diff reads from the capture at or before the mark, so the first capture after it has
    /// something to differ from, and nothing older is read.
    /// </summary>
    internal async Task<List<ChangeWatchChange>> ReadChangesAsync(
        int serverId, IReadOnlyDictionary<string, DateTime> marks, DateTime end, CancellationToken cancellationToken)
    {
        var changes = new List<ChangeWatchChange>();
        var naiveEnd = Naive(end);
        DateTime Mark(string source) => Naive(marks[source]);

        var serverMark = Mark("server_config");
        foreach (var c in ConfigChangeDiff.DiffServerConfigChanges(
                     await DarlingConfigHistoryReader.GetServerConfigSnapshotsAsync(_readSource, serverId, serverMark, cancellationToken), serverMark, naiveEnd)
                     .Where(c => c.ChangeTime > serverMark))
        {
            changes.Add(new ChangeWatchChange($"server_config:{c.ConfigurationName}:{Stamp(c.ChangeTime)}", "server_config", Utc(c.ChangeTime),
                $"server setting {c.ConfigurationName}: {c.ChangeDescription}"));
        }

        var databaseMark = Mark("database_config");
        foreach (var c in ConfigChangeDiff.DiffDatabaseConfigChanges(
                     await DarlingConfigHistoryReader.GetDatabaseConfigSnapshotsAsync(_readSource, serverId, DatabaseFilter.All, databaseMark, cancellationToken),
                     databaseMark, naiveEnd).Where(c => c.ChangeTime > databaseMark))
        {
            /* #6566: log_reuse_wait_desc is the engine's live reason the log cannot be truncated, a status that flips with no operator action. The analysis
               leaves it out (ConfigChangeAttribution.DatabaseStatusColumns); a watch counted it as a configuration change and opened requests on it. */
            if (!ConfigChangeAttribution.IsAttributableDatabaseSetting(c.SettingName))
            {
                continue;
            }


            changes.Add(new ChangeWatchChange($"database_config:{c.DatabaseName}:{c.SettingName}:{Stamp(c.ChangeTime)}", "database_config", Utc(c.ChangeTime),
                $"database {c.DatabaseName} {c.SettingName}: {c.ChangeDescription}", c.DatabaseName));
        }

        /* A trace_flags run that finds no flag stores no rows, so the last flag being turned off is in the run record only. */
        var flagMark = Mark("trace_flags");
        var flagSnapshots = await DarlingConfigHistoryReader.GetTraceFlagSnapshotsAsync(_readSource, serverId, flagMark, cancellationToken);
        /* #6239 round 3, M1: the empty runs are read from the instant the snapshot read starts (its baseline capture), not from the mark, so an empty run
           between the baseline and the mark reaches the diff: a flag turned off and back on is seen, and a flag that was off is not reported off again. */
        var flagFloor = flagSnapshots.Count > 0 && flagSnapshots.Min(x => x.CaptureTime) < flagMark ? flagSnapshots.Min(x => x.CaptureTime) : flagMark;
        var emptyFlagRuns = await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(_readSource, serverId, flagFloor, naiveEnd, cancellationToken);
        foreach (var c in ConfigChangeDiff.DiffTraceFlagChanges(flagSnapshots, emptyFlagRuns, flagMark, naiveEnd).Where(c => c.ChangeTime > flagMark))
        {
            changes.Add(new ChangeWatchChange($"trace_flag:{c.TraceFlag}:{Stamp(c.ChangeTime)}", "trace_flag", Utc(c.ChangeTime), c.ChangeDescription));
        }

        await using (var command = Bind(_readSource.CreateCommand(ScopedConfigChangesSql), serverId, Mark("database_scoped_config"), naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var time = reader.GetDateTime(0);
                var database = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var setting = reader.IsDBNull(2) ? "" : reader.GetString(2);
                changes.Add(new ChangeWatchChange($"database_scoped_config:{database}:{setting}:{Stamp(time)}", "database_scoped_config", Utc(time),
                    $"database-scoped setting {setting} in {database}: changed from {(reader.IsDBNull(4) ? "(none)" : reader.GetString(4))} to {(reader.IsDBNull(3) ? "(none)" : reader.GetString(3))}",
                    database));
            }
        }

        await using (var command = Bind(_readSource.CreateCommand(PlanActionsSql), serverId, Mark("plan_actions"), naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.GetString(2);
                changes.Add(new ChangeWatchChange($"plan_force:action:{reader.GetInt64(0)}", "plan_force", Utc(reader.GetDateTime(1)),
                    $"plan {reader.GetInt64(4)} of query {reader.GetInt64(3)} in {database}: {reader.GetString(5)} by the plan-force ledger{ChangeWatchPlanForcing.Suffix(ChangeWatchPlanForcing.Manual)}", database, reader.GetInt64(3), reader.GetInt64(4)));
            }
        }

        /* #6551: a plan change in a database this server holds as a readable secondary was made on the primary, and the primary's watch records it once.
           The plan-force ledger (above) is this server's own action and is kept. */
        var replicaDatabases = await ReadReplicaDatabasesAsync(serverId, end, cancellationToken);
        var gapLimit = CaptureGapLimit(await ForcedPlansFrequencyMinutesAsync(serverId, cancellationToken));
        await using (var command = WithGapLimit(Bind(_readSource.CreateCommand(PlanSnapshotChangesSql), serverId, Mark("plan_snapshots"), naiveEnd), gapLimit))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var forced = string.Equals(reader.GetString(0), "forced", StringComparison.Ordinal);
                var database = reader.GetString(2);
                if (replicaDatabases.Contains(database))
                {
                    continue;
                }

                var time = reader.GetDateTime(1);
                var queryId = reader.GetInt64(3);
                var planId = reader.GetInt64(4);
                /* #6550: the forcing type of the capture the plan was seen in (the previous one for an unforce), so the change says whether the server forced it itself. */
                var forcingType = ChangeWatchPlanForcing.Suffix(reader.IsDBNull(5) ? null : reader.GetString(5));
                changes.Add(new ChangeWatchChange(
                    $"plan_force:{(forced ? "snapshot" : "unforced")}:{database}:{queryId}:{planId}:{Stamp(time)}", "plan_force", Utc(time),
                    $"plan {planId} of query {queryId} in {database}: {(forced ? "forced" : "unforced (in the database's previous forced-plan capture, not in this one)")}{forcingType}",
                    database, queryId, planId));
            }
        }

        /* #6581 slice 2: a Query Store hint set, changed or cleared. The same secondary rule as the plan snapshots: a readable secondary's hints are the primary's.
           A server whose hints collector does not run (SQL Server before 2022) writes no marker row, so nothing is diffed and no hint change is recorded for it. */
        var hintGapLimit = CaptureGapLimit(await CollectorFrequencyMinutesAsync(HintsCollector, serverId, cancellationToken));
        await using (var command = WithGapLimit(Bind(_readSource.CreateCommand(HintSnapshotChangesSql), serverId, Mark("hint_snapshots"), naiveEnd), hintGapLimit))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.GetString(2);
                if (replicaDatabases.Contains(database))
                {
                    continue;
                }

                var what = reader.GetString(0);
                var time = reader.GetDateTime(1);
                var queryId = reader.GetInt64(3);
                var hintId = reader.GetInt64(4);
                var text = reader.IsDBNull(5) ? "(no text)" : reader.GetString(5);
                var description = what switch
                {
                    "set" => $"Query Store hint {hintId} set on query {queryId} in {database}: {text}",
                    "changed" => $"Query Store hint {hintId} on query {queryId} in {database} changed from {(reader.IsDBNull(6) ? "(no text)" : reader.GetString(6))} to {text}",
                    _ => $"Query Store hint {hintId} cleared from query {queryId} in {database} (it was: {text})",
                };
                changes.Add(new ChangeWatchChange($"query_store_hint:{what}:{database}:{queryId}:{hintId}:{Stamp(time)}", "query_store_hint", Utc(time),
                    description, database, queryId));
            }
        }

        return changes;
    }

    /// <summary>Adds PlanSnapshotChangesSql's fourth parameter: the longest gap, in whole minutes, between two captures of a database that is still diffed.</summary>
    internal static NpgsqlCommand WithGapLimit(NpgsqlCommand command, TimeSpan gapLimit)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (int)gapLimit.TotalMinutes });
        return command;
    }

    private NpgsqlCommand Bind(NpgsqlCommand command, int serverId, DateTime start, DateTime end)
    {
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
        return command;
    }

    /// <summary>
    /// Evaluates every open watch whose window has ended. Each is CLAIMED first (one UPDATE from open to evaluating), so one pass or instance
    /// evaluates it and opens its one request; a claim that went stale (the claimer died) is taken over, and a request already opened for the watch
    /// is found by its key and not opened again. A watch whose evaluation, or whose request open, fails stays claimed and is tried again once the claim is stale.
    /// </summary>
    public async Task EvaluateDueAsync(CancellationToken cancellationToken = default)
    {
        foreach (var watch in await _store.DueAsync(cancellationToken))
        {
            try
            {
                if (await _store.ClaimAsync(watch.WatchId, cancellationToken) is not { } attempt)
                {
                    continue;
                }

                if (attempt > ChangeWatchStore.MaxAttempts)
                {
                    /* #6239 round 3, M2: claimed over and over without one ending in a failure being recorded (the claimer kept dying): stop. */
                    await _store.FailAttemptAsync(watch.WatchId, $"the watch was claimed {attempt} times without finishing", cancellationToken);
                    continue;
                }

                if (await _store.FindRequestAsync(watch.WatchId, cancellationToken) is { } existing)
                {
                    /* A claimer opened the request and died before it closed the watch. The windows are fixed, so the evaluation is read again and
                       stored with the result: the takeover must not leave a request_opened watch with no measures (#6239 round 3, L7). */
                    var taken = await _evaluator.EvaluateAsync(watch, cancellationToken);
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultRequestOpened, existing, taken.ResultJson, cancellationToken);
                    continue;
                }

                var evaluation = await _evaluator.EvaluateAsync(watch, cancellationToken);
                if (!evaluation.HadData)
                {
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultNoData, null, evaluation.ResultJson, cancellationToken);
                    continue;
                }

                if (!evaluation.Moved)
                {
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultFlat, null, evaluation.ResultJson, cancellationToken);
                    _logger.LogDebug("Change watch {WatchId} on {Server} closed: nothing moved", watch.WatchId, watch.ServerName);
                    continue;
                }

                if (await _store.IsAgentOnlyAsync(watch.WatchId, CaptureGapLimit(await ForcedPlansFrequencyMinutesAsync(watch.ServerId, cancellationToken)), cancellationToken))
                {
                    /* #6239 round 3, L9: an agent's own plan change under a change-watch request is recorded and measured, and opens no further request. */
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultAgentChange, null, evaluation.ResultJson, cancellationToken);
                    continue;
                }

                if (watch.Changes.Count > 0 && watch.Changes.All(c => string.Equals(c.PlanForcingType, ChangeWatchPlanForcing.Auto, StringComparison.Ordinal)))
                {
                    /* #6550: automatic plan correction forcing and reverting plans is SQL Server doing its job, and the regression that set it off has its own
                       alerts. The watch is recorded and measured, and opens no request; a watch that mixes AUTO with any other change still does. */
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultAutoPlanCorrection, null, evaluation.ResultJson, cancellationToken);
                    continue;
                }

                var moved = 0;
                foreach (var m in evaluation.Measures)
                {
                    moved += m.Moved ? 1 : 0;
                }

                var facts = new InvestigationAlertFacts(moved, 1, false, evaluation.DetailText, evaluation.ResultJson);
                Exception? failure = null;
                var opened = await _dispatch.OpenChangeWatchAsync(
                    watch.ServerId.ToString(CultureInfo.InvariantCulture), watch.ServerName, watch.WindowEndsUtc, facts, cancellationToken,
                    ChangeWatchStore.RequestKey(watch.WatchId), ex => failure = ex);
                if (opened is null && failure is not null)
                {
                    /* #6239 round 3, L6: a failed open is not "no route". The watch stays claimed; the request key makes the retry safe. */
                    _logger.LogWarning("Change watch {WatchId} on {Server}: the request could not be opened; it is tried again: {Message}",
                        watch.WatchId, watch.ServerName, failure.Message);
                    await RecordFailureAsync(watch, failure.Message, cancellationToken);
                    continue;
                }

                var requestId = opened?.RequestId;
                await _store.CloseAsync(
                    watch.WatchId, requestId is null ? ChangeWatchStore.ResultMovedNoRequest : ChangeWatchStore.ResultRequestOpened, requestId, evaluation.ResultJson, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Change watch {WatchId} on {Server} could not be evaluated; it is tried again when its claim goes stale: {Message}",
                    watch.WatchId, watch.ServerName, ex.Message);
                await RecordFailureAsync(watch, ex.Message, cancellationToken);
            }
        }
    }

    /// <summary>#6239 round 3, M2: counts a failed attempt against the watch; at <see cref="ChangeWatchStore.MaxAttempts"/> it closes as <c>failed</c> with the error.</summary>
    private async Task RecordFailureAsync(ChangeWatchRow watch, string message, CancellationToken cancellationToken)
    {
        try
        {
            if (await _store.FailAttemptAsync(watch.WatchId, message, cancellationToken))
            {
                _logger.LogWarning("Change watch {WatchId} on {Server} failed {Attempts} times and is closed as failed: {Message}",
                    watch.WatchId, watch.ServerName, ChangeWatchStore.MaxAttempts, message);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Change watch {WatchId} on {Server}: the failure could not be recorded: {Message}", watch.WatchId, watch.ServerName, ex.Message);
        }
    }
}
