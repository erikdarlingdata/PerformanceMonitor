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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.ChangeWatch;

/// <summary>
/// #6234: the change watch's worker pass, run on the investigation sweep's tick. It does two things for SQL Server targets.
/// <para><b>Detect.</b> It reads the recorded changes of the last hour (the same snapshot readers and diff the
/// <c>get_server_config_changes</c>, <c>get_database_config_changes</c> and <c>get_trace_flag_changes</c> tools use; the
/// database-scoped configuration snapshots; plan forcing by an agent, from the plan-force ledger, and by a person, from the forced-plan
/// snapshots) and records them: each change opens a watch for its server or extends the open one.</para>
/// <para><b>Evaluate.</b> Every open watch whose window has ended (including one that ended while the service was down: the pass
/// runs at start) is compared with the hour before and the same hour a week earlier. When a measure moved, ONE request opens through
/// the dispatcher's own gate and route (<c>change_watch</c> trigger kind); otherwise the watch closes quietly.</para>
/// </summary>
public sealed partial class ChangeWatchPass
{
    /// <summary>How far back a pass looks for a change. A change is recorded once under its key, so a longer look only costs reads.</summary>
    public static readonly TimeSpan DetectLookback = TimeSpan.FromMinutes(70);

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
       capture in a run (failover, Query Store off, the collector failed for it) has nothing to compare, so it is never read as an unforce. The previous
       capture is looked for a day back. A change the plan-force ledger explains (a succeeded action on the plan within 30 minutes before the capture) is skipped. */
    private const string PlanSnapshotChangesSql = @"
WITH caps AS (
    SELECT t, database_name, LAG(t) OVER (PARTITION BY database_name ORDER BY t) AS prev_t
    FROM (SELECT DISTINCT collection_time AS t, database_name
          FROM collect.query_store_forced_plans
          WHERE server_id = $1 AND collection_time > $2 - interval '1 day' AND collection_time <= $3 AND database_name IS NOT NULL) AS d
), fresh AS (
    SELECT t, database_name, prev_t FROM caps WHERE t > $2 AND prev_t IS NOT NULL
)
SELECT 'forced' AS change_kind, n.t, n.database_name, f.query_id, f.plan_id
FROM fresh AS n
JOIN collect.query_store_forced_plans AS f
  ON f.server_id = $1 AND f.collection_time = n.t AND f.database_name = n.database_name AND f.plan_id IS NOT NULL AND f.query_id IS NOT NULL
WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_forced_plans AS p
                  WHERE p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name
                    AND p.query_id = f.query_id AND p.plan_id = f.plan_id)
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = $1 AND a.database_name = f.database_name AND a.query_id = f.query_id AND a.plan_id = f.plan_id
                    AND a.outcome = 'succeeded' AND a.action_time >= n.t - interval '30 minutes' AND a.action_time <= n.t)
UNION ALL
SELECT 'unforced', n.t, n.database_name, p.query_id, p.plan_id
FROM fresh AS n
JOIN collect.query_store_forced_plans AS p
  ON p.server_id = $1 AND p.collection_time = n.prev_t AND p.database_name = n.database_name AND p.plan_id IS NOT NULL AND p.query_id IS NOT NULL
WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_forced_plans AS f
                  WHERE f.server_id = $1 AND f.collection_time = n.t AND f.database_name = n.database_name
                    AND f.query_id = p.query_id AND f.plan_id = p.plan_id)
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = $1 AND a.database_name = p.database_name AND a.query_id = p.query_id AND a.plan_id = p.plan_id
                    AND a.outcome = 'succeeded' AND a.action_time >= n.t - interval '30 minutes' AND a.action_time <= n.t)";

    private readonly NpgsqlDataSource _readSource;
    private readonly ChangeWatchStore _store;
    private readonly ChangeWatchEvaluator _evaluator;
    private readonly InvestigationDispatchCore _dispatch;
    private readonly ILogger _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly int _commandTimeoutSeconds;

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

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private static string Stamp(DateTime time) => Utc(time).ToString("yyyyMMddTHHmmssfff", CultureInfo.InvariantCulture);

    /// <summary>One detect pass and one evaluate pass. A failure on one server is logged and the others go on.</summary>
    public async Task RunOnceAsync(CancellationToken cancellationToken = default)
    {
        /* #6239 round 3, M4: a due watch is evaluated BEFORE detection, so a change found after its window ended opens a new watch and never stretches the old one. */
        await EvaluateDueAsync(cancellationToken);
        await DetectAsync(cancellationToken);
    }

    /// <summary>Records the last hour's changes of every target (SQL Server and PostgreSQL), opening or extending its watch.</summary>
    public async Task DetectAsync(CancellationToken cancellationToken = default)
    {
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
        foreach (var (id, name, postgres) in servers)
        {
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
                    /* A change after the open watch's window waits for that watch to be evaluated: the marks stay, and the next pass reads it again. */
                    _logger.LogDebug("A change on {Server} is after its open watch's window; it is recorded once that watch has been evaluated", name);
                    continue;
                }

                /* Only after the changes are recorded: a failed read or record leaves the marks, and the next pass reads the same rows again. */
                await _store.SetMarksAsync(id, sources.ToDictionary(s => s, _ => end - MarkSlack, StringComparer.Ordinal), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Change detection failed for {Server}; it is tried again next pass: {Message}", name, ex.Message);
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
                    $"plan {reader.GetInt64(4)} of query {reader.GetInt64(3)} in {database}: {reader.GetString(5)} by the plan-force ledger", database, reader.GetInt64(3)));
            }
        }

        await using (var command = Bind(_readSource.CreateCommand(PlanSnapshotChangesSql), serverId, Mark("plan_snapshots"), naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var forced = string.Equals(reader.GetString(0), "forced", StringComparison.Ordinal);
                var database = reader.GetString(2);
                var time = reader.GetDateTime(1);
                var queryId = reader.GetInt64(3);
                var planId = reader.GetInt64(4);
                changes.Add(new ChangeWatchChange(
                    $"plan_force:{(forced ? "snapshot" : "unforced")}:{database}:{queryId}:{planId}:{Stamp(time)}", "plan_force", Utc(time),
                    $"plan {planId} of query {queryId} in {database}: {(forced ? "forced" : "unforced (in the database's previous forced-plan capture, not in this one)")}",
                    database, queryId));
            }
        }

        return changes;
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
                if (!await _store.ClaimAsync(watch.WatchId, cancellationToken))
                {
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

                if (await _store.IsAgentOnlyAsync(watch.WatchId, cancellationToken))
                {
                    /* #6239 round 3, L9: an agent's own plan change under a change-watch request is recorded and measured, and opens no further request. */
                    await _store.CloseAsync(watch.WatchId, ChangeWatchStore.ResultAgentChange, null, evaluation.ResultJson, cancellationToken);
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
            }
        }
    }
}
