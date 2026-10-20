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
    WHERE server_id = $1 AND {PerformanceMonitor.Darling.Service.Mcp.DarlingConfigHistoryReader.BaselineBound("v_database_scoped_config", "$1", "$2")}
    WINDOW w AS (PARTITION BY database_name, configuration_name ORDER BY capture_time)
) AS x
WHERE rn > 1 AND capture_time > $2 AND capture_time <= $3
  AND (value IS DISTINCT FROM prev_value OR value_for_secondary IS DISTINCT FROM prev_secondary)";

    private const string PlanActionsSql = @"
SELECT action_id, action_time, database_name, query_id, plan_id, action
FROM collect.plan_force_actions
WHERE server_id = $1 AND outcome = 'succeeded' AND action_time > $2 AND action_time <= $3";

    /* A person unforced the LAST forced plan: an empty forced-plan snapshot is not stored, so the rows never show it. The collector's own
       run record does: a SUCCESS run that stored zero rows (CollectorRunFacts.RanFoundNothing's evidence) after a snapshot that held plans
       means every plan in that snapshot is no longer forced. Only the FIRST empty run after a snapshot counts (a second one has nothing
       left to unforce), and a plan the ledger unforced in the half hour before is the ledger's change, not a person's. */
    private const string PlanUnforceAllSql = @"
SELECT r.collection_time, f.database_name, f.query_id, f.plan_id
FROM v_collection_log AS r
CROSS JOIN LATERAL (SELECT max(p.collection_time) AS t FROM collect.query_store_forced_plans AS p
                    WHERE p.server_id = r.server_id AND p.collection_time < r.collection_time) AS s
JOIN collect.query_store_forced_plans AS f ON f.server_id = r.server_id AND f.collection_time = s.t
WHERE r.server_id = $1 AND r.collector_name = 'query_store_forced_plans' AND r.status = 'SUCCESS' AND r.rows_collected = 0
  AND r.collection_time > $2 AND r.collection_time <= $3
  AND NOT EXISTS (SELECT 1 FROM v_collection_log AS r2
                  WHERE r2.server_id = r.server_id AND r2.collector_name = r.collector_name AND r2.status = 'SUCCESS' AND r2.rows_collected = 0
                    AND r2.collection_time > s.t AND r2.collection_time < r.collection_time)
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = f.server_id AND a.database_name IS NOT DISTINCT FROM f.database_name
                    AND a.query_id = f.query_id AND a.plan_id = f.plan_id AND a.outcome = 'succeeded'
                    AND a.action_time >= r.collection_time - interval '30 minutes' AND a.action_time <= r.collection_time)";

    /* A plan that appears in a forced-plan snapshot after earlier snapshots that did not hold it: forced by a person (the ledger's own
       forcing is read above). The server's first-ever snapshot is the baseline, never a change, and a snapshot a ledger row explains is skipped. */
    private const string PlanSnapshotChangesSql = @"
SELECT f.collection_time, f.database_name, f.query_id, f.plan_id
FROM collect.query_store_forced_plans AS f
WHERE f.server_id = $1 AND f.collection_time > $2 AND f.collection_time <= $3
  AND f.collection_time > (SELECT min(b.collection_time) FROM collect.query_store_forced_plans AS b WHERE b.server_id = f.server_id)
  AND NOT EXISTS (SELECT 1 FROM collect.query_store_forced_plans AS p
                  WHERE p.server_id = f.server_id AND p.database_name IS NOT DISTINCT FROM f.database_name
                    AND p.query_id = f.query_id AND p.plan_id = f.plan_id
                    AND p.collection_time < f.collection_time AND p.collection_time >= f.collection_time - interval '1 day')
  AND NOT EXISTS (SELECT 1 FROM collect.plan_force_actions AS a
                  WHERE a.server_id = f.server_id AND a.database_name IS NOT DISTINCT FROM f.database_name
                    AND a.query_id = f.query_id AND a.plan_id = f.plan_id AND a.outcome = 'succeeded'
                    AND a.action_time >= f.collection_time - interval '30 minutes' AND a.action_time <= f.collection_time)";

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
        await DetectAsync(cancellationToken);
        await EvaluateDueAsync(cancellationToken);
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
                var bounds = marks.ToDictionary(m => m.Key, m => m.Value > floor ? m.Value : floor, StringComparer.Ordinal);
                var changes = postgres
                    ? await ReadPostgresChangesAsync(id, bounds, end, cancellationToken)
                    : await ReadChangesAsync(id, bounds, end, cancellationToken);
                if (changes.Count > 0 && await _store.RecordChangesAsync(id, name, changes, cancellationToken) is { } watchId)
                {
                    _logger.LogInformation("Change watch {WatchId} on {Server} now names {Count} change(s) seen in the last hour", watchId, name, changes.Count);
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
        var emptyFlagRuns = await DarlingCollectorRunFactsReader.ReadEmptyTraceFlagRunsAsync(_readSource, serverId, flagMark, naiveEnd, cancellationToken);
        foreach (var c in ConfigChangeDiff.DiffTraceFlagChanges(
                     await DarlingConfigHistoryReader.GetTraceFlagSnapshotsAsync(_readSource, serverId, flagMark, cancellationToken),
                     emptyFlagRuns, flagMark, naiveEnd).Where(c => c.ChangeTime > flagMark))
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
                var database = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var time = reader.GetDateTime(0);
                changes.Add(new ChangeWatchChange($"plan_force:snapshot:{database}:{reader.GetInt64(2)}:{reader.GetInt64(3)}:{Stamp(time)}", "plan_force", Utc(time),
                    $"plan {reader.GetInt64(3)} of query {reader.GetInt64(2)} in {database}: forced", database, reader.GetInt64(2)));
            }
        }

        await using (var command = Bind(_readSource.CreateCommand(PlanUnforceAllSql), serverId, Mark("plan_snapshots"), naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var time = reader.GetDateTime(0);
                changes.Add(new ChangeWatchChange($"plan_force:unforced:{database}:{reader.GetInt64(2)}:{reader.GetInt64(3)}:{Stamp(time)}", "plan_force", Utc(time),
                    $"plan {reader.GetInt64(3)} of query {reader.GetInt64(2)} in {database}: unforced (the forced-plan collector ran and stored no forced plan)", database, reader.GetInt64(2)));
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

    /// <summary>Evaluates every open watch whose window has ended. A watch whose evaluation fails stays open and is tried again next pass.</summary>
    public async Task EvaluateDueAsync(CancellationToken cancellationToken = default)
    {
        foreach (var watch in await _store.DueAsync(cancellationToken))
        {
            try
            {
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

                var moved = 0;
                foreach (var m in evaluation.Measures)
                {
                    moved += m.Moved ? 1 : 0;
                }

                var facts = new InvestigationAlertFacts(moved, 1, false, evaluation.DetailText, evaluation.ResultJson);
                var opened = await _dispatch.OpenChangeWatchAsync(
                    watch.ServerId.ToString(CultureInfo.InvariantCulture), watch.ServerName, watch.WindowEndsUtc, facts, cancellationToken);
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
                _logger.LogWarning("Change watch {WatchId} on {Server} could not be evaluated; it is tried again next pass: {Message}",
                    watch.WatchId, watch.ServerName, ex.Message);
            }
        }
    }
}
