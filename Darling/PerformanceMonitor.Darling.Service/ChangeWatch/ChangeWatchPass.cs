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
public sealed class ChangeWatchPass
{
    /// <summary>How far back a pass looks for a change. A change is recorded once under its key, so a longer look only costs reads.</summary>
    public static readonly TimeSpan DetectLookback = TimeSpan.FromMinutes(70);

    private const string ServersSql = "SELECT s.server_id, s.server_name FROM servers AS s WHERE s.is_enabled AND " + DarlingFleetReader.SqlServerCollectedTargetSql;

    private const string ScopedConfigChangesSql = @"
SELECT capture_time, database_name, configuration_name, value, prev_value, value_for_secondary, prev_secondary
FROM (
    SELECT capture_time, database_name, configuration_name, value, value_for_secondary,
           LAG(value) OVER w AS prev_value, LAG(value_for_secondary) OVER w AS prev_secondary, row_number() OVER w AS rn
    FROM v_database_scoped_config
    WHERE server_id = $1 AND capture_time >= $2 - interval '2 days'
    WINDOW w AS (PARTITION BY database_name, configuration_name ORDER BY capture_time)
) AS x
WHERE rn > 1 AND capture_time >= $2 AND capture_time <= $3
  AND (value IS DISTINCT FROM prev_value OR value_for_secondary IS DISTINCT FROM prev_secondary)";

    private const string PlanActionsSql = @"
SELECT action_id, action_time, database_name, query_id, plan_id, action
FROM collect.plan_force_actions
WHERE server_id = $1 AND outcome = 'succeeded' AND action_time >= $2 AND action_time <= $3";

    /* A plan that appears in a forced-plan snapshot after earlier snapshots that did not hold it: forced by a person (the ledger's own
       forcing is read above). The server's first-ever snapshot is the baseline, never a change, and a snapshot a ledger row explains is skipped. */
    private const string PlanSnapshotChangesSql = @"
SELECT f.collection_time, f.database_name, f.query_id, f.plan_id
FROM collect.query_store_forced_plans AS f
WHERE f.server_id = $1 AND f.collection_time >= $2 AND f.collection_time <= $3
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

    /// <summary>Records the last hour's changes of every SQL Server target, opening or extending its watch.</summary>
    public async Task DetectAsync(CancellationToken cancellationToken = default)
    {
        var servers = new List<(int Id, string Name)>();
        await using (var command = _readSource.CreateCommand(ServersSql))
        {
            command.CommandTimeout = _commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0), reader.GetString(1)));
            }
        }

        var end = DateTime.SpecifyKind(_utcNow(), DateTimeKind.Utc);
        var start = end - DetectLookback;
        foreach (var (id, name) in servers)
        {
            try
            {
                var changes = await ReadChangesAsync(id, start, end, cancellationToken);
                if (changes.Count > 0 && await _store.RecordChangesAsync(id, name, changes, cancellationToken) is { } watchId)
                {
                    _logger.LogInformation("Change watch {WatchId} on {Server} now names {Count} change(s) seen in the last hour", watchId, name, changes.Count);
                }
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

    internal async Task<List<ChangeWatchChange>> ReadChangesAsync(int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var changes = new List<ChangeWatchChange>();
        var naiveStart = Naive(start);
        var naiveEnd = Naive(end);

        foreach (var c in ConfigChangeDiff.DiffServerConfigChanges(
                     await DarlingConfigHistoryReader.GetServerConfigSnapshotsAsync(_readSource, serverId, cancellationToken), naiveStart, naiveEnd))
        {
            changes.Add(new ChangeWatchChange($"server_config:{c.ConfigurationName}:{Stamp(c.ChangeTime)}", "server_config", Utc(c.ChangeTime),
                $"server setting {c.ConfigurationName}: {c.ChangeDescription}"));
        }

        foreach (var c in ConfigChangeDiff.DiffDatabaseConfigChanges(
                     await DarlingConfigHistoryReader.GetDatabaseConfigSnapshotsAsync(_readSource, serverId, cancellationToken), naiveStart, naiveEnd))
        {
            changes.Add(new ChangeWatchChange($"database_config:{c.DatabaseName}:{c.SettingName}:{Stamp(c.ChangeTime)}", "database_config", Utc(c.ChangeTime),
                $"database {c.DatabaseName} {c.SettingName}: {c.ChangeDescription}", c.DatabaseName));
        }

        foreach (var c in ConfigChangeDiff.DiffTraceFlagChanges(
                     await DarlingConfigHistoryReader.GetTraceFlagSnapshotsAsync(_readSource, serverId, cancellationToken), naiveStart, naiveEnd))
        {
            changes.Add(new ChangeWatchChange($"trace_flag:{c.TraceFlag}:{Stamp(c.ChangeTime)}", "trace_flag", Utc(c.ChangeTime), c.ChangeDescription));
        }

        await using (var command = Bind(_readSource.CreateCommand(ScopedConfigChangesSql), serverId, naiveStart, naiveEnd))
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

        await using (var command = Bind(_readSource.CreateCommand(PlanActionsSql), serverId, naiveStart, naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.GetString(2);
                changes.Add(new ChangeWatchChange($"plan_force:action:{reader.GetInt64(0)}", "plan_force", Utc(reader.GetDateTime(1)),
                    $"plan {reader.GetInt64(4)} of query {reader.GetInt64(3)} in {database}: {reader.GetString(5)} by the plan-force ledger", database));
            }
        }

        await using (var command = Bind(_readSource.CreateCommand(PlanSnapshotChangesSql), serverId, naiveStart, naiveEnd))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var time = reader.GetDateTime(0);
                changes.Add(new ChangeWatchChange($"plan_force:snapshot:{database}:{reader.GetInt64(2)}:{reader.GetInt64(3)}:{Stamp(time)}", "plan_force", Utc(time),
                    $"plan {reader.GetInt64(3)} of query {reader.GetInt64(2)} in {database}: forced", database));
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
