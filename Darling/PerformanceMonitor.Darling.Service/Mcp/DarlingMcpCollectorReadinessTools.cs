/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9417: the install check. After an install or a restart an operator waits to see that every collector that is due has had a clean cycle
/// since the service came up. <c>get_collection_health</c> counts over seven days and lists only collectors that have logged, so it cannot
/// name the ones that have not run yet. This read keys everything to the service's own start instant (the one
/// <c>get_collection_health</c> reports as <c>service.started_at</c>): per server and collector, the first good cycle since the start, the
/// errors and skips since, and the collectors that are due and have not had a good cycle.
///
/// <para><b>What "due" means.</b> The collectors a server runs, minus the ones that are not waiting on anything: disabled by the schedule, not
/// collected on the server's engine (or Always On off, or a readable Query Store secondary), and the ones that run at a fixed time of day.
/// A collector that ran recently before the start waits out the rest of its interval, as the service's own seeding does
/// (<see cref="DarlingWorker.ComputeSeededNextDue"/>): until then it is <c>not yet due</c>, not waiting. One that never ran, or was overdue at
/// the start, is due from the start.</para>
///
/// <para><b>First full cycle.</b> The first moment every collector that was due at the start had a good cycle, on every enrolled server.
/// Null with "not yet" until it happens, and it never goes back: a collector that came due later and keeps failing is waiting, but it does not hold the first full cycle.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCollectorReadinessTools
{
    /// <summary>The waiting rows one answer lists. The counts at the top cover every waiting pair, listed or not.</summary>
    internal const int MaxWaitingRows = 200;

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_collector_readiness"), Description(
        "Install check: since THIS service process started, which due collectors have not had a good cycle yet. One-line verdict first, then the waiting list, then done. "
        + "Omit server_name for every enabled server (waiting rows plus one done summary per server); give one server for every collector's first good cycle, errors and skips since the start. "
        + "Disabled collectors and ones not collected on the server's engine are not waiting. <<GUIDE>> "
        + "Reads the collection log since the service's own start instant (the started_at get_collection_health carries) and the collector schedules. "
        + "verdict says either that every due collector has a good cycle since the start, or how many have not and the longest wait. "
        + "waiting lists each (server, collector) due with no good cycle since the start: waiting_since is when it came due (the start, or later for a collector that ran just before the restart and waits out its interval), plus errors_since_start, skips_since_start and the newest error. "
        + "not_yet_due_count (collectors_not_yet_due per server in the fleet answer) counts collectors that ran just before the restart and are not due again yet, and fixed_time_count (collectors_fixed_time) counts collectors that run at a set time of day and have no good cycle since the start, and neither is waiting. "
        + "done lists the collectors with a good cycle since the start: first_good_at, errors_since_start and skips_since_start for one server, one summary row per server (counts of collectors, no error counts) for the fleet. "
        + "first_full_cycle is the first moment every collector that was due at the start had a good cycle on every enrolled server: status reached with at, or not yet with at null. "
        + "A good cycle is a SUCCESS run. An error is ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING or ABANDONED. A skip is any other status, such as SKIPPED or YIELDED. "
        + "The start is the process start, so a restart resets the read. waiting is cut at 200 rows, oldest wait first; waiting_count is the total.")]
    public static Task<string> GetCollectorReadiness(
        NpgsqlDataSource postgres,
        [Description("Optional server name or display name. Omit for every enabled server.")] string? server_name = null,
        CancellationToken cancellationToken = default) =>
        GetCollectorReadiness(postgres, server_name, AlertReadFailureCounter.Shared.CountingSince, DateTime.UtcNow, cancellationToken);

    /// <summary>The read with the start instant and the clock passed in, so a test sets the start without restarting a process.</summary>
    internal static async Task<string> GetCollectorReadiness(
        NpgsqlDataSource postgres, string? server_name, DateTime startUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        int? onlyServerId = null;
        if (!string.IsNullOrWhiteSpace(server_name))
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
            if (error is not null)
            {
                return error;
            }

            onlyServerId = resolved.ServerId;
        }

        try
        {
            var servers = await ReadServersAsync(postgres, cancellationToken);
            if (onlyServerId is int only)
            {
                servers = servers.Where(s => s.ServerId == only).ToList();
                if (servers.Count == 0)
                {
                    return McpHelpers.Status("empty", $"{server_name!.Trim()} is not enabled, so no collector is due on it.");
                }
            }

            if (servers.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No enabled server is registered, so no collector is due.");
            }

            var gatedByServer = await FleetEngineGatedCollectors.ReadAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var alwaysOnOff = await FleetAlwaysOnOff.ReadAsync(postgres, McpCommandDeadlines.ReadSeconds, cancellationToken);
            var queryStoreSecondary = await DarlingGatedCollectorRows.QueryStoreSecondaryFleetMessagesAsync(postgres, servers, cancellationToken);
            var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, serverId: null, cancellationToken, throwOnFailure: true);
            var runTimes = (await DarlingCliCommands.ReadCollectorRunTimeRowsAsync(postgres, null, cancellationToken))
                .Select(r => new RunTimeOverride(r.ServerId, r.CollectorName, r.RunAtMinute))
                .ToList();
            var schedules = StoreConfigProvider.MergeRunTimes(overrides, runTimes);

            /* The (server, collector) pairs a server is expected to run, and what keeps the rest out of the wait. */
            var pairs = new List<Pair>();
            var notCollected = 0;
            var disabled = 0;
            foreach (var server in servers)
            {
                gatedByServer.TryGetValue(server.ServerId, out var gated);
                foreach (var definition in CollectorCatalog.All)
                {
                    var name = definition.Name;
                    if ((gated is not null && gated.Contains(name))
                        || (DarlingGatedCollectorRows.IsAlwaysOnCollector(name) && alwaysOnOff.Contains(server.ServerId))
                        || (name == DarlingGatedCollectorRows.QueryStoreSecondaryCollector && queryStoreSecondary.ContainsKey(server.ServerId)))
                    {
                        notCollected++;
                        continue;
                    }

                    if (!CollectorScheduleDefaults.All.ContainsKey(name))
                    {
                        continue;
                    }

                    var schedule = StoreConfigProvider.ResolveSchedule(name, server.ServerId, schedules);
                    if (!schedule.Enabled)
                    {
                        disabled++;
                        continue;
                    }

                    pairs.Add(new Pair(
                        server.ServerId, name,
                        CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes),
                        schedule.RunAtMinute is not null));
                }
            }

            var firstGood = await ReadFirstGoodAsync(postgres, pairs, startUtc, nowUtc, cancellationToken);
            var before = await ReadLastRunBeforeStartAsync(postgres, pairs, startUtc, cancellationToken);

            return await ComposeAsync(
                servers, pairs, firstGood, before, startUtc, nowUtc, notCollected, disabled, onlyServerId is not null,
                detailPairs => ReadDetailAsync(postgres, detailPairs, startUtc, nowUtc, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collector_readiness", ex);
        }
    }

    internal sealed record Pair(int ServerId, string Collector, int IntervalMinutes, bool RunsAtFixedTime);

    internal sealed record Detail(long Errors, long Skips, string? LastError, DateTime? LastErrorAt);

    private static async Task<List<CollectorFleetLiveness.ServerFacts>> ReadServersAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var servers = new List<CollectorFleetLiveness.ServerFacts>();
        await using var command = postgres.CreateCommand(DarlingMcpDataTools.FleetServersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            servers.Add(new CollectorFleetLiveness.ServerFacts(
                reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? CollectorEngineCapability.UnknownEngineEdition : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                CollectorEngineCapability.AwsRdsKindOf(
                    !reader.IsDBNull(5) && reader.GetBoolean(5), !reader.IsDBNull(6) && reader.GetBoolean(6)),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7)));
        }

        return servers;
    }

    /// <summary>
    /// The first good cycle of each pair since the start, by one keyed index probe per pair (oldest first on <c>idx_collection_log_watermark</c>),
    /// so the cost follows the number of pairs and not how long the service has been up (#9417). A good cycle is a SUCCESS that did not abandon
    /// its work. The abandoned-by-note rule is the one collection health uses, made NULL-safe here because a row with a NULL
    /// <c>rows_collected</c> must count as a run that did not abandon, never drop out of the SUCCESS count. A pair with no good cycle has no row.
    /// </summary>
    internal static readonly string FirstGoodSql = $@"
SELECT p.server_id, p.collector_name, g.collection_time
FROM unnest($1::int[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL (
    SELECT x.collection_time
    FROM collection_log x
    WHERE x.server_id = p.server_id
    AND   x.collector_name = p.collector_name
    AND   x.collection_time >= $3
    AND   x.collection_time <= $4
    AND   x.status = 'SUCCESS'
    AND   NOT COALESCE({EnumeratedCollectorDriver.AbandonedByNotePredicateSql}, false)
    ORDER BY x.collection_time
    LIMIT 1
) g";

    /// <summary>
    /// The errors, the other non-success runs and the newest error with its text since the start, per pair, one keyed range read each. The caller
    /// names only the pairs it will show (the waiting rows, or every pair of the one server asked about), so this never scans the fleet's rows
    /// since the start (#9417). An error is a failure status or a SUCCESS that abandoned its work.
    /// </summary>
    internal static readonly string DetailSql = $@"
SELECT p.server_id, p.collector_name, d.errors, d.skips, d.last_error_at, d.last_error
FROM unnest($1::int[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL (
    SELECT COUNT(*) FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})
                            OR (status = 'SUCCESS' AND COALESCE({EnumeratedCollectorDriver.AbandonedByNotePredicateSql}, false))) AS errors,
           COUNT(*) FILTER (WHERE status IS NULL OR status NOT IN ('SUCCESS', {CollectorFleetLiveness.FailureStatusSqlList})) AS skips,
           MAX(collection_time) FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})) AS last_error_at,
           (array_agg(left(error_message, {CollectorFleetLiveness.MaxMessageChars}) ORDER BY collection_time DESC)
                FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})))[1] AS last_error
    FROM collection_log x
    WHERE x.server_id = p.server_id
    AND   x.collector_name = p.collector_name
    AND   x.collection_time >= $3
    AND   x.collection_time <= $4
) d";

    internal static async Task<Dictionary<(int ServerId, string Collector), DateTime>> ReadFirstGoodAsync(
        NpgsqlDataSource postgres, List<Pair> pairs, DateTime startUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<(int, string), DateTime>();
        if (pairs.Count == 0)
        {
            return rows;
        }

        await using var command = postgres.CreateCommand(FirstGoodSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddPairParameters(command, pairs);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, nowUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows[(reader.GetInt32(0), reader.GetString(1))] = reader.GetDateTime(2);
        }

        return rows;
    }

    internal static async Task<Dictionary<(int ServerId, string Collector), Detail>> ReadDetailAsync(
        NpgsqlDataSource postgres, List<Pair> pairs, DateTime startUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<(int, string), Detail>();
        if (pairs.Count == 0)
        {
            return rows;
        }

        await using var command = postgres.CreateCommand(DetailSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddPairParameters(command, pairs);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, nowUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows[(reader.GetInt32(0), reader.GetString(1))] = new Detail(
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4));
        }

        return rows;
    }

    private static void AddPairParameters(NpgsqlCommand command, List<Pair> pairs)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = pairs.Select(p => p.ServerId).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pairs.Select(p => p.Collector).ToArray() });
    }

    /// <summary>
    /// The newest run of each pair before the start, by one keyed index seek per pair, looking back no further than
    /// <see cref="DarlingWorker.WatermarkFloorLookback"/> from the start: the floor the worker seeds its schedule with, so an older run reads as
    /// never run here exactly as it does for the worker. A pair with no run in that span has no entry.
    /// </summary>
    internal const string LastRunBeforeStartSql = @"
SELECT p.server_id, p.collector_name, l.collection_time
FROM unnest($1::int[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL (
    SELECT x.collection_time
    FROM collection_log x
    WHERE x.server_id = p.server_id
    AND   x.collector_name = p.collector_name
    AND   x.collection_time < $3
    AND   x.collection_time >= $4
    ORDER BY x.collection_time DESC
    LIMIT 1
) l";

    private static async Task<Dictionary<(int ServerId, string Collector), DateTime>> ReadLastRunBeforeStartAsync(
        NpgsqlDataSource postgres, List<Pair> pairs, DateTime startUtc, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<(int, string), DateTime>();
        if (pairs.Count == 0)
        {
            return rows;
        }

        await using var command = postgres.CreateCommand(LastRunBeforeStartSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = pairs.Select(p => p.ServerId).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = pairs.Select(p => p.Collector).ToArray() });
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc - DarlingWorker.WatermarkFloorLookback);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows[(reader.GetInt32(0), reader.GetString(1))] = reader.GetDateTime(2);
        }

        return rows;
    }

    /// <summary>One pair's place in the wait, from what the log holds. Pure, so a test drives every state with a fixed clock.</summary>
    internal enum PairState
    {
        Done,
        Waiting,
        NotYetDue,
        FixedTime,
    }

    /// <summary>
    /// The state of one pair, and when it came due. A good cycle since the start is done. Otherwise the pair is due from the start when it never
    /// ran or was overdue at the start (<c>last run + interval</c> at or before the start, the service's seeding rule), and from
    /// <c>last run + interval</c> when it ran recently: waiting once that has passed, not yet due before. A collector that runs at a fixed time
    /// of day has no interval rule, so it is never counted as waiting.
    /// </summary>
    internal static (PairState State, DateTime? DueAt, bool DueAtStart) Classify(
        DateTime? firstGoodUtc, DateTime? lastRunBeforeStartUtc, int intervalMinutes, bool runsAtFixedTime, DateTime startUtc, DateTime nowUtc)
    {
        if (runsAtFixedTime)
        {
            return (firstGoodUtc is null ? PairState.FixedTime : PairState.Done, null, false);
        }

        var dueAt = lastRunBeforeStartUtc is DateTime last ? last.AddMinutes(intervalMinutes) : startUtc;
        var dueAtStart = dueAt <= startUtc;
        if (dueAtStart)
        {
            dueAt = startUtc;
        }

        if (firstGoodUtc is not null)
        {
            return (PairState.Done, dueAt, dueAtStart);
        }

        return (dueAt <= nowUtc ? PairState.Waiting : PairState.NotYetDue, dueAt, dueAtStart);
    }

    private static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o");

    private static string Name(CollectorFleetLiveness.ServerFacts s) => string.IsNullOrEmpty(s.DisplayName) ? s.ServerName : s.DisplayName;

    /// <summary>
    /// Builds the answer. <paramref name="readDetail"/> reads the errors, skips and newest error for the pairs it is given: the waiting rows that
    /// will be shown, or every pair when one server was asked about. The fleet answer's per-server done rows carry no error counts, because those
    /// would need every pair's rows since the start (#9417).
    /// </summary>
    internal static async Task<string> ComposeAsync(
        List<CollectorFleetLiveness.ServerFacts> servers,
        List<Pair> pairs,
        Dictionary<(int ServerId, string Collector), DateTime> firstGood,
        Dictionary<(int ServerId, string Collector), DateTime> before,
        DateTime startUtc, DateTime nowUtc, int notCollected, int disabled, bool singleServer,
        Func<List<Pair>, Task<Dictionary<(int ServerId, string Collector), Detail>>> readDetail)
    {
        var byServer = servers.ToDictionary(s => s.ServerId);
        var waiting = new List<(CollectorFleetLiveness.ServerFacts Server, Pair Pair, DateTime DueAt)>();
        var doneRows = new List<(CollectorFleetLiveness.ServerFacts Server, string Collector, DateTime FirstGood)>();
        var notYetDue = 0;
        var fixedTime = 0;
        var dueAtStartTotal = 0;
        var dueAtStartDone = 0;
        DateTime? lastFirstGood = null;
        var perServer = servers.ToDictionary(s => s.ServerId, _ => new ServerTally());

        foreach (var pair in pairs)
        {
            var server = byServer[pair.ServerId];
            DateTime? good = firstGood.TryGetValue((pair.ServerId, pair.Collector), out var g0) ? g0 : null;
            DateTime? last = before.TryGetValue((pair.ServerId, pair.Collector), out var ran) ? ran : null;
            var (state, dueAt, dueAtStart) = Classify(
                good, last, pair.IntervalMinutes, pair.RunsAtFixedTime, startUtc, nowUtc);
            var tally = perServer[pair.ServerId];
            if (dueAtStart)
            {
                dueAtStartTotal++;
                tally.DueAtStart++;
            }

            switch (state)
            {
                case PairState.Done:
                    doneRows.Add((server, pair.Collector, good!.Value));
                    tally.Done++;
                    if (dueAtStart)
                    {
                        dueAtStartDone++;
                        tally.DueAtStartDone++;
                        lastFirstGood = lastFirstGood is null || good > lastFirstGood ? good : lastFirstGood;
                        tally.LastDueAtStartGood = tally.LastDueAtStartGood is null || good > tally.LastDueAtStartGood ? good : tally.LastDueAtStartGood;
                    }

                    break;
                case PairState.Waiting:
                    waiting.Add((server, pair, dueAt!.Value));
                    tally.Waiting++;
                    break;
                case PairState.NotYetDue:
                    notYetDue++;
                    tally.NotYetDue++;
                    break;
                default:
                    fixedTime++;
                    tally.FixedTime++;
                    break;
            }
        }

        var ordered = waiting
            .OrderBy(w => w.DueAt)
            .ThenBy(w => Name(w.Server), StringComparer.OrdinalIgnoreCase)
            .ThenBy(w => w.Pair.Collector, StringComparer.Ordinal)
            .ToList();
        var waitingServers = ordered.Select(w => w.Server.ServerId).Distinct().Count();

        /* The first full cycle is over the pairs that were due at the start, so a collector that only came due later, done or waiting, cannot move it. Once every due-at-start pair has a good cycle it stays reached. */
        var reached = dueAtStartTotal > 0 && dueAtStartDone == dueAtStartTotal;
        string fullCycleStatus;
        if (reached)
        {
            fullCycleStatus = "reached";
        }
        else if (dueAtStartTotal == 0)
        {
            fullCycleStatus = "nothing due at the start";
        }
        else
        {
            fullCycleStatus = "not yet";
        }

        var dueTotal = doneRows.Count + ordered.Count;
        /* The all-done verdict names each count as the reply's own fields do: not_yet_due_count as "not due yet" and fixed_time_count as the set-time collectors with no good cycle since the start (#9585). The waiting verdict names neither, and the two fields and the note carry them. */
        string verdict;
        if (ordered.Count == 0)
        {
            verdict = $"Every due collector has a good cycle since the service started at {Stamp(startUtc)}: {doneRows.Count} done on {servers.Count} server(s)"
                + (notYetDue > 0 ? $", {notYetDue} not due yet" : "")
                + (fixedTime > 0 ? $", {fixedTime} run at a set time and have no good cycle since the start" : "")
                + (reached ? $". First full cycle at {Stamp(lastFirstGood!.Value)}." : ".");
        }
        else
        {
            var longest = ordered[0];
            verdict = $"{ordered.Count} of {dueTotal} due collector(s) on {waitingServers} server(s) have no good cycle since the service started at {Stamp(startUtc)}. "
                + $"Longest wait: {longest.Pair.Collector} on {Name(longest.Server)} since {Stamp(longest.DueAt)}. "
                + (reached ? $"First full cycle at {Stamp(lastFirstGood!.Value)}." : "First full cycle: not yet.");
        }

        var shown = ordered.Take(MaxWaitingRows).ToList();
        var detail = await readDetail(singleServer ? pairs : shown.Select(w => w.Pair).ToList());
        Detail? DetailOf(string collector, int serverId) => detail.TryGetValue((serverId, collector), out var d) ? d : null;

        var shownWaiting = shown.Select(w =>
        {
            var d = DetailOf(w.Pair.Collector, w.Pair.ServerId);
            return new
            {
                server = Name(w.Server),
                collector = w.Pair.Collector,
                waiting_since = Stamp(w.DueAt),
                errors_since_start = d?.Errors ?? 0,
                skips_since_start = d?.Skips ?? 0,
                last_error = d?.LastError,
                last_error_at = d?.LastErrorAt is DateTime at ? Stamp(at) : null,
            };
        }).ToList();

        object done = singleServer
            ? doneRows
                .OrderBy(d => d.FirstGood)
                .ThenBy(d => d.Collector, StringComparer.Ordinal)
                .Select(d => (object)new
                {
                    collector = d.Collector,
                    first_good_at = Stamp(d.FirstGood),
                    errors_since_start = DetailOf(d.Collector, d.Server.ServerId)?.Errors ?? 0,
                    skips_since_start = DetailOf(d.Collector, d.Server.ServerId)?.Skips ?? 0,
                }).ToList()
            : servers
                .OrderBy(s => Name(s), StringComparer.OrdinalIgnoreCase)
                .Select(s =>
                {
                    var t = perServer[s.ServerId];
                    return (object)new
                    {
                        server = Name(s),
                        collectors_done = t.Done,
                        collectors_waiting = t.Waiting,
                        collectors_not_yet_due = t.NotYetDue,
                        collectors_fixed_time = t.FixedTime,
                        first_full_cycle_at = t.DueAtStart > 0 && t.DueAtStartDone == t.DueAtStart && t.LastDueAtStartGood is DateTime g ? Stamp(g) : null,
                    };
                }).ToList();

        return JsonSerializer.Serialize(new
        {
            status = "ok",
            verdict,
            service_started_at = Stamp(startUtc),
            start_source = "process_start",
            as_of = Stamp(nowUtc),
            servers_enrolled = servers.Count,
            first_full_cycle = new
            {
                status = fullCycleStatus,
                at = reached ? Stamp(lastFirstGood!.Value) : null,
            },
            due_collectors = dueTotal,
            done_count = doneRows.Count,
            waiting_count = ordered.Count,
            not_yet_due_count = notYetDue,
            fixed_time_count = fixedTime,
            not_collected_count = notCollected,
            disabled_count = disabled,
            truncated = ordered.Count > MaxWaitingRows,
            waiting = shownWaiting,
            done,
            note = "A collector counts as waiting only when it is enabled, collected on the server's engine, not on a fixed run time, and due: it never ran, was overdue at the start, or has reached the end of the interval it was waiting out. "
                + "not_yet_due_count and fixed_time_count are collectors that are not waiting.",
        }, McpHelpers.JsonOptions);
    }

    private sealed class ServerTally
    {
        public int Done;
        public int Waiting;
        public int NotYetDue;
        public int FixedTime;
        public int DueAtStart;
        public int DueAtStartDone;
        public DateTime? LastDueAtStartGood;
    }
}
