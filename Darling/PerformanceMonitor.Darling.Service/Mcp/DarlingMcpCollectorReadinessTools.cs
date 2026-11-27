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

    /// <summary>The characters of a failure's text one waiting row carries (#9880). A longer text is cut at a word boundary and the row says so.</summary>
    internal const int RowErrorChars = 300;

    /// <summary>The characters of a failure's text one precondition group carries (#9880). The group carries the text once for every server it covers, so it affords the whole remedy of the setup steps the service writes.</summary>
    internal const int GroupErrorChars = 1000;

    /// <summary>The characters the failure read fetches, so the cut lands in code at a word boundary and never inside a word in SQL (#9880).</summary>
    internal const int ErrorReadChars = 4000;

    /// <summary>Example servers a precondition group names. The count says how many the group covers.</summary>
    internal const int ExampleServers = 5;

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
        + "A collector held by a setup step on the monitored server (a named skip: an extension not created or not preloaded, a grant refused, an Extended Events session not running) is not waiting on the service: precondition_count counts those, precondition_held lists each step once with the number of servers it holds and a few example servers (the full text of the step is kept whole there), and waiting_count does not include them, so it reaches zero on a finished install. "
        + "The start is the process start, so a restart resets the read. waiting is cut at 200 rows, oldest wait first; waiting_count is the total. The reply is also held to the 32,768-byte response budget: budget_truncated = true says waiting rows were left off the end, and budget_note names the offset that reads on; over_budget = true says the per-server summaries alone are over it and server_name reads one server. "
        + "A waiting row's last_error is cut at a word boundary when it is long, and last_error_cut says so: get_collection_log (server_name, collector_name) carries the whole text.")]
    public static Task<string> GetCollectorReadiness(
        NpgsqlDataSource postgres,
        [Description("Optional server name or display name. Omit for every enabled server.")] string? server_name = null,
        [Description("Waiting rows to skip before the page, in the answer's order (a page start). Default 0; budget_note names the offset that reads on when budget_truncated is true.")] int offset = 0,
        CancellationToken cancellationToken = default) =>
        GetCollectorReadiness(postgres, server_name, AlertReadFailureCounter.Shared.CountingSince, DateTime.UtcNow, cancellationToken, offset);

    /// <summary>The read with the start instant and the clock passed in, so a test sets the start without restarting a process.</summary>
    internal static async Task<string> GetCollectorReadiness(
        NpgsqlDataSource postgres, string? server_name, DateTime startUtc, DateTime nowUtc, CancellationToken cancellationToken, int offset = 0,
        int budgetBytes = McpResponseBudget.DefaultBytes)
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
                detailPairs => ReadDetailAsync(postgres, detailPairs, startUtc, nowUtc, cancellationToken),
                failurePairs => ReadFailuresAsync(postgres, failurePairs, startUtc, nowUtc, cancellationToken),
                offset, budgetBytes);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collector_readiness", ex);
        }
    }

    internal sealed record Pair(int ServerId, string Collector, int IntervalMinutes, bool RunsAtFixedTime);

    internal sealed record Detail(long Errors, long Skips);

    /// <summary>The newest failed run of a pair since the start: its <c>collection_log</c> status, when it ran and its text (read to <see cref="ErrorReadChars"/>).</summary>
    internal sealed record Failure(string Status, DateTime At, string? Text);

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
    /// The errors and the other non-success runs since the start, per pair, one keyed range read each. The caller
    /// names only the pairs it will show (the waiting rows on the page, or every pair of the one server asked about), so this never scans the fleet's rows
    /// since the start (#9417). An error is a failure status or a SUCCESS that abandoned its work.
    /// </summary>
    internal static readonly string DetailSql = $@"
SELECT p.server_id, p.collector_name, d.errors, d.skips
FROM unnest($1::int[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL (
    SELECT COUNT(*) FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})
                            OR (status = 'SUCCESS' AND COALESCE({EnumeratedCollectorDriver.AbandonedByNotePredicateSql}, false))) AS errors,
           COUNT(*) FILTER (WHERE status IS NULL OR status NOT IN ('SUCCESS', {CollectorFleetLiveness.FailureStatusSqlList})) AS skips
    FROM collection_log x
    WHERE x.server_id = p.server_id
    AND   x.collector_name = p.collector_name
    AND   x.collection_time >= $3
    AND   x.collection_time <= $4
) d";

    /// <summary>
    /// The newest failed run of each waiting pair since the start (#9880): its status, time and text, by one keyed descending probe per pair on
    /// <c>idx_collection_log_watermark</c> that stops at the first failure, so the cost follows the number of waiting pairs and not how many rows each
    /// has logged. The status is what tells a collector held by a setup step on the monitored server from one that has not had its first cycle:
    /// <see cref="CollectorRuntimePrecondition.IsNamedSkip"/>, the classification the service already has. The text is read to
    /// <see cref="ErrorReadChars"/> so the cut that follows lands on a word boundary in code, never inside a word in SQL. A pair with no failure has no row.
    /// </summary>
    internal static readonly string FailureSql = $@"
SELECT p.server_id, p.collector_name, f.status, f.collection_time, left(f.error_message, {ErrorReadChars})
FROM unnest($1::int[], $2::text[]) AS p(server_id, collector_name)
CROSS JOIN LATERAL (
    SELECT x.status, x.collection_time, x.error_message
    FROM collection_log x
    WHERE x.server_id = p.server_id
    AND   x.collector_name = p.collector_name
    AND   x.collection_time >= $3
    AND   x.collection_time <= $4
    AND   x.status IN ({CollectorFleetLiveness.FailureStatusSqlList})
    ORDER BY x.collection_time DESC
    LIMIT 1
) f";

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
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3));
        }

        return rows;
    }

    internal static async Task<Dictionary<(int ServerId, string Collector), Failure>> ReadFailuresAsync(
        NpgsqlDataSource postgres, List<Pair> pairs, DateTime startUtc, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<(int, string), Failure>();
        if (pairs.Count == 0)
        {
            return rows;
        }

        await using var command = postgres.CreateCommand(FailureSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddPairParameters(command, pairs);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, nowUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows[(reader.GetInt32(0), reader.GetString(1))] = new Failure(
                reader.GetString(2), reader.GetDateTime(3), reader.IsDBNull(4) ? null : reader.GetString(4));
        }

        return rows;
    }

    /// <summary>
    /// <paramref name="text"/> cut to at most <paramref name="max"/> characters at a word boundary (#9880): the last whitespace at or before the
    /// limit, dropping the trailing punctuation that separated the words, so a remedy never ends "listed in sha". A text with no whitespace in
    /// range is cut at the limit on a whole text element. The cut carries no marker of its own: the caller's <c>*_cut</c> flag is the marker.
    /// </summary>
    internal static (string? Text, bool Cut) CutAtWord(string? text, int max)
    {
        if (text is null || text.Length <= max)
        {
            return (text, false);
        }

        var end = max;
        /* A space right after the limit means the text up to it already ends on a word. */
        if (!char.IsWhiteSpace(text[max]))
        {
            while (end > 0 && !char.IsWhiteSpace(text[end - 1]))
            {
                end--;
            }

            if (end == 0)
            {
                end = McpHelpers.TextElementCutLength(text, max);
            }
        }

        var kept = text[..end].TrimEnd().TrimEnd(',', ';', ':', '-', '(');
        return (kept.Length == 0 ? text[..McpHelpers.TextElementCutLength(text, max)] : kept, true);
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
    /// Builds the answer. <paramref name="readDetail"/> reads the errors and skips for the pairs it is given: the waiting rows on the page, or every
    /// pair when one server was asked about. The fleet answer's per-server done rows carry no error counts, because those would need every pair's rows
    /// since the start (#9417). <paramref name="readFailures"/> reads the newest failed run of every waiting pair (#9880): a pair whose newest failure
    /// is a named skip (<see cref="CollectorRuntimePrecondition.IsNamedSkip"/>) is held by a setup step on the monitored server, not by the service, and
    /// is counted apart from a pair that has not had its first cycle. Held pairs are listed as groups, each distinct step once with the servers it
    /// holds. <paramref name="offset"/> starts the waiting page and <paramref name="budgetBytes"/> holds the whole reply to the response budget.
    /// </summary>
    internal static async Task<string> ComposeAsync(
        List<CollectorFleetLiveness.ServerFacts> servers,
        List<Pair> pairs,
        Dictionary<(int ServerId, string Collector), DateTime> firstGood,
        Dictionary<(int ServerId, string Collector), DateTime> before,
        DateTime startUtc, DateTime nowUtc, int notCollected, int disabled, bool singleServer,
        Func<List<Pair>, Task<Dictionary<(int ServerId, string Collector), Detail>>> readDetail,
        Func<List<Pair>, Task<Dictionary<(int ServerId, string Collector), Failure>>> readFailures,
        int offset = 0, int budgetBytes = McpResponseBudget.DefaultBytes)
    {
        var byServer = servers.ToDictionary(s => s.ServerId);
        var waiting = new List<(CollectorFleetLiveness.ServerFacts Server, Pair Pair, DateTime DueAt, bool DueAtStart)>();
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
                    waiting.Add((server, pair, dueAt!.Value, dueAtStart));
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

        /* A waiting pair whose newest failure is a named skip is held by a setup step on the monitored server (#9880). It leaves the wait and the
           first full cycle, which the service cannot finish for it, and is counted on its own. */
        var failures = await readFailures(waiting.Select(w => w.Pair).ToList());
        var heldRows = new List<(CollectorFleetLiveness.ServerFacts Server, Pair Pair, DateTime DueAt, Failure Failure)>();
        var stillWaiting = new List<(CollectorFleetLiveness.ServerFacts Server, Pair Pair, DateTime DueAt)>();
        foreach (var w in waiting)
        {
            if (failures.TryGetValue((w.Pair.ServerId, w.Pair.Collector), out var failure) && CollectorRuntimePrecondition.IsNamedSkip(failure.Status))
            {
                var tally = perServer[w.Pair.ServerId];
                tally.Waiting--;
                tally.Held++;
                if (w.DueAtStart)
                {
                    dueAtStartTotal--;
                    tally.DueAtStart--;
                }

                heldRows.Add((w.Server, w.Pair, w.DueAt, failure));
            }
            else
            {
                stillWaiting.Add((w.Server, w.Pair, w.DueAt));
            }
        }

        var ordered = stillWaiting
            .OrderBy(w => w.DueAt)
            .ThenBy(w => Name(w.Server), StringComparer.OrdinalIgnoreCase)
            .ThenBy(w => w.Pair.Collector, StringComparer.Ordinal)
            .ToList();
        var waitingServers = ordered.Select(w => w.Server.ServerId).Distinct().Count();

        /* The first full cycle is over the pairs that were due at the start and are not held by a setup step, so a collector that only came due later, done or waiting, cannot move it. Once every such pair has a good cycle it stays reached. */
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

        var held = heldRows.Count;
        var dueTotal = doneRows.Count + ordered.Count + held;
        var heldClause = held == 0
            ? ""
            : $"{held} more collector(s) on {heldRows.Select(h => h.Server.ServerId).Distinct().Count()} server(s) are held by a setup step on the monitored server, which the service cannot do (precondition_held names each step and the servers it holds)";

        /* The all-done verdict names each count as the reply's own fields do: not_yet_due_count as "not due yet" and fixed_time_count as the set-time collectors with no good cycle since the start (#9585). The waiting verdict names neither, and the two fields and the note carry them. */
        string verdict;
        if (ordered.Count == 0)
        {
            verdict = $"Every due collector {(held > 0 ? "that a setup step is not holding " : "")}has a good cycle since the service started at {Stamp(startUtc)}: {doneRows.Count} done on {servers.Count} server(s)"
                + (notYetDue > 0 ? $", {notYetDue} not due yet" : "")
                + (fixedTime > 0 ? $", {fixedTime} run at a set time and have no good cycle since the start" : "")
                + (held > 0 ? $", {heldClause}" : "")
                + (reached ? $". First full cycle at {Stamp(lastFirstGood!.Value)}." : ".");
        }
        else
        {
            var longest = ordered[0];
            verdict = $"{ordered.Count} of {dueTotal} due collector(s) on {waitingServers} server(s) have no good cycle since the service started at {Stamp(startUtc)}. "
                + $"Longest wait: {longest.Pair.Collector} on {Name(longest.Server)} since {Stamp(longest.DueAt)}. "
                + (held > 0 ? $"{char.ToUpperInvariant(heldClause[0])}{heldClause[1..]}. " : "")
                + (reached ? $"First full cycle at {Stamp(lastFirstGood!.Value)}." : "First full cycle: not yet.");
        }

        var start = Math.Clamp(offset, 0, ordered.Count);
        var page = ordered.Skip(start).Take(MaxWaitingRows).ToList();
        var detail = await readDetail(singleServer ? pairs : page.Select(w => w.Pair).ToList());
        Detail? DetailOf(string collector, int serverId) => detail.TryGetValue((serverId, collector), out var d) ? d : null;

        var waitingRows = page.Select(w =>
        {
            var d = DetailOf(w.Pair.Collector, w.Pair.ServerId);
            failures.TryGetValue((w.Pair.ServerId, w.Pair.Collector), out var failure);
            var (text, cut) = CutAtWord(failure?.Text, RowErrorChars);
            return (object)new
            {
                server = Name(w.Server),
                collector = w.Pair.Collector,
                waiting_since = Stamp(w.DueAt),
                errors_since_start = d?.Errors ?? 0,
                skips_since_start = d?.Skips ?? 0,
                last_error = text,
                last_error_cut = cut,
                last_error_at = failure is null ? null : Stamp(failure.At),
            };
        }).ToList();

        /* Each distinct step once: the text is the same on every server that lacks the step, so the group carries it with the count and a few example servers. */
        var heldGroups = heldRows
            .GroupBy(h => (h.Pair.Collector, h.Failure.Status, Text: h.Failure.Text ?? ""))
            .Select(g =>
            {
                var (text, cut) = CutAtWord(string.IsNullOrEmpty(g.Key.Text) ? null : g.Key.Text, GroupErrorChars);
                var names = g.Select(h => Name(h.Server)).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToList();
                return new
                {
                    collector = g.Key.Collector,
                    status = g.Key.Status,
                    servers = names.Count,
                    example_servers = names.Take(ExampleServers).ToList(),
                    waiting_since = Stamp(g.Min(h => h.DueAt)),
                    last_error = text,
                    last_error_cut = cut,
                    last_error_at = Stamp(g.Max(h => h.Failure.At)),
                };
            })
            .OrderBy(g => g.collector, StringComparer.Ordinal)
            .ThenBy(g => g.status, StringComparer.Ordinal)
            .ThenBy(g => g.last_error, StringComparer.Ordinal)
            .ToList();

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
                        collectors_held_by_precondition = t.Held,
                        collectors_not_yet_due = t.NotYetDue,
                        collectors_fixed_time = t.FixedTime,
                        first_full_cycle_at = t.DueAtStart > 0 && t.DueAtStartDone == t.DueAtStart && t.LastDueAtStartGood is DateTime g ? Stamp(g) : null,
                    };
                }).ToList();

        string Reply(int n, bool overBudget = false)
        {
            var shown = waitingRows.Take(n).ToList();
            var rowsAfter = start + n < ordered.Count;
            var budgetCut = n < waitingRows.Count;
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
                precondition_count = held,
                not_yet_due_count = notYetDue,
                fixed_time_count = fixedTime,
                not_collected_count = notCollected,
                disabled_count = disabled,
                truncated = rowsAfter,
                budget_truncated = budgetCut,
                over_budget = overBudget,
                budget_note = overBudget
                    ? $"OVER BUDGET: the per-server summary and the precondition groups alone are over the {budgetBytes:#,0}-byte response budget at {n:#,0} waiting row(s), so no row can be left off to fit it. Ask one server at a time (server_name) for its collectors."
                    + (budgetCut ? $" {waitingRows.Count - n:#,0} waiting row(s) were left off: call again with offset = {start + n} for the next rows." : "")
                    : budgetCut
                    ? McpResponseBudget.PageCutNote(
                        ordered.Count - start, n, "oldest wait first", "waiting rows",
                        $"Call again with offset = {start + n} for the next rows. waiting_count counts every waiting pair, listed or not.")
                    : null,
                waiting = shown,
                precondition_held = heldGroups,
                done,
                note = "A collector counts as waiting only when it is enabled, collected on the server's engine, not on a fixed run time, and due: it never ran, was overdue at the start, or has reached the end of the interval it was waiting out, and no setup step on the monitored server is holding it. "
                    + "not_yet_due_count and fixed_time_count are collectors that are not waiting. "
                    + "precondition_count is the collectors whose newest failure since the start is a named skip (EXTENSION_MISSING, PERMISSIONS or SESSION_MISSING): a setup step on the monitored server, listed in precondition_held with the servers it holds, and not counted in waiting_count or in first_full_cycle. "
                    + "A last_error with last_error_cut = true was cut at a word boundary: get_collection_log (server_name, collector_name) carries the whole text.",
            }, McpHelpers.JsonOptions);
        }

        /* The waiting rows are what the budget cuts, oldest wait first, so the next page starts where this one ends. The reply is never below one row: when the summaries alone are over the budget the reply says so (over_budget) and names the per-server read. */
        var fit = McpResponseBudget.RowsThatFit(waitingRows.Count, n => System.Text.Encoding.UTF8.GetByteCount(Reply(n)), budgetBytes);
        var fitted = Reply(fit);
        return System.Text.Encoding.UTF8.GetByteCount(fitted) > budgetBytes ? Reply(fit, overBudget: true) : fitted;
    }

    private sealed class ServerTally
    {
        public int Done;
        public int Waiting;
        public int NotYetDue;
        public int FixedTime;
        public int Held;
        public int DueAtStart;
        public int DueAtStartDone;
        public DateTime? LastDueAtStartGood;
    }
}
