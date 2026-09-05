/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service side of the store&lt;-&gt;service COMMAND plane (Stage 2): the Viewer (Stage 3) enqueues rows
/// into <c>config.config_command</c>; this executor CLAIMS one at a time, EXECUTES it, and REPORTS the
/// terminal outcome back on the same row so the Viewer can poll for the result (esp. <c>test_connect</c>'s
/// probe facts). Mirrors <see cref="StoreConfigProvider"/>'s style: pure, testable dispatch
/// (<see cref="ResolvePlan"/>) separated from the store I/O, failure-isolated (a bad command is reported
/// <c>failed</c>, never thrown out of the poller).
///
/// <para>The claim is safe against multiple service instances: <see cref="ClaimCommandSql"/> selects the
/// oldest <c>pending</c> row <c>FOR UPDATE SKIP LOCKED</c>, so two services polling the same store each
/// grab a DIFFERENT row (or none) instead of racing the same one. The desired-state commands
/// (pause/resume/enable/disable/reload) only WRITE the <c>config.*</c> tables — the bump trigger fires the
/// <c>config_version</c> reload beacon and the worker's existing reconcile applies them; the executor never
/// mutates the live collection loop's state directly. The imperative commands that DO need the running loop
/// (<c>snapshot_now</c>/<c>analyze_now</c>) are delegated to the worker through
/// <see cref="IDarlingCommandHost"/>.</para>
/// </summary>
public sealed class DarlingCommandExecutor
{
    /// <summary>
    /// The atomic claim (the correctness-critical piece): mark the OLDEST <c>pending</c> row
    /// <c>in_progress</c> and RETURN it, guarded by <c>FOR UPDATE SKIP LOCKED</c> in the sub-select so
    /// concurrent service instances never claim the same row. Public const so a test can pin the shape.
    /// </summary>
    public const string ClaimCommandSql = @"
UPDATE config.config_command
SET status = 'in_progress',
    claimed_at = now() AT TIME ZONE 'UTC',
    service_instance = $1
WHERE command_id = (
    SELECT command_id
    FROM config.config_command
    WHERE status = 'pending'
    ORDER BY command_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING command_id, command_type, target_server_id, args_json, requested_by";

    /// <summary>
    /// Writes the terminal outcome back on the claimed row (succeeded/failed + result_status/json). Internal
    /// so the round-trip test can pin + exercise it; $1 command_id, $2 status, $3 result_status, $4 result_json.
    ///
    /// <para><b>Secret retention (#1262 Low follow-up):</b> nulls <c>args_json</c> as the row reaches a
    /// terminal state — a <c>test_connect</c> command's <c>args_json</c> carries an inline credential blob,
    /// and there is no reason to retain it once the probe has run. The claim already captured args_json into
    /// the in-memory <see cref="ClaimedCommand"/> before execution, and the Viewer's terminal-result poll
    /// reads <c>result_status</c>/<c>result_json</c> (never args), so scrubbing it here is transparent. The
    /// stale-command reaper scrubs it the same way (<see cref="ReclaimStaleCommandsSql"/>) for a row that
    /// never reported. result_json holds only probe facts / an error string, never a secret, so it stays.</para>
    /// </summary>
    internal const string ReportCommandSql = @"
UPDATE config.config_command
SET status = $2,
    completed_at = now() AT TIME ZONE 'UTC',
    result_status = $3,
    result_json = $4,
    args_json = NULL
WHERE command_id = $1";

    /// <summary>
    /// The stale-command reaper (#1262 Stage 2 robustness follow-up): reclaims a command abandoned
    /// <c>in_progress</c> because the claiming service crashed or restarted between the claim and the report.
    /// Without this the row stays <c>in_progress</c> forever and a Viewer polling it for a terminal result
    /// waits indefinitely. A row whose <c>claimed_at</c> is older than the stale timeout ($1, a PG interval)
    /// is marked terminal <c>failed</c> with a clear <c>result_status</c> so the poll completes, and its
    /// <c>args_json</c> is nulled at the same time (the same terminal secret scrub the report path does).
    /// Public const so a test can pin the shape.
    ///
    /// <para><b>Why fail-terminal, not re-queue to <c>pending</c>:</b> re-running a reclaimed command is the
    /// UNSAFE choice — the imperative commands are not all idempotent (a half-run <c>snapshot_now</c> already
    /// wrote its collector rows, so a re-run double-writes them and races the shared delta baselines), and a
    /// command that CRASHED the service would crash it again on every reclaim (a poison-message loop that
    /// never drains). Marking it <c>failed</c> always unblocks the Viewer (the actual bug) and leaves the
    /// idempotent commands (pause/resume/test_connect/toggles) for an operator to re-issue by hand. The
    /// <see cref="StaleCommandTimeout"/> is wide enough that a merely-slow live command is never reaped.</para>
    /// </summary>
    public const string ReclaimStaleCommandsSql = @"
UPDATE config.config_command
SET status = 'failed',
    completed_at = now() AT TIME ZONE 'UTC',
    result_status = 'reclaimed (service restart or timeout)',
    result_json = '{""success"": false, ""error"": ""command reclaimed: the claiming service did not report a result within the stale-command timeout (a service restart or a crash mid-command)""}'::jsonb,
    args_json = NULL
WHERE status = 'in_progress'
  AND claimed_at < (now() AT TIME ZONE 'UTC') - $1";

    /// <summary>
    /// The stale-command reaper timeout: an <c>in_progress</c> command whose <c>claimed_at</c> is older than
    /// this is reclaimed as <c>failed</c>. Five minutes is a wide margin over the slowest command — a
    /// <c>test_connect</c> is bounded by the SQL connect timeout (~15-30s) and <c>analyze_now</c> self-caps
    /// at 120s — so the reaper can never catch a command that is merely slow, only one genuinely abandoned.
    /// A hardcoded default (not a config knob) per the "defaults over speculative config" rule.
    /// </summary>
    public static readonly TimeSpan StaleCommandTimeout = TimeSpan.FromMinutes(5);

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    private readonly NpgsqlDataSource _postgres;
    private readonly IDarlingCommandHost _host;
    private readonly string _serviceInstance;
    private readonly ILogger? _logger;

    public DarlingCommandExecutor(NpgsqlDataSource postgres, IDarlingCommandHost host, string serviceInstance, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _host = host ?? throw new ArgumentNullException(nameof(host));
        _serviceInstance = serviceInstance ?? throw new ArgumentNullException(nameof(serviceInstance));
        _logger = logger;
    }

    /// <summary>
    /// Claims and executes at most one pending command, reporting its outcome. Returns true if a command was
    /// claimed (so the caller can drain a backlog by looping until it returns false). Store-unreachable and
    /// any per-command failure are swallowed (logged) — the poller must never die for one bad tick.
    /// </summary>
    public async Task<bool> PollOnceAsync(CancellationToken cancellationToken)
    {
        ClaimedCommand? claimed;
        try
        {
            claimed = await ClaimNextAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Could not poll the command queue — will retry next tick: {Message}", ex.Message);
            return false;
        }

        if (claimed is null)
        {
            return false;
        }

        await ExecuteAndReportAsync(claimed, cancellationToken);
        return true;
    }

    /// <summary>
    /// Runs one stale-command reaper sweep (<see cref="ReclaimStaleCommandsSql"/>) and returns the number of
    /// commands reclaimed. Failure-isolated like the rest of the poller — a store blip logs and returns 0
    /// rather than killing the command loop. Cheap enough to run every tick: the WHERE is served by the
    /// status index and matches nothing in the normal case (commands are claimed and reported within
    /// seconds), and an UPDATE that changes no rows writes no WAL.
    /// </summary>
    public async Task<int> ReclaimStaleCommandsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(ReclaimStaleCommandsSql, connection);
            command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = StaleCommandTimeout });

            var reclaimed = await command.ExecuteNonQueryAsync(cancellationToken);
            if (reclaimed > 0)
            {
                _logger?.LogWarning(
                    "Reclaimed {Count} stale in_progress command(s) claimed over {Timeout} ago with no result reported (a service restart or a crash mid-command) — marked failed so the Viewer's poll completes",
                    reclaimed, StaleCommandTimeout);
            }

            return reclaimed;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning("Could not run the stale-command reaper — will retry next tick: {Message}", ex.Message);
            return 0;
        }
    }

    /// <summary>Atomically claims the oldest pending command (or null when the queue is empty).</summary>
    public async Task<ClaimedCommand?> ClaimNextAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(ClaimCommandSql, connection);
        command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
        command.Parameters.AddWithValue(_serviceInstance);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ClaimedCommand(
            CommandId: reader.GetInt64(0),
            CommandType: reader.GetString(1),
            TargetServerId: reader.IsDBNull(2) ? null : reader.GetInt32(2),
            ArgsJson: reader.IsDBNull(3) ? null : reader.GetFieldValue<string>(3),
            RequestedBy: reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    /// <summary>
    /// Executes one claimed command and writes its terminal result. Never throws out (an unexpected
    /// exception is caught and reported as <c>failed</c>/<c>error</c>), so the poller keeps running.
    /// </summary>
    public async Task ExecuteAndReportAsync(ClaimedCommand command, CancellationToken cancellationToken)
    {
        CommandOutcome outcome;
        try
        {
            outcome = await RunAsync(command, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError("Command {Id} ({Type}) failed: {Message}", command.CommandId, command.CommandType, ex.Message);
            outcome = new CommandOutcome(false, "error", ErrorJson(ex.Message));
        }

        await ReportAsync(command.CommandId, outcome, cancellationToken);
    }

    private async Task<CommandOutcome> RunAsync(ClaimedCommand command, CancellationToken cancellationToken)
    {
        var plan = ResolvePlan(command);
        switch (plan.Kind)
        {
            case CommandKind.Fail:
                _logger?.LogWarning("Command {Id}: {Reason} (type '{Type}')", command.CommandId, plan.FailReason, command.CommandType);
                return new CommandOutcome(false, plan.FailReason!, ErrorJson(plan.FailReason!));

            case CommandKind.StoreWrite:
                await ExecuteStoreWriteAsync(plan, cancellationToken);
                _logger?.LogInformation("Command {Id} ({Type}) => {Result}", command.CommandId, command.CommandType, plan.SuccessStatus);
                return new CommandOutcome(true, plan.SuccessStatus, DetailJson(plan.SuccessStatus));

            case CommandKind.Probe:
            {
                var server = DeserializeServer(command.ArgsJson!);
                if (server is null)
                {
                    return new CommandOutcome(false, "invalid args_json", ErrorJson("test_connect args_json did not deserialize to a server definition"));
                }

                var probe = await DarlingServerConnector.ProbeAsync(server, _logger, cancellationToken);
                var (resultStatus, resultJson) = MapProbeResult(probe);
                _logger?.LogInformation("Command {Id} (test_connect '{Server}') => {Result}", command.CommandId, server.DisplayName, resultStatus);
                return new CommandOutcome(probe.Success, resultStatus, resultJson);
            }

            case CommandKind.Snapshot:
                return await _host.SnapshotNowAsync(command.TargetServerId!.Value, cancellationToken);

            case CommandKind.Analyze:
                return await _host.AnalyzeNowAsync(command.TargetServerId!.Value, cancellationToken);

            case CommandKind.Purge:
            {
                /* Fleet-wide over the shared tables (no target server). The optional custom retention comes
                   from args_json (default = the configured fleet horizons); parse it here like the other
                   host-delegated branches re-derive their args. */
                var customRetentionDays = TryReadInt(command.ArgsJson, "retention_days", "retentionDays");
                return await _host.PurgeNowAsync(customRetentionDays, cancellationToken);
            }

            case CommandKind.FetchPlan:
            {
                /* Re-parse args_json here (the pure dispatch already validated it parses) so the host receives
                   the structured request — mirrors the Probe branch re-deriving its MonitoredServer. */
                if (!PlanFetchRequest.TryParse(command.ArgsJson, out var fetchRequest))
                {
                    return new CommandOutcome(false, "invalid args_json",
                        ErrorJson("fetch_plan args_json did not carry a plan_handle or sql_handle"));
                }

                return await _host.FetchPlanAsync(command.TargetServerId!.Value, fetchRequest, cancellationToken);
            }

            case CommandKind.ExecuteActualPlan:
            {
                /* Re-parse args_json here (the pure dispatch already validated it parses) so the host receives
                   the structured request — mirrors the FetchPlan branch. The payload is an IDENTIFIER ONLY
                   (query_hash + database_name); the host resolves the text/plan from the store and re-executes. */
                if (!ActualPlanRequest.TryParse(command.ArgsJson, out var actualPlanRequest))
                {
                    return new CommandOutcome(false, "invalid args_json",
                        ErrorJson("execute_actual_plan args_json did not carry a query_hash"));
                }

                return await _host.ExecuteActualPlanAsync(command.TargetServerId!.Value, actualPlanRequest, cancellationToken);
            }

            case CommandKind.TestHypotheticalIndex:
            {
                /* Re-parsed here so the host receives the structured request, mirroring the two branches
                   above. The payload is an IDENTIFIER plus a candidate; the host resolves the statement
                   text from the store and never takes it from the caller. */
                if (!HypotheticalIndexRequest.TryParse(command.ArgsJson, out var hypotheticalRequest))
                {
                    return new CommandOutcome(false, "invalid args_json",
                        ErrorJson("test_hypothetical_index args_json was missing or malformed"));
                }

                return await _host.TestHypotheticalIndexAsync(
                    command.TargetServerId!.Value, hypotheticalRequest, cancellationToken);
            }

            case CommandKind.FetchActiveQueries:
                /* Worker-delegated like fetch_plan (needs the target's LIVE connection to read the running-request
                   DMVs). Takes NO args — the target_server_id on the command row is the whole request; the host
                   runs the shared Active Queries collector's query on demand and returns the rows in result_json. */
                return await _host.FetchActiveQueriesLiveAsync(command.TargetServerId!.Value, cancellationToken);

            default:
                return new CommandOutcome(false, "unknown command_type", ErrorJson("unknown command_type"));
        }
    }

    /// <summary>
    /// PURE dispatch: maps a claimed command to a plan without touching the store, so every branch — incl.
    /// the argument-validation failures and the unknown-type fallback — is unit-testable. Store-write
    /// commands carry their parameterized SQL (the toggled boolean is a compile-time literal, never user
    /// input; every value is a bound parameter); the imperative/probe commands carry only a kind.
    /// </summary>
    internal static CommandPlan ResolvePlan(ClaimedCommand command)
    {
        switch ((command.CommandType ?? string.Empty).Trim().ToLowerInvariant())
        {
            case "pause":
                return StoreWrite("UPDATE config.config_service SET paused = TRUE WHERE id = 1", null, "paused");

            case "resume":
                return StoreWrite("UPDATE config.config_service SET paused = FALSE WHERE id = 1", null, "resumed");

            case "reload":
                /* No state change — an UPDATE bumps config_version via the self-bump trigger, forcing the
                   worker's next-sweep reload (an explicit resync). */
                return StoreWrite("UPDATE config.config_service SET updated_at = now() AT TIME ZONE 'UTC' WHERE id = 1", null, "reload signaled");

            case "enable_server":
                return command.TargetServerId is int enableId
                    ? StoreWrite(
                        "UPDATE config.config_monitored_servers SET is_enabled = TRUE, modified_at = now() AT TIME ZONE 'UTC' WHERE server_id = $1",
                        new object?[] { enableId }, "server enabled")
                    : Fail("enable_server requires target_server_id");

            case "disable_server":
                return command.TargetServerId is int disableId
                    ? StoreWrite(
                        "UPDATE config.config_monitored_servers SET is_enabled = FALSE, modified_at = now() AT TIME ZONE 'UTC' WHERE server_id = $1",
                        new object?[] { disableId }, "server disabled")
                    : Fail("disable_server requires target_server_id");

            case "enable_collector":
                return ResolveCollectorToggle(command, enabled: true);

            case "disable_collector":
                return ResolveCollectorToggle(command, enabled: false);

            case "test_connect":
                return string.IsNullOrWhiteSpace(command.ArgsJson)
                    ? Fail("test_connect requires args_json with the server definition")
                    : new CommandPlan(CommandKind.Probe, null, null, "connected", null);

            case "snapshot_now":
                return command.TargetServerId is int
                    ? new CommandPlan(CommandKind.Snapshot, null, null, "snapshot complete", null)
                    : Fail("snapshot_now requires target_server_id");

            case "analyze_now":
                return command.TargetServerId is int
                    ? new CommandPlan(CommandKind.Analyze, null, null, "analysis complete", null)
                    : Fail("analyze_now requires target_server_id");

            case "purge_now":
                /* The daily retention purge on demand. Fleet-wide over the SHARED store tables, so — unlike
                   snapshot_now/analyze_now — it needs NO target_server_id. An optional custom retention
                   (args_json.retention_days) is read at execution time; absent = the configured fleet horizons.
                   Always resolvable: there are no required arguments. */
                return new CommandPlan(CommandKind.Purge, null, null, "purge complete", null);

            case "fetch_plan":
                /* Worker-delegated like snapshot_now (needs the target's LIVE runtime connection to read its
                   plan cache). Requires a target server AND args_json carrying a plan_handle or sql_handle. */
                if (command.TargetServerId is null)
                {
                    return Fail("fetch_plan requires target_server_id");
                }

                return PlanFetchRequest.TryParse(command.ArgsJson, out _)
                    ? new CommandPlan(CommandKind.FetchPlan, null, null, "plan fetched", null)
                    : Fail("fetch_plan requires args_json with a plan_handle or sql_handle");

            case "execute_actual_plan":
                /* Worker-delegated (needs the target's LIVE runtime connection to RE-EXECUTE the query) and the
                   store (to resolve the query text/plan from the identifier). Requires a target server AND
                   args_json carrying the stored-row key. This is the one command that WRITES to a target (it
                   re-applies a modifying query's changes), so it rides the same read-write-seat-only enqueue as
                   every other command — a read-only viewer cannot reach it. */
                if (command.TargetServerId is null)
                {
                    return Fail("execute_actual_plan requires target_server_id");
                }

                return ActualPlanRequest.TryParse(command.ArgsJson, out _)
                    ? new CommandPlan(CommandKind.ExecuteActualPlan, null, null, "actual plan captured", null)
                    : Fail("execute_actual_plan requires args_json with a query_hash");

            case "test_hypothetical_index":
                /* Worker-delegated (needs the target's LIVE connection to plan against its statistics) and the
                   store (to resolve the statement text from the queryid). #2612: the ONE PostgreSQL command
                   that makes the product act rather than read, so it rides the same read-write-seat-only
                   enqueue as execute_actual_plan — but it is a lesser class than that one, and the message
                   says so rather than leaving the reader to infer it: EXPLAIN without ANALYZE does not run
                   the statement, a hypothetical index is never written anywhere, and both are undone before
                   the call returns. */
                if (command.TargetServerId is null)
                {
                    return Fail("test_hypothetical_index requires target_server_id");
                }

                return HypotheticalIndexRequest.TryParse(command.ArgsJson, out _)
                    ? new CommandPlan(CommandKind.TestHypotheticalIndex, null, null, "hypothetical index tested", null)
                    : Fail("test_hypothetical_index requires args_json with queryid, schemaName, tableName and a "
                           + "non-empty columns array of plain identifiers");

            case "fetch_active_queries":
                /* Worker-delegated like fetch_plan (needs the target's LIVE runtime connection to read what is
                   running now). Requires a target server; takes NO args_json (the whole request is "read this
                   server's active queries"). Read-only DMV read — NOT consent-class like execute_actual_plan —
                   but it rides the same read-write-seat-only enqueue as every other command. */
                return command.TargetServerId is int
                    ? new CommandPlan(CommandKind.FetchActiveQueries, null, null, "active queries fetched", null)
                    : Fail("fetch_active_queries requires target_server_id");

            default:
                return Fail("unknown command_type");
        }
    }

    /// <summary>
    /// enable/disable_collector -> upsert <c>config_collector_schedules.enabled</c> for the collector named in
    /// args_json, scoped to <c>target_server_id</c> (else args_json.server_id, else fleet-wide when both are
    /// absent). Touches ONLY the enabled column (ON CONFLICT DO UPDATE), so an existing frequency/retention
    /// override is preserved. The two ON CONFLICT arbiters match V17's partial unique indexes (a PK cannot
    /// span the nullable server_id).
    /// </summary>
    private static CommandPlan ResolveCollectorToggle(ClaimedCommand command, bool enabled)
    {
        var verb = enabled ? "enable_collector" : "disable_collector";
        var collectorName = TryReadString(command.ArgsJson, "collector_name", "collectorName");
        if (string.IsNullOrWhiteSpace(collectorName))
        {
            return Fail($"{verb} requires args_json.collector_name");
        }

        if (!CollectorScheduleDefaults.All.ContainsKey(collectorName))
        {
            return Fail($"{verb}: unknown collector '{collectorName}'");
        }

        var flag = enabled ? "TRUE" : "FALSE";
        var successStatus = enabled ? "collector enabled" : "collector disabled";
        var serverId = command.TargetServerId ?? TryReadInt(command.ArgsJson, "server_id", "serverId");

        if (serverId is int scopedId)
        {
            return StoreWrite(
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ($1, $2, " + flag + ") " +
                "ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET enabled = EXCLUDED.enabled",
                new object?[] { scopedId, collectorName }, successStatus);
        }

        return StoreWrite(
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES (NULL, $1, " + flag + ") " +
            "ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET enabled = EXCLUDED.enabled",
            new object?[] { collectorName }, successStatus + " (fleet-wide)");
    }

    /// <summary>test_connect result mapping (pure): success carries the probe facts, failure the error.</summary>
    internal static (string ResultStatus, string ResultJson) MapProbeResult(ConnectionProbeResult probe)
    {
        if (probe.Success)
        {
            /* The PostgreSQL facts ride alongside rather than replacing anything: an existing consumer
               reading majorVersion/engineEdition keeps working, and one that knows about engine can tell
               why those are 0 on a Postgres target instead of reading it as a half-failed probe. */
            var json = JsonSerializer.Serialize(new
            {
                success = true,
                majorVersion = probe.MajorVersion,
                engineEdition = probe.EngineEdition,
                engineEditionDescription = probe.EngineEditionDescription,
                isAzureSqlDb = probe.IsAzureSqlDb,
                isAzureManagedInstance = probe.IsAzureManagedInstance,
                isAwsRds = probe.IsAwsRds,
                hasMsdbAccess = probe.HasMsdbAccess,
                engine = probe.Engine.ToString(),
                postgresMajorVersion = probe.PostgresMajorVersion,
                postgresVersionNum = probe.PostgresVersionNum,
                isAurora = probe.IsAurora,
                isInRecovery = probe.IsInRecovery,
                facts = DarlingServerConnector.DescribeProbeFacts(probe),
            });
            return ("connected", json);
        }

        return ("connection_failed", ErrorJson(probe.Error ?? "connection failed"));
    }

    private async Task ExecuteStoreWriteAsync(CommandPlan plan, CancellationToken cancellationToken)
    {
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(plan.Sql, connection);
        command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
        if (plan.Parameters is not null)
        {
            foreach (var parameter in plan.Parameters)
            {
                command.Parameters.AddWithValue(parameter ?? (object)DBNull.Value);
            }
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task ReportAsync(long commandId, CommandOutcome outcome, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(ReportCommandSql, connection);
            command.CommandTimeout = ServiceCommandDeadlines.CommandPlaneSeconds;
            command.Parameters.AddWithValue(commandId);
            command.Parameters.AddWithValue(outcome.Success ? "succeeded" : "failed");
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)outcome.ResultStatus ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = (object?)outcome.ResultJson ?? DBNull.Value });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Failure-isolated: a store blip writing the result must not kill the poller; the row stays
               in_progress and a later reader can see it never terminated. */
            _logger?.LogWarning("Could not write result for command {Id}: {Message}", commandId, ex.Message);
        }
    }

    private static MonitoredServer? DeserializeServer(string argsJson)
    {
        try
        {
            return JsonSerializer.Deserialize<MonitoredServer>(argsJson, s_jsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static string? TryReadString(string? json, params string[] names)
    {
        var value = TryReadProperty(json, names);
        return value.HasValue && value.Value.ValueKind == JsonValueKind.String ? value.Value.GetString() : null;
    }

    private static int? TryReadInt(string? json, params string[] names)
    {
        var value = TryReadProperty(json, names);
        return value.HasValue && value.Value.ValueKind == JsonValueKind.Number && value.Value.TryGetInt32(out var i) ? i : null;
    }

    private static JsonElement? TryReadProperty(string? json, string[] names)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            foreach (var name in names)
            {
                if (document.RootElement.TryGetProperty(name, out var element))
                {
                    return element.Clone();
                }
            }
        }
        catch (JsonException)
        {
            /* Malformed args_json — the caller degrades to "missing". */
        }

        return null;
    }

    private static CommandPlan StoreWrite(string sql, object?[]? parameters, string successStatus) =>
        new(CommandKind.StoreWrite, sql, parameters, successStatus, null);

    private static CommandPlan Fail(string reason) =>
        new(CommandKind.Fail, null, null, string.Empty, reason);

    private static string ErrorJson(string error) => JsonSerializer.Serialize(new { success = false, error });

    private static string DetailJson(string detail) => JsonSerializer.Serialize(new { success = true, detail });
}

/// <summary>A row claimed from <c>config.config_command</c>.</summary>
public sealed record ClaimedCommand(long CommandId, string CommandType, int? TargetServerId, string? ArgsJson, string? RequestedBy);

/// <summary>The terminal outcome written back on a command row.</summary>
public sealed record CommandOutcome(bool Success, string ResultStatus, string? ResultJson);

/// <summary>How a command is carried out — the four execution shapes plus the fail-fast fallback.</summary>
public enum CommandKind
{
    /// <summary>A parameterized write to the <c>config.*</c> tables (the bump trigger drives the reload).</summary>
    StoreWrite,

    /// <summary><c>test_connect</c>: deserialize args_json, probe the server, report the facts/error.</summary>
    Probe,

    /// <summary><c>snapshot_now</c>: run the running loop's collectors for a server now (via the host).</summary>
    Snapshot,

    /// <summary><c>analyze_now</c>: force an immediate analysis pass for a server now (via the host).</summary>
    Analyze,

    /// <summary><c>purge_now</c>: run the retention purge across the shared store now (fleet-wide, via the host).</summary>
    Purge,

    /// <summary><c>fetch_plan</c>: read a plan from a server's LIVE plan cache by plan_handle or sql_handle (via the host).</summary>
    FetchPlan,

    /// <summary><c>execute_actual_plan</c>: RE-EXECUTE a stored query (resolved from the store by the identifier) to
    /// capture its ACTUAL plan with runtime stats (via the host). The one command that writes to a target.</summary>
    ExecuteActualPlan,

    /// <summary><c>fetch_active_queries</c>: read the LIVE running-request DMV snapshot from a server on demand (via
    /// the host, running the shared Active Queries collector's query) and return the rows in result_json. Read-only.</summary>
    FetchActiveQueries,

    /// <summary><c>test_hypothetical_index</c>: plan one stored statement twice on a PostgreSQL target — with and
    /// without a candidate index the planner can see but nothing builds — and report whether it would be used
    /// (#2612). Nothing is executed, nothing is written, and the experiment is undone before the call returns.</summary>
    TestHypotheticalIndex,

    /// <summary>Bad arguments or an unknown command_type — report failed without touching the store.</summary>
    Fail,
}

/// <summary>The resolved (pure) plan for one command — see <see cref="DarlingCommandExecutor.ResolvePlan"/>.</summary>
public sealed record CommandPlan(CommandKind Kind, string? Sql, object?[]? Parameters, string SuccessStatus, string? FailReason);

/// <summary>
/// The worker's callback surface for the two imperative commands that need the LIVE collection loop:
/// <c>snapshot_now</c> (run a server's collectors now) and <c>analyze_now</c> (force an analysis pass now).
/// Kept an interface so the executor's dispatch is testable with a fake host, and so the executor never
/// reaches into the worker's mutable loop state directly.
/// </summary>
public interface IDarlingCommandHost
{
    Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken);

    Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken);

    /// <summary>
    /// <c>purge_now</c>: run the retention purge over the shared store immediately (the daily
    /// <see cref="DarlingRetention.PurgeAsync"/> on demand). Fleet-wide over shared tables, so no target
    /// server; <paramref name="customRetentionDays"/> (from args_json) purges every collector to that horizon
    /// when set, else the configured fleet horizons apply.
    /// </summary>
    Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken);

    /// <summary>
    /// <c>fetch_plan</c>: read an execution plan from the target server's LIVE plan cache (by plan_handle or by
    /// sql_handle + offsets — see <see cref="PlanFetchRequest"/>) on the server's runtime connection, and return
    /// the plan XML (or a "not in cache" / "not connected" outcome). Read-only, like the other worker-delegated
    /// commands but reaching the target SQL Server rather than the store.
    /// </summary>
    Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// <c>execute_actual_plan</c>: RE-EXECUTE a stored query against the target server (SET STATISTICS XML) to
    /// capture its ACTUAL plan with runtime stats, and return the plan XML. The request is an IDENTIFIER ONLY
    /// (<see cref="ActualPlanRequest"/>, the stored-row key) — the host resolves the query text + database +
    /// estimated plan XML from the SERVICE'S OWN store (never from the payload, so no SQL text can ride the
    /// command plane) and re-executes as the server's stored monitoring credential. Unlike every other
    /// worker-delegated command this WRITES to the target (it re-applies any INSERT/UPDATE/DELETE/MERGE), so
    /// the viewer gates it behind informed consent; the permission-denied / timeout cases return a legible
    /// outcome rather than a raw SQL error.
    /// </summary>
    Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Plans one stored PostgreSQL statement with and without a candidate index (#2612).
    ///
    /// <para>Consent-class like <see cref="ExecuteActualPlanAsync"/> in that it makes the product ACT on a
    /// monitored server, and unlike it in what that costs: no statement is executed, nothing is written,
    /// the candidate exists only inside one session, and the session is cleaned before this returns.</para>
    /// </summary>
    Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// <c>fetch_active_queries</c>: read the LIVE running-request DMV snapshot from the target server on demand
    /// (the shared Active Queries collector's query, run on the server's runtime connection) and return the rows
    /// in <c>result_json</c> — the on-demand, live counterpart of the collector's stored hourly snapshot, for the
    /// viewer's Current Active Queries tab. Read-only (<c>sys.dm_exec_requests</c>/<c>sessions</c> + the sql_text /
    /// query_plan DMFs), so — unlike <see cref="ExecuteActualPlanAsync"/> — it is NOT consent-class; it still
    /// reaches the target SQL Server like the other worker-delegated commands, and the not-connected / timeout /
    /// permission cases return a legible outcome rather than a raw SQL error.
    /// </summary>
    Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken);
}
