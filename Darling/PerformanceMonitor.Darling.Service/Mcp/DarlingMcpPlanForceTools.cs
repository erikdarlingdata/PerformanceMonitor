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
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>force_query_store_plan</c> and <c>unforce_query_store_plan</c> (#5632, step 7): the first tools that change a monitored SQL Server.
/// They are the ONLY service code that touches <see cref="SqlServerRemediationExecutor"/> (<c>PlanForceNoWritePathTests</c> pins that, and
/// that the service assembly holds none of the write statements: those live in the Remediation assembly).
/// <para><b>Every call is gated, whatever arm the host serves.</b> <c>mcp.allowWrites</c> never bypasses it: the call needs an
/// <c>agent_action</c>, and the four conditions of <see cref="ServerActionGate"/> (the service's <c>agent_actions_enabled</c>, a Level 3
/// request, that server's <c>force_plan</c> switch, that server's remediation login as an <c>env:</c> or <c>file:</c> reference). A dry run
/// passes the gate too, because it signs in with the remediation login. The tools ship off: no server has the switch on.</para>
/// <para><b>The journal.</b> A write goes through <see cref="InvestigationRequestStore.RunAgentActionAsync"/>, the same journal the Level 2 tools
/// write, so the Agent changes list shows it: <c>action_called</c> with the before state read on the server, then <c>action_done</c> with the
/// state read again, or <c>action_failed</c> with the server's message. Its hooks carry the server write as they carry a store write: the
/// write hook runs the force, and a failure inside it is journaled as <c>action_failed</c> and its transaction rolled back. A refusal that
/// needs no write (the plan is not the query's, or is already in the asked state) is journaled once per request, tool, code and hour.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanForceTools
{
    internal const string ForceToolName = "force_query_store_plan";
    internal const string UnforceToolName = "unforce_query_store_plan";
    internal const string EvictToolName = "evict_cached_plan";
    internal const string KillToolName = "kill_session";

    /// <summary>Refusal codes of this tool; the gate's codes are <see cref="AgentActionCodes"/> and the executor's are <see cref="RemediationResult.Reasons"/>.</summary>
    internal const string PostgresTarget = "postgres_target";
    internal const string NoOwnerSource = "owner_source_unavailable";
    internal const string BadLoginTime = "bad_login_time";
    internal const string ServerClockUnknown = "server_clock_unknown";

    [McpServerTool(Name = "force_query_store_plan"), Description(
        "Agent arm, SQL Server only: forces one Query Store plan for a query on the monitored server (replaces another plan of the query that is forced now). Dry run by default: returns the exact statement and the before state, writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's force_plan switch and remediation login; ships off. " +
        "<<GUIDE>>" +
        "Call get_plan_force_precheck first and weigh its warnings; it only warns. This tool blocks only a call that cannot do anything: plan_not_found (the plan is not among that query's Query Store rows) or plan_already_forced. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_force) with the plan that was forced before and how (forcing_type MANUAL or AUTO on SQL Server 2022 and later, else unknown), so the way back is known. " +
        "After a write, check with get_forced_plans. Nothing in Darling unforces on its own: unforce_query_store_plan is the way back, and a person can undo a force from the Agent changes list. Answers status dry_run, done, refused (reason) or failed, with before, after, statement and journal_id.")]
    public static Task<string> ForceQueryStorePlan(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("The plan_id to force.")] long plan_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and the before state, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, force: true, server_name, database_name, query_id, plan_id, agent_action, dry_run, cancellationToken);

    [McpServerTool(Name = "unforce_query_store_plan"), Description(
        "Agent arm, SQL Server only: removes the forced plan of one Query Store plan on the monitored server. Dry run by default: returns the exact statement and the before state, writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's force_plan switch and remediation login; ships off. " +
        "<<GUIDE>>" +
        "Call get_plan_force_precheck first. This tool blocks only a call that cannot do anything: plan_not_found or plan_not_forced. An agent may unforce any force, including one automatic plan correction or a person placed; the change is journaled in Agent changes (kind plan_unforce) with how that plan was forced, so it can be forced again, and a person can undo it from the Agent changes list. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too. After a write, check with get_forced_plans. Nothing in Darling unforces on its own. Answers status dry_run, done, refused (reason) or failed, with before, after, statement and journal_id.")]
    public static Task<string> UnforceQueryStorePlan(
        NpgsqlDataSource postgres,
        [Description("The database_name from get_query_store_top.")] string database_name,
        [Description("The query_id from get_query_store_top.")] long query_id,
        [Description("The plan_id to unforce.")] long plan_id,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and the before state, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, force: false, server_name, database_name, query_id, plan_id, agent_action, dry_run, cancellationToken);

    [McpServerTool(Name = "evict_cached_plan"), Description(
        "Agent arm, SQL Server only: evicts ONE plan handle from the monitored server's plan cache, so the next run compiles a new plan. Dry run by default: returns the exact statement, what Darling collected for that handle and any warnings, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's evict_plan switch and remediation login (with ALTER SERVER STATE); ships off. " +
        "<<GUIDE>>" +
        "plan_handle is the 0x hex text that get_top_queries_by_cpu and get_procedure_stats return. It evicts every statement of the batch that handle covers; the answer says how many Darling collected. There is no undo: an evicted plan cannot be put back, and the next run compiles a new plan. " +
        "This tool blocks only a call that cannot do anything: bad_plan_handle, evict_unsupported_on_platform (Azure SQL Database), evict_permission_denied (the login lacks ALTER SERVER STATE) or plan_not_cached. A batch of several statements, or a handle Darling never collected, is a warning, not a refusal. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_evict) with the statements Darling collected for the handle. " +
        "After a write, wait for the next query_stats collection (about a minute), then call get_compiles_after_evict with the journal_id to see whether the next compile used the same plan shape. Answers status dry_run, done, refused (reason) or failed, with statement, before, warnings and journal_id.")]
    public static Task<string> EvictCachedPlan(
        NpgsqlDataSource postgres,
        [Description("The plan_handle (0x and hex) from get_top_queries_by_cpu or get_procedure_stats.")] string plan_handle,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and what Darling collected, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunEvictAsync(postgres, ownerSource?.Read(), logger, server_name, plan_handle, agent_action, dry_run, cancellationToken);

    [McpServerTool(Name = "kill_session"), Description(
        "Agent arm: kills ONE session on the monitored SQL Server or PostgreSQL server, after checking in the same batch or statement that it is still the session you named. Dry run by default: runs that check, returns what the server says about the session, and kills nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's kill_session switch and remediation login (SQL Server: with ALTER ANY CONNECTION, which can kill any session on that server; PostgreSQL: with pg_signal_backend and pg_read_all_stats); ships off. Azure SQL Database kill is NOT built. " +
        "<<GUIDE>>" +
        "SQL Server: take session_id and login_time from get_blocking (blocking_session_id and blocking_login_time, UTC, pass it back unchanged). SQL Server reuses a session id after a disconnect, so a kill without the login_time check could hit a new, unrelated session. The check and the KILL run in one batch but are not atomic: a session can end and its id be reused between them. " +
        "PostgreSQL: pass backend_id exactly as get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns it, a string of digits, and leave login_time out. backend_id is the session's start time in epoch seconds followed by its pid in seven digits. The pid and the start second are checked in the SAME statement that calls pg_terminate_backend, so a pid the operating system gave to a new session is never ended; a different session with the same pid and the same start second would need the operating system to wrap its whole pid space inside that second. session_id, if you pass it, must be that pid. A login that cannot signal the session (no pg_signal_backend, a superuser's session, or no pg_read_all_stats to see its start time) is refused before any kill is tried. Only client sessions are ended. " +
        "The tool sets no limit of its own. Whether to kill a running session, an idle one, one with an open transaction, or a login on a protected list is the agent's instruction file's call, not this tool's. " +
        "On SQL Server the answer lists the session's login_name, host_name, program_name, status, open_transaction_count, last_request_end_time and log_bytes_used (no statement text). If log_bytes_used is above 0 the kill rolls the work back: the session keeps its locks until the rollback ends, so do not kill it again, and read get_blocking to see it clear. After a write, rollback says whether a rollback is running and its percent_complete, read once. On PostgreSQL it lists the backend_id, login_name, application_name, client_addr, database, state, backend_type, backend_start, xact_start and state_change (no statement text); ending a session rolls its open transaction back at once. There is no undo. " +
        "Refusals: own_session, session_gone, session_reused (the id now belongs to another session; its login_time or backend_id is returned), not_user_session, kill_permission_denied (SQL Server: the login lacks ALTER ANY CONNECTION), not_permitted (PostgreSQL: the login cannot signal that session; the message names the missing grant), bad_session_id, bad_login_time, bad_backend_id, server_clock_unknown (no clock collected for the server yet) and kill_not_built_on_platform (Azure SQL Database). " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind session_kill). Answers status dry_run, done, refused (reason) or failed, with session, warnings, rollback and journal_id.")]
    public static Task<string> KillSession(
        NpgsqlDataSource postgres,
        [Description("SQL Server: the session id, blocking_session_id from get_blocking (required there). PostgreSQL: leave it out, or pass the pid that backend_id holds.")] int? session_id = null,
        [Description("SQL Server: the session's login time as get_blocking returns it (blocking_login_time, ISO 8601 UTC), required there. It must be the value you read, not a later one. PostgreSQL: leave it out.")] string? login_time = null,
        [Description("PostgreSQL: the session's backend_id exactly as get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns it, a string of digits (required there). SQL Server: leave it out.")] string? backend_id = null,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: run the check and kill nothing. Pass false to kill.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunKillAsync(postgres, ownerSource?.Read(), logger, server_name, session_id, login_time, backend_id, agent_action, dry_run, cancellationToken);

    private static string Answer(string status, string? reason, string message, object? extra = null)
    {
        var node = new JsonObject { ["status"] = status };
        if (reason is not null)
            node["reason"] = reason;
        node["message"] = message;
        if (extra is not null)
            foreach (var property in JsonSerializer.SerializeToNode(extra, McpHelpers.JsonOptions)!.AsObject())
                node[property.Key] = property.Value?.DeepClone();
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// Adds the <c>notice</c> a made change carries (not <c>warnings</c>: evict and kill already answer a list under that key) (#5632, step 13): the store recorded the change late, or could not record it,
    /// and what a later read could not show. A change the server accepted is never answered as an error.
    /// </summary>
    internal static string WithWarnings(string answer, AgentActionResult result, params string?[] more)
    {
        var parts = new List<string>();
        if (result.JournalMissing)
            parts.Add("The change was made on the server, but the store could not record it. The agent changes list shows it as result unknown.");
        else if (result.RecordedLate)
            parts.Add("The change was made on the server. The store recorded it late.");
        foreach (var extra in more)
            if (extra is not null)
                parts.Add(extra);
        if (parts.Count == 0)
            return answer;
        var node = JsonNode.Parse(answer)!.AsObject();
        node["notice"] = string.Join(" ", parts);
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The tool body, so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, bool force, string? server_name,
        string database_name, long query_id, long plan_id, string? agent_action, bool dry_run, CancellationToken ct)
    {
        var tool = force ? ForceToolName : UnforceToolName;
        var kind = force ? AgentChangeKinds.PlanForce : AgentChangeKinds.PlanUnforce;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct);
            if (error is not null)
                return error;
            if (string.IsNullOrWhiteSpace(database_name))
                return McpHelpers.Refusal("database_name", "database_name is required: the Query Store database the plan is in.");
            if (query_id <= 0 || plan_id <= 0)
                return McpHelpers.Refusal("plan_id", "query_id and plan_id are positive numbers from get_query_store_top.");

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, plan_id.ToString(CultureInfo.InvariantCulture), database_name.Trim());

            /* 1: SQL Server only. A PostgreSQL target is refused with its own reason, before the gate is asked, and the refusal is journaled. */
            var (_, engineKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                var problem = InvestigationToolCore.ParseAgentAction(agent_action, out var parsed);
                if (problem is not null)
                {
                    await store.RecordAgentActionRefusalAsync(null, null, tool, problem.Reason, ct);
                    return DarlingAgentActionGate.RefusalAnswer(problem);
                }

                var refusal = new AgentActionRefusal(PostgresTarget,
                    $"{resolved.ServerName} is a PostgreSQL target. Forcing a plan is SQL Server only; there is no PostgreSQL force.");
                await store.RecordAgentActionRefusalAsync(parsed!.RequestId, parsed.AgentId, tool, refusal.Reason, ct);
                return DarlingAgentActionGate.RefusalAnswer(refusal);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 2: the gate, for a dry run too. */
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.ForcePlan, agent_action, target, ct);
            if (decision.Refusal is { } gateRefusal)
                return DarlingAgentActionGate.RefusalAnswer(gateRefusal);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 3: the server row through the owner data source, and the executor built from it. */
            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.ServerName} has no row in the monitored-server settings.");
            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

            /* 4: the before state, read on the server. */
            var before = await executor.ReadPlanStateAsync(permit, query_id, plan_id, ct);
            if (before.State is not { } beforeState)
                return await NotRunAsync(store, action!, tool, before.Outcome, before.Reason, before.Message, ct);

            if (BlockReason(beforeState, force) is { } block)
            {
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, block.Reason, ct);
                return Answer("refused", block.Reason, block.Message, new { dry_run, before = beforeState });
            }

            var statement = PlanForceStatement.For(beforeState.Database, query_id, plan_id, force);

            /* 5: a dry run runs the executor's own checks and journals nothing. */
            if (dry_run)
            {
                var dry = force
                    ? await executor.ForcePlanAsync(permit, query_id, plan_id, dryRun: true, ct)
                    : await executor.UnforcePlanAsync(permit, query_id, plan_id, dryRun: true, ct);
                return dry.Outcome == RemediationOutcome.Done
                    ? Answer("dry_run", null, dry.Message, new { dry_run = true, statement = dry.Statement ?? statement, before = beforeState })
                    : Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message, new { dry_run = true, statement, before = beforeState });
            }

            /* 6: the write, journaled. */
            PlanForceState? after = null;
            var afterNotRead = false;
            var changed = new ServerChangeSignal();
            try
            {
                var hooks = new AgentActionHooks
                {
                    ServerChange = changed,
                    LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(beforeState)),
                    Write = async (_, _, token) =>
                    {
                        var written = force
                            ? await executor.ForcePlanAsync(permit, query_id, plan_id, dryRun: false, token)
                            : await executor.UnforcePlanAsync(permit, query_id, plan_id, dryRun: false, token);
                        /* The executor says the server accepted the write: from here the store's journal never says failed (#5632, step 13). */
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement, before = beforeState }));
                        /* The read after the write is a warning when it fails, not an error for a change that was made (#5632, step 13). */
                        try
                        {
                            var read = await executor.ReadPlanStateAsync(permit, query_id, plan_id, token);
                            after = read.State;
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            logger.LogWarning("The state read after plan {PlanId} was {Verb} for query {QueryId} failed: {Message}", plan_id, force ? "forced" : "unforced", query_id, ex.Message);
                        }

                        afterNotRead = after is null;
                        return JsonSerializer.Serialize(new { statement, state = after, message = written.Message });
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, kind, action!, target), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return WithWarnings(Answer("done", null,
                    $"Plan {plan_id} was {(force ? "forced" : "unforced")} for query {query_id} in {beforeState.Database}. Check with get_forced_plans.",
                    new { dry_run = false, statement, before = beforeState, after, journal_id = result.CalledId }),
                    result, afterNotRead ? "The state after the change was not read; check with get_forced_plans." : null);
            }
            catch (PlanForceWriteException ex)
            {
                return ex.Answer;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    /// <summary>The evict tool body (#5632, step 10), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunEvictAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name,
        string? plan_handle, string? agent_action, bool dry_run, CancellationToken ct)
    {
        const string tool = EvictToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);

            /* 1: a handle that does not parse can do nothing. Refused before the gate, and journaled as a block is. */
            if (!PlanEvictStatement.TryNormalize(plan_handle, out var handle))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadHandle,
                    "plan_handle must be 0x followed by hex digits in pairs, at most 64 bytes, as get_top_queries_by_cpu returns it. Nothing was run.", ct);
            }

            /* 2: SQL Server only. */
            var (_, engineKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.ServerName} is a PostgreSQL target. Evicting a plan is SQL Server only; there is no PostgreSQL eviction.", ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id and the journal's target.id are the handle in its one normal form. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, handle, null);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.EvictPlan, agent_action, target, ct);
            if (decision.Refusal is { } gateRefusal)
                return DarlingAgentActionGate.RefusalAnswer(gateRefusal);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.ServerName} has no row in the monitored-server settings.");
            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

            /* 4: the before image comes from the store, not the server: what Darling collected for this handle in the last 24 hours. */
            var (statements, objectName) = await DarlingPlanEvictStore.ReadBeforeAsync(postgres, resolved.ServerId, handle, DateTime.UtcNow, ct);
            var warnings = EvictWarnings(statements.Count);
            var before = new { plan_handle = handle, object_name = objectName, statements };
            var statement = PlanEvictStatement.For(handle);

            /* 5: the executor's own checks run on a dry run and before a write: edition, the grant, and that the handle is cached now. */
            var dry = await executor.EvictPlanAsync(permit, handle, dryRun: true, ct);
            if (dry.Outcome != RemediationOutcome.Done)
            {
                if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, ct);
                return Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
                    new { dry_run, statement = dry.Statement ?? statement, before, warnings });
            }

            if (dry_run)
                return Answer("dry_run", null, dry.Message + " An eviction has no undo.", new { dry_run = true, statement = dry.Statement ?? statement, before, warnings });

            /* 6: the write, journaled. action_called holds the store's before image; action_done holds the statement and the message. */
            try
            {
                var changed = new ServerChangeSignal();
                var hooks = new AgentActionHooks
                {
                    ServerChange = changed,
                    LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(before, McpHelpers.JsonOptions)),
                    Write = async (_, _, token) =>
                    {
                        var written = await executor.EvictPlanAsync(permit, handle, dryRun: false, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement, before, warnings }));
                        return JsonSerializer.Serialize(new { statement = written.Statement ?? statement, message = written.Message }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.PlanEvict, action!, target), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return WithWarnings(Answer("done", null,
                    "The plan was evicted from the plan cache. There is no undo; the next run compiles a new plan. Wait for the next query_stats collection (about a minute), then call get_compiles_after_evict with this journal_id.",
                    new { dry_run = false, statement, before, warnings, journal_id = result.CalledId }), result);
            }
            catch (PlanForceWriteException ex)
            {
                return ex.Answer;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    /// <summary>The kill tool body (#5632, step 12), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunKillAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name,
        int? session_id, string? login_time, string? backend_id, string? agent_action, bool dry_run, CancellationToken ct)
    {
        const string tool = KillToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);

            /* 1: the engine decides how the session is named. A PostgreSQL target has its own arm (pid plus start second from backend_id). */
            var (_, engineKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
                return await RunKillPostgresAsync(owner, logger, store, resolved, session_id, backend_id, agent_action, dry_run, ct);

            /* 2: a session id or a login time that cannot be read can do nothing. Refused before the gate, and journaled as a block is. */
            if (session_id is not > 0)
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadSession,
                    "session_id is a positive whole number, the blocking_session_id get_blocking returns. Nothing was run.", ct);
            }

            var sessionId = session_id.Value;
            if (!TryReadLoginTime(login_time, out var loginUtc))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, BadLoginTime,
                    "login_time is the blocking_login_time get_blocking returns (ISO 8601, UTC), passed back unchanged. Nothing was run.", ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id and the journal's target.id are the session id in decimal. */
            var idText = sessionId.ToString(CultureInfo.InvariantCulture);
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, idText, null);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.KillSession, agent_action, target, ct);
            if (decision.Refusal is { } gateRefusal)
                return DarlingAgentActionGate.RefusalAnswer(gateRefusal);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 4: login_time is the engine's own local clock. Without the server's clock Darling cannot say what the UTC value means there, and it does not guess. */
            var clock = await DarlingServerClockReader.ReadOrNullAsync(postgres, resolved.ServerId, ct);
            if (clock is null)
            {
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, ServerClockUnknown, ct);
                return Answer("refused", ServerClockUnknown,
                    $"Darling has not collected {resolved.ServerName}'s clock yet, so it cannot turn login_time into that server's own time. Nothing was run.");
            }

            var loginLocal = clock.ToServerLocal(loginUtc);

            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.ServerName} has no row in the monitored-server settings.");
            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

            /* 5: one batch decides, for a dry run and before a write: the engine, own session, gone, reused, not a user session, the grant. */
            var dry = await executor.KillSessionAsync(permit, sessionId, loginLocal, dryRun: true, ct);
            if (dry.Outcome != RemediationOutcome.Done)
            {
                if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, ct);
                return KillNotDone(dry, clock, dry_run);
            }

            var before = KillSessionImage(dry.Session, clock);
            var warnings = KillWarnings(dry.Session);
            if (dry_run)
                return Answer("dry_run", null, dry.Message + " There is no undo.", new { dry_run = true, session = before, warnings });

            RemediationResult? killed = null;

            /* 6: the kill, journaled. action_called holds the facts the dry batch read; action_done holds the facts the kill batch read and the rollback read. */
            try
            {
                var changed = new ServerChangeSignal();
                var hooks = new AgentActionHooks
                {
                    ServerChange = changed,
                    LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(new { session = before }, McpHelpers.JsonOptions)),
                    Write = async (_, _, token) =>
                    {
                        var written = await executor.KillSessionAsync(permit, sessionId, loginLocal, dryRun: false, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, KillNotDone(written, clock, dryRun: false));
                        killed = written;
                        return JsonSerializer.Serialize(new
                        {
                            message = written.Message,
                            session = KillSessionImage(written.Session, clock),
                            rollback = KillRollbackImage(written.Rollback),
                        }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.SessionKill, action!, target), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return WithWarnings(Answer("done", null,
                    $"Session {sessionId} was killed. There is no undo."
                    + (killed?.Rollback is { InProgress: true } ? " A rollback is running: the session keeps its locks until it ends. Do not kill it again; read get_blocking to see it clear." : ""),
                    new
                    {
                        dry_run = false,
                        session = KillSessionImage(killed?.Session ?? dry.Session, clock),
                        warnings,
                        rollback = KillRollbackImage(killed?.Rollback),
                        journal_id = result.CalledId,
                    }), result);
            }
            catch (PlanForceWriteException ex)
            {
                return ex.Answer;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    internal const string BadBackendId = "bad_backend_id";

    /// <summary>
    /// The PostgreSQL arm of the kill tool (#5632). The agent names the session by the <c>backend_id</c> the reads return; it is unpacked into the
    /// pid and the start second, and the executor checks both in the SAME statement that calls <c>pg_terminate_backend</c> (the start second is
    /// derived the way the collector derives it). Seconds are enough: for a different backend to carry the same pid and the same start second the
    /// operating system would have to wrap its whole pid space inside that one second. Same gate, same journal kind (<c>session_kill</c>), no undo.
    /// A login that may not signal the session is refused with <c>not_permitted</c> before any kill is tried.
    /// </summary>
    private static async Task<string> RunKillPostgresAsync(NpgsqlDataSource? owner, ILogger logger, InvestigationRequestStore store,
        (int ServerId, string ServerName) resolved, int? session_id, string? backend_id, string? agent_action, bool dry_run, CancellationToken ct)
    {
        const string tool = KillToolName;

        /* 1: a backend id that cannot be read can do nothing. Refused before the gate, and journaled as a block is. */
        if (!PostgresBackendId.TryUnpack(backend_id, out var pid, out var startEpoch))
        {
            return await RefuseBeforeGateAsync(store, agent_action, tool, BadBackendId,
                "backend_id is the value get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns for the session, a string of digits, passed back unchanged. Nothing was run.", ct);
        }

        if (session_id is { } given && given != pid)
        {
            return await RefuseBeforeGateAsync(store, agent_action, tool, BadBackendId,
                "session_id is not the pid inside backend_id. Pass backend_id alone, or the matching pid with it. Nothing was run.", ct);
        }

        if (owner is null)
        {
            return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
        }

        /* 2: the gate, for a dry run too. The permit's Id and the journal's target.id are the pid in decimal. */
        var idText = pid.ToString(CultureInfo.InvariantCulture);
        var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, idText, null);
        var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.KillSession, agent_action, target, ct);
        if (decision.Refusal is { } gateRefusal)
            return DarlingAgentActionGate.RefusalAnswer(gateRefusal);
        var permit = decision.Permit!;
        _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

        var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
        if (row is null)
            return Answer("failed", "server_row_missing", $"{resolved.ServerName} has no row in the monitored-server settings.");
        var executor = new PostgresRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

        /* 3: the read decides, for a dry run and before a write: gone, own session, start time, reused, client session, and what the login may do. */
        var dry = await executor.KillBackendAsync(permit, pid, startEpoch, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, ct);
            return PgKillNotDone(dry, dry_run);
        }

        var before = PgKillSessionImage(dry.PgSession);
        var warnings = PgKillWarnings(dry.PgSession);
        if (dry_run)
            return Answer("dry_run", null, dry.Message + " There is no undo.", new { dry_run = true, session = before, warnings });

        RemediationResult? killed = null;

        /* 4: the kill, journaled. action_called holds the facts the dry read found; action_done holds the facts the kill read. */
        try
        {
            var changed = new ServerChangeSignal();
            var hooks = new AgentActionHooks
            {
                ServerChange = changed,
                LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(new { session = before }, McpHelpers.JsonOptions)),
                Write = async (_, _, token) =>
                {
                    var written = await executor.KillBackendAsync(permit, pid, startEpoch, dryRun: false, token);
                    /* The server took the signal: from here the store's journal never says failed (#5632, step 13). */
                    if (written.Wrote)
                        changed.MarkServerChanged();
                    if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                        throw new PlanForceWriteException(written.Message, PgKillNotDone(written, dryRun: false));
                    killed = written;
                    return JsonSerializer.Serialize(new { message = written.Message, session = PgKillSessionImage(written.PgSession) }, McpHelpers.JsonOptions);
                },
            };
            var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.SessionKill, action!, target), hooks, ct);
            if (result.Refusal is { } refused)
                return DarlingAgentActionGate.RefusalAnswer(refused);
            return WithWarnings(Answer("done", null, $"Backend {pid} was ended. There is no undo.",
                new { dry_run = false, session = PgKillSessionImage(killed?.PgSession ?? dry.PgSession), warnings, journal_id = result.CalledId }), result);
        }
        catch (PlanForceWriteException ex)
        {
            return ex.Answer;
        }
    }

    /// <summary>A refusal or a failure of the PostgreSQL kill. A reused pid shows the session's actual backend id and nothing else about it.</summary>
    private static string PgKillNotDone(RemediationResult result, bool dryRun)
    {
        var status = result.Outcome == RemediationOutcome.Refused ? "refused" : "failed";
        if (result.Reason == RemediationResult.Reasons.SessionReused)
            return Answer(status, result.Reason, result.Message, new { dry_run = dryRun, actual_backend_id = result.PgSession?.BackendId });
        return Answer(status, result.Reason, result.Message, new { dry_run = dryRun, session = PgKillSessionImage(result.PgSession) });
    }

    private static string? PgUtcText(DateTime? utc) =>
        utc is { } value ? DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture) : null;

    /// <summary>The backend's facts as the answer and the journal show them. Times are UTC. No statement text is ever read.</summary>
    internal static object? PgKillSessionImage(PgKillSessionFacts? facts) => facts is null ? null : new
    {
        session_id = facts.Pid,
        backend_id = facts.BackendId,
        backend_start = PgUtcText(facts.BackendStartUtc),
        login_name = facts.UserName,
        application_name = facts.ApplicationName,
        client_addr = facts.ClientAddr,
        database = facts.DatabaseName,
        state = facts.State,
        backend_type = facts.BackendType,
        open_transaction = facts.XactStartUtc is not null,
        xact_start = PgUtcText(facts.XactStartUtc),
        state_change = PgUtcText(facts.StateChangeUtc),
    };

    /// <summary>What a PostgreSQL kill will cost, with no threshold: an open transaction is rolled back. A warning, never a refusal.</summary>
    internal static string[] PgKillWarnings(PgKillSessionFacts? facts) =>
        facts?.XactStartUtc is not null
            ? [$"This session has an open transaction (started {PgUtcText(facts.XactStartUtc)}). Ending the session rolls that transaction back and its uncommitted work is lost."]
            : [];

    private static readonly string[] LoginTimeFormats =
    {
        "yyyy-MM-ddTHH:mm:ssK",
        "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
    };

    /// <summary>
    /// The login time as the agent passes it: the ISO 8601 UTC text <c>get_blocking</c> returns (naive, so no zone means UTC). Fractions are kept
    /// (the engine's <c>datetime</c> has 3.33 ms steps), and nothing is truncated to seconds.
    /// </summary>
    internal static bool TryReadLoginTime(string? text, out DateTime utc)
    {
        utc = default;
        if (string.IsNullOrWhiteSpace(text))
            return false;
        /* Exact formats, not the general parser: 01/02/2026 must not read as two different days on two hosts. These are the shapes get_blocking writes
           ("o" on a naive UTC time, so no zone) and the same with Z or an offset. */
        if (!DateTime.TryParseExact(text.Trim(), LoginTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            return false;
        utc = DateTime.SpecifyKind(parsed, DateTimeKind.Unspecified);
        return true;
    }

    /// <summary>A refusal or a failure of the kill batch. A reused id shows the session's actual login time and nothing else about the session.</summary>
    private static string KillNotDone(RemediationResult result, ServerClock clock, bool dryRun)
    {
        var status = result.Outcome == RemediationOutcome.Refused ? "refused" : "failed";
        if (result.Reason == RemediationResult.Reasons.SessionReused)
            return Answer(status, result.Reason, result.Message, new { dry_run = dryRun, actual_login_time = UtcText(clock, result.Session?.LoginTime) });
        return Answer(status, result.Reason, result.Message, new { dry_run = dryRun, session = result.Session is null ? null : KillSessionImage(result.Session, clock) });
    }

    private static string? UtcText(ServerClock clock, DateTime? local) => local is { } value ? clock.ToUtc(value).ToString("o", CultureInfo.InvariantCulture) : null;

    /// <summary>The session's facts as the answer and the journal show them. Times are UTC, as get_blocking returns them, and the last request's end also in the server's own clock.</summary>
    internal static object? KillSessionImage(KillSessionFacts? facts, ServerClock clock) => facts is null ? null : new
    {
        session_id = facts.SessionId,
        login_time = UtcText(clock, facts.LoginTime),
        login_name = facts.LoginName,
        host_name = facts.HostName,
        program_name = facts.ProgramName,
        status = facts.Status,
        open_transaction_count = facts.OpenTransactionCount,
        last_request_end_time = UtcText(clock, facts.LastRequestEndTime),
        last_request_end_time_server_local = facts.LastRequestEndTime?.ToString("o", CultureInfo.InvariantCulture),
        log_bytes_used = facts.LogBytesUsed,
    };

    private static object? KillRollbackImage(KillRollback? rollback) =>
        rollback is null ? null : new { in_progress = rollback.InProgress, percent_complete = rollback.PercentComplete };

    /// <summary>What a kill will cost, with no threshold: any log used means a rollback. A warning, never a refusal.</summary>
    internal static string[] KillWarnings(KillSessionFacts? facts) =>
        facts?.LogBytesUsed is > 0
            ? [$"This session has used {facts.LogBytesUsed} bytes of transaction log. Killing it rolls that work back: it keeps its locks until the rollback ends, and the rollback can block others for as long as the work took. Do not kill it again; read get_blocking to see it clear."]
            : [];

    /// <summary>What a person should know before the eviction runs. A warning, never a refusal.</summary>
    internal static string[] EvictWarnings(int statementCount) => statementCount switch
    {
        0 => ["Darling has not collected this handle in the last 24 hours, so get_compiles_after_evict cannot follow it. The eviction still runs."],
        _ => [$"This handle covers {(statementCount >= DarlingPlanEvictStore.MaxStatements ? "at least " : "")}{statementCount} statement{(statementCount == 1 ? "" : "s")} Darling collected. Evicting it evicts every statement in that batch."],
    };

    /// <summary>A refusal that comes before the gate (a bad handle, a PostgreSQL target): journaled with the request when the argument parses, and without it when it does not.</summary>
    private static async Task<string> RefuseBeforeGateAsync(InvestigationRequestStore store, string? agentAction, string tool, string code, string message, CancellationToken ct)
    {
        var problem = InvestigationToolCore.ParseAgentAction(agentAction, out var parsed);
        if (problem is not null)
        {
            await store.RecordAgentActionRefusalAsync(null, null, tool, problem.Reason, ct);
            return DarlingAgentActionGate.RefusalAnswer(problem);
        }

        var refusal = new AgentActionRefusal(code, message);
        await store.RecordAgentActionRefusalAsync(parsed!.RequestId, parsed.AgentId, tool, refusal.Reason, ct);
        return DarlingAgentActionGate.RefusalAnswer(refusal);
    }

    /// <summary>
    /// A person's undo of an agent's plan force or unforce (#5632, step 9), for the web route and the Viewer's <c>undo_plan_change</c> command: one
    /// method, so both run the same code. Its signature names only Storage, Common and Npgsql types, so the callers never name the remediation assembly.
    /// <para><b>The server state decides.</b> It reads the query's state on the remediation connection first and decides from that, not from the
    /// journal's result: an undo of a force of plan N puts plan M (the plan forced before) back when N is still forced, or unforces N when none was
    /// forced before; an undo of an unforce of N forces N again when no plan of the query is forced. Anything else is refused,
    /// <c>changed_since</c> when another change was made since, <c>nothing_to_undo</c> when the server shows the change never took effect.</para>
    /// <para><b>Show, then confirm.</b> Without <paramref name="confirm"/> it returns the exact statement and the state it read, and writes and
    /// journals nothing. With it, and with <paramref name="shownStatement"/> equal to the statement it would run now, it writes and journals
    /// <c>action_undone</c> under the same advisory lock the store kinds' undo takes. A confirm without the shown statement writes nothing and
    /// answers the confirm step again, so no caller can write a statement a person was not shown.</para>
    /// <para><b>Gate.</b> The two server-side conditions only (that server's <c>force_plan</c> switch and an <c>env:</c> or <c>file:</c> remediation login),
    /// through <see cref="AgentChangeUndo.PlanUndoSession.AuthorizeAsync"/>; the agent-only conditions are not asked of a person. A refusal is journaled
    /// with the person as the actor.</para>
    /// </summary>
    internal static async Task<PlanUndoAnswer> UndoPlanChangeAsync(AgentChangeUndo changes, NpgsqlDataSource? owner, ILogger? logger, long calledId, string login,
        bool confirm, string? shownStatement, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(changes);
        logger ??= NullLogger.Instance;
        PlanUndoBegin begin;
        try
        {
            begin = await changes.BeginPlanUndoAsync(calledId, login, ct);
        }
        catch (ArgumentException)
        {
            return PlanUndoAnswer.Refused("bad_login", "The signed-in name cannot be recorded as the person who undid this change.");
        }

        if (begin.Session is not { } session)
        {
            var refusal = begin.Refusal!;
            return PlanUndoAnswer.Refused(refusal.Code ?? "not_undoable", refusal.Words);
        }

        await using (session)
        {
            try
            {
                var answer = await DecideAndRunAsync(session, owner, logger, confirm, shownStatement, ct);
                await session.CommitAsync(ct);
                return answer;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("Undo of plan change {CalledId} failed: {Message}", calledId, ex.Message);
                throw;
            }
        }
    }

    private static async Task<PlanUndoAnswer> DecideAndRunAsync(AgentChangeUndo.PlanUndoSession session, NpgsqlDataSource? owner, ILogger logger,
        bool confirm, string? shownStatement, CancellationToken ct)
    {
        var facts = session.Facts;
        if (owner is null)
            return PlanUndoAnswer.Refused(NoOwnerSource, "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly.");

        /* The permit for the plan the change named: it reads the state, and it is the plan an unforce or a no-prior-force undo writes on. */
        var decision = await session.AuthorizeAsync(facts.PlanId, owner, ct);
        if (decision.Refusal is { } gateRefusal)
            return PlanUndoAnswer.Refused(gateRefusal.Reason, gateRefusal.Message);
        var permit = decision.Permit!;

        var row = await ReadServerRowAsync(owner, facts.ServerId, ct);
        if (row is null)
            return PlanUndoAnswer.Refused("server_row_missing", $"{facts.ServerName} has no row in the monitored-server settings.");
        var executor = new SqlServerRemediationExecutor(facts.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

        var read = await executor.ReadPlanStateAsync(permit, facts.QueryId, facts.PlanId, ct);
        if (read.State is not { } state)
        {
            if (read.Outcome == RemediationOutcome.Refused && read.Reason is not null)
                await session.RecordRefusalAsync(read.Reason, ct);
            return new PlanUndoAnswer(read.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, read.Reason, read.Message);
        }

        var stateNode = JsonSerializer.SerializeToNode(state);
        var plan = PlanUndoDecision.Decide(facts, state);
        if (plan.Refusal is { } refused)
        {
            await session.RecordRefusalAsync(refused.Reason, ct);
            return new PlanUndoAnswer(PlanUndoAnswer.RefusedStatus, refused.Reason, refused.Message, null, stateNode);
        }

        var step = plan.Step!;
        var statement = PlanForceStatement.For(state.Database, facts.QueryId, step.PlanId, step.Force);
        var stepPermit = step.PlanId == facts.PlanId ? permit : null;
        if (stepPermit is null)
        {
            var other = await session.AuthorizeAsync(step.PlanId, owner, ct);
            if (other.Refusal is { } otherRefusal)
                return PlanUndoAnswer.Refused(otherRefusal.Reason, otherRefusal.Message);
            stepPermit = other.Permit!;
        }

        /* The executor's own dry run, for the confirm step and before a write: it checks the plan is there and in the state the statement needs. */
        var dry = step.Force
            ? await executor.ForcePlanAsync(stepPermit, facts.QueryId, step.PlanId, dryRun: true, ct)
            : await executor.UnforcePlanAsync(stepPermit, facts.QueryId, step.PlanId, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await session.RecordRefusalAsync(dry.Reason, ct);
            return new PlanUndoAnswer(dry.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, dry.Reason, dry.Message, statement, stateNode);
        }

        statement = dry.Statement ?? statement;
        if (!confirm || shownStatement is null || !string.Equals(shownStatement, statement, StringComparison.Ordinal))
        {
            var lead = !confirm ? ""
                : shownStatement is null ? "A confirm must carry the statement that was shown, so nothing was written. Review this statement and confirm it. "
                : "The server's state changed since the statement was shown, so nothing was written. Review the new statement and confirm again. ";
            return new PlanUndoAnswer(PlanUndoAnswer.Confirm, null, lead + step.Words, statement, stateNode, null, step.RestoredAs);
        }

        var written = step.Force
            ? await executor.ForcePlanAsync(stepPermit, facts.QueryId, step.PlanId, dryRun: false, ct)
            : await executor.UnforcePlanAsync(stepPermit, facts.QueryId, step.PlanId, dryRun: false, ct);
        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
        {
            await session.RecordRefusalAsync(written.Reason ?? "write_failed", ct);
            return new PlanUndoAnswer(written.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, written.Reason, written.Message, statement, stateNode);
        }

        var after = await executor.ReadPlanStateAsync(stepPermit, facts.QueryId, step.PlanId, ct);
        var afterNode = after.State is null ? null : JsonSerializer.SerializeToNode(after.State);
        await session.JournalUndoneAsync(statement, stateNode, afterNode, step.RestoredAs, ct);
        return new PlanUndoAnswer(PlanUndoAnswer.Undone, null, step.DoneWords, statement, stateNode, afterNode, step.RestoredAs, facts.CalledId, session.Actor);
    }

    /// <summary>The only two blocks, as Erik ruled: the call cannot do anything. Everything else is the pre-check's warning.</summary>
    internal static AgentActionRefusal? BlockReason(PlanForceState state, bool force)
    {
        if (state.RequestedPlan is null)
            return new AgentActionRefusal(RemediationResult.Reasons.PlanNotFound,
                $"Plan {state.PlanId} is not among query {state.QueryId}'s rows in {state.Database}'s Query Store on the server. Nothing was written.");
        if (force && state.RequestedPlan.IsForced)
            return new AgentActionRefusal(RemediationResult.Reasons.AlreadyForced,
                $"Plan {state.PlanId} is already forced for query {state.QueryId}. Nothing was written.");
        if (!force && !state.RequestedPlan.IsForced)
            return new AgentActionRefusal(RemediationResult.Reasons.NotForced,
                $"Plan {state.PlanId} is not forced for query {state.QueryId}. Nothing was written.");
        return null;
    }

    /// <summary>The before read could not finish: a refusal of the remediation code (no login, a slot that cannot be read) is journaled once an hour; a failure is only answered.</summary>
    private static async Task<string> NotRunAsync(InvestigationRequestStore store, AgentAction action, string tool, RemediationOutcome outcome,
        string? reason, string message, CancellationToken ct)
    {
        if (outcome == RemediationOutcome.Refused && reason is not null)
            await store.RecordAgentActionRefusalAsync(action.RequestId, action.AgentId, tool, reason, ct);
        return Answer(outcome == RemediationOutcome.Refused ? "refused" : "failed", reason, message);
    }

    private sealed record ServerRow(RemediationServer Server, string? UserName, string? SecretSlot);

    internal const string ServerRowSql = @"
SELECT host, database, encrypt_mode, trust_server_certificate, multi_subnet_failover, remediation_username, remediation_encrypted_password, port
FROM config.config_monitored_servers
WHERE server_id = $1";

    /// <summary><paramref name="postgres"/> is the store owner's data source here, not the mcp role's: the secret column is not readable by that role.</summary>
    private static async Task<ServerRow?> ReadServerRowAsync(NpgsqlDataSource postgres, int serverId, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(ServerRowSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        return new ServerRow(
            new RemediationServer(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetBoolean(3), reader.GetBoolean(4), reader.IsDBNull(7) ? 0 : reader.GetInt32(7)),
            reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6));
    }

    /// <summary>One statement an undo would run, and the words around it.</summary>
    internal sealed record UndoStep(bool Force, long PlanId, string? RestoredAs, string Words, string DoneWords);

    /// <summary>What <see cref="PlanUndoDecision.Decide"/> decided: a step to run, or a refusal.</summary>
    internal sealed record UndoPlan(UndoStep? Step, AgentActionRefusal? Refusal);

    /// <summary>The decision from the state the server reported (rule 3), pure so the table can be tested without a server.</summary>
    internal static class PlanUndoDecision
    {
        internal const string ChangedSince = "changed_since";
        internal const string NothingToUndo = "nothing_to_undo";

        internal static UndoPlan Decide(PlanChangeFacts facts, PlanForceState state)
        {
            var forcedNow = state.ForcedPlan?.PlanId;
            var done = facts.Result == AgentChangeResult.Done;
            var where = $"query {facts.QueryId} in {state.Database}";
            var now = forcedNow is long k ? $"plan {k} is forced now" : "no plan is forced now";
            var neverHappened = done ? "" : " The server shows the change never took effect, so there is nothing to undo.";
            if (facts.IsForce)
            {
                if (forcedNow == facts.PlanId)
                {
                    if (facts.BeforeForcedPlanId is long m && m != facts.PlanId)
                    {
                        var auto = string.Equals(facts.BeforeForcingType, "AUTO", StringComparison.OrdinalIgnoreCase);
                        return new UndoPlan(new UndoStep(true, m, auto ? "manual" : null,
                            $"Undo would force plan {m} again for {where} (it replaces plan {facts.PlanId}, which is forced now)." + (auto ? AutoWords(m) : ""),
                            $"Plan {m} is forced again for {where}; plan {facts.PlanId} is no longer forced." + (auto ? AutoWords(m) : "")), null);
                    }

                    return new UndoPlan(new UndoStep(false, facts.PlanId, null,
                        $"Undo would unforce plan {facts.PlanId} for {where}; no plan was forced before the agent's change.",
                        $"Plan {facts.PlanId} is unforced for {where}."), null);
                }

                if (!done && forcedNow == facts.BeforeForcedPlanId)
                    return Refuse(NothingToUndo, $"Nothing to undo: plan {facts.PlanId} was not forced for {where} ({now}).{neverHappened}");
                return Refuse(ChangedSince, $"Not undone: the plan state changed since the agent's force of plan {facts.PlanId} ({now}), so it was left as it is. Review it and change it by hand if you still want it back.");
            }

            /* An unforce of plan N: the way back is to force N again, only when no plan of the query is forced. */
            if (forcedNow is null)
            {
                var auto = string.Equals(facts.BeforeForcingType, "AUTO", StringComparison.OrdinalIgnoreCase);
                return new UndoPlan(new UndoStep(true, facts.PlanId, auto ? "manual" : null,
                    $"Undo would force plan {facts.PlanId} again for {where}; no plan is forced now." + (auto ? AutoWords(facts.PlanId) : ""),
                    $"Plan {facts.PlanId} is forced again for {where}." + (auto ? AutoWords(facts.PlanId) : "")), null);
            }

            if (!done && forcedNow == facts.PlanId)
                return Refuse(NothingToUndo, $"Nothing to undo: plan {facts.PlanId} is still forced for {where}.{neverHappened}");
            return Refuse(ChangedSince, $"Not undone: {now} for {where}, so the agent's unforce of plan {facts.PlanId} was left as it is. Review it and change it by hand if you still want plan {facts.PlanId} back.");
        }

        private static string AutoWords(long planId) =>
            $" Automatic plan correction placed plan {planId}'s force; Query Store cannot restore that type, so it comes back as a manual force.";

        private static UndoPlan Refuse(string code, string message) => new(null, new AgentActionRefusal(code, message));
    }

    /// <summary>A failure inside the write hook before the server accepted anything: the gate journals <c>action_failed</c> with <see cref="Exception.Message"/> (the server's own words, already scrubbed of the secret) and the tool answers <see cref="Answer"/>.</summary>
    private sealed class PlanForceWriteException(string message, string answer) : Exception(message)
    {
        internal string Answer { get; } = answer;
    }
}
