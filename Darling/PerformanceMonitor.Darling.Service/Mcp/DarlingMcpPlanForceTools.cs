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
using System.Linq;
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
/// needs no write (the plan is not the query's, or is already in the asked state) is journaled once per request, verified token, tool, code and hour.</para>
/// </summary>
[McpServerToolType]
public sealed partial class DarlingMcpPlanForceTools
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
        "After a write, check with get_forced_plans. Nothing in Darling unforces on its own: unforce_query_store_plan is the way back, and a person can undo a force from the Agent changes list. Answers status dry_run, done, refused (reason) or failed, with before, after, statement and journal_id. A done answer also carries undo: the tool and arguments that put the change back (undoing a force that replaced an AUTO force says that forcing it again makes it manual).")]
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
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, force: true, server_name, database_name, query_id, plan_id, agent_action, dry_run, cancellationToken, served?.Policy.AgentTokenName, namedAgentNames: served?.NamedAgentNames);

    [McpServerTool(Name = "unforce_query_store_plan"), Description(
        "Agent arm, SQL Server only: removes the forced plan of one Query Store plan on the monitored server. Dry run by default: returns the exact statement and the before state, writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's force_plan switch and remediation login; ships off. " +
        "<<GUIDE>>" +
        "Call get_plan_force_precheck first. This tool blocks only a call that cannot do anything: plan_not_found or plan_not_forced. An agent may unforce any force, including one automatic plan correction or a person placed; the change is journaled in Agent changes (kind plan_unforce) with how that plan was forced, so it can be forced again, and a person can undo it from the Agent changes list. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too. After a write, check with get_forced_plans. Nothing in Darling unforces on its own. Answers status dry_run, done, refused (reason) or failed, with before, after, statement and journal_id. A done answer also carries undo: force_query_store_plan with the same plan (a plan that automatic plan correction forced says that forcing it again makes it manual).")]
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
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, force: false, server_name, database_name, query_id, plan_id, agent_action, dry_run, cancellationToken, served?.Policy.AgentTokenName, namedAgentNames: served?.NamedAgentNames);

    [McpServerTool(Name = "evict_cached_plan"), Description(
        "Agent arm, SQL Server only: evicts ONE plan handle from the monitored server's plan cache, so the next run compiles a new plan. Dry run by default: returns the exact statement, what Darling collected for that handle and any warnings, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's evict_plan switch and remediation login (with ALTER SERVER STATE); ships off. " +
        "<<GUIDE>>" +
        "plan_handle is the 0x hex text that get_top_queries_by_cpu and get_procedure_stats return. It evicts every statement of the batch that handle covers; the answer says how many Darling collected. There is no undo: an evicted plan cannot be put back, and the next run compiles a new plan, so a done answer carries undo null. " +
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
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunEvictAsync(postgres, ownerSource?.Read(), logger, server_name, plan_handle, agent_action, dry_run, cancellationToken, served?.Policy.AgentTokenName, namedAgentNames: served?.NamedAgentNames);

    [McpServerTool(Name = "kill_session"), Description(
        "Agent arm: kills ONE session on the monitored SQL Server or PostgreSQL server, after checking in the same batch or statement that it is still the session you named. Dry run by default: returns what the server says about the session and kills nothing; a write needs dry_run=false. Needs agent_action at Level 3, that server's kill_session switch (ships off) and a remediation login with kill rights (grants in the guide). Refuses Darling's own sessions, session_gone and session_reused. " +
        "<<GUIDE>>" +
        "Grants the remediation login needs. SQL Server: ALTER ANY CONNECTION, which can kill any session on that server. Azure SQL Database: a user in each database it may kill in, with VIEW DATABASE STATE and KILL DATABASE CONNECTION. PostgreSQL: pg_signal_backend and pg_read_all_stats. " +
        "SQL Server: take session_id and login_time from get_blocking (blocking_session_id and blocking_login_time, UTC, pass it back unchanged). SQL Server reuses a session id after a disconnect, so a kill without the login_time check could hit a new, unrelated session. The check and the KILL run in one batch but are not atomic: a session can end and its id be reused between them. " +
        "Azure SQL Database: also pass database_name, the database_name of the get_blocking row you read. A session is reached only from inside its own database, so the kill connects there (never master), read-write, and checks that the session's database is that one. The database must be the server's registered database or one Darling collected a blocking row from in the last day, and not excluded. Hyperscale: the kill reaches only the primary; a session on a named replica or a read-only secondary answers session_gone. " +
        "PostgreSQL: pass backend_id exactly as get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns it, a string of digits, and session_id as the pid the same read returned (root_pid or pid); leave login_time out. backend_id is the session's start time in epoch seconds followed by its pid in seven digits, and session_id must agree with it. The pid and the start second are checked in the SAME statement that calls pg_terminate_backend, so a pid given to a session that started in another second is never ended. On Windows a pid can come back within about a second, so there a new session can share the pid and the start second. A login that cannot signal the session (no pg_signal_backend, a superuser's session, or no pg_read_all_stats to see its start time) is refused before any kill is tried. Only client sessions are ended. " +
        "The tool sets no limit of its own. Whether to kill a running session, an idle one, one with an open transaction, or a login on a protected list is the agent's instruction file's call, not this tool's. " +
        "On SQL Server the answer lists the session's login_name, host_name, program_name, status, open_transaction_count, last_request_end_time and log_bytes_used (no statement text). If log_bytes_used is above 0 the kill rolls the work back: the session keeps its locks until the rollback ends, so do not kill it again, and read get_blocking to see it clear. After a write, rollback says whether a rollback is running and its percent_complete, read once. On PostgreSQL it lists the backend_id, login_name, application_name, client_addr, database, state, wait_event, backend_type, backend_start, xact_start and state_change (no statement text); ending a session rolls its open transaction back, except a session waiting for a synchronous standby, which has already committed. After a write, still_there says whether the backend was still there when read right after the signal. There is no undo. " +
        "Refusals: Azure SQL Database only: database_required, database_excluded, database_not_monitored, wrong_database, not_primary, view_state_permission_denied, session_in_other_database, database_unavailable (paused or resuming serverless, nothing sent) fails. Every engine: tool_login_session (a session of Darling itself: its program or application name starts with PerformanceMonitorDarling AND its login is the monitoring or remediation login, or any session of the remediation login), own_session, session_gone, session_reused (the id now belongs to another session; nothing about that session is returned), backend_id_has_no_session_start (PostgreSQL: the id holds the server's start time because the monitoring login lacks pg_read_all_stats), not_user_session, kill_permission_denied (SQL Server: the login lacks ALTER ANY CONNECTION), not_permitted (PostgreSQL: the login cannot signal that session; the message names the missing grant), bad_session_id, bad_login_time, bad_backend_id, server_clock_unknown (no clock collected for the server yet), sign_in_not_strong_enough (PostgreSQL: the server does not offer SCRAM-SHA-256 sign-in over TLS, which only a person can change on the server) and search_path_not_pinned (PostgreSQL: the connection did not get the pinned search_path, as when a connection pooler drops the startup options; point the server entry at the server directly). " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind session_kill). Answers status dry_run, done, refused (reason), failed, or outcome_unknown (the kill was sent and its reply was lost, so the session may have ended; read get_blocking or get_pg_session_states before trying again), with session, warnings, rollback and journal_id.")]
    public static Task<string> KillSession(
        NpgsqlDataSource postgres,
        [Description("SQL Server: the session id, blocking_session_id from get_blocking. PostgreSQL: the pid (root_pid from get_pg_blocking, pid from get_pg_session_states), matching backend_id. Required on both.")] int? session_id = null,
        [Description("SQL Server: the session's login time as get_blocking returns it (blocking_login_time, ISO 8601 UTC), required there. It must be the value you read, not a later one. PostgreSQL: leave it out.")] string? login_time = null,
        [Description("PostgreSQL: the session's backend_id exactly as get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns it, a string of digits (required there). SQL Server: leave it out.")] string? backend_id = null,
        [Description("Azure SQL Database: the database the session is in, the database_name of the get_blocking row (required there). Other engines: leave it out.")] string? database_name = null,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: run the check and kill nothing. Pass false to kill.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunKillAsync(postgres, ownerSource?.Read(), logger, server_name, session_id, login_time, backend_id, agent_action, dry_run, cancellationToken, database_name, served?.Policy.AgentTokenName, served?.NamedAgentNames);

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
    /// The gate's refusal as an answer (#6455). A dry run asks the same gate a write does, so a call with no agent_action is refused
    /// as <c>agent_action_required</c> before the per-server switch is read. The message now says why a dry run needs the claim, so
    /// the refusal does not read as if the tool wrongly demanded one for a call that writes nothing, and it says what the server's
    /// switch for this action reads now (<paramref name="switchState"/>, from <see cref="ReadSwitchStateAsync"/>), so the dry run
    /// still shows whether the real call could run.
    /// </summary>
    internal static string GateRefusalAnswer(AgentActionRefusal refusal, bool dryRun, string? switchState = null) =>
        dryRun && refusal.Reason == AgentActionCodes.Required
            ? DarlingAgentActionGate.RefusalAnswer(refusal with
            {
                Message = refusal.Message + " A dry run needs it too: it signs in to the monitored server with the remediation login and reads that server's action switch, so it is gated like a write, although it writes nothing."
                    + (switchState is null ? string.Empty : " " + switchState),
            })
            : DarlingAgentActionGate.RefusalAnswer(refusal);

    /// <summary>
    /// The refusal answer for a gate refusal, reading the server's switch for the action first when a dry run was refused for want of the
    /// claim (#6455). The read is <see cref="ServerActionSwitches.ListAsync"/>, the one the switch screens use; it needs no claim. A read
    /// that fails leaves the note out: the refusal itself must still be answered.
    /// </summary>
    private static async Task<string> GateRefusalAnswerAsync(
        AgentActionRefusal refusal, bool dryRun, NpgsqlDataSource owner, ILogger? logger, int serverId, string serverName, string serverAction, CancellationToken ct) =>
        GateRefusalAnswer(refusal, dryRun,
            dryRun && refusal.Reason == AgentActionCodes.Required
                ? await ReadSwitchStateAsync(owner, logger, serverId, serverName, serverAction, ct)
                : null);

    /// <summary>One sentence saying whether the server's switch for the action is on or off, or null when it cannot be read.</summary>
    internal static async Task<string?> ReadSwitchStateAsync(NpgsqlDataSource owner, ILogger? logger, int serverId, string serverName, string serverAction, CancellationToken ct)
    {
        try
        {
            var rows = await ServerActionSwitches.ListAsync(owner, ct);
            var state = rows.FirstOrDefault(row => row.ServerId == serverId)?.Actions.FirstOrDefault(a => string.Equals(a.Action, serverAction, StringComparison.Ordinal));
            if (state is null)
                return null;
            return $"For reference, {ServerActionNames.Label(serverAction)} on {serverName} is currently {(state.Enabled ? "on" : "off")}"
                + (state.Enabled ? "." : $", so the real call would be refused as {AgentActionCodes.SwitchOff} until it is turned on.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Could not read the {Action} switch for server {ServerId} to add it to a dry-run refusal", serverAction, serverId);
            return null;
        }
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
        string database_name, long query_id, long plan_id, string? agent_action, bool dry_run, CancellationToken ct, string? agentTokenName = null,
        Func<DateTime>? utcNow = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        var tool = force ? ForceToolName : UnforceToolName;
        var kind = force ? AgentChangeKinds.PlanForce : AgentChangeKinds.PlanUnforce;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;
            if (string.IsNullOrWhiteSpace(database_name))
                return McpHelpers.Refusal("database_name", "database_name is required: the Query Store database the plan is in.");
            if (query_id <= 0 || plan_id <= 0)
                return McpHelpers.Refusal("plan_id", "query_id and plan_id are positive numbers from get_query_store_top.");

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, utcNow, owner);
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, plan_id.ToString(CultureInfo.InvariantCulture), database_name.Trim());

            /* 1: SQL Server only. A PostgreSQL target is refused with its own reason, before the gate is asked, and the refusal is journaled. */
            var (_, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                var problem = InvestigationToolCore.ParseAgentAction(agent_action, out var parsed);
                if (problem is not null)
                {
                    await store.RecordAgentActionRefusalAsync(DarlingAgentActionGate.LooseRequestId(agent_action),
                        DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, DarlingAgentActionGate.LooseAgentId(agent_action)), tool, problem.Reason, agentTokenName, ct);
                    return DarlingAgentActionGate.RefusalAnswer(problem);
                }

                var refusal = new AgentActionRefusal(PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. Forcing a plan is SQL Server only; there is no PostgreSQL force.");
                await store.RecordAgentActionRefusalAsync(parsed!.RequestId, DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, parsed.AgentId), tool, refusal.Reason, agentTokenName, ct);
                return DarlingAgentActionGate.RefusalAnswer(refusal);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 2: the gate, for a dry run too. */
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.ForcePlan, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, target.ServerId, target.ServerName, ServerActionNames.ForcePlan, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 3: the prepare step: the server row, the excluded database, the before state, the block reason and, on a dry run, the executor's own checks. */
            var prepared = await PrepareForcePlanAsync(owner, logger, permit, resolved.ServerId, resolved.DisplayName, force, database_name.Trim(), query_id, plan_id, dry_run,
                (reason, token) => store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, reason, agentTokenName, token), ct);
            if (prepared.Answer is { } preparedAnswer)
                return preparedAnswer;
            var facts = prepared.Facts!;
            var executor = prepared.Executor!;
            var beforeState = facts.Before;
            var statement = facts.Statement;

            /* 6: the write, journaled. */
            PlanForceState? after = null;
            var afterNotRead = false;
            RemediationResult? unknown = null;
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
                        var written = await WriteForcePlanAsync(executor, permit, facts, token);
                        /* The executor says the server accepted the write: from here the store's journal never says failed (#5632, step 13). */
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome == RemediationOutcome.Unknown)
                        {
                            /* The write was sent and its reply was lost, or the call was cancelled while it ran: neither done nor failed (#5632), as for a kill. */
                            changed.MarkOutcomeUnknown();
                            unknown = written;
                            return null;
                        }

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
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, kind, action!, target, agentTokenName), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                if (result.OutcomeUnknown && unknown is not null)
                {
                    /* A force whose reply was lost may still have taken effect: read the plan again, and when it is forced, journal action_done late so the
                       post-force checks run. A read that fails or shows it not forced leaves the call as it is, with no result row. */
                    if (force && result.CalledId is long unknownCalled)
                        await RecordUnknownForceTookEffectAsync(store, executor, permit, logger, action!, resolved.ServerId, unknownCalled, statement, query_id, plan_id);
                    return Answer("outcome_unknown", null, unknown.Message, new { dry_run = false, statement, before = beforeState, journal_id = result.CalledId });
                }

                return WithWarnings(Answer("done", null,
                    $"Plan {plan_id} was {(force ? "forced" : "unforced")} for query {query_id} in {beforeState.Database}. Check with get_forced_plans.",
                    new { dry_run = false, statement, before = beforeState, after, undo = AgentPlanUndoBuilder.For(force, resolved.ServerName, beforeState.Database, query_id, plan_id, beforeState.ForcedPlan?.PlanId, beforeState.ForcedPlan?.ForcingType), journal_id = result.CalledId }),
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

    /// <summary>How long the read after a force with an unknown outcome may take. It has its own token: the call's may be the one that fired.</summary>
    private static readonly TimeSpan UnknownForceReadTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A forward force was sent and its reply was lost (#5632): the plan may be forced, and a force with no <c>action_done</c> gets no post-force
    /// checks. Reads the plan's state again with a fresh token (on the executor's own connection, which is a fresh one per call) and, when the plan is
    /// forced now, journals <c>action_done</c> marked <c>recorded_late</c>, as for a force the store could not record. Best effort: a read or a
    /// write that fails, or a plan that is not forced, leaves the call with no result row, which reads as "result unknown".
    /// </summary>
    private static async Task RecordUnknownForceTookEffectAsync(InvestigationRequestStore store, SqlServerRemediationExecutor executor, ServerActionPermit permit,
        ILogger logger, AgentAction action, int serverId, long calledId, string statement, long queryId, long planId)
    {
        try
        {
            using var fresh = new CancellationTokenSource(UnknownForceReadTimeout);
            var read = await executor.ReadPlanStateAsync(permit, queryId, planId, fresh.Token);
            if (read.State is not { } state || state.RequestedPlan is not { IsForced: true })
                return;
            var after = JsonSerializer.Serialize(new { statement, state, message = "The reply to the force was lost; the plan was found forced when the server was read again." });
            await store.RecordForceTookEffectAsync(action.RequestId, serverId, action.AgentId, calledId, after,
                "The reply to the force was lost; the plan was found forced when the server was read again.");
        }
        catch (Exception ex)
        {
            logger.LogWarning("The state read after the force of plan {PlanId} for query {QueryId} with an unknown outcome failed: {Message}", planId, queryId, ex.Message);
        }
    }

    /// <summary>The evict tool body (#5632, step 10), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunEvictAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name,
        string? plan_handle, string? agent_action, bool dry_run, CancellationToken ct, string? agentTokenName = null,
        Func<DateTime>? utcNow = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = EvictToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, utcNow, owner);

            /* 1: a handle that does not parse can do nothing. Refused before the gate, and journaled as a block is. */
            if (!PlanEvictStatement.TryNormalize(plan_handle, out var handle))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadHandle,
                    "plan_handle must be 0x followed by hex digits in pairs, at most 64 bytes, as get_top_queries_by_cpu returns it. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            /* 2: SQL Server only. */
            var (_, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. Evicting a plan is SQL Server only; there is no PostgreSQL eviction.", agentTokenName, namedAgentNames, ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id and the journal's target.id are the handle in its one normal form. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, handle, null);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.EvictPlan, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, target.ServerId, target.ServerName, ServerActionNames.EvictPlan, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 4: the prepare step: the server row, the before image from the store (what Darling collected for this handle in the last 24 hours), and the executor's own checks
               (edition, the grant, and that the handle is cached now). */
            var prepared = await PrepareEvictPlanAsync(postgres, owner, logger, permit, resolved.ServerId, resolved.DisplayName, handle, dry_run,
                (reason, token) => store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, reason, agentTokenName, token), ct);
            if (prepared.Answer is { } preparedAnswer)
                return preparedAnswer;
            var facts = prepared.Facts!;
            var executor = prepared.Executor!;
            var before = facts.Before;
            var warnings = facts.Warnings;
            var statement = facts.Statement;

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
                        var written = await WriteEvictPlanAsync(executor, permit, facts, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement, before, warnings }));
                        return JsonSerializer.Serialize(new { statement = written.Statement ?? statement, message = written.Message }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.PlanEvict, action!, target, agentTokenName), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return WithWarnings(Answer("done", null,
                    "The plan was evicted from the plan cache. There is nothing to put back: the next run compiles a new plan. Wait for the next query_stats collection (about a minute), then call get_compiles_after_evict with this journal_id.",
                    new { dry_run = false, statement, before, warnings, undo = (AgentPlanUndo?)null, journal_id = result.CalledId }), result);
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
        int? session_id, string? login_time, string? backend_id, string? agent_action, bool dry_run, CancellationToken ct, string? database_name = null, string? agentTokenName = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = KillToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: owner);

            /* 1: the engine decides how the session is named. A PostgreSQL target has its own arm (pid plus start second from backend_id). */
            var (engineEdition, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
                return await RunKillPostgresAsync(owner, logger, store, resolved, session_id, backend_id, agent_action, dry_run, ct, agentTokenName, namedAgentNames);

            /* An Azure SQL Database scopes KILL and the session views to the connected database (#5632): the kill connects to the session's
               own database, which the agent names. The batch checks the engine again, so a registry that has not recorded it yet is refused there. */
            var azure = engineEdition == 5; /* SERVERPROPERTY('EngineEdition') of an Azure SQL Database, as the registry stores it */

            /* 2: a session id or a login time that cannot be read can do nothing. Refused before the gate, and journaled as a block is. */
            if (session_id is not > 0)
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadSession,
                    "session_id is a positive whole number, the blocking_session_id get_blocking returns. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            var sessionId = session_id.Value;
            if (!TryReadLoginTime(login_time, out var loginUtc))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, BadLoginTime,
                    "login_time is the blocking_login_time get_blocking returns (ISO 8601, UTC), passed back unchanged. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id and the journal's target.id are the session id in decimal. */
            var idText = sessionId.ToString(CultureInfo.InvariantCulture);
            string? targetDatabase = null;
            if (azure)
            {
                /* The database is part of what the permit and the journal name: a permit for session 87 in one database never reaches session 87 in
                   another. Its shape is checked before the gate; the registry and the collected data are checked after it (#5632). */
                targetDatabase = AzureDatabaseShape(database_name);
                if (targetDatabase is null)
                    return await RefuseBeforeGateAsync(store, agent_action, tool, AzureDatabaseRequired, AzureDatabaseRequiredMessage, agentTokenName, namedAgentNames, ct);
            }

            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, idText, targetDatabase);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.KillSession, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, target.ServerId, target.ServerName, ServerActionNames.KillSession, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 4: login_time is the engine's own local clock. Without the server's clock Darling cannot say what the UTC value means there, and it does not guess. */
            var clock = await DarlingServerClockReader.ReadOrNullAsync(postgres, resolved.ServerId, ct);
            if (clock is null)
            {
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, ServerClockUnknown, agentTokenName, ct);
                return Answer("refused", ServerClockUnknown,
                    $"Darling has not collected {resolved.DisplayName}'s clock yet, so it cannot turn login_time into that server's own time. Nothing was run.");
            }

            var loginLocal = clock.ToServerLocal(loginUtc);

            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.DisplayName} has no row in the monitored-server settings.");
            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger, row.MonitoringUserName);

            /* 4b: Azure SQL Database. The database is never free-form: it is checked against the registry entry and the collected data. */
            if (azure)
            {
                var refusal = await AzureKillDatabaseAsync(postgres, resolved.ServerId, row, targetDatabase!, ct);
                if (refusal is not null)
                {
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, refusal.Reason, agentTokenName, ct);
                    return DarlingAgentActionGate.RefusalAnswer(refusal);
                }
            }

            /* The Azure kill reads its database from the permit, the one the gate was asked about. */
            Task<RemediationResult> Kill(bool dryRun, CancellationToken token) => !azure
                ? executor.KillSessionAsync(permit, sessionId, loginLocal, dryRun, token)
                : executor.KillAzureSessionAsync(permit, sessionId, loginLocal, dryRun, token);

            /* 5: one batch decides, for a dry run and before a write: the engine, own session, gone, reused, not a user session, the grant. */
            var dry = await Kill(true, ct);
            if (dry.Outcome != RemediationOutcome.Done)
            {
                if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, agentTokenName, ct);
                return KillNotDone(dry, clock, dry_run);
            }

            var before = KillSessionImage(dry.Session, clock);
            var warnings = KillWarnings(dry.Session);
            if (dry_run)
                return Answer("dry_run", null, dry.Message + " There is no undo.", new { dry_run = true, session = before, warnings });

            RemediationResult? killed = null;
            RemediationResult? unknown = null;

            /* 6: the kill, journaled. action_called holds the facts the dry batch read; action_done holds the facts the kill batch read and the rollback read.
               A kill whose reply was lost leaves action_called alone, which reads "result unknown". */
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
                        var written = await Kill(false, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome == RemediationOutcome.Unknown)
                        {
                            /* The batch was sent and its reply lost: neither done nor failed. */
                            changed.MarkOutcomeUnknown();
                            unknown = written;
                            return null;
                        }

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
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.SessionKill, action!, target, agentTokenName), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                if (result.OutcomeUnknown && unknown is not null)
                    return OutcomeUnknownAnswer(unknown, before, result);
                return WithWarnings(Answer("done", null,
                    $"Session {sessionId} was killed. There is no undo."
                    + (killed?.Rollback is { InProgress: true } ? " A rollback is running: the session keeps its locks until it ends. Do not kill it again; read get_blocking to see it clear." : "")
                    /* The read after the KILL failed inside the batch (#5632): the kill stands, and whether it rolls back was not read. */
                    + (killed is { Rollback: null } ? " Whether a rollback is running was not read. Do not kill it again; read get_blocking to see its locks clear." : ""),
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
    /// derived the way the collector derives it). The agent passes the pid as well, and it must be the one inside backend_id, so an id changed on
    /// the way (a digit off) is refused rather than naming a neighbour. The start second is not a full guarantee: on Windows a pid can come back
    /// within about a second. Same gate, same journal kind (<c>session_kill</c>), no undo. A login that may not signal the session is refused with
    /// <c>not_permitted</c> before any kill is tried.
    /// </summary>
    private static async Task<string> RunKillPostgresAsync(NpgsqlDataSource? owner, ILogger logger, InvestigationRequestStore store,
        (int ServerId, string ServerName, string DisplayName) resolved, int? session_id, string? backend_id, string? agent_action, bool dry_run, CancellationToken ct, string? agentTokenName = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = KillToolName;

        /* 1: a backend id that cannot be read can do nothing. Refused before the gate, and journaled as a block is. */
        if (!PostgresBackendId.TryUnpack(backend_id, out var pid, out var startEpoch))
        {
            return await RefuseBeforeGateAsync(store, agent_action, tool, BadBackendId,
                "backend_id is the value get_pg_blocking (root_backend_id) or get_pg_session_states (backend_id) returns for the session, a string of digits, passed back unchanged. Nothing was run.", agentTokenName, namedAgentNames, ct);
        }

        if (session_id is not { } given || given != pid)
        {
            return await RefuseBeforeGateAsync(store, agent_action, tool, BadBackendId,
                "session_id must be the session's pid (root_pid from get_pg_blocking, pid from get_pg_session_states), and it must agree with the pid inside backend_id. Pass both exactly as the read returned them. Nothing was run.", agentTokenName, namedAgentNames, ct);
        }

        if (owner is null)
        {
            return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
        }

        /* 2: the gate, for a dry run too. The permit's Id and the journal's target.id are the pid in decimal. */
        var idText = pid.ToString(CultureInfo.InvariantCulture);
        var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, idText, null);
        var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.KillSession, agent_action, target, ct, agentTokenName, namedAgentNames);
        if (decision.Refusal is { } gateRefusal)
            return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, target.ServerId, target.ServerName, ServerActionNames.KillSession, ct);
        var permit = decision.Permit!;
        _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

        var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
        if (row is null)
            return Answer("failed", "server_row_missing", $"{resolved.DisplayName} has no row in the monitored-server settings.");
        var executor = new PostgresRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger, row.MonitoringUserName);

        /* 3: the read decides, for a dry run and before a write: gone, own session, start time, reused, client session, and what the login may do. */
        var dry = await executor.KillBackendAsync(permit, pid, startEpoch, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, agentTokenName, ct);
            return PgKillNotDone(dry, dry_run);
        }

        var before = PgKillSessionImage(dry.PgSession);
        var warnings = PgKillWarnings(dry.PgSession);
        if (dry_run)
            return Answer("dry_run", null, dry.Message + " There is no undo.", new { dry_run = true, session = before, warnings });

        RemediationResult? killed = null;
        RemediationResult? unknown = null;

        /* 4: the kill, journaled. action_called holds the facts the dry read found; action_done holds the facts the kill read. A kill whose
           reply was lost leaves action_called alone, which reads "result unknown". */
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
                    if (written.Outcome == RemediationOutcome.Unknown)
                    {
                        /* The statement was sent and its reply lost: neither done nor failed. */
                        changed.MarkOutcomeUnknown();
                        unknown = written;
                        return null;
                    }

                    if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                        throw new PlanForceWriteException(written.Message, PgKillNotDone(written, dryRun: false));
                    killed = written;
                    return JsonSerializer.Serialize(new { message = written.Message, session = PgKillSessionImage(written.PgSession), still_there = written.PgStillThere }, McpHelpers.JsonOptions);
                },
            };
            var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.SessionKill, action!, target, agentTokenName), hooks, ct);
            if (result.Refusal is { } refused)
                return DarlingAgentActionGate.RefusalAnswer(refused);
            if (result.OutcomeUnknown && unknown is not null)
                return OutcomeUnknownAnswer(unknown, PgKillSessionImage(dry.PgSession), result);
            return WithWarnings(Answer("done", null, (killed?.Message ?? $"Backend {pid} was signalled to end.") + " There is no undo.",
                new { dry_run = false, session = PgKillSessionImage(killed?.PgSession ?? dry.PgSession), still_there = killed?.PgStillThere, warnings, journal_id = result.CalledId }), result);
        }
        catch (PlanForceWriteException ex)
        {
            return ex.Answer;
        }
    }

    /// <summary>
    /// A kill whose statement was sent and whose reply was lost (#5632), on either engine: status <c>outcome_unknown</c>, nothing journaled as failed
    /// (the call reads "result unknown" in the agent changes list), and the message says the session may have ended.
    /// </summary>
    private static string OutcomeUnknownAnswer(RemediationResult result, object? session, AgentActionResult journal) =>
        Answer("outcome_unknown", null, result.Message + " There is no undo.", new { dry_run = false, session, journal_id = journal.CalledId });

    /// <summary>
    /// A refusal or a failure of the PostgreSQL kill. A reused pid answers nothing about the session that has it now: not its id, not its
    /// facts. That session is not the one the agent named.
    /// </summary>
    private static string PgKillNotDone(RemediationResult result, bool dryRun)
    {
        var status = result.Outcome == RemediationOutcome.Refused ? "refused" : "failed";
        if (result.Reason == RemediationResult.Reasons.SessionReused)
            return Answer(status, result.Reason, result.Message, new { dry_run = dryRun });
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
        wait_event = facts.WaitEvent,
        backend_type = facts.BackendType,
        open_transaction = facts.XactStartUtc is not null,
        xact_start = PgUtcText(facts.XactStartUtc),
        state_change = PgUtcText(facts.StateChangeUtc),
    };

    /// <summary>
    /// What a PostgreSQL kill will cost, with no threshold: an open transaction is rolled back. A session waiting for a synchronous standby has
    /// already committed on this server, so nothing rolls back; its client just never hears the commit confirmed. A warning, never a refusal.
    /// </summary>
    internal static string[] PgKillWarnings(PgKillSessionFacts? facts)
    {
        if (facts is null)
            return [];
        if (facts.WaitsForSyncReplication)
            return ["This session is waiting for a synchronous standby to confirm its commit. The transaction has already committed on this server, so ending the session does not roll it back: the commit stays, may not have reached the standby yet, and the client is never told it finished."];
        return facts.XactStartUtc is not null
            ? [$"This session has an open transaction (started {PgUtcText(facts.XactStartUtc)}). Ending the session rolls that transaction back and its uncommitted work is lost."]
            : [];
    }

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

    /// <summary>A refusal or a failure of the kill batch. A reused id answers nothing about the session that has it now: not its login time, not its facts.</summary>
    private static string KillNotDone(RemediationResult result, ServerClock clock, bool dryRun)
    {
        var status = result.Outcome == RemediationOutcome.Refused ? "refused" : "failed";
        if (result.Reason == RemediationResult.Reasons.SessionReused)
            return Answer(status, result.Reason, result.Message, new { dry_run = dryRun });
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
    private static async Task<string> RefuseBeforeGateAsync(InvestigationRequestStore store, string? agentAction, string tool, string code, string message,
        string? agentTokenName, IReadOnlySet<string>? namedAgentNames, CancellationToken ct)
    {
        var problem = InvestigationToolCore.ParseAgentAction(agentAction, out var parsed);
        if (problem is not null)
        {
            await store.RecordAgentActionRefusalAsync(DarlingAgentActionGate.LooseRequestId(agentAction),
                DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, DarlingAgentActionGate.LooseAgentId(agentAction)), tool, problem.Reason, agentTokenName, ct);
            return DarlingAgentActionGate.RefusalAnswer(problem);
        }

        var refusal = new AgentActionRefusal(code, message);
        /* #6542: a named agent token journals under its own name, never the agent_id it passed. */
        await store.RecordAgentActionRefusalAsync(parsed!.RequestId, DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, parsed.AgentId), tool, refusal.Reason, agentTokenName, ct);
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
                /* The server took the undo and the store's insert or commit failed (#5632, step 13): the session recorded it late, or could not. The
                   answer says so; it is never an error for a change that was made. */
                if (answer.Status == PlanUndoAnswer.Undone && session.JournalMissing)
                    return answer with { Message = answer.Message + " The change was made on the server, but the store could not record it. The agent changes list may still offer it for undo." };
                if (answer.Status == PlanUndoAnswer.Undone && session.RecordedLate)
                    return answer with { Message = answer.Message + " The store recorded it late." };
                return answer;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("Undo of plan change {CalledId} failed: {Message}", calledId, ex.Message);
                throw;
            }
        }
    }

    /// <summary>What a person is told when an undo's write was sent and its reply was lost (#5632): our own words, never the server's.</summary>
    internal const string UndoOutcomeUnknownWords =
        "The undo was sent to the server, but its reply was lost, so the plan may or may not have changed. Nothing was recorded as undone. Check the plan's current state in Query Store before you try again; the change stays on the list, and a second try reads the server first.";

    private static async Task<PlanUndoAnswer> DecideAndRunAsync(AgentChangeUndo.PlanUndoSession session, NpgsqlDataSource? owner, ILogger logger,
        bool confirm, string? shownStatement, CancellationToken ct)
    {
        var facts = session.Facts;
        if (owner is null)
            return PlanUndoAnswer.Refused(NoOwnerSource, "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly.");

        if (facts.IsGuideDrop)
            return await DecideAndRunGuideRecreateUndoAsync(session, owner, logger, confirm, shownStatement, ct);
        if (facts.IsGuide)
            return await DecideAndRunGuideUndoAsync(session, owner, logger, confirm, shownStatement, ct);

        /* The permit for the plan the change named: it reads the state, and it is the plan an unforce or a no-prior-force undo writes on. */
        var decision = await session.AuthorizeAsync(facts.PlanId, owner, ct);
        if (decision.Refusal is { } gateRefusal)
            return PlanUndoAnswer.Refused(gateRefusal.Reason, gateRefusal.Message);
        var permit = decision.Permit!;

        var row = await ReadServerRowAsync(owner, facts.ServerId, ct);
        if (row is null)
            return PlanUndoAnswer.Refused("server_row_missing", $"{facts.ServerName} has no row in the monitored-server settings.");

        /* A database the server's entry excludes is one Darling does not act in, so the force undo refuses too, as the plan guide undo does. */
        if (IsExcludedDatabase(row.ExcludedDatabases, facts.Database))
        {
            await session.RecordRefusalAsync(AzureDatabaseExcluded, ct);
            return PlanUndoAnswer.Refused(AzureDatabaseExcluded, ExcludedDatabaseRefusal(facts.Database, ForceUndoExcludedGuidance).Message);
        }

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
        if (written.Outcome == RemediationOutcome.Unknown)
        {
            /* The write was sent and its reply was lost, or the call was cancelled while it ran (#5632): the plan may have changed. It is not
               journaled as undone (the server did not confirm it) or refused (it was not): the attempt is an action_called row with no result,
               as for a kill, so a possible change by a person shows in the Agent changes list as "result unknown" and not only in the service log.
               The change stays offered for undo, and the server-state check on the next try decides whether the undo still applies. */
            logger.LogWarning("The undo of plan change {CalledId} was sent, but its outcome is unknown: {Message}", facts.CalledId, written.Message);
            await session.RecordOutcomeUnknownAsync(statement);
            return new PlanUndoAnswer(PlanUndoAnswer.OutcomeUnknown, RemediationResult.Reasons.OutcomeUnknown, UndoOutcomeUnknownWords, statement, stateNode);
        }

        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
        {
            await session.RecordRefusalAsync(written.Reason ?? "write_failed", ct);
            return new PlanUndoAnswer(written.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, written.Reason, written.Message, statement, stateNode);
        }

        /* The executor says the server accepted the write: from here a store failure is recorded late, never lost (#5632, step 13). */
        await session.MarkServerChangedAsync(statement, stateNode, step.RestoredAs);

        /* The read after the write is a warning when it fails, not an error for a change that was made. */
        JsonNode? afterNode = null;
        try
        {
            var after = await executor.ReadPlanStateAsync(stepPermit, facts.QueryId, step.PlanId, ct);
            afterNode = after.State is null ? null : JsonSerializer.SerializeToNode(after.State);
        }
        catch (Exception ex)
        {
            logger.LogWarning("The state read after undoing plan change {CalledId} failed: {Message}", facts.CalledId, ex.Message);
        }

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
        string? reason, string message, string? agentTokenName, CancellationToken ct)
    {
        if (outcome == RemediationOutcome.Refused && reason is not null)
            await store.RecordAgentActionRefusalAsync(action.RequestId, action.AgentId, tool, reason, agentTokenName, ct);
        return Answer(outcome == RemediationOutcome.Refused ? "refused" : "failed", reason, message);
    }

    private sealed record ServerRow(RemediationServer Server, string? UserName, string? SecretSlot, string? MonitoringUserName = null,
        string[]? ExcludedDatabases = null);

    internal const string ServerRowSql = @"
SELECT host, database, encrypt_mode, trust_server_certificate, multi_subnet_failover, remediation_username, remediation_encrypted_password, port,
       username, excluded_databases
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
            reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(8) ? null : reader.GetString(8), reader.IsDBNull(9) ? null : reader.GetFieldValue<string[]>(9));
    }

    internal const string AzureDatabaseRequired = RemediationResult.Reasons.DatabaseRequired;
    internal const string AzureDatabaseExcluded = "database_excluded";
    internal const string AzureDatabaseUnknown = "database_not_monitored";

    /// <summary>
    /// Whether Darling holds a blocking row from this database for this server in the last day (#5632): a DMV blocking snapshot or a
    /// blocked-process report. $1 server_id, $2 database name, compared exactly as the collector stored it.
    /// </summary>
    internal const string AzureDatabaseSeenSql = @"
SELECT EXISTS (SELECT 1 FROM v_dmv_blocking_snapshots WHERE server_id = $1 AND database_name = $2 AND collection_time >= (now() AT TIME ZONE 'UTC') - interval '1 day')
    OR EXISTS (SELECT 1 FROM blocked_process_reports WHERE server_id = $1 AND database_name = $2 AND collection_time >= (now() AT TIME ZONE 'UTC') - interval '1 day')";

    /// <summary>
    /// The database an Azure SQL Database kill connects to (#5632). Required; never master; not one the server entry excludes; and one the
    /// target is known to hold: the entry's own database, or a database Darling collected a blocking row from for this server in the last day.
    /// The kill batch then checks on the server that the connection landed there, read-write, and that the session is in it.
    /// </summary>
    private static async Task<AgentActionRefusal?> AzureKillDatabaseAsync(NpgsqlDataSource postgres, int serverId, ServerRow row,
        string database, CancellationToken ct)
    {
        if (IsExcludedDatabase(row.ExcludedDatabases, database))
            return ExcludedDatabaseRefusal(database);

        if (string.Equals(row.Server.Database, database, StringComparison.OrdinalIgnoreCase))
            return null;

        await using var command = postgres.CreateCommand(AzureDatabaseSeenSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = database, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        var seen = await command.ExecuteScalarAsync(ct) is true;
        return seen
            ? null
            : new AgentActionRefusal(AzureDatabaseUnknown,
                $"Database {database} is not this server's registered database, and Darling has collected no blocking row from it in the last day, so the kill does not connect to it. Pass the database_name of the get_blocking row you read. Nothing was run.");
    }

    /// <summary>The refusal for a database the server's entry excludes, shared by the Azure kill, <c>recompile_module</c> and the plan guide tools.</summary>
    internal static AgentActionRefusal ExcludedDatabaseRefusal(string database) => ExcludedDatabaseRefusal(database, null);

    /// <summary>The sentence the force undo adds after the shared refusal, because an undo is a person's way back and the refusal alone leaves them no next step.</summary>
    internal const string ForceUndoExcludedGuidance =
        "To reverse this change, unforce or force the plan by hand in that database, or remove the database from the server's exclusion list and undo it again.";

    /// <summary>The shared refusal followed by <paramref name="guidance"/> when there is one.</summary>
    internal static AgentActionRefusal ExcludedDatabaseRefusal(string database, string? guidance) =>
        new(AzureDatabaseExcluded, $"Database {database} is excluded in this server's entry, so Darling does not act in it. Nothing was run."
            + (string.IsNullOrEmpty(guidance) ? "" : " " + guidance));

    /// <summary>Whether the server entry's excluded list names <paramref name="database"/>: entries are compared trimmed, and without regard to case (#5632).</summary>
    internal static bool IsExcludedDatabase(string[]? excluded, string database) =>
        excluded is not null && excluded.Any(e => string.Equals(e?.Trim(), database, StringComparison.OrdinalIgnoreCase));

    internal const string AzureDatabaseRequiredMessage =
        "This server is an Azure SQL Database, where a session is reached only from inside its own database: pass database_name, the database_name of the get_blocking row you read (never master). Nothing was run.";

    /// <summary>
    /// The shape of <c>database_name</c> for an Azure SQL Database kill (#5632): trimmed, 1 to 128 characters, no control character, and not
    /// <c>master</c>. Null when it is not one. Checked before the gate, because the database is part of what the gate permits and the journal records.
    /// </summary>
    internal static string? AzureDatabaseShape(string? databaseName)
    {
        var database = databaseName?.Trim();
        return string.IsNullOrEmpty(database) || database.Length > 128 || database.Any(char.IsControl)
            || string.Equals(database, "master", StringComparison.OrdinalIgnoreCase)
                ? null
                : database;
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
