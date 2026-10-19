/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    /// <summary>Refusal codes of this tool; the gate's codes are <see cref="AgentActionCodes"/> and the executor's are <see cref="RemediationResult.Reasons"/>.</summary>
    internal const string PostgresTarget = "postgres_target";
    internal const string NoOwnerSource = "owner_source_unavailable";

    [McpServerTool(Name = "force_query_store_plan"), Description(
        "Agent arm, SQL Server only: forces one Query Store plan for a query on the monitored server (replaces another plan of the query that is forced now). Dry run by default: returns the exact statement and the before state, writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's force_plan switch and remediation login; ships off. " +
        "<<GUIDE>>" +
        "Call get_plan_force_precheck first and weigh its warnings; it only warns. This tool blocks only a call that cannot do anything: plan_not_found (the plan is not among that query's Query Store rows) or plan_already_forced. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_force) with the plan that was forced before and how (forcing_type MANUAL or AUTO on SQL Server 2022 and later, else unknown), so the way back is known. " +
        "After a write, check with get_forced_plans. Nothing in Darling unforces on its own: unforce_query_store_plan is the way back. Answers status dry_run, done, refused (reason) or failed, with before, after, statement and journal_id.")]
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
        "Call get_plan_force_precheck first. This tool blocks only a call that cannot do anything: plan_not_found or plan_not_forced. An agent may unforce any force, including one automatic plan correction or a person placed; the change is journaled in Agent changes (kind plan_unforce) with how that plan was forced, so it can be forced again. " +
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

            var statement = SqlServerRemediationExecutor.StatementFor(beforeState.Database, query_id, plan_id, force);

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
            try
            {
                var hooks = new AgentActionHooks
                {
                    LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(beforeState)),
                    Write = async (_, _, token) =>
                    {
                        var written = force
                            ? await executor.ForcePlanAsync(permit, query_id, plan_id, dryRun: false, token)
                            : await executor.UnforcePlanAsync(permit, query_id, plan_id, dryRun: false, token);
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement, before = beforeState }));
                        var read = await executor.ReadPlanStateAsync(permit, query_id, plan_id, token);
                        after = read.State;
                        return JsonSerializer.Serialize(new { statement, state = after, message = written.Message });
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, kind, action!, target), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return Answer("done", null,
                    $"Plan {plan_id} was {(force ? "forced" : "unforced")} for query {query_id} in {beforeState.Database}. Check with get_forced_plans.",
                    new { dry_run = false, statement, before = beforeState, after, journal_id = result.CalledId });
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
SELECT host, database, encrypt_mode, trust_server_certificate, multi_subnet_failover, remediation_username, remediation_encrypted_password
FROM config.config_monitored_servers
WHERE server_id = $1";

    private static async Task<ServerRow?> ReadServerRowAsync(NpgsqlDataSource owner, int serverId, CancellationToken ct)
    {
        await using var connection = await owner.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(ServerRowSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;
        return new ServerRow(
            new RemediationServer(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetBoolean(3), reader.GetBoolean(4)),
            reader.IsDBNull(5) ? null : reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetString(6));
    }

    /// <summary>A failure inside the write hook: the gate journals <c>action_failed</c> with <see cref="Exception.Message"/> (the server's own words, already scrubbed of the secret) and the tool answers <see cref="Answer"/>.</summary>
    private sealed class PlanForceWriteException(string message, string answer) : Exception(message)
    {
        internal string Answer { get; } = answer;
    }
}
