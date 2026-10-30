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
using System.Text.Json;
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
/// <c>recompile_module</c> (#6569): the fourth Level 3 server action tool, beside <c>evict_cached_plan</c> and in the same shape (the gate for a
/// dry run too, the executor's own checks, the journal, no undo). It lives in its own file of the same class so it shares the answer and
/// refusal helpers of the plan tools without adding to that file.
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    internal const string RecompileToolName = "recompile_module";



    /// <summary>The recompile tool body (#6569), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunRecompileAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name,
        string? database_name, string? module_name, string? agent_action, bool dry_run, CancellationToken ct, string? agentTokenName = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = RecompileToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: owner);

            /* 1: a name that does not parse can do nothing. Refused before the gate, and journaled as a block is. */
            if (!ModuleRecompileStatement.TryNormalize(module_name, out var module))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadModuleName,
                    "module_name is the two-part schema.name of a procedure, function or trigger, as get_procedure_stats returns it. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            var database = database_name?.Trim();
            if (string.IsNullOrEmpty(database) || database.Length > 128 || ContainsControlCharacter(database))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadDatabase,
                    "database_name is the database the module is in, at most 128 characters. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            /* 2: SQL Server only (an Azure SQL Database has sp_recompile too). */
            var (_, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. Recompiling a module is SQL Server only; there is no PostgreSQL equivalent.", agentTokenName, namedAgentNames, ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id is the name in its one normal form and its Database is the database. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, module, database);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.RecompileModule, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, resolved.ServerId, resolved.ServerName, ServerActionNames.RecompileModule, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.DisplayName} has no row in the monitored-server settings.");

            /* 3b: a database the server's entry excludes is one Darling does not act in. Refused before the dry run reads anything, with the Azure kill's sentence. */
            if (IsExcludedDatabase(row.ExcludedDatabases, database))
            {
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, AzureDatabaseExcluded, agentTokenName, ct);
                return DarlingAgentActionGate.RefusalAnswer(ExcludedDatabaseRefusal(database));
            }

            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);
            var statement = ModuleRecompileStatement.For(database, module);

            /* 4: the executor's own checks run on a dry run and before a write: the name resolves, it is a procedure, function or trigger, the grant. */
            var dry = await executor.RecompileModuleAsync(permit, module, dryRun: true, ct);
            /* The journal and the answer name the object the server resolved (its schema and name), not the input text. Before it resolves, only the input is known. */
            var resolvedModule = dry.ObjectSchema is null || dry.ObjectName is null
                ? module
                : "[" + dry.ObjectSchema.Replace("]", "]]", StringComparison.Ordinal) + "].[" + dry.ObjectName.Replace("]", "]]", StringComparison.Ordinal) + "]";
            var before = new { database, module = resolvedModule, schema = dry.ObjectSchema, name = dry.ObjectName, object_type = dry.ObjectType };
            if (dry.Outcome != RemediationOutcome.Done)
            {
                if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, agentTokenName, ct);
                return Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
                    new { dry_run, statement = dry.Statement ?? statement, before });
            }

            if (dry_run)
                return Answer("dry_run", null, dry.Message + " A recompile has no undo.", new { dry_run = true, statement = dry.Statement ?? statement, before });

            /* 5: the write, journaled. action_called holds the object read on the server; action_done holds the statement and the message. */
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
                        var written = await executor.RecompileModuleAsync(permit, module, dryRun: false, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement, before }));
                        return JsonSerializer.Serialize(new { statement = written.Statement ?? statement, message = written.Message }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.ModuleRecompile, action!, target, agentTokenName), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                return WithWarnings(Answer("done", null,
                    "The module was marked for recompile. There is nothing to put back: its next run compiles a new plan.",
                    new { dry_run = false, statement, before, undo = (AgentPlanUndo?)null, journal_id = result.CalledId }), result);
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

    private static bool ContainsControlCharacter(string text)
    {
        foreach (var c in text)
            if (char.IsControl(c))
                return true;
        return false;
    }

    /* The tool method is the last member on purpose: the payload contract census reads a tool's span up to the next tool or the end of the file, and the
       body above has its own catch, which the census must not read as this one-line method's. */
    [McpServerTool(Name = "recompile_module"), Description(
        "Agent arm, SQL Server only: marks ONE procedure, function or trigger for recompile with sys.sp_recompile, so its next run compiles a new plan. Dry run by default: returns the exact statement and what the object is, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's recompile_module switch and remediation login (with ALTER on the object); ships off. " +
        "<<GUIDE>>" +
        "module_name is the two-part schema.name from get_procedure_stats or get_top_procedures_by_cpu; database_name is the database it lives in. The object name is resolved in that database first and goes to the server as a parameter, never as batch text. There is no undo: the next run compiles a new plan, so a done answer carries undo null. " +
        "This tool blocks only a call that cannot do anything: bad_module_name, bad_database, module_not_found (the name does not resolve in that database), not_a_module (a table, a view or another kind of object), recompile_permission_denied (the login lacks ALTER on the object). " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind module_recompile). Answers status dry_run, done, refused (reason) or failed, with statement, before and journal_id.")]
    public static Task<string> RecompileModule(
        NpgsqlDataSource postgres,
        [Description("The database the procedure, function or trigger is in.")] string database_name,
        [Description("The two-part schema.name of the procedure, function or trigger, as get_procedure_stats returns it.")] string module_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and what the object is, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunRecompileAsync(postgres, ownerSource?.Read(), logger, server_name, database_name, module_name, agent_action, dry_run, cancellationToken, served?.Policy.AgentTokenName, served?.NamedAgentNames);
}
