/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// <c>create_plan_guide</c> (#6569, slice 2a): the fifth Level 3 server action tool, in the shape of <c>recompile_module</c> (the gate for a dry run too, the executor's
/// own checks, the journal) with an undo. Its definition is journaled whole, so a person's undo drops the guide by name (<see cref="UndoPlanChangeAsync"/>) and a re-create
/// can read the definition back (slice 2b). Every value reaches the server as a parameter of <c>sys.sp_create_plan_guide</c>; none is spliced into batch text.
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    internal const string CreatePlanGuideToolName = "create_plan_guide";

    /// <summary>The create tool body (#6569), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunCreatePlanGuideAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name, string? database_name,
        string? name, string? statement, string? type, string? module_or_batch, string? parameters, string? hints, string? agent_action, bool dry_run, CancellationToken ct,
        string? agentTokenName = null)
    {
        const string tool = CreatePlanGuideToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);

            /* 1: arguments that cannot make a guide are refused before the gate, and journaled as a block is. */
            var (definition, badCode, badMessage) = PlanGuideDefinition.TryCreate(name, statement, type, module_or_batch, parameters, hints);
            if (definition is null)
                return await RefuseBeforeGateAsync(store, agent_action, tool, badCode!, badMessage!, ct, agentTokenName);

            var database = database_name?.Trim();
            if (string.IsNullOrEmpty(database) || database.Length > 128 || ContainsControlCharacter(database))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadDatabase,
                    "database_name is the database the guide is created in, at most 128 characters. Nothing was run.", ct, agentTokenName);
            }

            /* 2: SQL Server only (Azure SQL Database and Managed Instance have sp_create_plan_guide too). */
            var (_, engineKind) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. A plan guide is SQL Server only; there is no PostgreSQL equivalent.", ct, agentTokenName);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id is the guide's name and its Database is the database. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, definition.Name, database);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.CreatePlanGuide, agent_action, target, ct, agentTokenName);
            if (decision.Refusal is { } gateRefusal)
                return GateRefusalAnswer(gateRefusal, dry_run);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            var row = await ReadServerRowAsync(owner, resolved.ServerId, ct);
            if (row is null)
                return Answer("failed", "server_row_missing", $"{resolved.DisplayName} has no row in the monitored-server settings.");

            /* 3b: a database the server's entry excludes is one Darling does not act in. */
            if (IsExcludedDatabase(row.ExcludedDatabases, database))
            {
                await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, AzureDatabaseExcluded, ct);
                return DarlingAgentActionGate.RefusalAnswer(ExcludedDatabaseRefusal(database));
            }

            var executor = new SqlServerRemediationExecutor(resolved.ServerId, row.Server, row.UserName, row.SecretSlot, logger);
            var statementText = PlanGuideStatement.Create(database, definition);

            /* 4: the executor's own checks run on a dry run and before a write: the name is free, an OBJECT guide's module resolves and holds the statement, the grants. */
            var dry = await executor.CreatePlanGuideAsync(permit, definition, dryRun: true, ct);
            var before = new
            {
                database,
                guide = new { name = definition.Name, type = definition.Type, statement = definition.Statement, module_or_batch = definition.ModuleOrBatch, @params = definition.Params, hints = definition.Hints },
                cached_matches = dry.CachedMatches,
            };
            if (dry.Outcome != RemediationOutcome.Done)
            {
                if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                    await store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, dry.Reason, ct);
                return Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
                    new { dry_run, statement = dry.Statement ?? statementText, before });
            }

            if (dry_run)
                return Answer("dry_run", null, dry.Message + " A person can undo the create from Agent changes: the undo drops the guide.",
                    new { dry_run = true, statement = dry.Statement ?? statementText, before, matches = dry.CachedMatches });

            /* 5: the write, journaled. action_called holds the definition; action_done holds the statement and the message; a guide that fails validation is dropped in the batch and journaled action_failed. */
            RemediationResult? unknown = null;
            RemediationResult? done = null;
            var changed = new ServerChangeSignal();
            try
            {
                var hooks = new AgentActionHooks
                {
                    ServerChange = changed,
                    LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, logger, token),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>(JsonSerializer.Serialize(before, McpHelpers.JsonOptions)),
                    Write = async (_, _, token) =>
                    {
                        var written = await executor.CreatePlanGuideAsync(permit, definition, dryRun: false, token);
                        if (written.Wrote)
                            changed.MarkServerChanged();
                        if (written.Outcome == RemediationOutcome.Unknown)
                        {
                            /* The write was sent and its reply was lost, or the call was cancelled while it ran: neither done nor failed (#5632). */
                            changed.MarkOutcomeUnknown();
                            unknown = written;
                            return null;
                        }

                        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
                            throw new PlanForceWriteException(written.Message, Answer(written.Outcome == RemediationOutcome.Refused ? "refused" : "failed",
                                written.Reason, written.Message, new { dry_run = false, statement = statementText, before, validation = written.Validation }));
                        done = written;
                        return JsonSerializer.Serialize(new { statement = written.Statement ?? statementText, message = written.Message, matches = written.CachedMatches }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.PlanGuideCreate, action!, target), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                if (result.OutcomeUnknown && unknown is not null)
                    return Answer("outcome_unknown", null, unknown.Message, new { dry_run = false, statement = statementText, before, journal_id = result.CalledId });

                return WithWarnings(Answer("done", null,
                    (done?.Message ?? $"Plan guide {definition.Name} was created in {database}.") + " A person can undo it from Agent changes: the undo drops the guide.",
                    new { dry_run = false, statement = statementText, before, matches = done?.CachedMatches, undo = (AgentPlanUndo?)null, journal_id = result.CalledId }), result);
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

    /// <summary>
    /// A person's undo of a plan guide create (#6569, slice 2a), run by <see cref="UndoPlanChangeAsync"/> in the same show-then-confirm shape as a plan undo. It drops the guide by name
    /// through the executor's drop, which drops only a guide that still holds the journaled statement and hints (<c>guide_changed</c> otherwise). A guide that is already gone is
    /// <c>guide_not_found</c>: the answer says so and nothing changes. The gate is that server's <c>create_plan_guide</c> switch and remediation login.
    /// </summary>
    private static async Task<PlanUndoAnswer> DecideAndRunGuideUndoAsync(AgentChangeUndo.PlanUndoSession session, NpgsqlDataSource owner, ILogger logger,
        bool confirm, string? shownStatement, CancellationToken ct)
    {
        var facts = session.Facts;
        var guideName = facts.GuideName!;
        string? Text(string key) => facts.Guide is System.Text.Json.Nodes.JsonObject g && g[key] is System.Text.Json.Nodes.JsonValue v && v.TryGetValue(out string? s) ? s : null;
        var expected = new PlanGuideDefinition(guideName, Text("type") ?? "", Text("statement") ?? "", Text("module_or_batch"), Text("params"), Text("hints") ?? "");
        var stateNode = facts.Guide?.DeepClone();

        var decision = await session.AuthorizeAsync(guideName, owner, ct);
        if (decision.Refusal is { } gateRefusal)
            return PlanUndoAnswer.Refused(gateRefusal.Reason, gateRefusal.Message);
        var permit = decision.Permit!;

        var row = await ReadServerRowAsync(owner, facts.ServerId, ct);
        if (row is null)
            return PlanUndoAnswer.Refused("server_row_missing", $"{facts.ServerName} has no row in the monitored-server settings.");
        var executor = new SqlServerRemediationExecutor(facts.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

        var statement = PlanGuideStatement.Drop(facts.Database, guideName);
        var dry = await executor.DropPlanGuideAsync(permit, guideName, expected, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await session.RecordRefusalAsync(dry.Reason, ct);
            return new PlanUndoAnswer(dry.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, dry.Reason, dry.Message, dry.Statement ?? statement, stateNode);
        }

        statement = dry.Statement ?? statement;
        var words = $"Dropping plan guide {guideName} from {facts.Database} undoes its creation: the statement runs without the guide's hints again.";
        if (!confirm || shownStatement is null || !string.Equals(shownStatement, statement, StringComparison.Ordinal))
        {
            var lead = !confirm ? ""
                : shownStatement is null ? "A confirm must carry the statement that was shown, so nothing was written. Review this statement and confirm it. "
                : "The server's state changed since the statement was shown, so nothing was written. Review the new statement and confirm again. ";
            return new PlanUndoAnswer(PlanUndoAnswer.Confirm, null, lead + words, statement, stateNode);
        }

        var written = await executor.DropPlanGuideAsync(permit, guideName, expected, dryRun: false, ct);
        if (written.Outcome == RemediationOutcome.Unknown)
        {
            logger.LogWarning("The undo of plan guide create {CalledId} was sent, but its outcome is unknown: {Message}", facts.CalledId, written.Message);
            await session.RecordOutcomeUnknownAsync(statement);
            return new PlanUndoAnswer(PlanUndoAnswer.OutcomeUnknown, RemediationResult.Reasons.OutcomeUnknown,
                "The undo was sent to the server, but its reply was lost, so the plan guide may or may not be gone. Nothing was recorded as undone. Read sys.plan_guides before you try again; the change stays on the list, and a second try reads the server first.",
                statement, stateNode);
        }

        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
        {
            await session.RecordRefusalAsync(written.Reason ?? "write_failed", ct);
            return new PlanUndoAnswer(written.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, written.Reason, written.Message, statement, stateNode);
        }

        await session.MarkServerChangedAsync(statement, stateNode, null);
        await session.JournalUndoneAsync(statement, stateNode, null, null, ct);
        return new PlanUndoAnswer(PlanUndoAnswer.Undone, null, $"Plan guide {guideName} was dropped from {facts.Database}.", statement, stateNode, null, null, facts.CalledId, session.Actor);
    }

    /* The tool method is the last member on purpose: the payload contract census reads a tool's span up to the next tool or the end of the file, and the
       body above has its own catch, which the census must not read as this one-line method's. */
    [McpServerTool(Name = "create_plan_guide"), Description(
        "Agent arm, SQL Server only (Azure SQL Database and Managed Instance too): creates ONE plan guide with sys.sp_create_plan_guide, so a statement runs with the OPTION (...) hints you give it (OPTIMIZE FOR, RECOMPILE, MAXDOP, USE PLAN) without a change to the application. Dry run by default: returns the exact statement and what the guide matches, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's create_plan_guide switch and remediation login (with ALTER on the database, and on the module for an OBJECT guide); ships off. " +
        "<<GUIDE>>" +
        "type is OBJECT (module_or_batch is the two-part schema.name of a procedure, function or trigger, and statement must appear exactly in its definition) or SQL (module_or_batch is the batch text, or empty when statement is the batch; params is the parameter list for a parameterized batch). TEMPLATE is refused. hints is required and is the OPTION (...) clause. Every value goes to the server as a parameter, never as batch text. " +
        "Before it creates, the guide is matched and reported: an OBJECT guide's module must resolve and hold the statement, a SQL guide reports how many cached batches equal its text (zero is allowed: the guide then matches nothing cached right now). A name already used in the database is refused. After it creates, the guide is validated with sys.fn_validate_plan_guide in the same batch: an invalid guide is dropped again and the call is journaled failed with the server's message. " +
        "This tool blocks only a call that cannot do anything: bad_guide_name, bad_guide_type, bad_guide_argument, bad_module_name, bad_database, guide_exists, module_not_found, not_a_module, statement_not_in_module, guide_permission_denied, guide_invalid. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_guide_create) with the whole definition, and a person can undo it there: the undo drops the guide by name. Answers status dry_run, done, refused (reason) or failed, with statement, before, matches and journal_id.")]
    public static Task<string> CreatePlanGuide(
        NpgsqlDataSource postgres,
        [Description("The database the guide is created in.")] string database_name,
        [Description("The plan guide's name: 1 to 128 characters, not starting with #. A name already used in the database is refused.")] string name,
        [Description("The text of the statement the guide applies to, exactly as written in the module or batch (case and spacing included).")] string statement,
        [Description("OBJECT or SQL. TEMPLATE is refused.")] string type,
        [Description("The OPTION (...) clause to attach, for example OPTION (RECOMPILE), OPTION (MAXDOP 1), OPTION (OPTIMIZE FOR (@p = 1)) or OPTION (USE PLAN N'<xml>').")] string hints,
        [Description("OBJECT: the two-part schema.name of the procedure, function or trigger. SQL: the batch text, or empty when statement is the whole batch.")] string? module_or_batch = null,
        [Description("SQL only: the parameter list of a parameterized batch, for example '@p int'. Empty for none.")] string? @params = null,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and what the guide matches, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunCreatePlanGuideAsync(postgres, ownerSource?.Read(), logger, server_name, database_name, name, statement, type, module_or_batch, @params, hints, agent_action, dry_run, cancellationToken,
            served?.Policy.AgentTokenName);
}
