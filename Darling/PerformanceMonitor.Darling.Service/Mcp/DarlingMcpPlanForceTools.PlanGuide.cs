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
/// <c>create_plan_guide</c> (#6569, slice 2a) and <c>drop_plan_guide</c> (slice 2b): the fifth and sixth Level 3 server action tools, in the shape of <c>recompile_module</c> (the gate for a dry run too, the executor's
/// own checks, the journal) with an undo. Its definition is journaled whole, so a person's undo drops the guide by name (<see cref="UndoPlanChangeAsync"/>) and the drop
/// journals the definition it read, so a person's undo re-creates the guide from it (slice 2b). Every value reaches the server as a parameter of <c>sys.sp_create_plan_guide</c>; none is spliced into batch text.
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    internal const string CreatePlanGuideToolName = "create_plan_guide";

    /// <summary>The refusal reason for an Azure SQL target (#6569).</summary>
    internal const string EngineNotSupported = "engine_not_supported";

    /// <summary>The refusal reason for a drop of a guide its Undo could not re-create (#6569): the definition read does not pass the checks a create makes.</summary>
    internal const string GuideNotUndoable = "guide_not_undoable";

    /* SERVERPROPERTY('EngineEdition') as the registry stores it: 5 is an Azure SQL Database, 8 an Azure SQL Managed Instance. The numbers live in
       ServerActionNames, which the switch rule reads too (#6727), so a switch cannot turn on for an edition these tools always refuse. */
    private const int AzureSqlDatabaseEdition = ServerActionNames.AzureSqlDatabaseEdition;
    private const int AzureManagedInstanceEdition = ServerActionNames.AzureManagedInstanceEdition;

    /// <summary>The create tool body (#6569), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunCreatePlanGuideAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name, string? database_name,
        string? name, string? statement, string? type, string? module_or_batch, string? parameters, string? hints, string? agent_action, bool dry_run, CancellationToken ct,
        string? agentTokenName = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = CreatePlanGuideToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: owner);

            /* 1: arguments that cannot make a guide are refused before the gate, and journaled as a block is. */
            var (definition, badCode, badMessage) = PlanGuideDefinition.TryCreate(name, statement, type, module_or_batch, parameters, hints);
            if (definition is null)
                return await RefuseBeforeGateAsync(store, agent_action, tool, badCode!, badMessage!, agentTokenName, namedAgentNames, ct);

            var database = database_name?.Trim();
            if (string.IsNullOrEmpty(database) || database.Length > 128 || ContainsControlCharacter(database))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadDatabase,
                    "database_name is the database the guide is created in, at most 128 characters. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            /* 2: SQL Server only. Microsoft's sp_create_plan_guide page lists Azure SQL Database and Managed Instance, but the sp_control_plan_guide page, which the
               drop of an invalid guide and the Undo need, lists SQL Server alone, so an Azure target is refused rather than half supported. */
            var (engineEdition, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. A plan guide is SQL Server only; there is no PostgreSQL equivalent.", agentTokenName, namedAgentNames, ct);
            }

            if (engineEdition is AzureSqlDatabaseEdition or AzureManagedInstanceEdition)
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, EngineNotSupported,
                    $"{resolved.DisplayName} is an Azure SQL target. Darling creates plan guides on SQL Server only: Microsoft documents sp_control_plan_guide, which the drop of an invalid guide and the Undo use, for SQL Server alone. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id is the guide's name and its Database is the database. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, definition.Name, database);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.CreatePlanGuide, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, resolved.ServerId, resolved.ServerName, ServerActionNames.CreatePlanGuide, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 4: the prepare step: the server row, the excluded database and the executor's own checks (the name is free, an OBJECT guide's module resolves and holds the statement, the grants). */
            var prepared = await PrepareCreatePlanGuideAsync(owner, logger, permit, resolved.ServerId, resolved.DisplayName, database, definition, dry_run,
                (reason, token) => store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, reason, agentTokenName, token), ct);
            if (prepared.Answer is { } preparedAnswer)
                return preparedAnswer;
            var facts = prepared.Facts!;
            var executor = prepared.Executor!;
            var before = facts.Before;
            var statementText = facts.Statement;

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
                        var written = await WriteCreatePlanGuideAsync(executor, permit, facts, token);
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
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.PlanGuideCreate, action!, target, agentTokenName), hooks, ct);
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
        if (GuideUndoNameRefusal(guideName) is { } badName)
            return badName;
        string? Text(string key) => facts.Guide is System.Text.Json.Nodes.JsonObject g && g[key] is System.Text.Json.Nodes.JsonValue v && v.TryGetValue(out string? s) ? s : null;
        var expected = new PlanGuideDefinition(guideName, Text("type") ?? "", Text("statement") ?? "", Text("module_or_batch"), Text("params"), Text("hints"));
        var stateNode = facts.Guide?.DeepClone();

        var decision = await session.AuthorizeAsync(guideName, owner, ct);
        if (decision.Refusal is { } gateRefusal)
            return PlanUndoAnswer.Refused(gateRefusal.Reason, gateRefusal.Message);
        var permit = decision.Permit!;

        var row = await ReadServerRowAsync(owner, facts.ServerId, ct);
        if (row is null)
            return PlanUndoAnswer.Refused("server_row_missing", $"{facts.ServerName} has no row in the monitored-server settings.");
        if (await GuideUndoServerRefusalAsync(session, owner, row, ct) is { } serverRefusal)
            return serverRefusal;
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

    internal const string DropPlanGuideToolName = "drop_plan_guide";

    /// <summary>The guide name a plan guide Undo acts on must pass the check a create and a drop make on it (1 to 128 characters, no control character, not a leading #), so the server never cuts it to fit its <c>NVarChar(128)</c> parameter (#6569).</summary>
    private static PlanUndoAnswer? GuideUndoNameRefusal(string guideName) =>
        string.Equals(PlanGuideDefinition.TryName(guideName), guideName, StringComparison.Ordinal) ? null
            : PlanUndoAnswer.Refused("not_undoable", "The plan guide name journaled with this change is not a valid plan guide name (1 to 128 characters, no control characters, not starting with #), so it cannot be undone from here. Nothing was run.");

    /// <summary>
    /// What both plan guide Undos check once the server's row is read, as the create and drop tools do before they act (#6569): the database must not be in the server's excluded list, and the
    /// engine must be SQL Server (not PostgreSQL, not an Azure SQL Database or Managed Instance). A refusal is recorded on the session the way the other Undo refusals are. Null when the server passes.
    /// </summary>
    private static async Task<PlanUndoAnswer?> GuideUndoServerRefusalAsync(AgentChangeUndo.PlanUndoSession session, NpgsqlDataSource owner, ServerRow row, CancellationToken ct)
    {
        var facts = session.Facts;
        if (IsExcludedDatabase(row.ExcludedDatabases, facts.Database))
        {
            await session.RecordRefusalAsync(AzureDatabaseExcluded, ct);
            return PlanUndoAnswer.Refused(AzureDatabaseExcluded, ExcludedDatabaseRefusal(facts.Database).Message);
        }

        var (engineEdition, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(owner, facts.ServerId, ct);
        if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
        {
            await session.RecordRefusalAsync(PostgresTarget, ct);
            return PlanUndoAnswer.Refused(PostgresTarget, $"{facts.ServerName} is a PostgreSQL target. A plan guide is SQL Server only, so this change cannot be undone from here. Nothing was run.");
        }

        if (engineEdition is AzureSqlDatabaseEdition or AzureManagedInstanceEdition)
        {
            await session.RecordRefusalAsync(EngineNotSupported, ct);
            return PlanUndoAnswer.Refused(EngineNotSupported, $"{facts.ServerName} is an Azure SQL target. Darling creates and drops plan guides on SQL Server only, so this change cannot be undone from here. Nothing was run.");
        }

        return null;
    }

    /// <summary>The definition a drop reads, in the shape the journal keeps and the Undo re-creates from (#6569, slice 2b): the create's own definition fields plus <c>is_disabled</c>.</summary>
    internal static object GuideImage(PlanGuideDefinition guide, bool disabled) =>
        new { name = guide.Name, type = guide.Type, statement = guide.Statement, module = ObjectModule(guide), module_or_batch = guide.ModuleOrBatch, @params = guide.Params, hints = guide.Hints, is_disabled = disabled };

    /// <summary>The create answer's guide image: the same fields as <see cref="GuideImage"/> without <c>is_disabled</c>, which a new guide does not have yet.</summary>
    private static object GuideCreateImage(PlanGuideDefinition guide) =>
        new { name = guide.Name, type = guide.Type, statement = guide.Statement, module = ObjectModule(guide), module_or_batch = guide.ModuleOrBatch, @params = guide.Params, hints = guide.Hints };

    /// <summary>
    /// An OBJECT guide's module name (<c>[schema].[name]</c>), under its own key. <c>module_or_batch</c> holds batch text for a SQL guide, so the data-detail
    /// masker withholds it; an object's name is kept at every level, as it is everywhere else in an answer, so the module needs a key of its own that is kept.
    /// Null for a SQL or TEMPLATE guide.
    /// </summary>
    private static string? ObjectModule(PlanGuideDefinition guide) =>
        string.Equals(guide.Type, "OBJECT", StringComparison.OrdinalIgnoreCase) ? guide.ModuleOrBatch : null;

    /// <summary>The drop tool body (#6569, slice 2b), so a test drives it with its own data sources. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunDropPlanGuideAsync(NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string? server_name, string? database_name,
        string? name, string? agent_action, bool dry_run, CancellationToken ct, string? agentTokenName = null, IReadOnlySet<string>? namedAgentNames = null)
    {
        const string tool = DropPlanGuideToolName;
        logger ??= NullLogger.Instance;
        try
        {
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, ct, NameClash.Refuse);
            if (error is not null)
                return error;

            var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: owner);

            /* 1: arguments that cannot name a guide are refused before the gate, and journaled as a block is. */
            var guideName = PlanGuideDefinition.TryName(name);
            if (guideName is null)
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadGuideName, PlanGuideDefinition.BadNameMessage, agentTokenName, namedAgentNames, ct);

            var database = database_name?.Trim();
            if (string.IsNullOrEmpty(database) || database.Length > 128 || ContainsControlCharacter(database))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, RemediationResult.Reasons.BadDatabase,
                    "database_name is the database the guide is dropped from, at most 128 characters. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            /* 2: SQL Server only, as the create is: sp_control_plan_guide is documented for SQL Server alone. */
            var (engineEdition, engineKind, _) = await DarlingEngineCapability.ReadServerEngineAsync(postgres, resolved.ServerId, ct);
            if (engineKind is not null && engineKind.Contains("postgres", StringComparison.OrdinalIgnoreCase))
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, PostgresTarget,
                    $"{resolved.DisplayName} is a PostgreSQL target. A plan guide is SQL Server only; there is no PostgreSQL equivalent.", agentTokenName, namedAgentNames, ct);
            }

            if (engineEdition is AzureSqlDatabaseEdition or AzureManagedInstanceEdition)
            {
                return await RefuseBeforeGateAsync(store, agent_action, tool, EngineNotSupported,
                    $"{resolved.DisplayName} is an Azure SQL target. Darling drops plan guides on SQL Server only: Microsoft documents sp_control_plan_guide for SQL Server alone. Nothing was run.", agentTokenName, namedAgentNames, ct);
            }

            if (owner is null)
            {
                return DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(NoOwnerSource,
                    "The service has not published its store owner connection yet, so this server's remediation login cannot be read. Try again shortly."));
            }

            /* 3: the gate, for a dry run too. The permit's Id is the guide's name and its Database is the database. */
            var target = new AgentActionTarget(resolved.ServerId, resolved.ServerName, guideName, database);
            var decision = await ServerActionGate.AuthorizeAsync(store, owner, logger, tool, ServerActionNames.DropPlanGuide, agent_action, target, ct, agentTokenName, namedAgentNames);
            if (decision.Refusal is { } gateRefusal)
                return await GateRefusalAnswerAsync(gateRefusal, dry_run, owner, logger, resolved.ServerId, resolved.ServerName, ServerActionNames.DropPlanGuide, ct);
            var permit = decision.Permit!;
            _ = InvestigationToolCore.ParseAgentAction(agent_action, out var action);

            /* 4: the prepare step: the server row, the excluded database, the one batch that reads the guide and the executor's refusals of a missing guide and a missing grant,
               and the refusal of a guide the Undo could not re-create. */
            var prepared = await PrepareDropPlanGuideAsync(owner, logger, permit, resolved.ServerId, resolved.DisplayName, database, guideName, dry_run,
                (reason, token) => store.RecordAgentActionRefusalAsync(action!.RequestId, action.AgentId, tool, reason, agentTokenName, token), ct);
            if (prepared.Answer is { } preparedAnswer)
                return preparedAnswer;
            var facts = prepared.Facts!;
            var executor = prepared.Executor!;
            var before = facts.Before;
            var statementText = facts.Statement;

            /* 5: the write, journaled. action_called holds the definition; the drop runs only while the guide still holds the statement and hints read above (guide_changed otherwise). */
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
                        var written = await WriteDropPlanGuideAsync(executor, permit, facts, token);
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
                                written.Reason, written.Message, new { dry_run = false, statement = statementText, before }));
                        done = written;
                        return JsonSerializer.Serialize(new { statement = written.Statement ?? statementText, message = written.Message }, McpHelpers.JsonOptions);
                    },
                };
                var result = await store.RunAgentActionAsync(new AgentActionCall(tool, AgentChangeKinds.PlanGuideDrop, action!, target, agentTokenName), hooks, ct);
                if (result.Refusal is { } refused)
                    return DarlingAgentActionGate.RefusalAnswer(refused);
                if (result.OutcomeUnknown && unknown is not null)
                    return Answer("outcome_unknown", null, unknown.Message, new { dry_run = false, statement = statementText, before, journal_id = result.CalledId });

                return WithWarnings(Answer("done", null,
                    (done?.Message ?? $"Plan guide {guideName} was dropped from {database}.") + " A person can undo it from Agent changes: the undo re-creates the guide from the journaled definition.",
                    new { dry_run = false, statement = statementText, before, undo = (AgentPlanUndo?)null, journal_id = result.CalledId }), result);
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
    /// A person's undo of a plan guide drop (#6569, slice 2b), run by <see cref="UndoPlanChangeAsync"/> in the same show-then-confirm shape as a plan undo. It re-creates the guide
    /// from the definition the drop journaled, through the create path (the name must be free, an OBJECT guide's module must still hold the statement, the new guide is validated and
    /// dropped again when it is invalid), and disables it when it was disabled. A name that exists now is <c>guide_exists</c>: the answer says so and nothing changes. The gate is that
    /// server's <c>drop_plan_guide</c> switch and remediation login, the switch of the action being undone.
    /// </summary>
    private static async Task<PlanUndoAnswer> DecideAndRunGuideRecreateUndoAsync(AgentChangeUndo.PlanUndoSession session, NpgsqlDataSource owner, ILogger logger,
        bool confirm, string? shownStatement, CancellationToken ct)
    {
        var facts = session.Facts;
        var guideName = facts.GuideName!;
        string? Text(string key) => facts.Guide is System.Text.Json.Nodes.JsonObject g && g[key] is System.Text.Json.Nodes.JsonValue v && v.TryGetValue(out string? s) ? s : null;
        var disabled = facts.Guide is System.Text.Json.Nodes.JsonObject d && d["is_disabled"] is System.Text.Json.Nodes.JsonValue dv && dv.TryGetValue(out bool b) && b;
        var stateNode = facts.Guide?.DeepClone();
        if (GuideUndoNameRefusal(guideName) is { } badName)
            return badName;
        /* The journaled definition goes through the same shape checks the create tool applies: hints are an OPTION (...) clause, an XML showplan or none, an OBJECT guide's module still normalizes to [schema].[name], a TEMPLATE guide has no module and has params, the length caps. */
        var (definition, _, badMessage) = PlanGuideDefinition.TryCreate(guideName, Text("statement"), Text("type"), Text("module_or_batch"), Text("params"), Text("hints"));
        if (definition is null)
            return PlanUndoAnswer.Refused("not_undoable", "The definition journaled with this drop does not pass the checks a create makes (" + badMessage!.Replace(" Nothing was run.", "", StringComparison.Ordinal) + "), so the guide cannot be re-created from it.");

        var decision = await session.AuthorizeAsync(guideName, owner, ct);
        if (decision.Refusal is { } gateRefusal)
            return PlanUndoAnswer.Refused(gateRefusal.Reason, gateRefusal.Message);
        var permit = decision.Permit!;

        var row = await ReadServerRowAsync(owner, facts.ServerId, ct);
        if (row is null)
            return PlanUndoAnswer.Refused("server_row_missing", $"{facts.ServerName} has no row in the monitored-server settings.");
        if (await GuideUndoServerRefusalAsync(session, owner, row, ct) is { } serverRefusal)
            return serverRefusal;
        var executor = new SqlServerRemediationExecutor(facts.ServerId, row.Server, row.UserName, row.SecretSlot, logger);

        var statement = PlanGuideStatement.Create(facts.Database, definition, disabled);
        var dry = await executor.CreatePlanGuideAsync(permit, definition, dryRun: true, ct, disableAfter: disabled);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await session.RecordRefusalAsync(dry.Reason, ct);
            return new PlanUndoAnswer(dry.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, dry.Reason, dry.Message, dry.Statement ?? statement, stateNode);
        }

        statement = dry.Statement ?? statement;
        var words = $"Re-creating plan guide {guideName} in {facts.Database} undoes its drop: the statement runs with the guide's hints again" + (disabled ? ", and the guide is disabled as it was." : ".");
        if (!confirm || shownStatement is null || !string.Equals(shownStatement, statement, StringComparison.Ordinal))
        {
            var lead = !confirm ? ""
                : shownStatement is null ? "A confirm must carry the statement that was shown, so nothing was written. Review this statement and confirm it. "
                : "The server's state changed since the statement was shown, so nothing was written. Review the new statement and confirm again. ";
            return new PlanUndoAnswer(PlanUndoAnswer.Confirm, null, lead + words, statement, stateNode);
        }

        var written = await executor.CreatePlanGuideAsync(permit, definition, dryRun: false, ct, disableAfter: disabled);
        if (written.Outcome == RemediationOutcome.Unknown)
        {
            logger.LogWarning("The undo of plan guide drop {CalledId} was sent, but its outcome is unknown: {Message}", facts.CalledId, written.Message);
            await session.RecordOutcomeUnknownAsync(statement);
            return new PlanUndoAnswer(PlanUndoAnswer.OutcomeUnknown, RemediationResult.Reasons.OutcomeUnknown,
                "The undo was sent to the server, but its reply was lost, so the plan guide may or may not exist again. Nothing was recorded as undone. Read sys.plan_guides before you try again; the change stays on the list, and a second try reads the server first.",
                statement, stateNode);
        }

        if (written.Outcome != RemediationOutcome.Done || !written.Wrote)
        {
            await session.RecordRefusalAsync(written.Reason ?? "write_failed", ct);
            return new PlanUndoAnswer(written.Outcome == RemediationOutcome.Refused ? PlanUndoAnswer.RefusedStatus : PlanUndoAnswer.Failed, written.Reason, written.Message, statement, stateNode);
        }

        await session.MarkServerChangedAsync(statement, stateNode, null);
        await session.JournalUndoneAsync(statement, stateNode, null, null, ct);
        return new PlanUndoAnswer(PlanUndoAnswer.Undone, null, $"Plan guide {guideName} was re-created in {facts.Database}" + (disabled ? " and disabled, as it was." : "."), statement, stateNode, null, null, facts.CalledId, session.Actor);
    }

    /* The tool methods are the last members on purpose: the payload contract census reads a tool's span up to the next tool or the end of the file, and the
       body above has its own catch, which the census must not read as this one-line method's. */
    [McpServerTool(Name = "create_plan_guide"), Description(
        "Agent arm, SQL Server only (Azure SQL is refused): creates ONE plan guide with sys.sp_create_plan_guide, so a statement runs with the OPTION (...) hints you give it (OPTIMIZE FOR, RECOMPILE, MAXDOP, USE PLAN) without a change to the application. Dry run by default: returns the exact statement and what the guide matches, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's create_plan_guide switch and remediation login (with ALTER on the module for an OBJECT guide, ALTER on the database for a SQL guide); ships off. " +
        "<<GUIDE>>" +
        "type is OBJECT (module_or_batch is the two-part schema.name of a procedure, function or trigger, and statement must appear exactly in its definition) or SQL (module_or_batch is the batch text, or empty when statement is the batch; params is the parameter list for a parameterized batch) or TEMPLATE (the guide applies to every statement that parameterizes to the form in statement: module_or_batch stays empty, params is required, and hints is OPTION (PARAMETERIZATION SIMPLE) or OPTION (PARAMETERIZATION FORCED); get statement and params for a query from sys.sp_get_query_template, because the parameterized text has to match exactly). " +
        "hints has three forms. (1) An OPTION (...) clause, for example OPTION (RECOMPILE) or OPTION (MAXDOP 1): the usual form. (2) An XML showplan (text that starts with <, optionally after an <?xml ...?> line): the statement is compiled to that plan, so use it only for a plan you took from the plan cache or Query Store for this same statement, and read the guide's validation result. (3) Empty: the guide applies no hints and removes the OPTION hints the statement itself carries, so use it only when that is what the person asked for. Table hints that change isolation or locking (NOLOCK, READUNCOMMITTED, READPAST, TABLOCKX, XLOCK and the like) change which rows the query returns or how it blocks other sessions: use them only when the person asked for them, or after comparing the query's results and run time with and without the hint, and state what you compared in your report. Every value goes to the server as a parameter, never as batch text. " +
        "Before it creates, the guide is matched and reported: an OBJECT guide's module must resolve and hold the statement, a SQL guide reports how many cached batches compiled in its database equal its text (zero is allowed: the guide then matches nothing cached right now); a TEMPLATE guide reports no count, because it matches by the statement's parameterized form, and needs ALTER on the database like a SQL guide. A name already used in the database is refused. After it creates, the guide is validated with sys.fn_validate_plan_guide in the same batch: an invalid guide is dropped again and the call is journaled failed with the server's message. " +
        "This tool blocks only a call that cannot do anything: bad_guide_name, bad_guide_type, bad_guide_argument, bad_module_name, bad_database, engine_not_supported, guide_exists, module_not_found, not_a_module, statement_not_in_module, guide_permission_denied, guide_invalid. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_guide_create) with the whole definition, and a person can undo it there: the undo drops the guide by name. Answers status dry_run, done, refused (reason) or failed, with statement, before, matches and journal_id.")]
    public static Task<string> CreatePlanGuide(
        NpgsqlDataSource postgres,
        [Description("The database the guide is created in.")] string database_name,
        [Description("The plan guide's name: 1 to 128 characters, not starting with #. A name already used in the database is refused.")] string name,
        [Description("The text of the statement the guide applies to, exactly as written in the module or batch (case and spacing included).")] string statement,
        [Description("OBJECT, SQL or TEMPLATE. TEMPLATE: module_or_batch empty, params required, hints OPTION (PARAMETERIZATION SIMPLE|FORCED); statement and params come from sys.sp_get_query_template.")] string type,
        [Description("An OPTION (...) clause, for example OPTION (RECOMPILE) or OPTION (MAXDOP 1); or an XML showplan (starts with <); or empty for no hints (removes the statement's own OPTION hints).")] string? hints = null,
        [Description("OBJECT: the two-part schema.name of the procedure, function or trigger. SQL: the batch text, or empty when statement is the whole batch.")] string? module_or_batch = null,
        [Description("SQL: the parameter list of a parameterized batch, for example '@p int'; empty for none. TEMPLATE: required. OBJECT: empty.")] string? @params = null,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and what the guide matches, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunCreatePlanGuideAsync(postgres, ownerSource?.Read(), logger, server_name, database_name, name, statement, type, module_or_batch, @params, hints, agent_action, dry_run, cancellationToken,
            served?.Policy.AgentTokenName, served?.NamedAgentNames);

    [McpServerTool(Name = "drop_plan_guide"), Description(
        "Agent arm, SQL Server only (an Azure SQL target is refused as engine_not_supported): drops ONE plan guide by name with sys.sp_control_plan_guide N'DROP', so the statement it applied to runs without the guide's hints again. Dry run by default: reads the guide and returns the exact statement and the whole definition that would be journaled, and writes nothing. A write needs dry_run=false. Needs agent_action at Level 3 plus that server's drop_plan_guide switch and remediation login (with ALTER on the module for an OBJECT guide, ALTER on the database for a SQL guide); ships off. " +
        "<<GUIDE>>" +
        "The guide is read from sys.plan_guides in the database_name you give, in the same batch that drops it. A name that is not there is refused as guide_not_found. Every guide form is dropped: OBJECT, SQL and TEMPLATE (a SQL or TEMPLATE guide needs ALTER on the database), with OPTION, XML showplan or no (NULL) hints, and the Undo re-creates any of them from the journaled definition. Read the guide with a dry run first and check the name: dropping a guide stops its hints or its forced plan applying to the statement. " +
        "This tool blocks only a call that cannot do anything: bad_guide_name, bad_database, engine_not_supported, guide_not_found, guide_changed (the guide was replaced between the read and the drop), guide_permission_denied. " +
        "mcp.allowWrites never bypasses the gate, and a dry run needs it too (it signs in with the remediation login). The change is journaled in Agent changes (kind plan_guide_drop) with the whole definition (name, type, statement, module_or_batch, params, hints, is_disabled), and a person can undo it there: the undo re-creates the guide from that definition (validated, and disabled again if it was disabled), and is refused as guide_exists if a guide with that name exists by then. Answers status dry_run, done, refused (reason) or failed, with statement, before and journal_id.")]
    public static Task<string> DropPlanGuide(
        NpgsqlDataSource postgres,
        [Description("The database the guide is dropped from.")] string database_name,
        [Description("The plan guide's name, as sys.plan_guides shows it.")] string name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Required: the agent_action object (request_id, claim_id, agent_id, reason, evidence); see the tool guide. The reason is recorded with the change.")] string? agent_action = null,
        [Description("Default true: return the exact statement and the definition that would be journaled, write nothing. Pass false to write.")] bool dry_run = true,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        DarlingMcpServedPolicy? served = null,
        CancellationToken cancellationToken = default) =>
        RunDropPlanGuideAsync(postgres, ownerSource?.Read(), logger, server_name, database_name, name, agent_action, dry_run, cancellationToken, served?.Policy.AgentTokenName, served?.NamedAgentNames);
}
