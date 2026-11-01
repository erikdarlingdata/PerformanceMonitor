/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The prepare step and the write step of the plan tools (#7108, child d0 of #6980): force and unforce, evict, recompile, plan guide create and plan guide drop.
/// <para><b>Prepare</b> runs every check the tool ran inline before it wrote: the store owner's server row, the excluded database, the server's state read,
/// the block reason and the executor's dry run. It takes stored facts (the server id and the parsed target fields) and a permit, never the
/// <c>agent_action</c> text, and it returns the facts that decide the change (see <see cref="PlanPrepared{T}"/>). A refusal is journaled through the
/// <see cref="PlanRefusalSink"/> the caller passes, so the agent's path journals as it always did and a later caller can journal elsewhere.</para>
/// <para><b>Write</b> takes those facts and makes the change. Where the design's binding table says the write checks the statement, it builds the
/// statement from the permit's own target and refuses with <see cref="StatementChanged"/> when it is not the prepared one. The tools call prepare
/// and then write, so every tool answers as it did before the split.</para>
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    /// <summary>The write would run another statement than the one prepared. Nothing is written.</summary>
    internal const string StatementChanged = "statement_changed";

    /// <summary>Journals one refusal reason of a prepare step. The agent's path writes it with the call's request and agent.</summary>
    internal delegate Task PlanRefusalSink(string reason, CancellationToken ct);

    /// <summary>
    /// What a prepare step returns. <see cref="Facts"/> is set when every check passed; <see cref="Executor"/> is the executor the write step uses.
    /// <see cref="Answer"/> is the answer the tool gives now: always set for a refusal or a failure, and set for a passed dry run when the caller asked for one.
    /// </summary>
    internal sealed record PlanPrepared<T>(T? Facts, SqlServerRemediationExecutor? Executor, string? Answer) where T : class
    {
        internal static PlanPrepared<T> End(string answer) => new(null, null, answer);
    }

    /// <summary>Force or unforce. Binding: <see cref="Statement"/>, <see cref="ForcedPlanId"/> (the plan of the query forced now) and <see cref="IsForced"/> (the asked plan). <see cref="Before"/> is the image the journal keeps.</summary>
    internal sealed record ForcePlanFacts(bool Force, string Database, long QueryId, long PlanId, string Statement, long? ForcedPlanId, bool IsForced, PlanForceState Before);

    /// <summary>Evict. Binding: <see cref="Statement"/>; the dry run proved the handle is cached. <see cref="Before"/> comes from statistics that change every minute, so it is not binding.</summary>
    internal sealed record EvictPlanFacts(string Handle, string Statement, object Before, string[] Warnings);

    /// <summary>Recompile. Binding: <see cref="Statement"/> and <see cref="ObjectType"/>. <see cref="Before"/> is the journal image.</summary>
    internal sealed record RecompileModuleFacts(string Database, string Module, string Statement, string? ObjectType, object Before);

    /// <summary>Plan guide create. Binding: <see cref="Statement"/>. <see cref="Before"/> is the journal image.</summary>
    internal sealed record CreatePlanGuideFacts(string Database, PlanGuideDefinition Definition, string Statement, object Before);

    /// <summary>Plan guide drop. Binding: <see cref="Guide"/>, the full definition read, which the drop itself checks again on the server.</summary>
    internal sealed record DropPlanGuideFacts(string Database, string GuideName, PlanGuideDefinition Guide, bool Disabled, string Statement, object Before);

    /// <summary>Reads the server row, refuses an excluded database (when <paramref name="database"/> is set) and builds the executor, in the order the tools always did.</summary>
    private static async Task<(SqlServerRemediationExecutor? Executor, string? Answer)> OpenExecutorAsync(NpgsqlDataSource owner, ILogger logger, int serverId,
        string serverDisplayName, string? database, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var row = await ReadServerRowAsync(owner, serverId, ct);
        if (row is null)
            return (null, Answer("failed", "server_row_missing", $"{serverDisplayName} has no row in the monitored-server settings."));

        /* A database the server's entry excludes is one Darling does not act in (#5632). Refused before the dry run reads anything. */
        if (database is not null && IsExcludedDatabase(row.ExcludedDatabases, database))
        {
            await onRefusal(AzureDatabaseExcluded, ct);
            return (null, DarlingAgentActionGate.RefusalAnswer(ExcludedDatabaseRefusal(database)));
        }

        return (new SqlServerRemediationExecutor(serverId, row.Server, row.UserName, row.SecretSlot, logger), null);
    }

    /// <summary>The refusal the executor's dry run gave, journaled, and its answer. A failure is only answered.</summary>
    private static async Task<string> DryNotDoneAsync(RemediationResult dry, bool dryRun, string statement, object? before, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
            await onRefusal(dry.Reason, ct);
        return Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
            new { dry_run = dryRun, statement = dry.Statement ?? statement, before });
    }

    private static RemediationResult StatementChangedResult(string action, string prepared, string now) =>
        new(RemediationOutcome.Refused, action, false, false, StatementChanged,
            $"The statement this change would run is not the one that was prepared (prepared: {prepared} now: {now}). Nothing was written.");

    // ---- force and unforce ----

    internal static async Task<PlanPrepared<ForcePlanFacts>> PrepareForcePlanAsync(NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit, int serverId,
        string serverDisplayName, bool force, string database, long queryId, long planId, bool dryRun, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var (executor, ended) = await OpenExecutorAsync(owner, logger, serverId, serverDisplayName, database, onRefusal, ct);
        if (ended is not null)
            return PlanPrepared<ForcePlanFacts>.End(ended);

        /* The before state, read on the server. */
        var before = await executor!.ReadPlanStateAsync(permit, queryId, planId, ct);
        if (before.State is not { } beforeState)
        {
            /* The before read could not finish: a refusal of the remediation code (no login, a slot that cannot be read) is journaled; a failure is only answered. */
            if (before.Outcome == RemediationOutcome.Refused && before.Reason is not null)
                await onRefusal(before.Reason, ct);
            return PlanPrepared<ForcePlanFacts>.End(Answer(before.Outcome == RemediationOutcome.Refused ? "refused" : "failed", before.Reason, before.Message));
        }

        if (BlockReason(beforeState, force) is { } block)
        {
            await onRefusal(block.Reason, ct);
            return PlanPrepared<ForcePlanFacts>.End(Answer("refused", block.Reason, block.Message, new { dry_run = dryRun, before = beforeState }));
        }

        var statement = PlanForceStatement.For(beforeState.Database, queryId, planId, force);
        var facts = new ForcePlanFacts(force, beforeState.Database, queryId, planId, statement, beforeState.ForcedPlan?.PlanId, beforeState.RequestedPlan!.IsForced, beforeState);
        if (!dryRun)
            return new PlanPrepared<ForcePlanFacts>(facts, executor, null);

        /* A dry run runs the executor's own checks and journals nothing. */
        var dry = force
            ? await executor.ForcePlanAsync(permit, queryId, planId, dryRun: true, ct)
            : await executor.UnforcePlanAsync(permit, queryId, planId, dryRun: true, ct);
        return ForceDryRunPrepared(dry, facts, executor);
    }

    /// <summary>
    /// The prepare result after a force or unforce dry run. A dry run that does not pass ends with its answer and no facts or executor, as the other prepares
    /// do, so a caller cannot key on facts of a change that was refused or failed.
    /// </summary>
    internal static PlanPrepared<ForcePlanFacts> ForceDryRunPrepared(RemediationResult dry, ForcePlanFacts facts, SqlServerRemediationExecutor executor)
    {
        var state = facts.Before;
        if (dry.Outcome != RemediationOutcome.Done)
            return PlanPrepared<ForcePlanFacts>.End(Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
                new { dry_run = true, statement = facts.Statement, before = state }));
        return new PlanPrepared<ForcePlanFacts>(facts, executor,
            Answer("dry_run", null, dry.Message, new { dry_run = true, statement = dry.Statement ?? facts.Statement, before = state }));
    }

    /// <summary>
    /// Makes the change. Checks the statement: the one the permit's database gives must be the one prepared. The statement is built from the permit's database
    /// and the facts' own ids, so this check catches a different database only. A caller that approves a stored change compares the fresh prepare facts with the stored binding.
    /// </summary>
    internal static Task<RemediationResult> WriteForcePlanAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, ForcePlanFacts facts, CancellationToken ct)
    {
        if (permit.Database is { Length: > 0 } database)
        {
            var now = PlanForceStatement.For(database, facts.QueryId, facts.PlanId, facts.Force);
            if (!string.Equals(now, facts.Statement, StringComparison.Ordinal))
                return Task.FromResult(StatementChangedResult(facts.Force ? "force_plan" : "unforce_plan", facts.Statement, now));
        }

        return facts.Force
            ? executor.ForcePlanAsync(permit, facts.QueryId, facts.PlanId, dryRun: false, ct)
            : executor.UnforcePlanAsync(permit, facts.QueryId, facts.PlanId, dryRun: false, ct);
    }

    // ---- evict ----

    internal static async Task<PlanPrepared<EvictPlanFacts>> PrepareEvictPlanAsync(NpgsqlDataSource postgres, NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit,
        int serverId, string serverDisplayName, string handle, bool dryRun, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var (executor, ended) = await OpenExecutorAsync(owner, logger, serverId, serverDisplayName, null, onRefusal, ct);
        if (ended is not null)
            return PlanPrepared<EvictPlanFacts>.End(ended);

        /* The before image comes from the store, not the server: what Darling collected for this handle in the last 24 hours. */
        var (statements, objectName) = await DarlingPlanEvictStore.ReadBeforeAsync(postgres, serverId, handle, DateTime.UtcNow, ct);
        var warnings = EvictWarnings(statements.Count);
        var before = new { plan_handle = handle, object_name = objectName, statements };
        var statement = PlanEvictStatement.For(handle);

        /* The executor's own checks run on a dry run and before a write: edition, the grant, and that the handle is cached now. */
        var dry = await executor!.EvictPlanAsync(permit, handle, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done)
        {
            if (dry.Outcome == RemediationOutcome.Refused && dry.Reason is not null)
                await onRefusal(dry.Reason, ct);
            return PlanPrepared<EvictPlanFacts>.End(Answer(dry.Outcome == RemediationOutcome.Refused ? "refused" : "failed", dry.Reason, dry.Message,
                new { dry_run = dryRun, statement = dry.Statement ?? statement, before, warnings }));
        }

        var facts = new EvictPlanFacts(handle, statement, before, warnings);
        return new PlanPrepared<EvictPlanFacts>(facts, executor, dryRun
            ? Answer("dry_run", null, dry.Message + " An eviction has no undo.", new { dry_run = true, statement = dry.Statement ?? statement, before, warnings })
            : null);
    }

    internal static Task<RemediationResult> WriteEvictPlanAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, EvictPlanFacts facts, CancellationToken ct)
    {
        /* The statement is rebuilt from the permit's own handle, so this check catches a permit for another handle only (#7108). */
        if (PlanEvictStatement.TryNormalize(permit.Id, out var normal) && PlanEvictStatement.For(normal) is { } now
            && !string.Equals(now, facts.Statement, StringComparison.Ordinal))
            return Task.FromResult(StatementChangedResult("evict_plan", facts.Statement, now));
        return executor.EvictPlanAsync(permit, facts.Handle, dryRun: false, ct);
    }

    // ---- recompile ----

    internal static async Task<PlanPrepared<RecompileModuleFacts>> PrepareRecompileModuleAsync(NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit, int serverId,
        string serverDisplayName, string database, string module, bool dryRun, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var (executor, ended) = await OpenExecutorAsync(owner, logger, serverId, serverDisplayName, database, onRefusal, ct);
        if (ended is not null)
            return PlanPrepared<RecompileModuleFacts>.End(ended);
        var statement = ModuleRecompileStatement.For(database, module);

        /* The executor's own checks run on a dry run and before a write: the name resolves, it is a procedure, function or trigger, the grant. */
        var dry = await executor!.RecompileModuleAsync(permit, module, dryRun: true, ct);
        /* The journal and the answer name the object the server resolved (its schema and name), not the input text. Before it resolves, only the input is known. */
        var resolvedModule = dry.ObjectSchema is null || dry.ObjectName is null
            ? module
            : "[" + dry.ObjectSchema.Replace("]", "]]", StringComparison.Ordinal) + "].[" + dry.ObjectName.Replace("]", "]]", StringComparison.Ordinal) + "]";
        var before = new { database, module = resolvedModule, schema = dry.ObjectSchema, name = dry.ObjectName, object_type = dry.ObjectType };
        if (dry.Outcome != RemediationOutcome.Done)
            return PlanPrepared<RecompileModuleFacts>.End(await DryNotDoneAsync(dry, dryRun, statement, before, onRefusal, ct));

        var facts = new RecompileModuleFacts(database, module, statement, dry.ObjectType, before);
        return new PlanPrepared<RecompileModuleFacts>(facts, executor, dryRun
            ? Answer("dry_run", null, dry.Message + " A recompile has no undo.", new { dry_run = true, statement = dry.Statement ?? statement, before })
            : null);
    }

    internal static Task<RemediationResult> WriteRecompileModuleAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, RecompileModuleFacts facts, CancellationToken ct)
    {
        /* The statement is rebuilt from the permit's own database and module, so this check catches a permit for another target only (#7108). */
        if (permit.Database is { Length: > 0 } database && permit.Id is { Length: > 0 } module)
        {
            var now = ModuleRecompileStatement.For(database, module);
            if (!string.Equals(now, facts.Statement, StringComparison.Ordinal))
                return Task.FromResult(StatementChangedResult("recompile_module", facts.Statement, now));
        }

        return executor.RecompileModuleAsync(permit, facts.Module, dryRun: false, ct);
    }

    // ---- plan guide create ----

    internal static async Task<PlanPrepared<CreatePlanGuideFacts>> PrepareCreatePlanGuideAsync(NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit, int serverId,
        string serverDisplayName, string database, PlanGuideDefinition definition, bool dryRun, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var (executor, ended) = await OpenExecutorAsync(owner, logger, serverId, serverDisplayName, database, onRefusal, ct);
        if (ended is not null)
            return PlanPrepared<CreatePlanGuideFacts>.End(ended);
        var statementText = PlanGuideStatement.Create(database, definition);

        /* The executor's own checks run on a dry run and before a write: the name is free, an OBJECT guide's module resolves and holds the statement, the grants. */
        var dry = await executor!.CreatePlanGuideAsync(permit, definition, dryRun: true, ct);
        var before = new
        {
            database,
            guide = GuideCreateImage(definition),
            cached_matches = dry.CachedMatches,
        };
        if (dry.Outcome != RemediationOutcome.Done)
            return PlanPrepared<CreatePlanGuideFacts>.End(await DryNotDoneAsync(dry, dryRun, statementText, before, onRefusal, ct));

        var facts = new CreatePlanGuideFacts(database, definition, statementText, before);
        return new PlanPrepared<CreatePlanGuideFacts>(facts, executor, dryRun
            ? Answer("dry_run", null, dry.Message + " A person can undo the create from Agent changes: the undo drops the guide.",
                new { dry_run = true, statement = dry.Statement ?? statementText, before, matches = dry.CachedMatches })
            : null);
    }

    internal static Task<RemediationResult> WriteCreatePlanGuideAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, CreatePlanGuideFacts facts, CancellationToken ct)
    {
        /* The statement is rebuilt from the permit's own target plus the facts' ids, so this check catches a different database only (#7108). */
        if (permit.Database is { Length: > 0 } database)
        {
            var now = PlanGuideStatement.Create(database, facts.Definition);
            if (!string.Equals(now, facts.Statement, StringComparison.Ordinal))
                return Task.FromResult(StatementChangedResult("create_plan_guide", facts.Statement, now));
        }

        return executor.CreatePlanGuideAsync(permit, facts.Definition, dryRun: false, ct);
    }

    // ---- plan guide drop ----

    internal static async Task<PlanPrepared<DropPlanGuideFacts>> PrepareDropPlanGuideAsync(NpgsqlDataSource owner, ILogger logger, ServerActionPermit permit, int serverId,
        string serverDisplayName, string database, string guideName, bool dryRun, PlanRefusalSink onRefusal, CancellationToken ct)
    {
        var (executor, ended) = await OpenExecutorAsync(owner, logger, serverId, serverDisplayName, database, onRefusal, ct);
        if (ended is not null)
            return PlanPrepared<DropPlanGuideFacts>.End(ended);
        var statementText = PlanGuideStatement.Drop(database, guideName);

        /* One batch in the request's database reads the guide, and the executor refuses a missing guide and a missing grant before anything is dropped. */
        var dry = await executor!.DropPlanGuideAsync(permit, guideName, expected: null, dryRun: true, ct);
        if (dry.Outcome != RemediationOutcome.Done || dry.Guide is null)
            return PlanPrepared<DropPlanGuideFacts>.End(await DryNotDoneAsync(dry, dryRun, statementText, new { database }, onRefusal, ct));

        var read = dry.Guide;

        /* The Undo re-creates the guide through the create's own checks, so a guide that would not pass them (hints or statement over the 4,000,000-character cap,
           an OBJECT guide on a module name that does not normalize) is not dropped: the drop could not be undone. Refused on a dry run as well as a write, before the gate's permit is used. */
        var (undoable, _, undoMessage) = PlanGuideDefinition.TryCreate(read.Name, read.Statement, read.Type, read.ModuleOrBatch, read.Params, read.Hints);
        if (undoable is null)
        {
            var notUndoable = "Plan guide " + guideName + " was not dropped: its definition does not pass the checks a create makes ("
                + undoMessage!.Replace(" Nothing was run.", "", StringComparison.Ordinal) + "), so the Undo could not re-create it. Nothing was run.";
            await onRefusal(GuideNotUndoable, ct);
            return PlanPrepared<DropPlanGuideFacts>.End(Answer("refused", GuideNotUndoable, notUndoable, new { dry_run = dryRun, statement = dry.Statement ?? statementText, before = new { database } }));
        }

        var disabled = dry.GuideDisabled == true;
        var before = new { database, guide = GuideImage(read, disabled) };
        var facts = new DropPlanGuideFacts(database, guideName, read, disabled, statementText, before);
        return new PlanPrepared<DropPlanGuideFacts>(facts, executor, dryRun
            ? Answer("dry_run", null, dry.Message + " A person can undo the drop from Agent changes: the undo re-creates the guide from the definition journaled with the drop.",
                new { dry_run = true, statement = dry.Statement ?? statementText, before })
            : null);
    }

    /// <summary>Makes the change. The drop runs only while the guide still holds the full definition prepared (<c>guide_changed</c> otherwise), which is the binding's check.</summary>
    internal static Task<RemediationResult> WriteDropPlanGuideAsync(SqlServerRemediationExecutor executor, ServerActionPermit permit, DropPlanGuideFacts facts, CancellationToken ct) =>
        executor.DropPlanGuideAsync(permit, facts.GuideName, facts.Guide, dryRun: false, ct);
}
