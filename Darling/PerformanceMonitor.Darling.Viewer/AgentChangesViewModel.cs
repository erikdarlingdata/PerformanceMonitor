/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The Agent changes list's data and writes for the Viewer (#5917). The Viewer signs in as the store admin and reads in
/// full; the undo, the list and the switch are the storage layer's <see cref="AgentChangeUndo"/>, the one the web routes share.</summary>
public sealed partial class ViewerDataService
{
    /// <summary>Reads the agent calls, the refusals and the switch.</summary>
    public Task<AgentChangesRead> ReadAgentChangesAsync(CancellationToken cancellationToken = default) =>
        new AgentChangeUndo(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).ReadAsync(200, cancellationToken);

    /// <summary>Undoes one agent change as <paramref name="login"/>; a refusal comes back as the result, a read-only role throws.</summary>
    public async Task<AgentUndoResult> UndoAgentChangeAsync(long calledId, string login, CancellationToken cancellationToken = default)
    {
        try
        {
            return await new AgentChangeUndo(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).UndoAsync(calledId, login, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    /// <summary>The command type for a person's undo of a plan force or unforce (#5632, step 9); matches the service executor's <c>undo_plan_change</c> case.</summary>
    public const string CommandUndoPlanChange = "undo_plan_change";

    /// <summary>The wait for an undo's answer: the service reads the server, then writes to it.</summary>
    public static readonly TimeSpan UndoPlanChangeTimeout = TimeSpan.FromSeconds(90);

    /// <summary>
    /// A person's undo of a plan force or unforce (#5632, step 9). The Viewer cannot write to a monitored server and cannot resolve its
    /// remediation credential, so it asks the service through the command queue and reads the answer from <c>result_json</c>.
    /// Without <paramref name="confirm"/> the answer is the exact statement and the state read, and nothing is written; with it (and the
    /// statement that was shown) the service writes. <paramref name="login"/> is the person, sent as <c>requested_by</c>. A read-only role throws.
    /// </summary>
    public async Task<PlanUndoAnswer> UndoPlanChangeAsync(long calledId, string login, bool confirm, string? statement, CancellationToken cancellationToken = default)
    {
        _ = AgentChangeUndo.PersonActor(login);
        var args = System.Text.Json.JsonSerializer.Serialize(new { called_id = calledId, confirm, statement });
        var commandId = await EnqueueCommandAsync(CommandUndoPlanChange, targetServerId: null, args, login, cancellationToken);
        var result = await PollCommandResultAsync(commandId, UndoPlanChangeTimeout, cancellationToken);
        if (result is null)
        {
            return new PlanUndoAnswer(PlanUndoAnswer.Failed, "timed_out", "The service is running the undo and has not answered yet. Look at the list again later before trying twice.");
        }

        try
        {
            await DeleteCommandAsync(commandId, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The row holds no secret; the service-side reaper is the backstop. */
        }

        return PlanUndoAnswer.TryRead(result.ResultJson)
            ?? new PlanUndoAnswer(PlanUndoAnswer.RefusedStatus, result.ResultStatus, ReadError(result.ResultJson) ?? "The service could not run the undo.");
    }

    private static string? ReadError(string? json)
    {
        try
        {
            return json is not null && System.Text.Json.Nodes.JsonNode.Parse(json) is System.Text.Json.Nodes.JsonObject o && o["error"] is System.Text.Json.Nodes.JsonValue v && v.TryGetValue(out string? text) ? text : null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }

    /// <summary>Turns every agent change on or off and journals it by the person. False when the store has no service row yet: nothing was
    /// written (<see cref="AgentChangeUndo.SwitchStoreNotSetUpMessage"/>).</summary>
    public async Task<bool> SetAgentActionsEnabledAsync(bool enabled, string login, CancellationToken cancellationToken = default)
    {
        try
        {
            return await new AgentChangeUndo(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).SetEnabledAsync(enabled, login, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }
}

/// <summary>One row of the Agent changes grid (#5917).</summary>
public sealed class AgentChangeViewRow
{
    public AgentChangeViewRow(AgentChangeRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        Row = row;
    }

    /// <summary>The store row.</summary>
    public AgentChangeRow Row { get; }

    public long CalledId => Row.CalledId;

    /// <summary>The call time, the sort key of the When column.</summary>
    public DateTime CalledAtUtc => Row.CalledAtUtc;

    public string WhenDisplay => Row.CalledAtUtc.ToLocalTime().ToString("g", CultureInfo.CurrentCulture);

    public string Server => Row.ServerLabel;

    /// <summary>The kind word, in plain words for the two plan force kinds (#5632) and as journaled for the rest.</summary>
    public string Kind => Row.Kind switch
    {
        AgentChangeKinds.PlanForce => "force plan",
        AgentChangeKinds.PlanUnforce => "unforce plan",
        AgentChangeKinds.PlanEvict => "evict plan",
        AgentChangeKinds.SessionKill => "kill session",
        AgentChangeKinds.ModuleRecompile => "recompile module",
        AgentChangeKinds.PlanGuideCreate => "create plan guide",
        AgentChangeKinds.PlanGuideDrop => "drop plan guide",
        var other => other ?? "",
    };

    public string Tool => Row.Tool ?? "";

    public string Agent => Row.Actor;

    public string Why => Row.Reason ?? "";

    /// <summary>What the Result column says. A call with no done or failed row is "result unknown": the change may or may not have
    /// happened, so it has no Undo.</summary>
    public string ResultDisplay => ResultText(Row);

    public string MutedDisplay => Row.MutedAlerts is int n ? n.ToString(CultureInfo.InvariantCulture) + " alerts muted since" : "";

    /// <summary>The Review column (#5632, step 8): whether an agent kept or removed a plan force after Darling's checks. Empty for any other change.</summary>
    public string ReviewDisplay => Row.ForceReview?.Words ?? "";

    /// <summary>True when Undo may be tried now.</summary>
    public bool CanUndo => Row.CanTryUndo(DateTime.UtcNow);

    internal static string ResultText(AgentChangeRow row)
    {
        if (row.UndoneAtUtc is not null)
            return "undone by " + (row.UndoneBy ?? "a person");
        return row.Result switch
        {
            AgentChangeResult.Done => "done",
            AgentChangeResult.Failed => "failed",
            _ => "result unknown",
        };
    }
}

/// <summary>One row of the refusals grid (#5917).</summary>
public sealed class AgentRefusalViewRow
{
    public AgentRefusalViewRow(AgentRefusalRow row)
    {
        ArgumentNullException.ThrowIfNull(row);
        Row = row;
    }

    public AgentRefusalRow Row { get; }

    /// <summary>The refusal time, the sort key of the Refused at column.</summary>
    public DateTime AtUtc => Row.AtUtc;

    public string WhenDisplay => Row.AtUtc.ToLocalTime().ToString("g", CultureInfo.CurrentCulture);

    public string Server => Row.ServerLabel;

    public string Tool => Row.Tool ?? "";

    public string Agent => Row.Actor;

    public string Code => Row.Code ?? "";

    public static IReadOnlyList<AgentRefusalViewRow> From(IEnumerable<AgentRefusalRow> rows) => rows.Select(r => new AgentRefusalViewRow(r)).ToList();
}
