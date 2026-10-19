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

    /// <summary>Turns every agent change on or off and journals it by the person.</summary>
    public async Task SetAgentActionsEnabledAsync(bool enabled, string login, CancellationToken cancellationToken = default)
    {
        try
        {
            await new AgentChangeUndo(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds).SetEnabledAsync(enabled, login, cancellationToken);
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

    public string Server => Row.ServerId is int id ? "Server " + id.ToString(CultureInfo.InvariantCulture) : "";

    /// <summary>The kind word, in plain words for the two plan force kinds (#5632) and as journaled for the rest.</summary>
    public string Kind => Row.Kind switch
    {
        AgentChangeKinds.PlanForce => "force plan",
        AgentChangeKinds.PlanUnforce => "unforce plan",
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

    public string Server => Row.ServerId is int id ? "Server " + id.ToString(CultureInfo.InvariantCulture) : "";

    public string Tool => Row.Tool ?? "";

    public string Agent => Row.Actor;

    public string Code => Row.Code ?? "";

    public static IReadOnlyList<AgentRefusalViewRow> From(IEnumerable<AgentRefusalRow> rows) => rows.Select(r => new AgentRefusalViewRow(r)).ToList();
}
