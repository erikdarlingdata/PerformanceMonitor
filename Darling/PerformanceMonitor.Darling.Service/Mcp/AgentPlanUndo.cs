/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>The call that puts a plan change back: the tool and the arguments it needs, in the answer's snake_case (#6207, part of #5632).</summary>
public sealed record AgentPlanUndo(
    [property: JsonPropertyName("tool")] string Tool,
    [property: JsonPropertyName("arguments")] AgentPlanUndoArguments Arguments,
    [property: JsonPropertyName("note"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Note);

/// <summary>The arguments of an <see cref="AgentPlanUndo"/> call. <c>server_name</c> is the registry name the call resolved to.</summary>
public sealed record AgentPlanUndoArguments(
    [property: JsonPropertyName("server_name")] string ServerName,
    [property: JsonPropertyName("database_name")] string DatabaseName,
    [property: JsonPropertyName("query_id")] long QueryId,
    [property: JsonPropertyName("plan_id")] long PlanId);

/// <summary>
/// Builds the <c>undo</c> a written force or unforce answers (#6207, part of #5632). Pure, and it takes plain values (not the executor's types) from the before image the tool already holds, so a test
/// covers every case without a server. An eviction has no undo (the next run compiles a new plan), so it has no builder: its answer carries a null.
/// </summary>
public static class AgentPlanUndoBuilder
{
    /// <summary>
    /// The undo of a made change, or null when there is nothing to put back. <paramref name="force"/> true is a force of <paramref name="planId"/>:
    /// with no plan forced before the undo unforces it, and with another plan P forced before the undo forces P again (an AUTO force of P becomes a
    /// manual one, and the note says so). A force of the plan that is already forced is blocked before the write, so it has no undo here. An unforce is
    /// undone by forcing the same plan.
    /// </summary>
    public static AgentPlanUndo? For(bool force, string serverName, string databaseName, long queryId, long planId, long? forcedBeforePlanId, string? forcedBeforeType)
    {
        if (!force)
            return Build(DarlingMcpPlanForceTools.ForceToolName, serverName, databaseName, queryId, planId, null);
        if (forcedBeforePlanId is not { } before)
            return Build(DarlingMcpPlanForceTools.UnforceToolName, serverName, databaseName, queryId, planId, null);
        if (before == planId)
            return null;
        var note = string.Equals(forcedBeforeType, "AUTO", StringComparison.OrdinalIgnoreCase)
            ? string.Create(CultureInfo.InvariantCulture, $"Plan {before} was forced by automatic plan correction. Forcing it again makes it a manual force, not an automatic one.")
            : null;
        return Build(DarlingMcpPlanForceTools.ForceToolName, serverName, databaseName, queryId, before, note);
    }

    private static AgentPlanUndo Build(string tool, string serverName, string databaseName, long queryId, long planId, string? note) =>
        new(tool, new AgentPlanUndoArguments(serverName, databaseName, queryId, planId), note);
}
