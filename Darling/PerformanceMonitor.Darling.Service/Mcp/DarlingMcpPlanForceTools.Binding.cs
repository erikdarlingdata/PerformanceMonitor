/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The binding of each Level 3 tool (#7109): the facts a proposal stores and a press compares. Both sides build them here from the prepare step's
/// result, so they cannot drift apart. A press that read the same facts gets the same keys and values, and any real change shows as a changed key.
/// What changes every minute and does not decide the change (evict's before image from collected statistics, the journal images) stays out.
/// </summary>
public sealed partial class DarlingMcpPlanForceTools
{
    /// <summary>Force and unforce: the statement, the plan forced now and whether the asked plan is forced.</summary>
    internal static JsonObject ForcePlanBinding(ForcePlanFacts facts) =>
        BindingObject(new { statement = facts.Statement, forced_plan_id = facts.ForcedPlanId, is_forced = facts.IsForced });

    /// <summary>Evict: the statement. The dry run proved the handle is cached. The before image is not binding.</summary>
    internal static JsonObject EvictPlanBinding(EvictPlanFacts facts) => BindingObject(new { statement = facts.Statement });

    /// <summary>Recompile: the statement and the object type the server reported.</summary>
    internal static JsonObject RecompileModuleBinding(RecompileModuleFacts facts) =>
        BindingObject(new { statement = facts.Statement, object_type = facts.ObjectType });

    /// <summary>Create plan guide: the statement.</summary>
    internal static JsonObject CreatePlanGuideBinding(CreatePlanGuideFacts facts) => BindingObject(new { statement = facts.Statement });

    /// <summary>Drop plan guide: the full definition read on the server, its disabled flag and the statement.</summary>
    internal static JsonObject DropPlanGuideBinding(DropPlanGuideFacts facts) =>
        BindingObject(new { guide = GuideImage(facts.Guide), disabled = facts.Disabled, statement = facts.Statement });

    /// <summary>
    /// SQL Server and Azure kill: the session, its login time and <c>last_request_end_time</c> (UTC, with the server-local values the kill batch
    /// compares), its status and open transaction count. A session that ran a new batch since the proposal has a new last end time, so the press
    /// ends the proposal <c>superseded</c>. A batch between the confirming press and the write is refused by the kill batch itself.
    /// </summary>
    internal static JsonObject SqlKillBindingObject(SqlKillBinding binding) => BindingObject(new
    {
        session_id = binding.SessionId,
        login_time = binding.LoginTimeUtc,
        login_time_server_local = binding.LoginTimeServerLocal,
        last_request_end_time = binding.LastRequestEndTimeUtc,
        last_request_end_time_server_local = binding.LastRequestEndTimeServerLocal,
        status = binding.Status,
        open_transaction_count = binding.OpenTransactionCount,
    });

    /// <summary>PostgreSQL kill: the binding as <see cref="PgKillBindingImage"/> shapes it.</summary>
    internal static JsonObject PgKillBindingObject(PgKillBinding binding) => BindingObject(PgKillBindingImage(binding));

    private static JsonObject BindingObject(object image) => JsonSerializer.SerializeToNode(image, McpHelpers.JsonOptions)!.AsObject();
}
