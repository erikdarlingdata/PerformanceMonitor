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
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Whether a Level 3 action on a request needs a person's approval (#7109), and whether the routes allow single-person approval.</summary>
public sealed record ActionApprovalState(bool Needed, bool SinglePerson);

/// <summary>
/// The approval rule (#7109, design section 3 of #6980), in one place: the server-action gate reports it and the agent's write refuses it, both
/// through this method over the same live routes and the same request row.
/// </summary>
public static class ActionApprovalRules
{
    /// <summary>What a Level 3 tool answers with its proposal when nobody can approve it.</summary>
    public const string NobodyCanApproveMessage = "Nobody can approve this. Map a person, or turn on single-person approval.";

    /// <summary>
    /// The rule. <paramref name="routes"/> holds, for each metric of the request, the route <c>NotificationRouter.ResolveAgent</c> picks (null
    /// when none does). The request needs approval when any of them has <c>action_approval</c> <c>required</c> (an unknown word reads as
    /// <c>required</c>), and always when it is a chat request, whatever the routes say: a person's text can come from anywhere, so a press is
    /// the one step a person must take before a change to a server. <see cref="ActionApprovalState.SinglePerson"/> holds only when every
    /// picked route has <c>approval_single_person</c> on: one route without it needs a second person.
    /// </summary>
    public static ActionApprovalState ForRequest(IEnumerable<NotificationRoute?> routes, string? triggerKind)
    {
        ArgumentNullException.ThrowIfNull(routes);
        var needed = string.Equals(triggerKind, InvestigationTriggerKinds.Chat, StringComparison.Ordinal);
        var any = false;
        var singlePerson = true;
        foreach (var route in routes)
        {
            if (route is null)
                continue;
            any = true;
            if (NotificationRouteKinds.NormalizeActionApproval(route.ActionApproval) == NotificationRouteKinds.ActionApprovalRequired)
                needed = true;
            if (!route.ApprovalSinglePerson)
                singlePerson = false;
        }

        return new ActionApprovalState(needed, any && singlePerson);
    }
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>
    /// An enabled person with at least one identity (so mapped to a sign-in) holding <c>approve</c> with the action's kind, scoped to the whole
    /// fleet, to the server, or to a tag the server carries.
    /// </summary>
    internal const string AnyApproverSql = @"
SELECT EXISTS (
    SELECT 1
    FROM config.person AS p
    JOIN config.person_grant AS g ON g.person_id = p.person_id
    WHERE p.enabled
      AND g.capability = 'approve'
      AND $2 = ANY (g.kinds)
      AND EXISTS (SELECT 1 FROM config.person_identity AS i WHERE i.person_id = p.person_id)
      AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
           OR g.scope_server_id = $1
           OR g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = $1)))";

    /// <summary>
    /// True when nobody can approve a <paramref name="serverAction"/> proposal on <paramref name="serverId"/> (#7109): the routes do not allow
    /// single-person approval and no enabled mapped person holds <c>approve</c> for that server and that kind (the #7107 grants). The one
    /// check the agent's <c>proposed</c> answer, the route page and the proposal notice use. Reads the people tables, so it runs on a store
    /// built over the owner's data source.
    /// </summary>
    public async Task<bool> NobodyCanApproveAsync(int serverId, string serverAction, bool singlePerson, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serverAction);
        if (singlePerson)
            return false;
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, AnyApproverSql, P(serverId), PText(serverAction));
        return await command.ExecuteScalarAsync(cancellationToken) is not true;
    }
}
