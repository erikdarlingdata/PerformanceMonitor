/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>The list route of the proposal page (#7109).</summary>
    internal const string ProposalsRoute = "/api/proposals";

    /// <summary>The route the notification route page reads for the routes nobody can approve on (#7109).</summary>
    internal const string NobodyCanApproveRoutesRoute = "/api/notification-routes/nobody-can-approve";

    /// <summary>The sentence the route page, the proposal notice and the agent's answer share (#7109).</summary>
    internal const string NobodyCanApproveSentence = "Nobody can approve this. Map a person, or turn on single-person approval.";

    /// <summary>What the proposal page says to a session that presses as an administrator, before the press (#7109).</summary>
    internal const string AdministratorPressNote =
        "Your sign-in is not mapped to an enabled person, so you press as an administrator. A route that needs two people takes a press only "
        + "from a mapped person who holds the approve grant, so that a second person checks every server action. An administrator press works "
        + "only on a route with single-person approval. An administrator can map your sign-in on Admin > People.";

    /// <summary>What the proposal page says to a session that cannot press at all (#7109).</summary>
    internal const string ProposalPressSignInNote =
        "Sign in to the web to press a proposal. An admin sign-in presses as an administrator on a route with single-person approval. Any sign-in "
        + "mapped to an enabled person on Admin > People presses as that person, with that person's approve grant.";

    /// <summary>What the proposal page and the press say to a viewer-role sign-in that maps to no enabled person (#7109).</summary>
    internal const string ProposalPressViewerNotMappedNote =
        "Your sign-in can read but not edit, so it presses a proposal only as the person it is mapped to, and it is not mapped to an enabled "
        + "person. An administrator can map your sign-in on Admin > People and give that person the approve grant.";

    /// <summary>The note for a session that cannot press: a viewer-role sign-in is told it needs a mapping, every other session how to sign in.</summary>
    private static string CannotPressNote(HttpContext context) =>
        !DarlingWebSeat.IsSignedInAdmin(context) && PersonDirectoryCommands.WebIdentityOf(DarlingWebSeat.FromContext(context)) is not null
            ? ProposalPressViewerNotMappedNote
            : ProposalPressSignInNote;

    /// <summary>The most proposals the list answers.</summary>
    internal const int ProposalsLimit = 200;

    /// <summary>
    /// Who presses for this request (#7109), from the seat on the server side, never from the request body: the person whose enabled web
    /// identity is the seat's owner key (<see cref="PersonDirectoryCommands.WebIdentityOf"/>), else an administrator path. A seat with no owner
    /// key (the shared web token, an older cookie) and a signed-in admin whose key maps to no enabled person are administrator paths. The press
    /// API checks the grant, open rule 1 and the same person on both presses in its own transaction, so a mapping read here that goes stale
    /// before the press is caught there. A seat that cannot edit (a viewer-role sign-in) presses only as the enabled person its owner key maps
    /// to, never as an administrator: a grant gives the approve power to that person whatever the role of the sign-in. Null for a seat that
    /// cannot press: the reads-only seat, and a viewer-role sign-in that maps to no enabled person.
    /// </summary>
    internal static async Task<(ProposalApprover Approver, PersonRow? Person)?> ProposalApproverForSeatAsync(HttpContext context, NpgsqlDataSource people,
        CancellationToken cancellationToken)
    {
        var seat = DarlingWebSeat.FromContext(context);
        var canEdit = DarlingWebSeat.IsSignedInAdmin(context);
        var key = PersonDirectoryCommands.WebIdentityOf(seat);
        if (!canEdit && key is null)
        {
            return null;
        }

        if (key is not null)
        {
            foreach (var person in await PersonDirectory.ListAsync(people, cancellationToken))
            {
                if (!person.Enabled)
                {
                    continue;
                }

                var identity = person.Identities.FirstOrDefault(i =>
                    string.Equals(i.Kind, PersonIdentityKinds.Web, StringComparison.Ordinal) && string.Equals(i.ExternalId, key, StringComparison.Ordinal));
                if (identity is not null)
                {
                    return (ProposalApprover.Person(person.PersonId, identity.IdentityId, ServerActionSurfaces.Web), person);
                }
            }
        }

        return canEdit ? (ProposalApprover.Administrator(ServerActionSurfaces.Web), null) : null;
    }

    /// <summary>
    /// The proposal page routes (#7109). <c>GET /api/proposals</c> (newest first, <c>?state=</c> narrows) and <c>GET /api/proposals/{id}</c>
    /// answer the proposals with the press fields of the requesting session: <c>can_press</c>, <c>presses_as</c> (<c>person</c>,
    /// <c>administrator</c> or null) and <c>press_note</c>. A proposal's statement and every other text in its target, binding and facts pass
    /// through <paramref name="dataDetail"/>'s masker: with <c>maskLiterals</c> the literals are replaced, with <c>metadataOnly</c> the statement
    /// is withheld. The agent's reason is free text under the same masker.
    /// <para><c>POST /api/proposals/{id}/first-press</c> and <c>POST /api/proposals/{id}/confirm</c> call <see cref="ActionProposalPress"/>. They
    /// run the checks of the other web write routes in the same order before the store is touched: 415 without <c>application/json</c> (a
    /// cross-site form or plain-text post), 400 for a body that is not a JSON object, 403 unless the request is a signed-in session that may
    /// edit or a viewer-role sign-in mapped to an enabled person (the host's gate lets a sign-in with an owner key reach exactly these two
    /// routes, <see cref="DarlingWebSeat.IsProposalPressPath"/>), 403 when the login an admin write would run on cannot decide
    /// (<see cref="AgentChangeUndo.CanDecideAsync"/>), 503 before the owner connection is published. The body names nothing the press reads: the approver comes from the seat (<see cref="ProposalApproverForSeatAsync"/>). The
    /// answer is the press result: 200 for <c>first_pressed</c> and <c>run</c>, 404 for an unknown proposal, 409 for <c>superseded</c> and every
    /// refusal, 502 when the press could not read the server or the write failed or lost its reply.</para>
    /// </summary>
    internal static void MapProposals(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource,
        McpDataDetail dataDetail, bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        var reads = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds);

        /* Whether the web may make changes on this store at all: the login an admin seat's write would run on (the admin arm's pool when it is
           available, else the web login). The press itself writes on the owner connection whatever the seat, so a viewer-role person asks the
           same question an admin seat asks. Asking the viewer pool instead would refuse every viewer-role press on a managed store, and nothing
           here grants that pool anything. */
        AgentChangeUndo DecideProbe() =>
            adminArm?.Source is { } armSource ? new AgentChangeUndo(armSource, McpCommandDeadlines.ReadSeconds) : changes;

        async Task<JsonObject> SeatFieldsAsync(HttpContext context, JsonObject body)
        {
            var seat = await ProposalApproverForSeatAsync(context, postgres, context.RequestAborted);
            var canPress = seat is not null && await DecideProbe().CanDecideAsync(context.RequestAborted);
            body["can_press"] = canPress;
            body["presses_as"] = !canPress ? null : seat!.Value.Approver.IsPerson ? "person" : "administrator";
            body["person_name"] = canPress ? seat!.Value.Person?.DisplayName : null;
            body["press_note"] = seat is null ? CannotPressNote(context) : !canPress ? ProposalPressSignInNote : seat!.Value.Approver.IsPerson ? null : AdministratorPressNote;
            return body;
        }

        app.MapGet("/api/proposals", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var state = context.Request.Query["state"].ToString();
                state = string.IsNullOrWhiteSpace(state) ? null : state.Trim();
                if (state is not null && !ProposalStates.All.Contains(state, StringComparer.Ordinal))
                {
                    return ErrorResult("state must be one of: " + string.Join(", ", ProposalStates.All) + ".", StatusCodes.Status400BadRequest);
                }

                var rows = await reads.ListProposalsAsync(null, state, ProposalsLimit, context.RequestAborted);
                var names = await RequestServerNamesAsync(postgres, rows.Select(r => r.RequestId), context.RequestAborted);
                var masker = McpDataDetailMasker.ForAnswer(dataDetail);
                var list = new JsonArray();
                foreach (var row in rows)
                {
                    list.Add(ProposalJson(row, names.GetValueOrDefault(row.RequestId), masker));
                }

                var body = await SeatFieldsAsync(context, new JsonObject { ["proposals_returned"] = rows.Count, ["proposals"] = list });
                return DarlingWebStatementSweep.JsonText(body, ProposalsRoute, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, ProposalsRoute, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapGet("/api/proposals/{id:guid}", async (HttpContext context, Guid id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var row = await reads.GetProposalAsync(id, context.RequestAborted);
                if (row is null)
                {
                    return ErrorResult("No proposal has that id.", StatusCodes.Status404NotFound);
                }

                var names = await RequestServerNamesAsync(postgres, [row.RequestId], context.RequestAborted);
                var body = await SeatFieldsAsync(context, new JsonObject
                {
                    ["proposal"] = ProposalJson(row, names.GetValueOrDefault(row.RequestId), McpDataDetailMasker.ForAnswer(dataDetail)),
                });
                return DarlingWebStatementSweep.JsonText(body, ProposalsRoute + "/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, ProposalsRoute + "/{id}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        async Task<IResult> PressAsync(HttpContext context, string route,
            Func<ActionProposalPress, ProposalApprover, CancellationToken, Task<ProposalPressResult>> press)
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                /* The body is required to be a JSON object, as on every other write route, and nothing in it is read: the approver is the seat. */
                if (JsonNode.Parse(await ReadBodyAsync(context)) is not JsonObject)
                {
                    return ErrorResult("Send the press as a JSON object, such as {}.", StatusCodes.Status400BadRequest);
                }

                /* Who presses comes first: the store login the web runs on says nothing about who is calling. A session that may edit presses as
                   its mapped person or as an administrator. A viewer-role sign-in presses only as its mapped enabled person, so one that maps to
                   nobody is refused here, before the press API runs. */
                var seat = await ProposalApproverForSeatAsync(context, postgres, context.RequestAborted);
                if (seat is null)
                {
                    return ErrorResult(CannotPressNote(context), StatusCodes.Status403Forbidden);
                }

                if (!await DecideProbe().CanDecideAsync(context.RequestAborted))
                {
                    return ErrorResult(DarlingWebStoreWrites.Refused("press a proposal", managedStore, adminWritesOff: adminWritesOff),
                        StatusCodes.Status403Forbidden);
                }

                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return ErrorResult("The service has not published its store owner connection yet, so the press cannot run. Try again in a moment.",
                        StatusCodes.Status503ServiceUnavailable);
                }

                var result = await press(new ActionProposalPress(owner, logger), seat.Value.Approver, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(PressJson(result, McpDataDetailMasker.ForAnswer(dataDetail)), route, logger,
                    stopwatch.ElapsedMilliseconds, context.RequestAborted, PressStatus(result));
            }
            catch (Exception ex) when (ex is JsonException)
            {
                return ErrorResult("The request body is not valid.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        }

        app.MapPost("/api/proposals/{id:guid}/first-press", (HttpContext context, Guid id) =>
            PressAsync(context, ProposalsRoute + "/{id}/first-press", (p, a, ct) => p.FirstPressAsync(id, a, ct)));
        app.MapPost("/api/proposals/{id:guid}/confirm", (HttpContext context, Guid id) =>
            PressAsync(context, ProposalsRoute + "/{id}/confirm", (p, a, ct) => p.ConfirmAsync(id, a, ct)));

        app.MapGet("/api/notification-routes/nobody-can-approve", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var ids = new JsonArray();
                foreach (var routeId in await NobodyCanApproveRouteIdsAsync(postgres, context.RequestAborted))
                {
                    ids.Add(routeId);
                }

                var body = new JsonObject { ["route_ids"] = ids, ["message"] = NobodyCanApproveSentence };
                return DarlingWebStatementSweep.JsonText(body, NobodyCanApproveRoutesRoute, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, NobodyCanApproveRoutesRoute, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The HTTP status of a press answer (#7109).</summary>
    internal static int PressStatus(ProposalPressResult result) => result.Status switch
    {
        "first_pressed" or ProposalStates.Run => StatusCodes.Status200OK,
        _ when result.Code == InvestigationRequestStore.ProposalPressCodes.NotFound => StatusCodes.Status404NotFound,
        ProposalStates.Failed or ProposalStates.Unknown => StatusCodes.Status502BadGateway,
        _ => StatusCodes.Status409Conflict,
    };

    /// <summary>A press answer as the page reads it: <c>{status, code, message, facts, changed}</c>, the facts through the masker.</summary>
    internal static JsonObject PressJson(ProposalPressResult result, McpDataDetailMasker masker)
    {
        var changed = new JsonArray();
        foreach (var key in result.Changed ?? [])
        {
            changed.Add(key);
        }

        return new JsonObject
        {
            ["status"] = result.Status,
            ["code"] = result.Code,
            ["message"] = result.Message,
            ["facts"] = MaskedJson(result.Facts?.ToJsonString(), masker),
            ["changed"] = changed,
        };
    }

    /// <summary>One proposal as the page reads it. The target, binding, first press facts and outcome pass through the masker by key, so a
    /// <c>statement</c> follows <c>mcp.dataDetail</c>.</summary>
    internal static JsonObject ProposalJson(ActionProposalRow row, string? serverName, McpDataDetailMasker masker) => new()
    {
        ["proposal_id"] = row.ProposalId.ToString("D", CultureInfo.InvariantCulture),
        ["request_id"] = row.RequestId.ToString("D", CultureInfo.InvariantCulture),
        ["kind"] = InvestigationRequestStore.ServerActionForTool(row.Tool),
        ["tool"] = row.Tool,
        ["agent"] = row.Agent,
        ["server_id"] = row.ServerId,
        ["server_name"] = serverName,
        ["state"] = row.State,
        ["target"] = MaskedJson(row.TargetJson, masker),
        ["binding"] = MaskedJson(row.BindingJson, masker),
        ["agent_reason"] = row.Reason is null ? null : masker.FreeText(row.Reason),
        ["proposed_at_utc"] = Utc(row.ProposedAt),
        ["expires_at_utc"] = Utc(row.ExpiresAt),
        ["first_press_by"] = row.FirstPressBy,
        ["first_press_at_utc"] = row.FirstPressAt is { } pressed ? Utc(pressed) : null,
        ["first_press_facts"] = MaskedJson(row.FirstPressFactsJson, masker),
        ["decided_by"] = row.DecidedBy,
        ["decided_at_utc"] = row.DecidedAt is { } decided ? Utc(decided) : null,
        ["finished_at_utc"] = row.FinishedAt is { } finished ? Utc(finished) : null,
        ["outcome"] = MaskedJson(row.OutcomeJson, masker),
    };

    private static string Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    private static JsonNode? MaskedJson(string? json, McpDataDetailMasker masker) => json is null ? null : JsonNode.Parse(masker.Json(json));

    /// <summary>The server name of each request, as the request recorded it.</summary>
    private static async Task<Dictionary<Guid, string>> RequestServerNamesAsync(NpgsqlDataSource postgres, IEnumerable<Guid> requestIds, CancellationToken ct)
    {
        var names = new Dictionary<Guid, string>();
        var ids = requestIds.Distinct().ToArray();
        if (ids.Length == 0)
        {
            return names;
        }

        await using var command = postgres.CreateCommand("SELECT request_id, server_name FROM config.investigation_requests WHERE request_id = ANY($1)");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<Guid[]> { TypedValue = ids });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(1))
            {
                names[reader.GetGuid(0)] = reader.GetString(1);
            }
        }

        return names;
    }

    /// <summary>
    /// The routes where nobody can approve (#7109): Level 3, approval not <c>off</c> (an unknown word reads as required, as the approval rule
    /// reads it), no single-person approval, and no enabled person with a sign-in holds <c>approve</c> of any kind on a scope that reaches any
    /// server of the route. A grant on the fleet reaches every route. A server route is reached by a grant on that server or on a tag it
    /// carries, a tag route by a grant on that tag or on a server that carries it, and a fleet route by any approve grant.
    /// </summary>
    internal const string NobodyCanApproveRoutesSql = @"
SELECT r.route_id
FROM config.config_notification_routes AS r
WHERE r.agent_level = 3
  AND r.action_approval IS DISTINCT FROM 'off'
  AND NOT r.approval_single_person
  AND NOT EXISTS (
      SELECT 1
      FROM config.person AS p
      JOIN config.person_grant AS g ON g.person_id = p.person_id
      WHERE p.enabled
        AND g.capability = 'approve'
        AND EXISTS (SELECT 1 FROM config.person_identity AS i WHERE i.person_id = p.person_id)
        AND (   (g.scope_server_id IS NULL AND g.scope_tag_id IS NULL)
             OR (r.scope_server_id IS NULL AND r.scope_tag_id IS NULL)
             OR g.scope_server_id = r.scope_server_id
             OR g.scope_tag_id IN (SELECT m.tag_id FROM config.server_tag_map AS m WHERE m.server_id = r.scope_server_id)
             OR g.scope_tag_id = r.scope_tag_id
             OR g.scope_server_id IN (SELECT m.server_id FROM config.server_tag_map AS m WHERE m.tag_id = r.scope_tag_id)))
ORDER BY r.route_id";

    internal static async Task<IReadOnlyList<int>> NobodyCanApproveRouteIdsAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var ids = new List<int>();
        await using var command = postgres.CreateCommand(NobodyCanApproveRoutesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }
}
