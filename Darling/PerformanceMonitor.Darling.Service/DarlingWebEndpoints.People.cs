/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>The route the Admin People tab reads (#7107).</summary>
    internal const string PeopleRoute = "/api/people";

    /// <summary>The 403 a refused people write answers when the web's store login cannot decide (#7107).</summary>
    internal static string PeopleRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change people, sign-ins or grants", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>
    /// The people directory routes (#7107). <c>GET /api/people</c> answers <c>{can_decide, signed_in_admin, sign_in_key, people_returned,
    /// people: [...]}</c>, where <c>people</c> is the same shape the MCP <c>get_people</c> read answers. <c>sign_in_key</c> is the requesting
    /// session's own owner key (<see cref="PersonDirectoryCommands.WebIdentityOf"/>), or null for the shared web token, a reads-only session or an
    /// older cookie. It is only ever the caller's own: it comes from the request's seat, never from the store, and knowing it signs nobody in.
    /// <c>PUT /api/people/{command}</c>, with the command's args as the JSON body, runs one of the seven people writes through
    /// <see cref="PersonDirectoryCommands.RunForWebSeatAsync"/>. It runs the checks of <c>PUT /api/servers/{id}/actions/{action}</c> in the same
    /// order before the store is touched: 415 without <c>application/json</c>, 400 for a body that is not a JSON object, 403 unless the request is a
    /// signed-in session that may edit (the shared web token is one, as on the switch route), 400 for a signed-in name that cannot be recorded,
    /// 403 when the web's store login cannot decide (<see cref="AgentChangeUndo.CanDecideAsync"/>), and 503 before the owner connection is
    /// published. The write runs on that owner connection and is journaled by <c>person:&lt;signed-in name&gt;</c>, surface <c>web</c>.
    /// </summary>
    internal static void MapPeople(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource,
        bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* The access check runs as the login the change is allowed under (#6600), as on the switch route; the write stays on the owner connection. */
        AgentChangeUndo DecideProbe(HttpContext context) =>
            adminArm is null ? changes : new AgentChangeUndo(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/people", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rows = await PersonDirectory.ListAsync(postgres, context.RequestAborted);
                var body = DarlingMcpPeopleTools.ToJson(rows);
                var signedInAdmin = DarlingWebSeat.IsSignedInAdmin(context);
                body["can_decide"] = signedInAdmin && await DecideProbe(context).CanDecideAsync(context.RequestAborted);
                body["signed_in_admin"] = signedInAdmin;
                body["sign_in_key"] = PersonDirectoryCommands.WebIdentityOf(DarlingWebSeat.FromContext(context));
                return DarlingWebStatementSweep.JsonText(body, PeopleRoute, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, PeopleRoute, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/people/{command}", async (HttpContext context, string command) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var text = await ReadBodyAsync(context);
                if (JsonNode.Parse(text) is not JsonObject)
                {
                    return ErrorResult("Send the change as a JSON object.", StatusCodes.Status400BadRequest);
                }

                /* A signed-in admin comes first: the store login the web runs on says nothing about who is calling. */
                if (!DarlingWebSeat.IsSignedInAdmin(context))
                {
                    return SignInRequiredResult();
                }

                if (InvestigationPersonWrites.ValidateActor(NoteDecider(DarlingWebSeat.FromContext(context))) is not null)
                {
                    return ErrorResult("The signed-in name cannot be recorded as the person who made this change.", StatusCodes.Status400BadRequest);
                }

                /* The admin check comes before anything is read or written. */
                if (!await DecideProbe(context).CanDecideAsync(context.RequestAborted))
                {
                    return ErrorResult(PeopleRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);
                }

                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return ErrorResult("The service has not published its store owner connection yet, so the change cannot be written. Try again in a moment.",
                        StatusCodes.Status503ServiceUnavailable);
                }

                var result = await PersonDirectoryCommands.RunForWebSeatAsync(context, owner, command, text, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(result.ToJson(), "/api/people/{command}", logger, stopwatch.ElapsedMilliseconds,
                    context.RequestAborted, PersonWriteStatus(result));
            }
            catch (Exception ex) when (ex is JsonException)
            {
                return ErrorResult("The request body is not valid.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/people/{command}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The HTTP status of a people write's answer (#7107): 200 for a change or no change, 404 for a missing row, 409 for a duplicate or a
    /// lost race, 403 for a request that is not a signed-in admin, and 400 for every other refusal.</summary>
    internal static int PersonWriteStatus(PersonWriteResult result) => result.Succeeded ? StatusCodes.Status200OK : result.Code switch
    {
        "sign_in_required" => StatusCodes.Status403Forbidden,
        "unknown_person" or "unknown_identity" or "unknown_grant" or "unknown_server" or "unknown_tag" => StatusCodes.Status404NotFound,
        "identity_taken" or "grant_exists" or "conflict" => StatusCodes.Status409Conflict,
        _ => StatusCodes.Status400BadRequest,
    };
}
