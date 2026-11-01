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
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>
    /// The pending agent notes screen's routes (#5903): <c>GET /api/investigation-notes/pending</c> lists the notes an agent
    /// proposed that nobody has decided, and <c>PUT /api/investigation-notes/{id}/approve</c> and <c>.../reject</c> decide
    /// one. The write routes answer as the mute-rule writes do: <c>application/json</c> required (the same CSRF discipline),
    /// born gated by the host's write gate (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> refuses every unsafe
    /// method to a read-only seat), 404 when the note is gone or already decided, 403 with <see cref="NoteDecisionRefusedMessage"/>
    /// when the store refuses the write for privilege (SQLSTATE 42501: deciding a note is an <c>admin</c> role write, and the web
    /// host runs on whatever login it was given, the <c>viewer</c> role on a managed or compose store), 400 when the person
    /// rule refuses the seat's name (<see cref="InvestigationPersonWrites.ValidateActor"/>), a fixed-sentence 500 or 503
    /// for any other store failure. The decision is stamped as every web write is: the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>
    /// (the signed-in subject, else <c>web</c>) shaped as <c>person:&lt;name&gt;</c>, as the Viewer stamps a decision. The list body goes through the statement sweep (#4348) like every other body that carries text an agent wrote.
    /// </summary>
    private static void MapInvestigationNotes(WebApplication app, NpgsqlDataSource postgres, InvestigationRequestStore notes, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: a decision is written as the login the seat's admin writes use; the pending list stays on the web's own pool. */
        InvestigationRequestStore DecisionStore(HttpContext context) =>
            adminArm is null ? notes : new InvestigationRequestStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-notes/pending", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                /* One row past the cap tells a full page from a cut one (#5903). */
                var pending = await notes.ListPendingNotesAsync(PendingNotesLimit + 1, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    PendingNotesBody(pending), "/api/investigation-notes/pending", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/pending", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes/{id:long}/approve", async (HttpContext context, long id) =>
        {
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var decidedBy = NoteDecider(DarlingWebSeat.FromContext(context));
            if (InvestigationPersonWrites.ValidateActor(decidedBy) is not null)
            {
                return PersonActorRefusedResult();
            }

            try
            {
                if (!await DecisionStore(context).ApproveNoteAsync(id, decidedBy, context.RequestAborted))
                {
                    return ErrorResult("That note is not pending: it was already decided, or it is gone.", StatusCodes.Status404NotFound);
                }

                return DarlingWebStatementSweep.JsonText(
                    NoteDecisionBody("approved", id, decidedBy), "/api/investigation-notes/{id}/approve", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return NoteDecisionRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/{id}/approve", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes/{id:long}/reject", async (HttpContext context, long id) =>
        {
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var decidedBy = NoteDecider(DarlingWebSeat.FromContext(context));
            if (InvestigationPersonWrites.ValidateActor(decidedBy) is not null)
            {
                return PersonActorRefusedResult();
            }

            try
            {
                if (!await DecisionStore(context).RejectNoteAsync(id, decidedBy, context.RequestAborted))
                {
                    return ErrorResult("That note is not pending: it was already decided, or it is gone.", StatusCodes.Status404NotFound);
                }

                return DarlingWebStatementSweep.JsonText(
                    NoteDecisionBody("rejected", id, decidedBy), "/api/investigation-notes/{id}/reject", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return NoteDecisionRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/{id}/reject", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// The threshold-overrides grid's routes (#5916 part 4): <c>GET /api/threshold-overrides</c> lists the overrides and
    /// <c>PUT /api/threshold-overrides</c> sets or clears one (the body and the answers are <see cref="DarlingWebThresholdOverrides"/>'s).
    /// The write answers as the note decisions do (#5903): <c>application/json</c> required, born gated by the host's write gate,
    /// 403 with <see cref="DarlingWebThresholdOverrides.RefusedMessage"/> when the store login holds no write grant on the table
    /// (the <c>viewer</c> role of a managed or compose store), 400 for a refused key, value or server. Every body goes through the
    /// statement sweep (#4348): <c>set_by</c> on an agent's override is text an agent named.
    /// </summary>
    private static void MapThresholdOverrides(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        app.MapGet("/api/threshold-overrides", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return DarlingWebStatementSweep.JsonText(
                    await DarlingWebThresholdOverrides.ListBodyAsync(postgres, context.RequestAborted, WebAdminArm.SourceFor(adminArm, context, postgres)),
                    "/api/threshold-overrides", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/threshold-overrides", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/threshold-overrides", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonObject? body;
            try
            {
                body = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted) as JsonObject;
            }
            catch (JsonException)
            {
                body = null;
            }

            if (body is null)
            {
                return ErrorResult("The body must be a JSON object: {server_name, key, value, database_name?, clear?}.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (status, answer) = await DarlingWebThresholdOverrides.ChangeAsync(
                    WebAdminArm.SourceFor(adminArm, context, postgres), logger, body, DarlingWebSeat.FromContext(context).EditorPrincipal,
                    context.RequestAborted, managedStore, adminWritesOff);
                return DarlingWebStatementSweep.JsonText(
                    answer, "/api/threshold-overrides", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/threshold-overrides", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// The agent setup routes (#6264), level with the desktop Viewer: <c>GET /api/agent-setup/access</c> says whether this login can write;
    /// <c>POST /api/notification-routes</c>, <c>PUT /api/notification-routes/{id}</c> and <c>DELETE /api/notification-routes/{id}</c> create, edit and
    /// delete a route's agent channel, match, scope, enabled flag, resolution override, fallback and destinations (webhook URLs and routing key sealed
    /// as the Viewer and the MCP route tools seal them, never answered; email recipients); <c>GET /api/environment-profiles</c> lists the profiles and <c>PUT /api/environment-profiles</c> saves or removes one.
    /// The writes need <c>application/json</c>, are born gated by the host's write gate (a read-only seat is refused every unsafe method), answer
    /// 403 when the store refuses for privilege (SQLSTATE 42501: the <c>viewer</c> role of a managed or compose store), 404 for a route that is
    /// gone and 400 with the shared rule's sentence for a refused value. The work is <see cref="DarlingWebAgentSetup"/>'s.
    /// </summary>
    private static void MapAgentSetup(WebApplication app, NpgsqlDataSource postgres, ILogger logger, IPasswordKeyRing? passwordKeys, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        app.MapGet("/api/agent-setup/access", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return Results.Json(DarlingWebAgentSetup.ForSeat(
                    await DarlingWebAgentSetup.AccessAsync(WebAdminArm.SourceFor(adminArm, context, postgres), context.RequestAborted), DarlingWebSeat.IsSignedInAdmin(context)));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-setup/access", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapGet("/api/environment-profiles", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return DarlingWebStatementSweep.JsonText(
                    DarlingWebAgentSetup.ForSeat(
                        await DarlingWebAgentSetup.ProfilesBodyAsync(postgres, context.RequestAborted, WebAdminArm.SourceFor(adminArm, context, postgres)),
                        DarlingWebSeat.IsSignedInAdmin(context)),
                    "/api/environment-profiles", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/environment-profiles", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/environment-profiles", async Task<IResult> (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/environment-profiles",
                (body, editor, ct) => DarlingWebAgentSetup.SaveProfileAsync(WebAdminArm.SourceFor(adminArm, context, postgres), body, editor, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/environment-profiles", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapPost("/api/notification-routes", async Task<IResult> (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/notification-routes",
                (body, editor, ct) => DarlingWebAgentSetup.SaveRouteAsync(WebAdminArm.SourceFor(adminArm, context, postgres), logger, null, body, editor, passwordKeys ?? DarlingPasswordKey.Current, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/notification-routes", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapPut("/api/notification-routes/{id:int}", async Task<IResult> (HttpContext context, int id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/notification-routes/{id}",
                (body, editor, ct) => DarlingWebAgentSetup.SaveRouteAsync(WebAdminArm.SourceFor(adminArm, context, postgres), logger, id, body, editor, passwordKeys ?? DarlingPasswordKey.Current, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/notification-routes/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        /* #7041: the Daily Health Report settings. The body is the daily_report group of get_alert_settings; update_alert_settings' own
           body validates and writes it. A signed-in admin only, like the other agent setup writes. */
        app.MapPut("/api/alert-settings/daily-report", async Task<IResult> (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/alert-settings/daily-report",
                (body, editor, ct) => DarlingWebAlertSettings.SaveDailyReportAsync(
                    WebAdminArm.SourceFor(adminArm, context, postgres), body, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/alert-settings/daily-report", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapDelete("/api/notification-routes/{id:int}", async (HttpContext context, int id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["error"] = InvestigationWriteSignInMessage }, "/api/notification-routes/{id}", logger,
                    stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }

            /* #6265: the person is named as a note decision names them; a signed-in name the journal cannot hold is refused before anything runs. */
            if (PersonWriteActor(context) is null)
            {
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["error"] = PersonWriteActorRefusal }, "/api/notification-routes/{id}", logger,
                    stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var (status, answer) = await DarlingWebAgentSetup.DeleteRouteAsync(
                    WebAdminArm.SourceFor(adminArm, context, postgres), logger, id, DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted,
                    managedStore, adminWritesOff);
                return DarlingWebStatementSweep.JsonText(
                    answer, "/api/notification-routes/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/notification-routes/{id}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>One agent setup write: <c>application/json</c> required (the same CSRF discipline as the other web writes), the body a JSON
    /// object, the seat's <see cref="DarlingWebSeat.EditorPrincipal"/> as the editor. Always answers a status and a body for the route to send
    /// through the statement sweep: a refusal's sentence, or the fixed-sentence 500 or 503 of a failure.</summary>
    private static async Task<(int Status, JsonNode Body)> AgentSetupRunAsync(
        HttpContext context, ILogger logger, string route,
        Func<JsonObject, string, CancellationToken, Task<(int Status, JsonNode Body)>> write)
    {
        /* #6274: every agent setup write needs a signed-in admin. A dashboard with no sign-in stores no seat, so the default shared-token
           seat that FromContext hands back would let any process on the host change routes and profiles with no credential. */
        if (!DarlingWebSeat.IsSignedInAdmin(context))
        {
            return (StatusCodes.Status403Forbidden, new JsonObject { ["error"] = InvestigationWriteSignInMessage });
        }

        if (!IsJsonContentType(context.Request.ContentType))
        {
            return (StatusCodes.Status415UnsupportedMediaType, new JsonObject { ["error"] = "Content-Type must be application/json." });
        }

        JsonObject? body;
        try
        {
            body = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted) as JsonObject;
        }
        catch (JsonException)
        {
            body = null;
        }

        if (body is null)
        {
            return (StatusCodes.Status400BadRequest, new JsonObject { ["error"] = "The body must be a JSON object." });
        }

        /* #6265: the person is named as a note decision names them (InvestigationPersonWrites.PersonActor); a signed-in name the journal
           cannot hold is refused with the same fixed sentence, before anything runs. */
        if (PersonWriteActor(context) is null)
        {
            return (StatusCodes.Status400BadRequest, new JsonObject { ["error"] = PersonWriteActorRefusal });
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            return await write(body, DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
            return (DarlingWebFailureLog.StatusCode(ex), DarlingWebFailureLog.Body(ex));
        }
    }

    /// <summary>The 403 sentence a note decision answers when the store refuses the write (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, and deciding a note is an <c>admin</c> role write (#5903).</summary>
    internal static string NoteDecisionRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("decide notes", managedStore, "Decide them in the Viewer app", adminWritesOff: adminWritesOff);

    /// <summary>The sentence a person-made investigation change answers with 403 when the request holds no signed-in admin seat (#6265):
    /// a dashboard with no sign-in configured has no caller to name, so the change is made in the Viewer or after signing in.</summary>
    internal const string InvestigationWriteSignInMessage = "Sign in to the web as an admin to make this change, or use the Viewer.";

    /// <summary>The 403 answered to a change that needs a signed-in admin: <see cref="InvestigationWriteSignInMessage"/>, nothing written.</summary>
    private static IResult SignInRequiredResult() => ErrorResult(InvestigationWriteSignInMessage, StatusCodes.Status403Forbidden);

    /// <summary>The 403 a refused note decision answers: the fixed <see cref="NoteDecisionRefusedMessage"/> sentence in the mute-rule error envelope.</summary>
    private static IResult NoteDecisionRefusedResult(bool managedStore, bool adminWritesOff = false) =>
        ErrorResult(NoteDecisionRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);

    /// <summary>True when the store refused the statement for privilege (SQLSTATE 42501).</summary>
    internal static bool IsInsufficientPrivilege(Exception ex) => ex is PostgresException { SqlState: "42501" };

    /// <summary>The most pending notes one list answers. A fleet that proposes more than this has a backlog the screen cannot
    /// show whole; the list says so (<c>truncated</c>), and deciding the oldest shows the rest.</summary>
    internal const int PendingNotesLimit = 200;

    /// <summary>Who a note decision is stamped as: <c>person:&lt;signed-in name&gt;</c>, the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>
    /// shaped by <see cref="InvestigationPersonWrites.PersonActor"/> as the Viewer and the web's own person writes record a person (#6265).
    /// The signed-in subject, else <c>web</c> for the shared token, so that token's decision is <c>person:web</c>.</summary>
    internal static string NoteDecider(DarlingWebSeat seat) => InvestigationPersonWrites.PersonActor(seat.EditorPrincipal);

    /// <summary>The list body: <c>{notes_returned, truncated, notes: [{note_id, server_id, server_name, metric_name, family,
    /// story_path_hash, note_text, author, source_request_id, proposed_at_utc, expires_at_utc}]}</c>, oldest proposal first.
    /// <paramref name="pending"/> is read with <see cref="PendingNotesLimit"/> + 1 rows: the first <see cref="PendingNotesLimit"/>
    /// are returned and <c>truncated</c> is true when the extra row exists.</summary>
    internal static JsonObject PendingNotesBody(IReadOnlyList<PendingInvestigationNote> pending)
    {
        var array = new JsonArray();
        foreach (var note in pending.Take(PendingNotesLimit))
        {
            array.Add(new JsonObject
            {
                ["note_id"] = note.NoteId,
                ["server_id"] = note.ServerId,
                ["server_name"] = note.ServerName,
                ["metric_name"] = note.MetricName,
                ["family"] = note.Family,
                ["story_path_hash"] = note.StoryPathHash,
                ["note_text"] = note.NoteText,
                ["author"] = note.Author,
                ["source_request_id"] = note.SourceRequestId?.ToString(),
                ["proposed_at_utc"] = note.ProposedAtUtc.ToString("o", CultureInfo.InvariantCulture),
                ["expires_at_utc"] = note.ExpiresAtUtc.ToString("o", CultureInfo.InvariantCulture),
            });
        }

        return new JsonObject { ["notes_returned"] = array.Count, ["truncated"] = pending.Count > PendingNotesLimit, ["notes"] = array };
    }

    internal static JsonObject NoteDecisionBody(string status, long noteId, string decidedBy) =>
        new() { ["status"] = status, ["note_id"] = noteId, ["decided_by"] = decidedBy };

    /* ─────────────────────── #5917 agent changes: the list, one-click undo and the switch ─────────────────────── */

    /// <summary>The 403 sentence an undo or a switch answers when the store refuses the write for privilege (SQLSTATE 42501): the web host
    /// runs on whatever login <c>postgres.webConnectionString</c> names, the <c>viewer</c> role on a managed or compose store (#5917).</summary>
    internal static string AgentChangeRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("undo or switch off agent changes", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>The 403 a refused undo or switch answers: the fixed <see cref="AgentChangeRefusedMessage"/> sentence in the error envelope.</summary>
    private static IResult AgentChangeRefusedResult(bool managedStore, bool adminWritesOff = false) =>
        ErrorResult(AgentChangeRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);

    /// <summary>
    /// The Agent changes routes (#5917): <c>GET /api/agent-changes</c> lists the calls (with their result, or "unknown"), the refusals,
    /// the switch and <c>can_decide</c> (false under the viewer role: the page then lists only); <c>GET /api/agent-changes/{id}/muted-alerts</c>
    /// lists the alerts muted since a mute change; <c>PUT /api/agent-changes/{id}/undo</c> undoes one change and <c>PUT /api/agent-changes/switch</c>
    /// turns every agent change on or off. The writes need <c>application/json</c>, are gated by the host's write gate, answer 403 with
    /// <see cref="AgentChangeRefusedMessage"/> when the store refuses for privilege, 404 when the call is not in the journal, 409 with the
    /// refusal ("changed since", name taken, already undone, no undo) and 400 for a bad body or login. Journal text goes through
    /// <c>InvestigationFieldMasking.JournalDetail</c> at the web's level (full, as the other web reads of the journal).
    /// </summary>
    private static void MapAgentChanges(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource = null,
        bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: the access check, the undo and the switch run as the login the seat's admin writes use (a plan undo journals through it too);
           the list and the muted alerts stay on the web's own pool. */
        AgentChangeUndo AdminChanges(HttpContext context) =>
            adminArm is null ? changes : new AgentChangeUndo(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/agent-changes", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                /* #6979: ?request=<id> reads one request's changes by request_id in SQL, so an old request is not cut off by the newest-200 limit. */
                Guid? requestId = null;
                if (context.Request.Query.TryGetValue("request", out var requestText) && requestText.Count > 0)
                {
                    if (!Guid.TryParse(requestText[0], out var parsed))
                        return ErrorResult("The request must be a request id (a GUID).", StatusCodes.Status400BadRequest);
                    requestId = parsed;
                }

                var read = await changes.ReadAsync(AgentChangesLimit, context.RequestAborted, requestId);
                var canDecide = await AdminChanges(context).CanDecideAsync(context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    AgentChangesBody(read, canDecide, DateTime.UtcNow), "/api/agent-changes", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapGet("/api/agent-changes/{id:long}/muted-alerts", async (HttpContext context, long id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var alerts = await changes.ReadMutedAlertsAsync(id, 200, context.RequestAborted);
                var array = new JsonArray();
                foreach (var a in alerts)
                {
                    array.Add(new JsonObject
                    {
                        ["alert_time_utc"] = a.AlertTimeUtc.ToString("o", CultureInfo.InvariantCulture), ["server_id"] = a.ServerId,
                        ["server_name"] = a.ServerName, ["metric_name"] = a.MetricName, ["current_value"] = a.CurrentValue, ["threshold_value"] = a.ThresholdValue,
                    });
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["alerts_returned"] = array.Count, ["alerts"] = array },
                    "/api/agent-changes/{id}/muted-alerts", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/{id}/muted-alerts", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/agent-changes/{id:long}/undo", async (HttpContext context, long id) =>
        {
            /* #6265: an undo is a person's act, so it needs a signed-in admin before anything is read or written. */
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var login = DarlingWebSeat.FromContext(context).EditorPrincipal;
            try
            {
                /* #5632 step 9: a plan force or unforce is undone by the service's plan force code, which reads the server first. The first request
                   (no confirm) answers the statement and the state and writes nothing; {"confirm": true, "statement": "<shown>"} writes. */
                var writeChanges = AdminChanges(context);
                if ((await changes.GetAsync(id, context.RequestAborted))?.IsPlanChange == true)
                {
                    /* A plan undo writes to the monitored server, so it needs a signed-in admin, not just the web's store login (#6269). */
                    if (!DarlingWebSeat.IsSignedInAdmin(context))
                    {
                        return SignInRequiredResult();
                    }

                    if (!await writeChanges.CanDecideAsync(context.RequestAborted))
                    {
                        return AgentChangeRefusedResult(managedStore, adminWritesOff);
                    }

                    var (planBody, planStatus) = await UndoPlanChangeAsync(context, writeChanges, ownerSource, logger, id, login);
                    return DarlingWebStatementSweep.JsonText(planBody, "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, planStatus);
                }

                var result = await writeChanges.UndoAsync(id, login, context.RequestAborted);
                if (result.Undone)
                {
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["status"] = "undone", ["called_id"] = id, ["undone_by"] = AgentChangeUndo.PersonActor(login), ["message"] = result.Words },
                        "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = result.Words, ["code"] = result.Code },
                    "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted,
                    result.Code == "not_found" ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict);
            }
            catch (ArgumentException)
            {
                return ErrorResult("The signed-in name cannot be recorded as the person who undid this change.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return AgentChangeRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/{id}/undo", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/agent-changes/switch", async (HttpContext context) =>
        {
            /* #6265: the service-wide agent switch is a person's act, so it needs a signed-in admin before anything is read or written. */
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var login = DarlingWebSeat.FromContext(context).EditorPrincipal;
            try
            {
                if (JsonNode.Parse(await ReadBodyAsync(context)) is not JsonObject body || body["enabled"] is not JsonValue flag || !flag.TryGetValue(out bool enabled))
                {
                    return ErrorResult("Send {\"enabled\": true} or {\"enabled\": false}.", StatusCodes.Status400BadRequest);
                }

                if (!await AdminChanges(context).SetEnabledAsync(enabled, login, context.RequestAborted))
                {
                    /* #6401: no service row yet, so nothing was written; the switch never creates the row. */
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = AgentChangeUndo.SwitchStoreNotSetUpMessage, ["code"] = "store_not_set_up" },
                        "/api/agent-changes/switch", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["status"] = enabled ? "on" : "off", ["enabled"] = enabled },
                    "/api/agent-changes/switch", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is JsonException or ArgumentException)
            {
                return ErrorResult("The request body is not valid, or the signed-in name cannot be recorded.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return AgentChangeRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/switch", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// A person's undo of a plan force or unforce (#5632, step 9). 200 <c>{status: "confirm", statement, state, message}</c> without a confirm
    /// (nothing written); 200 <c>{status: "undone", ...}</c> with <c>{"confirm": true}</c>; a refusal is 409 <c>{error, code}</c> (404 for
    /// <c>not_found</c>); a server that took no write is 502. A viewer-role store is 403 before anything else, as the store kinds' undo answers it
    /// (the route checks that, then sweeps what this returns).
    /// </summary>
    internal static async Task<(JsonObject Body, int Status)> UndoPlanChangeAsync(HttpContext context, AgentChangeUndo changes, StoreOwnerSource? ownerSource, ILogger logger,
        long id, string login)
    {
        var confirm = false;
        string? shown = null;
        var raw = await ReadBodyAsync(context);
        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                if (JsonNode.Parse(raw) is not JsonObject body)
                {
                    return (new JsonObject { ["error"] = "Send {} or {\"confirm\": true, \"statement\": \"<the statement shown>\"}." }, StatusCodes.Status400BadRequest);
                }

                confirm = body["confirm"] is JsonValue flag && flag.TryGetValue(out bool yes) && yes;
                shown = body["statement"] is JsonValue text && text.TryGetValue(out string? sentence) ? sentence : null;
            }
            catch (JsonException)
            {
                return (new JsonObject { ["error"] = "The request body is not valid JSON." }, StatusCodes.Status400BadRequest);
            }
        }

        /* #6569: an undo whose reply is lost journals a result-unknown action_called row, which only the store owner may write, so the plan undo runs on the owner source, as the
           MCP undo does. The gate above (IsSignedInAdmin, CanDecide) stays on the web login: it is the web's own, not the journal writer's. */
        var owner = ownerSource?.Read();
        var planChanges = owner is null ? changes : new AgentChangeUndo(owner, McpCommandDeadlines.ReadSeconds);
        var answer = await Mcp.DarlingMcpPlanForceTools.UndoPlanChangeAsync(planChanges, owner, logger, id, login, confirm, shown, context.RequestAborted);
        if (answer.Succeeded)
        {
            return (answer.ToJson(), StatusCodes.Status200OK);
        }

        return UndoRefusalBody(answer);
    }

    /// <summary>The words the browser gets when a plan undo does not succeed.</summary>
    internal const string UndoFailedWords =
        "The undo did not complete on the server. The Darling service log has the server's own words.";

    /// <summary>
    /// The body and status of a plan undo that was refused or failed (#5632, step 9). A refusal carries this code's own sentence. A failure
    /// carries the monitored server's exception text (the executor scrubs the secret from it, but it can still name hosts and addresses), so
    /// the browser gets <see cref="UndoFailedWords"/> and the code instead (#4283). The executor already logged the server's words.
    /// </summary>
    internal static (JsonObject Body, int Status) UndoRefusalBody(PlanUndoAnswer undoRefusal)
    {
        var failed = undoRefusal.Status == PlanUndoAnswer.Failed;
        var refused = new JsonObject
        {
            ["error"] = failed ? UndoFailedWords : undoRefusal.Message,
            ["code"] = undoRefusal.Code,
            ["status"] = undoRefusal.Status,
        };
        if (undoRefusal.State is not null)
        {
            refused["state"] = undoRefusal.State.DeepClone();
        }

        return (refused, undoRefusal.Code == "not_found" ? StatusCodes.Status404NotFound
            : undoRefusal.Code == "bad_login" ? StatusCodes.Status400BadRequest
            : failed ? StatusCodes.Status502BadGateway
            : StatusCodes.Status409Conflict);
    }

    /// <summary>The most calls and refusals the Agent changes list answers.</summary>
    internal const int AgentChangesLimit = 200;

    /// <summary>The list body: <c>{enabled, can_decide, changes: [{called_id, called_at_utc, request_id, server_id, server_name (the registry's name, or "Server N (removed)"), actor, reason, tool, kind, detail,
    /// result (done, failed or unknown), result_detail, undone_at_utc, undone_by, muted_alerts, can_undo}], refusals: [...]}</c>. A call with no
    /// result row is <c>unknown</c>. <c>detail</c> goes through the journal masking helper at the web's level.</summary>
    internal static JsonObject AgentChangesBody(AgentChangesRead read, bool canDecide, DateTime nowUtc)
    {
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.Full);
        var calls = new JsonArray();
        foreach (var c in read.Changes)
        {
            calls.Add(new JsonObject
            {
                ["called_id"] = c.CalledId,
                ["called_at_utc"] = c.CalledAtUtc.ToString("o", CultureInfo.InvariantCulture),
                ["request_id"] = c.RequestId?.ToString(),
                ["server_id"] = c.ServerId,
                ["server_name"] = c.ServerName, ["server_removed"] = c.ServerId is not null && c.ServerName is null,
                ["actor"] = c.Actor,
                ["reason"] = c.Reason,
                ["tool"] = c.Tool,
                ["kind"] = c.Kind,
                ["detail"] = InvestigationFieldMasking.JournalDetail(c.DetailJson, masker),
                ["result"] = c.Result.ToString().ToLowerInvariant(),
                ["result_detail"] = c.ResultJson is null ? null : InvestigationFieldMasking.JournalDetail(c.ResultJson, masker),
                ["undone_at_utc"] = c.UndoneAtUtc?.ToString("o", CultureInfo.InvariantCulture),
                ["undone_by"] = c.UndoneBy,
                ["muted_alerts"] = c.MutedAlerts,
                ["force_review"] = c.ForceReview?.Words,
                ["force_checks_opened"] = c.ForceReview?.Opened,
                ["force_checks_answered"] = c.ForceReview?.Answered,
                ["can_undo"] = c.CanTryUndo(nowUtc),
            });
        }

        var refusals = new JsonArray();
        foreach (var r in read.Refusals)
        {
            refusals.Add(new JsonObject
            {
                ["journal_id"] = r.JournalId, ["at_utc"] = r.AtUtc.ToString("o", CultureInfo.InvariantCulture), ["request_id"] = r.RequestId?.ToString(),
                ["server_id"] = r.ServerId, ["server_name"] = r.ServerName, ["server_removed"] = r.ServerId is not null && r.ServerName is null,
                ["actor"] = r.Actor, ["code"] = r.Code, ["tool"] = r.Tool,
            });
        }

        return new JsonObject { ["enabled"] = read.Enabled, ["can_decide"] = canDecide, ["changes"] = calls, ["refusals"] = refusals };
    }

    /* ─────────────────────── #3450 mute rules: the dedicated write endpoints ─────────────────────── */

    /// <summary>
    /// The mute-rule WRITE surface (#3450): create / update / set-enabled / delete, the web twins of the four
    /// MCP alert-tuning verbs. <see cref="ExcludedToolNames"/> was never a trust boundary — only the list of
    /// tools with no <i>generic</i> <c>/api/read</c> mirror — so, exactly as the Custom Views tools got
    /// <c>/api/views</c>, the mute verbs get dedicated routes here; the read half already ships as
    /// <c>/api/read/get_mute_rules</c>.
    ///
    /// <para><b>Who may call these.</b> Nothing here names a seat, and that is the design: every unsafe method
    /// is born gated by the host's group-level write gate (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/>
    /// refuses a read-only seat everything non-GET except <c>POST /api/compose/run</c>), so an OIDC viewer seat
    /// gets 403 on all four routes while the admin and shared-token seats — and the tokenless loopback operator —
    /// pass, the same enforcement <c>/api/views</c> and <c>/api/alerts</c> ride. <c>application/json</c> is
    /// required on every route (the CSRF defense — see <see cref="IsJsonContentType"/> for why a non-simple
    /// Content-Type closes the simple-request forgery hole), the DELETE included, matching the views surface.</para>
    ///
    /// <para><b>Zero drift with MCP, by construction.</b> Each route is a thin HTTP mapping over the SAME core
    /// the matching MCP verb runs — <see cref="Mcp.DarlingMcpAlertTools.CreateMuteRuleCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.UpdateMuteRuleCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.SetMuteRuleEnabledCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.DeleteMuteRuleCore"/> — over the host's least-privilege VIEWER pool
    /// (whose narrow floor — <c>config_mute_rules</c> plus the two <c>config_service</c> beacon columns the
    /// SECURITY-INVOKER bump trigger writes as the caller — is provisioned beside the mcp role's in
    /// <c>DarlingManagedRoles</c>). The response BODY is the verb's own <c>{status, ...}</c> envelope, verbatim;
    /// only the HTTP status is added (<see cref="MuteRuleEnvelopeStatus"/>), so a web client and an MCP client
    /// read one truth. That buys the cores' invariants unduplicated: PARTIAL update with explicit-null clears
    /// through the store's own full-row <c>UpdateAsync</c>, an <c>unchanged</c> answer for a write that would
    /// change nothing, a re-read after every write so the reported rule is the STORED one, and
    /// <c>created_at_utc</c> never moving through any path — the #3306 clock the whole verb family exists to
    /// preserve. One cosmetic consequence is accepted with it: a refusal's prose speaks the MCP verbs' names
    /// ("use set_mute_rule_enabled"), which is the shared parser's vocabulary and maps 1:1 onto these routes.</para>
    ///
    /// <para><b>The operational caveat these endpoints inherit.</b> Alert rows spell <c>server_name</c> two ways
    /// — the monitor's own self-alert family uses the display-short name, engine alerts use the registry name —
    /// so a rule created or edited from ANY surface can go inert by matching the wrong spelling while looking
    /// scoped. Copy the spelling off the alert rows being muted (<c>/api/read/get_alert_history</c>) rather than
    /// retyping it; the update core's own description carries the same warning on the MCP side.</para>
    ///
    /// <para><b>Route shapes.</b> Create is a POST returning 201 with the stored rule — no <c>Location</c>
    /// header, deliberately: there is no per-rule GET (the read is the list, <c>/api/read/get_mute_rules</c>),
    /// and a Location that 404s is worse than none. Update is a PATCH — not the views surface's PUT — because
    /// the body is a partial document (send only the fields to change; an explicit null clears one) and calling
    /// that a full replace would misstate the one semantic the verb exists for. Set-enabled is a PUT of the tiny
    /// <c>{"enabled": bool}</c> sub-resource, the reversible flag flip that never touches another field. The id
    /// is the rule's GUID string from <c>get_mute_rules</c> / create's response.</para>
    /// </summary>
    private static void MapMuteRules(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new PgMuteRuleStore(postgres);

        /* A server_id in a create or update body must name a registered server, the same check the MCP tools
           make, so the web cannot write a rule keyed on an id that mutes nothing. */
        Func<int, Task<string?>> serverNameLookup = id => Mcp.DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id);

        /* Create — 201 with the STORED rule (re-read after the insert); 400 on a bad body/field/expiry; 409 with
           status already_exists (and the existing rule's id) when an enabled, unexpired rule already has the same
           scope, patterns and expiry, so a client retry leaves one rule (#4734). The
           body is one JSON object of the get_mute_rules field shape; {} is legal and creates a rule that mutes
           EVERY alert (the same whole-fleet silence an argument-less create_mute_rule builds — scope fields
           narrow, they are not required). application/json required. */
        app.MapPost("/api/mute-rules", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.CreateMuteRuleCore(store, await ReadBodyAsync(context), serverNameLookup);
            return MuteRuleToolResult(result, "/api/mute-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        /* Update — PARTIAL, the merged update_mute_rule semantics verbatim: the body carries ONLY the fields to
           change, an explicit JSON null clears one, a field not sent does not move, 'enabled' is refused toward
           the flag route, and created_at_utc is structurally out of reach. 200 updated/unchanged, 404 unknown
           id, 400 bad field/value with nothing written. The raw body text IS the core's changes_json — no
           web-side re-parse to drift. application/json required. */
        app.MapPatch("/api/mute-rules/{id}", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, await ReadBodyAsync(context), serverNameLookup);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Set-enabled — the reversible flag flip that keeps the rule's id, scope, reason and creation date (the
           reason it exists; delete-and-recreate resets the #3306 clock and delivers the suppressed alerts in
           between). 200 updated/unchanged, 404 unknown id. The body is exactly {"enabled": true|false}: strict,
           because the ONE field this route may move is the one field the update route refuses — a stray sibling
           key here is a caller who wanted PATCH. application/json required. */
        app.MapPut("/api/mute-rules/{id}/enabled", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonNode? root;
            try
            {
                root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            }
            catch (JsonException)
            {
                return ErrorResult("Request body is not valid JSON.", StatusCodes.Status400BadRequest);
            }

            if (root is not JsonObject body || body["enabled"] is not JsonValue enabledValue
                || !enabledValue.TryGetValue<bool>(out var enabled) || body.Count != 1)
            {
                return ErrorResult(
                    "Request body must be exactly {\"enabled\": true|false}; any other field belongs to PATCH /api/mute-rules/{id}.",
                    StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, id, enabled);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}/enabled", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Delete — 200 with the verb's {status:"deleted", rule_id} envelope rather than the views surface's
           204, deliberately: the envelope is the ONE shape both surfaces answer with, and it names the id the
           caller should stop citing. 404 unknown id. Permanent — prefer the enabled route for anything
           reversible. application/json required (the same CSRF discipline as the bodyless views DELETE). */
        app.MapDelete("/api/mute-rules/{id}", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.DeleteMuteRuleCore(store, id);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }
}
