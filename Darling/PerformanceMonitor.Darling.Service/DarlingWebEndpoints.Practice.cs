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
    /// <summary>The most alert keys one dismiss request may carry.</summary>
    internal const int MaxDismissKeys = 1000;

    /// <summary>The most request-body bytes the dismiss route reads before it answers 400.</summary>
    internal const int MaxDismissBodyBytes = 256 * 1024;

    /// <summary>The longest metric name a dismiss key may carry; real metric names are far shorter.</summary>
    internal const int MaxDismissMetricNameLength = 512;

    /// <summary>
    /// <c>POST /api/alert-history/dismiss</c>: marks the listed Alert History rows dismissed (the web twin of the
    /// Viewer's Dismiss Selected; Dismiss All is the client sending every visible key, so there is no
    /// dismiss-everything verb). The body is <c>{"alerts":[{"alert_time":"…","server_id":1,"metric_name":"…"}]}</c>,
    /// the identity the Viewer's dismiss keys on, taken from the row <c>get_alert_history</c> returned.
    /// <c>application/json</c> is required (415 otherwise, the CSRF defense the mute routes use); a read-only
    /// seat is refused by the host's group-level write gate before this handler runs
    /// (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> allows only GET/HEAD/OPTIONS for it). The write
    /// runs on the host's <c>viewer</c> pool, whose only privilege here is the column-level
    /// <c>UPDATE (dismissed)</c> on <c>config_alert_log</c>. Keys are deduped; a key that names no alert row is
    /// counted in <c>unknown</c>, not an error. 200 answers <c>{requested, dismissed, already_dismissed, unknown}</c>.
    /// </summary>
    private static void MapAlertHistoryDismiss(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        app.MapPost("/api/alert-history/dismiss", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxDismissBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult(BodyTooLargeText(MaxDismissBodyBytes), StatusCodes.Status400BadRequest);
            }

            var parsed = ParseDismissBody(body, out var keys, out var refusal);
            if (!parsed)
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var outcome = await AlertDismissStore.DismissKeysAsync(
                    postgres, keys, McpCommandDeadlines.ReadSeconds, context.RequestAborted);

                /* Who dismissed is not stored on the row; this line is the only record. Counts only, no alert text. */
                logger.LogInformation(
                    "Alert dismiss by {Principal}: requested {Requested}, dismissed {Dismissed}, known {Known}",
                    DarlingWebSeat.FromContext(context).EditorPrincipal, outcome.Requested, outcome.Dismissed, outcome.Known);

                return JsonNodeResult(new JsonObject
                {
                    ["requested"] = outcome.Requested,
                    ["dismissed"] = outcome.Dismissed,
                    ["already_dismissed"] = Math.Max(0, outcome.Known - outcome.Dismissed),
                    ["unknown"] = Math.Max(0, outcome.Requested - outcome.Known),
                });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-history/dismiss", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /* ─────────────────────── #5913 feedback on investigation reports ─────────────────────── */

    /// <summary>The longest feedback request body the route reads (two 4000-character fields plus the rest, with room for escapes).</summary>
    internal const int MaxFeedbackBodyBytes = 64 * 1024;

    /// <summary>The most reported requests one feedback list answers (the Viewer's list holds the same number).</summary>
    internal const int FeedbackListLimit = 200;

    /// <summary>The 403 sentence a feedback record answers when the store refuses the write (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, and recording feedback is an <c>admin</c> role write (#5913).</summary>
    internal static string FeedbackRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("record feedback", managedStore, "Give feedback in the Viewer app", adminWritesOff: adminWritesOff);

    /* ─────────────────────── #5918 practice scores ─────────────────────── */

    /// <summary>
    /// The web's list of practice scores (#5918, #5632 Phase B): <c>GET /api/investigation-practice-scores</c> answers the score per agent
    /// over the practice requests opened in the last <c>hours_back</c> hours (default 720, at most 8760): <c>{hours_back, agents: [{agent_id,
    /// cases, scored, matched, unscored, points, possible, percent, missed_step_cases}]}</c>, agents in name order, <c>percent</c> null for an
    /// agent with nothing scored yet. This route only reads: the score is advisory, so nothing here decides anything or writes
    /// (a practice request is started by <see cref="MapInvestigationPersonWrites"/>).
    /// </summary>
    internal static void MapInvestigationPractice(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new InvestigationPracticeStore(postgres, McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-practice-scores", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var hours = 720;
            if (context.Request.Query.TryGetValue("hours_back", out var hoursText)
                && (!int.TryParse(hoursText.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out hours) || hours < 1 || hours > 8760))
            {
                return ErrorResult("hours_back must be a whole number from 1 to 8760.", StatusCodes.Status400BadRequest);
            }

            try
            {
                var scores = await store.ReadScoresAsync(DateTime.UtcNow.AddHours(-hours), context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    PracticeScoresBody(scores, hours), "/api/investigation-practice-scores", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-practice-scores", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /* ─────────────────────── #6265 close a request, write a note, start a practice request ─────────────────────── */

    /// <summary>The 403 sentence these three writes answer when the store refuses them for privilege (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, the <c>viewer</c> role on a managed or compose store (#6265).</summary>
    internal static string InvestigationWriteRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("close a request, write a note or start a practice request", managedStore, "Do it in the Viewer app", adminWritesOff: adminWritesOff);

    /// <summary>The most bytes one of these write bodies may carry (a note is at most 2,000 characters).</summary>
    internal const int MaxInvestigationWriteBodyBytes = 64 * 1024;

    /// <summary>
    /// The three investigation writes a person makes (#6265, #5632), the web's twins of the desktop Viewer's: <c>PUT /api/investigation-requests/{id}/close</c>
    /// (body <c>{reason}</c>) closes an open, claimed or reported request; <c>PUT /api/investigation-notes</c> (body <c>{note_text, server_id?, metric_name?, days?}</c>)
    /// writes a note a person stands behind, approved as written (an agent's proposed note still waits for Approve); <c>PUT /api/investigation-practice-requests/{id}</c>
    /// opens a practice request replaying one judged case. They call the same storage code the Viewer does
    /// (<see cref="InvestigationRequestStore.CloseRequestAsync"/>, <see cref="InvestigationRequestStore.WriteNoteAsync"/>,
    /// <see cref="InvestigationPracticeStore.OpenAsync"/>), so the states, the refusals and the journal rows are the same: <c>closed</c>,
    /// <c>note_written</c> and <c>opened</c>, each by <c>person:&lt;signed-in name&gt;</c>. Born gated as the other web writes are: <c>application/json</c>
    /// required, a read-only seat refused by the host's write gate, a store login without the admin role answered 403 with
    /// <see cref="InvestigationWriteRefusedMessage"/>. Each route needs a signed-in admin
    /// (<see cref="DarlingWebSeat.IsSignedInAdmin"/>): a dashboard with no sign-in configured has no caller to name, so it answers 403 with
    /// <see cref="InvestigationWriteSignInMessage"/> and writes nothing. An agent's MCP token is not a web sign-in either.
    /// 400 for a value the rules refuse (in plain words), 404 for an unknown request or case, 409 for a request that cannot be closed or a case that
    /// cannot be replayed, a fixed-sentence 500 or 503 for any other store failure.
    /// </summary>
    internal static void MapInvestigationPersonWrites(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        var requests = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);

        /* #6600: the three writes run as the login the seat's admin writes use; the registered-server check stays on the web's own pool. */
        InvestigationRequestStore WriteRequests(HttpContext context) =>
            adminArm is null ? requests : new InvestigationRequestStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);
        InvestigationPracticeStore WritePractice(HttpContext context) =>
            new(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapPut("/api/investigation-requests/{requestId}/close", async (HttpContext context, string requestId) =>
        {
            const string route = "/api/investigation-requests/{requestId}/close";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var request))
            {
                return ErrorResult(RequestIdNotGuidText, StatusCodes.Status400BadRequest);
            }

            var (body, bodyRefusal) = await ReadInvestigationWriteBodyAsync(context);
            if (body is null)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = bodyRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var reason = body["reason"] is JsonValue reasonValue && reasonValue.TryGetValue<string>(out var reasonText) ? reasonText : null;
            if (InvestigationPersonWrites.ValidateCloseReason(reason) is { } reasonRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = reasonRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                if (!await WriteRequests(context).CloseRequestAsync(request, actor, reason!, context.RequestAborted))
                {
                    return ErrorResult("That request is not open: it is already closed or expired, or it is gone.", StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "closed", ["request_id"] = request.ToString(), ["closed_by"] = actor },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes", async (HttpContext context) =>
        {
            const string route = "/api/investigation-notes";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var (body, bodyRefusal) = await ReadInvestigationWriteBodyAsync(context);
            if (body is null)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = bodyRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var text = body["note_text"] is JsonValue textValue && textValue.TryGetValue<string>(out var noteText) ? noteText : null;
            var metric = body["metric_name"] is JsonValue metricValue && metricValue.TryGetValue<string>(out var metricText) ? metricText : null;
            int? serverId = null;
            if (body["server_id"] is { } serverNode)
            {
                if (serverNode is not JsonValue serverValue || !serverValue.TryGetValue<int>(out var id) || id < 1)
                {
                    return ErrorResult("server_id must be a whole number, or left out for a fleet note.", StatusCodes.Status400BadRequest);
                }

                serverId = id;
            }

            var days = InvestigationPersonWrites.NoteDefaultDays;
            if (body["days"] is { } daysNode && (daysNode is not JsonValue daysValue || !daysValue.TryGetValue<int>(out days)))
            {
                return ErrorResult("days must be a whole number.", StatusCodes.Status400BadRequest);
            }

            if (InvestigationPersonWrites.ValidateNote(text, days, metric) is { } noteRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = noteRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                if (serverId is { } named && !await requests.ServerRegisteredAsync(named, context.RequestAborted))
                {
                    return ErrorResult("That server is not registered.", StatusCodes.Status404NotFound);
                }

                var noteId = await WriteRequests(context).WriteNoteAsync(serverId, metric, text!, actor, days, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "approved", ["note_id"] = noteId, ["author"] = actor, ["server_id"] = serverId, ["days"] = days },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-practice-requests/{requestId}", async (HttpContext context, string requestId) =>
        {
            const string route = "/api/investigation-practice-requests/{requestId}";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var source))
            {
                return ErrorResult(RequestIdNotGuidText, StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var (outcome, practiceId, storeSentence) = await WritePractice(context).OpenAsync(source, actor, context.RequestAborted);
                if (outcome != InvestigationPracticeOutcome.Opened)
                {
                    /* The store's own sentence says why the case was not replayed (never an exception's text). */
                    return DarlingWebStatementSweep.JsonText(
                        new JsonObject { ["error"] = storeSentence ?? "The practice request was not opened.", ["outcome"] = outcome.ToString(), ["request_id"] = practiceId?.ToString() },
                        route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted,
                        outcome == InvestigationPracticeOutcome.NotFound ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "opened", ["request_id"] = practiceId?.ToString(), ["source_request_id"] = source.ToString(), ["opened_by"] = actor },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The journal actor of a person's investigation write: <c>person:&lt;signed-in name&gt;</c> (the seat's
    /// <see cref="DarlingWebSeat.EditorPrincipal"/>), or null when the built actor is not one the journal may hold: a name the undo path
    /// refuses (invisible, format or bidi characters, whitespace, over 64 characters) or an actor longer than 128.</summary>
    private static string? PersonWriteActor(HttpContext context)
    {
        var actor = InvestigationPersonWrites.PersonActor(DarlingWebSeat.FromContext(context).EditorPrincipal);
        return InvestigationPersonWrites.ValidateActor(actor) is null ? actor : null;
    }

    private const string PersonWriteActorRefusal = "The signed-in name cannot be recorded as the person who made this change.";

    /// <summary>The 400 a person-made change answers when the signed-in name cannot be recorded: the fixed <see cref="PersonWriteActorRefusal"/> sentence, never the name.</summary>
    private static IResult PersonActorRefusedResult() => ErrorResult(PersonWriteActorRefusal, StatusCodes.Status400BadRequest);

    /// <summary>Reads a bounded JSON object body for the investigation writes: the object, or the plain sentence that refuses it.</summary>
    private static async Task<(JsonObject? Body, string? Refusal)> ReadInvestigationWriteBodyAsync(HttpContext context)
    {
        string text;
        try
        {
            text = await ReadBoundedBodyAsync(context, MaxInvestigationWriteBodyBytes);
        }
        catch (InvalidDataException)
        {
            return (null, BodyTooLargeText(MaxInvestigationWriteBodyBytes));
        }

        try
        {
            if (JsonNode.Parse(text) is JsonObject body)
            {
                /* A JsonObject reads its properties lazily, so a duplicate key surfaces as an ArgumentException on first access, outside
                   this try. Touch Count here so the duplicate is refused as a body that is not a JSON object, not an unhandled 500. */
                _ = body.Count;
                return (body, null);
            }
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
        }

        return (null, "The body must be a JSON object.");
    }

    /// <summary>PURE: the practice scores body (see <see cref="MapInvestigationPractice"/>).</summary>
    internal static JsonObject PracticeScoresBody(IReadOnlyList<InvestigationPracticeScore> scores, int hoursBack)
    {
        var agents = new JsonArray();
        foreach (var s in scores)
        {
            agents.Add(new JsonObject
            {
                ["agent_id"] = s.AgentId,
                ["cases"] = s.Cases,
                ["scored"] = s.Scored,
                ["matched"] = s.Matched,
                ["unscored"] = s.Unscored,
                ["points"] = s.Points,
                ["possible"] = s.Possible,
                ["percent"] = s.Percent is { } p ? Math.Round(p * 100, 1) : null,
                ["missed_step_cases"] = s.MissedStepCases,
            });
        }

        return new JsonObject { ["hours_back"] = hoursBack, ["agents"] = agents };
    }

    /// <summary>
    /// <c>GET /api/webhook-signing</c> (#5998): whether the settings row's generic webhook and each route that has its own key holds
    /// a signing key, whether a rotation is in its window and when it ends. A status page only: the web has no write surface for
    /// notifications, and the answer is served from what the service read when it applied the store's configuration, so it holds no
    /// key text and does not depend on the web login being able to read the key columns.
    /// </summary>
    internal static void MapWebhookSigningStatus(WebApplication app, CollectorRuntimeState collector, Func<DateTime>? utcNow = null)
    {
        var clock = utcNow ?? (() => DateTime.UtcNow);
        app.MapGet("/api/webhook-signing", () => Results.Text(collector.SigningStatus.Answer(clock()).ToJsonString(), "application/json"));
    }

    /// <summary>
    /// The feedback routes (#5913): <c>GET /api/investigation-feedback</c> lists the reported requests of the last
    /// <c>hours_back</c> hours (default 168, at most 2160) with the feedback people gave each, and the false-positive counts per alert
    /// family; <c>PUT /api/investigation-feedback/{requestId}</c> records the signed-in person's feedback on one report.
    /// The write answers as the mute-rule writes do: <c>application/json</c> required, a read-only seat refused by the host's write gate,
    /// 400 for a value the rules refuse (<see cref="InvestigationFeedbackRules"/>, in plain words), 404 for an unknown request, 409 for
    /// a request with no report, 403 with <see cref="FeedbackRefusedMessage"/> when the store refuses the write for privilege (the web
    /// decides only under an admin store login, as the notes screen does), a fixed-sentence 500 or 503 for any other store failure.
    /// The author is the seat's <see cref="DarlingWebSeat.EditorPrincipal"/> shaped by <see cref="InvestigationFeedbackRules.AuthorFromIdentity"/>.
    /// A second record by the same author replaces the first. Bodies go through the statement sweep (#4348): they carry text an agent
    /// and a person wrote.
    /// </summary>
    internal static void MapInvestigationFeedback(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        var store = new InvestigationFeedbackStore(postgres, McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-feedback", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var hours = 168;
            if (context.Request.Query.TryGetValue("hours_back", out var hoursText)
                && (!int.TryParse(hoursText.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out hours) || hours < 1 || hours > 2160))
            {
                return ErrorResult("hours_back must be a whole number from 1 to 2160.", StatusCodes.Status400BadRequest);
            }

            try
            {
                var since = DateTime.UtcNow.AddHours(-hours);
                /* One connection for the list, the counts and the capability probe (#5913). */
                await using var connection = await postgres.OpenConnectionAsync(context.RequestAborted);
                var (rows, truncated) = await store.ListReportedAsync(connection, since, null, FeedbackListLimit, context.RequestAborted);
                var counts = await store.FamilyCountsAsync(connection, since, null, context.RequestAborted);
                /* #6600: the capability probe runs as the login the write would use. */
                var recordSource = WebAdminArm.SourceFor(adminArm, context, postgres);
                bool canRecord;
                if (ReferenceEquals(recordSource, postgres))
                {
                    canRecord = await store.CanRecordAsync(connection, context.RequestAborted);
                }
                else
                {
                    await using var recordConnection = await recordSource.OpenConnectionAsync(context.RequestAborted);
                    canRecord = await store.CanRecordAsync(recordConnection, context.RequestAborted);
                }

                return DarlingWebStatementSweep.JsonText(
                    FeedbackListBody(rows, truncated, counts, hours, canRecord), "/api/investigation-feedback", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-feedback", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-feedback/{requestId}", async (HttpContext context, string requestId) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var request))
            {
                return ErrorResult(RequestIdNotGuidText, StatusCodes.Status400BadRequest);
            }

            string text;
            try
            {
                text = await ReadBoundedBodyAsync(context, MaxFeedbackBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult(BodyTooLargeText(MaxFeedbackBodyBytes), StatusCodes.Status400BadRequest);
            }

            if (!TryParseFeedbackBody(text, request, DarlingWebSeat.FromContext(context).EditorPrincipal, out var submission, out var refusal))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = refusal! }, "/api/investigation-feedback/{requestId}",
                    logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var writeStore = new InvestigationFeedbackStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);
                var result = await writeStore.RecordAsync(submission!, context.RequestAborted);
                /* The store's own sentence for a refused, unknown or unreported request (never an exception's text). */
                var (outcome, sentence) = result;
                if (outcome is not (InvestigationFeedbackOutcome.Recorded or InvestigationFeedbackOutcome.Replaced))
                {
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = sentence! },"/api/investigation-feedback/{requestId}",
                        logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, FeedbackRefusalStatus(outcome));
                }

                var receipt = FeedbackReceipt(outcome, request, submission!.Author);
                return DarlingWebStatementSweep.JsonText(
                    receipt, "/api/investigation-feedback/{requestId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsFeedbackPrivilegeRefusal(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = FeedbackRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-feedback/{requestId}",
                    logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-feedback/{requestId}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The 403 sentence the export answers a login that may not record feedback (#6236).</summary>
    internal static string InvestigationExportRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("export investigation cases", managedStore, "Run --export-investigations on the host", adminWritesOff: adminWritesOff);

    /// <summary>The 403 sentence the export answers a request that is not a signed-in admin session (#6236).</summary>
    internal const string InvestigationExportSeatMessage =
        "Exporting investigation cases needs a signed-in dashboard account with the admin role. A viewer account, and a dashboard with no sign-in "
        + "configured, cannot: run --export-investigations on the host instead.";

    /// <summary>The route name the export is admitted and recorded under (#6236).</summary>
    internal const string InvestigationExportRoute = "investigation_export";

    /// <summary>The seconds a second export is told to wait when one is already running (#6236).</summary>
    internal const int InvestigationExportRetryAfterSeconds = 30;

    /// <summary>
    /// The download of the investigation cases that have feedback (#6236): <c>GET /api/investigation-export?since=&amp;until=&amp;server=&amp;verdict=</c>
    /// answers JSON lines (<c>application/x-ndjson</c>, one case per line, the same bytes <c>--export-investigations</c> writes) as an attachment.
    /// Two gates, both required: the request is a signed-in dashboard session whose account holds the admin role
    /// (<see cref="DarlingWebSeat.IsSignedInAdmin"/>; a viewer account and a dashboard with no sign-in are refused), and the web login holds INSERT and
    /// UPDATE on the feedback table (<see cref="InvestigationFeedbackStore.CanRecordAsync"/>). The route is heavy from its first call (<see cref="ReadAdmission.AlwaysHeavyRoutes"/>), only one
    /// export runs at a time (a second answers 429 with Retry-After), and the read-admission slot is released before the file is sent.
    /// Text is masked at the <c>mcp.dataDetail</c> the service started with, with the store's pseudonym key, by <see cref="InvestigationFeedbackExport"/>,
    /// the same code the command line runs. The cases are written one at a time to a work file in the service's own data folder and the file is then sent
    /// through the same handle (deleted when it closes), so a refused case refuses the whole download and no case is held in memory. 400 for a date, verdict or server it cannot use.
    /// </summary>
    internal static void MapInvestigationExport(
        WebApplication app, NpgsqlDataSource postgres, ILogger logger, McpDataDetail dataDetail, IPseudonymKeySource? pseudonymKeys,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null, InvestigationExportGate? exportGate = null,
        bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        exportGate ??= new InvestigationExportGate(InvestigationExportGate.FolderFor(DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig())));
        var feedbackStore = new InvestigationFeedbackStore(postgres, McpCommandDeadlines.ReadSeconds);
        var requestStore = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);

        app.MapGet("/api/investigation-export", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportSeatMessage }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }

            var since = ExportDate(context, "since", out var badSince);
            var until = ExportDate(context, "until", out var badUntil);
            if (badSince || badUntil)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = "since and until are dates such as 2026-10-01 or 2026-10-01T06:00:00Z." }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var verdict = context.Request.Query.TryGetValue("verdict", out var verdictText) ? verdictText.ToString() : null;
            if (InvestigationFeedbackExport.FilterRefusal(since, until, verdict) is { } filterRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = filterRefusal }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            /* One export runs at a time, from before its work file exists until the file is gone: a second request answers busy and
               creates nothing. */
            if (!exportGate.TryEnter())
            {
                return BusyResult(new ReadAdmissionRefusal(
                    "An investigation export is already running. Try again when it has finished.", InvestigationExportRetryAfterSeconds, 1, 0));
            }

            /* Up to 2000 cases are read, so the route is heavy from its first call (ReadAdmission.AlwaysHeavyRoutes). The slot covers the
               store read into the work file only: it is released before the file is sent. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, InvestigationExportRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                exportGate.Exit();
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, InvestigationExportRoute, ReadWindowBand.None, context, readScope.Scope);
            try
            {
                /* #6600: the access probe runs as the login a feedback record would use; the cases are still read on the web's own pool. */
                await using (var connection = await WebAdminArm.SourceFor(adminArm, context, postgres).OpenConnectionAsync(context.RequestAborted))
                {
                    if (!await feedbackStore.CanRecordAsync(connection, context.RequestAborted))
                    {
                        run.Complete(ReadOutcome.GateFailed);
                        return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
                    }
                }

                var (serverId, serverLabel, serverError) = await InvestigationFeedbackExport.ResolveServerAsync(postgres, context.Request.Query.TryGetValue("server", out var serverText) ? serverText.ToString() : null, context.RequestAborted);
                if (serverError is not null)
                {
                    run.Complete(ReadOutcome.Ok);
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = serverError }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
                }

                var key = await PseudonymKeySources.KeyForAsync(pseudonymKeys, dataDetail, context.RequestAborted);
                /* One handle for the whole life of the file: written, rewound and sent through the same handle, never reopened by name. */
                await using var file = InvestigationExportFile.CreateScratch(exportGate.Folder);
                var outcome = await InvestigationFeedbackExport.WriteAsync(
                    requestStore, since, until, serverId, verdict, dataDetail, key, file, context.RequestAborted);
                await file.FlushAsync(context.RequestAborted);
                file.Position = 0;

                if (outcome.Refused)
                {
                    return DarlingWebStatementSweep.Refusal("/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                context.Response.ContentType = "application/x-ndjson; charset=utf-8";
                context.Response.Headers.ContentDisposition = "attachment; filename=\"investigation-cases.jsonl\"";
                if (outcome.Truncated)
                {
                    context.Response.Headers["X-Export-Truncated"] = InvestigationRequestStore.FeedbackExportLimit.ToString(CultureInfo.InvariantCulture);
                }

                if (outcome.HeldBack > 0)
                {
                    context.Response.Headers["X-Export-Held-Back"] = outcome.HeldBack.ToString(CultureInfo.InvariantCulture);
                }

                if (serverLabel is not null)
                {
                    context.Response.Headers["X-Export-Server"] = Uri.EscapeDataString(serverLabel);
                }

                /* The store work is done: the slot and the route's timing end here, so a slow download holds neither. The one-export gate
                   stays held until the file is gone. */
                run.Complete(ReadOutcome.Ok);
                run.Dispose();
                ticket.Dispose();
                if (exportGate.BeforeSend is { } beforeSend)
                {
                    await beforeSend();
                }

                context.Response.ContentLength = file.Length;
                await file.CopyToAsync(context.Response.Body, context.RequestAborted);
                return Results.Empty;
            }
            catch (Exception ex) when (IsFeedbackPrivilegeRefusal(ex))
            {
                run.Complete(ReadOutcome.GateFailed);
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-export", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
            finally
            {
                exportGate.Exit();
            }
        });
    }

    /// <summary>One date query parameter of the export (#6236): null when absent, with <paramref name="bad"/> set when it is not a date.</summary>
    private static DateTime? ExportDate(HttpContext context, string name, out bool bad)
    {
        bad = false;
        if (!context.Request.Query.TryGetValue(name, out var text) || string.IsNullOrWhiteSpace(text))
            return null;
        if (DateTimeOffset.TryParse(text.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var when))
            return when.UtcDateTime;
        bad = true;
        return null;
    }

    internal static JsonObject FeedbackReceipt(InvestigationFeedbackOutcome outcome, Guid request, string author) => new()
    {
        ["status"] = outcome == InvestigationFeedbackOutcome.Recorded ? "recorded" : "replaced",
        ["request_id"] = request.ToString(),
        ["author"] = author,
    };

    /// <summary>The status a refused feedback record answers: 404 for an unknown request, 409 for one with no report, else 400.</summary>
    private static int FeedbackRefusalStatus(InvestigationFeedbackOutcome outcome) => outcome switch
    {
        InvestigationFeedbackOutcome.NotFound => StatusCodes.Status404NotFound,
        InvestigationFeedbackOutcome.NoReport => StatusCodes.Status409Conflict,
        _ => StatusCodes.Status400BadRequest,
    };

    /// <summary>True when the store refused the feedback write for privilege (SQLSTATE 42501).</summary>
    private static bool IsFeedbackPrivilegeRefusal(Exception ex) => ex is PostgresException { SqlState: "42501" };

    /// <summary>
    /// PURE: parses a feedback body, <c>{"verdict": "right|partly|wrong", "corrected_verdict": null|"real"|"false_positive"|"needs_person",
    /// "missed_step": null|"...", "comment": null|"..."}</c>, into the submission for <paramref name="request"/> by <paramref name="identity"/>.
    /// False with a caller-facing <paramref name="refusal"/> when the body is not a JSON object, a field is not text, or a value fails
    /// <see cref="InvestigationFeedbackRules"/>. Unknown fields are ignored.
    /// </summary>
    internal static bool TryParseFeedbackBody(string body, Guid request, string identity, out InvestigationFeedbackSubmission? submission, out string? refusal)
    {
        submission = null;
        refusal = null;
        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(body);
        }
        catch (JsonException)
        {
            refusal = "The body is not JSON.";
            return false;
        }

        if (parsed is not JsonObject obj)
        {
            refusal = "The body must be a JSON object with a verdict.";
            return false;
        }

        string? Field(string name, out bool bad)
        {
            bad = false;
            if (!obj.TryGetPropertyValue(name, out var node) || node is null)
            {
                return null;
            }

            if (node is JsonValue value && value.TryGetValue<string>(out var text))
            {
                return text;
            }

            bad = true;
            return null;
        }

        var verdict = Field("verdict", out var badVerdict);
        var corrected = Field("corrected_verdict", out var badCorrected);
        var missed = Field("missed_step", out var badMissed);
        var comment = Field("comment", out var badComment);
        if (badVerdict || badCorrected || badMissed || badComment)
        {
            refusal = "verdict, corrected_verdict, missed_step and comment must each be text or null.";
            return false;
        }

        /* A sign-in name the table's author rule cannot hold is shaped, not refused: DOMAIN\erik reads DOMAIN_erik-1a2b3c4d (a hash suffix keeps two such sign-ins apart). */
        var author = InvestigationFeedbackRules.AuthorFromIdentity(identity) ?? "web";
        var candidate = new InvestigationFeedbackSubmission(
            request, verdict ?? "", string.IsNullOrWhiteSpace(corrected) ? null : corrected,
            string.IsNullOrWhiteSpace(missed) ? null : missed, string.IsNullOrWhiteSpace(comment) ? null : comment, author);
        var problems = InvestigationFeedbackRules.Validate(candidate);
        if (problems.Count > 0)
        {
            refusal = InvestigationFeedbackRules.RefusalMessage(problems);
            return false;
        }

        submission = candidate;
        return true;
    }

    /// <summary>The list body: <c>{hours_back, can_record, requests_returned, truncated, families: [{family, reported, with_feedback, marked_wrong,
    /// agent_false_positives, false_positives}], requests: [{request_id, server_name, family, first_metric, opened_at_utc, reported_at_utc,
    /// agent_id, report_verdict, root_cause, summary, feedback: [{verdict, corrected_verdict, missed_step, comment, author, recorded_at_utc}]}]}</c>,
    /// newest report first. <c>can_record</c> is whether the web dashboard's own store login may write feedback (false on managed and
    /// compose stores, where it connects as the viewer role): the page shows its form only when it is true (#5913).</summary>
    internal static JsonObject FeedbackListBody(
        IReadOnlyList<InvestigationFeedbackListRow> rows, bool truncated, IReadOnlyList<InvestigationFamilyCounts> counts, int hoursBack,
        bool canRecord)
    {
        static string Iso(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
        var requests = new JsonArray();
        foreach (var row in rows)
        {
            var feedback = new JsonArray();
            foreach (var f in row.Feedback)
            {
                feedback.Add(new JsonObject
                {
                    ["verdict"] = f.Verdict,
                    ["corrected_verdict"] = f.CorrectedVerdict,
                    ["missed_step"] = f.MissedStep,
                    ["comment"] = f.Comment,
                    ["author"] = f.Author,
                    ["recorded_at_utc"] = Iso(f.CreatedAtUtc),
                });
            }

            requests.Add(new JsonObject
            {
                ["request_id"] = row.RequestId.ToString(),
                ["server_name"] = row.ServerName,
                ["family"] = row.Family,
                ["first_metric"] = row.FirstMetric,
                ["opened_at_utc"] = Iso(row.OpenedAtUtc),
                ["reported_at_utc"] = Iso(row.ReportedAtUtc),
                ["agent_id"] = row.AgentId,
                ["report_verdict"] = row.ReportVerdict,
                ["root_cause"] = row.RootCause,
                ["summary"] = row.Summary,
                ["feedback"] = feedback,
            });
        }

        return new JsonObject
        {
            ["hours_back"] = hoursBack,
            ["can_record"] = canRecord,
            ["requests_returned"] = requests.Count,
            ["truncated"] = truncated,
            ["families"] = InvestigationToolCore.FamilyCountsPayload(counts),
            ["requests"] = requests,
        };
    }

    /// <summary>
    /// PURE: parses a dismiss body into its distinct keys, in first-seen order. False with a caller-facing
    /// <paramref name="refusal"/> when the body is not a JSON object with a non-empty <c>alerts</c> array of at
    /// most <see cref="MaxDismissKeys"/> objects, each with an ISO-8601 <c>alert_time</c>, an integer
    /// <c>server_id</c> and a non-empty <c>metric_name</c>. A time that carries an offset is converted to UTC; a
    /// bare time is taken as the store's naive UTC, which is how <c>get_alert_history</c> spells it.
    /// </summary>
    internal static bool ParseDismissBody(string body, out List<AlertDismissKey> keys, out string? refusal)
    {
        keys = new List<AlertDismissKey>();
        refusal = null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(body);
        }
        catch (JsonException)
        {
            refusal = "Request body must be a JSON object.";
            return false;
        }

        /* A duplicate property name makes the JsonObject indexer throw ArgumentException; that is a caller's
           malformed body (400), not a server fault. */
        JsonNode? alertsNode = null;
        try
        {
            alertsNode = (root as JsonObject)?["alerts"];
        }
        catch (ArgumentException)
        {
            refusal = "Request body must be a JSON object with one alerts array.";
            return false;
        }

        if (root is not JsonObject || alertsNode is not JsonArray alerts)
        {
            refusal = "Request body must be {\"alerts\": [{\"alert_time\", \"server_id\", \"metric_name\"}, ...]}.";
            return false;
        }

        if (alerts.Count == 0)
        {
            refusal = "alerts must name at least one alert.";
            return false;
        }

        if (alerts.Count > MaxDismissKeys)
        {
            refusal = $"alerts may name at most {MaxDismissKeys} alerts per request.";
            return false;
        }

        var seen = new HashSet<AlertDismissKey>();
        foreach (var item in alerts)
        {
            if (item is not JsonObject entry
                || !TryReadString(entry["alert_time"], out var timeText)
                || !TryReadString(entry["metric_name"], out var metric)
                || metric.Length == 0 || metric.Length > MaxDismissMetricNameLength
                || entry["server_id"] is not JsonValue idValue || !idValue.TryGetValue<int>(out var serverId)
                || !DateTime.TryParse(
                    timeText, CultureInfo.InvariantCulture,
                    DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var time))
            {
                refusal = "Each alert needs an ISO-8601 alert_time, an integer server_id and a non-empty metric_name.";
                return false;
            }

            var key = new AlertDismissKey(DateTime.SpecifyKind(time, DateTimeKind.Unspecified), serverId, metric);
            if (seen.Add(key))
            {
                keys.Add(key);
            }
        }

        return true;
    }

    private static bool TryReadString(JsonNode? node, out string value)
    {
        value = "";
        if (node is JsonValue v && v.TryGetValue<string>(out var text) && text is not null)
        {
            value = text;
            return true;
        }

        return false;
    }

    /// <summary>The 400 text for a body over an endpoint's limit. It names the limit, which each endpoint passes in.</summary>
    internal static string BodyTooLargeText(int maxBytes) =>
        $"The request is larger than the {maxBytes / 1024} KB this endpoint accepts. Send less data.";

    /// <summary>The 403 text when a read-only account tries a change.</summary>
    internal const string ReadOnlyAccountText =
        "This account has read-only access, so it cannot make this change. Ask an admin, or sign in with an admin account.";

    /// <summary>The 400 text when a request id is not a GUID.</summary>
    internal const string RequestIdNotGuidText = "The request id must be a GUID such as 3f2504e0-4f89-11d3-9a0c-0305e82c3301.";

    /// <summary>Reads the request body, refusing (<see cref="InvalidDataException"/>) past <paramref name="maxBytes"/>.</summary>
    private static async Task<string> ReadBoundedBodyAsync(HttpContext context, int maxBytes)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[8192];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
        {
            if (buffer.Length + read > maxBytes)
            {
                throw new InvalidDataException(BodyTooLargeText(maxBytes));
            }

            buffer.Write(chunk, 0, read);
        }

        return System.Text.Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }
}
