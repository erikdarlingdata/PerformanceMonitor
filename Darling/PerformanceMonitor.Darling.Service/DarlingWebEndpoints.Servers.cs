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
    /// <c>GET /api/admin/servers</c> (#5239): the Admin page's Manage Servers grid - every configured server,
    /// enabled or disabled, with its authentication mode, monthly cost and added time (see
    /// <see cref="DarlingAdminServersReader"/>). It is a web-only route: <c>list_servers</c> keeps its
    /// enabled-only answer for every other consumer, and a route here adds nothing to the MCP <c>tools/list</c>.
    /// The gate is the one every web route sits behind (the sign-in gate, then the seat's method gate, which
    /// lets a read-only seat make a GET); the read runs on the viewer-role pool, whose column grant on
    /// <c>config_monitored_servers</c> leaves out the credential columns, and the statement names none of them.
    /// </summary>
    /// <summary>The most request-body bytes the run-collectors route reads (a list of collector names).</summary>
    internal const int MaxCollectorRunBodyBytes = 16 * 1024;

    /// <summary>
    /// The run-collectors-now routes (#5941). <c>POST /api/collectors/run</c> (body: the server name, optional collectors) queues a <c>snapshot_now</c> for
    /// one server: an empty body runs every enabled collector, <c>{"collectors":["name", ...]}</c> runs only those, each
    /// even when disabled. It needs the edit seat (a read-only seat gets 403) and goes through <see cref="SnapshotNowRequests"/>,
    /// the same guard the MCP tool uses: 202 queued, 409 when a run for the server is queued or going, 429 (with Retry-After)
    /// within 60 seconds of the last one, 400 for an unknown collector name, 404 for an unknown server. The page then polls
    /// <c>GET /api/collectors/run/{commandId}</c> for the result the service wrote: the collectors run,
    /// the rows, and the ones it skipped with the reason.
    /// </summary>
    internal static void MapCollectorRuns(
        WebApplication app, ISnapshotNowStore store, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, TimeProvider? clock = null)
    {
        var requests = new SnapshotNowRequests(store, clock);

        app.MapPost("/api/collectors/run", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxCollectorRunBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            IReadOnlyList<string>? collectors = null;
            try
            {
                var parsedBody = JsonNode.Parse(body) as JsonObject;
                serverName = parsedBody?["server"]?.GetValue<string>();
                if (parsedBody?["collectors"] is JsonNode list)
                {
                    if (!SnapshotNowArgs.TryParse(new JsonObject { ["collectors"] = list.DeepClone() }.ToJsonString(), out var parsed, out var parseError))
                    {
                        return ErrorResult(parseError!, StatusCodes.Status400BadRequest);
                    }

                    collectors = parsed;
                }
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name and an optional \"collectors\" list.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            SnapshotRequestOutcome outcome;
            var stopwatch = Stopwatch.StartNew();
            int id;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await requests.RequestAsync(id, collectors, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"run collectors request failed ({ex.GetType().Name})", "/api/collectors/run", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (outcome.Accepted)
            {
                logger.LogInformation("Web: {Principal} queued a collector run for server {ServerId} ({Scope}).",
                    seat.EditorPrincipal, id, collectors is { Count: > 0 } ? string.Join(",", collectors) : "all enabled");
                return JsonNodeResult(
                    new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
            }

            var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
            if (outcome.NextAllowedUtc is { } next)
            {
                refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            var status = outcome.Code switch
            {
                SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                _ => StatusCodes.Status400BadRequest,
            };
            return JsonNodeResult(refusal, status);
        });

        app.MapGet("/api/collectors/run/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            SnapshotCommandState? state;
            var stopwatch = Stopwatch.StartNew();
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"run collectors read failed ({ex.GetType().Name})", "/api/collectors/run/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                return ErrorResult("No such collector run.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            var node = new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["result_status"] = state.ResultStatus,
            };
            if (state.ResultJson is not null)
            {
                try
                {
                    node["result"] = JsonNode.Parse(state.ResultJson);
                }
                catch (JsonException)
                {
                    /* The service wrote it; if it is not JSON the page shows the status alone. */
                }
            }

            return JsonNodeResult(node);
        });
    }

    /// <summary>The most request-body bytes the live Active Queries route reads (a server name).</summary>
    internal const int MaxActiveQueriesLiveBodyBytes = 4 * 1024;

    /// <summary>
    /// The live Active Queries routes (#6386), built like the run-collectors-now routes. <c>POST /api/active-queries/live</c> (body: the
    /// server name) queues the same argument-less <c>fetch_active_queries</c> command the Viewer's Current Active Queries queues, through
    /// <c>config.request_active_queries_live</c> (the viewer role has no INSERT on the command queue). It needs the edit seat (403
    /// otherwise): 202 queued, 409 when a read for the server is queued or going, 429 (with Retry-After) within 10 seconds of the last,
    /// 404 for an unknown server, 403 when the web's store login cannot execute the function (a store provisioned before this release).
    /// <c>GET /api/active-queries/live/{commandId}</c> returns the command's state and the service's <c>result_json</c> (the running
    /// requests, one object per request, PascalCase as the service serializes them). <c>GET /api/active-queries/live/available</c>
    /// says whether this seat and store login can queue one, so the page draws the control or the note.
    /// </summary>
    internal static void MapActiveQueriesLive(
        WebApplication app, IActiveQueriesLiveStore store, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, bool managedStore, TimeProvider? clock = null,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        app.MapGet("/api/active-queries/live/available", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var storeCan = seat.CanEdit && await store.CanQueueAsync(context.RequestAborted);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["can_queue"] = storeCan, ["can_edit"] = seat.CanEdit, ["managed_store"] = managedStore });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries availability failed ({ex.GetType().Name})", "/api/active-queries/live/available", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });

        app.MapPost("/api/active-queries/live", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxActiveQueriesLiveBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            try
            {
                serverName = (JsonNode.Parse(body) as JsonObject)?["server"]?.GetValue<string>();
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            int id;
            SnapshotRequestOutcome outcome;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await ActiveQueriesLiveRequests.RequestAsync(store, id, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries request failed ({ex.GetType().Name})", "/api/active-queries/live", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (!outcome.Accepted)
            {
                var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
                if (outcome.NextAllowedUtc is { } next)
                {
                    refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                    var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                    context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                return JsonNodeResult(refusal, outcome.Code switch
                {
                    SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                    SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                    SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                    ActiveQueriesLiveRequests.StoreRefusedCode => StatusCodes.Status403Forbidden,
                    _ => StatusCodes.Status400BadRequest,
                });
            }

            logger.LogInformation("Web: {Principal} queued a live active-queries read for server {ServerId}.", seat.EditorPrincipal, id);
            return JsonNodeResult(new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
        });

        app.MapGet("/api/active-queries/live/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            SnapshotCommandState? state;
            var stopwatch = Stopwatch.StartNew();
            /* The result read carries the plan XML a live read stored, so it is admitted and recorded like the other web reads. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, ActiveQueriesLiveResultRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, ActiveQueriesLiveResultRoute, ReadWindowBand.None, context, readScope.Scope);
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries read failed ({ex.GetType().Name})", "/api/active-queries/live/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                run.Complete(ReadOutcome.Ok);
                return ErrorResult("No such live read.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            var node = new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["result_status"] = state.ResultStatus,
            };
            if (state.ResultJson is not null)
            {
                try
                {
                    node["result"] = JsonNode.Parse(state.ResultJson);
                }
                catch (JsonException)
                {
                    /* The service wrote it; if it is not JSON the page shows the status alone. */
                }
            }

            run.Complete(ReadOutcome.Ok);
            return JsonNodeResult(node);
        });
    }

    /// <summary>The most request-body bytes the analyze-now route reads (a server name).</summary>
    internal const int MaxAnalyzeNowBodyBytes = 4 * 1024;

    /// <summary>The longest finding title the Ask AI route builds a prompt around (a headline is a sentence).</summary>
    internal const int MaxAskAiTitleLength = 500;

    /// <summary>
    /// The Recommendations tab's routes (#6389). <c>POST /api/analyze-now</c> (body: the server name) queues the same argument-less
    /// <c>analyze_now</c> command the Viewer's Generate now queues, through <c>config.request_analyze_now</c> (the viewer role has no INSERT
    /// on the command queue). Built like <see cref="MapActiveQueriesLive"/>: edit seat only (403 otherwise), 202 queued, 409 when a run for
    /// the server is queued or going, 429 (with Retry-After) within 15 seconds of the last, 404 for an unknown server, 403 when the web's
    /// store login cannot execute the function. <c>GET /api/analyze-now/{commandId}</c> is the command's state (the page polls it and
    /// re-reads the findings when it finishes); <c>GET /api/analyze-now/available</c> says whether this seat and store login can queue a
    /// run. <c>GET /api/analysis/ask-ai-prompt</c> returns the text the Viewer's Ask AI copies, from the one shared builder: no key and no
    /// model call, only a prompt for the reader's own MCP client.
    /// </summary>
    internal static void MapAnalyzeNow(
        WebApplication app, IAnalyzeNowStore store, NpgsqlDataSource postgres, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, bool managedStore, TimeProvider? clock = null)
    {
        app.MapGet("/api/analyze-now/available", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var storeCan = seat.CanEdit && await store.CanQueueAsync(context.RequestAborted);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["can_queue"] = storeCan, ["can_edit"] = seat.CanEdit, ["managed_store"] = managedStore });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now availability failed ({ex.GetType().Name})", "/api/analyze-now/available", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });

        app.MapPost("/api/analyze-now", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxAnalyzeNowBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            try
            {
                serverName = (JsonNode.Parse(body) as JsonObject)?["server"]?.GetValue<string>();
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            int id;
            SnapshotRequestOutcome outcome;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await AnalyzeNowRequests.RequestAsync(store, id, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now request failed ({ex.GetType().Name})", "/api/analyze-now", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (!outcome.Accepted)
            {
                var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
                if (outcome.NextAllowedUtc is { } next)
                {
                    refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                    var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                    context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                return JsonNodeResult(refusal, outcome.Code switch
                {
                    SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                    SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                    SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                    AnalyzeNowRequests.StoreRefusedCode => StatusCodes.Status403Forbidden,
                    _ => StatusCodes.Status400BadRequest,
                });
            }

            logger.LogInformation("Web: {Principal} queued an analysis run for server {ServerId}.", seat.EditorPrincipal, id);
            return JsonNodeResult(new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
        });

        app.MapGet("/api/analyze-now/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            var stopwatch = Stopwatch.StartNew();
            SnapshotCommandState? state;
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now read failed ({ex.GetType().Name})", "/api/analyze-now/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                return ErrorResult("No such analysis run.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            return JsonNodeResult(new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["succeeded"] = state.Status == "succeeded",
                ["result_status"] = state.ResultStatus,
            });
        });

        app.MapGet("/api/analysis/ask-ai-prompt", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var server = context.Request.Query["server"].ToString();
            var title = context.Request.Query["title"].ToString();
            if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(title) || title.Length > MaxAskAiTitleLength)
            {
                return ErrorResult("The request must name the server and a finding title (at most " + MaxAskAiTitleLength + " characters).", StatusCodes.Status400BadRequest);
            }

            DateTime? start = null;
            DateTime? end = null;
            var startText = context.Request.Query["start"].ToString();
            var endText = context.Request.Query["end"].ToString();
            if (startText.Length > 0 || endText.Length > 0)
            {
                const System.Globalization.DateTimeStyles utc = System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal;
                if (!DateTime.TryParse(startText, System.Globalization.CultureInfo.InvariantCulture, utc, out var parsedStart)
                    || !DateTime.TryParse(endText, System.Globalization.CultureInfo.InvariantCulture, utc, out var parsedEnd))
                {
                    return ErrorResult("start and end must be UTC timestamps.", StatusCodes.Status400BadRequest);
                }

                start = parsedStart;
                end = parsedEnd;
            }

            try
            {
                var (serverId, resolveError) = await resolveServer(server, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                var clocks = await DarlingServerClocksReader.GetAsync(postgres, serverId, McpCommandDeadlines.ReadSeconds, context.RequestAborted);
                var prompt = PerformanceMonitor.Analysis.RecommendationAskAiPrompt.BuildFor(
                    server, title, start, end, DarlingServerClocksReader.ClockFor(clocks, serverId), (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["prompt"] = prompt });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"ask ai prompt failed ({ex.GetType().Name})", "/api/analysis/ask-ai-prompt", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });
    }

    /// <summary>
    /// What the run route answers when the server resolver hands back an error string (#5941 review). The resolver has three
    /// shapes and each gets its own answer: a registry read fault (a store fault, not the caller's request) is logged and answers
    /// the fixed 500/503 body; a name that matches no server ("Could not resolve server. ...") is a 404 with the envelope's plain
    /// message; any other refusal envelope (a name that matches several servers) is a 400 with its plain message. The raw
    /// envelope JSON is never the body.
    /// </summary>
    internal static IResult CollectorRunResolveFailure(string error, ILogger logger, long elapsedMs, CancellationToken requestAborted)
    {
        if (ClassifyToolResponse(error) == ToolResponseKind.ServerError)
        {
            return ServerErrorResult(error, "/api/collectors/run", logger, elapsedMs, requestAborted);
        }

        var message = McpHelpers.ErrorMessageOf(error);
        var noMatch = !McpHelpers.IsRefusalEnvelope(error)
            || message.StartsWith("Could not resolve server", StringComparison.Ordinal);
        return ErrorResult(message, noMatch ? StatusCodes.Status404NotFound : StatusCodes.Status400BadRequest);
    }

    /// <summary>The 403 a refused switch change answers when the web signs in to the store as the viewer role (#6269).</summary>
    internal static string ServerActionRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change server action switches", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>
    /// The server action switch routes (#6269). <c>GET /api/server-actions</c> lists every server with a state for each action
    /// (<c>{can_decide, servers_returned, servers: [{server_id, server_name, engine, engine_kind, actions: [{action, valid, enabled, set_by,
    /// updated_at_utc}]}]}</c>; <c>can_decide</c> is false under the viewer role, which lists only). <c>PUT /api/servers/{id}/actions/{action}</c>
    /// with <c>{"enabled": true|false}</c> sets one switch: 200 <c>{status: set|unchanged, ...}</c>; 400 for a bad body, an unknown action or a
    /// signed-in name that cannot be recorded; 403 under the viewer role (and for a read-only seat, which the host's method gate refuses first);
    /// 404 for an unknown server; 409 when the server's engine cannot run the action; 415 without <c>application/json</c>; 503 before the service
    /// has published its store owner connection. <b>The write runs on that owner connection</b>, because no other role can write the table; the
    /// admin check is the web login's own privileges (<see cref="AgentChangeUndo.CanDecideAsync"/>) and the seat's method gate. The change is
    /// journaled by <c>person:&lt;login&gt;</c>, surface <c>web</c>.
    /// </summary>
    internal static void MapServerActions(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: the access check runs as the login the switch change is allowed under; the write itself stays on the owner connection. */
        AgentChangeUndo DecideProbe(HttpContext context) =>
            adminArm is null ? changes : new AgentChangeUndo(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/server-actions", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rows = await ServerActionSwitches.ListAsync(postgres, context.RequestAborted);
                var body = Mcp.DarlingMcpServerActionTools.ListToJson(rows);
                body["can_decide"] = DarlingWebSeat.IsSignedInAdmin(context) && await DecideProbe(context).CanDecideAsync(context.RequestAborted);
                body["signed_in_admin"] = DarlingWebSeat.IsSignedInAdmin(context);
                return DarlingWebStatementSweep.JsonText(body, "/api/server-actions", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/server-actions", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/servers/{id:int}/actions/{action}", async (HttpContext context, int id, string action) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (JsonNode.Parse(await ReadBodyAsync(context)) is not JsonObject body || body["enabled"] is not JsonValue flag || !flag.TryGetValue(out bool enabled))
                {
                    return ErrorResult("Send {\"enabled\": true} or {\"enabled\": false}.", StatusCodes.Status400BadRequest);
                }

                /* A signed-in admin comes first: the store login the web runs on says nothing about who is calling (#6269). */
                if (!DarlingWebSeat.IsSignedInAdmin(context))
                {
                    return SignInRequiredResult();
                }

                /* The same actor every web person write records: ONE person: prefix on the signed-in name (NoteDecider), held to the shared 256-unit name rule. */
                var actor = NoteDecider(DarlingWebSeat.FromContext(context));
                if (InvestigationPersonWrites.ValidateActor(actor) is not null)
                {
                    return ErrorResult("The signed-in name cannot be recorded as the person who changed this switch.", StatusCodes.Status400BadRequest);
                }

                /* The admin check comes before anything is read or written. */
                if (!await DecideProbe(context).CanDecideAsync(context.RequestAborted))
                {
                    return ErrorResult(ServerActionRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);
                }

                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return ErrorResult("The service has not published its store owner connection yet, so the switch cannot be written. Try again in a moment.",
                        StatusCodes.Status503ServiceUnavailable);
                }

                var result = await ServerActionSwitches.SetAsync(owner, id, action, enabled, actor, ServerActionSurfaces.Web, context.RequestAborted);
                var status = result.Succeeded ? StatusCodes.Status200OK
                    : result.Code == "unknown_server" ? StatusCodes.Status404NotFound
                    : result.Code is "not_for_engine" or "switch_busy" ? StatusCodes.Status409Conflict
                    : StatusCodes.Status400BadRequest;
                return DarlingWebStatementSweep.JsonText(result.ToJson(), "/api/servers/{id}/actions/{action}", logger, stopwatch.ElapsedMilliseconds,
                    context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is JsonException)
            {
                return ErrorResult("The request body is not valid.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/servers/{id}/actions/{action}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    internal static void MapAdminServers(WebApplication app, NpgsqlDataSource postgres)
    {
        app.MapGet(DarlingAdminServersReader.Route, async (HttpContext context) =>
        {
            var rows = await DarlingAdminServersReader.ReadAsync(postgres, context.RequestAborted);
            return Results.Text(DarlingAdminServersReader.Render(rows, DateTime.UtcNow), "application/json");
        });
    }

    /// <summary>
    /// The fleet server-tag write routes (#5085), the same shape as <see cref="MapMuteRules"/>: each route parses
    /// its JSON body and hands it to the matching <c>DarlingMcpServerTagTools.*Core</c> over a
    /// <see cref="ServerTagStore"/> on the viewer-role pool, so the web surface and the MCP tools share one set of
    /// rules (names, colours, depth, cycles, sibling uniqueness) and one set of answers. <c>application/json</c> is
    /// required on every route, the same CSRF discipline as the mute-rule routes. The admin gate is the seat's
    /// method gate (<see cref="DarlingWebSeat.IsRequestAllowed"/> refuses every non-GET from a read-only seat);
    /// the viewer role's two single-table grants are only the floor beneath it.
    ///
    /// <para><b>Routes.</b> <c>POST /api/server-tags</c> (<c>{name, parent_id?, colour?}</c>, 201 on
    /// <c>created</c>); <c>PATCH /api/server-tags/{id}</c> (the body IS the partial <c>changes_json</c>, or the single field <c>{"move":"up"|"down"}</c> to reorder siblings, #6094);
    /// <c>DELETE /api/server-tags/{id}?confirm=true</c> (the flag rides the query string, so the DELETE stays
    /// bodyless like <c>DELETE /api/mute-rules/{id}</c>); <c>POST /api/server-tags/{id}/servers</c> and
    /// <c>DELETE /api/server-tags/{id}/servers</c> (<c>{server_ids: [...]}</c> in the body).</para>
    ///
    /// <para><b>Statuses.</b> See <see cref="ServerTagEnvelopeStatus"/>: the mute-rule mapping plus
    /// <c>conflict</c> and <c>confirm_required</c> as 409, the latter with its full envelope (the
    /// <c>affected_rules</c> a delete would orphan) as the body.</para>
    /// </summary>
    internal static void MapServerTags(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new ServerTagStore(postgres, DarlingMcpServerTagTools.WriteCommandSeconds);

        app.MapPost("/api/server-tags", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (!TryReadCreateBody(body, out var name, out var parentId, out var colour, out var createError))
            {
                return ErrorResult(createError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.CreateServerTagCore(store, name, parentId, colour, context.RequestAborted);
            LogServerTagWrite(logger, context, "create", null, result, 0);
            return ServerTagToolResult(result, "/api/server-tags", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/server-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.UpdateServerTagCore(store, id, changes, context.RequestAborted);
            LogServerTagWrite(logger, context, "update", id, result, 0);
            return ServerTagToolResult(result, "/api/server-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/server-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (confirm, confirmRefusal) = ParseConfirm(context.Request.Query);
            if (confirmRefusal is not null)
            {
                return ErrorResult(confirmRefusal, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.DeleteServerTagCore(store, id, confirm, context.RequestAborted);
            LogServerTagWrite(logger, context, "delete", id, result, 0);
            return ServerTagToolResult(result, "/api/server-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/server-tags/{id:int}/servers", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (serverIds, idsError) = await ReadServerIdsAsync(context);
            if (serverIds is null)
            {
                return ErrorResult(idsError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.AssignServerTagCore(store, id, serverIds, context.RequestAborted);
            LogServerTagWrite(logger, context, "assign", id, result, serverIds.Count);
            return ServerTagToolResult(result, "/api/server-tags/{id}/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/server-tags/{id:int}/servers", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (serverIds, idsError) = await ReadServerIdsAsync(context);
            if (serverIds is null)
            {
                return ErrorResult(idsError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.UnassignServerTagCore(store, id, serverIds, context.RequestAborted);
            LogServerTagWrite(logger, context, "unassign", id, result, serverIds.Count);
            return ServerTagToolResult(result, "/api/server-tags/{id}/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>
    /// The query-tag web routes (#5986), the same shape as <see cref="MapServerTags"/>: each write route parses its JSON
    /// body and hands it to the matching <c>DarlingMcpQueryTagTools.*Core</c> or <c>DarlingMcpQueryTagRuleTools.*Core</c> over
    /// a store on the viewer-role pool, so the page and the MCP tools share one set of rules and one set of answers.
    /// <c>application/json</c> is required on every write (the CSRF discipline of the other write routes). The admin gate is
    /// the seat's method gate (<see cref="DarlingWebSeat.IsRequestAllowed"/> refuses every non-GET from a read-only seat);
    /// the viewer role's grants on the three tag tables are only the floor beneath it. The writer's name is stamped on a
    /// new assignment or rule as <c>created_by</c> (the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>), not <c>mcp</c>.
    ///
    /// <para><b>Tag routes.</b> <c>POST /api/query-tags</c> (<c>{name, parent_id?, colour?}</c>, 201);
    /// <c>PATCH /api/query-tags/{id}</c> (the body IS the partial <c>changes_json</c>);
    /// <c>DELETE /api/query-tags/{id}?confirm=true</c> (409 <c>confirm_required</c> with the counts until the flag rides along);
    /// <c>POST /api/query-tags/{id}/assignments</c> and <c>DELETE /api/query-tags/{id}/assignments</c>
    /// (<c>{kind, query_hash?, database_name?, schema_name?, object_name?, server_id?}</c>; assign also takes <c>query_id?</c>).
    /// Unassign is keyed by the assignment's key and server scope, as the MCP tool is: the tag id in the path is not part of
    /// the key.</para>
    ///
    /// <para><b>Rule routes.</b> <c>POST /api/query-tag-rules</c> (<c>{field, pattern?, tag_id?, priority?, enabled?, server_id?}</c>,
    /// 201); <c>PATCH /api/query-tag-rules/{id}</c> (the partial <c>changes_json</c>); <c>DELETE /api/query-tag-rules/{id}</c>;
    /// <c>GET /api/query-tag-rules[?server_id=]</c> (the list, in the order the rules apply) and
    /// <c>GET /api/query-tag-rules/test?...</c> (the dry run: <c>rule_id</c> or a draft's <c>field</c>, <c>pattern</c>,
    /// <c>tag_id</c>, <c>priority</c>, plus <c>server_id</c> and <c>window_hours</c>). The test is a GET, so a read-only seat
    /// can run it: it writes nothing, and it is admitted like any other read. Both reads are swept (#4348) because a rule's
    /// pattern and a sample's matched value are text.</para>
    /// </summary>
    internal static void MapQueryTags(WebApplication app, NpgsqlDataSource postgres, ILogger logger,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        var tags = new QueryTagStore(postgres, Mcp.DarlingMcpQueryTagTools.WriteCommandSeconds);
        var rules = new QueryTagRuleStore(postgres, Mcp.DarlingMcpQueryTagRuleTools.CommandSeconds);

        app.MapPost("/api/query-tags", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "A query tag", "name", "parent_id", "colour");
            var name = reader.String("name");
            var parentId = reader.Int("parent_id");
            var colour = reader.String("colour");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.CreateQueryTagCore(tags, name, parentId, colour, context.RequestAborted);
            LogQueryTagWrite(logger, context, "create", null, result);
            return QueryTagToolResult(result, "/api/query-tags", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/query-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.UpdateQueryTagCore(tags, id, changes, context.RequestAborted);
            LogQueryTagWrite(logger, context, "update", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (confirm, bodyError) = ParseConfirm(context.Request.Query);
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.DeleteQueryTagCore(tags, id, confirm, context.RequestAborted);
            LogQueryTagWrite(logger, context, "delete", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/query-tags/{id:int}/assignments", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "An assignment", "kind", "query_hash", "database_name", "schema_name", "object_name", "server_id", "query_id");
            var kind = reader.String("kind");
            var queryHash = reader.String("query_hash");
            var databaseName = reader.String("database_name");
            var schemaName = reader.String("schema_name");
            var objectName = reader.String("object_name");
            var serverId = reader.Int("server_id");
            var queryId = reader.Long("query_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.AssignQueryTagCore(
                tags, id, kind, queryHash, databaseName, schemaName, objectName, serverId, queryId,
                DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
            LogQueryTagWrite(logger, context, "assign", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}/assignments", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tags/{id:int}/assignments", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "An unassignment", "kind", "query_hash", "database_name", "schema_name", "object_name", "server_id");
            var kind = reader.String("kind");
            var queryHash = reader.String("query_hash");
            var databaseName = reader.String("database_name");
            var schemaName = reader.String("schema_name");
            var objectName = reader.String("object_name");
            var serverId = reader.Int("server_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.UnassignQueryTagCore(
                tags, kind, queryHash, databaseName, schemaName, objectName, serverId, context.RequestAborted);
            LogQueryTagWrite(logger, context, "unassign", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}/assignments", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/query-tag-rules", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "A query tag rule", "field", "pattern", "tag_id", "priority", "enabled", "server_id");
            var field = reader.String("field");
            var pattern = reader.String("pattern");
            var tagId = reader.Int("tag_id");
            var priority = reader.Int("priority");
            var enabled = reader.Bool("enabled");
            var serverId = reader.Int("server_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.CreateCore(
                rules, field, pattern, tagId, priority ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultPriority, enabled ?? true, serverId,
                DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-create", null, result);
            return QueryTagToolResult(result, "/api/query-tag-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/query-tag-rules/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.UpdateCore(rules, id, changes, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-update", id, result);
            return QueryTagToolResult(result, "/api/query-tag-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tag-rules/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.DeleteCore(rules, id, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-delete", id, result);
            return QueryTagToolResult(result, "/api/query-tag-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapGet("/api/query-tag-rules", async (HttpContext context) =>
        {
            if (!TryReadQueryTagInt(context.Request.Query, "server_id", out var serverId, out var bodyError))
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.ListCore(rules, serverId, context.RequestAborted);
            return ToHttpResult(result, "/api/query-tag-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapGet("/api/query-tag-rules/test", async (HttpContext context) =>
        {
            var query = context.Request.Query;
            if (!TryReadQueryTagInt(query, "rule_id", out var ruleId, out var bodyError)
                || !TryReadQueryTagInt(query, "tag_id", out var tagId, out bodyError)
                || !TryReadQueryTagInt(query, "priority", out var priority, out bodyError)
                || !TryReadQueryTagInt(query, "server_id", out var serverId, out bodyError)
                || !TryReadQueryTagInt(query, "window_hours", out var windowHours, out bodyError)
                || !TrySingleQueryValue(query, "field", out var field)
                || !TrySingleQueryValue(query, "pattern", out var pattern))
            {
                return ErrorResult(bodyError ?? "Each parameter may be given once.", StatusCodes.Status400BadRequest);
            }

            /* The dry run reads up to 10,000 stored sightings like any collected-data read: admitted and recorded under its MCP twin's name. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, "test_query_tag_rule", ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, "test_query_tag_rule", ReadWindowBand.None, context, readScope.Scope);
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.TestCore(
                rules, ruleId, field.Length == 0 ? null : field, pattern.Length == 0 ? null : pattern, tagId,
                priority ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultPriority, serverId,
                windowHours ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultWindowHours, TimeProvider.System, context.RequestAborted);
            run.Complete(ToolResultOutcome(result, context.RequestAborted));
            return ToHttpResult(result, "/api/query-tag-rules/test", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>An optional whole-number query parameter: absent or blank is null, given more than once or not a number is a refusal sentence.</summary>
    private static bool TryReadQueryTagInt(IQueryCollection query, string key, out int? value, out string? error)
    {
        value = null;
        error = null;
        if (!TrySingleQueryValue(query, key, out var text))
        {
            error = $"{key} must be given once.";
            return false;
        }

        if (text.Length == 0)
        {
            return true;
        }

        if (!int.TryParse(text, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed))
        {
            error = $"{key} must be a whole number.";
            return false;
        }

        value = parsed;
        return true;
    }

    /// <summary>Who changed a query tag, assignment or rule is on the row only for assignments and rules; this line records the verb for
    /// every successful write. Verb and id only, never a name, pattern or body text.</summary>
    private static void LogQueryTagWrite(ILogger logger, HttpContext context, string verb, long? id, string result)
    {
        if (ServerTagEnvelopeStatus(result) is < 200 or >= 300)
        {
            return;
        }

        logger.LogInformation("Query tag {Verb} by {Principal}: id {Id}", verb, DarlingWebSeat.FromContext(context).EditorPrincipal, id);
    }

    /// <summary>The envelope pass-through the query-tag write routes share: the answer is swept first (#4348, a pattern or a
    /// name echoes text a caller typed), then answered exactly as <see cref="ServerTagToolResult"/> answers.</summary>
    internal static IResult QueryTagToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK)
    {
        /* #6341 round 1: a ServerError is answered with a fixed body and is not swept; the status and the one log line come from
           the tool's own result, never from the swept text. */
        if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
        {
            return ServerTagToolResult(result, route, logger, elapsedMs, requestAborted, successStatus);
        }

        var swept = DarlingWebStatementSweep.Apply(result);
        if (swept.Refused) return DarlingWebStatementSweep.Refusal(route, logger, elapsedMs, requestAborted);
        return ServerTagToolResult(result, route, logger, elapsedMs, requestAborted, successStatus, swept.Text);
    }

    /// <summary>Parses the request body as one JSON object, or names why it is not.</summary>
    private static async Task<(JsonObject? Body, string? Error)> ReadJsonObjectAsync(HttpContext context)
    {
        string text;
        try
        {
            text = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
        }
        catch (InvalidDataException)
        {
            return (null, "Request body is too large.");
        }

        try
        {
            var root = JsonNode.Parse(text);
            if (root is not JsonObject body)
            {
                return (null, "Request body must be a JSON object.");
            }

            /* A duplicate property name throws ArgumentException on the first enumeration; force it here so it
               answers 400 (a caller's malformed body), like the dismiss route. */
            _ = body.Count;
            return (body, null);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }
        catch (ArgumentException)
        {
            return (null, "Request body has a duplicate field.");
        }
    }

    /// <summary>The most request-body bytes a server-tag write route reads before it answers 400 (1000 ids is about 12 KB).</summary>
    internal const int MaxServerTagBodyBytes = 64 * 1024;

    /// <summary>Who changed tag scope is not stored on the row; this line is the only record. Verb, tag id and
    /// counts only, never a tag name or body text. Logged for a successful write (not invalid / refused).</summary>
    private static void LogServerTagWrite(ILogger logger, HttpContext context, string verb, int? tagId, string result, int serverCount)
    {
        if (ServerTagEnvelopeStatus(result) is < 200 or >= 300)
        {
            return;
        }

        logger.LogInformation(
            "Server tag {Verb} by {Principal}: tag {TagId}, servers {ServerCount}",
            verb, DarlingWebSeat.FromContext(context).EditorPrincipal, tagId, serverCount);
    }

    /// <summary>Reads <c>{name, parent_id?, colour?}</c>: only those keys, with their JSON types, so a stray or
    /// mistyped field is a 400 here rather than a silently dropped one.</summary>
    internal static bool TryReadCreateBody(JsonObject body, out string? name, out int? parentId, out string? colour, out string? error)
    {
        name = null;
        parentId = null;
        colour = null;
        error = null;

        foreach (var property in body)
        {
            if (property.Key is not ("name" or "parent_id" or "colour"))
            {
                error = $"Unknown field '{property.Key}'. A server tag takes name, parent_id and colour.";
                return false;
            }
        }

        if (body["name"] is JsonValue nameValue && nameValue.TryGetValue<string>(out var nameText))
        {
            name = nameText;
        }
        else if (body["name"] is not null)
        {
            error = "name must be a string.";
            return false;
        }

        if (body["parent_id"] is JsonNode parentNode)
        {
            if (parentNode is JsonValue parentValue && parentValue.TryGetValue<int>(out var parent))
            {
                parentId = parent;
            }
            else
            {
                error = "parent_id must be a whole number, or null for a root tag.";
                return false;
            }
        }

        if (body["colour"] is JsonNode colourNode)
        {
            if (colourNode is JsonValue colourValue && colourValue.TryGetValue<string>(out var colourText))
            {
                colour = colourText;
            }
            else
            {
                error = "colour must be a #RRGGBB string, or null for the palette colour.";
                return false;
            }
        }

        return true;
    }

    /// <summary>Reads <c>{server_ids: [int, ...]}</c> from the body, or names why it cannot.</summary>
    private static async Task<(List<int>? ServerIds, string? Error)> ReadServerIdsAsync(HttpContext context)
    {
        var (body, error) = await ReadJsonObjectAsync(context);
        if (body is null)
        {
            return (null, error);
        }

        if (body.Count != 1 || body["server_ids"] is not JsonArray array)
        {
            return (null, "Request body must be exactly {\"server_ids\": [whole numbers]}.");
        }

        var ids = new List<int>(array.Count);
        foreach (var element in array)
        {
            if (element is JsonValue value && value.TryGetValue<int>(out var id))
            {
                ids.Add(id);
            }
            else
            {
                return (null, "server_ids must hold only whole numbers.");
            }
        }

        return (ids, null);
    }

    /// <summary>
    /// The HTTP status a server-tag verb's returned string maps to: the mute-rule mapping
    /// (<see cref="MuteRuleEnvelopeStatus"/>: <c>invalid</c> 400, <c>not_found</c> 404, the caught-exception
    /// envelope 500, any other envelope the success status) plus the two statuses the tag verbs add —
    /// <c>conflict</c> (a sibling already holds the name) and <c>confirm_required</c> (a delete that would orphan
    /// custom alert rules until repeated with <c>confirm=true</c>) — both 409. The body is untouched either way,
    /// so <c>affected_rules</c> reaches the caller.
    /// </summary>
    internal static int ServerTagEnvelopeStatus(string result, int successStatus = StatusCodes.Status200OK)
    {
        if (ClassifyToolResponse(result) is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal)
        {
            try
            {
                var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
                if (status is "conflict" or "confirm_required")
                {
                    return StatusCodes.Status409Conflict;
                }
            }
            catch (JsonException)
            {
                /* Not a shape the cores produce; the mute-rule mapping below answers it. */
            }
        }

        var mapped = MuteRuleEnvelopeStatus(result, successStatus);
        if (mapped != successStatus)
        {
            return mapped;
        }

        /* Allow-list: only a known success status answers 2xx; a status a future verb adds is a 500 until it is
           mapped here, never a silent 200. */
        try
        {
            var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
            return status is "created" or "updated" or "unchanged" or "deleted" or "assigned" or "unassigned" or "moved"
                ? successStatus
                : StatusCodes.Status500InternalServerError;
        }
        catch (JsonException)
        {
            return StatusCodes.Status500InternalServerError;
        }
    }

    /// <summary>The envelope pass-through the server-tag routes share: <see cref="MuteRuleToolResult"/>'s error
    /// handling (the caught-exception sentence never reaches the wire) with <see cref="ServerTagEnvelopeStatus"/>'s
    /// status.</summary>
    internal static IResult ServerTagToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK, string? writtenBody = null)
    {
        var kind = ClassifyToolResponse(result);
        if (kind is ToolResponseKind.ServerError)
        {
            return ServerErrorResult(McpHelpers.ErrorMessageOf(result), route, logger, elapsedMs, requestAborted);
        }

        /* #6341 round 1: the status comes from the tool's own result; writtenBody (the swept text) only replaces what is written. */
        var httpStatus = ServerTagEnvelopeStatus(result, successStatus);
        var body = writtenBody ?? result;
        return kind is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal
            ? Results.Text(body, "application/json", statusCode: httpStatus)
            : ErrorResult(McpHelpers.ErrorMessageOf(body), httpStatus);
    }

    /// <summary>Reads the raw request body text — what the mute-rule cores parse themselves, so the web layer
    /// adds no parse of its own to drift from theirs.</summary>
    private static async Task<string> ReadBodyAsync(HttpContext context)
    {
        using var reader = new StreamReader(context.Request.Body);
        return await reader.ReadToEndAsync(context.RequestAborted);
    }

    /// <summary>
    /// Maps a mute-rule verb's returned string onto the HTTP status the web surface answers with, leaving the
    /// body untouched: <c>invalid</c> → 400, <c>not_found</c> → 404, <c>already_exists</c> → 409 (#4734: a create
    /// that repeats a rule already in force is a conflict, the status the views and custom-rule routes give theirs;
    /// the body is still the verb's envelope, carrying the existing rule's id), any other envelope (created /
    /// updated / unchanged / deleted) → <paramref name="successStatus"/>; the cores' caught-exception envelope
    /// (<c>McpHelpers.FormatError</c>, <c>{"status":"error", ...}</c>) → 500 (classified by
    /// <see cref="ClassifyToolResponse"/>, like the read surface, and BEFORE the status switch below so the
    /// failure word is never read as a verb outcome); any other bare string is a shape the cores do not produce and maps to the client-correctable
    /// 400 for the reason the read surface's mapping does. <c>unchanged</c> deliberately shares the success
    /// code: it is the retry-safe "already so" answer, and the envelope's own <c>status</c> field carries the
    /// distinction a caller might act on. Pure, so the whole table pins without a server.
    ///
    /// <para>Since #3739 the <c>invalid</c> → 400 arm is answered by the classifier's <c>Refusal</c> kind — the
    /// same recognizer the read surface uses, so the two surfaces cannot disagree about the word — and the
    /// <c>"invalid"</c> case in the parsed switch below is kept as the belt-and-braces for an envelope that was
    /// serialized some other way (whitespace, a different key order) and so missed the prefix test.</para>
    /// </summary>
    internal static int MuteRuleEnvelopeStatus(string result, int successStatus = StatusCodes.Status200OK)
    {
        switch (ClassifyToolResponse(result))
        {
            case ToolResponseKind.ServerError:
                return StatusCodes.Status500InternalServerError;
            case ToolResponseKind.Refusal:
            case ToolResponseKind.ClientError:
                return StatusCodes.Status400BadRequest;
        }

        try
        {
            var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
            return status switch
            {
                "invalid" => StatusCodes.Status400BadRequest,
                "not_found" => StatusCodes.Status404NotFound,
                "already_exists" => StatusCodes.Status409Conflict,
                _ => successStatus,
            };
        }
        catch (JsonException)
        {
            /* A '{'-leading string that does not parse is not a shape the cores produce — refuse to claim
               success over a body nobody can read. */
            return StatusCodes.Status500InternalServerError;
        }
    }

    /// <summary>The envelope pass-through the mute-rule routes share: the verb's own body, verbatim, under the
    /// status <see cref="MuteRuleEnvelopeStatus"/> assigns — a refusal (<c>invalid</c>) included, which is the
    /// shape the read surface adopted from here in #3739.
    ///
    /// <para><b>#4283: the ServerError arm never puts the tool's caught-exception text on the wire.</b> Classified
    /// FIRST, before <see cref="MuteRuleEnvelopeStatus"/> ever runs, and answered through
    /// <see cref="ServerErrorResult"/> exactly as <see cref="ToHttpResult"/> answers the read surface's — logged
    /// ONCE, the body a fixed message, 503 when the sentence carries a caught statement_timeout's 57014 token,
    /// 500 otherwise. Any other error (a bare non-JSON string) is still wrapped as <c>{"error": sentence}</c>.</para>
    /// </summary>
    internal static IResult MuteRuleToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK, SensitiveStatements.JudgeBudget? sweepBudget = null)
    {
        /* #6341 round 1: classified, and the status read, from the tool's own result BEFORE the sweep, like ToHttpResult: the
           sweep can withhold the status word and must not decide the status or whether the fault is logged. */
        var kind = ClassifyToolResponse(result);
        if (kind is ToolResponseKind.ServerError)
        {
            return ServerErrorResult(McpHelpers.ErrorMessageOf(result), route, logger, elapsedMs, requestAborted);
        }

        var httpStatus = MuteRuleEnvelopeStatus(result, successStatus);

        /* #4348: swept over the body that is written. A mute rule echoes the pattern it was given, which can be a statement. */
        var swept = DarlingWebStatementSweep.Apply(result, sweepBudget);
        if (swept.Refused) return DarlingWebStatementSweep.Refusal(route, logger, elapsedMs, requestAborted);

        return kind is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal
            ? Results.Text(swept.Text, "application/json", statusCode: httpStatus)
            : ErrorResult(McpHelpers.ErrorMessageOf(swept.Text), httpStatus);
    }
}
