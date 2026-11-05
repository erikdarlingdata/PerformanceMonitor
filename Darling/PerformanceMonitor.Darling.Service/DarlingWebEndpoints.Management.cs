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
    /// <summary>How long one <c>POST /api/servers</c> request may hold the one-at-a-time slot.</summary>
    internal static readonly TimeSpan ServerAddSlotTimeout = TimeSpan.FromSeconds(120);

    internal const string ServerAddTimedOutText = "Adding servers took too long; check the server list before retrying. If every later add is refused as busy, restart the service.";

    /// <summary>The most servers one <c>POST /api/servers</c> request may carry.</summary>
    internal const int MaxServersPerAddRequest = 20;

    /// <summary>The most request-body bytes the add route reads before it answers 400 (twenty entries are a few KB).</summary>
    internal const int MaxServerAddBodyBytes = 64 * 1024;

    /// <summary>What a redacted secret is replaced with in any text the add route puts on the wire or in a log.</summary>
    private const string RedactedSecret = "[redacted]";

    /// <summary>
    /// <c>POST /api/servers</c>: adds monitored servers (the web twin of the MCP <c>add_servers</c> tool). The body
    /// is the same JSON ARRAY of server objects the tool takes, and the route hands it to the SAME core
    /// (<see cref="DarlingMcpServerAdminTools.AddServersAsync(NpgsqlDataSource, string, DarlingMcpServerAdminTools.ServerProbe, CancellationToken)"/>),
    /// so the web surface accepts exactly what the tool accepts, probes the server from the service before saving,
    /// and answers with the core's own <c>{requested, added, skipped, collided, failed, results}</c> envelope. The
    /// core's whole-request refusal (<c>{"status":"invalid"}</c>) is a 400; every other envelope is a 200, with
    /// each entry's outcome in <c>results</c>.
    ///
    /// <para><b>Gates.</b> A read-only seat is refused by the host's group-level write gate
    /// (<see cref="DarlingWebSeat.IsRequestAllowed"/>) and again here, because this is the one route that carries a
    /// credential. <c>application/json</c> is required (415 otherwise, the CSRF defense the mute routes use). At most
    /// <see cref="MaxServersPerAddRequest"/> servers per request (400 above that). One add runs at a time per host
    /// process: a second request that arrives while one is running answers 429, because each entry is probed over
    /// the network and a caller must not be able to queue unbounded probes from the service. The write runs on the
    /// host's <c>viewer</c> pool, whose <c>INSERT</c> on <c>config_monitored_servers</c> is the floor beneath the
    /// seat gate; <c>encrypted_password</c> stays SELECT-carved from it.</para>
    ///
    /// <para><b>Credentials.</b> A password or client secret travels only in the request body. The route never echoes
    /// the body, never logs it, and every 400 it composes is a fixed sentence (a parse failure names no part of the
    /// body, because a JSON parser's message can quote the character it stopped at). Any submitted secret that
    /// appears in the core's answer is replaced with <see cref="RedactedSecret"/> before it is written, as a
    /// backstop: the core's answers carry none today. The core's caught-exception envelope never reaches the wire
    /// either: it answers through <see cref="ServerErrorResult"/>'s fixed body, like every other write route.</para>
    ///
    /// <para><b>Audit.</b> Who added a server is not stored on the row, so one Information line per added server is
    /// the record: the editor principal, the server name and the auth mode, never the secret.</para>
    /// </summary>
    internal static void MapServers(
        WebApplication app, NpgsqlDataSource postgres, ILogger logger,
        Func<string, Task<string>>? addServers = null,
        TimeSpan? addTimeout = null,
        Func<int, string, Task<string>>? editServer = null,
        TimeSpan? editTimeout = null,
        Func<int, Task<DarlingMcpServerAdminTools.ServerEditRow?>>? readServer = null,
        bool allowWindowsAuthOverUnverifiedTls = false)
    {
        var slotTimeout = addTimeout ?? ServerAddSlotTimeout;
        /* web.allowWindowsAuthOverUnverifiedTls: the add and the edit refuse Windows authentication over a connection whose server
           identity is not verified unless it is on, the rule MCP applies under mcp.allowWindowsAuthOverUnverifiedTls. */
        var windowsAuthScreen = DarlingUnverifiedWindowsAuthScreen.ForWeb(allowWindowsAuthOverUnverifiedTls);
        var editSlotTimeout = editTimeout ?? ServerEditSlotTimeout;

        /* One gate per host: MapAll runs once per process. ADD and EDIT share it (#5240): both probe over the network
           and both read-then-write the same identity set, so one server write runs at a time per host process. */
        var serverWriteInFlight = new SemaphoreSlim(1, 1);

        /* The seam a test stands a stub probe or a held add in through; production runs the core itself. */
        addServers ??= body => DarlingMcpServerAdminTools.AddServers(postgres, body, logger, windowsAuthScreen);

        /* The edit core and the by-id read, with the same seam. The edit core gets NO audit logger: the route writes the one
           audit line itself, with the signed-in principal. It gets the logger for a failed connection test's driver
           text alone (probeLogger), which the reply to the browser does not carry. */
        editServer ??= (id, body) => DarlingMcpServerAdminTools.EditServerByIdAsync(postgres, id, body, null, CancellationToken.None, probeLogger: logger, webScreen: windowsAuthScreen);
        readServer ??= id => new DarlingMcpServerAdminTools.PostgresServerEditStore(postgres).ReadRowAsync(id, CancellationToken.None);

        app.MapPost("/api/servers", async (HttpContext context) =>
        {
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult(ReadOnlyAccountText, StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxServerAddBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult(BodyTooLargeText(MaxServerAddBodyBytes), StatusCodes.Status400BadRequest);
            }

            if (!TryReadServerAddBody(body, out var entries, out var refusal))
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            /* Both are read BEFORE the slot call: an add that outlives the wait commits after the request is over, and
               its audit line is written then, from these two, never from the HttpContext. */
            var principal = DarlingWebSeat.FromContext(context).EditorPrincipal;
            var secrets = SubmittedSecrets(entries);

            var ran = await RunInServerWriteSlotAsync(
                serverWriteInFlight, () => addServers(body), slotTimeout, AddSlotLabels, logger,
                auditLateAnswer: late =>
                {
                    var lateAnswer = RedactAddAnswer(late, secrets);
                    LogServerAddFailures(logger, principal, lateAnswer);
                    LogServerAdds(logger, principal, entries, lateAnswer);
                });
            if (ran.Early is not null)
            {
                return ran.Early;
            }

            var result = ran.Answer!;
            var stopwatch = ran.Stopwatch!;

            if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
            {
                return ServerErrorResult(RedactSecrets(McpHelpers.ErrorMessageOf(result), secrets), "/api/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            var answer = RedactAddAnswer(result, secrets);
            LogServerAddFailures(logger, principal, answer);
            LogServerAdds(logger, principal, entries, answer);
            return Results.Text(answer, "application/json", statusCode: MuteRuleEnvelopeStatus(answer));
        });

        /* #5240: edit one server in place. Gates in add's order: the sign-in's edit right (403), JSON content type (415), a bounded
           body (400), a JSON object without duplicate keys (400), the token (400), then the shared slot (429) and the
           core with a timeout (503). The host's group-level method gate has already refused a read-only sign-in
           every unsafe method. */
        app.MapPatch("/api/servers/{id:int}", async (HttpContext context, int id) =>
        {
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult(ReadOnlyAccountText, StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxServerEditBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult(BodyTooLargeText(MaxServerEditBodyBytes), StatusCodes.Status400BadRequest);
            }

            if (!TryReadServerEditBody(body, out var changes, out var refusal))
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            /* Both are read BEFORE the slot call: an edit that outlives the wait commits after the request is over, and
               its audit line is written then, from these two, never from the HttpContext. */
            var principal = DarlingWebSeat.FromContext(context).EditorPrincipal;
            var secrets = new List<string>();
            if (TryGetString(changes, "password") is { Length: > 0 } secret)
            {
                secrets.Add(secret);
            }

            /* #5452: the typed AWS external ID is redacted wherever a password is. */
            if (TryGetString(changes, "aws_external_id") is { Length: > 0 } externalId)
            {
                secrets.Add(externalId);
                if (externalId.Trim() is { Length: > 0 } trimmedExternalId && trimmedExternalId != externalId)
                {
                    secrets.Add(trimmedExternalId);
                }
            }

            var ran = await RunInServerWriteSlotAsync(
                serverWriteInFlight, () => editServer(id, body), editSlotTimeout, EditSlotLabels, logger,
                auditLateAnswer: late => LogServerEdit(logger, principal, id, RedactEditAnswer(late, secrets)));
            if (ran.Early is not null)
            {
                return ran.Early;
            }

            var result = ran.Answer!;
            if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
            {
                var sentence = RedactSecrets(McpHelpers.ErrorMessageOf(result), secrets);
                if (sentence.Contains(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, StringComparison.Ordinal))
                {
                    /* A store whose roles predate the edit function (#5240): still a 500, but the body says what to do
                       instead of the generic sentence. The text is ours; PostgreSQL's own error never reaches this far. */
                    DarlingWebFailureLog.Report(logger, "/api/servers/{id}", ran.Stopwatch!.ElapsedMilliseconds, sentence, context.RequestAborted);
                    return ErrorResult(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, StatusCodes.Status500InternalServerError);
                }

                return ServerErrorResult(sentence, "/api/servers/{id}", logger, ran.Stopwatch!.ElapsedMilliseconds, context.RequestAborted);
            }

            var answer = RedactEditAnswer(result, secrets);
            LogServerEdit(logger, principal, id, answer);
            return Results.Text(answer, "application/json", statusCode: ServerEditEnvelopeStatus(answer));
        });

        /* The edit form's read (#5240): the editable NON-secret values plus modified_at, an opaque string. Web-only
           (no MCP tool). It cannot say whether a secret is stored: the viewer role cannot even evaluate
           encrypted_password. The host's group gate lets every GET through for a read-only seat, so the route
           checks the edit right itself. */
        app.MapGet("/api/admin/servers/{id:int}", async (HttpContext context, int id) =>
        {
            /* The form's read pre-fills username and the TLS posture, which the Manage Servers list
               (DarlingAdminServersReader) withholds from every seat. Only a seat that can submit the edit may read it. */
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult(ReadOnlyAccountText, StatusCodes.Status403Forbidden);
            }

            var stopwatch = Stopwatch.StartNew();
            DarlingMcpServerAdminTools.ServerEditRow? row;
            try
            {
                row = await readServer(id);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"admin server read failed ({ex.GetType().Name})", "/api/admin/servers/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (row is null)
            {
                return ErrorResult("This server's definition no longer exists.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            return Results.Text(JsonSerializer.Serialize(DarlingMcpServerAdminTools.CurrentValuesOf(row), McpHelpers.JsonOptions), "application/json");
        });
    }

    /// <summary>The most request-body bytes the edit route reads (a partial change of a dozen scalar fields).</summary>
    internal const int MaxServerEditBodyBytes = 8 * 1024;

    /// <summary>
    /// PURE: parses the edit body, or names why it is not acceptable with a fixed sentence that quotes no part of the
    /// body. It must be a JSON object with no duplicate key, and the web route ALWAYS requires
    /// <c>expected_modified_at</c> (a non-empty string): a form that did not read the row first cannot edit it.
    /// </summary>
    internal static bool TryReadServerEditBody(string body, out JsonObject changes, out string? refusal)
    {
        changes = new JsonObject();
        refusal = null;
        try
        {
            if (JsonNode.Parse(body) is not JsonObject parsed)
            {
                refusal = "Request body must be a JSON object.";
                return false;
            }

            /* A duplicate property name throws ArgumentException on the first enumeration; force it here. */
            _ = parsed.Count;
            if (TryGetString(parsed, "expected_modified_at") is not { Length: > 0 })
            {
                refusal = "expected_modified_at is required: send the modified_at the edit form read.";
                return false;
            }

            changes = parsed;
            return true;
        }
        catch (JsonException)
        {
            refusal = "Request body is not valid JSON.";
            return false;
        }
        catch (ArgumentException)
        {
            refusal = "Request body has a duplicate field.";
            return false;
        }
    }

    /// <summary>
    /// The HTTP status an edit answer maps to: <see cref="ServerTagEnvelopeStatus"/> plus <c>collides</c> as 409, and
    /// <c>connection_failed</c> as 200 with the status in the body (add answers a failed probe the same way), so the
    /// page branches on <c>status</c> and no new convention is invented. Pure.
    /// </summary>
    internal static int ServerEditEnvelopeStatus(string result)
    {
        if (ClassifyToolResponse(result) is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal)
        {
            try
            {
                switch (JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null)
                {
                    case "collides":
                        return StatusCodes.Status409Conflict;
                    case "connection_failed":
                        return StatusCodes.Status200OK;
                }
            }
            catch (JsonException)
            {
                /* Not a shape the core produces; the tag mapping below answers it. */
            }
        }

        return ServerTagEnvelopeStatus(result);
    }

    /// <summary>The edit answer with every occurrence of the submitted secret removed from its one free-text field,
    /// <c>message</c> (the only field a driver's text can reach: connection_failed, collides). Structured fields
    /// (engine, username, host, server, modified_at, numbers, keys) are never rewritten, so a short or common secret
    /// cannot corrupt them. An answer that does not parse is replaced by a fixed body, never passed through.</summary>
    internal static string RedactEditAnswer(string answer, IReadOnlyList<string> secrets)
    {
        if (secrets.Count == 0)
        {
            return answer;
        }

        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return RedactSecrets(answer, secrets);
            }

            RedactField(envelope, "message", secrets);
            return envelope.ToJsonString();
        }
        catch (JsonException)
        {
            return "{\"status\":\"error\",\"message\":\"The answer could not be read.\"}";
        }
    }

    /// <summary>One Information line per saved edit: who, the server id and the field NAMES. Never a value, never the
    /// secret. A refused probe gets a line with the core's sanitized detail (redacted already).</summary>
    private static void LogServerEdit(ILogger logger, string principal, int id, string answer)
    {
        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return;
            }

            switch (TryGetString(envelope, "status"))
            {
                case "updated":
                    var fields = envelope["changed"] is JsonArray changed
                        ? string.Join(",", changed.Select(n => n is JsonValue v && v.TryGetValue<string>(out var f) ? f : "?"))
                        : "";
                    logger.LogInformation(
                        "Server edited by {Principal}: id {ServerId}, fields {Fields}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), id, DarlingHttpRefusalLog.Sanitize(fields, 256));
                    if (fields.Split(',').Contains("aws_role_arn", StringComparer.Ordinal))
                    {
                        /* #5452: old and new role ARN with the principal. The external ID is never in this line. */
                        logger.LogInformation(
                            "AWS role changed by {Principal}: id {ServerId}, from {OldRole} to {NewRole}",
                            DarlingHttpRefusalLog.Sanitize(principal, 256), id,
                            DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "old_aws_role_arn") ?? "(none)", 2200),
                            DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "new_aws_role_arn") ?? "(none)", 2200));
                    }
                    break;
                case "connection_failed":
                    logger.LogInformation(
                        "Server edit failed for {Principal}: id {ServerId}, connection_failed: {Detail}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), id, DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "message") ?? "", 1024));
                    break;
            }
        }
        catch (JsonException)
        {
            /* An unreadable answer has nothing to log. */
        }
    }

    /// <summary>What <see cref="RunInServerWriteSlotAsync"/> says for one server-write route.</summary>
    internal sealed record ServerWriteSlotLabels(string BusyText, string TimedOutText, string ToolName, string Route, string LogVerb);

    internal static readonly ServerWriteSlotLabels AddSlotLabels = new(
        "A server add or edit is already running. Wait for it to finish, then try again.",
        ServerAddTimedOutText, "add_servers", "/api/servers", "POST /api/servers: the add");

    internal static readonly ServerWriteSlotLabels EditSlotLabels = new(
        "A server add or edit is already running. Wait for it to finish, then try again.",
        ServerEditTimedOutText, "edit_server", "/api/servers/{id}", "PATCH /api/servers/{id}: the edit");

    /// <summary>How long one <c>PATCH /api/servers/{id}</c> request may hold the slot (one probe, not twenty).</summary>
    internal static readonly TimeSpan ServerEditSlotTimeout = TimeSpan.FromSeconds(60);

    internal const string ServerEditTimedOutText = "Editing the server took too long; check the server's settings before retrying. If every later add or edit is refused as busy, restart the service.";

    /// <summary>The outcome of <see cref="RunInServerWriteSlotAsync"/>: either an <see cref="Early"/> answer (busy,
    /// timed out, or faulted) or the core's <see cref="Answer"/> and how long it ran.</summary>
    internal sealed record ServerWriteRun(IResult? Early, string? Answer, Stopwatch? Stopwatch);

    /// <summary>
    /// The ONE place the server-write slot is taken, held and freed (#5268's fix, shared by add and edit so the next
    /// server-write route cannot reintroduce the race). <c>Wait(0)</c> or 429; the core starts; two exits can free the
    /// slot, the handler when the core finished inside the wait and the core's own continuation when it outlives the
    /// wait, and both go through one <see cref="SingleReleaseSlot"/> so it is freed at most once; the handler frees it
    /// in <c>finally</c> BEFORE the caller writes any answer, so a client that sends again the moment it reads the
    /// answer finds the slot free. A core that outlives <paramref name="timeout"/> answers 503 and keeps the slot
    /// until it finishes, and a core that outlives it can still commit: <paramref name="auditLateAnswer"/> receives
    /// its answer when it finishes, so the route's audit line is written for a write the client was told nothing
    /// about. It runs after the request is over, so it must not touch the HttpContext. Only the exception TYPE is
    /// ever logged or answered: its message could quote a value the request carried.
    /// </summary>
    private static async Task<ServerWriteRun> RunInServerWriteSlotAsync(
        SemaphoreSlim gate, Func<Task<string>> start, TimeSpan timeout, ServerWriteSlotLabels labels, ILogger logger,
        Action<string>? auditLateAnswer = null)
    {
        if (!gate.Wait(0))
        {
            return new ServerWriteRun(ErrorResult(labels.BusyText, StatusCodes.Status429TooManyRequests), null, null);
        }

        var stopwatch = Stopwatch.StartNew();
        var slot = new SingleReleaseSlot(gate);

        /* The core takes no request token, like the tool: a client that disconnects mid-write must not leave it
           half-attempted and unaudited. */
        Task<string> running;
        try
        {
            running = start();
        }
        catch (Exception ex)
        {
            /* start threw before returning a task: nothing is running, so free the slot here. EVERY synchronous throw
               is caught, cancellation included: the slot is not released in a finally, so a type that escaped this
               catch would hold it for the life of the process and answer 429 forever. */
            slot.Release();
            return new ServerWriteRun(
                ServerErrorResult($"{labels.ToolName} failed ({ex.GetType().Name})", labels.Route, logger, stopwatch.ElapsedMilliseconds, CancellationToken.None), null, null);
        }

        /* The slot is freed by the core itself, once, when it finishes (completed or faulted) - never by the wait's
           timeout, so a core that outlives the timeout still holds the slot. */
        _ = running.ContinueWith(_ => slot.Release(), TaskScheduler.Default);

        try
        {
            var answer = await running.WaitAsync(timeout);
            return new ServerWriteRun(null, answer, stopwatch);
        }
        catch (TimeoutException)
        {
            _ = running.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            if (auditLateAnswer is not null)
            {
                /* The core still commits after the 503: write its audit line when it finishes. Never touch HttpContext here. */
                _ = running.ContinueWith(
                    t => auditLateAnswer(t.Result), CancellationToken.None, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
            }

            logger.LogWarning("{Verb} did not finish within {Seconds} s; the slot stays held until it does", labels.LogVerb, (int)timeout.TotalSeconds);
            return new ServerWriteRun(ErrorResult(labels.TimedOutText, StatusCodes.Status503ServiceUnavailable), null, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The cores catch their own faults, so this is a backstop; only the TYPE is logged. */
            return new ServerWriteRun(
                ServerErrorResult($"{labels.ToolName} failed ({ex.GetType().Name})", labels.Route, logger, stopwatch.ElapsedMilliseconds, CancellationToken.None), null, null);
        }
        finally
        {
            /* A core that finished inside the wait frees the slot here, before any response is produced. One still
               running (the timeout path) keeps it: only its own continuation frees it. */
            if (running.IsCompleted)
            {
                slot.Release();
            }
        }
    }

    /// <summary>
    /// Frees one acquired slot of a one-slot gate at most once, however many exits call <see cref="Release"/> and in
    /// whatever order. A plain second <c>Release</c> is not harmless: if another add has already taken the slot in
    /// between, the extra release frees the slot that add holds, and a third add runs beside it.
    /// </summary>
    internal sealed class SingleReleaseSlot
    {
        private readonly SemaphoreSlim gate;
        private readonly object sync = new();
        private readonly Action? beforeFree;
        private bool released;

        /// <param name="gate">The one-slot gate to free.</param>
        /// <param name="beforeFree">A test seam only: runs inside the first release, after it claimed the slot and before
        /// it frees the gate, so a test can stand a second caller in that window. Production passes nothing.</param>
        internal SingleReleaseSlot(SemaphoreSlim gate, Action? beforeFree = null)
        {
            this.gate = gate;
            this.beforeFree = beforeFree;
        }

        /// <summary>
        /// Frees the slot on the first call; every later call does nothing, but it does not RETURN until the first
        /// call has really freed the gate (#6440). A flag flipped with Interlocked.Exchange before gate.Release() let a
        /// second caller see "already released" and return while the first caller, preempted between the flag and
        /// the Release, had not freed the gate yet: the handler then sent its answer, the client sent its next
        /// edit, and that edit was refused with 429. The lock makes the second caller wait that moment out. Nothing
        /// inside it blocks or awaits.
        /// </summary>
        internal void Release()
        {
            lock (sync)
            {
                if (released)
                {
                    return;
                }

                released = true;
                beforeFree?.Invoke();
                gate.Release();
            }
        }
    }

    /// <summary>
    /// PURE: parses the add body into its entries. False with a fixed, caller-facing <paramref name="refusal"/>
    /// when the body is not a JSON array of 1 to <see cref="MaxServersPerAddRequest"/> objects. The refusal names
    /// no part of the body.
    /// </summary>
    internal static bool TryReadServerAddBody(string body, out List<JsonObject> entries, out string? refusal)
    {
        entries = new List<JsonObject>();
        refusal = null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(body);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            refusal = "Request body is not valid JSON.";
            return false;
        }

        if (root is not JsonArray array)
        {
            refusal = "Request body must be a JSON array of server objects.";
            return false;
        }

        if (array.Count == 0)
        {
            refusal = "Request body must hold at least one server object.";
            return false;
        }

        if (array.Count > MaxServersPerAddRequest)
        {
            refusal = $"A request may add at most {MaxServersPerAddRequest} servers; send the rest in another request.";
            return false;
        }

        foreach (var element in array)
        {
            if (element is not JsonObject entry)
            {
                refusal = "Every entry must be a JSON object.";
                return false;
            }

            try
            {
                /* A duplicate property name throws ArgumentException on the first enumeration; force it here so it
                   answers 400, like the dismiss route. */
                _ = entry.Count;
            }
            catch (ArgumentException)
            {
                refusal = "An entry has a duplicate field.";
                return false;
            }

            entries.Add(entry);
        }

        return true;
    }

    /// <summary>The non-empty <c>password</c> values the request carried (a SQL password or a service-principal client secret),
    /// and the typed <c>aws_external_id</c> (#5452), redacted the same way.</summary>
    private static List<string> SubmittedSecrets(List<JsonObject> entries)
    {
        var secrets = new List<string>();
        foreach (var entry in entries)
        {
            if (TryGetString(entry, "password") is { Length: > 0 } secret)
            {
                secrets.Add(secret);
            }

            if (TryGetString(entry, "aws_external_id") is { Length: > 0 } externalId)
            {
                secrets.Add(externalId);
                if (externalId.Trim() is { Length: > 0 } trimmedExternalId && trimmedExternalId != externalId)
                {
                    secrets.Add(trimmedExternalId);
                }
            }
        }

        return secrets;
    }

    /// <summary>PURE: <paramref name="text"/> with every occurrence of every secret replaced.</summary>
    internal static string RedactSecrets(string text, IReadOnlyList<string> secrets)
    {
        foreach (var secret in secrets)
        {
            text = text.Replace(secret, RedactedSecret, StringComparison.Ordinal);
        }

        return text;
    }

    /// <summary>The core's envelope with any submitted secret removed from its free-text fields (<c>detail</c>,
    /// <c>message</c>, <c>server</c>). The envelope keeps its shape; an envelope that does not parse is replaced
    /// by a fixed refusal rather than passed through unredacted.</summary>
    internal static string RedactAddAnswer(string answer, IReadOnlyList<string> secrets)
    {
        if (secrets.Count == 0)
        {
            return answer;
        }

        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return RedactSecrets(answer, secrets);
            }

            RedactField(envelope, "message", secrets);
            if (envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    RedactField(row, "detail", secrets);
                    RedactField(row, "server", secrets);
                }
            }

            return envelope.ToJsonString();
        }
        catch (JsonException)
        {
            return "{\"status\":\"error\",\"message\":\"The answer could not be read.\"}";
        }
    }

    private static void RedactField(JsonObject node, string key, IReadOnlyList<string> secrets)
    {
        if (node[key] is JsonValue value && value.TryGetValue<string>(out var text))
        {
            node[key] = RedactSecrets(text, secrets);
        }
    }

    /// <summary>The auth mode an add entry names, in the words the tool documents; absent means Windows.</summary>
    private static string AuthModeOf(JsonObject entry) => TryGetString(entry, "auth")?.Trim().ToLowerInvariant() switch
    {
        null or "" or "windows" => "Windows",
        "sql" => "SQL",
        "serviceprincipal" => "ServicePrincipal",
        "managedidentity" => "ManagedIdentity",
        _ => "other",
    };

    /// <summary>One Information line per failed row with the core's full detail (secrets already redacted), so the
    /// operator keeps what the web answer no longer shows.</summary>
    private static void LogServerAddFailures(ILogger logger, string principal, string answer)
    {
        try
        {
            if (JsonNode.Parse(answer) is JsonObject envelope && envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    var status = TryGetString(row, "status");
                    if (status != DarlingMcpServerAdminTools.AddStatus.ConnectionFailed && status != DarlingMcpServerAdminTools.AddStatus.NotSaved)
                    {
                        continue;
                    }

                    logger.LogInformation(
                        "Server add failed for {Principal}: {Server}, {Status}: {Detail}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256),
                        DarlingHttpRefusalLog.Sanitize(TryGetString(row, "server") ?? "", 256),
                        status,
                        DarlingHttpRefusalLog.Sanitize(TryGetString(row, "detail") ?? "", 1024));
                }
            }
        }
        catch (JsonException)
        {
            /* An unreadable answer has no rows to log. */
        }
    }

    /// <summary>One Information line per server this request ADDED: who, which server and its auth mode. Request
    /// text is sanitized before it reaches the log; the secret is never read here.</summary>
    private static void LogServerAdds(ILogger logger, string principal, List<JsonObject> entries, string answer)
    {
        var authByName = new Dictionary<string, string>(StringComparer.Ordinal);
        var roleByName = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            var host = TryGetString(entry, "host")?.Trim();
            var name = TryGetString(entry, "display_name") is { Length: > 0 } displayName ? displayName.Trim() : host;
            if (!string.IsNullOrEmpty(name))
            {
                authByName.TryAdd(name, AuthModeOf(entry));
                if (TryGetString(entry, "aws_role_arn")?.Trim() is { Length: > 0 } awsRole)
                {
                    roleByName.TryAdd(name, awsRole);
                }
            }
        }

        try
        {
            if (JsonNode.Parse(answer) is JsonObject { } envelope && envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    if (TryGetString(row, "status") != DarlingMcpServerAdminTools.AddStatus.Added)
                    {
                        continue;
                    }

                    var server = TryGetString(row, "server") ?? "";
                    logger.LogInformation(
                        "Server added by {Principal}: {Server}, auth {AuthMode}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), DarlingHttpRefusalLog.Sanitize(server, 256), authByName.GetValueOrDefault(server, "other"));
                    if (roleByName.TryGetValue(server, out var addedRole))
                    {
                        /* #5452: the role an add set, with the principal. The external ID is never logged. */
                        logger.LogInformation(
                            "AWS role set by {Principal} on add: {Server}, role {NewRole}",
                            DarlingHttpRefusalLog.Sanitize(principal, 256), DarlingHttpRefusalLog.Sanitize(server, 256), DarlingHttpRefusalLog.Sanitize(addedRole, 2200));
                    }
                }
            }
        }
        catch (JsonException)
        {
            /* The answer was written by the core; an unreadable one has no added rows to audit. */
        }
    }
}
