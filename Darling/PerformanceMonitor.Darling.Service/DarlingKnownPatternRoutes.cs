/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Service.Hosting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The web dashboard's known-pattern editor writes (#6334): <c>POST /api/known-patterns</c> (add, 201), <c>PUT /api/known-patterns/{id}</c>
/// (replace the whole entry, 200) and <c>DELETE /api/known-patterns/{id}</c> (200). The list is the existing read, <c>/api/read/get_known_patterns</c>.
/// They follow the mute-rule routes: <c>application/json</c> is required on every write (415 otherwise, the CSRF discipline), a read-only seat
/// is refused by the host's method gate (<see cref="DarlingWebSeat.IsRequestAllowed"/>) before a route runs, a bad body is a 400 carrying the
/// reason, and an unknown id is a 404. Writes go through <see cref="PgKnownPatternStore"/> only, on the viewer-role pool, which holds EXECUTE on the
/// three store functions and nothing else on the table. The entry's <c>created_by</c> is the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>.
/// A server id must name a registered server, as a mute rule's must. There is no MCP twin: entries are written by operators only.
///
/// <para>Body (add and replace, one JSON object): <c>note</c> (required), <c>server_id</c>, <c>metric_name</c>, <c>time_of_day</c> ("HH:mm"),
/// <c>time_zone</c> (IANA id), <c>window_minutes</c>, <c>expires_at_utc</c> (ISO 8601). A field left out or null means any / UTC / the default window / never.
/// The refusal text is <see cref="KnownPatternInput.Validate"/>'s, the same in the Viewer.</para>
/// </summary>
internal static class DarlingKnownPatternRoutes
{
    /// <summary>The most request-body bytes a write reads: a note is at most 500 characters, so a few KB is plenty.</summary>
    internal const int MaxBodyBytes = 16 * 1024;

    private static readonly string[] BodyFields =
        ["server_id", "metric_name", "time_of_day", "time_zone", "window_minutes", "note", "expires_at_utc"];

    internal static void Map(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new PgKnownPatternStore(postgres);
        Func<int, Task<string?>> serverLookup = id => Mcp.DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id);

        app.MapPost("/api/known-patterns", async (HttpContext context) =>
        {
            if (!DarlingWebEndpoints.IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaType();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (input, refusal) = await ReadInputAsync(context, serverLookup);
                if (input is null)
                {
                    return Invalid(refusal!);
                }

                var principal = DarlingWebSeat.FromContext(context).EditorPrincipal;
                var id = await store.InsertAsync(input, principal, context.RequestAborted);
                logger.LogInformation("Known pattern add by {Principal}: id {Id}", principal, id);
                return Results.Json(new JsonObject { ["status"] = "created", ["id"] = id }, statusCode: StatusCodes.Status201Created);
            }
            catch (ArgumentException ex)
            {
                return Invalid(ex.Message);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return Failed(ex, logger, stopwatch, context, "/api/known-patterns");
            }
        });

        app.MapPut("/api/known-patterns/{id:long}", async (HttpContext context, long id) =>
        {
            if (!DarlingWebEndpoints.IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaType();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (input, refusal) = await ReadInputAsync(context, serverLookup);
                if (input is null)
                {
                    return Invalid(refusal!);
                }

                if (!await store.UpdateAsync(id, input, context.RequestAborted))
                {
                    return NotFound();
                }

                logger.LogInformation("Known pattern edit by {Principal}: id {Id}", DarlingWebSeat.FromContext(context).EditorPrincipal, id);
                return Results.Json(new JsonObject { ["status"] = "updated", ["id"] = id });
            }
            catch (ArgumentException ex)
            {
                return Invalid(ex.Message);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return Failed(ex, logger, stopwatch, context, "/api/known-patterns/{id}");
            }
        });

        app.MapDelete("/api/known-patterns/{id:long}", async (HttpContext context, long id) =>
        {
            if (!DarlingWebEndpoints.IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaType();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (!await store.DeleteAsync(id, context.RequestAborted))
                {
                    return NotFound();
                }

                logger.LogInformation("Known pattern delete by {Principal}: id {Id}", DarlingWebSeat.FromContext(context).EditorPrincipal, id);
                return Results.Json(new JsonObject { ["status"] = "deleted", ["id"] = id });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return Failed(ex, logger, stopwatch, context, "/api/known-patterns/{id}");
            }
        });
    }

    /// <summary>Reads the body into the store's input, or the reason it cannot be (a malformed body or a field of the wrong type).
    /// The store's own <see cref="KnownPatternInput.Validate"/> check still runs on the result, so its text is what a bad value reads.</summary>
    internal static async Task<(KnownPatternInput? Input, string? Refusal)> ReadInputAsync(HttpContext context, Func<int, Task<string?>> serverLookup)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[4096];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
        {
            if (buffer.Length + read > MaxBodyBytes)
            {
                return (null, "Request body is too large.");
            }

            buffer.Write(chunk, 0, read);
        }

        var text = System.Text.Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);

        JsonObject body;
        try
        {
            if (JsonNode.Parse(text) is not JsonObject parsed)
            {
                return (null, "Request body must be a JSON object.");
            }

            _ = parsed.Count;
            body = parsed;
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }
        catch (ArgumentException)
        {
            return (null, "Request body has a duplicate field.");
        }

        foreach (var pair in body)
        {
            if (Array.IndexOf(BodyFields, pair.Key) < 0)
            {
                return (null, $"'{pair.Key}' is not a known-pattern field. The fields are {string.Join(", ", BodyFields)}.");
            }
        }

        var (serverId, e1) = Number(body, "server_id");
        var (windowMinutes, e2) = Number(body, "window_minutes");
        var (metric, e3) = Str(body, "metric_name");
        var (timeOfDayText, e4) = Str(body, "time_of_day");
        var (zone, e5) = Str(body, "time_zone");
        var (note, e6) = Str(body, "note");
        var (expiresText, e7) = Str(body, "expires_at_utc");
        var error = e1 ?? e2 ?? e3 ?? e4 ?? e5 ?? e6 ?? e7;
        if (error is not null)
        {
            return (null, error);
        }

        TimeOnly? timeOfDay = null;
        if (!string.IsNullOrWhiteSpace(timeOfDayText))
        {
            if (!TimeOnly.TryParseExact(timeOfDayText.Trim(), ["HH:mm", "H:mm", "HH:mm:ss"], CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsedTime))
            {
                return (null, "time_of_day must be a time such as 02:00.");
            }

            timeOfDay = parsedTime;
        }

        DateTime? expires = null;
        if (!string.IsNullOrWhiteSpace(expiresText))
        {
            if (!DateTime.TryParse(expiresText.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsedExpiry))
            {
                return (null, "expires_at_utc must be a date and time such as 2026-12-31T00:00:00Z.");
            }

            expires = parsedExpiry;
        }

        var input = new KnownPatternInput(serverId, metric, timeOfDay, zone, windowMinutes, note, expires);

        /* The reason first, so a bad note reads the same here as in the Viewer; then the server, which only the store behind the registry can say. */
        var refusal = input.Validate();
        if (refusal is not null)
        {
            return (null, refusal);
        }

        if (serverId is { } id && await serverLookup(id) is null)
        {
            return (null, $"server_id {id} is not a registered server.");
        }

        return (input, null);
    }

    private static (int? Value, string? Error) Number(JsonObject body, string name)
    {
        if (body[name] is null)
        {
            return (null, null);
        }

        return body[name] is JsonValue value && value.TryGetValue<int>(out var number)
            ? (number, null)
            : (null, $"{name} must be a whole number or null.");
    }

    private static (string? Value, string? Error) Str(JsonObject body, string name)
    {
        if (body[name] is null)
        {
            return (null, null);
        }

        return body[name] is JsonValue value && value.TryGetValue<string>(out var text)
            ? (text, null)
            : (null, $"{name} must be text or null.");
    }

    private static IResult Invalid(string message) =>
        Results.Json(new JsonObject { ["status"] = "invalid", ["error"] = message }, statusCode: StatusCodes.Status400BadRequest);

    private static IResult NotFound() =>
        Results.Json(new JsonObject { ["status"] = "not_found", ["error"] = "Known pattern not found." }, statusCode: StatusCodes.Status404NotFound);

    private static IResult UnsupportedMediaType() =>
        Results.Json(new JsonObject { ["error"] = "Content-Type must be application/json." }, statusCode: StatusCodes.Status415UnsupportedMediaType);

    private static IResult Failed(Exception ex, ILogger logger, Stopwatch stopwatch, HttpContext context, string route)
    {
        DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
        return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
    }
}
