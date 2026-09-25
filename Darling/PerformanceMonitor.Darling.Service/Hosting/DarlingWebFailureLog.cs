/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// #4276: what an unhandled exception from a web-dashboard route becomes — one log line through the
/// service's own logger, and a JSON body shaped like <c>DarlingWebEndpoints.ErrorResult</c> — shared by the
/// top-of-pipeline backstop (<see cref="Mcp.DarlingWebHostService.ConfigurePipeline"/>) and the
/// <c>/api/read/*</c> dispatcher (<see cref="DarlingWebEndpoints.MapAll"/>) so neither the wording nor the
/// timeout/error split can drift between the two call sites.
///
/// <para><b>Why this needed its own logger, not <c>app.Logger</c>.</b> <c>ConfigurePipeline</c> clears the
/// web host's log providers on purpose (request noise has no seat in the service log), which leaves
/// <c>app.Logger</c> a logger with nowhere to write — an endpoint that logged through it would degrade with
/// no trace, which is the exact gap #4276 reports for <c>/api/ag</c> and <c>/api/fleet</c>: both had no
/// try/catch, so their exception reached ASP.NET Core's own (silenced) error logging and the browser got an
/// empty 500. Both call sites here pass the service's real <see cref="ILogger"/> instead.</para>
/// </summary>
internal static class DarlingWebFailureLog
{
    /// <summary>The body's message for a statement timeout — deliberately silent on WHERE it timed out or
    /// why; that detail is in the service log, not on the wire to whoever is holding the browser.</summary>
    internal const string TimeoutMessage = "The store took too long to answer this read. Try again in a moment.";

    /// <summary>The body's message for anything else. Never the exception text (ruled #4276): a stack-shaped
    /// string on the wire is an information leak for no reader's benefit — the service log carries the real
    /// exception for whoever can act on it.</summary>
    internal const string GenericMessage = "Something went wrong answering this request. The service log names what failed.";

    /// <summary>
    /// A statement timeout, either side of the connection: the server cancelling us at its own
    /// <c>statement_timeout</c> (<see cref="PostgresException"/>, SQLSTATE <c>57014</c>), or Npgsql's own
    /// client-side <c>CommandTimeout</c> elapsing first (an <see cref="NpgsqlException"/> wrapping a
    /// <see cref="TimeoutException"/> — the shape the driver actually throws; a bare
    /// <see cref="TimeoutException"/> is not one Npgsql produces, but is accepted too so a hand-built test
    /// exception classifies the same as the real one).
    /// </summary>
    internal static bool IsStatementTimeout(Exception exception) =>
        exception is PostgresException { SqlState: "57014" }
        || exception is TimeoutException
        || (exception is NpgsqlException && exception.InnerException is TimeoutException);

    /// <summary>The SQLSTATE named in the log line, or "(none)" for anything that isn't a
    /// <see cref="PostgresException"/> — a client-side timeout and a plain bug both carry none.</summary>
    private static string SqlState(Exception exception) =>
        (exception as PostgresException)?.SqlState ?? "(none)";

    /// <summary>
    /// The ONE log line for a failed request (#4276): a Warning for a timeout, an Error for anything else,
    /// naming the route, the elapsed milliseconds, the kind, the exception type and the SQLSTATE. No
    /// throttle — unlike <c>DarlingHttpRefusalLog</c>'s refusals, a failed READ on an operator's own LAN
    /// dashboard is not adversary-shaped traffic, so there is no flood to bound. <paramref name="route"/> is
    /// request-supplied (the backstop passes <c>context.Request.Path.Value</c> straight off the wire), so it
    /// goes through <see cref="DarlingHttpRefusalLog.Sanitize"/> the same way that log sanitizes a Host
    /// header, before either branch below writes it.
    /// </summary>
    internal static void Report(ILogger logger, string route, long elapsedMs, Exception exception)
    {
        var safeRoute = DarlingHttpRefusalLog.Sanitize(route, 256);

        if (IsStatementTimeout(exception))
        {
            logger.LogWarning(
                exception,
                "Web dashboard read {Route} timed out after {ElapsedMs} ms ({Kind}): {ExceptionType}, SQLSTATE {SqlState}",
                safeRoute, elapsedMs, "timeout", exception.GetType().Name, SqlState(exception));
            return;
        }

        logger.LogError(
            exception,
            "Web dashboard read {Route} failed after {ElapsedMs} ms ({Kind}): {ExceptionType}, SQLSTATE {SqlState}",
            safeRoute, elapsedMs, "error", exception.GetType().Name, SqlState(exception));
    }

    /// <summary>The HTTP status a failure answers with: 503 for a timeout (tell-apart-from-a-bug, per the
    /// issue), 500 for anything else.</summary>
    internal static int StatusCode(Exception exception) =>
        IsStatementTimeout(exception) ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status500InternalServerError;

    /// <summary>The response body: <c>{"error": "…"}</c>, the same shape
    /// <c>DarlingWebEndpoints.ErrorResult</c> already writes, so the viewer's existing error display handles
    /// it without a new code path.</summary>
    internal static JsonObject Body(Exception exception) =>
        new() { ["error"] = IsStatementTimeout(exception) ? TimeoutMessage : GenericMessage };
}
