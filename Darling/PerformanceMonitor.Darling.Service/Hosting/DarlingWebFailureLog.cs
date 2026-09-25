/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
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

    /// <summary>
    /// <see cref="IsStatementTimeout(Exception)"/>'s twin for a tool that has already caught its own exception
    /// and stringified it (#4283): a tool catches internally and returns <c>McpHelpers.FormatError</c>'s
    /// envelope, so by the time the web mapping sees the failure there is no <see cref="Exception"/> left to
    /// inspect — only the sentence <c>ErrorSentence</c> built, <c>"Error during {op}: {ex.Message}"</c>. That
    /// throws away <see cref="PostgresException.SqlState"/> as a structured field, but NOT as text: verified
    /// against Npgsql 10.0.3, <see cref="PostgresException"/> never overrides <c>Message</c> (it stays
    /// <see cref="Exception"/>'s own property, set from the base constructor), and the base constructor is
    /// called with <c>$"{SqlState}: {MessageText}"</c> — so <c>ex.Message</c> for a statement_timeout always
    /// starts with the literal, un-translated <c>57014</c> token even though the text after it
    /// (<c>MessageText</c>) is <c>lc_messages</c>-dependent. Matching the CODE rather than the prose keeps this
    /// out of the message-text-matching trap <c>PgBaselineProvider.IsCommandTimeout</c>'s own doc warns against.
    ///
    /// <para>A client-side Npgsql <c>CommandTimeout</c> (the <see cref="NpgsqlException"/>/<see
    /// cref="TimeoutException"/> arm of <see cref="IsStatementTimeout(Exception)"/>) carries no SQLSTATE and so
    /// is NOT matched here — it falls through to the generic 500. Accepted: <c>McpCommandDeadlines</c>'s own
    /// comment says the store's statement_timeout deliberately fires first on an MCP/web read, so a tool-caught
    /// timeout reaches this classifier as a real 57014 in practice (the shape <c>McpReadCommandTimeoutTests</c>
    /// pins), not as the client-side race.</para>
    ///
    /// <para>Word-boundary, not a bare substring — the same defense
    /// <c>MigrationDataMovingRungCensusPins</c>'s <c>s_cancelTrap</c> uses against a digit run or identifier
    /// that merely contains <c>57014</c> (a table name, a row count) rather than naming the SQLSTATE.</para>
    /// </summary>
    private static readonly Regex s_sentenceTimeoutToken = new(@"\b57014\b", RegexOptions.Compiled);

    internal static bool IsStatementTimeoutSentence(string sentence) =>
        sentence is not null && s_sentenceTimeoutToken.IsMatch(sentence);

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

    /// <summary>
    /// <see cref="Report(ILogger,string,long,Exception)"/>'s twin for a tool-caught failure (#4283): no
    /// <see cref="Exception"/> survives, so the log line carries the sentence itself (the same text a
    /// pre-#4283 response would have put on the wire) instead of an exception object and its type name.
    /// <paramref name="route"/> is sanitized exactly as the exception overload sanitizes it; the sentence is
    /// NOT — it is our own <c>McpHelpers.ErrorSentence</c> output, not request-supplied.
    /// </summary>
    internal static void Report(ILogger logger, string route, long elapsedMs, string sentence)
    {
        var safeRoute = DarlingHttpRefusalLog.Sanitize(route, 256);

        if (IsStatementTimeoutSentence(sentence))
        {
            logger.LogWarning(
                "Web dashboard read {Route} timed out after {ElapsedMs} ms ({Kind}): {Sentence}",
                safeRoute, elapsedMs, "timeout", sentence);
            return;
        }

        logger.LogError(
            "Web dashboard read {Route} failed after {ElapsedMs} ms ({Kind}): {Sentence}",
            safeRoute, elapsedMs, "error", sentence);
    }

    /// <summary>The HTTP status a failure answers with: 503 for a timeout (tell-apart-from-a-bug, per the
    /// issue), 500 for anything else.</summary>
    internal static int StatusCode(Exception exception) => StatusCodeFor(IsStatementTimeout(exception));

    /// <summary><see cref="StatusCode(Exception)"/>'s twin over the tool-caught sentence.</summary>
    internal static int StatusCode(string sentence) => StatusCodeFor(IsStatementTimeoutSentence(sentence));

    private static int StatusCodeFor(bool isTimeout) =>
        isTimeout ? StatusCodes.Status503ServiceUnavailable : StatusCodes.Status500InternalServerError;

    /// <summary>The response body: <c>{"error": "…"}</c>, the same shape
    /// <c>DarlingWebEndpoints.ErrorResult</c> already writes, so the viewer's existing error display handles
    /// it without a new code path.</summary>
    internal static JsonObject Body(Exception exception) => BodyFor(IsStatementTimeout(exception));

    /// <summary><see cref="Body(Exception)"/>'s twin over the tool-caught sentence.</summary>
    internal static JsonObject Body(string sentence) => BodyFor(IsStatementTimeoutSentence(sentence));

    private static JsonObject BodyFor(bool isTimeout) =>
        new() { ["error"] = isTimeout ? TimeoutMessage : GenericMessage };
}
