/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.CompilerServices;
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
    /// <para>Anchored to a known prefix, not a bare word-boundary (#4283 L1): a word boundary alone still lets
    /// <c>57014</c> match anywhere in the tail text — a repeated value, a port number, an identifier that merely
    /// contains the digits — the same trap <c>MigrationDataMovingRungCensusPins</c>'s <c>s_cancelTrap</c> guards
    /// against for a different sentence shape. Anchoring to the START of the sentence and requiring the token
    /// immediately after one of the three known prefixes (<c>ErrorSentence</c>'s <c>"Error during {op}: "</c>,
    /// the compose route's <c>"Error running query: "</c>, and the resolver's own
    /// <see cref="Mcp.DarlingServerResolver.RegistryReadFaultPrefix"/>) means the token can only match where a
    /// real SQLSTATE sits, not wherever the digits happen to recur later in the same sentence.</para>
    /// </summary>
    private static readonly Regex s_sentenceTimeoutToken = new(
        $@"^(Error during [^:]+: |Error running query: |{Regex.Escape(Mcp.DarlingServerResolver.RegistryReadFaultPrefix)})57014: ",
        RegexOptions.Compiled);

    internal static bool IsStatementTimeoutSentence(string sentence) =>
        sentence is not null && s_sentenceTimeoutToken.IsMatch(sentence);

    /// <summary>The SQLSTATE named in the log line, or "(none)" for anything that isn't a
    /// <see cref="PostgresException"/> — a client-side timeout and a plain bug both carry none.</summary>
    private static string SqlState(Exception exception) =>
        (exception as PostgresException)?.SqlState ?? "(none)";

    /// <summary>W12: why a timeout-shaped exception happened. A server-side <c>statement_timeout</c> carries SQLSTATE
    /// 57014; anything else here is Npgsql giving up waiting on its own side: the command's client deadline, or the wait for
    /// a free pooled connection. The driver words those two the same way, and the exception text is never read (#4283), so
    /// the line names both rather than guessing one.</summary>
    internal static string Cause(Exception exception)
    {
        return SqlState(exception) == "57014" ? "statement_timeout" : "client_timeout_or_pool_wait";
    }

    /// <summary>What one request's failure reporting tracks: whether a failure line was written, and the server name the
    /// registry gave the read (<see cref="NoteResolvedServer"/>).</summary>
    internal sealed class RequestTracking
    {
        /// <summary>True once <see cref="Report(ILogger,string,long,Exception)"/> or its sentence twin wrote the request's line.</summary>
        public bool Reported { get; set; }

        /// <summary>The name the registry resolved the read's server to, or null when the read resolved none.</summary>
        public string? ServerName { get; set; }

        /// <summary>True once the request got through the auth gate (or, on a server with none, the Host guard), so a throw after
        /// that point is a signed-in caller's failure. A throw before it can come from a caller nobody has authenticated, and the
        /// observer sends that line through a throttle (L1, round 2).</summary>
        public bool PassedAuth { get; set; }
    }

    private static readonly System.Threading.AsyncLocal<RequestTracking?> s_requestReported = new();

    /// <summary>W12: starts tracking whether the current request's failure got a log line. The outermost observer calls it
    /// before anything else runs; <see cref="Report(ILogger,string,long,Exception)"/> and its sentence twin tick the
    /// box, so a 5xx answered WITHOUT a report is the one the observer reports (<see cref="ReportUnlogged"/>).</summary>
    internal static RequestTracking BeginRequestTracking()
    {
        var box = new RequestTracking();
        s_requestReported.Value = box;
        return box;
    }

    /// <summary>Round 2, L1: the auth gate (or the Host guard of a server with none) calls this once the request is let through.
    /// Ignored outside a tracked request.</summary>
    internal static void NotePassedAuth()
    {
        if (s_requestReported.Value is { } box) box.PassedAuth = true;
    }

    /// <summary>Ticks the request's tracking as reported without writing a line: the observer's throttled arm uses it when the
    /// throttle folded the line, so the post-check does not write a second one for the same request.</summary>
    internal static void MarkReported()
    {
        var box = s_requestReported.Value;
        if (box is not null) box.Reported = true;
    }

    /// <summary>S5: the server resolver notes the registry's own name for the server a read resolved, so a failure line names the
    /// server as the registry does (a name the diagnostics bundle's aliaser knows whole) and not as the request spelled it.
    /// Ignored outside a tracked request.</summary>
    internal static void NoteResolvedServer(string? serverName)
    {
        if (s_requestReported.Value is { } box && !string.IsNullOrEmpty(serverName)) box.ServerName ??= serverName;
    }

    /// <summary>
    /// W12: the one log line for an /api answer of 500 or more that no failure report covered, so a 503 (or any 5xx) never
    /// leaves the page with a red strip and the service log with nothing. Warning for 503, Error for the rest.
    /// <paramref name="route"/> is <see cref="RouteOf"/>'s text: the path and the server the read named.
    /// </summary>
    internal static void ReportUnlogged(ILogger logger, string route, int status, long elapsedMs)
    {
        var safeRoute = DarlingHttpRefusalLog.Sanitize(route, 256);
        if (status == StatusCodes.Status503ServiceUnavailable)
        {
            logger.LogWarning(
                "Web dashboard read {Route} answered HTTP {Status} after {ElapsedMs} ms ({Kind}): the route gave no cause of its own",
                safeRoute, status, elapsedMs, "unreported");
            return;
        }

        logger.LogError(
            "Web dashboard read {Route} answered HTTP {Status} after {ElapsedMs} ms ({Kind}): the route gave no cause of its own",
            safeRoute, status, elapsedMs, "unreported");
    }

    /// <summary>W12: the request's path, plus the server the read named (<c>server</c> or <c>server_name</c>) so a
    /// failed read's line says WHICH server it was for. S5: the server is written as the registry names it when the read
    /// resolved one (<see cref="NoteResolvedServer"/>); otherwise it is the request's own text passed through the diagnostics
    /// bundle's aliaser (<see cref="BundleAliaser"/>), which rewrites what it can recognise without a name list (IP addresses,
    /// secrets), because a bundle can only replace the names it knows and a partial or unregistered spelling is not one.
    /// The whole route is sanitized by the caller.</summary>
    internal static string RouteOf(HttpRequest request)
    {
        var path = request.Path.Value ?? "/";
        var server = s_requestReported.Value?.ServerName
            ?? (request.Query.TryGetValue("server", out var a) && a.Count > 0 && !string.IsNullOrEmpty(a[0]) ? AliasRequestText(a[0])
            : request.Query.TryGetValue("server_name", out var b) && b.Count > 0 && !string.IsNullOrEmpty(b[0]) ? AliasRequestText(b[0])
            : null);
        return server is null ? path : path + " (server " + server + ")";
    }

    /// <summary>The request text through a bundle aliaser that knows no names: its secret guard and address rewrite still apply.
    /// Capped first, so a long value is not scanned in full.</summary>
    private static string? AliasRequestText(string? text)
    {
        if (string.IsNullOrEmpty(text)) return null;
        var capped = text.Length > 256 ? text[..256] : text;
        return new BundleAliaser().Alias(capped);
    }

    /// <summary>
    /// The ONE log line for a failed request (#4276): a Warning for a timeout, an Error for anything else,
    /// naming the route, the elapsed milliseconds, the kind, the exception type and the SQLSTATE. No
    /// throttle here — unlike <c>DarlingHttpRefusalLog</c>'s refusals, a failed READ on an operator's own LAN
    /// dashboard is not adversary-shaped traffic, so there is no flood to bound. That holds for a request that got through the auth
    /// gate. The observer sits ahead of the Host guard and the auth gate, so a throw from one of those, or from compression,
    /// may belong to a caller nobody has authenticated: the observer sends THAT line through the refusal log's throttle before it
    /// calls this (<see cref="RequestTracking.PassedAuth"/>). <paramref name="route"/> is
    /// request-supplied (the backstop passes <c>context.Request.Path.Value</c> straight off the wire), so it
    /// goes through <see cref="DarlingHttpRefusalLog.Sanitize"/> the same way that log sanitizes a Host
    /// header, before either branch below writes it.
    /// </summary>
    internal static void Report(ILogger logger, string route, long elapsedMs, Exception exception)
    {
        var safeRoute = DarlingHttpRefusalLog.Sanitize(route, 256);
        MarkReported();

        if (IsStatementTimeout(exception))
        {
            logger.LogWarning(
                exception,
                "Web dashboard read {Route} timed out after {ElapsedMs} ms ({Kind}, cause {Cause}): {ExceptionType}, SQLSTATE {SqlState}",
                safeRoute, elapsedMs, "timeout", Cause(exception), exception.GetType().Name, SqlState(exception));
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
        MarkReported();

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
