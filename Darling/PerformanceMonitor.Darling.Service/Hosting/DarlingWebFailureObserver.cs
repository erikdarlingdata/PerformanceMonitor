/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The web host's one failure observer (#4276, W12): the FIRST middleware registered, ahead of response compression, the
/// Host-allowlist guard, the method gate and the auth gate, so a throw from any of them is logged too. It replaces the #4276
/// backstop that sat behind the gates and could not see them; a request still gets exactly one failure line, because the
/// routes' own <see cref="DarlingWebFailureLog.Report(ILogger,string,long,Exception,System.Threading.CancellationToken)"/> calls tick the per-request tracking this
/// reads, and a thrown exception is reported here only when nothing reported it first.
///
/// <para>It decides nothing about a request before <c>next</c> runs (no refusal, no write), so putting it ahead of the Host guard
/// adds no unauthenticated surface: what it logs after the fact is the path, the status and the elapsed time, plus the server the
/// read named, sanitized (<see cref="DarlingWebFailureLog.RouteOf"/>). A client that closed the page is not a failure and is not
/// logged, and nothing is written to a caller who is gone. Context: two <c>/api/read</c> 503s on a real host left no line and no
/// code path was found; this is how the next one is caught.</para>
/// </summary>
internal sealed class DarlingWebFailureObserver
{
    private readonly RequestDelegate _next;
    private readonly ILogger _logger;
    private readonly DarlingHttpRefusalLog _preAuthThrottle;
    private readonly bool _managedStore;

    /// <param name="preAuthThrottle">Round 2, L1: the throttle for the failure line of a request that has not passed the auth gate
    /// (<see cref="DarlingWebFailureLog.RequestTracking.PassedAuth"/>). The host passes the refusal log it created for this started
    /// server, so a rebind starts with a clean budget.</param>
    /// <param name="managedStore">Whether the host runs on the managed store; it picks the words of the 403 a refused write answers with.</param>
    public DarlingWebFailureObserver(RequestDelegate next, ILogger logger, DarlingHttpRefusalLog preAuthThrottle, bool managedStore = false)
    {
        _next = next;
        _logger = logger;
        _preAuthThrottle = preAuthThrottle;
        _managedStore = managedStore;
    }

    /// <summary>
    /// #5678: true when the request's caller has gone and the exception in flight is therefore not the service's failure to report:
    /// the request token has fired. The token decides, not the exception's type, so every type the old filter swallowed (an
    /// OperationCanceledException, an IOException, and so a BadHttpRequestException, which derives from IOException and was
    /// swallowed here before its own arm) is still swallowed, and so is the NpgsqlException a connect timeout throws.
    /// </summary>
    internal static bool IsCallerGone(CancellationToken requestAborted) => requestAborted.IsCancellationRequested;

    public async Task InvokeAsync(HttpContext context)
    {
        var stopwatch = Stopwatch.StartNew();
        var tracking = DarlingWebFailureLog.BeginRequestTracking();
        tracking.IsWrite = !(HttpMethods.IsGet(context.Request.Method) || HttpMethods.IsHead(context.Request.Method) || HttpMethods.IsOptions(context.Request.Method));
        tracking.ManagedStore = _managedStore;
        try
        {
            await _next(context);
        }
        catch (Exception) when (IsCallerGone(context.RequestAborted))
        {
            /* #5678: the caller's cancelled token decides, not the exception's type. A caller who left while the store was being
               reached can surface as an OperationCanceledException, as an IOException (a TCP reset, or "The client reset the
               request stream." on HTTP/2 -- #4286 review, Low 2) or, when a connect timeout beats the driver's look at the token,
               as an NpgsqlException that no cancellation filter would match. Same classification ASP.NET Core's own exception
               handler middleware makes for a client abort: no line, no body. A handler that logs a failure itself sends the same
               caller one Debug line (DarlingWebFailureLog.Report, given the request token). */
        }
        catch (BadHttpRequestException bad)
        {
            /* #4281 review, finding 3: Kestrel throws this for a malformed or oversized request body (a 413/400/408 a client
               can trigger on purpose at no cost) -- not a service failure, so no generic 500 and no Error line. Debug only,
               and the exception object, not a {Message} template argument, carries the text (#4286 review, Low 5). */
            _logger.LogDebug(bad, "Web dashboard request rejected ({StatusCode})", bad.StatusCode);

            if (!context.Response.HasStarted)
            {
                context.Response.StatusCode = bad.StatusCode;
            }
        }
        catch (Exception ex)
        {
            /* Round 2, L1: this arm now also sees throws from compression, the Host guard and the auth gate, so the request may
               not be authenticated. Such a line goes through the refusal log's throttle (once per source per window); the answer
               below is unchanged. A request that got through the gate keeps the unthrottled line it always had. */
            if (tracking.PassedAuth)
            {
                DarlingWebFailureLog.Report(_logger, DarlingWebFailureLog.RouteOf(context.Request), stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
            }
            else
            {
                var decision = _preAuthThrottle.Observe(
                    DarlingRefusalGate.PreAuthFailure, DarlingHttpRefusalLog.DescribeSource(context.Connection.RemoteIpAddress), DateTime.UtcNow);
                if (decision.Log)
                {
                    DarlingWebFailureLog.Report(_logger, DarlingWebFailureLog.RouteOf(context.Request), stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                }
                else
                {
                    DarlingWebFailureLog.MarkReported();
                }
            }

            /* Only log, per the ruling, once the response has already started -- there is no header or body left to change. */
            if (!context.Response.HasStarted)
            {
                context.Response.StatusCode = DarlingWebFailureLog.StatusCode(ex);
                context.Response.ContentType = "application/json; charset=utf-8";
                await context.Response.WriteAsync(DarlingWebFailureLog.Body(ex).ToJsonString());
            }
        }

        /* A read that answered 5xx (a 503 above all) with no failure report of its own still leaves one line, naming the
           endpoint and the server, so the page's red strip always has a service-log line behind it. /api/ping answers 503 on
           purpose for a stopped collector and has its own contract. The route text is built here, after the read, so a server the
           read resolved is written under the registry's name. */
        if (!tracking.Reported
            && context.Response.StatusCode >= StatusCodes.Status500InternalServerError
            && context.Request.Path.StartsWithSegments("/api")
            && !context.Request.Path.StartsWithSegments("/api/ping"))
        {
            DarlingWebFailureLog.ReportUnlogged(
                _logger, DarlingWebFailureLog.RouteOf(context.Request), context.Response.StatusCode, stopwatch.ElapsedMilliseconds);
        }
    }
}
