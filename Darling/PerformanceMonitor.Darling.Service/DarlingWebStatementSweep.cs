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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Hosting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #4348, the web side of the statement filter's read-time sweep. The MCP host sweeps each tool's result in one
/// call-tool filter; the web routes answer through their own writers, so the sweep runs at the top of each writer
/// that puts a tool's string (or a body built from tool strings) on the wire: <c>ToHttpResult</c>,
/// <c>MuteRuleToolResult</c>, and the three endpoints that write their own body (triage, the alert notebook and the
/// fleet sweep feed). Every route then returns through one of these or is on the short named list that
/// <c>WebRouteStatementCensusTests</c> pins.
///
/// <para>A body the sweep cannot read is never written. It becomes a 500 whose body is the one sentence
/// <see cref="SensitiveStatements.JsonRefusal"/>, the same sentence the MCP host answers with, and the service log
/// names the route.</para>
/// </summary>
internal static class DarlingWebStatementSweep
{
    /// <summary>The swept text, and whether the sweep refused it.</summary>
    internal readonly record struct Swept(string Text, bool Refused);

    /// <summary>Sweeps one tool string or JSON body. The same string instance comes back when nothing in it is
    /// named. Never throws and never hands the input back after a failure.</summary>
    internal static Swept Apply(string? output)
    {
        if (string.IsNullOrEmpty(output)) return new Swept(output ?? "", false);

        try
        {
            string judged = SensitiveStatements.Json(output);
            bool refused = string.Equals(judged, SensitiveStatements.JsonRefusal, StringComparison.Ordinal)
                && !string.Equals(output, SensitiveStatements.JsonRefusal, StringComparison.Ordinal);
            return refused ? new Swept(SensitiveStatements.JsonRefusal, true) : new Swept(judged, false);
        }
#pragma warning disable CA1031 // fail closed: the sweep never lets the input through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            return new Swept(SensitiveStatements.JsonRefusal, true);
        }
    }

    /// <summary>The answer for a body the sweep refused: logged once, then a 500 carrying the one sentence.</summary>
    internal static IResult Refusal(string route, ILogger? logger, long elapsedMs)
    {
        if (logger is not null) DarlingWebFailureLog.Report(logger, route, elapsedMs, SensitiveStatements.JsonRefusal);
        return Results.Json(new { error = SensitiveStatements.JsonRefusal }, statusCode: StatusCodes.Status500InternalServerError);
    }

    /// <summary>Writes a JSON body the endpoint built itself, after sweeping it.</summary>
    internal static IResult JsonText(JsonNode body, string route, ILogger? logger, long elapsedMs, int statusCode = StatusCodes.Status200OK)
    {
        var swept = Apply(body.ToJsonString());
        return swept.Refused
            ? Refusal(route, logger, elapsedMs)
            : Results.Text(swept.Text, "application/json", statusCode: statusCode);
    }
}
