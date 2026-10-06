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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5245: <c>GET /api/server-databases?server=&lt;route key&gt;</c>, the list the web viewer's database picker offers:
/// the user databases the store has collected for one server, answered as <c>{ "server": "...", "databases": [...], "truncated": false }</c>. The list stops at
/// <see cref="MaxDatabases"/> names and <c>truncated</c> is true when the server has more.
///
/// <para><b>Why a route and not an MCP tool.</b> No agent needs the list, so a tool would only add bytes to
/// <c>tools/list</c> and a Lite twin for nothing. The route runs <see cref="CollectedDatabases.NamesLimitedSql"/>, which is
/// <see cref="CollectedDatabases.NamesSql"/> (the constant the desktop viewer's Excluded Databases picker runs) plus a
/// row limit, so the two lists cannot drift. It is loaded when the
/// picker opens, as the desktop does, and it is read-only: the sign-in gate and the seat's method gate in front of
/// every route let a read-only seat make a GET, and the read runs on the viewer-role pool.</para>
///
/// <para><b>Names go out as data.</b> A database name is whatever the monitored server holds (a name may carry
/// <c>&lt;</c>, <c>&amp;</c> or a quote), so the body is JSON written by <see cref="JsonSerializer"/> with its default
/// encoder, which escapes <c>&lt; &gt; &amp; ' +</c> as <c>\uXXXX</c>, and it is sent as <c>application/json</c>.
/// The page shows each name with <c>textContent</c>, never as markup.</para>
///
/// <para>The server is resolved the way every other server-scoped web read resolves it
/// (<see cref="DarlingServerResolver.ResolveOrErrorAsync"/>), so an unknown name gets the same 400 refusal and a
/// registry fault the same fixed 5xx body.</para>
/// </summary>
internal static class DarlingServerDatabasesEndpoint
{
    /// <summary>The route the picker calls.</summary>
    internal const string Route = "/api/server-databases";

    /// <summary>Maps <see cref="Route"/>. Called once from <see cref="DarlingWebEndpoints.MapAll"/>.</summary>
    internal static void Map(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        app.MapGet(Route, async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(
                postgres, DarlingWebEndpoints.First(context, "server"), context.RequestAborted);
            if (error is not null)
            {
                return DarlingWebEndpoints.ToHttpResult(error, Route, logger, stopwatch.ElapsedMilliseconds);
            }

            try
            {
                var (names, truncated) = await ReadAsync(postgres, resolved.ServerId, MaxDatabases, context.RequestAborted);
                return Results.Text(Render(resolved.ServerName, names, truncated), "application/json");
            }
            catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* The real exception reaches the service log once; the body is the fixed message, never ex.Message. */
                DarlingWebFailureLog.Report(logger, Route, stopwatch.ElapsedMilliseconds, ex);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// The most names one answer carries. A server can hold tens of thousands of databases, and each one is a checkbox
    /// the picker builds every time it opens, so the list stops here and says so (<c>truncated</c>).
    /// </summary>
    internal const int MaxDatabases = 5000;

    /// <summary>
    /// The first <paramref name="cap"/> collected user databases of one server, ordered by name (the SQL's order), and
    /// whether more exist. The query asks for one row past the cap: that extra row is the proof of "more", and it is not
    /// returned.
    /// </summary>
    internal static async Task<(List<string> Names, bool Truncated)> ReadAsync(
        NpgsqlDataSource postgres, int serverId, int cap, CancellationToken cancellationToken)
    {
        var names = new List<string>();
        await using var command = postgres.CreateCommand(CollectedDatabases.NamesLimitedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = cap + 1 });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        var truncated = names.Count > cap;
        if (truncated)
        {
            names.RemoveRange(cap, names.Count - cap);
        }

        return (names, truncated);
    }

    /// <summary>The route's JSON body: <c>{"server": "...", "databases": [...], "truncated": false}</c>.</summary>
    internal static string Render(string serverName, IReadOnlyList<string> names, bool truncated) =>
        JsonSerializer.Serialize(new { server = serverName, databases = names, truncated });
}
