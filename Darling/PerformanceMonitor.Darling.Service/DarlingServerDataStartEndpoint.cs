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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #7386: <c>GET /api/server-data-start?server=&lt;route key&gt;</c>, when one server was first collected, answered as
/// <c>{ "server": "...", "data_start_utc": "2026-11-03T08:56:24.123Z" }</c> (null when the server has no row yet). The server
/// page calls it once when it opens and keeps the answer for ten minutes, so a chart over a range longer than the server's
/// history starts its time axis where the data starts, as the desktop viewer's charts do.
///
/// <para><b>Why a route and not part of <c>/api/fleet</c>.</b> The fleet read is a roll-up across every server, and a query
/// per server does not belong in it. The statement is <see cref="ServerDataStart"/>, the one copy the desktop viewer runs too:
/// the oldest <c>wait_stats</c> row for a SQL Server target, the oldest <c>pg_database_stats</c> row for a PostgreSQL target
/// (the engine is the server's <c>engine_kind</c>). One row, read in time order, on the viewer-role pool.</para>
///
/// <para>The answer carries a server name and a time, no collected text. The server is resolved the way every other
/// server-scoped web read resolves it, so an unknown name gets the same 400 refusal. A failed read is the fixed failure body,
/// and the page treats any failure as "no claim", so a chart keeps the picked range.</para>
/// </summary>
internal static class DarlingServerDataStartEndpoint
{
    /// <summary>The route the server page calls.</summary>
    internal const string Route = "/api/server-data-start";

    /// <summary>The name the route is admitted and recorded under: it has no MCP twin.</summary>
    internal const string AdmissionRoute = "server_data_start";

    private const string EngineSql = "SELECT engine_kind FROM servers WHERE server_id = $1";

    /// <summary>Maps <see cref="Route"/>. Called once from <see cref="DarlingWebEndpoints.MapAll"/>.</summary>
    internal static void Map(WebApplication app, NpgsqlDataSource postgres, ILogger logger,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        app.MapGet(Route, async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(
                postgres, DarlingWebEndpoints.First(context, "server"), context.RequestAborted);
            if (error is not null)
            {
                return DarlingWebEndpoints.ToHttpResult(error, Route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            using var readScope = ReadScope.Open(logger);
            using var ticket = await DarlingWebEndpoints.AdmitWebRouteAsync(readAdmission, AdmissionRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } busy)
            {
                return DarlingWebEndpoints.BusyResult(busy);
            }

            using var run = new DarlingWebEndpoints.WebRouteRun(readLatencyRecorder, AdmissionRoute, ReadWindowBand.None, context, readScope.Scope);
            try
            {
                var start = await ReadAsync(postgres, resolved.ServerId, context.RequestAborted);
                run.Complete(ReadOutcome.Ok);
                return Results.Text(Render(resolved.ServerName, start), "application/json");
            }
            catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                /* A connect timeout raised for an already-cancelled token is the browser leaving, not a fault (#5678). */
                context.RequestAborted.ThrowIfCancellationRequested();

                /* The real exception reaches the service log once; the body is the fixed message, never ex.Message. */
                DarlingWebFailureLog.Report(logger, Route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The first time the server was collected, as UTC, or null when it has no row yet (or no engine claim for it
    /// and no SQL Server row). The engine picks the statement: a PostgreSQL target reads <c>pg_database_stats</c>.</summary>
    internal static async Task<DateTime?> ReadAsync(NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        string? engineKind;
        await using (var engine = postgres.CreateCommand(EngineSql))
        {
            engine.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            engine.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            engineKind = await engine.ExecuteScalarAsync(cancellationToken) as string;
        }

        await using var command = postgres.CreateCommand(
            MonitoredEngineKind.IsPostgres(engineKind) ? ServerDataStart.PostgresSql : ServerDataStart.SqlServerSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime first ? DateTime.SpecifyKind(first, DateTimeKind.Utc) : null;
    }

    /// <summary>The route's JSON body: <c>{"server": "...", "data_start_utc": "...Z" or null}</c>.</summary>
    internal static string Render(string serverName, DateTime? dataStartUtc) =>
        JsonSerializer.Serialize(new
        {
            server = serverName,
            data_start_utc = dataStartUtc is DateTime first
                ? DateTime.SpecifyKind(first, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)
                : null,
        });
}
