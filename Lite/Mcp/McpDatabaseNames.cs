/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9065: Lite's read behind <see cref="McpDatabaseNameFilter"/>: the stored spellings of a database name on one server, from that
/// server's <c>database_states</c> lists (<see cref="LocalDataService.DatabaseNameCandidatesAsync"/>). Darling's twin is
/// <c>DarlingMcpDatabaseNames</c>.
///
/// <para>The server is resolved as every tool resolves it (<see cref="ServerResolver.ResolveOrError"/>); one that does not resolve
/// is left to the tool, which reports it. A server with no list (an Azure SQL Database, which writes none) has nothing to resolve
/// against. Lite's resolver has one way to resolve a server, so a tool that changes state resolves it the same way a read
/// does.</para>
/// </summary>
internal static class McpDatabaseNames
{
    /// <summary>The <see cref="McpDatabaseNameSource"/> the host registers: the store is the request's <see cref="LocalDataService"/>.</summary>
    public static async Task<McpDatabaseNameLookup?> CandidatesAsync(
        IServiceProvider? services, string? serverName, string databaseName, bool changesState, CancellationToken cancellationToken)
    {
        if (services?.GetService<ServerManager>() is not { } serverManager
            || services.GetService<LocalDataService>() is not { } dataService)
        {
            return null;
        }

        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, serverName);
        return error is null
            ? await dataService.DatabaseNameCandidatesAsync(resolved.ServerId, databaseName, nowUtc: null, cancellationToken)
            : null;
    }
}
