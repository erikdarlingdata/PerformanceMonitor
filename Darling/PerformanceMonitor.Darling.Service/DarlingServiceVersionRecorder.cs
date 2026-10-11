/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Records this service host's product version in the store at startup (#5655), so the viewers can show which version
/// the service runs instead of their own. The version is <see cref="DarlingCliCommands.ProductVersion"/>, the same
/// method <c>--version</c>, the diagnostics bundle and the MCP <c>get_collection_health</c> read use, so the four
/// surfaces cannot drift. The host is the machine name, as the password key's service row uses.
///
/// <para>Never throws and never delays the start: a write that fails (a store without the table, a refused connection, a
/// timeout) logs one warning and the service goes on. It writes <c>config.service_version</c> and not
/// <c>config.config_service</c>, whose self-bump trigger would make every service reload its configuration.</para>
/// </summary>
internal static class DarlingServiceVersionRecorder
{
    /// <summary>Upserts this host's row. Returns true when the row was written.</summary>
    internal static async Task<bool> RecordAsync(
        NpgsqlDataSource postgres, ILogger logger, DateTime processStartedUtc, CancellationToken cancellationToken)
    {
        return await RecordAsync(postgres, logger, Environment.MachineName, DarlingCliCommands.ProductVersion(), processStartedUtc, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>The same write with the host and version given, so a test can start "a second service" on one machine.</summary>
    internal static async Task<bool> RecordAsync(
        NpgsqlDataSource postgres, ILogger logger, string serviceHost, string productVersion, DateTime processStartedUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            await ServiceVersionTable.UpsertAsync(
                postgres, serviceHost, productVersion, processStartedUtc, ServiceCommandDeadlines.SerialLoopSeconds, cancellationToken)
                .ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "Could not record the service version ({Version}) for host {Host} in config.service_version; the viewers will show it as unknown",
                productVersion, serviceHost);
            return false;
        }
    }
}
