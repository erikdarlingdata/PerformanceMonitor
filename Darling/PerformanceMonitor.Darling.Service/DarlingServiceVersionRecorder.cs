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
/// timeout) logs one warning and the service goes on, and <see cref="DarlingServiceVersionWriter"/> tries it again on every
/// sweep until it succeeds. It writes <c>config.service_version</c> and not
/// <c>config.config_service</c>, whose self-bump trigger would make every service reload its configuration.</para>
/// </summary>
internal static class DarlingServiceVersionRecorder
{
    /// <summary>Upserts this host's row. Returns true when the row was written.</summary>
    internal static async Task<bool> RecordAsync(
        NpgsqlDataSource postgres, ILogger logger, DateTime processStartedUtc, CancellationToken cancellationToken,
        bool retry = false)
    {
        return await RecordAsync(postgres, logger, Environment.MachineName, DarlingCliCommands.ProductVersion(), processStartedUtc, cancellationToken, retry)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The same write with the host and version given, so a test can start "a second service" on one machine. A failure
    /// logs a warning, or only a debug line when <paramref name="retry"/> says an earlier attempt already warned.
    /// </summary>
    internal static async Task<bool> RecordAsync(
        NpgsqlDataSource postgres, ILogger logger, string serviceHost, string productVersion, DateTime processStartedUtc,
        CancellationToken cancellationToken, bool retry = false)
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
            if (retry)
            {
                logger.LogDebug(ex, "The service version ({Version}) for host {Host} still could not be recorded in config.service_version",
                    productVersion, serviceHost);
            }
            else
            {
                logger.LogWarning(ex,
                    "Could not record the service version ({Version}) for host {Host} in config.service_version; the viewers will show it as unknown. " +
                    "The service tries it again on every sweep.",
                    productVersion, serviceHost);
            }

            return false;
        }
    }
}

/// <summary>
/// Holds the service-version write until it succeeds (#5655). The start tries once; a write that failed is tried again
/// by <see cref="TryAsync"/> on every sweep, as the password key's pin step is while it waits, so a failover between the
/// migration and the write cannot leave the viewers saying "unknown (older than 3.11)" about this service for the rest of the
/// process. The first failure logs a warning; later ones log at debug. A written version is not written again.
/// </summary>
internal sealed class DarlingServiceVersionWriter
{
    private readonly Func<bool, CancellationToken, Task<bool>> _write;
    private bool _written;
    private bool _failed;

    /// <param name="write">One attempt. Its argument is true once an earlier attempt failed; it returns true when the row was written.</param>
    internal DarlingServiceVersionWriter(Func<bool, CancellationToken, Task<bool>> write)
    {
        _write = write;
    }

    /// <summary>True until a write has succeeded.</summary>
    internal bool Pending => !_written;

    /// <summary>One attempt, unless the row is already written. Only a cancellation escapes.</summary>
    internal async Task TryAsync(CancellationToken cancellationToken)
    {
        if (_written)
        {
            return;
        }

        _written = await _write(_failed, cancellationToken).ConfigureAwait(false);
        _failed = !_written;
    }
}
