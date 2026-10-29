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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// Refreshes the front door's directory (#6840): once at start, then every <c>refreshSeconds</c>. It gates itself like the MCP and
/// web hosts, so it does nothing unless front-door mode is on and a store is configured.
/// </summary>
internal sealed class DarlingFrontDoorDirectoryService : BackgroundService
{
    private readonly DarlingFrontDoorDirectory _directory;
    private readonly ILogger _logger;

    public DarlingFrontDoorDirectoryService(
        DarlingFrontDoorDirectory directory, ILogger<DarlingFrontDoorDirectoryService>? logger = null)
    {
        _directory = directory;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_directory.Active)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _directory.RefreshAsync(stoppingToken).ConfigureAwait(false);
                await Task.Delay(_directory.RefreshInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                /* A refresh records each store's failure itself, so this is only a bug in the loop. Wait a full interval so it
                   cannot spin. */
                _logger.LogError(ex, "Front door: the directory refresh failed.");
                try
                {
                    await Task.Delay(_directory.RefreshInterval, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }
}
