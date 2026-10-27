/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>Placeholder for part P2 (#6840): refreshes the front door's directory on a timer once it is built.</summary>
internal sealed class DarlingFrontDoorDirectoryService : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;
}
