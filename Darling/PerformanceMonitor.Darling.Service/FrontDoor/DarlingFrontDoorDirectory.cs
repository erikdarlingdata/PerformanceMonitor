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

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>Placeholder for part P2 (#6840): no stores and no routing yet. Replaced by the real directory.</summary>
internal sealed class DarlingFrontDoorDirectory : IFrontDoorDirectory
{
    public DarlingFrontDoorDirectory(FrontDoorConfig config)
    {
    }

    public IReadOnlyList<FrontDoorStoreState> Stores { get; } = Array.Empty<FrontDoorStoreState>();

    public FrontDoorStore? Find(string name) => null;

    public FrontDoorRoute ResolveServer(string serverName)
        => new FrontDoorRoute.Refused("The front door's directory is not built yet.");

    public Task RefreshOnMissAsync(CancellationToken ct) => Task.CompletedTask;

    public void Hold(string store, TimeSpan span, string why)
    {
    }
}
