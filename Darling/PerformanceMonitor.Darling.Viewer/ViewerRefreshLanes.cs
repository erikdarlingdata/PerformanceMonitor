/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8549: the shell's overlap guard for "refresh the tab on screen", in two lanes: the fleet tabs (Overview, Alerts, FinOps and the rest)
/// and the per-server tabs. The shell used to have one slot for all of them. A server tab opened while a fleet tab's load was running
/// (the Overview's first read at startup, or its own timer's refresh) found the slot taken, left a request and returned, and its reads
/// started only when the fleet load finished: 7.9 s of one first open in the field, 0.8 to 1.7 s of three later ones. A per-server tab
/// has its own overlap guard, so it needs the shell's slot only to keep two server tabs from loading at once, as before. The lanes
/// keep the old rule inside each lane: a trigger that arrives mid-load sets a request, and the running loop loads once more. A
/// trigger for one lane never waits on the other.
/// </summary>
internal sealed class ViewerRefreshLanes
{
    private bool _fleetInFlight;
    private bool _fleetRequested;
    private bool _serverInFlight;
    private bool _serverRequested;

    /// <summary>
    /// Runs <paramref name="load"/> for the tab on screen unless a load is already running in the same lane, in which case the running
    /// loop is asked to go round again and this returns at once. <paramref name="serverTabSelected"/> is read at the start of every pass:
    /// a pass that finds the other lane's tab on screen does nothing, because that tab's own trigger already started or queued its load
    /// in its own lane.
    /// </summary>
    public async Task RunAsync(Func<bool> serverTabSelected, Func<Task> load)
    {
        var server = serverTabSelected();
        if (InFlight(server))
        {
            SetRequested(server, true);
            return;
        }

        SetInFlight(server, true);
        try
        {
            do
            {
                SetRequested(server, false);
                if (serverTabSelected() == server)
                {
                    await load();
                }
            }
            while (Requested(server));
        }
        finally
        {
            SetInFlight(server, false);
        }
    }

    private bool InFlight(bool server) => server ? _serverInFlight : _fleetInFlight;

    private bool Requested(bool server) => server ? _serverRequested : _fleetRequested;

    private void SetInFlight(bool server, bool value)
    {
        if (server)
        {
            _serverInFlight = value;
        }
        else
        {
            _fleetInFlight = value;
        }
    }

    private void SetRequested(bool server, bool value)
    {
        if (server)
        {
            _serverRequested = value;
        }
        else
        {
            _fleetRequested = value;
        }
    }
}
