/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #7643: marks the collector runs an operator asked for by hand (<c>snapshot_now</c>, which the MCP tool <c>run_collectors_now</c>
/// sends) as against the scheduled sweep. The schema pass reads every database in full for such a run, so a change the change-tracking
/// session cannot see (no grant to create it) still shows up when the operator asks. Flows with the async call, so the dispatch deep in
/// the run sees it without a parameter on every collector.
/// </summary>
public static class DarlingSnapshotScope
{
    private static readonly AsyncLocal<bool> s_active = new();

    /// <summary>True inside <see cref="Enter"/>'s scope.</summary>
    public static bool IsActive => s_active.Value;

    /// <summary>Opens the scope; dispose it to leave. A scope inside a scope restores the outer state on disposal.</summary>
    public static IDisposable Enter()
    {
        var previous = s_active.Value;
        s_active.Value = true;
        return new Scope(previous);
    }

    private sealed class Scope : IDisposable
    {
        private readonly bool _previous;
        private bool _disposed;

        public Scope(bool previous) => _previous = previous;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            s_active.Value = _previous;
        }
    }
}
