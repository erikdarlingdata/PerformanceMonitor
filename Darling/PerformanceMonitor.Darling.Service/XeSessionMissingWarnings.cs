/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #4964: the collectors on one server that have already logged their missing-session line at Warning, so the sweeps
/// after the first one log the same line at Debug until a run of that collector succeeds.
/// </summary>
internal sealed class XeSessionMissingWarnings
{
    private readonly ConcurrentDictionary<string, bool> _warned = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Stub: answers Warning every time.</summary>
    public bool TryMarkWarned(string collectorName) => true;

    /// <summary>Stub: clears nothing.</summary>
    public void Clear(string collectorName)
    {
    }
}
