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
/// #4964: the collectors on one server that have already logged their missing-session line at Warning. The long-query,
/// deadlock and blocked-process collectors raise that line on every sweep while their Extended Events session cannot be
/// ensured, and each of those runs records SESSION_MISSING again, on purpose, so collection health keeps reading it.
/// What this changes is the level of the line: the first failing run logs it at Warning, and the runs after it log the
/// same line at Debug, until a run of that collector on that server succeeds.
///
/// <para>One instance per server (<c>ServerLoopState.XeSessionMissingWarnings</c>), keyed by collector name, so one
/// collector's run of failures never quiets another's first one. Kept in memory: a restart warns again. The sweep's
/// per-server body runs on a pool thread, so the map is a <see cref="ConcurrentDictionary{TKey, TValue}"/> and the
/// first-wins <c>TryAdd</c> is what decides which of two simultaneous failures is the first.</para>
/// </summary>
internal sealed class XeSessionMissingWarnings
{
    private readonly ConcurrentDictionary<string, bool> _warned = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// True when this failure is the collector's first since its last success, so the run logs its line at Warning.
    /// False for every failure after it, which logs the same line at Debug.
    /// </summary>
    public bool TryMarkWarned(string collectorName) => _warned.TryAdd(collectorName, true);

    /// <summary>
    /// Ends the collector's run of failures when one of its runs succeeds, so its next failure is a first one again.
    /// A collector that never failed has nothing to clear.
    /// </summary>
    public void Clear(string collectorName) => _warned.TryRemove(collectorName, out _);
}
