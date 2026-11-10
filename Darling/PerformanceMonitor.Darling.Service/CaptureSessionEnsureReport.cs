/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8528: what the service's own ensure of the capture sessions (deadlock, blocked process, client timeouts) last found on one
/// server, read by the Capture Down self-alert beside the store's newest collector rows. Keyed by collector name.
/// </summary>
/// <param name="RestartedAwaitingRead">The collectors whose session the ensure found stopped or absent and started, and that have not
/// written a run row since. Their newest SESSION_MISSING row is from before the start, so it is not a capture that is down: the
/// self-alert holds its verdict for them until the collector reads again.</param>
/// <param name="Failures">The last ensure failure per collector: a permission denial, or a session the ensure could not start or
/// create, with what the server said. The self-alert's detail quotes it instead of guessing at a cause.</param>
public sealed record CaptureSessionEnsureReport(
    IReadOnlyCollection<string> RestartedAwaitingRead,
    IReadOnlyDictionary<string, DarlingXeSessions.XeEnsureFailure> Failures)
{
    /// <summary>A report with nothing in it: no session awaiting its read, no failure recorded.</summary>
    public static readonly CaptureSessionEnsureReport Empty = new(
        Array.Empty<string>(),
        new Dictionary<string, DarlingXeSessions.XeEnsureFailure>(StringComparer.OrdinalIgnoreCase));

    /// <summary>Whether the ensure started this collector's session and the collector has not read it since.</summary>
    public bool IsAwaitingRead(string collectorName) =>
        RestartedAwaitingRead.Contains(collectorName, StringComparer.OrdinalIgnoreCase);

    /// <summary>The last ensure failure for this collector, or null when the ensure left its session in place or never ran.</summary>
    public DarlingXeSessions.XeEnsureFailure? FailureOf(string collectorName) =>
        Failures.TryGetValue(collectorName, out var failure) ? failure : null;
}

/// <summary>
/// #8528: the capture collectors whose session the ensure found stopped or absent and started, until each one's next run writes
/// its row. One instance per server (<c>ServerLoopState.CaptureSessionRestarts</c>). The ensure and the run write it on the sweep's
/// pool thread; the fleet self-alert pass reads it from another, so the set is a <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// In memory only: after a service restart a standing SESSION_MISSING row is judged as it stands.
/// </summary>
internal sealed class CaptureSessionRestarts
{
    private readonly ConcurrentDictionary<string, bool> _awaitingRead = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The ensure started this collector's session; its newest row is from before the start.</summary>
    public void Mark(string collectorName) => _awaitingRead[collectorName] = true;

    /// <summary>The collector wrote a run row after the start, so the newest row is the read's own.</summary>
    public void Clear(string collectorName) => _awaitingRead.TryRemove(collectorName, out _);

    /// <summary>The collectors still awaiting their read, as the self-alert pass is handed them.</summary>
    public IReadOnlyCollection<string> Snapshot() => _awaitingRead.Keys.ToArray();
}
