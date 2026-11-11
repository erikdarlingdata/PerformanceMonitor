/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #8533: the status bar's "Collection:" field while the collection loop is the thing to report: Running during a cycle, Paused,
/// the whole seconds since the last cycle ended, or Starting before the first one has. The age is worded to the second and the
/// loop ends a cycle once a minute, so the label repaints on a tick of its own, <see cref="AgeTick"/> apart, and whenever a cycle
/// starts or ends. A repaint tied to a 30 second timer would land on the same two points of every cycle and show the same two
/// numbers, and would leave "Running" up after the cycle it saw had ended. The loop's state, the clock, the tick and the cycle
/// signal are all handed in, so the cadence is tested without a window or a wait.
/// </summary>
internal sealed class CollectionStatusLabel
{
    /// <summary>How far apart the label's own tick repaints the age.</summary>
    internal static readonly TimeSpan AgeTick = TimeSpan.FromSeconds(1);

    private readonly Func<bool> _isCollecting;
    private readonly Func<bool> _isPaused;
    private readonly Func<DateTime?> _lastCollectionUtc;
    private readonly Func<DateTime> _utcNow;
    private readonly Action<string> _paint;

    /// <summary>
    /// Builds the label over the loop's three facts, a UTC clock and the paint, and subscribes its repaint to the tick and to the
    /// cycle signal: <paramref name="onAgeTick"/> and <paramref name="onCycleChanged"/> are each given the handler to raise.
    /// </summary>
    internal CollectionStatusLabel(
        Func<bool> isCollecting,
        Func<bool> isPaused,
        Func<DateTime?> lastCollectionUtc,
        Func<DateTime> utcNow,
        Action<string> paint,
        Action<Action> onAgeTick,
        Action<Action> onCycleChanged)
    {
        ArgumentNullException.ThrowIfNull(onAgeTick);
        ArgumentNullException.ThrowIfNull(onCycleChanged);
        _isCollecting = isCollecting ?? throw new ArgumentNullException(nameof(isCollecting));
        _isPaused = isPaused ?? throw new ArgumentNullException(nameof(isPaused));
        _lastCollectionUtc = lastCollectionUtc ?? throw new ArgumentNullException(nameof(lastCollectionUtc));
        _utcNow = utcNow ?? throw new ArgumentNullException(nameof(utcNow));
        _paint = paint ?? throw new ArgumentNullException(nameof(paint));
        onAgeTick(Repaint);
        onCycleChanged(Repaint);
    }

    /// <summary>Paints the field from the loop's state as it is now.</summary>
    internal void Repaint() => _paint(Text(_isCollecting(), _isPaused(), _lastCollectionUtc(), _utcNow()));

    /// <summary>The field's text for the loop's state at <paramref name="utcNow"/>.</summary>
    internal static string Text(bool isCollecting, bool isPaused, DateTime? lastCollectionUtc, DateTime utcNow)
    {
        if (isCollecting)
        {
            return "Collection: Running";
        }

        if (isPaused)
        {
            return "Collection: Paused";
        }

        if (lastCollectionUtc is not { } last)
        {
            return "Collection: Starting...";
        }

        var seconds = Math.Max(0, (utcNow - last).TotalSeconds);
        return string.Create(CultureInfo.InvariantCulture, $"Collection: {seconds:F0}s ago");
    }
}
