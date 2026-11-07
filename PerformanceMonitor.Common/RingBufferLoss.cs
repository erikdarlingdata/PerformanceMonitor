/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>One read of a ring-buffer session. <c>Total</c> and <c>Held</c> are null on a gated run; <c>Created</c> and <c>Dropped</c> are null when the session row is missing; a <c>Limit</c> of 0 means no count limit.</summary>
public readonly record struct RingBufferReading(long? Total, long? Held, long? Dropped, DateTime? Created, DateTime ReadAtUtc, bool Truncated, long? Limit);

/// <summary>What the previous read left in collector_state.</summary>
public readonly record struct RingBufferPrior(long Total, long Held, long Dropped, DateTime Created, DateTime ReadAtUtc, bool Truncated);

/// <summary>The loss one read found against its prior, and the prior to stage for the next read.</summary>
public readonly record struct RingBufferLossResult(bool Unknown, long Evicted, long Dropped, bool AtLeast, bool MemoryCapped, long WindowSeconds, RingBufferPrior? NextPrior);

/// <summary>
/// #8146: the events a ring_buffer target evicted before a collector read them, and the events its session dropped
/// before they reached any target. Pure arithmetic over two reads of the target header and the session row.
/// </summary>
public static class RingBufferLoss
{
    /// <summary><c>create_time</c> jitters by a few ms between reads of one running session (datetime rounding), so it is never compared exactly.</summary>
    public static readonly TimeSpan RestartTolerance = TimeSpan.FromSeconds(1);

    /// <summary>The target's <c>max_events_limit</c> when the session's field list does not carry one.</summary>
    private const long DefaultLimit = 1000;

    public static RingBufferLossResult Compute(RingBufferPrior? prior, RingBufferReading reading)
    {
        if (reading.Created is not { } created)
        {
            return new RingBufferLossResult(false, 0, 0, false, false, 0, null);
        }

        var dropped = reading.Dropped ?? 0;

        if (prior is not { } p)
        {
            RingBufferPrior? baseline = reading.Total is { } t
                ? new RingBufferPrior(t, reading.Held ?? 0, dropped, created, reading.ReadAtUtc, reading.Truncated)
                : null;
            return new RingBufferLossResult(false, 0, 0, false, false, 0, baseline);
        }

        var window = Math.Max(0L, (long)(reading.ReadAtUtc - p.ReadAtUtc).TotalSeconds);
        var restarted = (created - p.Created).Duration() > RestartTolerance
            || (reading.Total is { } total && total < p.Total);
        if (restarted)
        {
            var rebased = new RingBufferPrior(reading.Total ?? 0, reading.Held ?? 0, dropped, created, reading.ReadAtUtc, reading.Truncated);
            return new RingBufferLossResult(true, 0, 0, false, false, window, rebased);
        }

        long evicted = 0;
        var memoryCapped = false;
        if (reading.Total is { } now)
        {
            var held = reading.Held ?? 0;
            var delta = now - p.Total;
            if (delta > held)
            {
                evicted = delta - held;
                var limit = reading.Limit ?? DefaultLimit;
                memoryCapped = limit == 0 || held < limit;
            }
        }

        var lost = Math.Max(0L, dropped - p.Dropped);
        var next = new RingBufferPrior(
            reading.Total ?? p.Total,
            reading.Held ?? p.Held,
            dropped,
            created,
            reading.ReadAtUtc,
            reading.Total.HasValue ? reading.Truncated : p.Truncated);
        return new RingBufferLossResult(false, evicted, lost, evicted > 0 && p.Truncated, memoryCapped, window, next);
    }

    public static string ToStateValue(RingBufferPrior prior) =>
        string.Join(
            "|",
            prior.Total.ToString(CultureInfo.InvariantCulture),
            prior.Held.ToString(CultureInfo.InvariantCulture),
            prior.Dropped.ToString(CultureInfo.InvariantCulture),
            prior.Created.ToString("o", CultureInfo.InvariantCulture),
            prior.ReadAtUtc.ToString("o", CultureInfo.InvariantCulture),
            prior.Truncated ? "1" : "0");

    public static RingBufferPrior? TryParseState(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var parts = value.Split('|');
        if (parts.Length != 6
            || !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var total)
            || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var held)
            || !long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out var dropped)
            || !DateTime.TryParseExact(parts[3], "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var created)
            || !DateTime.TryParseExact(parts[4], "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var readAt)
            || (parts[5] != "0" && parts[5] != "1"))
        {
            return null;
        }

        return new RingBufferPrior(total, held, dropped, created, readAt, parts[5] == "1");
    }
}
