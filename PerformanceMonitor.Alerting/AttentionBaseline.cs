/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Alerting;

/// <summary>One calendar minute in which the attention collector had a successful run, with the attentions its count rows hold
/// (zero when the run counted nothing). The bucket of <c>tools/attention-rate-measure.sql</c> (#5736).</summary>
/// <param name="Minute">The minute (naive UTC, seconds zero).</param>
/// <param name="Attentions">The sum of the minute's <c>count</c> rows.</param>
public sealed record AttentionMinute(DateTime Minute, long Attentions);

/// <summary>One server's normal attention rate (#5736): the p99.9 of its 3-minute average attention rate over the trailing
/// <see cref="AttentionBaselineMath.LookbackDays"/> days, the <c>p999_3min_avg</c> column of <c>tools/attention-rate-measure.sql</c>.</summary>
/// <param name="P999ThreeMinuteAverage">The baseline, attentions per minute.</param>
/// <param name="MinutesObserved">Buckets (minutes with a successful run) the figure rests on.</param>
/// <param name="CapturedFrom">The first bucket's minute.</param>
/// <param name="CapturedTo">The last bucket's minute.</param>
public sealed record AttentionBaseline(double P999ThreeMinuteAverage, int MinutesObserved, DateTime CapturedFrom, DateTime CapturedTo);

/// <summary>The relative bars the Client Timeouts alert grades with (#5736): the server's baseline (null while it has under a
/// day of capture) and the two multiples. The floors are the settings' own Warning and Critical bars.</summary>
/// <param name="Baseline">The server's p99.9 3-minute average, or null when it has no baseline yet.</param>
/// <param name="WarningMultiple">Warning is at least this many times the baseline.</param>
/// <param name="CriticalMultiple">Critical is at least this many times the baseline.</param>
public sealed record AttentionRelativeBars(double? Baseline, int WarningMultiple, int CriticalMultiple);

/// <summary>
/// The baseline rule of the relative Client Timeouts bars (#5736), written once for both stores. The stores hand over the per-minute
/// buckets (<see cref="AttentionMinute"/>, the definition of <c>tools/attention-rate-measure.sql</c>); this class turns them into the
/// p99.9 of the 3-minute rolling average, exactly as that query's <c>p999_3min_avg</c>: a window is three CONSECUTIVE run minutes (a
/// hole breaks it), its value the mean of the three buckets, and the percentile is continuous (linear interpolation).
/// </summary>
public static class AttentionBaselineMath
{
    /// <summary>How far back the baseline reads, in days.</summary>
    public const int LookbackDays = 7;

    /// <summary>The capture a server needs before it has a baseline: the first bucket to the last must span this many days.</summary>
    public const int MinimumCaptureDays = 1;

    /// <summary>How long a computed baseline is kept before it is read again, in hours.</summary>
    public const int RefreshHours = 24;

    /// <summary>How long a failed baseline read waits before it is tried again, in minutes. Until then the floors alone grade.</summary>
    public const int RetryAfterFailureMinutes = 30;

    /// <summary>The percentile of the baseline.</summary>
    public const double Percentile = 0.999;

    /// <summary>Warning bar multiple, shipped default.</summary>
    public const int DefaultWarningMultiple = 3;

    /// <summary>Critical bar multiple, shipped default.</summary>
    public const int DefaultCriticalMultiple = 10;

    /// <summary>Both stores apply this on read: each multiple is at least 1 and Critical is never below Warning.</summary>
    public static (int Warning, int Critical) ClampMultiples(int warning, int critical)
    {
        var w = Math.Max(1, warning);
        return (w, Math.Max(w, critical));
    }

    /// <summary>
    /// The baseline of <paramref name="minutes"/>, or null when the server has under <see cref="MinimumCaptureDays"/> day of capture
    /// (first bucket to last) or no three-minute window exists.
    /// </summary>
    public static AttentionBaseline? Compute(IReadOnlyList<AttentionMinute> minutes)
    {
        if (minutes.Count < 3)
        {
            return null;
        }

        var ordered = minutes.OrderBy(m => m.Minute).ToList();
        var from = ordered[0].Minute;
        var to = ordered[^1].Minute;
        if (to - from < TimeSpan.FromDays(MinimumCaptureDays))
        {
            return null;
        }

        var averages = new List<double>(ordered.Count);
        for (var i = 2; i < ordered.Count; i++)
        {
            if (ordered[i].Minute - ordered[i - 2].Minute == TimeSpan.FromMinutes(2))
            {
                averages.Add((ordered[i - 2].Attentions + ordered[i - 1].Attentions + ordered[i].Attentions) / 3d);
            }
        }

        if (averages.Count == 0)
        {
            return null;
        }

        averages.Sort();
        var position = (averages.Count - 1) * Percentile;
        var low = (int)Math.Floor(position);
        var high = Math.Min(low + 1, averages.Count - 1);
        var value = averages[low] + ((averages[high] - averages[low]) * (position - low));
        return new AttentionBaseline(value, ordered.Count, from, to);
    }
}
