/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one definition of what a stored last-fired stamp means when it is AHEAD of the clock (#4732): a cooldown
/// or re-fire window that decides with <c>now - last &gt;= window</c> has to be able to read the stamp after
/// the wall clock steps backward.
///
/// <para>Left alone, a stamp written at T with the clock then stepped back by X reads as "fired X in the
/// future", and the check waits X plus the window before it says the problem again. A read-time
/// <c>min(last, now)</c> does not fix that: <c>max(now - last, 0) &gt;= window</c> is the same test, and a
/// stamp that stays ahead keeps counting from the step for as long as the step lasts. The stamp has to be
/// WRITTEN BACK. The first check that finds it ahead of now stores now in its place and counts the elapsed
/// time from there (0 at that check), so the next repeat is due one window after the first check that saw the
/// step. It is never due at once: a small step (an NTP correction of tens of milliseconds) must not re-send an
/// alert the operator was just told about.</para>
///
/// <para>A stamp at or before now is returned untouched, so a normal clock decides exactly as it always did.
/// A window of zero or less keeps its own meaning, because this type never sees the window: the caller's
/// comparison does.</para>
///
/// <para>Every stamp that can be seeded from stored state (an alert-history row, a delivery record) goes
/// through the same rule, at the point it is read for a decision: <see cref="TryGet"/> for a stamp held in a
/// dictionary, <see cref="Settle"/> for one held anywhere else.</para>
/// </summary>
public static class LastFiredStamp
{
    /// <summary>
    /// The stamp to count elapsed time from: <paramref name="lastUtc"/> itself, or <paramref name="nowUtc"/> when
    /// <paramref name="lastUtc"/> is ahead of it. For a stamp the caller keeps somewhere other than a dictionary;
    /// the caller writes the returned value back when it differs.
    /// </summary>
    public static DateTime Settle(DateTime lastUtc, DateTime nowUtc) => lastUtc > nowUtc ? nowUtc : lastUtc;

    /// <summary>
    /// Reads the stamp stored for <paramref name="key"/>, writing <paramref name="nowUtc"/> back in its place when
    /// the stored one is ahead of it (the clock stepped back since it was written). Returns false when there is
    /// no stamp; otherwise <paramref name="lastUtc"/> is the stamp to count elapsed time from.
    /// </summary>
    /// <param name="lastFiredUtc">The stamps, keyed the way the caller's cooldown is.</param>
    /// <param name="key">The cooldown's key.</param>
    /// <param name="nowUtc">The caller's clock reading for this check: the same one its comparison uses.</param>
    /// <param name="lastUtc">The stamp to count from, or the default when there is none.</param>
    public static bool TryGet<TKey>(
        IDictionary<TKey, DateTime> lastFiredUtc, TKey key, DateTime nowUtc, out DateTime lastUtc)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(lastFiredUtc);

        if (!lastFiredUtc.TryGetValue(key, out lastUtc))
        {
            return false;
        }

        if (lastUtc > nowUtc)
        {
            lastFiredUtc[key] = nowUtc;
            lastUtc = nowUtc;
        }

        return true;
    }
}
