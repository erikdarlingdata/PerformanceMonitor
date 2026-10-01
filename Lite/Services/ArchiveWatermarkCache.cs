/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// A thread-safe cache of what the <c>v_{table}</c> archive views return for a watermark read. The caller
/// reads the live table every cycle and combines it with the cached archive value (the greater maximum, the
/// lesser minimum), so rows the archive-and-reset moved into Parquet still count.
///
/// <para><b>Why it exists.</b> Without it every cycle would scan the Parquet files. The view's answer
/// cannot change within a generation except by live rows being added, which the live read sees on its own,
/// because Parquet files only change when <see cref="Database.DuckDbInitializer"/> rebuilds the views.</para>
///
/// <para><b>Bounded.</b> A key names the read (table, column, server, database), never a moving bound such
/// as a collection-time floor, so there is one entry per key and each is overwritten in place.</para>
///
/// <para><b>The generation rule.</b> Each entry stores the
/// <see cref="Database.DuckDbInitializer.ArchiveViewGeneration"/> it was read in, and is reused only while
/// the current generation is equal. The generation is sampled BEFORE the view is queried, so a rebuild that
/// lands mid-read leaves a stale-tagged entry that the next call re-reads rather than trusts. A null value
/// is a valid cached answer ("the archive holds nothing for this key").</para>
///
/// <para>One instance lives on each <c>RemoteCollectorService</c>, which the app creates once.</para>
/// </summary>
internal sealed class ArchiveWatermarkCache
{
    private readonly ConcurrentDictionary<string, (long Generation, object? Value)> _entries =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Returns the cached view result for <paramref name="key"/> when it was read in
    /// <paramref name="generation"/>; otherwise runs <paramref name="readView"/>, stores its result
    /// against <paramref name="generation"/> and returns it. An exception from <paramref name="readView"/>
    /// propagates and stores nothing.
    /// </summary>
    internal async Task<object?> GetOrReadAsync(string key, long generation, Func<Task<object?>> readView)
    {
        if (_entries.TryGetValue(key, out var entry) && entry.Generation == generation)
            return entry.Value;

        var value = await readView();
        _entries[key] = (generation, value);
        return value;
    }
}
