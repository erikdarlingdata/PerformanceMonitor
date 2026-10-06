/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
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
/// <para><b>Bounded.</b> A key names the read (table, column), never a moving bound such as a
/// collection-time floor, so there is one entry per key and each is overwritten in place. A read whose
/// answer depends on a slowly moving bound (the per-database floored read) carries that bound's bucket as
/// the entry's <c>variant</c> rather than in the key: a new bucket overwrites the entry instead of adding
/// one.</para>
///
/// <para><b>The generation rule.</b> Each entry stores the
/// <see cref="Database.DuckDbInitializer.ArchiveViewGeneration"/> it was read in, and is reused only while
/// the current generation is equal. The generation is sampled BEFORE the view is queried, so a rebuild that
/// lands mid-read leaves a stale-tagged entry that the next call re-reads rather than trusts. A null value
/// is a valid cached answer ("the archive holds nothing for this key"). A read that finishes after a newer
/// generation's entry landed never overwrites it.</para>
///
/// <para><b>Single flight (#5377).</b> Concurrent first callers for one key (one generation, one variant)
/// share ONE read: the first runs it and the rest await its result. Without that, a cold generation made
/// every per-database caller of a table start its own whole-archive scan at once. A joiner leaves on its own
/// token without disturbing the read; if the read itself is abandoned because ITS caller was cancelled, each
/// remaining joiner retries (one of them runs the read again) rather than inheriting that caller's
/// cancellation. A read that fails for any other reason fails every joiner with the same exception and
/// stores nothing, so the next call tries again. A caller that finds an OLDER generation's read still in
/// flight takes the flight over, so a newly bumped generation costs one read, not one per caller.</para>
///
/// <para>One instance lives on each <c>RemoteCollectorService</c>, which the app creates once.</para>
/// </summary>
internal sealed class ArchiveWatermarkCache
{
    private readonly ConcurrentDictionary<string, (long Generation, long Variant, object? Value)> _entries =
        new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, Flight> _flights = new(StringComparer.Ordinal);

    private sealed class Flight(long generation, long variant)
    {
        public long Generation { get; } = generation;
        public long Variant { get; } = variant;
        public TaskCompletionSource<object?> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// Returns the cached view result for <paramref name="key"/> when it was read in
    /// <paramref name="generation"/> (and with <paramref name="variant"/>); otherwise runs
    /// <paramref name="readView"/> once however many callers ask at the same time, stores its result and
    /// returns it. An exception from <paramref name="readView"/> propagates and stores nothing.
    /// <paramref name="cancellationToken"/> bounds only this caller's wait for a read another caller is
    /// running; <paramref name="readView"/> carries whatever token the running caller wants.
    /// </summary>
    internal async Task<object?> GetOrReadAsync(
        string key, long generation, Func<Task<object?>> readView,
        long variant = 0, CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_entries.TryGetValue(key, out var entry) && entry.Generation == generation && entry.Variant == variant)
                return entry.Value;

            var mine = new Flight(generation, variant);
            var flight = _flights.GetOrAdd(key, mine);
            if (ReferenceEquals(flight, mine))
                return await RunFlightAsync(key, generation, variant, mine, readView);

            if (flight.Generation < generation)
            {
                /* The running read belongs to an OLDER generation (its leader sampled before an archive pass bumped
                   it and may still be queued for a slot). Its answer is useless to this caller, and leaving it
                   registered would send every caller of the new generation to a whole-archive read of its own, the
                   per-database herd back right after each archive pass. Take the flight over: the first caller of
                   the new generation wins the swap and runs one read, the rest join it. The old leader keeps its
                   own joiners and removes only its own flight when it finishes. A lost swap goes round again. */
                if (_flights.TryUpdate(key, mine, flight))
                    return await RunFlightAsync(key, generation, variant, mine, readView);
                continue;
            }

            if (flight.Generation != generation || flight.Variant != variant)
            {
                /* A read for a newer generation or a different bucket is running for this key. It would answer a
                   different question, so this caller reads for itself and shares nothing. */
                var own = await readView();
                Store(key, generation, variant, own);
                return own;
            }

            try
            {
                return await flight.Done.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                /* The running caller was cancelled, not this one: go round again and read, or join whoever
                   does. */
            }
        }
    }

    private async Task<object?> RunFlightAsync(
        string key, long generation, long variant, Flight flight, Func<Task<object?>> readView)
    {
        try
        {
            var value = await readView();
            Store(key, generation, variant, value);
            _flights.TryRemove(new System.Collections.Generic.KeyValuePair<string, Flight>(key, flight));
            flight.Done.TrySetResult(value);
            return value;
        }
        catch (OperationCanceledException)
        {
            _flights.TryRemove(new System.Collections.Generic.KeyValuePair<string, Flight>(key, flight));
            flight.Done.TrySetCanceled();
            throw;
        }
        catch (Exception ex)
        {
            _flights.TryRemove(new System.Collections.Generic.KeyValuePair<string, Flight>(key, flight));
            flight.Done.TrySetException(ex);
            _ = flight.Done.Task.Exception; // observed here, so a flight nobody joined never reports unobserved
            throw;
        }
    }

    private void Store(string key, long generation, long variant, object? value) =>
        _entries.AddOrUpdate(
            key,
            (generation, variant, value),
            (_, old) => old.Generation > generation ? old : (generation, variant, value));
}
