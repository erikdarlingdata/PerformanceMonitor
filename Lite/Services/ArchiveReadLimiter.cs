/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// A small, process-wide limit on how many EXPENSIVE archive reads run at once (#5377): the cache-miss reads
/// of the <c>v_{table}</c> archive views and the forced-plan alert's read of <c>v_query_store_stats</c>.
///
/// <para><b>Why.</b> One DuckDB engine serves the whole process, and a read over the Parquet-backed views
/// binds and scans the whole archive. A user measured five of them at once taking 41 to 63 s each, against 3 s
/// alone: the engine shares its threads between them, so each finishes later and every caller waits longer
/// than it would have in line. Two at a time keeps each read near its solo time and makes the rest queue
/// cheaply, as an <c>await</c> that holds no lock and no thread.</para>
///
/// <para><b>Lock order: the limiter FIRST, then the process-wide database read lock, and the read lock is
/// released first.</b> A read waits for a slot holding nothing. If it took the read lock first and waited here
/// holding it, a pending CHECKPOINT or archive writer (<c>ReaderWriterLockSlim</c> parks new readers behind a
/// waiting writer) could never get in, because the waiter that holds the read lock needs a slot that only a
/// read behind the writer can free: a deadlock. In this order a thread that holds the read lock never waits
/// for a slot, so no wait-for cycle can form, and a writer waits only for reads that are already running and
/// will finish. The slot is taken with a token, so a cancelled waiter leaves promptly; it releases nothing,
/// because it took nothing.</para>
///
/// <para><b>Not thread-affine.</b> A <see cref="SemaphoreSlim"/> slot may be released from any thread, so the
/// <c>await</c> that takes it can resume anywhere; the read lock is entered only AFTER it, on the thread that
/// then runs the read (the premise <c>DuckDbInitializer.LockReleaser</c> documents).</para>
/// </summary>
internal sealed class ArchiveReadLimiter
{
    /// <summary>The width of the process-wide limiter: two archive reads at a time.</summary>
    internal const int SharedWidth = 2;

    /// <summary>The one limiter every archive-view read and the forced-plan alert read share.</summary>
    internal static ArchiveReadLimiter Shared { get; } = new(SharedWidth);

    private readonly SemaphoreSlim _slots;

    internal ArchiveReadLimiter(int width)
    {
        Width = width;
        _slots = new SemaphoreSlim(width, width);
    }

    /// <summary>How many reads may hold a slot at once.</summary>
    internal int Width { get; }

    /// <summary>How many slots are free right now.</summary>
    internal int Available => _slots.CurrentCount;

    /// <summary>
    /// Waits for a slot. Throws <see cref="OperationCanceledException"/> when <paramref name="cancellationToken"/>
    /// fires first, having taken nothing. Dispose the result to give the slot back; disposing twice gives it
    /// back once.
    /// </summary>
    internal async Task<IDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Slot(_slots);
    }

    private sealed class Slot(SemaphoreSlim slots) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                slots.Release();
        }
    }
}
