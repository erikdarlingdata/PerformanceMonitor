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

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Keeps the size-triggered database reset from deleting <c>monitor.duckdb</c> out from under a collection
/// that is still running (#2594).
///
/// <para><b>Why this is not the existing read/write lock.</b> <see cref="DuckDbInitializer.AcquireReadLock()"/>
/// is a <see cref="ReaderWriterLockSlim"/>, which is thread-affine and cannot be held across an <c>await</c> —
/// and a collection is almost entirely awaits. Holding it for a collection's lifetime would also block the
/// alert and mute stores, which take the write lock for their own reasons, for as long as the slowest
/// collector runs. Measured in the field: <c>index_object_stats</c> across thirteen databases took
/// <b>55 seconds</b> on one server, so that is the scale of stall it would introduce.</para>
///
/// <para><b>What actually went wrong.</b> Two things start collections and only one of them was sequenced.
/// <c>CollectionBackgroundService</c> runs collect-then-archive in a strict loop, so the reset cannot land
/// mid-cycle there. But <c>MainWindow.ConnectToServer</c> — opening a server tab — calls
/// <c>RunAllCollectorsForServerAsync</c> on a bare <c>Task.Run</c>, unsequenced against anything. When the
/// store crossed its 512 MB threshold while such a collection was in flight, the reset deleted and recreated
/// the database underneath it; the collection kept writing through connections that predated the reset and
/// its final <c>collection_log</c> insert failed with <c>Table with name collection_log does not exist</c>.
/// Nothing was lost on disk and a restart cleared it, which is exactly the signature of stale in-process
/// state rather than a damaged store.</para>
///
/// <para><b>The shape.</b> Collections are concurrent with each other and only excluded by a reset, so this
/// is a many-readers/one-writer gate built on <see cref="SemaphoreSlim"/> rather than a mutex: collections
/// do not serialise against one another, which would be a behaviour change nobody asked for.</para>
/// </summary>
public static class CollectionResetGate
{
    /// <summary>
    /// Held for the whole of a reset, and momentarily by a collection as it registers. A collection that
    /// arrives during a reset waits here rather than starting against a database about to be deleted.
    /// </summary>
    private static readonly SemaphoreSlim s_resetExclusive = new(1, 1);

    private static int s_collectionsInFlight;

    /// <summary>How long the reset waits for in-flight collections to finish before giving up for this tick.</summary>
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromMinutes(3);

    /// <summary>How often the drain wait re-checks. Short enough not to add meaningful latency to a reset.</summary>
    private static readonly TimeSpan DrainPollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>Collections currently registered. Exposed for tests and diagnostics.</summary>
    public static int CollectionsInFlight => Volatile.Read(ref s_collectionsInFlight);

    /// <summary>
    /// Registers a collection. Dispose the result when the collection is finished — including its
    /// <c>collection_log</c> write, which is the last thing it does and the write that failed in the field.
    /// </summary>
    public static async Task<IDisposable> BeginCollectionAsync(CancellationToken cancellationToken = default)
    {
        /* Taken and released immediately rather than held: it exists to make "a reset is running" a state a
           starting collection cannot slip past, not to serialise collections against each other. */
        await s_resetExclusive.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            Interlocked.Increment(ref s_collectionsInFlight);
        }
        finally
        {
            s_resetExclusive.Release();
        }

        return new CollectionScope();
    }

    /// <summary>
    /// Takes the gate for a destructive reset, waiting for in-flight collections to drain.
    ///
    /// <para>Returns null when a collection is still running after <see cref="DrainTimeout"/>. That is a
    /// deferral, not a failure: the reset is size-triggered, the store is a few megabytes over a soft
    /// threshold, and the next tick will try again. Deleting the file on time matters far less than not
    /// deleting it underneath a running collector.</para>
    /// </summary>
    public static async Task<IDisposable?> TryBeginResetAsync(CancellationToken cancellationToken = default)
    {
        if (!await s_resetExclusive.WaitAsync(TimeSpan.Zero, cancellationToken).ConfigureAwait(false))
        {
            /* Another reset holds it. Nothing to do — that one will finish the work this one wanted. */
            return null;
        }

        var deadline = DateTime.UtcNow + DrainTimeout;

        while (Volatile.Read(ref s_collectionsInFlight) > 0)
        {
            if (DateTime.UtcNow >= deadline || cancellationToken.IsCancellationRequested)
            {
                s_resetExclusive.Release();
                return null;
            }

            await Task.Delay(DrainPollInterval, cancellationToken).ConfigureAwait(false);
        }

        return new ResetScope();
    }

    /// <summary>Test seam: drops any state a failed test left behind.</summary>
    internal static void ResetForTests()
    {
        Volatile.Write(ref s_collectionsInFlight, 0);

        while (s_resetExclusive.CurrentCount == 0)
        {
            s_resetExclusive.Release();
        }
    }

    private sealed class CollectionScope : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            /* Idempotent: a double dispose would drive the counter negative and let a reset run while a
               collection is still writing, which is the whole failure this type exists to prevent. */
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Interlocked.Decrement(ref s_collectionsInFlight);
            }
        }
    }

    private sealed class ResetScope : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                s_resetExclusive.Release();
            }
        }
    }
}
