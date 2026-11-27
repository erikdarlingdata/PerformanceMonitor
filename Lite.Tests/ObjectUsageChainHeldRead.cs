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
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The store's READ lock held on its own thread until <see cref="ReleaseAsync"/> (#9596), so a test can show that a writer under test needs no
/// more than the read lock. A read lock must be released by the thread that took it, which is why the hold has a thread of its own. It lives
/// apart from the tests because <c>WriteLockBudgetTests</c> sums the waits of every file that takes the WRITE lock as a hold of that lock, and
/// this wait holds a read.
/// </summary>
internal sealed class ObjectUsageChainHeldRead : IDisposable
{
    private readonly ManualResetEventSlim _release = new(false);
    private readonly Task _holder;

    internal ObjectUsageChainHeldRead(DuckDbInitializer duckDb)
    {
        using var held = new ManualResetEventSlim(false);
        _holder = Task.Factory.StartNew(() =>
        {
            using (duckDb.AcquireReadLock())
            {
                held.Set();
                _release.Wait();
            }
        }, TaskCreationOptions.LongRunning);
        held.Wait();
    }

    /// <summary>Lets the holder thread drop the read lock and waits for it to finish.</summary>
    internal async Task ReleaseAsync()
    {
        _release.Set();
        await _holder;
    }

    public void Dispose() => _release.Dispose();
}
