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

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The DuckDB database lock (<c>DuckDbInitializer.s_dbLock</c>) is a static, thread-affine
/// <see cref="System.Threading.ReaderWriterLockSlim"/> (#2463). A test that takes it, awaits, and disposes it
/// after the await can resume on a different thread: the dispose then throws, the hold is never released, and
/// because the lock is static every later DuckDB test in the process waits on it forever (#7256).
/// A test that must hold the lock across a wait runs the whole hold on one dedicated thread with this helper,
/// takes and releases the lock there synchronously, and awaits only the helper's task from the test.
/// </summary>
internal static class DuckDbLockThread
{
    /// <summary>
    /// Runs <paramref name="work"/> on a new dedicated thread and returns its result. The lock is taken and
    /// released inside <paramref name="work"/>, so both happen on that one thread. Any wait inside must be
    /// bounded, so a regression fails the test instead of hanging the run.
    /// </summary>
    internal static Task<T> RunAsync<T>(Func<T> work)
    {
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                completion.SetResult(work());
            }
            catch (Exception ex)
            {
                completion.SetException(ex);
            }
        })
        {
            IsBackground = true,
            Name = "DuckDbLockThread",
        };
        thread.Start();
        return completion.Task;
    }
}
