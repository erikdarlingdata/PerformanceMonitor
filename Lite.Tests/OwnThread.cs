using System;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Runs a store-lock hold on ONE dedicated thread, start to finish. <c>DuckDbInitializer.s_dbLock</c> is a static
/// <see cref="ReaderWriterLockSlim"/>, which has thread affinity: entering it on one pool thread and disposing the
/// release on whichever thread resumes an <c>await</c> can throw <see cref="SynchronizationLockException"/>, and when it
/// does the lock is never released, which wedges every later test in the process (#5650). A test that takes a lock and
/// releases it must do both inside the delegate it gives <see cref="Run{T}"/>, without awaiting in between.
/// </summary>
internal static class OwnThread
{
    internal static T Run<T>(Func<T> work)
    {
        T result = default!;
        ExceptionDispatchInfo? failure = null;
        var thread = new Thread(() =>
        {
            try
            {
                result = work();
            }
            catch (Exception ex)
            {
                failure = ExceptionDispatchInfo.Capture(ex);
            }
        })
        { IsBackground = true, Name = "OwnThread lock hold" };
        thread.Start();
        thread.Join();
        failure?.Throw();
        return result;
    }

    internal static void Run(Action work) => Run<object?>(() =>
    {
        work();
        return null;
    });
}
