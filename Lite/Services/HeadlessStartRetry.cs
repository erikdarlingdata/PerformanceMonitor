/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9184: a headless start whose store fails to open (a file another process holds, a store that is being repaired) used to
/// exit with code 3 and stay down, because Task Scheduler's restart-on-failure setting does not restart a task whose action ran
/// and returned a nonzero code. So the process retries the open itself, waiting 10 seconds, then twice as long each time up to
/// 5 minutes, for 30 minutes in all. Only when that window is spent does it hand the last error back, and the start ends with
/// exit code 3 as before.
/// </summary>
public static class HeadlessStartRetry
{
    public static readonly TimeSpan FirstDelay = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan Window = TimeSpan.FromMinutes(30);

    /// <summary>The wait between attempts and the clock the window is measured on. A test passes both, so none of it sleeps.</summary>
    public sealed record Timing(Func<TimeSpan, CancellationToken, Task> Delay, Func<TimeSpan> Elapsed)
    {
        public static Timing ForProduction()
        {
            var watch = Stopwatch.StartNew();
            return new Timing(Task.Delay, () => watch.Elapsed);
        }
    }

    /// <summary>
    /// Runs <paramref name="attempt"/> until it succeeds or the window is spent. A failed attempt logs one line with its number,
    /// its error and the next delay. The last delay is cut to what is left of the window, so the final attempt runs when the
    /// window ends. The attempt that ends the window logs that no retry is left, then a line saying Lite is giving up, and its
    /// exception is rethrown.
    /// </summary>
    /// <param name="attempt">One try at the step. A throw is a failed attempt.</param>
    /// <param name="beforeWait">Runs before each wait. The caller opens the stop listener here, so <c>--stop</c> reaches a
    /// start that is waiting.</param>
    /// <param name="afterWait">Runs after each wait, including one that ends because <paramref name="stopRequested"/> was
    /// cancelled, and finishes before the next attempt starts. The caller closes the stop listener here, because an attempt
    /// opens the store and a newer build that finds the listener open kills the process after 20 seconds. Each of the two
    /// callbacks must be safe to call when its work is already done.</param>
    /// <param name="stopRequested">Cancelled when <c>--stop</c> asks this instance to exit. It ends a wait at once and ends the
    /// retry without another attempt: <see cref="OperationCanceledException"/> is thrown.</param>
    public static async Task RunAsync(
        Func<Task> attempt, Timing timing, Action<string> log, Action beforeWait, Action afterWait, CancellationToken stopRequested)
    {
        var delay = FirstDelay;
        for (var number = 1; ; number++)
        {
            /* A --stop that landed during a wait or during the last attempt ends the retry before the next attempt opens the store. */
            stopRequested.ThrowIfCancellationRequested();
            try
            {
                await attempt();
                /* A --stop queued while the attempt held the UI thread runs its shutdown after this point, so an attempt that then
                   succeeds must not let the start go on against a store that shutdown has closed. */
                stopRequested.ThrowIfCancellationRequested();
                return;
            }
            catch (Exception ex)
            {
                stopRequested.ThrowIfCancellationRequested();

                var left = Window - timing.Elapsed();
                if (left <= TimeSpan.Zero)
                {
                    log($"Headless start attempt {number} failed. No retry is left. Error: {ex.Message}");
                    log($"Headless start is giving up after {number} attempts over {Format(Window)}. Lite is exiting.");
                    throw;
                }

                var wait = delay < left ? delay : left;
                log($"Headless start attempt {number} failed. Next attempt in {Format(wait)}. Error: {ex.Message}");
                beforeWait();
                try
                {
                    await timing.Delay(wait, stopRequested);
                }
                finally
                {
                    afterWait();
                }

                delay = delay + delay > MaxDelay ? MaxDelay : delay + delay;
            }
        }
    }

    private static string Format(TimeSpan span) =>
        span.TotalMinutes >= 1 && span.Seconds == 0 ? $"{span.TotalMinutes:0} min" : $"{span.TotalSeconds:0} s";
}
