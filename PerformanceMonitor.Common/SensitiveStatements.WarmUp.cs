/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// The waiting half of the start-up warm-up (#5478, round 1 of #5484). The warm-up itself lives in
/// <c>AlertStatementFilter.WarmUpAsync</c> (Alerting), which Common cannot see, so it registers its task here, beside
/// the code it warms. Every path that judges a large document (an alert, an MCP or web read) asks here whether the
/// warm-up is still running.
/// <para><b>One deadline.</b> The warm-up records when it started. Any caller waits at most until start + 3 s, so once
/// that moment has passed nobody waits: a hung or slow warm-up costs the process 3 s once, not 3 s per caller, and a
/// timed-out async wait followed by a sync wait does not wait twice.</para>
/// <para><b>Size gate.</b> The blocking wait applies only to a call that will judge at least
/// <see cref="WarmUpWaitGateChars"/> characters in total. Below that a cold walk fits the base judging budget, so a
/// small alert never waits, and a UI thread that fires a small alert is never held for the warm-up.</para>
/// </summary>
public static partial class SensitiveStatements
{
    /// <summary>The longest, counted from the moment the warm-up started, that anyone waits for it.</summary>
    internal static readonly TimeSpan WarmUpWaitLimit = TimeSpan.FromSeconds(3);

    /// <summary>
    /// The text a call must judge, in total, before it waits for a running warm-up: 256 KB. Below it a cold walk of
    /// the filter code fits inside the base budget (<see cref="ReadBudget"/>), so waiting would only delay the call.
    /// </summary>
    internal const int WarmUpWaitGateChars = 256 * 1024;

    /// <summary>The warm-up task and the moment it started, written together so a reader never pairs one warm-up's task
    /// with another's start.</summary>
    private sealed record WarmUpState(Task Task, long StartedTimestamp);

    private static WarmUpState? s_warmUpState;

    /// <summary>True on the warm-up's own async flow, so its own judging calls never wait on it. The flag is async-local:
    /// it follows the warm-up across its awaits and never reaches a caller's thread.</summary>
    private static readonly AsyncLocal<bool> s_insideWarmUp = new();

    /// <summary>
    /// Registers a warm-up and starts <paramref name="body"/> on the thread pool. The registration (task and start time)
    /// is written BEFORE the body can run, so nothing the body does can see an older warm-up. The task it returns always
    /// completes normally: a body that throws is swallowed, because a warm-up that fails changes nothing (the first real
    /// call does the same work).
    /// </summary>
    internal static Task StartWarmUp(Func<Task> body)
    {
        ArgumentNullException.ThrowIfNull(body);

        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        SetWarmUp(done.Task, Stopwatch.GetTimestamp());

        _ = Task.Run(async () =>
        {
            s_insideWarmUp.Value = true;
            try
            {
                await body().ConfigureAwait(false);
            }
#pragma warning disable CA1031 // a warm-up that fails changes nothing: the first real call does the same work
            catch (Exception)
#pragma warning restore CA1031
            {
            }
            finally
            {
                done.TrySetResult();
            }
        });

        return done.Task;
    }

    /// <summary>The registered warm-up task, or null. Tests read it to prove the registration comes first.</summary>
    internal static Task? RegisteredWarmUp => Volatile.Read(ref s_warmUpState)?.Task;

    /// <summary>Sets (or, with null, clears) the registered warm-up. Tests stage a fake with it.</summary>
    internal static void SetWarmUp(Task? warmUp, long startedTimestamp) =>
        Volatile.Write(ref s_warmUpState, warmUp is null ? null : new WarmUpState(warmUp, startedTimestamp));

    /// <summary>
    /// Blocks while a warm-up is running, when this call will judge <paramref name="judgedChars"/> characters in total
    /// and that is at least <see cref="WarmUpWaitGateChars"/>; never past the single deadline. For callers that already
    /// run on a pool thread (the MCP and web sweep) or that carry small text (an alert). Never throws.
    /// </summary>
    internal static void WaitForWarmUp(long judgedChars)
    {
        if (judgedChars < WarmUpWaitGateChars)
        {
            return;
        }

        var state = Volatile.Read(ref s_warmUpState);
        if (state is not null)
        {
            WaitForWarmUp(state.Task, state.StartedTimestamp, WarmUpWaitLimit);
        }
    }

    /// <summary>
    /// True when there is nothing to wait for or the warm-up finished; false when the deadline (start + limit) passed
    /// first or the wait failed. Nothing waits when there is no warm-up, it already finished, the caller is the warm-up,
    /// or the deadline has passed. Never throws.
    /// </summary>
    internal static bool WaitForWarmUp(Task? warmUp, long startedTimestamp, TimeSpan limit)
    {
        if (warmUp is null || warmUp.IsCompleted || s_insideWarmUp.Value)
        {
            return true;
        }

        var remaining = limit - Stopwatch.GetElapsedTime(startedTimestamp);
        if (remaining <= TimeSpan.Zero)
        {
            return false;
        }

        try
        {
            return warmUp.Wait(remaining);
        }
#pragma warning disable CA1031 // a failed wait changes nothing: the call is judged as it would be cold
        catch (Exception)
#pragma warning restore CA1031
        {
            return false;
        }
    }

    /// <summary>
    /// The async twin, for callers on a thread that must not block (the alert engine's fire path, which Lite drives from
    /// its UI thread). Completed when there is no warm-up, it finished, the caller is the warm-up, or the deadline has
    /// passed; otherwise it waits for the warm-up or the deadline, whichever comes first. Never throws.
    /// </summary>
    internal static Task WhenWarmAsync()
    {
        var state = Volatile.Read(ref s_warmUpState);
        return state is null ? Task.CompletedTask : WhenWarmAsync(state.Task, state.StartedTimestamp, WarmUpWaitLimit);
    }

    internal static Task WhenWarmAsync(Task? warmUp, long startedTimestamp, TimeSpan limit)
    {
        if (warmUp is null || warmUp.IsCompleted || s_insideWarmUp.Value)
        {
            return Task.CompletedTask;
        }

        var remaining = limit - Stopwatch.GetElapsedTime(startedTimestamp);
        return remaining <= TimeSpan.Zero ? Task.CompletedTask : WaitUpToAsync(warmUp, remaining);
    }

    private static async Task WaitUpToAsync(Task warmUp, TimeSpan remaining)
    {
        try
        {
            await warmUp.WaitAsync(remaining).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // a timeout or a failed warm-up changes nothing: the call is judged as it would be cold
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }
}
