/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9535: the bounds on a deferred plan fetch (query_stats and procedure_stats). <c>sys.dm_exec_text_query_plan</c>
/// waits on the schema lock of an object its plan references, so one plan behind a schema modification lock holds a
/// whole fetch command until the command timeout, and every other plan in that command is lost with it.
///
/// <para>The keys go out in chunks of <see cref="ChunkKeys"/>, on a connection whose lock timeout is
/// <see cref="LockTimeoutMilliseconds"/>. A chunk that ends in a lock timeout or a command timeout is asked again one
/// key at a time, and a key that fails alone is skipped. The phase stops asking once its wall-clock budget is spent.
/// A skipped key and a key the budget did not reach are not cached, so the next run asks for them again.</para>
/// </summary>
internal static class BoundedPlanFetch
{
    /// <summary>
    /// The most keys in one fetch command. One waiting plan costs its chunk one lock timeout and then at most this
    /// many single-key commands, so the chunk is small enough that the retry stays inside the budget and large enough
    /// that a run with no waiting plan is a handful of round trips.
    /// </summary>
    internal const int ChunkKeys = 20;

    /// <summary>How long one fetch command waits on a lock before the engine raises error 1222.</summary>
    internal const int LockTimeoutMilliseconds = 1000;

    /// <summary>The command timeout of one fetch command, a chunk or a single key.</summary>
    internal const int CommandTimeoutSeconds = 5;

    /// <summary>The wall clock the fetch phase may spend before it stops asking for more keys.</summary>
    internal static readonly TimeSpan Budget = TimeSpan.FromSeconds(5);

    /// <summary>Sets the session's lock timeout for the fetch commands that follow on the connection.</summary>
    internal static readonly CollectorQuery SetLockTimeout =
        new("SET LOCK_TIMEOUT " + LockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture) + ";");

    /// <summary>Puts the session's lock timeout back to the engine default (wait without limit) after the phase.</summary>
    internal static readonly CollectorQuery RestoreLockTimeout = new("SET LOCK_TIMEOUT -1;");

    /// <summary>What one bounded phase did, in keys.</summary>
    internal sealed class Outcome
    {
        /// <summary>Keys a fetch command answered for (with a plan or without one).</summary>
        internal int Answered { get; set; }

        /// <summary>Keys whose single-key command ended in a lock timeout or a command timeout.</summary>
        internal int Skipped { get; set; }

        /// <summary>Keys the phase did not ask for because the budget was spent.</summary>
        internal int NotAttempted { get; set; }

        /// <summary>Chunks that were asked again one key at a time.</summary>
        internal int ChunkRetries { get; set; }
    }

    /// <summary>
    /// Fetches <paramref name="keys"/> in chunks and hands each answer to <paramref name="apply"/> under the key's
    /// position in <paramref name="keys"/>. <paramref name="fetch"/> answers by position in the list it is given.
    /// <paramref name="isBoundedFault"/> says which exceptions are a lock timeout or a command timeout; any other
    /// exception, and any exception once <paramref name="cancellationToken"/> is cancelled, propagates with the
    /// answers applied so far kept. <paramref name="elapsed"/> is the phase's clock. With a
    /// <paramref name="stallWatch"/>, every fetch command runs under it, so a command that stalls is sampled once.
    /// </summary>
    internal static async Task<Outcome> RunAsync<TKey, TResult>(
        IReadOnlyList<TKey> keys,
        Func<IReadOnlyList<TKey>, CancellationToken, Task<Dictionary<int, TResult>>> fetch,
        Action<int, TResult> apply,
        Func<Exception, bool> isBoundedFault,
        TimeSpan budget,
        Func<TimeSpan> elapsed,
        CancellationToken cancellationToken,
        PlanFetchStallWatch? stallWatch = null)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(fetch);
        ArgumentNullException.ThrowIfNull(apply);
        ArgumentNullException.ThrowIfNull(isBoundedFault);
        ArgumentNullException.ThrowIfNull(elapsed);

        var outcome = new Outcome();

        Task<Dictionary<int, TResult>> Ask(List<TKey> slice) =>
            stallWatch is null
                ? fetch(slice, cancellationToken)
                : stallWatch.WatchAsync(
                    () => fetch(slice, cancellationToken),
                    () => new StallProbeObservation((long)elapsed().TotalMilliseconds, outcome.Answered, -1, -1),
                    cancellationToken);

        for (var offset = 0; offset < keys.Count; offset += ChunkKeys)
        {
            if (elapsed() >= budget)
            {
                outcome.NotAttempted += keys.Count - offset;
                return outcome;
            }

            var count = Math.Min(ChunkKeys, keys.Count - offset);
            Dictionary<int, TResult>? answers = null;
            try
            {
                answers = await Ask(Slice(keys, offset, count));
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested && isBoundedFault(ex))
            {
                /* The chunk is asked again below, one key at a time. */
            }

            if (answers is not null)
            {
                Apply(answers, offset, apply, outcome);
                continue;
            }

            if (count == 1)
            {
                /* A chunk of one key is already the single-key command. */
                outcome.Skipped++;
                continue;
            }

            outcome.ChunkRetries++;
            for (var index = offset; index < offset + count; index++)
            {
                if (elapsed() >= budget)
                {
                    outcome.NotAttempted += keys.Count - index;
                    return outcome;
                }

                try
                {
                    var single = await Ask(Slice(keys, index, 1));
                    Apply(single, index, apply, outcome);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested && isBoundedFault(ex))
                {
                    outcome.Skipped++;
                }
            }
        }

        return outcome;
    }

    private static List<TKey> Slice<TKey>(IReadOnlyList<TKey> keys, int offset, int count)
    {
        var slice = new List<TKey>(count);
        for (var index = offset; index < offset + count; index++)
        {
            slice.Add(keys[index]);
        }

        return slice;
    }

    private static void Apply<TResult>(Dictionary<int, TResult> answers, int offset, Action<int, TResult> apply, Outcome outcome)
    {
        foreach (var (ord, answer) in answers)
        {
            apply(offset + ord, answer);
            outcome.Answered++;
        }
    }
}
