/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /// <summary>
    /// The most family reads one pass runs at once (#9149). Each read opens its own pooled connection, so this is also
    /// the most connections one on-demand pass holds. The service's pool is 24 wide and a pass that asks for more than
    /// this gets this.
    /// </summary>
    internal const int MaxFamilyParallelism = 4;

    /// <summary>
    /// Runs the family reads after the coverage stamp, and appends their facts to <paramref name="facts"/> in the order
    /// the families are listed, whatever order they finish in (#9149). <c>analyze_server</c> with an anchor ran about 146
    /// statements one after another, none of them slow alone, so against a store a network away the wait was the sum of
    /// the round trips. The families do not read each other's facts: each only adds its own, and the context state they
    /// write that a later step reads (the PLAN_REGRESSION candidates, the plan-advisory plans) is read by the drill-down
    /// after collection ends.
    ///
    /// <para>With <see cref="AnalysisContext.FamilyParallelism"/> at 1 this is the loop it replaced, byte for byte: each
    /// family runs against the shared list in turn. Above 1, each family gets its own list and its own failure buffer,
    /// which are merged in family order. If a family throws (only a stop from the pass's own token gets out of one), the
    /// pass waits for the families already running, then rethrows the failure of the earliest family in the list, as the
    /// serial loop would have, and starts no family it has not started yet.</para>
    /// </summary>
    internal static async Task RunFamiliesAsync(
        AnalysisContext context,
        List<Fact> facts,
        IReadOnlyList<Func<AnalysisContext, List<Fact>, Task>> families)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(facts);
        ArgumentNullException.ThrowIfNull(families);

        var parallelism = Math.Clamp(context.FamilyParallelism, 1, MaxFamilyParallelism);
        if (parallelism == 1 || families.Count < 2)
        {
            foreach (var family in families)
                await family(context, facts);
            return;
        }

        var lists = new List<Fact>?[families.Count];
        var failures = new List<CollectionFailure>?[families.Count];
        var tasks = new Task[families.Count];
        var stopStarting = 0;
        using var gate = new SemaphoreSlim(parallelism);

        for (var i = 0; i < families.Count; i++)
            tasks[i] = RunOne(i);

        /* Waits for every family to settle, failed or not. The earliest failure in family order is rethrown below, once the
           failures of the families before it are kept. (A continuation, not a catch: every catch in this file family is a
           classified one, and this is not a read.) */
        await Task.WhenAll(tasks).ContinueWith(static _ => { }, TaskScheduler.Default);

        var firstFailed = Array.FindIndex(tasks, t => t.IsFaulted || t.IsCanceled);
        var merged = firstFailed < 0 ? families.Count : firstFailed;
        for (var i = 0; i < merged; i++)
        {
            if (lists[i] is not { } familyFacts)
                continue; /* never started: a family after the failing one that the gate had not yet released */

            facts.AddRange(familyFacts);
            context.CollectionFailures.AddRange(failures[i]!);
        }

        if (firstFailed >= 0)
            await tasks[firstFailed];

        async Task RunOne(int index)
        {
            await gate.WaitAsync(CancellationToken.None);
            var finished = false;
            try
            {
                if (Volatile.Read(ref stopStarting) != 0)
                {
                    finished = true;
                    return;
                }

                lists[index] = [];
                failures[index] = context.BufferCollectionFailuresForThisFlow();
                await families[index](context, lists[index]!);
                finished = true;
            }
            finally
            {
                if (!finished)
                    Volatile.Write(ref stopStarting, 1);
                gate.Release();
            }
        }
    }
}
