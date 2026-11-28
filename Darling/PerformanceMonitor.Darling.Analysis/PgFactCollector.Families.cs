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
    /// The most family reads one pass runs at once (#9149), and the slots of the gate they run under. Each read opens
    /// its own pooled connection. #9879: the anomaly read an on-demand pass runs beside the families, and every detector
    /// it runs at once, holds one of the same slots and one connection, and the I/O detector holds a second connection
    /// while it computes its write-latency baseline on a cache miss. So one on-demand pass holds at most this many
    /// connections and one more at once. The service's pool is 24 wide and a pass that asks for more than this gets this.
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
    ///
    /// <para>#9879: when the context carries a <see cref="AnalysisContext.FamilyDeadline"/>, a family
    /// <paramref name="underDeadline"/> names (by default <see cref="RunsUnderTheFamilyDeadline"/>) reads through a token
    /// that also ends at the deadline. One the deadline stops, or one not yet started when it fires, adds no fact and no
    /// failure and is named in <see cref="AnalysisContext.FamiliesLeftOutForTime"/>, in family order. Every other family
    /// runs to its end. A stop from the pass's own token is still rethrown.</para>
    ///
    /// <para>#9879: when the context carries an <see cref="AnalysisContext.AnomalyReadBesideFamilies"/>, the families
    /// overlap and the coverage stamp found observed time in the window, the anomaly read runs beside the families. It
    /// starts ahead of the first family, so it holds one of the gate's slots from the start. The stage hands it the gate
    /// (<see cref="AnomalyReadBesideFamilies.FamilyGate"/>) for as long as it runs: the detector runs its first read on
    /// the slot the anomaly read holds and takes a slot from the same gate, behind the families already waiting, for
    /// each further read it runs at once (<c>PgAnomalyDetector.RunDetectorsAsync</c>). So the families and the detector's
    /// reads together never hold more than the gate's slots, and no family waits on a detector. It reads through the
    /// pass's token, never the deadline's, and records its
    /// failures to a list of its own. The stage waits for it to settle and leaves its facts, its failures and any
    /// exception with the pass, which places them after the families' as it does when the detector runs after
    /// collection. An unobserved window never starts it: the pass returns before the detector on one.</para>
    /// </summary>
    internal static async Task RunFamiliesAsync(
        AnalysisContext context,
        List<Fact> facts,
        IReadOnlyList<Func<AnalysisContext, List<Fact>, Task>> families,
        Predicate<Func<AnalysisContext, List<Fact>, Task>>? underDeadline = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(facts);
        ArgumentNullException.ThrowIfNull(families);

        var deadline = context.FamilyDeadline;
        underDeadline ??= RunsUnderTheFamilyDeadline;
        var leftOut = new bool[families.Count];

        var parallelism = Math.Clamp(context.FamilyParallelism, 1, MaxFamilyParallelism);
        if (parallelism == 1 || families.Count < 2)
        {
            for (var i = 0; i < families.Count; i++)
            {
                if (!deadline.CanBeCanceled || !underDeadline(families[i]))
                {
                    await families[i](context, facts);
                    continue;
                }

                /* A family under the deadline keeps its facts and failures aside until it finishes, so one the deadline stops
                   leaves neither behind. */
                var own = new List<Fact>();
                var ownFailures = context.BufferCollectionFailuresForThisFlow();
                try
                {
                    await RunUnderDeadline(i, own);
                }
                finally
                {
                    context.StopBufferingCollectionFailuresForThisFlow();
                }

                if (leftOut[i])
                {
                    context.FamiliesLeftOutForTime.Add(FamilyName(families[i]));
                    continue;
                }

                facts.AddRange(own);
                context.CollectionFailures.AddRange(ownFailures);
            }

            return;
        }

        var lists = new List<Fact>?[families.Count];
        var failures = new List<CollectionFailure>?[families.Count];
        var tasks = new Task[families.Count];
        var stopStarting = 0;
        using var gate = new SemaphoreSlim(parallelism);

        /* #9879: the anomaly read goes first. Every slot is free here, so it takes one before any family asks, where a
           read queued behind the families would start only as the last of them finished. */
        Task anomalyRead = Task.CompletedTask;
        if (context.ObservedDurationMs > 0 && context.AnomalyReadBesideFamilies is { } beside)
            anomalyRead = beside.Run = RunAnomalyRead(beside);

        for (var i = 0; i < families.Count; i++)
            tasks[i] = RunOne(i);

        /* Waits for every family and the anomaly read to settle, failed or not. The earliest failure in family order is
           rethrown below, once the failures of the families before it are kept. The anomaly read's own failure stays in its
           task for the pass. (A continuation, not a catch: every catch in this file family is a classified one, and this is
           not a read.) */
        Task[] settling = [.. tasks, anomalyRead];
        await Task.WhenAll(settling).ContinueWith(static _ => { }, TaskScheduler.Default);

        var firstFailed = Array.FindIndex(tasks, t => t.IsFaulted || t.IsCanceled);
        var merged = firstFailed < 0 ? families.Count : firstFailed;
        for (var i = 0; i < merged; i++)
        {
            if (leftOut[i])
            {
                context.FamiliesLeftOutForTime.Add(FamilyName(families[i]));
                continue;
            }

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
                if (deadline.CanBeCanceled && underDeadline(families[index]))
                    await RunUnderDeadline(index, lists[index]!);
                else
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

        /* #9879: the anomaly read on a gate slot of its own. Its failures go to its own list, so the pass's list holds only
           family failures until the pass has counted the families it could not read. The flow it starts on is the stage's
           own, which no family's deadline token reaches. The read carries the gate while it runs, so a detector that runs
           several of its reads at once takes their slots from this gate and not from one of its own. */
        async Task<List<Fact>> RunAnomalyRead(AnomalyReadBesideFamilies read)
        {
            await gate.WaitAsync(CancellationToken.None);
            try
            {
                read.Failures = context.BufferCollectionFailuresForThisFlow();
                read.FamilyGate = gate;
                return await read.Read(context);
            }
            finally
            {
                read.FamilyGate = null;
                gate.Release();
            }
        }

        /* #9879: runs one family through a token that ends at the deadline as well as at the pass's own token. Only a stop
           the deadline caused is taken here: the pass's token is checked first, so an abandoned pass still unwinds. */
        async Task RunUnderDeadline(int index, List<Fact> familyFacts)
        {
            if (deadline.IsCancellationRequested)
            {
                RecordBudgetCut(index);
                return;
            }

            var passToken = context.PassToken;
            using var both = CancellationTokenSource.CreateLinkedTokenSource(passToken, deadline);
            context.UseTokenForThisFlow(both.Token);
            try
            {
                await families[index](context, familyFacts);
            }
            catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, passToken) && deadline.IsCancellationRequested
                                       && AnalysisShutdown.IsShutdownResidue(ex))
            {
                /* The same shapes a family's own catch lets through once the token it reads is signalled, so nothing the
                   deadline caused reaches the pass as a failure. The filter opens as every other catch in this file family
                   does: an abandonment of the pass itself is never taken here, so it unwinds the pass whether or not the
                   deadline has also fired. A family the budget stopped is not a failed read, so the catch names it in the
                   left-out record (RecordBudgetCut) and records no collection failure (#9879). */
                RecordBudgetCut(index);
            }
            finally
            {
                context.UseTokenForThisFlow(null);
            }
        }

        /* #9879: the one writer of the left-out record. The reply's families_left_out is built from it, in family order, once
           the families settle, so a family the deadline cut is named there and adds no fact and no failure. The only callers
           are the two places the deadline decides a family is cut: one not yet started when it fires, and one it stops. */
        void RecordBudgetCut(int index) => leftOut[index] = true;
    }

    /// <summary>
    /// The families an on-demand pass runs under its time budget (#9879): the four that read every query-history or
    /// procedure-history row of the window, which is what a long window on a busy server makes slow. Every other family
    /// reads a bounded set of rows and always runs.
    /// </summary>
    internal static bool RunsUnderTheFamilyDeadline(Func<AnalysisContext, List<Fact>, Task> family) =>
        family.Method.Name is nameof(CollectParameterSensitivityFactsAsync)
            or nameof(CollectPlanAdvisoryFactsAsync)
            or nameof(CollectBadActorFactsAsync)
            or nameof(CollectProcedureStatsFactsAsync);

    /// <summary>The family label a reply names a family by: the same one a collection failure of it carries.</summary>
    private static string FamilyName(Func<AnalysisContext, List<Fact>, Task> family) =>
        CollectionFailure.FamilyOf(family.Method.Name);
}
