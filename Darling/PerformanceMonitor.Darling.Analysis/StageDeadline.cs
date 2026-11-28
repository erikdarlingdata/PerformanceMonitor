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

/// <summary>
/// #9879: runs one read of an on-demand pass under a stage's time budget. The family stage
/// (<see cref="AnalysisContext.FamilyDeadline"/>), the anomaly detector dispatch
/// (<see cref="AnalysisContext.DetectorDeadline"/>) and the drill-down stage
/// (<see cref="AnalysisContext.DrillDownDeadline"/>) each give every read they run under a deadline to this, for
/// both engines: the PostgreSQL-target collector gives it its heavy families too (#9966).
/// </summary>
internal static class StageDeadline
{
    /// <summary>
    /// Runs <paramref name="detectors"/> one after another, in order, and appends their facts to
    /// <paramref name="anomalies"/>. It is the detector loop of both engines: the SQL Server detector's when it runs one
    /// detector at a time, and the PostgreSQL-target detector's always (#9966).
    ///
    /// <para>When the context carries a <see cref="AnalysisContext.DetectorDeadline"/> (an on-demand pass), each detector
    /// reads through a token that also ends at the deadline and keeps its facts aside until it finishes. One the deadline
    /// stops, or one not started when it has fired, adds no fact and no failure line and is named in
    /// <see cref="AnalysisContext.DetectorsLeftOutForTime"/> (<see cref="PgAnomalyDetector.DetectorName"/>). With no
    /// deadline (the scheduled pass) each detector is called with the shared list, on the pass's token.</para>
    /// </summary>
    internal static async Task RunDetectorsInTurnAsync(
        AnalysisContext context, List<Fact> anomalies, IReadOnlyList<Func<AnalysisContext, List<Fact>, Task>> detectors)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(anomalies);
        ArgumentNullException.ThrowIfNull(detectors);

        var deadline = context.DetectorDeadline;
        foreach (var detector in detectors)
        {
            if (!deadline.CanBeCanceled)
            {
                await detector(context, anomalies);
                continue;
            }

            /* A detector under the deadline keeps its facts aside until it finishes, so one the deadline stops leaves
               none behind. */
            var own = new List<Fact>();
            if (await RunAsync(context, () => detector(context, own), deadline))
                anomalies.AddRange(own);
            else
                context.DetectorsLeftOutForTime.Add(PgAnomalyDetector.DetectorName(detector));
        }
    }

    /// <summary>
    /// Runs <paramref name="read"/> through a token that ends at <paramref name="deadline"/> as well as at the pass's own
    /// token, for the read's async flow only (<see cref="AnalysisContext.UseTokenForThisFlow"/>). True when the read ran
    /// to its end. False when the deadline had fired before it started, in which case it is not started, or stopped it.
    /// Only a stop the deadline caused is taken here: the pass's token is checked first, so an abandoned pass still
    /// unwinds, and any other failure of the read gets out as it does with no deadline.
    /// </summary>
    internal static async Task<bool> RunAsync(AnalysisContext context, Func<Task> read, CancellationToken deadline)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(read);

        if (deadline.IsCancellationRequested)
            return false;

        var passToken = context.PassToken;
        using var both = CancellationTokenSource.CreateLinkedTokenSource(passToken, deadline);
        context.UseTokenForThisFlow(both.Token);
        try
        {
            await read();
            return true;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, passToken) && deadline.IsCancellationRequested
                                   && AnalysisShutdown.IsShutdownResidue(ex))
        {
            /* The shapes a read's own catch lets through once the token it reads is signalled. A read the budget stopped
               is not a failed read: the caller names it in the pass's left-out record and no failure is logged. */
            return false;
        }
        finally
        {
            context.UseTokenForThisFlow(null);
        }
    }

    /// <summary>
    /// Starts the anomaly read of an on-demand pass beside a collector's family reads (#9879, #9966), or returns null
    /// when the pass carries none or its window was not observed. The read runs on a flow of its own: its collection
    /// failures go to <see cref="AnomalyReadBesideFamilies.Failures"/>, so the pass's list holds only family failures
    /// until the pass has counted the families it could not read, and no family's deadline token reaches it. With a
    /// <paramref name="gate"/> the read holds one of its slots and carries the gate while it runs, so a detector that
    /// runs several of its reads at once takes their slots from it and not from a gate of its own. A collector whose
    /// families run one after another passes none. The collector waits for the task to settle
    /// (<see cref="SettleAsync"/>) before its own stage ends and does not rethrow its failure: the pass awaits the task
    /// where it runs the detector.
    /// </summary>
    internal static Task<List<Fact>>? StartAnomalyReadBeside(AnalysisContext context, SemaphoreSlim? gate)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.ObservedDurationMs <= 0 || context.AnomalyReadBesideFamilies is not { } beside)
            return null;

        return beside.Run = RunAnomalyReadBesideAsync(context, beside, gate);
    }

    private static async Task<List<Fact>> RunAnomalyReadBesideAsync(
        AnalysisContext context, AnomalyReadBesideFamilies read, SemaphoreSlim? gate)
    {
        if (gate is not null)
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
            gate?.Release();
        }
    }

    /// <summary>
    /// Waits for a started anomaly read to settle, failed or not, and completes at once for none. The read's failure
    /// stays in its task for the pass, which rethrows it where it awaits the task. It is looked at here, so a pass that
    /// a family's failure ends before that await leaves no fault unobserved. (A continuation, not a catch: this is not
    /// a read.)
    /// </summary>
    internal static Task SettleAsync(Task? anomalyRead) =>
        anomalyRead is null
            ? Task.CompletedTask
            : anomalyRead.ContinueWith(static settled => { _ = settled.Exception; }, TaskScheduler.Default);
}
