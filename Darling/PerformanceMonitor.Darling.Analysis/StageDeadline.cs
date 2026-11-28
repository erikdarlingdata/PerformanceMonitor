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
/// #9879: runs one read of an on-demand pass under a stage's time budget. The anomaly detector dispatch
/// (<see cref="AnalysisContext.DetectorDeadline"/>) and the drill-down stage
/// (<see cref="AnalysisContext.DrillDownDeadline"/>) of both engines each give every read they start to this, and so
/// does the PostgreSQL-target collector for its heavy families (<see cref="AnalysisContext.FamilyDeadline"/>, #9966).
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
}
