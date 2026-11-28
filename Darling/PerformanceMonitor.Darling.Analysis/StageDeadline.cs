/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// #9879: runs one read of an on-demand pass under a stage's time budget. The family stage
/// (<see cref="AnalysisContext.FamilyDeadline"/>), the anomaly detector dispatch
/// (<see cref="AnalysisContext.DetectorDeadline"/>), the configuration change compare
/// (<see cref="AnalysisContext.ConfigCompareDeadline"/>) and the drill-down stage
/// (<see cref="AnalysisContext.DrillDownDeadline"/>) each give every read they run under a deadline to this.
/// </summary>
internal static class StageDeadline
{
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
