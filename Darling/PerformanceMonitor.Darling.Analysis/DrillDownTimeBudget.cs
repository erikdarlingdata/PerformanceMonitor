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
/// #9879: runs the drill-down of each finding of one pass, one finding after another, and stops at the pass's
/// <see cref="AnalysisContext.DrillDownDeadline"/> when it carries one. A drill-down reads the store for the detail under
/// a finding (top queries, plans, the rows a remediation is built from), and over a long window one finding's reads can
/// take longer than every stage before them.
///
/// <para>With a deadline (an on-demand pass), each drill-down reads through a token that also ends at the deadline. One
/// that has not finished when it fires is stopped and its finding keeps no drill-down: a drill-down stopped half way
/// would build a remediation from half the rows. A finding not reached by then is not drilled. Either one is named in
/// <see cref="AnalysisContext.DrillDownsLeftOutForTime"/> by its root fact key, and the finding itself stays in the
/// pass's answer. With no deadline (the scheduled pass) every drill-down runs to its end on the pass's token.</para>
/// </summary>
internal static class DrillDownTimeBudget
{
    internal static async Task RunAsync(
        List<AnalysisFinding> findings, AnalysisContext context, Func<AnalysisFinding, AnalysisContext, Task> enrichOne)
    {
        ArgumentNullException.ThrowIfNull(findings);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(enrichOne);

        var deadline = context.DrillDownDeadline;
        foreach (var finding in findings)
        {
            /* #2299: between findings is the natural abandon point. A drill-down's own catch does not swallow shutdown
               residue, so this throw (and any residue from a drill-down mid-read) unwinds the pass to the service's
               single Information line. */
            context.CancellationToken.ThrowIfCancellationRequested();

            if (!deadline.CanBeCanceled)
            {
                await enrichOne(finding, context);
                continue;
            }

            if (deadline.IsCancellationRequested)
            {
                RecordBudgetCut(finding);
                continue;
            }

            /* Only a stop the deadline caused is taken here: the pass's token is checked first, so an abandoned pass
               still unwinds. */
            var passToken = context.PassToken;
            using var both = CancellationTokenSource.CreateLinkedTokenSource(passToken, deadline);
            context.UseTokenForThisFlow(both.Token);
            try
            {
                await enrichOne(finding, context);
            }
            catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, passToken) && deadline.IsCancellationRequested
                                       && AnalysisShutdown.IsShutdownResidue(ex))
            {
                RecordBudgetCut(finding);
            }
            finally
            {
                context.UseTokenForThisFlow(null);
            }
        }

        /* The one writer of the left-out record. The reply's drill_downs_left_out is built from it, in finding order. */
        void RecordBudgetCut(AnalysisFinding finding)
        {
            finding.DrillDown = null;
            context.DrillDownsLeftOutForTime.Add(finding.RootFactKey);
        }
    }
}
