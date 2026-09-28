using System.Collections.Generic;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Walks every statement in a plan, including the ones nested inside a stored procedure or a
/// user-defined function body (#4514).
///
/// <para><b>Why this exists.</b> <see cref="ShowPlanParser"/> has always read a statement's
/// <see cref="PlanStatement.UdfPlans"/> and <see cref="PlanStatement.StoredProcPlan"/> — for an
/// <c>EXEC &lt;procedure&gt;</c> plan, that is where every statement actually lives, since the
/// EXEC statement itself carries no query plan of its own. Nothing downstream read those fields:
/// <see cref="PlanAnalyzer.Analyze"/>, <see cref="BenefitScorer.Score"/> and
/// <c>ShowPlanParser.ComputeOperatorCosts</c> all walked <c>batch.Statements</c> only, so a
/// procedure or function body got no findings, no benefit scores and no operator costs.</para>
/// </summary>
public static class PlanStatements
{
    /// <summary>
    /// Every statement in the plan, outermost first, each nested body following the statement
    /// that owns it.
    /// </summary>
    public static IEnumerable<PlanStatement> EnumerateAll(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var statement in EnumerateAll(batch.Statements))
                yield return statement;
        }
    }

    /// <summary>
    /// <paramref name="statements"/> and everything nested beneath them.
    ///
    /// <para>An explicit stack rather than recursion, because procedure bodies nest — a
    /// procedure calling a procedure calling a function — and unbounded recursion over
    /// caller-supplied plan XML is exactly the shape #4512's depth guard exists to bound.</para>
    /// </summary>
    public static IEnumerable<PlanStatement> EnumerateAll(IReadOnlyList<PlanStatement> statements)
    {
        var pending = new Stack<PlanStatement>();
        for (var i = statements.Count - 1; i >= 0; i--)
            pending.Push(statements[i]);

        while (pending.TryPop(out var statement))
        {
            yield return statement;

            /* Pushed in reverse so the bodies come back out in source order, and pushed AFTER
               the statement is yielded so a body follows the EXEC that owns it rather than
               preceding it. */
            for (var i = statement.UdfPlans.Count - 1; i >= 0; i--)
                PushAll(statement.UdfPlans[i].Statements, pending);

            if (statement.StoredProcPlan is not null)
                PushAll(statement.StoredProcPlan.Statements, pending);
        }
    }

    private static void PushAll(IReadOnlyList<PlanStatement> statements, Stack<PlanStatement> pending)
    {
        for (var i = statements.Count - 1; i >= 0; i--)
            pending.Push(statements[i]);
    }
}
