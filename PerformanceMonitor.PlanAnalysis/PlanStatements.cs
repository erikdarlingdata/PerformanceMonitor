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
    /// <para>Defined in terms of <see cref="EnumerateAllWithContainer(IReadOnlyList{PlanStatement})"/>
    /// (#4535, mirroring erikdarlingdata/PerformanceStudio dev (85492a1)
    /// <c>src/PlanViewer.Core/Services/PlanStatements.cs:38-45</c>) so the plain and the
    /// context-carrying enumeration are literally the same walk — a consumer picking either one
    /// gets the same statements in the same order.</para>
    /// </summary>
    public static IEnumerable<PlanStatement> EnumerateAll(IReadOnlyList<PlanStatement> statements)
    {
        foreach (var entry in EnumerateAllWithContainer(statements))
            yield return entry.Statement;
    }

    /// <summary>
    /// Every statement in the plan with the name of the module whose body it came from, for
    /// consumers that show statements to a person rather than just walking them (#4535). Ported
    /// from erikdarlingdata/PerformanceStudio dev (85492a1)
    /// <c>src/PlanViewer.Core/Services/PlanStatements.cs:59-72</c>.
    /// </summary>
    public static IEnumerable<StatementWithContainer> EnumerateAllWithContainer(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var entry in EnumerateAllWithContainer(batch.Statements))
                yield return entry;
        }
    }

    /// <summary>
    /// <paramref name="statements"/> and everything nested beneath them, each paired with the
    /// module path it lives in (null for the outer batch).
    ///
    /// <para>An explicit stack rather than recursion, because procedure bodies nest — a
    /// procedure calling a procedure calling a function — and unbounded recursion over
    /// caller-supplied plan XML is exactly the shape #4512's depth guard exists to bound.</para>
    /// Ported from erikdarlingdata/PerformanceStudio dev (85492a1)
    /// <c>src/PlanViewer.Core/Services/PlanStatements.cs:76-92</c>.
    /// </summary>
    public static IEnumerable<StatementWithContainer> EnumerateAllWithContainer(IReadOnlyList<PlanStatement> statements)
    {
        var pending = new Stack<StatementWithContainer>();
        for (var i = statements.Count - 1; i >= 0; i--)
            pending.Push(new StatementWithContainer(statements[i], ContainerPath: null));

        while (pending.TryPop(out var entry))
        {
            yield return entry;

            /* Pushed in reverse so the bodies come back out in source order, and pushed AFTER
               the statement is yielded so a body follows the EXEC that owns it rather than
               preceding it. */
            var statement = entry.Statement;
            for (var i = statement.UdfPlans.Count - 1; i >= 0; i--)
                PushAll(statement.UdfPlans[i], entry.ContainerPath, pending);

            if (statement.StoredProcPlan is not null)
                PushAll(statement.StoredProcPlan, entry.ContainerPath, pending);
        }
    }

    private static void PushAll(FunctionPlanInfo body, string? outerPath, Stack<StatementWithContainer> pending)
    {
        var path = AppendModule(outerPath, body.ProcName);
        for (var i = body.Statements.Count - 1; i >= 0; i--)
            pending.Push(new StatementWithContainer(body.Statements[i], path));
    }

    /// <summary>
    /// Chains module names for nesting, so a statement two bodies deep reads
    /// "dbo.Outer &gt; dbo.Inner" rather than pretending it sits directly in dbo.Inner's caller.
    /// A module the plan left unnamed contributes nothing to the path — the traversal reports
    /// what the plan said, not a placeholder — so its statements inherit the enclosing path (or
    /// none).
    /// </summary>
    private static string? AppendModule(string? outerPath, string? procName)
    {
        if (string.IsNullOrEmpty(procName))
            return outerPath;
        return outerPath is null ? procName : outerPath + " > " + procName;
    }
}

/// <summary>
/// One statement from <see cref="PlanStatements.EnumerateAllWithContainer(ParsedPlan)"/>:
/// the statement itself, and the proc/UDF module path it came from — null for a statement in
/// the outer batch, "dbo.Proc" for a procedure body, "dbo.Outer &gt; dbo.Inner" for nested
/// bodies.
/// </summary>
public readonly record struct StatementWithContainer(PlanStatement Statement, string? ContainerPath);
