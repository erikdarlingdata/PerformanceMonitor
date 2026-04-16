using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Second-pass analysis that calculates MaxBenefitPercent for each PlanWarning.
/// Runs after PlanAnalyzer.Analyze() — the analyzer creates findings, the scorer quantifies them.
/// Benefit = maximum % of elapsed time that could be saved by addressing the finding.
/// </summary>
public static class BenefitScorer
{
    // Warning types that map to specific scoring strategies
    private static readonly HashSet<string> OperatorTimeRules = new(StringComparer.OrdinalIgnoreCase)
    {
        "Filter Operator",      // Rule 1
        "Eager Index Spool",    // Rule 2
        "Spill",                // Rule 7
        // Key Lookup / RID Lookup (Rule 10) handled separately by ScoreKeyLookupWarning
        "Scan With Predicate",  // Rule 11
        "Non-SARGable Predicate", // Rule 12
        "Scan Cardinality Misestimate", // Rule 32
    };

    public static void Score(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var stmt in batch.Statements)
            {
                ScoreStatementWarnings(stmt);

                if (stmt.RootNode != null)
                    ScoreNodeTree(stmt.RootNode, stmt);

                if (stmt.WaitStats.Count > 0 && stmt.QueryTimeStats != null)
                    ScoreWaitStats(stmt);
            }
        }
    }

    private static void ScoreStatementWarnings(PlanStatement stmt)
    {
        var elapsedMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        foreach (var warning in stmt.PlanWarnings)
        {
            switch (warning.WarningType)
            {
                case "Ineffective Parallelism":   // Rule 25
                case "Parallel Wait Bottleneck":  // Rule 31
                    // These are meta-findings about parallelism efficiency.
                    // The benefit is the gap between actual and ideal elapsed time.
                    if (elapsedMs > 0 && stmt.QueryTimeStats != null)
                    {
                        var cpu = stmt.QueryTimeStats.CpuTimeMs;
                        var dop = stmt.DegreeOfParallelism;
                        if (dop > 1 && cpu > 0)
                        {
                            // Ideal elapsed = CPU / DOP. Benefit = (actual - ideal) / actual
                            var idealElapsed = (double)cpu / dop;
                            var benefit = Math.Max(0, (elapsedMs - idealElapsed) / elapsedMs * 100);
                            warning.MaxBenefitPercent = Math.Min(100, Math.Round(benefit, 1));
                        }
                    }
                    break;

                case "Serial Plan": // Rule 3
                    // Can't know how fast a parallel plan would be, but estimate:
                    // CPU-bound: benefit up to (1 - 1/maxDOP) * 100%
                    if (elapsedMs > 0 && stmt.QueryTimeStats != null)
                    {
                        var cpu = stmt.QueryTimeStats.CpuTimeMs;
                        // Assume server max DOP — use a conservative 4 if unknown
                        var potentialDop = 4;
                        if (cpu >= elapsedMs)
                        {
                            // CPU-bound: parallelism could help significantly
                            var benefit = (1.0 - 1.0 / potentialDop) * 100;
                            warning.MaxBenefitPercent = Math.Round(benefit, 1);
                        }
                        else
                        {
                            // Not CPU-bound: parallelism helps less
                            var cpuRatio = (double)cpu / elapsedMs;
                            var benefit = cpuRatio * (1.0 - 1.0 / potentialDop) * 100;
                            warning.MaxBenefitPercent = Math.Round(Math.Min(50, benefit), 1);
                        }
                    }
                    break;

                case "Memory Grant": // Rule 9
                    // Grant wait is the only part that affects this query's elapsed time
                    if (elapsedMs > 0 && stmt.MemoryGrant?.GrantWaitTimeMs > 0)
                    {
                        var benefit = (double)stmt.MemoryGrant.GrantWaitTimeMs / elapsedMs * 100;
                        warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
                    }
                    break;

                case "High Compile CPU": // Rule 19
                    if (elapsedMs > 0 && stmt.CompileCPUMs > 0)
                    {
                        var benefit = (double)stmt.CompileCPUMs / elapsedMs * 100;
                        warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
                    }
                    break;

                // Rules that cannot be quantified: leave MaxBenefitPercent as null
                // Rule 18 (Compile Memory Exceeded), Rule 20 (Local Variables),
                // Rule 27 (Optimize For Unknown)
            }
        }
    }

    private static void ScoreNodeTree(PlanNode node, PlanStatement stmt)
    {
        ScoreNodeWarnings(node, stmt);

        foreach (var child in node.Children)
            ScoreNodeTree(child, stmt);
    }

    private static void ScoreNodeWarnings(PlanNode node, PlanStatement stmt)
    {
        var elapsedMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        foreach (var warning in node.Warnings)
        {
            // Already scored (e.g., by a different pass)
            if (warning.MaxBenefitPercent != null)
                continue;

            if (warning.WarningType == "UDF Execution") // Rule 4
            {
                ScoreUdfWarning(warning, node, elapsedMs);
            }
            else if (warning.WarningType == "Filter Operator") // Rule 1
            {
                ScoreFilterWarning(warning, node, stmt);
            }
            else if (warning.WarningType == "Nested Loops High Executions") // Rule 16
            {
                ScoreNestedLoopsWarning(warning, node, stmt);
            }
            else if (warning.SpillDetails != null) // Rule 7
            {
                ScoreSpillWarning(warning, node, stmt);
            }
            else if (warning.WarningType is "Key Lookup" or "RID Lookup") // Rule 10
            {
                ScoreKeyLookupWarning(warning, node, stmt);
            }
            else if (OperatorTimeRules.Contains(warning.WarningType))
            {
                ScoreByOperatorTime(warning, node, stmt);
            }
            else if (warning.WarningType == "Row Estimate Mismatch") // Rule 5
            {
                ScoreEstimateMismatchWarning(warning, node, stmt);
            }
            // Rules that stay null: Scalar UDF (Rule 6, informational reference),
            // Parallel Skew (Rule 8 — will be integrated per-operator later),
            // Data Type Mismatch (Rule 13),
            // Lazy Spool Ineffective (Rule 14), Join OR Clause (Rule 15),
            // Many-to-Many Merge Join (Rule 17), CTE Multiple References (Rule 21),
            // Table Variable (Rule 22), Table-Valued Function (Rule 23),
            // Top Above Scan (Rule 24), Row Goal (Rule 26),
            // NOT IN with Nullable Column (Rule 28), Implicit Conversion (Rule 29),
            // Wide Index Suggestion (Rule 30), Estimated Plan CE Guess (Rule 33)
        }
    }

    /// <summary>
    /// Rule 4: UDF Execution — benefit is UDF elapsed time / statement elapsed.
    /// </summary>
    private static void ScoreUdfWarning(PlanWarning warning, PlanNode node, long stmtElapsedMs)
    {
        if (stmtElapsedMs > 0 && node.UdfElapsedTimeMs > 0)
        {
            var benefit = (double)node.UdfElapsedTimeMs / stmtElapsedMs * 100;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Rule 1: Filter Operator — benefit is child subtree elapsed / statement elapsed.
    /// The filter discards rows late; eliminating it means the child subtree work was unnecessary.
    /// </summary>
    private static void ScoreFilterWarning(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        if (node.HasActualStats && stmtMs > 0 && node.Children.Count > 0)
        {
            var childElapsed = node.Children.Max(c => c.ActualElapsedMs);
            var benefit = (double)childElapsed / stmtMs * 100;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
        else if (!node.HasActualStats && stmt.StatementSubTreeCost > 0 && node.Children.Count > 0)
        {
            // Estimated plan fallback: child subtree cost / statement cost
            var childCost = node.Children.Sum(c => c.EstimatedTotalSubtreeCost);
            var benefit = childCost / stmt.StatementSubTreeCost * 100;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Rule 16: Nested Loops High Executions — benefit is inner-side elapsed / statement elapsed.
    /// </summary>
    private static void ScoreNestedLoopsWarning(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        if (node.Children.Count >= 2)
        {
            var innerChild = node.Children[1];

            if (innerChild.HasActualStats && stmtMs > 0 && innerChild.ActualElapsedMs > 0)
            {
                var benefit = (double)innerChild.ActualElapsedMs / stmtMs * 100;
                warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
            }
            else if (!innerChild.HasActualStats && stmt.StatementSubTreeCost > 0)
            {
                var benefit = innerChild.EstimatedTotalSubtreeCost / stmt.StatementSubTreeCost * 100;
                warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
            }
        }
    }

    /// <summary>
    /// Rule 7: Spill — benefit is the spilling operator's self-time / statement elapsed.
    /// Exchange spills use the parallelism operator time (unreliable but best we have).
    /// </summary>
    private static void ScoreSpillWarning(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;
        if (stmtMs <= 0) return;

        long operatorMs;
        if (warning.SpillDetails?.SpillType == "Exchange")
            operatorMs = GetParallelismOperatorElapsedMs(node);
        else
            operatorMs = PlanAnalyzer.GetOperatorOwnElapsedMs(node);

        if (operatorMs > 0)
        {
            var benefit = (double)operatorMs / stmtMs * 100;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Generic operator-time scoring for rules where the fix would eliminate or
    /// drastically reduce the operator's work: Key Lookup, RID Lookup,
    /// Scan With Predicate, Non-SARGable Predicate, Eager Index Spool,
    /// Scan Cardinality Misestimate.
    /// </summary>
    private static void ScoreByOperatorTime(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        if (node.HasActualStats && stmtMs > 0)
        {
            var operatorMs = PlanAnalyzer.GetOperatorOwnElapsedMs(node);
            if (operatorMs > 0)
            {
                var benefit = (double)operatorMs / stmtMs * 100;
                warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
            }
            else
            {
                // Operator self-time is 0 — negligible benefit
                warning.MaxBenefitPercent = 0;
            }
        }
        else if (!node.HasActualStats && stmt.StatementSubTreeCost > 0)
        {
            // Estimated plan fallback: use operator cost percentage
            var benefit = (double)node.CostPercent;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Rule 10: Key Lookup / RID Lookup — benefit includes the lookup operator's time,
    /// plus the parent Nested Loops join when the NL only exists to drive the lookup
    /// (inner child is the lookup, outer child is a seek/scan with no subtree).
    /// </summary>
    private static void ScoreKeyLookupWarning(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

        if (node.HasActualStats && stmtMs > 0)
        {
            var operatorMs = PlanAnalyzer.GetOperatorOwnElapsedMs(node);

            // Check if the parent NL join is purely a lookup driver:
            // - Parent is Nested Loops
            // - Has exactly 2 children
            // - This node (the lookup) is the inner child (index 1)
            // - The outer child (index 0) is a simple seek/scan with no children
            var parent = node.Parent;
            if (parent != null
                && parent.PhysicalOp == "Nested Loops"
                && parent.Children.Count == 2
                && parent.Children[1] == node
                && parent.Children[0].Children.Count == 0)
            {
                operatorMs += PlanAnalyzer.GetOperatorOwnElapsedMs(parent);
            }

            if (operatorMs > 0)
            {
                var benefit = (double)operatorMs / stmtMs * 100;
                warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
            }
            else
            {
                warning.MaxBenefitPercent = 0;
            }
        }
        else if (!node.HasActualStats && stmt.StatementSubTreeCost > 0)
        {
            var benefit = (double)node.CostPercent;
            // Same parent-NL logic for estimated plans
            var parent = node.Parent;
            if (parent != null
                && parent.PhysicalOp == "Nested Loops"
                && parent.Children.Count == 2
                && parent.Children[1] == node
                && parent.Children[0].Children.Count == 0)
            {
                benefit += parent.CostPercent;
            }
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Rule 5: Row Estimate Mismatch — benefit is the harmed operator's time.
    /// If the mismatch caused a spill, benefit = spilling operator time.
    /// If it caused a bad join choice, benefit = join operator time.
    /// Otherwise, benefit is the misestimated operator's own time (conservative).
    /// </summary>
    private static void ScoreEstimateMismatchWarning(PlanWarning warning, PlanNode node, PlanStatement stmt)
    {
        var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;
        if (stmtMs <= 0 || !node.HasActualStats) return;

        // Walk up to find the harmed operator (same logic as AssessEstimateHarm)
        var harmedNode = FindHarmedOperator(node);
        if (harmedNode != null)
        {
            var operatorMs = PlanAnalyzer.GetOperatorOwnElapsedMs(harmedNode);
            if (operatorMs > 0)
            {
                var benefit = (double)operatorMs / stmtMs * 100;
                warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
                return;
            }
        }

        // Fallback: use the misestimated node's own time
        var ownMs = PlanAnalyzer.GetOperatorOwnElapsedMs(node);
        if (ownMs > 0)
        {
            var benefit = (double)ownMs / stmtMs * 100;
            warning.MaxBenefitPercent = Math.Round(Math.Min(100, benefit), 1);
        }
    }

    /// <summary>
    /// Walks up from a node with a bad row estimate to find the operator that was
    /// harmed by it (spilling sort/hash, or join that chose the wrong strategy).
    /// Returns null if no specific harm can be attributed.
    /// </summary>
    private static PlanNode? FindHarmedOperator(PlanNode node)
    {
        // The node itself has a spill — it harmed itself
        if (node.Warnings.Any(w => w.SpillDetails != null))
            return node;

        // Walk up through transparent operators
        var ancestor = node.Parent;
        while (ancestor != null)
        {
            if (ancestor.PhysicalOp == "Parallelism" ||
                ancestor.PhysicalOp == "Compute Scalar" ||
                ancestor.PhysicalOp == "Segment" ||
                ancestor.PhysicalOp == "Sequence Project" ||
                ancestor.PhysicalOp == "Top" ||
                ancestor.PhysicalOp == "Filter")
            {
                ancestor = ancestor.Parent;
                continue;
            }

            // Parent join — bad row count from below caused wrong join choice
            if (ancestor.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase))
            {
                if (ancestor.IsAdaptive)
                    return null; // Adaptive join self-corrects
                return ancestor;
            }

            // Parent Sort/Hash that spilled
            if (ancestor.Warnings.Any(w => w.SpillDetails != null))
                return ancestor;

            // Parent Sort/Hash with no spill — benign
            if (ancestor.PhysicalOp.Contains("Sort", StringComparison.OrdinalIgnoreCase) ||
                ancestor.PhysicalOp.Contains("Hash", StringComparison.OrdinalIgnoreCase))
                return null;

            break;
        }

        return null;
    }

    /// <summary>
    /// Calculates a Parallelism (exchange) operator's own elapsed time.
    /// Mirrors PlanAnalyzer.GetParallelismOperatorElapsedMs but accessible here.
    /// </summary>
    private static long GetParallelismOperatorElapsedMs(PlanNode node)
    {
        if (node.Children.Count == 0)
            return node.ActualElapsedMs;

        if (node.PerThreadStats.Count > 1)
            return PlanAnalyzer.GetOperatorOwnElapsedMs(node);

        var maxChildElapsed = node.Children.Max(c => c.ActualElapsedMs);
        return Math.Max(0, node.ActualElapsedMs - maxChildElapsed);
    }

    // ---------------------------------------------------------------
    //  Stage 2: Wait Stats Benefit
    // ---------------------------------------------------------------

    /// <summary>
    /// Calculates MaxBenefitPercent for each wait type in the statement's wait stats.
    /// Serial plans: simple ratio of wait time to elapsed time.
    /// Parallel plans: proportional allocation across relevant operators (Joe's formula).
    /// </summary>
    private static void ScoreWaitStats(PlanStatement stmt)
    {
        var elapsedMs = stmt.QueryTimeStats!.ElapsedTimeMs;
        if (elapsedMs <= 0) return;

        var isParallel = stmt.DegreeOfParallelism > 1 && stmt.RootNode != null;

        // Collect all operators with per-thread stats for parallel benefit calculation
        List<OperatorWaitProfile>? operatorProfiles = null;
        if (isParallel)
        {
            operatorProfiles = new List<OperatorWaitProfile>();
            CollectOperatorWaitProfiles(stmt.RootNode!, operatorProfiles);
        }

        foreach (var wait in stmt.WaitStats)
        {
            if (wait.WaitTimeMs <= 0) continue;

            var category = ClassifyWaitType(wait.WaitType);
            double benefitPct;

            if (category == "Parallelism" && isParallel)
            {
                // CXPACKET/CXCONSUMER/CXSYNC: benefit is the parallelism efficiency gap,
                // not the raw wait time. Threads waiting for other threads is a symptom
                // of imperfect parallelism, not directly addressable time.
                var cpu = stmt.QueryTimeStats!.CpuTimeMs;
                var dop = stmt.DegreeOfParallelism;
                if (cpu > 0 && dop > 1)
                {
                    var idealElapsed = (double)cpu / dop;
                    benefitPct = Math.Max(0, (elapsedMs - idealElapsed) / elapsedMs * 100);
                }
                else
                {
                    benefitPct = (double)wait.WaitTimeMs / elapsedMs * 100;
                }
            }
            else if (!isParallel || operatorProfiles == null || operatorProfiles.Count == 0)
            {
                // Serial plan or no operator data: simple ratio
                benefitPct = (double)wait.WaitTimeMs / elapsedMs * 100;
            }
            else
            {
                // Parallel plan: proportional allocation across relevant operators
                benefitPct = CalculateParallelWaitBenefit(wait, category, operatorProfiles, elapsedMs);
            }

            stmt.WaitBenefits.Add(new WaitBenefit
            {
                WaitType = wait.WaitType,
                MaxBenefitPercent = Math.Round(Math.Min(100, Math.Max(0, benefitPct)), 1),
                Category = category
            });
        }
    }

    /// <summary>
    /// Parallel wait benefit using Joe's formula:
    /// benefit = (SUM relevant operator max waits) * (total_wait_for_type) / (SUM relevant operator total waits)
    /// Then convert to % of statement elapsed time.
    /// </summary>
    private static double CalculateParallelWaitBenefit(
        WaitStatInfo wait, string category,
        List<OperatorWaitProfile> profiles, long stmtElapsedMs)
    {
        // Filter to operators relevant for this wait category
        var relevant = new List<OperatorWaitProfile>();
        foreach (var p in profiles)
        {
            if (IsOperatorRelevantForCategory(p, category))
                relevant.Add(p);
        }

        // If no operators match, fall back to simple ratio
        if (relevant.Count == 0)
            return (double)wait.WaitTimeMs / stmtElapsedMs * 100;

        // Joe's formula:
        // sum_max = SUM of each relevant operator's max per-thread wait time
        // sum_total = SUM of each relevant operator's total wait time across all threads
        // benefit_ms = sum_max * wait.WaitTimeMs / sum_total
        double sumMax = 0;
        double sumTotal = 0;
        foreach (var p in relevant)
        {
            sumMax += p.MaxThreadWaitMs;
            sumTotal += p.TotalWaitMs;
        }

        if (sumTotal <= 0)
            return (double)wait.WaitTimeMs / stmtElapsedMs * 100;

        var benefitMs = sumMax * wait.WaitTimeMs / sumTotal;
        return benefitMs / stmtElapsedMs * 100;
    }

    /// <summary>
    /// Determines if an operator is relevant for a given wait category.
    /// </summary>
    private static bool IsOperatorRelevantForCategory(OperatorWaitProfile profile, string category)
    {
        return category switch
        {
            "I/O" => profile.HasPhysicalReads,
            "CPU" => profile.HasCpuWork,
            "Parallelism" => profile.IsExchange,
            "Hash" => profile.IsHashOperator,
            "Sort" => profile.IsSortOperator,
            "Latch" => profile.HasTempDbActivity,
            "Lock" => true,  // any operator can be blocked by locks
            "Network" => false,  // ASYNC_NETWORK_IO is client-side, not attributable to operators
            "Memory" => false,  // memory waits are statement-level
            _ => true,  // unknown category: include all operators
        };
    }

    /// <summary>
    /// Walks the operator tree and collects wait time profiles for each operator.
    /// Wait time per thread = max(0, elapsed - cpu) for that thread.
    /// </summary>
    private static void CollectOperatorWaitProfiles(PlanNode node, List<OperatorWaitProfile> profiles)
    {
        if (node.HasActualStats && node.PerThreadStats.Count > 0)
        {
            long maxThreadWait = 0;
            long totalWait = 0;

            foreach (var ts in node.PerThreadStats)
            {
                var threadWait = Math.Max(0, ts.ActualElapsedMs - ts.ActualCPUMs);
                totalWait += threadWait;
                if (threadWait > maxThreadWait)
                    maxThreadWait = threadWait;
            }

            if (totalWait > 0 || maxThreadWait > 0)
            {
                profiles.Add(new OperatorWaitProfile
                {
                    Node = node,
                    MaxThreadWaitMs = maxThreadWait,
                    TotalWaitMs = totalWait,
                    HasPhysicalReads = node.ActualPhysicalReads > 0,
                    HasCpuWork = node.ActualCPUMs > 0,
                    IsExchange = node.PhysicalOp == "Parallelism",
                    IsHashOperator = node.PhysicalOp.StartsWith("Hash", StringComparison.OrdinalIgnoreCase),
                    IsSortOperator = node.PhysicalOp.StartsWith("Sort", StringComparison.OrdinalIgnoreCase),
                    HasTempDbActivity = node.Warnings.Any(w => w.SpillDetails != null)
                                     || node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase)
                });
            }
        }

        foreach (var child in node.Children)
            CollectOperatorWaitProfiles(child, profiles);
    }

    /// <summary>
    /// Classifies a wait type into a category for operator-to-wait mapping.
    /// </summary>
    internal static string ClassifyWaitType(string waitType)
    {
        var wt = waitType.ToUpperInvariant();
        return wt switch
        {
            _ when wt.StartsWith("PAGEIOLATCH") => "I/O",
            _ when wt.Contains("IO_COMPLETION") => "I/O",
            _ when wt.StartsWith("WRITELOG") => "I/O",
            _ when wt == "SOS_SCHEDULER_YIELD" => "CPU",
            _ when wt.StartsWith("CXPACKET") || wt.StartsWith("CXCONSUMER") => "Parallelism",
            _ when wt.StartsWith("CXSYNC") => "Parallelism",
            _ when wt.StartsWith("HT") => "Hash",
            _ when wt == "BPSORT" => "Sort",
            _ when wt == "BMPBUILD" => "Hash",
            _ when wt.StartsWith("PAGELATCH") => "Latch",
            _ when wt.StartsWith("LATCH_") => "Latch",
            _ when wt.StartsWith("LCK_") => "Lock",
            _ when wt == "ASYNC_NETWORK_IO" => "Network",
            _ when wt.Contains("MEMORY_ALLOCATION") => "Memory",
            _ when wt == "SOS_PHYS_PAGE_CACHE" => "Memory",
            _ => "Other"
        };
    }

    /// <summary>
    /// Per-operator wait time profile used for parallel benefit allocation.
    /// </summary>
    private sealed class OperatorWaitProfile
    {
        public PlanNode Node { get; init; } = null!;
        public long MaxThreadWaitMs { get; init; }
        public long TotalWaitMs { get; init; }
        public bool HasPhysicalReads { get; init; }
        public bool HasCpuWork { get; init; }
        public bool IsExchange { get; init; }
        public bool IsHashOperator { get; init; }
        public bool IsSortOperator { get; init; }
        public bool HasTempDbActivity { get; init; }
    }
}
