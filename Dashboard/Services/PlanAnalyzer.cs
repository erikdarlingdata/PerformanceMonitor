using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services;

/// <summary>
/// Post-parse analysis pass that walks a parsed plan tree and adds warnings
/// for common performance anti-patterns. Called after ShowPlanParser.Parse().
/// </summary>
public static class PlanAnalyzer
{
    public static void Analyze(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var stmt in batch.Statements)
            {
                AnalyzeStatement(stmt);

                if (stmt.RootNode != null)
                    AnalyzeNodeTree(stmt.RootNode);
            }
        }
    }

    private static void AnalyzeStatement(PlanStatement stmt)
    {
        // Rule 3: Serial plan with reason
        if (!string.IsNullOrEmpty(stmt.NonParallelPlanReason))
        {
            var reason = stmt.NonParallelPlanReason switch
            {
                "MaxDOPSetToOne" => "MAXDOP is set to 1",
                "EstimatedDOPIsOne" => "Estimated DOP is 1",
                "NoParallelPlansInDesktopOrExpressEdition" => "Express/Desktop edition does not support parallelism",
                "CouldNotGenerateValidParallelPlan" => "Optimizer could not generate a valid parallel plan",
                "QueryHintNoParallelSet" => "OPTION (MAXDOP 1) hint forces serial execution",
                _ => stmt.NonParallelPlanReason
            };

            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Serial Plan",
                Message = $"Query forced to run serially: {reason}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 9: Memory grant issues (statement-level)
        if (stmt.MemoryGrant != null)
        {
            var grant = stmt.MemoryGrant;

            // Excessive grant — granted far more than actually used
            if (grant.GrantedMemoryKB > 0 && grant.MaxUsedMemoryKB > 0)
            {
                var wasteRatio = (double)grant.GrantedMemoryKB / grant.MaxUsedMemoryKB;
                if (wasteRatio >= 10 && grant.GrantedMemoryKB > 1024)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Excessive Memory Grant",
                        Message = $"Granted {grant.GrantedMemoryKB:N0} KB but only used {grant.MaxUsedMemoryKB:N0} KB ({wasteRatio:F0}x overestimate). Wasted memory blocks other queries.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }

            // Grant wait — query had to wait for memory
            if (grant.GrantWaitTimeMs > 0)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Memory Grant Wait",
                    Message = $"Query waited {grant.GrantWaitTimeMs:N0}ms for a memory grant. Server may be under memory pressure.",
                    Severity = grant.GrantWaitTimeMs >= 5000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
        }
    }

    private static void AnalyzeNodeTree(PlanNode node)
    {
        AnalyzeNode(node);

        foreach (var child in node.Children)
            AnalyzeNodeTree(child);
    }

    private static void AnalyzeNode(PlanNode node)
    {
        // Rule 1: Filter operators — rows survived the tree just to be discarded
        if (node.PhysicalOp == "Filter" && !string.IsNullOrEmpty(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Filter Operator",
                Message = $"Filter discards rows late in the plan. Predicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 2: Eager Index Spools — optimizer building temporary indexes on the fly
        if (node.LogicalOp == "Eager Spool" &&
            node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase))
        {
            var message = "Optimizer is building a temporary index at runtime. A permanent index may help.";
            if (!string.IsNullOrEmpty(node.SuggestedIndex))
                message += $"\n\nSuggested index:\n{node.SuggestedIndex}";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Eager Index Spool",
                Message = message,
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 4: UDF timing — any node spending time in UDFs (actual plans)
        if (node.UdfCpuTimeUs > 0 || node.UdfElapsedTimeUs > 0)
        {
            var cpuMs = node.UdfCpuTimeUs / 1000.0;
            var elapsedMs = node.UdfElapsedTimeUs / 1000.0;
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "UDF Execution",
                Message = $"Scalar UDF executing on this operator. UDF elapsed: {elapsedMs:F1}ms, UDF CPU: {cpuMs:F1}ms",
                Severity = elapsedMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 5: Large estimate vs actual row gaps (actual plans only)
        if (node.HasActualStats && node.EstimateRows > 0)
        {
            var ratio = node.ActualRows / node.EstimateRows;
            if (ratio >= 10.0 || ratio <= 0.1)
            {
                var direction = ratio >= 10.0 ? "underestimated" : "overestimated";
                var factor = ratio >= 10.0 ? ratio : 1.0 / ratio;
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Row Estimate Mismatch",
                    Message = $"Estimated {node.EstimateRows:N0} rows, actual {node.ActualRows:N0} ({factor:F0}x {direction}). May cause poor plan choices.",
                    Severity = factor >= 100 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 6: Scalar UDF references (works on estimated plans too)
        foreach (var udf in node.ScalarUdfs)
        {
            var type = udf.IsClrFunction ? "CLR" : "T-SQL";
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scalar UDF",
                Message = $"Scalar {type} UDF reference: {udf.FunctionName}. Scalar UDFs execute row-by-row and prevent parallelism.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 7: Spill detection — promote severity for large spills
        foreach (var w in node.Warnings.ToList())
        {
            if (w.SpillDetails != null && w.SpillDetails.WritesToTempDb > 1000)
                w.Severity = PlanWarningSeverity.Critical;
        }

        // Rule 8: Parallel thread skew (actual plans with per-thread stats)
        if (node.PerThreadStats.Count > 1)
        {
            var totalRows = node.PerThreadStats.Sum(t => t.ActualRows);
            if (totalRows > 0)
            {
                var maxThread = node.PerThreadStats.OrderByDescending(t => t.ActualRows).First();
                var skewRatio = (double)maxThread.ActualRows / totalRows;
                if (skewRatio >= 0.9 && node.PerThreadStats.Count >= 4)
                {
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Parallel Skew",
                        Message = $"Thread {maxThread.ThreadId} processed {skewRatio:P0} of rows ({maxThread.ActualRows:N0}/{totalRows:N0}). Work is heavily skewed to one thread.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 10: Key Lookup with residual predicate
        if (node.Lookup && !string.IsNullOrEmpty(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Key Lookup",
                Message = $"Key Lookup with residual predicate. A covering index may eliminate this lookup. Predicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 11: Scan with residual predicate (not spools)
        if (node.PhysicalOp.Contains("Scan", StringComparison.OrdinalIgnoreCase) &&
            !node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase) &&
            !string.IsNullOrEmpty(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scan With Predicate",
                Message = $"Scan filtering rows with a residual predicate. An index on the predicate columns may help. Predicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength] + "...";
    }
}
