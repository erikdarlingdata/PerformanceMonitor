using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Post-parse analysis pass that walks a parsed plan tree and adds warnings
/// for common performance anti-patterns. Called after ShowPlanParser.Parse().
/// </summary>
public static class PlanAnalyzer
{
    private static readonly Regex FunctionInPredicateRegex = new(
        @"\b(CONVERT_IMPLICIT|CONVERT|CAST|isnull|coalesce|datepart|datediff|dateadd|year|month|day|upper|lower|ltrim|rtrim|trim|substring|left|right|charindex|replace|len|datalength|abs|floor|ceiling|round|reverse|stuff|format)\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex LeadingWildcardLikeRegex = new(
        @"\blike\b[^'""]*?N?'%",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly Regex CaseInPredicateRegex = new(
        @"\bCASE\s+(WHEN\b|$)",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // Matches CTE definitions: WITH name AS ( or , name AS (
    private static readonly Regex CteDefinitionRegex = new(
        @"(?:\bWITH\s+|\,\s*)(\w+)\s+AS\s*\(",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public static void Analyze(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var stmt in batch.Statements)
            {
                AnalyzeStatement(stmt);

                if (stmt.RootNode != null)
                    AnalyzeNodeTree(stmt.RootNode, stmt);
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

            // Large memory grant with sort/hash guidance
            if (grant.GrantedMemoryKB > 102400 && stmt.RootNode != null)
            {
                var consumers = new List<string>();
                FindMemoryConsumers(stmt.RootNode, consumers);

                var grantMB = grant.GrantedMemoryKB / 1024.0;
                var guidance = consumers.Count > 0
                    ? $" Memory consumers: {string.Join(", ", consumers)}. Check whether these operators are processing more rows than necessary."
                    : "";

                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Large Memory Grant",
                    Message = $"Query granted {grantMB:F0} MB of memory.{guidance}",
                    Severity = grantMB >= 512 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 18: Compile memory exceeded (early abort)
        if (stmt.StatementOptmEarlyAbortReason == "MemoryLimitExceeded")
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Compile Memory Exceeded",
                Message = "Optimization was aborted early because the compile memory limit was exceeded. The plan may be suboptimal. Simplify the query or break it into smaller parts.",
                Severity = PlanWarningSeverity.Critical
            });
        }

        // Rule 19: High compile CPU
        if (stmt.CompileCPUMs >= 1000)
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "High Compile CPU",
                Message = $"Query took {stmt.CompileCPUMs:N0}ms of CPU to compile. Complex queries with many joins or subqueries can cause excessive compile time.",
                Severity = stmt.CompileCPUMs >= 5000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 4 (statement-level): UDF execution timing from QueryTimeStats
        // Some plans report UDF timing only at the statement level, not per-node.
        if (stmt.QueryUdfCpuTimeMs > 0 || stmt.QueryUdfElapsedTimeMs > 0)
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "UDF Execution",
                Message = $"Scalar UDF executing in this statement. UDF elapsed: {stmt.QueryUdfElapsedTimeMs:N0}ms, UDF CPU: {stmt.QueryUdfCpuTimeMs:N0}ms",
                Severity = stmt.QueryUdfElapsedTimeMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 20: Local variables without RECOMPILE
        // Parameters with no CompiledValue are likely local variables — the optimizer
        // cannot sniff their values and uses density-based ("unknown") estimates.
        if (stmt.Parameters.Count > 0)
        {
            var unsnifffedParams = stmt.Parameters
                .Where(p => string.IsNullOrEmpty(p.CompiledValue))
                .ToList();

            if (unsnifffedParams.Count > 0)
            {
                var hasRecompile = stmt.StatementText.Contains("RECOMPILE", StringComparison.OrdinalIgnoreCase);
                if (!hasRecompile)
                {
                    var names = string.Join(", ", unsnifffedParams.Select(p => p.Name));
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Local Variables",
                        Message = $"Parameters without compiled values detected: {names}. These are likely local variables, which cause the optimizer to use density-based (\"unknown\") estimates. Consider using OPTION (RECOMPILE) or rewriting with parameters.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 21: CTE referenced multiple times
        if (!string.IsNullOrEmpty(stmt.StatementText))
        {
            DetectMultiReferenceCte(stmt);
        }
    }

    private static void AnalyzeNodeTree(PlanNode node, PlanStatement stmt)
    {
        AnalyzeNode(node, stmt);

        foreach (var child in node.Children)
            AnalyzeNodeTree(child, stmt);
    }

    private static void AnalyzeNode(PlanNode node, PlanStatement stmt)
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
        if (node.UdfCpuTimeMs > 0 || node.UdfElapsedTimeMs > 0)
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "UDF Execution",
                Message = $"Scalar UDF executing on this operator. UDF elapsed: {node.UdfElapsedTimeMs:N0}ms, UDF CPU: {node.UdfCpuTimeMs:N0}ms",
                Severity = node.UdfElapsedTimeMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 5: Large estimate vs actual row gaps (actual plans only)
        if (node.HasActualStats && node.EstimateRows > 0)
        {
            if (node.ActualRows == 0)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Row Estimate Mismatch",
                    Message = $"Estimated {node.EstimateRows:N0} rows, actual 0 rows returned. May cause poor plan choices.",
                    Severity = node.EstimateRows >= 100 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
            else
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

        // Rule 7: Spill detection — calculate operator time and set severity
        // based on what percentage of statement elapsed time the spill accounts for
        foreach (var w in node.Warnings.ToList())
        {
            if (w.SpillDetails != null && node.ActualElapsedMs > 0)
            {
                var operatorMs = GetOperatorOwnElapsedMs(node);
                var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;

                if (stmtMs > 0)
                {
                    var pct = (double)operatorMs / stmtMs;
                    w.Message += $" Operator time: {operatorMs:N0}ms ({pct:P0} of statement).";

                    if (pct >= 0.5)
                        w.Severity = PlanWarningSeverity.Critical;
                    else if (pct >= 0.1)
                        w.Severity = PlanWarningSeverity.Warning;
                }
            }
        }

        // Rule 8: Parallel thread skew (actual plans with per-thread stats)
        // Only warn when there are enough rows to meaningfully distribute across threads
        if (node.PerThreadStats.Count > 1)
        {
            var totalRows = node.PerThreadStats.Sum(t => t.ActualRows);
            var minRowsForSkew = node.PerThreadStats.Count * 1000;
            if (totalRows >= minRowsForSkew)
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

        // Rule 12: Non-SARGable predicate on scan
        var nonSargableReason = DetectNonSargablePredicate(node);
        if (nonSargableReason != null)
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Non-SARGable Predicate",
                Message = $"{nonSargableReason} prevents index seek, forcing a scan. Fix the predicate or add a computed column with an index. Predicate: {Truncate(node.Predicate!, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 11: Scan with residual predicate (skip if non-SARGable already flagged)
        if (nonSargableReason == null && IsRowstoreScan(node) && !string.IsNullOrEmpty(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scan With Predicate",
                Message = $"Scan filtering rows with a residual predicate. An index on the predicate columns may help. Predicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 13: Mismatched data types (GetRangeWithMismatchedTypes)
        if (node.PhysicalOp == "Compute Scalar" &&
            !string.IsNullOrEmpty(node.DefinedValues) &&
            node.DefinedValues.Contains("GetRangeWithMismatchedTypes", StringComparison.OrdinalIgnoreCase))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Data Type Mismatch",
                Message = "Implicit conversion due to mismatched data types. The column type does not match the parameter or literal type, forcing SQL Server to convert values at runtime. Fix the parameter type to match the column.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 14: Lazy Table Spool unfavorable rebind/rewind ratio
        if (node.LogicalOp == "Lazy Spool")
        {
            var rebinds = node.HasActualStats ? (double)node.ActualRebinds : node.EstimateRebinds;
            var rewinds = node.HasActualStats ? (double)node.ActualRewinds : node.EstimateRewinds;
            var source = node.HasActualStats ? "actual" : "estimated";

            if (rebinds > 100 && (rewinds == 0 || rebinds * 2 >= rewinds))
            {
                var severity = rebinds > rewinds
                    ? PlanWarningSeverity.Critical
                    : PlanWarningSeverity.Warning;

                var ratio = rewinds > 0
                    ? $"{rewinds / rebinds:F1}x more rewinds (cache hits) than rebinds (cache misses)"
                    : "no rewinds (cache hits) at all";

                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Lazy Spool Ineffective",
                    Message = $"Lazy spool has unfavorable rebind/rewind ratio ({source}): {rebinds:N0} rebinds, {rewinds:N0} rewinds — {ratio}. The spool cache is not providing significant benefit.",
                    Severity = severity
                });
            }
        }

        // Rule 15: Join OR clause (Concatenation + Constant Scan pattern)
        // Pattern: Concatenation → Compute Scalar → Constant Scan (one per OR branch)
        if (node.PhysicalOp == "Concatenation")
        {
            var constantScanBranches = node.Children
                .Count(c => c.PhysicalOp == "Constant Scan" ||
                            c.Children.Any(gc => gc.PhysicalOp == "Constant Scan"));

            if (constantScanBranches >= 2 && HasJoinAncestor(node))
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Join OR Clause",
                    Message = $"OR clause expansion in a join predicate. SQL Server rewrote the OR as {constantScanBranches} separate branches (Concatenation of Constant Scans), each evaluated independently. This pattern often causes excessive inner-side executions.",
                    Severity = PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 16: Nested Loops high inner-side execution count
        if (node.PhysicalOp == "Nested Loops" &&
            node.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase) &&
            node.Children.Count >= 2)
        {
            var innerChild = node.Children[1];

            if (innerChild.HasActualStats && innerChild.ActualExecutions > 1000)
            {
                var dop = stmt.DegreeOfParallelism > 0 ? stmt.DegreeOfParallelism : 1;
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Nested Loops High Executions",
                    Message = $"Nested Loops inner side executed {innerChild.ActualExecutions:N0} times (DOP {dop}). A Hash Join or Merge Join may be more efficient for this row count.",
                    Severity = innerChild.ActualExecutions > 100000
                        ? PlanWarningSeverity.Critical
                        : PlanWarningSeverity.Warning
                });
            }
            else if (!innerChild.HasActualStats && innerChild.EstimateRebinds > 1000)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Nested Loops High Executions",
                    Message = $"Nested Loops inner side estimated to execute {innerChild.EstimateRebinds + 1:N0} times. A Hash Join or Merge Join may be more efficient for this row count.",
                    Severity = innerChild.EstimateRebinds > 100000
                        ? PlanWarningSeverity.Critical
                        : PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 17: Many-to-many Merge Join
        if (node.ManyToMany && node.PhysicalOp.Contains("Merge", StringComparison.OrdinalIgnoreCase))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Many-to-Many Merge Join",
                Message = "Many-to-many Merge Join requires a worktable to handle duplicate values. This can be expensive with large numbers of duplicates.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 22: Table variables (Object name starts with @)
        if (!string.IsNullOrEmpty(node.ObjectName) &&
            node.ObjectName.Contains("@"))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Table Variable",
                Message = "Table variable detected. Table variables have no statistics, so the optimizer always estimates 1 row regardless of actual cardinality. Consider using a temp table (#table) for better estimates.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 23: Table-valued functions
        if (node.LogicalOp == "Table-valued function")
        {
            var funcName = node.ObjectName ?? node.PhysicalOp;
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Table-Valued Function",
                Message = $"Table-valued function: {funcName}. Multi-statement TVFs have no statistics and a fixed estimate of 1 row (pre-2017) or 100 rows (2017+). Consider inlining the logic or using an inline TVF.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 24: Top above a scan on the inner side of Nested Loops
        // This pattern means the scan executes once per outer row, and the Top
        // limits each iteration — but with no supporting index the scan is a
        // linear search repeated potentially millions of times.
        if (node.PhysicalOp == "Nested Loops" && node.Children.Count >= 2)
        {
            var inner = node.Children[1];

            // Walk through pass-through operators to find Top
            while (inner.PhysicalOp == "Compute Scalar" && inner.Children.Count > 0)
                inner = inner.Children[0];

            if (inner.PhysicalOp == "Top" && inner.Children.Count > 0)
            {
                // Walk through pass-through operators below the Top to find the scan
                var scanCandidate = inner.Children[0];
                while (scanCandidate.PhysicalOp == "Compute Scalar" && scanCandidate.Children.Count > 0)
                    scanCandidate = scanCandidate.Children[0];

                if (IsRowstoreScan(scanCandidate))
                {
                    var predInfo = !string.IsNullOrEmpty(scanCandidate.Predicate)
                        ? " The scan has a residual predicate, so it may read many rows before the Top is satisfied."
                        : "";
                    inner.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Top Above Scan",
                        Message = $"Top operator reads from {scanCandidate.PhysicalOp} (Node {scanCandidate.NodeId}) on the inner side of Nested Loops (Node {node.NodeId}).{predInfo} An index supporting the filter and ordering may convert this to a seek.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }
    }

    /// <summary>
    /// Returns true for rowstore scan operators (Index Scan, Clustered Index Scan,
    /// Table Scan). Excludes columnstore scans, spools, and constant scans.
    /// </summary>
    private static bool IsRowstoreScan(PlanNode node)
    {
        return node.PhysicalOp.Contains("Scan", StringComparison.OrdinalIgnoreCase) &&
               !node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase) &&
               !node.PhysicalOp.Contains("Constant", StringComparison.OrdinalIgnoreCase) &&
               !node.PhysicalOp.Contains("Columnstore", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Detects non-SARGable patterns in scan predicates.
    /// Returns a description of the issue, or null if the predicate is fine.
    /// </summary>
    private static string? DetectNonSargablePredicate(PlanNode node)
    {
        if (string.IsNullOrEmpty(node.Predicate))
            return null;

        // Only check rowstore scan operators — columnstore is designed to be scanned
        if (!IsRowstoreScan(node))
            return null;

        var predicate = node.Predicate;

        // CASE expression in predicate — check first because CASE bodies
        // often contain CONVERT_IMPLICIT that isn't the root cause
        if (CaseInPredicateRegex.IsMatch(predicate))
            return "CASE expression in predicate";

        // CONVERT_IMPLICIT — most common non-SARGable pattern
        if (predicate.Contains("CONVERT_IMPLICIT", StringComparison.OrdinalIgnoreCase))
            return "Implicit conversion (CONVERT_IMPLICIT)";

        // ISNULL / COALESCE wrapping column
        if (Regex.IsMatch(predicate, @"\b(isnull|coalesce)\s*\(", RegexOptions.IgnoreCase))
            return "ISNULL/COALESCE wrapping column";

        // Common function calls on columns
        var funcMatch = FunctionInPredicateRegex.Match(predicate);
        if (funcMatch.Success)
        {
            var funcName = funcMatch.Groups[1].Value.ToUpperInvariant();
            if (funcName != "CONVERT_IMPLICIT")
                return $"Function call ({funcName}) on column";
        }

        // Leading wildcard LIKE
        if (LeadingWildcardLikeRegex.IsMatch(predicate))
            return "Leading wildcard LIKE pattern";

        return null;
    }

    /// <summary>
    /// Detects CTEs that are referenced more than once in the statement text.
    /// Each reference re-executes the CTE since SQL Server does not materialize them.
    /// </summary>
    private static void DetectMultiReferenceCte(PlanStatement stmt)
    {
        var text = stmt.StatementText;
        var cteMatches = CteDefinitionRegex.Matches(text);
        if (cteMatches.Count == 0)
            return;

        foreach (Match match in cteMatches)
        {
            var cteName = match.Groups[1].Value;
            if (string.IsNullOrEmpty(cteName))
                continue;

            // Count references as FROM/JOIN targets after the CTE definition
            var refPattern = new Regex(
                $@"\b(FROM|JOIN)\s+{Regex.Escape(cteName)}\b",
                RegexOptions.IgnoreCase);
            var refCount = refPattern.Matches(text).Count;

            if (refCount > 1)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "CTE Multiple References",
                    Message = $"CTE \"{cteName}\" is referenced {refCount} times. SQL Server does not materialize CTEs — each reference re-executes the entire CTE query. Consider materializing into a temp table.",
                    Severity = PlanWarningSeverity.Warning
                });
            }
        }
    }

    /// <summary>
    /// Checks whether a node has a join operator as an ancestor.
    /// </summary>
    private static bool HasJoinAncestor(PlanNode node)
    {
        var ancestor = node.Parent;
        while (ancestor != null)
        {
            if (ancestor.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase))
                return true;
            ancestor = ancestor.Parent;
        }
        return false;
    }

    /// <summary>
    /// Finds Sort and Hash Match operators in the tree that consume memory.
    /// </summary>
    private static void FindMemoryConsumers(PlanNode node, List<string> consumers)
    {
        if (node.PhysicalOp.Contains("Sort", StringComparison.OrdinalIgnoreCase) &&
            !node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase))
        {
            var rows = node.HasActualStats
                ? $"{node.ActualRows:N0} actual rows"
                : $"{node.EstimateRows:N0} estimated rows";
            consumers.Add($"Sort (Node {node.NodeId}, {rows})");
        }
        else if (node.PhysicalOp.Contains("Hash", StringComparison.OrdinalIgnoreCase))
        {
            var rows = node.HasActualStats
                ? $"{node.ActualRows:N0} actual rows"
                : $"{node.EstimateRows:N0} estimated rows";
            consumers.Add($"Hash Match (Node {node.NodeId}, {rows})");
        }

        foreach (var child in node.Children)
            FindMemoryConsumers(child, consumers);
    }

    /// <summary>
    /// Calculates an operator's own elapsed time by subtracting child time.
    /// In batch mode, operator times are self-contained. In row mode, times are
    /// cumulative (include children), so we subtract the dominant child's time.
    /// Parallelism (exchange) operators are skipped because they have timing bugs.
    /// </summary>
    private static long GetOperatorOwnElapsedMs(PlanNode node)
    {
        if (node.ActualExecutionMode == "Batch")
            return node.ActualElapsedMs;

        // Row mode: subtract the dominant child's elapsed time
        var maxChildElapsed = 0L;
        foreach (var child in node.Children)
        {
            var childElapsed = child.ActualElapsedMs;

            // Exchange operators have timing bugs — skip to their child
            if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
                childElapsed = child.Children.Max(c => c.ActualElapsedMs);

            if (childElapsed > maxChildElapsed)
                maxChildElapsed = childElapsed;
        }

        return Math.Max(0, node.ActualElapsedMs - maxChildElapsed);
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength] + "...";
    }
}
