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
public static partial class PlanAnalyzer
{
    private static readonly Regex FunctionInPredicateRegex = FunctionInPredicateRegExp();

    private static readonly Regex LeadingWildcardLikeRegex = LeadingWildcardLikeRegExp();

    private static readonly Regex CaseInPredicateRegex = CaseInPredicateRegExp();

    // Matches CTE definitions: WITH name AS ( or , name AS (
    private static readonly Regex CteDefinitionRegex = CteDefinitionRegExp();

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
                "EstimatedDOPIsOne" => "Estimated DOP is 1 (the plan's estimated cost was below the cost threshold for parallelism)",
                "NoParallelPlansInDesktopOrExpressEdition" => "Express/Desktop edition does not support parallelism",
                "CouldNotGenerateValidParallelPlan" => "Optimizer could not generate a valid parallel plan. Common causes: scalar UDFs, inserts into table variables, certain system functions, or OPTION (MAXDOP 1) hints",
                "QueryHintNoParallelSet" => "OPTION (MAXDOP 1) hint forces serial execution",
                _ => stmt.NonParallelPlanReason
            };

            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Serial Plan",
                Message = $"Query running serially: {reason}.",
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
                if (wasteRatio >= 10 && grant.GrantedMemoryKB >= 1048576)
                {
                    var grantMB = grant.GrantedMemoryKB / 1024.0;
                    var usedMB = grant.MaxUsedMemoryKB / 1024.0;
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Excessive Memory Grant",
                        Message = $"Granted {grantMB:N0} MB but only used {usedMB:N0} MB ({wasteRatio:F0}x overestimate). The unused memory is reserved and unavailable to other queries.",
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
                    Message = $"Query waited {grant.GrantWaitTimeMs:N0}ms for a memory grant before it could start running. Other queries were using all available workspace memory.",
                    Severity = grant.GrantWaitTimeMs >= 5000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }

            // Large memory grant with sort/hash guidance
            if (grant.GrantedMemoryKB >= 1048576 && stmt.RootNode != null)
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
                    Severity = grantMB >= 4096 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 18: Compile memory exceeded (early abort)
        if (stmt.StatementOptmEarlyAbortReason == "MemoryLimitExceeded")
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Compile Memory Exceeded",
                Message = "Optimization was aborted early because the compile memory limit was exceeded. The plan is likely suboptimal. Simplify the query by breaking it into smaller steps using #temp tables.",
                Severity = PlanWarningSeverity.Critical
            });
        }

        // Rule 19: High compile CPU
        if (stmt.CompileCPUMs >= 1000)
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "High Compile CPU",
                Message = $"Query took {stmt.CompileCPUMs:N0}ms of CPU just to compile a plan (before any data was read). Simplify the query by breaking it into smaller steps using #temp tables.",
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
                Message = $"Scalar UDF cost in this statement: {stmt.QueryUdfElapsedTimeMs:N0}ms elapsed, {stmt.QueryUdfCpuTimeMs:N0}ms CPU. Scalar UDFs run once per row and prevent parallelism. Rewrite as an inline table-valued function, or dump results to a #temp table and apply the UDF only to the final result set.",
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
                        Message = $"Local variables detected: {names}. SQL Server cannot sniff local variable values at compile time, so it uses average density estimates instead of your actual values. Test with OPTION (RECOMPILE) to see if the plan improves. For a permanent fix, use dynamic SQL or a stored procedure to pass the values as parameters instead of local variables.",
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

        // Rule 27: OPTIMIZE FOR UNKNOWN in statement text
        if (!string.IsNullOrEmpty(stmt.StatementText) &&
            OptimizeForUnknownRegExp().IsMatch(stmt.StatementText))
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Optimize For Unknown",
                Message = "OPTIMIZE FOR UNKNOWN uses average density estimates instead of sniffed parameter values. This can help when parameter sniffing causes plan instability, but may produce suboptimal plans for skewed data distributions.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 25: Ineffective parallelism — parallel plan where CPU ≈ elapsed
        if (stmt.DegreeOfParallelism > 1 && stmt.QueryTimeStats != null)
        {
            var cpu = stmt.QueryTimeStats.CpuTimeMs;
            var elapsed = stmt.QueryTimeStats.ElapsedTimeMs;

            if (elapsed >= 1000 && cpu > 0)
            {
                var ratio = (double)cpu / elapsed;
                if (ratio >= 0.8 && ratio <= 1.3)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Ineffective Parallelism",
                        Message = $"Parallel plan (DOP {stmt.DegreeOfParallelism}) but CPU time ({cpu:N0}ms) is nearly equal to elapsed time ({elapsed:N0}ms). " +
                                  $"The work ran essentially serially despite the overhead of parallelism. " +
                                  $"Look for parallel thread skew, blocking exchanges, or serial zones in the plan that prevent effective parallel execution.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 31: Parallel wait bottleneck — elapsed time significantly exceeds CPU time
        if (stmt.DegreeOfParallelism > 1 && stmt.QueryTimeStats != null)
        {
            var cpu = stmt.QueryTimeStats.CpuTimeMs;
            var elapsed = stmt.QueryTimeStats.ElapsedTimeMs;

            if (elapsed >= 1000 && cpu > 0)
            {
                var ratio = (double)cpu / elapsed;
                if (ratio < 0.8)
                {
                    var waitPct = (1.0 - ratio) * 100;
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Parallel Wait Bottleneck",
                        Message = $"Parallel plan (DOP {stmt.DegreeOfParallelism}) with elapsed time ({elapsed:N0}ms) significantly exceeding CPU time ({cpu:N0}ms). " +
                                  $"Approximately {waitPct:N0}% of elapsed time was spent waiting rather than on CPU. " +
                                  $"Common causes include spills to tempdb, physical I/O reads, lock or latch contention, and memory grant waits.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 30: Missing index quality evaluation
        {
            // Detect duplicate suggestions for the same table
            var tableSuggestionCount = stmt.MissingIndexes
                .GroupBy(mi => $"{mi.Schema}.{mi.Table}", StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

            foreach (var mi in stmt.MissingIndexes)
            {
                var keyCount = mi.EqualityColumns.Count + mi.InequalityColumns.Count;
                var includeCount = mi.IncludeColumns.Count;
                var tableKey = $"{mi.Schema}.{mi.Table}";

                // Low-impact suggestion (< 25% improvement)
                if (mi.Impact < 25)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Low Impact Index",
                        Message = $"Missing index suggestion for {mi.Table} has only {mi.Impact:F0}% estimated impact. Low-impact indexes add maintenance overhead (insert/update/delete cost) that may not justify the modest query improvement.",
                        Severity = PlanWarningSeverity.Info
                    });
                }

                // Wide INCLUDE columns (> 5)
                if (includeCount > 5)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Wide Index Suggestion",
                        Message = $"Missing index suggestion for {mi.Table} has {includeCount} INCLUDE columns. This is a \"kitchen sink\" index — SQL Server suggests covering every column the query touches, but the resulting index would be very wide and expensive to maintain. Evaluate which columns are actually needed, or consider a narrower index with fewer includes.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
                // Wide key columns (> 4)
                else if (keyCount > 4)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Wide Index Suggestion",
                        Message = $"Missing index suggestion for {mi.Table} has {keyCount} key columns ({mi.EqualityColumns.Count} equality + {mi.InequalityColumns.Count} inequality). Wide key columns increase index size and maintenance cost. Evaluate whether all key columns are needed for seek predicates.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }

                // Multiple suggestions for same table
                if (tableSuggestionCount.TryGetValue(tableKey, out var count))
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Duplicate Index Suggestions",
                        Message = $"{count} missing index suggestions target {mi.Table}. Multiple suggestions for the same table often overlap — consolidate into fewer, broader indexes rather than creating all of them.",
                        Severity = PlanWarningSeverity.Warning
                    });
                    // Only warn once per table
                    tableSuggestionCount.Remove(tableKey);
                }
            }
        }

        // Rule 22 (statement-level): Table variable warnings
        if (stmt.RootNode != null)
        {
            var hasTableVar = false;
            var isModification = stmt.StatementType is "INSERT" or "UPDATE" or "DELETE" or "MERGE";
            var modifiesTableVar = false;
            CheckForTableVariables(stmt.RootNode, isModification, ref hasTableVar, ref modifiesTableVar);

            if (hasTableVar)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Table Variable",
                    Message = "Table variable detected. Table variables lack column-level statistics, which causes bad row estimates, join choices, and memory grant decisions. Replace with a #temp table.",
                    Severity = PlanWarningSeverity.Warning
                });
            }

            if (modifiesTableVar)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Table Variable",
                    Message = "This query modifies a table variable, which forces the entire plan to run single-threaded. SQL Server cannot use parallelism for modifications to table variables. Replace with a #temp table to allow parallel execution.",
                    Severity = PlanWarningSeverity.Critical
                });
            }
        }
    }

    private static void CheckForTableVariables(PlanNode node, bool isModification,
        ref bool hasTableVar, ref bool modifiesTableVar)
    {
        if (!string.IsNullOrEmpty(node.ObjectName) && node.ObjectName.StartsWith("@"))
        {
            hasTableVar = true;
            if (isModification && (node.PhysicalOp.Contains("Insert", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Update", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Delete", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Merge", StringComparison.OrdinalIgnoreCase)))
            {
                modifiesTableVar = true;
            }
        }
        foreach (var child in node.Children)
            CheckForTableVariables(child, isModification, ref hasTableVar, ref modifiesTableVar);
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
        // Quantify the impact by summing child subtree cost (reads, CPU, time).
        if (node.PhysicalOp == "Filter" && !string.IsNullOrEmpty(node.Predicate))
        {
            var impact = QuantifyFilterImpact(node);
            var predicate = Truncate(node.Predicate, 200);
            var message = "Filter operator discarding rows late in the plan.";
            if (!string.IsNullOrEmpty(impact))
                message += $"\n{impact}";
            message += $"\nPredicate: {predicate}";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Filter Operator",
                Message = message,
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 2: Eager Index Spools — optimizer building temporary indexes on the fly
        if (node.LogicalOp == "Eager Spool" &&
            node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase))
        {
            var message = "SQL Server is building a temporary index in TempDB at runtime because no suitable permanent index exists. This is expensive — it builds the index from scratch on every execution. Create a permanent index on the underlying table to eliminate this operator entirely.";
            if (!string.IsNullOrEmpty(node.SuggestedIndex))
                message += $"\n\nCreate this index:\n{node.SuggestedIndex}";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Eager Index Spool",
                Message = message,
                Severity = PlanWarningSeverity.Critical
            });
        }

        // Rule 4: UDF timing — any node spending time in UDFs (actual plans)
        if (node.UdfCpuTimeMs > 0 || node.UdfElapsedTimeMs > 0)
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "UDF Execution",
                Message = $"Scalar UDF executing on this operator ({node.UdfElapsedTimeMs:N0}ms elapsed, {node.UdfCpuTimeMs:N0}ms CPU). Scalar UDFs run once per row and prevent parallelism. Rewrite as an inline table-valued function, or dump the query results to a #temp table first and apply the UDF only to the final result set.",
                Severity = node.UdfElapsedTimeMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 5: Large estimate vs actual row gaps (actual plans only)
        // Only warn when the bad estimate actually causes observable harm:
        // - The node itself spilled (Sort/Hash with bad memory grant)
        // - A parent join may have chosen the wrong strategy
        // - Root nodes with no parent to harm are skipped
        // - Nodes whose only parents are Parallelism/Top/Sort (no spill) are skipped
        if (node.HasActualStats && node.EstimateRows > 0)
        {
            if (node.ActualRows == 0)
            {
                // Zero rows is always worth noting — resources were allocated for nothing
                if (node.EstimateRows >= 100)
                {
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Row Estimate Mismatch",
                        Message = $"Estimated {node.EstimateRows:N0} rows but actual 0 rows returned. SQL Server allocated resources for rows that never materialized.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
            else
            {
                // Compare per-execution actuals to estimates (SQL Server estimates are per-execution)
                var executions = node.ActualExecutions > 0 ? node.ActualExecutions : 1;
                var actualPerExec = (double)node.ActualRows / executions;
                var ratio = actualPerExec / node.EstimateRows;
                if (ratio >= 10.0 || ratio <= 0.1)
                {
                    var harm = AssessEstimateHarm(node, ratio);
                    if (harm != null)
                    {
                        var direction = ratio >= 10.0 ? "underestimated" : "overestimated";
                        var factor = ratio >= 10.0 ? ratio : 1.0 / ratio;
                        var actualDisplay = executions > 1
                            ? $"Actual {node.ActualRows:N0} ({actualPerExec:N0} rows x {executions:N0} executions)"
                            : $"Actual {node.ActualRows:N0}";
                        node.Warnings.Add(new PlanWarning
                        {
                            WarningType = "Row Estimate Mismatch",
                            Message = $"Estimated {node.EstimateRows:N0} vs {actualDisplay} — {factor:F0}x {direction}. {harm}",
                            Severity = factor >= 100 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                        });
                    }
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
                Message = $"Scalar {type} UDF: {udf.FunctionName}. Scalar UDFs run once per row and prevent parallelism. Rewrite as an inline table-valued function, or dump results to a #temp table and apply the UDF only to the final result set.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 7: Spill detection — calculate operator time and set severity
        // based on what percentage of statement elapsed time the spill accounts for.
        // Exchange spills on Parallelism operators get special handling since their
        // timing is unreliable but the write count tells the story.
        foreach (var w in node.Warnings.ToList())
        {
            if (w.SpillDetails == null)
                continue;

            var isExchangeSpill = w.SpillDetails.SpillType == "Exchange";

            if (isExchangeSpill)
            {
                // Exchange spills: severity based on write count since timing is unreliable
                var writes = w.SpillDetails.WritesToTempDb;
                if (writes >= 1_000_000)
                    w.Severity = PlanWarningSeverity.Critical;
                else if (writes >= 10_000)
                    w.Severity = PlanWarningSeverity.Warning;

                // Surface Parallelism operator time when available (actual plans)
                if (node.ActualElapsedMs > 0)
                {
                    var operatorMs = GetParallelismOperatorElapsedMs(node);
                    var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;
                    if (stmtMs > 0 && operatorMs > 0)
                    {
                        var pct = (double)operatorMs / stmtMs;
                        w.Message += $" Operator time: {operatorMs:N0}ms ({pct:P0} of statement).";
                    }
                }
            }
            else if (node.ActualElapsedMs > 0)
            {
                // Sort/Hash spills: severity based on operator time percentage
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
                var skewThreshold = node.PerThreadStats.Count == 2 ? 0.75 : 0.50;
                if (skewRatio >= skewThreshold)
                {
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Parallel Skew",
                        Message = $"Thread {maxThread.ThreadId} processed {skewRatio:P0} of rows ({maxThread.ActualRows:N0}/{totalRows:N0}). Work is heavily skewed to one thread, so parallelism isn't helping much.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 10: Key Lookup / RID Lookup with residual predicate
        // Check RID Lookup first — it's more specific (PhysicalOp) and also has Lookup=true
        if (node.PhysicalOp.StartsWith("RID Lookup", StringComparison.OrdinalIgnoreCase))
        {
            var message = "RID Lookup — this table is a heap (no clustered index). SQL Server found rows via a nonclustered index but had to follow row identifiers back to unordered heap pages. Heap lookups are more expensive than key lookups because pages are not sorted and may have forwarding pointers. Add a clustered index to the table.";
            if (!string.IsNullOrEmpty(node.Predicate))
                message += $" Predicate: {Truncate(node.Predicate, 200)}";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "RID Lookup",
                Message = message,
                Severity = PlanWarningSeverity.Warning
            });
        }
        else if (node.Lookup && !string.IsNullOrEmpty(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Key Lookup",
                Message = $"Key Lookup — SQL Server found rows via a nonclustered index but had to go back to the clustered index for additional columns. Alter the nonclustered index to add the predicate column as a key column or as an INCLUDE column.\nPredicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Critical
            });
        }

        // Rule 12: Non-SARGable predicate on scan
        var nonSargableReason = DetectNonSargablePredicate(node);
        if (nonSargableReason != null)
        {
            var nonSargableAdvice = nonSargableReason switch
            {
                "Implicit conversion (CONVERT_IMPLICIT)" =>
                    "Implicit conversion (CONVERT_IMPLICIT) prevents an index seek. Match the parameter or variable data type to the column data type.",
                "ISNULL/COALESCE wrapping column" =>
                    "ISNULL/COALESCE wrapping a column prevents an index seek. Rewrite the predicate to avoid wrapping the column, e.g. use \"WHERE col = @val OR col IS NULL\" instead of \"WHERE ISNULL(col, '') = @val\".",
                "Leading wildcard LIKE pattern" =>
                    "Leading wildcard LIKE prevents an index seek — SQL Server must scan every row. If substring search performance is critical, consider a full-text index or a trigram-based approach.",
                "CASE expression in predicate" =>
                    "CASE expression in a predicate prevents an index seek. Rewrite using separate WHERE clauses combined with OR, or split into multiple queries.",
                _ when nonSargableReason.StartsWith("Function call") =>
                    $"{nonSargableReason} prevents an index seek. Remove the function from the column side — apply it to the parameter instead, or create a computed column with the expression and index that.",
                _ =>
                    $"{nonSargableReason} prevents an index seek, forcing a scan."
            };

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Non-SARGable Predicate",
                Message = $"{nonSargableAdvice}\nPredicate: {Truncate(node.Predicate!, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 11: Scan with residual predicate (skip if non-SARGable already flagged)
        // A PROBE() alone is just a bitmap filter — not a real residual predicate.
        if (nonSargableReason == null && IsRowstoreScan(node) && !string.IsNullOrEmpty(node.Predicate) &&
            !IsProbeOnly(node.Predicate))
        {
            var displayPredicate = StripProbeExpressions(node.Predicate);
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scan With Predicate",
                Message = $"Scan with residual predicate — SQL Server is reading every row and filtering after the fact. Check that you have appropriate indexes.\nPredicate: {Truncate(displayPredicate, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 13: Mismatched data types (GetRangeWithMismatchedTypes / GetRangeThroughConvert)
        if (node.PhysicalOp == "Compute Scalar" && !string.IsNullOrEmpty(node.DefinedValues))
        {
            var hasMismatch = node.DefinedValues.Contains("GetRangeWithMismatchedTypes", StringComparison.OrdinalIgnoreCase);
            var hasConvert = node.DefinedValues.Contains("GetRangeThroughConvert", StringComparison.OrdinalIgnoreCase);

            if (hasMismatch || hasConvert)
            {
                var reason = hasMismatch
                    ? "Mismatched data types between the column and the parameter/literal. SQL Server is converting every row to compare, preventing index seeks. Match your data types — don't pass nvarchar to a varchar column, or int to a bigint column."
                    : "CONVERT/CAST wrapping a column in the predicate. SQL Server is converting every row to compare, preventing index seeks. Match your data types — convert the parameter/literal instead of the column.";

                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Data Type Mismatch",
                    Message = reason,
                    Severity = PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 14: Lazy Table Spool unfavorable rebind/rewind ratio
        // Rebinds = cache misses (child re-executes), rewinds = cache hits (reuse cached result)
        if (node.LogicalOp == "Lazy Spool" && !node.PhysicalOp.Contains("Index", StringComparison.OrdinalIgnoreCase))
        {
            var rebinds = node.HasActualStats ? (double)node.ActualRebinds : node.EstimateRebinds;
            var rewinds = node.HasActualStats ? (double)node.ActualRewinds : node.EstimateRewinds;
            var source = node.HasActualStats ? "actual" : "estimated";

            if (rebinds > 100 && rewinds < rebinds * 5)
            {
                var severity = rewinds < rebinds
                    ? PlanWarningSeverity.Critical
                    : PlanWarningSeverity.Warning;

                var ratio = rewinds > 0
                    ? $"{rewinds / rebinds:F1}x rewinds (cache hits) per rebind (cache miss)"
                    : "no rewinds (cache hits) at all";

                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Lazy Spool Ineffective",
                    Message = $"Lazy spool has low cache hit ratio ({source}): {rebinds:N0} rebinds (cache misses), {rewinds:N0} rewinds (cache hits) — {ratio}. The spool is caching results but rarely reusing them, adding overhead for no benefit.",
                    Severity = severity
                });
            }
        }

        // Rule 15: Join OR clause
        // Pattern: Nested Loops → Merge Interval → TopN Sort → [Compute Scalar] → Concatenation → [Compute Scalar] → 2+ Constant Scans
        if (node.PhysicalOp == "Concatenation")
        {
            var constantScanBranches = node.Children
                .Count(c => c.PhysicalOp == "Constant Scan" ||
                            (c.PhysicalOp == "Compute Scalar" &&
                             c.Children.Any(gc => gc.PhysicalOp == "Constant Scan")));

            if (constantScanBranches >= 2 && IsOrExpansionChain(node))
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Join OR Clause",
                    Message = $"OR in a join predicate. SQL Server rewrote the OR as {constantScanBranches} separate lookups, each evaluated independently — this multiplies the work on the inner side. Rewrite as separate queries joined with UNION ALL. For example, change \"FROM a JOIN b ON a.x = b.x OR a.y = b.y\" to \"FROM a JOIN b ON a.x = b.x UNION ALL FROM a JOIN b ON a.y = b.y\".",
                    Severity = PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 16: Nested Loops high inner-side execution count
        // Deep analysis: combine execution count + outer estimate mismatch + inner cost
        if (node.PhysicalOp == "Nested Loops" &&
            node.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase) &&
            !node.IsAdaptive &&
            node.Children.Count >= 2)
        {
            var outerChild = node.Children[0];
            var innerChild = node.Children[1];

            if (innerChild.HasActualStats && innerChild.ActualExecutions > 100000)
            {
                var dop = stmt.DegreeOfParallelism > 0 ? stmt.DegreeOfParallelism : 1;
                var details = new List<string>();

                // Core fact
                details.Add($"Nested Loops inner side executed {innerChild.ActualExecutions:N0} times (DOP {dop}).");

                // Outer side estimate mismatch — explains WHY the optimizer chose NL
                if (outerChild.HasActualStats && outerChild.EstimateRows > 0)
                {
                    var outerExecs = outerChild.ActualExecutions > 0 ? outerChild.ActualExecutions : 1;
                    var outerActualPerExec = (double)outerChild.ActualRows / outerExecs;
                    var outerRatio = outerActualPerExec / outerChild.EstimateRows;
                    if (outerRatio >= 10.0)
                    {
                        details.Add($"Outer side: estimated {outerChild.EstimateRows:N0} rows, actual {outerActualPerExec:N0} ({outerRatio:F0}x underestimate). The optimizer chose Nested Loops expecting far fewer iterations.");
                    }
                }

                // Inner side cost — reads and time spent doing the repeated work
                long innerReads = SumSubtreeReads(innerChild);
                if (innerReads > 0)
                    details.Add($"Inner side total: {innerReads:N0} logical reads.");

                if (innerChild.ActualElapsedMs > 0)
                {
                    var stmtMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0;
                    if (stmtMs > 0)
                    {
                        var pct = (double)innerChild.ActualElapsedMs / stmtMs * 100;
                        details.Add($"Inner side time: {innerChild.ActualElapsedMs:N0}ms ({pct:N0}% of statement).");
                    }
                    else
                    {
                        details.Add($"Inner side time: {innerChild.ActualElapsedMs:N0}ms.");
                    }
                }

                // Cause/recommendation
                var hasParams = stmt.Parameters.Count > 0;
                if (hasParams)
                    details.Add("This may be caused by parameter sniffing — the optimizer chose Nested Loops based on a sniffed value that produced far fewer outer rows.");
                else
                    details.Add("Consider whether a hash or merge join would be more appropriate for this row count.");

                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Nested Loops High Executions",
                    Message = string.Join(" ", details),
                    Severity = innerChild.ActualExecutions > 1000000
                        ? PlanWarningSeverity.Critical
                        : PlanWarningSeverity.Warning
                });
            }
            // Estimated plans: the optimizer knew the row count and chose Nested Loops
            // deliberately — don't second-guess it without actual execution data.
        }

        // Rule 17: Many-to-many Merge Join
        // In actual plans, the Merge Join operator reports logical reads when the worktable is used.
        // When ActualLogicalReads is 0, the worktable wasn't hit and the warning is noise.
        if (node.ManyToMany && node.PhysicalOp.Contains("Merge", StringComparison.OrdinalIgnoreCase) &&
            (!node.HasActualStats || node.ActualLogicalReads > 0))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Many-to-Many Merge Join",
                Message = node.HasActualStats
                    ? $"Many-to-many Merge Join — SQL Server created a worktable in TempDB ({node.ActualLogicalReads:N0} logical reads) because both sides have duplicate values in the join columns."
                    : "Many-to-many Merge Join — SQL Server will create a worktable in TempDB because both sides have duplicate values in the join columns.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 22: Table variables (Object name starts with @)
        if (!string.IsNullOrEmpty(node.ObjectName) &&
            node.ObjectName.StartsWith('@'))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Table Variable",
                Message = "Table variable detected. Table variables lack column-level statistics, which causes bad row estimates, join choices, and memory grant decisions. Replace with a #temp table.",
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
                Message = $"Table-valued function: {funcName}. Multi-statement TVFs have no statistics — SQL Server guesses 1 row (pre-2017) or 100 rows (2017+) regardless of actual size. Rewrite as an inline table-valued function if possible, or dump the function results into a #temp table and join to that instead.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 24: Top above a scan
        // Detects Top or Top N Sort operators feeding from a scan. This often means the
        // query is scanning the entire table/index and sorting just to return a few rows,
        // when an appropriate index could satisfy the request directly.
        {
            var isTop = node.PhysicalOp == "Top";
            var isTopNSort = node.LogicalOp == "Top N Sort";

            if ((isTop || isTopNSort) && node.Children.Count > 0)
            {
                // Walk through pass-through operators below the Top to find the scan
                var scanCandidate = node.Children[0];
                while ((scanCandidate.PhysicalOp == "Compute Scalar" || scanCandidate.PhysicalOp == "Parallelism")
                    && scanCandidate.Children.Count > 0)
                    scanCandidate = scanCandidate.Children[0];

                if (IsScanOperator(scanCandidate))
                {
                    var topLabel = isTopNSort ? "Top N Sort" : "Top";
                    var onInner = node.Parent?.PhysicalOp == "Nested Loops" && node.Parent.Children.Count >= 2
                        && node.Parent.Children[1] == node;
                    var innerNote = onInner
                        ? $" This is on the inner side of Nested Loops (Node {node.Parent!.NodeId}), so the scan repeats for every outer row."
                        : "";
                    var predInfo = !string.IsNullOrEmpty(scanCandidate.Predicate)
                        ? " The scan has a residual predicate, so it may read many rows before the Top is satisfied."
                        : "";
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Top Above Scan",
                        Message = $"{topLabel} reads from {scanCandidate.PhysicalOp} (Node {scanCandidate.NodeId}).{innerNote}{predInfo} An index on the ORDER BY columns could eliminate the scan and sort entirely.",
                        Severity = onInner ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 26: Row Goal (informational) — optimizer reduced estimate due to TOP/EXISTS/IN
        // Only surface on data access operators (seeks/scans) where the row goal actually matters
        var isDataAccess = node.PhysicalOp != null &&
            (node.PhysicalOp.Contains("Scan") || node.PhysicalOp.Contains("Seek"));
        if (isDataAccess && node.EstimateRowsWithoutRowGoal > 0 && node.EstimateRows > 0 &&
            node.EstimateRowsWithoutRowGoal > node.EstimateRows)
        {
            var reduction = node.EstimateRowsWithoutRowGoal / node.EstimateRows;
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Row Goal",
                Message = $"Row goal active: estimate reduced from {node.EstimateRowsWithoutRowGoal:N0} to {node.EstimateRows:N0} ({reduction:N0}x reduction) due to TOP, EXISTS, IN, or FAST hint. The optimizer chose this plan shape expecting to stop reading early. If the query reads all rows anyway, the plan choice may be suboptimal.",
                Severity = PlanWarningSeverity.Info
            });
        }

        // Rule 28: Row Count Spool — NOT IN with nullable column
        // Pattern: Row Count Spool with high rewinds, child scan has IS NULL predicate,
        // and statement text contains NOT IN
        if (node.PhysicalOp.Contains("Row Count Spool"))
        {
            var rewinds = node.HasActualStats ? (double)node.ActualRewinds : node.EstimateRewinds;
            if (rewinds > 10000 && HasNotInPattern(node, stmt))
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "NOT IN with Nullable Column",
                    Message = $"Row Count Spool with {rewinds:N0} rewinds. This pattern occurs when NOT IN is used with a nullable column — SQL Server cannot use an efficient Anti Semi Join because it must check for NULL values on every outer row. Rewrite as NOT EXISTS, or add WHERE column IS NOT NULL to the subquery.",
                    Severity = rewinds > 1_000_000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
                });
            }
        }

        // Rule 29: Enhance implicit conversion warnings — Seek Plan is more severe
        foreach (var w in node.Warnings.ToList())
        {
            if (w.WarningType == "Implicit Conversion" && w.Message.StartsWith("Seek Plan", StringComparison.Ordinal))
            {
                w.Severity = PlanWarningSeverity.Critical;
                w.Message = $"Implicit conversion prevented an index seek, forcing a scan instead. Fix the data type mismatch: ensure the parameter or variable type matches the column type exactly. {w.Message}";
            }
        }
    }

    /// <summary>
    /// Detects the NOT IN with nullable column pattern: statement has NOT IN,
    /// and a nearby Nested Loops Anti Semi Join has an IS NULL residual predicate.
    /// Checks ancestors and their children (siblings of ancestors) since the IS NULL
    /// predicate may be on a sibling Anti Semi Join rather than a direct parent.
    /// </summary>
    private static bool HasNotInPattern(PlanNode spoolNode, PlanStatement stmt)
    {
        // Check statement text for NOT IN
        if (string.IsNullOrEmpty(stmt.StatementText) ||
            !NotInRegExp().IsMatch(stmt.StatementText))
            return false;

        // Walk up the tree checking ancestors and their children
        var parent = spoolNode.Parent;
        while (parent != null)
        {
            if (IsAntiSemiJoinWithIsNull(parent))
                return true;

            // Check siblings: the IS NULL predicate may be on a sibling Anti Semi Join
            // (e.g. outer NL Anti Semi Join has two children: inner NL Anti Semi Join + Row Count Spool)
            foreach (var sibling in parent.Children)
            {
                if (sibling != spoolNode && IsAntiSemiJoinWithIsNull(sibling))
                    return true;
            }

            parent = parent.Parent;
        }

        return false;
    }

    private static bool IsAntiSemiJoinWithIsNull(PlanNode node) =>
        node.PhysicalOp == "Nested Loops" &&
        node.LogicalOp.Contains("Anti Semi", StringComparison.OrdinalIgnoreCase) &&
        !string.IsNullOrEmpty(node.Predicate) &&
        node.Predicate.Contains("IS NULL", StringComparison.OrdinalIgnoreCase);

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
    /// Returns true when the predicate contains ONLY PROBE() bitmap filter(s)
    /// with no real residual predicate. PROBE alone is a bitmap filter pushed
    /// down from a hash join — not interesting by itself. If a real predicate
    /// exists alongside PROBE (e.g. "[col]=(1) AND PROBE(...)"), returns false.
    /// </summary>
    private static bool IsProbeOnly(string predicate)
    {
        // Strip all PROBE(...) expressions — PROBE args can contain nested parens
        var stripped = Regex.Replace(predicate, @"PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)", "",
            RegexOptions.IgnoreCase).Trim();

        // Remove leftover AND/OR connectors and whitespace
        stripped = Regex.Replace(stripped, @"\b(AND|OR)\b", "", RegexOptions.IgnoreCase).Trim();

        // If nothing meaningful remains, it was PROBE-only
        return stripped.Length == 0;
    }

    /// <summary>
    /// Strips PROBE(...) bitmap filter expressions from a predicate for display,
    /// leaving only the real residual predicate columns.
    /// </summary>
    private static string StripProbeExpressions(string predicate)
    {
        var stripped = Regex.Replace(predicate, @"\s*AND\s+PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)", "",
            RegexOptions.IgnoreCase);
        stripped = Regex.Replace(stripped, @"PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)\s*AND\s+", "",
            RegexOptions.IgnoreCase);
        stripped = Regex.Replace(stripped, @"PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)", "",
            RegexOptions.IgnoreCase);
        return stripped.Trim();
    }

    /// <summary>
    /// Returns true for any scan operator including columnstore.
    /// Excludes spools and constant scans.
    /// </summary>
    private static bool IsScanOperator(PlanNode node)
    {
        return node.PhysicalOp.Contains("Scan", StringComparison.OrdinalIgnoreCase) &&
               !node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase) &&
               !node.PhysicalOp.Contains("Constant", StringComparison.OrdinalIgnoreCase);
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
        if (IsNullCoalesceRegExp().IsMatch(predicate))
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
            var refCount = refPattern.Count(text);

            if (refCount > 1)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "CTE Multiple References",
                    Message = $"CTE \"{cteName}\" is referenced {refCount} times. SQL Server re-executes the entire CTE each time — it does not materialize the results. Materialize into a #temp table instead.",
                    Severity = PlanWarningSeverity.Warning
                });
            }
        }
    }

    /// <summary>
    /// Verifies the OR expansion chain walking up from a Concatenation node:
    /// Nested Loops → Merge Interval → TopN Sort → [Compute Scalar] → Concatenation
    /// </summary>
    private static bool IsOrExpansionChain(PlanNode concatenationNode)
    {
        // Walk up, skipping Compute Scalar
        var parent = concatenationNode.Parent;
        while (parent != null && parent.PhysicalOp == "Compute Scalar")
            parent = parent.Parent;

        // Expect TopN Sort (XML says "TopN Sort", parser normalizes to "Top N Sort")
        if (parent == null || parent.LogicalOp != "Top N Sort")
            return false;

        // Walk up to Merge Interval
        parent = parent.Parent;
        if (parent == null || parent.PhysicalOp != "Merge Interval")
            return false;

        // Walk up to Nested Loops
        parent = parent.Parent;
        if (parent == null || parent.PhysicalOp != "Nested Loops")
            return false;

        return true;
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
    /// In batch mode, operator times are self-contained (exclusive).
    /// In row mode, times are cumulative (include all children below).
    /// For parallel plans, we calculate self-time per-thread then take the max,
    /// avoiding cross-thread subtraction errors.
    /// Exchange operators accumulate downstream wait time (e.g. from spilling
    /// children) so their self-time is unreliable — see sql.kiwi/2021/03.
    /// </summary>
    private static long GetOperatorOwnElapsedMs(PlanNode node)
    {
        if (node.ActualExecutionMode == "Batch")
            return node.ActualElapsedMs;

        // Parallel plan with per-thread data: calculate self-time per thread
        if (node.PerThreadStats.Count > 1)
            return GetPerThreadOwnElapsed(node);

        // Serial row mode: subtract all direct children's elapsed time
        return GetSerialOwnElapsed(node);
    }

    /// <summary>
    /// Per-thread self-time calculation for parallel row mode operators.
    /// For each thread: self = parent_elapsed[t] - sum(children_elapsed[t]).
    /// Returns max across threads.
    /// </summary>
    private static long GetPerThreadOwnElapsed(PlanNode node)
    {
        // Build lookup: threadId -> parent elapsed for this node
        var parentByThread = new Dictionary<int, long>();
        foreach (var ts in node.PerThreadStats)
            parentByThread[ts.ThreadId] = ts.ActualElapsedMs;

        // Build lookup: threadId -> sum of all direct children's elapsed
        var childSumByThread = new Dictionary<int, long>();
        foreach (var child in node.Children)
        {
            var childNode = child;

            // Exchange operators have unreliable times — look through to their child
            if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
                childNode = child.Children.OrderByDescending(c => c.ActualElapsedMs).First();

            foreach (var ts in childNode.PerThreadStats)
            {
                childSumByThread.TryGetValue(ts.ThreadId, out var existing);
                childSumByThread[ts.ThreadId] = existing + ts.ActualElapsedMs;
            }
        }

        // Self-time per thread = parent - children, take max across threads
        var maxSelf = 0L;
        foreach (var (threadId, parentMs) in parentByThread)
        {
            childSumByThread.TryGetValue(threadId, out var childMs);
            var self = Math.Max(0, parentMs - childMs);
            if (self > maxSelf) maxSelf = self;
        }

        return maxSelf;
    }

    /// <summary>
    /// Serial row mode self-time: subtract all direct children's elapsed.
    /// Exchange children are skipped through to their real child.
    /// </summary>
    private static long GetSerialOwnElapsed(PlanNode node)
    {
        var totalChildElapsed = 0L;
        foreach (var child in node.Children)
        {
            var childElapsed = child.ActualElapsedMs;

            // Exchange operators have unreliable times — skip to their child
            if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
                childElapsed = child.Children.Max(c => c.ActualElapsedMs);

            totalChildElapsed += childElapsed;
        }

        return Math.Max(0, node.ActualElapsedMs - totalChildElapsed);
    }

    /// <summary>
    /// Calculates a Parallelism (exchange) operator's own elapsed time.
    /// Exchange times are unreliable — they accumulate wait time caused by
    /// downstream operators (e.g. spilling sorts). This returns a best-effort
    /// value but callers should treat it with caution.
    /// </summary>
    private static long GetParallelismOperatorElapsedMs(PlanNode node)
    {
        if (node.Children.Count == 0)
            return node.ActualElapsedMs;

        if (node.PerThreadStats.Count > 1)
            return GetPerThreadOwnElapsed(node);

        var maxChildElapsed = node.Children.Max(c => c.ActualElapsedMs);
        return Math.Max(0, node.ActualElapsedMs - maxChildElapsed);
    }

    /// <summary>
    /// Quantifies the cost of work below a Filter operator by summing child subtree metrics.
    /// </summary>
    private static string QuantifyFilterImpact(PlanNode filterNode)
    {
        if (filterNode.Children.Count == 0)
            return "";

        var parts = new List<string>();

        // Rows input vs output — how many rows did the filter discard?
        var inputRows = filterNode.Children.Sum(c => c.ActualRows);
        if (filterNode.HasActualStats && inputRows > 0 && filterNode.ActualRows < inputRows)
        {
            var discarded = inputRows - filterNode.ActualRows;
            var pct = (double)discarded / inputRows * 100;
            parts.Add($"{discarded:N0} of {inputRows:N0} rows discarded ({pct:N0}%)");
        }

        // Logical reads across the entire child subtree
        long totalReads = 0;
        foreach (var child in filterNode.Children)
            totalReads += SumSubtreeReads(child);
        if (totalReads > 0)
            parts.Add($"{totalReads:N0} logical reads below");

        // Elapsed time: use the direct child's time (cumulative in row mode, includes its children)
        var childElapsed = filterNode.Children.Max(c => c.ActualElapsedMs);
        if (childElapsed > 0)
            parts.Add($"{childElapsed:N0}ms elapsed below");

        if (parts.Count == 0)
            return "";

        return string.Join("\n", parts.Select(p => "• " + p));
    }

    private static long SumSubtreeReads(PlanNode node)
    {
        long reads = node.ActualLogicalReads;
        foreach (var child in node.Children)
            reads += SumSubtreeReads(child);
        return reads;
    }

    /// <summary>
    /// Determines whether a row estimate mismatch actually caused observable harm.
    /// Returns a description of the harm, or null if the bad estimate is benign.
    /// </summary>
    private static string? AssessEstimateHarm(PlanNode node, double ratio)
    {
        // Root node: no parent to harm.
        // The synthetic statement root (SELECT/INSERT/etc.) has NodeId == -1.
        if (node.Parent == null || node.Parent.NodeId == -1)
            return null;

        // The node itself has a spill — bad estimate caused bad memory grant
        if (HasSpillWarning(node))
        {
            return ratio >= 10.0
                ? "The underestimate likely caused an insufficient memory grant, leading to a spill to TempDB."
                : "The overestimate may have caused an excessive memory grant, wasting workspace memory.";
        }

        // Sort/Hash that did NOT spill — estimate was wrong but no observable harm
        if ((node.PhysicalOp.Contains("Sort", StringComparison.OrdinalIgnoreCase) ||
             node.PhysicalOp.Contains("Hash", StringComparison.OrdinalIgnoreCase)) &&
            !HasSpillWarning(node))
        {
            return null;
        }

        // The node is a join — bad estimate means wrong join type or excessive work
        // Adaptive joins (2017+) switch strategy at runtime, so the estimate didn't lock in a bad choice.
        if (node.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase) && !node.IsAdaptive)
        {
            return ratio >= 10.0
                ? "The underestimate may have caused the optimizer to make poor choices."
                : "The overestimate may have caused the optimizer to make poor choices.";
        }

        // Walk up to check if a parent was harmed by this bad estimate
        var ancestor = node.Parent;
        while (ancestor != null)
        {
            // Transparent operators — skip through
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
            // Adaptive joins handle this at runtime, so skip them.
            if (ancestor.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase))
            {
                if (ancestor.IsAdaptive)
                    return null; // Adaptive join self-corrects — no harm

                return ratio >= 10.0
                    ? "The underestimate may have caused the optimizer to make poor choices."
                    : "The overestimate may have caused the optimizer to make poor choices.";
            }

            // Parent Sort/Hash that spilled — downstream bad estimate caused the spill
            if (HasSpillWarning(ancestor))
            {
                return ratio >= 10.0
                    ? $"The underestimate contributed to {ancestor.PhysicalOp} (Node {ancestor.NodeId}) spilling to TempDB."
                    : $"The overestimate contributed to {ancestor.PhysicalOp} (Node {ancestor.NodeId}) receiving an excessive memory grant.";
            }

            // Parent Sort/Hash with no spill — benign
            if (ancestor.PhysicalOp.Contains("Sort", StringComparison.OrdinalIgnoreCase) ||
                ancestor.PhysicalOp.Contains("Hash", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            // Any other operator — stop walking
            break;
        }

        // Default: the estimate is off but we can't identify specific harm
        return null;
    }

    /// <summary>
    /// Checks if a node has any spill-related warnings (Sort/Hash/Exchange spills).
    /// </summary>
    private static bool HasSpillWarning(PlanNode node)
    {
        return node.Warnings.Any(w => w.SpillDetails != null);
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength] + "...";
    }

    [GeneratedRegex(@"\b(CONVERT_IMPLICIT|CONVERT|CAST|isnull|coalesce|datepart|datediff|dateadd|year|month|day|upper|lower|ltrim|rtrim|trim|substring|left|right|charindex|replace|len|datalength|abs|floor|ceiling|round|reverse|stuff|format)\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex FunctionInPredicateRegExp();
    [GeneratedRegex(@"\blike\b[^'""]*?N?'%", RegexOptions.IgnoreCase)]
    private static partial Regex LeadingWildcardLikeRegExp();
    [GeneratedRegex(@"\bCASE\s+(WHEN\b|$)", RegexOptions.IgnoreCase)]
    private static partial Regex CaseInPredicateRegExp();
    [GeneratedRegex(@"(?:\bWITH\s+|\,\s*)(\w+)\s+AS\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex CteDefinitionRegExp();
    [GeneratedRegex(@"\b(isnull|coalesce)\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex IsNullCoalesceRegExp();
    [GeneratedRegex(@"OPTIMIZE\s+FOR\s+UNKNOWN", RegexOptions.IgnoreCase)]
    private static partial Regex OptimizeForUnknownRegExp();
    [GeneratedRegex(@"\bNOT\s+IN\b", RegexOptions.IgnoreCase)]
    private static partial Regex NotInRegExp();
}
