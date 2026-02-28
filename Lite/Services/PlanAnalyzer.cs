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
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Excessive Memory Grant",
                        Message = $"Granted {grant.GrantedMemoryKB:N0} KB but only used {grant.MaxUsedMemoryKB:N0} KB ({wasteRatio:F0}x overestimate). The unused memory is reserved and unavailable to other queries.",
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
            Regex.IsMatch(stmt.StatementText, @"OPTIMIZE\s+FOR\s+UNKNOWN", RegexOptions.IgnoreCase))
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Optimize For Unknown",
                Message = "OPTIMIZE FOR UNKNOWN forces average density estimates for all parameters instead of using the actual sniffed values. This rarely produces a better plan — it just trades one bad estimate for a different bad estimate. Address the root cause: add better indexes so the plan is less sensitive to parameter values, use OPTION (RECOMPILE) for volatile parameters, or restructure the query.",
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
                if (ratio <= 1.3)
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
                Message = $"Filter operator discarding rows late in the plan. Rows were read, joined, or processed only to be thrown away here. Predicate: {Truncate(node.Predicate, 200)}",
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
        if (node.HasActualStats && node.EstimateRows > 0)
        {
            if (node.ActualRows == 0)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Row Estimate Mismatch",
                    Message = $"Estimated {node.EstimateRows:N0} rows but actual 0 rows returned. SQL Server allocated resources for rows that never materialized.",
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
                        Message = $"Estimated {node.EstimateRows:N0} rows, actual {node.ActualRows:N0} ({factor:F0}x {direction}). Bad estimates cause SQL Server to choose wrong join types, memory grants, and parallelism.",
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
        if (node.PhysicalOp == "RID Lookup")
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
                Message = $"Key Lookup — SQL Server found rows via a nonclustered index but had to go back to the clustered index for additional columns. Alter the nonclustered index to add the predicate column as a key column or as an INCLUDE column. Predicate: {Truncate(node.Predicate, 200)}",
                Severity = PlanWarningSeverity.Warning
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
                    "Leading wildcard LIKE (e.g. LIKE '%text') prevents an index seek — SQL Server must scan every row. If possible, use full-text indexing or reverse the search pattern.",
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
                Message = $"{nonSargableAdvice} Predicate: {Truncate(node.Predicate!, 200)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 11: Scan with residual predicate (skip if non-SARGable already flagged)
        // A PROBE() alone is just a bitmap filter — not a real residual predicate.
        if (nonSargableReason == null && IsRowstoreScan(node) && !string.IsNullOrEmpty(node.Predicate) &&
            !IsProbeOnly(node.Predicate))
        {
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scan With Predicate",
                Message = $"Scan with residual predicate — SQL Server is reading every row and filtering after the fact. Create an index on the predicate columns. Predicate: {Truncate(node.Predicate, 200)}",
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
        if (node.LogicalOp == "Lazy Spool")
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
        if (node.PhysicalOp == "Nested Loops" &&
            node.LogicalOp.Contains("Join", StringComparison.OrdinalIgnoreCase) &&
            node.Children.Count >= 2)
        {
            var innerChild = node.Children[1];

            if (innerChild.HasActualStats && innerChild.ActualExecutions > 100000)
            {
                var dop = stmt.DegreeOfParallelism > 0 ? stmt.DegreeOfParallelism : 1;
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Nested Loops High Executions",
                    Message = $"Nested Loops inner side executed {innerChild.ActualExecutions:N0} times (DOP {dop}). This is likely caused by parameter sniffing or a bad row estimate on the outer side — the optimizer chose Nested Loops expecting far fewer rows.",
                    Severity = innerChild.ActualExecutions > 1000000
                        ? PlanWarningSeverity.Critical
                        : PlanWarningSeverity.Warning
                });
            }
            else if (!innerChild.HasActualStats && innerChild.EstimateRebinds > 100000)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Nested Loops High Executions",
                    Message = $"Nested Loops inner side estimated to execute {innerChild.EstimateRebinds + 1:N0} times. This may be caused by parameter sniffing or a poor estimate on the outer side.",
                    Severity = innerChild.EstimateRebinds > 1000000
                        ? PlanWarningSeverity.Critical
                        : PlanWarningSeverity.Warning
                });
            }
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
            node.ObjectName.StartsWith("@"))
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

                if (IsScanOperator(scanCandidate))
                {
                    var predInfo = !string.IsNullOrEmpty(scanCandidate.Predicate)
                        ? " The scan has a residual predicate, so it may read many rows before the Top is satisfied."
                        : "";
                    inner.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Top Above Scan",
                        Message = $"Top operator reads from {scanCandidate.PhysicalOp} (Node {scanCandidate.NodeId}) on the inner side of Nested Loops (Node {node.NodeId}).{predInfo} Create an index on the predicate columns to convert the scan into a seek.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 26: Row Goal (informational) — optimizer reduced estimate due to TOP/EXISTS/IN
        if (node.EstimateRowsWithoutRowGoal > 0 && node.EstimateRows > 0 &&
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
        if (node.PhysicalOp == "Row Count Spool")
        {
            var rewinds = node.HasActualStats ? (double)node.ActualRewinds : node.EstimateRewinds;
            if (rewinds > 10000 && HasNotInPattern(node, stmt))
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Row Count Spool (NOT IN)",
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
    /// and a parent Nested Loops Anti Semi Join has an IS NULL residual predicate.
    /// </summary>
    private static bool HasNotInPattern(PlanNode spoolNode, PlanStatement stmt)
    {
        // Check statement text for NOT IN
        if (string.IsNullOrEmpty(stmt.StatementText) ||
            !Regex.IsMatch(stmt.StatementText, @"\bNOT\s+IN\b", RegexOptions.IgnoreCase))
            return false;

        // Walk up to find the parent Nested Loops Anti Semi Join with IS NULL predicate
        var parent = spoolNode.Parent;
        while (parent != null)
        {
            if (parent.PhysicalOp == "Nested Loops" &&
                parent.LogicalOp.Contains("Anti Semi", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrEmpty(parent.Predicate) &&
                parent.Predicate.Contains("IS NULL", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
            parent = parent.Parent;
        }

        return false;
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

        // Expect TopN Sort
        if (parent == null || parent.LogicalOp != "TopN Sort")
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

    private static long GetParallelismOperatorElapsedMs(PlanNode node)
    {
        if (node.Children.Count == 0)
            return node.ActualElapsedMs;

        var maxChildElapsed = node.Children.Max(c => c.ActualElapsedMs);
        return Math.Max(0, node.ActualElapsedMs - maxChildElapsed);
    }

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength] + "...";
    }
}
