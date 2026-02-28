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
                if (wasteRatio >= 10 && grant.GrantedMemoryKB >= 1048576)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Excessive Memory Grant",
                        Message = $"Granted {grant.GrantedMemoryKB:N0} KB but only used {grant.MaxUsedMemoryKB:N0} KB ({wasteRatio:F0}x overestimate). The unused memory is reserved and unavailable to other queries, causing them to wait. This is usually caused by overestimated row counts — update statistics or use OPTION (RECOMPILE) so the optimizer sees the real row counts.",
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
                    Message = $"Query waited {grant.GrantWaitTimeMs:N0}ms for a memory grant before it could start running. This means other queries were using all available workspace memory. Reduce memory consumption by fixing overestimated row counts (update statistics), simplifying sorts/hashes, or increasing server memory.",
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
                    Message = $"Query granted {grantMB:F0} MB of memory — this is a significant amount that blocks other queries from getting memory.{guidance} Reduce the data volume being sorted or hashed by filtering rows earlier, removing unnecessary columns from ORDER BY, or breaking the query into smaller steps.",
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
                Message = $"Query took {stmt.CompileCPUMs:N0}ms of CPU just to compile a plan (before any data was read). This is usually caused by too many joins, subqueries, or CTEs in a single statement. Break the query into smaller steps using #temp tables to reduce the search space the optimizer has to evaluate.",
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
                Message = $"Scalar UDF cost in this statement: {stmt.QueryUdfElapsedTimeMs:N0}ms elapsed, {stmt.QueryUdfCpuTimeMs:N0}ms CPU. Scalar UDFs run once per row and force single-threaded execution. Inline the UDF logic directly into the query. On SQL Server 2019+, scalar UDF inlining may handle this automatically.",
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
                        Message = $"Local variables detected: {names}. SQL Server cannot \"sniff\" the values of local variables at compile time, so it uses average statistics (density vector) instead of your actual values. This often produces bad row estimates. Fix: add OPTION (RECOMPILE) to the query so the optimizer sees the real values at runtime, or pass the values as stored procedure parameters instead of local variables.",
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
                Message = $"Rows are flowing through the entire plan only to be thrown away by this Filter. Move the filtering logic earlier — add the predicate to a WHERE clause or an index so rows are eliminated at the source, not after all the expensive work is done. Predicate: {Truncate(node.Predicate, 200)}",
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
                Message = $"Scalar UDF executing on this operator ({node.UdfElapsedTimeMs:N0}ms elapsed, {node.UdfCpuTimeMs:N0}ms CPU). Scalar UDFs run once per row, prevent parallelism, and hide their cost from the optimizer. Inline the UDF logic directly into the query, or on SQL Server 2019+ check if scalar UDF inlining is enabled (SELECT is_inlineable FROM sys.sql_modules).",
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
                    Message = $"Estimated {node.EstimateRows:N0} rows but actual 0 rows returned. SQL Server allocated resources for rows that never materialized. Update statistics on the underlying tables (UPDATE STATISTICS tablename WITH FULLSCAN), or if using parameters, the plan may be cached for different data — try OPTION (RECOMPILE).",
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
                        Message = $"Estimated {node.EstimateRows:N0} rows, actual {node.ActualRows:N0} ({factor:F0}x {direction}). Bad estimates cause SQL Server to choose wrong join types, memory grants, and parallelism. Update statistics (UPDATE STATISTICS tablename WITH FULLSCAN). If using local variables or parameters with skewed data, try OPTION (RECOMPILE).",
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
                Message = $"Scalar {type} UDF: {udf.FunctionName}. Scalar UDFs execute once per row and force the entire query to run single-threaded (no parallelism). Rewrite the UDF logic as inline SQL in the query. On SQL Server 2019+, scalar UDF inlining may do this automatically — check with SELECT is_inlineable FROM sys.sql_modules WHERE object_id = OBJECT_ID('{udf.FunctionName}').",
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
                var skewThreshold = node.PerThreadStats.Count == 2 ? 0.75 : 0.50;
                if (skewRatio >= skewThreshold)
                {
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Parallel Skew",
                        Message = $"Thread {maxThread.ThreadId} processed {skewRatio:P0} of rows ({maxThread.ActualRows:N0}/{totalRows:N0}). The work is heavily skewed to one thread, so parallelism isn't helping much. This is usually caused by skewed data distribution. Check if the data can be partitioned more evenly, or if the query can be restructured to distribute work across threads.",
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
                Message = $"Key Lookup — SQL Server found the rows using a nonclustered index but had to go back to the clustered index to get additional columns. Add the missing columns as INCLUDE columns on the nonclustered index to make it a \"covering\" index and eliminate the lookup. Predicate: {Truncate(node.Predicate, 200)}",
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
                Message = $"{nonSargableReason} prevents SQL Server from using an index seek, forcing it to scan every row instead. Remove the function/conversion from the column side of the predicate — apply it to the parameter or literal instead. If that's not possible, create a computed column with the expression and index that. Predicate: {Truncate(node.Predicate!, 200)}",
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
                Message = $"SQL Server is reading every row in the table and then checking each one against this predicate. Create an index on the columns in the predicate to turn this scan into a seek. Predicate: {Truncate(node.Predicate, 200)}",
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
                    ? "Mismatched data types between the column and the parameter/literal. SQL Server is converting every row's value to compare, which prevents index seeks. Fix the parameter type in your application code to match the column type exactly (e.g., don't pass nvarchar to a varchar column, or int to a bigint column)."
                    : "CONVERT/CAST wrapping a column in the predicate. SQL Server must convert every row's value, which prevents index seeks and forces a scan. Move the conversion to the other side of the comparison — convert the parameter/literal instead of the column.";

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
                    Message = $"Lazy spool has low cache hit ratio ({source}): {rebinds:N0} rebinds (cache misses), {rewinds:N0} rewinds (cache hits) — {ratio}. The spool is caching results but rarely reusing them, so it's adding overhead for no benefit. This usually means the inner side of a Nested Loops join is being re-executed with different values each time. An index on the inner table's join columns may help.",
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
                    Message = $"Nested Loops inner side executed {innerChild.ActualExecutions:N0} times (DOP {dop}). That's {innerChild.ActualExecutions:N0} separate lookups into the inner table. For this many rows, a Hash Join or Merge Join would be more efficient. Check if bad row estimates on the outer side caused the optimizer to choose Nested Loops — update statistics or try OPTION (HASH JOIN) as a test.",
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
                    Message = $"Nested Loops inner side estimated to execute {innerChild.EstimateRebinds + 1:N0} times. That many separate lookups into the inner table is expensive. For this many rows, a Hash Join or Merge Join would be more efficient. Check if the outer side row estimate is accurate — update statistics or try OPTION (HASH JOIN) as a test.",
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
                    ? $"Many-to-many Merge Join — SQL Server created a worktable in TempDB ({node.ActualLogicalReads:N0} logical reads) because both sides have duplicate values in the join columns. Reduce duplicates by adding columns to the join, filtering earlier, or using a Hash Join instead (OPTION (HASH JOIN) as a test)."
                    : "Many-to-many Merge Join — SQL Server will create a worktable in TempDB because both sides have duplicate values in the join columns. Reduce duplicates by adding columns to the join, filtering earlier, or using a Hash Join instead.",
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
                Message = "Table variable detected. SQL Server always estimates 1 row for table variables regardless of actual size, which causes bad join and memory grant decisions. Replace with a #temp table — temp tables have statistics so the optimizer can make informed choices. On SQL Server 2019+ with compatibility level 150+, table variable deferred compilation may fix this automatically.",
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
                Message = $"Table-valued function: {funcName}. Multi-statement TVFs have no statistics — SQL Server guesses 1 row (pre-2017) or 100 rows (2017+) regardless of actual size. This causes bad join choices and memory grants. Rewrite as an inline table-valued function (single SELECT statement) or move the logic directly into the query.",
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
                        Message = $"Top operator reads from {scanCandidate.PhysicalOp} (Node {scanCandidate.NodeId}) on the inner side of Nested Loops (Node {node.NodeId}).{predInfo} This scan runs once per outer row — potentially thousands or millions of times. Create an index on the join/filter columns with appropriate ordering to convert the scan into a seek.",
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
                    Message = $"CTE \"{cteName}\" is referenced {refCount} times. SQL Server re-executes the entire CTE each time it's referenced — it does not cache or materialize the results. Replace the CTE with a #temp table: SELECT ... INTO #temp FROM (...), then reference #temp in each place you currently use \"{cteName}\".",
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

    private static string Truncate(string value, int maxLength)
    {
        return value.Length <= maxLength ? value : value[..maxLength] + "...";
    }
}
