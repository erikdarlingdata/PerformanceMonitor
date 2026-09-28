using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Identity of the object a scan or seek predicate belongs to — enough to tell its own columns
/// apart from a column on another table, or an outer reference a Nested Loops join passed it one
/// row at a time. Built once in <see cref="PlanAnalyzer"/>'s <c>DetectNonSargablePredicate</c> and
/// threaded down through <c>DetectNonSargablePattern</c>. Internal, not private, so string-level
/// tests can build one directly instead of loading a fixture plan.
/// </summary>
/// <param name="Alias">The scan's alias, or null when it has none. The parser strips brackets.</param>
/// <param name="Table">
/// The last part of the scan's object name — a real table's bare name, a temp table cleaned to
/// <c>#t</c>, or a table variable's own <c>@tv</c> (its ObjectName never carries a schema, so this
/// is the whole name).
/// </param>
/// <param name="IsTableVariable">Mirrors <c>PlanAnalyzer.IsTableVariable(node)</c>.</param>
/// <param name="BareOuterReferences">
/// Bare column names — never a real table's or a temp table's, always another unaliased table
/// variable's — that reach this scan as an outer reference rather than one of its own columns. Only
/// populated by ancestor Nested Loops whose INNER input holds this scan; see
/// <c>PlanAnalyzer.CollectBareOuterReferences</c>.
/// </param>
internal readonly record struct ScanIdentity(
    string? Alias,
    string? Table,
    bool IsTableVariable,
    IReadOnlySet<string> BareOuterReferences);

/// <summary>
/// Post-parse analysis pass that walks a parsed plan tree and adds warnings
/// for common performance anti-patterns. Called after ShowPlanParser.Parse().
/// </summary>
public static partial class PlanAnalyzer
{
    private static readonly Regex FunctionInPredicateRegex = FunctionInPredicateRegExp();

    private static readonly Regex LeadingWildcardLikeRegex = LeadingWildcardLikeRegExp();

    private static readonly Regex CaseInPredicateRegex = CaseInPredicateRegExp();


    private static readonly Regex IsNullCoalesceRegex = IsNullCoalesceRegExp();

    private static readonly Regex ConvertImplicitRegex = ConvertImplicitRegExp();

    // A column reference in a ScalarString is multi-part bracket-qualified ([schema].[table]).
    // A variable is a single bracket pair with an @ prefix ([@0]), so excluding @ from the first
    // part is what separates the two.
    private static readonly Regex ColumnReferenceRegex = ColumnReferenceRegExp();

    // An optimizer-generated expression name in a ScalarString ([Expr1003]) — a computed value,
    // never an actual column, even on a table variable scan where a bare name is otherwise read
    // as a column.
    private static readonly Regex ExpressionColumnRegex = ExpressionColumnRegExp();

    // One bracket part of a name BracketedNameRegex already matched whole — [schema], [table],
    // [col], each on its own — so IsColumnReference can split "[db].[schema].[table].[col]" or
    // "[alias].[col]" into parts and read off the one right before the column, the owner.
    private static readonly Regex NamePartRegex = NamePartRegExp();

    // A name in a ScalarString: one bracketed part or a dotted chain of them ([@p1], [Expr1003],
    // [db].[dbo].[T].[c]). A name followed by ( is a function call. String literals are matched
    // first, so a bracket inside one ('[x]') is never read as a name. Only a match with the name
    // group is a name.
    private static readonly Regex BracketedNameRegex = BracketedNameRegExp();

    // The operator a comparison turns on in a ScalarString: >=, <=, <>, !=, >, <, = or like.
    // Without like, [col] like upper([@p]) had no operator at all, fell to the assume-the-worst
    // default, and a function on the pattern was reported as a function on the column.
    private static readonly Regex ComparisonOperatorRegex = ComparisonOperatorRegExp();

    // What joins one comparison to the next in a compound predicate. String literals and
    // bracketed identifiers are matched first, so an AND inside one of them (N'Tom AND Jerry',
    // [Terms and Conditions]) is consumed whole and never reaches the capture group. Only a
    // Groups[1] match is a real operator.
    private static readonly Regex LogicalOperatorRegex = LogicalOperatorRegExp();

    /// <summary>
    /// #4512 follow-up: <c>ShowPlanParser.Parse</c> now returns trees up to <c>MaxParseDepth</c>
    /// (1,000) levels deep, parsed on its own dedicated 32 MB thread. This walk runs on
    /// whichever thread the CALLER is on instead — the Darling service's analysis pass
    /// (thread-pool, ~1.5 MB), the plan viewer's WPF UI thread (1 MB), or the analyze_plan_xml
    /// / analyze_query_plan / analyze_query_store_plan MCP tools and the web host that front
    /// them (thread-pool). Measured directly against a depth-999 tree shaped like this walk's
    /// own recursion (<see cref="AnalyzeNodeTree"/>/<see cref="CheckForTableVariables"/>): it
    /// survives on a 1 MB caller thread down to roughly 130 KB of stack, and on a 1.5 MB caller
    /// down to roughly 110 KB — both with well over 2x margin below the smallest real caller, so
    /// unlike the parser, this walk needs no dedicated thread of its own.
    /// </summary>
    public static void Analyze(ParsedPlan plan, CancellationToken cancellationToken = default)
    {
        /* #4514: every statement, including the ones inside a stored procedure or UDF body.
           This used to walk batch.Statements alone, so an EXEC <procedure> plan analyzed as a
           single statement with nothing to say about the statements actually doing the work.
           #4512: cancellationToken is checked per statement (the EnumerateAll loop below) and
           down the node-tree walk, so a caller cancelling mid-analysis stops the walk instead of
           running the analyzer to completion against a plan nobody is waiting for. */
        foreach (var stmt in PlanStatements.EnumerateAll(plan))
        {
            cancellationToken.ThrowIfCancellationRequested();
            AnalyzeStatement(stmt);

            if (stmt.RootNode != null)
                AnalyzeNodeTree(stmt.RootNode, stmt, cancellationToken);
        }
    }

    private static void AnalyzeStatement(PlanStatement stmt)
    {
        // Rule 3: Serial plan with reason
        // Skip: cost < 1 (CTFP is an integer so cost < 1 can never go parallel),
        // TRIVIAL optimization (can't go parallel anyway),
        // and 0ms actual elapsed time (not worth flagging).
        if (!string.IsNullOrEmpty(stmt.NonParallelPlanReason)
            && stmt.StatementSubTreeCost >= 1.0
            && stmt.StatementOptmLevel != "TRIVIAL"
            && !(stmt.QueryTimeStats != null && stmt.QueryTimeStats.ElapsedTimeMs == 0))
        {
            var reason = stmt.NonParallelPlanReason switch
            {
                // User/config forced serial
                "MaxDOPSetToOne" => "MAXDOP is set to 1",
                "QueryHintNoParallelSet" => "OPTION (MAXDOP 1) hint forces serial execution",
                "ParallelismDisabledByTraceFlag" => "Parallelism disabled by trace flag",

                // Passive — optimizer chose serial, nothing wrong
                "EstimatedDOPIsOne" => "Estimated DOP is 1 (the plan's estimated cost was below the cost threshold for parallelism)",

                // Edition/environment limitations
                "NoParallelPlansInDesktopOrExpressEdition" => "Express/Desktop edition does not support parallelism",
                "NoParallelCreateIndexInNonEnterpriseEdition" => "Parallel index creation requires Enterprise edition",
                "NoParallelPlansDuringUpgrade" => "Parallel plans disabled during upgrade",
                "NoParallelForPDWCompilation" => "Parallel plans not supported for PDW compilation",
                "NoParallelForCloudDBReplication" => "Parallel plans not supported during cloud DB replication",

                // Query constructs that block parallelism (actionable)
                "CouldNotGenerateValidParallelPlan" => "Optimizer could not generate a valid parallel plan. Common causes: scalar UDFs, inserts into table variables, certain system functions, or OPTION (MAXDOP 1) hints",
                "TSQLUserDefinedFunctionsNotParallelizable" => "T-SQL scalar UDF prevents parallelism. Rewrite as an inline table-valued function, or on SQL Server 2019+ check if the UDF is eligible for automatic inlining",
                "CLRUserDefinedFunctionRequiresDataAccess" => "CLR UDF with data access prevents parallelism",
                "NonParallelizableIntrinsicFunction" => "Non-parallelizable intrinsic function in the query",
                "TableVariableTransactionsDoNotSupportParallelNestedTransaction" => "Table variable transaction prevents parallelism. Consider using a #temp table instead",
                "UpdatingWritebackVariable" => "Updating a writeback variable prevents parallelism",
                "DMLQueryReturnsOutputToClient" => "DML with OUTPUT clause returning results to client prevents parallelism",
                "MixedSerialAndParallelOnlineIndexBuildNotSupported" => "Mixed serial/parallel online index build not supported",
                "NoRangesResumableCreate" => "Resumable index create cannot use parallelism for this operation",

                // Cursor limitations
                "NoParallelCursorFetchByBookmark" => "Cursor fetch by bookmark cannot use parallelism",
                "NoParallelDynamicCursor" => "Dynamic cursors cannot use parallelism",
                "NoParallelFastForwardCursor" => "Fast-forward cursors cannot use parallelism",

                // Memory-optimized / natively compiled
                "NoParallelForMemoryOptimizedTables" => "Memory-optimized tables do not support parallel plans",
                "NoParallelForDmlOnMemoryOptimizedTable" => "DML on memory-optimized tables cannot use parallelism",
                "NoParallelForNativelyCompiledModule" => "Natively compiled modules do not support parallelism",

                // Remote queries
                "NoParallelWithRemoteQuery" => "Remote queries cannot use parallelism",
                "NoRemoteParallelismForMatrix" => "Remote parallelism not available for this query shape",

                _ => stmt.NonParallelPlanReason
            };

            var isActionable = stmt.NonParallelPlanReason is
                "MaxDOPSetToOne" or "QueryHintNoParallelSet" or "ParallelismDisabledByTraceFlag"
                or "CouldNotGenerateValidParallelPlan"
                or "TSQLUserDefinedFunctionsNotParallelizable"
                or "CLRUserDefinedFunctionRequiresDataAccess"
                or "NonParallelizableIntrinsicFunction"
                or "TableVariableTransactionsDoNotSupportParallelNestedTransaction"
                or "UpdatingWritebackVariable"
                or "DMLQueryReturnsOutputToClient"
                or "NoParallelCursorFetchByBookmark"
                or "NoParallelDynamicCursor"
                or "NoParallelFastForwardCursor"
                or "NoParallelWithRemoteQuery"
                or "NoRemoteParallelismForMatrix";

            // MaxDOPSetToOne needs special handling: check whether the user explicitly
            // set MAXDOP 1 in the query text, or if it's a server/db/RG setting.
            // SQL Server truncates StatementText at ~4,000 characters in plan XML.
            if (stmt.NonParallelPlanReason == "MaxDOPSetToOne")
            {
                var text = MaskCommentsAndLiterals(stmt.StatementText); // #4524
                var hasMaxdop1InText = Regex.IsMatch(text, @"MAXDOP\s+1\b", RegexOptions.IgnoreCase);
                var isTruncated = stmt.IsTextTruncated;

                if (hasMaxdop1InText)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Serial Plan",
                        Message = $"Query running serially: {reason}.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
                else if (isTruncated)
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Serial Plan",
                        Message = $"Query running serially: {reason}. MAXDOP 1 may be set at the server, database, resource governor, or query level (query text was truncated).",
                        Severity = PlanWarningSeverity.Info
                    });
                }
                // else: not truncated, no MAXDOP 1 in text — server/db/RG setting, suppress entirely
            }
            else
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Serial Plan",
                    Message = $"Query running serially: {reason}.",
                    Severity = isActionable ? PlanWarningSeverity.Warning : PlanWarningSeverity.Info
                });
            }
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
                    var message = $"Granted {grantMB:N0} MB but only used {usedMB:N0} MB ({wasteRatio:F0}x overestimate). The unused memory is reserved and unavailable to other queries.";

                    // Note adaptive joins that chose Nested Loops at runtime — the grant
                    // was sized for a hash join that never happened.
                    if (stmt.RootNode != null && HasAdaptiveJoinChoseNestedLoop(stmt.RootNode))
                        message += " An adaptive join in this plan executed as a Nested Loop at runtime — the memory grant was sized for the hash join alternative that wasn't used.";

                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Excessive Memory Grant",
                        Message = message,
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

            // Large memory grant with top consumers
            if (grant.GrantedMemoryKB >= 1048576 && stmt.RootNode != null)
            {
                var consumers = new List<string>();
                FindMemoryConsumers(stmt.RootNode, consumers);

                var grantMB = grant.GrantedMemoryKB / 1024.0;
                var guidance = "";
                if (consumers.Count > 0)
                {
                    // Show only the top 3 consumers — listing 20+ is noise
                    var shown = consumers.Take(3);
                    var remaining = consumers.Count - 3;
                    guidance = $" Largest consumers: {string.Join(", ", shown)}";
                    if (remaining > 0)
                        guidance += $", and {remaining} more";
                    guidance += ".";
                }

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
                Message = $"Scalar UDF cost in this statement: {stmt.QueryUdfElapsedTimeMs:N0}ms elapsed, {stmt.QueryUdfCpuTimeMs:N0}ms CPU. Scalar UDFs run once per row and prevent parallelism. Options: rewrite as an inline table-valued function, assign the result to a variable if only one row is needed, dump results to a #temp table and apply the UDF to the final result set, or on SQL Server 2019+ check if the UDF is eligible for automatic scalar UDF inlining.",
                Severity = stmt.QueryUdfElapsedTimeMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 20: Local variables without RECOMPILE
        // Parameters with no CompiledValue are likely local variables — the optimizer
        // cannot sniff their values and uses density-based ("unknown") estimates.
        // Skip statements with cost < 1 (can't go parallel, estimate quality rarely matters).
        if (stmt.Parameters.Count > 0 && stmt.StatementSubTreeCost >= 1.0)
        {
            var unsnifffedParams = stmt.Parameters
                .Where(p => string.IsNullOrEmpty(p.CompiledValue))
                .ToList();

            if (unsnifffedParams.Count > 0)
            {
                var hasRecompile = MaskCommentsAndLiterals(stmt.StatementText) // #4524
                    .Contains("RECOMPILE", StringComparison.OrdinalIgnoreCase);
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

        // Rule 21 (CTE referenced multiple times) removed: for actual plans, SQL Server
        // runtime stats show exactly where time was spent, so a statement-text-pattern
        // warning about CTE reuse is guessing.

        // Rule 27: OPTIMIZE FOR UNKNOWN in statement text
        if (!string.IsNullOrEmpty(stmt.StatementText) &&
            OptimizeForUnknownRegExp().IsMatch(MaskCommentsAndLiterals(stmt.StatementText))) // #4524
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Optimize For Unknown",
                Message = "OPTIMIZE FOR UNKNOWN uses average density estimates instead of sniffed parameter values. This can help when parameter sniffing causes plan instability, but may produce suboptimal plans for skewed data distributions.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rules 25 (Ineffective Parallelism) and 31 (Parallel Wait Bottleneck) were removed.
        // The CPU:Elapsed ratio is now shown in the runtime summary, and wait stats speak
        // for themselves — no need for meta-warnings guessing at causes.

        // Rule 30: Missing index quality evaluation
        {
            // Detect duplicate suggestions for the same table
            var tableSuggestionCount = stmt.MissingIndexes
                .GroupBy(mi => $"{mi.Database}.{mi.Schema}.{mi.Table}", StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

            foreach (var mi in stmt.MissingIndexes)
            {
                var keyCount = mi.EqualityColumns.Count + mi.InequalityColumns.Count;
                var includeCount = mi.IncludeColumns.Count;
                var tableKey = $"{mi.Database}.{mi.Schema}.{mi.Table}";

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
            var referencingNodeIds = new List<int>();
            var modifyingNodeIds = new List<int>();
            CheckForTableVariables(stmt.RootNode, isModification, ref hasTableVar, ref modifiesTableVar,
                referencingNodeIds, modifyingNodeIds);

            if (hasTableVar && !modifiesTableVar)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Table Variable",
                    Message = "Table variable detected. Table variables lack column-level statistics, which causes bad row estimates, join choices, and memory grant decisions. Replace with a #temp table.",
                    Severity = PlanWarningSeverity.Warning,
                    OriginNodeIds = referencingNodeIds
                });
            }

            if (modifiesTableVar)
            {
                stmt.PlanWarnings.Add(new PlanWarning
                {
                    WarningType = "Table Variable",
                    Message = "This query modifies a table variable, which forces the entire plan to run single-threaded. SQL Server cannot use parallelism for modifications to table variables. Replace with a #temp table to allow parallel execution.",
                    Severity = PlanWarningSeverity.Critical,
                    OriginNodeIds = modifyingNodeIds
                });
            }
        }

        // Rule 36: Dynamic cursor. Dynamic cursors can prevent index usage
        // because they must tolerate underlying data changes between fetches, forcing
        // scans and extra work per fetch. Switching to FAST_FORWARD, STATIC, or KEYSET
        // often delivers a dramatic improvement.
        if (string.Equals(stmt.CursorActualType, "Dynamic", StringComparison.OrdinalIgnoreCase))
        {
            var cursorLabel = string.IsNullOrEmpty(stmt.CursorName) ? "Cursor" : $"Cursor \"{stmt.CursorName}\"";
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Dynamic Cursor",
                Message = $"{cursorLabel} is a dynamic cursor. Dynamic cursors tolerate underlying data changes between fetches, which prevents many index uses and forces extra work per fetch. If you don't need that semantic, switching to FAST_FORWARD (or STATIC / KEYSET, depending on requirements) typically gives a large performance improvement.",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Rule 37: CURSOR declaration without LOCAL. Default cursor scope
        // is GLOBAL in SQL Server, which puts cursors in a shared namespace and can
        // bloat the plan cache (Erik's writeup:
        // https://erikdarling.com/cursor-declarations-that-use-openjson-can-bloat-your-plan-cache/).
        if (!string.IsNullOrEmpty(stmt.StatementText))
        {
            var maskedText = MaskCommentsAndLiterals(stmt.StatementText); // #4524

            // DECLARE <name> [INSENSITIVE|SCROLL] CURSOR [qualifier(s)] FOR ...
            // In the T-SQL extended syntax, LOCAL/GLOBAL appear AFTER the CURSOR
            // keyword (only INSENSITIVE/SCROLL are legal before it), so the LOCAL
            // qualifier must be looked for between CURSOR and the FOR that introduces
            // the SELECT. Capturing tokens *before* CURSOR never sees LOCAL and would
            // fire on every cursor, including ones already declared LOCAL.
            var cursorDeclMatch = Regex.Match(
                maskedText,
                @"\bDECLARE\s+\w+\s+(?:INSENSITIVE\s+|SCROLL\s+)*CURSOR\b(.*?)\bFOR\b",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (cursorDeclMatch.Success)
            {
                var qualifiers = cursorDeclMatch.Groups[1].Value;
                if (!Regex.IsMatch(qualifiers, @"\bLOCAL\b", RegexOptions.IgnoreCase))
                {
                    stmt.PlanWarnings.Add(new PlanWarning
                    {
                        WarningType = "Cursor Missing LOCAL",
                        Message = "CURSOR declaration is missing the LOCAL keyword. Default cursor scope is GLOBAL, which puts the cursor in a shared namespace and can bloat the plan cache (see https://erikdarling.com/cursor-declarations-that-use-openjson-can-bloat-your-plan-cache/). Adding LOCAL is cheap and usually right.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 39: the plan's copy of the query text hit SQL Server's showplan cap.
        // Everything downstream that reads this text — advice, Copy Query Text, Open in Query
        // Editor — is working from a query that stops mid-statement.
        if (stmt.IsTextTruncated)
        {
            stmt.PlanWarnings.Add(new PlanWarning
            {
                WarningType = "Truncated Query Text",
                Message =
                    "SQL Server truncated this query's text at 4,000 characters when it wrote the plan, "
                    + "so the query shown here stops early and is not valid T-SQL on its own. "
                    + "Advice, copied text, and Open in Query Editor are all working from the shortened "
                    + "version. Go back to the original query text to re-run or format it.",
                Severity = PlanWarningSeverity.Info
            });
        }
    }

    // #4534: collects the operators it found, because this walk already knows exactly which ones
    // touched a table variable and used to throw that away. Two lists rather than one, since the
    // two warnings this feeds are about different operators: every operator referencing a table
    // variable, versus only the ones modifying it (which is what forces the plan serial).
    private static void CheckForTableVariables(PlanNode node, bool isModification,
        ref bool hasTableVar, ref bool modifiesTableVar,
        List<int>? referencingNodeIds = null, List<int>? modifyingNodeIds = null)
    {
        if (!string.IsNullOrEmpty(node.ObjectName) && node.ObjectName.StartsWith("@", StringComparison.OrdinalIgnoreCase))
        {
            hasTableVar = true;
            referencingNodeIds?.Add(node.NodeId);
            if (isModification && (node.PhysicalOp.Contains("Insert", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Update", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Delete", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Merge", StringComparison.OrdinalIgnoreCase)))
            {
                modifiesTableVar = true;
                modifyingNodeIds?.Add(node.NodeId);
            }
        }
        foreach (var child in node.Children)
            CheckForTableVariables(child, isModification, ref hasTableVar, ref modifiesTableVar,
                referencingNodeIds, modifyingNodeIds);
    }

    private static void AnalyzeNodeTree(PlanNode node, PlanStatement stmt, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        AnalyzeNode(node, stmt);

        foreach (var child in node.Children)
            AnalyzeNodeTree(child, stmt, cancellationToken);
    }

    private static void AnalyzeNode(PlanNode node, PlanStatement stmt)
    {
        // Rule 1: Filter operators — rows survived the tree just to be discarded
        // Quantify the impact by summing child subtree cost (reads, CPU, time).
        // Suppress when the filter's child subtree is trivial (low I/O, fast, cheap).
        if (node.PhysicalOp == "Filter" && !string.IsNullOrEmpty(node.Predicate)
            && node.Children.Count > 0)
        {
            // Gate: skip trivial filters based on actual stats or estimated cost
            bool isTrivial;
            if (node.HasActualStats)
            {
                long childReads = 0;
                foreach (var child in node.Children)
                    childReads += SumSubtreeReads(child);
                var childElapsed = node.Children.Max(c => c.ActualElapsedMs);
                isTrivial = childReads < 128 && childElapsed < 10;
            }
            else
            {
                var childCost = node.Children.Sum(c => c.EstimatedTotalSubtreeCost);
                isTrivial = childCost < 1.0;
            }

            if (!isTrivial)
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
        }

        // Rule 2: Eager Index Spools — optimizer building temporary indexes on the fly
        if (node.LogicalOp == "Eager Spool" &&
            node.PhysicalOp.Contains("Index", StringComparison.OrdinalIgnoreCase))
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
                Message = $"Scalar UDF executing on this operator ({node.UdfElapsedTimeMs:N0}ms elapsed, {node.UdfCpuTimeMs:N0}ms CPU). Scalar UDFs run once per row and prevent parallelism. Options: rewrite as an inline table-valued function, assign the result to a variable if only one row is needed, dump results to a #temp table and apply the UDF to the final result set, or on SQL Server 2019+ check if the UDF is eligible for automatic scalar UDF inlining.",
                Severity = node.UdfElapsedTimeMs >= 1000 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 5: Large estimate vs actual row gaps (actual plans only)
        // Only warn when the bad estimate actually causes observable harm:
        // - The node itself spilled (Sort/Hash with bad memory grant)
        // - A parent join may have chosen the wrong strategy
        // - Root nodes with no parent to harm are skipped
        // - Nodes whose only parents are Parallelism/Top/Sort (no spill) are skipped
        // An operator that never executed returned zero rows because it never ran, so its
        // zero is no evidence that the estimate was wrong.
        if (node.HasActualStats && node.EstimateRows > 0
            && node.ActualExecutions > 0
            && !node.Lookup) // Key lookups are point lookups (1 row per execution) — per-execution estimate is misleading
        {
            if (node.ActualRows == 0)
            {
                // Zero rows with a significant estimate — only warn on operators that
                // actually allocate meaningful resources (memory grants for hash/sort/spool).
                // Skip Parallelism, Bitmap, Compute Scalar, Filter, Concatenation, etc.
                // where 0 rows is just a consequence of upstream filtering.
                if (node.EstimateRows >= 100 && AllocatesResources(node))
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
                var executions = node.ActualExecutions;
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
        // Suppress when a Serial Plan finding is already on the statement for a UDF-related
        // reason — that finding already explains the issue, so this would be redundant.
        var serialPlanCoversUdf =
            (stmt.NonParallelPlanReason is
                "TSQLUserDefinedFunctionsNotParallelizable"
                or "CLRUserDefinedFunctionRequiresDataAccess"
                or "CouldNotGenerateValidParallelPlan")
            && stmt.PlanWarnings.Any(w => w.WarningType == "Serial Plan");
        if (!serialPlanCoversUdf)
        foreach (var udf in node.ScalarUdfs)
        {
            var type = udf.IsClrFunction ? "CLR" : "T-SQL";
            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scalar UDF",
                Message = $"Scalar {type} UDF: {udf.FunctionName}. Scalar UDFs run once per row and prevent parallelism. Options: rewrite as an inline table-valued function, assign the result to a variable if only one row is needed, dump results to a #temp table and apply the UDF to the final result set, or on SQL Server 2019+ check if the UDF is eligible for automatic scalar UDF inlining.",
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
                        w.Message += $" Operator time: {operatorMs:N0}ms ({pct * 100:N0}% of statement).";
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
                    w.Message += $" Operator time: {operatorMs:N0}ms ({pct * 100:N0}% of statement).";

                    if (pct >= 0.5)
                        w.Severity = PlanWarningSeverity.Critical;
                    else if (pct >= 0.1)
                        w.Severity = PlanWarningSeverity.Warning;
                }
            }
        }

        // Rule 8: Parallel thread skew (actual plans with per-thread stats)
        // Only warn when there are enough rows to meaningfully distribute across threads
        // Filter out thread 0 (coordinator) which typically does 0 rows in parallel operators
        if (node.PerThreadStats.Count > 1)
        {
            var workerThreads = node.PerThreadStats.Where(t => t.ThreadId > 0).ToList();
            if (workerThreads.Count < 2) workerThreads = node.PerThreadStats; // fallback
            var totalRows = workerThreads.Sum(t => t.ActualRows);
            var minRowsForSkew = workerThreads.Count * 1000;
            if (totalRows >= minRowsForSkew)
            {
                var maxThread = workerThreads.OrderByDescending(t => t.ActualRows).First();
                var skewRatio = (double)maxThread.ActualRows / totalRows;
                // At DOP 2, a 60/40 split is normal — use higher threshold
                var skewThreshold = workerThreads.Count <= 2 ? 0.80 : 0.50;
                if (skewRatio >= skewThreshold)
                {
                    var message = $"Thread {maxThread.ThreadId} processed {skewRatio * 100:N0}% of rows ({maxThread.ActualRows:N0}/{totalRows:N0}). Work is heavily skewed to one thread, so parallelism isn't helping much.";
                    var severity = PlanWarningSeverity.Warning;

                    // Batch mode sorts produce all output on a single thread by design
                    // unless their parent is a batch mode Window Aggregate
                    if (node.PhysicalOp == "Sort"
                        && (node.ActualExecutionMode ?? node.ExecutionMode) == "Batch"
                        && node.Parent?.PhysicalOp != "Window Aggregate")
                    {
                        message += " Batch mode sorts produce all output rows on a single thread by design, unless feeding a batch mode Window Aggregate.";
                        severity = PlanWarningSeverity.Info;
                    }
                    else
                    {
                        // Add practical context — skew is often hard to fix
                        message += " Common causes: uneven data distribution across partitions or hash buckets, or a scan/seek whose predicate sends most rows to one range. Reducing DOP or rewriting the query to avoid the skewed operation may help.";
                    }

                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Parallel Skew",
                        Message = message,
                        Severity = severity
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
        else if (node.Lookup)
        {
            var lookupMsg = "Key Lookup — SQL Server found rows via a nonclustered index but had to go back to the clustered index for additional columns.";

            // Show what columns the lookup is fetching
            if (!string.IsNullOrEmpty(node.OutputColumns))
                lookupMsg += $"\nColumns fetched: {Truncate(node.OutputColumns, 200)}";

            // Only call out the predicate if it actually filters rows
            if (!string.IsNullOrEmpty(node.Predicate))
            {
                var predicateFilters = node.HasActualStats && node.ActualExecutions > 0
                    && node.ActualRows < node.ActualExecutions;
                if (predicateFilters)
                    lookupMsg += $"\nResidual predicate (filtered {node.ActualExecutions - node.ActualRows:N0} rows): {Truncate(node.Predicate, 200)}";
            }

            lookupMsg += "\nTo eliminate the lookup, consider adding the needed columns as INCLUDE columns on the nonclustered index. This widens the index, so weigh the read benefit against write and storage overhead.";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Key Lookup",
                Message = lookupMsg,
                Severity = PlanWarningSeverity.Critical
            });
        }

        // Rule 12: Non-SARGable predicate on scan
        // Skip for 0-execution nodes — the operator never ran, so the warning is academic
        var nonSargableReason = (node.HasActualStats && node.ActualExecutions == 0)
            ? null : DetectNonSargablePredicate(node);
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
                _ when nonSargableReason.StartsWith("Function call", StringComparison.OrdinalIgnoreCase) =>
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
        // Skip for 0-execution nodes — the operator never ran
        if (nonSargableReason == null && IsRowstoreScan(node) && !string.IsNullOrEmpty(node.Predicate) &&
            !IsProbeOnly(node.Predicate) && !(node.HasActualStats && node.ActualExecutions == 0))
        {
            var displayPredicate = StripProbeExpressions(node.Predicate);
            var details = BuildScanImpactDetails(node, stmt);
            var severity = PlanWarningSeverity.Warning;

            // Elevate to Critical if the scan dominates the plan
            if (details.CostPct >= 90 || details.ElapsedPct >= 90)
                severity = PlanWarningSeverity.Critical;

            var message = "Scan with residual predicate — SQL Server is reading every row and filtering after the fact.";
            if (!string.IsNullOrEmpty(details.Summary))
                message += $" {details.Summary}";

            // If the statement is executing a dynamic cursor, that's usually
            // the reason an index didn't get used. Call it out so the user looks there
            // first rather than hunting for a missing index.
            var isDynamicCursor = string.Equals(stmt.CursorActualType, "Dynamic",
                StringComparison.OrdinalIgnoreCase);
            if (isDynamicCursor)
                message += " This query is running inside a dynamic cursor, which can prevent index usage; changing the cursor type (FAST_FORWARD / STATIC / KEYSET) often fixes scans like this without any indexing change.";
            else
                message += " Check that you have appropriate indexes.";

            // I/O waits specifically confirm the scan is hitting disk — elevate
            if (HasSignificantIoWaits(stmt.WaitStats) && details.CostPct >= 50
                && severity != PlanWarningSeverity.Critical)
                severity = PlanWarningSeverity.Critical;

            message += $"\nPredicate: {Truncate(displayPredicate, 200)}";

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Scan With Predicate",
                Message = message,
                Severity = severity
            });
        }

        // Rule 32: Cardinality misestimate on expensive scan — likely preventing index usage
        // When a scan dominates the plan AND the estimate is vastly higher than actual rows,
        // the optimizer chose a scan because it thought it needed most of the table.
        // With accurate estimates, it would likely seek instead.
        if (node.HasActualStats && IsRowstoreScan(node)
            && node.EstimateRows > 0 && node.ActualRows >= 0 && node.ActualRowsRead > 0)
        {
            var impact = BuildScanImpactDetails(node, stmt);
            var overestimateRatio = node.EstimateRows / Math.Max(1.0, node.ActualRows);
            var selectivity = (double)node.ActualRows / node.ActualRowsRead;

            // Fire when: scan is >= 50% of plan, estimate is >= 10x actual, and < 10% selectivity
            if ((impact.CostPct >= 50 || impact.ElapsedPct >= 50)
                && overestimateRatio >= 10.0
                && selectivity < 0.10)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Scan Cardinality Misestimate",
                    Message = $"Estimated {node.EstimateRows:N0} rows but only {node.ActualRows:N0} returned ({selectivity * 100:N3}% of {node.ActualRowsRead:N0} rows read). " +
                              $"The {overestimateRatio:N0}x overestimate likely caused the optimizer to choose a scan instead of a seek. " +
                              $"An index on the predicate columns could dramatically reduce I/O.",
                    Severity = PlanWarningSeverity.Critical
                });
            }
        }

        // Rule 34: Bare scan with no predicate — NC index or columnstore candidate.
        // When a Clustered Index Scan or heap Table Scan reads the full table with no
        // predicate but only outputs a few columns, a narrower nonclustered index could
        // cover the query with far less I/O. For analytical workloads, columnstore may
        // be a better fit regardless of column count.
        var isBareScanCandidate = (node.PhysicalOp == "Clustered Index Scan" || node.PhysicalOp == "Table Scan")
            && !node.Lookup
            && string.IsNullOrEmpty(node.Predicate)
            && !string.IsNullOrEmpty(node.OutputColumns);
        if (isBareScanCandidate)
        {
            var colCount = node.OutputColumns!.Split(',').Length;
            var isSignificant = node.HasActualStats
                ? GetOperatorOwnElapsedMs(node) > 0
                : node.CostPercent >= 20;

            if (isSignificant)
            {
                var scanKind = node.PhysicalOp == "Clustered Index Scan"
                    ? "Clustered index scan"
                    : "Heap table scan";

                if (colCount <= 3)
                {
                    // Narrow output: a nonclustered rowstore index can cover this cheaply.
                    var indexAdvice = node.PhysicalOp == "Clustered Index Scan"
                        ? "Consider a nonclustered index on the output columns (as key or INCLUDE) so SQL Server can read a narrower structure."
                        : "Consider a clustered or nonclustered index on the output columns so SQL Server can read a narrower structure.";

                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Bare Scan",
                        Message = $"{scanKind} reads the full table with no predicate, outputting {colCount} column(s): {Truncate(node.OutputColumns, 200)}. {indexAdvice} For analytical workloads, a columnstore index may be a better fit.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
                else
                {
                    // Wider output: rowstore NC index isn't a great fit (would have to
                    // carry too many columns), but columnstore doesn't care about column
                    // count. Suggest it for analytical / aggregate-style workloads.
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Bare Scan",
                        Message = $"{scanKind} reads the full table with no predicate, outputting {colCount} columns. A nonclustered rowstore index isn't a great fit for wide outputs, but if this is an analytical or aggregate-style query, a columnstore index (CCI or NCCI) can scan the same data far more cheaply — column count doesn't penalize columnstore the way it does rowstore indexes.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
        }

        // Rule 33: Estimated plan CE guess detection — scans with telltale default selectivity
        // When the optimizer uses a local variable or can't sniff, it falls back to density-based
        // guesses: 30% (equality), 10% (inequality), 9% (LIKE/between), ~16.43% (sqrt(30%)),
        // 1% (multi-inequality). On large tables, these guesses can hide the need for an index.
        if (!node.HasActualStats && IsRowstoreScan(node)
            && node.TableCardinality >= 100_000 && node.EstimateRows > 0
            && !string.IsNullOrEmpty(node.Predicate))
        {
            var impact = BuildScanImpactDetails(node, stmt);
            if (impact.CostPct >= 50)
            {
                var guessDesc = DetectCeGuess(node.EstimateRows, node.TableCardinality);
                if (guessDesc != null)
                {
                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Estimated Plan CE Guess",
                        Message = $"Estimated {node.EstimateRows:N0} rows from {node.TableCardinality:N0} row table — {guessDesc}. " +
                                  $"The optimizer may be using a default guess instead of accurate statistics. " +
                                  $"If actual selectivity is much lower, an index on the predicate columns could help significantly.",
                        Severity = PlanWarningSeverity.Warning
                    });
                }
            }
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
        if (node.LogicalOp == "Lazy Spool"
            && !node.PhysicalOp.Contains("Index", StringComparison.OrdinalIgnoreCase))
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
                .Where(c => c.PhysicalOp == "Constant Scan" ||
                            (c.PhysicalOp == "Compute Scalar" &&
                             c.Children.Any(gc => gc.PhysicalOp == "Constant Scan")))
                .ToList();

            // #4521: WHERE t.A IN (@p1, @p2) builds the same operator chain, as a dynamic seek
            // over the parameter values, and there is no join to rewrite. Only a lookup that
            // takes its value from another input's row makes the OR a join OR.
            if (constantScanBranches.Count >= 2 && IsOrExpansionChain(node) &&
                constantScanBranches.Any(LookupReadsAnotherInput))
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Join OR Clause",
                    Message = $"OR in a join predicate. SQL Server rewrote the OR as {constantScanBranches.Count} separate lookups, each evaluated independently — this multiplies the work on the inner side. Rewrite as separate queries joined with UNION ALL. For example, change \"FROM a JOIN b ON a.x = b.x OR a.y = b.y\" to \"FROM a JOIN b ON a.x = b.x UNION ALL FROM a JOIN b ON a.y = b.y\".",
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
            var isModificationOp = node.PhysicalOp.Contains("Insert", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Update", StringComparison.OrdinalIgnoreCase)
                || node.PhysicalOp.Contains("Delete", StringComparison.OrdinalIgnoreCase);

            node.Warnings.Add(new PlanWarning
            {
                WarningType = "Table Variable",
                Message = isModificationOp
                    ? "Modifying a table variable forces the entire plan to run single-threaded. Replace with a #temp table to allow parallel execution."
                    : "Table variable detected. Table variables lack column-level statistics, which causes bad row estimates, join choices, and memory grant decisions. Replace with a #temp table.",
                Severity = isModificationOp ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning
            });
        }

        // Rule 23: Table-valued functions
        // A function the engine supplies runs as the same operator: STRING_SPLIT, OPENJSON,
        // GENERATE_SERIES, and every DMV and DMF. Its Object names no database and no schema,
        // and a function a user wrote always has both. The advice below is about code the
        // user can rewrite, so the engine's own functions are skipped.
        var isEngineFunction = string.IsNullOrEmpty(node.DatabaseName) && string.IsNullOrEmpty(node.SchemaName);
        if (node.LogicalOp == "Table-valued function" && !isEngineFunction)
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
                        Message = $"{topLabel} reads from {FormatNodeRef(scanCandidate)}.{innerNote}{predInfo} An index on the ORDER BY columns could eliminate the scan and sort entirely.",
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
            // Require at least a 2x reduction to be worth mentioning — "1 to 1" or
            // tiny floating-point differences that display identically are noise
            if (reduction >= 2.0)
            {
                // If we have actual stats, check whether the row goal prediction was correct.
                // When actual rows <= the row goal estimate, the optimizer stopped early as planned — benign.
                var rowGoalWorked = false;
                if (node.HasActualStats)
                {
                    var executions = node.ActualExecutions > 0 ? node.ActualExecutions : 1;
                    var actualPerExec = (double)node.ActualRows / executions;
                    rowGoalWorked = actualPerExec <= node.EstimateRows;
                }

                if (!rowGoalWorked)
                {
                    // Try to identify the specific row goal cause from the statement text
                    var cause = IdentifyRowGoalCause(stmt.StatementText);

                    node.Warnings.Add(new PlanWarning
                    {
                        WarningType = "Row Goal",
                        Message = $"Row goal active: estimate reduced from {node.EstimateRowsWithoutRowGoal:N0} to {node.EstimateRows:N0} ({reduction:N0}x reduction) due to {cause}. The optimizer chose this plan shape expecting to stop reading early. If the query reads all rows anyway, the plan choice may be suboptimal.",
                        Severity = PlanWarningSeverity.Info
                    });
                }
            }
        }

        // Rule 28: Row Count Spool — NOT IN with nullable column
        // Pattern: Row Count Spool with high rewinds, child scan has IS NULL predicate,
        // and statement text contains NOT IN
        if ((node.PhysicalOp ?? "").Contains("Row Count Spool", StringComparison.Ordinal))
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
        // Skip for 0-execution nodes — the operator never ran
        if (!(node.HasActualStats && node.ActualExecutions == 0))
        foreach (var w in node.Warnings.ToList())
        {
            if (w.WarningType == "Implicit Conversion" && w.Message.StartsWith("Seek Plan", StringComparison.Ordinal))
            {
                w.Severity = PlanWarningSeverity.Critical;
                w.Message = $"Implicit conversion prevented an index seek, forcing a scan instead. Fix the data type mismatch: ensure the parameter or variable type matches the column type exactly. {w.Message}";
            }
        }

        // Rule 35: Expensive Operator — always show operators that take a significant
        // share of statement time even when no other rule has something to say. Threshold:
        // self-time >= 20% of statement elapsed. Only emits if no other warning is already
        // on the node, to avoid doubling up, and only once the statement itself has run long
        // enough (>= 1,000ms) that a 20% share means something — in a statement of a few ms,
        // one or two operators always take most of the time just because there's almost
        // nothing else to divide it among, so the share points at nothing. The benefit % is
        // just the self-time share.
        if (node.HasActualStats && node.Warnings.Count == 0
            && stmt.QueryTimeStats != null && stmt.QueryTimeStats.ElapsedTimeMs >= 1000)
        {
            var selfMs = GetOperatorOwnElapsedMs(node);
            var pct = (double)selfMs / stmt.QueryTimeStats.ElapsedTimeMs * 100;
            if (pct >= 20.0)
            {
                node.Warnings.Add(new PlanWarning
                {
                    WarningType = "Expensive Operator",
                    Message = $"{node.PhysicalOp} took {selfMs:N0}ms ({pct:N1}% of statement elapsed) but no specific rule identified a fix. Worth investigating: is the row volume necessary? Are upstream estimates driving this operator harder than it should be?",
                    Severity = pct >= 50 ? PlanWarningSeverity.Critical : PlanWarningSeverity.Warning,
                    MaxBenefitPercent = Math.Round(Math.Min(100.0, pct), 1)
                });
            }
        }

        // #4534: an operator warning's origin is the operator it is hanging off, so it is stamped
        // here rather than at each of the many sites above that add one. A rule you have to
        // remember at every construction site eventually gets forgotten, and the UI would quietly
        // lose a link that existed. This only fills what a rule left empty, so a rule that already
        // knows the operator that CAUSED the problem (rather than the one reporting it) keeps its
        // own answer.
        foreach (var warning in node.Warnings)
        {
            if (warning.OriginNodeIds.Count == 0)
                warning.OriginNodeIds.Add(node.NodeId);
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
            !NotInRegExp().IsMatch(MaskCommentsAndLiterals(stmt.StatementText))) // #4524
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
    /// True when a node scans or modifies a table variable rather than a real table. The scan's
    /// Object element renders a table variable's name as "[@tv]", where a real table always has a
    /// schema: "[db].[dbo].[t]" — so the leading @ alone tells them apart.
    /// </summary>
    private static bool IsTableVariable(PlanNode node) =>
        !string.IsNullOrEmpty(node.ObjectName) && node.ObjectName.StartsWith('@');

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

        return DetectNonSargablePattern(node.Predicate, BuildScanIdentity(node));
    }

    /// <summary>
    /// The <see cref="ScanIdentity"/> for a scan node, or null when it has no Object element at
    /// all — DetectNonSargablePattern falls back to its old, coarser behavior in that case rather
    /// than trying to match ownership against an identity with nothing in it.
    /// </summary>
    private static ScanIdentity? BuildScanIdentity(PlanNode node)
    {
        if (string.IsNullOrEmpty(node.ObjectName))
            return null;

        // ObjectName is "schema.table" (a table variable's has no schema, so it's just "@tv");
        // the owner a predicate names is always the bare table, never schema-qualified.
        var dot = node.ObjectName.LastIndexOf('.');
        var table = dot >= 0 ? node.ObjectName[(dot + 1)..] : node.ObjectName;

        return new ScanIdentity(node.ObjectAlias, table, IsTableVariable(node), CollectBareOuterReferences(node));
    }

    /// <summary>
    /// Bare column names that reach <paramref name="node"/> as an outer reference from another
    /// unaliased table variable, rather than one of node's own columns — the two look identical
    /// once rendered bare, so a predicate can't tell them apart by text alone.
    ///
    /// <para>Walks up through every Nested Loops ancestor whose INNER input (second child) holds
    /// node, the same as the plan diagram's own outer-vs-inner split, and keeps the OuterReferences
    /// entries with no ".". A table-qualified outer reference is rendered as "Table.Column" — that
    /// always names a different table than node's own bare columns, so only a dot-free entry can
    /// ever collide with one. A Nested Loops whose OUTER input holds node contributes nothing: that
    /// input is where the reference comes from, not where it is consumed, so node is not the one
    /// reading it.</para>
    /// </summary>
    private static HashSet<string> CollectBareOuterReferences(PlanNode node)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var child = node;
        var ancestor = node.Parent;

        while (ancestor != null)
        {
            if (ancestor.PhysicalOp == "Nested Loops" &&
                !string.IsNullOrEmpty(ancestor.OuterReferences) &&
                ancestor.Children.Count > 1 &&
                ancestor.Children[1] == child)
            {
                foreach (var reference in ancestor.OuterReferences.Split(", "))
                {
                    if (!reference.Contains('.'))
                        names.Add(reference);
                }
            }

            child = ancestor;
            ancestor = ancestor.Parent;
        }

        return names;
    }

    /// <summary>
    /// The pattern half of <see cref="DetectNonSargablePredicate"/>: which non-SARGable shape, if
    /// any, a predicate ScalarString has.
    ///
    /// <para>Internal so predicate shapes can be tested as raw strings. The shapes that matter
    /// (compound AND/OR predicates, date ranges, parenthesized groups, an AND inside a literal or a
    /// bracketed name) outnumber any sensible set of plan fixtures, and every one of them is decided
    /// entirely in this method and the helpers it calls.</para>
    ///
    /// <para><paramref name="identity"/> is the scanned object the caller has confirmed the predicate
    /// belongs to — null keeps today's coarser behavior unchanged for every existing caller: any
    /// dotted name counts as a column, and no bare name does. A real table or an aliased table
    /// variable always renders its own column dotted, at minimum [table].[col] or [alias].[col], but
    /// an unaliased table variable's own column has no dotted qualifier at all — see
    /// <see cref="ColumnReferenceRegex"/> and <see cref="IsColumnReference"/>.</para>
    /// </summary>
    internal static string? DetectNonSargablePattern(string predicate, ScanIdentity? identity = null)
    {
        // CASE expression in predicate — check first because CASE bodies
        // often contain CONVERT_IMPLICIT that isn't the root cause
        if (CaseInPredicateRegex.IsMatch(predicate))
            return "CASE expression in predicate";

        // CONVERT_IMPLICIT — most common non-SARGable pattern, but only when it converts the
        // COLUMN. Converting the parameter up to the column's type costs nothing.
        if (ConvertImplicitWrapsColumn(predicate, identity))
            return "Implicit conversion (CONVERT_IMPLICIT)";

        // ISNULL / COALESCE wrapping column — on the column side only. ISNULL(@p, 0) on the
        // parameter side is a runtime constant and seeks fine; flagging it contradicted this
        // warning's own "wrapping a column" message. col = ISNULL(@p, col) is still caught,
        // because the column sits inside the function, on its side of the comparison.
        foreach (Match isnullMatch in IsNullCoalesceRegex.Matches(predicate))
        {
            if (IsFunctionOnColumnSide(predicate, isnullMatch, identity))
                return "ISNULL/COALESCE wrapping column";
        }

        // Common function calls on columns — but only if the function wraps a column,
        // not a parameter/variable. Split on comparison operators to check which side
        // the function is on. Predicate format: [db].[schema].[table].[col]>func(...)
        // Every match, not just the first: a parameter-side CONVERT_IMPLICIT now falls through to
        // here, and it is skipped below. Taking only the first match would let a benign conversion
        // sitting to the left of a real function-on-column hide it.
        foreach (Match funcMatch in FunctionInPredicateRegex.Matches(predicate))
        {
            var funcName = funcMatch.Groups[1].Value.ToUpperInvariant();
            if (funcName != "CONVERT_IMPLICIT" && IsFunctionOnColumnSide(predicate, funcMatch, identity))
                return $"Function call ({funcName}) on column";
        }

        // Leading wildcard LIKE
        if (LeadingWildcardLikeRegex.IsMatch(predicate))
            return "Leading wildcard LIKE pattern";

        return null;
    }

    /// <summary>
    /// True when a lookup branch under an OR expansion's Concatenation builds its seek value from
    /// another input. A join OR does: in ON u.Id = p.OwnerUserId OR u.Id = p.LastEditorUserId the
    /// branches produce [Posts].[OwnerUserId] and [Posts].[LastEditorUserId], once per outer row.
    /// The dynamic seek for an IN list of parameters has the same operator shape, but its
    /// branches produce only parameters and literals ([@p1], (62)), which no outer row changes.
    /// </summary>
    private static bool LookupReadsAnotherInput(PlanNode branch)
    {
        var values = branch.PhysicalOp == "Constant Scan"
            ? branch.ConstantScanValues
            : branch.DefinedValues;

        // Nothing to read, so nothing proves a parameter list: keep the warning.
        if (string.IsNullOrEmpty(values))
            return true;

        return ReadsAnotherInput(values);
    }

    /// <summary>
    /// True when a ScalarString names anything other than a parameter or a variable: a column
    /// ([db].[dbo].[T].[c], or @tv.[c] as [v].[c] on a table variable) or an expression column
    /// ([Expr1003]). Function names ([dbo].[fn](...)) and string literals are skipped. An
    /// expression column counts too: an OR join on o.X + 1 renders its branches as [Expr1002],
    /// computed on the outer input. The Constant Scan under a lookup branch is normally empty,
    /// so the branch has no expression of its own to name, and a name that cannot be proved to
    /// be a parameter keeps the warning, as the shape check alone did. Internal so the shapes
    /// can be tested as raw strings.
    /// </summary>
    internal static bool ReadsAnotherInput(string scalarString)
    {
        foreach (Match match in BracketedNameRegex.Matches(scalarString))
        {
            if (!match.Groups["name"].Success || match.Groups["call"].Success)
                continue; // a string literal, or the name of a function

            var name = match.Groups["name"].Value;
            if (name.StartsWith("[@", StringComparison.Ordinal) &&
                !name.Contains("].[", StringComparison.Ordinal))
                continue; // a parameter or a variable: [@p1]

            return true;
        }

        return false;
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

        // If this Nested Loops is inside an Anti/Semi Join, this is a NOT IN/IN
        // subquery pattern (Merge Interval optimizing range lookups), not an OR expansion
        var nlParent = parent.Parent;
        if (nlParent != null && nlParent.LogicalOp != null &&
            nlParent.LogicalOp.Contains("Semi"))
            return false;

        return true;
    }

    /// <summary>
    /// Returns true if the plan contains an adaptive join that executed as a Nested Loop.
    /// Indicates a memory grant was sized for the hash alternative but never needed.
    /// </summary>
    private static bool HasAdaptiveJoinChoseNestedLoop(PlanNode node)
    {
        if (node.IsAdaptive && node.ActualJoinType != null
            && node.ActualJoinType.Contains("Nested", StringComparison.OrdinalIgnoreCase))
            return true;

        foreach (var child in node.Children)
            if (HasAdaptiveJoinChoseNestedLoop(child))
                return true;

        return false;
    }

    /// <summary>
    /// Finds Sort and Hash Match operators in the tree that consume memory.
    /// </summary>
    private static void FindMemoryConsumers(PlanNode node, List<string> consumers)
    {
        // Collect all consumers first, then sort by row count descending
        var raw = new List<(string Label, double Rows)>();
        FindMemoryConsumersRecursive(node, raw);

        foreach (var (label, _) in raw.OrderByDescending(c => c.Rows))
            consumers.Add(label);
    }

    private static void FindMemoryConsumersRecursive(PlanNode node, List<(string Label, double Rows)> consumers)
    {
        if (node.PhysicalOp.Contains("Sort", StringComparison.OrdinalIgnoreCase) &&
            !node.PhysicalOp.Contains("Spool", StringComparison.OrdinalIgnoreCase))
        {
            var rowCount = node.HasActualStats ? node.ActualRows : node.EstimateRows;
            var rows = node.HasActualStats
                ? $"{node.ActualRows:N0} actual rows"
                : $"{node.EstimateRows:N0} estimated rows";
            consumers.Add(($"Sort (Node {node.NodeId}, {rows})", rowCount));
        }
        else if (node.PhysicalOp.Contains("Hash", StringComparison.OrdinalIgnoreCase))
        {
            var rowCount = node.HasActualStats ? node.ActualRows : node.EstimateRows;
            var rows = node.HasActualStats
                ? $"{node.ActualRows:N0} actual rows"
                : $"{node.EstimateRows:N0} estimated rows";
            consumers.Add(($"Hash Match (Node {node.NodeId}, {rows})", rowCount));
        }

        foreach (var child in node.Children)
            FindMemoryConsumersRecursive(child, consumers);
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
    internal static long GetOperatorOwnElapsedMs(PlanNode node)
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
    /// Threads that actually did work. In a parallel plan thread 0 is the
    /// coordinator: it carries no rows, and its ActualElapsedMs is the wall clock
    /// of the whole parallel branch. Including it in a per-thread self-time
    /// calculation hands the operator the branch's entire duration.
    /// A serial plan has a single thread numbered 0, which IS a worker, so only
    /// exclude thread 0 when other threads exist.
    /// </summary>
    private static List<PerThreadRuntimeInfo> WorkThreads(PlanNode node)
    {
        var workers = node.PerThreadStats.Where(t => t.ThreadId > 0).ToList();
        return workers.Count > 0 ? workers : node.PerThreadStats;
    }

    /// <summary>
    /// Per-thread self-time calculation for parallel row mode operators.
    /// For each worker thread: self = parent[t] - sum(effective children[t]).
    /// Returns max across worker threads. Thread 0 (the coordinator) is excluded
    /// from the parent side by WorkThreads, and the child side looks through
    /// batch subtrees and pass-throughs the same way the serial path does.
    /// </summary>
    private static long GetPerThreadOwnElapsed(PlanNode node)
    {
        // Build lookup: threadId -> parent elapsed for this node (worker threads only)
        var parentByThread = new Dictionary<int, long>();
        foreach (var ts in WorkThreads(node))
            parentByThread[ts.ThreadId] = ts.ActualElapsedMs;

        // Build lookup: threadId -> sum of effective children's elapsed
        var childSumByThread = new Dictionary<int, long>();
        foreach (var child in node.Children)
            AddEffectiveChildElapsedByThread(child, childSumByThread);

        // Self-time per thread = parent - children, take max across worker threads
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
    /// Max per-thread self-CPU for this operator.
    /// Parallel: for each thread, self_cpu = thread_cpu - Σ same-thread child cpu; take max.
    /// Serial / single-thread: operator_cpu - Σ effective child cpu.
    /// Needed for external-wait benefit scoring (Joe's formula).
    /// </summary>
    internal static long GetOperatorMaxThreadOwnCpuMs(PlanNode node)
    {
        if (!node.HasActualStats || node.ActualCPUMs <= 0) return 0;

        if (node.PerThreadStats.Count > 1)
        {
            var parentByThread = new Dictionary<int, long>();
            foreach (var ts in WorkThreads(node))
                parentByThread[ts.ThreadId] = ts.ActualCPUMs;

            var childSumByThread = new Dictionary<int, long>();
            foreach (var child in node.Children)
                AddEffectiveChildCpuByThread(child, childSumByThread);

            var maxSelf = 0L;
            foreach (var (threadId, parentCpu) in parentByThread)
            {
                childSumByThread.TryGetValue(threadId, out var childCpu);
                var self = Math.Max(0, parentCpu - childCpu);
                if (self > maxSelf) maxSelf = self;
            }
            return maxSelf;
        }

        // Serial: operator_cpu - Σ effective child cpu
        var totalChildCpu = 0L;
        foreach (var child in node.Children)
            totalChildCpu += GetEffectiveChildCpuMs(child);
        return Math.Max(0, node.ActualCPUMs - totalChildCpu);
    }

    /// <summary>
    /// Per-thread mirror of <see cref="GetEffectiveChildCpuMs"/>, following the
    /// same look-through rules as <see cref="AddEffectiveChildElapsedByThread"/>
    /// (batch-mode subtree, pass-through nodes) so a row-mode operator can't be
    /// crowned above a batch subtree for CPU the same way it can't for elapsed.
    /// </summary>
    private static void AddEffectiveChildCpuByThread(PlanNode child, Dictionary<int, long> acc)
    {
        // Exchange operators have unreliable times — look through to their child
        if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
        {
            var dominant = child.Children.OrderByDescending(c => c.ActualCPUMs).First();
            AddEffectiveChildCpuByThread(dominant, acc);
            return;
        }

        var mode = child.ActualExecutionMode ?? child.ExecutionMode;
        if (mode == "Batch" && child.HasActualStats)
        {
            AddBatchSubtreeCpuByThread(child, acc);
            return;
        }

        if (child.HasActualStats && child.ActualCPUMs > 0)
        {
            foreach (var ts in WorkThreads(child))
            {
                acc.TryGetValue(ts.ThreadId, out var existing);
                acc[ts.ThreadId] = existing + ts.ActualCPUMs;
            }
            return;
        }

        // No runtime stats (e.g. a Compute Scalar pass-through): look through
        // to the descendants that have them.
        foreach (var grandchild in child.Children)
            AddEffectiveChildCpuByThread(grandchild, acc);
    }

    /// <summary>
    /// Per-thread CPU sum across a contiguous batch-mode zone, stopping at
    /// exchanges. The CPU twin of <see cref="AddBatchSubtreeElapsedByThread"/>.
    /// </summary>
    private static void AddBatchSubtreeCpuByThread(PlanNode node, Dictionary<int, long> acc)
    {
        foreach (var ts in WorkThreads(node))
        {
            acc.TryGetValue(ts.ThreadId, out var existing);
            acc[ts.ThreadId] = existing + ts.ActualCPUMs;
        }

        foreach (var child in node.Children)
        {
            if (child.PhysicalOp == "Parallelism") continue; // zone boundary

            var childMode = child.ActualExecutionMode ?? child.ExecutionMode;
            if (childMode == "Batch" && child.HasActualStats)
                AddBatchSubtreeCpuByThread(child, acc);
            else
                AddEffectiveChildCpuByThread(child, acc);
        }
    }

    private static long GetEffectiveChildCpuMs(PlanNode child)
    {
        if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
            return child.Children.Max(GetEffectiveChildCpuMs);
        if (child.ActualCPUMs > 0)
            return child.ActualCPUMs;
        if (child.Children.Count == 0)
            return 0;
        var sum = 0L;
        foreach (var grandchild in child.Children)
            sum += GetEffectiveChildCpuMs(grandchild);
        return sum;
    }

    /// <summary>
    /// What a child contributes to its parent's per-thread elapsed total. The
    /// per-thread mirror of the serial path's child look-through, and it must
    /// look through the same two shapes or the parent absorbs the subtree
    /// beneath them:
    ///
    ///   - A batch-mode child reports STANDALONE time, so only its own value
    ///     would come off and the rest of the batch zone would stay in the
    ///     parent.
    ///   - A pass-through child (Compute Scalar) carries no runtime stats at
    ///     all, so zero would come off.
    ///
    /// Together these can crown a row-mode operator above a batch subtree as
    /// the hottest operator in its plan, with the subtree's time double-counted.
    /// </summary>
    private static void AddEffectiveChildElapsedByThread(PlanNode child, Dictionary<int, long> acc)
    {
        // Exchange operators have unreliable times — look through to their child
        if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
        {
            var dominant = child.Children.OrderByDescending(c => c.ActualElapsedMs).First();
            AddEffectiveChildElapsedByThread(dominant, acc);
            return;
        }

        var mode = child.ActualExecutionMode ?? child.ExecutionMode;
        if (mode == "Batch" && child.HasActualStats)
        {
            AddBatchSubtreeElapsedByThread(child, acc);
            return;
        }

        if (child.HasActualStats && child.ActualElapsedMs > 0)
        {
            foreach (var ts in WorkThreads(child))
            {
                acc.TryGetValue(ts.ThreadId, out var existing);
                acc[ts.ThreadId] = existing + ts.ActualElapsedMs;
            }
            return;
        }

        // No runtime stats (e.g. a Compute Scalar pass-through): look through
        // to the descendants that have them.
        foreach (var grandchild in child.Children)
            AddEffectiveChildElapsedByThread(grandchild, acc);
    }

    /// <summary>
    /// Per-thread sum across a contiguous batch-mode zone, stopping at exchanges.
    /// Batch operators pipeline, so their times add rather than nest.
    /// </summary>
    private static void AddBatchSubtreeElapsedByThread(PlanNode node, Dictionary<int, long> acc)
    {
        foreach (var ts in WorkThreads(node))
        {
            acc.TryGetValue(ts.ThreadId, out var existing);
            acc[ts.ThreadId] = existing + ts.ActualElapsedMs;
        }

        foreach (var child in node.Children)
        {
            if (child.PhysicalOp == "Parallelism") continue; // zone boundary

            var childMode = child.ActualExecutionMode ?? child.ExecutionMode;
            if (childMode == "Batch" && child.HasActualStats)
                AddBatchSubtreeElapsedByThread(child, acc);
            else
                AddEffectiveChildElapsedByThread(child, acc);
        }
    }

    /// <summary>
    /// Serial row mode self-time: subtract all direct children's effective
    /// elapsed. The child side looks through the same two shapes as the
    /// per-thread path above — a pass-through child (Compute Scalar) with no
    /// runtime stats, and a batch-mode child's whole contiguous subtree —
    /// or the parent absorbs the subtree beneath them as its own self-time.
    /// </summary>
    private static long GetSerialOwnElapsed(PlanNode node)
    {
        var totalChildElapsed = 0L;
        foreach (var child in node.Children)
            totalChildElapsed += GetEffectiveChildElapsedMs(child);

        return Math.Max(0, node.ActualElapsedMs - totalChildElapsed);
    }

    /// <summary>
    /// What a child contributes to its parent's serial self-time. Exchange
    /// operators have unreliable times, so this looks through to their
    /// dominant child. A batch-mode child reports STANDALONE time, so this
    /// sums the whole contiguous batch zone rather than just the direct
    /// child. A child with no runtime stats at all (a Compute Scalar
    /// pass-through) contributes zero directly, so this looks through to the
    /// descendants that do have stats.
    /// </summary>
    private static long GetEffectiveChildElapsedMs(PlanNode child)
    {
        // Exchange operators: unreliable times, use max child
        if (child.PhysicalOp == "Parallelism" && child.Children.Count > 0)
            return child.Children.Max(GetEffectiveChildElapsedMs);

        var mode = child.ActualExecutionMode ?? child.ExecutionMode;
        if (mode == "Batch" && child.HasActualStats)
            return SumBatchSubtreeElapsedMs(child);

        if (child.ActualElapsedMs > 0)
            return child.ActualElapsedMs;

        // No runtime stats (e.g. a Compute Scalar pass-through): look through
        // to the descendants that have them.
        if (child.Children.Count == 0)
            return 0;

        var sum = 0L;
        foreach (var grandchild in child.Children)
            sum += GetEffectiveChildElapsedMs(grandchild);
        return sum;
    }

    /// <summary>
    /// Sums ActualElapsedMs across a contiguous batch-mode zone, stopping at
    /// exchange boundaries. Batch operators pipeline — elapsed times are
    /// standalone, not cumulative — so summing gives the total work the zone
    /// did, which is what a row-mode parent above the zone should subtract
    /// to get its own self-time.
    /// </summary>
    private static long SumBatchSubtreeElapsedMs(PlanNode node)
    {
        var sum = node.ActualElapsedMs;
        foreach (var child in node.Children)
        {
            if (child.PhysicalOp == "Parallelism") continue; // zone boundary

            var childMode = child.ActualExecutionMode ?? child.ExecutionMode;
            if (childMode == "Batch" && child.HasActualStats)
                sum += SumBatchSubtreeElapsedMs(child);
            else
                sum += GetEffectiveChildElapsedMs(child);
        }

        return sum;
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

    /// <summary>
    /// Returns a short label describing what a wait type means (e.g., "I/O — reading from disk").
    /// Public for use by UI components that annotate wait stats inline.
    /// </summary>
    public static string GetWaitLabel(string waitType)
    {
        var wt = waitType.ToUpperInvariant();
        return wt switch
        {
            _ when wt.StartsWith("PAGEIOLATCH", StringComparison.Ordinal) => "I/O — reading data from disk",
            _ when wt.Contains("IO_COMPLETION", StringComparison.Ordinal) => "I/O — spills to TempDB or eager writes",
            _ when wt == "SOS_SCHEDULER_YIELD" => "CPU — scheduler yielding",
            _ when wt.StartsWith("CXPACKET", StringComparison.Ordinal) || wt.StartsWith("CXCONSUMER", StringComparison.Ordinal) => "parallelism — thread skew",
            _ when wt.StartsWith("CXSYNC", StringComparison.Ordinal) => "parallelism — exchange synchronization",
            _ when wt == "HTBUILD" => "hash — building hash table",
            _ when wt == "HTDELETE" => "hash — cleaning up hash table",
            _ when wt == "HTREPARTITION" => "hash — repartitioning",
            _ when wt.StartsWith("HT", StringComparison.Ordinal) => "hash operation",
            _ when wt == "BPSORT" => "batch sort",
            _ when wt == "BMPBUILD" => "bitmap filter build",
            _ when wt.Contains("MEMORY_ALLOCATION_EXT", StringComparison.Ordinal) => "memory allocation",
            _ when wt.StartsWith("PAGELATCH", StringComparison.Ordinal) => "page latch — in-memory contention",
            _ when wt.StartsWith("LATCH_", StringComparison.Ordinal) => "latch contention",
            _ when wt.StartsWith("LCK_", StringComparison.Ordinal) => "lock contention",
            _ when wt == "LOGBUFFER" => "transaction log writes",
            _ when wt == "ASYNC_NETWORK_IO" => "network — client not consuming results",
            _ when wt == "SOS_PHYS_PAGE_CACHE" => "physical page cache contention",
            _ => ""
        };
    }

    /// <summary>
    /// Returns true if the statement has significant I/O waits (PAGEIOLATCH_*, IO_COMPLETION).
    /// Used for severity elevation decisions where I/O specifically indicates disk access.
    /// Thresholds: I/O waits >= 20% of total wait time AND >= 100ms absolute.
    /// </summary>
    private static bool HasSignificantIoWaits(List<WaitStatInfo> waits)
    {
        if (waits.Count == 0)
            return false;

        var totalMs = waits.Sum(w => w.WaitTimeMs);
        if (totalMs == 0)
            return false;

        long ioMs = 0;
        foreach (var w in waits)
        {
            var wt = w.WaitType.ToUpperInvariant();
            if (wt.StartsWith("PAGEIOLATCH", StringComparison.Ordinal) || wt.Contains("IO_COMPLETION", StringComparison.Ordinal))
                ioMs += w.WaitTimeMs;
        }

        var pct = (double)ioMs / totalMs * 100;
        return ioMs >= 100 && pct >= 20;
    }

    /// <summary>
    /// Formats a node reference for use in warning messages. Includes object name
    /// for data access operators where it helps identify which table is involved.
    /// </summary>
    private static string FormatNodeRef(PlanNode node)
    {
        if (!string.IsNullOrEmpty(node.ObjectName))
        {
            var objRef = !string.IsNullOrEmpty(node.DatabaseName)
                ? $"{node.DatabaseName}.{node.ObjectName}"
                : node.ObjectName;
            return $"{node.PhysicalOp} on {objRef} (Node {node.NodeId})";
        }

        return $"{node.PhysicalOp} (Node {node.NodeId})";
    }

    /// <summary>
    /// Blanks the contents of string literals and whole comments (<c>--</c> to end of line,
    /// and <c>/* */</c>, which nest in T-SQL) with spaces, so a hint or keyword found inside
    /// one of them does not count as code. Every other character stays where it was, so a
    /// match in the result is a match at the same position in the original text. Delimited
    /// identifiers (<c>[...]</c> and <c>"..."</c>) are stepped over unchanged, so a quote or
    /// a dash inside one does not start a string or a comment.
    /// </summary>
    private static string MaskCommentsAndLiterals(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return "";

        var chars = text.ToCharArray();
        var i = 0;
        while (i < chars.Length)
        {
            var c = chars[i];
            if (c == '\'' || c == '"' || c == '[')
            {
                var close = c == '[' ? ']' : c;
                var end = i + 1;
                while (end < chars.Length)
                {
                    if (chars[end] == close)
                    {
                        if (end + 1 < chars.Length && chars[end + 1] == close)
                        {
                            end += 2;
                            continue;
                        }
                        break;
                    }
                    end++;
                }
                if (c == '\'')
                {
                    for (var k = i + 1; k < end && k < chars.Length; k++)
                        chars[k] = ' ';
                }
                i = end + 1;
                continue;
            }

            if (c == '-' && i + 1 < chars.Length && chars[i + 1] == '-')
            {
                while (i < chars.Length && chars[i] != '\n' && chars[i] != '\r')
                    chars[i++] = ' ';
                continue;
            }

            if (c == '/' && i + 1 < chars.Length && chars[i + 1] == '*')
            {
                var depth = 0;
                while (i < chars.Length)
                {
                    if (chars[i] == '/' && i + 1 < chars.Length && chars[i + 1] == '*')
                    {
                        depth++;
                        chars[i++] = ' ';
                        chars[i++] = ' ';
                        continue;
                    }
                    if (chars[i] == '*' && i + 1 < chars.Length && chars[i + 1] == '/')
                    {
                        depth--;
                        chars[i++] = ' ';
                        chars[i++] = ' ';
                        if (depth == 0)
                            break;
                        continue;
                    }
                    if (chars[i] != '\n' && chars[i] != '\r')
                        chars[i] = ' ';
                    i++;
                }
                continue;
            }

            i++;
        }

        return new string(chars);
    }

    /// <summary>
    /// Identifies the specific cause of a row goal from the statement text.
    /// Returns a specific cause when detectable, or a generic list as fallback.
    /// </summary>
    private static string IdentifyRowGoalCause(string stmtText)
    {
        if (string.IsNullOrEmpty(stmtText))
            return "TOP, EXISTS, IN, or FAST hint";

        var text = MaskCommentsAndLiterals(stmtText).ToUpperInvariant();
        var causes = new List<string>(4);

        if (Regex.IsMatch(text, @"\bTOP\b"))
            causes.Add("TOP");
        if (Regex.IsMatch(text, @"\bEXISTS\b"))
            causes.Add("EXISTS");
        // IN with subquery — bare "IN (" followed by SELECT, not just "IN (1,2,3)"
        if (Regex.IsMatch(text, @"\bIN\s*\(\s*SELECT\b"))
            causes.Add("IN (subquery)");
        if (Regex.IsMatch(text, @"\bFAST\b"))
            causes.Add("FAST hint");

        return causes.Count > 0
            ? string.Join(", ", causes)
            : "TOP, EXISTS, IN, or FAST hint";
    }

    /// <summary>
    /// Returns true for operators that allocate meaningful resources based on row estimates.
    /// Hash Match (hash table), Sort (sort buffer), Spool (worktable).
    /// </summary>
    private static bool AllocatesResources(PlanNode node)
    {
        var op = node.PhysicalOp;
        return op.StartsWith("Hash", StringComparison.OrdinalIgnoreCase)
            || op.StartsWith("Sort", StringComparison.OrdinalIgnoreCase)
            || op.EndsWith("Spool", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record ScanImpact(double CostPct, double ElapsedPct, string? Summary);

    /// <summary>
    /// Builds impact details for a scan node: what % of plan time/cost it represents,
    /// and what fraction of rows survived filtering.
    /// </summary>
    private static ScanImpact BuildScanImpactDetails(PlanNode node, PlanStatement stmt)
    {
        var parts = new List<string>();

        // % of plan cost
        double costPct = 0;
        if (stmt.StatementSubTreeCost > 0 && node.EstimatedTotalSubtreeCost > 0)
        {
            costPct = node.EstimatedTotalSubtreeCost / stmt.StatementSubTreeCost * 100;
            if (costPct >= 50)
                parts.Add($"This scan is {costPct:N0}% of the plan cost.");
        }

        // % of elapsed time (actual plans)
        double elapsedPct = 0;
        if (node.HasActualStats && node.ActualElapsedMs > 0 &&
            stmt.QueryTimeStats != null && stmt.QueryTimeStats.ElapsedTimeMs > 0)
        {
            elapsedPct = (double)node.ActualElapsedMs / stmt.QueryTimeStats.ElapsedTimeMs * 100;
            if (elapsedPct >= 50)
                parts.Add($"This scan took {elapsedPct:N0}% of elapsed time.");
        }

        // Row selectivity: rows returned vs rows read (actual) or vs table cardinality (estimated)
        if (node.HasActualStats && node.ActualRowsRead > 0 && node.ActualRows < node.ActualRowsRead)
        {
            var selectivity = (double)node.ActualRows / node.ActualRowsRead * 100;
            if (selectivity < 10)
                parts.Add($"Only {selectivity:N3}% of rows survived filtering ({node.ActualRows:N0} of {node.ActualRowsRead:N0}).");
        }
        else if (!node.HasActualStats && node.TableCardinality > 0 && node.EstimateRows < node.TableCardinality)
        {
            var selectivity = node.EstimateRows / node.TableCardinality * 100;
            if (selectivity < 10)
                parts.Add($"Only {selectivity:N1}% of rows estimated to survive filtering.");
        }

        return new ScanImpact(costPct, elapsedPct, parts.Count > 0 ? string.Join(" ", parts) : null);
    }

    /// <summary>
    /// Checks whether a function call in a predicate is on the column side of the comparison.
    /// Predicate ScalarStrings look like: [db].[schema].[table].[col]>dateadd(day,(0),[@var])
    /// If the function is only on the parameter/literal side, it's still SARGable.
    ///
    /// <para><b>Only the function's own comparison is read.</b> A compound predicate is several
    /// comparisons joined by AND/OR, and the function belongs to exactly one of them. Splitting
    /// the whole predicate at its FIRST operator instead put every later comparison, column and
    /// all, on the function's side: in <c>[t].[A]=[@1] AND [t].[B]=CONVERT(tinyint,[@2],0)</c> the
    /// CONVERT looked like it shared a side with [t].[B], and so did the dateadd in the everyday
    /// range <c>[t].[d]&gt;=dateadd(day,(-7),getdate()) AND [t].[d]&lt;getdate()</c>.</para>
    ///
    /// <para><paramref name="identity"/> is passed straight to <see cref="IsColumnReference"/> to
    /// cover the unaliased table-variable case, e.g. <c>abs([X])=(1)</c>, and to tell that scan's
    /// own bare column apart from another unaliased table variable's outer reference in the same
    /// shape, e.g. <c>abs([A])</c> where A is an outer reference and only X is this scan's own
    /// column in <c>[X]=abs([A])</c>.</para>
    /// </summary>
    private static bool IsFunctionOnColumnSide(string predicate, Match funcMatch, ScanIdentity? identity = null)
    {
        var comparison = ComparisonContaining(predicate, funcMatch.Index, out var offset);

        var compMatch = ComparisonOperatorRegex.Match(comparison);
        if (!compMatch.Success)
            return true; // No comparison found — can't determine side, assume worst case

        var compPos = compMatch.Index;
        var funcPos = funcMatch.Index - offset;

        // The side of this comparison the function is on, and whether a column shares it
        string side = funcPos < compPos
            ? comparison[..compPos]
            : comparison[(compPos + compMatch.Length)..];

        // Same column-vs-variable distinction ConvertImplicitWrapsColumn needs, so it shares the
        // one helper rather than keeping a second copy of the logic in sync by hand.
        return IsColumnReference(side, identity);
    }

    /// <summary>
    /// The single comparison around <paramref name="position"/>: the text between the nearest
    /// AND/OR before it and the nearest after it. <paramref name="offset"/> is where that text
    /// starts in <paramref name="predicate"/>, so positions can be translated into it.
    ///
    /// <para>Operators are split on at every depth, not just the top level: a parenthesized group
    /// like <c>[t].[A]=(1) AND ([t].[B]=f([@p]) OR [t].[C]=(3))</c> has to come apart into its three
    /// comparisons, or the group would be read as one. The leftover grouping parentheses cannot
    /// move a comparison operator or add a column, so they are harmless. No function in a
    /// ScalarString takes AND/OR inside its arguments; CASE does, and it is caught earlier.</para>
    /// </summary>
    private static string ComparisonContaining(string predicate, int position, out int offset)
    {
        var start = 0;
        var end = predicate.Length;

        foreach (Match match in LogicalOperatorRegex.Matches(predicate))
        {
            if (!match.Groups[1].Success)
                continue; // a string literal or bracketed name, skipped whole

            if (match.Index + match.Length <= position)
            {
                start = match.Index + match.Length;
            }
            else
            {
                end = match.Index;
                break;
            }
        }

        offset = start;
        return predicate[start..end];
    }

    /// <summary>
    /// Checks whether any CONVERT_IMPLICIT in a predicate converts a COLUMN, which is the only
    /// version of it that costs a seek.
    ///
    /// <para><b>Why this is not just "contains CONVERT_IMPLICIT".</b> Data type precedence decides
    /// which side SQL Server converts, and it converts the LOWER-precedence side. Comparing a
    /// numeric(18,0) column to an int parameter converts the parameter UP:
    /// <c>[db].[dbo].[t].[col]=CONVERT_IMPLICIT(numeric(18,0),[@0],0)</c>. The column is untouched
    /// and still seekable — SQL Server will seek straight through that predicate given an index, and
    /// it raises no PlanAffectingConvert warning of its own. The damaging shape is the mirror image,
    /// <c>CONVERT_IMPLICIT(nvarchar(40),[db].[dbo].[t].[col],0)=[@d]</c>, where the conversion wraps
    /// the column and every row has to be converted before it can be compared.</para>
    ///
    /// <para>So the question is not whether a conversion is present but what is inside it, which is
    /// why this reads the CONVERT_IMPLICIT argument list rather than splitting on the comparison
    /// operator the way <see cref="IsFunctionOnColumnSide"/> does. The first argument is the target
    /// type and carries no brackets; a column reference in the remainder is the conversion input.</para>
    ///
    /// <para>Internal so the column-vs-variable line can be tested against raw predicate strings in
    /// showplan shape. <paramref name="identity"/> covers the unaliased table-variable case — a bare
    /// name with no dotted qualifier, e.g. <c>CONVERT_IMPLICIT(nvarchar(20),[S],0)=[@n]</c> — which
    /// this method cannot tell from a parameter or an expression on its own, and tells that scan's
    /// own bare columns apart from a bare outer reference off a different one; see
    /// <see cref="IsColumnReference"/>.</para>
    /// </summary>
    internal static bool ConvertImplicitWrapsColumn(string predicate, ScanIdentity? identity = null)
    {
        foreach (Match match in ConvertImplicitRegex.Matches(predicate))
        {
            // The regex ends at the opening paren, so its last character is where the args start.
            var arguments = ExtractBalancedArguments(predicate, match.Index + match.Length - 1);

            // Unparseable means we cannot tell what is being converted. Assume the worst, matching
            // IsFunctionOnColumnSide, rather than silently dropping a real conversion.
            if (arguments == null || IsColumnReference(arguments, identity))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Whether <paramref name="text"/> names a column belonging to <paramref name="identity"/> —
    /// the scanned object, not a column on some other table, or an outer reference a Nested Loops
    /// join passed it in one row at a time. A wrapped column stops an index seek only when it is
    /// actually the scanned table's own column: an outer reference already varies one row at a time
    /// no matter what wraps it, so it costs nothing extra, and a column on some other table was never
    /// going to seek this one anyway.
    ///
    /// <para>Null <paramref name="identity"/> means the caller has not identified a scan, and keeps
    /// this method's old, coarser behavior: <see cref="ColumnReferenceRegex"/> alone, so any dotted
    /// name counts as a column and no bare name does. That regex is still every non-null identity's
    /// first check too — a real table or an aliased table variable always renders its own column
    /// dotted, at minimum <c>[table].[col]</c> — but there it is followed by an ownership check: an
    /// outer reference and a column on another table are dotted too, and the fix is telling them
    /// apart, not giving up on dotted names altogether.</para>
    ///
    /// <para>Un-owned dotted names are read with <see cref="BracketedNameRegex"/>, one at a time:</para>
    /// <list type="bullet">
    /// <item>A run of two or more bracket parts right after literal <c> as </c> is the aliased form —
    /// <c>[..].[I].[X] as [i].[X]</c> or <c>@tv.[col] as [v].[col]</c> — and it owns the scan only when
    /// the scan has an alias and it matches the LAST-BUT-ONE part, the alias right before the column
    /// (case-insensitively). The part before <c> as </c> is not a reference of its own — in a self
    /// join it names the OTHER instance of the same table, under its own alias — so it is skipped
    /// outright rather than read as a second, competing reference.</item>
    /// <item>A run of two or more bracket parts with no <c> as </c> before it is the unaliased dotted
    /// form — <c>[db].[schema].[table].[col]</c>, or <c>[#t].[col]</c> for a temp table — and it owns
    /// the scan only when the scan has NO alias and its table matches the LAST-BUT-ONE part
    /// (case-insensitively, and cleaned of a temp table's full tempdb name). A scan with an alias
    /// always renders its own column through that alias, so an unaliased dotted reference elsewhere
    /// in the same predicate names a different object.</item>
    /// <item>A single bracket part is the bare form — read as a column only on an unaliased table
    /// variable, unless it is a parameter or variable (<c>[@p1]</c>), an optimizer expression
    /// (<c>[Expr1003]</c>), the name of a function call (followed by <c>(</c>) rather than a
    /// reference, or a name <see cref="ScanIdentity.BareOuterReferences"/> lists as another
    /// unaliased table variable's outer reference rather than this one's own column. A string
    /// literal that looks bracketed (<c>'[Y]'</c>) is never read as a name at all — see
    /// <see cref="BracketedNameRegex"/>.</item>
    /// </list>
    /// </summary>
    private static bool IsColumnReference(string text, ScanIdentity? identity)
    {
        if (identity == null)
            return ColumnReferenceRegex.IsMatch(text);

        var scan = identity.Value;

        foreach (Match match in BracketedNameRegex.Matches(text))
        {
            if (!match.Groups["name"].Success || match.Groups["call"].Success)
                continue; // a string literal, or the name of a function

            var name = match.Groups["name"].Value;

            // "<prefix> as [alias].[col]" — the prefix is not a reference of its own; the real
            // reference is the [alias].[col] pair that follows, matched separately on its own turn
            // through this loop.
            if (FollowedByAsBracket(text, match.Index + match.Length))
                continue;

            var parts = NamePartRegex.Matches(name);
            if (parts.Count >= 2)
            {
                var owner = StripBrackets(parts[^2].Value);

                if (PrecededByAs(text, match.Index))
                {
                    // Aliased: [alias].[col]. Owned only through a matching alias.
                    if (!string.IsNullOrEmpty(scan.Alias) &&
                        string.Equals(owner, scan.Alias, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                else
                {
                    // Unaliased dotted: [..].[table].[col]. An aliased scan's own column never
                    // renders this way, so this can only own the scan when the scan has none. The
                    // parser cleans a temp table's full tempdb name (#t___...___000000000003) down
                    // to #t in the scan's own name, so the owner here is cleaned the same way.
                    if (string.IsNullOrEmpty(scan.Alias) && !string.IsNullOrEmpty(scan.Table) &&
                        string.Equals(ShowPlanParser.CleanTempTableName(owner), scan.Table,
                            StringComparison.OrdinalIgnoreCase))
                        return true;
                }

                continue;
            }

            // A single bracketed part.
            if (name.StartsWith("[@", StringComparison.Ordinal))
                continue; // a parameter or a variable: [@p1]

            if (ExpressionColumnRegex.IsMatch(name))
                continue; // an optimizer-generated expression, not an actual column: [Expr1003]

            if (scan.IsTableVariable && string.IsNullOrEmpty(scan.Alias) &&
                !scan.BareOuterReferences.Contains(StripBrackets(name)))
                return true; // a bare name — this unaliased table variable's own column
        }

        return false;
    }

    /// <summary>
    /// True when <paramref name="text"/>[<paramref name="index"/>..] starts with the literal
    /// <c> as [</c> — the start of the <c>[alias].[col]</c> half of the aliased column-reference
    /// form, which follows the part that is not a reference of its own.
    /// </summary>
    private static bool FollowedByAsBracket(string text, int index) =>
        index >= 0 && index + 5 <= text.Length &&
        text.AsSpan(index, 5).Equals(" as [", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when <paramref name="text"/>[..<paramref name="index"/>] ends with the literal
    /// <c> as </c> — <paramref name="index"/> is a match's own start, so this reads the four
    /// characters right before it.
    /// </summary>
    private static bool PrecededByAs(string text, int index) =>
        index >= 4 && text.AsSpan(index - 4, 4).Equals(" as ", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Strips the brackets off one bracket part matched by <see cref="NamePartRegex"/> — never a
    /// whole dotted chain, so a plain Replace is enough; a "]]"-escaped literal bracket (which the
    /// parser, elsewhere, does not unescape either) passes through unchanged.
    /// </summary>
    private static string StripBrackets(string bracketPart) =>
        bracketPart.Replace("[", "").Replace("]", "");

    /// <summary>
    /// Returns the text between the parenthesis at <paramref name="openParenIndex"/> and its match,
    /// or null if the parentheses do not balance. Needed because the target type of a conversion can
    /// carry its own parentheses — numeric(18,0), varchar(50) — so the first ')' is not the end.
    /// </summary>
    private static string? ExtractBalancedArguments(string text, int openParenIndex)
    {
        var depth = 0;
        for (var i = openParenIndex; i < text.Length; i++)
        {
            if (text[i] == '(')
            {
                depth++;
            }
            else if (text[i] == ')')
            {
                depth--;
                if (depth == 0)
                    return text[(openParenIndex + 1)..i];
            }
        }

        return null;
    }

    /// <summary>
    /// Detects well-known CE default selectivity guesses by comparing EstimateRows to TableCardinality.
    /// Returns a description of the guess pattern, or null if no known pattern matches.
    /// </summary>
    private static string? DetectCeGuess(double estimateRows, double tableCardinality)
    {
        if (tableCardinality <= 0) return null;
        var selectivity = estimateRows / tableCardinality;

        // Known CE guess selectivities with a 2% tolerance band
        return selectivity switch
        {
            >= 0.29 and <= 0.31 => $"matches the 30% equality guess ({selectivity * 100:N1}%)",
            >= 0.098 and <= 0.102 => $"matches the 10% inequality guess ({selectivity * 100:N1}%)",
            >= 0.088 and <= 0.092 => $"matches the 9% LIKE/BETWEEN guess ({selectivity * 100:N1}%)",
            >= 0.155 and <= 0.175 => $"matches the ~16.4% compound predicate guess ({selectivity * 100:N1}%)",
            >= 0.009 and <= 0.011 => $"matches the 1% multi-inequality guess ({selectivity * 100:N1}%)",
            _ => null
        };
    }

    [GeneratedRegex(@"\b(CONVERT_IMPLICIT|CONVERT|CAST|isnull|coalesce|datepart|datediff|dateadd|year|month|day|upper|lower|ltrim|rtrim|trim|substring|left|right|charindex|replace|len|datalength|abs|floor|ceiling|round|reverse|stuff|format)\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex FunctionInPredicateRegExp();
    [GeneratedRegex(@"\blike\b[^'""]*?N?'%", RegexOptions.IgnoreCase)]
    private static partial Regex LeadingWildcardLikeRegExp();
    [GeneratedRegex(@"\bCASE\s+(WHEN\b|$)", RegexOptions.IgnoreCase)]
    private static partial Regex CaseInPredicateRegExp();
    [GeneratedRegex(@"\b(isnull|coalesce)\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex IsNullCoalesceRegExp();
    [GeneratedRegex(@"\bCONVERT_IMPLICIT\s*\(", RegexOptions.IgnoreCase)]
    private static partial Regex ConvertImplicitRegExp();
    // A column reference in a ScalarString is multi-part bracket-qualified ([schema].[table]).
    // A variable is a single bracket pair with an @ prefix ([@0]), so excluding @ from the first
    // part is what separates the two.
    [GeneratedRegex(@"\[[^\]@]+\]\.\[")]
    private static partial Regex ColumnReferenceRegExp();
    [GeneratedRegex(@"^\[Expr\d+\]$")]
    private static partial Regex ExpressionColumnRegExp();
    [GeneratedRegex(@"\[(?:[^\]]|\]\])*\]")]
    private static partial Regex NamePartRegExp();
    [GeneratedRegex(@"'(?:[^']|'')*'|(?<name>\[(?:[^\]]|\]\])*\](?:\.\[(?:[^\]]|\]\])*\])*)(?<call>\s*\()?")]
    private static partial Regex BracketedNameRegExp();
    // The operator a comparison turns on in a ScalarString: >=, <=, <>, !=, >, <, = or like.
    // Without like, [col] like upper([@p]) had no operator at all, fell to the assume-the-worst
    // default, and a function on the pattern was reported as a function on the column.
    [GeneratedRegex(@"(?<![<>])([<>=!]{1,2})(?![<>=])|\s(like)\s", RegexOptions.IgnoreCase)]
    private static partial Regex ComparisonOperatorRegExp();
    // What joins one comparison to the next in a compound predicate. String literals and
    // bracketed identifiers are matched first, so an AND inside one of them (N'Tom AND Jerry',
    // [Terms and Conditions]) is consumed whole and never reaches the capture group. Only a
    // Groups[1] match is a real operator.
    [GeneratedRegex(@"'(?:[^']|'')*'|\[(?:[^\]]|\]\])*\]|\s(AND|OR)\s", RegexOptions.IgnoreCase)]
    private static partial Regex LogicalOperatorRegExp();
    [GeneratedRegex(@"OPTIMIZE\s+FOR\s+UNKNOWN", RegexOptions.IgnoreCase)]
    private static partial Regex OptimizeForUnknownRegExp();
    [GeneratedRegex(@"\bNOT\s+IN\b", RegexOptions.IgnoreCase)]
    private static partial Regex NotInRegExp();
}
