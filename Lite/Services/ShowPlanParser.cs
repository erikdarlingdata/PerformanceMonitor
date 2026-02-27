using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml.Linq;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public static class ShowPlanParser
{
    private static readonly XNamespace Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    public static ParsedPlan Parse(string xml)
    {
        var plan = new ParsedPlan { RawXml = xml };

        XDocument doc;
        try
        {
            doc = XDocument.Parse(xml);
        }
        catch
        {
            return plan;
        }

        var root = doc.Root;
        if (root == null) return plan;

        plan.BuildVersion = root.Attribute("Version")?.Value;
        plan.Build = root.Attribute("Build")?.Value;
        plan.ClusteredMode = root.Attribute("ClusteredMode")?.Value is "true" or "1";

        // Standard path: ShowPlanXML → BatchSequence → Batch → Statements
        var batches = root.Descendants(Ns + "Batch");
        foreach (var batchEl in batches)
        {
            var batch = new PlanBatch();
            var statementsEl = batchEl.Element(Ns + "Statements");
            if (statementsEl != null)
            {
                foreach (var stmtEl in statementsEl.Elements())
                {
                    var stmts = ParseStatementAndChildren(stmtEl);
                    batch.Statements.AddRange(stmts);
                }
            }
            if (batch.Statements.Count > 0)
                plan.Batches.Add(batch);
        }

        // Fallback: some plan XML has StmtSimple directly under QueryPlan
        if (plan.Batches.Count == 0)
        {
            var batch = new PlanBatch();
            foreach (var stmtEl in root.Descendants(Ns + "StmtSimple"))
            {
                var stmt = ParseStatement(stmtEl);
                if (stmt != null)
                    batch.Statements.Add(stmt);
            }
            if (batch.Statements.Count > 0)
                plan.Batches.Add(batch);
        }

        ComputeOperatorCosts(plan);
        return plan;
    }

    /// <summary>
    /// Handles StmtSimple, StmtCond (IF/ELSE), and StmtCursor recursively.
    /// Returns a flat list of all parseable statements found.
    /// </summary>
    private static List<PlanStatement> ParseStatementAndChildren(XElement stmtEl)
    {
        var results = new List<PlanStatement>();
        var localName = stmtEl.Name.LocalName;

        if (localName == "StmtCond")
        {
            // IF/ELSE blocks — recurse into Condition, Then, Else
            var condEl = stmtEl.Element(Ns + "Condition");
            if (condEl != null)
            {
                foreach (var child in condEl.Elements())
                    results.AddRange(ParseStatementAndChildren(child));
            }

            var thenStmts = stmtEl.Element(Ns + "Then")?.Element(Ns + "Statements");
            if (thenStmts != null)
            {
                foreach (var child in thenStmts.Elements())
                    results.AddRange(ParseStatementAndChildren(child));
            }

            var elseStmts = stmtEl.Element(Ns + "Else")?.Element(Ns + "Statements");
            if (elseStmts != null)
            {
                foreach (var child in elseStmts.Elements())
                    results.AddRange(ParseStatementAndChildren(child));
            }
        }
        else if (localName == "StmtCursor")
        {
            // Cursor plans — parse each Operation's QueryPlan
            var cursorPlanEl = stmtEl.Element(Ns + "CursorPlan");
            if (cursorPlanEl != null)
            {
                var cursorName = cursorPlanEl.Attribute("CursorName")?.Value;
                var cursorActualType = cursorPlanEl.Attribute("CursorActualType")?.Value;
                var cursorRequestedType = cursorPlanEl.Attribute("CursorRequestedType")?.Value;
                var cursorConcurrency = cursorPlanEl.Attribute("CursorConcurrency")?.Value;
                var cursorForwardOnly = cursorPlanEl.Attribute("ForwardOnly")?.Value is "true" or "1";

                foreach (var opEl in cursorPlanEl.Elements(Ns + "Operation"))
                {
                    var opType = opEl.Attribute("OperationType")?.Value ?? "CursorOp";
                    var qpEl = opEl.Element(Ns + "QueryPlan");
                    if (qpEl == null) continue;

                    // Build a synthetic StmtSimple-like wrapper for ParseStatement
                    var relOpEl = qpEl.Element(Ns + "RelOp");
                    if (relOpEl == null) continue;

                    var stmt = ParseQueryPlanAsStatement(stmtEl, qpEl, relOpEl);
                    if (stmt != null)
                    {
                        // Override statement text with cursor context
                        if (string.IsNullOrEmpty(stmt.StatementText))
                            stmt.StatementText = $"Cursor: {cursorName} ({opType})";
                        stmt.CursorName = cursorName;
                        stmt.CursorActualType = cursorActualType;
                        stmt.CursorRequestedType = cursorRequestedType;
                        stmt.CursorConcurrency = cursorConcurrency;
                        stmt.CursorForwardOnly = cursorForwardOnly;
                        results.Add(stmt);
                    }
                }
            }
        }
        else
        {
            // StmtSimple or any other statement type
            var stmt = ParseStatement(stmtEl);
            if (stmt != null)
                results.Add(stmt);
        }

        return results;
    }

    private static PlanStatement? ParseStatement(XElement stmtEl)
    {
        var stmt = new PlanStatement
        {
            StatementText = stmtEl.Attribute("StatementText")?.Value ?? "",
            StatementType = stmtEl.Attribute("StatementType")?.Value ?? "",
            StatementSubTreeCost = ParseDouble(stmtEl.Attribute("StatementSubTreeCost")?.Value),
            StatementEstRows = ParseDouble(stmtEl.Attribute("StatementEstRows")?.Value)
        };

        var queryPlanEl = stmtEl.Element(Ns + "QueryPlan");

        // XSD gap: Dispatcher/PSP (on StmtSimple, not inside QueryPlan)
        var dispatcherEl = stmtEl.Element(Ns + "Dispatcher");
        if (dispatcherEl != null)
        {
            stmt.Dispatcher = new DispatcherInfo();
            foreach (var pspEl in dispatcherEl.Elements(Ns + "ParameterSensitivePredicate"))
            {
                var psp = new ParameterSensitivePredicateInfo
                {
                    LowBoundary = ParseDouble(pspEl.Attribute("LowBoundary")?.Value),
                    HighBoundary = ParseDouble(pspEl.Attribute("HighBoundary")?.Value)
                };
                var predEl = pspEl.Element(Ns + "Predicate")?.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                psp.PredicateText = predEl?.Attribute("ScalarString")?.Value;
                foreach (var statEl in pspEl.Elements(Ns + "StatisticsInfo"))
                {
                    psp.Statistics.Add(new OptimizerStatsUsageItem
                    {
                        StatisticsName = statEl.Attribute("Statistics")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                        TableName = statEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                        DatabaseName = statEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", ""),
                        SchemaName = statEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", ""),
                        ModificationCount = ParseLong(statEl.Attribute("ModificationCount")?.Value),
                        SamplingPercent = ParseDouble(statEl.Attribute("SamplingPercent")?.Value),
                        LastUpdate = statEl.Attribute("LastUpdate")?.Value
                    });
                }
                stmt.Dispatcher.ParameterSensitivePredicates.Add(psp);
            }
            foreach (var oppEl in dispatcherEl.Elements(Ns + "OptionalParameterPredicate"))
            {
                var opp = new OptionalParameterPredicateInfo();
                var oppPredEl = oppEl.Element(Ns + "Predicate")?.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                opp.PredicateText = oppPredEl?.Attribute("ScalarString")?.Value;
                stmt.Dispatcher.OptionalParameterPredicates.Add(opp);
            }
        }

        if (queryPlanEl == null) return stmt;

        ParseStmtAttributes(stmt, stmtEl);
        ParseQueryPlanElements(stmt, stmtEl, queryPlanEl);

        // Root RelOp — wrap in a synthetic statement-type node (SELECT, INSERT, etc.)
        var relOpEl = queryPlanEl.Element(Ns + "RelOp");
        if (relOpEl != null)
        {
            var opNode = ParseRelOp(relOpEl);
            var stmtType = stmt.StatementType.Length > 0
                ? stmt.StatementType.ToUpperInvariant()
                : "QUERY";

            var stmtNode = new PlanNode
            {
                NodeId = -1,
                PhysicalOp = stmtType,
                LogicalOp = stmtType,
                EstimatedTotalSubtreeCost = stmt.StatementSubTreeCost,
                EstimateRows = stmt.StatementEstRows,
                IconName = stmtType switch
                {
                    "SELECT" => "result",
                    "INSERT" => "insert",
                    "UPDATE" => "update",
                    "DELETE" => "delete",
                    _ => "language_construct_catch_all"
                }
            };
            opNode.Parent = stmtNode;
            stmtNode.Children.Add(opNode);
            stmt.RootNode = stmtNode;
        }

        // XSD gap: UDF sub-plans
        foreach (var udfEl in stmtEl.Elements(Ns + "UDF"))
        {
            var udfInfo = new FunctionPlanInfo
            {
                ProcName = udfEl.Attribute("ProcName")?.Value ?? "",
                IsNativelyCompiled = udfEl.Attribute("IsNativelyCompiled")?.Value is "true" or "1"
            };
            var udfStmts = udfEl.Element(Ns + "Statements");
            if (udfStmts != null)
            {
                foreach (var childStmt in udfStmts.Elements())
                {
                    var parsed = ParseStatementAndChildren(childStmt);
                    udfInfo.Statements.AddRange(parsed);
                }
            }
            stmt.UdfPlans.Add(udfInfo);
        }

        // XSD gap: StoredProc sub-plan
        var storedProcEl = stmtEl.Element(Ns + "StoredProc");
        if (storedProcEl != null)
        {
            var spInfo = new FunctionPlanInfo
            {
                ProcName = storedProcEl.Attribute("ProcName")?.Value ?? "",
                IsNativelyCompiled = storedProcEl.Attribute("IsNativelyCompiled")?.Value is "true" or "1"
            };
            var spStmts = storedProcEl.Element(Ns + "Statements");
            if (spStmts != null)
            {
                foreach (var childStmt in spStmts.Elements())
                {
                    var parsed = ParseStatementAndChildren(childStmt);
                    spInfo.Statements.AddRange(parsed);
                }
            }
            stmt.StoredProcPlan = spInfo;
        }

        return stmt;
    }

    /// <summary>
    /// Parse a QueryPlan element that comes from a cursor Operation (no parent StmtSimple attributes).
    /// </summary>
    private static PlanStatement? ParseQueryPlanAsStatement(XElement stmtEl, XElement queryPlanEl, XElement relOpEl)
    {
        var stmt = new PlanStatement
        {
            StatementText = stmtEl.Attribute("StatementText")?.Value ?? "",
            StatementType = stmtEl.Attribute("StatementType")?.Value ?? "SELECT",
            StatementSubTreeCost = ParseDouble(stmtEl.Attribute("StatementSubTreeCost")?.Value)
        };

        ParseStmtAttributes(stmt, stmtEl);
        ParseQueryPlanElements(stmt, stmtEl, queryPlanEl);

        var opNode = ParseRelOp(relOpEl);
        var stmtType = stmt.StatementType.Length > 0
            ? stmt.StatementType.ToUpperInvariant()
            : "QUERY";

        // Use subtree cost from RelOp if statement cost is 0
        if (stmt.StatementSubTreeCost <= 0)
            stmt.StatementSubTreeCost = opNode.EstimatedTotalSubtreeCost;

        var stmtNode = new PlanNode
        {
            NodeId = -1,
            PhysicalOp = stmtType,
            LogicalOp = stmtType,
            EstimatedTotalSubtreeCost = stmt.StatementSubTreeCost,
            EstimateRows = stmt.StatementEstRows,
            IconName = stmtType switch
            {
                "SELECT" => "result",
                "INSERT" => "insert",
                "UPDATE" => "update",
                "DELETE" => "delete",
                _ => "language_construct_catch_all"
            }
        };
        opNode.Parent = stmtNode;
        stmtNode.Children.Add(opNode);
        stmt.RootNode = stmtNode;

        return stmt;
    }

    /// <summary>
    /// Parse attributes from StmtSimple element.
    /// </summary>
    private static void ParseStmtAttributes(PlanStatement stmt, XElement stmtEl)
    {
        stmt.StatementOptmLevel = stmtEl.Attribute("StatementOptmLevel")?.Value;
        stmt.StatementOptmEarlyAbortReason = stmtEl.Attribute("StatementOptmEarlyAbortReason")?.Value;
        stmt.StatementParameterizationType = (int)ParseDouble(stmtEl.Attribute("StatementParameterizationType")?.Value);
        stmt.StatementSqlHandle = stmtEl.Attribute("StatementSqlHandle")?.Value;
        stmt.DatabaseContextSettingsId = ParseLong(stmtEl.Attribute("DatabaseContextSettingsId")?.Value);
        stmt.ParentObjectId = (int)ParseDouble(stmtEl.Attribute("ParentObjectId")?.Value);
        stmt.SecurityPolicyApplied = stmtEl.Attribute("SecurityPolicyApplied")?.Value is "true" or "1";
        stmt.BatchModeOnRowStoreUsed = stmtEl.Attribute("BatchModeOnRowStoreUsed")?.Value is "true" or "1";
        stmt.QueryHash = stmtEl.Attribute("QueryHash")?.Value;
        stmt.QueryPlanHash = stmtEl.Attribute("QueryPlanHash")?.Value;

        // Bug fix 1.3: CE version is on StmtSimple per XSD
        stmt.CardinalityEstimationModelVersion = (int)ParseDouble(stmtEl.Attribute("CardinalityEstimationModelVersion")?.Value);

        // Wave 3.6: Query Store hint attributes
        stmt.QueryStoreStatementHintId = (int)ParseDouble(stmtEl.Attribute("QueryStoreStatementHintId")?.Value);
        stmt.QueryStoreStatementHintText = stmtEl.Attribute("QueryStoreStatementHintText")?.Value;
        stmt.QueryStoreStatementHintSource = stmtEl.Attribute("QueryStoreStatementHintSource")?.Value;

        // XSD gap: Statement-level identifiers and handles
        stmt.StatementId = (int)ParseDouble(stmtEl.Attribute("StatementId")?.Value);
        stmt.StatementCompId = (int)ParseDouble(stmtEl.Attribute("StatementCompId")?.Value);
        stmt.TemplatePlanGuideDB = stmtEl.Attribute("TemplatePlanGuideDB")?.Value;
        stmt.TemplatePlanGuideName = stmtEl.Attribute("TemplatePlanGuideName")?.Value;
        stmt.ParameterizedPlanHandle = stmtEl.Attribute("ParameterizedPlanHandle")?.Value;
        stmt.BatchSqlHandle = stmtEl.Attribute("BatchSqlHandle")?.Value;
        stmt.ContainsLedgerTables = stmtEl.Attribute("ContainsLedgerTables")?.Value is "true" or "1";
        stmt.QueryCompilationReplay = (int)ParseDouble(stmtEl.Attribute("QueryCompilationReplay")?.Value);
    }

    /// <summary>
    /// Parse child elements of QueryPlan (memory grant, stats, parameters, etc.)
    /// </summary>
    private static void ParseQueryPlanElements(PlanStatement stmt, XElement stmtEl, XElement queryPlanEl)
    {
        // StatementSetOptions (child element of StmtSimple)
        var setOptsEl = stmtEl.Element(Ns + "StatementSetOptions");
        if (setOptsEl != null)
        {
            stmt.SetOptions = new SetOptionsInfo
            {
                AnsiNulls = setOptsEl.Attribute("ANSI_NULLS")?.Value is "true" or "1",
                AnsiPadding = setOptsEl.Attribute("ANSI_PADDING")?.Value is "true" or "1",
                AnsiWarnings = setOptsEl.Attribute("ANSI_WARNINGS")?.Value is "true" or "1",
                ArithAbort = setOptsEl.Attribute("ARITHABORT")?.Value is "true" or "1",
                ConcatNullYieldsNull = setOptsEl.Attribute("CONCAT_NULL_YIELDS_NULL")?.Value is "true" or "1",
                NumericRoundAbort = setOptsEl.Attribute("NUMERIC_ROUNDABORT")?.Value is "true" or "1",
                QuotedIdentifier = setOptsEl.Attribute("QUOTED_IDENTIFIER")?.Value is "true" or "1"
            };
        }

        // Memory grant info
        var memEl = queryPlanEl.Element(Ns + "MemoryGrantInfo");
        if (memEl != null)
        {
            stmt.MemoryGrant = new MemoryGrantInfo
            {
                SerialRequiredMemoryKB = ParseLong(memEl.Attribute("SerialRequiredMemory")?.Value),
                SerialDesiredMemoryKB = ParseLong(memEl.Attribute("SerialDesiredMemory")?.Value),
                RequiredMemoryKB = ParseLong(memEl.Attribute("RequiredMemory")?.Value),
                DesiredMemoryKB = ParseLong(memEl.Attribute("DesiredMemory")?.Value),
                RequestedMemoryKB = ParseLong(memEl.Attribute("RequestedMemory")?.Value),
                GrantedMemoryKB = ParseLong(memEl.Attribute("GrantedMemory")?.Value),
                MaxUsedMemoryKB = ParseLong(memEl.Attribute("MaxUsedMemory")?.Value),
                GrantWaitTimeMs = ParseLong(memEl.Attribute("GrantWaitTime")?.Value),
                LastRequestedMemoryKB = ParseLong(memEl.Attribute("LastRequestedMemory")?.Value),
                IsMemoryGrantFeedbackAdjusted = memEl.Attribute("IsMemoryGrantFeedbackAdjusted")?.Value
            };
        }

        // Statement-level metadata from QueryPlan attributes
        stmt.CachedPlanSizeKB = ParseLong(queryPlanEl.Attribute("CachedPlanSize")?.Value);
        stmt.DegreeOfParallelism = (int)ParseDouble(queryPlanEl.Attribute("DegreeOfParallelism")?.Value);
        stmt.NonParallelPlanReason = queryPlanEl.Attribute("NonParallelPlanReason")?.Value;
        stmt.RetrievedFromCache = queryPlanEl.Attribute("RetrievedFromCache")?.Value is "true" or "1";
        stmt.CompileTimeMs = ParseLong(queryPlanEl.Attribute("CompileTime")?.Value);
        stmt.CompileMemoryKB = ParseLong(queryPlanEl.Attribute("CompileMemory")?.Value);
        stmt.CompileCPUMs = ParseLong(queryPlanEl.Attribute("CompileCPU")?.Value);

        // Fallback: some plans have CE version on QueryPlan instead of StmtSimple
        if (stmt.CardinalityEstimationModelVersion == 0)
            stmt.CardinalityEstimationModelVersion = (int)ParseDouble(queryPlanEl.Attribute("CardinalityEstimationModelVersion")?.Value);

        // Wave 2.5: MaxQueryMemory
        stmt.MaxQueryMemoryKB = ParseLong(queryPlanEl.Attribute("MaxQueryMemory")?.Value);

        // Wave 3.1: EffectiveDOP + DOP feedback
        stmt.EffectiveDOP = (int)ParseDouble(queryPlanEl.Attribute("EffectiveDegreeOfParallelism")?.Value);
        stmt.DOPFeedbackAdjusted = queryPlanEl.Attribute("IsDOPFeedbackAdjusted")?.Value;

        // Wave 3.4: Plan Guide attributes
        stmt.PlanGuideDB = queryPlanEl.Attribute("PlanGuideDB")?.Value;
        stmt.PlanGuideName = queryPlanEl.Attribute("PlanGuideName")?.Value;
        stmt.UsePlan = queryPlanEl.Attribute("UsePlan")?.Value is "true" or "1";

        // Wave 3.5: ParameterizedText
        stmt.ParameterizedText = queryPlanEl.Element(Ns + "ParameterizedText")?.Value;

        // XSD gap: QueryPlan-level attributes
        stmt.ContainsInterleavedExecutionCandidates = queryPlanEl.Attribute("ContainsInterleavedExecutionCandidates")?.Value is "true" or "1";
        stmt.ContainsInlineScalarTsqlUdfs = queryPlanEl.Attribute("ContainsInlineScalarTsqlUdfs")?.Value is "true" or "1";
        stmt.QueryVariantID = (int)ParseDouble(queryPlanEl.Attribute("QueryVariantID")?.Value);
        stmt.DispatcherPlanHandle = queryPlanEl.Attribute("DispatcherPlanHandle")?.Value;
        stmt.ExclusiveProfileTimeActive = queryPlanEl.Attribute("ExclusiveProfileTimeActive")?.Value is "true" or "1";

        // XSD gap: OptimizationReplay
        var optReplayEl = queryPlanEl.Element(Ns + "OptimizationReplay");
        if (optReplayEl != null)
            stmt.OptimizationReplayScript = optReplayEl.Attribute("Script")?.Value;

        // Missing indexes
        stmt.MissingIndexes = ParseMissingIndexes(queryPlanEl);

        // Wave 2.8: QueryPlan-level warnings
        var planWarningsEl = queryPlanEl.Element(Ns + "Warnings");
        if (planWarningsEl != null)
            stmt.PlanWarnings = ParseWarningsFromElement(planWarningsEl);

        // OptimizerHardwareDependentProperties
        var hwEl = queryPlanEl.Element(Ns + "OptimizerHardwareDependentProperties");
        if (hwEl != null)
        {
            stmt.HardwareProperties = new OptimizerHardwareInfo
            {
                EstimatedAvailableMemoryGrant = ParseLong(hwEl.Attribute("EstimatedAvailableMemoryGrant")?.Value),
                EstimatedPagesCached = ParseLong(hwEl.Attribute("EstimatedPagesCached")?.Value),
                EstimatedAvailableDOP = (int)ParseDouble(hwEl.Attribute("EstimatedAvailableDegreeOfParallelism")?.Value),
                MaxCompileMemory = ParseLong(hwEl.Attribute("MaxCompileMemory")?.Value)
            };
        }

        // OptimizerStatsUsage
        var statsUsageEl = queryPlanEl.Element(Ns + "OptimizerStatsUsage");
        if (statsUsageEl != null)
        {
            foreach (var statEl in statsUsageEl.Elements(Ns + "StatisticsInfo"))
            {
                stmt.StatsUsage.Add(new OptimizerStatsUsageItem
                {
                    StatisticsName = statEl.Attribute("Statistics")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    TableName = statEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    DatabaseName = statEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", ""),
                    SchemaName = statEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", ""),
                    ModificationCount = ParseLong(statEl.Attribute("ModificationCount")?.Value),
                    SamplingPercent = ParseDouble(statEl.Attribute("SamplingPercent")?.Value),
                    LastUpdate = statEl.Attribute("LastUpdate")?.Value
                });
            }
        }

        // ThreadStat (actual plans)
        var threadStatEl = queryPlanEl.Element(Ns + "ThreadStat");
        if (threadStatEl != null)
        {
            stmt.ThreadStats = new ThreadStatInfo
            {
                Branches = (int)ParseDouble(threadStatEl.Attribute("Branches")?.Value),
                UsedThreads = (int)ParseDouble(threadStatEl.Attribute("UsedThreads")?.Value)
            };
            foreach (var trEl in threadStatEl.Elements(Ns + "ThreadReservation"))
            {
                stmt.ThreadStats.Reservations.Add(new ThreadReservation
                {
                    NodeId = (int)ParseDouble(trEl.Attribute("NodeId")?.Value),
                    ReservedThreads = (int)ParseDouble(trEl.Attribute("ReservedThreads")?.Value)
                });
            }
        }

        // ParameterList
        var paramListEl = queryPlanEl.Element(Ns + "ParameterList");
        if (paramListEl != null)
        {
            foreach (var paramEl in paramListEl.Elements(Ns + "ColumnReference"))
            {
                stmt.Parameters.Add(new PlanParameter
                {
                    Name = paramEl.Attribute("Column")?.Value ?? "",
                    DataType = paramEl.Attribute("ParameterDataType")?.Value ?? "",
                    CompiledValue = paramEl.Attribute("ParameterCompiledValue")?.Value,
                    RuntimeValue = paramEl.Attribute("ParameterRuntimeValue")?.Value
                });
            }
        }

        // WaitStats (actual plans)
        var waitStatsEl = queryPlanEl.Element(Ns + "WaitStats");
        if (waitStatsEl != null)
        {
            foreach (var waitEl in waitStatsEl.Elements(Ns + "Wait"))
            {
                stmt.WaitStats.Add(new WaitStatInfo
                {
                    WaitType = waitEl.Attribute("WaitType")?.Value ?? "",
                    WaitTimeMs = ParseLong(waitEl.Attribute("WaitTimeMs")?.Value),
                    WaitCount = ParseLong(waitEl.Attribute("WaitCount")?.Value)
                });
            }
        }

        // QueryTimeStats (actual plans)
        var queryTimeEl = queryPlanEl.Element(Ns + "QueryTimeStats");
        if (queryTimeEl != null)
        {
            stmt.QueryTimeStats = new QueryTimeInfo
            {
                CpuTimeMs = ParseLong(queryTimeEl.Attribute("CpuTime")?.Value),
                ElapsedTimeMs = ParseLong(queryTimeEl.Attribute("ElapsedTime")?.Value)
            };
            stmt.QueryUdfCpuTimeMs = ParseLong(queryTimeEl.Attribute("UdfCpuTime")?.Value);
            stmt.QueryUdfElapsedTimeMs = ParseLong(queryTimeEl.Attribute("UdfElapsedTime")?.Value);
        }

        // Wave 3.12: TraceFlags
        foreach (var traceFlagsEl in queryPlanEl.Elements(Ns + "TraceFlags"))
        {
            var isCompile = traceFlagsEl.Attribute("IsCompileTime")?.Value is "true" or "1";
            foreach (var tf in traceFlagsEl.Elements(Ns + "TraceFlag"))
            {
                stmt.TraceFlags.Add(new TraceFlagInfo
                {
                    Value = (int)ParseDouble(tf.Attribute("Value")?.Value),
                    Scope = tf.Attribute("Scope")?.Value ?? "",
                    IsCompileTime = isCompile
                });
            }
        }

        // Wave 3.13: IndexedViewInfo
        var ivInfoEl = queryPlanEl.Element(Ns + "IndexedViewInfo");
        if (ivInfoEl != null)
        {
            foreach (var objEl in ivInfoEl.Elements(Ns + "Object"))
            {
                var db = objEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", "");
                var schema = objEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", "");
                var table = objEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "");
                var index = objEl.Attribute("Index")?.Value?.Replace("[", "").Replace("]", "");
                var parts = new List<string>();
                if (!string.IsNullOrEmpty(db)) parts.Add(db);
                if (!string.IsNullOrEmpty(schema)) parts.Add(schema);
                if (!string.IsNullOrEmpty(table)) parts.Add(table);
                if (!string.IsNullOrEmpty(index)) parts.Add(index);
                var name = string.Join(".", parts);
                if (!string.IsNullOrEmpty(name))
                    stmt.IndexedViews.Add(name);
            }
        }

        // XSD gap: CardinalityFeedback
        var ceFeedbackEl = queryPlanEl.Element(Ns + "CardinalityFeedback");
        if (ceFeedbackEl != null)
        {
            foreach (var entry in ceFeedbackEl.Elements(Ns + "Entry"))
            {
                stmt.CardinalityFeedback.Add(new CardinalityFeedbackEntry
                {
                    Key = ParseLong(entry.Attribute("Key")?.Value),
                    Value = ParseLong(entry.Attribute("Value")?.Value)
                });
            }
        }
    }

    private static PlanNode ParseRelOp(XElement relOpEl)
    {
        var node = new PlanNode
        {
            NodeId = (int)ParseDouble(relOpEl.Attribute("NodeId")?.Value),
            PhysicalOp = relOpEl.Attribute("PhysicalOp")?.Value ?? "",
            LogicalOp = relOpEl.Attribute("LogicalOp")?.Value ?? "",
            EstimatedTotalSubtreeCost = ParseDouble(relOpEl.Attribute("EstimatedTotalSubtreeCost")?.Value),
            EstimateRows = ParseDouble(relOpEl.Attribute("EstimateRows")?.Value),
            EstimateIO = ParseDouble(relOpEl.Attribute("EstimateIO")?.Value),
            EstimateCPU = ParseDouble(relOpEl.Attribute("EstimateCPU")?.Value),
            EstimateRebinds = ParseDouble(relOpEl.Attribute("EstimateRebinds")?.Value),
            EstimateRewinds = ParseDouble(relOpEl.Attribute("EstimateRewinds")?.Value),
            EstimatedRowSize = (int)ParseDouble(relOpEl.Attribute("AvgRowSize")?.Value),
            Parallel = relOpEl.Attribute("Parallel")?.Value is "true" or "1",
            Partitioned = relOpEl.Attribute("Partitioned")?.Value is "true" or "1",
            ExecutionMode = relOpEl.Attribute("EstimatedExecutionMode")?.Value,
            IsAdaptive = relOpEl.Attribute("IsAdaptive")?.Value is "true" or "1",
            AdaptiveThresholdRows = ParseDouble(relOpEl.Attribute("AdaptiveThresholdRows")?.Value),
            EstimatedJoinType = relOpEl.Attribute("EstimatedJoinType")?.Value,
            // Wave 3.14: Estimated DOP per operator
            EstimatedDOP = (int)ParseDouble(relOpEl.Attribute("EstimatedAvailableDegreeOfParallelism")?.Value),
            // XSD gap: RelOp-level metadata
            GroupExecuted = relOpEl.Attribute("GroupExecuted")?.Value is "true" or "1",
            RemoteDataAccess = relOpEl.Attribute("RemoteDataAccess")?.Value is "true" or "1",
            OptimizedHalloweenProtectionUsed = relOpEl.Attribute("OptimizedHalloweenProtectionUsed")?.Value is "true" or "1",
            StatsCollectionId = ParseLong(relOpEl.Attribute("StatsCollectionId")?.Value)
        };

        // Map to icon
        node.IconName = PlanIconMapper.GetIconName(node.PhysicalOp);

        // Handle operator-specific element
        var physicalOpEl = GetOperatorElement(relOpEl);
        if (physicalOpEl != null)
        {
            // Object reference (table/index name) — scoped to stop at child RelOps
            var objEl = ScopedDescendants(physicalOpEl, Ns + "Object").FirstOrDefault();
            if (objEl != null)
            {
                var db = objEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", "");
                var schema = objEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", "");
                var table = objEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "");
                var index = objEl.Attribute("Index")?.Value?.Replace("[", "").Replace("]", "");

                node.DatabaseName = db;
                node.IndexName = index;

                var shortParts = new List<string>();
                if (!string.IsNullOrEmpty(schema)) shortParts.Add(schema);
                if (!string.IsNullOrEmpty(table)) shortParts.Add(table);
                node.ObjectName = shortParts.Count > 0 ? string.Join(".", shortParts) : null;

                var fullParts = new List<string>();
                if (!string.IsNullOrEmpty(db)) fullParts.Add(db);
                if (!string.IsNullOrEmpty(schema)) fullParts.Add(schema);
                if (!string.IsNullOrEmpty(table)) fullParts.Add(table);
                var fullName = string.Join(".", fullParts);
                if (!string.IsNullOrEmpty(index))
                    fullName += $".{index}";
                node.FullObjectName = !string.IsNullOrEmpty(fullName) ? fullName : null;

                node.StorageType = objEl.Attribute("Storage")?.Value;
                node.ServerName = objEl.Attribute("Server")?.Value?.Replace("[", "").Replace("]", "");
                node.ObjectAlias = objEl.Attribute("Alias")?.Value?.Replace("[", "").Replace("]", "");
                node.IndexKind = objEl.Attribute("IndexKind")?.Value;
                node.FilteredIndex = objEl.Attribute("Filtered")?.Value is "true" or "1";
                node.TableReferenceId = (int)ParseDouble(objEl.Attribute("TableReferenceId")?.Value);
            }

            // Hash keys for hash match operators
            var hashKeysProbeEl = physicalOpEl.Element(Ns + "HashKeysProbe");
            if (hashKeysProbeEl != null)
            {
                var cols = hashKeysProbeEl.Elements(Ns + "ColumnReference")
                    .Select(c => FormatColumnRef(c))
                    .Where(s => !string.IsNullOrEmpty(s));
                node.HashKeysProbe = string.Join(", ", cols);
            }
            var hashKeysBuildEl = physicalOpEl.Element(Ns + "HashKeysBuild");
            if (hashKeysBuildEl != null)
            {
                var cols = hashKeysBuildEl.Elements(Ns + "ColumnReference")
                    .Select(c => FormatColumnRef(c))
                    .Where(s => !string.IsNullOrEmpty(s));
                node.HashKeysBuild = string.Join(", ", cols);
            }

            // Ordered attribute
            node.Ordered = physicalOpEl.Attribute("Ordered")?.Value == "true" || physicalOpEl.Attribute("Ordered")?.Value == "1";

            // Seek predicates — scoped to stop at child RelOps
            var seekPreds = ScopedDescendants(physicalOpEl, Ns + "SeekPredicateNew")
                .Concat(ScopedDescendants(physicalOpEl, Ns + "SeekPredicate"));
            var seekParts = new List<string>();
            foreach (var sp in seekPreds)
            {
                var scalarOps = sp.Descendants(Ns + "ScalarOperator");
                foreach (var so in scalarOps)
                {
                    var val = so.Attribute("ScalarString")?.Value;
                    if (!string.IsNullOrEmpty(val))
                        seekParts.Add(val);
                }
            }
            if (seekParts.Count > 0)
                node.SeekPredicates = string.Join(" AND ", seekParts);

            // Residual predicate
            var predEl = physicalOpEl.Elements(Ns + "Predicate").FirstOrDefault();
            if (predEl != null)
            {
                var scalarOp = predEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.Predicate = scalarOp?.Attribute("ScalarString")?.Value;
            }

            // Partitioning type (for parallelism operators)
            node.PartitioningType = physicalOpEl.Attribute("PartitioningType")?.Value;

            // Build/Probe residuals (Hash Match)
            var buildResEl = physicalOpEl.Element(Ns + "BuildResidual");
            if (buildResEl != null)
            {
                var so = buildResEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.BuildResidual = so?.Attribute("ScalarString")?.Value;
            }
            var probeResEl = physicalOpEl.Element(Ns + "ProbeResidual");
            if (probeResEl != null)
            {
                var so = probeResEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.ProbeResidual = so?.Attribute("ScalarString")?.Value;
            }

            // Wave 2.1/2.2: Merge Residual + PassThru (Merge Join + Nested Loops)
            var residualEl = physicalOpEl.Element(Ns + "Residual");
            if (residualEl != null)
            {
                var so = residualEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.MergeResidual = so?.Attribute("ScalarString")?.Value;
            }
            var passThruEl = physicalOpEl.Element(Ns + "PassThru");
            if (passThruEl != null)
            {
                var so = passThruEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.PassThru = so?.Attribute("ScalarString")?.Value;
            }

            // OrderBy columns (Sort operator)
            var orderByEl = physicalOpEl.Element(Ns + "OrderBy");
            if (orderByEl != null)
            {
                var obParts = orderByEl.Elements(Ns + "OrderByColumn")
                    .Select(obc =>
                    {
                        var ascending = obc.Attribute("Ascending")?.Value != "false";
                        var colRef = obc.Element(Ns + "ColumnReference");
                        var name = colRef != null ? FormatColumnRef(colRef) : "";
                        return string.IsNullOrEmpty(name) ? "" : $"{name} {(ascending ? "ASC" : "DESC")}";
                    })
                    .Where(s => !string.IsNullOrEmpty(s));
                var obStr = string.Join(", ", obParts);
                if (!string.IsNullOrEmpty(obStr))
                    node.OrderBy = obStr;
            }

            // OuterReferences (Nested Loops)
            var outerRefsEl = physicalOpEl.Element(Ns + "OuterReferences");
            if (outerRefsEl != null)
            {
                var refs = outerRefsEl.Elements(Ns + "ColumnReference")
                    .Select(c => FormatColumnRef(c))
                    .Where(s => !string.IsNullOrEmpty(s));
                var refsStr = string.Join(", ", refs);
                if (!string.IsNullOrEmpty(refsStr))
                    node.OuterReferences = refsStr;
            }

            // Inner/Outer side join columns (Merge Join)
            node.InnerSideJoinColumns = ParseColumnList(physicalOpEl, "InnerSideJoinColumns");
            node.OuterSideJoinColumns = ParseColumnList(physicalOpEl, "OuterSideJoinColumns");

            // GroupBy columns (Hash/Stream Aggregate)
            node.GroupBy = ParseColumnList(physicalOpEl, "GroupBy");

            // Partition columns (Parallelism)
            node.PartitionColumns = ParseColumnList(physicalOpEl, "PartitionColumns");

            // Wave 2.6: Parallelism HashKeys
            node.HashKeys = ParseColumnList(physicalOpEl, "HashKeys");

            // Segment column
            var segColEl = physicalOpEl.Element(Ns + "SegmentColumn")?.Element(Ns + "ColumnReference");
            if (segColEl != null)
                node.SegmentColumn = FormatColumnRef(segColEl);

            // Defined values (Compute Scalar)
            var definedValsEl = physicalOpEl.Element(Ns + "DefinedValues");
            if (definedValsEl != null)
            {
                var dvParts = new List<string>();
                foreach (var dvEl in definedValsEl.Elements(Ns + "DefinedValue"))
                {
                    var colRef = dvEl.Element(Ns + "ColumnReference");
                    var scalarOp = dvEl.Element(Ns + "ScalarOperator");
                    var colName = colRef != null ? FormatColumnRef(colRef) : "";
                    var expr = scalarOp?.Attribute("ScalarString")?.Value ?? "";
                    if (!string.IsNullOrEmpty(colName) && !string.IsNullOrEmpty(expr))
                        dvParts.Add($"{colName} = {expr}");
                    else if (!string.IsNullOrEmpty(expr))
                        dvParts.Add(expr);
                    else if (!string.IsNullOrEmpty(colName))
                        dvParts.Add(colName);
                }
                if (dvParts.Count > 0)
                    node.DefinedValues = string.Join("; ", dvParts);
            }

            // IndexScan / TableScan properties
            node.ScanDirection = physicalOpEl.Attribute("ScanDirection")?.Value;
            node.ForcedIndex = physicalOpEl.Attribute("ForcedIndex")?.Value is "true" or "1";
            node.ForceScan = physicalOpEl.Attribute("ForceScan")?.Value is "true" or "1";
            node.ForceSeek = physicalOpEl.Attribute("ForceSeek")?.Value is "true" or "1";
            node.NoExpandHint = physicalOpEl.Attribute("NoExpandHint")?.Value is "true" or "1";
            node.Lookup = physicalOpEl.Attribute("Lookup")?.Value is "true" or "1";
            node.DynamicSeek = physicalOpEl.Attribute("DynamicSeek")?.Value is "true" or "1";

            // Table cardinality and rows to be read (on <RelOp> per XSD)
            node.TableCardinality = ParseDouble(relOpEl.Attribute("TableCardinality")?.Value);
            node.EstimatedRowsRead = ParseDouble(relOpEl.Attribute("EstimatedRowsRead")?.Value);
            if (node.EstimatedRowsRead == 0)
                node.EstimatedRowsRead = ParseDouble(relOpEl.Attribute("EstimateRowsWithoutRowGoal")?.Value);

            // TOP operator properties
            var topExprEl = physicalOpEl.Element(Ns + "TopExpression")?.Descendants(Ns + "ScalarOperator").FirstOrDefault();
            if (topExprEl != null)
                node.TopExpression = topExprEl.Attribute("ScalarString")?.Value;
            node.IsPercent = physicalOpEl.Attribute("IsPercent")?.Value is "true" or "1";
            node.WithTies = physicalOpEl.Attribute("WithTies")?.Value is "true" or "1";

            // Wave 2.7: Top OffsetExpression, RowCount, Rows
            var offsetEl = physicalOpEl.Element(Ns + "OffsetExpression")?.Descendants(Ns + "ScalarOperator").FirstOrDefault();
            if (offsetEl != null)
                node.OffsetExpression = offsetEl.Attribute("ScalarString")?.Value;
            node.RowCount = physicalOpEl.Attribute("RowCount")?.Value is "true" or "1";
            node.TopRows = (int)ParseDouble(physicalOpEl.Attribute("Rows")?.Value);

            // Sort properties
            node.SortDistinct = physicalOpEl.Attribute("Distinct")?.Value is "true" or "1";

            // Filter properties
            node.StartupExpression = physicalOpEl.Attribute("StartupExpression")?.Value is "true" or "1";

            // Nested Loops properties
            node.NLOptimized = physicalOpEl.Attribute("Optimized")?.Value is "true" or "1";
            node.WithOrderedPrefetch = physicalOpEl.Attribute("WithOrderedPrefetch")?.Value is "true" or "1";
            node.WithUnorderedPrefetch = physicalOpEl.Attribute("WithUnorderedPrefetch")?.Value is "true" or "1";

            // Hash Match properties
            node.ManyToMany = physicalOpEl.Attribute("ManyToMany")?.Value is "true" or "1";
            node.BitmapCreator = physicalOpEl.Attribute("BitmapCreator")?.Value is "true" or "1";

            // Parallelism properties
            node.Remoting = physicalOpEl.Attribute("Remoting")?.Value is "true" or "1";
            node.LocalParallelism = physicalOpEl.Attribute("LocalParallelism")?.Value is "true" or "1";

            // Wave 3.8: Spool Stack + PrimaryNodeId
            node.SpoolStack = physicalOpEl.Attribute("Stack")?.Value is "true" or "1";
            node.PrimaryNodeId = (int)ParseDouble(physicalOpEl.Attribute("PrimaryNodeId")?.Value);

            // Eager Index Spool — suggest CREATE INDEX from SeekPredicateNew + OutputList
            if (node.LogicalOp == "Eager Spool")
            {
                var spoolSeek = physicalOpEl.Element(Ns + "SeekPredicateNew")
                             ?? physicalOpEl.Element(Ns + "SeekPredicate");
                if (spoolSeek != null)
                {
                    var rangeCols = spoolSeek.Descendants(Ns + "RangeColumns")
                        .SelectMany(rc => rc.Elements(Ns + "ColumnReference"));

                    var keyColumns = new List<string>();
                    string? tblSchema = null;
                    string? tblName = null;

                    foreach (var col in rangeCols)
                    {
                        var colName = col.Attribute("Column")?.Value;
                        if (!string.IsNullOrEmpty(colName))
                            keyColumns.Add(colName);
                        tblSchema ??= col.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", "");
                        tblName ??= col.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "");
                    }

                    if (keyColumns.Count > 0 && !string.IsNullOrEmpty(tblName))
                    {
                        var includeCols = relOpEl.Element(Ns + "OutputList")?.Elements(Ns + "ColumnReference")
                            .Select(c => c.Attribute("Column")?.Value)
                            .Where(c => !string.IsNullOrEmpty(c) && !keyColumns.Contains(c))
                            .ToList() ?? new List<string?>();

                        var prefix = !string.IsNullOrEmpty(tblSchema) ? $"{tblSchema}.{tblName}" : tblName;
                        var keyStr = string.Join(", ", keyColumns);
                        var sql = $"CREATE INDEX [{string.Join("_", keyColumns)}] ON {prefix} ({keyStr})";
                        if (includeCols.Count > 0)
                            sql += $" INCLUDE ({string.Join(", ", includeCols)})";
                        sql += ";";
                        node.SuggestedIndex = sql;
                    }
                }
            }

            // Wave 3.9: Update DMLRequestSort + ActionColumn
            node.DMLRequestSort = physicalOpEl.Attribute("DMLRequestSort")?.Value is "true" or "1";
            var actionColEl = physicalOpEl.Element(Ns + "ActionColumn")?.Element(Ns + "ColumnReference");
            if (actionColEl != null)
                node.ActionColumn = FormatColumnRef(actionColEl);

            // SET predicate (UPDATE operator)
            var setPredicateEl = physicalOpEl.Element(Ns + "SetPredicate");
            if (setPredicateEl != null)
            {
                var so = setPredicateEl.Descendants(Ns + "ScalarOperator").FirstOrDefault();
                node.SetPredicate = so?.Attribute("ScalarString")?.Value;
            }

            // ActualJoinType from runtime info on adaptive joins
            node.ActualJoinType = physicalOpEl.Attribute("ActualJoinType")?.Value;

            // XSD gap: ForceSeekColumnCount (IndexScan)
            node.ForceSeekColumnCount = (int)ParseDouble(physicalOpEl.Attribute("ForceSeekColumnCount")?.Value);

            // XSD gap: PartitionId (IndexScan, TableScan, Sort, NestedLoops, AdaptiveJoin)
            var partitionIdEl = physicalOpEl.Element(Ns + "PartitionId");
            if (partitionIdEl != null)
            {
                var pidCols = partitionIdEl.Elements(Ns + "ColumnReference")
                    .Select(c => FormatColumnRef(c))
                    .Where(s => !string.IsNullOrEmpty(s));
                var pidStr = string.Join(", ", pidCols);
                if (!string.IsNullOrEmpty(pidStr))
                    node.PartitionId = pidStr;
            }

            // XSD gap: StarJoinInfo (Hash, Merge, NL, AdaptiveJoin)
            var starJoinEl = physicalOpEl.Element(Ns + "StarJoinInfo");
            if (starJoinEl != null)
            {
                node.IsStarJoin = starJoinEl.Attribute("Root")?.Value is "true" or "1";
                node.StarJoinOperationType = starJoinEl.Attribute("OperationType")?.Value;
            }

            // XSD gap: ProbeColumn (NL, Parallelism, Update)
            var probeColEl = physicalOpEl.Element(Ns + "ProbeColumn")?.Element(Ns + "ColumnReference");
            if (probeColEl != null)
                node.ProbeColumn = FormatColumnRef(probeColEl);

            // XSD gap: InRow (Parallelism)
            node.InRow = physicalOpEl.Attribute("InRow")?.Value is "true" or "1";

            // XSD gap: ComputeSequence (ComputeScalar)
            node.ComputeSequence = physicalOpEl.Attribute("ComputeSequence")?.Value is "true" or "1";

            // XSD gap: RollupInfo (StreamAggregate)
            var rollupEl = physicalOpEl.Element(Ns + "RollupInfo");
            if (rollupEl != null)
            {
                node.RollupHighestLevel = (int)ParseDouble(rollupEl.Attribute("HighestLevel")?.Value);
                foreach (var rlEl in rollupEl.Elements(Ns + "RollupLevel"))
                    node.RollupLevels.Add((int)ParseDouble(rlEl.Attribute("Level")?.Value));
            }

            // XSD gap: TVF ParameterList
            var tvfParamListEl = physicalOpEl.Element(Ns + "ParameterList");
            if (tvfParamListEl != null)
            {
                var tvfCols = tvfParamListEl.Elements(Ns + "ColumnReference")
                    .Select(c => FormatColumnRef(c))
                    .Where(s => !string.IsNullOrEmpty(s));
                var tvfStr = string.Join(", ", tvfCols);
                if (!string.IsNullOrEmpty(tvfStr))
                    node.TvfParameters = tvfStr;
                // Also check for ScalarOperator children (TVF can have scalar params)
                if (string.IsNullOrEmpty(node.TvfParameters))
                {
                    var tvfScalars = tvfParamListEl.Elements(Ns + "ScalarOperator")
                        .Select(s => s.Attribute("ScalarString")?.Value)
                        .Where(s => !string.IsNullOrEmpty(s));
                    var tvfScalarStr = string.Join(", ", tvfScalars);
                    if (!string.IsNullOrEmpty(tvfScalarStr))
                        node.TvfParameters = tvfScalarStr;
                }
            }

            // XSD gap: OriginalActionColumn (Update)
            var origActionColEl = physicalOpEl.Element(Ns + "OriginalActionColumn")?.Element(Ns + "ColumnReference");
            if (origActionColEl != null)
                node.OriginalActionColumn = FormatColumnRef(origActionColEl);

            // XSD gap: Scalar UDF structured detection
            foreach (var udfEl in ScopedDescendants(physicalOpEl, Ns + "UserDefinedFunction"))
            {
                var udfRef = new ScalarUdfReference
                {
                    FunctionName = udfEl.Attribute("FunctionName")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    IsClrFunction = udfEl.Attribute("IsClrFunction")?.Value is "true" or "1"
                };
                var clrEl = udfEl.Element(Ns + "CLRFunction");
                if (clrEl != null)
                {
                    udfRef.ClrAssembly = clrEl.Attribute("Assembly")?.Value;
                    udfRef.ClrClass = clrEl.Attribute("Class")?.Value;
                    udfRef.ClrMethod = clrEl.Attribute("Method")?.Value;
                }
                if (!string.IsNullOrEmpty(udfRef.FunctionName))
                    node.ScalarUdfs.Add(udfRef);
            }

            // XSD gap: TieColumns (Top operator)
            node.TieColumns = ParseColumnList(physicalOpEl, "TieColumns");

            // XSD gap: UDXName (Extension operator)
            node.UdxName = physicalOpEl.Attribute("UDXName")?.Value;

            // XSD gap: Operator-level IndexedViewInfo
            var opIvInfoEl = physicalOpEl.Element(Ns + "IndexedViewInfo");
            if (opIvInfoEl != null)
            {
                foreach (var ivObjEl in opIvInfoEl.Elements(Ns + "Object"))
                {
                    var ivDb = ivObjEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", "");
                    var ivSchema = ivObjEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", "");
                    var ivTable = ivObjEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "");
                    var ivIndex = ivObjEl.Attribute("Index")?.Value?.Replace("[", "").Replace("]", "");
                    var ivParts = new List<string>();
                    if (!string.IsNullOrEmpty(ivDb)) ivParts.Add(ivDb);
                    if (!string.IsNullOrEmpty(ivSchema)) ivParts.Add(ivSchema);
                    if (!string.IsNullOrEmpty(ivTable)) ivParts.Add(ivTable);
                    if (!string.IsNullOrEmpty(ivIndex)) ivParts.Add(ivIndex);
                    var ivName = string.Join(".", ivParts);
                    if (!string.IsNullOrEmpty(ivName))
                        node.OperatorIndexedViews.Add(ivName);
                }
            }

            // XSD gap: NamedParameterList (IndexScan)
            var namedParamListEl = physicalOpEl.Element(Ns + "NamedParameterList");
            if (namedParamListEl != null)
            {
                foreach (var npEl in namedParamListEl.Elements(Ns + "NamedParameter"))
                {
                    var np = new NamedParameterInfo
                    {
                        Name = npEl.Attribute("Name")?.Value ?? ""
                    };
                    var npScalar = npEl.Element(Ns + "ScalarOperator");
                    if (npScalar != null)
                        np.ScalarString = npScalar.Attribute("ScalarString")?.Value;
                    if (!string.IsNullOrEmpty(np.Name))
                        node.NamedParameters.Add(np);
                }
            }

            // XSD gap: Remote operator metadata
            node.RemoteDestination = physicalOpEl.Attribute("RemoteDestination")?.Value;
            node.RemoteSource = physicalOpEl.Attribute("RemoteSource")?.Value;
            node.RemoteObject = physicalOpEl.Attribute("RemoteObject")?.Value;
            node.RemoteQuery = physicalOpEl.Attribute("RemoteQuery")?.Value;
        }

        // Output columns
        var outputList = relOpEl.Element(Ns + "OutputList");
        if (outputList != null)
        {
            var cols = outputList.Elements(Ns + "ColumnReference")
                .Select(c =>
                {
                    var col = c.Attribute("Column")?.Value ?? "";
                    var tbl = c.Attribute("Table")?.Value ?? "";
                    return string.IsNullOrEmpty(tbl) ? col : $"{tbl}.{col}";
                })
                .Where(s => !string.IsNullOrEmpty(s));
            var colList = string.Join(", ", cols);
            if (!string.IsNullOrEmpty(colList))
                node.OutputColumns = colList.Replace("[", "").Replace("]", "");
        }

        // Warnings
        node.Warnings = ParseWarnings(relOpEl);

        // Wave 3.2: MemoryFractions (on RelOp)
        var memFracEl = relOpEl.Element(Ns + "MemoryFractions");
        if (memFracEl != null)
        {
            node.MemoryFractionInput = ParseDouble(memFracEl.Attribute("Input")?.Value);
            node.MemoryFractionOutput = ParseDouble(memFracEl.Attribute("Output")?.Value);
        }

        // Wave 3.3: RunTimePartitionSummary (on RelOp)
        var rtPartEl = relOpEl.Element(Ns + "RunTimePartitionSummary");
        if (rtPartEl != null)
        {
            var partAccEl = rtPartEl.Element(Ns + "PartitionsAccessed");
            if (partAccEl != null)
            {
                node.PartitionsAccessed = (int)ParseDouble(partAccEl.Attribute("PartitionCount")?.Value);
                var ranges = partAccEl.Elements(Ns + "PartitionRange")
                    .Select(r => $"{r.Attribute("Start")?.Value}-{r.Attribute("End")?.Value}");
                node.PartitionRanges = string.Join(", ", ranges);
                if (string.IsNullOrEmpty(node.PartitionRanges))
                    node.PartitionRanges = null;
            }
        }

        // Wave 2.4: Per-operator memory grants (MemoryGrant on RelOp)
        var memGrantEl = relOpEl.Element(Ns + "MemoryGrant");
        if (memGrantEl != null)
        {
            node.MemoryGrantKB = ParseLong(memGrantEl.Attribute("GrantedMemory")?.Value);
            node.DesiredMemoryKB = ParseLong(memGrantEl.Attribute("DesiredMemory")?.Value);
            node.MaxUsedMemoryKB = ParseLong(memGrantEl.Attribute("MaxUsedMemory")?.Value);
        }

        // Runtime information (actual plan)
        var runtimeEl = relOpEl.Element(Ns + "RunTimeInformation");
        if (runtimeEl != null)
        {
            node.HasActualStats = true;
            long totalRows = 0, totalExecutions = 0, totalRowsRead = 0;
            long totalRebinds = 0, totalRewinds = 0;
            long maxElapsed = 0, totalCpu = 0;
            long totalLogicalReads = 0, totalPhysicalReads = 0;
            long totalScans = 0, totalReadAheads = 0;
            long totalLobLogicalReads = 0, totalLobPhysicalReads = 0, totalLobReadAheads = 0;
            long totalSegmentReads = 0, totalSegmentSkips = 0;
            long totalUdfCpu = 0, maxUdfElapsed = 0;
            string? actualExecMode = null;

            foreach (var thread in runtimeEl.Elements(Ns + "RunTimeCountersPerThread"))
            {
                totalRows += ParseLong(thread.Attribute("ActualRows")?.Value);
                totalExecutions += ParseLong(thread.Attribute("ActualExecutions")?.Value);
                totalRowsRead += ParseLong(thread.Attribute("ActualRowsRead")?.Value);
                totalRebinds += ParseLong(thread.Attribute("ActualRebinds")?.Value);
                totalRewinds += ParseLong(thread.Attribute("ActualRewinds")?.Value);
                totalCpu += ParseLong(thread.Attribute("ActualCPUms")?.Value);
                totalLogicalReads += ParseLong(thread.Attribute("ActualLogicalReads")?.Value);
                totalPhysicalReads += ParseLong(thread.Attribute("ActualPhysicalReads")?.Value);
                totalScans += ParseLong(thread.Attribute("ActualScans")?.Value);
                totalReadAheads += ParseLong(thread.Attribute("ActualReadAheads")?.Value);
                totalLobLogicalReads += ParseLong(thread.Attribute("ActualLobLogicalReads")?.Value);
                totalLobPhysicalReads += ParseLong(thread.Attribute("ActualLobPhysicalReads")?.Value);
                totalLobReadAheads += ParseLong(thread.Attribute("ActualLobReadAheads")?.Value);

                // Wave 3.10: Columnstore segment reads/skips
                totalSegmentReads += ParseLong(thread.Attribute("ActualSegmentReads")?.Value);
                totalSegmentSkips += ParseLong(thread.Attribute("ActualSegmentSkips")?.Value);

                // Wave 3.11: UDF timing
                totalUdfCpu += ParseLong(thread.Attribute("UdfCpuTime")?.Value);
                var udfElapsed = ParseLong(thread.Attribute("UdfElapsedTime")?.Value);
                if (udfElapsed > maxUdfElapsed) maxUdfElapsed = udfElapsed;

                actualExecMode ??= thread.Attribute("ActualExecutionMode")?.Value;

                var elapsed = ParseLong(thread.Attribute("ActualElapsedms")?.Value);
                if (elapsed > maxElapsed) maxElapsed = elapsed;
            }

            node.ActualRows = totalRows;
            node.ActualExecutions = totalExecutions;
            node.ActualRowsRead = totalRowsRead;
            node.ActualRebinds = totalRebinds;
            node.ActualRewinds = totalRewinds;
            node.ActualElapsedMs = maxElapsed;
            node.ActualCPUMs = totalCpu;
            node.ActualLogicalReads = totalLogicalReads;
            node.ActualPhysicalReads = totalPhysicalReads;
            node.ActualScans = totalScans;
            node.ActualReadAheads = totalReadAheads;
            node.ActualLobLogicalReads = totalLobLogicalReads;
            node.ActualLobPhysicalReads = totalLobPhysicalReads;
            node.ActualLobReadAheads = totalLobReadAheads;
            node.ActualExecutionMode = actualExecMode;
            node.ActualSegmentReads = totalSegmentReads;
            node.ActualSegmentSkips = totalSegmentSkips;
            node.UdfCpuTimeMs = totalUdfCpu;
            node.UdfElapsedTimeMs = maxUdfElapsed;

            // Store per-thread data for parallel skew analysis
            foreach (var thread in runtimeEl.Elements(Ns + "RunTimeCountersPerThread"))
            {
                node.PerThreadStats.Add(new PerThreadRuntimeInfo
                {
                    ThreadId = (int)ParseDouble(thread.Attribute("Thread")?.Value),
                    ActualRows = ParseLong(thread.Attribute("ActualRows")?.Value),
                    ActualExecutions = ParseLong(thread.Attribute("ActualExecutions")?.Value),
                    ActualElapsedMs = ParseLong(thread.Attribute("ActualElapsedms")?.Value),
                    ActualCPUMs = ParseLong(thread.Attribute("ActualCPUms")?.Value),
                    ActualRowsRead = ParseLong(thread.Attribute("ActualRowsRead")?.Value),
                    ActualLogicalReads = ParseLong(thread.Attribute("ActualLogicalReads")?.Value),
                    ActualPhysicalReads = ParseLong(thread.Attribute("ActualPhysicalReads")?.Value),
                    ActualScans = ParseLong(thread.Attribute("ActualScans")?.Value),
                    ActualReadAheads = ParseLong(thread.Attribute("ActualReadAheads")?.Value),
                    FirstActiveTime = ParseLong(thread.Attribute("FirstActiveTime")?.Value),
                    LastActiveTime = ParseLong(thread.Attribute("LastActiveTime")?.Value),
                    OpenTime = ParseLong(thread.Attribute("OpenTime")?.Value),
                    FirstRowTime = ParseLong(thread.Attribute("FirstRowTime")?.Value),
                    LastRowTime = ParseLong(thread.Attribute("LastRowTime")?.Value),
                    CloseTime = ParseLong(thread.Attribute("CloseTime")?.Value),
                    InputMemoryGrant = ParseLong(thread.Attribute("InputMemoryGrant")?.Value),
                    OutputMemoryGrant = ParseLong(thread.Attribute("OutputMemoryGrant")?.Value),
                    UsedMemoryGrant = ParseLong(thread.Attribute("UsedMemoryGrant")?.Value),
                    Batches = ParseLong(thread.Attribute("Batches")?.Value),
                    ActualEndOfScans = ParseLong(thread.Attribute("ActualEndOfScans")?.Value),
                    ActualLocallyAggregatedRows = ParseLong(thread.Attribute("ActualLocallyAggregatedRows")?.Value),
                    IsInterleavedExecuted = thread.Attribute("IsInterleavedExecuted")?.Value is "true" or "1",
                    RowRequalifications = ParseLong(thread.Attribute("RowRequalifications")?.Value)
                });
            }
        }

        // Recurse into child RelOps
        foreach (var childRelOp in FindChildRelOps(relOpEl))
        {
            var childNode = ParseRelOp(childRelOp);
            childNode.Parent = node;
            node.Children.Add(childNode);
        }

        return node;
    }

    private static XElement? GetOperatorElement(XElement relOpEl)
    {
        foreach (var child in relOpEl.Elements())
        {
            var name = child.Name.LocalName;
            if (name != "OutputList" && name != "RunTimeInformation" && name != "Warnings"
                && name != "MemoryFractions" && name != "RunTimePartitionSummary"
                && name != "MemoryGrant" && name != "InternalInfo")
            {
                return child;
            }
        }
        return null;
    }

    private static IEnumerable<XElement> FindChildRelOps(XElement relOpEl)
    {
        var operatorEl = GetOperatorElement(relOpEl);
        if (operatorEl == null) yield break;

        foreach (var child in operatorEl.Elements(Ns + "RelOp"))
            yield return child;

        foreach (var child in operatorEl.Elements())
        {
            if (child.Name.LocalName == "RelOp") continue;
            foreach (var nestedRelOp in child.Elements(Ns + "RelOp"))
                yield return nestedRelOp;
        }
    }

    private static List<MissingIndex> ParseMissingIndexes(XElement queryPlanEl)
    {
        var result = new List<MissingIndex>();
        var missingIndexesEl = queryPlanEl.Element(Ns + "MissingIndexes");
        if (missingIndexesEl == null) return result;

        foreach (var groupEl in missingIndexesEl.Elements(Ns + "MissingIndexGroup"))
        {
            var impact = ParseDouble(groupEl.Attribute("Impact")?.Value);
            foreach (var indexEl in groupEl.Elements(Ns + "MissingIndex"))
            {
                var mi = new MissingIndex
                {
                    Database = indexEl.Attribute("Database")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    Schema = indexEl.Attribute("Schema")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    Table = indexEl.Attribute("Table")?.Value?.Replace("[", "").Replace("]", "") ?? "",
                    Impact = impact
                };

                foreach (var colGroup in indexEl.Elements(Ns + "ColumnGroup"))
                {
                    var usage = colGroup.Attribute("Usage")?.Value ?? "";
                    var cols = colGroup.Elements(Ns + "Column")
                        .Select(c => c.Attribute("Name")?.Value?.Replace("[", "").Replace("]", "") ?? "")
                        .Where(s => !string.IsNullOrEmpty(s))
                        .ToList();

                    switch (usage)
                    {
                        case "EQUALITY": mi.EqualityColumns = cols; break;
                        case "INEQUALITY": mi.InequalityColumns = cols; break;
                        case "INCLUDE": mi.IncludeColumns = cols; break;
                    }
                }

                var keyCols = mi.EqualityColumns.Concat(mi.InequalityColumns).ToList();
                if (keyCols.Count > 0)
                {
                    var create = $"CREATE NONCLUSTERED INDEX [IX_{mi.Table}_{string.Join("_", keyCols.Take(3))}]\nON {mi.Schema}.{mi.Table} ({string.Join(", ", keyCols)})";
                    if (mi.IncludeColumns.Count > 0)
                        create += $"\nINCLUDE ({string.Join(", ", mi.IncludeColumns)})";
                    mi.CreateStatement = create;
                }

                result.Add(mi);
            }
        }
        return result;
    }

    /// <summary>
    /// Parse warnings from a parent element that contains a &lt;Warnings&gt; child (e.g. RelOp).
    /// </summary>
    private static List<PlanWarning> ParseWarnings(XElement parentEl)
    {
        var warningsEl = parentEl.Element(Ns + "Warnings");
        if (warningsEl == null) return new List<PlanWarning>();
        return ParseWarningsFromElement(warningsEl);
    }

    /// <summary>
    /// Parse warnings directly from a &lt;Warnings&gt; element.
    /// </summary>
    private static List<PlanWarning> ParseWarningsFromElement(XElement warningsEl)
    {
        var result = new List<PlanWarning>();

        // No join predicate
        if (warningsEl.Attribute("NoJoinPredicate")?.Value is "true" or "1")
        {
            result.Add(new PlanWarning
            {
                WarningType = "No Join Predicate",
                Message = "This join has no join predicate (possible cross join)",
                Severity = PlanWarningSeverity.Critical
            });
        }

        if (warningsEl.Attribute("SpatialGuess")?.Value is "true" or "1")
        {
            result.Add(new PlanWarning
            {
                WarningType = "Spatial Guess",
                Message = "Spatial index selectivity was guessed",
                Severity = PlanWarningSeverity.Info
            });
        }

        if (warningsEl.Attribute("UnmatchedIndexes")?.Value is "true" or "1")
        {
            result.Add(new PlanWarning
            {
                WarningType = "Unmatched Indexes",
                Message = "Indexes could not be matched due to parameterization",
                Severity = PlanWarningSeverity.Warning
            });
        }

        if (warningsEl.Attribute("FullUpdateForOnlineIndexBuild")?.Value is "true" or "1")
        {
            result.Add(new PlanWarning
            {
                WarningType = "Full Update for Online Index Build",
                Message = "Full update required for online index build operation",
                Severity = PlanWarningSeverity.Info
            });
        }

        // Spill to TempDb (with enhanced details — Wave 3.7)
        foreach (var spillEl in warningsEl.Elements(Ns + "SpillToTempDb"))
        {
            var spillLevel = spillEl.Attribute("SpillLevel")?.Value ?? "?";
            var threadCount = spillEl.Attribute("SpilledThreadCount")?.Value ?? "?";
            var msg = $"Spill level {spillLevel}, {threadCount} thread(s)";

            var grantedKB = ParseLong(spillEl.Attribute("GrantedMemoryKB")?.Value);
            var usedKB = ParseLong(spillEl.Attribute("UsedMemoryKB")?.Value);
            var writes = ParseLong(spillEl.Attribute("WritesToTempDb")?.Value);
            var reads = ParseLong(spillEl.Attribute("ReadsFromTempDb")?.Value);
            if (grantedKB > 0 || writes > 0)
            {
                msg += $" — Granted: {grantedKB:N0} KB, Used: {usedKB:N0} KB";
                if (writes > 0) msg += $", Writes: {writes:N0}";
                if (reads > 0) msg += $", Reads: {reads:N0}";
            }

            result.Add(new PlanWarning
            {
                WarningType = "Spill to TempDb",
                Message = msg,
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Sort spill details
        foreach (var sortSpillEl in warningsEl.Elements(Ns + "SortSpillDetails"))
        {
            result.Add(new PlanWarning
            {
                WarningType = "Sort Spill",
                Message = $"Sort spill — Granted: {ParseLong(sortSpillEl.Attribute("GrantedMemoryKb")?.Value):N0} KB, Used: {ParseLong(sortSpillEl.Attribute("UsedMemoryKb")?.Value):N0} KB, Writes: {ParseLong(sortSpillEl.Attribute("WritesToTempDb")?.Value):N0}, Reads: {ParseLong(sortSpillEl.Attribute("ReadsFromTempDb")?.Value):N0}",
                Severity = PlanWarningSeverity.Warning,
                SpillDetails = new SpillDetail
                {
                    SpillType = "Sort",
                    GrantedMemoryKB = ParseLong(sortSpillEl.Attribute("GrantedMemoryKb")?.Value),
                    UsedMemoryKB = ParseLong(sortSpillEl.Attribute("UsedMemoryKb")?.Value),
                    WritesToTempDb = ParseLong(sortSpillEl.Attribute("WritesToTempDb")?.Value),
                    ReadsFromTempDb = ParseLong(sortSpillEl.Attribute("ReadsFromTempDb")?.Value)
                }
            });
        }

        // Hash spill details
        foreach (var hashSpillEl in warningsEl.Elements(Ns + "HashSpillDetails"))
        {
            result.Add(new PlanWarning
            {
                WarningType = "Hash Spill",
                Message = $"Hash spill — Granted: {ParseLong(hashSpillEl.Attribute("GrantedMemoryKb")?.Value):N0} KB, Used: {ParseLong(hashSpillEl.Attribute("UsedMemoryKb")?.Value):N0} KB, Writes: {ParseLong(hashSpillEl.Attribute("WritesToTempDb")?.Value):N0}, Reads: {ParseLong(hashSpillEl.Attribute("ReadsFromTempDb")?.Value):N0}",
                Severity = PlanWarningSeverity.Warning,
                SpillDetails = new SpillDetail
                {
                    SpillType = "Hash",
                    GrantedMemoryKB = ParseLong(hashSpillEl.Attribute("GrantedMemoryKb")?.Value),
                    UsedMemoryKB = ParseLong(hashSpillEl.Attribute("UsedMemoryKb")?.Value),
                    WritesToTempDb = ParseLong(hashSpillEl.Attribute("WritesToTempDb")?.Value),
                    ReadsFromTempDb = ParseLong(hashSpillEl.Attribute("ReadsFromTempDb")?.Value)
                }
            });
        }

        // Exchange spill details
        foreach (var exchSpillEl in warningsEl.Elements(Ns + "ExchangeSpillDetails"))
        {
            result.Add(new PlanWarning
            {
                WarningType = "Exchange Spill",
                Message = $"Exchange spill — Writes: {ParseLong(exchSpillEl.Attribute("WritesToTempDb")?.Value):N0}",
                Severity = PlanWarningSeverity.Warning,
                SpillDetails = new SpillDetail
                {
                    SpillType = "Exchange",
                    WritesToTempDb = ParseLong(exchSpillEl.Attribute("WritesToTempDb")?.Value)
                }
            });
        }

        // SpillOccurred
        var spillOccurredEl = warningsEl.Element(Ns + "SpillOccurred");
        if (spillOccurredEl != null)
        {
            result.Add(new PlanWarning
            {
                WarningType = "Spill Occurred",
                Message = "Spill occurred during execution (from last query plan stats)",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Memory grant warning
        var memWarnEl = warningsEl.Element(Ns + "MemoryGrantWarning");
        if (memWarnEl != null)
        {
            var kind = memWarnEl.Attribute("GrantWarningKind")?.Value ?? "Unknown";
            var requested = ParseLong(memWarnEl.Attribute("RequestedMemory")?.Value);
            var granted = ParseLong(memWarnEl.Attribute("GrantedMemory")?.Value);
            var maxUsed = ParseLong(memWarnEl.Attribute("MaxUsedMemory")?.Value);
            result.Add(new PlanWarning
            {
                WarningType = "Memory Grant",
                Message = $"{kind}: Requested {requested:N0} KB, Granted {granted:N0} KB, Used {maxUsed:N0} KB",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Implicit conversions
        foreach (var convertEl in warningsEl.Elements(Ns + "PlanAffectingConvert"))
        {
            var issue = convertEl.Attribute("ConvertIssue")?.Value ?? "Unknown";
            var expr = convertEl.Attribute("Expression")?.Value ?? "";
            result.Add(new PlanWarning
            {
                WarningType = "Implicit Conversion",
                Message = $"{issue}: {expr}",
                Severity = issue.Contains("Cardinality") ? PlanWarningSeverity.Warning : PlanWarningSeverity.Critical
            });
        }

        // Columns with no statistics
        var noStatsEl = warningsEl.Element(Ns + "ColumnsWithNoStatistics");
        if (noStatsEl != null)
        {
            var cols = noStatsEl.Elements(Ns + "ColumnReference")
                .Select(c => c.Attribute("Column")?.Value ?? "")
                .Where(s => !string.IsNullOrEmpty(s));
            result.Add(new PlanWarning
            {
                WarningType = "Missing Statistics",
                Message = $"No statistics on: {string.Join(", ", cols)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Wave 2.3: Columns with stale statistics
        var staleStatsEl = warningsEl.Element(Ns + "ColumnsWithStaleStatistics");
        if (staleStatsEl != null)
        {
            var cols = staleStatsEl.Elements(Ns + "ColumnReference")
                .Select(c => c.Attribute("Column")?.Value ?? "")
                .Where(s => !string.IsNullOrEmpty(s));
            result.Add(new PlanWarning
            {
                WarningType = "Stale Statistics",
                Message = $"Stale statistics on: {string.Join(", ", cols)}",
                Severity = PlanWarningSeverity.Warning
            });
        }

        // Wait warnings
        foreach (var waitEl in warningsEl.Elements(Ns + "Wait"))
        {
            result.Add(new PlanWarning
            {
                WarningType = "Wait",
                Message = $"{waitEl.Attribute("WaitType")?.Value}: {waitEl.Attribute("WaitTime")?.Value}ms",
                Severity = PlanWarningSeverity.Info
            });
        }

        return result;
    }

    private static void ComputeOperatorCosts(ParsedPlan plan)
    {
        foreach (var batch in plan.Batches)
        {
            foreach (var stmt in batch.Statements)
            {
                if (stmt.RootNode == null) continue;
                var totalCost = stmt.StatementSubTreeCost > 0
                    ? stmt.StatementSubTreeCost
                    : stmt.RootNode.EstimatedTotalSubtreeCost;
                if (totalCost <= 0) totalCost = 1;
                ComputeNodeCosts(stmt.RootNode, totalCost);
            }
        }
    }

    private static void ComputeNodeCosts(PlanNode node, double totalStatementCost)
    {
        var childrenSubtreeCost = node.Children.Sum(c => c.EstimatedTotalSubtreeCost);
        node.EstimatedOperatorCost = Math.Max(0, node.EstimatedTotalSubtreeCost - childrenSubtreeCost);
        node.CostPercent = (int)Math.Round((node.EstimatedOperatorCost / totalStatementCost) * 100);
        node.CostPercent = Math.Min(100, Math.Max(0, node.CostPercent));

        foreach (var child in node.Children)
            ComputeNodeCosts(child, totalStatementCost);
    }

    private static IEnumerable<XElement> ScopedDescendants(XElement element, XName name)
    {
        foreach (var child in element.Elements())
        {
            if (child.Name == Ns + "RelOp") continue;
            if (child.Name == name) yield return child;
            foreach (var desc in ScopedDescendants(child, name))
                yield return desc;
        }
    }

    private static string? ParseColumnList(XElement parent, string elementName)
    {
        var el = parent.Element(Ns + elementName);
        if (el == null) return null;
        var cols = el.Elements(Ns + "ColumnReference")
            .Select(c => FormatColumnRef(c))
            .Where(s => !string.IsNullOrEmpty(s));
        var result = string.Join(", ", cols);
        return string.IsNullOrEmpty(result) ? null : result;
    }

    private static string FormatColumnRef(XElement colRef)
    {
        var col = colRef.Attribute("Column")?.Value ?? "";
        var tbl = colRef.Attribute("Table")?.Value ?? "";
        var result = string.IsNullOrEmpty(tbl) ? col : $"{tbl}.{col}";
        return result.Replace("[", "").Replace("]", "");
    }

    private static double ParseDouble(string? value)
    {
        if (string.IsNullOrEmpty(value)) return 0;
        return double.TryParse(value, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var result) ? result : 0;
    }

    private static long ParseLong(string? value)
    {
        if (string.IsNullOrEmpty(value)) return 0;
        return long.TryParse(value, out var result) ? result : 0;
    }
}
