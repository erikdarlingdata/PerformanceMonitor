using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml.Linq;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services;

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
                    var stmt = ParseStatement(stmtEl);
                    if (stmt != null)
                        batch.Statements.Add(stmt);
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

    private static PlanStatement? ParseStatement(XElement stmtEl)
    {
        var stmt = new PlanStatement
        {
            StatementText = stmtEl.Attribute("StatementText")?.Value ?? "",
            StatementType = stmtEl.Attribute("StatementType")?.Value ?? "",
            StatementSubTreeCost = ParseDouble(stmtEl.Attribute("StatementSubTreeCost")?.Value),
            StatementEstRows = (int)ParseDouble(stmtEl.Attribute("StatementEstRows")?.Value)
        };

        var queryPlanEl = stmtEl.Element(Ns + "QueryPlan");
        if (queryPlanEl == null) return stmt;

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
                MaxUsedMemoryKB = ParseLong(memEl.Attribute("MaxUsedMemory")?.Value)
            };
        }

        // Missing indexes
        stmt.MissingIndexes = ParseMissingIndexes(queryPlanEl);

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

        return stmt;
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
            Parallel = relOpEl.Attribute("Parallel")?.Value == "true" || relOpEl.Attribute("Parallel")?.Value == "1",
            ExecutionMode = relOpEl.Attribute("EstimatedExecutionMode")?.Value
        };

        // Map to icon
        node.IconName = PlanIconMapper.GetIconName(node.PhysicalOp);

        // Handle special icon cases
        var physicalOpEl = GetOperatorElement(relOpEl);
        if (physicalOpEl != null)
        {
            // Object reference (table/index name)
            var objEl = physicalOpEl.Descendants(Ns + "Object").FirstOrDefault();
            if (objEl != null)
            {
                var schema = objEl.Attribute("Schema")?.Value;
                var table = objEl.Attribute("Table")?.Value;
                var index = objEl.Attribute("Index")?.Value;
                var parts = new List<string>();
                if (!string.IsNullOrEmpty(schema)) parts.Add(schema);
                if (!string.IsNullOrEmpty(table)) parts.Add(table);
                if (!string.IsNullOrEmpty(index)) parts.Add(index);
                node.ObjectName = string.Join(".", parts).Replace("[", "").Replace("]", "");
            }

            // Ordered attribute
            node.Ordered = physicalOpEl.Attribute("Ordered")?.Value == "true" || physicalOpEl.Attribute("Ordered")?.Value == "1";

            // Seek predicates
            var seekPreds = physicalOpEl.Descendants(Ns + "SeekPredicateNew")
                .Concat(physicalOpEl.Descendants(Ns + "SeekPredicate"));
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

        // Runtime information (actual plan)
        var runtimeEl = relOpEl.Element(Ns + "RunTimeInformation");
        if (runtimeEl != null)
        {
            node.HasActualStats = true;
            long totalRows = 0, totalExecutions = 0, totalRowsRead = 0;
            long totalRebinds = 0, totalRewinds = 0;
            long maxElapsed = 0, totalCpu = 0;
            long totalLogicalReads = 0, totalPhysicalReads = 0;

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
        }

        // Memory fractions
        var memFractions = relOpEl.Element(Ns + "MemoryFractions");
        if (memFractions != null)
        {
            // Memory grant data from statement level is propagated later
        }

        // Recurse into child RelOps
        // Children can be in various operator-specific elements
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
        // The operator-specific element is the first child that isn't OutputList, RunTimeInformation, etc.
        foreach (var child in relOpEl.Elements())
        {
            var name = child.Name.LocalName;
            if (name != "OutputList" && name != "RunTimeInformation" && name != "Warnings"
                && name != "MemoryFractions" && name != "RunTimePartitionSummary"
                && name != "InternalInfo")
            {
                return child;
            }
        }
        return null;
    }

    private static IEnumerable<XElement> FindChildRelOps(XElement relOpEl)
    {
        // Child RelOps are nested inside the operator-specific element
        var operatorEl = GetOperatorElement(relOpEl);
        if (operatorEl == null) yield break;

        // Direct RelOp children of the operator element
        foreach (var child in operatorEl.Elements(Ns + "RelOp"))
            yield return child;

        // Some operators nest RelOps deeper (e.g., Hash has BuildResidual/ProbeResidual)
        // Walk one level of non-RelOp children to find nested RelOps
        foreach (var child in operatorEl.Elements())
        {
            if (child.Name.LocalName == "RelOp") continue; // Already yielded
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

                // Generate CREATE INDEX statement
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

    private static List<PlanWarning> ParseWarnings(XElement relOpEl)
    {
        var result = new List<PlanWarning>();
        var warningsEl = relOpEl.Element(Ns + "Warnings");
        if (warningsEl == null) return result;

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

        // Spill to TempDb
        foreach (var spillEl in warningsEl.Elements(Ns + "SpillToTempDb"))
        {
            var spillLevel = spillEl.Attribute("SpillLevel")?.Value ?? "?";
            var threadCount = spillEl.Attribute("SpilledThreadCount")?.Value ?? "?";
            result.Add(new PlanWarning
            {
                WarningType = "Spill to TempDb",
                Message = $"Spill level {spillLevel}, {threadCount} thread(s)",
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
                if (totalCost <= 0) totalCost = 1; // Avoid division by zero
                ComputeNodeCosts(stmt.RootNode, totalCost);
            }
        }
    }

    private static void ComputeNodeCosts(PlanNode node, double totalStatementCost)
    {
        // Operator cost = subtree cost - sum of children's subtree costs
        var childrenSubtreeCost = node.Children.Sum(c => c.EstimatedTotalSubtreeCost);
        node.EstimatedOperatorCost = Math.Max(0, node.EstimatedTotalSubtreeCost - childrenSubtreeCost);
        node.CostPercent = (int)Math.Round((node.EstimatedOperatorCost / totalStatementCost) * 100);
        node.CostPercent = Math.Min(100, Math.Max(0, node.CostPercent));

        foreach (var child in node.Children)
            ComputeNodeCosts(child, totalStatementCost);
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
