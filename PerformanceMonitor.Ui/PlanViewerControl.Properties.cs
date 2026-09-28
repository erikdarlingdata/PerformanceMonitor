/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Ui;

public partial class PlanViewerControl
{
    #region Properties Panel

    /// <summary>
    /// One row of the properties panel in WPF terms: the pure <see cref="PropertyRow"/> the
    /// filter and copy menu work from, plus every control the row occupies (so the filter can
    /// hide them) and the shared copy menu.
    /// </summary>
    private sealed class PropertyPanelRow
    {
        public PropertyRow Model { get; init; } = new();
        public List<FrameworkElement> Controls { get; } = new();
        public ContextMenu? Menu { get; set; }
    }

    private sealed class PropertyPanelSection
    {
        public string Title { get; init; } = "";
        public Expander Expander { get; init; } = null!;
        public List<PropertyPanelRow> Rows { get; } = new();
        public PropertySection Model { get; init; } = null!;
    }

    private void ShowPropertiesPanel(PlanNode node)
    {
        PropertiesContent.Children.Clear();
        _currentPropertySection = null;
        _propertySections.Clear();
        _currentSection = null;

        // Header
        var headerText = node.PhysicalOp;
        if (node.LogicalOp != node.PhysicalOp && !string.IsNullOrEmpty(node.LogicalOp)
            && !node.PhysicalOp.Contains(node.LogicalOp, StringComparison.OrdinalIgnoreCase))
            headerText += $" ({node.LogicalOp})";
        PropertiesHeader.Text = headerText;
        PropertiesSubHeader.Text = $"Node ID: {node.NodeId}";

        // === General Section ===
        AddPropertySection("General");
        AddPropertyRow("Physical Operation", node.PhysicalOp);
        AddPropertyRow("Logical Operation", node.LogicalOp);
        AddPropertyRow("Node ID", $"{node.NodeId}");
        if (!string.IsNullOrEmpty(node.ExecutionMode))
            AddPropertyRow("Execution Mode", node.ExecutionMode);
        if (!string.IsNullOrEmpty(node.ActualExecutionMode) && node.ActualExecutionMode != node.ExecutionMode)
            AddPropertyRow("Actual Exec Mode", node.ActualExecutionMode);
        AddPropertyRow("Parallel", node.Parallel ? "True" : "False");
        if (node.Partitioned)
            AddPropertyRow("Partitioned", "True");
        if (node.EstimatedDOP > 0)
            AddPropertyRow("Estimated DOP", $"{node.EstimatedDOP}");

        // Scan/seek-related properties — always show for operators that have object references
        if (!string.IsNullOrEmpty(node.FullObjectName))
        {
            AddPropertyRow("Ordered", node.Ordered ? "True" : "False");
            if (!string.IsNullOrEmpty(node.ScanDirection))
                AddPropertyRow("Scan Direction", node.ScanDirection);
            AddPropertyRow("Forced Index", node.ForcedIndex ? "True" : "False");
            AddPropertyRow("ForceScan", node.ForceScan ? "True" : "False");
            AddPropertyRow("ForceSeek", node.ForceSeek ? "True" : "False");
            AddPropertyRow("NoExpandHint", node.NoExpandHint ? "True" : "False");
            if (node.Lookup)
                AddPropertyRow("Lookup", "True");
            if (node.DynamicSeek)
                AddPropertyRow("Dynamic Seek", "True");
        }

        if (!string.IsNullOrEmpty(node.StorageType))
            AddPropertyRow("Storage", node.StorageType);
        if (node.IsAdaptive)
            AddPropertyRow("Adaptive", "True");
        if (node.SpillOccurredDetail)
            AddPropertyRow("Spill Occurred", "True");

        // === Object Section ===
        if (!string.IsNullOrEmpty(node.FullObjectName))
        {
            AddPropertySection("Object");
            AddPropertyRow("Full Name", node.FullObjectName, isCode: true);
            if (!string.IsNullOrEmpty(node.ServerName))
                AddPropertyRow("Server", node.ServerName);
            if (!string.IsNullOrEmpty(node.DatabaseName))
                AddPropertyRow("Database", node.DatabaseName);
            if (!string.IsNullOrEmpty(node.ObjectAlias))
                AddPropertyRow("Alias", node.ObjectAlias);
            if (!string.IsNullOrEmpty(node.IndexName))
                AddPropertyRow("Index", node.IndexName);
            if (!string.IsNullOrEmpty(node.IndexKind))
                AddPropertyRow("Index Kind", node.IndexKind);
            if (node.FilteredIndex)
                AddPropertyRow("Filtered Index", "True");
            if (node.TableReferenceId > 0)
                AddPropertyRow("Table Ref Id", $"{node.TableReferenceId}");
        }

        // === Operator Details Section ===
        var hasOperatorDetails = !string.IsNullOrEmpty(node.OrderBy)
            || !string.IsNullOrEmpty(node.TopExpression)
            || !string.IsNullOrEmpty(node.GroupBy)
            || !string.IsNullOrEmpty(node.PartitionColumns)
            || !string.IsNullOrEmpty(node.HashKeys)
            || !string.IsNullOrEmpty(node.SegmentColumn)
            || !string.IsNullOrEmpty(node.DefinedValues)
            || !string.IsNullOrEmpty(node.OuterReferences)
            || !string.IsNullOrEmpty(node.InnerSideJoinColumns)
            || !string.IsNullOrEmpty(node.OuterSideJoinColumns)
            || !string.IsNullOrEmpty(node.ActionColumn)
            || node.ManyToMany || node.PhysicalOp == "Merge Join" || node.BitmapCreator
            || node.SortDistinct || node.StartupExpression
            || node.NLOptimized || node.WithOrderedPrefetch || node.WithUnorderedPrefetch
            || node.WithTies || node.Remoting || node.LocalParallelism
            || node.SpoolStack || node.DMLRequestSort || node.NonClusteredIndexCount > 0
            || !string.IsNullOrEmpty(node.OffsetExpression) || node.TopRows > 0
            || !string.IsNullOrEmpty(node.ConstantScanValues)
            || !string.IsNullOrEmpty(node.UdxUsedColumns);

        if (hasOperatorDetails)
        {
            AddPropertySection("Operator Details");
            if (!string.IsNullOrEmpty(node.OrderBy))
                AddPropertyRow("Order By", node.OrderBy, isCode: true);
            if (!string.IsNullOrEmpty(node.TopExpression))
            {
                var topText = node.TopExpression;
                if (node.IsPercent) topText += " PERCENT";
                if (node.WithTies) topText += " WITH TIES";
                AddPropertyRow("Top", topText);
            }
            if (node.SortDistinct)
                AddPropertyRow("Distinct Sort", "True");
            if (node.StartupExpression)
                AddPropertyRow("Startup Expression", "True");
            if (node.NLOptimized)
                AddPropertyRow("Optimized", "True");
            if (node.WithOrderedPrefetch)
                AddPropertyRow("Ordered Prefetch", "True");
            if (node.WithUnorderedPrefetch)
                AddPropertyRow("Unordered Prefetch", "True");
            if (node.BitmapCreator)
                AddPropertyRow("Bitmap Creator", "True");
            if (node.Remoting)
                AddPropertyRow("Remoting", "True");
            if (node.LocalParallelism)
                AddPropertyRow("Local Parallelism", "True");
            if (!string.IsNullOrEmpty(node.GroupBy))
                AddPropertyRow("Group By", node.GroupBy, isCode: true);
            if (!string.IsNullOrEmpty(node.PartitionColumns))
                AddPropertyRow("Partition Columns", node.PartitionColumns, isCode: true);
            if (!string.IsNullOrEmpty(node.HashKeys))
                AddPropertyRow("Hash Keys", node.HashKeys, isCode: true);
            if (!string.IsNullOrEmpty(node.OffsetExpression))
                AddPropertyRow("Offset", node.OffsetExpression);
            if (node.TopRows > 0)
                AddPropertyRow("Rows", $"{node.TopRows}");
            if (node.SpoolStack)
                AddPropertyRow("Stack Spool", "True");
            if (node.PrimaryNodeId > 0)
                AddPropertyRow("Primary Node Id", $"{node.PrimaryNodeId}");
            if (node.DMLRequestSort)
                AddPropertyRow("DML Request Sort", "True");
            if (node.NonClusteredIndexCount > 0)
            {
                AddPropertyRow("NC Indexes Maintained", $"{node.NonClusteredIndexCount}");
                foreach (var ixName in node.NonClusteredIndexNames)
                    AddPropertyRow("", ixName, isCode: true);
            }
            if (!string.IsNullOrEmpty(node.ActionColumn))
                AddPropertyRow("Action Column", node.ActionColumn, isCode: true);
            if (!string.IsNullOrEmpty(node.SegmentColumn))
                AddPropertyRow("Segment Column", node.SegmentColumn, isCode: true);
            if (!string.IsNullOrEmpty(node.DefinedValues))
                AddPropertyRow("Defined Values", node.DefinedValues, isCode: true);
            if (!string.IsNullOrEmpty(node.OuterReferences))
                AddPropertyRow("Outer References", node.OuterReferences, isCode: true);
            if (!string.IsNullOrEmpty(node.InnerSideJoinColumns))
                AddPropertyRow("Inner Join Cols", node.InnerSideJoinColumns, isCode: true);
            if (!string.IsNullOrEmpty(node.OuterSideJoinColumns))
                AddPropertyRow("Outer Join Cols", node.OuterSideJoinColumns, isCode: true);
            if (node.PhysicalOp == "Merge Join")
                AddPropertyRow("Many to Many", node.ManyToMany ? "Yes" : "No");
            else if (node.ManyToMany)
                AddPropertyRow("Many to Many", "Yes");
            if (!string.IsNullOrEmpty(node.ConstantScanValues))
                AddPropertyRow("Values", node.ConstantScanValues, isCode: true);
            if (!string.IsNullOrEmpty(node.UdxUsedColumns))
                AddPropertyRow("UDX Columns", node.UdxUsedColumns, isCode: true);
            if (node.RowCount)
                AddPropertyRow("Row Count", "True");
            if (node.ForceSeekColumnCount > 0)
                AddPropertyRow("ForceSeek Columns", $"{node.ForceSeekColumnCount}");
            if (!string.IsNullOrEmpty(node.PartitionId))
                AddPropertyRow("Partition Id", node.PartitionId, isCode: true);
            if (node.IsStarJoin)
                AddPropertyRow("Star Join Root", "True");
            if (!string.IsNullOrEmpty(node.StarJoinOperationType))
                AddPropertyRow("Star Join Type", node.StarJoinOperationType);
            if (!string.IsNullOrEmpty(node.ProbeColumn))
                AddPropertyRow("Probe Column", node.ProbeColumn, isCode: true);
            if (node.InRow)
                AddPropertyRow("In-Row", "True");
            if (node.ComputeSequence)
                AddPropertyRow("Compute Sequence", "True");
            if (node.RollupHighestLevel > 0)
                AddPropertyRow("Rollup Highest Level", $"{node.RollupHighestLevel}");
            if (node.RollupLevels.Count > 0)
                AddPropertyRow("Rollup Levels", string.Join(", ", node.RollupLevels));
            if (!string.IsNullOrEmpty(node.TvfParameters))
                AddPropertyRow("TVF Parameters", node.TvfParameters, isCode: true);
            if (!string.IsNullOrEmpty(node.OriginalActionColumn))
                AddPropertyRow("Original Action Col", node.OriginalActionColumn, isCode: true);
            if (!string.IsNullOrEmpty(node.TieColumns))
                AddPropertyRow("WITH TIES Columns", node.TieColumns, isCode: true);
            if (!string.IsNullOrEmpty(node.UdxName))
                AddPropertyRow("UDX Name", node.UdxName);
            if (node.GroupExecuted)
                AddPropertyRow("Group Executed", "True");
            if (node.RemoteDataAccess)
                AddPropertyRow("Remote Data Access", "True");
            if (node.OptimizedHalloweenProtectionUsed)
                AddPropertyRow("Halloween Protection", "True");
            if (node.StatsCollectionId > 0)
                AddPropertyRow("Stats Collection Id", $"{node.StatsCollectionId}");
        }

        // === Scalar UDFs ===
        if (node.ScalarUdfs.Count > 0)
        {
            AddPropertySection("Scalar UDFs");
            foreach (var udf in node.ScalarUdfs)
            {
                var udfDetail = udf.FunctionName;
                if (udf.IsClrFunction)
                {
                    udfDetail += " (CLR)";
                    if (!string.IsNullOrEmpty(udf.ClrAssembly))
                        udfDetail += $"\n  Assembly: {udf.ClrAssembly}";
                    if (!string.IsNullOrEmpty(udf.ClrClass))
                        udfDetail += $"\n  Class: {udf.ClrClass}";
                    if (!string.IsNullOrEmpty(udf.ClrMethod))
                        udfDetail += $"\n  Method: {udf.ClrMethod}";
                }
                AddPropertyRow("UDF", udfDetail, isCode: true);
            }
        }

        // === Named Parameters (IndexScan) ===
        if (node.NamedParameters.Count > 0)
        {
            AddPropertySection("Named Parameters");
            foreach (var np in node.NamedParameters)
                AddPropertyRow(np.Name, np.ScalarString ?? "", isCode: true);
        }

        // === Per-Operator Indexed Views ===
        if (node.OperatorIndexedViews.Count > 0)
        {
            AddPropertySection("Operator Indexed Views");
            foreach (var iv in node.OperatorIndexedViews)
                AddPropertyRow("View", iv, isCode: true);
        }

        // === Suggested Index (Eager Spool) ===
        if (!string.IsNullOrEmpty(node.SuggestedIndex))
        {
            AddPropertySection("Suggested Index");
            AddPropertyRow("CREATE INDEX", node.SuggestedIndex, isCode: true);
        }

        // === Remote Operator ===
        if (!string.IsNullOrEmpty(node.RemoteDestination) || !string.IsNullOrEmpty(node.RemoteSource)
            || !string.IsNullOrEmpty(node.RemoteObject) || !string.IsNullOrEmpty(node.RemoteQuery))
        {
            AddPropertySection("Remote Operator");
            if (!string.IsNullOrEmpty(node.RemoteDestination))
                AddPropertyRow("Destination", node.RemoteDestination);
            if (!string.IsNullOrEmpty(node.RemoteSource))
                AddPropertyRow("Source", node.RemoteSource);
            if (!string.IsNullOrEmpty(node.RemoteObject))
                AddPropertyRow("Object", node.RemoteObject, isCode: true);
            if (!string.IsNullOrEmpty(node.RemoteQuery))
                AddPropertyRow("Query", node.RemoteQuery, isCode: true);
        }

        // === Foreign Key References Section ===
        if (node.ForeignKeyReferencesCount > 0 || node.NoMatchingIndexCount > 0 || node.PartialMatchingIndexCount > 0)
        {
            AddPropertySection("Foreign Key References");
            if (node.ForeignKeyReferencesCount > 0)
                AddPropertyRow("FK References", $"{node.ForeignKeyReferencesCount}");
            if (node.NoMatchingIndexCount > 0)
                AddPropertyRow("No Matching Index", $"{node.NoMatchingIndexCount}");
            if (node.PartialMatchingIndexCount > 0)
                AddPropertyRow("Partial Matching Index", $"{node.PartialMatchingIndexCount}");
        }

        // === Adaptive Join Section ===
        if (node.IsAdaptive)
        {
            AddPropertySection("Adaptive Join");
            if (!string.IsNullOrEmpty(node.EstimatedJoinType))
                AddPropertyRow("Est. Join Type", node.EstimatedJoinType);
            if (!string.IsNullOrEmpty(node.ActualJoinType))
                AddPropertyRow("Actual Join Type", node.ActualJoinType);
            if (node.AdaptiveThresholdRows > 0)
                AddPropertyRow("Threshold Rows", $"{node.AdaptiveThresholdRows:N1}");
        }

        // === Estimated Costs Section ===
        AddPropertySection("Estimated Costs");
        AddPropertyRow("Operator Cost", $"{MetricFormatter.FormatCost(node.EstimatedOperatorCost)} ({node.CostPercent}%)");
        AddPropertyRow("Subtree Cost", MetricFormatter.FormatCost(node.EstimatedTotalSubtreeCost));
        AddPropertyRow("I/O Cost", MetricFormatter.FormatCost(node.EstimateIO));
        AddPropertyRow("CPU Cost", MetricFormatter.FormatCost(node.EstimateCPU));

        // === Estimated Rows Section ===
        AddPropertySection("Estimated Rows");
        var estExecs = 1 + node.EstimateRebinds;
        AddPropertyRow("Est. Executions", $"{estExecs:N0}");
        AddPropertyRow("Est. Rows Per Exec", $"{node.EstimateRows:N1}");
        AddPropertyRow("Est. Rows All Execs", $"{node.EstimateRows * Math.Max(1, estExecs):N1}");
        if (node.EstimatedRowsRead > 0)
            AddPropertyRow("Est. Rows to Read", $"{node.EstimatedRowsRead:N1}");
        if (node.EstimateRowsWithoutRowGoal > 0)
            AddPropertyRow("Est. Rows (No Row Goal)", $"{node.EstimateRowsWithoutRowGoal:N1}");
        if (node.TableCardinality > 0)
            AddPropertyRow("Table Cardinality", $"{node.TableCardinality:N0}");
        AddPropertyRow("Avg Row Size", $"{node.EstimatedRowSize} B");
        AddPropertyRow("Est. Rebinds", $"{node.EstimateRebinds:N1}");
        AddPropertyRow("Est. Rewinds", $"{node.EstimateRewinds:N1}");

        // === Actual Stats Section (if actual plan) ===
        if (node.HasActualStats)
        {
            AddPropertySection("Actual Statistics");
            AddPropertyRow("Actual Rows", $"{node.ActualRows:N0}");
            if (node.ActualRowsRead > 0)
                AddPropertyRow("Actual Rows Read", $"{node.ActualRowsRead:N0}");
            AddPropertyRow("Actual Executions", $"{node.ActualExecutions:N0}");
            if (node.ActualRebinds > 0)
                AddPropertyRow("Actual Rebinds", $"{node.ActualRebinds:N0}");
            if (node.ActualRewinds > 0)
                AddPropertyRow("Actual Rewinds", $"{node.ActualRewinds:N0}");

            // Runtime partition summary
            if (node.PartitionsAccessed > 0)
            {
                AddPropertyRow("Partitions Accessed", $"{node.PartitionsAccessed}");
                if (!string.IsNullOrEmpty(node.PartitionRanges))
                    AddPropertyRow("Partition Ranges", node.PartitionRanges);
            }

            // Rows and executions list every thread, idle ones included: a thread sitting at
            // zero while its siblings work is the whole point of looking at the breakdown.
            AddPerThreadBreakdown(node,
                ("Rows", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualRows), true, ""),
                ("Rows Read", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualRowsRead), false, ""),
                ("Executions", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualExecutions), true, ""));

            // Timing
            if (node.ActualElapsedMs > 0 || node.ActualCPUMs > 0
                || node.UdfCpuTimeMs > 0 || node.UdfElapsedTimeMs > 0)
            {
                AddPropertySection("Actual Timing");
                if (node.ActualElapsedMs > 0)
                    AddPropertyRow("Elapsed Time", $"{node.ActualElapsedMs:N0} ms");
                if (node.ActualCPUMs > 0)
                    AddPropertyRow("CPU Time", $"{node.ActualCPUMs:N0} ms");
                if (node.UdfElapsedTimeMs > 0)
                    AddPropertyRow("UDF Elapsed", $"{node.UdfElapsedTimeMs:N0} ms");
                if (node.UdfCpuTimeMs > 0)
                    AddPropertyRow("UDF CPU", $"{node.UdfCpuTimeMs:N0} ms");

                AddPerThreadBreakdown(node,
                    ("Elapsed", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualElapsedMs), false, " ms"),
                    ("CPU", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualCPUMs), false, " ms"));
            }

            // I/O
            var hasIo = node.ActualLogicalReads > 0 || node.ActualPhysicalReads > 0
                || node.ActualScans > 0 || node.ActualReadAheads > 0
                || node.ActualSegmentReads > 0 || node.ActualSegmentSkips > 0;
            if (hasIo)
            {
                AddPropertySection("Actual I/O");
                AddPropertyRow("Logical Reads", $"{node.ActualLogicalReads:N0}");
                if (node.ActualPhysicalReads > 0)
                    AddPropertyRow("Physical Reads", $"{node.ActualPhysicalReads:N0}");
                if (node.ActualScans > 0)
                    AddPropertyRow("Scans", $"{node.ActualScans:N0}");
                if (node.ActualReadAheads > 0)
                    AddPropertyRow("Read-Ahead Reads", $"{node.ActualReadAheads:N0}");
                if (node.ActualSegmentReads > 0)
                    AddPropertyRow("Segment Reads", $"{node.ActualSegmentReads:N0}");
                if (node.ActualSegmentSkips > 0)
                    AddPropertyRow("Segment Skips", $"{node.ActualSegmentSkips:N0}");

                AddPerThreadBreakdown(node,
                    ("Logical Reads", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualLogicalReads), false, ""),
                    ("Physical Reads", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualPhysicalReads), false, ""),
                    ("Scans", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualScans), false, ""),
                    ("Read-Ahead Reads", (Func<PerThreadRuntimeInfo, long>)(t => t.ActualReadAheads), false, ""));
            }

            // LOB I/O
            var hasLobIo = node.ActualLobLogicalReads > 0 || node.ActualLobPhysicalReads > 0
                || node.ActualLobReadAheads > 0;
            if (hasLobIo)
            {
                AddPropertySection("Actual LOB I/O");
                if (node.ActualLobLogicalReads > 0)
                    AddPropertyRow("LOB Logical Reads", $"{node.ActualLobLogicalReads:N0}");
                if (node.ActualLobPhysicalReads > 0)
                    AddPropertyRow("LOB Physical Reads", $"{node.ActualLobPhysicalReads:N0}");
                if (node.ActualLobReadAheads > 0)
                    AddPropertyRow("LOB Read-Aheads", $"{node.ActualLobReadAheads:N0}");
            }
        }

        // === Predicates Section ===
        var hasPredicates = !string.IsNullOrEmpty(node.SeekPredicates) || !string.IsNullOrEmpty(node.Predicate)
            || !string.IsNullOrEmpty(node.HashKeysProbe) || !string.IsNullOrEmpty(node.HashKeysBuild)
            || !string.IsNullOrEmpty(node.BuildResidual) || !string.IsNullOrEmpty(node.ProbeResidual)
            || !string.IsNullOrEmpty(node.MergeResidual) || !string.IsNullOrEmpty(node.PassThru)
            || !string.IsNullOrEmpty(node.SetPredicate)
            || node.GuessedSelectivity;
        if (hasPredicates)
        {
            AddPropertySection("Predicates");
            if (!string.IsNullOrEmpty(node.SeekPredicates))
                AddPropertyRow("Seek Predicate", node.SeekPredicates, isCode: true);
            if (!string.IsNullOrEmpty(node.Predicate))
                AddPropertyRow("Predicate", node.Predicate, isCode: true);
            if (!string.IsNullOrEmpty(node.HashKeysBuild))
                AddPropertyRow("Hash Keys (Build)", node.HashKeysBuild, isCode: true);
            if (!string.IsNullOrEmpty(node.HashKeysProbe))
                AddPropertyRow("Hash Keys (Probe)", node.HashKeysProbe, isCode: true);
            if (!string.IsNullOrEmpty(node.BuildResidual))
                AddPropertyRow("Build Residual", node.BuildResidual, isCode: true);
            if (!string.IsNullOrEmpty(node.ProbeResidual))
                AddPropertyRow("Probe Residual", node.ProbeResidual, isCode: true);
            if (!string.IsNullOrEmpty(node.MergeResidual))
                AddPropertyRow("Merge Residual", node.MergeResidual, isCode: true);
            if (!string.IsNullOrEmpty(node.PassThru))
                AddPropertyRow("Pass Through", node.PassThru, isCode: true);
            if (!string.IsNullOrEmpty(node.SetPredicate))
                AddPropertyRow("Set Predicate", node.SetPredicate, isCode: true);
            if (node.GuessedSelectivity)
                AddPropertyRow("Guessed Selectivity", "True (optimizer guessed, no statistics)");
        }

        // === Output Columns ===
        if (!string.IsNullOrEmpty(node.OutputColumns))
        {
            AddPropertySection("Output");
            AddPropertyRow("Columns", node.OutputColumns, isCode: true);
        }

        // === Memory ===
        if (node.MemoryGrantKB > 0 || node.DesiredMemoryKB > 0 || node.MaxUsedMemoryKB > 0
            || node.MemoryFractionInput > 0 || node.MemoryFractionOutput > 0
            || node.InputMemoryGrantKB > 0 || node.OutputMemoryGrantKB > 0 || node.UsedMemoryGrantKB > 0)
        {
            AddPropertySection("Memory");
            if (node.MemoryGrantKB > 0) AddPropertyRow("Granted", $"{node.MemoryGrantKB:N0} KB");
            if (node.DesiredMemoryKB > 0) AddPropertyRow("Desired", $"{node.DesiredMemoryKB:N0} KB");
            if (node.MaxUsedMemoryKB > 0) AddPropertyRow("Max Used", $"{node.MaxUsedMemoryKB:N0} KB");
            if (node.InputMemoryGrantKB > 0) AddPropertyRow("Input Grant", $"{node.InputMemoryGrantKB:N0} KB");
            if (node.OutputMemoryGrantKB > 0) AddPropertyRow("Output Grant", $"{node.OutputMemoryGrantKB:N0} KB");
            if (node.UsedMemoryGrantKB > 0) AddPropertyRow("Used Grant", $"{node.UsedMemoryGrantKB:N0} KB");
            if (node.MemoryFractionInput > 0) AddPropertyRow("Fraction Input", $"{node.MemoryFractionInput:F4}");
            if (node.MemoryFractionOutput > 0) AddPropertyRow("Fraction Output", $"{node.MemoryFractionOutput:F4}");
        }

        // === Root node only: statement-level sections ===
        if (node.Parent == null && _currentStatement != null)
        {
            var s = _currentStatement;

            // === Statement Text ===
            if (!string.IsNullOrEmpty(s.StatementText) || !string.IsNullOrEmpty(s.StmtUseDatabaseName))
            {
                AddPropertySection("Statement");
                if (!string.IsNullOrEmpty(s.StatementText))
                    AddPropertyRow("Text", s.StatementText, isCode: true);
                if (!string.IsNullOrEmpty(s.ParameterizedText) && s.ParameterizedText != s.StatementText)
                    AddPropertyRow("Parameterized", s.ParameterizedText, isCode: true);
                if (!string.IsNullOrEmpty(s.StmtUseDatabaseName))
                    AddPropertyRow("USE Database", s.StmtUseDatabaseName);
            }

            // === Cursor Info ===
            if (!string.IsNullOrEmpty(s.CursorName))
            {
                AddPropertySection("Cursor Info");
                AddPropertyRow("Cursor Name", s.CursorName);
                if (!string.IsNullOrEmpty(s.CursorActualType))
                    AddPropertyRow("Actual Type", s.CursorActualType);
                if (!string.IsNullOrEmpty(s.CursorRequestedType))
                    AddPropertyRow("Requested Type", s.CursorRequestedType);
                if (!string.IsNullOrEmpty(s.CursorConcurrency))
                    AddPropertyRow("Concurrency", s.CursorConcurrency);
                AddPropertyRow("Forward Only", s.CursorForwardOnly ? "True" : "False");
            }

            // === Statement Memory Grant ===
            if (s.MemoryGrant != null)
            {
                var mg = s.MemoryGrant;
                AddPropertySection("Memory Grant Info");
                AddPropertyRow("Granted", $"{mg.GrantedMemoryKB:N0} KB");
                AddPropertyRow("Max Used", $"{mg.MaxUsedMemoryKB:N0} KB");
                AddPropertyRow("Requested", $"{mg.RequestedMemoryKB:N0} KB");
                AddPropertyRow("Desired", $"{mg.DesiredMemoryKB:N0} KB");
                AddPropertyRow("Required", $"{mg.RequiredMemoryKB:N0} KB");
                AddPropertyRow("Serial Required", $"{mg.SerialRequiredMemoryKB:N0} KB");
                AddPropertyRow("Serial Desired", $"{mg.SerialDesiredMemoryKB:N0} KB");
                if (mg.GrantWaitTimeMs > 0)
                    AddPropertyRow("Grant Wait Time", $"{mg.GrantWaitTimeMs:N0} ms");
                if (mg.LastRequestedMemoryKB > 0)
                    AddPropertyRow("Last Requested", $"{mg.LastRequestedMemoryKB:N0} KB");
                if (!string.IsNullOrEmpty(mg.IsMemoryGrantFeedbackAdjusted))
                    AddPropertyRow("Feedback Adjusted", mg.IsMemoryGrantFeedbackAdjusted);
            }

            // === Statement Info ===
            AddPropertySection("Statement Info");
            if (!string.IsNullOrEmpty(s.StatementOptmLevel))
                AddPropertyRow("Optimization Level", s.StatementOptmLevel);
            if (!string.IsNullOrEmpty(s.StatementOptmEarlyAbortReason))
                AddPropertyRow("Early Abort Reason", s.StatementOptmEarlyAbortReason);
            if (s.CardinalityEstimationModelVersion > 0)
                AddPropertyRow("CE Model Version", $"{s.CardinalityEstimationModelVersion}");
            if (s.DegreeOfParallelism > 0)
                AddPropertyRow("DOP", $"{s.DegreeOfParallelism}");
            if (s.EffectiveDOP > 0)
                AddPropertyRow("Effective DOP", $"{s.EffectiveDOP}");
            if (!string.IsNullOrEmpty(s.DOPFeedbackAdjusted))
                AddPropertyRow("DOP Feedback", s.DOPFeedbackAdjusted);
            if (!string.IsNullOrEmpty(s.NonParallelPlanReason))
                AddPropertyRow("Non-Parallel Reason", s.NonParallelPlanReason);
            if (s.MaxQueryMemoryKB > 0)
                AddPropertyRow("Max Query Memory", $"{s.MaxQueryMemoryKB:N0} KB");
            if (s.QueryPlanMemoryGrantKB > 0)
                AddPropertyRow("QueryPlan Memory Grant", $"{s.QueryPlanMemoryGrantKB:N0} KB");
            AddPropertyRow("Compile Time", $"{s.CompileTimeMs:N0} ms");
            AddPropertyRow("Compile CPU", $"{s.CompileCPUMs:N0} ms");
            AddPropertyRow("Compile Memory", $"{s.CompileMemoryKB:N0} KB");
            if (s.CachedPlanSizeKB > 0)
                AddPropertyRow("Cached Plan Size", $"{s.CachedPlanSizeKB:N0} KB");
            AddPropertyRow("Retrieved From Cache", s.RetrievedFromCache ? "True" : "False");
            AddPropertyRow("Batch Mode On RowStore", s.BatchModeOnRowStoreUsed ? "True" : "False");
            AddPropertyRow("Security Policy", s.SecurityPolicyApplied ? "True" : "False");
            AddPropertyRow("Parameterization Type", $"{s.StatementParameterizationType}");
            if (!string.IsNullOrEmpty(s.QueryHash))
                AddPropertyRow("Query Hash", s.QueryHash, isCode: true);
            if (!string.IsNullOrEmpty(s.QueryPlanHash))
                AddPropertyRow("Plan Hash", s.QueryPlanHash, isCode: true);
            if (!string.IsNullOrEmpty(s.StatementSqlHandle))
                AddPropertyRow("SQL Handle", s.StatementSqlHandle, isCode: true);
            AddPropertyRow("DB Settings Id", $"{s.DatabaseContextSettingsId}");
            AddPropertyRow("Parent Object Id", $"{s.ParentObjectId}");

            // Plan Guide
            if (!string.IsNullOrEmpty(s.PlanGuideName))
            {
                AddPropertyRow("Plan Guide", s.PlanGuideName);
                if (!string.IsNullOrEmpty(s.PlanGuideDB))
                    AddPropertyRow("Plan Guide DB", s.PlanGuideDB);
            }
            if (s.UsePlan)
                AddPropertyRow("USE PLAN", "True");

            // Query Store Hints
            if (s.QueryStoreStatementHintId > 0)
            {
                AddPropertyRow("QS Hint Id", $"{s.QueryStoreStatementHintId}");
                if (!string.IsNullOrEmpty(s.QueryStoreStatementHintText))
                    AddPropertyRow("QS Hint", s.QueryStoreStatementHintText, isCode: true);
                if (!string.IsNullOrEmpty(s.QueryStoreStatementHintSource))
                    AddPropertyRow("QS Hint Source", s.QueryStoreStatementHintSource);
            }

            // === Feature Flags ===
            if (s.ContainsInterleavedExecutionCandidates || s.ContainsInlineScalarTsqlUdfs
                || s.ContainsLedgerTables || s.ExclusiveProfileTimeActive || s.QueryCompilationReplay > 0
                || s.QueryVariantID > 0)
            {
                AddPropertySection("Feature Flags");
                if (s.ContainsInterleavedExecutionCandidates)
                    AddPropertyRow("Interleaved Execution", "True");
                if (s.ContainsInlineScalarTsqlUdfs)
                    AddPropertyRow("Inline Scalar UDFs", "True");
                if (s.ContainsLedgerTables)
                    AddPropertyRow("Ledger Tables", "True");
                if (s.ExclusiveProfileTimeActive)
                    AddPropertyRow("Exclusive Profile Time", "True");
                if (s.QueryCompilationReplay > 0)
                    AddPropertyRow("Compilation Replay", $"{s.QueryCompilationReplay}");
                if (s.QueryVariantID > 0)
                    AddPropertyRow("Query Variant ID", $"{s.QueryVariantID}");
            }

            // === PSP Dispatcher ===
            if (s.Dispatcher != null)
            {
                AddPropertySection("PSP Dispatcher");
                if (!string.IsNullOrEmpty(s.DispatcherPlanHandle))
                    AddPropertyRow("Plan Handle", s.DispatcherPlanHandle, isCode: true);
                foreach (var psp in s.Dispatcher.ParameterSensitivePredicates)
                {
                    var range = $"[{psp.LowBoundary:N0} — {psp.HighBoundary:N0}]";
                    var predText = psp.PredicateText ?? "";
                    AddPropertyRow("Predicate", $"{predText} {range}", isCode: true);
                    foreach (var stat in psp.Statistics)
                    {
                        var statLabel = !string.IsNullOrEmpty(stat.TableName)
                            ? $"  {stat.TableName}.{stat.StatisticsName}"
                            : $"  {stat.StatisticsName}";
                        AddPropertyRow(statLabel, $"Modified: {stat.ModificationCount:N0}, Sampled: {stat.SamplingPercent:F1}%", indent: true);
                    }
                }
                foreach (var opt in s.Dispatcher.OptionalParameterPredicates)
                {
                    if (!string.IsNullOrEmpty(opt.PredicateText))
                        AddPropertyRow("Optional Predicate", opt.PredicateText, isCode: true);
                }
            }

            // === Cardinality Feedback ===
            if (s.CardinalityFeedback.Count > 0)
            {
                AddPropertySection("Cardinality Feedback");
                foreach (var cf in s.CardinalityFeedback)
                    AddPropertyRow($"Node {cf.Key}", $"{cf.Value:N0}");
            }

            // === Optimization Replay ===
            if (!string.IsNullOrEmpty(s.OptimizationReplayScript))
            {
                AddPropertySection("Optimization Replay");
                AddPropertyRow("Script", s.OptimizationReplayScript, isCode: true);
            }

            // === Template Plan Guide ===
            if (!string.IsNullOrEmpty(s.TemplatePlanGuideName))
            {
                AddPropertyRow("Template Plan Guide", s.TemplatePlanGuideName);
                if (!string.IsNullOrEmpty(s.TemplatePlanGuideDB))
                    AddPropertyRow("Template Guide DB", s.TemplatePlanGuideDB);
            }

            // === Handles ===
            if (!string.IsNullOrEmpty(s.ParameterizedPlanHandle) || !string.IsNullOrEmpty(s.BatchSqlHandle))
            {
                AddPropertySection("Handles");
                if (!string.IsNullOrEmpty(s.ParameterizedPlanHandle))
                    AddPropertyRow("Parameterized Plan", s.ParameterizedPlanHandle, isCode: true);
                if (!string.IsNullOrEmpty(s.BatchSqlHandle))
                    AddPropertyRow("Batch SQL Handle", s.BatchSqlHandle, isCode: true);
            }

            // === Set Options ===
            if (s.SetOptions != null)
            {
                var so = s.SetOptions;
                AddPropertySection("Set Options");
                AddPropertyRow("ANSI_NULLS", so.AnsiNulls ? "True" : "False");
                AddPropertyRow("ANSI_PADDING", so.AnsiPadding ? "True" : "False");
                AddPropertyRow("ANSI_WARNINGS", so.AnsiWarnings ? "True" : "False");
                AddPropertyRow("ARITHABORT", so.ArithAbort ? "True" : "False");
                AddPropertyRow("CONCAT_NULL", so.ConcatNullYieldsNull ? "True" : "False");
                AddPropertyRow("NUMERIC_ROUNDABORT", so.NumericRoundAbort ? "True" : "False");
                AddPropertyRow("QUOTED_IDENTIFIER", so.QuotedIdentifier ? "True" : "False");
            }

            // === Optimizer Hardware Properties ===
            if (s.HardwareProperties != null)
            {
                var hw = s.HardwareProperties;
                AddPropertySection("Hardware Properties");
                AddPropertyRow("Available Memory", $"{hw.EstimatedAvailableMemoryGrant:N0} KB");
                AddPropertyRow("Pages Cached", $"{hw.EstimatedPagesCached:N0}");
                AddPropertyRow("Available DOP", $"{hw.EstimatedAvailableDOP}");
                if (hw.MaxCompileMemory > 0)
                    AddPropertyRow("Max Compile Memory", $"{hw.MaxCompileMemory:N0} KB");
            }

            // === Plan Version ===
            if (_currentPlan != null && (!string.IsNullOrEmpty(_currentPlan.BuildVersion) || !string.IsNullOrEmpty(_currentPlan.Build)))
            {
                AddPropertySection("Plan Version");
                if (!string.IsNullOrEmpty(_currentPlan.BuildVersion))
                    AddPropertyRow("Build Version", _currentPlan.BuildVersion);
                if (!string.IsNullOrEmpty(_currentPlan.Build))
                    AddPropertyRow("Build", _currentPlan.Build);
                if (_currentPlan.ClusteredMode)
                    AddPropertyRow("Clustered Mode", "True");
            }

            // === Optimizer Stats Usage ===
            if (s.StatsUsage.Count > 0)
            {
                AddPropertySection("Statistics Used");
                foreach (var stat in s.StatsUsage)
                {
                    var statLabel = !string.IsNullOrEmpty(stat.TableName)
                        ? $"{stat.TableName}.{stat.StatisticsName}"
                        : stat.StatisticsName;
                    var statDetail = $"Modified: {stat.ModificationCount:N0}, Sampled: {stat.SamplingPercent:F1}%";
                    if (!string.IsNullOrEmpty(stat.LastUpdate))
                        statDetail += $", Updated: {stat.LastUpdate}";
                    AddPropertyRow(statLabel, statDetail);
                }
            }

            // === Parameters ===
            if (s.Parameters.Count > 0)
            {
                AddPropertySection("Parameters");
                foreach (var p in s.Parameters)
                {
                    var paramText = p.DataType;
                    if (!string.IsNullOrEmpty(p.CompiledValue))
                        paramText += $", Compiled: {p.CompiledValue}";
                    if (!string.IsNullOrEmpty(p.RuntimeValue))
                        paramText += $", Runtime: {p.RuntimeValue}";
                    AddPropertyRow(p.Name, paramText);
                }
            }

            // === Query Time Stats (actual plans) ===
            if (s.QueryTimeStats != null)
            {
                AddPropertySection("Query Time Stats");
                AddPropertyRow("CPU Time", $"{s.QueryTimeStats.CpuTimeMs:N0} ms");
                AddPropertyRow("Elapsed Time", $"{s.QueryTimeStats.ElapsedTimeMs:N0} ms");
                if (s.QueryUdfCpuTimeMs > 0)
                    AddPropertyRow("UDF CPU Time", $"{s.QueryUdfCpuTimeMs:N0} ms");
                if (s.QueryUdfElapsedTimeMs > 0)
                    AddPropertyRow("UDF Elapsed Time", $"{s.QueryUdfElapsedTimeMs:N0} ms");
            }

            // === Thread Stats (actual plans) ===
            if (s.ThreadStats != null)
            {
                AddPropertySection("Thread Stats");
                AddPropertyRow("Branches", $"{s.ThreadStats.Branches}");
                AddPropertyRow("Used Threads", $"{s.ThreadStats.UsedThreads}");
                var totalReserved = s.ThreadStats.Reservations.Sum(r => r.ReservedThreads);
                if (totalReserved > 0)
                {
                    AddPropertyRow("Reserved Threads", $"{totalReserved}");
                    if (totalReserved > s.ThreadStats.UsedThreads)
                        AddPropertyRow("Inactive Threads", $"{totalReserved - s.ThreadStats.UsedThreads}");
                }
                foreach (var res in s.ThreadStats.Reservations)
                    AddPropertyRow($"  Node {res.NodeId}", $"{res.ReservedThreads} reserved");
            }

            // === Wait Stats (actual plans) ===
            if (s.WaitStats.Count > 0)
            {
                AddPropertySection("Wait Stats");
                foreach (var w in s.WaitStats.OrderByDescending(w => w.WaitTimeMs))
                    AddPropertyRow(w.WaitType, $"{w.WaitTimeMs:N0} ms ({w.WaitCount:N0} waits)");
            }

            // === Trace Flags ===
            if (s.TraceFlags.Count > 0)
            {
                AddPropertySection("Trace Flags");
                foreach (var tf in s.TraceFlags)
                {
                    var tfLabel = $"TF {tf.Value}";
                    var tfDetail = $"{tf.Scope}{(tf.IsCompileTime ? ", Compile-time" : ", Runtime")}";
                    AddPropertyRow(tfLabel, tfDetail);
                }
            }

            // === Indexed Views ===
            if (s.IndexedViews.Count > 0)
            {
                AddPropertySection("Indexed Views");
                foreach (var iv in s.IndexedViews)
                    AddPropertyRow("View", iv, isCode: true);
            }

            // === Plan-Level Warnings ===
            if (s.PlanWarnings.Count > 0)
            {
                AddPropertySection("Plan Warnings");
                foreach (var w in PlanWarningDisplay.OrderByBenefit(s.PlanWarnings))
                {
                    var warnColor = PlanWarningDisplay.WarningSeverityColorHex(w.Severity);
                    var warnPanel = new StackPanel { Margin = new Thickness(10, 2, 10, 2) };
                    var planWarnHeaderText = PlanWarningDisplay.PlanWarningHeader(w);
                    var planWarnHeaderBlock = new TextBlock
                    {
                        Text = planWarnHeaderText,
                        FontWeight = FontWeights.SemiBold,
                        FontSize = 11,
                        Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor))
                    };
                    AttachOriginNavigation(planWarnHeaderBlock, planWarnHeaderText, w.OriginNodeIds);
                    warnPanel.Children.Add(planWarnHeaderBlock);
                    warnPanel.Children.Add(new TextBlock
                    {
                        Text = w.Message,
                        FontSize = 11,
                        Foreground = TooltipFgBrush,
                        TextWrapping = TextWrapping.Wrap,
                        Margin = new Thickness(16, 0, 0, 0)
                    });
                    if (!string.IsNullOrEmpty(w.ActionableFix))
                    {
                        warnPanel.Children.Add(new TextBlock
                        {
                            Text = w.ActionableFix,
                            FontSize = 11,
                            FontStyle = FontStyles.Italic,
                            Foreground = TooltipFgBrush,
                            TextWrapping = TextWrapping.Wrap,
                            Margin = new Thickness(16, 2, 0, 0)
                        });
                    }
                    (_currentPropertySection ?? PropertiesContent).Children.Add(warnPanel);
                    RegisterBuiltWarningRow(planWarnHeaderText, w.Message, warnPanel);
                }
            }

            // === Missing Indexes ===
            if (s.MissingIndexes.Count > 0)
            {
                AddPropertySection("Missing Indexes");
                foreach (var mi in s.MissingIndexes)
                {
                    AddPropertyRow($"{mi.Schema}.{mi.Table}", $"Impact: {mi.Impact:F1}%");
                    if (!string.IsNullOrEmpty(mi.CreateStatement))
                        AddPropertyRow("CREATE INDEX", mi.CreateStatement, isCode: true);
                }
            }
        }

        // === Warnings ===
        if (node.HasWarnings)
        {
            AddPropertySection("Warnings");
            foreach (var w in PlanWarningDisplay.OrderByBenefit(node.Warnings))
            {
                var warnColor = PlanWarningDisplay.WarningSeverityColorHex(w.Severity);
                var warnPanel = new StackPanel { Margin = new Thickness(10, 2, 10, 2) };
                var opWarnHeaderText = PlanWarningDisplay.PlanWarningHeader(w);
                var opWarnHeaderBlock = new TextBlock
                {
                    Text = opWarnHeaderText,
                    FontWeight = FontWeights.SemiBold,
                    FontSize = 11,
                    Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor))
                };
                AttachOriginNavigation(opWarnHeaderBlock, opWarnHeaderText, w.OriginNodeIds);
                warnPanel.Children.Add(opWarnHeaderBlock);
                warnPanel.Children.Add(new TextBlock
                {
                    Text = w.Message,
                    FontSize = 11,
                    Foreground = TooltipFgBrush,
                    TextWrapping = TextWrapping.Wrap,
                    Margin = new Thickness(16, 0, 0, 0)
                });
                if (!string.IsNullOrEmpty(w.ActionableFix))
                {
                    warnPanel.Children.Add(new TextBlock
                    {
                        Text = w.ActionableFix,
                        FontSize = 11,
                        FontStyle = FontStyles.Italic,
                        Foreground = TooltipFgBrush,
                        TextWrapping = TextWrapping.Wrap,
                        Margin = new Thickness(16, 2, 0, 0)
                    });
                }
                PropertiesContent.Children.Add(warnPanel);
                RegisterBuiltWarningRow(opWarnHeaderText, w.Message, warnPanel);
            }
        }

        // The filter box keeps its text across selections, so a rebuilt panel has to re-apply it.
        ApplyPropertiesFilter();

        // Show the panel. The width is set only when the panel is opening: setting it on every
        // selection would throw away whatever width the user had dragged out, on every click.
        if (PropertiesPanel.Visibility != Visibility.Visible)
        {
            PropertiesColumn.MinWidth = MinPropertiesWidth;
            PropertiesColumn.MaxWidth = MaxPropertiesWidth;
            PropertiesColumn.Width = new GridLength(
                PerformanceMonitor.PlanAnalysis.PropertyRows.ClampWidth(_propertiesPanelWidth));
            PropertiesSplitter.Visibility = Visibility.Visible;
            PropertiesPanel.Visibility = Visibility.Visible;
        }
    }

    private void PropertiesFilter_TextChanged(object sender, TextChangedEventArgs e)
        => ApplyPropertiesFilter();

    private void PropertiesFilter_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
    {
        if (e.Key != System.Windows.Input.Key.Escape) return;
        PropertiesFilterBox.Text = "";
        e.Handled = true;
    }

    /// <summary>
    /// Hides every row whose label and value miss the filter text, and hides a section outright
    /// once nothing in it is left showing. A section whose own title matches the filter keeps
    /// all of its rows, so typing a section name jumps to that section rather than emptying it.
    /// </summary>
    private void ApplyPropertiesFilter()
    {
        var filter = PropertiesFilterBox.Text?.Trim() ?? "";

        foreach (var section in _propertySections)
        {
            var sectionMatches = PerformanceMonitor.PlanAnalysis.PropertyRows.SectionTitleMatches(section.Title, filter);

            var visibleCount = 0;
            foreach (var row in section.Rows)
            {
                var visible = PerformanceMonitor.PlanAnalysis.PropertyRows.RowMatches(row.Model, filter, sectionMatches);
                foreach (var control in row.Controls)
                    control.Visibility = visible ? Visibility.Visible : Visibility.Collapsed;
                if (visible) visibleCount++;
            }

            section.Expander.Visibility = PerformanceMonitor.PlanAnalysis.PropertyRows.SectionVisible(visibleCount)
                ? Visibility.Visible : Visibility.Collapsed;
        }
    }

    /// <summary>
    /// Moves a section's per-thread numbers out of the flat row list and into one collapsed
    /// sub-expander, grouped under a small header per metric. Ported from PerformanceStudio's
    /// Avalonia viewer (dev @ ff7f1d9). The shaping (which metrics to show, in what order, and
    /// whether the header carries a skew suffix) lives in <see cref="ThreadBreakdown"/> so this
    /// method only renders what that helper returns.
    /// </summary>
    private void AddPerThreadBreakdown(
        PlanNode node,
        params (string Metric, Func<PerThreadRuntimeInfo, long> Value, bool IncludeIdleThreads, string Unit)[] metrics)
    {
        var breakdown = ThreadBreakdown.Build(node, metrics);
        if (breakdown == null)
            return;

        var panel = new StackPanel { Margin = new Thickness(10, 2, 6, 4) };
        var groupCount = 0;

        foreach (var group in breakdown.Groups)
        {
            groupCount++;
            panel.Children.Add(new TextBlock
            {
                Text = group.Metric,
                FontSize = 10,
                FontWeight = FontWeights.SemiBold,
                Foreground = SectionHeaderBrush,
                Margin = new Thickness(0, groupCount == 1 ? 0 : 5, 0, 1)
            });

            var threadGrid = new Grid { Margin = new Thickness(8, 0, 0, 0) };
            threadGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(72) });
            threadGrid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

            var rowIndex = 0;
            foreach (var t in group.Threads)
            {
                threadGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

                var threadLabelBlock = new TextBlock
                {
                    Text = $"Thread {t.ThreadId}",
                    FontSize = 10,
                    Foreground = MutedBrush
                };
                Grid.SetRow(threadLabelBlock, rowIndex);
                Grid.SetColumn(threadLabelBlock, 0);
                threadGrid.Children.Add(threadLabelBlock);

                var threadValueBlock = new TextBlock
                {
                    Text = $"{t.Value:N0}{group.Unit}",
                    FontSize = 10,
                    Foreground = TooltipFgBrush,
                    TextWrapping = TextWrapping.Wrap
                };
                Grid.SetRow(threadValueBlock, rowIndex);
                Grid.SetColumn(threadValueBlock, 1);
                threadGrid.Children.Add(threadValueBlock);

                rowIndex++;
            }

            panel.Children.Add(threadGrid);
        }

        var header = new StackPanel { Orientation = Orientation.Horizontal };
        header.Children.Add(new TextBlock
        {
            Text = breakdown.HeaderText,
            FontWeight = FontWeights.SemiBold,
            FontSize = 11,
            Foreground = SectionHeaderBrush,
            VerticalAlignment = VerticalAlignment.Center
        });
        if (breakdown.IsSkewed)
        {
            // #4629: was the fixed OrangeBrush (#FFB347, 1.78:1 on Light's white — under WCAG AA).
            // WarningBrush is the same theme token the app's other warning text already uses.
            header.Children.Add(new TextBlock
            {
                Text = breakdown.SkewSuffix,
                FontWeight = FontWeights.SemiBold,
                FontSize = 11,
                Foreground = WarningBrush,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(6, 0, 0, 0)
            });
        }

        var expander = new Expander
        {
            IsExpanded = false,
            Header = header,
            Content = panel,
            Margin = new Thickness(0, 2, 0, 2),
            Padding = new Thickness(0),
            Foreground = SectionHeaderBrush,
            BorderThickness = new Thickness(0),
            HorizontalAlignment = HorizontalAlignment.Stretch,
            HorizontalContentAlignment = HorizontalAlignment.Stretch
        };

        var target = _currentPropertySection ?? PropertiesContent;
        target.Children.Add(expander);
    }

    private void AddPropertySection(string title)
    {
        var contentPanel = new StackPanel();
        var expander = new Expander
        {
            IsExpanded = true,
            Header = new TextBlock
            {
                Text = title,
                FontWeight = FontWeights.SemiBold,
                FontSize = 11,
                Foreground = SectionHeaderBrush
            },
            Content = contentPanel,
            Margin = new Thickness(0, 2, 0, 0),
            Padding = new Thickness(0),
            Foreground = SectionHeaderBrush,
            Background = (TryFindResource("BackgroundLighterBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromArgb(0x18, 0x4F, 0xA3, 0xFF)),
            BorderBrush = PropSeparatorBrush,
            BorderThickness = new Thickness(0, 0, 0, 1)
        };
        PropertiesContent.Children.Add(expander);
        _currentPropertySection = contentPanel;
        _currentSection = new PropertyPanelSection { Title = title, Expander = expander, Model = new PropertySection { Title = title } };
        _propertySections.Add(_currentSection);
    }

    private void AddPropertyRow(string label, string value, bool isCode = false, bool indent = false)
    {
        var grid = new Grid { Margin = new Thickness(10, 3, 10, 3) };
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(140) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

        var labelBlock = new TextBlock
        {
            Text = label,
            FontSize = indent ? 10 : 11,
            Foreground = MutedBrush,
            VerticalAlignment = VerticalAlignment.Top,
            TextWrapping = TextWrapping.Wrap,
            Margin = indent ? new Thickness(16, 0, 0, 0) : new Thickness(0)
        };
        Grid.SetColumn(labelBlock, 0);
        grid.Children.Add(labelBlock);

        var valueBox = new TextBox
        {
            Text = value,
            FontSize = indent ? 10 : 11,
            Foreground = TooltipFgBrush,
            TextWrapping = TextWrapping.Wrap,
            IsReadOnly = true,
            BorderThickness = new Thickness(0),
            Background = Brushes.Transparent,
            Padding = new Thickness(0),
            VerticalAlignment = VerticalAlignment.Top
        };
        if (isCode) valueBox.FontFamily = new FontFamily("Consolas");
        Grid.SetColumn(valueBox, 1);
        grid.Children.Add(valueBox);

        var target = _currentPropertySection ?? PropertiesContent;
        target.Children.Add(grid);

        if (_currentSection != null)
        {
            var entry = new PropertyPanelRow
            {
                Model = new PropertyRow { Label = label, Value = value, IsCode = isCode, SearchText = $"{label} {value}" }
            };
            entry.Controls.Add(grid);
            AttachPropertyRowMenu(valueBox, entry);
            _currentSection.Rows.Add(entry);
            _currentSection.Model.Rows.Add(entry.Model);
        }
    }

    /// <summary>
    /// Wraps a warning panel built directly under <see cref="PropertiesContent"/> (the plan-level
    /// and per-operator warning lists, which are prose panels rather than label/value grids) as a
    /// filterable, copyable row on the section most recently opened by <see cref="AddPropertySection"/>.
    /// </summary>
    private void RegisterBuiltWarningRow(string header, string body, FrameworkElement panel)
    {
        if (_currentSection == null) return;

        var text = $"  {header}\n    {body}".TrimEnd();
        var entry = new PropertyPanelRow
        {
            Model = new PropertyRow { Label = header, Value = body, BlockText = text, SearchText = $"{header} {body}" }
        };
        entry.Controls.Add(panel);
        AttachPropertyRowMenu(panel, entry);
        _currentSection.Rows.Add(entry);
        _currentSection.Model.Rows.Add(entry.Model);
    }

    /// <summary>
    /// Gives a control the row's copy menu ('Copy value' / 'Copy name and value' / 'Copy all
    /// properties'), replacing the stock read-only TextBox menu (greyed-out Cut/Copy, live Paste
    /// on data nobody can edit).
    /// </summary>
    private void AttachPropertyRowMenu(FrameworkElement control, PropertyPanelRow entry)
    {
        control.ContextMenu = entry.Menu ??= BuildPropertyRowMenu(entry);
    }

    private ContextMenu BuildPropertyRowMenu(PropertyPanelRow entry)
    {
        var menu = new ContextMenu();

        var copyValueItem = new MenuItem { Header = "Copy value" };
        copyValueItem.Click += (_, _) => TrySetClipboardText(entry.Model.CopyValue);
        menu.Items.Add(copyValueItem);

        var copyRowItem = new MenuItem { Header = "Copy name and value" };
        copyRowItem.Click += (_, _) => TrySetClipboardText(entry.Model.CopyLabelAndValue);
        menu.Items.Add(copyRowItem);

        menu.Items.Add(new Separator());

        var copyAllItem = new MenuItem { Header = "Copy all properties" };
        copyAllItem.Click += (_, _) => TrySetClipboardText(BuildPropertiesText());
        menu.Items.Add(copyAllItem);

        return menu;
    }

    /// <summary>
    /// The whole panel as plain text, for "Copy all properties": rendered from the row model
    /// (<see cref="PerformanceMonitor.PlanAnalysis.PropertyRows.BuildPropertiesText"/>), not the
    /// visual tree, so a code value pastes back out verbatim.
    /// </summary>
    private string BuildPropertiesText() =>
        PerformanceMonitor.PlanAnalysis.PropertyRows.BuildPropertiesText(
            PropertiesHeader.Text, PropertiesSubHeader.Text, _propertySections.ConvertAll(s => s.Model));

    /// <summary>
    /// Guarded clipboard write for the copy menu, routed through <see cref="ClipboardText.TrySetText"/>
    /// (the shared bounded-retry guard against transient CLIPBRD_E_CANT_OPEN - see #4582/#4600) instead
    /// of a bare <see cref="Clipboard.SetText(string)"/>.
    /// </summary>
    private static void TrySetClipboardText(string text) => ClipboardText.TrySetText(text ?? "");

    private void CloseProperties_Click(object sender, RoutedEventArgs e)
    {
        ClosePropertiesPanel();
    }

    private void ClosePropertiesPanel()
    {
        PropertiesPanel.Visibility = Visibility.Collapsed;
        PropertiesSplitter.Visibility = Visibility.Collapsed;
        // Clear the open-state bounds first: MinWidth clamps the column whatever its Width says,
        // so leaving it set would hold a strip open on a closed panel.
        PropertiesColumn.MinWidth = 0;
        PropertiesColumn.MaxWidth = double.PositiveInfinity;
        PropertiesColumn.Width = new GridLength(0);

        // Deselect node
        if (_selectedNodeBorder != null)
        {
            _selectedNodeBorder.BorderBrush = _selectedNodeOriginalBorder;
            _selectedNodeBorder.BorderThickness = _selectedNodeOriginalThickness;
            _selectedNodeBorder = null;
        }
        _selectedNode = null;
    }

    #endregion

    #region Banners

    private void ShowMissingIndexes(List<MissingIndex> indexes)
    {
        MissingIndexContent.Children.Clear();

        if (indexes.Count > 0)
        {
            MissingIndexHeader.Text = $"  Missing Index Suggestions ({indexes.Count})";

            foreach (var mi in indexes)
            {
                var itemPanel = new StackPanel { Margin = new Thickness(0, 4, 0, 0) };

                var headerRow = new StackPanel { Orientation = Orientation.Horizontal };
                headerRow.Children.Add(new TextBlock
                {
                    Text = mi.Table,
                    FontWeight = FontWeights.SemiBold,
                    Foreground = TooltipFgBrush,
                    FontSize = 12
                });
                headerRow.Children.Add(new TextBlock
                {
                    Text = $" \u2014 Impact: ",
                    Foreground = MutedBrush,
                    FontSize = 12
                });
                // #4629: was the fixed OrangeBrush; see the skew-text comment above.
                headerRow.Children.Add(new TextBlock
                {
                    Text = $"{mi.Impact:F1}%",
                    Foreground = WarningBrush,
                    FontSize = 12
                });
                itemPanel.Children.Add(headerRow);

                if (!string.IsNullOrEmpty(mi.CreateStatement))
                {
                    itemPanel.Children.Add(new TextBox
                    {
                        Text = mi.CreateStatement,
                        FontFamily = new FontFamily("Consolas"),
                        FontSize = 11,
                        Foreground = TooltipFgBrush,
                        Background = Brushes.Transparent,
                        BorderThickness = new Thickness(0),
                        IsReadOnly = true,
                        TextWrapping = TextWrapping.Wrap,
                        Margin = new Thickness(12, 2, 0, 0)
                    });
                }

                MissingIndexContent.Children.Add(itemPanel);
            }

            MissingIndexEmpty.Visibility = Visibility.Collapsed;
            SetInsightQuiet(MissingIndexHeader, IndexAccentBrush, MissingIndexAccent, isEmpty: false);
        }
        else
        {
            MissingIndexHeader.Text = "Missing Index Suggestions";
            MissingIndexEmpty.Visibility = Visibility.Visible;
            SetInsightQuiet(MissingIndexHeader, IndexAccentBrush, MissingIndexAccent, isEmpty: true);
        }
    }

    /// <summary>
    /// Fills the Parameters card: the row grid (Name | Data Type | Compiled | Runtime, columns
    /// dropped per <see cref="ParameterCard.Columns"/> when nothing carries them), a runtime value
    /// tinted the warning colour when it looks sniffed, and the annotation lines below the table.
    /// Ported from PerformanceStudio dev's <c>ShowParameters</c>
    /// (erikdarlingdata/PerformanceStudio@85492a1); the row/flag/annotation logic itself lives in
    /// the pure <see cref="ParameterCard"/> so it can be pinned outside WPF.
    /// </summary>
    private void ShowParameters(PlanStatement statement)
    {
        ParametersContent.Children.Clear();
        ParametersEmpty.Visibility = Visibility.Collapsed;

        var parameters = statement.Parameters;
        var maskedText = PlanAnalyzer.MaskCommentsAndLiterals(statement.StatementText);
        var unresolved = ParameterCard.FindUnresolvedVariables(statement.StatementText, parameters, statement.RootNode);

        if (parameters.Count == 0)
        {
            ParametersHeader.Text = ParameterCard.HeaderText(0);

            var annotations = ParameterCard.Annotations(parameters, statement.StatementText, maskedText, unresolved);
            if (annotations.Count > 0)
            {
                // Local variables are still something to say, so this card stays lit.
                foreach (var annotation in annotations)
                    AddParameterAnnotation(annotation);
                SetInsightQuiet(ParametersHeader, ParamsAccentBrush, ParametersAccent, isEmpty: false);
            }
            else
            {
                ParametersEmpty.Visibility = Visibility.Visible;
                SetInsightQuiet(ParametersHeader, ParamsAccentBrush, ParametersAccent, isEmpty: true);
            }
            return;
        }

        ParametersHeader.Text = ParameterCard.HeaderText(parameters.Count);
        SetInsightQuiet(ParametersHeader, ParamsAccentBrush, ParametersAccent, isEmpty: false);

        var columns = ParameterCard.Columns(parameters);
        var allCompiledNull = parameters.All(p => p.CompiledValue == null);

        var colDef = new List<GridLength> { GridLength.Auto, GridLength.Auto };
        int compiledCol = -1, runtimeCol = -1;
        int nextCol = 2;
        if (columns.ShowCompiled)
        {
            colDef.Add(new GridLength(1, GridUnitType.Star));
            compiledCol = nextCol++;
        }
        if (columns.ShowRuntime)
        {
            colDef.Add(new GridLength(1, GridUnitType.Star));
            runtimeCol = nextCol++;
        }

        var grid = new Grid();
        foreach (var width in colDef)
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = width });

        var columnHeaderBrush = ParamsAccentBrush;
        var valueBrush = TooltipFgBrush;
        var missingBrush = ErrorBrush;
        var sniffedBrush = WarningBrush;

        int rowIndex = 0;
        grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
        AddParamCell(grid, rowIndex, 0, "Parameter", columnHeaderBrush, FontWeights.SemiBold);
        AddParamCell(grid, rowIndex, 1, "Data Type", columnHeaderBrush, FontWeights.SemiBold);
        if (compiledCol >= 0)
            AddParamCell(grid, rowIndex, compiledCol, columns.CompiledHeaderText, columnHeaderBrush, FontWeights.SemiBold);
        if (runtimeCol >= 0)
            AddParamCell(grid, rowIndex, runtimeCol, "Runtime", columnHeaderBrush, FontWeights.SemiBold);
        rowIndex++;

        foreach (var param in parameters)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var row = ParameterCard.Row(param, allCompiledNull);

            AddParamCell(grid, rowIndex, 0, row.Name, valueBrush, FontWeights.SemiBold);
            AddParamCell(grid, rowIndex, 1, row.DataType, valueBrush);

            if (compiledCol >= 0)
            {
                var compiledBrush = row.CompiledIsMissing ? missingBrush : valueBrush;
                AddParamCell(grid, rowIndex, compiledCol, row.CompiledText, compiledBrush);
            }

            if (runtimeCol >= 0)
            {
                var tooltip = row.Sniffed
                    ? "Runtime value differs from compiled — possible parameter sniffing"
                    : null;
                AddParamCell(grid, rowIndex, runtimeCol, row.RuntimeText, row.Sniffed ? sniffedBrush : valueBrush, tooltip: tooltip);
            }

            rowIndex++;
        }

        ParametersContent.Children.Add(grid);

        foreach (var annotation in ParameterCard.Annotations(parameters, statement.StatementText, maskedText, unresolved))
            AddParameterAnnotation(annotation);
    }

    private static void AddParamCell(Grid grid, int row, int col, string text, Brush brush,
        FontWeight fontWeight = default, string? tooltip = null)
    {
        var tb = new TextBlock
        {
            Text = text,
            FontSize = 11,
            FontWeight = fontWeight == default ? FontWeights.Normal : fontWeight,
            Foreground = brush,
            Margin = new Thickness(0, 2, 10, 2),
            TextTrimming = TextTrimming.CharacterEllipsis,
            MaxWidth = 200,
            Background = Brushes.Transparent
        };
        // Name and DataType columns are short — no need for max width.
        if (col <= 1)
            tb.MaxWidth = double.PositiveInfinity;
        if (tooltip != null)
            tb.ToolTip = tooltip;
        else if (text.Length > 30)
            tb.ToolTip = text;
        Grid.SetRow(tb, row);
        Grid.SetColumn(tb, col);
        grid.Children.Add(tb);
    }

    private void AddParameterAnnotation(ParameterCardAnnotation annotation)
    {
        var brush = annotation.Tone == ParameterCardAnnotationTone.Accent
            ? AccentBrush
            : WarningBrush;
        ParametersContent.Children.Add(new TextBlock
        {
            Text = annotation.Text,
            FontSize = 11,
            FontStyle = FontStyles.Italic,
            Foreground = brush,
            TextWrapping = TextWrapping.Wrap,
            Margin = new Thickness(0, 6, 0, 0)
        });
    }

    private void ShowWaitStats(List<WaitStatInfo> waits, List<PlanWarning> statementWarnings, bool isActualPlan)
    {
        WaitStatsContent.Children.Clear();

        if (waits.Count == 0)
        {
            WaitStatsHeader.Text = "Wait Stats";
            // The populated branch below hangs the previous statement's total on this tooltip;
            // without clearing it here, an empty statement would still answer a hover with a stale count.
            // PlanDisplayText.WaitStatsHeaderTooltip returns null for a zero wait count, so this clears it.
            WaitStatsHeader.ToolTip = PlanDisplayText.WaitStatsHeaderTooltip(0, 0);
            WaitStatsEmpty.Text = isActualPlan
                ? "No wait stats recorded"
                : "No wait stats (estimated plan)";
            WaitStatsEmpty.Visibility = Visibility.Visible;
            SetInsightQuiet(WaitStatsHeader, WaitsAccentBrush, WaitStatsAccent, isEmpty: true);
            return;
        }

        WaitStatsEmpty.Visibility = Visibility.Collapsed;
        SetInsightQuiet(WaitStatsHeader, WaitsAccentBrush, WaitStatsAccent, isEmpty: false);

        var sorted = waits.OrderByDescending(w => w.WaitTimeMs).ToList();
        var maxWait = sorted[0].WaitTimeMs;
        var totalWait = sorted.Sum(w => w.WaitTimeMs);

        // The header ellipsizes in a narrow card, so the total it carries goes on a tooltip too.
        WaitStatsHeader.Text = $"  Wait Stats \u2014 {totalWait:N0}ms total";
        WaitStatsHeader.ToolTip = PlanDisplayText.WaitStatsHeaderTooltip(sorted.Count, totalWait);

        /* Ported from PerformanceStudio (erikdarlingdata/PerformanceStudio@78a3370, refined @80de6fc):
           the wait type and the duration are both star columns; the bar and the trailing "up to N%"
           benefit are Auto. Star columns take whatever is left after the Auto ones and shrink to zero
           if they must, so the two text columns ellipsize (each with its full value on a tooltip) and
           nothing overflows the card — this control's ScrollViewer has horizontal scrolling disabled,
           so an Auto column that wants more than it's given would otherwise clip mid-word or force a
           sideways scrollbar back on. Star-sizing the name also left-aligns every bar into a column,
           which is what makes them comparable at a glance. */
        var grid = new Grid();
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });

        for (int i = 0; i < sorted.Count; i++)
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

        for (int i = 0; i < sorted.Count; i++)
        {
            var w = sorted[i];
            var barFraction = maxWait > 0 ? (double)w.WaitTimeMs / maxWait : 0;
            var category = GetWaitCategory(w.WaitType);
            var color = GetWaitCategoryColor(category);

            // Wait type name, colored by category, capped at 150px with an ellipsis so one long
            // type cannot widen (or clip) the row; the full name is always on the tooltip.
            var nameText = new TextBlock
            {
                Text = w.WaitType,
                FontSize = 12,
                Foreground = TooltipFgBrush,
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxWidth = 150,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 2, 10, 2),
                Background = Brushes.Transparent
            };
            nameText.ToolTip = $"{w.WaitType} \u2014 {category} wait";
            Grid.SetRow(nameText, i);
            Grid.SetColumn(nameText, 0);
            grid.Children.Add(nameText);

            // Bar: the category color at a fixed width, a compact proportional indicator.
            var colorBar = new Border
            {
                Width = Math.Max(4, barFraction * 60),
                Height = 14,
                Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString(color)),
                CornerRadius = new CornerRadius(2),
                HorizontalAlignment = HorizontalAlignment.Left,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 2, 8, 2)
            };
            Grid.SetRow(colorBar, i);
            Grid.SetColumn(colorBar, 1);
            grid.Children.Add(colorBar);

            // Duration text: the other flexible column, so this is what gives when the strip is narrow.
            var durationText = new TextBlock
            {
                Text = $"{w.WaitTimeMs:N0}ms ({w.WaitCount:N0} waits)",
                FontSize = 12,
                Foreground = TooltipFgBrush,
                TextTrimming = TextTrimming.CharacterEllipsis,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 2, 8, 2),
                Background = Brushes.Transparent
            };
            durationText.ToolTip = $"{w.WaitTimeMs:N0} ms across {w.WaitCount:N0} waits";
            Grid.SetRow(durationText, i);
            Grid.SetColumn(durationText, 2);
            grid.Children.Add(durationText);

            // Benefit % (if the analyzer scored one for this wait type) — Auto so it is never the
            // thing that gets clipped.
            var benefitText = WaitRowText.Benefit(w.WaitType, statementWarnings);
            if (benefitText != null)
            {
                var benefitBlock = new TextBlock
                {
                    Text = benefitText,
                    FontSize = 11,
                    Foreground = MutedBrush,
                    VerticalAlignment = VerticalAlignment.Center,
                    Margin = new Thickness(0, 2, 0, 2),
                    Background = Brushes.Transparent
                };
                benefitBlock.ToolTip =
                    $"{benefitText.Replace("up to", "Up to", StringComparison.Ordinal)} of this statement's runtime could be recovered by removing {w.WaitType} waits";
                Grid.SetRow(benefitBlock, i);
                Grid.SetColumn(benefitBlock, 3);
                grid.Children.Add(benefitBlock);
            }
        }

        WaitStatsContent.Children.Add(grid);
    }

    // Wait category + color come from the shared ChartPalette (PerformanceStudio's ~21-category
    // taxonomy + palette), with a light-theme override for the near-invisible pale colors (D3).
    // #4520/#4546: the source tag and the header text (with its benefit suffix) now live in
    // PlanWarningDisplay, shared with the test suite (WPF can't run a unit test on macOS).

    private static string GetWaitCategory(string waitType) => ChartPalette.WaitCategory(waitType);

    private static string GetWaitCategoryColor(string category)
        => ChartPalette.WaitColor(category, ThemeManager.HasLightBackground);

    private void ShowRuntimeSummary(PlanStatement statement)
    {
        RuntimeSummaryContent.Children.Clear();

        // #4570: title, row order, memory-grant colors/spill flag match
        // erikdarlingdata/PerformanceStudio@40ade29 and @5731ae9; the row list itself is built by
        // the shared, pure PlanDisplayText.BuildRuntimeSummaryRows so it can be pinned on macOS
        // (this control is net10.0-windows-only and can't run a unit test there).
        RuntimeSummaryTitle.Text = PlanDisplayText.RuntimeSummaryTitle(statement);

        var labelBrush = MutedBrush;
        var valueBrush = TooltipFgBrush;

        var grid = new Grid();
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        int rowIndex = 0;

        void AddRow(string label, string value, string? colorKey)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var labelText = new TextBlock
            {
                Text = label,
                FontSize = 11,
                Foreground = labelBrush,
                HorizontalAlignment = HorizontalAlignment.Left,
                Margin = new Thickness(0, 1, 8, 1)
            };
            Grid.SetRow(labelText, rowIndex);
            Grid.SetColumn(labelText, 0);
            grid.Children.Add(labelText);

            var valueText = new TextBlock
            {
                Text = value,
                FontSize = 11,
                Foreground = colorKey == null ? valueBrush : (Brush)FindResource(colorKey),
                Margin = new Thickness(0, 1, 0, 1)
            };
            Grid.SetRow(valueText, rowIndex);
            Grid.SetColumn(valueText, 1);
            grid.Children.Add(valueText);

            rowIndex++;
        }

        foreach (var row in PlanDisplayText.BuildRuntimeSummaryRows(statement))
            AddRow(row.Label, row.Value, row.ColorKey);

        RuntimeSummaryContent.Children.Add(grid);
        SetInsightQuiet(RuntimeSummaryTitle, TooltipFgBrush, RuntimeSummaryAccent, isEmpty: false);
    }

    /// <summary>
    /// Fills the Server Context card: the server's name/edition/version, hardware, and instance
    /// settings, in <see cref="ServerContextCard"/>'s order, or the quiet empty state when no
    /// <see cref="ServerMetadata"/> has been set. Ported from PerformanceStudio dev's
    /// <c>ShowServerContext</c> (erikdarlingdata/PerformanceStudio@85492a1); the row logic itself
    /// lives in the pure <see cref="ServerContextCard"/> so it can be pinned outside WPF.
    /// </summary>
    private void ShowServerContext()
    {
        ServerContextContent.Children.Clear();

        var rows = ServerContextCard.Rows(ServerMetadata);
        if (rows.Count == 0)
        {
            ServerContextEmpty.Text = ServerContextCard.EmptyText;
            ServerContextEmpty.Visibility = Visibility.Visible;
            SetInsightQuiet(ServerContextHeader, ServerAccentBrush, ServerContextAccent, isEmpty: true);
            return;
        }

        ServerContextEmpty.Visibility = Visibility.Collapsed;
        SetInsightQuiet(ServerContextHeader, ServerAccentBrush, ServerContextAccent, isEmpty: false);

        var labelBrush = MutedBrush;
        var valueBrush = TooltipFgBrush;

        var grid = new Grid();
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

        for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var row = rows[rowIndex];
            var labelText = new TextBlock
            {
                Text = row.Label,
                FontSize = 11,
                Foreground = labelBrush,
                HorizontalAlignment = HorizontalAlignment.Left,
                Margin = new Thickness(0, 1, 8, 1)
            };
            Grid.SetRow(labelText, rowIndex);
            Grid.SetColumn(labelText, 0);
            grid.Children.Add(labelText);

            var valueText = new TextBlock
            {
                Text = row.Value,
                FontSize = 11,
                Foreground = valueBrush,
                Margin = new Thickness(0, 1, 0, 1)
            };
            Grid.SetRow(valueText, rowIndex);
            Grid.SetColumn(valueText, 1);
            grid.Children.Add(valueText);
        }

        ServerContextContent.Children.Add(grid);
    }

    /// <summary>
    /// Formats a memory value given in KB to a human-readable string.
    /// Under 1,024 KB: show KB. 1,024-1,048,576 KB: show MB (1 decimal). Over 1,048,576 KB: show GB (2 decimals).
    /// </summary>
    private static string FormatMemoryGrantKB(long kb)
    {
        if (kb < 1024)
            return $"{kb:N0} KB";
        if (kb < 1024 * 1024)
            return $"{kb / 1024.0:N1} MB";
        return $"{kb / (1024.0 * 1024.0):N2} GB";
    }

    private void UpdateInsightsHeader()
    {
        InsightsPanel.Visibility = Visibility.Visible;
        InsightsHeader.Text = "  Plan Insights";
    }

    #endregion
}
