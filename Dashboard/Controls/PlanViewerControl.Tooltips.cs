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
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Controls;

public partial class PlanViewerControl
{
    #region Tooltips

    private ToolTip BuildNodeTooltip(PlanNode node, List<PlanWarning>? allWarnings = null)
    {
        var tip = new ToolTip
        {
            Background = TooltipBgBrush,
            BorderBrush = TooltipBorderBrush,
            Foreground = TooltipFgBrush,
            Padding = new Thickness(12),
            MaxWidth = 500
        };

        var stack = new StackPanel();

        // Header
        var headerText = node.PhysicalOp;
        if (node.LogicalOp != node.PhysicalOp && !string.IsNullOrEmpty(node.LogicalOp)
            && !node.PhysicalOp.Contains(node.LogicalOp, StringComparison.OrdinalIgnoreCase))
            headerText += $" ({node.LogicalOp})";
        stack.Children.Add(new TextBlock
        {
            Text = headerText,
            FontWeight = FontWeights.Bold,
            FontSize = 13,
            Margin = new Thickness(0, 0, 0, 8)
        });

        // Cost
        AddTooltipSection(stack, "Costs");
        AddTooltipRow(stack, "Cost", $"{node.CostPercent}% of statement ({node.EstimatedOperatorCost:F6})");
        AddTooltipRow(stack, "Subtree Cost", $"{node.EstimatedTotalSubtreeCost:F6}");

        // Rows
        AddTooltipSection(stack, "Rows");
        AddTooltipRow(stack, "Estimated Rows", $"{node.EstimateRows:N1}");
        if (node.HasActualStats)
        {
            AddTooltipRow(stack, "Actual Rows", $"{node.ActualRows:N0}");
            if (node.ActualRowsRead > 0)
                AddTooltipRow(stack, "Actual Rows Read", $"{node.ActualRowsRead:N0}");
            AddTooltipRow(stack, "Actual Executions", $"{node.ActualExecutions:N0}");
        }

        // I/O and CPU estimates
        if (node.EstimateIO > 0 || node.EstimateCPU > 0 || node.EstimatedRowSize > 0)
        {
            AddTooltipSection(stack, "Estimates");
            if (node.EstimateIO > 0) AddTooltipRow(stack, "I/O Cost", $"{node.EstimateIO:F6}");
            if (node.EstimateCPU > 0) AddTooltipRow(stack, "CPU Cost", $"{node.EstimateCPU:F6}");
            if (node.EstimatedRowSize > 0) AddTooltipRow(stack, "Avg Row Size", $"{node.EstimatedRowSize} B");
        }

        // Actual I/O
        if (node.HasActualStats && (node.ActualLogicalReads > 0 || node.ActualPhysicalReads > 0))
        {
            AddTooltipSection(stack, "Actual I/O");
            AddTooltipRow(stack, "Logical Reads", $"{node.ActualLogicalReads:N0}");
            if (node.ActualPhysicalReads > 0)
                AddTooltipRow(stack, "Physical Reads", $"{node.ActualPhysicalReads:N0}");
            if (node.ActualScans > 0)
                AddTooltipRow(stack, "Scans", $"{node.ActualScans:N0}");
            if (node.ActualReadAheads > 0)
                AddTooltipRow(stack, "Read-Aheads", $"{node.ActualReadAheads:N0}");
        }

        // Actual timing
        if (node.HasActualStats && (node.ActualElapsedMs > 0 || node.ActualCPUMs > 0))
        {
            AddTooltipSection(stack, "Timing");
            if (node.ActualElapsedMs > 0)
                AddTooltipRow(stack, "Elapsed Time", $"{node.ActualElapsedMs:N0} ms");
            if (node.ActualCPUMs > 0)
                AddTooltipRow(stack, "CPU Time", $"{node.ActualCPUMs:N0} ms");
        }

        // Parallelism
        if (node.Parallel || !string.IsNullOrEmpty(node.ExecutionMode) || !string.IsNullOrEmpty(node.PartitioningType))
        {
            AddTooltipSection(stack, "Parallelism");
            if (node.Parallel) AddTooltipRow(stack, "Parallel", "Yes");
            if (!string.IsNullOrEmpty(node.ExecutionMode))
                AddTooltipRow(stack, "Execution Mode", node.ExecutionMode);
            if (!string.IsNullOrEmpty(node.ActualExecutionMode) && node.ActualExecutionMode != node.ExecutionMode)
                AddTooltipRow(stack, "Actual Exec Mode", node.ActualExecutionMode);
            if (!string.IsNullOrEmpty(node.PartitioningType))
                AddTooltipRow(stack, "Partitioning", node.PartitioningType);
        }

        // Object — show full qualified name
        if (!string.IsNullOrEmpty(node.FullObjectName))
        {
            AddTooltipSection(stack, "Object");
            AddTooltipRow(stack, "Name", node.FullObjectName, isCode: true);
            if (node.Ordered) AddTooltipRow(stack, "Ordered", "True");
            if (!string.IsNullOrEmpty(node.ScanDirection))
                AddTooltipRow(stack, "Scan Direction", node.ScanDirection);
        }
        else if (!string.IsNullOrEmpty(node.ObjectName))
        {
            AddTooltipSection(stack, "Object");
            AddTooltipRow(stack, "Name", node.ObjectName, isCode: true);
            if (node.Ordered) AddTooltipRow(stack, "Ordered", "True");
            if (!string.IsNullOrEmpty(node.ScanDirection))
                AddTooltipRow(stack, "Scan Direction", node.ScanDirection);
        }

        // NC index maintenance count
        if (node.NonClusteredIndexCount > 0)
            AddTooltipRow(stack, "NC Indexes Maintained", string.Join(", ", node.NonClusteredIndexNames));

        // Operator details (key items only in tooltip)
        var hasTooltipDetails = !string.IsNullOrEmpty(node.OrderBy)
            || !string.IsNullOrEmpty(node.TopExpression)
            || !string.IsNullOrEmpty(node.GroupBy)
            || !string.IsNullOrEmpty(node.OuterReferences);
        if (hasTooltipDetails)
        {
            AddTooltipSection(stack, "Details");
            if (!string.IsNullOrEmpty(node.OrderBy))
                AddTooltipRow(stack, "Order By", node.OrderBy, isCode: true);
            if (!string.IsNullOrEmpty(node.TopExpression))
                AddTooltipRow(stack, "Top", node.IsPercent ? $"{node.TopExpression} PERCENT" : node.TopExpression);
            if (!string.IsNullOrEmpty(node.GroupBy))
                AddTooltipRow(stack, "Group By", node.GroupBy, isCode: true);
            if (!string.IsNullOrEmpty(node.OuterReferences))
                AddTooltipRow(stack, "Outer References", node.OuterReferences, isCode: true);
        }

        // Predicates
        if (!string.IsNullOrEmpty(node.SeekPredicates) || !string.IsNullOrEmpty(node.Predicate))
        {
            AddTooltipSection(stack, "Predicates");
            if (!string.IsNullOrEmpty(node.SeekPredicates))
                AddTooltipRow(stack, "Seek", node.SeekPredicates, isCode: true);
            if (!string.IsNullOrEmpty(node.Predicate))
                AddTooltipRow(stack, "Residual", node.Predicate, isCode: true);
        }

        // Output columns
        if (!string.IsNullOrEmpty(node.OutputColumns))
        {
            AddTooltipSection(stack, "Output");
            AddTooltipRow(stack, "Columns", node.OutputColumns, isCode: true);
        }

        // Warnings — use allWarnings (includes statement-level) for root, node.Warnings for others
        var warnings = allWarnings ?? (node.HasWarnings ? node.Warnings : null);
        if (warnings != null && warnings.Count > 0)
        {
            stack.Children.Add(new Separator { Margin = new Thickness(0, 6, 0, 6) });

            if (allWarnings != null)
            {
                // Root node: show distinct warning type names only
                var distinct = warnings
                    .GroupBy(w => w.WarningType)
                    .Select(g => (Type: g.Key, MaxSeverity: g.Max(w => w.Severity), Count: g.Count()))
                    .OrderByDescending(g => g.MaxSeverity)
                    .ThenBy(g => g.Type);

                foreach (var (type, severity, count) in distinct)
                {
                    var warnColor = severity == PlanWarningSeverity.Critical ? "#E57373"
                        : severity == PlanWarningSeverity.Warning ? "#FFB347" : "#6BB5FF";
                    var label = count > 1 ? $"\u26A0 {type} ({count})" : $"\u26A0 {type}";
                    stack.Children.Add(new TextBlock
                    {
                        Text = label,
                        Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor)),
                        FontSize = 11,
                        Margin = new Thickness(0, 2, 0, 0)
                    });
                }
            }
            else
            {
                // Individual node: show full warning messages
                foreach (var w in warnings)
                {
                    var warnColor = w.Severity == PlanWarningSeverity.Critical ? "#E57373"
                        : w.Severity == PlanWarningSeverity.Warning ? "#FFB347" : "#6BB5FF";
                    stack.Children.Add(new TextBlock
                    {
                        Text = $"\u26A0 {w.WarningType}: {w.Message}",
                        Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor)),
                        FontSize = 11,
                        TextWrapping = TextWrapping.Wrap,
                        Margin = new Thickness(0, 2, 0, 0)
                    });
                }
            }
        }

        // Footer hint
        stack.Children.Add(new TextBlock
        {
            Text = "Click to view full properties",
            FontSize = 10,
            FontStyle = FontStyles.Italic,
            Foreground = MutedBrush,
            Margin = new Thickness(0, 8, 0, 0)
        });

        tip.Content = stack;
        return tip;
    }

    private void AddTooltipSection(StackPanel parent, string title)
    {
        parent.Children.Add(new TextBlock
        {
            Text = title,
            FontSize = 10,
            FontWeight = FontWeights.SemiBold,
            Foreground = SectionHeaderBrush,
            Margin = new Thickness(0, 6, 0, 2)
        });
    }

    private void AddTooltipRow(StackPanel parent, string label, string value, bool isCode = false)
    {
        var row = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 1, 0, 1) };
        row.Children.Add(new TextBlock
        {
            Text = $"{label}: ",
            Foreground = MutedBrush,
            FontSize = 11,
            MinWidth = 120
        });
        var valueBlock = new TextBlock
        {
            Text = value,
            FontSize = 11,
            TextWrapping = TextWrapping.Wrap,
            MaxWidth = 350
        };
        if (isCode) valueBlock.FontFamily = new FontFamily("Consolas");
        row.Children.Add(valueBlock);
        parent.Children.Add(row);
    }

    #endregion
}
