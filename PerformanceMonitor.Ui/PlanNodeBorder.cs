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
using System.Windows.Automation;
using System.Windows.Automation.Peers;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The box that draws one operator in the plan viewer. A plain <see cref="Border"/> has no UI Automation peer, so the tooltip
/// summary (cost, rows, warnings) that sighted users read on hover was invisible to a screen reader (walk finding D21). This
/// subclass gives the box a peer whose name is the operator and whose help text is that summary; the on-screen tooltip is unchanged.
/// </summary>
internal sealed class PlanNodeBorder : Border
{
    protected override AutomationPeer OnCreateAutomationPeer() => new PlanNodeBorderPeer(this);

    private sealed class PlanNodeBorderPeer(PlanNodeBorder owner) : FrameworkElementAutomationPeer(owner)
    {
        protected override string GetClassNameCore() => nameof(PlanNodeBorder);

        protected override AutomationControlType GetAutomationControlTypeCore() => AutomationControlType.Group;

        protected override bool IsControlElementCore() => true;

        protected override bool IsContentElementCore() => true;
    }

    /// <summary>
    /// The text a screen reader reads for an operator: the same facts as the hover tooltip, in sentences. Costs, rows (actual ones
    /// when the plan has them), the object, and the warnings (a node's own, or for the root every distinct warning type).
    /// </summary>
    internal static string Summary(PlanNode node, IReadOnlyList<PlanWarning>? warnings)
    {
        var parts = new List<string>
        {
            $"Cost {node.CostPercent}% of statement ({MetricFormatter.FormatCost(node.EstimatedOperatorCost)}), subtree cost {MetricFormatter.FormatCost(node.EstimatedTotalSubtreeCost)}.",
            node.HasActualStats
                ? $"Estimated rows {node.EstimateRows:N1}, actual rows {node.ActualRows:N0}, actual executions {node.ActualExecutions:N0}."
                : $"Estimated rows {node.EstimateRows:N1}."
        };

        var objectName = !string.IsNullOrEmpty(node.FullObjectName) ? node.FullObjectName : node.ObjectName;
        if (!string.IsNullOrEmpty(objectName))
        {
            parts.Add($"Object {objectName}.");
        }

        var all = warnings ?? (node.HasWarnings ? node.Warnings : null);
        if (all is { Count: > 0 })
        {
            var types = all.Select(w => w.WarningType).Where(t => !string.IsNullOrEmpty(t)).Distinct().ToList();
            parts.Add($"{all.Count} {(all.Count == 1 ? "warning" : "warnings")}: {string.Join(", ", types)}.");
        }

        return string.Join(" ", parts);
    }

    /// <summary>The operator's name as the box shows it: the physical operator, and the logical one when it adds something.</summary>
    internal static string OperatorName(PlanNode node)
    {
        var name = node.PhysicalOp;
        if (node.LogicalOp != node.PhysicalOp && !string.IsNullOrEmpty(node.LogicalOp)
            && !node.PhysicalOp.Contains(node.LogicalOp, StringComparison.OrdinalIgnoreCase))
        {
            name += $" ({node.LogicalOp})";
        }

        return name;
    }
}
