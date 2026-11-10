/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// How the plan viewer's Statements panel, plan canvas and Properties panel share the viewer's width. Each panel opens at the width it
/// asks for: 450 px for the Statements panel, or the width it was dragged to, and the remembered width for the Properties panel. At
/// their defaults the two and their splitters take 840 px, which in a 900 px viewer leaves the canvas 60 px, less than one
/// operator. When the widths asked for leave the canvas less than <see cref="MinCanvasWidth"/> px, the Statements panel narrows first, no further than
/// <see cref="MinStatementsWidth"/> px, and then the Properties panel, no further than its own minimum. A viewer narrower than the
/// three minimums gives the canvas what the two panels leave at theirs.
/// </summary>
public static class PlanPanelLayout
{
    /// <summary>The width, in px, the Statements panel opens at.</summary>
    public const double DefaultStatementsWidth = 450;

    /// <summary>The width, in px, the Statements panel is not narrowed below to make room for the canvas.</summary>
    public const double MinStatementsWidth = 200;

    /// <summary>The canvas width, in px, the panels make room for: an operator's 150 px box after the layout's 40 px padding, and a scroll bar.</summary>
    public const double MinCanvasWidth = 240;

    /// <summary>The width, in px, of the splitter beside each open panel.</summary>
    public const double SplitterWidth = 5;

    /// <summary>
    /// The widths the two panels take in a viewer <paramref name="viewerWidth"/> px wide, and the minimum the canvas column holds.
    /// A null width is a closed panel, which takes no width and has no splitter. A panel asked for at less than its minimum keeps
    /// the width asked for.
    /// </summary>
    public static PlanPanelWidths Fit(double viewerWidth, double? statementsWidth, double? propertiesWidth)
    {
        var statements = statementsWidth ?? 0;
        var properties = propertiesWidth ?? 0;
        var splitters = (statementsWidth is null ? 0 : SplitterWidth) + (propertiesWidth is null ? 0 : SplitterWidth);

        var over = statements + properties + splitters + MinCanvasWidth - viewerWidth;
        if (over > 0)
        {
            var given = Math.Min(over, Math.Max(0, statements - MinStatementsWidth));
            statements -= given;
            over -= given;
        }

        if (over > 0)
            properties -= Math.Min(over, Math.Max(0, properties - PerformanceMonitor.PlanAnalysis.PropertyRows.MinPropertiesWidth));

        var canvas = Math.Max(0, viewerWidth - statements - properties - splitters);
        return new PlanPanelWidths(statements, properties, Math.Min(MinCanvasWidth, canvas));
    }
}

/// <summary>The widths, in px, of the Statements panel and the Properties panel, and the minimum width the canvas column holds.</summary>
public readonly record struct PlanPanelWidths(double Statements, double Properties, double CanvasMinimum);
