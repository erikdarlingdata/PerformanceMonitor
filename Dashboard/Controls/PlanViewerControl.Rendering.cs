/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

using WpfPath = System.Windows.Shapes.Path;

namespace PerformanceMonitorDashboard.Controls;

public partial class PlanViewerControl
{
    private void RenderStatement(PlanStatement statement)
    {
        _currentStatement = statement;
        PlanCanvas.Children.Clear();
        _selectedNodeBorder = null;
        PlanScrollViewer.ScrollToHome();

        if (statement.RootNode == null) return;

        // Layout
        PlanLayoutEngine.Layout(statement);
        var (width, height) = PlanLayoutEngine.GetExtents(statement.RootNode);
        PlanCanvas.Width = width;
        PlanCanvas.Height = height;

        // Render edges first (behind nodes)
        RenderEdges(statement.RootNode);

        // Render nodes
        var allWarnings = new List<PlanWarning>();
        CollectWarnings(statement.RootNode, allWarnings);
        RenderNodes(statement.RootNode, allWarnings.Count);

        // Update banners
        ShowMissingIndexes(statement.MissingIndexes);
        ShowWaitStats(statement.WaitStats, statement.QueryTimeStats != null);
        ShowRuntimeSummary(statement);
        UpdateInsightsHeader();

        // Update cost text
        CostText.Text = $"Statement Cost: {statement.StatementSubTreeCost:F4}";
    }

    #region Node Rendering

    private void RenderNodes(PlanNode node, int totalWarningCount = -1)
    {
        var visual = CreateNodeVisual(node, totalWarningCount);
        Canvas.SetLeft(visual, node.X);
        Canvas.SetTop(visual, node.Y);
        PlanCanvas.Children.Add(visual);

        foreach (var child in node.Children)
            RenderNodes(child);
    }

    private Border CreateNodeVisual(PlanNode node, int totalWarningCount = -1)
    {
        var isExpensive = node.IsExpensive;

        var border = new Border
        {
            Width = PlanLayoutEngine.NodeWidth,
            MinHeight = PlanLayoutEngine.NodeHeightMin,
            Background = isExpensive
                ? new SolidColorBrush(Color.FromArgb(0x30, 0xE5, 0x73, 0x73))
                : (Brush)FindResource("BackgroundLightBrush"),
            BorderBrush = isExpensive
                ? Brushes.OrangeRed
                : (Brush)FindResource("BorderBrush"),
            BorderThickness = new Thickness(isExpensive ? 2 : 1),
            CornerRadius = new CornerRadius(4),
            Padding = new Thickness(6, 4, 6, 4),
            Cursor = Cursors.Hand,
            SnapsToDevicePixels = true,
            Tag = node
        };

        // Tooltip — root node includes statement-level PlanWarnings
        if (totalWarningCount > 0 && _currentStatement != null)
        {
            var allWarnings = new List<PlanWarning>();
            allWarnings.AddRange(_currentStatement.PlanWarnings);
            CollectWarnings(node, allWarnings);
            border.ToolTip = BuildNodeTooltip(node, allWarnings);
        }
        else
        {
            border.ToolTip = BuildNodeTooltip(node);
        }

        // Click to select + show properties
        border.MouseLeftButtonUp += Node_Click;

        // Right-click context menu
        border.ContextMenu = BuildNodeContextMenu(node);

        var stack = new StackPanel { HorizontalAlignment = HorizontalAlignment.Center };

        // Icon row: icon + optional warning/parallel indicators
        var iconRow = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Center };

        var icon = PlanIconMapper.GetIcon(node.IconName);
        if (icon != null)
        {
            iconRow.Children.Add(new Image
            {
                Source = icon,
                Width = 32,
                Height = 32,
                Margin = new Thickness(0, 0, 0, 2)
            });
        }

        // Warning indicator badge (orange triangle with !)
        if (node.HasWarnings)
        {
            var warnBadge = new Grid
            {
                Width = 20, Height = 20,
                Margin = new Thickness(4, 0, 0, 0),
                VerticalAlignment = VerticalAlignment.Center
            };
            warnBadge.Children.Add(new Polygon
            {
                Points = new PointCollection
                {
                    new Point(10, 0), new Point(20, 18), new Point(0, 18)
                },
                Fill = Brushes.Orange
            });
            warnBadge.Children.Add(new TextBlock
            {
                Text = "!",
                FontSize = 12,
                FontWeight = FontWeights.ExtraBold,
                Foreground = Brushes.White,
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 3, 0, 0)
            });
            iconRow.Children.Add(warnBadge);
        }

        // Parallel indicator badge (amber circle with arrows)
        if (node.Parallel)
        {
            var parBadge = new Grid
            {
                Width = 20, Height = 20,
                Margin = new Thickness(4, 0, 0, 0),
                VerticalAlignment = VerticalAlignment.Center
            };
            parBadge.Children.Add(new Ellipse
            {
                Width = 20, Height = 20,
                Fill = new SolidColorBrush(Color.FromRgb(0xFF, 0xC1, 0x07))
            });
            parBadge.Children.Add(new TextBlock
            {
                Text = "\u21C6",
                FontSize = 12,
                FontWeight = FontWeights.Bold,
                Foreground = new SolidColorBrush(Color.FromRgb(0x33, 0x33, 0x33)),
                HorizontalAlignment = HorizontalAlignment.Center,
                VerticalAlignment = VerticalAlignment.Center
            });
            iconRow.Children.Add(parBadge);
        }

        // Nonclustered index count badge (modification operators maintaining multiple NC indexes)
        if (node.NonClusteredIndexCount > 0)
        {
            var ncBadge = new Border
            {
                Background = new SolidColorBrush(Color.FromRgb(0x6C, 0x75, 0x7D)),
                CornerRadius = new CornerRadius(4),
                Padding = new Thickness(4, 1, 4, 1),
                Margin = new Thickness(4, 0, 0, 0),
                VerticalAlignment = VerticalAlignment.Center,
                Child = new TextBlock
                {
                    Text = $"+{node.NonClusteredIndexCount} NC",
                    FontSize = 10,
                    FontWeight = FontWeights.SemiBold,
                    Foreground = Brushes.White
                }
            };
            iconRow.Children.Add(ncBadge);
        }

        stack.Children.Add(iconRow);

        // Operator name — use full name, let TextTrimming handle overflow
        stack.Children.Add(new TextBlock
        {
            Text = node.PhysicalOp,
            FontSize = 10,
            FontWeight = FontWeights.SemiBold,
            Foreground = (Brush)FindResource("ForegroundBrush"),
            TextAlignment = TextAlignment.Center,
            TextTrimming = TextTrimming.CharacterEllipsis,
            MaxWidth = PlanLayoutEngine.NodeWidth - 16,
            HorizontalAlignment = HorizontalAlignment.Center
        });

        // Cost percentage
        var costColor = node.CostPercent >= 50 ? Brushes.OrangeRed
            : node.CostPercent >= 25 ? Brushes.Orange
            : (Brush)FindResource("ForegroundBrush");

        stack.Children.Add(new TextBlock
        {
            Text = $"Cost: {node.CostPercent}%",
            FontSize = 10,
            Foreground = costColor,
            TextAlignment = TextAlignment.Center,
            HorizontalAlignment = HorizontalAlignment.Center
        });

        // Actual plan stats: elapsed time, CPU time, and row counts
        if (node.HasActualStats)
        {
            var fgBrush = (Brush)FindResource("ForegroundBrush");

            // Elapsed time — red if >= 1 second
            var elapsedSec = node.ActualElapsedMs / 1000.0;
            var elapsedBrush = elapsedSec >= 1.0 ? Brushes.OrangeRed : fgBrush;
            stack.Children.Add(new TextBlock
            {
                Text = $"{elapsedSec:F3}s",
                FontSize = 10,
                Foreground = elapsedBrush,
                TextAlignment = TextAlignment.Center,
                HorizontalAlignment = HorizontalAlignment.Center
            });

            // CPU time — red if >= 1 second
            var cpuSec = node.ActualCPUMs / 1000.0;
            var cpuBrush = cpuSec >= 1.0 ? Brushes.OrangeRed : fgBrush;
            stack.Children.Add(new TextBlock
            {
                Text = $"CPU: {cpuSec:F3}s",
                FontSize = 9,
                Foreground = cpuBrush,
                TextAlignment = TextAlignment.Center,
                HorizontalAlignment = HorizontalAlignment.Center
            });

            // Actual rows of Estimated rows (accuracy %) — red if off by 10x+
            var estRows = node.EstimateRows;
            var accuracyRatio = estRows > 0 ? node.ActualRows / estRows : (node.ActualRows > 0 ? double.MaxValue : 1.0);
            var rowBrush = (accuracyRatio < 0.1 || accuracyRatio > 10.0) ? Brushes.OrangeRed : fgBrush;
            var accuracy = estRows > 0
                ? $" ({accuracyRatio * 100:F0}%)"
                : "";
            stack.Children.Add(new TextBlock
            {
                Text = $"{node.ActualRows:N0} of {estRows:N0}{accuracy}",
                FontSize = 9,
                Foreground = rowBrush,
                TextAlignment = TextAlignment.Center,
                HorizontalAlignment = HorizontalAlignment.Center,
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxWidth = PlanLayoutEngine.NodeWidth - 16
            });
        }

        // Object name — show full object name, use ellipsis for overflow
        if (!string.IsNullOrEmpty(node.ObjectName))
        {
            stack.Children.Add(new TextBlock
            {
                Text = node.FullObjectName ?? node.ObjectName,
                FontSize = 9,
                Foreground = (Brush)FindResource("ForegroundBrush"),
                TextAlignment = TextAlignment.Center,
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxWidth = PlanLayoutEngine.NodeWidth - 16,
                HorizontalAlignment = HorizontalAlignment.Center,
                ToolTip = node.FullObjectName ?? node.ObjectName
            });
        }

        // Total warning count badge on root node
        if (totalWarningCount > 0)
        {
            var badgeRow = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 2, 0, 0)
            };
            badgeRow.Children.Add(new TextBlock
            {
                Text = "\u26A0",
                FontSize = 13,
                Foreground = OrangeBrush,
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 0, 4, 0)
            });
            badgeRow.Children.Add(new TextBlock
            {
                Text = $"{totalWarningCount} warning{(totalWarningCount == 1 ? "" : "s")}",
                FontSize = 12,
                FontWeight = FontWeights.SemiBold,
                Foreground = OrangeBrush,
                VerticalAlignment = VerticalAlignment.Center
            });
            stack.Children.Add(badgeRow);
        }

        border.Child = stack;
        return border;
    }

    #endregion

    #region Edge Rendering

    private void RenderEdges(PlanNode node)
    {
        foreach (var child in node.Children)
        {
            var path = CreateElbowConnector(node, child);
            PlanCanvas.Children.Add(path);

            RenderEdges(child);
        }
    }

    private WpfPath CreateElbowConnector(PlanNode parent, PlanNode child)
    {
        var parentRight = parent.X + PlanLayoutEngine.NodeWidth;
        var parentCenterY = parent.Y + PlanLayoutEngine.GetNodeHeight(parent) / 2;
        var childLeft = child.X;
        var childCenterY = child.Y + PlanLayoutEngine.GetNodeHeight(child) / 2;

        // Arrow thickness based on row estimate (logarithmic)
        var rows = child.HasActualStats ? child.ActualRows : child.EstimateRows;
        var thickness = Math.Max(2, Math.Min(Math.Floor(Math.Log(Math.Max(1, rows))), 12));

        var midX = (parentRight + childLeft) / 2;

        var geometry = new PathGeometry();
        var figure = new PathFigure
        {
            StartPoint = new Point(parentRight, parentCenterY),
            IsClosed = false
        };
        figure.Segments.Add(new LineSegment(new Point(midX, parentCenterY), true));
        figure.Segments.Add(new LineSegment(new Point(midX, childCenterY), true));
        figure.Segments.Add(new LineSegment(new Point(childLeft, childCenterY), true));
        geometry.Figures.Add(figure);

        return new WpfPath
        {
            Data = geometry,
            Stroke = EdgeBrush,
            StrokeThickness = thickness,
            StrokeLineJoin = PenLineJoin.Round,
            ToolTip = BuildEdgeTooltipContent(child),
            SnapsToDevicePixels = true
        };
    }

    private Border BuildEdgeTooltipContent(PlanNode child)
    {
        var grid = new Grid { MinWidth = 240 };
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        int row = 0;

        void AddRow(string label, string value)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            var lbl = new TextBlock
            {
                Text = label,
                Foreground = MutedBrush,
                FontSize = 12,
                Margin = new Thickness(0, 1, 12, 1)
            };
            var val = new TextBlock
            {
                Text = value,
                Foreground = TooltipFgBrush,
                FontSize = 12,
                FontWeight = FontWeights.SemiBold,
                HorizontalAlignment = HorizontalAlignment.Right,
                Margin = new Thickness(0, 1, 0, 1)
            };
            Grid.SetRow(lbl, row);
            Grid.SetColumn(lbl, 0);
            Grid.SetRow(val, row);
            Grid.SetColumn(val, 1);
            grid.Children.Add(lbl);
            grid.Children.Add(val);
            row++;
        }

        if (child.HasActualStats)
            AddRow("Actual Number of Rows for All Executions", $"{child.ActualRows:N0}");

        AddRow("Estimated Number of Rows Per Execution", $"{child.EstimateRows:N0}");

        var executions = 1.0 + child.EstimateRebinds + child.EstimateRewinds;
        var estimatedRowsAllExec = child.EstimateRows * executions;
        AddRow("Estimated Number of Rows for All Executions", $"{estimatedRowsAllExec:N0}");

        if (child.EstimatedRowSize > 0)
        {
            AddRow("Estimated Row Size", FormatBytes(child.EstimatedRowSize));
            var dataSize = estimatedRowsAllExec * child.EstimatedRowSize;
            AddRow("Estimated Data Size", FormatBytes(dataSize));
        }

        return new Border
        {
            Background = TooltipBgBrush,
            BorderBrush = TooltipBorderBrush,
            BorderThickness = new Thickness(1),
            Padding = new Thickness(10, 6, 10, 6),
            CornerRadius = new CornerRadius(4),
            Child = grid
        };
    }

    private static string FormatBytes(double bytes)
    {
        if (bytes < 1024) return $"{bytes:N0} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024:N0} KB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024 * 1024):N0} MB";
        return $"{bytes / (1024L * 1024 * 1024):N1} GB";
    }

    #endregion
}
