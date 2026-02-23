using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using Microsoft.Win32;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

using WpfPath = System.Windows.Shapes.Path;

namespace PerformanceMonitorLite.Controls;

public partial class PlanViewerControl : UserControl
{
    private ParsedPlan? _currentPlan;
    private PlanStatement? _currentStatement;
    private double _zoomLevel = 1.0;
    private const double ZoomStep = 0.15;
    private const double MinZoom = 0.1;
    private const double MaxZoom = 3.0;
    private string _label = "";

    public PlanViewerControl()
    {
        InitializeComponent();
    }

    public void LoadPlan(string planXml, string label)
    {
        _label = label;
        _currentPlan = ShowPlanParser.Parse(planXml);

        var allStatements = _currentPlan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .ToList();

        if (allStatements.Count == 0)
        {
            EmptyState.Visibility = Visibility.Visible;
            PlanScrollViewer.Visibility = Visibility.Collapsed;
            return;
        }

        EmptyState.Visibility = Visibility.Collapsed;
        PlanScrollViewer.Visibility = Visibility.Visible;

        // Populate statement selector
        if (allStatements.Count > 1)
        {
            StatementSelector.Items.Clear();
            for (int i = 0; i < allStatements.Count; i++)
            {
                var s = allStatements[i];
                var text = s.StatementText.Length > 80
                    ? s.StatementText[..80] + "..."
                    : s.StatementText;
                if (string.IsNullOrWhiteSpace(text))
                    text = $"Statement {i + 1}";
                StatementSelector.Items.Add(new ComboBoxItem
                {
                    Content = $"[{s.StatementSubTreeCost:F4}] {text}",
                    Tag = i
                });
            }
            StatementSelector.SelectedIndex = 0;
            StatementSelector.Visibility = Visibility.Visible;
        }
        else
        {
            StatementSelector.Visibility = Visibility.Collapsed;
            RenderStatement(allStatements[0]);
        }
    }

    public void Clear()
    {
        PlanCanvas.Children.Clear();
        _currentPlan = null;
        _currentStatement = null;
        EmptyState.Visibility = Visibility.Visible;
        PlanScrollViewer.Visibility = Visibility.Collapsed;
        MissingIndexBanner.Visibility = Visibility.Collapsed;
        WarningsBanner.Visibility = Visibility.Collapsed;
        StatementSelector.Visibility = Visibility.Collapsed;
        CostText.Text = "";
    }

    private void RenderStatement(PlanStatement statement)
    {
        _currentStatement = statement;
        PlanCanvas.Children.Clear();

        if (statement.RootNode == null) return;

        // Layout
        PlanLayoutEngine.Layout(statement);
        var (width, height) = PlanLayoutEngine.GetExtents(statement.RootNode);
        PlanCanvas.Width = width;
        PlanCanvas.Height = height;

        // Render edges first (behind nodes)
        RenderEdges(statement.RootNode);

        // Render nodes
        RenderNodes(statement.RootNode);

        // Update banners
        ShowMissingIndexes(statement.MissingIndexes);
        ShowWarnings(statement.RootNode);

        // Update cost text
        CostText.Text = $"Statement Cost: {statement.StatementSubTreeCost:F4}";
    }

    private void RenderNodes(PlanNode node)
    {
        var visual = CreateNodeVisual(node);
        Canvas.SetLeft(visual, node.X);
        Canvas.SetTop(visual, node.Y);
        PlanCanvas.Children.Add(visual);

        foreach (var child in node.Children)
            RenderNodes(child);
    }

    private Border CreateNodeVisual(PlanNode node)
    {
        var isExpensive = node.IsExpensive;

        var border = new Border
        {
            Width = PlanLayoutEngine.NodeWidth,
            Height = PlanLayoutEngine.NodeHeight,
            Background = isExpensive
                ? new SolidColorBrush(Color.FromArgb(0x30, 0xE5, 0x73, 0x73))
                : (Brush)FindResource("BackgroundLightBrush"),
            BorderBrush = isExpensive
                ? Brushes.OrangeRed
                : (Brush)FindResource("BorderBrush"),
            BorderThickness = new Thickness(isExpensive ? 2 : 1),
            CornerRadius = new CornerRadius(4),
            Padding = new Thickness(6, 4, 6, 4),
            ToolTip = BuildNodeTooltip(node),
            Cursor = Cursors.Hand,
            SnapsToDevicePixels = true
        };

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

        // Warning indicator
        if (node.HasWarnings)
        {
            iconRow.Children.Add(new TextBlock
            {
                Text = "\u26A0",
                FontSize = 12,
                Foreground = Brushes.Orange,
                VerticalAlignment = VerticalAlignment.Top,
                Margin = new Thickness(2, 0, 0, 0)
            });
        }

        // Parallel indicator
        if (node.Parallel)
        {
            iconRow.Children.Add(new TextBlock
            {
                Text = "\u21C6",
                FontSize = 11,
                Foreground = new SolidColorBrush(Color.FromRgb(0x6B, 0xB5, 0xFF)),
                VerticalAlignment = VerticalAlignment.Top,
                Margin = new Thickness(2, 0, 0, 0),
                ToolTip = "Parallel execution"
            });
        }

        stack.Children.Add(iconRow);

        // Operator name
        var opName = node.PhysicalOp;
        if (opName.Length > 22)
            opName = opName[..20] + "...";

        stack.Children.Add(new TextBlock
        {
            Text = opName,
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
            : (Brush)FindResource("ForegroundMutedBrush");

        stack.Children.Add(new TextBlock
        {
            Text = $"Cost: {node.CostPercent}%",
            FontSize = 10,
            Foreground = costColor,
            TextAlignment = TextAlignment.Center,
            HorizontalAlignment = HorizontalAlignment.Center
        });

        // Object name (if present, small text)
        if (!string.IsNullOrEmpty(node.ObjectName))
        {
            var objName = node.ObjectName;
            if (objName.Length > 24) objName = objName[..22] + "...";
            stack.Children.Add(new TextBlock
            {
                Text = objName,
                FontSize = 9,
                Foreground = (Brush)FindResource("ForegroundMutedBrush"),
                TextAlignment = TextAlignment.Center,
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxWidth = PlanLayoutEngine.NodeWidth - 16,
                HorizontalAlignment = HorizontalAlignment.Center
            });
        }

        border.Child = stack;
        return border;
    }

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
        var parentCenterY = parent.Y + PlanLayoutEngine.NodeHeight / 2;
        var childLeft = child.X;
        var childCenterY = child.Y + PlanLayoutEngine.NodeHeight / 2;

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

        var rowText = child.HasActualStats
            ? $"Actual Rows: {child.ActualRows:N0}"
            : $"Estimated Rows: {child.EstimateRows:N0}";

        return new WpfPath
        {
            Data = geometry,
            Stroke = new SolidColorBrush(Color.FromRgb(0x6B, 0x72, 0x80)),
            StrokeThickness = thickness,
            StrokeLineJoin = PenLineJoin.Round,
            ToolTip = rowText,
            SnapsToDevicePixels = true
        };
    }

    private ToolTip BuildNodeTooltip(PlanNode node)
    {
        var tip = new ToolTip
        {
            Background = new SolidColorBrush(Color.FromRgb(0x1A, 0x1D, 0x23)),
            BorderBrush = new SolidColorBrush(Color.FromRgb(0x3A, 0x3D, 0x45)),
            Foreground = new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB)),
            Padding = new Thickness(12),
            MaxWidth = 500
        };

        var stack = new StackPanel();

        // Header
        var headerText = node.PhysicalOp;
        if (node.LogicalOp != node.PhysicalOp && !string.IsNullOrEmpty(node.LogicalOp))
            headerText += $" ({node.LogicalOp})";
        stack.Children.Add(new TextBlock
        {
            Text = headerText,
            FontWeight = FontWeights.Bold,
            FontSize = 13,
            Margin = new Thickness(0, 0, 0, 8)
        });

        // Cost
        AddTooltipRow(stack, "Cost", $"{node.CostPercent}% of statement ({node.EstimatedOperatorCost:F6})");
        AddTooltipRow(stack, "Subtree Cost", $"{node.EstimatedTotalSubtreeCost:F6}");

        // Rows
        AddTooltipRow(stack, "Estimated Rows", $"{node.EstimateRows:N1}");
        if (node.HasActualStats)
        {
            AddTooltipRow(stack, "Actual Rows", $"{node.ActualRows:N0}");
            if (node.ActualRowsRead > 0)
                AddTooltipRow(stack, "Actual Rows Read", $"{node.ActualRowsRead:N0}");
            AddTooltipRow(stack, "Actual Executions", $"{node.ActualExecutions:N0}");
        }

        // I/O and CPU estimates
        if (node.EstimateIO > 0) AddTooltipRow(stack, "Estimated I/O", $"{node.EstimateIO:F6}");
        if (node.EstimateCPU > 0) AddTooltipRow(stack, "Estimated CPU", $"{node.EstimateCPU:F6}");
        if (node.EstimatedRowSize > 0) AddTooltipRow(stack, "Avg Row Size", $"{node.EstimatedRowSize} B");

        // Actual I/O (if available)
        if (node.HasActualStats && (node.ActualLogicalReads > 0 || node.ActualPhysicalReads > 0))
        {
            AddTooltipRow(stack, "Logical Reads", $"{node.ActualLogicalReads:N0}");
            if (node.ActualPhysicalReads > 0)
                AddTooltipRow(stack, "Physical Reads", $"{node.ActualPhysicalReads:N0}");
        }

        // Actual timing
        if (node.HasActualStats && node.ActualElapsedMs > 0)
        {
            AddTooltipRow(stack, "Elapsed Time", $"{node.ActualElapsedMs:N0} ms");
            if (node.ActualCPUMs > 0)
                AddTooltipRow(stack, "CPU Time", $"{node.ActualCPUMs:N0} ms");
        }

        // Parallelism
        if (node.Parallel)
            AddTooltipRow(stack, "Parallel", "Yes");
        if (!string.IsNullOrEmpty(node.ExecutionMode))
            AddTooltipRow(stack, "Execution Mode", node.ExecutionMode);
        if (!string.IsNullOrEmpty(node.PartitioningType))
            AddTooltipRow(stack, "Partitioning", node.PartitioningType);

        // Object
        if (!string.IsNullOrEmpty(node.ObjectName))
            AddTooltipRow(stack, "Object", node.ObjectName);
        if (node.Ordered)
            AddTooltipRow(stack, "Ordered", "True");

        // Predicates
        if (!string.IsNullOrEmpty(node.SeekPredicates))
            AddTooltipRow(stack, "Seek Predicate", node.SeekPredicates, isCode: true);
        if (!string.IsNullOrEmpty(node.Predicate))
            AddTooltipRow(stack, "Predicate", node.Predicate, isCode: true);

        // Output columns
        if (!string.IsNullOrEmpty(node.OutputColumns))
            AddTooltipRow(stack, "Output", node.OutputColumns, isCode: true);

        // Warnings
        if (node.HasWarnings)
        {
            stack.Children.Add(new Separator { Margin = new Thickness(0, 6, 0, 6) });
            foreach (var w in node.Warnings)
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

        // Node ID
        stack.Children.Add(new TextBlock
        {
            Text = $"Node ID: {node.NodeId}",
            FontSize = 10,
            Foreground = new SolidColorBrush(Color.FromRgb(0x6B, 0x72, 0x80)),
            Margin = new Thickness(0, 8, 0, 0)
        });

        tip.Content = stack;
        return tip;
    }

    private static void AddTooltipRow(StackPanel parent, string label, string value, bool isCode = false)
    {
        var row = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(0, 1, 0, 1) };
        row.Children.Add(new TextBlock
        {
            Text = $"{label}: ",
            Foreground = new SolidColorBrush(Color.FromRgb(0x6B, 0x72, 0x80)),
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

    private void ShowMissingIndexes(List<MissingIndex> indexes)
    {
        if (indexes.Count > 0)
        {
            MissingIndexList.ItemsSource = indexes;
            MissingIndexBanner.Visibility = Visibility.Visible;
        }
        else
        {
            MissingIndexBanner.Visibility = Visibility.Collapsed;
        }
    }

    private void ShowWarnings(PlanNode root)
    {
        var allWarnings = new List<PlanWarning>();
        CollectWarnings(root, allWarnings);

        if (allWarnings.Count > 0)
        {
            var criticalCount = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical);
            var warningCount = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Warning);
            var parts = new List<string>();
            if (criticalCount > 0) parts.Add($"{criticalCount} critical");
            if (warningCount > 0) parts.Add($"{warningCount} warning(s)");
            WarningsSummaryText.Text = $"Plan has {string.Join(", ", parts)} — hover over nodes with \u26A0 for details";
            WarningsBanner.Visibility = Visibility.Visible;
        }
        else
        {
            WarningsBanner.Visibility = Visibility.Collapsed;
        }
    }

    private static void CollectWarnings(PlanNode node, List<PlanWarning> warnings)
    {
        warnings.AddRange(node.Warnings);
        foreach (var child in node.Children)
            CollectWarnings(child, warnings);
    }

    // Zoom handlers
    private void ZoomIn_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel + ZoomStep);
    private void ZoomOut_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel - ZoomStep);

    private void ZoomFit_Click(object sender, RoutedEventArgs e)
    {
        if (PlanCanvas.Width <= 0 || PlanCanvas.Height <= 0) return;

        var viewWidth = PlanScrollViewer.ActualWidth;
        var viewHeight = PlanScrollViewer.ActualHeight;
        if (viewWidth <= 0 || viewHeight <= 0) return;

        var fitZoom = Math.Min(viewWidth / PlanCanvas.Width, viewHeight / PlanCanvas.Height);
        SetZoom(Math.Min(fitZoom, 1.0)); // Don't zoom beyond 100% for fit
    }

    private void SetZoom(double level)
    {
        _zoomLevel = Math.Max(MinZoom, Math.Min(MaxZoom, level));
        ZoomTransform.ScaleX = _zoomLevel;
        ZoomTransform.ScaleY = _zoomLevel;
        ZoomLevelText.Text = $"{(int)(_zoomLevel * 100)}%";
    }

    private void PlanScrollViewer_PreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (Keyboard.Modifiers == ModifierKeys.Control)
        {
            e.Handled = true;
            SetZoom(_zoomLevel + (e.Delta > 0 ? ZoomStep : -ZoomStep));
        }
    }

    private void SavePlan_Click(object sender, RoutedEventArgs e)
    {
        if (_currentPlan == null || string.IsNullOrEmpty(_currentPlan.RawXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan Files (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"plan_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
        };

        if (dialog.ShowDialog() == true)
        {
            File.WriteAllText(dialog.FileName, _currentPlan.RawXml);
        }
    }

    private void StatementSelector_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (StatementSelector.SelectedItem is ComboBoxItem item && item.Tag is int index)
        {
            var allStatements = _currentPlan?.Batches
                .SelectMany(b => b.Statements)
                .Where(s => s.RootNode != null)
                .ToList();

            if (allStatements != null && index >= 0 && index < allStatements.Count)
                RenderStatement(allStatements[index]);
        }
    }
}
