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

    // Node selection
    private Border? _selectedNodeBorder;
    private Brush? _selectedNodeOriginalBorder;
    private Thickness _selectedNodeOriginalThickness;

    // Brushes
    private static readonly SolidColorBrush SelectionBrush = new(Color.FromRgb(0x4F, 0xA3, 0xFF));
    private static readonly SolidColorBrush TooltipBgBrush = new(Color.FromRgb(0x1A, 0x1D, 0x23));
    private static readonly SolidColorBrush TooltipBorderBrush = new(Color.FromRgb(0x3A, 0x3D, 0x45));
    private static readonly SolidColorBrush TooltipFgBrush = new(Color.FromRgb(0xE4, 0xE6, 0xEB));
    private static readonly SolidColorBrush MutedBrush = new(Color.FromRgb(0xE4, 0xE6, 0xEB));
    private static readonly SolidColorBrush EdgeBrush = new(Color.FromRgb(0x6B, 0x72, 0x80));
    private static readonly SolidColorBrush SectionHeaderBrush = new(Color.FromRgb(0x4F, 0xA3, 0xFF));
    private static readonly SolidColorBrush PropSeparatorBrush = new(Color.FromRgb(0x2A, 0x2D, 0x35));
    private static readonly SolidColorBrush OrangeBrush = new(Color.FromRgb(0xFF, 0xB3, 0x47));

    // Current property section for collapsible groups
    private StackPanel? _currentPropertySection;

    // Canvas panning
    private bool _isPanning;
    private Point _panStart;
    private double _panStartOffsetX;
    private double _panStartOffsetY;

    public PlanViewerControl()
    {
        InitializeComponent();
    }

    public void LoadPlan(string planXml, string label, string? queryText = null)
    {
        _label = label;

        if (!string.IsNullOrEmpty(queryText))
        {
            QueryTextBox.Text = queryText;
            QueryTextExpander.Visibility = Visibility.Visible;
        }
        else
        {
            QueryTextExpander.Visibility = Visibility.Collapsed;
        }
        _currentPlan = ShowPlanParser.Parse(planXml);
        PlanAnalyzer.Analyze(_currentPlan);

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
            StatementLabel.Visibility = Visibility.Visible;
            StatementSelector.Visibility = Visibility.Visible;
            CostText.Visibility = Visibility.Visible;
        }
        else
        {
            StatementLabel.Visibility = Visibility.Collapsed;
            StatementSelector.Visibility = Visibility.Collapsed;
            CostText.Visibility = Visibility.Collapsed;
            RenderStatement(allStatements[0]);
        }
    }

    public void Clear()
    {
        PlanCanvas.Children.Clear();
        _currentPlan = null;
        _currentStatement = null;
        _selectedNodeBorder = null;
        EmptyState.Visibility = Visibility.Visible;
        PlanScrollViewer.Visibility = Visibility.Collapsed;
        InsightsPanel.Visibility = Visibility.Collapsed;
        StatementLabel.Visibility = Visibility.Collapsed;
        StatementSelector.Visibility = Visibility.Collapsed;
        CostText.Text = "";
        CostText.Visibility = Visibility.Collapsed;
        ClosePropertiesPanel();
    }

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

        // Update insights panel
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
            ToolTip = BuildNodeTooltip(node),
            Cursor = Cursors.Hand,
            SnapsToDevicePixels = true,
            Tag = node
        };

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

        var rowText = child.HasActualStats
            ? $"Actual Rows: {child.ActualRows:N0}"
            : $"Estimated Rows: {child.EstimateRows:N0}";

        return new WpfPath
        {
            Data = geometry,
            Stroke = EdgeBrush,
            StrokeThickness = thickness,
            StrokeLineJoin = PenLineJoin.Round,
            ToolTip = rowText,
            SnapsToDevicePixels = true
        };
    }

    #endregion

    #region Node Selection & Properties Panel

    private void Node_Click(object sender, MouseButtonEventArgs e)
    {
        if (sender is Border border && border.Tag is PlanNode node)
        {
            SelectNode(border, node);
            e.Handled = true;
        }
    }

    private void SelectNode(Border border, PlanNode node)
    {
        // Deselect previous
        if (_selectedNodeBorder != null)
        {
            _selectedNodeBorder.BorderBrush = _selectedNodeOriginalBorder;
            _selectedNodeBorder.BorderThickness = _selectedNodeOriginalThickness;
        }

        // Select new
        _selectedNodeOriginalBorder = border.BorderBrush;
        _selectedNodeOriginalThickness = border.BorderThickness;
        _selectedNodeBorder = border;
        border.BorderBrush = SelectionBrush;
        border.BorderThickness = new Thickness(2);

        ShowPropertiesPanel(node);
    }

    private ContextMenu BuildNodeContextMenu(PlanNode node)
    {
        var menu = new ContextMenu();

        var propsItem = new MenuItem { Header = "Properties" };
        propsItem.Click += (_, _) =>
        {
            // Find the border for this node by checking Tags
            foreach (var child in PlanCanvas.Children)
            {
                if (child is Border b && b.Tag == node)
                {
                    SelectNode(b, node);
                    break;
                }
            }
        };
        menu.Items.Add(propsItem);

        menu.Items.Add(new Separator());

        var copyOpItem = new MenuItem { Header = "Copy Operator Name" };
        copyOpItem.Click += (_, _) => Clipboard.SetDataObject(node.PhysicalOp, false);
        menu.Items.Add(copyOpItem);

        if (!string.IsNullOrEmpty(node.FullObjectName))
        {
            var copyObjItem = new MenuItem { Header = "Copy Object Name" };
            copyObjItem.Click += (_, _) => Clipboard.SetDataObject(node.FullObjectName, false);
            menu.Items.Add(copyObjItem);
        }

        if (!string.IsNullOrEmpty(node.Predicate))
        {
            var copyPredItem = new MenuItem { Header = "Copy Predicate" };
            copyPredItem.Click += (_, _) => Clipboard.SetDataObject(node.Predicate, false);
            menu.Items.Add(copyPredItem);
        }

        if (!string.IsNullOrEmpty(node.SeekPredicates))
        {
            var copySeekItem = new MenuItem { Header = "Copy Seek Predicate" };
            copySeekItem.Click += (_, _) => Clipboard.SetDataObject(node.SeekPredicates, false);
            menu.Items.Add(copySeekItem);
        }

        return menu;
    }

    private void ShowPropertiesPanel(PlanNode node)
    {
        PropertiesContent.Children.Clear();
        _currentPropertySection = null;

        // Header
        var headerText = node.PhysicalOp;
        if (node.LogicalOp != node.PhysicalOp && !string.IsNullOrEmpty(node.LogicalOp))
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
            || node.ManyToMany || node.BitmapCreator
            || node.SortDistinct || node.StartupExpression
            || node.NLOptimized || node.WithOrderedPrefetch || node.WithUnorderedPrefetch
            || node.WithTies || node.Remoting || node.LocalParallelism
            || node.SpoolStack || node.DMLRequestSort
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
            if (node.ManyToMany)
                AddPropertyRow("Many to Many", "True");
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
        AddPropertyRow("Operator Cost", $"{node.EstimatedOperatorCost:F6} ({node.CostPercent}%)");
        AddPropertyRow("Subtree Cost", $"{node.EstimatedTotalSubtreeCost:F6}");
        AddPropertyRow("I/O Cost", $"{node.EstimateIO:F6}");
        AddPropertyRow("CPU Cost", $"{node.EstimateCPU:F6}");

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
            if (node.PerThreadStats.Count > 1)
                foreach (var t in node.PerThreadStats)
                    AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualRows:N0}", indent: true);
            if (node.ActualRowsRead > 0)
            {
                AddPropertyRow("Actual Rows Read", $"{node.ActualRowsRead:N0}");
                if (node.PerThreadStats.Count > 1)
                    foreach (var t in node.PerThreadStats.Where(t => t.ActualRowsRead > 0))
                        AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualRowsRead:N0}", indent: true);
            }
            AddPropertyRow("Actual Executions", $"{node.ActualExecutions:N0}");
            if (node.PerThreadStats.Count > 1)
                foreach (var t in node.PerThreadStats)
                    AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualExecutions:N0}", indent: true);
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

            // Timing
            if (node.ActualElapsedMs > 0 || node.ActualCPUMs > 0
                || node.UdfCpuTimeMs > 0 || node.UdfElapsedTimeMs > 0)
            {
                AddPropertySection("Actual Timing");
                if (node.ActualElapsedMs > 0)
                {
                    AddPropertyRow("Elapsed Time", $"{node.ActualElapsedMs:N0} ms");
                    if (node.PerThreadStats.Count > 1)
                        foreach (var t in node.PerThreadStats.Where(t => t.ActualElapsedMs > 0))
                            AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualElapsedMs:N0} ms", indent: true);
                }
                if (node.ActualCPUMs > 0)
                {
                    AddPropertyRow("CPU Time", $"{node.ActualCPUMs:N0} ms");
                    if (node.PerThreadStats.Count > 1)
                        foreach (var t in node.PerThreadStats.Where(t => t.ActualCPUMs > 0))
                            AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualCPUMs:N0} ms", indent: true);
                }
                if (node.UdfElapsedTimeMs > 0)
                    AddPropertyRow("UDF Elapsed", $"{node.UdfElapsedTimeMs:N0} ms");
                if (node.UdfCpuTimeMs > 0)
                    AddPropertyRow("UDF CPU", $"{node.UdfCpuTimeMs:N0} ms");
            }

            // I/O
            var hasIo = node.ActualLogicalReads > 0 || node.ActualPhysicalReads > 0
                || node.ActualScans > 0 || node.ActualReadAheads > 0
                || node.ActualSegmentReads > 0 || node.ActualSegmentSkips > 0;
            if (hasIo)
            {
                AddPropertySection("Actual I/O");
                AddPropertyRow("Logical Reads", $"{node.ActualLogicalReads:N0}");
                if (node.PerThreadStats.Count > 1)
                    foreach (var t in node.PerThreadStats.Where(t => t.ActualLogicalReads > 0))
                        AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualLogicalReads:N0}", indent: true);
                if (node.ActualPhysicalReads > 0)
                {
                    AddPropertyRow("Physical Reads", $"{node.ActualPhysicalReads:N0}");
                    if (node.PerThreadStats.Count > 1)
                        foreach (var t in node.PerThreadStats.Where(t => t.ActualPhysicalReads > 0))
                            AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualPhysicalReads:N0}", indent: true);
                }
                if (node.ActualScans > 0)
                {
                    AddPropertyRow("Scans", $"{node.ActualScans:N0}");
                    if (node.PerThreadStats.Count > 1)
                        foreach (var t in node.PerThreadStats.Where(t => t.ActualScans > 0))
                            AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualScans:N0}", indent: true);
                }
                if (node.ActualReadAheads > 0)
                {
                    AddPropertyRow("Read-Ahead Reads", $"{node.ActualReadAheads:N0}");
                    if (node.PerThreadStats.Count > 1)
                        foreach (var t in node.PerThreadStats.Where(t => t.ActualReadAheads > 0))
                            AddPropertyRow($"  Thread {t.ThreadId}", $"{t.ActualReadAheads:N0}", indent: true);
                }
                if (node.ActualSegmentReads > 0)
                    AddPropertyRow("Segment Reads", $"{node.ActualSegmentReads:N0}");
                if (node.ActualSegmentSkips > 0)
                    AddPropertyRow("Segment Skips", $"{node.ActualSegmentSkips:N0}");
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
                foreach (var w in s.PlanWarnings)
                {
                    var warnColor = w.Severity == PlanWarningSeverity.Critical ? "#E57373"
                        : w.Severity == PlanWarningSeverity.Warning ? "#FFB347" : "#6BB5FF";
                    var warnPanel = new StackPanel { Margin = new Thickness(10, 2, 10, 2) };
                    warnPanel.Children.Add(new TextBlock
                    {
                        Text = $"\u26A0 {w.WarningType}",
                        FontWeight = FontWeights.SemiBold,
                        FontSize = 11,
                        Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor))
                    });
                    warnPanel.Children.Add(new TextBlock
                    {
                        Text = w.Message,
                        FontSize = 11,
                        Foreground = TooltipFgBrush,
                        TextWrapping = TextWrapping.Wrap,
                        Margin = new Thickness(16, 0, 0, 0)
                    });
                    (_currentPropertySection ?? PropertiesContent).Children.Add(warnPanel);
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
            foreach (var w in node.Warnings)
            {
                var warnColor = w.Severity == PlanWarningSeverity.Critical ? "#E57373"
                    : w.Severity == PlanWarningSeverity.Warning ? "#FFB347" : "#6BB5FF";
                var warnPanel = new StackPanel { Margin = new Thickness(10, 2, 10, 2) };
                warnPanel.Children.Add(new TextBlock
                {
                    Text = $"\u26A0 {w.WarningType}",
                    FontWeight = FontWeights.SemiBold,
                    FontSize = 11,
                    Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(warnColor))
                });
                warnPanel.Children.Add(new TextBlock
                {
                    Text = w.Message,
                    FontSize = 11,
                    Foreground = TooltipFgBrush,
                    TextWrapping = TextWrapping.Wrap,
                    Margin = new Thickness(16, 0, 0, 0)
                });
                PropertiesContent.Children.Add(warnPanel);
            }
        }

        // Show the panel
        PropertiesColumn.Width = new GridLength(320);
        PropertiesSplitter.Visibility = Visibility.Visible;
        PropertiesPanel.Visibility = Visibility.Visible;
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
            Background = new SolidColorBrush(Color.FromArgb(0x18, 0x4F, 0xA3, 0xFF)),
            BorderBrush = PropSeparatorBrush,
            BorderThickness = new Thickness(0, 0, 0, 1)
        };
        PropertiesContent.Children.Add(expander);
        _currentPropertySection = contentPanel;
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

        var valueBlock = new TextBlock
        {
            Text = value,
            FontSize = indent ? 10 : 11,
            Foreground = TooltipFgBrush,
            TextWrapping = TextWrapping.Wrap
        };
        if (isCode) valueBlock.FontFamily = new FontFamily("Consolas");
        Grid.SetColumn(valueBlock, 1);
        grid.Children.Add(valueBlock);

        var target = _currentPropertySection ?? PropertiesContent;
        target.Children.Add(grid);
    }

    private void CloseProperties_Click(object sender, RoutedEventArgs e)
    {
        ClosePropertiesPanel();
    }

    private void ClosePropertiesPanel()
    {
        PropertiesPanel.Visibility = Visibility.Collapsed;
        PropertiesSplitter.Visibility = Visibility.Collapsed;
        PropertiesColumn.Width = new GridLength(0);

        // Deselect node
        if (_selectedNodeBorder != null)
        {
            _selectedNodeBorder.BorderBrush = _selectedNodeOriginalBorder;
            _selectedNodeBorder.BorderThickness = _selectedNodeOriginalThickness;
            _selectedNodeBorder = null;
        }
    }

    #endregion

    #region Tooltips

    private ToolTip BuildNodeTooltip(PlanNode node)
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

    private static void AddTooltipSection(StackPanel parent, string title)
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

    private static void AddTooltipRow(StackPanel parent, string label, string value, bool isCode = false)
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

    #region Insights Panel

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
                    Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
                    FontSize = 12
                });
                headerRow.Children.Add(new TextBlock
                {
                    Text = $" \u2014 Impact: ",
                    Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
                    FontSize = 12
                });
                headerRow.Children.Add(new TextBlock
                {
                    Text = $"{mi.Impact:F1}%",
                    Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#FFB347")),
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
                        Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
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
        }
        else
        {
            MissingIndexHeader.Text = "Missing Index Suggestions";
            MissingIndexEmpty.Visibility = Visibility.Visible;
        }
    }

    private static void CollectWarnings(PlanNode node, List<PlanWarning> warnings)
    {
        warnings.AddRange(node.Warnings);
        foreach (var child in node.Children)
            CollectWarnings(child, warnings);
    }

    private void ShowWaitStats(List<WaitStatInfo> waits, bool isActualPlan)
    {
        WaitStatsContent.Children.Clear();

        if (waits.Count == 0)
        {
            WaitStatsHeader.Text = "Wait Stats";
            WaitStatsEmpty.Text = isActualPlan
                ? "No wait stats recorded"
                : "No wait stats (estimated plan)";
            WaitStatsEmpty.Visibility = Visibility.Visible;
            return;
        }

        WaitStatsEmpty.Visibility = Visibility.Collapsed;

        var sorted = waits.OrderByDescending(w => w.WaitTimeMs).ToList();
        var maxWait = sorted[0].WaitTimeMs;
        var totalWait = sorted.Sum(w => w.WaitTimeMs);

        WaitStatsHeader.Text = $"  Wait Stats \u2014 {totalWait:N0}ms total";

        var longestName = sorted.Max(w => w.WaitType.Length);
        var nameColWidth = longestName * 6.5 + 10;

        var maxBarWidth = 300;

        var grid = new Grid();
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(nameColWidth) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(maxBarWidth + 16) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });

        for (int i = 0; i < sorted.Count; i++)
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

        for (int i = 0; i < sorted.Count; i++)
        {
            var w = sorted[i];
            var barFraction = maxWait > 0 ? (double)w.WaitTimeMs / maxWait : 0;
            var color = GetWaitCategoryColor(GetWaitCategory(w.WaitType));

            var nameText = new TextBlock
            {
                Text = w.WaitType,
                FontSize = 12,
                Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 2, 10, 2)
            };
            Grid.SetRow(nameText, i);
            Grid.SetColumn(nameText, 0);
            grid.Children.Add(nameText);

            var colorBar = new Border
            {
                Width = Math.Max(4, barFraction * maxBarWidth),
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

            var durationText = new TextBlock
            {
                Text = $"{w.WaitTimeMs:N0}ms ({w.WaitCount:N0} waits)",
                FontSize = 12,
                Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(0, 2, 0, 2)
            };
            Grid.SetRow(durationText, i);
            Grid.SetColumn(durationText, 2);
            grid.Children.Add(durationText);
        }

        WaitStatsContent.Children.Add(grid);
    }

    private static string GetWaitCategory(string waitType)
    {
        if (waitType.StartsWith("SOS_SCHEDULER_YIELD", StringComparison.Ordinal) ||
            waitType.StartsWith("CXPACKET", StringComparison.Ordinal) ||
            waitType.StartsWith("CXCONSUMER", StringComparison.Ordinal) ||
            waitType.StartsWith("CXSYNC_PORT", StringComparison.Ordinal) ||
            waitType.StartsWith("CXSYNC_CONSUMER", StringComparison.Ordinal))
            return "CPU";

        if (waitType.StartsWith("PAGEIOLATCH", StringComparison.Ordinal) ||
            waitType.StartsWith("WRITELOG", StringComparison.Ordinal) ||
            waitType.StartsWith("IO_COMPLETION", StringComparison.Ordinal) ||
            waitType.StartsWith("ASYNC_IO_COMPLETION", StringComparison.Ordinal))
            return "I/O";

        if (waitType.StartsWith("LCK_M_", StringComparison.Ordinal))
            return "Lock";

        if (waitType == "RESOURCE_SEMAPHORE" || waitType == "CMEMTHREAD")
            return "Memory";

        if (waitType == "ASYNC_NETWORK_IO")
            return "Network";

        return "Other";
    }

    private static string GetWaitCategoryColor(string category)
    {
        return category switch
        {
            "CPU" => "#4FA3FF",
            "I/O" => "#FFB347",
            "Lock" => "#E57373",
            "Memory" => "#9B59B6",
            "Network" => "#2ECC71",
            _ => "#6BB5FF"
        };
    }

    private void ShowRuntimeSummary(PlanStatement statement)
    {
        RuntimeSummaryContent.Children.Clear();

        var labelColor = "#E4E6EB";
        var valueColor = "#E4E6EB";

        var grid = new Grid();
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        int rowIndex = 0;

        void AddRow(string label, string value)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var labelText = new TextBlock
            {
                Text = label,
                FontSize = 11,
                Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(labelColor)),
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
                Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString(valueColor)),
                Margin = new Thickness(0, 1, 0, 1)
            };
            Grid.SetRow(valueText, rowIndex);
            Grid.SetColumn(valueText, 1);
            grid.Children.Add(valueText);

            rowIndex++;
        }

        if (statement.QueryTimeStats != null)
        {
            AddRow("Elapsed", $"{statement.QueryTimeStats.ElapsedTimeMs:N0}ms");
            AddRow("CPU", $"{statement.QueryTimeStats.CpuTimeMs:N0}ms");
        }

        if (statement.MemoryGrant != null)
        {
            var mg = statement.MemoryGrant;
            AddRow("Memory grant", $"{mg.GrantedMemoryKB:N0} KB granted, {mg.MaxUsedMemoryKB:N0} KB used");
            if (mg.GrantWaitTimeMs > 0)
                AddRow("Grant wait", $"{mg.GrantWaitTimeMs:N0}ms");
        }

        if (statement.DegreeOfParallelism > 0)
            AddRow("DOP", statement.DegreeOfParallelism.ToString());
        else if (statement.NonParallelPlanReason != null)
            AddRow("Serial", statement.NonParallelPlanReason);

        if (statement.ThreadStats != null)
        {
            var ts = statement.ThreadStats;
            AddRow("Branches", ts.Branches.ToString());
            var totalReserved = ts.Reservations.Sum(r => r.ReservedThreads);
            if (totalReserved > 0)
            {
                var threadText = ts.UsedThreads == totalReserved
                    ? $"{ts.UsedThreads} used ({totalReserved} reserved)"
                    : $"{ts.UsedThreads} used of {totalReserved} reserved ({totalReserved - ts.UsedThreads} inactive)";
                AddRow("Threads", threadText);
            }
            else
            {
                AddRow("Threads", $"{ts.UsedThreads} used");
            }
        }

        if (statement.CardinalityEstimationModelVersion > 0)
            AddRow("CE model", statement.CardinalityEstimationModelVersion.ToString());

        if (statement.CompileTimeMs > 0)
            AddRow("Compile time", $"{statement.CompileTimeMs:N0}ms");
        if (statement.CachedPlanSizeKB > 0)
            AddRow("Cached plan size", $"{statement.CachedPlanSizeKB:N0} KB");

        if (!string.IsNullOrEmpty(statement.StatementOptmLevel))
            AddRow("Optimization", statement.StatementOptmLevel);
        if (!string.IsNullOrEmpty(statement.StatementOptmEarlyAbortReason))
            AddRow("Early abort", statement.StatementOptmEarlyAbortReason);

        RuntimeSummaryContent.Children.Add(grid);
    }

    private void UpdateInsightsHeader()
    {
        InsightsPanel.Visibility = Visibility.Visible;
        InsightsHeader.Text = "  Plan Insights";
    }

    #endregion

    #region Zoom

    private void ZoomIn_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel + ZoomStep);
    private void ZoomOut_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel - ZoomStep);

    private void ZoomFit_Click(object sender, RoutedEventArgs e)
    {
        if (PlanCanvas.Width <= 0 || PlanCanvas.Height <= 0) return;

        var viewWidth = PlanScrollViewer.ActualWidth;
        var viewHeight = PlanScrollViewer.ActualHeight;
        if (viewWidth <= 0 || viewHeight <= 0) return;

        var fitZoom = Math.Min(viewWidth / PlanCanvas.Width, viewHeight / PlanCanvas.Height);
        SetZoom(Math.Min(fitZoom, 1.0));
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

    #endregion

    #region Canvas Panning

    private void PlanScrollViewer_PreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        // Don't intercept scrollbar interactions
        if (IsScrollBarAtPoint(e))
            return;

        // Don't pan if clicking on a node
        if (IsNodeAtPoint(e))
            return;

        _isPanning = true;
        _panStart = e.GetPosition(PlanScrollViewer);
        _panStartOffsetX = PlanScrollViewer.HorizontalOffset;
        _panStartOffsetY = PlanScrollViewer.VerticalOffset;
        PlanScrollViewer.Cursor = Cursors.SizeAll;
        PlanScrollViewer.CaptureMouse();
        e.Handled = true;
    }

    private void PlanScrollViewer_PreviewMouseMove(object sender, MouseEventArgs e)
    {
        if (!_isPanning) return;

        var current = e.GetPosition(PlanScrollViewer);
        var dx = current.X - _panStart.X;
        var dy = current.Y - _panStart.Y;

        PlanScrollViewer.ScrollToHorizontalOffset(Math.Max(0, _panStartOffsetX - dx));
        PlanScrollViewer.ScrollToVerticalOffset(Math.Max(0, _panStartOffsetY - dy));
        e.Handled = true;
    }

    private void PlanScrollViewer_PreviewMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        if (!_isPanning) return;
        _isPanning = false;
        PlanScrollViewer.Cursor = Cursors.Arrow;
        PlanScrollViewer.ReleaseMouseCapture();
        e.Handled = true;
    }

    /// <summary>Check if the mouse event originated from a ScrollBar.</summary>
    private static bool IsScrollBarAtPoint(MouseButtonEventArgs e)
    {
        var source = e.OriginalSource as DependencyObject;
        while (source != null)
        {
            if (source is System.Windows.Controls.Primitives.ScrollBar)
                return true;
            source = VisualTreeHelper.GetParent(source);
        }
        return false;
    }

    /// <summary>Check if the mouse event originated from a node Border (has PlanNode in Tag).</summary>
    private static bool IsNodeAtPoint(MouseButtonEventArgs e)
    {
        var source = e.OriginalSource as DependencyObject;
        while (source != null)
        {
            if (source is Border b && b.Tag is PlanNode)
                return true;
            source = VisualTreeHelper.GetParent(source);
        }
        return false;
    }

    #endregion

    #region Save & Statement Selection

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

    #endregion
}
