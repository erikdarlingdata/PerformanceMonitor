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
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.Common;

using WpfPath = System.Windows.Shapes.Path;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared block-chain viewer: renders ONE chain — the lead blocker (apex) at the left with its full
/// transitive victim hierarchy flowing right — for the session the user opened it from. The clicked
/// session is highlighted within the tree. Mirrors PlanViewerControl's conventions — Canvas +
/// ScaleTransform zoom/pan, click-to-select properties, theme lifecycle (subscribe in ctor, never
/// unsubscribe on Unloaded, expose <see cref="Cleanup"/> for the host window to call on close).
/// </summary>
public partial class BlockingChainControl : UserControl, IGraphViewer
{
    private const double NodeWidth = BlockingChainLayout.NodeWidth;
    private const double NodeHeight = 166;

    private BlockingChainModel? _model;
    private double _zoomLevel = 1.0;
    private const double ZoomStep = 0.15;
    private const double MinZoom = 0.1;
    private const double MaxZoom = 3.0;

    // The session the viewer was opened from — highlighted (auto-selected) on load.
    private int _entrySpid;
    private int _entryEcid;

    // Node selection
    private Border? _selectedNodeBorder;
    private Brush? _selectedNodeOriginalBorder;
    private Thickness _selectedNodeOriginalThickness;
    private BlockingChainNode? _selectedNode;

    // Canvas panning
    private bool _isPanning;
    private Point _panStart;
    private double _panStartOffsetX;
    private double _panStartOffsetY;

    // Accent brushes — chosen to read on every theme (same palette as PlanViewerControl).
    private static readonly SolidColorBrush ApexBrush = new(Color.FromRgb(0x4F, 0xA3, 0xFF));        // LEAD BLOCKER badge fill only (kept bright so the apex pops; the border is theme-aware below)
    private static readonly SolidColorBrush SelectionBrush = new(Color.FromRgb(0x8E, 0x5B, 0xFF));   // selected node border (purple — distinct from apex)
    private static readonly SolidColorBrush SleepingBrush = new(Color.FromRgb(0x90, 0xA4, 0xAE));    // sleeping-apex badge (slate — distinct from the warning amber)
    private static readonly SolidColorBrush EdgeBrush = new(Color.FromRgb(0x6B, 0x72, 0x80));

    // Theme-aware chrome resolved at call time from the app's merged resource dictionaries.
    private Brush NodeBackgroundBrush =>
        (TryFindResource("BackgroundLightBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x2A, 0x2D, 0x35));
    private Brush NodeBorderBrush =>
        (TryFindResource("BorderBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x3A, 0x3D, 0x45));
    private Brush ForegroundBrush =>
        (TryFindResource("ForegroundBrush") as Brush) ?? Brushes.White;
    private Brush MutedBrush =>
        (TryFindResource("ForegroundMutedBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB));
    // Longest-wait emphasis (border + the wait number). Theme-aware so it stays legible on Light/CoolBreeze
    // (orange-on-white fails AA); fallback is the Dark amber.
    private Brush WarningBrush =>
        (TryFindResource("WarningTextBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0xFF, 0xB3, 0x47));
    // Apex node border. Theme-aware so it clears >=3:1 as a border on the light node card (bright #4FA3FF
    // is only ~2.6:1 on white); fallback is the Dark blue. (The badge fill keeps the bright ApexBrush.)
    private Brush ApexBorderBrush =>
        (TryFindResource("ApexBorderBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x4F, 0xA3, 0xFF));

    public BlockingChainControl()
    {
        InitializeComponent();
        /* Subscribe for the life of the control. Do NOT unsubscribe on Unloaded — a TabControl fires
           Unloaded on tab switch, which would permanently detach this handler. The host window calls
           Cleanup() when it is actually closed. */
        ThemeManager.ThemeChanged += OnThemeChanged;
    }

    /// <summary>Unsubscribes from theme changes. The host window calls this on close.</summary>
    public void Cleanup()
    {
        ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private void OnThemeChanged(string _)
    {
        if (_model == null) return;

        var nodeToRestore = _selectedNode;
        Render();

        if (nodeToRestore == null) return;
        SelectNodeByModel(nodeToRestore);
    }

    /// <summary>
    /// Loads the single reconstructed chain that contains the clicked session and highlights that session.
    /// Pure UI work — the caller does the (off-UI-thread) fetch, reconstruct, single-chain select, and
    /// tree-build, then hands the finished one-root <see cref="BlockingChainModel"/> here.
    /// </summary>
    /// <param name="model">The model holding exactly one root (the chain rooted at its apex).</param>
    /// <param name="entrySpid">SPID of the session the viewer was opened from (highlighted on load).</param>
    /// <param name="entryEcid">Its execution-context id — the spid:ecid identity disambiguator.</param>
    public void LoadModel(BlockingChainModel model, int entrySpid, int entryEcid, string? emptyStateDetail = null)
    {
        _model = model ?? throw new ArgumentNullException(nameof(model));
        _entrySpid = entrySpid;
        _entryEcid = entryEcid;

        if (!string.IsNullOrWhiteSpace(emptyStateDetail))
            EmptyStateDetail.Text = emptyStateDetail;

        BuildFlagBanner();

        if (_model.Roots.Count == 0)
        {
            ShowEmptyState(true);
            return;
        }

        ShowEmptyState(false);
        Render();

        // Highlight (auto-select) the clicked session within the tree so the user lands on "their" node.
        var entry = _model.Roots
            .SelectMany(EnumerateTree)
            .FirstOrDefault(n => n.Spid == _entrySpid && n.Ecid == _entryEcid);
        if (entry != null)
        {
            SelectNodeByModel(entry);
            ScrollNodeIntoView(entry);
        }
    }

    private void ShowEmptyState(bool empty)
    {
        EmptyState.Visibility = empty ? Visibility.Visible : Visibility.Collapsed;
        ChainScrollViewer.Visibility = empty ? Visibility.Collapsed : Visibility.Visible;
    }

    private void BuildFlagBanner()
    {
        if (_model == null) return;
        var parts = new List<string>();
        if (_model.CycleDetected) parts.Add("cycle detected");
        if (_model.DepthCapped) parts.Add("depth capped");
        if (_model.TraversalTruncated) parts.Add("traversal truncated");

        if (parts.Count == 0)
        {
            FlagBanner.Visibility = Visibility.Collapsed;
            FlagBanner.Text = "";
        }
        else
        {
            // Reads as a warning (⚠ + the theme-aware amber via FlagBanner's Foreground) — these caveats
            // mean the rendered chain may be incomplete or misleading.
            FlagBanner.Visibility = Visibility.Visible;
            FlagBanner.Text = "⚠ " + string.Join("  ·  ", parts);
        }
    }

    #region Rendering

    private void Render()
    {
        ChainCanvas.Children.Clear();
        _selectedNodeBorder = null;

        if (_model == null || _model.Roots.Count == 0) return;

        ChainScrollViewer.ScrollToHome();

        var (width, height) = BlockingChainLayout.Layout(_model.Roots, _ => NodeHeight);
        ChainCanvas.Width = width;
        ChainCanvas.Height = height;

        // Per-chain longest-wait highlight (only one chain is shown, so it's always meaningful).
        long maxWait = 0;
        foreach (var root in _model.Roots)
            Walk(root, n => { if (n.WaitTimeMs > maxWait) maxWait = n.WaitTimeMs; });
        var longestWaitNode = maxWait > 0
            ? _model.Roots.SelectMany(EnumerateTree).FirstOrDefault(n => n.WaitTimeMs == maxWait)
            : null;

        foreach (var root in _model.Roots)
            RenderEdges(root);
        foreach (var root in _model.Roots)
            RenderNodes(root, longestWaitNode);
    }

    private void RenderNodes(BlockingChainNode node, BlockingChainNode? longestWaitNode)
    {
        var visual = CreateNodeVisual(node, ReferenceEquals(node, longestWaitNode));
        Canvas.SetLeft(visual, node.X);
        Canvas.SetTop(visual, node.Y);
        ChainCanvas.Children.Add(visual);

        foreach (var child in node.Children)
            RenderNodes(child, longestWaitNode);
    }

    private Border CreateNodeVisual(BlockingChainNode node, bool isLongestWait)
    {
        var border = new Border
        {
            Width = NodeWidth,
            Height = NodeHeight,
            Background = NodeBackgroundBrush,
            BorderBrush = node.IsApex ? ApexBorderBrush : (isLongestWait ? WarningBrush : NodeBorderBrush),
            BorderThickness = new Thickness(node.IsApex || isLongestWait ? 2 : 1),
            CornerRadius = new CornerRadius(4),
            Padding = new Thickness(8, 6, 8, 6),
            Cursor = Cursors.Hand,
            SnapsToDevicePixels = true,
            ClipToBounds = true,
            Tag = node
        };
        border.MouseLeftButtonUp += Node_Click;
        border.ContextMenu = BuildNodeContextMenu(node);

        var stack = new StackPanel();

        // Header: WHO this session is (login) is primary; the SPID is a secondary id. Falls back to the
        // SPID as the primary label when the source data didn't carry a login (e.g. the Dashboard apex).
        var header = new StackPanel { Orientation = Orientation.Horizontal };
        var hasLogin = !string.IsNullOrEmpty(node.LoginName);
        header.Children.Add(new TextBlock
        {
            Text = hasLogin ? node.LoginName : $"SPID {node.Spid}",
            FontSize = 12,
            FontWeight = FontWeights.Bold,
            Foreground = ForegroundBrush,
            TextTrimming = TextTrimming.CharacterEllipsis,
            MaxWidth = NodeWidth - 110,
            VerticalAlignment = VerticalAlignment.Center
        });
        if (hasLogin)
            header.Children.Add(new TextBlock
            {
                Text = $"SPID {node.Spid}",
                FontSize = 10,
                Foreground = MutedBrush,
                Margin = new Thickness(6, 0, 0, 0),
                VerticalAlignment = VerticalAlignment.Center
            });
        if (node.IsApex)
            header.Children.Add(MakeBadge("LEAD BLOCKER", ApexBrush));
        if (node.IsApexSleeping)
            header.Children.Add(MakeBadge("SLEEPING", SleepingBrush));
        stack.Children.Add(header);

        // Host · client app
        var hostApp = JoinNonEmpty(" · ", node.HostName, node.ClientApp);
        if (!string.IsNullOrEmpty(hostApp))
            stack.Children.Add(MutedLine(hostApp));

        // Database
        if (!string.IsNullOrEmpty(node.DatabaseName))
            stack.Children.Add(MutedLine(node.DatabaseName));

        // Contended object — what this victim is waiting on (the apex waits on no one, so has none)
        if (!string.IsNullOrEmpty(node.ContentiousObject))
            stack.Children.Add(MutedLine(node.ContentiousObject));

        // Lock mode + wait (victims only — the apex waits on no one)
        if (!node.IsApex && (node.WaitTimeMs > 0 || !string.IsNullOrEmpty(node.LockMode)))
        {
            var waitText = string.IsNullOrEmpty(node.LockMode)
                ? FormatWait(node.WaitTimeMs)
                : $"{node.LockMode} · {FormatWait(node.WaitTimeMs)}";
            stack.Children.Add(new TextBlock
            {
                Text = waitText,
                FontSize = 11,
                FontWeight = FontWeights.SemiBold,
                Foreground = isLongestWait ? WarningBrush : ForegroundBrush,
                Margin = new Thickness(0, 1, 0, 0)
            });
        }

        // SQL preview (clipped to fit the fixed-height card)
        if (!string.IsNullOrEmpty(node.SqlText))
        {
            stack.Children.Add(new TextBlock
            {
                Text = CollapseWhitespace(node.SqlText),
                FontSize = 10,
                FontFamily = new FontFamily("Consolas"),
                Foreground = MutedBrush,
                TextWrapping = TextWrapping.Wrap,
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxHeight = 30,
                Margin = new Thickness(0, 3, 0, 0)
            });
        }

        border.Child = stack;
        return border;
    }

    private TextBlock MutedLine(string text) => new()
    {
        Text = text,
        FontSize = 11,
        Foreground = MutedBrush,
        TextTrimming = TextTrimming.CharacterEllipsis,
        Margin = new Thickness(0, 1, 0, 0)
    };

    private static Border MakeBadge(string text, Brush brush) => new()
    {
        Background = brush,
        CornerRadius = new CornerRadius(3),
        Padding = new Thickness(4, 0, 4, 0),
        Margin = new Thickness(6, 0, 0, 0),
        VerticalAlignment = VerticalAlignment.Center,
        Child = new TextBlock
        {
            Text = text,
            FontSize = 9,
            FontWeight = FontWeights.Bold,
            Foreground = Brushes.Black
        }
    };

    private void RenderEdges(BlockingChainNode node)
    {
        foreach (var child in node.Children)
        {
            ChainCanvas.Children.Add(CreateElbowConnector(node, child));
            RenderEdges(child);
        }
    }

    // Connector only — no edge label. The child's lock mode + wait already live on its card (with the
    // longest-wait emphasis), and a label here would be painted over by the child card anyway.
    private WpfPath CreateElbowConnector(BlockingChainNode parent, BlockingChainNode child)
    {
        var parentRight = parent.X + NodeWidth;
        var parentCenterY = parent.Y + NodeHeight / 2;
        var childLeft = child.X;
        var childCenterY = child.Y + NodeHeight / 2;
        var midX = (parentRight + childLeft) / 2;

        var geometry = new PathGeometry();
        var figure = new PathFigure { StartPoint = new Point(parentRight, parentCenterY), IsClosed = false };
        figure.Segments.Add(new LineSegment(new Point(midX, parentCenterY), true));
        figure.Segments.Add(new LineSegment(new Point(midX, childCenterY), true));
        figure.Segments.Add(new LineSegment(new Point(childLeft, childCenterY), true));
        geometry.Figures.Add(figure);

        return new WpfPath
        {
            Data = geometry,
            Stroke = EdgeBrush,
            StrokeThickness = 2,
            StrokeLineJoin = PenLineJoin.Round,
            SnapsToDevicePixels = true
        };
    }

    #endregion

    #region Node selection & properties

    private void Node_Click(object sender, MouseButtonEventArgs e)
    {
        if (sender is Border border && border.Tag is BlockingChainNode node)
        {
            SelectNode(border, node);
            e.Handled = true;
        }
    }

    /// <summary>Finds the rendered border for a model node and selects it.</summary>
    private void SelectNodeByModel(BlockingChainNode node)
    {
        foreach (var child in ChainCanvas.Children)
        {
            if (child is Border b && ReferenceEquals(b.Tag, node))
            {
                SelectNode(b, node);
                return;
            }
        }
    }

    private void SelectNode(Border border, BlockingChainNode node)
    {
        if (_selectedNodeBorder != null)
        {
            _selectedNodeBorder.BorderBrush = _selectedNodeOriginalBorder;
            _selectedNodeBorder.BorderThickness = _selectedNodeOriginalThickness;
        }

        _selectedNodeOriginalBorder = border.BorderBrush;
        _selectedNodeOriginalThickness = border.BorderThickness;
        _selectedNodeBorder = border;
        _selectedNode = node;
        // Purple at 3px — distinct hue AND weight from the apex (blue, 2px) and longest-wait (amber, 2px),
        // so a selected victim can never be mistaken for the lead blocker.
        border.BorderBrush = SelectionBrush;
        border.BorderThickness = new Thickness(3);

        ShowProperties(node);
    }

    /// <summary>Scrolls the canvas so a node is comfortably in view (used for the entry node on load).</summary>
    private void ScrollNodeIntoView(BlockingChainNode node)
    {
        // Defer until the ScrollViewer has its extent (first layout pass may not be done yet).
        Dispatcher.BeginInvoke(new Action(() =>
        {
            ChainScrollViewer.ScrollToHorizontalOffset(Math.Max(0, node.X * _zoomLevel - 60));
            ChainScrollViewer.ScrollToVerticalOffset(Math.Max(0, node.Y * _zoomLevel - 60));
        }), System.Windows.Threading.DispatcherPriority.Loaded);
    }

    private ContextMenu BuildNodeContextMenu(BlockingChainNode node)
    {
        var menu = new ContextMenu();

        var propsItem = new MenuItem { Header = "Properties" };
        propsItem.Click += (_, _) => SelectNodeByModel(node);
        menu.Items.Add(propsItem);

        if (!string.IsNullOrEmpty(node.SqlText))
        {
            var copySql = new MenuItem { Header = "Copy SQL Text" };
            copySql.Click += (_, _) => Clipboard.SetDataObject(node.SqlText, false);
            menu.Items.Add(copySql);
        }

        return menu;
    }

    private void ShowProperties(BlockingChainNode node)
    {
        PropertiesContent.Children.Clear();
        PropertiesHeader.Text = $"SPID {node.Spid}{(node.IsApex ? " (lead blocker)" : "")}";

        AddPropRow("SPID", node.Spid.ToString());
        AddPropRow("Role", node.IsApex ? (node.IsApexSleeping ? "Lead blocker (sleeping)" : "Lead blocker") : "Blocked");
        if (!string.IsNullOrEmpty(node.LoginName))
            AddPropRow("Login", node.LoginName);
        if (!string.IsNullOrEmpty(node.HostName))
            AddPropRow("Host", node.HostName);
        if (!string.IsNullOrEmpty(node.ClientApp))
            AddPropRow("Client App", node.ClientApp);
        if (node.TranStarted.HasValue)
            AddPropRow("Transaction Started", node.TranStarted.Value.ToString("yyyy-MM-dd HH:mm:ss"));
        if (!string.IsNullOrEmpty(node.DatabaseName))
            AddPropRow("Database", node.DatabaseName);
        if (!string.IsNullOrEmpty(node.ContentiousObject))
            AddPropRow("Contended Object", node.ContentiousObject);
        if (!node.IsApex)
        {
            if (!string.IsNullOrEmpty(node.LockMode))
                AddPropRow("Lock Mode", node.LockMode);
            AddPropRow("Wait Time", FormatWait(node.WaitTimeMs));
        }
        AddPropRow("Direct Victims", node.Children.Count.ToString());

        if (!string.IsNullOrEmpty(node.SqlText))
        {
            PropertiesContent.Children.Add(new TextBlock
            {
                Text = "SQL Text",
                FontWeight = FontWeights.SemiBold,
                Foreground = MutedBrush,
                FontSize = 11,
                Margin = new Thickness(0, 10, 0, 4)
            });
            PropertiesContent.Children.Add(new TextBox
            {
                Text = node.SqlText,
                IsReadOnly = true,
                FontFamily = new FontFamily("Consolas"),
                FontSize = 11,
                Background = NodeBackgroundBrush,
                Foreground = ForegroundBrush,
                BorderThickness = new Thickness(0),
                TextWrapping = TextWrapping.Wrap,
                MaxHeight = 240,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto
            });
        }

        OpenPropertiesPanel();
    }

    private void AddPropRow(string label, string value)
    {
        var grid = new Grid { Margin = new Thickness(0, 1, 0, 1) };
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(120) });
        grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

        var lbl = new TextBlock { Text = label, Foreground = MutedBrush, FontSize = 11, TextWrapping = TextWrapping.Wrap };
        var val = new TextBlock { Text = value, Foreground = ForegroundBrush, FontSize = 11, FontWeight = FontWeights.SemiBold, TextWrapping = TextWrapping.Wrap };
        Grid.SetColumn(lbl, 0);
        Grid.SetColumn(val, 1);
        grid.Children.Add(lbl);
        grid.Children.Add(val);
        PropertiesContent.Children.Add(grid);
    }

    private void OpenPropertiesPanel()
    {
        PropertiesColumn.Width = new GridLength(340);
        PropertiesSplitter.Visibility = Visibility.Visible;
        PropertiesPanel.Visibility = Visibility.Visible;
    }

    private void CloseProperties_Click(object sender, RoutedEventArgs e)
    {
        PropertiesPanel.Visibility = Visibility.Collapsed;
        PropertiesSplitter.Visibility = Visibility.Collapsed;
        PropertiesColumn.Width = new GridLength(0);
    }

    #endregion

    #region Zoom & pan (mirrors PlanViewerControl.Interaction)

    private void ZoomIn_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel + ZoomStep);
    private void ZoomOut_Click(object sender, RoutedEventArgs e) => SetZoom(_zoomLevel - ZoomStep);

    private void ZoomFit_Click(object sender, RoutedEventArgs e)
    {
        if (ChainCanvas.Width <= 0 || ChainCanvas.Height <= 0) return;
        var viewWidth = ChainScrollViewer.ActualWidth;
        var viewHeight = ChainScrollViewer.ActualHeight;
        if (viewWidth <= 0 || viewHeight <= 0) return;

        var fitZoom = Math.Min(viewWidth / ChainCanvas.Width, viewHeight / ChainCanvas.Height);
        SetZoom(Math.Min(fitZoom, 1.0));
    }

    private void SetZoom(double level)
    {
        _zoomLevel = Math.Max(MinZoom, Math.Min(MaxZoom, level));
        ZoomTransform.ScaleX = _zoomLevel;
        ZoomTransform.ScaleY = _zoomLevel;
        ZoomLevelText.Text = $"{(int)(_zoomLevel * 100)}%";
    }

    private void ChainScrollViewer_PreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (Keyboard.Modifiers == ModifierKeys.Control)
        {
            e.Handled = true;
            SetZoom(_zoomLevel + (e.Delta > 0 ? ZoomStep : -ZoomStep));
        }
    }

    private void BlockingChainControl_PreviewMouseDown(object sender, MouseButtonEventArgs e) => Focus();

    private void ChainScrollViewer_PreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (IsScrollBarAtPoint(e) || IsNodeAtPoint(e)) return;

        _isPanning = true;
        _panStart = e.GetPosition(ChainScrollViewer);
        _panStartOffsetX = ChainScrollViewer.HorizontalOffset;
        _panStartOffsetY = ChainScrollViewer.VerticalOffset;
        ChainScrollViewer.Cursor = Cursors.SizeAll;
        ChainScrollViewer.CaptureMouse();
        e.Handled = true;
    }

    private void ChainScrollViewer_PreviewMouseMove(object sender, MouseEventArgs e)
    {
        if (!_isPanning) return;
        var current = e.GetPosition(ChainScrollViewer);
        ChainScrollViewer.ScrollToHorizontalOffset(Math.Max(0, _panStartOffsetX - (current.X - _panStart.X)));
        ChainScrollViewer.ScrollToVerticalOffset(Math.Max(0, _panStartOffsetY - (current.Y - _panStart.Y)));
        e.Handled = true;
    }

    private void ChainScrollViewer_PreviewMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        if (!_isPanning) return;
        _isPanning = false;
        ChainScrollViewer.Cursor = Cursors.Arrow;
        ChainScrollViewer.ReleaseMouseCapture();
        e.Handled = true;
    }

    private static bool IsScrollBarAtPoint(MouseButtonEventArgs e)
    {
        var source = e.OriginalSource as DependencyObject;
        while (source != null)
        {
            if (source is System.Windows.Controls.Primitives.ScrollBar) return true;
            source = VisualTreeHelper.GetParent(source);
        }
        return false;
    }

    private static bool IsNodeAtPoint(MouseButtonEventArgs e)
    {
        var source = e.OriginalSource as DependencyObject;
        while (source != null)
        {
            if (source is Border b && b.Tag is BlockingChainNode) return true;
            source = VisualTreeHelper.GetParent(source);
        }
        return false;
    }

    #endregion

    #region Helpers

    private static string FormatWait(long ms)
    {
        if (ms <= 0) return "0 ms";
        if (ms < 1000) return $"{ms} ms";
        if (ms < 60_000) return $"{ms / 1000.0:F1} s";
        return $"{ms / 60_000}m {(ms % 60_000) / 1000}s";
    }

    private static string JoinNonEmpty(string separator, params string[] parts) =>
        string.Join(separator, parts.Where(p => !string.IsNullOrEmpty(p)));

    private static string CollapseWhitespace(string s)
    {
        var trimmed = s.Trim();
        var sb = new System.Text.StringBuilder(trimmed.Length);
        var lastWasSpace = false;
        foreach (var c in trimmed)
        {
            if (char.IsWhiteSpace(c))
            {
                if (!lastWasSpace) sb.Append(' ');
                lastWasSpace = true;
            }
            else
            {
                sb.Append(c);
                lastWasSpace = false;
            }
        }
        return sb.ToString();
    }

    private static IEnumerable<BlockingChainNode> EnumerateTree(BlockingChainNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var d in EnumerateTree(child))
                yield return d;
    }

    private static void Walk(BlockingChainNode node, Action<BlockingChainNode> action)
    {
        action(node);
        foreach (var child in node.Children)
            Walk(child, action);
    }

    #endregion
}
