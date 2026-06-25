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
/// Shared deadlock graph viewer: renders the waits-for <b>cycle</b> among processes — process cards laid
/// out on rings (one ring per independent cycle, since a real deadlock is usually a bundle of small
/// cycles) with directional "waits for" arrows labeled by the contended resource. Mirrors
/// PlanViewerControl / BlockingChainControl conventions — Canvas + ScaleTransform zoom/pan,
/// click-to-select properties, theme lifecycle (subscribe in ctor, never unsubscribe on Unloaded, expose
/// <see cref="Cleanup"/> for the host window to call on close). NOT a process|resource bipartite graph.
/// </summary>
public partial class DeadlockGraphControl : UserControl, IGraphViewer
{
    private const double NodeWidth = DeadlockGraphLayout.NodeWidth;
    private const double NodeHeight = DeadlockGraphLayout.NodeHeight;

    private DeadlockGraphModel? _model;
    private double _zoomLevel = 1.0;
    private const double ZoomStep = 0.15;
    private const double MinZoom = 0.1;
    private const double MaxZoom = 3.0;

    // Node selection
    private Border? _selectedNodeBorder;
    private Brush? _selectedNodeOriginalBorder;
    private Thickness _selectedNodeOriginalThickness;
    private DeadlockProcessNode? _selectedNode;

    // Canvas panning
    private bool _isPanning;
    private Point _panStart;
    private double _panStartOffsetX;
    private double _panStartOffsetY;

    // Accent brushes — chosen to read on every theme (same palette family as the other viewers).
    private static readonly SolidColorBrush VictimBadgeBrush = new(Color.FromRgb(0xE5, 0x39, 0x35)); // VICTIM badge fill (red)
    private static readonly SolidColorBrush SelectionBrush = new(Color.FromRgb(0x8E, 0x5B, 0xFF));    // selected node border (purple)
    private static readonly SolidColorBrush ProcBadgeBrush = new(Color.FromRgb(0x4F, 0xA3, 0xFF));    // procedure badge fill (blue)

    // Theme-aware chrome resolved at call time from the app's merged resource dictionaries.
    private Brush NodeBackgroundBrush =>
        (TryFindResource("BackgroundLightBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x2A, 0x2D, 0x35));
    private Brush NodeBorderBrush =>
        (TryFindResource("BorderBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x3A, 0x3D, 0x45));
    private Brush ForegroundBrush =>
        (TryFindResource("ForegroundBrush") as Brush) ?? Brushes.White;
    private Brush MutedBrush =>
        (TryFindResource("ForegroundMutedBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB));
    private Brush CanvasBackgroundBrush =>
        (TryFindResource("BackgroundBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x1E, 0x1E, 0x1E));
    // Victim border + badge accent. Theme-aware so it clears >=3:1 as a border on Light/CoolBreeze.
    private Brush VictimBrush =>
        (TryFindResource("DeadlockVictimBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0xFF, 0x5C, 0x5C));
    // Waits-for arrow. Theme-aware so it reads on every canvas background.
    private Brush EdgeBrush =>
        (TryFindResource("DeadlockEdgeBrush") as Brush) ?? new SolidColorBrush(Color.FromRgb(0x8B, 0x93, 0xA1));

    public DeadlockGraphControl()
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

        if (nodeToRestore != null)
            SelectNodeByModel(nodeToRestore);
    }

    /// <summary>
    /// Loads a parsed model into the viewer. Pure UI work — the caller does the (off-UI-thread) fetch and
    /// parse, then hands the finished <see cref="DeadlockGraphModel"/> here.
    /// </summary>
    public void LoadModel(DeadlockGraphModel model, string? emptyStateDetail = null)
    {
        _model = model ?? throw new ArgumentNullException(nameof(model));

        if (!string.IsNullOrWhiteSpace(emptyStateDetail))
            EmptyStateDetail.Text = emptyStateDetail;

        if (_model.IsEmpty)
        {
            SummaryText.Text = string.Empty;
            ParallelBanner.Visibility = Visibility.Collapsed;
            ShowEmptyState(true);
            return;
        }

        ShowEmptyState(false);
        Render();        // assigns _model.ComponentBoxes, which BuildSummary's cycle count reads
        BuildSummary();

        // Open with the most informative node selected so the viewer lands with the details card showing
        // (mirrors the block-chain viewer, which auto-selects the clicked session). Prefer a victim — the
        // rolled-back process is the one you most want to inspect — else the first process.
        var initial = _model.Processes.FirstOrDefault(p => p.IsVictim) ?? _model.Processes.FirstOrDefault();
        if (initial != null)
            SelectNodeByModel(initial);
    }

    private void ShowEmptyState(bool empty)
    {
        EmptyState.Visibility = empty ? Visibility.Visible : Visibility.Collapsed;
        GraphScrollViewer.Visibility = empty ? Visibility.Collapsed : Visibility.Visible;
    }

    private void BuildSummary()
    {
        if (_model == null) return;

        var victims = _model.Processes.Count(p => p.IsVictim);
        var procs = _model.Processes.Count;
        var summary = $"{procs} process{(procs == 1 ? "" : "es")} · {victims} victim{(victims == 1 ? "" : "s")}";
        // A single <deadlock> can batch several independent cycles that share no resource — call the count out
        // so the separate boxes don't read as "two unrelated deadlocks shown together by mistake."
        var cycles = _model.ComponentBoxes.Count(b => b.NodeCount >= 2);
        if (cycles > 1)
            summary += $" · {cycles} independent cycles";
        // Shown only when the parsed XML carried an event wrapper with a timestamp (e.g. raw collect XML
        // or the Azure report); the bare <deadlock> the apps store has none, so this stays off there.
        if (_model.EventTime.HasValue)
            summary += $" · {_model.EventTime.Value:yyyy-MM-dd HH:mm:ss} UTC";
        SummaryText.Text = summary;

        if (_model.IsParallel)
        {
            ParallelBanner.Visibility = Visibility.Visible;
            ParallelBanner.Text = "⚠ parallel (intra-query) deadlock";
        }
        else
        {
            ParallelBanner.Visibility = Visibility.Collapsed;
            ParallelBanner.Text = "";
        }
    }

    #region Rendering

    private void Render()
    {
        if (_model == null) return;

        GraphCanvas.Children.Clear();
        _selectedNodeBorder = null;

        if (_model.Processes.Count == 0) return;

        GraphScrollViewer.ScrollToHome();

        var (width, height) = DeadlockGraphLayout.Layout(_model);
        GraphCanvas.Width = width;
        GraphCanvas.Height = height;

        // Cycle frames first, so they sit behind the edges and cards as a grouping backdrop.
        RenderCycleBoxes();

        var byId = _model.Processes.ToDictionary(p => p.Id, StringComparer.Ordinal);

        // Edges (under the cards), then cards, then edge labels on top so they stay legible.
        var labels = new List<(Point Pos, string Text, string Tooltip)>();
        foreach (var edge in _model.Edges)
            RenderEdge(edge, byId, labels);

        foreach (var node in _model.Processes)
            RenderNode(node);

        foreach (var (pos, text, tooltip) in labels)
            RenderEdgeLabel(pos, text, tooltip);
    }

    /// <summary>
    /// Draws a dashed, labelled frame around each independent cycle when one deadlock batched more than one
    /// (the cycles share no resource — see <see cref="DeadlockGraphLayout"/>). A lone cycle needs no frame.
    /// </summary>
    private void RenderCycleBoxes()
    {
        // A "cycle" is a component with >= 2 processes; a lone node (only a parallel self-edge) is not one and
        // the parallel banner already explains it. Frame the cycles only when one deadlock holds more than one.
        var cycles = _model!.ComponentBoxes.Where(b => b.NodeCount >= 2).ToList();
        if (cycles.Count <= 1) return;

        const double pad = 22; // inflate past the cards and leave a strip at the top for the label

        int number = 0;
        foreach (var box in cycles)
        {
            number++;
            var frame = new System.Windows.Shapes.Rectangle
            {
                Width = box.Width + pad * 2,
                Height = box.Height + pad * 2,
                RadiusX = 8,
                RadiusY = 8,
                Stroke = MutedBrush,
                StrokeThickness = 1,
                StrokeDashArray = new DoubleCollection { 4, 3 },
                Fill = Brushes.Transparent,
                IsHitTestVisible = false,
                SnapsToDevicePixels = true
            };
            Canvas.SetLeft(frame, box.X - pad);
            Canvas.SetTop(frame, box.Y - pad);
            GraphCanvas.Children.Add(frame);

            var label = new TextBlock
            {
                Text = $"Cycle {number}",
                FontSize = 11,
                FontWeight = FontWeights.SemiBold,
                Foreground = MutedBrush,
                IsHitTestVisible = false
            };
            Canvas.SetLeft(label, box.X - pad + 6);
            Canvas.SetTop(label, box.Y - pad + 3);
            GraphCanvas.Children.Add(label);
        }
    }

    private void RenderNode(DeadlockProcessNode node)
    {
        var visual = CreateNodeVisual(node);
        Canvas.SetLeft(visual, node.X);
        Canvas.SetTop(visual, node.Y);
        GraphCanvas.Children.Add(visual);
    }

    private Border CreateNodeVisual(DeadlockProcessNode node)
    {
        var border = new Border
        {
            Width = NodeWidth,
            Height = NodeHeight,
            Background = NodeBackgroundBrush,
            BorderBrush = node.IsVictim ? VictimBrush : NodeBorderBrush,
            BorderThickness = new Thickness(node.IsVictim ? 2.5 : 1),
            CornerRadius = new CornerRadius(4),
            Padding = new Thickness(8, 6, 8, 6),
            Cursor = Cursors.Hand,
            SnapsToDevicePixels = true,
            ClipToBounds = true,
            Tag = node,
            ToolTip = BuildNodeTooltip(node)
        };
        border.MouseLeftButtonUp += Node_Click;
        border.ContextMenu = BuildNodeContextMenu(node);

        var stack = new StackPanel();

        // Header: SPID (+ ecid for parallel threads) + VICTIM badge
        var header = new StackPanel { Orientation = Orientation.Horizontal };
        header.Children.Add(new TextBlock
        {
            Text = node.Ecid > 0 ? $"SPID {node.Spid} · ecid {node.Ecid}" : $"SPID {node.Spid}",
            FontSize = 13,
            FontWeight = FontWeights.Bold,
            Foreground = ForegroundBrush,
            VerticalAlignment = VerticalAlignment.Center
        });
        if (node.IsVictim)
            header.Children.Add(MakeBadge("VICTIM", VictimBadgeBrush, Brushes.White));
        stack.Children.Add(header);

        // Procedure (the context that differentiates processes in a single-app workload)
        if (!string.IsNullOrEmpty(node.ProcName))
        {
            stack.Children.Add(new TextBlock
            {
                Text = node.ProcName,
                FontSize = 11,
                Foreground = MutedBrush,
                TextTrimming = TextTrimming.CharacterEllipsis,
                Margin = new Thickness(0, 1, 0, 0)
            });
        }

        // Contended resource — the object SQL Server resolved for the lock this process waits on. The deadlock
        // graph names it directly, so this is the at-a-glance answer to "what did they fight over?".
        if (!string.IsNullOrEmpty(node.ContentiousObject))
        {
            stack.Children.Add(new TextBlock
            {
                Text = node.ContentiousObject,
                FontSize = 11,
                Foreground = ForegroundBrush,
                TextTrimming = TextTrimming.CharacterEllipsis,
                Margin = new Thickness(0, 1, 0, 0)
            });
        }

        // Requested lock mode + wait time
        var lockText = BuildLockWaitText(node);
        if (!string.IsNullOrEmpty(lockText))
        {
            stack.Children.Add(new TextBlock
            {
                Text = lockText,
                FontSize = 11,
                FontWeight = FontWeights.SemiBold,
                Foreground = ForegroundBrush,
                Margin = new Thickness(0, 1, 0, 0)
            });
        }

        // SQL preview (clipped). The victim's red border already flags the rolled-back process — no
        // strikethrough on the text (it hurt readability for no added signal).
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
                MaxHeight = 46,
                Margin = new Thickness(0, 3, 0, 0)
            });
        }

        border.Child = stack;
        return border;
    }

    private string BuildLockWaitText(DeadlockProcessNode node)
    {
        var wait = node.WaitTimeMs > 0 ? FormatWait(node.WaitTimeMs) : "";
        if (!string.IsNullOrEmpty(node.LockMode) && !string.IsNullOrEmpty(wait))
            return $"requests {node.LockMode} · waited {wait}";
        if (!string.IsNullOrEmpty(node.LockMode))
            return $"requests {node.LockMode}";
        if (!string.IsNullOrEmpty(wait))
            return $"waited {wait}";
        return "";
    }

    private string BuildNodeTooltip(DeadlockProcessNode node)
    {
        var lines = new List<string>();
        if (!string.IsNullOrEmpty(node.DatabaseName)) lines.Add($"Database: {node.DatabaseName}");
        if (!string.IsNullOrEmpty(node.LoginName)) lines.Add($"Login: {node.LoginName}");
        if (!string.IsNullOrEmpty(node.HostName)) lines.Add($"Host: {node.HostName}");
        if (!string.IsNullOrEmpty(node.ClientApp)) lines.Add($"App: {node.ClientApp}");
        if (!string.IsNullOrEmpty(node.IsolationLevel)) lines.Add($"Isolation: {node.IsolationLevel}");
        if (!string.IsNullOrEmpty(node.ContentiousObject)) lines.Add($"Contended: {node.ContentiousObject}");
        if (!string.IsNullOrEmpty(node.WaitResource)) lines.Add($"Wait resource: {node.WaitResource}");
        lines.Add("Click for full details");
        return string.Join("\n", lines);
    }

    private static Border MakeBadge(string text, Brush fill, Brush fg) => new()
    {
        Background = fill,
        CornerRadius = new CornerRadius(3),
        Padding = new Thickness(4, 0, 4, 0),
        Margin = new Thickness(6, 0, 0, 0),
        VerticalAlignment = VerticalAlignment.Center,
        Child = new TextBlock
        {
            Text = text,
            FontSize = 9,
            FontWeight = FontWeights.Bold,
            Foreground = fg
        }
    };

    private void RenderEdge(
        DeadlockWaitEdge edge,
        IReadOnlyDictionary<string, DeadlockProcessNode> byId,
        List<(Point, string, string)> labels)
    {
        if (!byId.TryGetValue(edge.WaiterProcessId, out var waiter) ||
            !byId.TryGetValue(edge.OwnerProcessId, out var owner))
            return;

        var tooltip = BuildEdgeTooltip(edge);
        var shortText = BuildShortEdgeLabel(edge);

        if (edge.IsSelfEdge)
        {
            RenderSelfLoop(waiter);
            labels.Add((new Point(waiter.X + NodeWidth * 0.5, waiter.Y - 30), shortText, tooltip));
            return;
        }

        var wc = Center(waiter);
        var oc = Center(owner);

        var startPt = ClipToRect(wc, oc);
        var endPt = ClipToRect(oc, wc);

        // Bow the curve to the right of travel so the two arrows of a 2-cycle separate (A->B and B->A
        // travel in opposite directions, so "right of travel" puts them on opposite sides).
        var dir = Normalize(oc - wc);
        var perp = new Vector(-dir.Y, dir.X);
        var chordMid = new Point((startPt.X + endPt.X) / 2, (startPt.Y + endPt.Y) / 2);
        const double bow = 38;
        var control = chordMid + perp * bow;

        var geometry = new PathGeometry();
        var figure = new PathFigure { StartPoint = startPt, IsClosed = false };
        figure.Segments.Add(new QuadraticBezierSegment(control, endPt, true));
        geometry.Figures.Add(figure);

        GraphCanvas.Children.Add(new WpfPath
        {
            Data = geometry,
            Stroke = EdgeBrush,
            StrokeThickness = 2,
            StrokeLineJoin = PenLineJoin.Round,
            SnapsToDevicePixels = true,
            IsHitTestVisible = false
        });

        // Arrowhead at the owner end, aligned to the curve's tangent there.
        AddArrowhead(endPt, Normalize(endPt - control));

        // Label near the curve apex (quadratic point at t=0.5).
        var apex = new Point(
            0.25 * startPt.X + 0.5 * control.X + 0.25 * endPt.X,
            0.25 * startPt.Y + 0.5 * control.Y + 0.25 * endPt.Y);
        labels.Add((apex, shortText, tooltip));
    }

    private void RenderSelfLoop(DeadlockProcessNode node)
    {
        // A small loop above the top edge, with an arrowhead pointing back down into the card.
        double cx = node.X + NodeWidth * 0.5;
        double topY = node.Y;
        const double rx = 20, ry = 16;
        double loopCy = topY - ry;

        var loop = new Ellipse
        {
            Width = rx * 2,
            Height = ry * 2,
            Stroke = EdgeBrush,
            StrokeThickness = 2,
            Fill = Brushes.Transparent,
            IsHitTestVisible = false
        };
        Canvas.SetLeft(loop, cx - rx);
        Canvas.SetTop(loop, loopCy - ry);
        GraphCanvas.Children.Add(loop);

        // Arrowhead at the bottom of the loop, pointing down into the card.
        AddArrowhead(new Point(cx + 6, topY), new Vector(0, 1));
    }

    private void AddArrowhead(Point tip, Vector dir)
    {
        if (dir.Length < 1e-6) dir = new Vector(1, 0);
        dir.Normalize();
        var perp = new Vector(-dir.Y, dir.X);
        const double len = 12, half = 6;
        var basePt = tip - dir * len;
        var p1 = basePt + perp * half;
        var p2 = basePt - perp * half;

        GraphCanvas.Children.Add(new Polygon
        {
            Points = new PointCollection { tip, p1, p2 },
            Fill = EdgeBrush,
            IsHitTestVisible = false
        });
    }

    private void RenderEdgeLabel(Point pos, string text, string tooltip)
    {
        if (string.IsNullOrEmpty(text)) return;

        var label = new Border
        {
            Background = CanvasBackgroundBrush,
            BorderBrush = NodeBorderBrush,
            BorderThickness = new Thickness(1),
            CornerRadius = new CornerRadius(3),
            Padding = new Thickness(4, 1, 4, 1),
            ToolTip = tooltip,
            Child = new TextBlock
            {
                Text = text,
                FontSize = 10,
                Foreground = MutedBrush
            }
        };

        // Center the label on the apex point.
        label.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        Canvas.SetLeft(label, pos.X - label.DesiredSize.Width / 2);
        Canvas.SetTop(label, pos.Y - label.DesiredSize.Height / 2);
        GraphCanvas.Children.Add(label);
    }

    private static Point Center(DeadlockProcessNode n) =>
        new(n.X + NodeWidth / 2, n.Y + NodeHeight / 2);

    /// <summary>Intersection of the ray from <paramref name="center"/> toward <paramref name="toward"/>
    /// with the card rectangle centered on <paramref name="center"/>.</summary>
    private static Point ClipToRect(Point center, Point toward)
    {
        double dx = toward.X - center.X;
        double dy = toward.Y - center.Y;
        if (Math.Abs(dx) < 1e-6 && Math.Abs(dy) < 1e-6) return center;

        const double halfW = NodeWidth / 2, halfH = NodeHeight / 2;
        double tx = Math.Abs(dx) > 1e-6 ? halfW / Math.Abs(dx) : double.PositiveInfinity;
        double ty = Math.Abs(dy) > 1e-6 ? halfH / Math.Abs(dy) : double.PositiveInfinity;
        double t = Math.Min(tx, ty);
        return new Point(center.X + dx * t, center.Y + dy * t);
    }

    private static Vector Normalize(Vector v)
    {
        if (v.Length < 1e-6) return new Vector(1, 0);
        v.Normalize();
        return v;
    }

    private string BuildShortEdgeLabel(DeadlockWaitEdge edge)
    {
        // Trim the long "PAGE schema.db.table" to the table name for the on-canvas label; the full label
        // is in the tooltip. Append the requested mode when present.
        var label = edge.ResourceLabel;
        var spaceIdx = label.IndexOf(' ');
        if (spaceIdx > 0 && spaceIdx < label.Length - 1)
        {
            var rest = label[(spaceIdx + 1)..];
            var lastDot = rest.LastIndexOf('.');
            if (lastDot >= 0 && lastDot < rest.Length - 1)
                label = rest[(lastDot + 1)..];
        }
        return string.IsNullOrEmpty(edge.RequestMode) ? label : $"{label} · {edge.RequestMode}";
    }

    private static string BuildEdgeTooltip(DeadlockWaitEdge edge)
    {
        var lines = new List<string> { edge.ResourceLabel };
        if (!string.IsNullOrEmpty(edge.RequestMode))
            lines.Add($"Waiter requests: {edge.RequestMode}");
        if (!string.IsNullOrEmpty(edge.OwnerMode))
            lines.Add($"Owner holds: {edge.OwnerMode}");
        return string.Join("\n", lines);
    }

    #endregion

    #region Node selection & properties

    private void Node_Click(object sender, MouseButtonEventArgs e)
    {
        if (sender is Border border && border.Tag is DeadlockProcessNode node)
        {
            SelectNode(border, node);
            e.Handled = true;
        }
    }

    /// <summary>Finds the rendered border for a model node and selects it.</summary>
    private void SelectNodeByModel(DeadlockProcessNode node)
    {
        foreach (var child in GraphCanvas.Children)
        {
            if (child is Border b && ReferenceEquals(b.Tag, node))
            {
                SelectNode(b, node);
                return;
            }
        }
    }

    private void SelectNode(Border border, DeadlockProcessNode node)
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
        // Purple at 3px — a distinct hue AND weight from the victim red border so a selected victim still
        // reads as a victim.
        border.BorderBrush = SelectionBrush;
        border.BorderThickness = new Thickness(3);

        ShowProperties(node);
    }

    private ContextMenu BuildNodeContextMenu(DeadlockProcessNode node)
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

    private void ShowProperties(DeadlockProcessNode node)
    {
        PropertiesContent.Children.Clear();
        PropertiesHeader.Text = $"SPID {node.Spid}{(node.IsVictim ? " (victim)" : "")}";

        AddPropRow("SPID", node.Spid.ToString());
        if (node.Ecid > 0) AddPropRow("ECID", node.Ecid.ToString());
        AddPropRow("Role", node.IsVictim ? "Victim (rolled back)" : "Survivor");
        if (!string.IsNullOrEmpty(node.DatabaseName)) AddPropRow("Database", node.DatabaseName);
        if (!string.IsNullOrEmpty(node.ProcName)) AddPropRow("Procedure", node.ProcName);
        if (!string.IsNullOrEmpty(node.LockMode)) AddPropRow("Requested Lock", node.LockMode);
        if (node.WaitTimeMs > 0) AddPropRow("Wait Time", FormatWait(node.WaitTimeMs));
        if (!string.IsNullOrEmpty(node.ContentiousObject)) AddPropRow("Contended Object", node.ContentiousObject);
        if (!string.IsNullOrEmpty(node.WaitResource)) AddPropRow("Wait Resource", node.WaitResource);
        AddPropRow("Priority", node.Priority.ToString());
        if (!string.IsNullOrEmpty(node.Status)) AddPropRow("Status", node.Status);
        if (!string.IsNullOrEmpty(node.IsolationLevel)) AddPropRow("Isolation", node.IsolationLevel);
        if (!string.IsNullOrEmpty(node.LoginName)) AddPropRow("Login", node.LoginName);
        if (!string.IsNullOrEmpty(node.HostName)) AddPropRow("Host", node.HostName);
        if (!string.IsNullOrEmpty(node.ClientApp)) AddPropRow("Application", node.ClientApp);

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
        // Clear the selection highlight too, so a closed panel stays closed — including across a theme
        // change, where OnThemeChanged only re-opens the panel when a node is still selected.
        if (_selectedNodeBorder != null)
        {
            _selectedNodeBorder.BorderBrush = _selectedNodeOriginalBorder;
            _selectedNodeBorder.BorderThickness = _selectedNodeOriginalThickness;
            _selectedNodeBorder = null;
        }
        _selectedNode = null;

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
        if (GraphCanvas.Width <= 0 || GraphCanvas.Height <= 0) return;
        var viewWidth = GraphScrollViewer.ActualWidth;
        var viewHeight = GraphScrollViewer.ActualHeight;
        if (viewWidth <= 0 || viewHeight <= 0) return;

        var fitZoom = Math.Min(viewWidth / GraphCanvas.Width, viewHeight / GraphCanvas.Height);
        SetZoom(Math.Min(fitZoom, 1.0));
    }

    private void SetZoom(double level)
    {
        _zoomLevel = Math.Max(MinZoom, Math.Min(MaxZoom, level));
        ZoomTransform.ScaleX = _zoomLevel;
        ZoomTransform.ScaleY = _zoomLevel;
        ZoomLevelText.Text = $"{(int)(_zoomLevel * 100)}%";
    }

    private void GraphScrollViewer_PreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        if (Keyboard.Modifiers == ModifierKeys.Control)
        {
            e.Handled = true;
            SetZoom(_zoomLevel + (e.Delta > 0 ? ZoomStep : -ZoomStep));
        }
    }

    private void DeadlockGraphControl_PreviewMouseDown(object sender, MouseButtonEventArgs e)
    {
        Focus();
    }

    private void GraphScrollViewer_PreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        if (IsScrollBarAtPoint(e) || IsNodeAtPoint(e)) return;

        _isPanning = true;
        _panStart = e.GetPosition(GraphScrollViewer);
        _panStartOffsetX = GraphScrollViewer.HorizontalOffset;
        _panStartOffsetY = GraphScrollViewer.VerticalOffset;
        GraphScrollViewer.Cursor = Cursors.SizeAll;
        GraphScrollViewer.CaptureMouse();
        e.Handled = true;
    }

    private void GraphScrollViewer_PreviewMouseMove(object sender, MouseEventArgs e)
    {
        if (!_isPanning) return;
        var current = e.GetPosition(GraphScrollViewer);
        GraphScrollViewer.ScrollToHorizontalOffset(Math.Max(0, _panStartOffsetX - (current.X - _panStart.X)));
        GraphScrollViewer.ScrollToVerticalOffset(Math.Max(0, _panStartOffsetY - (current.Y - _panStart.Y)));
        e.Handled = true;
    }

    private void GraphScrollViewer_PreviewMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        if (!_isPanning) return;
        _isPanning = false;
        GraphScrollViewer.Cursor = Cursors.Arrow;
        GraphScrollViewer.ReleaseMouseCapture();
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
            if (source is Border b && b.Tag is DeadlockProcessNode) return true;
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

    #endregion
}
