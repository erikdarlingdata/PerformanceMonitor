/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Controls;

public partial class PlanViewerControl : UserControl
{
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
        _selectedNode = node;
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

    private void PlanViewerControl_PreviewMouseDown(object sender, MouseButtonEventArgs e)
    {
        // Don't steal focus from interactive controls (ComboBox, TextBox, Button, etc.)
        // ComboBox dropdown items live in a separate visual tree (Popup), so also check
        // for ComboBoxItem to avoid stealing focus when selecting dropdown items.
        if (e.OriginalSource is System.Windows.Controls.Primitives.TextBoxBase
            || e.OriginalSource is ComboBox
            || e.OriginalSource is ComboBoxItem
            || FindVisualParent<ComboBox>(e.OriginalSource as DependencyObject) != null
            || FindVisualParent<ComboBoxItem>(e.OriginalSource as DependencyObject) != null
            || FindVisualParent<DataGrid>(e.OriginalSource as DependencyObject) != null)
            return;

        Focus();
    }

    private static T? FindVisualParent<T>(DependencyObject? child) where T : DependencyObject
    {
        while (child != null)
        {
            if (child is T parent) return parent;
            child = VisualTreeHelper.GetParent(child);
        }
        return null;
    }

    private void PlanViewerControl_PreviewKeyDown(object sender, KeyEventArgs e)
    {
        if (e.Key == Key.V && Keyboard.Modifiers == ModifierKeys.Control
            && e.OriginalSource is not TextBox)
        {
            var text = Clipboard.GetText();
            if (!string.IsNullOrWhiteSpace(text))
            {
                e.Handled = true;
                try
                {
                    System.Xml.Linq.XDocument.Parse(text);
                }
                catch (System.Xml.XmlException ex)
                {
                    MessageBox.Show(
                        $"The plan XML is not valid:\n\n{ex.Message}",
                        "Invalid Plan XML",
                        MessageBoxButton.OK,
                        MessageBoxImage.Warning);
                    return;
                }
                LoadPlan(text, "Pasted Plan");
            }
        }
    }

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
}
