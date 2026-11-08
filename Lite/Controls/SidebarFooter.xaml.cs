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
using System.Windows.Controls.Primitives;
using System.Windows.Media;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The sidebar footer (#8169): Add Server, Manage Servers, Investigations and Settings as icon buttons, and a More menu with
/// the other nine actions. The window handles every action through the events below; the control only lays them out.
/// </summary>
public partial class SidebarFooter : UserControl
{
    public SidebarFooter()
    {
        InitializeComponent();
        MoreMenu.CustomPopupPlacementCallback = PlaceMoreMenu;
    }

    public event RoutedEventHandler? AddServerClick;
    public event RoutedEventHandler? ManageServersClick;
    public event RoutedEventHandler? InvestigationsClick;
    public event RoutedEventHandler? SettingsClick;
    public event RoutedEventHandler? AddMultipleServersClick;
    public event RoutedEventHandler? ManageTagsClick;
    public event RoutedEventHandler? ManageQueryTagsClick;
    public event RoutedEventHandler? OpenPlanViewerClick;
    public event RoutedEventHandler? ImportDataClick;
    public event RoutedEventHandler? ImportSettingsClick;
    public event RoutedEventHandler? ViewLogClick;
    public event RoutedEventHandler? OpenLogFolderClick;
    public event RoutedEventHandler? AboutClick;

    /// <summary>The Import Data menu item, which the window disables and relabels while an import runs.</summary>
    internal MenuItem ImportDataMenuItem => ImportDataItem;

    /// <summary>
    /// #8306: the footer in the sidebar's 40 px rail. Compact stacks the five buttons one per row, each the icon alone, centred, with the
    /// button's label as its tooltip (a button with a tooltip of its own keeps it). Not compact restores the 2x2 block with More across the
    /// bottom, icon and label side by side.
    /// </summary>
    internal void SetCompact(bool compact)
    {
        var buttons = new[] { AddServerButton, ManageServersButton, InvestigationsButton, SettingsButton, MoreButton };
        for (var i = 0; i < buttons.Length; i++)
        {
            var button = buttons[i];
            var panel = (StackPanel)button.Content;
            var icon = (TextBlock)panel.Children[0];
            var label = (TextBlock)panel.Children[1];
            label.Visibility = compact ? Visibility.Collapsed : Visibility.Visible;
            icon.Margin = compact ? new Thickness(0) : new Thickness(0, 0, 6, 0);
            button.Padding = compact ? new Thickness(0, 4, 0, 4) : new Thickness(6, 4, 6, 4);
            button.HorizontalContentAlignment = compact ? HorizontalAlignment.Center : HorizontalAlignment.Left;
            if (button.ToolTip is null || button.ToolTip is string tip && tip == label.Text)
            {
                button.ToolTip = compact ? label.Text : null;
            }

            Grid.SetRow(button, compact ? i : i / 2);
            Grid.SetColumn(button, compact ? 0 : i % 2);
            Grid.SetColumnSpan(button, compact || button == MoreButton ? 2 : 1);
        }
    }

    private void More_Click(object sender, RoutedEventArgs e)
    {
        MoreMenu.PlacementTarget = MoreButton;
        MoreMenu.IsOpen = true;
    }

    /// <summary>
    /// Opens the More menu beside its button, where it fits: level with the button's top when the window has room below, else
    /// level with the button's bottom, so it grows upward and stays inside the window. The button sits at the bottom of the
    /// sidebar, and WPF places a popup against the screen, not the window, so a window shorter than the screen bounds nothing on
    /// its own. The sizes arrive in the popup's own units; the button's height in both units gives the scale.
    /// </summary>
    private CustomPopupPlacement[] PlaceMoreMenu(Size popupSize, Size targetSize, Point offset)
    {
        var y = 0.0;
        if (MoreButton.ActualHeight > 0
            && Window.GetWindow(MoreButton) is { } window
            && VisualTreeHelper.GetChildrenCount(window) > 0
            && VisualTreeHelper.GetChild(window, 0) is FrameworkElement client)
        {
            try
            {
                var scale = targetSize.Height / MoreButton.ActualHeight;
                var buttonTop = MoreButton.TransformToAncestor(client).Transform(new Point(0, 0)).Y * scale;
                if (popupSize.Height > client.ActualHeight * scale - buttonTop)
                {
                    y = Math.Max(-buttonTop, targetSize.Height - popupSize.Height);
                }
            }
            catch (InvalidOperationException)
            {
                /* The button is not under the window's client root (a detached control): the menu opens where it would by default. */
            }
        }

        return new[] { new CustomPopupPlacement(new Point(targetSize.Width, y), PopupPrimaryAxis.Vertical) };
    }

    private void AddServer_Click(object sender, RoutedEventArgs e) => AddServerClick?.Invoke(this, e);
    private void ManageServers_Click(object sender, RoutedEventArgs e) => ManageServersClick?.Invoke(this, e);
    private void Investigations_Click(object sender, RoutedEventArgs e) => InvestigationsClick?.Invoke(this, e);
    private void Settings_Click(object sender, RoutedEventArgs e) => SettingsClick?.Invoke(this, e);
    private void AddMultipleServers_Click(object sender, RoutedEventArgs e) => AddMultipleServersClick?.Invoke(this, e);
    private void ManageTags_Click(object sender, RoutedEventArgs e) => ManageTagsClick?.Invoke(this, e);
    private void ManageQueryTags_Click(object sender, RoutedEventArgs e) => ManageQueryTagsClick?.Invoke(this, e);
    private void OpenPlanViewer_Click(object sender, RoutedEventArgs e) => OpenPlanViewerClick?.Invoke(this, e);
    private void ImportData_Click(object sender, RoutedEventArgs e) => ImportDataClick?.Invoke(this, e);
    private void ImportSettings_Click(object sender, RoutedEventArgs e) => ImportSettingsClick?.Invoke(this, e);
    private void ViewLog_Click(object sender, RoutedEventArgs e) => ViewLogClick?.Invoke(this, e);
    private void OpenLogFolder_Click(object sender, RoutedEventArgs e) => OpenLogFolderClick?.Invoke(this, e);
    private void About_Click(object sender, RoutedEventArgs e) => AboutClick?.Invoke(this, e);
}
