/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;

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

    private void More_Click(object sender, RoutedEventArgs e)
    {
        MoreMenu.PlacementTarget = MoreButton;
        MoreMenu.IsOpen = true;
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
