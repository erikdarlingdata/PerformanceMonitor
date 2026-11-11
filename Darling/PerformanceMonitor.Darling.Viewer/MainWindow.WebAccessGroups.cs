/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitor.Darling.Viewer;

public partial class MainWindow
{
    private async Task LoadAdvancedWebAccessAsync()
    {
        try
        {
            if (_dataService is null || !await _dataService.CanManageWebAccessGroupsAsync()) return;
            ShowAdvancedWebAccess(await _dataService.IsAdvancedWebAccessEnabledAsync());
            AdvancedWebAccessCheckBox.Visibility = Visibility.Visible;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("App", "Could not load advanced web access", ex);
        }
    }

    private void ShowAdvancedWebAccess(bool enabled)
    {
        AdvancedWebAccessCheckBox.IsChecked = enabled;
        WebAccessGroupsSidebarButton.Visibility = enabled ? Visibility.Visible : Visibility.Collapsed;
    }

    private async void AdvancedWebAccess_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null) return;
        var enabled = AdvancedWebAccessCheckBox.IsChecked == true;
        AdvancedWebAccessCheckBox.IsEnabled = false;
        try
        {
            await _dataService.SetAdvancedWebAccessEnabledAsync(enabled);
            ShowAdvancedWebAccess(enabled);
        }
        catch (Exception)
        {
            ShowAdvancedWebAccess(!enabled);
            StatusText.Text = "Advanced web access could not be saved. Reconnect and retry.";
        }
        finally { AdvancedWebAccessCheckBox.IsEnabled = !_storeUnavailable; }
    }

    private async void WebAccessGroups_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null) return;
        WebAccessGroupsSidebarButton.IsEnabled = false;
        try
        {
            if (!await _dataService.CanManageWebAccessGroupsAsync())
            {
                StatusText.Text = "Web access groups can only be managed by a Viewer administrator.";
                return;
            }
            if (!await _dataService.IsAdvancedWebAccessEnabledAsync())
            {
                ShowAdvancedWebAccess(false);
                return;
            }
            new WebAccessGroupsWindow(_dataService) { Owner = this }.ShowDialog();
        }
        catch (Exception)
        {
            StatusText.Text = "Web access group permissions could not be checked. Reconnect and retry.";
        }
        finally { WebAccessGroupsSidebarButton.IsEnabled = !_storeUnavailable; }
    }
}
