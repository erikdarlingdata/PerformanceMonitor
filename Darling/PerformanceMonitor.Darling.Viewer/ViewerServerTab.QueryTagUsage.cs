/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>#5988 part B: opens "Resource use by tag" for this server, with an All servers choice for the fleet. Read-only.</summary>
    private void ResourceUseByTagButton_Click(object sender, RoutedEventArgs e)
    {
        var here = (_server.ServerId, _server.DisplayName);
        new QueryTagUsageWindow(
            _dataService, here,
            async () => (await _dataService.GetServersAsync()).Where(s => s.IsEnabled).Select(s => (s.ServerId, s.DisplayName)).ToList())
        { Owner = Window.GetWindow(this) }.Show();
    }
}
