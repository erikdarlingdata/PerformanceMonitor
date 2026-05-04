/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        private ServerHealthStatus? _lastKnownStatus;

        /// <summary>
        /// Raised when the user acknowledges a sub-tab alert (Locking, Memory, etc.)
        /// so the sidebar badge can be updated.
        /// </summary>
        public event EventHandler? AlertAcknowledged;

        #region Badge Updates

        /// <summary>
        /// Gets the server ID for this tab.
        /// </summary>
        public string ServerId => _serverConnection.Id;

        /// <summary>
        /// Updates the sub-tab badges based on server health status.
        /// </summary>
        public void UpdateBadges(ServerHealthStatus? status, AlertStateService alertService)
        {
            // Cache latest health status for acknowledge baseline snapshots
            if (status != null)
                _lastKnownStatus = status;

            if (status == null || status.IsOnline != true)
            {
                // Hide all badges when server is offline or no status
                LockingBadge.Visibility = Visibility.Collapsed;
                MemoryBadge.Visibility = Visibility.Collapsed;
                ResourceMetricsBadge.Visibility = Visibility.Collapsed;
                return;
            }

            // Locking badge: blocking or deadlocks
            var showLocking = alertService.ShouldShowBadge(_serverConnection.Id, "Locking", status);
            LockingBadge.Visibility = showLocking ? Visibility.Visible : Visibility.Collapsed;

            // Memory badge: memory pressure
            var showMemory = alertService.ShouldShowBadge(_serverConnection.Id, "Memory", status);
            MemoryBadge.Visibility = showMemory ? Visibility.Visible : Visibility.Collapsed;

            // Resource Metrics badge: high CPU
            var showResourceMetrics = alertService.ShouldShowBadge(_serverConnection.Id, "Resource Metrics", status);
            ResourceMetricsBadge.Visibility = showResourceMetrics ? Visibility.Visible : Visibility.Collapsed;
        }

        /// <summary>
        /// Sets up context menus for sub-tabs that have alert badges.
        /// </summary>
        private void SetupSubTabContextMenus()
        {
            // Add context menus to the tabs with badges
            var tabsWithBadges = new[]
            {
                (Tab: LockingTabItem, Badge: LockingBadge, Name: "Locking"),
                (Tab: MemoryTabItem, Badge: MemoryBadge, Name: "Memory"),
                (Tab: ResourceMetricsTabItem, Badge: ResourceMetricsBadge, Name: "Resource Metrics")
            };

            foreach (var (tab, badge, name) in tabsWithBadges)
            {
                var localBadge = badge; // Capture for closure
                var localName = name;

                var contextMenu = new ContextMenu();

                var acknowledgeItem = new MenuItem
                {
                    Header = "Acknowledge Alert",
                    Tag = name,
                    Icon = new TextBlock { Text = "✓", FontWeight = FontWeights.Bold }
                };
                acknowledgeItem.Click += AcknowledgeSubTabAlert_Click;

                var silenceItem = new MenuItem
                {
                    Header = "Silence This Tab",
                    Tag = name,
                    Icon = new TextBlock { Text = "🔇" }
                };
                silenceItem.Click += SilenceSubTab_Click;

                var unsilenceItem = new MenuItem
                {
                    Header = "Unsilence",
                    Tag = name,
                    Icon = new TextBlock { Text = "🔔" }
                };
                unsilenceItem.Click += UnsilenceSubTab_Click;

                contextMenu.Items.Add(acknowledgeItem);
                contextMenu.Items.Add(silenceItem);
                contextMenu.Items.Add(new Separator());
                contextMenu.Items.Add(unsilenceItem);

                // Update menu items based on silenced state and alert presence when opened
                contextMenu.Opened += (s, args) =>
                {
                    var alertService = GetAlertService();
                    if (alertService != null)
                    {
                        var isSilenced = alertService.IsSubTabSilenced(_serverConnection.Id, localName);
                        var hasAlert = localBadge.Visibility == Visibility.Visible;

                        // Acknowledge only enabled if there's a visible alert
                        acknowledgeItem.IsEnabled = hasAlert;
                        silenceItem.IsEnabled = !isSilenced;
                        unsilenceItem.IsEnabled = isSilenced;
                    }
                };

                // Attach context menu to the TabItem for reliable right-click
                tab.ContextMenu = contextMenu;
            }
        }

        private AlertStateService? GetAlertService()
        {
            var mainWindow = Window.GetWindow(this) as MainWindow;
            return mainWindow?.AlertStateService;
        }

        private void AcknowledgeSubTabAlert_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                if (alertService != null)
                {
                    alertService.AcknowledgeAlert(_serverConnection.Id, tabName, _lastKnownStatus);

                    // Hide the badge immediately
                    var badge = tabName switch
                    {
                        "Locking" => LockingBadge,
                        "Memory" => MemoryBadge,
                        "Resource Metrics" => ResourceMetricsBadge,
                        _ => null
                    };
                    if (badge != null)
                    {
                        badge.Visibility = Visibility.Collapsed;
                    }

                    AlertAcknowledged?.Invoke(this, EventArgs.Empty);
                }
            }
        }

        private void SilenceSubTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                if (alertService != null)
                {
                    alertService.SilenceSubTab(_serverConnection.Id, tabName);

                    // Hide the badge immediately
                    var badge = tabName switch
                    {
                        "Locking" => LockingBadge,
                        "Memory" => MemoryBadge,
                        "Resource Metrics" => ResourceMetricsBadge,
                        _ => null
                    };
                    if (badge != null)
                    {
                        badge.Visibility = Visibility.Collapsed;
                    }
                }
            }
        }

        private void UnsilenceSubTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                alertService?.UnsilenceSubTab(_serverConnection.Id, tabName);
            }
        }

        #endregion
    }
}
