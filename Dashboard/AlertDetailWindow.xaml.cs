/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System.Windows;
using PerformanceMonitorDashboard.Controls;

namespace PerformanceMonitorDashboard
{
    public partial class AlertDetailWindow : Window
    {
        public AlertDetailWindow(AlertHistoryDisplayItem item)
        {
            InitializeComponent();

            TimeText.Text = item.TimeLocal;
            ServerText.Text = item.ServerName;
            MetricText.Text = item.MetricName;
            CurrentValueText.Text = item.CurrentValue;
            ThresholdText.Text = item.ThresholdValue;
            NotificationText.Text = item.NotificationType;
            StatusText.Text = item.StatusDisplay;

            if (item.Muted)
                MutedBanner.Visibility = Visibility.Visible;

            if (!string.IsNullOrWhiteSpace(item.DetailText))
            {
                DetailTextBox.Text = item.DetailText;
                DetailPanel.Visibility = Visibility.Visible;
            }
        }
    }
}
