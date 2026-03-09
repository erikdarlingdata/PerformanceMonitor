/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

public partial class AlertDetailWindow : Window
{
    public AlertDetailWindow(AlertHistoryRow item)
    {
        InitializeComponent();

        TimeText.Text = item.TimeLocal;
        ServerText.Text = item.ServerName;
        MetricText.Text = item.MetricName;
        CurrentValueText.Text = item.CurrentValueDisplay;
        ThresholdText.Text = item.ThresholdValueDisplay;
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
