/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class ServerHealthCard : UserControl
    {
        // Severity colors
        private static readonly SolidColorBrush HealthyBrush = new(Color.FromRgb(0x22, 0xC5, 0x5E));   // Green
        private static readonly SolidColorBrush WarningBrush = new(Color.FromRgb(0xF5, 0x9E, 0x0B));   // Amber
        private static readonly SolidColorBrush CriticalBrush = new(Color.FromRgb(0xEF, 0x44, 0x44));  // Red
        private static readonly SolidColorBrush UnknownBrush = new(Color.FromRgb(0x88, 0x88, 0x88));   // Gray

        public event EventHandler<ServerHealthStatus>? CardClicked;
        public event EventHandler<ServerHealthStatus>? EditServerRequested;
        public event EventHandler<ServerHealthStatus>? CheckVersionRequested;

        public ServerHealthCard()
        {
            InitializeComponent();
            DataContextChanged += OnDataContextChanged;
        }

        private void OnDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (e.OldValue is ServerHealthStatus oldStatus)
            {
                oldStatus.PropertyChanged -= OnStatusPropertyChanged;
            }

            if (e.NewValue is ServerHealthStatus newStatus)
            {
                newStatus.PropertyChanged += OnStatusPropertyChanged;
                UpdateAllIndicators(newStatus);
            }
        }

        private void OnStatusPropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (sender is not ServerHealthStatus status) return;

            // Use CheckAccess to avoid deadlocks - if we're on the UI thread, execute directly
            void UpdateUI()
            {
                switch (e.PropertyName)
                {
                    case nameof(ServerHealthStatus.CpuSeverity):
                        UpdateIndicator(CpuIndicator, status.CpuSeverity);
                        break;
                    case nameof(ServerHealthStatus.MemorySeverity):
                        UpdateIndicator(MemoryIndicator, status.MemorySeverity);
                        break;
                    case nameof(ServerHealthStatus.BlockingSeverity):
                        UpdateIndicator(BlockingIndicator, status.BlockingSeverity);
                        break;
                    case nameof(ServerHealthStatus.ThreadsSeverity):
                        UpdateIndicator(ThreadsIndicator, status.ThreadsSeverity);
                        break;
                    case nameof(ServerHealthStatus.DeadlockSeverity):
                        UpdateIndicator(DeadlockIndicator, status.DeadlockSeverity);
                        break;
                    case nameof(ServerHealthStatus.CollectorSeverity):
                        UpdateIndicator(CollectorIndicator, status.CollectorSeverity);
                        break;
                    case nameof(ServerHealthStatus.OverallSeverity):
                        UpdateIndicator(OverallStatusIndicator, status.OverallSeverity);
                        UpdateBorderColor(status.OverallSeverity);
                        break;
                    case nameof(ServerHealthStatus.IsOnline):
                        UpdateOfflineOverlay(status.IsOnline);
                        UpdateAllIndicators(status);
                        break;
                    case nameof(ServerHealthStatus.IsLoading):
                        // Handled by XAML DataTrigger
                        break;
                }
            }

            if (Dispatcher.CheckAccess())
            {
                UpdateUI();
            }
            else
            {
                Dispatcher.BeginInvoke(UpdateUI);
            }
        }

        private void UpdateAllIndicators(ServerHealthStatus status)
        {
            UpdateIndicator(CpuIndicator, status.CpuSeverity);
            UpdateIndicator(MemoryIndicator, status.MemorySeverity);
            UpdateIndicator(BlockingIndicator, status.BlockingSeverity);
            UpdateIndicator(ThreadsIndicator, status.ThreadsSeverity);
            UpdateIndicator(DeadlockIndicator, status.DeadlockSeverity);
            UpdateIndicator(CollectorIndicator, status.CollectorSeverity);
            UpdateIndicator(OverallStatusIndicator, status.OverallSeverity);
            UpdateBorderColor(status.OverallSeverity);
            UpdateOfflineOverlay(status.IsOnline);
        }

        private void UpdateIndicator(System.Windows.Shapes.Ellipse indicator, HealthSeverity severity)
        {
            indicator.Fill = GetBrushForSeverity(severity);
        }

        private void UpdateBorderColor(HealthSeverity severity)
        {
            // Only show colored border for critical/warning states
            if (severity == HealthSeverity.Critical)
            {
                CardBorder.BorderBrush = CriticalBrush;
                CardBorder.BorderThickness = new Thickness(2);
            }
            else if (severity == HealthSeverity.Warning)
            {
                CardBorder.BorderBrush = WarningBrush;
                CardBorder.BorderThickness = new Thickness(2);
            }
            else
            {
                CardBorder.BorderBrush = (Brush)FindResource("BorderBrush");
                CardBorder.BorderThickness = new Thickness(1);
            }
        }

        private void UpdateOfflineOverlay(bool? isOnline)
        {
            if (isOnline == false)
            {
                OfflineOverlay.Visibility = Visibility.Visible;
                MetricsGrid.Opacity = 0.3;
            }
            else
            {
                OfflineOverlay.Visibility = Visibility.Collapsed;
                MetricsGrid.Opacity = 1.0;
            }
        }

        private static SolidColorBrush GetBrushForSeverity(HealthSeverity severity)
        {
            return severity switch
            {
                HealthSeverity.Healthy => HealthyBrush,
                HealthSeverity.Warning => WarningBrush,
                HealthSeverity.Critical => CriticalBrush,
                _ => UnknownBrush
            };
        }

        private void Card_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            if (DataContext is ServerHealthStatus status)
            {
                CardClicked?.Invoke(this, status);
            }
        }

        private void OpenInNewTab_Click(object sender, RoutedEventArgs e)
        {
            if (DataContext is ServerHealthStatus status)
                CardClicked?.Invoke(this, status);
        }

        private void EditServer_Click(object sender, RoutedEventArgs e)
        {
            if (DataContext is ServerHealthStatus status)
                EditServerRequested?.Invoke(this, status);
        }

        private void CheckVersion_Click(object sender, RoutedEventArgs e)
        {
            if (DataContext is ServerHealthStatus status)
                CheckVersionRequested?.Invoke(this, status);
        }
    }
}
