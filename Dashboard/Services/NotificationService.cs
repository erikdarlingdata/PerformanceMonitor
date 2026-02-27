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
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitorDashboard.Interfaces;

namespace PerformanceMonitorDashboard.Services
{
    public class NotificationService : IDisposable
    {
        private TaskbarIcon? _trayIcon;
        private readonly Window _mainWindow;
        private readonly IUserPreferencesService _preferencesService;
        private bool _disposed;

        public NotificationService(Window mainWindow, IUserPreferencesService? preferencesService = null)
        {
            _mainWindow = mainWindow;
            _preferencesService = preferencesService ?? new UserPreferencesService();
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
        }

        public void Initialize()
        {
            // Dispose any existing icon first
            if (_trayIcon != null)
            {
                _trayIcon.Visibility = Visibility.Collapsed;
                _trayIcon.Dispose();
                _trayIcon = null;
            }

            _trayIcon = new TaskbarIcon();

            bool HasLightBackground = Helpers.ThemeManager.HasLightBackground;

            /* Custom tooltip styled to match current theme */
            _trayIcon.TrayToolTip = new Border
            {
                Background = new SolidColorBrush(HasLightBackground
                    ? (Color)ColorConverter.ConvertFromString("#FFFFFF")
                    : (Color)ColorConverter.ConvertFromString("#22252b")),
                BorderBrush = new SolidColorBrush(HasLightBackground
                    ? (Color)ColorConverter.ConvertFromString("#DEE2E6")
                    : (Color)ColorConverter.ConvertFromString("#33363e")),
                BorderThickness = new Thickness(1),
                Padding = new Thickness(10, 8, 10, 8),
                CornerRadius = new CornerRadius(4),
                Child = new TextBlock
                {
                    Text = "SQL Server Performance Monitor",
                    Foreground = new SolidColorBrush(HasLightBackground
                        ? (Color)ColorConverter.ConvertFromString("#1A1D23")
                        : (Color)ColorConverter.ConvertFromString("#E4E6EB")),
                    FontSize = 12
                }
            };

            // Load icon from embedded resource using pack URI
            try
            {
                var iconUri = new Uri("pack://application:,,,/EDD.ico", UriKind.Absolute);
                _trayIcon.IconSource = new BitmapImage(iconUri);
            }
            catch
            {
                // Icon loading failed, tray icon will be blank but functional
            }

            var contextMenu = new ContextMenu();

            var showItem = new MenuItem
            {
                Header = "Show Dashboard",
                Icon = new TextBlock { Text = "📊", Background = Brushes.Transparent }
            };
            showItem.Click += (s, e) => ShowMainWindow();

            var settingsItem = new MenuItem
            {
                Header = "Settings...",
                Icon = new TextBlock { Text = "⚙", Background = Brushes.Transparent }
            };
            settingsItem.Click += (s, e) => OpenSettings();

            var separatorItem = new Separator();

            var exitItem = new MenuItem
            {
                Header = "Exit",
                Icon = new TextBlock { Text = "✕", Background = Brushes.Transparent }
            };
            exitItem.Click += (s, e) => ExitApplication();

            contextMenu.Items.Add(showItem);
            contextMenu.Items.Add(settingsItem);
            contextMenu.Items.Add(separatorItem);
            contextMenu.Items.Add(exitItem);

            _trayIcon.ContextMenu = contextMenu;

            // Double-click to show window
            _trayIcon.TrayMouseDoubleClick += (s, e) => ShowMainWindow();
        }

        public void ShowNotification(string title, string message, NotificationType type = NotificationType.Info)
        {
            if (_trayIcon == null) return;

            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotificationsEnabled) return;

            var icon = type switch
            {
                NotificationType.Error => BalloonIcon.Error,
                NotificationType.Warning => BalloonIcon.Warning,
                NotificationType.Success => BalloonIcon.Info,
                _ => BalloonIcon.Info
            };

            // Ensure we're on the UI thread for WPF operations
            if (_mainWindow.Dispatcher.CheckAccess())
            {
                _trayIcon?.ShowBalloonTip(title, message, icon);
            }
            else
            {
                _mainWindow.Dispatcher.Invoke(() => _trayIcon?.ShowBalloonTip(title, message, icon));
            }
        }

        public void ShowServerOnlineNotification(string serverName)
        {
            ShowNotification(
                "Server Online",
                $"{serverName} is now responding",
                NotificationType.Success);
        }

        public void ShowServerOfflineNotification(string serverName, string? errorMessage = null)
        {
            var message = string.IsNullOrEmpty(errorMessage)
                ? $"{serverName} is not responding"
                : $"{serverName}: {errorMessage}";

            ShowNotification(
                "Server Offline",
                message,
                NotificationType.Error);
        }

        public void ShowConnectionRestoredNotification(string serverName)
        {
            ShowNotification(
                "Connection Restored",
                $"{serverName} connection restored",
                NotificationType.Success);
        }

        public void ShowBlockingNotification(string serverName, int blockedSessions, int durationSeconds)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnBlocking) return;

            ShowNotification(
                "Blocking Detected",
                $"{serverName}: {blockedSessions} blocked session(s), longest {durationSeconds}s",
                NotificationType.Warning);
        }

        public void ShowDeadlockNotification(string serverName, int deadlockCount)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnDeadlock) return;

            var plural = deadlockCount == 1 ? "" : "s";
            ShowNotification(
                "Deadlock Detected",
                $"{serverName}: {deadlockCount} deadlock{plural} detected",
                NotificationType.Error);
        }

        public void ShowHighCpuNotification(string serverName, int cpuPercent)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnHighCpu) return;

            ShowNotification(
                "High CPU",
                $"{serverName}: CPU at {cpuPercent}%",
                NotificationType.Warning);
        }

        public void ShowPoisonWaitNotification(string serverName, string waitType, double avgMs)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnPoisonWaits) return;

            ShowNotification(
                "Poison Wait",
                $"{serverName}: {waitType} avg {avgMs:F0}ms/wait",
                NotificationType.Error);
        }

        public void ShowLongRunningQueryNotification(string serverName, int sessionId, long elapsedMinutes, string queryPreview)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnLongRunningQueries) return;

            var preview = string.IsNullOrEmpty(queryPreview) ? "" : $" — {queryPreview}";
            ShowNotification(
                "Long-Running Query",
                $"{serverName}: Session #{sessionId} running {elapsedMinutes}m{preview}",
                NotificationType.Warning);
        }

        public void ShowTempDbSpaceNotification(string serverName, double usedPercent)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnTempDbSpace) return;

            ShowNotification(
                "TempDB Space",
                $"{serverName}: TempDB {usedPercent:F0}% used",
                NotificationType.Warning);
        }

        public void ShowLongRunningJobNotification(string serverName, string jobName, long currentMinutes, decimal percentOfAvg)
        {
            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotifyOnLongRunningJobs) return;

            ShowNotification(
                "Long-Running Job",
                $"{serverName}: {jobName} at {percentOfAvg:F0}% of avg ({currentMinutes}m)",
                NotificationType.Warning);
        }

        private void ShowMainWindow()
        {
            _mainWindow.Show();
            _mainWindow.WindowState = WindowState.Normal;
            _mainWindow.Activate();
        }

        private void OpenSettings()
        {
            ShowMainWindow();
            // Trigger settings via the main window
            if (_mainWindow is MainWindow mainWin)
            {
                var settingsWindow = new SettingsWindow(_preferencesService) { Owner = mainWin };
                settingsWindow.ShowDialog();
            }
        }

        private void ExitApplication()
        {
            if (_mainWindow is MainWindow mainWin)
            {
                mainWin.ExitApplication();
            }
            else
            {
                Application.Current.Shutdown();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;

            if (disposing)
            {
                Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;

                if (_trayIcon != null)
                {
                    // Hide the icon before disposing to ensure it's removed from tray
                    _trayIcon.Visibility = Visibility.Collapsed;
                    _trayIcon.Dispose();
                    _trayIcon = null;
                }
            }

            _disposed = true;
        }

        private void OnThemeChanged(string theme)
        {
            _mainWindow.Dispatcher.InvokeAsync(Initialize);
        }

    }

    public enum NotificationType
    {
        Info,
        Success,
        Warning,
        Error
    }
}

