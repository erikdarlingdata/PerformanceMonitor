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
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitor.Ui;

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
            ThemeManager.ThemeChanged += OnThemeChanged;
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

            bool HasLightBackground = ThemeManager.HasLightBackground;

            /* Custom tooltip styled to match current theme.
               Note: Hardcodet TrayToolTip can rarely trigger a race condition in Popup.CreateWindow
               that throws "The root Visual of a VisualTarget cannot have a parent." (issue #422).
               The DispatcherUnhandledException handler silently swallows this specific crash. */
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

        /// <summary>
        /// Shows a themed, button-less balloon (the same card chrome as the snoozable condition cards)
        /// for resolved/cleared conditions, so an "all clear" toast no longer renders as a plain,
        /// unthemed Windows balloon. Pass <see cref="ToastSeverity.Success"/> for a green-check "resolved"
        /// accent. Honors the notifications-enabled pref and marshals to the UI thread, like
        /// <see cref="ShowNotification"/>.
        /// </summary>
        public void ShowStyledNotification(string title, string message, ToastSeverity severity)
        {
            if (_trayIcon == null) return;

            var prefs = _preferencesService.GetPreferences();
            if (!prefs.NotificationsEnabled) return;

            void Show()
            {
                var trayIcon = _trayIcon;
                if (trayIcon == null) return;
                var balloon = new Controls.StyledBalloon(title, message, severity);
                trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, 10000);
            }

            if (_mainWindow.Dispatcher.CheckAccess())
                Show();
            else
                _mainWindow.Dispatcher.Invoke(Show);
        }

        /// <summary>
        /// Shows a custom interactive popup with Snooze 15m / 1h / 4h and Dismiss buttons.
        /// Snooze buttons create a temporary mute rule scoped to <paramref name="serverName"/> + <paramref name="metricName"/>.
        /// </summary>
        public void ShowSnoozableNotification(
            string title,
            string message,
            NotificationType type,
            string serverName,
            string metricName,
            MuteRuleService muteRuleService)
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

            void Show()
            {
                var trayIcon = _trayIcon;
                if (trayIcon == null) return;
                var balloon = new Controls.SnoozeBalloon(title, message, icon, serverName, metricName, muteRuleService, () => trayIcon.CloseBalloon());
                trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, 15000);
            }

            if (_mainWindow.Dispatcher.CheckAccess())
                Show();
            else
                _mainWindow.Dispatcher.Invoke(Show);
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

        /// <summary>
        /// Restores the main window from the tray. Also used as the #1050 resume-restore callback.
        /// </summary>
        internal void ShowMainWindow()
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
                ThemeManager.ThemeChanged -= OnThemeChanged;

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

