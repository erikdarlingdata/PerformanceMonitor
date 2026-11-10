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
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Manages the system tray icon and minimize-to-tray behavior.
/// </summary>
public class SystemTrayService : IDisposable
{
    private TaskbarIcon? _trayIcon;
    private readonly Window _mainWindow;
    private readonly Action _restoreWindow;
    private readonly CollectionBackgroundService? _backgroundService;
    private bool _disposed;
    private MenuItem? _pauseResumeItem;
    private TextBlock? _tooltipText;

    /// <param name="restoreWindow">
    /// The window's own WPF restore (e.g. <c>MainWindow.RestoreFromTray</c>). The tray must restore
    /// through WPF's <see cref="Window.Show"/> path, never a raw Win32 ShowWindow, or a tray-hidden
    /// window comes back blank (#1050).
    /// </param>
    public SystemTrayService(Window mainWindow, Action restoreWindow, CollectionBackgroundService? backgroundService = null)
    {
        _mainWindow = mainWindow;
        _restoreWindow = restoreWindow ?? throw new ArgumentNullException(nameof(restoreWindow));
        _backgroundService = backgroundService;
        ThemeManager.ThemeChanged += OnThemeChanged;
    }

    /// <summary>
    /// Initializes the system tray icon with context menu. Safe to call again: it disposes the old icon and
    /// re-subscribes the StateChanged handler exactly once. A theme change does not come through here; it restyles
    /// the icon that exists (<see cref="ApplyTheme"/>).
    /// </summary>
    public void Initialize()
    {
        _trayIcon?.Dispose();

        _trayIcon = new TaskbarIcon();

        /* Load icon */
        try
        {
            var iconUri = new Uri("pack://application:,,,/EDD.ico", UriKind.Absolute);
            _trayIcon.IconSource = new BitmapImage(iconUri);
        }
        catch
        {
            /* Icon loading failed - tray icon will be blank but functional */
        }

        ApplyTheme();

        /* Double-click to show window */
        _trayIcon.TrayMouseDoubleClick += (s, e) => _restoreWindow();

        /* Handle minimize to tray. Unsubscribe first, so a second Initialize does not stack a duplicate
           StateChanged handler on the app-lifetime window. */
        _mainWindow.StateChanged -= MainWindow_StateChanged;
        _mainWindow.StateChanged += MainWindow_StateChanged;
    }

    /// <summary>
    /// Styles the tray icon's tooltip and context menu for the current theme, on the icon that already exists. A theme
    /// change runs this in place of rebuilding the icon: disposing the icon leaves a toast on screen open, with its close
    /// timer stopped and no icon that can close it, and the next toast opens over it (#8475). The toast itself follows the
    /// theme through <see cref="WindowlessThemeFollower"/>. The tooltip and the pause item say what the collection is doing now.
    /// </summary>
    private void ApplyTheme()
    {
        if (_trayIcon == null) return;

        bool HasLightBackground = ThemeManager.HasLightBackground;
        bool paused = _backgroundService?.IsPaused == true;

        /* Custom tooltip styled to match current theme.
           Note: Hardcodet TrayToolTip can rarely trigger a race condition in Popup.CreateWindow
           that throws "The root Visual of a VisualTarget cannot have a parent." (issue #422).
           The DispatcherUnhandledException handler silently swallows this specific crash. */
        _tooltipText = new TextBlock
        {
            Text = paused ? "Performance Monitor Lite (Paused)" : "Performance Monitor Lite",
            Foreground = new SolidColorBrush(HasLightBackground
                ? (Color)ColorConverter.ConvertFromString("#1A1D23")
                : (Color)ColorConverter.ConvertFromString("#E4E6EB")),
            FontSize = 12
        };
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
            Child = _tooltipText
        };

        /* Build context menu */
        var contextMenu = new ContextMenu();

        var showItem = new MenuItem { Header = "_Show Window", Icon = new TextBlock { Text = "📊", Background = Brushes.Transparent } };
        showItem.Click += (s, e) => _restoreWindow();
        contextMenu.Items.Add(showItem);

        contextMenu.Items.Add(new Separator());

        _pauseResumeItem = new MenuItem { Header = paused ? "Resume Co_llection" : "Pause Co_llection", Icon = new TextBlock { Text = paused ? "▶" : "⏸", Background = Brushes.Transparent } };
        _pauseResumeItem.Click += (s, e) => ToggleCollection();
        contextMenu.Items.Add(_pauseResumeItem);

        contextMenu.Items.Add(new Separator());

        var exitItem = new MenuItem { Header = "E_xit", Icon = new TextBlock { Text = "✕", Background = Brushes.Transparent } };
        exitItem.Click += (s, e) => ExitApplication();
        contextMenu.Items.Add(exitItem);

        _trayIcon.ContextMenu = contextMenu;
    }

    private void MainWindow_StateChanged(object? sender, EventArgs e)
    {
        if (_mainWindow.WindowState == WindowState.Minimized && App.MinimizeToTray)
        {
            _mainWindow.Hide();
        }
    }

    private void ToggleCollection()
    {
        if (_backgroundService == null) return;

        _backgroundService.IsPaused = !_backgroundService.IsPaused;

        if (_pauseResumeItem != null)
        {
            _pauseResumeItem.Header = _backgroundService.IsPaused ? "Resume Co_llection" : "Pause Co_llection";
            _pauseResumeItem.Icon = new TextBlock { Text = _backgroundService.IsPaused ? "▶" : "⏸", Background = Brushes.Transparent };
        }

        if (_tooltipText != null)
        {
            _tooltipText.Text = _backgroundService.IsPaused
                ? "Performance Monitor Lite (Paused)"
                : "Performance Monitor Lite";
        }
    }

    private void ExitApplication()
    {
        Application.Current.Shutdown();
    }

    /// <summary>
    /// Shows a balloon notification from the system tray icon.
    /// </summary>
    public void ShowNotification(string title, string message, BalloonIcon icon)
    {
        _trayIcon?.ShowBalloonTip(title, message, icon);
    }

    /// <summary>
    /// Shows a themed, button-less balloon (the same card chrome as the snoozable condition cards)
    /// for resolved/cleared conditions, so an "all clear" toast no longer renders as a plain, unthemed
    /// Windows balloon. Pass <see cref="ToastSeverity.Success"/> for a green-check "resolved" accent.
    /// No-op if the tray icon hasn't been initialized.
    /// </summary>
    public void ShowStyledNotification(string title, string message, ToastSeverity severity)
    {
        if (_trayIcon == null)
            return;

        var trayIcon = _trayIcon;
        var balloon = new StyledBalloon(title, message, severity);
        trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, StyledToastTimeoutMs);
        HoldWhileRead(balloon, trayIcon, StyledToastTimeoutMs);
    }

    /// <summary>
    /// Shows a custom interactive balloon with snooze buttons that create a temporary
    /// mute rule scoped to <paramref name="serverName"/> + <paramref name="metricName"/>.
    /// Falls back to a plain balloon if the tray icon hasn't been initialized.
    /// </summary>
    public void ShowSnoozableNotification(
        string title,
        string message,
        BalloonIcon icon,
        string serverName,
        string metricName,
        MuteRuleService muteRuleService)
    {
        if (_trayIcon == null)
            return;

        var trayIcon = _trayIcon;
        var balloon = new SnoozeBalloon(title, message, icon, serverName, metricName, muteRuleService, () => trayIcon.CloseBalloon());
        trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, SnoozeToastTimeoutMs);
        HoldWhileRead(balloon, trayIcon, SnoozeToastTimeoutMs);
    }

    /// <summary>How long a plain (cleared or resolved) card stays, and how long a snoozable one stays, without a reader on it.</summary>
    private const int StyledToastTimeoutMs = 10000;
    private const int SnoozeToastTimeoutMs = 15000;

    /// <summary>
    /// #8475: a card stays while the pointer is on it or keyboard focus is inside it, and its timeout runs again from the
    /// moment the reader leaves, so a message long enough to scroll is not closed under the reader.
    /// </summary>
    private static void HoldWhileRead(FrameworkElement balloon, TaskbarIcon trayIcon, int timeoutMs) =>
        BalloonHoverHold.Attach(balloon, TimeSpan.FromMilliseconds(timeoutMs), trayIcon.ResetBalloonCloseTimer, trayIcon.CloseBalloon);

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing && _trayIcon != null)
        {
            ThemeManager.ThemeChanged -= OnThemeChanged;
            _mainWindow.StateChanged -= MainWindow_StateChanged;
            _trayIcon.Visibility = Visibility.Collapsed;
            _trayIcon.Dispose();
            _trayIcon = null;
        }

        _disposed = true;
    }

    private void OnThemeChanged(string _)
    {
        _mainWindow.Dispatcher.InvokeAsync(ApplyTheme);
    }
}

