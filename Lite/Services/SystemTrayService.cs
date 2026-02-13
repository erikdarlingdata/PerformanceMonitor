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

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Manages the system tray icon and minimize-to-tray behavior.
/// </summary>
public class SystemTrayService : IDisposable
{
    private TaskbarIcon? _trayIcon;
    private readonly Window _mainWindow;
    private readonly CollectionBackgroundService? _backgroundService;
    private bool _disposed;
    private MenuItem? _pauseResumeItem;
    private TextBlock? _tooltipText;

    public SystemTrayService(Window mainWindow, CollectionBackgroundService? backgroundService = null)
    {
        _mainWindow = mainWindow;
        _backgroundService = backgroundService;
    }

    /// <summary>
    /// Initializes the system tray icon with context menu.
    /// </summary>
    public void Initialize()
    {
        _trayIcon?.Dispose();

        _trayIcon = new TaskbarIcon();

        /* Custom dark tooltip (native ToolTipText uses Windows light theme) */
        _tooltipText = new TextBlock
        {
            Text = "Performance Monitor Lite",
            Foreground = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")),
            FontSize = 12
        };
        _trayIcon.TrayToolTip = new Border
        {
            Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#22252b")),
            BorderBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#33363e")),
            BorderThickness = new Thickness(1),
            Padding = new Thickness(10, 8, 10, 8),
            CornerRadius = new CornerRadius(4),
            Child = _tooltipText
        };

        /* Load dark theme for context menu styling */
        var darkTheme = new ResourceDictionary
        {
            Source = new Uri("pack://application:,,,/Themes/DarkTheme.xaml", UriKind.Absolute)
        };

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

        /* Build context menu with dark theme */
        var contextMenu = new ContextMenu();
        contextMenu.Resources.MergedDictionaries.Add(darkTheme);

        var showItem = new MenuItem { Header = "Show Window", Icon = new TextBlock { Text = "📊", Background = Brushes.Transparent } };
        showItem.Click += (s, e) => ShowMainWindow();
        contextMenu.Items.Add(showItem);

        contextMenu.Items.Add(new Separator());

        _pauseResumeItem = new MenuItem { Header = "Pause Collection", Icon = new TextBlock { Text = "⏸", Background = Brushes.Transparent } };
        _pauseResumeItem.Click += (s, e) => ToggleCollection();
        contextMenu.Items.Add(_pauseResumeItem);

        contextMenu.Items.Add(new Separator());

        var exitItem = new MenuItem { Header = "Exit", Icon = new TextBlock { Text = "✕", Background = Brushes.Transparent } };
        exitItem.Click += (s, e) => ExitApplication();
        contextMenu.Items.Add(exitItem);

        _trayIcon.ContextMenu = contextMenu;

        /* Double-click to show window */
        _trayIcon.TrayMouseDoubleClick += (s, e) => ShowMainWindow();

        /* Handle minimize to tray */
        _mainWindow.StateChanged += MainWindow_StateChanged;
    }

    private void MainWindow_StateChanged(object? sender, EventArgs e)
    {
        if (_mainWindow.WindowState == WindowState.Minimized)
        {
            _mainWindow.Hide();
        }
    }

    private void ShowMainWindow()
    {
        _mainWindow.Show();
        _mainWindow.ShowInTaskbar = true;
        _mainWindow.WindowState = WindowState.Normal;
        _mainWindow.Activate();
    }

    private void ToggleCollection()
    {
        if (_backgroundService == null) return;

        _backgroundService.IsPaused = !_backgroundService.IsPaused;

        if (_pauseResumeItem != null)
        {
            _pauseResumeItem.Header = _backgroundService.IsPaused ? "Resume Collection" : "Pause Collection";
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
            _mainWindow.StateChanged -= MainWindow_StateChanged;
            _trayIcon.Visibility = Visibility.Collapsed;
            _trayIcon.Dispose();
            _trayIcon = null;
        }

        _disposed = true;
    }
}
