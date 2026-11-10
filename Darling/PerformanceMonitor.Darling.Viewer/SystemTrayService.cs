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
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's system-tray icon + minimize-to-tray behavior, ported from Lite's SystemTrayService and
/// adapted for the headless viewer:
/// <list type="bullet">
/// <item>there is no in-process collector to pause, so the context menu is just Show Window / Exit (Lite's
/// Pause/Resume item is dropped) — collection is the separate Darling service's job;</item>
/// <item>minimize-to-tray reads a caller-supplied <c>Func&lt;bool&gt;</c> (the live
/// <see cref="ViewerAppSettings.MinimizeToTray"/> value) instead of Lite's <c>App.MinimizeToTray</c> static;</item>
/// <item>the snoozable toast takes a persist-snooze callback (the viewer writes the mute rule straight to
/// Postgres) instead of Lite's in-process <c>MuteRuleService</c>.</item>
/// </list>
/// The tray icon is created once and lives for the process lifetime; toasts are surfaced by the
/// MainWindow's alert-history poll (there is no in-process alert engine in the viewer).
/// </summary>
public sealed class SystemTrayService : IDisposable
{
    private const string TooltipText = "Performance Monitor Darling Viewer";

    private TaskbarIcon? _trayIcon;
    private readonly Window _mainWindow;
    private readonly Action _restoreWindow;
    private readonly Func<bool> _minimizeToTrayEnabled;
    private bool _disposed;
    private TextBlock? _tooltipText;

    /// <param name="mainWindow">The window whose minimize is intercepted and hidden to the tray.</param>
    /// <param name="restoreWindow">
    /// The window's own WPF restore (the viewer's <c>MainWindow.SurfaceWindow</c>, shared with the
    /// single-instance surface path). The tray must restore through WPF's <see cref="Window.Show"/> path,
    /// never a raw Win32 ShowWindow, or a tray-hidden window comes back blank (Lite #1050).
    /// </param>
    /// <param name="minimizeToTrayEnabled">Reads the live Minimize-to-tray setting each time the window minimizes.</param>
    public SystemTrayService(Window mainWindow, Action restoreWindow, Func<bool> minimizeToTrayEnabled)
    {
        _mainWindow = mainWindow ?? throw new ArgumentNullException(nameof(mainWindow));
        _restoreWindow = restoreWindow ?? throw new ArgumentNullException(nameof(restoreWindow));
        _minimizeToTrayEnabled = minimizeToTrayEnabled ?? throw new ArgumentNullException(nameof(minimizeToTrayEnabled));
        ThemeManager.ThemeChanged += OnThemeChanged;
    }

    /// <summary>
    /// Initializes the system tray icon with its context menu. Safe to call again: it disposes the old icon and
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
    /// theme through <see cref="WindowlessThemeFollower"/>.
    /// </summary>
    private void ApplyTheme()
    {
        if (_trayIcon == null)
        {
            return;
        }

        bool hasLightBackground = ThemeManager.HasLightBackground;

        /* Custom tooltip styled to match current theme.
           Note: Hardcodet TrayToolTip can rarely trigger a race condition in Popup.CreateWindow
           that throws "The root Visual of a VisualTarget cannot have a parent." (issue #422).
           App.xaml.cs's IsTrayToolTipCrash suppresses it: e.Handled on the Dispatcher path (keeps the
           app alive) + log-only on the AppDomain path -- matching Lite's proven handling of #422. */
        _tooltipText = new TextBlock
        {
            Text = TooltipText,
            Foreground = new SolidColorBrush(hasLightBackground
                ? (Color)ColorConverter.ConvertFromString("#1A1D23")
                : (Color)ColorConverter.ConvertFromString("#E4E6EB")),
            FontSize = 12
        };
        _trayIcon.TrayToolTip = new Border
        {
            Background = new SolidColorBrush(hasLightBackground
                ? (Color)ColorConverter.ConvertFromString("#FFFFFF")
                : (Color)ColorConverter.ConvertFromString("#22252b")),
            BorderBrush = new SolidColorBrush(hasLightBackground
                ? (Color)ColorConverter.ConvertFromString("#DEE2E6")
                : (Color)ColorConverter.ConvertFromString("#33363e")),
            BorderThickness = new Thickness(1),
            Padding = new Thickness(10, 8, 10, 8),
            CornerRadius = new CornerRadius(4),
            Child = _tooltipText
        };

        /* Build context menu — Show Window / Exit. The viewer has no in-process collection to pause. */
        var contextMenu = new ContextMenu();

        var showItem = new MenuItem { Header = "_Show Window", Icon = new TextBlock { Text = "📊", Background = Brushes.Transparent } };
        showItem.Click += (s, e) => _restoreWindow();
        contextMenu.Items.Add(showItem);

        contextMenu.Items.Add(new Separator());

        var exitItem = new MenuItem { Header = "E_xit", Icon = new TextBlock { Text = "✕", Background = Brushes.Transparent } };
        exitItem.Click += (s, e) => ExitApplication();
        contextMenu.Items.Add(exitItem);

        _trayIcon.ContextMenu = contextMenu;
    }

    private void MainWindow_StateChanged(object? sender, EventArgs e)
    {
        if (_mainWindow.WindowState == WindowState.Minimized && _minimizeToTrayEnabled())
        {
            _mainWindow.Hide();
        }
    }

    private static void ExitApplication()
    {
        Application.Current.Shutdown();
    }

    /// <summary>
    /// Shows a plain balloon notification from the system tray icon. No-op if the tray icon hasn't been
    /// initialized.
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
        {
            return;
        }

        var trayIcon = _trayIcon;
        var balloon = new StyledBalloon(title, message, severity);
        trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, StyledToastTimeoutMs);
        HoldWhileRead(balloon, trayIcon, StyledToastTimeoutMs);
    }

    /// <summary>
    /// Shows a custom interactive balloon with snooze buttons. The <paramref name="onSnooze"/> callback
    /// persists a temporary mute rule scoped to the alert's server + metric (the viewer writes it straight
    /// to <c>config_mute_rules</c>). No-op if the tray icon hasn't been initialized.
    /// </summary>
    public void ShowSnoozableNotification(
        string title,
        string message,
        BalloonIcon icon,
        Func<TimeSpan, Task> onSnooze)
    {
        if (_trayIcon == null)
        {
            return;
        }

        var trayIcon = _trayIcon;
        var balloon = new SnoozeBalloon(title, message, icon, onSnooze, () => trayIcon.CloseBalloon());
        trayIcon.ShowCustomBalloon(balloon, System.Windows.Controls.Primitives.PopupAnimation.Slide, SnoozeToastTimeoutMs);
        HoldWhileRead(balloon, trayIcon, SnoozeToastTimeoutMs);
    }

    /// <summary>How long a plain (cleared or resolved) card stays, and how long a snoozable one stays, without a reader on it.</summary>
    private const int StyledToastTimeoutMs = 10000;
    private const int SnoozeToastTimeoutMs = 15000;

    /// <summary>
    /// #8475: a card stays while the pointer is on it or keyboard focus is inside it, and its timeout runs again from the
    /// moment the reader leaves, so a message long enough to scroll is not closed under the reader. The hold's two actions
    /// act only while this card is the balloon the tray is showing: a card that the next toast has replaced, or that Dismiss
    /// has closed, neither stops the current balloon's timer nor closes it.
    /// </summary>
    private static void HoldWhileRead(FrameworkElement balloon, TaskbarIcon trayIcon, int timeoutMs) =>
        BalloonHoverHold.Attach(
            balloon,
            TimeSpan.FromMilliseconds(timeoutMs),
            holdOpen: () => { if (IsShowing(trayIcon, balloon)) trayIcon.ResetBalloonCloseTimer(); },
            close: () => { if (IsShowing(trayIcon, balloon)) trayIcon.CloseBalloon(); });

    /// <summary>Whether <paramref name="balloon"/> is the custom balloon the tray icon has on screen now.</summary>
    private static bool IsShowing(TaskbarIcon trayIcon, FrameworkElement balloon) =>
        ReferenceEquals(trayIcon.CustomBalloon?.Child, balloon);

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        ThemeManager.ThemeChanged -= OnThemeChanged;
        _mainWindow.StateChanged -= MainWindow_StateChanged;

        if (_trayIcon != null)
        {
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
