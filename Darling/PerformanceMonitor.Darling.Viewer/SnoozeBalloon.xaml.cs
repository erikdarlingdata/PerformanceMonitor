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
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// An interactive tray toast with Snooze (15m / 1h / 4h) and Dismiss, ported from Lite's SnoozeBalloon.
/// The one adaptation for the headless viewer: instead of holding Lite's in-process
/// <c>MuteRuleService</c> + a server/metric pair, it takes an <paramref name="onSnooze"/> callback the
/// caller supplies. The viewer's callback writes a temporary <c>config_mute_rules</c> row straight to
/// Postgres (scoped to the alert's server + metric), which the running Darling service re-reads on its
/// next config load — the store is the coordination point, so there is no IPC to the service.
/// </summary>
public partial class SnoozeBalloon : UserControl
{
    private readonly Func<TimeSpan, Task> _onSnooze;
    private readonly Action _closePopup;
    private bool _closed;

    /// <param name="onSnooze">Persists a snooze of the given duration (writes the scoped mute rule).</param>
    /// <param name="closePopup">Tears the popup down — call <c>TaskbarIcon.CloseBalloon()</c>.</param>
    public SnoozeBalloon(
        string title,
        string message,
        BalloonIcon icon,
        Func<TimeSpan, Task> onSnooze,
        Action closePopup)
    {
        InitializeComponent();
        /* #8475: the card sits in a popup with no window above it, so a theme change has to be brought to it. */
        WindowlessThemeFollower.Attach(this);

        _onSnooze = onSnooze ?? throw new ArgumentNullException(nameof(onSnooze));
        _closePopup = closePopup ?? throw new ArgumentNullException(nameof(closePopup));

        TitleText.Text = title;
        MessageText.Text = message;
        ApplySeverity(icon);
    }

    private void ApplySeverity(BalloonIcon icon)
    {
        switch (icon)
        {
            case BalloonIcon.Error:
                SeverityIcon.Text = "⚠"; /* warning sign */
                SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0xDC, 0x26, 0x26));
                break;
            case BalloonIcon.Warning:
                SeverityIcon.Text = "⚠";
                SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0xF5, 0x9E, 0x0B));
                break;
            default:
                SeverityIcon.Text = "ℹ"; /* info */
                SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0x3B, 0x82, 0xF6));
                break;
        }
    }

    private void Snooze15Button_Click(object sender, RoutedEventArgs e) => Snooze(TimeSpan.FromMinutes(15));
    private void Snooze1hButton_Click(object sender, RoutedEventArgs e) => Snooze(TimeSpan.FromHours(1));
    private void Snooze4hButton_Click(object sender, RoutedEventArgs e) => Snooze(TimeSpan.FromHours(4));

    private async void Snooze(TimeSpan duration)
    {
        if (_closed) return;
        _closed = true;

        try
        {
            await _onSnooze(duration);
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("SnoozeBalloon", $"Failed to add snooze rule: {ex.Message}");
        }

        CloseBalloon();
    }

    private void DismissButton_Click(object sender, RoutedEventArgs e)
    {
        if (_closed) return;
        _closed = true;
        CloseBalloon();
    }

    private void CloseBalloon()
    {
        /* Hardcodet's BalloonClosingEvent is emitted BY the library when it closes; it has no
           listener that translates a balloon-initiated raise back into a close. To actually
           tear the popup down we have to call TaskbarIcon.CloseBalloon() directly. */
        _closePopup();
    }
}
