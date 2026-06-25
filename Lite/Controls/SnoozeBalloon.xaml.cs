/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class SnoozeBalloon : UserControl
{
    private readonly MuteRuleService _muteRuleService;
    private readonly string _serverName;
    private readonly string _metricName;
    private readonly Action _closePopup;
    private bool _closed;

    public SnoozeBalloon(
        string title,
        string message,
        BalloonIcon icon,
        string serverName,
        string metricName,
        MuteRuleService muteRuleService,
        Action closePopup)
    {
        InitializeComponent();

        _muteRuleService = muteRuleService;
        _serverName = serverName;
        _metricName = metricName;
        _closePopup = closePopup;

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

        var rule = new MuteRule
        {
            ServerName = _serverName,
            MetricName = _metricName,
            ExpiresAtUtc = DateTime.UtcNow + duration,
            Reason = $"Snoozed from popup ({FormatDuration(duration)})"
        };

        try
        {
            await _muteRuleService.AddRuleAsync(rule);
        }
        catch (Exception ex)
        {
            AppLogger.Warn("SnoozeBalloon", $"Failed to add snooze rule: {ex.Message}");
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

    private static string FormatDuration(TimeSpan d) =>
        d.TotalHours >= 1 ? $"{(int)d.TotalHours}h" : $"{(int)d.TotalMinutes}m";
}
