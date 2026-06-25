/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// A themed, button-less tray balloon for resolved/cleared (and other informational) notifications.
    /// Shares <see cref="SnoozeBalloon"/>'s card chrome (background/border/severity icon) so a "Cleared"
    /// or "Resolved" toast looks like the snoozable condition cards rather than a plain Windows balloon.
    /// </summary>
    public partial class StyledBalloon : UserControl
    {
        public StyledBalloon(string title, string message, ToastSeverity severity)
        {
            InitializeComponent();

            TitleText.Text = title;
            MessageText.Text = message;
            ApplySeverity(severity);
        }

        /* Mirrors SnoozeBalloon.ApplySeverity (identical Warning/Error/Info glyphs + colors) and adds the
           Success case — a green check (#16A34A), the same green the email/webhook "RESOLVED" badge uses —
           for cleared/resolved conditions. */
        private void ApplySeverity(ToastSeverity severity)
        {
            switch (severity)
            {
                case ToastSeverity.Error:
                    SeverityIcon.Text = "⚠";
                    SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0xDC, 0x26, 0x26));
                    break;
                case ToastSeverity.Warning:
                    SeverityIcon.Text = "⚠";
                    SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0xF5, 0x9E, 0x0B));
                    break;
                case ToastSeverity.Success:
                    SeverityIcon.Text = "✓";
                    SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0x16, 0xA3, 0x4A));
                    break;
                default:
                    SeverityIcon.Text = "ℹ";
                    SeverityIcon.Foreground = new SolidColorBrush(Color.FromRgb(0x3B, 0x82, 0xF6));
                    break;
            }
        }
    }
}
