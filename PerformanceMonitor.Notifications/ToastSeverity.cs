/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Visual severity for a styled tray balloon (<c>StyledBalloon</c>), mapped to an accent glyph + color.
/// Shared by both apps so the resolved/cleared "all clear" cards (Success = green check) render the same
/// themed chrome as the snoozable condition cards (Warning/Error) instead of a plain Windows balloon.
/// <c>Success</c> uses the same green (#16A34A) the email/webhook "RESOLVED" badge uses (see
/// <see cref="AlertSeverity"/>) so the resolved state reads the same across every surface.
/// </summary>
public enum ToastSeverity
{
    Info,
    Success,
    Warning,
    Error
}
