/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Per-app branding the shared notification renderers need: the edition label
/// shown in email/webhook output, and an optional snooze hint.
/// When <see cref="SnoozeHint"/> is <c>null</c> the snooze section is omitted
/// (Dashboard); when non-null it is emitted (Lite).
/// </summary>
public sealed record AlertBranding(string EditionName, string? SnoozeHint);
