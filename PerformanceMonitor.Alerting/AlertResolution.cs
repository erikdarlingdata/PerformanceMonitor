/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A condition-recovered notification the engine emits on an active→inactive transition — the
/// engine-side twin of Lite's tray-only "Resolved"/"Cleared" toasts
/// (<c>ShowStyledNotification(..., ToastSeverity.Success)</c> in <c>MainWindow.AlertEngine.cs</c>).
/// Lite records NO history row and sends NO email for these, so they deliberately do NOT flow
/// through <see cref="IAlertDeliverer"/>: the engine hands them to an optional host callback
/// instead. <see cref="Title"/>/<see cref="Message"/> carry Lite's exact strings so a forwarding
/// Lite renders identical toasts (always Success severity — every Lite resolution toast is);
/// Darling logs them and writes a history row for each; since #5825 it also posts one, when the store sends resolutions,
/// to the channels the alert's delivered fire reached (<c>DarlingAlertDeliverer.DeliverResolutionAsync</c>), still not
/// through <see cref="IAlertDeliverer"/>.
/// </summary>
/// <param name="ServerKey">The engine's stable identity for the server.</param>
/// <param name="ServerName">Display name (already embedded in <see cref="Message"/>).</param>
/// <param name="MetricName">The alert metric that recovered (e.g. "High CPU") — for host filtering.</param>
/// <param name="Title">Lite's toast title, verbatim (e.g. "CPU Resolved", "Blocking Cleared").</param>
/// <param name="Message">Lite's toast message, verbatim (e.g. "SERVER: Total CPU back to 12%").</param>
public sealed record AlertResolution(
    string ServerKey,
    string ServerName,
    string MetricName,
    string Title,
    string Message);
