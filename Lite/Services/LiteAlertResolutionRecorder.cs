/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The history row Lite writes when the shared engine reports an alert cleared (#6010 part 2). Darling writes the same row
/// from the same builder, so the two products read one shape: the resolution title as the metric, <c>none</c> as the
/// channel (so it is not an undelivered alert), and a <c>Resolves</c> member naming the firing metric, which is what the
/// investigation sweep reads to journal <c>resolved</c>. Lite sends a resolution to no email or webhook channel.
/// </summary>
internal static class LiteAlertResolutionRecorder
{
    public static Task RecordAsync(IAlertHistoryStore history, AlertResolution resolution) =>
        history.RecordAlertAsync(AlertResolutionRecords.BuildResolutionRecord(resolution));

    /// <summary>
    /// The engine's resolution callback: write the history row, then run <paramref name="afterRecord"/> (Lite's tray toast and log
    /// line). The row is written first, and a failed write never costs the toast: the store logs its own failure, and anything
    /// else is logged here and swallowed.
    /// </summary>
    public static Func<AlertResolution, CancellationToken, Task> Callback(
        IAlertHistoryStore history, Func<AlertResolution, CancellationToken, Task> afterRecord) =>
        async (resolution, cancellationToken) =>
        {
            try
            {
                await RecordAsync(history, resolution);
            }
            catch (Exception ex)
            {
                AppLogger.Error("Alerts", $"Could not record the '{resolution.Title}' resolution row: {ex.Message}");
            }

            await afterRecord(resolution, cancellationToken);
        };
}
