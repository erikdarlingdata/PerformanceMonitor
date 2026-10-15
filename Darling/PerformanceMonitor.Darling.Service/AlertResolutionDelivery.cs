/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// How one alert resolution is delivered (#5825), beside what <see cref="AlertResolution"/> carries.
/// </summary>
/// <param name="ExternalSource">The external alert source the clear came from (#5654), so a mute rule scoped to that
/// source stops it; null for every other clear.</param>
/// <param name="ClosePagerDuty">Whether this send closes the fire's PagerDuty incident. False for a caller that closes it
/// itself: the external alert drain resolves PagerDuty first and retries a failed resolve on its own schedule.</param>
/// <param name="ContextJson">The history row's context, when the caller has one to keep (the external drain's pending
/// id); null leaves the resolution row's context empty, as before.</param>
public sealed record AlertResolutionSendOptions(
    string? ExternalSource = null, bool ClosePagerDuty = true, string? ContextJson = null)
{
    /// <summary>An engine, PostgreSQL, custom or self-monitor clear: no source, PagerDuty closed here, no context.</summary>
    public static AlertResolutionSendOptions Default { get; } = new();
}

/// <summary>
/// Writes an alert resolution's history row and, when the store sends resolutions, posts the clear to the destinations
/// its delivered fire reached (#5825). <see cref="DarlingAlertDeliverer"/> is the one implementation; a producer that
/// holds an <see cref="IAlertDeliverer"/> asks for this interface and keeps the history-only row when it is absent.
/// </summary>
public interface IAlertResolutionSender
{
    /// <summary>Sends the clear where the setting says, then writes its history row. Never throws except for the
    /// caller's own cancellation: a failed send is recorded on the row, and the row write is failure-isolated.</summary>
    Task DeliverResolutionAsync(
        AlertResolution resolution, AlertResolutionSendOptions? options = null, CancellationToken cancellationToken = default);
}

/// <summary>One delivered fire row of an open alert (#5825): its time, stored value, detail text and context (whose
/// <c>Route</c> record says which channels delivered and which route supplied each).</summary>
public sealed record DeliveredFireRow(DateTime AlertTimeUtc, double? CurrentValue, string? DetailText, string? ContextJson);

/// <summary>An open alert's delivered fire rows, newest first, and the time of its first delivered fire (#5825).</summary>
public sealed record DeliveredFireHistory(DateTime FirstFiredAtUtc, IReadOnlyList<DeliveredFireRow> Rows);

/// <summary>
/// Reads an open alert's delivered fires from alert history (#5825), so a clear handled after a service restart still
/// goes where its fire went. <see cref="PgAlertHistoryStore"/> is the implementation.
/// </summary>
public interface IDeliveredFireReader
{
    /// <summary>The delivered, unmuted fire rows of <paramref name="metricName"/> on <paramref name="serverKey"/> written
    /// after the alert's last resolution row (stored under <paramref name="resolutionTitle"/>), newest first and at most
    /// <see cref="PgAlertHistoryStore.OpenDeliveredFireRowCap"/>; null when there are none or the key has no per-server
    /// rows. Throws on a read failure.</summary>
    Task<DeliveredFireHistory?> GetOpenDeliveredFiresAsync(
        string serverKey, string metricName, string resolutionTitle, CancellationToken cancellationToken = default);
}
