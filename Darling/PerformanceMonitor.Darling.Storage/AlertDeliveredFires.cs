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

namespace PerformanceMonitor.Darling.Storage;

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
