/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

public sealed class ConnectionAlertSendsInFlight
{
    public void Begin(string serverId)
    {
    }

    public void End(string serverId)
    {
    }

    public DateTime? RetryDueUtc(string serverId, FailedSendRetryTracker retries) => retries.DueUtc(serverId);
}
