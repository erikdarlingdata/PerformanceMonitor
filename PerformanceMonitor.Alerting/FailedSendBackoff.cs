/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>Streak bookkeeping for an alert whose every channel failed (#4752). Not written yet.</summary>
public sealed class FailedSendBackoff
{
    public static bool EveryChannelFailed(AlertDelivery? delivery) => throw new NotImplementedException();

    public TimeSpan RecordFailure(string family, string key, DateTime nowUtc, TimeSpan cap) => throw new NotImplementedException();

    public TimeSpan RecordFailure(string family, string key, DateTime nowUtc, TimeSpan cap, out int failures) => throw new NotImplementedException();

    public void RecordDelivered(string family, string key) => throw new NotImplementedException();

    public int TrackedCount => throw new NotImplementedException();
}
