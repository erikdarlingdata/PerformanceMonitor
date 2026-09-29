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

/// <summary>Stand-in so the tests compile; the real bookkeeping follows.</summary>
public sealed class FailedSendRetryTracker
{
    public bool Record(string key, AlertDelivery? delivery, DateTime nowUtc, TimeSpan cap) =>
        Record(key, delivery, nowUtc, cap, out _, out _);

    public bool Record(string key, AlertDelivery? delivery, DateTime nowUtc, TimeSpan cap, out TimeSpan delay, out int failures)
    {
        _ = key;
        _ = delivery;
        _ = nowUtc;
        _ = cap;
        delay = TimeSpan.Zero;
        failures = 0;
        return false;
    }

    public DateTime? DueUtc(string key) => null;

    public bool RetryPending(string key, DateTime nowUtc) => false;

    public void Clear(string key)
    {
    }
}
