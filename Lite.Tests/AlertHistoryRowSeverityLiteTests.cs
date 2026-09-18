/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3539 A8e, Lite's half: the Alert History grid row (<see cref="AlertHistoryRow"/>) colours itself by the
/// tier the alert FIRED at, read off the row's persisted <c>context_json</c>, and by its metric NAME only
/// when the row carries no tier. Lite's rows have always carried <c>context_json</c> (the in-app detail
/// dialog rehydrates it), so there is no DuckDB schema step here — the member the serializer now writes
/// is what the row reads. The shared decision and its fallback reasons are
/// <see cref="AlertHistoryRowSeverity"/>'s, pinned in full on the Darling side
/// (<c>AlertHistoryRowSeverityTests</c>); this file pins that Lite's row reaches it.
/// </summary>
public sealed class AlertHistoryRowSeverityLiteTests
{
    private static string WithSeverity(AlertSeverityLevel? level)
    {
        var context = new AlertContext { SeverityOverride = level };
        context.Details.Add(new AlertDetailItem { Heading = "THREADPOOL", Fields = { ("Accumulated wait", "61 s") } });
        return AlertContextSerializer.Serialize(context);
    }

    private static AlertHistoryRow Row(string metric, string? contextJson) => new()
    {
        AlertTime = new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc),
        MetricName = metric,
        CurrentValue = 61_000,
        ThresholdValue = 60_000,
        AlertSent = true,
        NotificationType = AlertDelivery.ChannelWebhook,
        ContextJson = contextJson,
    };

    /// <summary>A Poison Wait that fired WARNING (SQL Server grades it since #3539 A4) is an amber row, not
    /// the red its name earned every row before; one that fired CRITICAL is red; a row written before the
    /// tier was persisted keeps the name's red, which is faithful — every such SQL Server row was a
    /// presence-flat critical fire.</summary>
    [Fact]
    public void ThePoisonWaitRow_RendersTheTierItFiredAt()
    {
        var warning = Row("Poison Wait", WithSeverity(AlertSeverityLevel.Warning));
        Assert.True(warning.IsWarning);
        Assert.False(warning.IsCritical);
        Assert.False(warning.IsResolved);

        var critical = Row("Poison Wait", WithSeverity(AlertSeverityLevel.Critical));
        Assert.True(critical.IsCritical);
        Assert.False(critical.IsWarning);

        var legacy = Row("Poison Wait", null);
        Assert.True(legacy.IsCritical);
        Assert.False(legacy.IsWarning);
    }

    /// <summary>The other direction: a CRITICAL-graded low-disk fire (#1136) was an amber row by name and is
    /// red by tier; the presence-flat metrics carry no tier and keep the name's colour exactly.</summary>
    [Fact]
    public void GradedAboveTheName_IsRed_AndPresenceFlatIsUnchanged()
    {
        var lowDisk = Row("Volume Free Space", WithSeverity(AlertSeverityLevel.Critical));
        Assert.True(lowDisk.IsCritical);
        Assert.False(lowDisk.IsWarning);

        var deadlocks = Row("Deadlocks Detected", null);
        Assert.True(deadlocks.IsCritical);
        var cpu = Row("High CPU", null);
        Assert.True(cpu.IsWarning);
        Assert.False(cpu.IsCritical);

        var cleared = Row("Poison Waits Cleared", null);
        Assert.True(cleared.IsResolved);
        Assert.False(cleared.IsCritical);
        Assert.False(cleared.IsWarning);
    }
}
