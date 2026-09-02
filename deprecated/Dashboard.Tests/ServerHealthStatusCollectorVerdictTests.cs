/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitorDashboard.Models;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Pins the Overview health card's collector verdict (<see cref="ServerHealthStatus.CollectorDisplayText"/> /
/// <see cref="ServerHealthStatus.CollectorSeverity"/>) — #2784, the Dashboard sibling of the WPF viewer and
/// web (#2779/#2783) parity fix. The verdict used to key ONLY on the failing count, so an offline server —
/// whose collectors have gone stale, leaving FailedCollectorCount at 0 — rendered a green "OK / Healthy: 0,
/// Failing: 0". It now reads a neutral "Stale" off the live IsOnline signal, while a real failure on a
/// reachable server and a plain healthy server are unchanged.
/// </summary>
public class ServerHealthStatusCollectorVerdictTests
{
    [Fact]
    public void OfflineServer_CollectorsReadStaleNeutral_NotGreenOk()
    {
        var offline = new ServerHealthStatus(new ServerConnection()) { IsOnline = false, HealthyCollectorCount = 0, FailedCollectorCount = 0 };
        Assert.Equal("Stale", offline.CollectorDisplayText);
        Assert.Equal("No recent collection", offline.CollectorDetailText);
        Assert.Equal(HealthSeverity.Unknown, offline.CollectorSeverity);   // neutral, NOT green Healthy

        // Offline wins over a leftover stale failing count too — every count is unmeasured once the server is
        // dark, so a "2 failing" left over from the last collection must not keep reading as an active failure.
        var offlineWithStaleFailures = new ServerHealthStatus(new ServerConnection()) { IsOnline = false, FailedCollectorCount = 2 };
        Assert.Equal("Stale", offlineWithStaleFailures.CollectorDisplayText);
        Assert.Equal(HealthSeverity.Unknown, offlineWithStaleFailures.CollectorSeverity);
    }

    [Fact]
    public void ReachableServer_RealFailureStillRed_HealthyStillOk()
    {
        var failing = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 28, FailedCollectorCount = 2 };
        Assert.Equal("2 failed", failing.CollectorDisplayText);
        Assert.Equal(HealthSeverity.Warning, failing.CollectorSeverity);

        var healthy = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 30, FailedCollectorCount = 0 };
        Assert.Equal("OK", healthy.CollectorDisplayText);
        Assert.Equal("Healthy: 30, Failing: 0", healthy.CollectorDetailText);
        Assert.Equal(HealthSeverity.Healthy, healthy.CollectorSeverity);

        // Not-yet-connection-checked (IsOnline null) keeps the pre-#2784 reading — the "Stale" verdict is for
        // a KNOWN-offline server only, mirroring the web's `is_online === false`.
        var notChecked = new ServerHealthStatus(new ServerConnection()) { HealthyCollectorCount = 30, FailedCollectorCount = 0 };
        Assert.Equal("OK", notChecked.CollectorDisplayText);
        Assert.Equal(HealthSeverity.Healthy, notChecked.CollectorSeverity);
    }

    [Fact]
    public void GoingOffline_RaisesPropertyChanged_ForCollectorVerdict()
    {
        // The collector verdict now depends on IsOnline, so flipping the connection must repaint the dot /
        // value / detail — without these notifications the live-bound row would keep showing the stale "OK".
        var status = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, FailedCollectorCount = 0 };
        var raised = new List<string>();
        status.PropertyChanged += (_, e) => raised.Add(e.PropertyName ?? "");

        status.IsOnline = false;

        Assert.Contains(nameof(ServerHealthStatus.CollectorSeverity), raised);
        Assert.Contains(nameof(ServerHealthStatus.CollectorDisplayText), raised);
        Assert.Contains(nameof(ServerHealthStatus.CollectorDetailText), raised);
    }
}
