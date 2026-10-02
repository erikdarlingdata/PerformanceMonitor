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
/// <para>#3653 (#3635's class, mirrored): an ONLINE server with zero collectors banded — the
/// report.collection_health SUM came back NULL, or the read failed and left both counts at 0 — is
/// UNMEASURED, not healthy: Unknown, "--", "No collector banded yet". Any collector banded is unchanged.</para>
/// </summary>
public class ServerHealthStatusCollectorVerdictTests
{
    [Fact]
    public void OnlineServer_WithNoCollectorBanded_IsUnknown_NotGreenOk()
    {
        var unbanded = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 0, FailedCollectorCount = 0 };
        Assert.Equal(HealthSeverity.Unknown, unbanded.CollectorSeverity);
        Assert.Equal("--", unbanded.CollectorDisplayText);
        Assert.Equal("No collector banded yet", unbanded.CollectorDetailText);

        /* A failing count with no healthy denominator is still a Warning: a failure was observed even if the
           population was not. */
        var failingOnly = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 0, FailedCollectorCount = 1 };
        Assert.Equal(HealthSeverity.Warning, failingOnly.CollectorSeverity);
        Assert.Equal("1 failed", failingOnly.CollectorDisplayText);

        /* One collector banded lifts the verdict — the (0, 0) arm is exactly zero, not a threshold. */
        var oneBanded = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 1, FailedCollectorCount = 0 };
        Assert.Equal(HealthSeverity.Healthy, oneBanded.CollectorSeverity);
        Assert.Equal("OK", oneBanded.CollectorDisplayText);
        Assert.Equal("Healthy: 1, Failing: 0", oneBanded.CollectorDetailText);
    }

    [Fact]
    public void FirstCollectionLanding_RaisesPropertyChanged_ForCollectorVerdict()
    {
        // The verdict now depends on the healthy count too (0 -> N is Unknown -> Healthy), so the first
        // collection landing must repaint the dot / value / detail, not only the detail line it used to.
        var status = new ServerHealthStatus(new ServerConnection()) { IsOnline = true, HealthyCollectorCount = 0, FailedCollectorCount = 0 };
        var raised = new List<string>();
        status.PropertyChanged += (_, e) => raised.Add(e.PropertyName ?? "");

        status.HealthyCollectorCount = 30;

        Assert.Equal(HealthSeverity.Healthy, status.CollectorSeverity);
        Assert.Contains(nameof(ServerHealthStatus.CollectorSeverity), raised);
        Assert.Contains(nameof(ServerHealthStatus.CollectorDisplayText), raised);
        Assert.Contains(nameof(ServerHealthStatus.CollectorDetailText), raised);
    }

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
