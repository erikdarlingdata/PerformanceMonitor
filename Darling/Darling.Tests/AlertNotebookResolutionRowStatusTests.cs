/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8095 item 3: the alert notebook's status line for a row that is itself a resolution ("Blocking Cleared", "CPU Resolved"), the row
/// the Alert History "Open" link passes for a Cleared entry. The status arms look for a resolution of the metric they were given after
/// the anchor, so a resolution row found none and read "No resolution recorded" about the alert that is the resolution. The pure
/// <see cref="AlertNotebookEndpoint.StatusFromHistory"/> seam answers for such a row before the other arms run.
/// </summary>
public sealed class AlertNotebookResolutionRowStatusTests
{
    private static DarlingAlertReader.AlertHistoryReadRow Row(DateTime alertTime, string metric, int serverId = 7) =>
        new(alertTime, serverId, "SRV7", metric, 90.0, 80.0, true, "webhook", null, false, null, false, null);

    private static string Stamp(DateTime t) => t.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    [Fact]
    public void AResolutionRow_ReadsAsTheResolutionOfTheFiringBeforeIt()
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var cleared = fired + TimeSpan.FromMinutes(25);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(cleared, "Blocking Cleared"), Row(fired, "Blocking Detected") };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, "Blocking Cleared", cleared, matchedRow: rows[0], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolution of the Blocking Detected alert that fired at " + Stamp(fired), status);
    }

    [Fact]
    public void AResolutionRow_WithNoFiringInTheRowsGiven_StillReadsAsAResolution()
    {
        var cleared = new DateTime(2026, 1, 1, 10, 25, 0, DateTimeKind.Utc);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(cleared, "CPU Resolved") };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, "CPU Resolved", cleared, matchedRow: rows[0], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolution of a High CPU alert", status);
    }

    [Fact]
    public void AResolutionRow_NamedByTheMetricPlusCleared_IsReadTheSameWay()
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var cleared = fired + TimeSpan.FromMinutes(5);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(cleared, "Client Timeouts Cleared"), Row(fired, "Client Timeouts") };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, "Client Timeouts Cleared", cleared, matchedRow: rows[0], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolution of the Client Timeouts alert that fired at " + Stamp(fired), status);
    }

    [Fact]
    public void ARecoveryRow_ReadsAsTheResolutionOfTheFiringBeforeIt()
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var restored = fired + TimeSpan.FromMinutes(10);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(restored, "Server Restored"), Row(fired, "Server Unreachable") };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, "Server Restored", restored, matchedRow: rows[0], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolution of the Server Unreachable alert that fired at " + Stamp(fired), status);
    }

    [Fact]
    public void AFiringRow_KeepsTheResolvedArm()
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var cleared = fired + TimeSpan.FromMinutes(25);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(cleared, "Blocking Cleared"), Row(fired, "Blocking Detected") };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, "Blocking Detected", fired, matchedRow: rows[1], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolved at " + Stamp(cleared), status);
    }
}
