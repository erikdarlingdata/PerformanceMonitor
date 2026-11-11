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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8555, the alert notebook's small defects that need no store. A self-monitor recovery ("Capture Restored", "Capture Cleared",
/// "Collection Resumed") is matched to its firing like the #8095 Cleared family. An External alert that resolves to no server draws
/// no per-server read cell. The live round trip is <see cref="AlertNotebookSmallDefectsLiveTests"/>.
/// </summary>
public sealed class AlertNotebookSmallDefectsTests
{
    private static DarlingAlertReader.AlertHistoryReadRow Row(DateTime alertTime, string metric, int serverId = 7) =>
        new(alertTime, serverId, "SRV7", metric, 1.0, 1.0, true, "webhook", null, false, null, false, null);

    private static string Stamp(DateTime t) => t.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    [Theory]
    [InlineData("Capture Down", "Capture Restored")]
    [InlineData("Capture Down", "Capture Cleared")]
    [InlineData("Collection Stopped", "Collection Resumed")]
    public void ARecoveryRow_ReadsAsTheResolutionOfItsFiring(string firing, string recovery)
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var recovered = fired + TimeSpan.FromSeconds(75);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(recovered, recovery), Row(fired, firing) };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, recovery, recovered, matchedRow: rows[0], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolution of the " + firing + " alert that fired at " + Stamp(fired), status);
    }

    [Theory]
    [InlineData("Capture Down", "Capture Restored")]
    [InlineData("Capture Down", "Capture Cleared")]
    [InlineData("Collection Stopped", "Collection Resumed")]
    public void AFiringRow_ReadsResolvedAt_WhenItsRecoveryRowFollows(string firing, string recovery)
    {
        var fired = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var recovered = fired + TimeSpan.FromSeconds(75);
        var rows = new List<DarlingAlertReader.AlertHistoryReadRow> { Row(recovered, recovery), Row(fired, firing) };

        var status = AlertNotebookEndpoint.StatusFromHistory(rows, firing, fired, matchedRow: rows[1], serverId: 7, fleetLevelStore: false);

        Assert.Equal("Resolved at " + Stamp(recovered), status);
        Assert.Contains(recovery, AlertNotebookEndpoint.ResolutionRowNames(firing));
    }

    [Fact]
    public void AnExternalFiring_IsResolvedByItsOwnNamePlusResolved()
    {
        const string metric = "External:ci-runner:nightly";

        Assert.Equal(new[] { metric + " Resolved" }, AlertNotebookEndpoint.ResolutionRowNames(metric));
        Assert.Empty(AlertNotebookEndpoint.ResolutionRowNames(metric + " Resolved"));
        Assert.Equal(metric, AlertNotebookEndpoint.ResolvedMetricOf(metric + " Resolved"));
    }

    [Fact]
    public async Task AnExternalAlertWithNoResolvedServer_HasTheHeaderAndStatusAndNoReadCell()
    {
        var end = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        var (cells, _, _) = await AlertNotebookEndpoint.BuildCellsAsync(
            "External:ci-runner:nightly", "ci-runner", "2026-01-01T12:00:00Z", end, serverId: null, end, null!, null!, null, null, null, null,
            "No resolution recorded", "24", CancellationToken.None);

        Assert.Equal(new[] { "header", "status" }, cells.Select(c => c!["type"]!.GetValue<string>()).ToArray());
    }
}
