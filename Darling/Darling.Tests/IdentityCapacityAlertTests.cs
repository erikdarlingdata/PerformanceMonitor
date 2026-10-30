/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6875: the "Identity Capacity" self-alert. It judges each column by <see cref="IdentityCapacityMath.Band(double, double, double)"/> against the
/// bars that apply to its server and database, and these tests drive the evaluator with a controllable clock and a recording deliverer.
/// </summary>
public sealed class IdentityCapacityAlertTests
{
    private const int ServerId = 424243;
    private const string Name = "IDENTITY-SRV";
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A column known only by its stored percent (the collector stores the exact percent beside the value).</summary>
    private static IdentityColumnFact Column(string database, string table, double percent) =>
        new(database, "dbo", table, "Id", "int", 10, null, null, null, percent, T0, null, null);

    /// <summary>An int column 1.6 billion in, up 100 million over the last day: about five days left.</summary>
    private static IdentityColumnFact Moving(string database, string table) =>
        new(database, "dbo", table, "Id", "int", 10, 1_600_000_000, 1, 1, null, T0, 1_500_000_000, T0.AddDays(-1));

    private static IdentityCapacityRead Read(params IdentityColumnFact[] columns) =>
        new(columns, new[] { new IdentityScannedDatabase("Sales", T0) });

    [Fact]
    public async Task TheAlert_IsOffByDefault_InTheConfigAndInTheEvaluator()
    {
        var shipped = new AlertsConfig();
        Assert.False(shipped.IdentityCapacityEnabled);
        Assert.Equal(70, shipped.IdentityCapacityWarningPercent);
        Assert.Equal(90, shipped.IdentityCapacityCriticalPercent);

        var h = new DarlingSelfAlertTests.Harness();
        await h.Build().ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Sales", "Orders", 99)), Ct);
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        h.IdentityCapacityEnabled = true;
        await h.Build().ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Sales", "Orders", 99)), Ct);
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AColumnAtTheWarningBar_FiresOnce_NamingTheColumnItsPercentAndItsDaysLeft_ReFiresAfterTheCooldown_RisesToCritical_AndClearsWithOneRow()
    {
        var h = new DarlingSelfAlertTests.Harness { IdentityCapacityEnabled = true, ConnectionRefireMinutes = 30 };
        var e = h.Build();

        /* 74 percent of an int column that is moving: warning. A column under 70 percent is not named. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Moving("Sales", "Orders"), Column("Sales", "Quiet", 40)), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Identity Capacity", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Contains("Sales.dbo.Orders.Id: 74.5% used (warning), about 5 days to exhaustion.", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Quiet", fired.DetailText, StringComparison.Ordinal);

        /* It stands a minute later: still one send. */
        h.Now = h.Now.AddMinutes(1);
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Moving("Sales", "Orders")), Ct);
        Assert.Single(h.Deliverer.Outcomes);

        /* Past the cooldown it says so again. */
        h.Now = h.Now.AddMinutes(31);
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Moving("Sales", "Orders")), Ct);
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
        Assert.Contains("Still the case", h.Deliverer.Outcomes[1].DetailText, StringComparison.Ordinal);

        /* A column that reaches 90 percent is news straight away, at Critical, with no days for a column that has one sample. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Moving("Sales", "Orders"), Column("Sales", "Audit", 93.5)), Ct);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
        Assert.Equal(AlertSeverityLevel.Critical, h.Deliverer.Outcomes[2].Severity);
        Assert.Contains("Sales.dbo.Audit.Id: 93.5% used (critical), days to exhaustion unknown.", h.Deliverer.Outcomes[2].DetailText, StringComparison.Ordinal);

        /* Nothing at a bar any more: one audit row, no send, and a second pass writes nothing more. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Sales", "Orders", 12)), Ct);
        var cleared = Assert.Single(h.History.Records);
        Assert.Equal("Identity Capacity Cleared", cleared.MetricName);
        Assert.False(cleared.AlertSent);
        Assert.Equal(3, h.Deliverer.Outcomes.Count);
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Sales", "Orders", 12)), Ct);
        Assert.Single(h.History.Records);

        /* A new occurrence after the clear is a new alert. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Sales", "Orders", 75)), Ct);
        Assert.Equal(4, h.Deliverer.Outcomes.Count);
    }

    [Fact]
    public async Task APerDatabaseOverride_MovesTheBarsOfThatDatabaseOnly_AndNoScanJudgesNothing()
    {
        var overrides = new Dictionary<(string Wire, string? Database), int>
        {
            [(DarlingThresholdKeys.IdentityCapacityWarning, "Archive")] = 95,
        };
        var h = new DarlingSelfAlertTests.Harness
        {
            IdentityCapacityEnabled = true,
            ResolveThreshold = (wire, _, database, global) => overrides.TryGetValue((wire, database), out var v) ? v : global,
        };
        var e = h.Build();

        /* Archive is allowed to run to 95 percent, so 80 percent there is quiet, and the critical bar follows it up to 95. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Archive", "Old", 80), Column("Archive", "Older", 93)), Ct);
        Assert.Empty(h.Deliverer.Outcomes);

        /* The same percent in another database is not covered by it. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, Read(Column("Archive", "Old", 80), Column("Sales", "Orders", 80)), Ct);
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("Sales.dbo.Orders.Id", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Archive", fired.DetailText, StringComparison.Ordinal);

        /* No scan in the window: nothing is judged, so the standing alert is neither repeated nor cleared. */
        await e.ApplyIdentityCapacityAsync(ServerId, Name, null, Ct);
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Empty(h.History.Records);

        /* The threshold registry holds both bars with the server-and-database scope. */
        Assert.True(DarlingThresholdKeys.TryGet(DarlingThresholdKeys.IdentityCapacityWarning, out var warning));
        Assert.Equal(AlertThresholdScope.ServerAndDatabase, warning.Scope);
        Assert.True(DarlingThresholdKeys.TryGet(DarlingThresholdKeys.IdentityCapacityCritical, out var critical));
        Assert.Equal(AlertThresholdScope.ServerAndDatabase, critical.Scope);
    }

    [Fact]
    public void TheBandWithBars_MatchesTheDefaultBandAtTheDefaultBars_AndMovesWithTheBars()
    {
        foreach (var percent in new[] { 0d, 69.9999, 70, 89.9999, 90, 100 })
            Assert.Equal(IdentityCapacityMath.Band(percent), IdentityCapacityMath.Band(percent, 70, 90));
        Assert.Equal("ok", IdentityCapacityMath.Band(80, 85, 95));
        Assert.Equal("warning", IdentityCapacityMath.Band(85, 85, 95));
        Assert.Equal("critical", IdentityCapacityMath.Band(95, 85, 95));
    }
}
