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
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7647: a server with databases in RECOVERY_PENDING read Healthy on the Fleet page, because nothing on the card read database state. A state the Database
/// State alert grades Critical (SUSPECT, RECOVERY_PENDING, EMERGENCY) now bands the server Critical, any other deviation adds a "Database state" tile and a
/// Needs attention reason without moving the band, and a server whose database state is not collected says so rather than reading healthy or zero.
/// </summary>
public sealed class DatabaseStateBandsTheServerTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private static FleetServerCard Card(int serverId, string name, FleetDatabaseStates? states = null) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(serverId, name, name, null, null, false),
            default,
            default,
            default,
            default,
            default,
            new DarlingFleetReader.BlockingRow(0, 0, 0, 0),
            new DarlingFleetReader.DeadlockRow(0, null),
            new DarlingFleetReader.PgDeadlockRow(0, null, 0),
            Now.AddSeconds(-30),
            new DarlingFleetReader.CollectorCounts(40, 0, 40, 0, null, null),
            null,
            Now,
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default,
            databaseStates: states);

    private static FleetDatabaseStates States(params (string Db, string State)[] deviations) =>
        FleetDatabaseStates.Of(new DatabaseStateRead(true, deviations.Select(d => new DatabaseStateDeviation(d.Db, d.State, "ONLINE")).ToList()));

    [Fact]
    public void ADatabaseInRecoveryPending_BandsTheServerCritical_WithAReasonNamingIt()
    {
        var healthy = Card(1, "plain", States());
        var broken = Card(2, "restarted", States(("Sales", "RECOVERY_PENDING")));

        Assert.Equal(FleetHealthBand.Healthy, healthy.Band);
        Assert.Equal(FleetHealthBand.Critical, broken.Band);
        Assert.Equal(HealthSeverity.Critical, broken.OverallMetricSeverity);
        Assert.Equal(healthy.MeasuredMetricCount, broken.MeasuredMetricCount);

        var rollup = DarlingFleetReader.BuildRollup(new[] { healthy, broken }, Now, Now.AddHours(-1), Now);
        Assert.Equal(1, rollup.CriticalCount);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup, DarlingFleetReader.JsonOptions));
        var ranked = doc.RootElement.GetProperty("worst_servers")[0];
        Assert.Equal("restarted", ranked.GetProperty("display_name").GetString());
        Assert.Equal("Critical", ranked.GetProperty("band").GetString());
        var reason = ranked.GetProperty("reason").GetString();
        Assert.Contains("Database state critical", reason, StringComparison.Ordinal);
        Assert.Contains("Sales RECOVERY_PENDING", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineDatabase_AddsTheTileAndTheReason_ButDoesNotBandTheServerCritical()
    {
        var offline = Card(1, "archive-offline", States(("Archive", "OFFLINE")));

        Assert.NotEqual(HealthSeverity.Critical, offline.OverallMetricSeverity);
        Assert.Equal(new[] { "Database state" }, offline.AdvisoryTiles);
        Assert.True(offline.HasAdvisoryTile);

        var rollup = DarlingFleetReader.BuildRollup(new[] { offline }, Now, Now.AddHours(-1), Now);
        Assert.Equal(0, rollup.CriticalCount);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup, DarlingFleetReader.JsonOptions));
        var ranked = doc.RootElement.GetProperty("worst_servers")[0];
        Assert.Equal("Healthy", ranked.GetProperty("band").GetString());
        Assert.Contains("Archive OFFLINE", ranked.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheReason_NamesAFewDatabases_ThenAndNMore_CriticalOnesFirst()
    {
        var card = Card(1, "many", States(("A", "OFFLINE"), ("B", "OFFLINE"), ("C", "OFFLINE"), ("D", "SUSPECT"), ("E", "OFFLINE")));

        Assert.Equal(1, card.DatabaseStates!.CriticalCount);
        Assert.Equal(5, card.DatabaseStates.DeviationCount);
        Assert.Equal("D", card.DatabaseStates.Deviations[0].Database);
        Assert.Equal("Database state critical (D SUSPECT, A OFFLINE, B OFFLINE and 2 more)", card.DatabaseStates.Reason);
    }

    [Fact]
    public void AServerWhoseDatabaseStateIsNotCollected_IsNeitherHealthyNorZero_AndKeepsItsBand()
    {
        var uncollected = Card(1, "no-states", FleetDatabaseStates.Of(new DatabaseStateRead(false, Array.Empty<DatabaseStateDeviation>())));

        Assert.False(uncollected.DatabaseStates!.Collected);
        Assert.Equal(0, uncollected.DatabaseStates.DeviationCount);
        Assert.Empty(uncollected.AdvisoryTiles);
        Assert.Equal(FleetHealthBand.Healthy, uncollected.Band);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(uncollected, DarlingFleetReader.JsonOptions));
        Assert.False(doc.RootElement.GetProperty("database_states").GetProperty("collected").GetBoolean());
    }

    [Fact]
    public void TheViewersCard_BandsAndExplainsOnTheSameRule()
    {
        ServerSummaryItem Viewer(DatabaseStateRead read)
        {
            var item = new ServerSummaryItem
            {
                ServerName = "v",
                DisplayName = "v",
                ServerId = 1,
                HealthyCollectorCount = 40,
                CollectorCount = 40,
                DeadlockWindow = TimeSpan.FromHours(1),
                BlockingWindow = TimeSpan.FromHours(1),
                LastCollectionTime = Now.AddSeconds(-30),
                DatabaseStates = read,
            };
            item.ApplyFreshness(Now);
            return item;
        }

        var critical = Viewer(new DatabaseStateRead(true, new[] { new DatabaseStateDeviation("Sales", "RECOVERY_PENDING", "") }));
        Assert.Equal(HealthSeverity.Critical, critical.OverallMetricSeverity);
        Assert.Contains("Sales RECOVERY_PENDING", critical.DatabaseStateReason, StringComparison.Ordinal);

        var offline = Viewer(new DatabaseStateRead(true, new[] { new DatabaseStateDeviation("Archive", "OFFLINE", "ONLINE") }));
        Assert.NotEqual(HealthSeverity.Critical, offline.OverallMetricSeverity);
        Assert.Equal(new[] { "Database state" }, offline.AdvisoryTiles);

        var uncollected = Viewer(new DatabaseStateRead(false, Array.Empty<DatabaseStateDeviation>()));
        Assert.NotEqual(HealthSeverity.Critical, uncollected.OverallMetricSeverity);
        Assert.Empty(uncollected.AdvisoryTiles);
        Assert.Equal("", uncollected.DatabaseStateReason);
    }
}
