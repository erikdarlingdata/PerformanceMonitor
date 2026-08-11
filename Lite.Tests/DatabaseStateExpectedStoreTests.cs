/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the baseline-deviation database-state alert's store surface
/// (<see cref="LocalDataService.GetDatabaseStateDeviationsAsync"/> — the alert read — plus the override
/// editor's <see cref="LocalDataService.GetDatabaseStateExpectationsAsync"/> /
/// <see cref="LocalDataService.SetDatabaseStateExpectedAsync"/> /
/// <see cref="LocalDataService.ResetDatabaseStateExpectedToCurrentAsync"/>). The write paths are
/// covered deliberately: an early cut used a bare <c>current_timestamp</c> inside an INSERT VALUES row
/// and an ON CONFLICT DO UPDATE SET, which DuckDB binds as a column reference and rejects at runtime —
/// no unit test would have caught it without exercising these exact upserts against a real store.
/// </summary>
public sealed class DatabaseStateExpectedStoreTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 7788;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public DatabaseStateExpectedStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>Seeds one database_states snapshot (all rows share collection_time; each row a unique collection_id).</summary>
    private async Task SeedSnapshotAsync(DateTime when, params (string Db, string State, bool Standby)[] dbs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        int dbId = 1;
        foreach (var (db, state, standby) in dbs)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = when });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "DEMO-SQL" });
            cmd.Parameters.Add(new DuckDBParameter { Value = db });
            cmd.Parameters.Add(new DuckDBParameter { Value = dbId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = state });
            cmd.Parameters.Add(new DuckDBParameter { Value = standby });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Writes a baseline row DIRECTLY, which is the only way to reproduce a row the OLD seeder guessed
    /// (#2189). <c>SetDatabaseStateExpectedAsync</c> always stamps <c>is_user_override = true</c>, so it
    /// cannot express "the product guessed this" — and that distinction is the whole thing the repair keys on.
    /// </summary>
    private async Task SetExpectedAsync(string database, string expectedState, bool userOverride)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
VALUES ($1, $2, $3, $4, now()::TIMESTAMP)";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = expectedState });
        cmd.Parameters.Add(new DuckDBParameter { Value = userOverride });
        await cmd.ExecuteNonQueryAsync();
    }

    private static readonly DateTime T0 = new(2026, 8, 1, 9, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task FirstObservation_SeedsBaseline_AndReportsNoDeviation()
    {
        await SeedSnapshotAsync(T0, ("master", "ONLINE", false), ("App", "ONLINE", false), ("LogShip", "RESTORING", true));
        var service = new LocalDataService(_duckDb);

        var deviations = await service.GetDatabaseStateDeviationsAsync(ServerId);
        Assert.Empty(deviations); // one snapshot -> no second sample -> nothing fires

        var rows = await service.GetDatabaseStateExpectationsAsync(ServerId);
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.False(r.IsUserOverride));
        Assert.All(rows, r => Assert.Equal(r.CurrentState, r.ExpectedState));
        // A standby log-shipping secondary baselines at the effective STANDBY state, not its raw state_desc.
        Assert.Equal("STANDBY", rows.Single(r => r.DatabaseName == "LogShip").ExpectedState);
    }

    [Fact]
    public async Task StateChange_DoesNotFireOnASingleSample_ThenFiresOnTwoConsecutive()
    {
        await SeedSnapshotAsync(T0, ("App", "ONLINE", false));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId); // baseline App = ONLINE

        // One OFFLINE sample (a transient — e.g. mid-restart) must NOT fire.
        await SeedSnapshotAsync(T0.AddMinutes(1), ("App", "OFFLINE", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        // A second consecutive OFFLINE — the condition stuck — fires.
        await SeedSnapshotAsync(T0.AddMinutes(2), ("App", "OFFLINE", false));
        var app = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("App", app.DatabaseName);
        Assert.Equal("OFFLINE", app.StateDesc);
        Assert.Equal("ONLINE", app.ExpectedState);
    }

    /// <summary>Seeds a baseline then two consecutive snapshots in <paramref name="state"/>, so the two-sample
    /// deviation rule fires for the single database "App".</summary>
    private async Task DriveAppToStableStateAsync(LocalDataService service, string state)
    {
        await SeedSnapshotAsync(T0, ("App", "ONLINE", false));
        await service.GetDatabaseStateDeviationsAsync(ServerId); // baseline ONLINE
        await SeedSnapshotAsync(T0.AddMinutes(1), ("App", state, false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("App", state, false));
    }

    [Fact]
    public async Task SetExpected_Ignore_SuppressesTheDeviation()
    {
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");
        Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId)); // deviating (two samples)

        await service.SetDatabaseStateExpectedAsync(ServerId, "App", PerformanceMonitor.Alerting.DatabaseStateTokens.Ignore);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // ignored → suppressed
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.True(row.IsUserOverride);
        Assert.True(row.IsIgnored);
    }

    [Fact]
    public async Task SetExpected_ToSpecificState_StopsDeviationForThatState()
    {
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "RESTORING");

        // Accept RESTORING as the new expected state (an override, not the baseline).
        await service.SetDatabaseStateExpectedAsync(ServerId, "App", PerformanceMonitor.Alerting.DatabaseStateTokens.Restoring);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.True(row.IsUserOverride);
        Assert.Equal("RESTORING", row.ExpectedState);
    }

    [Fact]
    public async Task ResetToCurrent_RebaselinesAndClearsOverride()
    {
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");
        Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));

        await service.ResetDatabaseStateExpectedToCurrentAsync(ServerId, "App");

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // expected now == current (OFFLINE)
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.False(row.IsUserOverride);
        Assert.Equal("OFFLINE", row.ExpectedState);
    }

    [Fact]
    public async Task CriticalFirstObservation_IsNotBaselined_AndAlertsAsPending()
    {
        // Two consecutive SUSPECT samples (a stuck outage, not a restart transient).
        await SeedSnapshotAsync(T0, ("Payments", "SUSPECT", false), ("App", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(1), ("Payments", "SUSPECT", false), ("App", "ONLINE", false));
        var service = new LocalDataService(_duckDb);

        var suspect = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId)); // App baselined, quiet
        Assert.Equal("Payments", suspect.DatabaseName);
        Assert.Equal("SUSPECT", suspect.StateDesc);
        Assert.Equal("", suspect.ExpectedState); // pending — a critical first observation writes NO baseline

        var rows = await service.GetDatabaseStateExpectationsAsync(ServerId);
        Assert.Equal("", rows.Single(r => r.DatabaseName == "Payments").ExpectedState);
        Assert.Equal("ONLINE", rows.Single(r => r.DatabaseName == "App").ExpectedState);
    }

    [Fact]
    public async Task CriticalTransient_DoesNotFire_WhenRecoveryCompletesWithinOneSample()
    {
        // A restart: RECOVERY_PENDING for one collection, then ONLINE. The two-sample rule keeps it silent.
        await SeedSnapshotAsync(T0, ("App", "RECOVERY_PENDING", false));
        var service = new LocalDataService(_duckDb);
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // one sample, nothing fires

        await SeedSnapshotAsync(T0.AddMinutes(1), ("App", "ONLINE", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // recovered before a second critical sample

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState); // baselined ONLINE once healthy
    }

    [Fact]
    public async Task PendingCritical_AutoAcceptsBaseline_WhenItRecoversToNonCritical()
    {
        await SeedSnapshotAsync(T0, ("Payments", "SUSPECT", false));
        await SeedSnapshotAsync(T0.AddMinutes(1), ("Payments", "SUSPECT", false));
        var service = new LocalDataService(_duckDb);
        Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId)); // pending, alerts

        await SeedSnapshotAsync(T0.AddMinutes(2), ("Payments", "ONLINE", false));

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // recovered -> baselines ONLINE
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState);
        Assert.False(row.IsUserOverride);
    }

    [Fact]
    public async Task MidRestoreFirstObservation_IsNotBaselined_ThenBaselinesWhatItSettlesInto()
    {
        // #2189: RESTORING is as transient as RECOVERY_PENDING, and baselining it inverts the alert forever —
        // the database then "deviates" by being healthy. Observed on the production fleet as 636 alerts in 24
        // hours from 5 databases reading "Expected: RESTORING, Current: ONLINE".
        await SeedSnapshotAsync(T0, ("Restoring", "RESTORING", false));
        var service = new LocalDataService(_duckDb);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("", (await service.GetDatabaseStateExpectationsAsync(ServerId))
            .Single(r => r.DatabaseName == "Restoring").ExpectedState); // pending, NOT baselined RESTORING

        // Restore finishes. Now there is a real steady state to learn.
        await SeedSnapshotAsync(T0.AddMinutes(1), ("Restoring", "ONLINE", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState);
        Assert.False(row.IsUserOverride);
    }

    [Fact]
    public async Task AlreadyPoisonedRestoringBaseline_IsRepairedAndReseeded_InOneCall()
    {
        // The seed is insert-if-absent, so it can never correct a row written before the #2189 fix. The repair
        // statement runs BEFORE the seed precisely so a poisoned baseline is dropped and the real steady state
        // re-learned in the SAME call, rather than leaving the database with no baseline for a cycle.
        await SeedSnapshotAsync(T0, ("Restored", "ONLINE", false));
        var service = new LocalDataService(_duckDb);

        // Write the poisoned baseline the way the old seeder would have: guessed, not an operator override.
        await SetExpectedAsync("Restored", "RESTORING", userOverride: false);
        Assert.Equal("RESTORING", (await service.GetDatabaseStateExpectationsAsync(ServerId))
            .Single().ExpectedState);

        // Reading deviations runs repair-then-seed. Healthy database, so it must end up ONLINE and silent.
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState);
        Assert.False(row.IsUserOverride);
    }

    [Fact]
    public async Task OperatorOverrideOnATransientState_SurvivesTheRepair()
    {
        // Somebody who deliberately sets a database's expectation to RESTORING — a permanently log-shipped
        // target, say — means it. The repair is scoped to guessed baselines only, and this is the assertion
        // that keeps it that way.
        await SeedSnapshotAsync(T0, ("Deliberate", "RESTORING", false));
        var service = new LocalDataService(_duckDb);
        await SetExpectedAsync("Deliberate", "RESTORING", userOverride: true);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("RESTORING", row.ExpectedState);
        Assert.True(row.IsUserOverride);
    }

    [Fact]
    public async Task StandbySecondary_BaselinesAsStandby_AndDoesNotChurnOnLogRestores()
    {
        // A standby log-shipping secondary: is_in_standby stays 1 while state_desc flickers ONLINE/RESTORING
        // on each log restore. The effective STANDBY state is stable, so it baselines STANDBY and never fires.
        await SeedSnapshotAsync(T0, ("LogShip", "ONLINE", true));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId);

        await SeedSnapshotAsync(T0.AddMinutes(1), ("LogShip", "RESTORING", true));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("LogShip", "RESTORING", true));

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // STANDBY == STANDBY, no churn
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("STANDBY", row.ExpectedState);
        Assert.Equal("STANDBY", row.CurrentState);
    }
}
