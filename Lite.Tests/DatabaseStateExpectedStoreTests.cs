/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
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

    private static readonly DateTime T0 = new(2026, 8, 1, 9, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// Runs the deviation sweep until <paramref name="settled"/> holds, up to <paramref name="cycles"/> times
    /// (#2266). Returns the LAST result either way, so a real regression still fails on its own assertion with
    /// its own message.
    ///
    /// <para><b>Why any retry is correct here rather than a tolerance hack.</b>
    /// <c>GetDatabaseStateDeviationsAsync</c> does its seeding, the #2189 heal, the #2203 forget and the prune
    /// inside a BEST-EFFORT block: it opens the write connection with a 5-second lock acquisition and, on
    /// <c>TimeoutException</c>, skips the whole maintenance block and runs the deviation read anyway. That is
    /// deliberate and documented — skipping is the only lossless option when archival holds the lock. The write
    /// lock is <b>static, shared by the whole process</b> (the method's own comment says so), and xunit runs
    /// test classes in parallel, so another class can hold it long enough for a cycle here to skip its
    /// maintenance.</para>
    ///
    /// <para>The observed flake is exactly that: <c>ExpectedState = SUSPECT</c> with <c>StateDesc = ONLINE</c> —
    /// a combination only reachable by skipping the heal while completing the read. So asserting the heal lands
    /// in ONE cycle asserts something the design does not promise; asserting it lands within a FEW cycles is the
    /// contract. No tuned number is involved: the semantics are "eventually", and the count only needs to
    /// exceed one.</para>
    ///
    /// <para>It cannot mask a regression, which is the property that makes it acceptable in 23 tests' worth of
    /// company: a heal that is genuinely broken never settles, all cycles run, and the caller's own assertion
    /// fails on the final result exactly as it does today.</para>
    /// </summary>
    private static async Task<List<DatabaseStateInfo>> SweepUntilAsync(
        LocalDataService service,
        Func<List<DatabaseStateInfo>, bool> settled,
        int cycles = 5)
    {
        List<DatabaseStateInfo> result;
        do
        {
            result = await service.GetDatabaseStateDeviationsAsync(ServerId);
        }
        while (!settled(result) && --cycles > 0);

        return result;
    }

    /// <summary>
    /// Writes an expectation row directly, bypassing the service's writers — the only way to reproduce a
    /// row the OLD seed wrote (#2189), since no current code path can produce one any more.
    /// </summary>
    private async Task SeedExpectedAsync(string database, string expected, bool isOverride)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, $2, $3, $4, now()::TIMESTAMP";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = expected });
        cmd.Parameters.Add(new DuckDBParameter { Value = isOverride });
        await cmd.ExecuteNonQueryAsync();
    }

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

    /* ---------------- #2189: transient states are never learned, and ONLINE un-learns a stale one ---------------- */

    [Fact]
    public async Task OnboardedMidRestore_NeverLearnsRestoring_AndIsSilentBeforeAndAfterTheRestoreCompletes()
    {
        /* #2189, the reported bug, end to end. A database swept into monitoring during a consolidation is
           mid-restore, not in a steady state anybody chose — so nothing is learned while it restores, and
           when the restore finishes the STEADY state is what gets learned. The old seed learned RESTORING
           here and the database then "deviated" by being healthy, forever. */
        var service = new LocalDataService(_duckDb);

        await SeedSnapshotAsync(T0, ("App", "RESTORING", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        await SeedSnapshotAsync(T0.AddMinutes(1), ("App", "RESTORING", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("App", "RESTORING", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId)); // a restore in progress is not news

        var pending = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("", pending.ExpectedState);

        await SeedSnapshotAsync(T0.AddMinutes(3), ("App", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(4), ("App", "ONLINE", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        var settled = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", settled.ExpectedState);
        Assert.False(settled.IsUserOverride);
    }

    [Fact]
    public async Task TransientStates_AreNeverLearnedAsABaseline_RestoringAndRecovering()
    {
        /* Both halves of "mid-something": RESTORING is a restore in flight, RECOVERING a database still
           coming up. Neither is a steady state, and the states around them are unaffected — a healthy
           database still baselines on its first observation. */
        await SeedSnapshotAsync(T0,
            ("Restoring", "RESTORING", false), ("Recovering", "RECOVERING", false), ("Healthy", "ONLINE", false));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId);

        var rows = await service.GetDatabaseStateExpectationsAsync(ServerId);
        Assert.Equal("", rows.Single(r => r.DatabaseName == "Restoring").ExpectedState);
        Assert.Equal("", rows.Single(r => r.DatabaseName == "Recovering").ExpectedState);
        Assert.Equal("ONLINE", rows.Single(r => r.DatabaseName == "Healthy").ExpectedState);
    }

    [Fact]
    public async Task PoisonedRestoringBaseline_HealsToOnline_RatherThanAlertingForeverOnBeingHealthy()
    {
        /* The other half of #2189, and the half the widened seed cannot reach: rows that were ALREADY
           written. Five databases on the reporting fleet sat like this — baselined RESTORING during a
           consolidation, then ~127 identical alerts each in 24 hours for the crime of being ONLINE. No
           current code path can write this row any more, so it is planted directly. */
        await SeedExpectedAsync("pecan", "RESTORING", isOverride: false);
        await SeedSnapshotAsync(T0, ("pecan", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(1), ("pecan", "ONLINE", false));
        var service = new LocalDataService(_duckDb);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState);
        Assert.False(row.IsUserOverride);
    }

    [Fact]
    public async Task RebaselinedByHandMidRestore_StillHealsOnceTheRestoreCompletes()
    {
        /* The seed is not the only way into a transient baseline: "reset to current" pressed while a restore
           is running writes exactly the same poisoned row, and always will. The heal is what makes that a
           self-correcting mistake instead of a trap armed for the next operator. */
        await SeedSnapshotAsync(T0, ("App", "RESTORING", false));
        var service = new LocalDataService(_duckDb);
        await service.ResetDatabaseStateExpectedToCurrentAsync(ServerId, "App");
        Assert.Equal("RESTORING", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);

        await SeedSnapshotAsync(T0.AddMinutes(1), ("App", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("App", "ONLINE", false));

        /* #2266: same best-effort maintenance exposure as the outage sibling below — structurally identical
           test, so it can flake the same way even though only the SUSPECT one has been seen to. */
        Assert.Empty(await SweepUntilAsync(service, deviations => deviations.Count == 0));
        Assert.Equal("ONLINE", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);
    }

    [Fact]
    public async Task RebaselinedByHandDuringAnOutage_HealsOnceTheDatabaseRecovers()
    {
        /* The seed is not the only writer of inferred baselines, and never will be: "reset to current"
           records whatever it sees with NO state filter, so pressing it during an outage writes SUSPECT as
           the accepted normal. That silences the database while it is corrupt (the operator's own doing) and
           then, without this, makes it deviate by RECOVERING. Same shape as the restore case, integrity
           flavour — which is why the heal keys off the seed's refusal list rather than RESTORING alone. */
        await SeedSnapshotAsync(T0, ("Payments", "SUSPECT", false));
        var service = new LocalDataService(_duckDb);
        await service.ResetDatabaseStateExpectedToCurrentAsync(ServerId, "Payments");
        var planted = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("SUSPECT", planted.ExpectedState);
        Assert.False(planted.IsUserOverride);

        await SeedSnapshotAsync(T0.AddMinutes(1), ("Payments", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("Payments", "ONLINE", false));

        /* #2266: the heal rides a best-effort maintenance block, so a cycle whose write-lock acquisition times
           out against another parallel test class skips it silently. Sweep until it lands. */
        Assert.Empty(await SweepUntilAsync(service, deviations => deviations.Count == 0));
        Assert.Equal("ONLINE", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);
    }

    [Fact]
    public async Task AutoBaselinedOffline_IsNeverHealed_SoParkingItAgainStaysQuiet()
    {
        /* The heal is deliberately NOT "ONLINE overwrites anything the machine inferred". OFFLINE is a steady
           state the seed is happy to learn, so it is a legitimate baseline, and rewriting it on the first
           ONLINE sighting would be this bug inverted: bring a parked database up for an hour of maintenance,
           re-park it, and it now deviates forever against a baseline it never had - which in Lite, with no
           persisted alerted-state memory to edge-trigger against, means an alert every cooldown for good.

           Coming UP still alerts, because that is a real departure from the accepted normal. Going back to it
           is silence, and the baseline is the same one it started with. */
        await SeedSnapshotAsync(T0, ("Parked", "OFFLINE", false));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId); // learns OFFLINE

        await SeedSnapshotAsync(T0.AddMinutes(1), ("Parked", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("Parked", "ONLINE", false));
        var up = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("ONLINE", up.StateDesc);
        Assert.Equal("OFFLINE", up.ExpectedState);

        await SeedSnapshotAsync(T0.AddMinutes(3), ("Parked", "OFFLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(4), ("Parked", "OFFLINE", false));

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("OFFLINE", row.ExpectedState);
        Assert.False(row.IsUserOverride);
    }

    [Fact]
    public async Task StandbySecondaryRecoveredOutOfStandby_StillAlerts_BecauseItHasStoppedBeingASecondary()
    {
        /* The other state the heal must not touch, and the sharper of the two. A STANDBY secondary that turns
           up truly ONLINE (is_in_standby now 0) has been RECOVERED - log shipping is broken and that is
           exactly what this alert exists to say. Healing it would swap that alert for silence and then fire
           when the operator re-established standby, announcing the repair instead of the break. */
        await SeedSnapshotAsync(T0, ("LogShip", "ONLINE", true));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId); // learns STANDBY

        await SeedSnapshotAsync(T0.AddMinutes(1), ("LogShip", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("LogShip", "ONLINE", false));

        var fired = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("ONLINE", fired.StateDesc);
        Assert.Equal("STANDBY", fired.ExpectedState);
        Assert.Equal("STANDBY", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);
    }

    [Fact]
    public async Task OperatorParkedOffline_StillAlertsWhenTheDatabaseComesBackOnline()
    {
        /* #2166's composition contract, which the heal must not eat. The heal second-guesses only what the
           machine inferred; an operator who DECLARED an expected state meant it, so a parked database coming
           back ONLINE is a deviation from a real intent and still fires — and the override survives the sweep
           rather than being quietly rewritten to ONLINE underneath them. */
        await SeedSnapshotAsync(T0, ("Parked", "OFFLINE", false));
        var service = new LocalDataService(_duckDb);
        await service.SetDatabaseStateExpectedAsync(ServerId, "Parked", PerformanceMonitor.Alerting.DatabaseStateTokens.Offline);

        await SeedSnapshotAsync(T0.AddMinutes(1), ("Parked", "ONLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("Parked", "ONLINE", false));

        var fired = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("ONLINE", fired.StateDesc);
        Assert.Equal("OFFLINE", fired.ExpectedState);

        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.True(row.IsUserOverride);
        Assert.Equal("OFFLINE", row.ExpectedState);
    }

    [Fact]
    public async Task NorecoveryLogShippingSecondary_StaysQuietForever_AndCanBeOptedIntoCoverageByHand()
    {
        /* The #1986 property that must survive #2189: a log-shipping secondary restored WITH NORECOVERY sits
           in RESTORING permanently (is_in_standby is 0 — only a read-only STANDBY secondary sets that), and
           must never page. It now gets there by staying PENDING rather than by learning RESTORING, which is
           silent for the same reason: the no-baseline arm only alerts on the integrity states.

           The cost is that a pending database has no baseline to deviate FROM, so the operator who wants
           deviation coverage on a permanent secondary declares it — and that being an override is the point,
           since it is a genuine choice about a database only they can classify. */
        var service = new LocalDataService(_duckDb);
        for (int minute = 0; minute < 6; minute++)
        {
            await SeedSnapshotAsync(T0.AddMinutes(minute), ("Secondary", "RESTORING", false));
            Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        }

        Assert.Equal("", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);

        await service.SetDatabaseStateExpectedAsync(ServerId, "Secondary", PerformanceMonitor.Alerting.DatabaseStateTokens.Restoring);
        await SeedSnapshotAsync(T0.AddMinutes(6), ("Secondary", "OFFLINE", false));
        await SeedSnapshotAsync(T0.AddMinutes(7), ("Secondary", "OFFLINE", false));

        var fired = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("OFFLINE", fired.StateDesc);
        Assert.Equal("RESTORING", fired.ExpectedState);
    }

    [Fact]
    public async Task StandbySecondary_IsNeverHealedToOnline_BecauseItsEffectiveStateIsStandby()
    {
        /* The trap inside the heal. A standby secondary reports state_desc = 'ONLINE' with is_in_standby set,
           so a heal written against the RAW column would re-baseline every log-shipping secondary from
           STANDBY to ONLINE and then alert it forever for being STANDBY — #2189 recreated for exactly the
           database family #1986 works hardest to keep quiet. Matching the EFFECTIVE state is what prevents it. */
        await SeedSnapshotAsync(T0, ("LogShip", "ONLINE", true));
        var service = new LocalDataService(_duckDb);
        await service.GetDatabaseStateDeviationsAsync(ServerId); // baselines STANDBY

        await SeedSnapshotAsync(T0.AddMinutes(1), ("LogShip", "ONLINE", true));
        await SeedSnapshotAsync(T0.AddMinutes(2), ("LogShip", "RESTORING", true));

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("STANDBY", (await service.GetDatabaseStateExpectationsAsync(ServerId)).Single().ExpectedState);
    }

    [Fact]
    public async Task AlertedState_RoundTripsToTheDeviationRead_SoTheEdgeTriggerCanEngage()
    {
        // #2203: until Lite persisted this, `alreadyAnnounced` was always false here and a database parked
        // OFFLINE for a month alerted every cooldown forever — the original #2166 complaint, still live in
        // Lite after the Darling half shipped. The whole feature depends on this value surviving the round
        // trip, so pin the trip rather than the write.
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");

        var before = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("", before.LastAlertedState); // never announced yet

        var store = new DuckDbAlertHistoryStore(_duckDb);
        await store.SaveDatabaseStateAlertedAsync(ServerId, "App", "OFFLINE");

        var after = Assert.Single(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Equal("OFFLINE", after.LastAlertedState);
        Assert.Equal("OFFLINE", after.StateDesc); // still deviating — the engine is what goes quiet, not the read
    }

    [Fact]
    public async Task RecoveredDatabase_HasItsAlertedStateClearedByTheStore_NotJustByTheEngine()
    {
        // The restart-gap invariant, and the reason this clear is store-derived rather than engine-derived.
        // The engine also clears on the falling edge it witnesses, but that path runs off an in-memory active
        // set that empties on restart — so a restart landing between an alert and the recovery would leave the
        // memory sticky forever and swallow the next parking entirely. Nothing is held in memory here: the
        // deviation read alone must heal it.
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");
        await service.GetDatabaseStateDeviationsAsync(ServerId);

        var store = new DuckDbAlertHistoryStore(_duckDb);
        await store.SaveDatabaseStateAlertedAsync(ServerId, "App", "OFFLINE");
        Assert.Equal("OFFLINE", (await service.GetDatabaseStateDeviationsAsync(ServerId)).Single().LastAlertedState);

        // Operator brings it back. It stops deviating, so it drops out of the read entirely...
        await SeedSnapshotAsync(T0.AddMinutes(3), ("App", "ONLINE", false));
        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));

        // ...and the memory must be gone, or a second parking weeks later reads as already-announced.
        Assert.Null(await AlertedStateAsync("App"));
    }

    [Fact]
    public async Task IgnoredDatabase_AlsoHasItsAlertedStateCleared()
    {
        // An operator silencing a database should not leave a memory behind that outlives the silence.
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");
        await service.GetDatabaseStateDeviationsAsync(ServerId);

        var store = new DuckDbAlertHistoryStore(_duckDb);
        await store.SaveDatabaseStateAlertedAsync(ServerId, "App", "OFFLINE");
        await service.SetDatabaseStateExpectedAsync(ServerId, "App", PerformanceMonitor.Alerting.DatabaseStateTokens.Ignore);

        Assert.Empty(await service.GetDatabaseStateDeviationsAsync(ServerId));
        Assert.Null(await AlertedStateAsync("App"));
    }

    [Fact]
    public async Task ClearAlertedState_IsTheImmediatePath_AndLeavesTheBaselineIntact()
    {
        // The engine's own falling-edge call. It must forget the announcement WITHOUT disturbing the baseline —
        // clearing the expected state instead would re-baseline the database and silence a real deviation.
        var service = new LocalDataService(_duckDb);
        await DriveAppToStableStateAsync(service, "OFFLINE");
        await service.GetDatabaseStateDeviationsAsync(ServerId);

        var store = new DuckDbAlertHistoryStore(_duckDb);
        await store.SaveDatabaseStateAlertedAsync(ServerId, "App", "OFFLINE");
        await store.ClearDatabaseStateAlertedAsync(ServerId, "App");

        Assert.Null(await AlertedStateAsync("App"));
        var row = Assert.Single(await service.GetDatabaseStateExpectationsAsync(ServerId));
        Assert.Equal("ONLINE", row.ExpectedState); // baseline untouched
    }

    /// <summary>Reads the raw memory column, so a test can distinguish "cleared" from "never set".</summary>
    private async Task<string?> AlertedStateAsync(string database)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT last_alerted_state FROM config_database_state_expected WHERE server_id = $1 AND database_name = $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? null : (string)value;
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
