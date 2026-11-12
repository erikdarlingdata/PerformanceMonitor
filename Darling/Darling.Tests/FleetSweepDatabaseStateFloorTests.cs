/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #7647: Fleet Overview bands a server Critical while a database sits in SUSPECT, RECOVERY_PENDING or EMERGENCY. A sweep bands from counts of events in
/// its span, so without the same floor it read that server Healthy while the card read Critical. The state sets the floor beside the identity limit (#7669).
/// </summary>
public sealed class FleetSweepDatabaseStateFloorTests
{
    private static readonly DateTime Now = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Span = TimeSpan.FromHours(1);

    private static FleetDatabaseStates States(params (string Db, string State)[] deviations) =>
        FleetDatabaseStates.Of(new DatabaseStateRead(true, deviations.Select(d => new DatabaseStateDeviation(d.Db, d.State, "ONLINE")).ToList()));

    private static FleetSweepServerReading Reading(int id, string name, int memoryPressureEvents, FleetDatabaseStates? states, FleetIdentityCapacity? identity = null) =>
        new(id, name,
            new DailyHealthSignals { HasData = true, CollectionRuns = 60, MemoryPressureEvents = memoryPressureEvents, Window = Span },
            0, null, IdentityCapacity: identity, DatabaseStates: states);

    private static FleetSweepComposition Compose(params FleetSweepServerReading[] readings) =>
        FleetSweepEngine.Compose(
            Now, Now - Span, alertsEnabled: true, serversExpected: readings.Length, readings,
            previousRun: null, Array.Empty<FleetSweepServerVerdict>(), Array.Empty<FleetSweepWatchItem>(),
            new FleetSweepInstrumentCounters(Now.AddHours(-6), AlertPassesTotal: 500, AlertReadFailuresTotal: 0),
            DeadlockRateThresholds.Default);

    [Fact]
    public void ACriticalDatabaseStateWithNoEventsInTheSpan_BandsCritical_WithTheCardsOwnWords()
    {
        var states = States(("Sales", "SUSPECT"));
        var composition = Compose(Reading(1, "suspect", 0, states), Reading(2, "quiet", 0, null));

        var verdict = composition.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("Critical", verdict.Band);
        Assert.Equal(states.Reason, verdict.BandReason);
        Assert.Equal("Healthy", composition.Verdicts.Single(v => v.ServerId == 2).Band);
    }

    [Fact]
    public void ACriticalDatabaseStateWithEvents_KeepsTheEventReasons_AndAddsTheClause()
    {
        var states = States(("Sales", "EMERGENCY"));
        var verdict = Compose(Reading(1, "both", 5, states)).Verdicts.Single();

        Assert.Equal("Critical", verdict.Band);
        Assert.EndsWith("; " + states.Reason, verdict.BandReason);
        Assert.True(verdict.BandReason!.Length > states.Reason.Length + 2);
    }

    [Fact]
    public void ADatabaseStateAndAnIdentityLimit_NameBothInTheReason()
    {
        var states = States(("Sales", "RECOVERY_PENDING"));
        var identity = new FleetIdentityCapacity(IdentityCapacityMath.BandCritical, 1, 1, "Sales", "dbo.Orders", "Id", 100.0, "0");
        var verdict = Compose(Reading(1, "both", 0, states, identity)).Verdicts.Single();

        Assert.Equal("Critical", verdict.Band);
        Assert.Contains("Identity capacity critical", verdict.BandReason);
        Assert.Contains(states.Reason, verdict.BandReason);
    }

    [Fact]
    public void ADeviationTheAlertDoesNotGradeCritical_AndAStateThatWasNotCollected_FloorNothing()
    {
        var offline = Reading(1, "offline", 0, States(("Archive", "OFFLINE")));
        var notCollected = Reading(2, "silent", 0, FleetDatabaseStates.Of(new DatabaseStateRead(false, Array.Empty<DatabaseStateDeviation>())));
        var composition = Compose(offline, notCollected);

        Assert.All(composition.Verdicts, v => Assert.Equal("Healthy", v.Band));
    }

    [Fact]
    public void AServerWithNoDataInTheSpan_StaysNoData_EvenWithACriticalDatabaseState()
    {
        var silent = new FleetSweepServerReading(1, "silent", new DailyHealthSignals { HasData = false, Window = Span }, 0, null,
            DatabaseStates: States(("Sales", "SUSPECT")));
        var verdict = Compose(silent).Verdicts.Single();

        Assert.Equal("No Data", verdict.Band);
        Assert.DoesNotContain("Database state", verdict.BandReason);
    }

    [Fact]
    public async Task ARunOverAStoreHoldingASuspectDatabase_StoresACriticalVerdictForThatServerOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep database state floor test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        const int suspectId = 976901;
        const int quietId = 976902;
        var now = DateTime.UtcNow;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
            {
                await migrate.OpenAsync(ct);
                await PgMigrations.MigrateAsync(migrate, null, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                SearchPath = "collect,config,public",
            }.ConnectionString);

            await using (var seed = await postgres.OpenConnectionAsync(ct))
            {
                await DarlingMcpTestData.RegisterServerAsync(seed, suspectId, "suspect-server", ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, quietId, "quiet-server", ct);
                foreach (var (id, name, state) in new[] { (suspectId, "suspect-server", "SUSPECT"), (quietId, "quiet-server", "ONLINE") })
                {
                    /* A collector run inside the span and nothing else: no event of any kind. */
                    await DarlingMcpTestData.ExecAsync(seed, ct,
                        "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)",
                        CollectionIdGenerator.Next(), id, name, "wait_stats", DarlingMcpTestData.Naive(now.AddMinutes(-5)), "SUCCESS");

                    /* Two snapshots, the alert's own two-sample rule. */
                    foreach (var minutesAgo in new[] { 20, 10 })
                    {
                        await DarlingMcpTestData.ExecAsync(seed, ct,
                            "INSERT INTO collect.database_states (collection_id, collection_time, server_id, server_name, database_name, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,false)",
                            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)), id, name, "Sales", state);
                    }
                }
            }

            await FleetSweepEngine.RunAsync(
                postgres,
                new (int, string)[] { (suspectId, "suspect-server"), (quietId, "quiet-server") },
                TimeSpan.FromMinutes(15), alertsEnabled: true, NullLogger.Instance, ct);

            var run = await FleetSweepStore.GetLatestSweepAsync(postgres, ct);
            Assert.NotNull(run);
            var verdicts = await FleetSweepStore.GetServerVerdictsForEngineAsync(postgres, run!.SweepId, ct);
            Assert.Equal("Critical", verdicts.Single(v => v.ServerId == suspectId).Band);
            Assert.Equal("Healthy", verdicts.Single(v => v.ServerId == quietId).Band);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
