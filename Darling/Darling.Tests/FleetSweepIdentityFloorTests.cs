/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
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
/// #7669: Fleet Overview bands a server from its state now, so a column at its identity limit is Critical there until someone acts. A sweep bands
/// from counts of events in its span, and none of those counts carries that state, so the same server read Warning on every sweep. The state now
/// sets a floor on the sweep's band, read through the card's own reader.
/// </summary>
public sealed class FleetSweepIdentityFloorTests
{
    private static readonly DateTime Now = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Span = TimeSpan.FromHours(1);

    private static FleetIdentityCapacity Crit => new(IdentityCapacityMath.BandCritical, 1, 1, "Sales", "dbo.Orders", "Id", 100.0, "0");
    private static FleetIdentityCapacity Warn => new(IdentityCapacityMath.BandWarning, 2, 0);

    private static FleetSweepServerReading Reading(int id, string name, int memoryPressureEvents, FleetIdentityCapacity? identity) =>
        new(id, name,
            new DailyHealthSignals { HasData = true, CollectionRuns = 60, MemoryPressureEvents = memoryPressureEvents, Window = Span },
            0, null, IdentityCapacity: identity);

    private const string CritClause = "Identity capacity critical (1 column at its limit)";

    private static FleetSweepServerReading NoDataReading(int id, string name, FleetIdentityCapacity? identity) =>
        new(id, name, new DailyHealthSignals { HasData = false, Window = Span }, 0, null, IdentityCapacity: identity);

    private static FleetSweepComposition Compose(params FleetSweepServerReading[] readings) =>
        FleetSweepEngine.Compose(
            Now, Now - Span, alertsEnabled: true, serversExpected: readings.Length, readings,
            previousRun: null, Array.Empty<FleetSweepServerVerdict>(), Array.Empty<FleetSweepWatchItem>(),
            new FleetSweepInstrumentCounters(Now.AddHours(-6), AlertPassesTotal: 500, AlertReadFailuresTotal: 0),
            DeadlockRateThresholds.Default);

    [Fact]
    public void AColumnAtItsLimitWithNoEventsInTheSpan_BandsCritical_AndIsListedAsCritical()
    {
        var composition = Compose(Reading(1, "limit", 0, Crit), Reading(2, "quiet", 0, null));

        var verdict = composition.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("Critical", verdict.Band);
        Assert.Equal(CritClause, verdict.BandReason);
        Assert.Equal("Healthy", composition.Verdicts.Single(v => v.ServerId == 2).Band);

        using var report = JsonDocument.Parse(composition.Run.ReportJson);
        var critical = report.RootElement.GetProperty("critical_servers").EnumerateArray().Select(e => e.GetProperty("server").GetString()).ToList();
        Assert.Equal(new[] { "limit" }, critical);
        Assert.Equal(1, report.RootElement.GetProperty("fleet").GetProperty("bands").GetProperty("Critical").GetInt32());
    }

    [Fact]
    public void AColumnAtItsLimit_AppearsInTheChangesAsAWarningToCriticalTransition()
    {
        var first = Compose(Reading(1, "limit", 5, null));
        var second = FleetSweepEngine.Compose(
            Now.AddMinutes(15), Now.AddMinutes(15) - Span, alertsEnabled: true, serversExpected: 1,
            new[] { Reading(1, "limit", 5, Crit) },
            first.Run, first.Verdicts, Array.Empty<FleetSweepWatchItem>(),
            new FleetSweepInstrumentCounters(Now.AddHours(-6), AlertPassesTotal: 500, AlertReadFailuresTotal: 0),
            DeadlockRateThresholds.Default);

        using var report = JsonDocument.Parse(second.Run.ReportJson);
        var change = Assert.Single(report.RootElement.GetProperty("changes").GetProperty("band_transitions").EnumerateArray());
        Assert.Equal("Warning", change.GetProperty("from").GetString());
        Assert.Equal("Critical", change.GetProperty("to").GetString());
        Assert.Equal("5 memory-pressure events; " + CritClause, change.GetProperty("reason").GetString());
    }

    [Fact]
    public void AServerWithNoDataInTheSpan_StaysNoData_EvenWithAColumnAtItsLimit()
    {
        var composition = Compose(NoDataReading(1, "silent", Crit));

        var verdict = composition.Verdicts.Single();
        Assert.Equal("No Data", verdict.Band);
        Assert.Equal("No collection inside the sweep span.", verdict.BandReason);
        Assert.DoesNotContain("Identity", verdict.BandReason);

        using var report = JsonDocument.Parse(composition.Run.ReportJson);
        Assert.Empty(report.RootElement.GetProperty("critical_servers").EnumerateArray());
    }

    [Fact]
    public void AServerWithOnlyEvents_KeepsItsEventBand_AndAWarningBarColumnDoesNotFloorIt()
    {
        var composition = Compose(Reading(1, "events", 5, null), Reading(2, "near", 0, Warn));

        var events = composition.Verdicts.Single(v => v.ServerId == 1);
        Assert.Equal("Warning", events.Band);
        Assert.Equal("5 memory-pressure events", events.BandReason);
        Assert.Equal("Healthy", composition.Verdicts.Single(v => v.ServerId == 2).Band);

        using var report = JsonDocument.Parse(composition.Run.ReportJson);
        Assert.Empty(report.RootElement.GetProperty("critical_servers").EnumerateArray());
    }

    [Fact]
    public async Task ARunOverAStoreHoldingAColumnAtItsLimit_StoresACriticalVerdictForThatServerOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sweep identity floor test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        const int limitId = 966901;
        const int quietId = 966902;
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
                await DarlingMcpTestData.RegisterServerAsync(seed, limitId, "limit-server", ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, quietId, "quiet-server", ct);
                foreach (var (id, name) in new[] { (limitId, "limit-server"), (quietId, "quiet-server") })
                {
                    /* A collector run inside the span and nothing else: no event of any kind. */
                    await DarlingMcpTestData.ExecAsync(seed, ct,
                        "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)",
                        CollectionIdGenerator.Next(), id, name, "wait_stats", DarlingMcpTestData.Naive(now.AddMinutes(-5)), "SUCCESS");
                }

                var scannedAt = DarlingMcpTestData.Naive(now.AddMinutes(-30));
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    "INSERT INTO collect.identity_capacity (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
                    CollectionIdGenerator.Next(), scannedAt, limitId, "limit-server", "Sales");
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    @"INSERT INTO collect.identity_capacity
(collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, column_name, data_type, type_precision, last_value, seed_value, increment_value, percent_used)
VALUES ($1,$2,$3,$4,$5,'dbo','Orders','Id','int',10,2147483647::numeric,1::numeric,1::numeric,100)",
                    CollectionIdGenerator.Next(), scannedAt, limitId, "limit-server", "Sales");
            }

            await FleetSweepEngine.RunAsync(
                postgres,
                new (int, string)[] { (limitId, "limit-server"), (quietId, "quiet-server") },
                TimeSpan.FromMinutes(15), alertsEnabled: true, NullLogger.Instance, ct);

            var run = await FleetSweepStore.GetLatestSweepAsync(postgres, ct);
            Assert.NotNull(run);
            var verdicts = await FleetSweepStore.GetServerVerdictsForEngineAsync(postgres, run!.SweepId, ct);
            Assert.Equal("Critical", verdicts.Single(v => v.ServerId == limitId).Band);
            Assert.Equal("Healthy", verdicts.Single(v => v.ServerId == quietId).Band);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
