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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches
   nothing on the shared one, so serializing it against the live-postgres collection would cost suite time
   and buy no isolation. */

/// <summary>
/// The half of the V123 store no source pin reaches: whether the DDL and the five statements PARSE,
/// whether the writer's single transaction round-trips a whole sweep, whether the watch upsert
/// preserves the birth record and standing evidence across a real conflict, and whether the viewer
/// probe — three sites wide on paper — actually returns one boolean per map parameter against a
/// migrated store and maps it to exactly this build's version.
///
/// <para>Split out of <see cref="FleetSweepStateRungTests"/> per the LivePostgresCollectionHygieneTests
/// shape: a class that reaches the store drags its whole file into a serialization decision, and that
/// file's pure pins have no business paying for one.</para>
/// </summary>
public sealed class FleetSweepStoreLivePostgresTests
{
    /// <summary>
    /// One full sweep cycle, twice, against a freshly-migrated scratch store: sweep 1 persists whole
    /// and reads back; sweep 2 diffs against it (previous_sweep_id), advances one watch item to open
    /// and carries another through a miss — proving the upsert keeps first-seen and standing evidence
    /// while taking the engine's state wholesale.
    /// </summary>
    [Fact]
    public async Task ASweepRoundTrips_AndTheWatchUpsertAdvancesWithoutRewritingBirthRecords()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-sweep store round-trip (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An empty store answers null, not a throw and not a phantom row. */
        Assert.Null(await FleetSweepStore.GetLatestSweepAsync(postgres, ct));
        Assert.Empty(await FleetSweepStore.GetActiveWatchItemsAsync(postgres, ct));

        var sweepOneAt = new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc);
        var sweepOne = new FleetSweepRun(
            SweepId: 1001,
            SweptAtUtc: sweepOneAt,
            SpanStartUtc: sweepOneAt.AddHours(-1),
            SpanEndUtc: sweepOneAt,
            PreviousSweepId: null,
            AlertsEnabled: false,
            ServersExpected: 2,
            ServersReported: 2,
            InstrumentsAlive: true,
            InstrumentLivenessJson: """{"alert_pass_counter":{"read":true,"advancing":true}}""",
            ReportJson: """{"sweep":1}""");

        var firstSighting = FleetSweepWatchStateMachine.FirstSighting();

        await FleetSweepStore.RecordSweepAsync(
            postgres,
            sweepOne,
            new[]
            {
                new FleetSweepServerVerdict(1, "server-a", "Healthy", null, null, null),
                new FleetSweepServerVerdict(2, "server-b", "Critical", null, "cpu pinned across the span", """{"cpu":99}"""),
            },
            new[]
            {
                /* The muted-mode contract: alerts_enabled is false above, so the condition that would
                   have paged rides the ledger with its evidence. */
                new FleetSweepWouldHavePagedEntry(2, "high_cpu", """{"cpu":99,"minutes":18}"""),
            },
            new[]
            {
                new FleetSweepWatchItem(
                    2, "cpu-pinned", "cpu above the measured band", firstSighting.State,
                    firstSighting.ConsecutiveHits, firstSighting.ConsecutiveMisses,
                    FirstSeenSweepId: 1001, LastSeenSweepId: 1001, OpenedSweepId: null, ClosedSweepId: null,
                    FirstSeenAtUtc: sweepOneAt, LastSeenAtUtc: sweepOneAt,
                    EvidenceJson: """{"cpu":99}"""),
            },
            ct);

        var latest = await FleetSweepStore.GetLatestSweepAsync(postgres, ct);
        Assert.NotNull(latest);
        Assert.Equal(sweepOne, latest);
        Assert.Equal(DateTimeKind.Utc, latest!.SweptAtUtc.Kind);

        var verdicts = await FleetSweepStore.GetServerVerdictsAsync(postgres, 1001, null, ct);
        Assert.Equal(2, verdicts.Count);
        Assert.Equal("server-a", verdicts[0].ServerName);
        Assert.Equal("cpu pinned across the span", verdicts.Single(v => v.ServerId == 2).BandReason);

        var ledger = await FleetSweepStore.GetWouldHavePagedAsync(postgres, 1001, null, ct);
        Assert.Equal("high_cpu", Assert.Single(ledger).AlertFamily);

        /* Sweep 2: the watch item is sighted again (second consecutive hit — it OPENS), and the diff
           anchor is the sweep just read. */
        var sweepTwoAt = sweepOneAt.AddHours(1);
        var advanced = FleetSweepWatchStateMachine.Advance(
            firstSighting.State, conditionHeld: true,
            firstSighting.ConsecutiveHits, firstSighting.ConsecutiveMisses);
        Assert.True(advanced.JustOpened);

        await FleetSweepStore.RecordSweepAsync(
            postgres,
            sweepOne with
            {
                SweepId = 1002,
                SweptAtUtc = sweepTwoAt,
                SpanStartUtc = sweepOneAt,
                SpanEndUtc = sweepTwoAt,
                PreviousSweepId = latest.SweepId,
                ReportJson = """{"sweep":2}""",
            },
            new[]
            {
                new FleetSweepServerVerdict(1, "server-a", "Healthy", "Healthy", null, null),
                new FleetSweepServerVerdict(2, "server-b", "Critical", "Critical", "cpu still pinned", null),
            },
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            new[]
            {
                new FleetSweepWatchItem(
                    2, "cpu-pinned", "cpu above the measured band", advanced.State,
                    advanced.ConsecutiveHits, advanced.ConsecutiveMisses,
                    FirstSeenSweepId: 1001, LastSeenSweepId: 1002, OpenedSweepId: 1002, ClosedSweepId: null,
                    FirstSeenAtUtc: sweepOneAt, LastSeenAtUtc: sweepTwoAt,
                    /* A hit sweep carries fresh evidence; the upsert takes it. */
                    EvidenceJson: """{"cpu":97}"""),
            },
            ct);

        var newest = await FleetSweepStore.GetLatestSweepAsync(postgres, ct);
        Assert.Equal(1002, newest!.SweepId);
        Assert.Equal(1001, newest.PreviousSweepId);

        /* The span read honors BOTH bounds: a span ending before sweep 2 returns only sweep 1. */
        var spanned = await FleetSweepStore.GetSweepsBySpanAsync(
            postgres, sweepOneAt.AddMinutes(-1), sweepOneAt.AddMinutes(1), null, ct);
        Assert.Equal(1001, Assert.Single(spanned).SweepId);

        var open = Assert.Single(await FleetSweepStore.GetWatchItemsByStateAsync(
            postgres, FleetSweepWatchStateMachine.Open, null, ct));
        Assert.Equal(1001, open.FirstSeenSweepId);
        Assert.Equal(1002, open.OpenedSweepId);
        Assert.Equal("""{"cpu":97}""", open.EvidenceJson);

        /* Sweep 3: a MISS — the engine passes null evidence, and the row must keep the standing
           evidence while the counters and stamps move. This is the COALESCE the rung test pins as
           text, proven against a real conflict. */
        var sweepThreeAt = sweepTwoAt.AddHours(1);
        var missed = FleetSweepWatchStateMachine.Advance(
            advanced.State, conditionHeld: false, advanced.ConsecutiveHits, advanced.ConsecutiveMisses);
        Assert.Equal(FleetSweepWatchStateMachine.Carried, missed.State);

        await FleetSweepStore.RecordSweepAsync(
            postgres,
            sweepOne with
            {
                SweepId = 1003,
                SweptAtUtc = sweepThreeAt,
                SpanStartUtc = sweepTwoAt,
                SpanEndUtc = sweepThreeAt,
                PreviousSweepId = 1002,
                AlertsEnabled = true,
                ReportJson = """{"sweep":3}""",
            },
            Array.Empty<FleetSweepServerVerdict>(),
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            new[]
            {
                new FleetSweepWatchItem(
                    2, "cpu-pinned", "cpu above the measured band", missed.State,
                    missed.ConsecutiveHits, missed.ConsecutiveMisses,
                    FirstSeenSweepId: 1001, LastSeenSweepId: 1003, OpenedSweepId: 1002, ClosedSweepId: null,
                    FirstSeenAtUtc: sweepOneAt, LastSeenAtUtc: sweepThreeAt,
                    EvidenceJson: null),
            },
            ct);

        var carried = Assert.Single(await FleetSweepStore.GetActiveWatchItemsAsync(postgres, ct));
        Assert.Equal(FleetSweepWatchStateMachine.Carried, carried.State);
        Assert.Equal(1, carried.ConsecutiveMisses);
        Assert.Equal(1001, carried.FirstSeenSweepId);
        Assert.Equal(sweepOneAt, carried.FirstSeenAtUtc);
        Assert.Equal("""{"cpu":97}""", carried.EvidenceJson);
    }

    /// <summary>
    /// The writer's transaction is all-or-nothing, proven by making a child row fail after the run
    /// row was accepted: a verdict with a NUL byte in a text column (22021 — the realistic data-shape
    /// fault, the PgFindingStore reasoning) must roll the RUN back too, so the store never holds a
    /// sweep document with no servers in it.
    /// </summary>
    [Fact]
    public async Task AChildRowFault_RollsBackTheWholeSweep()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-sweep transaction test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var sweptAt = new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc);
        var run = new FleetSweepRun(
            2001, sweptAt, sweptAt.AddHours(-1), sweptAt, null, true, 1, 1, true, "{}", "{}");

        await Assert.ThrowsAnyAsync<Exception>(() => FleetSweepStore.RecordSweepAsync(
            postgres,
            run,
            new[] { new FleetSweepServerVerdict(1, "server-\0a", "Healthy", null, null, null) },
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            Array.Empty<FleetSweepWatchItem>(),
            ct));

        /* The run row must NOT survive its verdicts' failure — a rolled-back sweep leaves the
           previous sweep as the newest complete one, which here is none at all. */
        Assert.Null(await FleetSweepStore.GetLatestSweepAsync(postgres, ct));
    }

    /// <summary>
    /// The worklist names read (#3482), against a real store: the whole retained history in one map —
    /// a server that departed after an early sweep is still nameable off that sweep's verdicts, which
    /// is the read's reason to exist (the newest sweep cannot name a server hysteresis is still
    /// carrying) — and the rename-determinism contract proven live: two names for one server_id, and
    /// the NEWEST wins, because the statement's ORDER BY delivers it last and the reader overwrites
    /// (the #3476 rule, held on the new statement too).
    /// </summary>
    [Fact]
    public async Task TheWorklistNamesRead_CoversDepartedServers_AndTheNewestNameWins()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live worklist-names test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An empty store answers an empty map, not a throw — the log-and-degrade posture's happy
           twin: nothing to name is not a fault. */
        Assert.Empty(await FleetSweepStore.GetSweepServerNamesAsync(postgres, null, ct));

        var sweepOneAt = new DateTime(2026, 9, 15, 10, 0, 0, DateTimeKind.Utc);
        var run = new FleetSweepRun(
            3001, sweepOneAt, sweepOneAt.AddHours(-1), sweepOneAt, null, true, 2, 2, true,
            """{"alert_pass_counter":{"read":true}}""", """{"sweep":1}""");

        /* Sweep 1: server 1 under its ORIGINAL name, server 2 present for the last time — the
           departed server whose name will exist ONLY in this older sweep's verdicts. */
        await FleetSweepStore.RecordSweepAsync(
            postgres,
            run,
            new[]
            {
                new FleetSweepServerVerdict(1, "name-original", "Healthy", null, null, null),
                new FleetSweepServerVerdict(2, "server-departed", "Healthy", null, null, null),
            },
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            Array.Empty<FleetSweepWatchItem>(),
            ct);

        /* Sweep 2: server 1 RENAMED, server 2 gone from the fleet. */
        await FleetSweepStore.RecordSweepAsync(
            postgres,
            run with
            {
                SweepId = 3002,
                SweptAtUtc = sweepOneAt.AddHours(1),
                SpanStartUtc = sweepOneAt,
                SpanEndUtc = sweepOneAt.AddHours(1),
                PreviousSweepId = 3001,
                ReportJson = """{"sweep":2}""",
            },
            new[] { new FleetSweepServerVerdict(1, "name-renamed", "Healthy", "Healthy", null, null) },
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            Array.Empty<FleetSweepWatchItem>(),
            ct);

        var names = await FleetSweepStore.GetSweepServerNamesAsync(postgres, null, ct);

        /* Two names for one id: the NEWEST sighting's spelling wins, deterministically. */
        Assert.Equal("name-renamed", names[1]);

        /* The departed server is still nameable — the whole-history read's reason to exist: the
           newest sweep's verdicts no longer carry it, but a watch item may still. */
        Assert.Equal("server-departed", names[2]);
        Assert.Equal(2, names.Count);
    }

    /// <summary>
    /// The probe's arity, against a real store rather than by counting text: the SELECT returns
    /// exactly one boolean per <c>MapProbedSchemaVersion</c> parameter — the agreement the V122 lane
    /// verified and this rung extends by one — and feeding the map the ACTUAL sentinel row of a
    /// freshly-migrated store answers exactly this build's version. This is the whole connect-time
    /// gate exercised end to end, minus only the viewer instance around it.
    /// </summary>
    [Fact]
    public async Task TheProbeAgainstAMigratedStore_ReturnsOneColumnPerMapParameter_AndMapsToThisBuild()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live probe-arity test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        /* One column per parameter — the drift the three-site discipline exists to prevent, measured
           on the wire instead of inferred from the source. */
        Assert.Equal(arity, reader.FieldCount);

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, sentinels)!);
    }
}
