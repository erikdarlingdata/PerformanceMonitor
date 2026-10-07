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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5448: what the stored caveat says when the built-days read falls back, and what it keeps saying when a server's
/// PLAN_REGRESSION read times out on every scheduled pass.
///
/// <para><b>The fallback.</b> The fact asks for the built days first. If that read fails it runs the exact-bound read, the
/// shipped one, which is whole: nothing is missing, so no caveat may say so. If the exact read fails too, its own catch
/// records the failure, once. Driven through the real collector on a scratch store, with the failures made by renaming the
/// tables and columns the reads name.</para>
///
/// <para><b>The timeout stays visible.</b> A scheduled pass's timeout is stored in <c>collect.analysis_collection_caveats</c>
/// until a clean pass deletes it, and <c>get_collection_health</c> (<c>stored_caveats</c>) reads that table, so a server
/// whose every run times out stays visible, with its first-seen time kept and its last-seen time advancing.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionBuiltDaysFallbackLiveTests
{
    private const int ServerId = -5448301;
    private const string ServerName = "BuiltDaysFallbackSrv";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WhenTheBuiltDaysReadFails_AndTheExactReadSucceeds_NoCaveatSaysDataIsMissing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 fallback caveat test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (periodStart, periodEnd) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Control: with the built days readable the fact reads the day totals, finds the regression, records nothing. */
        var control = NewContext(periodStart, periodEnd);
        var controlFact = (await new PgFactCollector(postgres).CollectFactsAsync(control)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.True(control.PlanRegressionReadsIntervalTable);
        Assert.NotNull(control.PlanRegressionWindowStart);
        Assert.Empty(PlanRegressionFailures(control));

        /* The built-days read now fails (42P01); the exact-bound read does not touch that table. */
        await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily_built RENAME TO plan_regression_daily_built_gone", ct);
        var fallback = NewContext(periodStart, periodEnd);
        var fallbackFact = (await new PgFactCollector(postgres).CollectFactsAsync(fallback)).Single(f => f.Key == "PLAN_REGRESSION");

        Assert.True(fallback.PlanRegressionReadsIntervalTable);
        Assert.Null(fallback.PlanRegressionWindowStart);
        Assert.Equal(controlFact.Value, fallbackFact.Value);
        Assert.Equal(controlFact.Metadata["offender_count"], fallbackFact.Metadata["offender_count"]);

        /* No caveat, in any of the three places one would show: the pass's failures, the sentence the tools render, and so
           the rows the pass hands the caveat store. */
        Assert.Empty(PlanRegressionFailures(fallback));
        Assert.DoesNotContain("plan_regression", CollectionCaveats.Describe(fallback.CollectionFailures, fallback.CollectionFamilyCount), StringComparison.Ordinal);
    }

    /// <summary>
    /// The built-days read and the daily read share one REPEATABLE READ transaction (#5448). Between them, from another
    /// connection, the day totals are deleted and committed: the daily read must still see the totals that made those days
    /// built, so the fact equals the undisturbed one. At READ COMMITTED the second read would see no totals for days the
    /// first called built, and the regression's cheap plan would vanish from both halves.
    /// </summary>
    [Fact]
    public async Task ADailyTotalsChange_BetweenTheBuiltDaysReadAndTheDailyRead_IsNotSeenByTheDailyRead()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 snapshot test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (periodStart, periodEnd) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var control = NewContext(periodStart, periodEnd);
        var controlFact = (await new PgFactCollector(postgres).CollectFactsAsync(control)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.NotNull(control.PlanRegressionWindowStart);

        var seamRan = false;
        PgFactCollector.TestOnlyAfterBuiltDaysRead.Value = async token =>
        {
            seamRan = true;
            await using var other = new NpgsqlCommand("DELETE FROM collect.plan_regression_daily WHERE server_id = $1", connection);
            other.Parameters.AddWithValue(ServerId);
            await other.ExecuteNonQueryAsync(token);
        };
        try
        {
            var disturbed = NewContext(periodStart, periodEnd);
            var disturbedFact = (await new PgFactCollector(postgres).CollectFactsAsync(disturbed)).Single(f => f.Key == "PLAN_REGRESSION");

            Assert.True(seamRan);
            Assert.NotNull(disturbed.PlanRegressionWindowStart);
            Assert.Equal(controlFact.Value, disturbedFact.Value);
            Assert.Equal(controlFact.Metadata["offender_count"], disturbedFact.Metadata["offender_count"]);
        }
        finally
        {
            PgFactCollector.TestOnlyAfterBuiltDaysRead.Value = null;
        }
    }

    /// <summary>
    /// A late row marks its day (#5448): T-14, the window's first day, is built; a real late insert (the trigger is on) with
    /// a first execution in it takes that day and the next one out of the built days, so the fact reads them live, and it
    /// still finds the regression.
    /// </summary>
    [Fact]
    public async Task ADayMarkedByALateRow_DropsOutOfTheBuiltDays_AndIsReadLive()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 late-row test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (periodStart, periodEnd) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var windowFloor = PgFactCollector.PlanRegressionWindowFloor(periodStart.AddDays(-PgFactCollector.PlanRegressionWindowDays));
        var markedDay = DateOnly.FromDateTime(windowFloor);
        var before = await BuiltDaysAsync(connection, windowFloor, ct);
        Assert.Contains(markedDay, before);
        Assert.Contains(markedDay.AddDays(1), before);

        await InsertIntervalAsync(connection, queryId: 3, planId: 31, "0xLATE", cpuUs: 5000, execs: 10, firstExec: windowFloor.AddHours(10), ct);

        var after = await BuiltDaysAsync(connection, windowFloor, ct);
        Assert.DoesNotContain(markedDay, after);
        Assert.DoesNotContain(markedDay.AddDays(1), after);
        Assert.Equal(before.Count - 2, after.Count);

        var pass = NewContext(periodStart, periodEnd);
        var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass)).Single(f => f.Key == "PLAN_REGRESSION");
        Assert.NotNull(pass.PlanRegressionWindowStart);
        Assert.Empty(PlanRegressionFailures(pass));
        Assert.True(fact.Value > 1);
    }

    private static async Task<List<DateOnly>> BuiltDaysAsync(NpgsqlConnection connection, DateTime windowFloor, CancellationToken ct)
    {
        var days = new List<DateOnly>();
        await using var cmd = new NpgsqlCommand(PlanRegressionDaily.BuiltDaysSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, windowFloor);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) days.Add(reader.GetFieldValue<DateOnly>(0));
        return days;
    }

    [Fact]
    public async Task WhenBothReadsFail_TheExactReadsFailureIsTheOneRecorded_Once()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 fallback caveat test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (periodStart, periodEnd) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The built-days read fails on a missing table (42P01); the exact read fails on a missing column (42703). Only the
           second is the fact's failure. */
        await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily_built RENAME TO plan_regression_daily_built_gone", ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest RENAME COLUMN avg_duration_us TO avg_duration_us_gone", ct);

        var pass = NewContext(periodStart, periodEnd);
        await new PgFactCollector(postgres).CollectFactsAsync(pass);

        Assert.True(pass.PlanRegressionReadsIntervalTable);
        Assert.Null(pass.PlanRegressionOffenders);
        var failure = Assert.Single(PlanRegressionFailures(pass));
        Assert.Equal(CollectionFailureOutcome.MissingSchema, failure.Outcome);
        Assert.Contains("42703", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("42P01", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A server whose every scheduled pass times out stays visible: the stored caveat survives each re-sighting with its
    /// first-seen time kept and its last-seen time advancing, <c>get_collection_health</c>'s <c>stored_caveats</c> reports it as
    /// <c>plan_regression</c> / <c>timeout</c>, and a clean pass clears it.
    /// </summary>
    [Fact]
    public async Task WhenTheDailyReadThrows_TheWindowStartStaysUnset_ForTheDrillDown()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 fallback caveat test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (periodStart, periodEnd) = await SeedAndBuildAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The built days still read (they come from the built and coverage tables), so the fact chooses the daily statement;
           that statement then fails on the missing totals table (42P01). The edge M is recorded only once a read has
           succeeded, so the drill-down is not told to start from an edge no fact read from. */
        await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily RENAME TO plan_regression_daily_gone", ct);

        var pass = NewContext(periodStart, periodEnd);
        await new PgFactCollector(postgres).CollectFactsAsync(pass);

        Assert.True(pass.PlanRegressionReadsIntervalTable);
        Assert.Null(pass.PlanRegressionOffenders);
        Assert.Null(pass.PlanRegressionWindowStart);
        var failure = Assert.Single(PlanRegressionFailures(pass));
        Assert.Contains("42P01", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWhoseEveryScheduledRunTimesOut_KeepsItsStoredCaveat_UntilACleanPassClearsIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5448 timeout visibility test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(
            new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" }.ConnectionString);

        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException());
        var first = new DateTime(2026, 10, 7, 3, 0, 0, DateTimeKind.Utc);

        /* Three scheduled passes, every one of which times out reading PLAN_REGRESSION. The recording and the grouping into
           unread families are the pass's own (PgFactCollector.ReportCollectionFailure, DarlingAnalysisService). */
        for (var run = 0; run < 3; run++)
        {
            var context = NewContext(first, first.AddHours(4));
            context.RecordCollectionFailure(
                CollectionFailure.FamilyOf("CollectPlanRegressionFactsAsync"), "CollectPlanRegressionFactsAsync",
                PgFactCollector.ClassifyOutcome(timeout), timeout);
            var unread = context.CollectionFailures
                .GroupBy(f => f.Family, StringComparer.Ordinal)
                .Select(g => new CollectionCaveatStore.UnreadFamily(g.Key, CollectionFailure.Label(g.Last().Outcome)))
                .ToArray();

            await CollectionCaveatStore.ApplyPassAsync(postgres, ServerId, unread, first.AddMinutes(run * 15), null, ct);
        }

        var stored = await DarlingCollectionCaveatReader.ReadAsync(postgres, ServerId, null, ct);
        var caveat = Assert.Single(stored);
        Assert.Equal("plan_regression", caveat.Family);
        Assert.Equal("timeout", caveat.Reason);
        Assert.Equal(first, caveat.FirstSeenUtc);
        Assert.Equal(first.AddMinutes(30), caveat.LastSeenUtc);

        /* What get_collection_health returns: its health payload with the stored caveats spliced in. */
        var health = JsonNode.Parse(DarlingCollectionCaveatReader.AttachToJson("{\"server\":\"s\"}", stored))!.AsObject();
        var entry = Assert.Single(health[DarlingCollectionCaveatReader.PayloadKey]!.AsArray())!.AsObject();
        Assert.Equal("plan_regression", (string?)entry["family"]);
        Assert.Equal("timeout", (string?)entry["reason"]);

        /* A clean pass is the evidence it recovered: the row goes, and the payload carries no stored_caveats key. */
        await CollectionCaveatStore.ApplyPassAsync(
            postgres, ServerId, Array.Empty<CollectionCaveatStore.UnreadFamily>(), first.AddMinutes(45), null, ct);
        var after = await DarlingCollectionCaveatReader.ReadAsync(postgres, ServerId, null, ct);
        Assert.Empty(after);
        Assert.Null(JsonNode.Parse(DarlingCollectionCaveatReader.AttachToJson("{\"server\":\"s\"}", after))!.AsObject()[DarlingCollectionCaveatReader.PayloadKey]);
    }

    private static List<CollectionFailure> PlanRegressionFailures(AnalysisContext context) =>
        context.CollectionFailures.Where(f => f.Family == "plan_regression").ToList();

    private static AnalysisContext NewContext(DateTime periodStart, DateTime periodEnd) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = periodStart,
        TimeRangeEnd = periodEnd,
        ServerUtcOffset = TimeSpan.Zero,
    };

    /// <summary>
    /// One query with two plans, the cheap one seen five days before the pass window and the costly one in it (a 10x CPU
    /// regression), plus an old steady query so the interval table's floor is below the read's bound, and a coverage claim
    /// below the window so the fact reads the interval table. Then T-14 through T-3 are built for the server, so the fact
    /// can read day totals.
    /// </summary>
    private static async Task<(DateTime Start, DateTime End)> SeedAndBuildAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var end = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        var start = end.AddHours(-4);
        var today = now.Date;

        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);
        await InsertIntervalAsync(connection, queryId: 1, planId: 11, "0xCHEAP", cpuUs: 20_000, execs: 100, firstExec: start.AddDays(-5).AddHours(-1), ct);
        await InsertIntervalAsync(connection, queryId: 1, planId: 12, "0xCOSTLY", cpuUs: 200_000, execs: 100, firstExec: end.AddHours(-1), ct);
        await InsertIntervalAsync(connection, queryId: 2, planId: 21, "0xSTEADY", cpuUs: 2000, execs: 100, firstExec: start.AddDays(-16), ct);
        await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late", ct);

        await using (var coverage = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $2) "
            + "ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since", connection))
        {
            coverage.Parameters.AddWithValue(ServerId);
            coverage.Parameters.AddWithValue(NpgsqlDbType.Timestamp, today.AddDays(-40));
            await coverage.ExecuteNonQueryAsync(ct);
        }

        for (var d = -14; d <= -3; d++)
        {
            await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(today.AddDays(d)), now, ct);
        }

        return (start, end);
    }

    private static async Task InsertIntervalAsync(
        NpgsqlConnection connection, long queryId, long planId, string planHash, long cpuUs, long execs, DateTime firstExec, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES ($1, 'fallbackdb', $2, $3, NULL, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, 0)", connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(planId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(35));
        cmd.Parameters.AddWithValue(planHash);
        cmd.Parameters.AddWithValue("qh" + queryId);
        cmd.Parameters.AddWithValue(execs);
        cmd.Parameters.AddWithValue(cpuUs);
        cmd.Parameters.AddWithValue(cpuUs * 3);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(30));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
