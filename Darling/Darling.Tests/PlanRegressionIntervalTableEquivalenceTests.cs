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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953's merge gate: PLAN_REGRESSION and its drill-down return IDENTICAL results from the latest-snapshot interval
/// table and from the shipped raw read, over the same snapshots, on a store seeded through the real write path. Then,
/// once raw is purged below the best plans, the table read still reaches them and the raw read does not: the ruled
/// window extension, and the only way the two may differ.
///
/// <para>Both reads run with exactly the parameters the collectors bind (the fact's <c>$3</c> skew bound, the
/// drill-down's offender arrays), and the rows are compared in order, column for column. The seed carries real
/// regressions (a cheap best plan days before an expensive latest one, on a NULL and a named replica role), repeated
/// open-interval snapshots, a query with one plan, and non-Regular rows, so the comparison is not vacuous.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race the
   live collection, and serializing it would be pure slowdown. */
public sealed class PlanRegressionIntervalTableEquivalenceTests
{
    private const int ServerId = -3953002;

    /// <summary>The analysis window's start: the 14-day comparison window reaches back to <c>T0 - 4 days</c>.</summary>
    private static readonly DateTime TimeRangeStart = new(2026, 9, 11, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime T0 = TimeRangeStart.AddDays(-10);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheFactAndTheDrillDown_ReturnTheSameRowsFromTheTableAsFromRaw_ThenTheTableReachesPastRawsPurge()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 equivalence test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await SeedAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), ct);

        /* ---- identical over the same snapshots ---- */
        var rawFact = await FactRowsAsync(connection, PgFactCollector.PlanRegressionSql, ct);
        var tableFact = await FactRowsAsync(connection, PgFactCollector.PlanRegressionTableSql, ct);
        Assert.True(rawFact.Count >= 2, $"the seed produced {rawFact.Count} PLAN_REGRESSION row(s); the comparison needs real regressions");
        Assert.Equal(rawFact, tableFact);

        var unrestrictedRaw = await DrillDownRowsAsync(connection, PgDrillDownCollector.RegressedQueriesSql, null, ct);
        var unrestrictedTable = await DrillDownRowsAsync(connection, PgDrillDownCollector.RegressedQueriesTableSql, null, ct);
        Assert.True(unrestrictedRaw.Count >= 2, $"the drill-down returned {unrestrictedRaw.Count} row(s); the comparison needs real regressions");
        Assert.Equal(unrestrictedRaw, unrestrictedTable);

        /* The drill-down's normal path (#3902): restricted to the fact's offenders. */
        var offenders = await OffendersAsync(connection, ct);
        Assert.Equal(
            await DrillDownRowsAsync(connection, PgDrillDownCollector.RegressedQueriesSql, offenders, ct),
            await DrillDownRowsAsync(connection, PgDrillDownCollector.RegressedQueriesTableSql, offenders, ct));

        /* ---- raw purged below the best plans: only the table still sees the regression ---- */
        await ExecAsync(connection, "DELETE FROM collect.query_store_stats WHERE server_id = @server_id AND collection_time < @cutoff",
            T0.AddDays(6), ct);
        var purgedRaw = await FactRowsAsync(connection, PgFactCollector.PlanRegressionSql, ct);
        var extendedTable = await FactRowsAsync(connection, PgFactCollector.PlanRegressionTableSql, ct);
        Assert.Empty(purgedRaw);
        Assert.Equal(tableFact, extendedTable);
    }

    [Fact]
    public async Task TheSourceDecision_PicksTheTableOnlyWhenItsCoverageHoldsTheRawReadsSnapshots_AndNeverWithPendingBatches()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 decision test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A raw bound inside the seeded span: on a plain store raw has no chunk floor, so the rule also needs the
           bound at or above the table's own floor (the seed's oldest interval, T0 + 12 h). */
        var rawBound = T0.AddDays(1);

        /* No coverage row: raw. */
        Assert.False(await QueryStoreIntervalLatest.ReadsTableAsync(connection, ServerId, rawBound, 30, null, ct));

        /* Coverage created by the first apply, from the clock: on a plain store raw keeps its history, so the table
           is not chosen until the raw read's own bound passes the coverage start. */
        await SeedAsync(new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), ct);
        Assert.False(await QueryStoreIntervalLatest.ReadsTableAsync(connection, ServerId, rawBound, 30, null, ct));

        /* A coverage start at or below the raw read's bound: the table. */
        await ExecAsync(connection, "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @cutoff WHERE server_id = @server_id",
            rawBound, ct);
        Assert.True(await QueryStoreIntervalLatest.ReadsTableAsync(connection, ServerId, rawBound, 30, null, ct));

        /* A pending batch: raw, whatever the coverage says. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsA', @cutoff)",
            T0, ct);
        Assert.False(await QueryStoreIntervalLatest.ReadsTableAsync(connection, ServerId, rawBound, 30, null, ct));
    }

    [Theory]
    [InlineData(null, false, null, false)]                       // no coverage
    [InlineData(-20.0, true, null, false)]                       // pending batches
    [InlineData(-20.0, false, null, true)]                       // plain raw (no floor), coverage below the bound
    [InlineData(-10.0, false, null, false)]                      // plain raw, coverage above the bound
    [InlineData(-10.0, false, -11.0, false)]                     // armed raw floor below coverage: raw still reads older
    [InlineData(-10.0, false, -9.0, true)]                       // armed raw floor above coverage: the switch
    public void UseTable_FollowsTheRule(double? filledSinceDays, bool hasPending, double? rawFloorDays, bool expected)
    {
        var now = new DateTime(2026, 9, 23, 0, 0, 0, DateTimeKind.Unspecified);
        var rawBound = now.AddDays(-15);
        DateTime? filledSince = filledSinceDays is double f ? now.AddDays(f) : null;
        DateTime? rawFloor = rawFloorDays is double r ? now.AddDays(r) : null;

        Assert.Equal(expected, QueryStoreIntervalLatest.UseTable(filledSince, hasPending, rawFloor, rawBound, tableFloor: now.AddDays(-30)));
    }

    [Fact]
    public void UseTable_RefusesAWindowReachingBelowTheTablesFloor_WhenRawStillHoldsIt()
    {
        var now = new DateTime(2026, 9, 23, 0, 0, 0, DateTimeKind.Unspecified);
        var rawBound = now.AddDays(-20);

        /* A held store: raw keeps 30 days, the table's floor is 16 days back, and an anchored pass reaches 20. */
        Assert.False(QueryStoreIntervalLatest.UseTable(now.AddDays(-25), false, now.AddDays(-30), rawBound, now.AddDays(-16)));

        /* The same window where raw holds nothing older than the table: the table. */
        Assert.True(QueryStoreIntervalLatest.UseTable(now.AddDays(-25), false, now.AddDays(-15), rawBound, now.AddDays(-16)));
    }

    /* ---- the seed ---------------------------------------------------------------------------------------- */

    private static QueryStoreCollector.Row Row(
        string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
        long cpuUs, string? role = null, string type = "Regular") => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = planId,
            ExecutionTypeDesc = type,
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0xQ" + queryId.ToString(CultureInfo.InvariantCulture),
            QueryPlanHash = "0xP" + planId.ToString(CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = cpuUs * 3,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            ReplicaRole = role,
            RuntimeStatsIntervalId = intervalId,
        };

    /// <summary>
    /// Each interval is shipped as an open snapshot and then its closed one, at the collector's cadence. Query 100
    /// regresses on the NULL role (cheap plan days 1-3, expensive days 8-9); query 200 on a named role; query 300 has
    /// one plan; query 400 is seen only as non-Regular.
    /// </summary>
    private static async Task SeedAsync(DarlingCollectorRunner runner, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "qsil-eq-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsil-eq", Host = "qsil-eq-host" },
            ConnectionString = "Server=qsil-eq-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsil-eq-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        var interval = 1000L;
        for (var day = 0; day < 10; day++)
        {
            var start = T0.AddDays(day).AddHours(12);
            interval++;

            var rows = new List<(double Fraction, QueryStoreCollector.Row Row)>();
            void Both(string db, long query, long plan, long execs, long cpu, string? role = null, string type = "Regular")
            {
                rows.Add((0.5, Row(db, query, plan, interval, start, start.AddMinutes(25), execs / 2, cpu, role, type)));
                rows.Add((1.0, Row(db, query, plan, interval, start, start.AddMinutes(55), execs, cpu, role, type)));
            }

            if (day is >= 1 and <= 3) Both("qsA", 100, 1001, 300, 100);
            if (day is 8 or 9) Both("qsA", 100, 1002, 20000, 1000);
            if (day == 2) Both("qsB", 200, 2001, 500, 200, role: "secondary1");
            if (day == 9) Both("qsB", 200, 2002, 30000, 900, role: "secondary1");
            Both("qsB", 300, 3001, 1000, 50);
            if (day == 5) Both("qsA", 400, 4001, 90, 70, type: "Aborted");

            foreach (var (fraction, batch) in new[] { 0.5, 1.0 }.Select(f => (f, rows.Where(r => r.Fraction == f).Select(r => r.Row).ToList())))
            {
                var collectionTime = start.AddMinutes(fraction == 0.5 ? 30 : 65);
                foreach (var perDatabase in batch.GroupBy(r => r.DatabaseName))
                {
                    await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, perDatabase.ToList(), server, collectionTime, context, ct);
                }
            }
        }
    }

    /* ---- the two reads, bound as the collectors bind them ------------------------------------------------ */

    private static async Task<List<string>> FactRowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var windowStart = TimeRangeStart.AddDays(-14);
        var collectionBound = TimeRangeStart.AddDays(-15);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(collectionBound);
        if (sql == PgFactCollector.PlanRegressionTableSql)
        {
            command.Parameters.AddWithValue(collectionBound);
        }

        return await RowsAsync(command, ct);
    }

    private static async Task<List<string>> DrillDownRowsAsync(
        NpgsqlConnection connection, string sql, (string[] Databases, long[] QueryIds)? offenders, CancellationToken ct)
    {
        var windowStart = TimeRangeStart.AddDays(-14);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(TimeRangeStart);
        command.Parameters.AddWithValue(TimeRangeStart.AddHours(4));
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = offenders is null ? DBNull.Value : offenders.Value.Databases,
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            Value = offenders is null ? DBNull.Value : offenders.Value.QueryIds,
        });
        if (sql == PgDrillDownCollector.RegressedQueriesTableSql)
        {
            command.Parameters.AddWithValue(windowStart.AddDays(-1));
        }

        return await RowsAsync(command, ct);
    }

    private static async Task<(string[] Databases, long[] QueryIds)> OffendersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var windowStart = TimeRangeStart.AddDays(-14);
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(TimeRangeStart.AddDays(-15));
        var databases = new List<string>();
        var queryIds = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            databases.Add(reader.GetString(8));
            queryIds.Add(Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
        }

        return (databases.ToArray(), queryIds.ToArray());
    }

    private static async Task<List<string>> RowsAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i)
                    ? "<null>"
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "";
            }

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("cutoff", cutoff);
        await command.ExecuteNonQueryAsync(ct);
    }
}
