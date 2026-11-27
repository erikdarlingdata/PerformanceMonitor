/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9195: the Query Store top read with no database filter. The newest snapshot of each interval is picked by a hash aggregate instead of a
/// window sort, which is what made a server-wide read cost most of its time on the rows of every database. The result must not move:
/// each interval counts once at its newest snapshot (the collector re-stores an open interval with a growing execution_count), groups
/// whose replica_role is NULL stay one group, and the server-wide top N is the union of the per-database top N.
/// </summary>
public sealed class QueryStoreServerWideTopTests : IDisposable
{
    private const int ServerId = 9195;
    private const int Databases = 6;
    private const int Queries = 12;
    private const int Intervals = 5;
    private const int Snapshots = 3;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_9195_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DateTime _now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private sealed record Snapshot(int Db, int Query, int Interval, int Snap, long Executions, double DurationUs, string? Role)
    {
        public DateTime Collected(DateTime now) => now.AddHours(-1 - Interval).AddMinutes(10 * Snap);
        public string Database => "db" + Db.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>Every database has every query. An interval is stored Snapshots times with a growing count and the cost of one execution fixed.
    /// Odd databases carry a replica_role, even ones leave it NULL.</summary>
    private static List<Snapshot> Generate()
    {
        var rows = new List<Snapshot>();
        for (var d = 0; d < Databases; d++)
            for (var q = 0; q < Queries; q++)
                for (var i = 0; i < Intervals; i++)
                    for (var s = 0; s < Snapshots; s++)
                        rows.Add(new Snapshot(d, q, i, s, 10L * (s + 1) + ((d + q + i) % 7), 1000.0 + (d * 997) + (q * 131) + i, d % 2 == 1 ? "Primary" : null));

        /* Two intervals are stored a second time at the SAME collection_time as their newest snapshot, inserted last. The newest snapshot is then
           decided by execution_count: one extra row has the lower count (it must lose) and one the higher count (it must win), so neither the
           insert order nor the collection_id can pick the right row. */
        rows.Add(new Snapshot(0, 0, 0, Snapshots - 1, 3, 1000.0, null));
        rows.Add(new Snapshot(1, 1, 1, Snapshots - 1, 900, 1000.0 + 997 + 131 + 1, "Primary"));
        return rows;
    }

    private async Task<LocalDataService> SeedAsync()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();

        var values = new StringBuilder();
        var id = 0L;
        foreach (var r in Generate())
        {
            var collected = r.Collected(_now).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            var intervalStart = _now.AddHours(-1 - r.Interval).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            if (values.Length > 0) values.Append(',');
            var role = r.Role is null ? "NULL" : "'" + r.Role + "'";
            values.Append(string.Create(CultureInfo.InvariantCulture,
                $"({++id}, TIMESTAMP '{collected}', {ServerId}, 'srv', '{r.Database}', {r.Query}, {r.Query}, 'Regular', TIMESTAMP '{intervalStart}', TIMESTAMP '{collected}', '0xQH{r.Query}', {r.Executions}, {r.DurationUs}, {r.DurationUs}, '0xPH{r.Query}', false, 0, {r.Interval + 100}, {role}, TIMESTAMP '{intervalStart}')"));
        }

        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText =
                "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, "
                + "first_execution_time, last_execution_time, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, query_plan_hash, is_forced_plan, "
                + "force_failure_count, runtime_stats_interval_id, replica_role, interval_start_time_utc) VALUES " + values;
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        return new LocalDataService(_duckDb);
    }

    /// <summary>What the read must answer, worked out from the rows alone: each interval at its newest snapshot, summed per group, ranked on cost.</summary>
    private static List<(string Database, long Query, long Executions)> Expected(string? database)
        => Generate()
            .Where(r => database is null || r.Database == database)
            .GroupBy(r => (r.Db, r.Query, r.Interval))
            .Select(g => g.OrderByDescending(r => r.Snap).ThenByDescending(r => r.Executions).First())
            .GroupBy(r => (r.Database, r.Query, r.Role))
            .Select(g => (Database: g.Key.Database, Query: (long)g.Key.Query, Executions: g.Sum(r => r.Executions), Cost: g.Sum(r => r.Executions * r.DurationUs)))
            .OrderByDescending(g => g.Cost).ThenBy(g => g.Database, StringComparer.Ordinal).ThenBy(g => g.Query)
            .Select(g => (g.Database, g.Query, g.Executions))
            .ToList();

    [Fact]
    public async Task ServerWideTop_CountsEachIntervalOnceAtItsNewestSnapshot_ForEveryDatabase()
    {
        var service = await SeedAsync();

        var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, 24, 25, asOfUtc: _now);

        var expected = Expected(null).Take(25).ToList();
        Assert.Equal(expected, rows.Select(r => (r.DatabaseName, r.QueryId, r.TotalExecutions)).ToList());
        Assert.Contains(rows, r => r.ReplicaRole is null);
        Assert.Contains(rows, r => r.ReplicaRole == "Primary");
    }

    [Fact]
    public async Task NewestSnapshot_IsTheLaterCollectionTime_AndTheGreaterExecutionCountOnATie()
    {
        var service = await SeedAsync();

        var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, 24, 1000, asOfUtc: _now);

        var byGroup = rows.ToDictionary(r => (r.DatabaseName, r.QueryId));
        var all = Generate();
        /* db0 query 0, interval 0: the tied extra row (3) loses to the stored 30-something at the same time. db1 query 1, interval 1: the tied
           extra row (900) wins. Every other interval contributes its last snapshot. */
        foreach (var key in new[] { ("db0", 0L), ("db1", 1L) })
        {
            var expected = all.Where(r => r.Database == key.Item1 && r.Query == key.Item2)
                .GroupBy(r => r.Interval)
                .Sum(g => g.OrderByDescending(r => r.Snap).ThenByDescending(r => r.Executions).First().Executions);
            Assert.Equal(expected, byGroup[key].TotalExecutions);
        }
        Assert.True(byGroup[("db1", 1L)].TotalExecutions >= 900, "the greater execution_count of a tied newest snapshot must win");
    }

    [Fact]
    public async Task NewestSnapshot_OnATimeTieWithANullExecutionCount_IsTheRowWithTheCount()
    {
        /* #9195: execution_count is nullable. Inside a struct DuckDB orders a NULL field above every value, so the aggregate's
           (collection_time, execution_count) pick took the NULL row where the window put it last (NULLS LAST under DESC). Two snapshots of one
           interval at one collection_time, counts 5 and NULL, the NULL row inserted last: the read must answer with the 5 row. */
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();
        var collected = _now.AddHours(-2).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        var intervalStart = _now.AddHours(-3).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText =
                "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, "
                + "first_execution_time, last_execution_time, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, query_plan_hash, is_forced_plan, "
                + "force_failure_count, runtime_stats_interval_id, replica_role, interval_start_time_utc) VALUES "
                + $"(1, TIMESTAMP '{collected}', {ServerId}, 'srv', 'db0', 7, 7, 'Regular', TIMESTAMP '{intervalStart}', TIMESTAMP '{collected}', '0xQH7', 5, 100, 100, '0xPH7', false, 0, 100, NULL, TIMESTAMP '{intervalStart}'),"
                + $"(2, TIMESTAMP '{collected}', {ServerId}, 'srv', 'db0', 7, 7, 'Regular', TIMESTAMP '{intervalStart}', TIMESTAMP '{collected}', '0xQH7', NULL, 900, 900, '0xPH7', false, 0, 100, NULL, TIMESTAMP '{intervalStart}')";
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
        var service = new LocalDataService(_duckDb);

        var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, 24, 10, asOfUtc: _now);

        var row = Assert.Single(rows);
        Assert.Equal(5, row.TotalExecutions);
        Assert.Equal(0.1, row.AvgDurationMs, 6);
    }

    [Fact]
    public async Task OneDatabaseTop_IsTheSameRowsAsBefore()
    {
        var service = await SeedAsync();

        var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, 24, 8, databaseNames: new[] { "db3" }, asOfUtc: _now);

        Assert.Equal(Expected("db3").Take(8).ToList(), rows.Select(r => (r.DatabaseName, r.QueryId, r.TotalExecutions)).ToList());
    }

    [Fact]
    public async Task ServerWideTop_IsTheUnionOfThePerDatabaseTops()
    {
        var service = await SeedAsync();
        const int top = 10;

        var serverWide = await service.GetQueryStoreTopQueriesAsync(ServerId, 24, top, asOfUtc: _now);

        var perDatabase = new List<QueryStoreRow>();
        for (var d = 0; d < Databases; d++)
            perDatabase.AddRange(await service.GetQueryStoreTopQueriesAsync(ServerId, 24, top, databaseNames: new[] { "db" + d }, asOfUtc: _now));
        var union = perDatabase
            .OrderByDescending(r => r.TotalExecutions * r.AvgDurationMs).ThenBy(r => r.DatabaseName, StringComparer.Ordinal).ThenBy(r => r.QueryId)
            .Take(top)
            .Select(r => (r.DatabaseName, r.QueryId, r.TotalExecutions))
            .ToList();

        Assert.Equal(union, serverWide.Select(r => (r.DatabaseName, r.QueryId, r.TotalExecutions)).ToList());
    }
}
