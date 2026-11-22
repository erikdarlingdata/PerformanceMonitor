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
/// #9195: the Query Store top read with no database filter, at 24 hours and at 7 days, on a store with Query Store rows for 20 databases.
/// It returns the rows worked out from the store's rows alone (each interval at its newest snapshot, summed per group, ranked on cost),
/// and the number of store statements it issues does not grow with the number of databases: it is counted through the service's command
/// seam, not timed.
/// </summary>
public sealed class QueryStoreUnfilteredReadTests : IDisposable
{
    private const int ServerId = 9195;
    private const int Queries = 100;
    private const int Top = 10;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_9195u_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DateTime _now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
    private DuckDbInitializer? _duckDb;

    public void Dispose()
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>Interval start, in hours before now: three inside 24 hours and two between 24 hours and 7 days, so the two windows read different rows.</summary>
    private static readonly int[] IntervalHoursAgo = [2, 5, 9, 30, 100];

    private sealed record Snapshot(int Db, int Query, int Interval, int Snap, long Executions, double DurationUs)
    {
        public string Database => "db" + Db.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>Every database has every query, in every interval, stored twice with a growing count (the open interval is stored again each cycle).</summary>
    private static List<Snapshot> Generate(int databases)
    {
        var rows = new List<Snapshot>();
        for (var d = 0; d < databases; d++)
            for (var q = 0; q < Queries; q++)
                for (var i = 0; i < IntervalHoursAgo.Length; i++)
                    for (var s = 0; s < 2; s++)
                        rows.Add(new Snapshot(d, q, i, s, 10L * (s + 1) + ((d + q + i) % 7), 1000.0 + (d * 997) + (q * 131) + (i * 53)));
        return rows;
    }

    private async Task<LocalDataService> SeedAsync(int databases)
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();

        var values = new StringBuilder();
        var id = 0L;
        foreach (var r in Generate(databases))
        {
            var intervalStart = _now.AddHours(-IntervalHoursAgo[r.Interval]);
            var collected = intervalStart.AddMinutes(10 + (20 * r.Snap));
            if (values.Length > 0) values.Append(',');
            values.Append(string.Create(CultureInfo.InvariantCulture,
                $"({++id}, TIMESTAMP '{collected:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'srv', '{r.Database}', {r.Query}, {r.Query}, 'Regular', TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{collected:yyyy-MM-dd HH:mm:ss}', '0xQH{r.Query}', {r.Executions}, {r.DurationUs}, {r.DurationUs}, '0xPH{r.Query}', false, 0, {100 + r.Interval}, NULL, TIMESTAMP '{intervalStart:yyyy-MM-dd HH:mm:ss}')"));
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

    /// <summary>What the read must answer for a window, from the rows alone: each interval at its newest snapshot, summed per group, ranked on cost.</summary>
    private static List<(string Database, long Query, long Executions)> Expected(int databases, int hours)
        => Generate(databases)
            .Where(r => IntervalHoursAgo[r.Interval] <= hours)
            .GroupBy(r => (r.Db, r.Query, r.Interval))
            .Select(g => g.OrderByDescending(r => r.Snap).First())
            .GroupBy(r => (r.Database, r.Query))
            .Select(g => (g.Key.Database, Query: (long)g.Key.Query, Executions: g.Sum(r => r.Executions), Cost: g.Sum(r => r.Executions * r.DurationUs)))
            .OrderByDescending(g => g.Cost).ThenBy(g => g.Database, StringComparer.Ordinal).ThenBy(g => g.Query)
            .Select(g => (g.Database, g.Query, g.Executions))
            .Take(Top)
            .ToList();

    [Theory]
    [InlineData(24)]
    [InlineData(168)]
    public async Task UnfilteredTop_OnTwentyDatabases_IsTheRowsTheStoreHolds(int hours)
    {
        var service = await SeedAsync(20);

        var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, hours, Top, asOfUtc: _now);

        Assert.Equal(Expected(20, hours), rows.Select(r => (r.DatabaseName, r.QueryId, r.TotalExecutions)).ToList());
        /* The two windows must differ on this store, or the test would not tell a 24 hour read from a 7 day one. */
        Assert.NotEqual(Expected(20, 24), Expected(20, 168));
    }

    [Theory]
    [InlineData(24)]
    [InlineData(168)]
    public async Task UnfilteredTop_IssuesTheSameNumberOfStatements_HoweverManyDatabasesThereAre(int hours)
    {
        var few = await StatementsAsync(5, hours);
        var many = await StatementsAsync(20, hours);

        Assert.True(few > 0);
        Assert.Equal(few, many);
    }

    /// <summary>The commands one unfiltered read creates on a fresh store of <paramref name="databases"/> databases. Both stores rank at least
    /// top + 200 groups, so the candidate cap is reached on each and what is left to vary is the database count.</summary>
    private async Task<int> StatementsAsync(int databases, int hours)
    {
        _duckDb?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
        var service = await SeedAsync(databases);
        var commands = 0;
        service.CommandCreated = () => commands++;
        await service.GetQueryStoreTopQueriesAsync(ServerId, hours, Top, asOfUtc: _now);
        service.CommandCreated = null;
        return commands;
    }
}
