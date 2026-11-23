/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9373: the grid's by-keys statements bound to the kept keys' distinct database names and query ids in every interval-table arm and the
/// daily arm. The filter is a superset of what the keys match, so every statement must return the rows it returned with both arrays NULL
/// (the read before the filter), in the same order. The seed holds groups that share a database name with a kept key, groups that share a
/// query id with one (so the filter admits their rows and the key match has to drop them), groups with a NULL database name and a NULL
/// query id, and rows in the margin arm, the closed-day run and the open arm.
/// </summary>
[Collection(ViewerLoggerStaticsCollection.Name)]
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race live collection. */
public sealed class QueryStoreGridByKeysPrefilterLiveTests
{
    private const string Wide = QueryStoreTopSplitLiveTests.Wide;
    private const int ServerId = 9373;
    private const int Top = 500;

    /// <summary>Day zero. The window starts at 06:00 on it, the closed-day run is days 1 to 3, and the window's literal end is noon on day 5.</summary>
    private static readonly DateTime D0 = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = D0.AddHours(6);
    private static readonly DateTime RunStart = D0.AddDays(1);
    private static readonly DateTime RunEnd = D0.AddDays(4);
    private static readonly DateTime LiteralEnd = D0.AddDays(5).AddHours(12);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static string Ts(DateTime value) => QueryStoreTopSplitLiveTests.Ts(value);

    /// <summary>One group of the seed: its number (also its plan id), database name, query id, and the average duration of every row.</summary>
    private sealed record Group(int Number, string? Database, long? QueryId, int AvgDurationUs);

    /// <summary>
    /// 1 and 2 are the heaviest and are kept at a limit of three, with 5. 3 and 4 swap the database names and query ids of 1 and 2, so a filter on
    /// the two arrays alone admits their rows. 6 shares 5's database, 9 is a database no kept key has. 7 has no database name and 8 has no query
    /// id: a limit of four or more keeps 7 and the arrays are bound NULL.
    /// </summary>
    private static readonly Group[] AllGroups =
    [
        new(1, "qsA", 1, 9000),
        new(2, "qsB", 2, 8000),
        new(3, "qsA", 2, 10),
        new(4, "qsB", 1, 10),
        new(5, "qsC", 3, 7000),
        new(6, "qsC", 4, 20),
        new(7, null, 5, 500),
        new(8, "qsA", null, 400),
        new(9, "qsD", 6, 300),
    ];

    private static IEnumerable<Group> GroupsWithoutNulls() => AllGroups.Where(g => g.Database is not null && g.QueryId is not null);

    private static string Sql(string? text) => text is null ? "NULL" : $"'{text}'";

    private static string Sql(long? number) => number is { } n ? n.ToString(CultureInfo.InvariantCulture) : "NULL";

    private static string Sql(DateTime? stamp) => stamp is { } s ? $"timestamp '{Ts(s)}'" : "NULL";

    private static int _interval;

    /// <summary>One group's row in the margin arm (before the run), one in the run's days and one in the open arm, with an interval start, plus
    /// a row without one in the open arm for the even groups.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, IEnumerable<Group> groups, CancellationToken ct)
    {
        var values = new List<string>();
        foreach (var g in groups)
        {
            var starts = new List<(DateTime? Start, DateTime Collected, DateTime First)>();
            foreach (var start in new[] { D0.AddHours(7).AddMinutes(g.Number), D0.AddDays(2).AddHours(10).AddMinutes(g.Number), RunEnd.AddHours(3).AddMinutes(g.Number) })
            {
                starts.Add((start, start.AddHours(1).AddMinutes(5), start.AddMinutes(10)));
            }

            if (g.Number % 2 == 0)
            {
                var legacy = RunEnd.AddHours(5).AddMinutes(g.Number);
                starts.Add((null, legacy, legacy));
            }

            foreach (var (start, collected, first) in starts)
            {
                values.Add(string.Create(CultureInfo.InvariantCulture,
                    $"(timestamp '{Ts(collected)}', {ServerId}, {Sql(g.Database)}, {Sql(g.QueryId)}, {g.Number}, 'Regular', timestamp '{Ts(first)}', timestamp '{Ts(first.AddMinutes(20))}', 'select 1', 10, {g.AvgDurationUs}, {++_interval}, {Sql(start)})"));
            }
        }

        await ExecAsync(connection, $@"
INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES
{string.Join(",\n", values)};
ANALYZE {Wide};", ct);
    }

    private static void AddShared(NpgsqlCommand command, DateTime start, DateTime? end, IReadOnlyList<string>? databases)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(start));
        command.Parameters.Add(end is { } e
            ? QueryStoreTopSplitLiveTests.Timestamp(e)
            : new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(databases));
    }

    private static void AddRun(NpgsqlCommand command)
    {
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(RunStart));
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(RunEnd));
    }

    private static void AddNullFilter(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = DBNull.Value });
    }

    /// <summary>Every column that identifies or sizes a grid row, so two reads that differ in any of them are different lines.</summary>
    private static string Line(ViewerQueryStoreRow r) => JsonSerializer.Serialize(new object?[]
    {
        r.DatabaseName, r.QueryId, r.PlanId, r.ReplicaRole, r.ExecutionTypeDesc, r.TotalExecutions, r.AvgDurationMs, r.MinDurationMs, r.MaxDurationMs,
        r.FirstExecutionTime, r.LastExecutionTime,
    });

    private sealed record Statements(string Label, string KeysSql, string ByKeysSql, bool Rollup);

    private static IEnumerable<Statements> AllStatements()
    {
        yield return new("table", ViewerDataService.QueryStoreTopTableKeysSql, ViewerDataService.QueryStoreTopTableByKeysSql, false);
        yield return new("table split", ViewerDataService.QueryStoreTopTableSplitKeysSql, ViewerDataService.QueryStoreTopTableSplitByKeysSql, false);
        var rollup = ViewerDataService.QueryStoreTopRollupStatements(splitBuilt: false, TimeSpan.FromHours(1));
        yield return new("rollup", rollup.KeysSql, rollup.TopSql, true);
        var rollupSplit = ViewerDataService.QueryStoreTopRollupStatements(splitBuilt: true, TimeSpan.FromHours(1));
        yield return new("rollup split", rollupSplit.KeysSql, rollupSplit.TopSql, true);
    }

    /// <summary>The kept keys of the keys statement, held as the grid holds them, and the keys' database names and query ids in rank order.</summary>
    private static async Task<(ViewerDataService.TableReadKeys Keys, List<(string? Database, long? QueryId)> Ids)> ReadKeysAsync(
        NpgsqlConnection connection, Statements statements, DateTime? end, IReadOnlyList<string>? databases, int limit, CancellationToken ct)
    {
        var keys = new ViewerDataService.TableReadKeys();
        var ids = new List<(string? Database, long? QueryId)>();
        await using var command = new NpgsqlCommand(statements.KeysSql, connection) { CommandTimeout = 120 };
        AddShared(command, WindowStart, end, databases);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        if (statements.Rollup)
        {
            AddRun(command);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add(reader);
            ids.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1)));
        }

        return (keys, ids);
    }

    /// <summary>The by-keys statement as the grid runs it, with plain index scans off. <paramref name="bindFilter"/> binds the kept keys' arrays;
    /// without it both arrays are NULL, which reads every arm as the statement did before the filter.</summary>
    private static async Task<List<string>> ReadByKeysAsync(
        NpgsqlConnection connection, Statements statements, ViewerDataService.TableReadKeys keys, DateTime? end, IReadOnlyList<string>? databases,
        bool bindFilter, CancellationToken ct)
    {
        await ExecAsync(connection, "SET enable_indexscan = off", ct);
        try
        {
            await using var command = new NpgsqlCommand(statements.ByKeysSql, connection) { CommandTimeout = 120 };
            AddShared(command, WindowStart, end, databases);
            keys.AddParameters(command);
            if (statements.Rollup)
            {
                AddRun(command);
            }

            if (bindFilter)
            {
                keys.AddFilterParameters(command);
            }
            else
            {
                AddNullFilter(command);
            }

            await using var reader = await command.ExecuteReaderAsync(ct);
            var (rows, _) = await TopFill.ReadPageAsync(reader, ViewerDataService.QueryStorePageOrdinal, ViewerDataService.ReadQueryStoreTopRow, ct);
            return rows.Select(Line).ToList();
        }
        finally
        {
            await ExecAsync(connection, "RESET enable_indexscan", ct);
        }
    }

    /// <summary>A store with the seed in the interval table, the closed-day run's days built into the daily table, and the split's index.</summary>
    private static async Task<NpgsqlConnection> OpenSeededAsync(ScratchPostgres scratch, IEnumerable<Group> groups, CancellationToken ct, bool filler = false)
    {
        var connection = await QueryStoreTopSplitLiveTests.OpenAsync(scratch, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        await SeedAsync(connection, groups, ct);
        if (filler)
        {
            /* 60,000 rows of 3,000 other groups over the seed's four days, so a scan of the arms has a choice to make and the daily table holds
               the run's days. Their duration is far under every seeded group's. */
            await ExecAsync(connection, $@"
INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT s.t + interval '65 minutes', {ServerId}, 'fill' || (g % 7), 1000 + (g % 3000), 1000 + (g % 3000), 'Regular', s.t + interval '10 minutes',
       s.t + interval '30 minutes', 'select 1', 1, 5, 100000 + g, s.t
FROM generate_series(1, 60000) AS g
CROSS JOIN LATERAL (SELECT timestamp '{Ts(D0.AddHours(7))}' + (g % 6000) * interval '1 minute' AS t) AS s;
ANALYZE {Wide};", ct);
        }

        for (var day = 1; day <= 3; day++)
        {
            Assert.True(await QueryStoreClosedDayRollup.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(D0.AddDays(day)), RunEnd.AddDays(3), ct) >= 0,
                $"day {day} of the run was not built");
        }

        await ExecAsync(connection, "ANALYZE collect.query_store_grid_daily", ct);
        return connection;
    }

    /// <summary>The window's shapes: a closed end over every database, an open end over every database, and an open end over two.</summary>
    private static IEnumerable<(string Label, DateTime? End, string[]? Databases)> Shapes() =>
    [
        ("closed end", LiteralEnd, null),
        ("open end", null, null),
        ("open end, two databases", null, new[] { "qsA", "qsC" }),
    ];

    /// <summary>
    /// Every by-keys statement returns the same rows, in the same order, with the kept keys' arrays bound as with both NULL, at a limit that keeps
    /// three groups (the arrays are bound), at one that keeps the group with no database name (they are bound NULL) and at every group. The page
    /// holds exactly the kept keys' groups, so the groups that share a database or a query id with one are read by the arms and dropped by the match.
    /// </summary>
    [Fact]
    public async Task EveryByKeysStatement_ReturnsTheSameRows_WithTheArraysBoundAsWithNull_AndTheKeptKeysAreThePage()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9373 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenSeededAsync(scratch, AllGroups, ct);

        foreach (var statements in AllStatements())
        {
            foreach (var (shape, end, databases) in Shapes())
            {
                foreach (var limit in new[] { 3, 4, 9 })
                {
                    var label = $"{statements.Label}, {shape}, limit {limit}";
                    var (keys, ids) = await ReadKeysAsync(connection, statements, end, databases, limit, ct);
                    Assert.True(ids.Count > 0, $"{label}: the keys statement kept nothing");

                    var bound = await ReadByKeysAsync(connection, statements, keys, end, databases, bindFilter: true, ct);
                    var unbound = await ReadByKeysAsync(connection, statements, keys, end, databases, bindFilter: false, ct);
                    Assert.Equal(unbound, bound);

                    /* The page is the kept groups' rows and nothing else: one row per kept key, in each key's own database and query id. */
                    var page = bound.Select(line => JsonSerializer.Deserialize<JsonElement[]>(line)!)
                        .Select(row => (Database: row[0].ValueKind == JsonValueKind.Null ? "" : row[0].GetString()!, QueryId: row[1].GetInt64()))
                        .ToList();
                    /* The grid row reads a NULL database name as "" and a NULL query id as 0. */
                    var kept = ids.Select(i => (Database: i.Database ?? "", QueryId: i.QueryId ?? 0)).ToList();
                    Assert.Equal(kept.OrderBy(i => i.Database).ThenBy(i => i.QueryId).ToList(), page.OrderBy(i => i.Database).ThenBy(i => i.QueryId).ToList());

                    var (databaseArray, queryIdArray) = keys.FilterArrays();
                    Assert.Equal(ids.Any(i => i.Database is null || i.QueryId is null), databaseArray is null);
                    Assert.Equal(databaseArray is null, queryIdArray is null);
                }
            }
        }

        /* The seed does stress the filter: at a limit of three, the groups that share a database name or a query id with a kept key are not kept. */
        var three = await ReadKeysAsync(connection, AllStatements().First(), null, null, 3, ct);
        Assert.Equal(new (string?, long?)[] { ("qsA", 1), ("qsB", 2), ("qsC", 3) }, three.Ids.OrderBy(i => i.Database).Select(i => (i.Database, i.QueryId)).ToArray());
        Assert.NotNull(three.Keys.FilterArrays().DatabaseNames);
    }

    /// <summary>
    /// The grid's own read through the service: the page it returns is the one-statement read's, which carries no by-keys filter, before and
    /// after the split's index is built, with and without groups that have a NULL database name or query id.
    /// </summary>
    [Fact]
    public async Task TheGrid_ReturnsTheOneStatementReadsRows_WithAndWithoutNullKeyGroups_BeforeAndAfterTheIndexIsBuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9373 live test.");
        var ct = TestContext.Current.CancellationToken;

        /* A window that holds no whole UTC day, so the grid keeps its interval statements instead of the rollup's. */
        var start = WindowStart;
        var end = start.AddHours(30);

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await QueryStoreTopSplitLiveTests.OpenAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, start, ct);
        await SeedAsync(connection, GroupsWithoutNulls(), ct);
        await ExecAsync(connection, $"""
            UPDATE collect.query_store_interval_wide_coverage SET filled_since = timestamp '{Ts(start.AddDays(-1))}' WHERE server_id = {ServerId}
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
            VALUES ({ServerId}, 'qs-by-keys-prefilter', 'qs-by-keys-prefilter', TRUE, 16, now(), now())
            ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, {ServerId}, 'qs-by-keys-prefilter', d.d, 60
            FROM generate_series(timestamp '{Ts(start.AddDays(-2))}', timestamp '{Ts(end)}', interval '6 hours') AS c(c)
            CROSS JOIN (VALUES ('qsA'), ('qsB'), ('qsC'), ('qsD')) AS d(d)
            """, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        async Task<List<string>> GridAsync()
        {
            var read = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, start, end, Top, null, null, null, ct);
            Assert.True(read.Plan is { UseTable: true }, "the grid read did not take the interval table, so the comparison would prove nothing");
            return read.Rows.Select(Line).ToList();
        }

        /* The one-statement read, with no by-keys filter in it: the rows the grid must return. */
        async Task<List<string>> OracleAsync()
        {
            await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection) { CommandTimeout = 120 };
            AddShared(command, start, null, null);
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
            await using var reader = await command.ExecuteReaderAsync(ct);
            var (rows, _) = await TopFill.ReadPageAsync(reader, ViewerDataService.QueryStorePageOrdinal, ViewerDataService.ReadQueryStoreTopRow, ct);
            return rows.Select(Line).ToList();
        }

        var withoutNulls = await GridAsync();
        Assert.True(withoutNulls.Count(l => l.Contains("\"qs", StringComparison.Ordinal)) >= GroupsWithoutNulls().Count(), "every seeded group is in the grid");
        Assert.Equal(await OracleAsync(), withoutNulls);

        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        Assert.Equal(await OracleAsync(), await GridAsync());

        await SeedAsync(connection, AllGroups.Where(g => g.Database is null || g.QueryId is null), ct);
        var withNulls = await GridAsync();
        Assert.Equal(withoutNulls.Count + 2, withNulls.Count);
        Assert.Equal(await OracleAsync(), withNulls);
    }

    /// <summary>The grid's key arrays are the kept keys' distinct database names and query ids, and NULL for both when a key has a NULL in either.</summary>
    [Fact]
    public void TheFilterArrays_AreTheDistinctValues_AndBothNull_WhenAKeyHasANull()
    {
        static ViewerDataService.TableReadKeys Keys(params (string? Database, long? QueryId)[] keys)
        {
            using var table = new DataTable();
            table.Columns.Add("database_name", typeof(string));
            table.Columns.Add("query_id", typeof(long));
            table.Columns.Add("plan_id", typeof(long));
            table.Columns.Add("query_hash", typeof(string));
            table.Columns.Add("execution_type_desc", typeof(string));
            table.Columns.Add("replica_role", typeof(string));
            foreach (var (database, queryId) in keys)
            {
                table.Rows.Add((object?)database ?? DBNull.Value, (object?)queryId ?? DBNull.Value, 1L, "0x01", "Regular", DBNull.Value);
            }

            var held = new ViewerDataService.TableReadKeys();
            using var reader = table.CreateDataReader();
            while (reader.Read())
            {
                held.Add(reader);
            }

            return held;
        }

        var (databases, queryIds) = Keys(("qsA", 1), ("qsB", 2), ("qsA", 2), ("qsC", 1)).FilterArrays();
        Assert.Equal(new[] { "qsA", "qsB", "qsC" }, databases!.OrderBy(d => d));
        Assert.Equal(new[] { 1L, 2L }, queryIds!.OrderBy(q => q));

        var firstTwo = Keys(("qsA", 1), ("qsB", 2), (null, 3)).First(2).FilterArrays();
        Assert.Equal(new[] { "qsA", "qsB" }, firstTwo.DatabaseNames!.OrderBy(d => d));

        Assert.Equal((null, null), Keys(("qsA", 1), (null, 2)).FilterArrays());
        Assert.Equal((null, null), Keys(("qsA", 1), ("qsB", null)).FilterArrays());
        Assert.Equal((null, null), Keys((null, null)).FilterArrays());

        var none = Keys().FilterArrays();
        Assert.Empty(none.DatabaseNames!);
        Assert.Empty(none.QueryIds!);
    }

    /// <summary>
    /// The plan of each by-keys statement with the arrays bound: every scan of the interval table, and of the daily table where the statement reads
    /// it, carries <c>query_id = ANY</c> in its filter, recheck or index condition, so the oldest days are not read in full. With the arrays bound
    /// NULL no scan carries it.
    /// </summary>
    [Fact]
    public async Task EveryScanOfEveryByKeysStatement_CarriesTheFilter_WhenTheArraysAreBound_AndNoneWhenTheyAreNull()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9373 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenSeededAsync(scratch, AllGroups, ct, filler: true);

        foreach (var statements in AllStatements())
        {
            var (keys, ids) = await ReadKeysAsync(connection, statements, null, null, 3, ct);
            Assert.Equal(3, ids.Count);

            foreach (var bindFilter in new[] { true, false })
            {
                var label = $"{statements.Label}, arrays {(bindFilter ? "bound" : "NULL")}";
                await ExecAsync(connection, "SET enable_indexscan = off", ct);
                try
                {
                    await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + statements.ByKeysSql, connection) { CommandTimeout = 120 };
                    AddShared(explain, WindowStart, null, null);
                    keys.AddParameters(explain);
                    if (statements.Rollup)
                    {
                        AddRun(explain);
                    }

                    if (bindFilter)
                    {
                        keys.AddFilterParameters(explain);
                    }
                    else
                    {
                        AddNullFilter(explain);
                    }

                    using var plan = JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
                    var scans = new List<(string Table, string Conditions, string Index)>();
                    Collect(plan.RootElement[0].GetProperty("Plan"), scans);

                    var intervalScans = scans.Where(s => IsInterval(s.Table)).ToList();
                    Assert.True(intervalScans.Count > 0, $"{label}: no scan of the interval table in the plan");
                    if (statements.Rollup)
                    {
                        Assert.Contains(scans, s => IsDaily(s.Table));
                    }

                    foreach (var scan in scans.Where(s => IsInterval(s.Table) || IsDaily(s.Table)))
                    {
                        Assert.True(bindFilter == scan.Conditions.Contains("query_id = ANY", StringComparison.Ordinal),
                            $"{label}: a scan of {scan.Table} {(bindFilter ? "has no" : "has the")} query_id = ANY condition: {scan.Conditions} (index {scan.Index})");
                    }
                }
                finally
                {
                    await ExecAsync(connection, "RESET enable_indexscan", ct);
                }
            }
        }
    }

    /* A table the statement reads in two places is aliased query_store_interval_wide and query_store_interval_wide_1. */
    private static bool IsInterval(string alias) => alias.StartsWith("query_store_interval_wide", StringComparison.Ordinal);

    private static bool IsDaily(string alias) => alias.StartsWith("query_store_grid_daily", StringComparison.Ordinal);

    private static void IndexNames(JsonElement node, List<string> names)
    {
        if (node.TryGetProperty("Index Name", out var name))
        {
            names.Add(name.GetString()!);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                IndexNames(child, names);
            }
        }
    }

    /// <summary>The scans of a plan, by the table (the alias, so a hypertable's chunk reads as the table) with the conditions they carry and
    /// the index a bitmap scan beneath them uses.</summary>
    private static void Collect(JsonElement node, List<(string Table, string Conditions, string Index)> scans)
    {
        if (node.TryGetProperty("Alias", out var alias) && node.TryGetProperty("Relation Name", out _))
        {
            var present = node;
            var conditions = string.Join(" ", new[] { "Filter", "Recheck Cond", "Index Cond" }
                .Where(name => present.TryGetProperty(name, out _))
                .Select(name => present.GetProperty(name).GetString()));
            var indexes = new List<string>();
            IndexNames(node, indexes);
            scans.Add((alias.GetString()!, conditions, string.Join(",", indexes)));
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Collect(child, scans);
            }
        }
    }
}
