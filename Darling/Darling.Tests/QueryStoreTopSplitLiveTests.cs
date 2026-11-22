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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9276: the Query Store top read's window as two arms, on a real store. The one-floor read keeps a 26 hour
/// <c>first_execution_time</c> floor for every row, so on a store with a day of older intervals it fetches their heap rows only for
/// the window to drop them. With <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> built, the split read gives rows with
/// an interval start an hour's floor and reads the rows without one through the partial index. The tests prove the two shapes return
/// the same rows (a NULL-start row whose first execution is hours before the window included), that the split read drops far fewer
/// fetched rows, that the index leaves the upsert's update heap-only while an interval's start is unchanged, and that the read can
/// tell the index is built.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreTopSplitLiveTests
{
    internal const string Wide = "collect.query_store_interval_wide";
    internal const int ServerId = 9276;

    /// <summary>Rows with an interval start inside the window (query_id 1..WindowRows) and rows with none (query_id 10001..10000+NullStartRows); the seed and the asserts share them.</summary>
    internal const int WindowRows = 300;
    internal const int NullStartRows = 20;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    internal static string Ts(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /* Three groups of rows for one server, all inside the days the migrated store accepts:
         window rows      query_id 1..300: an interval start inside the window, first execution ten minutes after it;
         NULL-start rows  query_id 10001..10000+NullStartRows: no interval start, collected inside the window, first execution FIVE HOURS before the
                          window starts, so only the 26 hour floor keeps them (the one-hour floor of an interval-start row would drop them);
         older rows       query_id 20001..23000: an interval start BEFORE the window and a first execution two to twenty hours before it.
                          The window drops all of them, and the one-floor read fetches every one of them first. */
    internal static async Task SeedAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct, int nullStartFirstExecHoursBefore = 5, string database = "db")
    {
        var start = end.AddHours(-24);
        await ExecAsync(connection, string.Create(CultureInfo.InvariantCulture, $@"
INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT s + interval '1 hour', {ServerId}, '{database}', g, g, 'Regular', s + interval '10 minutes', s + interval '50 minutes',
       'select 1', 10 + g, 1000 + g, g, s
FROM (SELECT g, timestamp '{Ts(start)}' + make_interval(mins => (g % 1380)) AS s FROM generate_series(1, {WindowRows}) g) AS w;

INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT timestamp '{Ts(start)}' + make_interval(mins => 30 + (g - 10000)), {ServerId}, '{database}', g, g, 'Regular', timestamp '{Ts(start)}' - interval '{nullStartFirstExecHoursBefore} hours',
       timestamp '{Ts(start)}' - interval '{nullStartFirstExecHoursBefore - 1} hours', 'select 1', 10 + g, 1000 + g, -1, NULL
FROM generate_series(10001, {10000 + NullStartRows}) g;

INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT f + interval '1 hour', {ServerId}, '{database}', g, g, 'Regular', f, f + interval '20 minutes',
       'select 1', 10 + g, 1000 + g, g, f - interval '5 minutes'
FROM (SELECT g, timestamp '{Ts(start)}' - interval '2 hours' - make_interval(mins => (g % 1080)) AS f FROM generate_series(20001, 23000) g) AS o;

ANALYZE {Wide};"), ct);
    }

    internal static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    internal static NpgsqlParameter Timestamp(DateTime value) =>
        new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

    private static NpgsqlParameter NullText() => new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value };

    /// <summary>The statement's rows exactly as the product binds it, stringified and sorted so two runs compare exactly.</summary>
    private static async Task<List<string>> RunAsync(NpgsqlConnection connection, string sql, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Timestamp(end.AddHours(-24)));
        command.Parameters.Add(Timestamp(end));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 500 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(NullText());
        command.Parameters.Add(NullText());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(500) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v switch
            {
                DBNull => "NULL",
                DateTime stamp => stamp.ToString("O", CultureInfo.InvariantCulture),
                _ => Convert.ToString(v, CultureInfo.InvariantCulture),
            })));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary>
    /// The rows the statement's scans of the wide table fetched and then dropped: the sum of <c>Rows Removed by Filter</c> over the plan,
    /// from <c>EXPLAIN ANALYZE</c> with sequential scans off (a store far larger than this one is read through its btrees, and the
    /// small seed would otherwise read a leaf whole).
    /// </summary>
    private static async Task<long> RowsRemovedByFilterAsync(NpgsqlConnection connection, string sql, DateTime end, CancellationToken ct)
    {
        await ExecAsync(connection, "SET plan_cache_mode = force_custom_plan", ct);
        await ExecAsync(connection, "SET enable_seqscan = off", ct);
        await ExecAsync(connection, "PREPARE split_probe(integer, timestamp, timestamp, integer, text[], text, text, integer) AS " + sql, ct);
        try
        {
            var arguments = string.Create(
                CultureInfo.InvariantCulture,
                $"{ServerId}, '{Ts(end.AddHours(-24))}', '{Ts(end)}', 500, NULL, NULL, NULL, {TopFill.FirstCandidates(500)}");
            await using var command = new NpgsqlCommand($"EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE split_probe({arguments})", connection) { CommandTimeout = 120 };
            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            long removed = 0;
            void Walk(JsonElement node)
            {
                if (node.ValueKind == JsonValueKind.Object)
                {
                    foreach (var property in node.EnumerateObject())
                    {
                        if (property.Name == "Rows Removed by Filter")
                        {
                            removed += property.Value.GetInt64();
                        }
                        else
                        {
                            Walk(property.Value);
                        }
                    }
                }
                else if (node.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in node.EnumerateArray())
                    {
                        Walk(item);
                    }
                }
            }

            Walk(document.RootElement);
            return removed;
        }
        finally
        {
            await ExecAsync(connection, "DEALLOCATE split_probe", ct);
            await ExecAsync(connection, "RESET enable_seqscan", ct);
        }
    }

    [Fact]
    public async Task TheSplitRead_ReturnsTheRowsOfTheOneFloorRead_AndFetchesFarFewerDroppedRowsOnceTheIndexIsBuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9276 live test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        await SeedAsync(connection, end, ct);

        Assert.False(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));

        var oneFloor = await RunAsync(connection, DarlingDataReader.QueryStoreTopTableSql, end, ct);
        var split = await RunAsync(connection, DarlingDataReader.QueryStoreTopTableSplitSql, end, ct);

        /* Non-vacuous: the window rows and all 20 NULL-start rows, whose first execution is five hours before the window, come back
           in both shapes, and not one of the 3,000 older rows does. */
        Assert.Equal(WindowRows + NullStartRows, oneFloor.Count);
        Assert.Equal(oneFloor, split);
        Assert.Equal(NullStartRows, split.Count(row => long.Parse(row.Split('|')[1], CultureInfo.InvariantCulture) > 10000));

        var oneFloorRemoved = await RowsRemovedByFilterAsync(connection, DarlingDataReader.QueryStoreTopTableSql, end, ct);
        var splitRemoved = await RowsRemovedByFilterAsync(connection, DarlingDataReader.QueryStoreTopTableSplitSql, end, ct);
        Assert.True(oneFloorRemoved >= 2500, $"the one-floor read must fetch the older rows to drop them; it dropped {oneFloorRemoved}");
        Assert.True(splitRemoved * 10 <= oneFloorRemoved, $"the split read dropped {splitRemoved} fetched rows against {oneFloorRemoved} for the one-floor read");
    }

    [Fact]
    public async Task TheUpsertsSetList_StaysHot_WhenTheIntervalStartIsUnchanged_AndNotWhenANullStartIsFilled()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9276 live test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        await SeedAsync(connection, end, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct), "the claim is about the index being present");

        /* Counters change, the start is written back with the value it has: value to the same value on the window rows, NULL to NULL on
           the NULL-start rows. Both kinds are in the update. */
        var where = $"server_id = {ServerId} AND (query_id <= 50 OR query_id BETWEEN 10001 AND {10000 + NullStartRows})";
        var sameStart = QueryStoreIntervalWideBrinIndexLiveTests.UpsertShapedSet + ", interval_start_time_utc = interval_start_time_utc";
        var (updated, hot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(connection, sameStart, where, ct);
        Assert.Equal(50 + NullStartRows, updated);
        Assert.Equal(updated, hot);

        /* The cost the index does have: filling a NULL start changes a column its predicate names, so those updates are not HOT. */
        var (fillUpdated, fillHot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(
            connection, "interval_start_time_utc = collection_time", $"server_id = {ServerId} AND query_id BETWEEN 10001 AND {10000 + NullStartRows}", ct);
        Assert.Equal(NullStartRows, fillUpdated);
        Assert.Equal(0, fillHot);

        /* The control the HOT claim can fail on: the same partial index over a column the counters change makes the same update non-HOT. */
        await ExecAsync(connection, $"DROP INDEX {QueryStoreBackgroundIndexes.WideNullStartFirstExecIndexName}", ct);
        await ExecAsync(connection, $"CREATE INDEX ix_counter_probe ON {Wide} (server_id, first_execution_time) WHERE execution_count IS NULL", ct);
        var (controlUpdated, controlHot) = await QueryStoreIntervalWideBrinIndexLiveTests.UpdateAndReadHotAsync(connection, sameStart, where, ct);
        Assert.Equal(50 + NullStartRows, controlUpdated);
        Assert.Equal(0, controlHot);
    }

    [Fact]
    public async Task TheAbortsSplitRead_ReturnsTheRowsOfItsOneFloorRead()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9276 live test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        await SeedAsync(connection, end, ct);
        await ExecAsync(connection, $"UPDATE {Wide} SET execution_type_desc = 'Aborted' WHERE server_id = {ServerId} AND (query_id % 3 = 0)", ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);

        var oneFloor = await RunAbortsAsync(connection, DarlingQueryStoreAbortsReader.TableSql, end, ct);
        var split = await RunAbortsAsync(connection, DarlingQueryStoreAbortsReader.TableSplitSql, end, ct);
        Assert.True(oneFloor.Count >= 100, $"the aborted rows must come back; {oneFloor.Count} did");
        Assert.Contains(oneFloor, row => long.Parse(row.Split('|')[1], CultureInfo.InvariantCulture) > 10000);
        Assert.Equal(oneFloor, split);
    }

    /// <summary>
    /// The shipped path. <c>GetQueryStoreTopWithReachAsync</c> and <c>DarlingQueryStoreAbortsReader.ReadAsync</c> pick the split statement once
    /// <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built, then narrow its floor to the server's own interval length and
    /// add the tag join and the self-key test. This runs both readers on a store the gate accepts, with 60-minute health rows (a three hour
    /// floor), once before the index exists and once after <c>EnsureAllAsync</c> built it, and requires the replies to be the same and to
    /// hold the NULL-start rows. The one-floor statement at the full 26 hour floor is the oracle for the reply itself.
    /// </summary>
    [Fact]
    public async Task TheShippedTopAndAbortsReaders_ReplyTheSame_BeforeAndAfterTheIndexIsBuilt_AtANarrowedEdgeMargin()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9276 live test.");
        var ct = TestContext.Current.CancellationToken;

        /* A fixed window that starts at 06:00, so it holds no whole UTC day: the top read keeps its interval statement instead of the daily one. */
        var start = new DateTime(2026, 9, 15, 6, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddHours(24);

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        /* The grid seed gives the gate its coverage; the rows of SeedAsync go in beside it in the same database. The NULL-start rows' first
           execution is two hours before the window, inside the three hour floor the health rows resolve to. */
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, start, ct);
        await SeedAsync(connection, end, ct, nullStartFirstExecHoursBefore: 2, database: "qsA");
        await ExecAsync(connection, $"UPDATE {Wide} SET execution_type_desc = 'Aborted' WHERE server_id = {ServerId} AND query_id < 20001 AND (query_id % 3 = 0)", ct);
        await ExecAsync(connection, $"""
            UPDATE collect.query_store_interval_wide_coverage SET filled_since = timestamp '{Ts(start.AddDays(-1))}' WHERE server_id = {ServerId}
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
            VALUES ({ServerId}, 'qs-top-split', 'qs-top-split', TRUE, 16, now(), now())
            ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, {ServerId}, 'qs-top-split', d.d, 60
            FROM generate_series(timestamp '{Ts(start.AddDays(-2))}', timestamp '{Ts(end)}', interval '6 hours') AS c(c)
            CROSS JOIN (VALUES ('qsA'), ('qsB')) AS d(d)
            """, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);

        Assert.Equal(TimeSpan.FromHours(3), await QueryStoreIntervalWide.ResolveEdgeMarginAsync(connection, ServerId, start, end, 60, ct));

        async Task<(List<(string Database, long Query, long Plan, string Outcome)> Top, List<DarlingQueryStoreAbortsReader.AbortRow> Aborts)> ReadBothAsync()
        {
            using var scope = ReadScope.Open(new CapturingTestLogger());
            var top = await DarlingDataReader.GetQueryStoreTopWithReachAsync(postgres, ServerId, start, end, 500, DatabaseFilter.All, null, null, ct);
            Assert.NotNull(top.Table);
            var aborts = await DarlingQueryStoreAbortsReader.ReadAsync(postgres, ServerId, start, end, 500, DatabaseFilter.All, ct);
            Assert.NotNull(aborts.Table);
            Assert.Null(scope.Fallback);
            return (top.Rows.Select(r => (r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc)).OrderBy(k => k).ToList(), aborts.Rows);
        }

        Assert.False(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var before = await ReadBothAsync();
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var after = await ReadBothAsync();

        /* Non-vacuous: the window's rows and all the NULL-start rows come back from the top read, and aborted rows from the aborts read,
           NULL-start ones among them. */
        Assert.True(before.Top.Count(k => k.Query is > 10000 and < 20000) == NullStartRows, "every NULL-start row must be in the top reply");
        Assert.True(before.Top.Count(k => k.Query <= WindowRows) >= WindowRows, "the window rows must be in the top reply");
        Assert.DoesNotContain(before.Top, k => k.Query >= 20001);
        Assert.Contains(before.Aborts, row => row.QueryId > 10000);
        Assert.Equal(before.Top, after.Top);
        Assert.Equal(before.Aborts, after.Aborts);

        /* The oracle: the one-floor statement at the full 26 hour floor names the same statements. */
        var oracle = (await RunAsync(connection, DarlingDataReader.QueryStoreTopTableSql, end, ct))
            .Select(row => long.Parse(row.Split('|')[1], CultureInfo.InvariantCulture)).Distinct().OrderBy(q => q).ToList();
        Assert.Equal(oracle, after.Top.Select(k => k.Query).Distinct().OrderBy(q => q).ToList());
    }

    private static async Task<List<string>> RunAbortsAsync(NpgsqlConnection connection, string sql, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        DarlingMcpReadParameters.AddWindow(command, ServerId, end.AddHours(-24), end);
        DarlingMcpReadParameters.AddInt(command, 500);
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v switch
            {
                DBNull => "NULL",
                DateTime stamp => stamp.ToString("O", CultureInfo.InvariantCulture),
                _ => Convert.ToString(v, CultureInfo.InvariantCulture),
            })));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
