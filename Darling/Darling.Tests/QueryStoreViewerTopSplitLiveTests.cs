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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9283: the Viewer grid's table read with its window as two arms, on a real store. The MCP top read has taken that shape since #9276,
/// once <see cref="QueryStoreBackgroundIndexes.WideNullStartFirstExec"/> is built. The grid's statement differs: its end can be open
/// ($3 NULL), and it runs as a keys statement and then a by-keys statement. The tests prove the split pair returns the rows of the
/// one-statement read it replaces (a NULL-start row whose first execution is hours before the window included, at a literal end and at an
/// open end), that its keys statement reads far fewer heap rows to drop, and that the grid answers the same before and after the index
/// is built.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreViewerTopSplitLiveTests
{
    private const string Wide = QueryStoreTopSplitLiveTests.Wide;
    private const int ServerId = QueryStoreTopSplitLiveTests.ServerId;
    private const int Top = 500;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static void AddShared(NpgsqlCommand command, DateTime start, DateTime? end)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(start));
        command.Parameters.Add(end is { } e
            ? QueryStoreTopSplitLiveTests.Timestamp(e)
            : new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
    }

    private static async Task<List<string>> ReadAllAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
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

        return rows;
    }

    /// <summary>The one-statement read the split pair stands in for, every column of every row, in the order it returns them.</summary>
    private static async Task<List<string>> OneStatementAsync(NpgsqlConnection connection, DateTime start, DateTime? end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection) { CommandTimeout = 120 };
        AddShared(command, start, end);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        return await ReadAllAsync(command, ct);
    }

    /// <summary>The grid's two statements as it runs them: the kept keys, then the rows of those keys with plain index scans off.</summary>
    private static async Task<List<string>> TwoStatementsAsync(
        NpgsqlConnection connection, string keysSql, string byKeysSql, DateTime start, DateTime? end, CancellationToken ct)
    {
        var keys = new ViewerDataService.TableReadKeys();
        await using (var keysCommand = new NpgsqlCommand(keysSql, connection) { CommandTimeout = 120 })
        {
            AddShared(keysCommand, start, end);
            keysCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
            await using var keyReader = await keysCommand.ExecuteReaderAsync(ct);
            while (await keyReader.ReadAsync(ct))
            {
                keys.Add(keyReader);
            }
        }

        await ExecAsync(connection, "SET enable_indexscan = off", ct);
        try
        {
            await using var byKeys = new NpgsqlCommand(byKeysSql, connection) { CommandTimeout = 120 };
            AddShared(byKeys, start, end);
            keys.AddParameters(byKeys);
            keys.AddFilterParameters(byKeys);
            return await ReadAllAsync(byKeys, ct);
        }
        finally
        {
            await ExecAsync(connection, "RESET enable_indexscan", ct);
        }
    }

    /// <summary>
    /// The keys statement's plan under <c>EXPLAIN (ANALYZE, BUFFERS)</c>, sequential scans off (a store far larger than this one is read through
    /// its btrees): the rows its scans fetched and dropped, and the shared blocks (hit plus read) the whole plan touched.
    /// </summary>
    private static async Task<(long RowsRemoved, long Blocks)> ExplainKeysAsync(NpgsqlConnection connection, string keysSql, DateTime start, DateTime? end, CancellationToken ct)
    {
        await ExecAsync(connection, "SET plan_cache_mode = force_custom_plan", ct);
        await ExecAsync(connection, "SET enable_seqscan = off", ct);
        await ExecAsync(connection, "PREPARE viewer_split_probe(integer, timestamp, timestamp, integer, text[], integer) AS " + keysSql, ct);
        try
        {
            var endText = end is { } e ? $"'{QueryStoreTopSplitLiveTests.Ts(e)}'" : "NULL";
            var arguments = string.Create(CultureInfo.InvariantCulture, $"{ServerId}, '{QueryStoreTopSplitLiveTests.Ts(start)}', {endText}, {Top}, NULL, {Top}");
            await using var command = new NpgsqlCommand(
                $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE viewer_split_probe({arguments})", connection) { CommandTimeout = 120 };
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
            var root = document.RootElement[0].GetProperty("Plan");
            var blocks = root.GetProperty("Shared Hit Blocks").GetInt64() + root.GetProperty("Shared Read Blocks").GetInt64();
            return (removed, blocks);
        }
        finally
        {
            await ExecAsync(connection, "DEALLOCATE viewer_split_probe", ct);
            await ExecAsync(connection, "RESET enable_seqscan", ct);
        }
    }

    /// <summary>
    /// Same seed as the MCP split test: window rows with an interval start, NULL-start rows whose first execution is five hours before the window
    /// (kept only by the 26 hour floor), and 3,000 older rows the window drops. With the index built the split pair returns the one-statement
    /// read's rows at a literal end and at an open end, and the open end returns more rows than the literal end, so the two ends are not the same
    /// read. The split keys statement then fetches and drops far fewer rows, and touches fewer blocks.
    /// </summary>
    [Fact]
    public async Task TheSplitKeysAndByKeysStatements_ReturnTheRowsOfTheOneStatementRead_AtALiteralAndAnOpenEnd_AndDropFarFewerFetchedRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9283 live test.");
        var ct = TestContext.Current.CancellationToken;
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-24);
        var literalEnd = start.AddHours(2);

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await QueryStoreTopSplitLiveTests.OpenAsync(scratch, ct);
        await QueryStoreTopSplitLiveTests.SeedAsync(connection, end, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));

        var literalOracle = await OneStatementAsync(connection, start, literalEnd, ct);
        var literalSplit = await TwoStatementsAsync(
            connection, ViewerDataService.QueryStoreTopTableSplitKeysSql, ViewerDataService.QueryStoreTopTableSplitByKeysSql, start, literalEnd, ct);
        var openOracle = await OneStatementAsync(connection, start, null, ct);
        var openSplit = await TwoStatementsAsync(
            connection, ViewerDataService.QueryStoreTopTableSplitKeysSql, ViewerDataService.QueryStoreTopTableSplitByKeysSql, start, null, ct);

        /* Non-vacuous: the open end reads every window row and all 20 NULL-start rows, the literal end fewer, and no older row is read at either. */
        Assert.Equal(QueryStoreTopSplitLiveTests.WindowRows + QueryStoreTopSplitLiveTests.NullStartRows, openOracle.Count);
        Assert.InRange(literalOracle.Count, QueryStoreTopSplitLiveTests.NullStartRows + 1, openOracle.Count - 1);
        Assert.Equal(literalOracle, literalSplit);
        Assert.Equal(openOracle, openSplit);
        Assert.Equal(QueryStoreTopSplitLiveTests.NullStartRows, openSplit.Count(row => row.Split('|')[1] is { } id && long.Parse(id, CultureInfo.InvariantCulture) is > 10000 and < 20000));

        foreach (var (label, endOrNull) in new[] { ("literal end", (DateTime?)literalEnd), ("open end", null) })
        {
            var oneFloor = await ExplainKeysAsync(connection, ViewerDataService.QueryStoreTopTableKeysSql, start, endOrNull, ct);
            var split = await ExplainKeysAsync(connection, ViewerDataService.QueryStoreTopTableSplitKeysSql, start, endOrNull, ct);
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"Total: #9283 {label}: one-floor keys read dropped {oneFloor.RowsRemoved} rows, {oneFloor.Blocks} shared blocks. split keys read dropped {split.RowsRemoved} rows, {split.Blocks} shared blocks."));
            Assert.True(oneFloor.RowsRemoved >= 2500, $"{label}: the one-floor read must fetch the older rows to drop them; it dropped {oneFloor.RowsRemoved}");
            Assert.True(split.RowsRemoved * 10 <= oneFloor.RowsRemoved, $"{label}: the split read dropped {split.RowsRemoved} fetched rows against {oneFloor.RowsRemoved} for the one-floor read");
            Assert.True(split.Blocks < oneFloor.Blocks, $"{label}: the split read touched {split.Blocks} blocks against {oneFloor.Blocks} for the one-floor read");
        }
    }

    /// <summary>
    /// The shipped grid read, before and after the index is built: the same rows both times, the NULL-start rows among them, none of the older
    /// rows, and the same as the one-statement oracle at the full 26 hour floor. Before the build the read keeps its one-floor statements (the
    /// fallback), after it the grid is served by the split pair.
    /// </summary>
    [Fact]
    public async Task TheGrid_ReadsTheSameRows_BeforeAndAfterTheIndexIsBuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9283 live test.");
        var ct = TestContext.Current.CancellationToken;

        /* A fixed window that starts at 06:00, so it holds no whole UTC day: the grid keeps its interval statements instead of the rollup's. */
        var start = new DateTime(2026, 9, 15, 6, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddHours(24);

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await QueryStoreTopSplitLiveTests.OpenAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        /* The grid seed gives the gate its coverage and the rows of SeedAsync go in beside it. The NULL-start rows' first execution is two hours
           before the window, inside the three hour floor the health rows resolve to. */
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, start, ct);
        await QueryStoreTopSplitLiveTests.SeedAsync(connection, end, ct, nullStartFirstExecHoursBefore: 2, database: "qsA");
        await ExecAsync(connection, $"""
            UPDATE collect.query_store_interval_wide_coverage SET filled_since = timestamp '{QueryStoreTopSplitLiveTests.Ts(start.AddDays(-1))}' WHERE server_id = {ServerId}
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
            VALUES ({ServerId}, 'qs-viewer-top-split', 'qs-viewer-top-split', TRUE, 16, now(), now())
            ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE
            """, ct);
        await ExecAsync(connection, $"""
            INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, interval_length_minutes)
            SELECT row_number() OVER (), c.c, {ServerId}, 'qs-viewer-top-split', d.d, 60
            FROM generate_series(timestamp '{QueryStoreTopSplitLiveTests.Ts(start.AddDays(-2))}', timestamp '{QueryStoreTopSplitLiveTests.Ts(end)}', interval '6 hours') AS c(c)
            CROSS JOIN (VALUES ('qsA'), ('qsB')) AS d(d)
            """, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        async Task<List<(string Database, long Query, long Plan, string Outcome)>> ReadGridAsync()
        {
            var read = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, start, end, Top, null, null, null, ct);
            Assert.NotNull(read.Plan);
            return read.Rows.Select(r => (r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc)).OrderBy(k => k).ToList();
        }

        Assert.False(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var before = await ReadGridAsync();
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Wide}", ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var after = await ReadGridAsync();

        Assert.Equal(QueryStoreTopSplitLiveTests.NullStartRows, before.Count(k => k.Query is > 10000 and < 20000));
        Assert.True(before.Count(k => k.Query <= QueryStoreTopSplitLiveTests.WindowRows) >= QueryStoreTopSplitLiveTests.WindowRows, "the window rows must be in the grid");
        Assert.DoesNotContain(before, k => k.Query >= 20001);
        Assert.Equal(before, after);

        /* The oracle: the one-statement read at the full 26 hour floor names the same statements. */
        var oracle = (await OneStatementAsync(connection, start, end, ct)).Select(row => row.Split('|')[1])
            .Select(id => long.Parse(id, CultureInfo.InvariantCulture)).Distinct().OrderBy(q => q).ToList();
        Assert.Equal(oracle, after.Select(k => k.Query).Distinct().OrderBy(q => q).ToList());
    }
}
