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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5583: the module map refresh groups <c>procedure_stats</c> inside <c>src</c>, before the tail's
/// <c>DISTINCT ON</c>. Two pins. The RESULT pin: grouping by the names too changes nothing a reader can see (the
/// newest row's names per handle, its <c>last_seen</c>, the watermark), across a handle that renames, a handle on
/// two servers, NULL names and a row exactly at the bound. The PLAN pin: at volume the shipped statements put an
/// aggregate under <c>src</c> and never sort the raw scan, which is the 2.19 GB spill (21.5 s of 35.5 s) the observed
/// large store paid. Each fact mints its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingModuleMapGroupedSourceLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the module map's grouped-source pins (each mints its own scratch database).";

    private static readonly DateTime Anchor = new(2026, 1, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static long s_collectionId = 9_000_000L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task RunLiveAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));
        var bodySucceeded = false;
        try
        {
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task InsertAsync(NpgsqlConnection c, DateTime t, string server, string handle, string? db, string? schema, string? obj, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle)
VALUES ($1,$2,1,$3,$4,$5,$6,$7)", c);
        cmd.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = t });
        cmd.Parameters.AddWithValue(server);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)db ?? DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)schema ?? DBNull.Value);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)obj ?? DBNull.Value);
        cmd.Parameters.AddWithValue(handle);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One handle's map row as <c>db|schema|object|last_seen</c>, NULL names as <c>~</c>; null when absent.</summary>
    private static async Task<string?> MapRowAsync(NpgsqlConnection c, string server, string handle, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"SELECT coalesce(database_name,'~') || '|' || coalesce(schema_name,'~') || '|' || coalesce(object_name,'~') || '|' || to_char(last_seen, 'YYYY-MM-DD HH24:MI:SS')
FROM collect.module_map WHERE server_name = $1 AND sql_handle = $2", c);
        cmd.Parameters.AddWithValue(server);
        cmd.Parameters.AddWithValue(handle);
        var v = await cmd.ExecuteScalarAsync(ct);
        return v is DBNull ? null : (string?)v;
    }

    private static async Task<int> MapCountAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT count(*)::int FROM collect.module_map", c);
        return (int)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static string At(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    /// <summary>Seeds the cases every result fact shares, relative to <paramref name="start"/> (the window's bound):
    /// <list type="bullet">
    /// <item><c>0xRENAMED</c> on server A: three name sets, the newest set in the MIDDLE of the insert order and
    /// recurring, so only the newest-by-time rule can pick it;</item>
    /// <item><c>0xSHARED</c> on A and B with different names: a handle seen on two servers attributes per server;</item>
    /// <item><c>0xNULLS</c>: NULL database/schema/object on every row, one NULL-name group;</item>
    /// <item><c>0xNULLTHENNAMED</c> (a NULL-named row older than a named one) and <c>0xNAMEDTHENNULL</c> (the
    /// reverse): the newest row wins even when its names are NULL;</item>
    /// <item><c>0xATBOUND</c>: one row exactly at the bound (read: the bound is <c>&gt;=</c>);</item>
    /// <item><c>0xBEFORE</c>: one row a tick before the bound (not read).</item></list></summary>
    private static async Task SeedAsync(NpgsqlConnection c, DateTime start, CancellationToken ct)
    {
        await InsertAsync(c, start.AddMinutes(10), "A", "0xRENAMED", "Db", "dbo", "old_name", ct);
        await InsertAsync(c, start.AddMinutes(50), "A", "0xRENAMED", "Db", "dbo", "newest_name", ct);
        await InsertAsync(c, start.AddMinutes(30), "A", "0xRENAMED", "Db", "dbo", "middle_name", ct);
        await InsertAsync(c, start.AddMinutes(5), "A", "0xRENAMED", "Db", "dbo", "old_name", ct);
        await InsertAsync(c, start.AddMinutes(20), "A", "0xRENAMED", "Db", "dbo", "newest_name", ct);

        await InsertAsync(c, start.AddMinutes(15), "A", "0xSHARED", "DbA", "dbo", "on_a", ct);
        await InsertAsync(c, start.AddMinutes(45), "B", "0xSHARED", "DbB", "sch", "on_b", ct);

        await InsertAsync(c, start.AddMinutes(11), "A", "0xNULLS", null, null, null, ct);
        await InsertAsync(c, start.AddMinutes(12), "A", "0xNULLS", null, null, null, ct);

        await InsertAsync(c, start.AddMinutes(10), "A", "0xNULLTHENNAMED", null, null, null, ct);
        await InsertAsync(c, start.AddMinutes(40), "A", "0xNULLTHENNAMED", "Db", "dbo", "named_later", ct);
        await InsertAsync(c, start.AddMinutes(10), "A", "0xNAMEDTHENNULL", "Db", "dbo", "named_earlier", ct);
        await InsertAsync(c, start.AddMinutes(40), "A", "0xNAMEDTHENNULL", null, null, null, ct);

        await InsertAsync(c, start, "A", "0xATBOUND", "Db", "dbo", "at_bound", ct);
        await InsertAsync(c, start.AddTicks(-10), "A", "0xBEFORE", "Db", "dbo", "before_bound", ct);
    }

    private static async Task AssertResultAsync(NpgsqlConnection c, DateTime start, int upserted, bool beforeBoundIsExcluded, CancellationToken ct)
    {
        Assert.Equal("Db|dbo|newest_name|" + At(start.AddMinutes(50)), await MapRowAsync(c, "A", "0xRENAMED", ct));
        Assert.Equal("DbA|dbo|on_a|" + At(start.AddMinutes(15)), await MapRowAsync(c, "A", "0xSHARED", ct));
        Assert.Equal("DbB|sch|on_b|" + At(start.AddMinutes(45)), await MapRowAsync(c, "B", "0xSHARED", ct));
        Assert.Equal("~|~|~|" + At(start.AddMinutes(12)), await MapRowAsync(c, "A", "0xNULLS", ct));
        Assert.Equal("Db|dbo|named_later|" + At(start.AddMinutes(40)), await MapRowAsync(c, "A", "0xNULLTHENNAMED", ct));
        Assert.Equal("~|~|~|" + At(start.AddMinutes(40)), await MapRowAsync(c, "A", "0xNAMEDTHENNULL", ct));
        Assert.Equal("Db|dbo|at_bound|" + At(start), await MapRowAsync(c, "A", "0xATBOUND", ct));
        if (beforeBoundIsExcluded)
        {
            Assert.Null(await MapRowAsync(c, "A", "0xBEFORE", ct));
        }

        /* one map row per (server, handle): RENAMED, SHARED x2, NULLS, NULLTHENNAMED, NAMEDTHENNULL, ATBOUND. */
        Assert.Equal(7, upserted);
        Assert.Equal(7, await MapCountAsync(c, ct));
        Assert.Equal(start.AddMinutes(50), await DarlingModuleMap.ReadWatermarkAsync(c, ct));
    }

    [Fact]
    public async Task RefreshSince_KeepsTheNewestRowsNamesPerHandle_AcrossRenamesServersNullsAndTheBound()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            await SeedAsync(connection, Anchor, ct);

            await using var cmd = new NpgsqlCommand(DarlingModuleMap.RefreshSinceSql, connection);
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Anchor });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var upserted = reader.GetInt32(0);
            await reader.CloseAsync();

            await AssertResultAsync(connection, Anchor, upserted, beforeBoundIsExcluded: true, ct);
        });
    }

    [Fact]
    public async Task RefreshDaily_KeepsTheNewestRowsNamesPerHandle_AcrossRenamesServersAndNulls()
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            /* The daily statement reads now() - 2 days with no bound parameter, so seed three hours back from the
               wall clock (far inside the window) and leave the before-the-bound case out of the assertion. */
            var now = DateTime.UtcNow.AddHours(-3);
            var start = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            await SeedAsync(connection, start, ct);

            var upserted = await DarlingModuleMap.RefreshAsync(connection, null, ct);

            /* 0xBEFORE is inside the daily window, so it is mapped too: 8 rows, not 7. */
            Assert.Equal(8, upserted);
            Assert.Equal("Db|dbo|before_bound|" + At(start.AddTicks(-10)), await MapRowAsync(connection, "A", "0xBEFORE", ct));
            await using var del = new NpgsqlCommand("DELETE FROM collect.module_map WHERE sql_handle = '0xBEFORE'", connection);
            await del.ExecuteNonQueryAsync(ct);
            await AssertResultAsync(connection, start, 7, beforeBoundIsExcluded: false, ct);
        });
    }

    /// <summary>The volume seed: 1.2 M rows over 3,000 handles ("a few thousand"; the observed store read 10.6 M rows
    /// for 21,636 handles), one generate_series statement. What matters to the plan is the rows per group.</summary>
    private const int VolumeRows = 1_200_000;

    private const int VolumeHandles = 3_000;

    private static async Task SeedVolumeAsync(NpgsqlConnection c, DateTime start, CancellationToken ct)
    {
        await using (var seed = new NpgsqlCommand(
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle)
SELECT 20000000 + g, $1 + (g % 100000) * interval '1 second', 1, 'vol' || (g % 3), 'Db' || (g % 7), 'dbo', 'proc_' || (g % $3), '0x' || (g % $3)
FROM generate_series(1, $2) AS g", c) { CommandTimeout = 600 })
        {
            seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
            seed.Parameters.AddWithValue(VolumeRows);
            seed.Parameters.AddWithValue(VolumeHandles);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using var analyze = new NpgsqlCommand("ANALYZE collect.procedure_stats", c);
        await analyze.ExecuteNonQueryAsync(ct);
    }

    private static IEnumerable<JsonElement> Children(JsonElement node)
    {
        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                yield return child;
            }
        }
    }

    private static bool IsNode(JsonElement node, string type) =>
        node.TryGetProperty("Node Type", out var t) && t.GetString() == type;

    /// <summary>True when a relation scan is reachable from <paramref name="node"/> without passing an aggregate:
    /// the node's input is the raw rows. A hypertable scan names its CHUNKS in <c>Relation Name</c>, so any
    /// relation-bearing node counts, not just one named procedure_stats.</summary>
    private static bool ReadsRawRows(JsonElement node)
    {
        foreach (var child in Children(node))
        {
            if (IsNode(child, "Aggregate"))
            {
                continue;
            }

            if (child.TryGetProperty("Relation Name", out _) || ReadsRawRows(child))
            {
                return true;
            }
        }

        return false;
    }

    private static void Collect(JsonElement node, List<JsonElement> sorts, List<JsonElement> aggregates)
    {
        if (IsNode(node, "Sort"))
        {
            sorts.Add(node);
        }

        if (IsNode(node, "Aggregate"))
        {
            aggregates.Add(node);
        }

        foreach (var child in Children(node))
        {
            Collect(child, sorts, aggregates);
        }
    }

    /// <summary>Asserts the plan shape of <paramref name="sql"/> over the volume seed: an aggregate sits over the
    /// relation scan (the <c>src</c> grouping), and no Sort has the raw rows as its input. The since statement binds
    /// <c>$1</c>; the daily one binds nothing and its seed is placed inside its <c>now() - 2 days</c> window.</summary>
    private static async Task AssertGroupedBeforeSortAsync(string sql, bool bindsSince)
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            var seedStart = bindsSince ? Anchor : DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-3), DateTimeKind.Unspecified);
            await SeedVolumeAsync(connection, seedStart, ct);

            await using var cmd = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection);
            if (bindsSince)
            {
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Anchor });
            }

            using var plan = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(ct))!);
            var sorts = new List<JsonElement>();
            var aggregates = new List<JsonElement>();
            Collect(plan.RootElement[0].GetProperty("Plan"), sorts, aggregates);

            Assert.Contains(aggregates, a => ReadsRawRows(a));
            foreach (var sort in sorts)
            {
                Assert.False(ReadsRawRows(sort),
                    "A Sort node reads the raw procedure_stats rows with no aggregate between them: the 2.19 GB spill of #5583.");
            }
        });
    }

    [Fact]
    public async Task RefreshSince_AtVolume_AggregatesUnderSrc_AndNeverSortsTheRawScan()
    {
        await AssertGroupedBeforeSortAsync(DarlingModuleMap.RefreshSinceSql, bindsSince: true);
    }

    [Fact]
    public async Task RefreshDaily_AtVolume_AggregatesUnderSrc_AndNeverSortsTheRawScan()
    {
        await AssertGroupedBeforeSortAsync(DarlingModuleMap.RefreshSql, bindsSince: false);
    }
}
