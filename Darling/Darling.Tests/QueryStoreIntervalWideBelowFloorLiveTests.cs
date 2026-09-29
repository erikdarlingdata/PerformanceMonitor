/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// #4689: below raw's chunk floor the <c>_wide</c> read reaches down to
/// <see cref="QueryStoreIntervalWide.ExactBelowFloorStart"/>. Each test seeds through the real write path
/// (<see cref="QueryStoreIntervalWideGridLiveTests.SeedGridAsync"/>), captures raw's answer BEFORE dropping raw's
/// chunks, then shows the table read at the resolved <see cref="QueryStoreIntervalWide.WideReadPlan.ReadStart"/>
/// equals it, and that a read from any earlier bound does not.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres and works entirely inside it (the chunk drops and
   the retention delete run against that database), so it cannot race live collection. */
public sealed class QueryStoreIntervalWideBelowFloorLiveTests
{
    private const int ServerId = -4689001;
    private const string ServerName = "qsiw-below-floor";
    private const int TestTop = 50;

    /* Relative to the wall clock so the tool tests' hours_back window (168 h back from now) covers the seed. */
    private static readonly DateTime S = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-6), DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime End { get; set; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<Rig> StartAsync(bool timescale, CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4689 live tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (timescale)
        {
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, S, ct);
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsiw-below-floor', 'qsiw-below-floor', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);
        await ForceFilledSinceAsync(connection, S.AddDays(-1), ct);
        var end = (DateTime)(await ScalarAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct))!;
        return new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, End = end };
    }

    private static Task<QueryStoreIntervalWide.WideReadPlan> ResolveAsync(Rig rig, CancellationToken ct) =>
        QueryStoreIntervalWide.ResolveReadAsync(
            rig.Connection, ServerId, S, rig.End, rig.End, DarlingDataReader.QueryStoreTopMinWindow, 60, null, ct);

    private static Task DropRawChunksOlderThanAsync(Rig rig, DateTime cutoff, CancellationToken ct) =>
        ExecAsync(rig.Connection, "SELECT drop_chunks('collect.query_store_stats', older_than => @cutoff)", cutoff, ct);

    private const string ChunkFloorSql = @"
SELECT MIN(range_start) AT TIME ZONE 'UTC'
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = 'query_store_stats';";

    [Fact]
    public async Task BelowFloor_TableEqualsRawTakenBeforePurge_McpTop()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: true, ct);

        await SnapshotAsync(rig, "snap_raw", DarlingDataReader.QueryStoreTopSql, S, ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(1), ct);
        var rawFloor = (DateTime?)await ScalarAsync(rig.Connection, ChunkFloorSql, ct);
        Assert.True(rawFloor is DateTime f && f > S, $"expected raw's floor to rise past {S:o}; got {rawFloor:o}");

        var plan = await ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Equal(S, plan.BelowFloorStart);
        Assert.Equal(S, plan.ReadStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.Window, plan.StartBound);

        await SnapshotAsync(rig, "snap_table", DarlingDataReader.QueryStoreTopTableSql, plan.ReadStart, ct);
        var d = await DiffAsync(rig.Connection, "snap_raw", "snap_table", ct);
        Assert.True(d.CountA > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(d.CountA, d.CountB);
        Assert.Equal(0, d.AOnly);
        Assert.Equal(0, d.BOnly);
    }

    [Fact]
    public async Task FilledSinceBound_IsLoadBearing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: true, ct);

        /* The seed already holds table rows collected before S + 12 h (day 0, at S + 1 h 10 m): backdated rows
           outside the coverage claim. */
        await ForceFilledSinceAsync(rig.Connection, S.AddHours(12), ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(1), ct);
        var plan = await ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Equal(S.AddHours(12), plan.ReadStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.FilledSince, plan.StartBound);

        await SnapshotAsync(rig, "snap_raw", DarlingDataReader.QueryStoreTopSql, S.AddHours(12), ct);
        await SnapshotAsync(rig, "snap_table", DarlingDataReader.QueryStoreTopTableSql, plan.ReadStart, ct);
        var ok = await DiffAsync(rig.Connection, "snap_raw", "snap_table", ct);
        Assert.True(ok.CountA > 0);
        Assert.Equal(0, ok.AOnly);
        Assert.Equal(0, ok.BOnly);

        await SnapshotAsync(rig, "snap_table_s", DarlingDataReader.QueryStoreTopTableSql, S, ct);
        var wrong = await DiffAsync(rig.Connection, "snap_raw", "snap_table_s", ct);
        Assert.True(wrong.AOnly + wrong.BOnly > 0, "a read bound at the window start must differ once filled_since is later");
    }

    [Fact]
    public async Task DedupeKey_WithoutExecutionType_Differs()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: false, ct);

        /* Day 0 holds an Aborted and an Exception row for one interval and plan. */
        const string withType = "first_execution_time, execution_type_desc, replica_role";
        Assert.Contains(withType, DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);
        var withoutType = DarlingDataReader.QueryStoreTopSql.Replace(withType, "first_execution_time, replica_role", StringComparison.Ordinal);

        await SnapshotAsync(rig, "snap_raw", DarlingDataReader.QueryStoreTopSql, S, ct);
        await SnapshotAsync(rig, "snap_ref", withoutType, S, ct);
        await SnapshotAsync(rig, "snap_table", DarlingDataReader.QueryStoreTopTableSql, S, ct);

        var real = await DiffAsync(rig.Connection, "snap_raw", "snap_table", ct);
        Assert.Equal(0, real.AOnly + real.BOnly);
        var reference = await DiffAsync(rig.Connection, "snap_ref", "snap_table", ct);
        Assert.True(reference.AOnly + reference.BOnly > 0, "a dedupe without execution_type_desc must not equal the table read");
    }

    [Fact]
    public async Task Boundary_IsInclusive()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: true, ct);

        var edge = (DateTime)(await ScalarAsync(rig.Connection,
            "SELECT MIN(collection_time) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND query_id = 3", ct))!;
        await ForceFilledSinceAsync(rig.Connection, edge, ct);
        await SnapshotAsync(rig, "snap_raw", DarlingDataReader.QueryStoreTopSql, edge, ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(2), ct);

        var plan = await ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Equal(edge, plan.ReadStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.FilledSince, plan.StartBound);

        await SnapshotAsync(rig, "snap_table", DarlingDataReader.QueryStoreTopTableSql, plan.ReadStart, ct);
        var d = await DiffAsync(rig.Connection, "snap_raw", "snap_table", ct);
        Assert.True(await ScalarAsync(rig.Connection, "SELECT COUNT(*) FROM snap_table WHERE query_id = 3", ct) is long n && n > 0,
            "the row collected exactly at the read start must be returned");
        Assert.Equal(0, d.AOnly);
        Assert.Equal(0, d.BOnly);
    }

    [Fact]
    public async Task TablePurgeEdge_Bound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: true, ct);

        await SnapshotAsync(rig, "snap_raw", DarlingDataReader.QueryStoreTopSql, S, ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(2), ct);
        await PurgeTableAsync(rig.Connection, S.AddMinutes(90), ct);

        var floor = (DateTime)(await ScalarAsync(rig.Connection,
            "SELECT MIN(first_execution_time) FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct))!;
        Assert.Equal(S.AddHours(2), floor);

        var plan = await ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.Equal(floor + QueryStoreIntervalWide.IntervalSpanMargin, plan.ReadStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.TablePurgeEdge, plan.StartBound);

        await SnapshotAsync(rig, "snap_table_s", DarlingDataReader.QueryStoreTopTableSql, S, ct);
        var wrong = await DiffAsync(rig.Connection, "snap_raw", "snap_table_s", ct);
        Assert.True(wrong.AOnly > 0, "reading from the window start must miss the intervals the purge deleted");
    }

    [Fact]
    public async Task PlainStore_ReadStartIsWindowStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: false, ct);

        var plan = await ResolveAsync(rig, ct);
        Assert.Null(plan.BelowFloorStart);
        Assert.Equal(S, plan.ReadStart);
        Assert.Equal(S, plan.ClampedStart);
        Assert.Equal(QueryStoreIntervalWide.WideStartBound.Window, plan.StartBound);
        Assert.Contains("collection_time >= $2", DarlingDataReader.QueryStoreTopTableSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Disclosure_EffectiveStart_IsTheExactBound_AndTheNoteNamesTheReason()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(timescale: true, ct);

        await ForceFilledSinceAsync(rig.Connection, S.AddHours(12), ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(1), ct);
        var rawJson = await DarlingMcpDataTools.GetQueryStoreTop(rig.Postgres, ServerName, 168, TestTop, cancellationToken: ct);
        var filled = JsonDocument.Parse(rawJson).RootElement;
        Assert.Equal("interval_table", filled.GetProperty("history_source").GetString());
        Assert.Equal(S.AddHours(12), DateTime.Parse(filled.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
        Assert.True(filled.GetProperty("window_truncated").GetBoolean());
        Assert.Contains("began keeping complete history", filled.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);

        await ForceFilledSinceAsync(rig.Connection, S.AddDays(-1), ct);
        await PurgeTableAsync(rig.Connection, S.AddMinutes(90), ct);
        await DropRawChunksOlderThanAsync(rig, S.AddDays(2), ct);
        var edge = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(rig.Postgres, ServerName, 168, TestTop, cancellationToken: ct)).RootElement;
        Assert.Equal("interval_table", edge.GetProperty("history_source").GetString());
        Assert.Equal(S.AddDays(1).AddHours(2), DateTime.Parse(edge.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
        Assert.Contains("keeps 9 days", edge.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
    }

    /* ---- helpers ------------------------------------------------------------------------------------------ */

    /// <summary>Runs the real retention delete for the interval table, slice by slice, until it deletes nothing.</summary>
    private static async Task PurgeTableAsync(NpgsqlConnection connection, DateTime cutoff, CancellationToken ct)
    {
        var sql = DarlingRetention.TimeSlicedDeleteSql("collect.query_store_interval_wide", "first_execution_time");
        int deleted;
        do
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
            deleted = await command.ExecuteNonQueryAsync(ct);
        }
        while (deleted > 0);
    }

    private static async Task SnapshotAsync(Rig rig, string name, string topSql, DateTime start, CancellationToken ct)
    {
        await ExecAsync(rig.Connection, "DROP TABLE IF EXISTS " + name, null, ct);
        await using var command = new NpgsqlCommand("CREATE TEMP TABLE " + name + " AS " + topSql, rig.Connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(rig.End, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TestTop });
        for (var i = 0; i < 3; i++)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(long AOnly, long BOnly, long CountA, long CountB)> DiffAsync(NpgsqlConnection connection, string a, string b, CancellationToken ct)
    {
        static long L(object? o) => Convert.ToInt64(o, CultureInfo.InvariantCulture);
        return (
            L(await ScalarAsync(connection, $"SELECT COUNT(*) FROM ((SELECT * FROM {a}) EXCEPT ALL (SELECT * FROM {b})) AS d", ct)),
            L(await ScalarAsync(connection, $"SELECT COUNT(*) FROM ((SELECT * FROM {b}) EXCEPT ALL (SELECT * FROM {a})) AS d", ct)),
            L(await ScalarAsync(connection, $"SELECT COUNT(*) FROM {a}", ct)),
            L(await ScalarAsync(connection, $"SELECT COUNT(*) FROM {b}", ct)));
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime? cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        if (cutoff is DateTime c)
        {
            command.Parameters.AddWithValue("cutoff", DateTime.SpecifyKind(c, DateTimeKind.Unspecified));
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        var v = await command.ExecuteScalarAsync(ct);
        return v is DBNull ? null : v;
    }
}
