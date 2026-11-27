/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9148 LIVE: <c>get_active_queries</c> with <c>snapshots_only</c> over a long window. The list needs a capture time and a count,
/// but its first CTE was read by three others, so the planner stored every row of the window, query text and all, in a temp file
/// before the first count. The list's CTE is now read in place. The first test holds the answer to an independent count of the
/// seeded captures (WAITFOR rows, NULL text, NULL program names, monitor sessions, head blockers, both window edges); the second
/// holds the statement's temp-file writes to zero at the planner's default work_mem, where the old statement wrote thousands of blocks.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class ActiveQuerySnapshotsSpillLiveTests
{
    private const int ServerId = 91480;
    private const int Captures = 1200;

    [Fact]
    public async Task TheList_CountsEachCapturesPopulation_AsAnIndependentCountOfTheSeedDoes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9148 snapshot list tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var end = await SeedAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        // The window is (start, end]: the oldest capture sits exactly on start and is out, the newest sits on end and is in.
        var start = end.AddMinutes(-(Captures - 1));
        var page = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, Captures + 10, DatabaseFilter.All, false, null, ct);

        var expected = Enumerable.Range(0, Captures - 1)
            .Select(m => (Time: end.AddMinutes(-m), Count: ExpectedRows(m)))
            .ToList();
        Assert.Equal(expected.Select(x => x.Time), page.Snapshots.Select(s => s.CollectionTime));
        Assert.Equal(expected.Select(x => (long)x.Count), page.Snapshots.Select(s => s.RowCount));
        Assert.Equal(expected.Count, page.SnapshotCount);

        // The cap cuts snapshots from the old end, and the count above it stays the whole population's.
        var capped = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 7, DatabaseFilter.All, false, null, ct);
        Assert.Equal(expected.Take(7).Select(x => x.Time), capped.Snapshots.Select(s => s.CollectionTime));
        Assert.Equal(expected.Count, capped.SnapshotCount);

        // blocking_only rides the same population: the victims that pass the other filters, plus every head blocker in a capture.
        var blocking = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, Captures + 10, DatabaseFilter.All, true, null, ct);
        Assert.Equal(
            Enumerable.Range(0, Captures - 1).Select(m => (long)ExpectedBlockingRows(m)),
            blocking.Snapshots.Select(s => s.RowCount));
    }

    [Fact]
    public async Task TheListStatement_WritesNoTempFiles_ForAWindowOfWideRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live #9148 snapshot list tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var end = await SeedAsync(connection, ct);

        // The planner's default memory for one sort or hash, so a server tuned with more room does not hide the spill.
        await using (var set = new NpgsqlCommand("SET work_mem = '4MB'", connection))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingSessionReader.ActiveQuerySnapshotsSql, connection);
        explain.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end.AddMinutes(-(Captures - 1)), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end, NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 101 });
        explain.Parameters.Add(DatabaseFilter.All.Parameter());
        explain.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        explain.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = false });

        using var document = JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
        var tempWritten = document.RootElement[0].GetProperty("Plan").GetProperty("Temp Written Blocks").GetInt64();

        // The old statement stored the whole window in a temp file: about 3,000 blocks at this size.
        Assert.True(tempWritten == 0, $"The snapshot list wrote {tempWritten} temp blocks for a window of {Captures} captures; it should count them without storing the rows.");
    }

    private static readonly TimeSpan Micros = TimeSpan.FromTicks(1234560);

    /// <summary>One capture per minute, newest at the returned instant, which carries 123456 microseconds. Capture m holds sessions 1..(20 + m % 21),
    /// shaped by session number: text is NULL at r%7=0, a WAITFOR at r%7=1, long text otherwise; program is NULL at r%10=0, a monitor session
    /// at r%10=1, else an application; r%11=5 is blocked by the session before it, which makes that session a head blocker in its capture.</summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        var end = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified) + Micros;
        await ExecAsync(connection, "INSERT INTO servers (server_id, server_name) VALUES ($1, 'snapshot-spill')", ct, ServerId);
        await ExecAsync(connection,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status,
                                           blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, logical_reads, reads, writes, granted_query_memory_gb,
                                           transaction_isolation_level, open_transaction_count, login_name, host_name, program_name, dop, parallel_worker_count)
              SELECT (m::bigint * 1000) + r, $1 - make_interval(mins => m), $2, 'snapshot-spill', 100 + r, 'db' || (r % 5),
                     '00 00:00:05.000',
                     CASE WHEN r % 7 = 0 THEN NULL WHEN r % 7 = 1 THEN 'WAITFOR DELAY ''00:10''' ELSE repeat(md5(m::text || '/' || r::text), 20) END,
                     'running', CASE WHEN r % 11 = 5 THEN 100 + r - 1 ELSE 0 END, CASE WHEN r % 11 = 5 THEN 'LCK_M_S' END,
                     100, r * 1000, 5000, 100, 10, 0, 0.5, 'Read Committed', 1, 'sa', 'APP01',
                     CASE WHEN r % 10 = 0 THEN NULL WHEN r % 10 = 1 THEN 'PerformanceMonitor Lite' ELSE 'app' END, 1, 0
              FROM generate_series(0, $3 - 1) AS m, generate_series(1, 20 + m % 21) AS r",
            ct, end, ServerId, Captures);
        await ExecAsync(connection, "ANALYZE query_snapshots", ct);
        return end;
    }

    private static bool IsHead(int r, int rows) => r % 11 == 4 && r + 1 <= rows;

    private static bool TextOk(int r) => r % 7 != 0 && r % 7 != 1;

    private static bool ProgramOk(int r) => r % 10 != 1;

    private static int ExpectedRows(int m)
    {
        var rows = 20 + m % 21;
        return Enumerable.Range(1, rows).Count(r => IsHead(r, rows) || (TextOk(r) && ProgramOk(r)));
    }

    private static int ExpectedBlockingRows(int m)
    {
        var rows = 20 + m % 21;
        return Enumerable.Range(1, rows).Count(r => IsHead(r, rows) || (r % 11 == 5 && TextOk(r) && ProgramOk(r)));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync(ct);
    }
}
