/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6302: get_active_queries lists the snapshots in a window (<c>snapshots_only</c>) and reads one snapshot's rows
/// (<c>collection_time</c>), which the web's Active Queries picker is built on. The row read caps ROWS, so a busy window's page can
/// hold two captures and hide the rest; the list groups in SQL under the same population (the WAITFOR trim, database, blocking_only and
/// wait_type filters), so a capture it names holds exactly the rows the table then shows. The seed has four captures: three rows, two
/// rows, one row plus an idle WAITFOR shell, and a capture holding only a WAITFOR shell (trimmed, so never listed). The captures
/// carry microseconds, which the picked time must match exactly. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ActiveQueriesSnapshotListLiveTests
{
    private const string ServerName = "darling-active-queries-snapshot-list-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "SnapDbA";
    private const string DbB = "SnapDbB";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    [Fact]
    public async Task TheList_IsNewestFirst_CountsTheTrimmedPopulation_AndTheCapCutsSnapshotsNotRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries snapshot list test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);

            var list = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, false, null, ct);
            // Captures 1 to 3, newest first; capture 4 holds only a WAITFOR shell and is not a snapshot; capture 3's shell is not a row.
            Assert.Equal(new long[] { 1, 2, 3 }, list.Snapshots.Select(s => s.RowCount).ToArray());
            Assert.Equal(list.Snapshots.OrderByDescending(s => s.CollectionTime).Select(s => s.CollectionTime), list.Snapshots.Select(s => s.CollectionTime));
            Assert.Equal(3, list.SnapshotCount);

            // The cap is on snapshots, and the count above it is the whole population's.
            var capped = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 2, DatabaseFilter.All, false, null, ct);
            Assert.Equal(2, capped.Snapshots.Count);
            Assert.Equal(3, capped.SnapshotCount);
            Assert.Equal(list.Snapshots[0].CollectionTime, capped.Snapshots[0].CollectionTime);

            // The row read's own cap does hide captures: 3 rows of the newest two captures leave the oldest unnamed, which is why the list exists.
            var rowPage = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 3, DatabaseFilter.All, false, null, ct);
            Assert.Equal(2, rowPage.Rows.Select(r => r.CollectionTime).Distinct().Count());

            // The filters ride the list: one database, one wait, blocking_only.
            var inB = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.One(DbB), false, null, ct);
            Assert.Equal(new long[] { 1, 1 }, inB.Snapshots.Select(s => s.RowCount).ToArray());
            var waiting = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, false, "lck_m_s", ct);
            Assert.Single(waiting.Snapshots);
            Assert.Equal(1, waiting.Snapshots[0].RowCount);
            var blocking = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, true, null, ct);
            Assert.Single(blocking.Snapshots);
            Assert.Equal(2, blocking.Snapshots[0].RowCount); // the victim and its head blocker
            var nothing = await DarlingSessionReader.GetActiveQuerySnapshotsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.One("NoSuchDb"), false, null, ct);
            Assert.Empty(nothing.Snapshots);
            Assert.Equal(0, nothing.SnapshotCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheTool_ListsSnapshots_AndAListedTimeReadsExactlyThatSnapshotsRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries snapshot list test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);

            var list = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, snapshots_only: true, cancellationToken: ct));
            var snapshots = list.GetProperty("snapshots").EnumerateArray().ToArray();
            Assert.Equal(3, snapshots.Length);
            Assert.Equal(3, list.GetProperty("total_snapshot_times").GetInt64());
            Assert.False(list.GetProperty("truncated").GetBoolean());
            Assert.False(list.TryGetProperty("queries", out _));

            // The cap truncates the list, and says so.
            var cut = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, limit: 2, snapshots_only: true, cancellationToken: ct));
            Assert.Equal(2, cut.GetProperty("snapshots").GetArrayLength());
            Assert.True(cut.GetProperty("truncated").GetBoolean());

            // Each listed time, sent back as it was read, returns that snapshot's rows and no other's (the seed's microseconds must match).
            foreach (var snapshot in snapshots)
            {
                var at = snapshot.GetProperty("collection_time").GetString()!;
                var rows = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, collection_time: at, cancellationToken: ct));
                var queries = rows.GetProperty("queries").EnumerateArray().ToArray();
                Assert.Equal(snapshot.GetProperty("row_count").GetInt64(), queries.Length);
                Assert.All(queries, q => Assert.Equal(at, q.GetProperty("collection_time").GetString()));
                Assert.Equal(at, rows.GetProperty("collection_time").GetString());
            }

            // The filters still apply inside one snapshot.
            var newestWithTwoRows = snapshots[1].GetProperty("collection_time").GetString()!;
            var inB = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, database_name: DbB, collection_time: newestWithTwoRows, cancellationToken: ct));
            Assert.Equal(1, inB.GetProperty("queries").GetArrayLength());

            // A time no capture has, an unparseable one, and both inputs at once.
            var miss = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, collection_time: "2001-01-01T00:00:00.0000000", cancellationToken: ct));
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            var bad = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, collection_time: "yesterday", cancellationToken: ct);
            Assert.True(McpHelpers.IsRefusalEnvelope(bad));
            Assert.Contains("\"parameter\":\"collection_time\"", bad, StringComparison.Ordinal);
            var both = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, collection_time: newestWithTwoRows, snapshots_only: true, cancellationToken: ct);
            Assert.True(McpHelpers.IsRefusalEnvelope(both));
            Assert.Contains("\"parameter\":\"snapshots_only\"", both, StringComparison.Ordinal);

            // Neither input: the page the tool always returned, every row of the window, with no snapshot echo.
            var plain = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, cancellationToken: ct));
            Assert.Equal(6, plain.GetProperty("queries").GetArrayLength());
            Assert.False(plain.TryGetProperty("collection_time", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>#6860: the snapshots form names a display-named server by its display name (#6179), not its connection key, in the
    /// answer's <c>server</c>, in both empty sentences and in the not_collected sentence, as the row-list form does.</summary>
    [Fact]
    public async Task ADisplayNamedServer_IsAnsweredByItsDisplayName_NotItsConnectionKey()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries snapshot list test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(cs!, ct);
            await using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.servers SET display_name = 'Orders Primary' WHERE server_id = $1", ServerId);
            }

            var list = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, snapshots_only: true, cancellationToken: ct));
            Assert.Equal("Orders Primary", list.GetProperty("server").GetString());
            Assert.DoesNotContain(ServerName, list.GetRawText(), StringComparison.Ordinal);

            var filtered = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, "Orders Primary", database_name: "no_such_db", snapshots_only: true, cancellationToken: ct));
            Assert.Equal("empty", filtered.GetProperty("status").GetString());
            Assert.Contains("Orders Primary", filtered.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain(ServerName, filtered.GetRawText(), StringComparison.Ordinal);

            var miss = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, collection_time: "2001-01-01T00:00:00.0000000", cancellationToken: ct));
            Assert.Contains("Orders Primary", miss.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain(ServerName, miss.GetRawText(), StringComparison.Ordinal);

            await using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}");
                await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.servers SET engine_kind = 'postgres' WHERE server_id = $1", ServerId);
            }

            var unmet = Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, snapshots_only: true, cancellationToken: ct));
            Assert.Equal("not_collected", unmet.GetProperty("status").GetString());
            Assert.Contains("Orders Primary", unmet.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain(ServerName, unmet.GetRawText(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Four captures at 5, 4, 3 and 2 minutes before now, each with 123456 microseconds. 1: sessions 51 (A, a victim of 91), 91 (B, its head
    /// blocker) and 52 (A). 2: 53 (A) and 54 (B). 3: 55 (A) and an idle WAITFOR shell (A). 4: a WAITFOR shell alone. 51 waits on LCK_M_S.
    /// </summary>
    private static async Task<DateTime> SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var micro = TimeSpan.FromTicks(1234560);
        var sessions = new (int Minutes, int Session, string Db, int Blocker, string Text)[]
        {
            (5, 51, DbA, 91, "SELECT 51"), (5, 91, DbB, 0, "SELECT 91"), (5, 52, DbA, 0, "SELECT 52"),
            (4, 53, DbA, 0, "SELECT 53"), (4, 54, DbB, 0, "SELECT 54"),
            (3, 55, DbA, 0, "SELECT 55"), (3, 56, DbA, 0, "WAITFOR DELAY '00:10'"),
            (2, 57, DbA, 0, "WAITFOR DELAY '00:10'"),
        };
        foreach (var s in sessions)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-s.Minutes)).Add(micro), ServerId, ServerName, s.Session, s.Db, "00 00:00:05.000",
                s.Text, "running", s.Blocker, s.Blocker > 0 ? "LCK_M_S" : null, s.Blocker > 0 ? 500L : 0L,
                1000L, 1000L, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", "SSMS", 1, 0);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
