/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch databases through ScratchPostgres and never touch another
   test's rows, so they are deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5632, V199: the four head-blocker evidence columns on <c>collect.dmv_blocking_snapshots</c>. A row written through the product's own
/// binary COPY writer (positional, so column ORDER is the contract) reads back through <c>v_dmv_blocking_snapshots</c> with all four values
/// on a store built fresh and on a store rewound to V198 and migrated up, and both stores end with the four columns LAST and in the same order.
/// </summary>
public sealed class BlockingHeadEvidenceRungTests
{
    private const int TestServerId = -563211;

    private static readonly string[] EvidenceColumns =
        ["blocking_login_time", "blocking_last_request_end_time", "blocking_open_transaction_count", "blocking_tran_log_bytes_used"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheRung_IsV199_AndAddsOnlyNullableDefaultlessColumns_ThenRefreshesTheView()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 199);
        Assert.Equal("blocking-head-evidence", rung.Name);
        Assert.True(StorageVersion.SchemaVersion >= 199); /* not the top any more: V200 (#6098) sits above this rung */
        foreach (var column in EvidenceColumns)
        {
            Assert.Contains("ADD COLUMN IF NOT EXISTS " + column + " ", rung.Sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("DEFAULT", rung.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", rung.Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_dmv_blocking_snapshots AS SELECT * FROM dmv_blocking_snapshots", rung.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFreshStore_AndOneUpgradedFromV198_StoreAndReadBackTheEvidence_WithTheFourColumnsLast()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the head-blocker evidence rung pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: the generated table already carries the columns (from the collector's payload) and the rung's ALTERs no-op. */
            await PgMigrations.MigrateAsync(connection, ct);
            var freshColumns = await ColumnOrderAsync(connection, ct);
            Assert.Equal(EvidenceColumns, freshColumns.TakeLast(4).ToArray());
            await WriteAndAssertAsync(connection, 1, ct);

            /* Upgraded: a store at V198 has none of the four. The rung adds them, after every column the table already had. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 198, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 199", ct);
            await ExecAsync(connection, "DELETE FROM collect.dmv_blocking_snapshots", ct);
            Assert.Empty((await ColumnOrderAsync(connection, ct)).Intersect(EvidenceColumns));

            await PgMigrations.MigrateAsync(connection, ct);
            var upgradedColumns = await ColumnOrderAsync(connection, ct);
            Assert.Equal(freshColumns, upgradedColumns);
            await WriteAndAssertAsync(connection, 2, ct);

            /* A row collected before the rung reads NULL in all four, not 0 and not a default. */
            await ExecAsync(connection,
                "INSERT INTO collect.dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time) "
                + "VALUES (" + CollectionIdGenerator.Next() + ", timestamp '2026-10-19 10:00:00', " + TestServerId + ", 'evidence-test', -9, timestamp '2026-10-19 10:00:00')", ct);
            Assert.Equal("<null>|<null>|<null>|<null>", (string)(await ScalarAsync(connection,
                "SELECT COALESCE(blocking_login_time::text, '<null>') || '|' || COALESCE(blocking_last_request_end_time::text, '<null>') || '|' "
                + "|| COALESCE(blocking_open_transaction_count::text, '<null>') || '|' || COALESCE(blocking_tran_log_bytes_used::text, '<null>') "
                + "FROM v_dmv_blocking_snapshots WHERE server_id = " + TestServerId + " AND monitor_loop = -9", ct))!);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task WriteAndAssertAsync(NpgsqlConnection connection, int loop, CancellationToken ct)
    {
        var definition = DmvBlockingSnapshotCollector.Instance;
        var runTime = new DateTime(2026, 10, 19, 12, 0, 0, DateTimeKind.Unspecified);
        var login = new DateTime(2026, 10, 19, 7, 55, 1, DateTimeKind.Unspecified);
        var lastEnd = new DateTime(2026, 10, 19, 8, 30, 15, DateTimeKind.Unspecified);
        var row = new DmvBlockingSnapshotCollector.Row(
            "db", 71, 0, null, 55, 0, null, 4500L, "X", "sleeping", "[dbo].[Orders]", "UPDATE ...", "BEGIN TRAN ...",
            "app_user", "web01", "MyApp", "admin", "jump01", "SSMS", login, lastEnd, 1, 9876543210L);
        var context = new CollectorContext { ServerId = TestServerId, ServerName = "evidence-test", CollectionTime = runTime.AddSeconds(-loop), Deltas = null! };

        var writer = new PgCollectorRowWriter();
        await using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(runTime).Value(TestServerId).Value("evidence-test");
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
            await importer.CompleteAsync(ct);
        }

        var expected = "2026-10-19 07:55:01|2026-10-19 08:30:15|1|9876543210";
        Assert.Equal(expected, (string)(await ScalarAsync(connection,
            "SELECT to_char(blocking_login_time, 'YYYY-MM-DD HH24:MI:SS') || '|' || to_char(blocking_last_request_end_time, 'YYYY-MM-DD HH24:MI:SS') || '|' "
            + "|| blocking_open_transaction_count || '|' || blocking_tran_log_bytes_used "
            + "FROM v_dmv_blocking_snapshots WHERE server_id = " + TestServerId + " AND monitor_loop = " + DmvBlockingSnapshotCollector.SyntheticMonitorLoop(context.CollectionTime), ct))!);
        /* The columns before the four still hold what they held: the positional write did not slide. */
        Assert.Equal("sleeping|SSMS", (string)(await ScalarAsync(connection,
            "SELECT blocking_status || '|' || blocking_client_app FROM v_dmv_blocking_snapshots WHERE server_id = " + TestServerId
            + " AND monitor_loop = " + DmvBlockingSnapshotCollector.SyntheticMonitorLoop(context.CollectionTime), ct))!);
    }

    private static async Task<string[]> ColumnOrderAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = 'collect.dmv_blocking_snapshots'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum", connection)
        { CommandTimeout = 60 };
        var names = new System.Collections.Generic.List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
