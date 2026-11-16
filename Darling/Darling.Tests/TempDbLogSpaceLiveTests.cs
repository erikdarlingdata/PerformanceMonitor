/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8881, Darling's half, against live PostgreSQL: the tempdb log columns written by V269 (or the generated table) come back through the
/// alert read as the log's percent, reasons and holder, and a row stored without them reads back null, never zero.
/// </summary>
[Collection("live-postgres")]
public sealed class TempDbLogSpaceLiveTests
{
    /* #1776 own-store: negative sentinel server ids no registered server uses; the rows are deleted in the cleanup. */
    private const int Logged = -9881, Unlogged = -9882;

    [Fact]
    public async Task TheAlertRead_ReturnsTheLogAndItsHolder_AndNullForARowWithoutIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb log test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var adapter = new DarlingAlertReadAdapter(dataSource);

        var bodySucceeded = false;
        try
        {
            await DeleteAsync(connection, ct);
            var now = DateTime.UtcNow;
            await InsertAsync(connection, Logged, now, withLog: true, ct);
            await InsertAsync(connection, Unlogged, now, withLog: false, ct);

            var logged = await adapter.GetTempDbSpaceAsync(Logged.ToString(CultureInfo.InvariantCulture), ct);
            Assert.NotNull(logged);
            Assert.Equal(91.25, logged!.LogUsedPercent!.Value, precision: 2);
            Assert.Equal(2048d, logged.LogTotalMb!.Value, precision: 2);
            Assert.Equal("ACTIVE_TRANSACTION", logged.LogReuseWaitDesc);
            Assert.Equal("ACTIVE_TRANSACTION", logged.LogTruncationHoldupReason);
            Assert.Equal(57, logged.HolderSessionId);
            Assert.Equal("etl_login", logged.HolderLoginName);
            Assert.Equal("nightly load", logged.HolderProgramName);
            Assert.Equal(734003200L, logged.HolderLogBytesUsed);
            Assert.NotNull(logged.HolderBeginTimeUtc);

            var unlogged = await adapter.GetTempDbSpaceAsync(Unlogged.ToString(CultureInfo.InvariantCulture), ct);
            Assert.NotNull(unlogged);
            Assert.Equal(100d, unlogged!.TotalReservedMb, precision: 2);
            Assert.Null(unlogged.LogUsedPercent);
            Assert.Null(unlogged.LogTruncationHoldupReason);
            Assert.Null(unlogged.HolderSessionId);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteAsync(c, token));
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, bool withLog, CancellationToken ct)
    {
        var sql = withLog
            ? @"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, total_reserved_mb, unallocated_mb,
    log_total_mb, log_used_mb, log_used_percent, log_reuse_wait_desc, log_truncation_holdup_reason, holder_session_id, holder_login_name,
    holder_program_name, holder_begin_time, holder_log_bytes_used, holder_log_bytes_reserved)
VALUES ($1, $2, $3, 'sentinel', 100, 900, 2048, 1868.8, 91.25, 'ACTIVE_TRANSACTION', 'ACTIVE_TRANSACTION', 57, 'etl_login', 'nightly load', $2, 734003200, 1048576000)"
            : @"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, total_reserved_mb, unallocated_mb)
VALUES ($1, $2, $3, 'sentinel', 100, 900)";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(collectionTimeUtc.Ticks);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM tempdb_stats WHERE server_id IN ({Logged}, {Unlogged});", connection) { CommandTimeout = 60 };
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
