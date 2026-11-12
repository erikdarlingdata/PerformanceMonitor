/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7083 against a live SQL Server: the DMV blocking snapshot must show the STATEMENT a blocker is running, not the
/// definition of the procedure that statement sits in. Gated on <c>DARLING_TEST_SQL</c> (optional
/// <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL
/// Server, and on <c>DARLING_TEST_SQL_READONLY=1</c>. It creates and drops one scratch table and one procedure in tempdb.
/// </summary>
public sealed class DmvBlockingSnapshotStatementTextLiveSqlTests
{
    private const string Skip = "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the statement-text live test; DARLING_TEST_SQL_READONLY=1 skips it.";

    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-7083-statement-text-live-test",
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task RunAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> SpidAsync(SqlConnection connection, CancellationToken ct)
    {
        await using var command = new SqlCommand("SELECT @@SPID;", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /* The real snapshot batch and reader, run from the observer's connection. */
    private static async Task<List<DmvBlockingSnapshotCollector.Row>> SnapshotAsync(SqlConnection connection, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = CollectorContext.NoState,
        };
        var query = DmvBlockingSnapshotCollector.Instance.BuildQuery(context);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, ct);
    }

    /* Polls the real snapshot from the observer until it shows the blocker/waiter pair. The snapshot drops a lock wait under
       two seconds, so the pair appears a little after the waiter starts. */
    private static async Task<DmvBlockingSnapshotCollector.Row> WaitForPairAsync(
        SqlConnection observer, int blockerSpid, int waiterSpid, Task waiting, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(25);
        while (DateTime.UtcNow < deadline)
        {
            if (waiting.IsFaulted)
            {
                await waiting;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
            var seen = await SnapshotAsync(observer, ct);
            var row = seen.Find(r => r.BlockingSpid == blockerSpid && r.BlockedSpid == waiterSpid);
            if (row.BlockingSpid == blockerSpid)
            {
                return row;
            }
        }

        throw new Xunit.Sdk.XunitException("the snapshot never saw the blocker blocking the waiter");
    }

    [Fact]
    public async Task BlockerRunningInsideAProcedure_ShowsTheRunningStatement_NotTheProcedureDefinition()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, Skip);
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var table = "tempdb.dbo.pm7083_t_" + suffix;
        var proc = "tempdb.dbo.pm7083_p_" + suffix;
        var tableCreated = false;
        var procCreated = false;

        SqlConnection? blocker = null, waiter = null, observer = null;
        Task? running = null, waiting = null;
        try
        {
            blocker = new SqlConnection(connectionString);
            waiter = new SqlConnection(connectionString);
            observer = new SqlConnection(connectionString);
            await blocker.OpenAsync(ct);
            await waiter.OpenAsync(ct);
            await observer.OpenAsync(ct);

            await RunAsync(blocker, "CREATE TABLE " + table + " (id integer NOT NULL PRIMARY KEY, v integer NOT NULL); INSERT " + table + " (id, v) VALUES (1, 0);", ct);
            tableCreated = true;

            /* The procedure takes the lock, then spends its time on a WAITFOR: that WAITFOR is the statement the blocker is running. */
            await RunAsync(
                blocker,
                "CREATE PROCEDURE " + proc + " AS BEGIN SET NOCOUNT ON; BEGIN TRANSACTION; UPDATE " + table
                + " SET v = 1 WHERE id = 1; WAITFOR DELAY '00:00:12'; ROLLBACK TRANSACTION; END;",
                ct);
            procCreated = true;

            var blockerSpid = await SpidAsync(blocker, ct);
            var waiterSpid = await SpidAsync(waiter, ct);
            running = RunAsync(blocker, "EXEC " + proc + ";", ct);
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            waiting = RunAsync(waiter, "SELECT v FROM " + table + " WITH (READCOMMITTEDLOCK) WHERE id = 1;", ct);

            var row = await WaitForPairAsync(observer, blockerSpid, waiterSpid, waiting, ct);

            Assert.Contains("WAITFOR DELAY", row.BlockingSqlText, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("CREATE PROCEDURE", row.BlockingSqlText, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("UPDATE", row.BlockingSqlText, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("SELECT v FROM", row.BlockedSqlText, StringComparison.OrdinalIgnoreCase);

            await running.WaitAsync(TimeSpan.FromSeconds(40), ct);
            await waiting.WaitAsync(TimeSpan.FromSeconds(40), ct);
        }
        finally
        {
            await LiveCleanupAsync(connectionString!, blocker, running, waiting, table, tableCreated, proc, procCreated);
            if (waiter is not null)
            {
                await waiter.DisposeAsync();
            }

            if (observer is not null)
            {
                await observer.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task IdleBlockerWithNoRunningStatement_SaysTheTextIsTheLastBatch()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, Skip);
        var ct = TestContext.Current.CancellationToken;
        var table = "tempdb.dbo.pm7083_i_" + Guid.NewGuid().ToString("N")[..8];
        var tableCreated = false;

        SqlConnection? blocker = null, waiter = null, observer = null;
        Task? waiting = null;
        try
        {
            blocker = new SqlConnection(connectionString);
            waiter = new SqlConnection(connectionString);
            observer = new SqlConnection(connectionString);
            await blocker.OpenAsync(ct);
            await waiter.OpenAsync(ct);
            await observer.OpenAsync(ct);

            await RunAsync(blocker, "CREATE TABLE " + table + " (id integer NOT NULL PRIMARY KEY, v integer NOT NULL); INSERT " + table + " (id, v) VALUES (1, 0);", ct);
            tableCreated = true;

            /* The blocker ends its batch with the transaction still open: a sleeping session, no request, no offsets. */
            await RunAsync(blocker, "BEGIN TRANSACTION; UPDATE " + table + " SET v = 1 WHERE id = 1;", ct);
            var blockerSpid = await SpidAsync(blocker, ct);
            var waiterSpid = await SpidAsync(waiter, ct);
            waiting = RunAsync(waiter, "SELECT v FROM " + table + " WITH (READCOMMITTEDLOCK) WHERE id = 1;", ct);

            var row = await WaitForPairAsync(observer, blockerSpid, waiterSpid, waiting, ct);

            Assert.StartsWith("[no running statement", row.BlockingSqlText, StringComparison.Ordinal);
            Assert.Contains("UPDATE", row.BlockingSqlText, StringComparison.OrdinalIgnoreCase);

            await RunAsync(blocker, "IF @@TRANCOUNT > 0 ROLLBACK;", ct);
            await waiting.WaitAsync(TimeSpan.FromSeconds(30), ct);
        }
        finally
        {
            await LiveCleanupAsync(connectionString!, blocker, null, waiting, table, tableCreated, null, false);
            if (waiter is not null)
            {
                await waiter.DisposeAsync();
            }

            if (observer is not null)
            {
                await observer.DisposeAsync();
            }
        }
    }

    private static async Task LiveCleanupAsync(
        string connectionString, SqlConnection? blocker, Task? running, Task? waiting,
        string table, bool tableCreated, string? proc, bool procCreated)
    {
        /* Closing the blocker's connection kills a procedure still in its WAITFOR and rolls its transaction back, which frees the waiter. */
        if (blocker is not null)
        {
            await blocker.DisposeAsync();
        }

        foreach (var task in new[] { running, waiting })
        {
            if (task is null)
            {
                continue;
            }

            try
            {
                await task.WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (Exception ex) when (ex is SqlException or TimeoutException or OperationCanceledException or InvalidOperationException)
            {
                /* The body already failed and its exception is the one reported. */
            }
        }

        if (!tableCreated && !procCreated)
        {
            return;
        }

        await using var cleanup = new SqlConnection(connectionString);
        await cleanup.OpenAsync(CancellationToken.None);
        if (procCreated && proc is not null)
        {
            await RunAsync(cleanup, "DROP PROCEDURE IF EXISTS " + proc + ";", CancellationToken.None);
        }

        if (tableCreated)
        {
            await RunAsync(cleanup, "DROP TABLE IF EXISTS " + table + ";", CancellationToken.None);
        }
    }
}
