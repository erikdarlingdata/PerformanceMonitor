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
/// #6896 against a live SQL Server: when the session that runs the blocking snapshot is itself the head blocker, the snapshot must not
/// report its own session as the blocker. Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server, and on <c>DARLING_TEST_SQL_READONLY=1</c>.
/// It creates and drops one scratch table in tempdb.
/// </summary>
public sealed class DmvBlockingSnapshotSelfBlockerLiveSqlTests
{
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
            ApplicationName = "pm-6896-self-blocker-live-test",
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

    private static CollectorContext NewContext() => new()
    {
        ServerId = 1,
        ServerName = "live-sql",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        State = CollectorContext.NoState,
    };

    /* The real snapshot batch and reader, run on the given connection, so @@SPID is that connection's session. */
    private static async Task<List<DmvBlockingSnapshotCollector.Row>> SnapshotAsync(SqlConnection connection, CancellationToken ct)
    {
        var context = NewContext();
        var query = DmvBlockingSnapshotCollector.Instance.BuildQuery(context);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, ct);
    }

    /* The real waiting-tasks query and reader, run on the given connection. */
    private static async Task<List<WaitingTasksCollector.Row>> WaitsAsync(SqlConnection connection, CancellationToken ct)
    {
        var context = NewContext();
        var query = WaitingTasksCollector.Instance.BuildQuery(context);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await WaitingTasksCollector.Instance.ReadAsync(reader, context, ct);
    }

    private static async Task<int> SpidAsync(SqlConnection connection, CancellationToken ct)
    {
        await using var command = new SqlCommand("SELECT @@SPID;", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task SnapshotAndWaitingTasksRunByTheBlockerItself_DoNotNameItsOwnSessionAsBlocker()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the self-blocker live test; DARLING_TEST_SQL_READONLY=1 skips it.");
        var ct = TestContext.Current.CancellationToken;
        var table = "tempdb.dbo.pm6896_" + Guid.NewGuid().ToString("N")[..8];
        var tableCreated = false;

        SqlConnection? blocker = null, waiter = null, observer = null;
        Task? waiting = null;
        var waitingObserved = false;
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

            /* The blocker holds an exclusive lock to the end of its open transaction, then the waiter blocks on it. */
            await RunAsync(blocker, "BEGIN TRANSACTION; UPDATE " + table + " SET v = 1 WHERE id = 1;", ct);
            var blockerSpid = await SpidAsync(blocker, ct);
            var waiterSpid = await SpidAsync(waiter, ct);
            var observerSpid = await SpidAsync(observer, ct);
            Assert.NotEqual(blockerSpid, observerSpid);
            Assert.NotEqual(waiterSpid, observerSpid);

            waiting = RunAsync(waiter, "SELECT v FROM " + table + " WITH (READCOMMITTEDLOCK) WHERE id = 1;", ct);

            /* The snapshot drops a lock wait under two seconds, so poll from the observer (a third session, neither blocker nor waiter)
               until the real snapshot batch shows the pair. A waiter that failed is surfaced at once instead of timing the poll out. */
            var found = false;
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (!found && DateTime.UtcNow < deadline)
            {
                if (waiting.IsFaulted)
                {
                    waitingObserved = true;
                    await waiting;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                var seen = await SnapshotAsync(observer, ct);
                found = seen.Exists(r => r.BlockingSpid == blockerSpid && r.BlockedSpid == waiterSpid);
            }

            Assert.True(found, "the snapshot run from a third session never saw the blocker blocking the waiter");

            /* The waiting-tasks read sees the same pair from the observer: the waiter's row names the blocker. */
            var observed = (await WaitsAsync(observer, ct)).Find(r => r.SessionId == waiterSpid);
            Assert.NotEqual(default, observed);
            Assert.Equal((short)blockerSpid, observed.BlockingSessionId);

            /* The real snapshot batch ON THE BLOCKER'S connection: @@SPID is the blocker's own session, so it must not be named as the blocker. */
            var ownRows = await SnapshotAsync(blocker, ct);
            Assert.DoesNotContain(ownRows, r => r.BlockingSpid == blockerSpid);

            /* The real waiting-tasks read on the blocker's connection: the waiter's wait is real, so its row stays, with no blocker. */
            var ownWait = (await WaitsAsync(blocker, ct)).Find(r => r.SessionId == waiterSpid);
            Assert.NotEqual(default, ownWait);
            Assert.Null(ownWait.BlockingSessionId);

            /* Release the lock so the waiter finishes. A waiter that faulted fails the test here. */
            await RunAsync(blocker, "IF @@TRANCOUNT > 0 ROLLBACK;", ct);
            waitingObserved = true;
            await waiting.WaitAsync(TimeSpan.FromSeconds(30), ct);
        }
        finally
        {
            if (blocker is not null)
            {
                try
                {
                    await RunAsync(blocker, "IF @@TRANCOUNT > 0 ROLLBACK;", CancellationToken.None);
                }
                catch (SqlException)
                {
                    /* The session is already gone. */
                }
            }

            if (waiting is not null && !waitingObserved)
            {
                try
                {
                    await waiting.WaitAsync(TimeSpan.FromSeconds(30));
                }
                catch (Exception ex) when (ex is SqlException or TimeoutException or OperationCanceledException)
                {
                    /* The body already failed and its exception is the one reported. The waiter is closed with its connection below. */
                }
            }

            if (waiter is not null)
            {
                await waiter.DisposeAsync();
            }

            if (observer is not null)
            {
                await observer.DisposeAsync();
            }

            if (blocker is not null)
            {
                if (tableCreated)
                {
                    await RunAsync(blocker, "DROP TABLE " + table + ";", CancellationToken.None);
                }

                await blocker.DisposeAsync();
            }
        }
    }
}
