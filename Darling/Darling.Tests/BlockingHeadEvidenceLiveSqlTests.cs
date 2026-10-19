/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 against a live SQL Server: two idle head blockers, the blocking capture's REAL batch (<see cref="DmvBlockingSnapshotCollector.BuildQuery"/>)
/// and its real <c>ReadAsync</c>. Head A holds a shared lock under <c>HOLDLOCK</c> and has written no log; head B has updated a row, so its
/// transaction has used log. Each head then sits idle with its transaction open, which is the shape a person judges before a kill: a sleeping
/// head with no log bytes used has done no work to roll back.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips
/// where the runner has no SQL Server. It creates and drops its own scratch database; <c>DARLING_TEST_SQL_READONLY=1</c> skips it, because it
/// creates objects and a server that holds real monitoring data must not get them.</para>
/// </summary>
public sealed class BlockingHeadEvidenceLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live head-blocker evidence test; DARLING_TEST_SQL_READONLY=1 skips it.";

    private static string? ConnectionString(string database = "master")
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
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-5632-head-evidence-live-test",
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

    private static async Task<object?> ScalarAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task<List<DmvBlockingSnapshotCollector.Row>> CollectAsync(string connectionString, CancellationToken ct)
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

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        /* The shipped command builder, so the text and the parameter bindings are the host's own, not a copy of them. */
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, ct);
    }

    [Fact]
    public async Task TwoIdleHeadBlockers_ReadAsSleepingWithTheirLoginTimeOneOpenTransactionAndExactLogBytes()
    {
        var master = ConnectionString();
        Assert.SkipWhen(master is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var database = "pm5632_" + Guid.NewGuid().ToString("N")[..8];
        var databaseCreated = false;

        SqlConnection? headA = null, headB = null, waiterA = null, waiterB = null;
        Task? waitingA = null, waitingB = null;
        try
        {
            await using (var admin = new SqlConnection(master!))
            {
                await admin.OpenAsync(ct);
                await RunAsync(admin, "CREATE DATABASE [" + database + "];", ct);
                databaseCreated = true;
            }

            var scratch = ConnectionString(database)!;
            await using (var setup = new SqlConnection(scratch))
            {
                await setup.OpenAsync(ct);
                await RunAsync(setup, @"
CREATE TABLE dbo.head_a (id integer NOT NULL PRIMARY KEY, v integer NOT NULL);
CREATE TABLE dbo.head_b (id integer NOT NULL PRIMARY KEY, v integer NOT NULL);
INSERT dbo.head_a (id, v) VALUES (1, 0);
INSERT dbo.head_b (id, v) VALUES (1, 0);", ct);

                /* The first transaction to touch a new database records 120 bytes of log even for a read (measured on SQL Server 2025), so a
                   throwaway transaction here keeps head A's transaction a clean "wrote nothing" case. */
                await RunAsync(setup, "BEGIN TRANSACTION; SELECT v FROM dbo.head_a WITH (HOLDLOCK, ROWLOCK) WHERE id = 1; ROLLBACK;", ct);
            }

            headA = new SqlConnection(scratch);
            headB = new SqlConnection(scratch);
            waiterA = new SqlConnection(scratch);
            waiterB = new SqlConnection(scratch);
            foreach (var connection in new[] { headA, headB, waiterA, waiterB })
            {
                await connection.OpenAsync(ct);
            }

            /* Head A: a shared lock held to the end of the transaction, and no log written. Head B: an update, so log is used. Both then sit idle. */
            await RunAsync(headA, "BEGIN TRANSACTION; SELECT v FROM dbo.head_a WITH (HOLDLOCK, ROWLOCK) WHERE id = 1;", ct);
            await RunAsync(headB, "BEGIN TRANSACTION; UPDATE dbo.head_b SET v = 1 WHERE id = 1;", ct);
            var spidA = Convert.ToInt32(await ScalarAsync(headA, "SELECT @@SPID;", ct), System.Globalization.CultureInfo.InvariantCulture);
            var spidB = Convert.ToInt32(await ScalarAsync(headB, "SELECT @@SPID;", ct), System.Globalization.CultureInfo.InvariantCulture);

            waitingA = RunAsync(waiterA, "UPDATE dbo.head_a SET v = 2 WHERE id = 1;", ct);
            waitingB = RunAsync(waiterB, "UPDATE dbo.head_b SET v = 2 WHERE id = 1;", ct);

            /* The capture drops a lock wait under two seconds, so poll until both blockers have been waited on that long. */
            DmvBlockingSnapshotCollector.Row rowA = default, rowB = default;
            var found = false;
            var deadline = DateTime.UtcNow.AddSeconds(60);
            while (!found && DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                var rows = await CollectAsync(master!, ct);
                var a = rows.Where(r => r.BlockingSpid == spidA).ToList();
                var b = rows.Where(r => r.BlockingSpid == spidB).ToList();
                if (a.Count > 0 && b.Count > 0)
                {
                    rowA = a[0];
                    rowB = b[0];
                    found = true;
                }
            }

            Assert.True(found, "the capture never saw both idle heads blocking a waiter");

            /* What the server itself says the heads' sign-in times are. */
            var loginA = (DateTime)(await ScalarAsync(headA, "SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID;", ct))!;
            var loginB = (DateTime)(await ScalarAsync(headB, "SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID;", ct))!;

            foreach (var (row, login) in new[] { (rowA, loginA), (rowB, loginB) })
            {
                /* An idle head has no request, so blocking_status falls back to the session's own status: no new status column is needed. */
                Assert.Equal("sleeping", row.BlockingStatus);
                Assert.Equal(1, row.BlockingOpenTransactionCount);
                Assert.Equal(login, row.BlockingLoginTime);
                Assert.NotNull(row.BlockingLastRequestEndTime);
                Assert.True(row.BlockingLastRequestEndTime >= row.BlockingLoginTime);
            }

            /* A wrote no log (0, or no row for it); B's update used some, in exact bytes (an MB figure would round this to 0). */
            Assert.True(rowA.BlockingTranLogBytesUsed is null or 0, $"head A (no write) reported {rowA.BlockingTranLogBytesUsed} log bytes");
            Assert.True(rowB.BlockingTranLogBytesUsed > 0, $"head B (an update) reported {rowB.BlockingTranLogBytesUsed} log bytes");
            Assert.True(rowB.BlockingTranLogBytesUsed < 1048576, "the value is bytes, not megabytes");
        }
        finally
        {
            foreach (var head in new[] { headA, headB })
            {
                if (head is not null)
                {
                    try
                    {
                        await RunAsync(head, "IF @@TRANCOUNT > 0 ROLLBACK;", CancellationToken.None);
                    }
                    catch (SqlException)
                    {
                        /* The session is already gone. */
                    }
                }
            }

            foreach (var waiting in new[] { waitingA, waitingB })
            {
                if (waiting is not null)
                {
                    try
                    {
                        await waiting.WaitAsync(TimeSpan.FromSeconds(30));
                    }
                    catch (Exception ex) when (ex is SqlException or TimeoutException or OperationCanceledException)
                    {
                        /* A waiter that never got its lock is closed with its connection below. */
                    }
                }
            }

            foreach (var connection in new[] { headA, headB, waiterA, waiterB })
            {
                if (connection is not null)
                {
                    await connection.DisposeAsync();
                }
            }

            if (databaseCreated)
            {
                await using var admin = new SqlConnection(master!);
                await admin.OpenAsync(CancellationToken.None);
                await RunAsync(admin, "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "];", CancellationToken.None);
            }
        }
    }
}
