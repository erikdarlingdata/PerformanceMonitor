/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    [Fact]
    public async Task SnapshotRunByTheBlockerItself_DoesNotNameItsOwnSessionAsBlocker()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the self-blocker live test; DARLING_TEST_SQL_READONLY=1 skips it.");
        var ct = TestContext.Current.CancellationToken;
        var table = "tempdb.dbo.pm6896_" + Guid.NewGuid().ToString("N")[..8];
        var tableCreated = false;

        SqlConnection? blocker = null, waiter = null;
        Task? waiting = null;
        try
        {
            blocker = new SqlConnection(connectionString);
            waiter = new SqlConnection(connectionString);
            await blocker.OpenAsync(ct);
            await waiter.OpenAsync(ct);

            await RunAsync(blocker, "CREATE TABLE " + table + " (id integer NOT NULL PRIMARY KEY, v integer NOT NULL); INSERT " + table + " (id, v) VALUES (1, 0);", ct);
            tableCreated = true;

            /* The blocker holds an exclusive lock to the end of its open transaction, then the waiter blocks on it. */
            await RunAsync(blocker, "BEGIN TRANSACTION; UPDATE " + table + " SET v = 1 WHERE id = 1;", ct);
            int blockerSpid;
            await using (var spidCommand = new SqlCommand("SELECT @@SPID;", blocker))
            {
                blockerSpid = Convert.ToInt32(await spidCommand.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
            }

            waiting = RunAsync(waiter, "SELECT v FROM " + table + " WITH (READCOMMITTEDLOCK) WHERE id = 1;", ct);

            /* The snapshot drops a lock wait under two seconds, so let the waiter wait past that. */
            await Task.Delay(TimeSpan.FromSeconds(3), ct);

            var context = new CollectorContext
            {
                ServerId = 1,
                ServerName = "live-sql",
                CollectionTime = DateTime.UtcNow,
                Deltas = null!,
                State = CollectorContext.NoState,
            };
            var query = DmvBlockingSnapshotCollector.Instance.BuildQuery(context);

            /* Run the real snapshot batch ON THE BLOCKER'S connection, so @@SPID is the blocker's own session. */
            await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, blocker, 60);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, ct);

            Assert.DoesNotContain(rows, r => r.BlockingSpid == blockerSpid);
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

            if (waiter is not null)
            {
                await waiter.DisposeAsync();
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
