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
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9055 against a live SQL Server: the waiting-tasks query keeps <c>resource_description</c> for page latch and page I/O latch
/// waits only. Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth), so it skips where the runner has no SQL Server, and on <c>DARLING_TEST_SQL_READONLY=1</c>. It only reads: the first test
/// runs the collector's own expression over a literal list of wait types, the second runs the whole collector query.
/// </summary>
public sealed class WaitingTasksPageLatchResourceLiveSqlTests
{
    private const string SkipReason = "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the page latch resource_description live test; DARLING_TEST_SQL_READONLY=1 skips it.";

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
            ApplicationName = "pm-9055-page-latch-live-test",
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

    [Fact]
    public async Task Expression_KeepsThePageForPageLatchWaits_AndNothingElse()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;

        /* The alias and column names are the ones the collector query uses, so the expression runs exactly as it does there. */
        var sql = "SELECT wt.wait_type, " + WaitingTasksCollector.ResourceDescriptionExpression + " "
            + "FROM (VALUES (N'PAGELATCH_EX', N'5:1:4242'), (N'PAGELATCH_SH', N'2:3:1'), (N'PAGEIOLATCH_SH', N'7:1:99'), "
            + "(N'LCK_M_X', N'keylock hobtid=72057594038779904 dbid=5'), (N'PAGELATCHX', N'5:1:4242'), (N'PAGEIOLATCHX', N'5:1:4242'), "
            + "(N'LATCH_EX', N'ACCESS_METHODS_DATASET_PARENT'), (N'SOS_SCHEDULER_YIELD', NULL)) AS wt(wait_type, resource_description);";

        var found = new Dictionary<string, string?>(StringComparer.Ordinal);
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                found[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }

        Assert.Equal("5:1:4242", found["PAGELATCH_EX"]);
        Assert.Equal("2:3:1", found["PAGELATCH_SH"]);
        Assert.Equal("7:1:99", found["PAGEIOLATCH_SH"]);
        Assert.Null(found["LCK_M_X"]);
        Assert.Null(found["PAGELATCHX"]);
        Assert.Null(found["PAGEIOLATCHX"]);
        Assert.Null(found["LATCH_EX"]);
        Assert.Null(found["SOS_SCHEDULER_YIELD"]);
    }

    [Fact]
    public async Task CollectorQuery_RunsAndOnlyPageLatchRowsCarryAResourceDescription()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;

        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = CollectorContext.NoState,
        };

        List<WaitingTasksCollector.Row> rows;
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            var query = WaitingTasksCollector.Instance.BuildQuery(context);
            await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
            await using var reader = await command.ExecuteReaderAsync(ct);
            rows = await WaitingTasksCollector.Instance.ReadAsync(reader, context, ct);
        }

        /* The server may have no waiting task at that moment, so an empty page is fine: the point is that the batch runs and the
           reader takes the new column, and that no other wait type ever carries a description. */
        foreach (var row in rows.Where(r => r.ResourceDescription is not null))
        {
            Assert.True(
                row.WaitType!.StartsWith("PAGELATCH_", StringComparison.OrdinalIgnoreCase)
                || row.WaitType.StartsWith("PAGEIOLATCH_", StringComparison.OrdinalIgnoreCase),
                row.WaitType);
        }
    }
}
