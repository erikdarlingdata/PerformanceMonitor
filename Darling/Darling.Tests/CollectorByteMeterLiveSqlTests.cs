/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6629: the collector byte meter against a REAL SqlClient connection, made through the product's own path
/// (<see cref="CollectorByteMeter.Begin"/> plus <see cref="SqlServerTargetProvider.CreateConnection"/>). The unit tests feed the meter
/// stand-in sources, so nothing else shows that <c>RetrieveStatistics()</c> answers with the right figures for a connection that was
/// closed (and disposed) before the run's log row is written, which is the order <c>DarlingWorker</c> uses.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the host; optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth), as the other live SQL Server classes are. It only runs SELECTs of a generated string, so it is safe against any server.</para>
/// </summary>
public sealed class CollectorByteMeterLiveSqlTests
{
    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
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
            ApplicationName = "pm-6629-byte-meter-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    /* Opens a connection the product's way, reads one generated string of the given length, closes and disposes it. */
    private static async Task ReadStringAsync(string connectionString, int length)
    {
        var ct = TestContext.Current.CancellationToken;
        await using DbConnection connection = SqlServerTargetProvider.Instance.CreateConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT REPLICATE(CONVERT(varchar(max), 'x'), {length});";
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(length, reader.GetString(0).Length);
    }

    [Fact]
    public async Task OneConnectionClosedBeforeTheRead_CountsTheStringItReceived()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live byte meter test.");

        using var scope = CollectorByteMeter.Begin();
        await ReadStringAsync(connectionString!, 1_000_000);

        var bytes = scope.Meter.Snapshot();
        Assert.NotNull(bytes);
        Assert.InRange(bytes!.Value.Received, 1_000_000, 1_099_999);
        Assert.True(bytes.Value.Sent > 0, "bytes sent should be above 0");
    }

    [Fact]
    public async Task TwoConnectionsInOneRun_TotalIsTheirSum()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live byte meter test.");

        using var scope = CollectorByteMeter.Begin();
        await ReadStringAsync(connectionString!, 500_000);
        await ReadStringAsync(connectionString!, 500_000);

        var bytes = scope.Meter.Snapshot();
        Assert.NotNull(bytes);
        Assert.InRange(bytes!.Value.Received, 1_000_000, 1_199_999);
        Assert.True(bytes.Value.Sent > 0, "bytes sent should be above 0");
    }
}
