/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9013: the wait-type to category CASE is one helper shared by FinOps and the metrics port, so the two
/// cannot drift. Runs the generated CASE over a VALUES list on the live store, so every branch is proven reachable.</summary>
[Collection("live-postgres")]
public sealed class WaitCategorySqlLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EveryCategory_IsReachable_AndUnknownIsOther()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-category test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var sql = $"SELECT {WaitCategorySql.CaseExpression("w")} FROM (VALUES ('SOS_SCHEDULER_YIELD', 1),('CXPACKET', 2),('PAGEIOLATCH_SH', 3),('WRITELOG', 4),('RESOURCE_SEMAPHORE', 5),('ASYNC_NETWORK_IO', 6),('LCK_M_X', 7),('PAGELATCH_EX', 8),('SOMETHING_NEW', 9)) AS t(w, n) ORDER BY n";
        var categories = new List<string>();
        await using (var command = postgres.CreateCommand(sql))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                categories.Add(reader.GetString(0));
            }
        }

        Assert.Equal(new[] { "CPU", "Parallelism", "Storage", "Storage", "Memory", "Network", "Locks", "Latches", "Other" }, categories);
        /* #9013: the list the metrics port's label values are derived from is exactly what the CASE can return. */
        Assert.Equal(new HashSet<string>(WaitCategorySql.Categories), new HashSet<string>(categories));
    }
}
