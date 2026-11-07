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
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8079: memory_stats' <c>plan_cache_mb</c> is the plan cache once. The Plan Cache performance object carries one
/// <c>Cache Pages</c> row per cache store (SQL Plans, Object Plans, Bound Trees, Extended Stored Procedures, Temporary
/// Tables &amp; Table Variables) and a <c>_Total</c> row that is their sum, so a figure that adds every row counts the
/// cache twice. Both query texts, the Azure SQL Database one and the on-prem one, run through the collector's own
/// <see cref="MemoryStatsCollector.BuildQuery"/> and <see cref="MemoryStatsCollector.ReadAsync"/> against a live SQL
/// Server, and the figure each reads must sit inside the span the <c>_Total</c> instance covers on the same connection
/// just before and just after it, where twice the cache cannot.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It only reads.
/// Both texts bind on a box: the Azure text's <c>committed_target_kb</c> and <c>committed_kb</c> are in every edition's
/// <c>sys.dm_os_sys_info</c>.</para>
/// </summary>
public sealed class MemoryStatsPlanCacheTotalLiveSqlTests
{
    private const string PlanCacheTotalSql = @"
SELECT cntr_value
FROM sys.dm_os_performance_counters
WHERE counter_name = N'Cache Pages'
  AND object_name LIKE N'%:Plan Cache%'
  AND instance_name = N'_Total';";

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
            ApplicationName = "pm-8079-plan-cache-total-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static CollectorContext MakeContext(bool isAzureSqlDb)
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            State = new Dictionary<string, string>(StringComparer.Ordinal),
            HasCollectedBefore = false,
        };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PlanCacheMb_IsTheTotalInstance_NotEveryCacheStoreSummedWithIt(bool isAzureSqlDbText)
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live plan cache total test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        var totalBefore = await ReadTotalInstanceMbAsync(connection, ct);

        var context = MakeContext(isAzureSqlDbText);
        var query = MemoryStatsCollector.Instance.BuildQuery(context);
        MemoryStatsCollector.Row row;
        await using (var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            row = Assert.Single(await MemoryStatsCollector.Instance.ReadAsync(reader, context, ct));
        }

        var totalAfter = await ReadTotalInstanceMbAsync(connection, ct);

        /* The cache moves a little between the three reads (the reads themselves compile), so the figure is held to the
           span the two _Total reads bracket, widened by 15% or 1 MB, whichever is more. Twice the cache is outside it. */
        var low = Math.Min(totalBefore, totalAfter);
        var high = Math.Max(totalBefore, totalAfter);
        var slack = Math.Max(1m, high * 0.15m);

        Assert.True(
            row.PlanCacheMb >= low - slack && row.PlanCacheMb <= high + slack,
            $"plan_cache_mb {row.PlanCacheMb} is outside the _Total instance's span [{low}, {high}] (slack {slack}) for the {(isAzureSqlDbText ? "Azure SQL Database" : "on-prem")} text; the text is summing every Plan Cache instance, the _Total row among them");
    }

    private static async Task<decimal> ReadTotalInstanceMbAsync(SqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new SqlCommand(PlanCacheTotalSql, connection);
        var pages = Convert.ToDecimal(await command.ExecuteScalarAsync(ct));
        return pages * 8m / 1024m;
    }
}
