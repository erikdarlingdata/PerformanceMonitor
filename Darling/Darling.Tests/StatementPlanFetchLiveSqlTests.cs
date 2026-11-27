/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9195: <see cref="PgPlanFetcher.FetchStatementPlanXmlAsync"/> returns, from a real SQL Server, the plan of the one
/// statement the stored offsets name, and <see cref="PgPlanFetcher.FetchPlanXmlAsync"/> still returns the whole batch's.
/// <see cref="PgPlanFetcherStatementPlanTests"/> holds the command's shape; this class runs it. An ad hoc batch of four
/// statements is executed, its plan_handle and each statement's offsets are read from <c>sys.dm_exec_query_stats</c> the
/// way the collector reads them, and the fetch is asked for the second statement and for the last one with their stored offsets, and for (0, -1),
/// where -1 means "to the end of the batch" and is passed as it is. A plan handle that names no cached plan returns null.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It creates nothing: the
/// batches are selects over a system view, which leave plan cache entries and no object.</para>
/// </summary>
public sealed class StatementPlanFetchLiveSqlTests
{
    private const string SkipReason =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live statement plan fetch test.";

    private const int ServerId = 919_501;

    /// <summary>The four statements, each marked by a column alias that appears in its plan's statement text.</summary>
    private static readonly string[] Markers = ["pm9195_first", "pm9195_second", "pm9195_third", "pm9195_fourth"];

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
            ApplicationName = "pm-9195-statement-plan-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    /// <summary>The batch of four statements, text order, no terminator on the last.</summary>
    private static readonly string FourStatementBatch = string.Join(
        "; ", Array.ConvertAll(Markers, marker => $"SELECT COUNT_BIG(*) AS {marker} FROM sys.objects WHERE object_id > {Array.IndexOf(Markers, marker) + 1}"));

    /// <summary>
    /// Executes the batch, then reads the plan_handle and the (start, end) offsets of each of its statements, in text
    /// order, the way the collector reads them. A scan of a system view, because a constant select leaves no plan in
    /// the cache for query_stats to report.
    /// </summary>
    private static async Task<(string PlanHandle, (int Start, int End)[] Offsets)> RunBatchAsync(
        string connectionString, string batch, int statements)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        await using (var run = new SqlCommand(batch, connection))
        {
            await run.ExecuteNonQueryAsync(ct);
        }

        await using var read = new SqlCommand(@"
SELECT
    plan_handle = CONVERT(varchar(130), qs.plan_handle, 1),
    qs.statement_start_offset,
    qs.statement_end_offset
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE st.text = @batch
ORDER BY
    qs.statement_start_offset;", connection);
        read.Parameters.Add(new SqlParameter("@batch", SqlDbType.NVarChar, -1) { Value = batch });

        string? handle = null;
        var offsets = new System.Collections.Generic.List<(int, int)>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            handle ??= reader.GetString(0);
            Assert.Equal(handle, reader.GetString(0));
            offsets.Add((reader.GetInt32(1), reader.GetInt32(2)));
        }

        Assert.NotNull(handle);
        Assert.Equal(statements, offsets.Count);
        return (handle, offsets.ToArray());
    }

    private static void AssertOnlyStatement(string plan, int index)
    {
        for (var i = 0; i < Markers.Length; i++)
        {
            if (i == index)
                Assert.Contains(Markers[i], plan, StringComparison.Ordinal);
            else
                Assert.DoesNotContain(Markers[i], plan, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TheStatementFetch_ReturnsOnlyTheStatementsPlan_AndTheBatchFetchReturnsAllFour()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (handle, offsets) = await RunBatchAsync(connectionString!, FourStatementBatch, Markers.Length);
        var fetcher = new PgPlanFetcher(_ => connectionString);

        var second = await fetcher.FetchStatementPlanXmlAsync(ServerId, handle, offsets[1].Start, offsets[1].End, ct);
        var batch = await fetcher.FetchPlanXmlAsync(ServerId, handle, ct);

        Assert.NotNull(second);
        Assert.StartsWith("<ShowPlanXML", second, StringComparison.Ordinal);
        AssertOnlyStatement(second, 1);
        Assert.NotNull(batch);
        foreach (var marker in Markers)
        {
            Assert.Contains(marker, batch, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TheLastStatement_WithItsStoredOffsets_ReturnsOnlyItsOwnPlan()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var (handle, offsets) = await RunBatchAsync(connectionString!, FourStatementBatch, Markers.Length);

        var last = await new PgPlanFetcher(_ => connectionString)
            .FetchStatementPlanXmlAsync(ServerId, handle, offsets[^1].Start, offsets[^1].End, TestContext.Current.CancellationToken);

        Assert.NotNull(last);
        AssertOnlyStatement(last, Markers.Length - 1);
    }

    [Fact]
    public async Task AnEndOffsetOfMinusOne_IsPassedAsItIs_AndRunsToTheEndOfTheBatch()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var (handle, _) = await RunBatchAsync(connectionString!, FourStatementBatch, Markers.Length);

        var plan = await new PgPlanFetcher(_ => connectionString)
            .FetchStatementPlanXmlAsync(ServerId, handle, 0, -1, TestContext.Current.CancellationToken);

        Assert.NotNull(plan);
        foreach (var marker in Markers)
        {
            Assert.Contains(marker, plan, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task APlanHandleThatNamesNoCachedPlan_ReturnsNull_ThroughBothFetches()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var gone = "0x" + new string('0', 100);
        var fetcher = new PgPlanFetcher(_ => connectionString);

        Assert.Null(await fetcher.FetchStatementPlanXmlAsync(ServerId, gone, 0, -1, ct));
        Assert.Null(await fetcher.FetchPlanXmlAsync(ServerId, gone, ct));
    }
}
