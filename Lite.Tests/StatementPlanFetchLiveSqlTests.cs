/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9195, Lite's twin of <c>Darling.Tests.StatementPlanFetchLiveSqlTests</c>:
/// <see cref="SqlPlanFetcher.FetchStatementPlanXmlAsync"/> returns, from a real SQL Server, the plan of the one statement
/// the stored offsets name, and <see cref="SqlPlanFetcher.FetchPlanXmlAsync"/> still returns the whole batch's.
/// <see cref="SqlPlanFetcherStatementPlanTests"/> holds the command's parameters; this class runs the DMV call. An ad hoc
/// batch of four statements is executed, its plan_handle and each statement's offsets are read from
/// <c>sys.dm_exec_query_stats</c> the way the collector reads them, and the fetch is asked for the second statement and for
/// the last one with their stored offsets, and for (0, -1), where -1 means "to the end of the batch" and is passed as it is.
/// A plan handle that names no cached plan returns null.
///
/// <para>The fetcher is the real one: it finds the server through a <see cref="ServerManager"/> that holds the test
/// server (an in-memory credential store through <c>[InMemoryCredentials]</c>, a temp config folder) and resolves the
/// connection string the app resolves.</para>
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It creates nothing: the
/// batches are selects over a system view, which leave plan cache entries and no object.</para>
/// </summary>
[InMemoryCredentials]
public sealed class StatementPlanFetchLiveSqlTests : IDisposable
{
    private const string SkipReason =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live statement plan fetch test.";

    /// <summary>The four statements, each marked by a column alias that appears in its plan's statement text.</summary>
    private static readonly string[] Markers = ["pm9195_first", "pm9195_second", "pm9195_third", "pm9195_fourth"];

    /// <summary>The batch of four statements, text order, no terminator on the last.</summary>
    private static readonly string FourStatementBatch = string.Join(
        "; ", Array.ConvertAll(Markers, marker => $"SELECT COUNT_BIG(*) AS {marker} FROM sys.objects WHERE object_id > {Array.IndexOf(Markers, marker) + 1}"));

    private readonly string _root = Path.Combine(Path.GetTempPath(), "StatementPlanFetchLiveSql_" + Guid.NewGuid().ToString("N")[..8]);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private sealed record LiveServer(string ConnectionString, SqlPlanFetcher Fetcher, int ServerId);

    /// <summary>
    /// Null when no live server is configured. Otherwise the connection string the test reads query_stats with, and a
    /// <see cref="SqlPlanFetcher"/> over a manager that holds the same server, with its id as the collectors derive it.
    /// </summary>
    private LiveServer? Live()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        var windows = string.IsNullOrEmpty(user);

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-9195-statement-plan-live-test",
            IntegratedSecurity = windows,
        };
        if (!windows)
        {
            builder.UserID = user;
            builder.Password = password;
        }

        Directory.CreateDirectory(_root);
        var manager = new ServerManager(_root, null, Path.Combine(_root, ServerHostBindings.FileName));
        var server = new ServerConnection
        {
            ServerName = host,
            DisplayName = "pm-9195-live",
            AuthenticationType = windows ? AuthenticationTypes.Windows : AuthenticationTypes.SqlServer,
            EncryptMode = "Optional",
            TrustServerCertificate = true,
        };
        manager.AddServer(server, windows ? null : user, windows ? null : password);

        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        return new LiveServer(builder.ConnectionString, new SqlPlanFetcher(manager), serverId);
    }

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
        var offsets = new List<(int, int)>();
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
        var live = Live();
        Assert.SkipWhen(live is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (handle, offsets) = await RunBatchAsync(live!.ConnectionString, FourStatementBatch, Markers.Length);

        var second = await live.Fetcher.FetchStatementPlanXmlAsync(live.ServerId, handle, offsets[1].Start, offsets[1].End, ct);
        var batch = await live.Fetcher.FetchPlanXmlAsync(live.ServerId, handle, ct);

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
        var live = Live();
        Assert.SkipWhen(live is null, SkipReason);
        var (handle, offsets) = await RunBatchAsync(live!.ConnectionString, FourStatementBatch, Markers.Length);

        var last = await live.Fetcher.FetchStatementPlanXmlAsync(
            live.ServerId, handle, offsets[^1].Start, offsets[^1].End, TestContext.Current.CancellationToken);

        Assert.NotNull(last);
        AssertOnlyStatement(last, Markers.Length - 1);
    }

    [Fact]
    public async Task AnEndOffsetOfMinusOne_IsPassedAsItIs_AndRunsToTheEndOfTheBatch()
    {
        var live = Live();
        Assert.SkipWhen(live is null, SkipReason);
        var (handle, _) = await RunBatchAsync(live!.ConnectionString, FourStatementBatch, Markers.Length);

        var plan = await live.Fetcher.FetchStatementPlanXmlAsync(live.ServerId, handle, 0, -1, TestContext.Current.CancellationToken);

        Assert.NotNull(plan);
        foreach (var marker in Markers)
        {
            Assert.Contains(marker, plan, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task APlanHandleThatNamesNoCachedPlan_ReturnsNull_ThroughBothFetches()
    {
        var live = Live();
        Assert.SkipWhen(live is null, SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var gone = "0x" + new string('0', 100);

        Assert.Null(await live!.Fetcher.FetchStatementPlanXmlAsync(live.ServerId, gone, 0, -1, ct));
        Assert.Null(await live.Fetcher.FetchPlanXmlAsync(live.ServerId, gone, ct));
    }
}
