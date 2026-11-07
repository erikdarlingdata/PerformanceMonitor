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
/// #8080: tempdb_stats' top session is the session holding the most tempdb pages now, and its figure is what it holds.
/// <c>sys.dm_db_session_space_usage</c> counts for the life of the session: the <c>*_alloc_page_count</c> columns every
/// page it has allocated since it connected, the <c>*_dealloc_page_count</c> columns every page it has released, so the
/// pages it holds are the difference. Two sessions are staged on a live SQL Server: one that fills a 16 MB temp table and
/// keeps it, and one that fills and drops a 7.5 MB temp table twenty-four times over, so its lifetime allocation is many
/// times the other's hold while it holds nothing. The collector's own <see cref="TempDbStatsCollector.BuildQuery"/> and
/// <see cref="TempDbStatsCollector.ReadAsync"/> then run on a third connection: the top session must be the one keeping
/// its table, its figure that table's size, and <c>total_sessions_using_tempdb</c> the sessions holding pages now, which
/// leaves the churning session out.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It creates only
/// temp tables, which go when the connections close.</para>
/// </summary>
public sealed class TempDbStatsTopSessionHoldLiveSqlTests
{
    /* One row per page. The kept table is 2,048 pages, 16 MB. Each churned table is 960 pages, 7.5 MB, under the
       128-extent (8 MB) line above which SQL Server defers a drop to a background task; below it the drop releases the
       pages on the session itself, so the session's dealloc counters carry the release. */
    private const int HoldPages = 2048;
    private const int ChurnPages = 960;
    private const int ChurnFills = 24;

    private const string FillAndKeepSql = @"
SELECT TOP (@pages) pad = CONVERT(char(8000), N'h')
INTO #held
FROM sys.all_objects AS a
CROSS JOIN sys.all_objects AS b;
SELECT @@SPID;";

    private const string FillAndDropRepeatedlySql = @"
DECLARE @i integer = 0;
WHILE @i < @fills
BEGIN
    SELECT TOP (@pages) pad = CONVERT(char(8000), N'c')
    INTO #churn
    FROM sys.all_objects AS a
    CROSS JOIN sys.all_objects AS b;
    DROP TABLE #churn;
    SET @i += 1;
END;
SELECT @@SPID;";

    private const string SessionFiguresSql = @"
SELECT
    held_mb = CONVERT(decimal(18,2), ((user_objects_alloc_page_count - user_objects_dealloc_page_count) + (internal_objects_alloc_page_count - internal_objects_dealloc_page_count)) * 8 / 1024.0),
    lifetime_mb = CONVERT(decimal(18,2), (user_objects_alloc_page_count + internal_objects_alloc_page_count) * 8 / 1024.0)
FROM sys.dm_db_session_space_usage
WHERE session_id = @session_id;";

    private const string SessionsHoldingPagesSql = @"
SELECT COUNT_BIG(*)
FROM sys.dm_db_session_space_usage
WHERE (user_objects_alloc_page_count - user_objects_dealloc_page_count) + (internal_objects_alloc_page_count - internal_objects_dealloc_page_count) > 0;";

    private static string? ConnectionString(string applicationName)
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
            ApplicationName = applicationName,
            IntegratedSecurity = string.IsNullOrEmpty(user),
            /* Each connection is its own session for the whole test, so its temp table and its counters stay put. */
            Pooling = false,
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo(),
            State = new Dictionary<string, string>(StringComparer.Ordinal),
            HasCollectedBefore = false,
        };

    [Fact]
    public async Task TopSession_IsTheOneHoldingPages_AtItsHold_AndAChurningSessionIsNeitherTopNorCounted()
    {
        Assert.SkipWhen(ConnectionString("probe") is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live tempdb top session test.");
        var ct = TestContext.Current.CancellationToken;

        await using var holder = new SqlConnection(ConnectionString("pm-8080-tempdb-holder"));
        await using var churner = new SqlConnection(ConnectionString("pm-8080-tempdb-churner"));
        await using var collector = new SqlConnection(ConnectionString("pm-8080-tempdb-collector"));
        await holder.OpenAsync(ct);
        await churner.OpenAsync(ct);
        await collector.OpenAsync(ct);

        int holderSpid;
        await using (var fill = new SqlCommand(FillAndKeepSql, holder) { CommandTimeout = 120 })
        {
            fill.Parameters.AddWithValue("@pages", HoldPages);
            holderSpid = Convert.ToInt32(await fill.ExecuteScalarAsync(ct));
        }

        int churnerSpid;
        await using (var churn = new SqlCommand(FillAndDropRepeatedlySql, churner) { CommandTimeout = 300 })
        {
            churn.Parameters.AddWithValue("@pages", ChurnPages);
            churn.Parameters.AddWithValue("@fills", ChurnFills);
            churnerSpid = Convert.ToInt32(await churn.ExecuteScalarAsync(ct));
        }

        var context = MakeContext();
        var query = TempDbStatsCollector.Instance.BuildQuery(context);
        TempDbStatsCollector.Row row;
        await using (var command = SqlServerTargetProvider.Instance.CreateCommand(query, collector, 60))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            row = Assert.Single(await TempDbStatsCollector.Instance.ReadAsync(reader, context, ct));
        }

        var (holderHeldMb, holderLifetimeMb) = await ReadSessionFiguresAsync(collector, holderSpid, ct);
        var (churnerHeldMb, churnerLifetimeMb) = await ReadSessionFiguresAsync(collector, churnerSpid, ct);
        var sessionsHoldingPages = await ReadSessionsHoldingPagesAsync(collector, ct);

        /* The stage is set as intended: the churner allocated far more over its life than the holder keeps, and keeps
           next to nothing of it, so the two readings of "top" disagree. */
        Assert.True(holderHeldMb >= HoldPages * 8m / 1024m * 0.9m, $"the holder keeps {holderHeldMb} MB; the fill did not land");
        Assert.True(churnerLifetimeMb >= holderHeldMb * 4m, $"the churner's lifetime allocation {churnerLifetimeMb} MB is not well above the holder's {holderHeldMb} MB hold");
        Assert.True(churnerHeldMb < holderHeldMb * 0.1m, $"the churner still holds {churnerHeldMb} MB; its drops did not release the pages");

        Assert.True(
            row.TopSessionId == holderSpid,
            $"top_session_id is {row.TopSessionId} (the churner is {churnerSpid}, lifetime {churnerLifetimeMb} MB, holding {churnerHeldMb} MB); the session holding the most pages is {holderSpid} with {holderHeldMb} MB");
        Assert.True(
            Math.Abs(row.TopSessionMb - holderHeldMb) <= Math.Max(1m, holderHeldMb * 0.15m),
            $"top_session_tempdb_mb {row.TopSessionMb} is not the top session's hold of {holderHeldMb} MB (its lifetime allocation is {holderLifetimeMb} MB)");
        Assert.True(
            row.TotalSessions == sessionsHoldingPages,
            $"total_sessions_using_tempdb is {row.TotalSessions}; {sessionsHoldingPages} sessions hold tempdb pages now, and the churner (lifetime {churnerLifetimeMb} MB, holding {churnerHeldMb} MB) is not one of them");
    }

    private static async Task<(decimal HeldMb, decimal LifetimeMb)> ReadSessionFiguresAsync(SqlConnection connection, int sessionId, CancellationToken ct)
    {
        await using var command = new SqlCommand(SessionFiguresSql, connection);
        command.Parameters.AddWithValue("@session_id", (short)sessionId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"session {sessionId} has no row in sys.dm_db_session_space_usage");
        return (reader.GetDecimal(0), reader.GetDecimal(1));
    }

    private static async Task<long> ReadSessionsHoldingPagesAsync(SqlConnection connection, CancellationToken ct)
    {
        await using var command = new SqlCommand(SessionsHoldingPagesSql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }
}
