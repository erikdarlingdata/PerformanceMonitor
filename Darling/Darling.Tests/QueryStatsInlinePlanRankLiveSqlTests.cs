/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7344 against a live SQL Server: the inline plan form of <see cref="QueryStatsCollector"/> (plan capture on, the deferred
/// fetch off) hands its <see cref="QueryStatsCollector.PlansByRecency"/> recency slots to the rows the self-filter KEPT.
/// Ranked before the filter, the collector's own statements (every one carries the <c>PerformanceMonitorLite</c> marker, and
/// they are the newest rows when the collector reads) took the slots and the filter then threw them away, so a user
/// statement past the cap came back with no plan.
///
/// <para>The test runs one user statement (twice, so a plan stub under <c>optimize for ad hoc workloads</c> becomes a full
/// plan), then more than <see cref="QueryStatsCollector.PlansByRecency"/> distinct marker statements that are newer and
/// slower than it, then the collector's real batch and real <c>ReadAsync</c>, and asserts the user statement's row carries
/// a plan. The assertion is scoped to the row carrying the test's GUID, so other activity on the instance cannot decide it.
/// It creates no object: it runs SELECTs in an existing user database.</para>
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL
/// auth), so it skips where the runner has no SQL Server.</para>
/// </summary>
public sealed class QueryStatsInlinePlanRankLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live query_stats inline-plan rank test.";

    private static string? ConnectionString(string database)
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
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "darling-qs-rank-live",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    /// <summary>The collector excludes the system databases, so the statements must run in a user database that exists.</summary>
    private const string FindUserDatabaseSql = @"
SELECT TOP (1) d.name
FROM sys.databases AS d
WHERE d.database_id > 4
AND   d.state = 0
AND   d.is_read_only = 0
AND   HAS_DBACCESS(d.name) = 1
AND   d.name <> N'PerformanceMonitor'
ORDER BY d.database_id;";

    [Fact]
    public async Task InlinePlan_UserStatementBehindNewerCollectorStatements_StillGetsItsPlan()
    {
        var master = ConnectionString("master");
        Assert.SkipWhen(master is null, SkipMessage);

        var ct = TestContext.Current.CancellationToken;

        string? database;
        await using (var probe = new SqlConnection(master!))
        {
            await probe.OpenAsync(ct);
            using var find = new SqlCommand(FindUserDatabaseSql, probe) { CommandTimeout = 30 };
            database = (string?)await find.ExecuteScalarAsync(ct);
        }

        Assert.SkipWhen(database is null, "The target has no online, writable user database to run the test statements in.");

        var guid = Guid.NewGuid().ToString("N");
        await using var work = new SqlConnection(ConnectionString(database!));
        await work.OpenAsync(ct);

        /* Twice, byte-identical: the second execution replaces a compiled-plan stub (optimize for ad hoc workloads) with a
           full plan, so a missing plan below can only come from the ranking. */
        /* The GUID sits INSIDE the statement: the DMV's statement offsets skip a leading comment, so a comment before the SELECT
           would not be part of the row's query_text. */
        var userStatement = $"SELECT c = COUNT_BIG(*) /* qs-rank-user {guid} */ FROM sys.objects;";
        for (var i = 0; i < 2; i++)
        {
            await RunAsync(work, userStatement, ct);
        }

        /* More distinct marker statements than the recency half holds, all newer than the user statement. Each waits a few
           milliseconds so its total_elapsed_time is above the user statement's too: the weight half cannot rescue the user
           statement under the old ranking either. Distinct text per statement makes each its own cache entry. */
        var markerCount = QueryStatsCollector.PlansByRecency + 20;
        for (var i = 0; i < markerCount; i++)
        {
            await RunAsync(work, $"/* PerformanceMonitorLite qs-rank-marker {guid} {i} */ WAITFOR DELAY '00:00:00.020';", ct);
        }

        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo(),
            CapturePlanXml = true,
            DeferPlanXmlFetch = false,
        };

        var query = QueryStatsCollector.Instance.BuildQuery(context);
        List<QueryStatsCollector.Row> rows;
        await using (var collect = new SqlConnection(master!))
        {
            await collect.OpenAsync(ct);
            /* The shipped command builder, so the text and the parameter bindings are the host's own, not a copy of them. */
            await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, collect, 60);
            await using var reader = await command.ExecuteReaderAsync(ct);
            rows = (await QueryStatsCollector.Instance.ReadAsync(reader, context, ct)).ToList();
        }

        var mine = rows
            .Where(r => r.QueryText is not null && r.QueryText.Contains(guid, StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(mine);
        Assert.DoesNotContain(mine, r => r.QueryText!.Contains("PerformanceMonitorLite", StringComparison.Ordinal));
        Assert.All(mine, r => Assert.False(string.IsNullOrEmpty(r.QueryPlanXml),
            "the user statement is among the newest rows the self-filter kept, so it holds a plan slot; "
            + $"rows read: {rows.Count.ToString(CultureInfo.InvariantCulture)}"));
    }

    private static async Task RunAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
