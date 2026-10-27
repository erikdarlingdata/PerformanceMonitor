/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #6808: <c>PlanChurnSql</c> (the plan-churn arm of <c>get_query_store_clutter</c>) sorted a materialised copy of every raw
/// <c>query_store_stats</c> row in the window twice, once per <c>COUNT(DISTINCT collection_time)</c>, and the sorts spilled to disk
/// at 2.4 million rows. These tests plant a store whose answer is known from a plain C# model of the seed, run the shipped
/// statement under a small <c>work_mem</c> (<c>SET LOCAL</c>), and assert two things: the rows are exactly the model's, for
/// two windows and with and without a database choice, and the plan the statement actually ran has no spill.
/// </summary>
public sealed class QueryStoreClutterChurnPlanLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-churn test (it mints its own scratch database).";

    [Fact]
    public async Task PlanChurn_ReturnsTheSeededAnswerExactly_ForTwoWindows_WithAndWithoutADatabaseChoice_AndNeverSpills()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        await QueryStoreClutterChurnProbe.SeedAsync(connection, anchor, ct);

        /* the whole seed, then a window that starts in the middle of it (the 24 h shape: older collections fall out and
           the arrivals, the first collection and the seen-once count all move) */
        var whole = anchor.AddDays(-7);
        var cut = QueryStoreClutterChurnProbe.CollectionTime(anchor, 12);
        var end = anchor;

        foreach (var start in new[] { whole, cut })
        {
            foreach (var databases in new string[]?[] { null, [QueryStoreClutterChurnProbe.Small], [QueryStoreClutterChurnProbe.Big, QueryStoreClutterChurnProbe.Small] })
            {
                var label = $"window start {start:o}, databases {(databases is null ? "all" : string.Join("+", databases))}";
                var run = await QueryStoreClutterChurnProbe.RunAsync(connection, start, end, databases, ct);
                var expected = QueryStoreClutterChurnProbe.ExpectedFor(anchor, start, end, databases);
                Assert.True(expected.Count > 0, "the model must give a row for " + label);
                Assert.True(QueryStoreClutterChurnProbe.FirstDifference(run.Rows, expected) is null,
                    label + "\n" + QueryStoreClutterChurnProbe.FirstDifference(run.Rows, expected));
                Assert.True(QueryStoreClutterChurnProbe.SpillIn(run.PlanText) is null,
                    label + ": the statement spilled at work_mem " + QueryStoreClutterChurnProbe.WorkMem + ": " + QueryStoreClutterChurnProbe.SpillIn(run.PlanText) + "\n" + run.PlanText);
            }
        }
    }
}
