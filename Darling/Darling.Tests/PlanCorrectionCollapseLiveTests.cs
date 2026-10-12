/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5644 W21: the web's Plan Corrections read collapses the collector's per-cycle re-captures to one row per
/// recommendation. Three recommendations captured five times each: the per-capture read returns 15 rows, the
/// collapsed read returns 3, each at its newest capture with its first capture and its capture count. The cap then
/// bounds DISTINCT recommendations.
///
/// <para>Plants fifteen rows under a per-run server id and deletes them in a <c>finally</c> through
/// <see cref="LiveStoreCleanup"/>, creating and dropping no relations.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanCorrectionCollapseLiveTests
{
    private const string ServerName = "darling-5644-plan-correction-collapse";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const int Recommendations = 3;
    private const int Captures = 5;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task RepeatedCaptures_CollapseToOneRowPerRecommendation_WithFirstSeenAndCount()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live collapse test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            /* Whole seconds, so the planted and returned instants compare for exact equality. */
            var newest = DateTime.SpecifyKind(
                new DateTime(DateTime.UtcNow.AddMinutes(-5).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            var oldest = newest.AddMinutes(-5 * (Captures - 1));

            for (var recommendation = 1; recommendation <= Recommendations; recommendation++)
            {
                for (var capture = 0; capture < Captures; capture++)
                {
                    await PlantAsync(connection, oldest.AddMinutes(5 * capture), recommendation, capture, ct);
                }
            }

            var start = newest.AddHours(-1);
            var end = newest.AddHours(1);

            var perCapture = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(
                postgres, ServerId, start, end, cap: 50, cancellationToken: ct);
            Assert.Equal(Recommendations * Captures, perCapture.Count);
            Assert.All(perCapture, r => Assert.Null(r.CaptureCount));

            var collapsed = await DarlingPlanCorrectionReader.GetCollapsedPlanCorrectionsAsync(
                postgres, ServerId, start, end, cap: 50, cancellationToken: ct);

            Assert.Equal(Recommendations, collapsed.Count);
            Assert.All(collapsed, r =>
            {
                Assert.Equal(newest, r.CollectionTime);
                Assert.Equal(oldest, r.FirstSeen);
                Assert.Equal(Captures, r.CaptureCount);
                /* The newest capture's own fields, not the oldest's: the state advanced with each capture. */
                Assert.Equal("State" + (Captures - 1), r.RecommendationState);
            });
            Assert.Equal(new long?[] { 1, 2, 3 }, collapsed.Select(r => r.QueryId).OrderBy(q => q).ToArray());

            /* The cap bounds distinct recommendations: a cap of 2 returns 2 of the 3, not 2 captures of one. */
            var capped = await DarlingPlanCorrectionReader.GetCollapsedPlanCorrectionsAsync(
                postgres, ServerId, start, end, cap: 2, cancellationToken: ct);
            Assert.Equal(2, capped.Count);
            Assert.Equal(2, capped.Select(r => r.QueryId).Distinct().Count());

            /* A window that holds only the last two captures counts those two, and first_seen is the oldest in the window. */
            var narrow = await DarlingPlanCorrectionReader.GetCollapsedPlanCorrectionsAsync(
                postgres, ServerId, newest.AddMinutes(-5), end, cap: 50, cancellationToken: ct);
            Assert.Equal(Recommendations, narrow.Count);
            Assert.All(narrow, r =>
            {
                Assert.Equal(2, r.CaptureCount);
                Assert.Equal(newest.AddMinutes(-5), r.FirstSeen);
            });

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(NpgsqlConnection connection, DateTime collectionTime, int recommendation, int capture, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            """
            INSERT INTO plan_correction
                (collection_id, collection_time, server_id, server_name, database_name, recommendation_name,
                 recommendation_state, query_id, score)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, connection);
        command.Parameters.AddWithValue((long)(recommendation * 100 + capture));
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("collapse_probe_db");
        command.Parameters.AddWithValue("PlanRegression_" + recommendation);
        command.Parameters.AddWithValue("State" + capture);
        command.Parameters.AddWithValue((long)recommendation);
        command.Parameters.AddWithValue(70 + recommendation);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM plan_correction WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
