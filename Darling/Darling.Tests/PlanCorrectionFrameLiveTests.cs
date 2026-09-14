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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3419: the clock FRAME of a payload column, asserted as a residual against the collector-written UTC
/// <c>collection_time</c> rather than as the presence or absence of a de-skew expression.
///
/// <para><b>Why the residual and not the expression.</b> <c>McpPayloadClockFrameDisciplineTests</c> already
/// pins the expressions, and a source pin says nothing about direction: the four
/// <c>sys.dm_db_tuning_recommendations</c> lifecycle times carried a de-skew that was spelled exactly right,
/// applied to a source that was already UTC, and the result read four hours into the FUTURE. A pin worded
/// "no subtraction appears in this read" would also go green on a reordered SELECT list, on a conversion
/// moved into C#, or on a second de-skew spelled differently. What cannot be satisfied any of those ways is
/// the premise itself — a recommendation the engine refreshed at the instant of collection must come back AT
/// that instant — so that is what is asserted here, through the shipped reader, against a real store.</para>
///
/// <para><b>Both directions in one test, on one planted offset.</b> The already-UTC arm
/// (<c>plan_correction</c>) and the server-local arm (<c>blocked_process_reports</c>) are given values that
/// denote the SAME instant in their own frames — the recommendation stamps equal to <c>collection_time</c>,
/// the six blocking stamps 240 minutes behind it — and both must come back equal to that instant. So the
/// test fails if the plan-correction de-skew returns, and it fails if the blocking de-skew is removed, and
/// neither failure can be produced by the other's fix. That is also the answer to the question #3206's
/// census could not ask: the instrument distinguishes the two frames instead of answering the same way to
/// everything, which is checked explicitly below by comparing the two arms' STORED values.</para>
///
/// <para><b>The offset is deliberately non-zero and deliberately -240.</b> A server reporting 0 makes both
/// arms vacuous — subtracting nothing is indistinguishable from subtracting correctly — and -240 is the
/// production fleet's reported offset, which is the population the defect was visible in. The store on one
/// fleet carries both 0 and -240, so the zero case is real; it is unaffected by construction rather than by
/// test, and <see cref="TheOffsetUnderTest_IsNonZero_OrNeitherArmDiscriminates"/> keeps this test out of
/// that case.</para>
///
/// <para>Plants four rows and deletes them in a <c>finally</c> through <see cref="LiveStoreCleanup"/>,
/// creating and dropping no relations, so the fixture's end-of-collection residue diff sees nothing.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanCorrectionFrameLiveTests
{
    private const string ServerName = "darling-3419-clock-frame-probe";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>The production fleet's reported offset, and the one the defect was measured at.</summary>
    private const int OffsetMinutes = -240;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The premise both arms rest on. Named as its own test because an offset of 0 would let every
    /// assertion below pass while proving nothing, and a constant edited to 0 in passing is exactly the
    /// mutation a residual pin is otherwise blind to.
    /// </summary>
    [Fact]
    public void TheOffsetUnderTest_IsNonZero_OrNeitherArmDiscriminates()
    {
        Assert.NotEqual(0, OffsetMinutes);
    }

    [Fact]
    public async Task ARecommendationRefreshedAtCollection_ReturnsAtThatInstant_WhileABlockingStampInTheServersClockIsDeSkewedToIt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(
            string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live clock-frame residual test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            /* Whole seconds: both reads return timestamp values that are compared for exact equality, and a
               fractional component would survive the arithmetic and make the failure message unreadable
               without changing what is being tested. */
            var collectionTime = Naive(new DateTime(
                DateTime.UtcNow.AddMinutes(-5).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond));

            /* The server-local representation of the SAME instant: 240 minutes behind UTC, because the
               offset this server reports is -240. */
            var serverLocalSameInstant = collectionTime.AddMinutes(OffsetMinutes);

            await PlantOffsetAsync(connection, collectionTime, ct);
            await PlantRecommendationAsync(connection, collectionTime, ct);
            await PlantBlockedProcessReportAsync(connection, collectionTime, serverLocalSameInstant, ct);

            /* The two arms' stored values are 240 minutes apart. Asserted rather than assumed, because if
               the planting collapsed them the two arms below would agree for a reason that has nothing to do
               with either read. */
            Assert.Equal(
                -OffsetMinutes,
                (int)(collectionTime - serverLocalSameInstant).TotalMinutes);

            /* ── arm A: already UTC. The stored value equals collection_time and must come back equal to
                  it. An offset applied here ADDS four hours, because the offset is negative. ── */
            var recommendations = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(
                postgres, ServerId, collectionTime.AddHours(-1), collectionTime.AddHours(1), ct);

            var recommendation = Assert.Single(recommendations);
            Assert.Equal(collectionTime, recommendation.CollectionTime);

            var lifecycle = new List<(string Field, DateTime? Value)>
            {
                ("valid_since", recommendation.ValidSinceUtc),
                ("last_refresh", recommendation.LastRefreshUtc),
                ("execute_action_initiated_time", recommendation.ExecuteActionInitiatedTimeUtc),
                ("revert_action_initiated_time", recommendation.RevertActionInitiatedTimeUtc),
            };

            foreach (var (field, value) in lifecycle)
            {
                Assert.NotNull(value);

                Assert.True(
                    value!.Value <= recommendation.CollectionTime,
                    $"get_plan_corrections returned {field} = {value:O}, which is AFTER the "
                    + $"collection_time {recommendation.CollectionTime:O} on the same row. A recommendation "
                    + "cannot have been re-evaluated after the read that observed it, so this is the frame "
                    + "being wrong and not the data: sys.dm_db_tuning_recommendations reports these four in "
                    + $"UTC, and subtracting a {OffsetMinutes}-minute offset from a UTC value adds "
                    + $"{-OffsetMinutes} minutes.");

                Assert.Equal(collectionTime, value!.Value);
            }

            /* ── arm B: server-local. The stored value is 240 minutes behind collection_time and must come
                  back ON it, which is the de-skew doing its job. ── */
            var blocked = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(
                postgres, ServerId, collectionTime.AddHours(-1), collectionTime.AddHours(1), ct);

            var report = Assert.Single(blocked);
            Assert.Equal(collectionTime, report.EventTime!.Value);

            var serverClockStamps = new List<(string Field, DateTime? Value)>
            {
                ("blocked_last_tran_started", report.BlockedLastTranStartedUtc),
                ("blocking_last_tran_started", report.BlockingLastTranStartedUtc),
                ("blocked_last_batch_started", report.BlockedLastBatchStartedUtc),
                ("blocking_last_batch_started", report.BlockingLastBatchStartedUtc),
                ("blocked_last_batch_completed", report.BlockedLastBatchCompletedUtc),
                ("blocking_last_batch_completed", report.BlockingLastBatchCompletedUtc),
            };

            foreach (var (field, value) in serverClockStamps)
            {
                Assert.NotNull(value);

                Assert.True(
                    value!.Value == collectionTime,
                    $"get_blocking returned {field} = {value:O}, but the stored value denotes the same "
                    + $"instant as event_time {collectionTime:O} in a clock {OffsetMinutes} minutes off UTC. "
                    + $"Coming back at {serverLocalSameInstant:O} means the de-skew is gone; coming back "
                    + "anywhere else means the offset resolved to something other than this server's.");
            }

            /* The discrimination, stated: one read left its column alone and the other moved it by the whole
               offset, and both landed on the instant the values denote. A census that answered the same way
               to both is the defect this file exists to make impossible. */
            Assert.Equal(recommendation.LastRefreshUtc, report.BlockedLastBatchStartedUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The one <c>server_properties</c> row the offset CTE in every de-skewing read resolves to. The CTE
    /// takes the NEWEST non-null offset for the server, so one row is the whole of what it can see.
    /// </summary>
    private static async Task PlantOffsetAsync(NpgsqlConnection connection, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            """
            INSERT INTO server_properties
                (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
            VALUES ($1, $2, $3, $4, $5)
            """, connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(OffsetMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// One recommendation whose four lifecycle stamps are all equal to <c>collection_time</c> — the engine
    /// re-evaluated it at the instant the collector read the DMV. That is the physically achievable extreme
    /// and it is what the live fleet shows: the newest value of each of the four sits within a fraction of a
    /// minute of <c>collection_time</c> across tens of millions of rows. <c>recommendation_name</c> must be
    /// non-null or the read's own filter drops the row as an enablement-only one.
    /// </summary>
    private static async Task PlantRecommendationAsync(NpgsqlConnection connection, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            """
            INSERT INTO plan_correction
                (collection_id, collection_time, server_id, server_name, database_name, recommendation_name,
                 recommendation_state, valid_since, last_refresh, execute_action_initiated_time,
                 revert_action_initiated_time, score)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $2, $2, $2, $2, $8)
            """, connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("frame_probe_db");
        command.Parameters.AddWithValue("PlanRegression_frame_probe");
        command.Parameters.AddWithValue("Active");
        command.Parameters.AddWithValue(83);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// One blocked-process report whose <c>event_time</c> is the UTC instant and whose six transaction and
    /// batch stamps are that same instant expressed in a clock <see cref="OffsetMinutes"/> off UTC — which
    /// is what the engine renders into the report XML. The read must return all six ON <c>event_time</c>.
    /// </summary>
    private static async Task PlantBlockedProcessReportAsync(
        NpgsqlConnection connection, DateTime collectionTime, DateTime serverLocalSameInstant, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            """
            INSERT INTO blocked_process_reports
                (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
                 blocked_spid, blocking_spid, wait_time_ms,
                 blocked_last_tran_started, blocking_last_tran_started,
                 blocked_last_batch_started, blocking_last_batch_started,
                 blocked_last_batch_completed, blocking_last_batch_completed)
            VALUES ($1, $2, $3, $4, $2, $5, $6, $7, $8, $9, $9, $9, $9, $9, $9)
            """, connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("frame_probe_db");
        command.Parameters.AddWithValue(101);
        command.Parameters.AddWithValue(102);
        command.Parameters.AddWithValue(5_000L);
        command.Parameters.AddWithValue(serverLocalSameInstant);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM plan_correction WHERE server_id = {ServerId}; "
            + $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; "
            + $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; "
            + $"DELETE FROM server_properties WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
