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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live, end-to-end proof of #4346's <see cref="PlanForceActionDetailScrub"/> against a REAL PostgreSQL
/// store: a legacy row's raw exception text is rewritten to the safe sentence, a post-#4326 row is
/// untouched (byte-identical), a second start is a no-op, and a batch that fails leaves the marker unset
/// so a later run retries.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) rather
/// than sharing the live fixture, so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class PlanForceActionDetailScrubLiveTests
{
    private const string LegacyDetail =
        "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException: 28P01: password authentication failed for user \"darling_ro\" at host db-primary-02.internal)";

    private const string SafeDetail =
        "state_unavailable: the forcing and automatic-plan-correction state read failed (NpgsqlException; the log has the full error) \u2014 an unattended force cannot proceed on an unknown engine state";

    /// <summary>The fixed sentence <c>PgPlanForceActionStore.SanitizeDetailForAudit</c> replaces a legacy
    /// (non-safe-shape) <c>state_unavailable</c> block with — the WHOLE block is replaced, so the
    /// rewritten text has no parenthetical at all, unlike <see cref="SafeDetail"/>'s.</summary>
    private const string RedactedLegacyDetail =
        "state_unavailable: the forcing and automatic-plan-correction state read failed \u2014 an unattended force cannot proceed on an unknown engine state";

    /// <summary>
    /// Pins 1-3 together against one scratch store: a legacy row's raw credential/host text is rewritten
    /// to the sanitizer's safe sentence; a post-#4326 row (already the safe shape) is byte-identical
    /// after the scrub; and a second run is a no-op (0 candidates touched, marker already set).
    /// </summary>
    [Fact]
    public async Task TheScrub_RewritesLegacyRow_LeavesPostFixRowByteIdentical_AndIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4346 scrub test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var legacyId = await InsertActionAsync(connection, LegacyDetail, ct);
        var postFixId = await InsertActionAsync(connection, SafeDetail, ct);

        /* ── Prove the assertions below actually depend on the scrub: the raw legacy text is really
           there before it runs. ── */
        var beforeLegacy = await ReadRawDetailAsync(connection, legacyId, ct);
        Assert.Contains("darling_ro", beforeLegacy);
        Assert.Contains("db-primary-02", beforeLegacy);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        // ── Pin 1: legacy row is rewritten to the safe sentence. ──
        var first = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.Equal(1, first.RowsUpdated);

        // The coarse filter's candidate count matches the number of rows seeded: the legacy row AND
        // the post-#4326 row both mention the block's prefix, so both are read as candidates even
        // though only the legacy row actually changes under the sanitizer.
        Assert.Equal(2, first.CandidatesRead);

        var afterLegacy = await ReadRawDetailAsync(connection, legacyId, ct);
        Assert.DoesNotContain("darling_ro", afterLegacy);
        Assert.DoesNotContain("db-primary-02", afterLegacy);
        Assert.DoesNotContain("password authentication failed", afterLegacy);
        Assert.Equal(RedactedLegacyDetail, afterLegacy);

        // ── Pin 2: a post-#4326 row is byte-identical (untouched by the write batch). ──
        var afterPostFix = await ReadRawDetailAsync(connection, postFixId, ct);
        Assert.Equal(SafeDetail, afterPostFix);

        // ── Pin 3: a second start is a no-op — the marker already holds. ──
        var second = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.True(second.AlreadyDone);
        Assert.Equal(0, second.RowsUpdated);
        Assert.Equal(0, second.CandidatesRead);

        var markerValue = await ScalarTextAsync(connection,
            $"SELECT state_value FROM collect.collector_state WHERE server_id = {DarlingObservability.FleetServerId} " +
            $"AND collector_name = '{PlanForceActionDetailScrub.StateCollectorName}' AND state_key = '{PlanForceActionDetailScrub.DoneStateKey}'",
            ct);
        Assert.Equal("1", markerValue);
    }

    /// <summary>
    /// Pin 4: a batch that fails leaves the marker unset, so a later run retries. The failure is a real
    /// database error the scrub's own UPDATE hits — a CHECK constraint rejecting the SANITIZED text for
    /// one specific row — standing in for any per-batch failure without touching the scrub's own logic
    /// (the same technique <c>PgSettingScrubLiveTests.TheScrub_IsolatesAFailingServer_AndRetriesOnTheNextRun</c>
    /// uses). Because the scrub batches by row COUNT (<see cref="PlanForceActionDetailScrub.MaxKeysPerUpdate"/>,
    /// not per-row), a single-row batch containing the bad row fails whole; the marker is withheld and a
    /// second run — after the constraint is dropped — retries and completes.
    /// </summary>
    [Fact]
    public async Task TheScrub_LeavesTheMarkerUnset_WhenABatchFails_AndRetriesOnTheNextRun()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4346 batch-failure pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var badId = await InsertActionAsync(connection, LegacyDetail, ct);

        /* Rejects only the sanitized (safe) shape for THIS row — an honest constraint violation (23514)
           the scrub's batch UPDATE actually hits when it tries to write back the sanitizer's output for
           badId, standing in for any real per-row write failure without touching the product's logic. */
        await ExecAsync(connection,
            $"ALTER TABLE collect.plan_force_actions ADD CONSTRAINT chk_4346_forced_failure " +
            $"CHECK (action_id <> {badId} OR detail NOT LIKE 'state_unavailable: the forcing and automatic-plan-correction state read failed \u2014%')",
            ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.Equal(0, first.RowsUpdated);

        // The row still carries the raw legacy text — the failed batch never committed.
        var stillLegacy = await ReadRawDetailAsync(connection, badId, ct);
        Assert.Contains("darling_ro", stillLegacy);

        // The marker is withheld: the run failed a batch.
        var markerValue = await ScalarTextAsync(connection,
            $"SELECT state_value FROM collect.collector_state WHERE server_id = {DarlingObservability.FleetServerId} " +
            $"AND collector_name = '{PlanForceActionDetailScrub.StateCollectorName}' AND state_key = '{PlanForceActionDetailScrub.DoneStateKey}'",
            ct);
        Assert.Null(markerValue);

        // Remove the cause; the next run retries and completes.
        await ExecAsync(connection, "ALTER TABLE collect.plan_force_actions DROP CONSTRAINT chk_4346_forced_failure", ct);

        var second = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(second.AlreadyDone);
        Assert.Equal(1, second.RowsUpdated);

        var afterRetry = await ReadRawDetailAsync(connection, badId, ct);
        Assert.Equal(RedactedLegacyDetail, afterRetry);

        var markerValueAfterRetry = await ScalarTextAsync(connection,
            $"SELECT state_value FROM collect.collector_state WHERE server_id = {DarlingObservability.FleetServerId} " +
            $"AND collector_name = '{PlanForceActionDetailScrub.StateCollectorName}' AND state_key = '{PlanForceActionDetailScrub.DoneStateKey}'",
            ct);
        Assert.Equal("1", markerValueAfterRetry);
    }

    /// <summary>
    /// Pins the scrub across MORE than one write batch (<see cref="PlanForceActionDetailScrub.MaxKeysPerUpdate"/>
    /// legacy rows per batch): every legacy row is rewritten regardless of which batch carries it, the
    /// post-#4326 rows stay byte-identical, the marker is set, <c>RowsUpdated</c> equals the legacy count,
    /// and a second run is <c>AlreadyDone</c>.
    /// </summary>
    [Fact]
    public async Task TheScrub_RewritesEveryLegacyRow_AcrossMultipleBatches_AndIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4346 multi-batch scrub test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const int legacyCount = PlanForceActionDetailScrub.MaxKeysPerUpdate + 3;
        const int postFixCount = 3;

        var legacyIds = new List<long>(legacyCount);
        for (var i = 0; i < legacyCount; i++)
        {
            legacyIds.Add(await InsertActionAsync(connection, LegacyDetail, ct));
        }

        var postFixIds = new List<long>(postFixCount);
        for (var i = 0; i < postFixCount; i++)
        {
            postFixIds.Add(await InsertActionAsync(connection, SafeDetail, ct));
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.Equal(legacyCount, first.RowsUpdated);

        foreach (var legacyId in legacyIds)
        {
            var afterLegacy = await ReadRawDetailAsync(connection, legacyId, ct);
            Assert.Equal(RedactedLegacyDetail, afterLegacy);
        }

        foreach (var postFixId in postFixIds)
        {
            var afterPostFix = await ReadRawDetailAsync(connection, postFixId, ct);
            Assert.Equal(SafeDetail, afterPostFix);
        }

        var markerValue = await ScalarTextAsync(connection,
            $"SELECT state_value FROM collect.collector_state WHERE server_id = {DarlingObservability.FleetServerId} " +
            $"AND collector_name = '{PlanForceActionDetailScrub.StateCollectorName}' AND state_key = '{PlanForceActionDetailScrub.DoneStateKey}'",
            ct);
        Assert.Equal("1", markerValue);

        var second = await PlanForceActionDetailScrub.RunAsync(postgres, logger: null, ct);
        Assert.True(second.AlreadyDone);
        Assert.Equal(0, second.RowsUpdated);
        Assert.Equal(0, second.CandidatesRead);
    }

    private static async Task<long> InsertActionAsync(NpgsqlConnection connection, string detail, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO collect.plan_force_actions (
    action_time, server_id, server_name, database_name, query_id, plan_id,
    action, mode, actor, decision, reasons,
    regression_factor, latest_cpu_per_exec_us, best_cpu_per_exec_us,
    replica_role, parameter_sensitivity_cofired, outcome, detail, related_action_id)
VALUES (
    $1, -444450, 'darling-4346-scrub-e2e', 'appdb', 1, 1,
    'would_force', 'unattended', 'bot', 'withheld', '',
    0, 0, 0, NULL, false, 'blocked', $2, NULL)
RETURNING action_id", connection)
        { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue(DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Unspecified));
        cmd.Parameters.AddWithValue(detail);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string?> ReadRawDetailAsync(NpgsqlConnection connection, long actionId, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT detail FROM collect.plan_force_actions WHERE action_id = $1", connection)
        { CommandTimeout = 60 };
        cmd.Parameters.AddWithValue(actionId);
        return (await cmd.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task<string?> ScalarTextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        return (await cmd.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
