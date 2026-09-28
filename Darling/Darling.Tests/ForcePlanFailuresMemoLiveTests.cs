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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4659: the forced-plan failure read runs its two-hour aggregate only when the server's newest collection
/// has changed; otherwise the previous answer is reused, less any plan whose older collection left the window.
/// </summary>
[Collection("live-postgres")]
public sealed class ForcePlanFailuresMemoLiveTests
{
    private const int ServerId = -465900;

    private static ForcePlanFailureInfo Info(long plan) => new() { DatabaseName = "d", QueryId = 1, PlanId = plan };

    private static DateTime At(int minute) =>
        DateTime.SpecifyKind(new DateTime(2026, 1, 1, 12, 0, 0).AddMinutes(minute), DateTimeKind.Unspecified);

    [Fact]
    public void ReuseForWindow_KeepsRowsWhosePriorIsAfterTheStart_DropsEqualAndOlder_AndKeepsOrder()
    {
        var rows = new List<(ForcePlanFailureInfo Info, DateTime PriorObservedAt)>
        {
            (Info(3), At(11)),
            (Info(1), At(10)),
            (Info(2), At(9)),
            (Info(4), At(20)),
        };

        var kept = DarlingAlertReadAdapter.ReuseForWindow(rows, At(10));

        Assert.Equal(new long[] { 3, 4 }, kept.Select(k => k.PlanId).ToArray());
    }

    [Fact]
    public void ReuseForWindow_AnEmptyListStaysEmpty()
    {
        Assert.Empty(DarlingAlertReadAdapter.ReuseForWindow(
            new List<(ForcePlanFailureInfo Info, DateTime PriorObservedAt)>(), At(0)));
    }

    [Fact]
    public async Task TheMemo_SkipsTheFullRead_UntilANewCollectionLands_AndMatchesTheDirectRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4659 memo live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var now = DateTime.UtcNow;
        var t0 = DateTime.SpecifyKind(new DateTime(now.Ticks - (now.Ticks % 10)), DateTimeKind.Unspecified).AddMinutes(-30);
        await SeedAsync(connection, t0, 10, 3, 0, ct);
        await SeedAsync(connection, t0.AddMinutes(5), 10, 5, 0, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var adapter = new DarlingAlertReadAdapter(postgres);
        var key = ServerId.ToString(CultureInfo.InvariantCulture);

        var first = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(1, adapter.ForcePlanFailuresFullReads);
        var forced = Assert.Single(first);
        Assert.Equal(10L, forced.PlanId);
        Assert.Equal(2L, forced.FailureDelta);
        Assert.Equal(await DirectAsync(connection, ct), first.Select(f => (f.PlanId, f.FailureDelta, f.TotalFailures)).ToList());

        var second = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(1, adapter.ForcePlanFailuresFullReads);
        Assert.Equal(first.Select(f => (f.PlanId, f.FailureDelta, f.ObservedAtUtc)), second.Select(f => (f.PlanId, f.FailureDelta, f.ObservedAtUtc)));
        Assert.Equal(await DirectAsync(connection, ct), second.Select(f => (f.PlanId, f.FailureDelta, f.TotalFailures)).ToList());

        /* A newer collection where the counter no longer rises: the answer changes, so the full read must run. */
        await SeedAsync(connection, t0.AddMinutes(10), 10, 5, 0, ct);
        var third = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(2, adapter.ForcePlanFailuresFullReads);
        Assert.Empty(third);
        Assert.Equal(await DirectAsync(connection, ct), third.Select(f => (f.PlanId, f.FailureDelta, f.TotalFailures)).ToList());
    }

    [Fact]
    public async Task ABackdatedCollection_LeavesTheMemoStale_UntilTheBackfillInvalidatesIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4659 invalidation live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var now = DateTime.UtcNow;
        var t0 = DateTime.SpecifyKind(new DateTime(now.Ticks - (now.Ticks % 10)), DateTimeKind.Unspecified).AddMinutes(-30);
        await SeedAsync(connection, t0, 10, 3, 0, ct);
        await SeedAsync(connection, t0.AddMinutes(10), 10, 5, 0, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var adapter = new DarlingAlertReadAdapter(postgres);
        var key = ServerId.ToString(CultureInfo.InvariantCulture);

        var first = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(2L, Assert.Single(first).FailureDelta);

        /* A backdated collection between the two: it becomes the older sighting (4 -> 5 is a delta of 1) and the
           newest collection does not move, so the probe alone cannot see it. */
        await SeedAsync(connection, t0.AddMinutes(5), 10, 4, 0, ct);
        var direct = await DirectAsync(connection, ct);
        Assert.Equal(1L, Assert.Single(direct).FailureDelta);

        var stale = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(1, adapter.ForcePlanFailuresFullReads);
        Assert.NotEqual(direct, stale.Select(f => (f.PlanId, f.FailureDelta, f.TotalFailures)).ToList());

        adapter.InvalidateForcePlanFailures(ServerId);
        var fresh = await adapter.GetForcePlanFailuresAsync(key, ct);
        Assert.Equal(2, adapter.ForcePlanFailuresFullReads);
        Assert.Equal(direct, fresh.Select(f => (f.PlanId, f.FailureDelta, f.TotalFailures)).ToList());
    }

    private static async Task<List<(long PlanId, long FailureDelta, long TotalFailures)>> DirectAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        var result = new List<(long, long, long)>();
        using var command = new NpgsqlCommand(DarlingAlertReadAdapter.ForcePlanFailuresSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified) - DarlingAlertReadAdapter.ForcePlanFailureWindow);
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add((reader.GetInt64(2), reader.GetInt64(5), reader.GetInt64(6)));
        }

        return result;
    }

    /// <summary>Forced plan 10 carrying <paramref name="failures"/> plus plain plan 20 at one collection.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime collectionTime, long plan, long failures, int unused, System.Threading.CancellationToken ct)
    {
        _ = unused;
        using var insert = new NpgsqlCommand(
            "INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, " +
            "execution_count, avg_duration_us, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason) " +
            "VALUES (1, $1, $2, 'memo-live', 'ForcedDb', 1, $3, 5, 900, 'MANUAL', TRUE, $4, 'GENERAL_FAILURE'), " +
            "(1, $1, $2, 'memo-live', 'PlainDb', 2, 20, 5, 900, 'NONE', FALSE, 0, NULL)", connection);
        insert.Parameters.AddWithValue(collectionTime);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(plan);
        insert.Parameters.AddWithValue(failures);
        await insert.ExecuteNonQueryAsync(ct);
    }
}
