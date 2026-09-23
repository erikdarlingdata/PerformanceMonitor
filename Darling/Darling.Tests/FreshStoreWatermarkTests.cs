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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3973: the fleet overview answers on a store whose <c>collection_health_hourly</c> aggregate has never
/// refreshed. That is every fresh TimescaleDB store until the aggregate's first policy run.
///
/// <para><b>The defect.</b> <see cref="DarlingFleetReader.CollectionHealthWatermarkSql"/> guarded the watermark
/// with <c>isfinite()</c> alone, on the assumption that "nothing materialized" reads <c>-infinity</c>. A
/// never-refreshed aggregate reports the minimum FINITE timestamp instead, 4714-11-24 BC, on TimescaleDB 2.28.1
/// and 2.30.1 alike. It passed the guard, Npgsql could not convert it to a <see cref="DateTime"/>, and
/// <c>get_fleet_overview</c> answered "Out of range of DateTime" while <c>/api/fleet</c> returned a bare 500.</para>
/// </summary>
public sealed class FreshStoreWatermarkTests
{
    /// <summary>
    /// The guard reads any watermark before the floor as nothing materialized, whichever sentinel the extension
    /// uses. The floor is a date no store can have collected before, so no real refresh can put the watermark
    /// below it, and it is well inside what a <see cref="DateTime"/> can hold.
    /// </summary>
    [Fact]
    public void TheWatermarkGuard_ReadsAnythingBeforeTheFloor_AsNothingMaterialized()
    {
        var sql = DarlingFleetReader.CollectionHealthWatermarkSql;

        Assert.Contains("isfinite(w)", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"w >= '{DarlingFleetReader.MaterializedWatermarkFloor:yyyy-MM-dd}'::timestamp",
            sql,
            StringComparison.Ordinal);
        Assert.Equal(new DateTime(2000, 1, 1), DarlingFleetReader.MaterializedWatermarkFloor);
        Assert.True(DarlingFleetReader.MaterializedWatermarkFloor > DateTime.MinValue);
    }

    /// <summary>
    /// And the guard's catch covers a value the client cannot convert, not only a server error: a guard read is
    /// allowed to fail, and a failed guard means the exact raw scan, never a failed overview. Pinned on the text
    /// because no guard read the SQL above lets through can produce one any more.
    /// </summary>
    [Fact]
    public void AGuardReadTheClientCannotConvert_FallsBackToTheRawScan()
    {
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");

        Assert.Contains("catch (Exception ex) when (ex is PostgresException or InvalidCastException)", reader, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG, TimescaleDB): the whole path on a store built the product's way. The aggregate is
/// created <c>WITH NO DATA</c> by <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/>, and its policy is
/// switched off so nothing refreshes behind the test (<see cref="CollectionHealthAggregateTests.OpenStoreAsync"/>).
/// The premise is measured rather than assumed, then the overview is read before and after the first refresh.
/// </summary>
/* #1776 own-store, like CollectionHealthAggregateTests: every test here creates and drops its own scratch database
   and never touches the shared one, so it is deliberately NOT in the "live-postgres" collection. */
public sealed class FreshStoreWatermarkLiveTests
{
    [Fact]
    public async Task TheOverview_AnswersBeforeTheFirstRefresh_AndAfterIt_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live #3973 test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;

        /* The premise: never refreshed, the extension's watermark is below the floor — and on the versions this
           was measured on it is FINITE, which is the value isfinite() alone let through. Read as text, because
           the point is that Npgsql cannot hold it as a DateTime. */
        var (finite, belowFloor, text) = await RawWatermarkAsync(connection, ct);
        Assert.True(belowFloor, $"a never-refreshed watermark read {text}, which is not below the floor");
        TestContext.Current.SendDiagnosticMessage($"#3973 never-refreshed watermark: {text} (isfinite = {finite})");

        /* Two servers the product would card, and three days of their collector runs. */
        foreach (var id in new[] { 1, 2 })
        {
            await ExecAsync(connection, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, $3)",
                id, "srv-" + id, DateTime.UtcNow.AddDays(-9));
        }

        await CollectionHealthAggregateTests.PlantAsync(connection, TimeSpan.FromDays(3), TimeSpan.FromMinutes(20), 20, 3_973_000, ct);

        /* The guard: nothing materialized reads as no watermark, and the composed read is usable, every bucket
           served real-time from raw. This read is what threw before the fix. */
        Assert.Null(await ScalarAsync(connection, DarlingFleetReader.CollectionHealthWatermarkSql, ct));

        var now = DateTime.UtcNow;
        var headEnd = DarlingFleetReader.CeilingHour(DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified));
        await using (var postgres = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            Assert.True(await DarlingFleetReader.CollectionHealthRollupUsableAsync(postgres, headEnd, ct));

            /* The surface #3973 was filed about: the overview answers, and both cards carry the collection-health
               half — three collectors each, read through the unmaterialized aggregate. */
            var fresh = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
            AssertThreeCollectorsEach(fresh);
        }

        /* After the first refresh the watermark is a real instant, and the same read keeps answering. A new data
           source, so the collection-health memo is not what answers. */
        await CollectionHealthAggregateTests.RunPolicyAsync(connection, jobId, ct);
        var mark = await ScalarAsync(connection, DarlingFleetReader.CollectionHealthWatermarkSql, ct);
        Assert.IsType<DateTime>(mark);
        Assert.True((DateTime)mark! > DarlingFleetReader.MaterializedWatermarkFloor);

        await using (var postgres = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var refreshed = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
            AssertThreeCollectorsEach(refreshed);
        }
    }

    /// <summary>
    /// The case the issue left unmeasured: a store with nothing in <c>collection_log</c> yet, whose aggregate's
    /// first policy run refreshes an empty window. Whatever that leaves the watermark at, the overview has to
    /// answer, and the guard has to hand the reader either nothing or a real instant.
    /// </summary>
    [Fact]
    public async Task ARefreshOverAnEmptyLog_StillLeavesTheOverviewAnswering_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live #3973 test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;

        await CollectionHealthAggregateTests.RunPolicyAsync(connection, jobId, ct);

        var (finite, belowFloor, text) = await RawWatermarkAsync(connection, ct);
        TestContext.Current.SendDiagnosticMessage($"#3973 watermark after a refresh over an empty log: {text} (isfinite = {finite}, below floor = {belowFloor})");

        var guarded = await ScalarAsync(connection, DarlingFleetReader.CollectionHealthWatermarkSql, ct);
        Assert.True(guarded is null || (DateTime)guarded > DarlingFleetReader.MaterializedWatermarkFloor);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var now = DateTime.UtcNow;
        var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
        Assert.Equal(0, result.TotalServers);
    }

    private static void AssertThreeCollectorsEach(FleetOverviewResult result)
    {
        var cards = result.Cards.Where(c => c.ServerId is 1 or 2).ToList();
        Assert.Equal(2, cards.Count);
        Assert.All(cards, c => Assert.Equal(3, c.CollectorCount));
    }

    /// <summary>The aggregate's watermark as the extension reports it, unguarded: whether it is finite, whether it
    /// is below the floor, and its text, which is the only way to hold a value a DateTime cannot.</summary>
    private static async Task<(bool Finite, bool BelowFloor, string Text)> RawWatermarkAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT isfinite(w), w < '{DarlingFleetReader.MaterializedWatermarkFloor:yyyy-MM-dd}'::timestamp, w::text
FROM (SELECT _timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(mat_hypertable_id)) AS w
      FROM _timescaledb_catalog.continuous_agg
      WHERE user_view_schema = 'collect'
      AND   user_view_name = '{TimescaleSupport.CollectionHealthHourlyView}') x", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the aggregate is not in the catalog");
        return (reader.GetBoolean(0), reader.GetBoolean(1), reader.GetString(2));
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
