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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V150 (#4469, #4477): two supporting indexes, <c>idx_collection_log_watermark</c> on
/// <c>collect.collection_log (server_id, collector_name, collection_time DESC)</c> for the per-collector
/// watermark lookup, and <c>idx_job_history_server_run</c> on <c>collect.job_history (server_id,
/// run_datetime DESC, instance_id DESC)</c> for the Viewer's Job History tab. This file is the RUNG
/// (ladder, viewer probe) and the schema-after-migrate proof: both indexes exist, plain
/// <c>CREATE INDEX IF NOT EXISTS</c>, idempotent on rerun.
///
/// <para>This file's "I am the top rung" claim moved to <c>AgGroupIdRungTests</c> (V151) now that V151
/// has landed; this file's own rung/probe facts below keep asserting what stays true forever (present,
/// in-order, gated behind the arm above it) rather than "is exactly the top".</para>
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectionLogWatermarkAndJobHistoryIndexesRungTests
{
    private const int RungVersion = 150;
    private const int PreviousVersion = 149;

    /// <summary>This rung's sentinel ordinal in the viewer probe — no longer the newest, since V151 landed
    /// above it.</summary>
    private const int ProbeOrdinal = 125;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered, and the ladder stays dense above the historical gap — the claim this class
    /// took over from <c>QueryStoreLivenessHotTouchLiveTests</c> (V149) moved on again to
    /// <c>AgGroupIdRungTests</c> (V151) now that V151 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegistered_AndTheLadderIsDenseAboveTheHistoricalGap()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-log-watermark-and-job-history-indexes", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as an arm gated below the
    /// current top's arm — a missing arm maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndTheArmSitsBelowTheCurrentTop()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("idx_collection_log_watermark", probe, StringComparison.Ordinal);
        Assert.Contains("idx_job_history_server_run", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal("hasCollectionLogWatermarkAndJobHistoryIndexes", method.GetParameters()[ProbeOrdinal].Name);

        /* Every rung above this one (V151's hasAgGroupId) must also be false, or the map finds the newer
           arm first and this assertion is checking the wrong rung's fallthrough. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        var behind = (object[])all.Clone();
        for (var i = ProbeOrdinal; i < arity; i++)
        {
            behind[i] = false;
        }
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* V151 (#4475) is now the top rung, so this arm no longer needs to be the LAST one — it only has to
           sit below the current top's arm, which is what the ladder-dense invariant above already
           guarantees is registered ahead of it. */
        var thisArm = viewer.IndexOf("if (hasCollectionLogWatermarkAndJobHistoryIndexes)", StringComparison.Ordinal);
        var topArm = viewer.IndexOf("if (hasAgGroupId)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V150 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(topArm >= 0 && topArm < thisArm, "the current top rung's arm must sit above the V150 arm");
        Assert.Contains(
            "return " + RungVersion.ToString(System.Globalization.CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..(viewer.IndexOf("if (hasHotLivenessTouch)", StringComparison.Ordinal))], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: both indexes exist. Run against <c>origin/dev</c> (pre-V150) this is
    /// RED — neither index exists — proving the pin actually checks the rung rather than a tautology.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_BothIndexesExist()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V150 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await IndexExistsAsync(connection, "idx_collection_log_watermark", ct),
            "V150 creates idx_collection_log_watermark");
        Assert.True(await IndexExistsAsync(connection, "idx_job_history_server_run", ct),
            "V150 creates idx_job_history_server_run");
    }

    /// <summary>
    /// Rerunning the migration ladder (as startup does on an already-migrated store) is idempotent: both
    /// <c>CREATE INDEX IF NOT EXISTS</c> statements do not error and the indexes still exist.
    /// </summary>
    [Fact]
    public async Task MigrateAsync_RunTwice_IsIdempotent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V150 idempotency pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await IndexExistsAsync(connection, "idx_collection_log_watermark", ct));
        Assert.True(await IndexExistsAsync(connection, "idx_job_history_server_run", ct));
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1)", connection);
        command.Parameters.AddWithValue(indexName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
