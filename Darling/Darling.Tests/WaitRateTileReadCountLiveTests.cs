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
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 hygiene, CI fix on PR #4185: <see cref="SqlServerStoreTileBehaviourTests"/>'s
/// <c>Waits_DetectAnomaliesAsync_ReadsWaitRateTileWindowSqlOnce_NotTwice</c> used to run <c>CREATE EXTENSION IF
/// NOT EXISTS pg_stat_statements</c> against the SHARED <c>live-postgres</c> store and skip only if that
/// statement failed. <c>CREATE EXTENSION IF NOT EXISTS</c> succeeds whether or not the library is in
/// <c>shared_preload_libraries</c> — only the LATER <c>pg_stat_statements_reset()</c> / view read throws 55000
/// — so the skip never fired on a rig that does not preload it. CI run 36082244687 failed the test with 55000
/// on job "Darling PG tests (2)", then failed the collection's own residue check (#1873): CI's <c>darling-pg</c>
/// job preloads only <c>timescaledb</c>, and the extension was left behind on a store the check expected
/// unchanged.
///
/// <para><b>Moved here</b>, its own scratch database (<c>#1776 own-store</c>): the skip now reads
/// <c>current_setting('shared_preload_libraries')</c> BEFORE creating anything, so a rig without the library
/// never runs <c>CREATE EXTENSION</c> at all, and whatever this test does create lives only in the scratch
/// database — dropped whole on dispose, so there is nothing left for the shared collection's residue check to
/// see either way.</para>
///
/// <para><b>The count is scoped to this database's <c>oid</c>.</b> <c>pg_stat_statements</c>' counters are
/// CLUSTER-wide, keyed by <c>dbid</c>, not private to one database — so a class running concurrently against a
/// different database on the same server could inflate this count, or a blind
/// <c>pg_stat_statements_reset()</c> could zero out another class's in-flight count out from under it. Both the
/// reset and the read filter on this scratch database's <c>oid</c>, so this test can run in parallel with every
/// other live class instead of needing <c>[Collection("live-postgres")]</c>.</para>
/// </summary>
public sealed class WaitRateTileReadCountLiveTests
{
    /* Wednesday 2026-01-14, 10:00 — matches SqlServerStoreTileBehaviourTests' anchor. No server_properties row
       is seeded, so keying is UTC. */
    private static readonly DateTime T = new(2026, 1, 14, 10, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// Seeds one hour of <c>wait_stats</c> (12 collections, 5-minute spacing) and runs
    /// <see cref="PgAnomalyDetector.DetectAnomaliesAsync"/> once, then sums <c>pg_stat_statements.calls</c> for
    /// the statement matched on a column-alias fragment unique to <c>WaitRateTileWindowSql</c> (no other
    /// statement in the codebase aliases a column <c>peak_ms_per_sec</c>). The doubled read this pins against
    /// has no observable effect on the fired fact (both reads always returned identical rows), so only the
    /// store round-trip COUNT can tell the fixed shape from the regressed one. One hour is enough: the doubled
    /// read used to fire unconditionally once <c>whole.Samples &gt; 0</c>, before the baseline is even
    /// fetched, so no 21-day baseline history is needed here.
    /// </summary>
    [Fact]
    public async Task Waits_DetectAnomaliesAsync_ReadsWaitRateTileWindowSqlOnce_NotTwice()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;

        /* THE FIX: skip BEFORE creating anything. shared_preload_libraries is a postmaster-level, cluster-wide
           setting, so it reads the same whichever database asks — checked on the server connection, before a
           scratch database even exists. */
        await using (var probe = new NpgsqlConnection(baseConnectionString))
        {
            await probe.OpenAsync(ct);
            await using var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe);
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            var loaded = preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements");
            Assert.SkipWhen(!loaded,
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}') - this rig's postgresql.conf must carry it in shared_preload_libraries to run this pin.");
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live tile-behaviour test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* #3893: collection_log too, as the worker's "collection_log hypertable" step does before its aggregate
           ensure (pinned in StoreObjectConvergenceTests) — same order IntervalHonestHourlyRollupLiveTests uses. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Created only inside the scratch database, now that the preload check above has already confirmed
           the library will load. */
        using (var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", connection))
        {
            await extCmd.ExecuteNonQueryAsync(ct);
        }

        var dbId = await ScratchDatabaseOidAsync(connection, scratch.DatabaseName, ct);

        const int serverId = -3653_08;
        const string serverName = "sqlstore-tile-waits-readcount";
        const string waitType = "TILE_TEST_WAIT";
        const string insertWait =
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        var id = 991_000L;
        for (var i = 0; i < 12; i++)
        {
            var totalMs = (i % 3) switch { 0 => 30000L, 1 => 60000L, _ => 90000L };
            await InsertAsync(connection, insertWait, id++, TruncateToSeconds(T.AddMinutes(5 * i)), serverId, serverName, waitType, 10L, totalMs);
        }

        /* Reset only THIS database's entries (userid 0 = any, dbid = this scratch database, queryid 0 = any):
           pg_stat_statements' counters are cluster-wide, and other test classes run against other scratch
           databases in parallel. A blind pg_stat_statements_reset() would zero their in-flight counts too. */
        using (var resetCmd = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", connection))
        {
            resetCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = dbId });
            await resetCmd.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new PgBaselineProvider(postgres);
        var detector = new PgAnomalyDetector(postgres, provider);
        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = serverName,
            TimeRangeStart = T,
            TimeRangeEnd = T.AddHours(1),
            ServerUtcOffset = TimeSpan.Zero
        };

        await detector.DetectAnomaliesAsync(context);

        long calls;
        using (var countCmd = new NpgsqlCommand(
            "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE '%peak_ms_per_sec%' AND query LIKE '%v_wait_stats%'",
            connection))
        {
            countCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = dbId });
            calls = (long)(await countCmd.ExecuteScalarAsync(ct))!;
        }

        Assert.Equal(1, calls);
    }

    /// <summary>The scratch database's own <c>oid</c>, read by name — the key both the reset and the read
    /// filter <c>pg_stat_statements</c> on, since its counters are cluster-wide rather than per-database.</summary>
    private static async Task<uint> ScratchDatabaseOidAsync(NpgsqlConnection connection, string databaseName, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand("SELECT oid FROM pg_database WHERE datname = $1", connection);
        cmd.Parameters.AddWithValue(databaseName);
        return (uint)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static DateTime TruncateToSeconds(DateTime dt) =>
        DateTime.SpecifyKind(new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Kind), DateTimeKind.Unspecified);

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
            command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
