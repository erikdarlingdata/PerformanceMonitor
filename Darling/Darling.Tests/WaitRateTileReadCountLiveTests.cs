/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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
/// on job "Darling PG tests (2)", then failed the collection's own residue check (#1873). It moved into its own
/// scratch database (<c>#1776 own-store</c>), where whatever it creates is dropped whole on dispose.
///
/// <para><b>The count no longer goes through <c>pg_stat_statements</c> (PR #4681's full-suite run, after PR
/// #4691).</b> The test passed alone and failed once in a full-suite run, and nobody recorded the count it read.
/// <c>pg_stat_statements</c> is the wrong instrument for a per-test count: it is ONE cluster-wide table that holds
/// <c>pg_stat_statements.max</c> = 5000 entries, and every parallel class that replays the migrations into its own
/// scratch database (utility tracking on, so every DDL statement is an entry) refills it within seconds. Each time
/// it is full it evicts the 250 lowest-usage entries, oldest once-called statements first. Measured on a
/// PostgreSQL 18.6 rig under the heavy scratch-database classes: about three evictions a second
/// (<c>pg_stat_statements_info.dealloc</c> 326 to 370 in a 15 s pause), and a window entry that was present with
/// <c>calls</c> = 1 right after the detector was gone 15 s later, so the count read 0 with the seed intact and
/// no NULL query text. Any stall between the detector's read and the count (a starved thread pool, a GC pause on
/// a loaded runner) is enough. Evicting the entry says nothing about how many times the product read the window,
/// so the pin could fail without the product being wrong, and it could never fail the other way.</para>
///
/// <para><b>The count is taken in this process instead</b>, from the <c>Npgsql</c> <see cref="ActivitySource"/>,
/// on which Npgsql opens one <see cref="Activity"/> per command. Nothing shared is read, so nothing another class
/// does can change it. Two things keep it exact. First, the listener counts only the commands run against THIS scratch
/// database (its name is unique to the test) whose text carries the window SQL's fragments, so parallel classes
/// running the same statement against their own databases are not counted. Second, every read on the path goes
/// through the test's own <see cref="NpgsqlDataSource"/>: <c>PgAnomalyDetector</c> and <c>PgBaselineProvider</c>
/// open a connection only through the injected data source (<c>_postgres.OpenConnectionAsync</c>, and neither
/// file calls <c>NpgsqlDataSource.Create</c>, <c>NpgsqlDataSourceBuilder</c> or <c>new NpgsqlConnection</c>). The
/// database-name filter does not even rely on that, since it would count a read through a second data source
/// too. A control command through the same data source proves the listener sees the database name and the text
/// before the count is trusted, so a renamed tag reads as a loud failure rather than a silent 0.</para>
/// </summary>
public sealed class WaitRateTileReadCountLiveTests
{
    /* Wednesday 2026-01-14, 10:00 — matches SqlServerStoreTileBehaviourTests' anchor. No server_properties row
       is seeded, so keying is UTC. */
    private static readonly DateTime T = new(2026, 1, 14, 10, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// Seeds one hour of <c>wait_stats</c> (12 collections, 5-minute spacing) and runs
    /// <see cref="PgAnomalyDetector.DetectAnomaliesAsync"/> once, then counts the commands Npgsql ran against the
    /// scratch database whose text carries the column-alias fragment unique to <c>WaitRateTileWindowSql</c> (no
    /// other statement in the codebase aliases a column <c>peak_ms_per_sec</c>). The doubled read this pins
    /// against has no observable effect on the fired fact (both reads always returned identical rows), so only the
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

        /* Both counters are process-wide listeners on Npgsql's ActivitySource, scoped to this scratch database
           by name, so a class running in parallel against another database is invisible to them. */
        using var windowReads = new NpgsqlCommandCounter(scratch.DatabaseName, "peak_ms_per_sec", "v_wait_stats");
        using var controlReads = new NpgsqlCommandCounter(scratch.DatabaseName, "wait_rate_read_count_control");

        /* The control: one command through the SAME data source the detector reads through. If the listener
           cannot see the database name or the command text (a renamed Npgsql tag), this fails here with a
           reason, instead of the count below reading a silent 0. */
        await using (var control = postgres.CreateCommand("SELECT 1 AS wait_rate_read_count_control"))
        {
            await control.ExecuteScalarAsync(ct);
        }
        Assert.True(controlReads.Count == 1,
            $"The Npgsql activity listener counted {controlReads.Count} control commands on '{scratch.DatabaseName}' instead of 1 - it cannot see the database name or the command text, so its window-read count would be meaningless.");

        await detector.DetectAnomaliesAsync(context);

        long calls = windowReads.Count;

        Assert.Equal(1, calls);
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

    /// <summary>
    /// Counts the commands Npgsql runs against ONE database whose text contains every one of a set of fragments.
    /// Npgsql opens an <see cref="Activity"/> per command on its <c>Npgsql</c> <see cref="ActivitySource"/> once a
    /// listener samples it, tagged with the database name and the command text; the count is taken when the
    /// activity stops, which is when the command's reader closes. The tags are matched by VALUE rather than by
    /// name (the same way <c>SharedBaselineCacheTests.CaptureAsync</c> finds the command text), because the
    /// OpenTelemetry attribute names Npgsql uses have changed between major versions.
    /// </summary>
    private sealed class NpgsqlCommandCounter : IDisposable
    {
        private readonly string _databaseName;
        private readonly string[] _fragments;
        private readonly ActivityListener _listener;
        private long _count;

        public NpgsqlCommandCounter(string databaseName, params string[] fragments)
        {
            _databaseName = databaseName;
            _fragments = fragments;
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "Npgsql",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = OnStopped,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public long Count => Interlocked.Read(ref _count);

        private void OnStopped(Activity activity)
        {
            var onThisDatabase = false;
            var carriesTheText = false;
            foreach (var tag in activity.TagObjects)
            {
                if (tag.Value is not string value)
                    continue;
                if (string.Equals(value, _databaseName, StringComparison.Ordinal))
                    onThisDatabase = true;
                else if (_fragments.All(fragment => value.Contains(fragment, StringComparison.Ordinal)))
                    carriesTheText = true;
            }
            if (onThisDatabase && carriesTheText)
                Interlocked.Increment(ref _count);
        }

        public void Dispose() => _listener.Dispose();
    }
}
