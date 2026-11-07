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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which hourly aggregates are built from another hourly aggregate on the phase grid, recovered from the shipped
/// CREATE text (<c>FROM collect.&lt;relation&gt;</c>) rather than from a hand list, so a hierarchical hourly
/// registered tomorrow is covered the moment it joins <see cref="TimescaleSupport.HourlyRefreshDefinitions"/>.
/// Shared by the grid pins in this file, TimescaleContinuousAggregateTests and TimescaleSupportTests.
/// </summary>
internal static class HourlyRefreshLineage
{
    /// <summary>The relation a CREATE selects <c>FROM</c> inside <c>collect</c>, or null when it names none.</summary>
    public static string? SourceOf(string createSql)
    {
        var match = Regex.Match(createSql, @"FROM\s+collect\.(\w+)", RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value : null;
    }

    /// <summary>Every hourly view on the grid paired with its source, where that source is itself an hourly view
    /// on the grid — the pairs whose refresh order inside the hour is load-bearing (#8053).</summary>
    public static IReadOnlyList<(string Child, string Parent)> HierarchicalPairs()
    {
        var views = new HashSet<string>(TimescaleSupport.HourlyRefreshDefinitions.Select(d => d.View), StringComparer.Ordinal);
        return TimescaleSupport.HourlyRefreshDefinitions
            .Select(d => (Child: d.View, Parent: SourceOf(d.CreateSql)))
            .Where(p => p.Parent is not null && views.Contains(p.Parent))
            .Select(p => (p.Child, p.Parent!))
            .ToArray();
    }

    /// <summary>The hourly views on the grid that are NOT built from another hourly view — the ones a light-band
    /// claim is about.</summary>
    public static bool IsHierarchical(string view)
        => HierarchicalPairs().Any(p => string.Equals(p.Child, view, StringComparison.Ordinal));
}

/// <summary>
/// #8053: an hourly continuous aggregate built FROM another hourly aggregate sees only what its parent has
/// materialized, and every hourly refresh policy is on a fixed minute of the hour, so the order inside the hour
/// is the product's to choose. A child whose minute comes before its parent's materializes the newest complete
/// bucket empty, is invalidated when the parent writes it, and materializes it again an hour later: every bucket
/// an hour late, every bucket twice. The pin here is derived from the shipped CREATE text, so it covers every such
/// pair rather than the one that exists today.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. */
public sealed class HourlyChildRefreshOrderTests
{
    /// <summary>
    /// Every hourly built from an hourly on the grid starts after its source's slot AND after its source's band:
    /// the whole window when the source is <see cref="TimescaleSupport.HeaviestHourlyRefreshView"/>, the guard
    /// otherwise. Positive control: the corrected Query Store hourly is built from the interval hourly, so the
    /// sweep has at least that pair to judge.
    /// </summary>
    [Fact]
    public void EveryHourlyBuiltFromAnHourly_StartsAfterItsSourcesSlotAndWindow()
    {
        var pairs = HourlyRefreshLineage.HierarchicalPairs();

        Assert.Contains(
            (TimescaleSupport.QueryStoreStatsCorrectedHourlyView, TimescaleSupport.QueryStoreStatsIntervalHourlyView),
            pairs);

        foreach (var (child, parent) in pairs)
        {
            var childMinute = TimescaleSupport.RefreshPhaseMinutesFor(child);
            var parentMinute = TimescaleSupport.RefreshPhaseMinutesFor(parent);
            var parentBand = string.Equals(parent, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal)
                ? TimescaleSupport.HeaviestRefreshWindowMinutes
                : TimescaleSupport.CompressionPhaseGuardMinutes;

            Assert.True(
                childMinute > parentMinute,
                $"{child} refreshes at :{childMinute.ToString("00", CultureInfo.InvariantCulture)}, before its source {parent} at "
                + $":{parentMinute.ToString("00", CultureInfo.InvariantCulture)} — every bucket reaches it an hour late and is materialized twice (#8053)");
            Assert.True(
                childMinute >= parentMinute + parentBand,
                $"{child} at :{childMinute.ToString("00", CultureInfo.InvariantCulture)} starts inside its source's {parentBand}-minute band from "
                + $":{parentMinute.ToString("00", CultureInfo.InvariantCulture)}, so it can read the parent before the parent's refresh has committed");
        }
    }

    private const int ServerId = -805301;
    private const string ServerName = "hourly-order";

    private static readonly string Parent = TimescaleSupport.QueryStoreStatsIntervalHourlyView;
    private static readonly string Child = TimescaleSupport.QueryStoreStatsCorrectedHourlyView;

    /// <summary>
    /// The jobs as the product creates them, run in the order their minutes put them in each hour. One raw row
    /// two hours back is inside both policies' windows; after one pass over the pair in minute order the parent
    /// holds its bucket and so does the child — it does not wait for the next hour's pass.
    /// </summary>
    [Fact]
    public async Task TheChild_HoldsTheBucket_OnTheFirstPassOverThePairInMinuteOrder()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8053 order test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8053 tests need TimescaleDB.");

            var parentJob = await JobAsync(connection, Parent, ct);
            var childJob = await JobAsync(connection, Child, ct);
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Parent), parentJob.Minute);
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Child), childJob.Minute);
            Assert.True(parentJob.FixedSchedule && childJob.FixedSchedule, "both hourly policies are on a fixed schedule");

            await InsertRowAsync(connection, ct);

            // The hour's order: whichever of the pair has the earlier minute runs first.
            foreach (var job in new[] { parentJob, childJob }.OrderBy(j => j.Minute))
            {
                await RunJobAsync(connection, job.JobId, ct);
            }

            Assert.Equal(1L, await CountAsync(connection, Parent, ct));
            Assert.True(
                await CountAsync(connection, Child, ct) == 1L,
                $"{Child} (:{childJob.Minute.ToString("00", CultureInfo.InvariantCulture)}) ran before {Parent} "
                + $"(:{parentJob.Minute.ToString("00", CultureInfo.InvariantCulture)}) and materialized the bucket empty — it reaches the child only on the next hour's pass (#8053)");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A store made on earlier code carries the pair on the earlier minutes, the child before the parent. The
    /// startup converge moves both onto the shipped grid, where the child is later, a second pass issues nothing,
    /// and the ensure that follows finds matching policies.
    /// </summary>
    [Fact]
    public async Task TheConverge_MovesAnOldChildMinute_AfterItsParents()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8053 converge test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            Assert.SkipWhen(!await PrepareAsync(connection, scratch.ConnectionString, ct), "The live #8053 tests need TimescaleDB.");

            // The earlier grid: the child on :04, the parent on :17.
            await SetMinuteAsync(connection, (await JobAsync(connection, Child, ct)).JobId, 4, ct);
            await SetMinuteAsync(connection, (await JobAsync(connection, Parent, ct)).JobId, 17, ct);
            Assert.Equal(4, (await JobAsync(connection, Child, ct)).Minute);
            Assert.Equal(17, (await JobAsync(connection, Parent, ct)).Minute);

            var log = new CapturingTestLogger();
            await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, log, ct);

            var child = await JobAsync(connection, Child, ct);
            var parent = await JobAsync(connection, Parent, ct);
            Assert.True(
                child.Minute > parent.Minute,
                $"after the converge {Child} is on :{child.Minute.ToString("00", CultureInfo.InvariantCulture)} and {Parent} on "
                + $":{parent.Minute.ToString("00", CultureInfo.InvariantCulture)} — the child still runs before its source (#8053)");
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Child), child.Minute);
            Assert.Equal(TimescaleSupport.RefreshPhaseMinutesFor(Parent), parent.Minute);
            Assert.True(child.FixedSchedule && parent.FixedSchedule);

            // Second pass: converged, nothing moves.
            Assert.Equal(0, await TimescaleSupport.ConvergeContinuousAggregateRefreshAsync(connection, new CapturingTestLogger(), ct));

            var ensureLog = new CapturingTestLogger();
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, ensureLog, ct);
            Assert.DoesNotContain("setup failed", ensureLog.Joined, StringComparison.Ordinal);
            Assert.Equal(child.Minute, (await JobAsync(connection, Child, ct)).Minute);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Migrates, enables TimescaleDB, builds every aggregate with its real policy, then stops the scheduler
    /// so only the explicit <c>run_job</c> calls in the tests run. False when TimescaleDB is not available.</summary>
    private static async Task<bool> PrepareAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, null, ct);

        if (!await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            return false;
        }

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return true;
    }

    /// <summary>One raw row two hours back: its bucket is complete and inside both policies' windows.</summary>
    private static async Task InsertRowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT 805301, t, $1::int, $2::text, 'OrderDb', 100, 1000,
    'Regular', t, 'mod', md5('order'), 10, 500, 300, 900, 700, 'PRIMARY', 1, date_trunc('hour', t)
FROM (SELECT (now() AT TIME ZONE 'UTC') - interval '2 hours' AS t) AS s", connection);
        seed.Parameters.AddWithValue(ServerId);
        seed.Parameters.AddWithValue(ServerName);
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunJobAsync(NpgsqlConnection connection, int jobId, CancellationToken ct)
    {
        await using var run = new NpgsqlCommand("CALL run_job($1::integer)", connection);
        run.Parameters.AddWithValue(jobId);
        await run.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($"SELECT count(*) FROM collect.{view} WHERE server_id = $1", connection);
        read.Parameters.AddWithValue(ServerId);
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task SetMinuteAsync(NpgsqlConnection connection, int jobId, int minute, CancellationToken ct)
    {
        await using var alter = new NpgsqlCommand(@"
SELECT alter_job(j.job_id, fixed_schedule => true,
    initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + ($2::int * INTERVAL '1 minute'))
FROM timescaledb_information.jobs AS j WHERE j.job_id = $1::integer", connection);
        alter.Parameters.AddWithValue(jobId);
        alter.Parameters.AddWithValue(minute);
        await alter.ExecuteNonQueryAsync(ct);
    }

    private sealed record JobState(int JobId, int Minute, bool FixedSchedule);

    /// <summary>The view's refresh job, matched on either identity the jobs view may report (the user view or its
    /// materialization), the way the product's own converge joins it.</summary>
    private static async Task<JobState> JobAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(@"
SELECT j.job_id, EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int, j.fixed_schedule
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  (ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name)
  OR  (ca.materialization_hypertable_schema = j.hypertable_schema AND ca.materialization_hypertable_name = j.hypertable_name)
WHERE j.proc_name = 'policy_refresh_continuous_aggregate' AND ca.view_schema = 'collect' AND ca.view_name = $1", connection);
        read.Parameters.AddWithValue(view);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{view} has no refresh policy job");
        return new JobState(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetInt32(1), reader.GetBoolean(2));
    }
}
