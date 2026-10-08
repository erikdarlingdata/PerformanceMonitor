/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5551: the startup sweeps stop a continuous aggregate's jobs, and wait out a worker already running, before they
/// drop it. A refresh running during <c>DROP MATERIALIZED VIEW ... CASCADE</c> gave <c>XX000: tuple concurrently
/// deleted</c> (#5416) and is the inferred cause of a PostgreSQL backend crash in CI (#5549). These pins hold the SQL
/// text and the call order in the source; the live classes below prove the behaviour.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class AggregateJobQuiescePinTests
{
    [Fact]
    public void JobLookup_JoinsEitherJobIdentity_BindsTheViewName_AndFollowsDependentAggregates()
    {
        var sql = TimescaleSupport.ContinuousAggregateJobsSql;

        Assert.Contains("ca.view_schema = 'collect'", sql, StringComparison.Ordinal);
        Assert.Contains("ca.view_name = $1::text", sql, StringComparison.Ordinal);
        /* The view name is a parameter, never spliced into the text. */
        Assert.DoesNotContain("'{", sql, StringComparison.Ordinal);
        /* Both identities a job can report (the user view, as timescaledb_information.jobs resolves it, and the
           materialization hypertable) - measured, see ContinuousAggregateRefreshStateSql. */
        Assert.Contains("a.view_schema = j.hypertable_schema AND a.view_name = j.hypertable_name", sql, StringComparison.Ordinal);
        Assert.Contains("a.mat_schema = j.hypertable_schema AND a.mat_name = j.hypertable_name", sql, StringComparison.Ordinal);
        /* Every job TYPE of the aggregate (refresh, compression, retention): no proc_name filter. */
        Assert.DoesNotContain("proc_name", sql, StringComparison.Ordinal);
        /* A hierarchical daily tier is dropped by the CASCADE too, so its jobs are found through the parent's
           materialization hypertable, which is what its own hypertable_name reports. */
        Assert.Contains("WITH RECURSIVE", sql, StringComparison.Ordinal);
        Assert.Contains("child.hypertable_name = parent.mat_name", sql, StringComparison.Ordinal);
        Assert.Contains("j.scheduled", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Unschedule_NamesOnlyScheduled_AndReschedule_IsItsInverse_ByIntegerId()
    {
        Assert.Equal(
            "SELECT alter_job(j.job_id, scheduled => false) FROM timescaledb_information.jobs AS j WHERE j.job_id = ANY($1::integer[])",
            TimescaleSupport.UnscheduleJobsSql);
        Assert.Equal(
            "SELECT alter_job(j.job_id, scheduled => true) FROM timescaledb_information.jobs AS j WHERE j.job_id = ANY($1::integer[])",
            TimescaleSupport.RescheduleJobsSql);
    }

    [Fact]
    public void WorkerCount_MatchesTheJobIdSuffix_InThisDatabaseOnly()
    {
        var sql = TimescaleSupport.AggregateJobWorkerCountSql;

        Assert.Contains("pg_stat_activity", sql, StringComparison.Ordinal);
        Assert.Contains("a.datname = current_database()", sql, StringComparison.Ordinal);
        Assert.Contains("a.backend_type LIKE '% [' || id::text || ']'", sql, StringComparison.Ordinal);
        Assert.Contains("unnest($1::integer[])", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void InformationViewProbe_GuardsAStoreWithoutTimescaleDb()
    {
        Assert.Contains("to_regclass('timescaledb_information.continuous_aggregates')", TimescaleSupport.ContinuousAggregateJobsProbeSql, StringComparison.Ordinal);
        Assert.Contains("to_regclass('timescaledb_information.jobs')", TimescaleSupport.ContinuousAggregateJobsProbeSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProductionCap_IsThirtySeconds_AndFitsEverySweepRelationInsideTheHourlyBudget()
    {
        var options = TimescaleSupport.AggregateJobQuiesceOptions.Default;

        Assert.Equal(TimeSpan.FromSeconds(30), options.Cap);
        Assert.Equal(TimeSpan.FromMilliseconds(250), options.PollInterval);

        /* Eight relations at most (two superseded, two retired, four reshaped), all stuck: 8 x 30 s = 240 s, inside
           the hourly convergence pass's five-minute budget. */
        var relations = TimescaleSupport.SupersededBaselineRelations.Length + TimescaleSupport.RetiredBaselineRelations.Length + 4;
        Assert.True(relations * options.Cap <= TimeSpan.FromMinutes(5),
            $"{relations} relations x {options.Cap} must stay inside the hourly pass's budget");
    }

    /// <summary>
    /// The census: every product statement that drops a continuous aggregate runs after
    /// <c>QuiesceContinuousAggregateJobsAsync</c> in the same loop body, and a NEW drop site fails this test until
    /// it is added here and given the same treatment. Comments are skipped; SQL text and statements are counted.
    /// </summary>
    [Fact]
    public void EveryProductDropOfAContinuousAggregate_IsPrecededByTheQuiesce()
    {
        var root = RepoRoot();
        var productFiles = new[] { "PerformanceMonitor.Darling.Storage", "PerformanceMonitor.Darling.Service" }
            .SelectMany(dir => Directory.EnumerateFiles(Path.Combine(root, "Darling", dir), "*.cs", SearchOption.AllDirectories))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToArray();

        var sites = new List<(string File, int Line)>();
        foreach (var file in productFiles)
        {
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                var trimmed = lines[i].TrimStart();
                var isComment = trimmed.StartsWith("//", StringComparison.Ordinal) || trimmed.StartsWith("*", StringComparison.Ordinal)
                    || trimmed.StartsWith("/*", StringComparison.Ordinal);
                if (!isComment && lines[i].Contains("DROP MATERIALIZED VIEW", StringComparison.Ordinal))
                {
                    sites.Add((Path.GetFileName(file), i + 1));
                }
            }
        }

        /* Two statements: the SQL text DropRetiredBaselineRelationSql builds (run by BOTH the superseded and the
           retired pass), and the reshape's own drop. Anything else is a new drop of an aggregate. */
        Assert.True(sites.Count == 2 && sites.All(s => s.File == "TimescaleSupport.cs"),
            "a product drop of a continuous aggregate appeared (or moved) - give it QuiesceContinuousAggregateJobsAsync and add it here: "
            + string.Join(", ", sites.Select(s => $"{s.File}:{s.Line}")));

        var source = File.ReadAllText(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs")).Replace("\r\n", "\n");
        var runners = new[]
        {
            "new NpgsqlCommand(DropRetiredBaselineRelationSql(legacy), connection)",
            "new NpgsqlCommand(DropRetiredBaselineRelationSql(view), connection)",
            "new NpgsqlCommand($\"DROP MATERIALIZED VIEW IF EXISTS collect.{view} CASCADE\", connection)",
        };
        foreach (var runner in runners)
        {
            var at = source.IndexOf(runner, StringComparison.Ordinal);
            Assert.True(at > 0, $"the drop statement '{runner}' moved");
            Assert.Equal(at, source.LastIndexOf(runner, StringComparison.Ordinal));
            var window = source[Math.Max(0, at - 900)..at];
            Assert.Matches(@"QuiesceContinuousAggregateJobsAsync\(connection, \w+, logger, quiesce, cancellationToken\);\s+if \(hold\.Skip\)\s+\{\s+continue;\s+\}", window);
            /* ...and the drop is followed by the hold being cleared (the jobs went with the aggregate). */
            var after = source[at..Math.Min(source.Length, at + 400)];
            Assert.Contains("hold = AggregateJobHold.None;", after, StringComparison.Ordinal);
        }

        /* The three catches start the jobs again, after the reopen. */
        Assert.Equal(3, Regex.Matches(source,
            @"ReopenBrokenConnectionAsync\([^;]*\)\)\s+\{\s+return dropped;\s+\}\s+(/\*.*?\*/\s+)?await ResumeContinuousAggregateJobsAsync\(connection, hold\.Unscheduled, \w+, logger, cancellationToken\);",
            RegexOptions.Singleline).Count);
        Assert.Equal(3, Regex.Matches(source, @"catch \(OperationCanceledException\) when \(hold\.Unscheduled\.Count > 0\)").Count);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Directory.Build.props")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, "could not locate the repo root from the test source path");
        return dir!;
    }
}

/// <summary>
/// #5551, live: the sweeps against a real TimescaleDB scheduler.
///
/// <para><b>#1776 own-store</b>, not <c>[Collection("live-postgres")]</c>: the tests run a real job worker, hold a
/// session advisory lock the worker waits on, plant event triggers and read every job of the database, none of which
/// can share the <c>darlingtest</c> store with other classes' aggregates and policies.</para>
///
/// <para>The worker is made to run deterministically: the aggregate selects through an <c>IMMUTABLE</c>-labelled
/// function that takes a shared advisory lock, and the test holds the exclusive one, so the refresh job the scheduler
/// launches sits in <c>pg_stat_activity</c> as <c>Refresh Continuous Aggregate Policy [id]</c> until the test
/// lets it go. The scheduler is left running (nothing here calls <c>stop_background_workers</c>); every policy starts a
/// day out so none fires on its own.</para>
/// </summary>
public sealed class AggregateJobQuiesceLiveTests
{
    private const long GateKey = 5551;
    private const string Retired = "cpu_utilization_baseline";

    private static readonly TimescaleSupport.AggregateJobQuiesceOptions FastCap = new(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(100));

    /// <summary>(a) The jobs are stopped before the drop, and the drop still removes the aggregate and its jobs.</summary>
    [Fact]
    public async Task Sweep_StopsTheAggregatesJobsBeforeTheDrop_AndTheDropStillRemovesThemAll_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await CreateRetiredAggregateAsync(connection, Retired, ct);
        await PlantDropProbeAsync(connection, Retired, ct);

        var jobIds = await JobIdsAsync(connection, Retired, ct);
        Assert.Equal(3, jobIds.Count);
        Assert.All(await JobStatesAsync(connection, jobIds, ct), s => Assert.True(s.Value, "every job starts scheduled"));

        var log = new CapturingTestLogger();
        Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, log, DateTime.UtcNow, FastCap, ct));

        /* The probe ran at ddl_command_start of the DROP: every job was already stopped by then. */
        Assert.Equal(3L, await ScalarAsync<long>(connection, "SELECT count(DISTINCT job_id) FROM collect.w5551_seen", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.w5551_seen WHERE scheduled", ct));
        Assert.False(await RelationExistsAsync(connection, Retired, ct), "the aggregate is gone");
        Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_name = '{Retired}'", ct));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>
    /// (b) A running job worker holds the drop until it exits. Real worker: the gate keeps it inside the refresh.
    /// While it runs the jobs are stopped (the scheduler cannot start another), the aggregate is still there and the
    /// sweep is still waiting; once the worker exits the drop goes ahead, with no warning.
    /// </summary>
    [Fact]
    public async Task RunningJobWorker_HoldsTheDrop_UntilItExits_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await CreateRetiredAggregateAsync(connection, Retired, ct);
        var jobIds = await JobIdsAsync(connection, Retired, ct);
        var refreshJob = await RefreshJobIdAsync(connection, Retired, ct);

        await using var gate = new NpgsqlConnection(scratch.ConnectionString);
        await gate.OpenAsync(ct);
        await ExecuteAsync(gate, $"SELECT pg_advisory_lock({GateKey})", ct);
        await using var sweeper = new NpgsqlConnection(scratch.ConnectionString);
        await sweeper.OpenAsync(ct);
        Task<int>? sweep = null;
        var gateOpen = false;
        try
        {
            await StartRefreshWorkerAsync(connection, refreshJob, ct);
            Assert.True(await WaitForAsync(async () => await WorkerCountAsync(connection, jobIds, ct) > 0, TimeSpan.FromSeconds(60), ct),
                "the scheduler never started the refresh worker");

            var log = new CapturingTestLogger();
            sweep = Task.Run(() => TimescaleSupport.DropRetiredBaselineAggregatesAsync(
                sweeper, log, DateTime.UtcNow, new TimescaleSupport.AggregateJobQuiesceOptions(TimeSpan.FromSeconds(120), TimeSpan.FromMilliseconds(100)), ct), ct);

            var workerPid = await ScalarAsync<int>(connection, $"SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND backend_type LIKE '% [{refreshJob}]'", ct);

            /* The jobs are stopped while the worker runs... */
            Assert.True(await WaitForAsync(async () => (await JobStatesAsync(connection, jobIds, ct)).Values.All(scheduled => !scheduled), TimeSpan.FromSeconds(30), ct),
                $"the sweep never stopped the jobs: {log.Joined}");

            /* ...and for as long as THAT worker is alive the aggregate must stay and the sweep must keep waiting. The
               invariant is checked on every look (the relation is read first, so a drop that landed under a live worker
               cannot be missed by a later read), not only after a fixed delay, because TimescaleDB's own scheduler may
               end a worker whose job was just stopped: the sweep waits for the exit either way, and once the worker is
               gone nothing is left to hold it. Looked at for two seconds, the window in which an early drop would show. */
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
            var workerEndedOnItsOwn = false;
            while (DateTime.UtcNow < deadline)
            {
                var existsBefore = await RelationExistsAsync(connection, Retired, ct);
                var alive = await ScalarAsync<long>(connection, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {workerPid} AND backend_type LIKE '% [{refreshJob}]'", ct) > 0;
                Assert.False(!existsBefore && alive, $"the aggregate was dropped under a running job worker: {log.Joined}");
                if (!alive)
                {
                    workerEndedOnItsOwn = true;
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
            }

            if (!workerEndedOnItsOwn)
            {
                Assert.False(sweep.IsCompleted, $"the sweep finished while a worker was running: {log.Joined}");
                Assert.True(await RelationExistsAsync(connection, Retired, ct), "the aggregate must still exist while its worker runs");
            }

            await ExecuteAsync(gate, $"SELECT pg_advisory_unlock({GateKey})", ct);
            gateOpen = true;

            Assert.Equal(1, await sweep.WaitAsync(TimeSpan.FromSeconds(60), ct));
            Assert.False(await RelationExistsAsync(connection, Retired, ct), "the drop goes ahead once the worker has exited");
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            if (!gateOpen)
            {
                await ExecuteAsync(gate, $"SELECT pg_advisory_unlock({GateKey})", CancellationToken.None);
            }

            if (sweep is not null)
            {
                try { await sweep.WaitAsync(TimeSpan.FromSeconds(60), CancellationToken.None); }
                catch (Exception) { /* the assertions above already carry the failure */ }
            }
        }
    }

    /// <summary>
    /// (c) At the cap, with the worker still running: the jobs are scheduled again, the aggregate stays, nothing is
    /// cancelled, one warning names it; the next pass, with the worker gone, drops it.
    /// </summary>
    [Fact]
    public async Task AtTheCap_TheJobsAreScheduledAgain_TheAggregateStays_AndTheNextPassDropsIt_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await CreateRetiredAggregateAsync(connection, Retired, ct);
        var jobIds = await JobIdsAsync(connection, Retired, ct);
        var refreshJob = await RefreshJobIdAsync(connection, Retired, ct);

        await using var gate = new NpgsqlConnection(scratch.ConnectionString);
        await gate.OpenAsync(ct);
        await ExecuteAsync(gate, $"SELECT pg_advisory_lock({GateKey})", ct);
        var gateOpen = false;
        try
        {
            await StartRefreshWorkerAsync(connection, refreshJob, ct);
            Assert.True(await WaitForAsync(async () => await WorkerCountAsync(connection, jobIds, ct) > 0, TimeSpan.FromSeconds(60), ct),
                "the scheduler never started the refresh worker");
            var workerPid = await ScalarAsync<int>(connection, $"SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND backend_type LIKE '% [{refreshJob}]'", ct);

            var log = new CapturingTestLogger();
            Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, log, DateTime.UtcNow, FastCap, ct));

            Assert.True(await RelationExistsAsync(connection, Retired, ct), "the aggregate stays at the cap");
            Assert.All(await JobStatesAsync(connection, jobIds, ct), s => Assert.True(s.Value, "every job it stopped is scheduled again"));
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            Assert.Contains(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains(Retired, StringComparison.Ordinal) && l.Contains("#5551", StringComparison.Ordinal));
            /* Not cancelled, not terminated: the very same backend is still running. */
            Assert.Equal(1L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {workerPid} AND backend_type LIKE '% [{refreshJob}]'", ct));

            await ExecuteAsync(gate, $"SELECT pg_advisory_unlock({GateKey})", ct);
            gateOpen = true;
            Assert.True(await WaitForAsync(async () => await WorkerCountAsync(connection, jobIds, ct) == 0, TimeSpan.FromSeconds(60), ct), "the worker finishes once released");

            var retry = new CapturingTestLogger();
            Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, retry, DateTime.UtcNow, FastCap, ct));
            Assert.False(await RelationExistsAsync(connection, Retired, ct), "the next pass drops it");
            Assert.Equal(0, retry.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            if (!gateOpen)
            {
                await ExecuteAsync(gate, $"SELECT pg_advisory_unlock({GateKey})", CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// (d) and (e): a drop that fails (planted <c>XX000</c>, which also breaks the connection, as the real race does)
    /// schedules the jobs again after the reopen; a job that was already stopped before the sweep stays stopped.
    /// </summary>
    [Fact]
    public async Task FailedDrop_SchedulesTheJobsAgain_ButNeverOneThatWasAlreadyStopped_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await CreateRetiredAggregateAsync(connection, Retired, ct);
        var jobIds = await JobIdsAsync(connection, Retired, ct);
        var refreshJob = await RefreshJobIdAsync(connection, Retired, ct);

        /* (e) stopped by hand before the sweep. */
        await ExecuteAsync(connection, $"SELECT alter_job({refreshJob}, scheduled => false)", ct);
        await PlantFailingDropAsync(connection, Retired, ct);

        var log = new CapturingTestLogger();
        Assert.Equal(0, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, log, DateTime.UtcNow, FastCap, ct));

        Assert.True(connection.State == System.Data.ConnectionState.Open, $"the connection is {connection.State}: {log.Joined}");
        Assert.True(await RelationExistsAsync(connection, Retired, ct), "the failed drop leaves the aggregate");
        var states = await JobStatesAsync(connection, jobIds, ct);
        Assert.False(states[refreshJob], "the job that was stopped before the sweep stays stopped");
        Assert.All(states.Where(s => s.Key != refreshJob), s => Assert.True(s.Value, $"job {s.Key} (stopped by the sweep) is scheduled again"));
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        Assert.Contains(log.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains($"retired baseline relation {Retired}", StringComparison.Ordinal));
    }

    /// <summary>
    /// The reshape: the daily tier built on the aggregate is dropped by the CASCADE, so ITS jobs are stopped too
    /// before the drop; a failed drop schedules both tiers' jobs again; a clean drop removes both tiers.
    /// </summary>
    [Fact]
    public async Task Reshape_StopsTheDependentTiersJobsToo_AndSchedulesBothAgainWhenTheDropFails_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* The OLD query_store_stats_hourly shape (it still has a query_id column) with a daily tier on top. */
        await ExecuteAsync(connection, "CREATE TABLE collect.w5551_qs (collection_time timestamp NOT NULL, query_id bigint NOT NULL, v double precision NOT NULL)", ct);
        await ExecuteAsync(connection, "SELECT create_hypertable('collect.w5551_qs', by_range('collection_time', INTERVAL '1 day'))", ct);
        await ExecuteAsync(connection, @"
CREATE MATERIALIZED VIEW collect.query_store_stats_hourly WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT query_id, time_bucket('1 hour', collection_time) AS bucket, sum(v) AS s FROM collect.w5551_qs GROUP BY 1, 2 WITH NO DATA", ct);
        await ExecuteAsync(connection, @"
CREATE MATERIALIZED VIEW collect.query_store_stats_daily WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT query_id, time_bucket('1 day', bucket) AS bucket, sum(s) AS s FROM collect.query_store_stats_hourly GROUP BY 1, 2 WITH NO DATA", ct);
        await ExecuteAsync(connection, "SELECT add_continuous_aggregate_policy('collect.query_store_stats_hourly', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', initial_start => now() + INTERVAL '1 day')", ct);
        await ExecuteAsync(connection, "SELECT add_continuous_aggregate_policy('collect.query_store_stats_daily', start_offset => INTERVAL '30 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day', initial_start => now() + INTERVAL '1 day')", ct);

        var hourlyJobs = await JobIdsAsync(connection, "query_store_stats_hourly", ct);
        var dailyJobs = await JobIdsAsync(connection, "query_store_stats_daily", ct);
        Assert.Single(hourlyJobs);
        Assert.Single(dailyJobs);
        var both = hourlyJobs.Concat(dailyJobs).ToArray();

        /* The lookup for the hourly aggregate finds the daily tier's job as well. */
        await using (var find = new NpgsqlCommand(TimescaleSupport.ContinuousAggregateJobsSql, connection))
        {
            find.Parameters.AddWithValue("query_store_stats_hourly");
            await using var reader = await find.ExecuteReaderAsync(ct);
            var found = new List<int>();
            while (await reader.ReadAsync(ct))
            {
                found.Add(reader.GetInt32(0));
            }

            Assert.Equal(both.OrderBy(i => i), found);
        }

        /* A failed drop first: both tiers keep refreshing, and the connection works for the next view. */
        await PlantFailingDropAsync(connection, "query_store_stats_hourly", ct);
        var failedLog = new CapturingTestLogger();
        Assert.Equal(0, await TimescaleSupport.DropStaleContinuousAggregatesAsync(connection, failedLog, FastCap, ct));
        Assert.True(connection.State == System.Data.ConnectionState.Open, $"the connection is {connection.State}: {failedLog.Joined}");
        Assert.True(await RelationExistsAsync(connection, "query_store_stats_hourly", ct));
        Assert.True(await RelationExistsAsync(connection, "query_store_stats_daily", ct));
        Assert.All(await JobStatesAsync(connection, both, ct), s => Assert.True(s.Value, $"job {s.Key} is scheduled again"));
        Assert.Equal(1, failedLog.CountAtLevel(LogLevel.Warning));

        /* Then a clean drop, observed at ddl_command_start: both tiers' jobs were already stopped, and both go. */
        await ExecuteAsync(connection, "DROP EVENT TRIGGER w5551_break_drop", ct);
        await PlantDropProbeAsync(connection, "query_store_stats_hourly", ct);
        var log = new CapturingTestLogger();
        Assert.Equal(1, await TimescaleSupport.DropStaleContinuousAggregatesAsync(connection, log, FastCap, ct));
        Assert.Equal(2L, await ScalarAsync<long>(connection, "SELECT count(DISTINCT job_id) FROM collect.w5551_seen", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.w5551_seen WHERE scheduled", ct));
        Assert.False(await RelationExistsAsync(connection, "query_store_stats_hourly", ct));
        Assert.False(await RelationExistsAsync(connection, "query_store_stats_daily", ct), "the CASCADE took the daily tier");
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_name IN ('query_store_stats_hourly', 'query_store_stats_daily')", ct));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>
    /// A plain view on a store with TimescaleDB, and a store with no TimescaleDB at all, have no jobs: the quiesce
    /// changes nothing, waits for nothing, and the drop is exactly what it was.
    /// </summary>
    [Fact]
    public async Task PlainViewAndPlainPostgresStore_AreNoOps_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5551 quiesce tests.");

        /* No extension: the information views do not exist, and the lookup is never run. */
        await using (var plain = await ScratchPostgres.CreateAsync(baseConnectionString!, ct))
        await using (var plainConnection = new NpgsqlConnection(plain.ConnectionString))
        {
            await plainConnection.OpenAsync(ct);
            Assert.False(await ScalarAsync<bool>(plainConnection, TimescaleSupport.ContinuousAggregateJobsProbeSql, ct));
            var hold = await TimescaleSupport.QuiesceContinuousAggregateJobsAsync(plainConnection, Retired, null, FastCap, ct);
            Assert.Empty(hold.Unscheduled);
            Assert.False(hold.Skip);
        }

        /* The extension, but the relation is a plain view (the fallback branch) or absent. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await ExecuteAsync(connection, "CREATE OR REPLACE VIEW collect.file_io_baseline AS SELECT 1 AS bucket", ct);
        var viewHold = await TimescaleSupport.QuiesceContinuousAggregateJobsAsync(connection, "file_io_baseline", null, FastCap, ct);
        Assert.Empty(viewHold.Unscheduled);
        Assert.False(viewHold.Skip);
        var absentHold = await TimescaleSupport.QuiesceContinuousAggregateJobsAsync(connection, "no_such_aggregate", null, FastCap, ct);
        Assert.Empty(absentHold.Unscheduled);
        Assert.False(absentHold.Skip);

        var log = new CapturingTestLogger();
        Assert.Equal(1, await TimescaleSupport.DropRetiredBaselineAggregatesAsync(connection, log, DateTime.UtcNow, FastCap, ct));
        Assert.False(await RelationExistsAsync(connection, "file_io_baseline", ct), "the plain view still drops");
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
    }

    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await TimescaleSupport.TryEnableAsync(connection, null, ct), "The live #5551 quiesce tests need TimescaleDB.");
        return connection;
    }

    /// <summary>The pre-#2007 retired aggregate with a refresh, a compression and a retention job, none due for a day.</summary>
    private static async Task CreateRetiredAggregateAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await ExecuteAsync(connection, "CREATE TABLE IF NOT EXISTS collect.w5551_src (collection_time timestamp NOT NULL, v double precision NOT NULL)", ct);
        await ExecuteAsync(connection, "SELECT create_hypertable('collect.w5551_src', by_range('collection_time', INTERVAL '1 day'), if_not_exists => true)", ct);
        await ExecuteAsync(connection, "INSERT INTO collect.w5551_src VALUES (((now() AT TIME ZONE 'UTC') - INTERVAL '2 hours'), 1)", ct);
        await ExecuteAsync(connection, @"
CREATE OR REPLACE FUNCTION collect.w5551_gate(x double precision) RETURNS double precision LANGUAGE plpgsql IMMUTABLE AS $f$
BEGIN
    PERFORM pg_advisory_lock_shared(5551);
    PERFORM pg_advisory_unlock_shared(5551);
    RETURN x;
END
$f$", ct);
        await ExecuteAsync(connection, $@"
CREATE MATERIALIZED VIEW collect.{view} WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 hour', collection_time) AS bucket, sum(collect.w5551_gate(v)) AS s FROM collect.w5551_src GROUP BY 1 WITH NO DATA", ct);
        await ExecuteAsync(connection, $"SELECT add_continuous_aggregate_policy('collect.{view}', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour', initial_start => now() + INTERVAL '1 day')", ct);
        await ExecuteAsync(connection, $"ALTER MATERIALIZED VIEW collect.{view} SET (timescaledb.compress = true)", ct);
        await ExecuteAsync(connection, $"SELECT add_compression_policy('collect.{view}', INTERVAL '7 days', initial_start => now() + INTERVAL '1 day')", ct);
        await ExecuteAsync(connection, $"SELECT add_retention_policy('collect.{view}', drop_after => INTERVAL '30 days', initial_start => now() + INTERVAL '1 day')", ct);
    }

    /// <summary>Records, at the start of the DROP, whether each of the view's jobs was scheduled. TimescaleDB runs the
    /// drop of an aggregate as <c>DROP VIEW</c> (measured), so the trigger listens for both tags, and it can fire more
    /// than once per drop - the assertions are over every row it wrote.</summary>
    private static async Task PlantDropProbeAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await ExecuteAsync(connection, "CREATE TABLE collect.w5551_seen (job_id integer, scheduled boolean)", ct);
        await ExecuteAsync(connection, $@"
CREATE OR REPLACE FUNCTION collect.w5551_probe() RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
    INSERT INTO collect.w5551_seen
    SELECT j.job_id, j.scheduled FROM timescaledb_information.jobs AS j
    WHERE j.hypertable_schema = 'collect' AND j.hypertable_name IN ('{view}', 'query_store_stats_daily')
    ORDER BY j.job_id;
END
$fn$", ct);
        await ExecuteAsync(connection, "CREATE EVENT TRIGGER w5551_probe ON ddl_command_start WHEN TAG IN ('DROP VIEW', 'DROP MATERIALIZED VIEW') EXECUTE FUNCTION collect.w5551_probe()", ct);
    }

    /// <summary>Raises XX000 from inside the view's DROP: the error, and the broken connection, the refresh race produces.</summary>
    private static async Task PlantFailingDropAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await ExecuteAsync(connection, $@"
CREATE OR REPLACE FUNCTION collect.w5551_break_drop() RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() WHERE object_name = '{view}')
    THEN
        RAISE EXCEPTION 'planted #5551 drop failure' USING ERRCODE = 'XX000';
    END IF;
END
$fn$", ct);
        await ExecuteAsync(connection, "CREATE EVENT TRIGGER w5551_break_drop ON sql_drop EXECUTE FUNCTION collect.w5551_break_drop()", ct);
    }

    private static async Task StartRefreshWorkerAsync(NpgsqlConnection connection, int refreshJob, CancellationToken ct)
        => await ExecuteAsync(connection, $"SELECT alter_job({refreshJob}, next_start => now(), scheduled => true)", ct);

    private static async Task<List<int>> JobIdsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var ids = new List<int>();
        await using var command = new NpgsqlCommand("SELECT job_id FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = $1 ORDER BY job_id", connection);
        command.Parameters.AddWithValue(view);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }

    private static async Task<int> RefreshJobIdAsync(NpgsqlConnection connection, string view, CancellationToken ct)
        => await ScalarAsync<int>(connection, $"SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_schema = 'collect' AND hypertable_name = '{view}'", ct);

    private static async Task<Dictionary<int, bool>> JobStatesAsync(NpgsqlConnection connection, IReadOnlyCollection<int> jobIds, CancellationToken ct)
    {
        var states = new Dictionary<int, bool>();
        await using var command = new NpgsqlCommand("SELECT job_id, scheduled FROM timescaledb_information.jobs WHERE job_id = ANY($1::integer[])", connection);
        command.Parameters.AddWithValue(jobIds.ToArray());
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            states[reader.GetInt32(0)] = reader.GetBoolean(1);
        }

        Assert.Equal(jobIds.Count, states.Count);
        return states;
    }

    private static async Task<long> WorkerCountAsync(NpgsqlConnection connection, IReadOnlyCollection<int> jobIds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(TimescaleSupport.AggregateJobWorkerCountSql, connection);
        command.Parameters.AddWithValue(jobIds.ToArray());
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> RelationExistsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
        => await ScalarAsync<bool>(connection, $"SELECT to_regclass('collect.{view}') IS NOT NULL", ct);

    private static async Task<bool> WaitForAsync(Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
        }

        return await condition();
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (T)Convert.ChangeType(value, typeof(T), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
