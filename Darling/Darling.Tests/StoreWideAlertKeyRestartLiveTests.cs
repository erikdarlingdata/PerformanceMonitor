// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7076, live against PostgreSQL: the store-wide alerts keyed by job (Store Job Over Cadence, the Policy Job bands) write their exact key to
/// <c>config_alert_log.alert_key</c> on the real deliverer's rows, and a restart finds its own open row by it. The "restart" is a second
/// evaluator built after the first one's rows were written, over the same store. A row from before V234 has no key and is not matched.
/// </summary>
public sealed class StoreWideAlertKeyRestartLiveTests
{
    private const long JobA = 900761;
    private const long JobB = 900762;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One service process: an evaluator over the real deliverer and the real store, on its own clock.</summary>
    private sealed class Process : IAsyncDisposable
    {
        private readonly NpgsqlDataSource _source;

        public Process(string connectionString, DateTime now)
        {
            _source = NpgsqlDataSource.Create(connectionString);
            var history = new PgAlertHistoryStore(_source, 10);
            var config = new DarlingConfig();
            var alertSettings = new DarlingAlertSettings(config);
            var webhooks = new WebhookAlertService(
                alertSettings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var deliverer = new DarlingAlertDeliverer(alertSettings, history, webhooks, NullLogger.Instance);
            Evaluator = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings { CooldownMinutes = 5 }, deliverer, history, _ => false,
                logger: new DarlingSelfAlertTests.CapturingLogger(), utcNow: () => now);
        }

        public DarlingSelfAlertEvaluator Evaluator { get; }

        public ValueTask DisposeAsync() => _source.DisposeAsync();
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string where)
    {
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM config_alert_log WHERE server_id = 0 AND {where}", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(Ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static Task CadenceAsync(Process process, params long[] jobIds) =>
        process.Evaluator.ApplyStoreJobCadenceAsync(
            jobIds.Select(id => new StoreJobCadenceReading(id, "refresh", 120_000, 60_000)).ToArray(), Ct);

    private static StorePolicyJobHealth Stuck(params long[] jobIds)
    {
        var stuck = jobIds.Select(id => new StuckPolicyJob(
            id, "wait_stats", "next_start is -infinity, the scheduler will never run it again",
            Arm: StuckPolicyJobArm.NextStartNegativeInfinity)).ToList();
        return new StorePolicyJobHealth(
            stuck,
            stuck.Select(j => new PolicyJobRunReading(j.JobId, j.Family, j.HypertableName, Held: false, LastRunStatus: null, TotalFailures: 0)).ToList());
    }

    /// <summary>One pass that reads <paramref name="stuckIds"/> as stuck and <paramref name="healthyIds"/> as running on schedule, the way the real reader lists every job it looked at.</summary>
    private static StorePolicyJobHealth StuckAmongHealthy(long[] stuckIds, params long[] healthyIds)
    {
        var reading = Stuck(stuckIds);
        var healthy = healthyIds.Select(id => new PolicyJobRunReading(
            id, TimescaleSupport.StorePolicyJobFamily.Compression, "wait_stats", Held: false, LastRunStatus: null, TotalFailures: 0));
        return new StorePolicyJobHealth(reading.Stuck, reading.Jobs.Concat(healthy).ToList());
    }

    private static StorePolicyJobHealth NoneStuck() => new(new List<StuckPolicyJob>(), new List<PolicyJobRunReading>());

    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        return connection;
    }

    [Fact]
    public async Task AStandingJobOverCadence_AfterARestart_WritesNoRow_AndASecondJobStillFires()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V234 live restart seed test (it mints its own scratch database).");
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, Ct);
        await using var connection = await OpenAsync(scratch);
        var bodySucceeded = false;
        try
        {
            var start = DateTime.UtcNow;
            await using (var first = new Process(scratch.ConnectionString, start))
            {
                await CadenceAsync(first, JobA);
            }

            Assert.Equal(1, await CountAsync(connection, $"metric_name = 'Store Job Over Cadence' AND alert_key = 'storejob:{JobA}'"));

            /* The restart: a new process, started after the row was written, the job still over its cadence. */
            await using (var second = new Process(scratch.ConnectionString, start.AddMinutes(1)))
            {
                await CadenceAsync(second, JobA);
                await CadenceAsync(second, JobA);
                Assert.Equal(1, await CountAsync(connection, "metric_name = 'Store Job Over Cadence'"));

                /* A second job over its cadence is its own alert and fires, with its own key. */
                await CadenceAsync(second, JobA, JobB);
                Assert.Equal(1, await CountAsync(connection, $"metric_name = 'Store Job Over Cadence' AND alert_key = 'storejob:{JobA}'"));
                Assert.Equal(1, await CountAsync(connection, $"metric_name = 'Store Job Over Cadence' AND alert_key = 'storejob:{JobB}'"));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AJobStuckBeforeARestart_IsNotReArmedOrAnnouncedAgain_AndAJobThatClearedWhileDownIsAnnouncedWhenItSticksAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V234 live restart seed test (it mints its own scratch database).");
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, Ct);
        await using var connection = await OpenAsync(scratch);
        var bodySucceeded = false;
        try
        {
            var start = DateTime.UtcNow;
            var firstRearms = new List<long>();
            await using (var first = new Process(scratch.ConnectionString, start))
            {
                await first.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA, JobB), id => { firstRearms.Add(id); return Task.FromResult(true); }, Ct);

                /* Still stuck on the next pass: the re-hang escalation, the last step both jobs announce. */
                await first.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA, JobB), id => { firstRearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA, JobB }, firstRearms);
            Assert.Equal(4, await CountAsync(connection, "metric_name = 'Compression Job Stuck' AND alert_key LIKE 'compressjob:90076_'"));

            var secondRearms = new List<long>();
            await using (var second = new Process(scratch.ConnectionString, start.AddMinutes(1)))
            {
                /* Restart. Job A is still stuck and was escalated: no re-arm, no new row. Job B cleared while the service was down. */
                await second.Evaluator.ApplyPolicyJobsStuckAsync(StuckAmongHealthy(new[] { JobA }, JobB), id => { secondRearms.Add(id); return Task.FromResult(true); }, Ct);
                await second.Evaluator.ApplyPolicyJobsStuckAsync(StuckAmongHealthy(new[] { JobA }, JobB), id => { secondRearms.Add(id); return Task.FromResult(true); }, Ct);
                Assert.Empty(secondRearms);
                Assert.Equal(2, await CountAsync(connection, $"metric_name = 'Compression Job Stuck' AND alert_key = 'compressjob:{JobA}'"));

                /* The first pass saw job B healthy with its episode still open in the history, so it closed that episode with its resolution row. */
                Assert.Equal(1, await CountAsync(connection, $"metric_name = 'Compression Job Recovered' AND alert_key = 'compressjob:{JobB}'"));

                /* Job B sticks again: a new episode, so it is re-armed and announced once more. */
                await second.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA, JobB), id => { secondRearms.Add(id); return Task.FromResult(true); }, Ct);
                Assert.Equal(new[] { JobB }, secondRearms);
                Assert.Equal(3, await CountAsync(connection, $"metric_name = 'Compression Job Stuck' AND alert_key = 'compressjob:{JobB}'"));
                Assert.Equal(2, await CountAsync(connection, $"metric_name = 'Compression Job Stuck' AND alert_key = 'compressjob:{JobA}'"));
            }

            /* A job that cleared inside a process writes its resolution under its own key, so a later restart reads the episode as closed. */
            await using (var third = new Process(scratch.ConnectionString, start.AddMinutes(2)))
            {
                await third.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), _ => Task.FromResult(true), Ct);
                await third.Evaluator.ApplyPolicyJobsStuckAsync(NoneStuck(), _ => Task.FromResult(true), Ct);
            }

            Assert.Equal(1, await CountAsync(connection, $"metric_name = 'Compression Job Recovered' AND alert_key = 'compressjob:{JobA}'"));
            var rearmsAfterClear = new List<long>();
            await using (var fourth = new Process(scratch.ConnectionString, start.AddMinutes(3)))
            {
                await fourth.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearmsAfterClear.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA }, rearmsAfterClear);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AJobReArmedBeforeARestart_AndStillStuckAfterIt_SendsItsReHungPageOnce_AndIsThenSeededEscalated()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V234 live restart seed test (it mints its own scratch database).");
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, Ct);
        await using var connection = await OpenAsync(scratch);
        var bodySucceeded = false;
        try
        {
            const string StuckRows = "metric_name = 'Compression Job Stuck' AND alert_key = 'compressjob:" + "900761'";
            var start = DateTime.UtcNow;
            var rearms = new List<long>();
            await using (var first = new Process(scratch.ConnectionString, start))
            {
                await first.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            /* The last row before the restart is the auto-re-arm page, and it says so. */
            Assert.Equal(new[] { JobA }, rearms);
            Assert.Equal(1, await CountAsync(connection, StuckRows + " AND context_json::jsonb ->> 'PolicyJobStep' = 'ReArmed'"));

            await using (var second = new Process(scratch.ConnectionString, start.AddMinutes(1)))
            {
                /* Restart, the job still stuck: it was never escalated, so the page it was owed is the "re-hung" escalation, sent once, with no new re-arm. */
                await second.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
                await second.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA }, rearms);
            Assert.Equal(2, await CountAsync(connection, StuckRows));
            Assert.Equal(1, await CountAsync(connection, StuckRows + " AND context_json::jsonb ->> 'PolicyJobStep' = 'Escalated'"));

            /* A second restart: the newest row is the escalation, so the job is seeded escalated and nothing is sent. */
            await using (var third = new Process(scratch.ConnectionString, start.AddMinutes(2)))
            {
                await third.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA }, rearms);
            Assert.Equal(2, await CountAsync(connection, StuckRows));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AJobStuckHealthyAndStuckAcrossTwoRestarts_IsReArmedAndAnnouncedAsANewEpisode()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V234 live restart seed test (it mints its own scratch database).");
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, Ct);
        await using var connection = await OpenAsync(scratch);
        var bodySucceeded = false;
        try
        {
            const string StuckRows = "metric_name = 'Compression Job Stuck' AND alert_key = 'compressjob:900761'";
            var start = DateTime.UtcNow;
            var rearms = new List<long>();
            await using (var first = new Process(scratch.ConnectionString, start))
            {
                /* Process 1: the job sticks, is re-armed, and announces the re-arm. The re-arm fixes it. */
                await first.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA }, rearms);

            await using (var second = new Process(scratch.ConnectionString, start.AddMinutes(1)))
            {
                /* Restart before the next hourly pass: the job is healthy on this process's first pass, so its open row is closed. */
                await second.Evaluator.ApplyPolicyJobsStuckAsync(StuckAmongHealthy(Array.Empty<long>(), JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(1, await CountAsync(connection, "metric_name = 'Compression Job Recovered' AND alert_key = 'compressjob:900761'"));

            await using (var third = new Process(scratch.ConnectionString, start.AddMinutes(2)))
            {
                /* Restart again, the job has hung again: a new episode, so it is re-armed and announced as a first detection, not as a re-hang. */
                await third.Evaluator.ApplyPolicyJobsStuckAsync(Stuck(JobA), id => { rearms.Add(id); return Task.FromResult(true); }, Ct);
            }

            Assert.Equal(new[] { JobA, JobA }, rearms);
            Assert.Equal(2, await CountAsync(connection, StuckRows));
            Assert.Equal(2, await CountAsync(connection, StuckRows + " AND context_json::jsonb ->> 'PolicyJobStep' = 'ReArmed'"));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task V234_ReadsNullOnAnUpgradedStore_AppliesTwice_AndALegacyRowIsNotMatched()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V234 live pin (it mints its own scratch database).");
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, Ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(Ct);
        await PgMigrations.MigrateAsync(connection, Ct);
        var bodySucceeded = false;
        try
        {
            /* A store one rung behind (the rewind artifact drops the column), holding a fire row an older build wrote for job A. */
            var rewind = RungRewindArtifacts.Ladder.Single(r => r.Version == 234);
            await using (var drop = new NpgsqlCommand(rewind.DropSql, connection))
            {
                await drop.ExecuteNonQueryAsync(Ct);
            }

            await using (var delete = new NpgsqlCommand("DELETE FROM darling_schema_version WHERE version >= 234", connection))
            {
                await delete.ExecuteNonQueryAsync(Ct);
            }

            var fireAt = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-30), DateTimeKind.Unspecified);
            await using (var insert = new NpgsqlCommand(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, detail_text)
VALUES ($1, 0, 'Monitor Store', 'Store Job Over Cadence', 200, 25, true, 'email', false, 'Store background refresh [900761] last ran too long')", connection))
            {
                insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = fireAt });
                await insert.ExecuteNonQueryAsync(Ct);
            }

            await PgMigrations.MigrateAsync(connection, Ct);

            Assert.Equal(1, await CountAsync(connection, "alert_key IS NULL AND metric_name = 'Store Job Over Cadence'"));

            /* Applied again, nothing changes. */
            await using (var delete = new NpgsqlCommand("DELETE FROM darling_schema_version WHERE version >= 234", connection))
            {
                await delete.ExecuteNonQueryAsync(Ct);
            }

            await PgMigrations.MigrateAsync(connection, Ct);
            Assert.Equal(1, await CountAsync(connection, "alert_key IS NULL AND metric_name = 'Store Job Over Cadence'"));

            /* The row from before the rung is not any key's open row, so the job announces once after the upgrade. */
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var store = new PgAlertHistoryStore(source, 10);
            var standing = await store.GetKeyedStandingAsync(
                $"storejob:{JobA}", "Store Job Over Cadence", new[] { "Store Job Cadence Recovered" }, null, DateTime.UtcNow.AddMinutes(1));
            Assert.Empty(standing.Fires);

            await using var start = new Process(scratch.ConnectionString, DateTime.UtcNow.AddMinutes(1));
            await CadenceAsync(start, JobA);
            Assert.Equal(1, await CountAsync(connection, $"alert_key = 'storejob:{JobA}'"));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(ConnectionString!, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
