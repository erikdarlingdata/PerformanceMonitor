/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static PerformanceMonitor.Darling.Storage.DarlingStoreMaintenanceReadinessReader;

namespace Darling.Tests;

/// <summary>The one rule for whether plain VACUUM can give store table space back (#8787), at its edges.</summary>
public sealed class DarlingStoreTableHealthTruncateRuleTests
{
    [Theory]
    [InlineData(100000, 1000)]
    [InlineData(16000, 1000)]
    [InlineData(8000, 500)]
    [InlineData(160, 10)]
    [InlineData(15, 1)]
    [InlineData(0, 1)]
    public void MinimumTrailingPages_IsTheSmallerOfAThousandPagesAndASixteenth(long relPages, long expected) =>
        Assert.Equal(expected, DarlingVacuumTruncateRule.MinimumTrailingPages(relPages));

    [Fact]
    public void CanReturnSpace_ANineHundredNinetyNinePageRunFailsAndAThousandPagePassesOnABigFile()
    {
        Assert.False(DarlingVacuumTruncateRule.CanReturnSpace(100000, 999, 999 * 8192.0, 999 * 8192.0, truncateOff: false));
        Assert.True(DarlingVacuumTruncateRule.CanReturnSpace(100000, 1000, 1000 * 8192.0, 1000 * 8192.0, truncateOff: false));
    }

    [Fact]
    public void CanReturnSpace_ASixteenthOfASmallFilePasses()
    {
        Assert.False(DarlingVacuumTruncateRule.CanReturnSpace(8000, 499, 499 * 8192.0, 499 * 8192.0, truncateOff: false));
        Assert.True(DarlingVacuumTruncateRule.CanReturnSpace(8000, 500, 500 * 8192.0, 500 * 8192.0, truncateOff: false));
    }

    [Fact]
    public void CanReturnSpace_TheRunMustHoldHalfTheEmptyBytes()
    {
        Assert.True(DarlingVacuumTruncateRule.CanReturnSpace(100000, 2000, 5000.0, 10000.0, truncateOff: false));
        Assert.False(DarlingVacuumTruncateRule.CanReturnSpace(100000, 2000, 4999.0, 10000.0, truncateOff: false));
    }

    [Fact]
    public void CanReturnSpace_NeverWhenVacuumTruncateIsOff() =>
        Assert.False(DarlingVacuumTruncateRule.CanReturnSpace(100000, 50000, 1e9, 1e9, truncateOff: true));

    [Fact]
    public void TailWindowPages_IsTheLargerOfTheMinimumAndHalfTheEmptyBytes()
    {
        Assert.Equal(1000, DarlingVacuumTruncateRule.TailWindowPages(100000, 100 * 8192L, 8192));
        Assert.Equal(5000, DarlingVacuumTruncateRule.TailWindowPages(100000, 10000 * 8192L, 8192));
        Assert.Equal(5001, DarlingVacuumTruncateRule.TailWindowPages(100000, 10001 * 8192L, 8192));
    }

    [Fact]
    public void ARelationWhoseWindowIsOverTheCap_IsNotProbed()
    {
        var past = new DarlingStoreTableHealthReader.TailRelation("collect.t", 1_000_000, 2L * (DarlingStoreTableHealthReader.TailProbeMaxWindowPages + 1) * 8192, false);
        Assert.True(past.WindowPages() > DarlingStoreTableHealthReader.TailProbeMaxWindowPages);
        var within = new DarlingStoreTableHealthReader.TailRelation("collect.t", 1_000_000, 2L * 5000 * 8192, false);
        Assert.Equal(5000, within.WindowPages());
    }

    private static StoreRunningVacuum Vacuum(StoreVacuumKind kind) =>
        kind == StoreVacuumKind.Hidden
            ? new StoreRunningVacuum(null, null, null, null, kind)
            : new StoreRunningVacuum("collect.zz_t", "scanning heap", 10, 100, kind);

    [Fact]
    public void AutovacuumAlone_IsANoteNotAReason_AndAManualOrWraparoundVacuumIsAReason()
    {
        var crashes = new DarlingStoreJobCrashReader.StoreJobCrashRead(true, null, Array.Empty<DarlingStoreJobCrashReader.StoreJobCrash>());
        var plain = JudgeVacuums(new[] { Vacuum(StoreVacuumKind.Autovacuum) });
        Assert.Empty(plain.Reasons);
        Assert.Single(plain.Notes);
        var safeVerdict = Verdict(true, plain.Reasons, true, 0, crashes, plain.Notes);
        Assert.StartsWith("Safe for maintenance now.", safeVerdict, StringComparison.Ordinal);
        Assert.Contains("1 autovacuum is running on collect.zz_t", safeVerdict, StringComparison.Ordinal);

        var manual = JudgeVacuums(new[] { Vacuum(StoreVacuumKind.Manual), Vacuum(StoreVacuumKind.Autovacuum) });
        Assert.Single(manual.Reasons);
        Assert.Contains("manual VACUUM", manual.Reasons[0], StringComparison.Ordinal);
        Assert.StartsWith("Not safe for maintenance now", Verdict(false, manual.Reasons, true, 0, crashes, manual.Notes), StringComparison.Ordinal);

        var wrap = JudgeVacuums(new[] { Vacuum(StoreVacuumKind.AutovacuumToPreventWraparound) });
        Assert.Single(wrap.Reasons);
        Assert.Contains("prevent wraparound", wrap.Reasons[0], StringComparison.Ordinal);

        var hiddenOnly = JudgeVacuums(new[] { Vacuum(StoreVacuumKind.Hidden) });
        Assert.Empty(hiddenOnly.Reasons);
        Assert.Contains("cannot read", Assert.Single(hiddenOnly.Notes), StringComparison.Ordinal);
    }
}

/// <summary>The crash backoff <c>next_start</c> (#8787) against a real store, as a superuser who can rewrite a job's stat row.</summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreJobCrashLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ACrashBackoffJob_ReadsNextStartAsNullAndCrashBackoffTrue_OnTheReadinessTool()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The scheduler would rewrite the row, so stop it first. Then a job's stat row says it crashed once and is backing off. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        int jobId;
        await using (var pick = new NpgsqlCommand("SELECT min(job_id) FROM timescaledb_information.jobs", connection))
        {
            jobId = (int)(await pick.ExecuteScalarAsync(ct))!;
        }

        await using (var write = new NpgsqlCommand(
            "INSERT INTO _timescaledb_internal.bgw_job_stat (job_id, last_start, last_finish, next_start, last_successful_finish, last_run_success, total_runs, total_duration, total_duration_failures, total_successes, total_failures, total_crashes, consecutive_failures, consecutive_crashes, flags) "
            + "VALUES ($1, now(), now(), '-infinity', now(), false, 3, '0', '0', 0, 0, 2, 0, 1, 0) "
            + "ON CONFLICT (job_id) DO UPDATE SET next_start = '-infinity', consecutive_crashes = 1, total_crashes = 2", connection))
        {
            write.Parameters.AddWithValue(jobId);
            await write.ExecuteNonQueryAsync(ct);
        }

        var read = await DarlingStoreJobCrashReader.ReadAsync(dataSource, ct);
        Assert.True(read.Available, read.UnavailableReason);
        var job = Assert.Single(read.Jobs, j => j.JobId == jobId);
        Assert.Null(job.NextStart);
        Assert.True(job.CrashBackoff);

        using var readiness = JsonDocument.Parse(await DarlingMcpStoreMaintenanceReadinessTools.GetStoreMaintenanceReadiness(dataSource, ct));
        var crashed = readiness.RootElement.GetProperty("job_crashes").GetProperty("jobs").EnumerateArray().Single(j => j.GetProperty("job_id").GetInt32() == jobId);
        Assert.Equal(JsonValueKind.Null, crashed.GetProperty("next_start").ValueKind);
        Assert.True(crashed.GetProperty("crash_backoff").GetBoolean());
        Assert.DoesNotContain("0001-01-01", readiness.RootElement.GetRawText(), StringComparison.Ordinal);
    }

    /// <summary>
    /// A running job carries one provisional crash (#9391): TimescaleDB adds it when the run starts and takes it back when the run
    /// finishes. The read must not call that a crash while the job has a live backend, and must still call a job that died mid-run
    /// (the same stored shape, no backend) crashed. It must say so to a role that is not a member of the worker's role and not in
    /// <c>pg_read_all_stats</c> too, as the <c>mcp</c> and <c>viewer</c> roles are: PostgreSQL hides <c>state</c> from such a role, and
    /// <c>timescaledb_information.job_stats</c> reads <c>state</c>.
    /// </summary>
    [Fact]
    public async Task ARunningJob_ReadsOneCrashLess_AndNotInBackoff_WhileADeadJobWithTheSameRowStaysCrashed()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9391 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        int jobId;
        string applicationName;
        await using (var pick = new NpgsqlCommand("SELECT job_id, application_name FROM timescaledb_information.jobs ORDER BY job_id LIMIT 1", connection))
        await using (var picked = await pick.ExecuteReaderAsync(ct))
        {
            Assert.True(await picked.ReadAsync(ct));
            jobId = picked.GetInt32(0);
            applicationName = picked.GetString(1);
        }

        /* The shape TimescaleDB writes when a run starts: one crash counted, last_finish and next_start at -infinity. */
        await using (var write = new NpgsqlCommand(
            "INSERT INTO _timescaledb_internal.bgw_job_stat (job_id, last_start, last_finish, next_start, last_successful_finish, last_run_success, total_runs, total_duration, total_duration_failures, total_successes, total_failures, total_crashes, consecutive_failures, consecutive_crashes, flags) "
            + "VALUES ($1, now(), '-infinity', '-infinity', now(), false, 3, '0', '0', 0, 0, 1, 0, 1, 0) "
            + "ON CONFLICT (job_id) DO UPDATE SET last_finish = '-infinity', next_start = '-infinity', consecutive_crashes = 1, total_crashes = 1", connection))
        {
            write.Parameters.AddWithValue(jobId);
            await write.ExecuteNonQueryAsync(ct);
        }

        var plainRole = await CreatePlainRoleAsync(connection, scratch, ct);
        await using var plainSource = NpgsqlDataSource.Create(plainRole.ConnectionString);
        try
        {
        foreach (var source in new[] { dataSource, plainSource })
        {
            var dead = await DarlingStoreJobCrashReader.ReadAsync(source, ct);
            Assert.True(dead.Available, dead.UnavailableReason);
            var deadJob = Assert.Single(dead.Jobs, j => j.JobId == jobId);
            Assert.False(deadJob.Running);
            Assert.Equal(1, deadJob.ConsecutiveCrashes);
            Assert.Equal(1, deadJob.TotalCrashes);
            Assert.True(deadJob.CrashBackoff);
        }

        /* A live backend that answers to the job's application name and is active is what job_stats calls Running. */
        await using var worker = new NpgsqlConnection(scratch.ConnectionString);
        await worker.OpenAsync(ct);
        await using (var name = new NpgsqlCommand("SELECT pg_catalog.set_config('application_name', $1, false)", worker))
        {
            name.Parameters.AddWithValue(applicationName);
            await name.ExecuteScalarAsync(ct);
        }

        await using var sleep = new NpgsqlCommand("SELECT pg_catalog.pg_sleep(60)", worker);
        var sleeping = sleep.ExecuteNonQueryAsync(ct);
        var bodySucceeded = false;
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(20);
            bool active;
            do
            {
                await using var look = new NpgsqlCommand(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = $1 AND state = 'active' AND pid <> pg_catalog.pg_backend_pid())", connection);
                look.Parameters.AddWithValue(applicationName);
                active = (bool)(await look.ExecuteScalarAsync(ct))!;
                if (!active)
                {
                    await Task.Delay(100, ct);
                }
            }
            while (!active && DateTime.UtcNow < deadline);
            Assert.True(active, "the stand-in worker never showed as active");

            /* The precondition of the plain-role read: this role cannot see the worker's state, only its datid and application_name. */
            await using (var hidden = plainSource.CreateCommand(
                "SELECT state, application_name FROM pg_catalog.pg_stat_activity WHERE application_name = $1 AND pid <> pg_catalog.pg_backend_pid()"))
            {
                hidden.Parameters.AddWithValue(applicationName);
                await using var seen = await hidden.ExecuteReaderAsync(ct);
                Assert.True(await seen.ReadAsync(ct), "the plain role should see the worker's row, with the columns PostgreSQL shows to every role");
                Assert.True(seen.IsDBNull(0), "the plain role must not see the worker's state, or this test does not stand in for mcp and viewer");
            }

            foreach (var source in new[] { dataSource, plainSource })
            {
                var running = await DarlingStoreJobCrashReader.ReadAsync(source, ct);
                Assert.True(running.Available, running.UnavailableReason);
                var runningJob = Assert.Single(running.Jobs, j => j.JobId == jobId);
                Assert.True(runningJob.Running);
                Assert.Equal(0, runningJob.ConsecutiveCrashes);
                Assert.Equal(0, runningJob.TotalCrashes);
                Assert.False(runningJob.CrashBackoff);
                Assert.DoesNotContain(running.Crashing, j => j.JobId == jobId);
            }

            using var readiness = JsonDocument.Parse(await DarlingMcpStoreMaintenanceReadinessTools.GetStoreMaintenanceReadiness(dataSource, ct));
            Assert.DoesNotContain(
                readiness.RootElement.GetProperty("job_crashes").GetProperty("jobs").EnumerateArray(),
                j => j.GetProperty("job_id").GetInt32() == jobId);
            bodySucceeded = true;
        }
        finally
        {
            sleep.Cancel();
            try
            {
                await sleeping;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                /* the cancel we asked for */
            }
        }

        Assert.True(bodySucceeded);
        }
        finally
        {
            await DropPlainRoleAsync(connection, plainRole);
        }
    }

    /// <summary>
    /// The same read against a real TimescaleDB job worker, as a role that cannot see its state (#9391). The stand-in above takes the
    /// job's application name from <c>jobs.application_name</c>: this one proves a worker really carries that name.
    /// </summary>
    [Fact]
    public async Task ARealJobWorker_ReadsAsRunning_ToARoleThatCannotSeeItsState()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9391 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await using (var make = new NpgsqlCommand(
            "CREATE PROCEDURE public.sleep_9391(job_id int, config jsonb) LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_catalog.pg_sleep(60); END $$", connection))
        {
            await make.ExecuteNonQueryAsync(ct);
        }

        int jobId;
        await using (var add = new NpgsqlCommand("SELECT add_job('public.sleep_9391', '1 hour', initial_start => now())", connection))
        {
            jobId = (int)(await add.ExecuteScalarAsync(ct))!;
        }

        var plainRole = await CreatePlainRoleAsync(connection, scratch, ct);
        await using var plainSource = NpgsqlDataSource.Create(plainRole.ConnectionString);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(60);
            DarlingStoreJobCrashReader.StoreJobCrash? job = null;
            while (DateTime.UtcNow < deadline)
            {
                var read = await DarlingStoreJobCrashReader.ReadAsync(plainSource, ct);
                Assert.True(read.Available, read.UnavailableReason);
                job = read.Jobs.SingleOrDefault(j => j.JobId == jobId);
                if (job is { Running: true })
                {
                    break;
                }

                await Task.Delay(500, ct);
            }

            Assert.NotNull(job);
            Assert.True(job.Running, "the job's worker never read as running to the plain role");
            Assert.Equal(0, job.ConsecutiveCrashes);
            Assert.False(job.CrashBackoff);
        }
        finally
        {
            await using (var stop = new NpgsqlCommand(
                "SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE application_name = $1", connection))
            {
                stop.Parameters.AddWithValue("User-Defined Action [" + jobId.ToString(System.Globalization.CultureInfo.InvariantCulture) + "]");
                await stop.ExecuteNonQueryAsync(System.Threading.CancellationToken.None);
            }

            await DropPlainRoleAsync(connection, plainRole);
        }
    }

    /// <summary>A role that is not the store owner, not a member of any other role and not in <c>pg_read_all_stats</c>: it may only connect.</summary>
    private static async Task<(string Name, string ConnectionString)> CreatePlainRoleAsync(NpgsqlConnection owner, ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        var name = "plain9391_" + Guid.NewGuid().ToString("N")[..8];
        await using (var create = new NpgsqlCommand(
            $"CREATE ROLE {name} LOGIN PASSWORD 'Plain9391Pw'; GRANT CONNECT ON DATABASE \"{scratch.DatabaseName}\" TO {name};", owner))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = name, Password = "Plain9391Pw" };
        return (name, builder.ConnectionString);
    }

    private static async Task DropPlainRoleAsync(NpgsqlConnection owner, (string Name, string ConnectionString) role)
    {
        NpgsqlConnection.ClearAllPools();
        await using var drop = new NpgsqlCommand($"DROP OWNED BY {role.Name}; DROP ROLE IF EXISTS {role.Name};", owner);
        await drop.ExecuteNonQueryAsync(System.Threading.CancellationToken.None);
    }

    [Theory]
    [InlineData(1, 1, true, 0, 0, false)]
    [InlineData(3, 7, true, 2, 6, false)]
    [InlineData(0, 0, true, 0, 0, false)]
    [InlineData(1, 1, false, 1, 1, true)]
    public void Adjust_TakesTheProvisionalCrashOutOfARunningJob_AndLeavesAStoppedOneAlone(
        int consecutive, long total, bool running, int expectedConsecutive, long expectedTotal, bool expectedBackoff)
    {
        var stored = new DarlingStoreJobCrashReader.StoreJobCrash(7, "policy_x", null, consecutive, total, 0, null, CrashBackoff: consecutive > 0);

        var adjusted = DarlingStoreJobCrashReader.Adjust(stored, running);

        Assert.Equal(expectedConsecutive, adjusted.ConsecutiveCrashes);
        Assert.Equal(expectedTotal, adjusted.TotalCrashes);
        Assert.Equal(expectedBackoff, adjusted.CrashBackoff);
        Assert.Equal(running, adjusted.Running);
    }
}

/// <summary>Measure reads <c>vacuum_truncate</c> in every spelling and judges by the shared rule (#8787).</summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreTableMeasureTruncateLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData("off")]
    [InlineData("0")]
    [InlineData("no")]
    public async Task VacuumTruncateOff_InAnySpelling_StopsMeasureSayingVacuumCanTruncate(string spelling)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var setup = new NpgsqlCommand(
            "CREATE TABLE collect.zz_trunc (id integer, payload text) WITH (autovacuum_enabled = false);"
            + "INSERT INTO collect.zz_trunc SELECT g, repeat('x', 200) FROM generate_series(1, 3000) AS g;"
            + "DELETE FROM collect.zz_trunc WHERE id > 1;", connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        await using (var vacuum = new NpgsqlCommand("VACUUM (TRUNCATE false) collect.zz_trunc", connection))
        {
            await vacuum.ExecuteNonQueryAsync(ct);
        }

        var before = (await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_trunc", ct)).Measure!;
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictVacuumCanTruncate, before.VerdictKind);
        Assert.False(before.VacuumTruncateDisabled);

        await using (var alter = new NpgsqlCommand($"ALTER TABLE collect.zz_trunc SET (vacuum_truncate = {spelling})", connection))
        {
            await alter.ExecuteNonQueryAsync(ct);
        }

        var after = (await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, "collect.zz_trunc", ct)).Measure!;
        Assert.True(after.VacuumTruncateDisabled);
        Assert.NotEqual(DarlingStoreTableMeasureReader.VerdictVacuumCanTruncate, after.VerdictKind);
        Assert.DoesNotContain("VACUUM can truncate", after.Verdict, StringComparison.Ordinal);
    }
}
