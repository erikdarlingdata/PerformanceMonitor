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
