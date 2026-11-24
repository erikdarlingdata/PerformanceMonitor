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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// The background step recorder (#9411) against a real store: what a step's row says for each way a step can end, that a failed step's
/// exception still reaches the caller, that a recorder write that cannot land never fails or throws out of the step, and that the hourly
/// prune removes only rows past the retention.
/// </summary>
[Collection("live-postgres")]
public sealed class StorePassStepRecorderLiveTests
{
    private static readonly DateTime Now = new(2026, 11, 1, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStepThatReturnsCounts_RecordsOutcomeOk_WithItsCounts()
    {
        await RunAsync(emptyStore: false, async (recorder, source, ct) =>
        {
            await recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "counted_step",
                () => Task.FromResult(new StorePassSteps.StepCounts(Built: 5, Removed: 2, Detail: "one left")));

            var page = await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct);
            var row = Assert.Single(page.Rows);
            Assert.Equal(StorePassSteps.StoreMaintenancePass, row.Pass);
            Assert.Equal("counted_step", row.Step);
            Assert.Equal(StorePassSteps.OutcomeOk, row.Outcome);
            Assert.Equal(5L, row.Built);
            Assert.Equal(2L, row.Removed);
            Assert.Equal("one left", row.Detail);
            Assert.True(row.FinishedAt >= row.StartedAt);
        });
    }

    [Fact]
    public async Task AStepThatCountsFailedBuilds_RecordsOutcomeFailed_WithoutThrowing()
    {
        await RunAsync(emptyStore: false, async (recorder, source, ct) =>
        {
            await recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "partly_failed", () => Task.FromResult(new StorePassSteps.StepCounts(Built: 3, Failed: 2)));

            var row = Assert.Single((await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct)).Rows);
            Assert.Equal(StorePassSteps.OutcomeFailed, row.Outcome);
            Assert.Equal(3L, row.Built);
            Assert.Equal("2 failed", row.Detail);
        });
    }

    [Fact]
    public async Task AThrowingStep_RecordsOutcomeFailed_WithTheOneLineTrimmedText_AndTheExceptionStillReachesTheCaller()
    {
        await RunAsync(emptyStore: false, async (recorder, source, ct) =>
        {
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "throwing_step", () => throw new InvalidOperationException("boom\r\nsecond line")));
            Assert.Equal("boom\r\nsecond line", thrown.Message);

            var longText = new string('x', StorePassSteps.MaxDetailLength + 200);
            await Assert.ThrowsAsync<InvalidOperationException>(() => recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "long_failure", () => throw new InvalidOperationException(longText)));

            var rows = (await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct)).Rows;
            Assert.Equal(2, rows.Count);
            var boom = Assert.Single(rows, r => r.Step == "throwing_step");
            Assert.Equal(StorePassSteps.OutcomeFailed, boom.Outcome);
            Assert.Equal("boom second line", boom.Detail);
            Assert.Null(boom.Built);
            Assert.Null(boom.Removed);
            var trimmed = Assert.Single(rows, r => r.Step == "long_failure");
            Assert.Equal(StorePassSteps.MaxDetailLength, trimmed.Detail!.Length);
        });
    }

    [Fact]
    public async Task ASkippedStep_RecordsOutcomeSkipped_WithItsReason()
    {
        await RunAsync(emptyStore: false, async (recorder, source, ct) =>
        {
            await recorder.StepAsync(
                StorePassSteps.StoreSelfMetricsPass, "skipped_step",
                () => Task.FromResult(new StorePassSteps.StepCounts(Skipped: true, Detail: "the extension is not installed")));

            var row = Assert.Single((await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct)).Rows);
            Assert.Equal(StorePassSteps.OutcomeSkipped, row.Outcome);
            Assert.Equal("the extension is not installed", row.Detail);
        });
    }

    [Fact]
    public async Task AStepCancelledByItsOwnTimeSlice_RecordsFailedAsCutShort_AndRethrows()
    {
        await RunAsync(emptyStore: false, async (recorder, source, ct) =>
        {
            /* The service is running (the recorder's shutdown token is live), so the step's own slice expiring is a failed step. */
            using var slice = new CancellationTokenSource();
            await Assert.ThrowsAsync<TaskCanceledException>(() => recorder.StepAsync(
                StorePassSteps.StoreSelfMetricsPass, "slice_expired_step", async () =>
                {
                    await slice.CancelAsync();
                    await Task.Delay(Timeout.Infinite, slice.Token);
                    return default;
                }));

            var row = Assert.Single((await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct)).Rows);
            Assert.Equal("slice_expired_step", row.Step);
            Assert.Equal(StorePassSteps.OutcomeFailed, row.Outcome);
            Assert.Equal("cut short by its time budget", row.Detail);
        });
    }

    [Fact]
    public async Task AStepThatThrowsAfterTheShutdownToken_RecordsNothing_AndRethrows()
    {
        await RunAsync(emptyStore: false, async (_, source, ct) =>
        {
            using var shutdown = new CancellationTokenSource();
            var recorder = new StorePassSteps.Recorder(source, NullLogger.Instance, () => Now, shutdown.Token);

            await Assert.ThrowsAsync<OperationCanceledException>(() => recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "shutdown_step", async () =>
                {
                    await shutdown.CancelAsync();
                    throw new OperationCanceledException(shutdown.Token);
                }));

            Assert.Empty((await StorePassSteps.ReadAsync(source, Now.AddDays(-1), Now.AddDays(1), null, null, 10, ct)).Rows);
        });
    }

    [Fact]
    public async Task AStoreWithNoStepTable_NeitherFailsNorThrowsFromTheStep_AndALaterFailureStillReachesTheCaller()
    {
        await RunAsync(emptyStore: true, async (recorder, _, _) =>
        {
            var ran = 0;
            await recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "no_table_ok", () =>
                {
                    ran++;
                    return Task.FromResult(new StorePassSteps.StepCounts(Built: 1));
                });
            await recorder.RecordAsync(StorePassSteps.StoreMaintenancePass, "no_table_recorded", Now, TimeSpan.FromSeconds(1), new StorePassSteps.StepCounts(Built: 1));
            Assert.Equal(1, ran);

            /* The step's own failure is what the caller sees, not the recorder's failed write. */
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => recorder.StepAsync(
                StorePassSteps.StoreMaintenancePass, "no_table_throws", () => throw new InvalidOperationException("the step's own failure")));
            Assert.Equal("the step's own failure", thrown.Message);
        });
    }

    [Fact]
    public async Task ThePrune_RemovesOnlyRowsPastTheRetention_AndRunsOnceAnHour()
    {
        await RunAsync(emptyStore: false, async (_, source, ct) =>
        {
            var clock = Now;
            var timed = new StorePassSteps.Recorder(source, NullLogger.Instance, () => clock);

            await using (var seed = source.CreateConnection())
            {
                await seed.OpenAsync(ct);
                await ExecAsync(seed, SeedRow("past_retention", Now.AddDays(-StorePassSteps.RetentionDays - 5)), ct);
                await ExecAsync(seed, SeedRow("inside_retention", Now.AddDays(-StorePassSteps.RetentionDays + 5)), ct);
                await ExecAsync(seed, SeedRow("recent", Now.AddDays(-1)), ct);
            }

            await timed.RecordAsync(StorePassSteps.StoreMaintenancePass, "first_write", Now, TimeSpan.Zero, default);
            Assert.Equal(new[] { "first_write", "inside_retention", "recent" }, await StepNamesAsync(source, ct));

            /* A second write ten minutes on does not prune again: a row that has aged out in the meantime stays until the hour is up. */
            await using (var seed = source.CreateConnection())
            {
                await seed.OpenAsync(ct);
                await ExecAsync(seed, SeedRow("late_past_retention", Now.AddDays(-StorePassSteps.RetentionDays - 1)), ct);
            }

            clock = Now.AddMinutes(10);
            await timed.RecordAsync(StorePassSteps.StoreMaintenancePass, "second_write", clock, TimeSpan.Zero, default);
            Assert.Contains("late_past_retention", await StepNamesAsync(source, ct));

            clock = Now.AddMinutes(61);
            await timed.RecordAsync(StorePassSteps.StoreMaintenancePass, "third_write", clock, TimeSpan.Zero, default);
            Assert.Equal(
                new[] { "first_write", "inside_retention", "recent", "second_write", "third_write" },
                await StepNamesAsync(source, ct));
        });
    }

    private static string SeedRow(string step, DateTime startedUtc)
    {
        var stamp = startedUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        return $"INSERT INTO collect.store_pass_steps (pass, step, started_at, finished_at, outcome) VALUES ('seed', '{step}', '{stamp}', '{stamp}', 'ok')";
    }

    private static async Task<List<string>> StepNamesAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        var names = new List<string>();
        await using var command = source.CreateCommand("SELECT step FROM collect.store_pass_steps ORDER BY step");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Mints a scratch store and hands the body a recorder on a fixed clock. A default store is a migrated clone, so the step table is there.
    /// <paramref name="emptyStore"/> leaves the database unmigrated, with no <c>collect</c> schema, which is a store the step table was never
    /// built on.
    /// </summary>
    private static async Task RunAsync(bool emptyStore, Func<StorePassSteps.Recorder, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the step recorder live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: emptyStore);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(new StorePassSteps.Recorder(source, NullLogger.Instance, () => Now), source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
