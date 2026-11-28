/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9432: the service records upgrade-run.json into collect.upgrade_runs. A run that goes in_progress, then succeeded, then verified over
/// three pickups ends as ONE row with each state seen, a run written while the store was down is recorded at the next pickup, the file
/// is removed only once the run is verified, failed or a week old, and a file that does not parse warns once and stays.
/// </summary>
/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */
public sealed class UpgradeRunPickupTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9432 tests (each mints its own scratch database).";

    private sealed class CountingLogger : ILogger
    {
        public int Warnings;

        public readonly System.Collections.Generic.List<string> Informations = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings++;
            }
            else if (logLevel == LogLevel.Information)
            {
                Informations.Add(formatter(state, exception));
            }
        }
    }

    private static string RunJson(Guid id, string state, string? error = null, DateTime? started = null) =>
        "{\"run_id\":\"" + id + "\",\"state\":\"" + state + "\",\"tool_version\":\"3.14.0\",\"target_build\":\"3.14.0\",\"started_utc\":\""
        + (started ?? DateTime.UtcNow.AddMinutes(-10)).ToUniversalTime().ToString("o") + "\",\"ended_utc\":"
        + (state == "in_progress" ? "null" : "\"" + DateTime.UtcNow.ToString("o") + "\"") + ",\"exit_code\":"
        + (state == "in_progress" ? "null" : state == "failed" ? "1" : "0") + ",\"stop_check\":\"clear\",\"rollback_folder\":\"C:\\\\x\\\\_rollback\","
        + "\"first_error\":" + (error is null ? "null" : "\"" + error + "\"") + ",\"verified_utc\":" + (state == "verified" ? "\"" + DateTime.UtcNow.ToString("o") + "\"" : "null") + "}";

    private static async Task<(long Rows, string State)> RowAsync(NpgsqlConnection connection, Guid id)
    {
        await using var command = new NpgsqlCommand("SELECT (SELECT count(*) FROM collect.upgrade_runs), (SELECT state FROM collect.upgrade_runs WHERE run_id = $1)", connection);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        await reader.ReadAsync(TestContext.Current.CancellationToken);
        return (reader.GetInt64(0), reader.IsDBNull(1) ? "" : reader.GetString(1));
    }

    [Fact]
    public async Task ThreePickups_InProgressSucceededVerified_GiveOneRowWithEachStateSeen_AndTheFileGoesAtVerified()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9432-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var id = Guid.NewGuid();
            var now = DateTime.UtcNow;

            File.WriteAllText(file, RunJson(id, "in_progress"));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            Assert.Equal((1L, "in_progress"), await RowAsync(connection, id));

            File.WriteAllText(file, RunJson(id, "succeeded"));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            Assert.Equal((1L, "succeeded"), await RowAsync(connection, id));
            Assert.True(File.Exists(file), "a succeeded run is not verified yet, so its file stays");

            File.WriteAllText(file, RunJson(id, "verified"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(10), ct)).Outcome);
            Assert.Equal((1L, "verified"), await RowAsync(connection, id));
            Assert.False(File.Exists(file), "a verified run's file is removed");
            Assert.Equal(UpgradeRunPickupOutcome.NoFile, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(15), ct)).Outcome);
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* #9821: the file upgrade-darling.sh --rollback writes, in the script's own layout. target_build is the build that is back and
       rollback_folder is the folder that holds the build the rollback replaced. */
    private static string RollbackRunJson(Guid id, string state, DateTime started) =>
        "{\n  \"run_id\": \"" + id + "\",\n  \"state\": \"" + state + "\",\n  \"tool_version\": \"3.14.1\",\n  \"target_build\": \"3.14.0\",\n"
        + "  \"started_utc\": \"" + started.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + "\",\n  \"ended_utc\": "
        + (state == "in_progress" ? "null" : "\"" + started.AddSeconds(33).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + "\"")
        + ",\n  \"exit_code\": " + (state == "in_progress" ? "null" : state == "failed" ? "1" : "0") + ",\n  \"stop_check\": \"clear\",\n"
        + "  \"rollback_folder\": \"/opt/darling.failed\",\n  \"first_error\": " + (state == "failed" ? "\"" + FailedRollbackError + "\"" : "null") + ",\n  \"verified_utc\": null\n}\n";

    private const string FailedRollbackError = "The build from /opt/darling.previous is back in /opt/darling, and its health check failed: the unit is not active.";

    /* #9821: a rollback's own run is a second row. The upgrade it undid is in_progress (undone before its build's first pass) or
       succeeded, the states upgrade-darling.sh writes, or verified, the furthest state a row reaches. The rule that a row never steps
       back is per run id, so it neither drops the rollback's row nor moves the upgrade's row. The newest row then names the build that
       is back and the folder of the build it replaced. */
    [Theory]
    [InlineData("in_progress", UpgradeRunPickupOutcome.Recorded)]
    [InlineData("succeeded", UpgradeRunPickupOutcome.Recorded)]
    [InlineData("verified", UpgradeRunPickupOutcome.RecordedAndRemoved)]
    public async Task ARollbacksOwnRun_IsASecondRowBesideTheUndoneUpgrade_AndTheNewestRowNamesTheBuildThatIsBack(
        string undoneState, UpgradeRunPickupOutcome undoneOutcome)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9821-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var upgrade = Guid.NewGuid();
            var rollback = Guid.NewGuid();
            var now = DateTime.UtcNow;

            File.WriteAllText(file, RunJson(upgrade, undoneState, started: now.AddMinutes(-30)));
            Assert.Equal(undoneOutcome, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            Assert.Equal((1L, undoneState), await RowAsync(connection, upgrade));

            File.WriteAllText(file, RollbackRunJson(rollback, "in_progress", now.AddMinutes(-5)));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            Assert.Equal((2L, "in_progress"), await RowAsync(connection, rollback));

            File.WriteAllText(file, RollbackRunJson(rollback, "succeeded", now.AddMinutes(-5)));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            Assert.Equal((2L, "succeeded"), await RowAsync(connection, rollback));
            Assert.Equal((2L, undoneState), await RowAsync(connection, upgrade));

            await using var newest = new NpgsqlCommand(
                "SELECT run_id, target_build, rollback_folder, exit_code FROM collect.upgrade_runs ORDER BY started_at DESC LIMIT 1", connection);
            await using var reader = await newest.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(rollback, reader.GetGuid(0));
            Assert.Equal("3.14.0", reader.GetString(1));
            Assert.Equal("/opt/darling.failed", reader.GetString(2));
            Assert.Equal(0, reader.GetInt32(3));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* #9821: the rollback's file parses as a run without a store, so a build that reads it never calls it malformed. */
    [Fact]
    public void ARollbacksFile_ParsesAsARunOfItsOwn()
    {
        var id = Guid.NewGuid();
        var started = new DateTime(2026, 11, 27, 0, 33, 5, DateTimeKind.Utc);

        Assert.True(UpgradeRunPickup.TryParse(RollbackRunJson(id, "succeeded", started), out var run, out var problem), problem);

        Assert.Equal(id, run!.RunId);
        Assert.Equal(UpgradeRuns.Succeeded, run.State);
        Assert.Equal("3.14.0", run.TargetBuild);
        Assert.Equal("/opt/darling.failed", run.RollbackFolder);
        Assert.Equal(started.AddSeconds(33), run.EndedUtc);
        Assert.Equal(0, run.ExitCode);
    }

    /* #9821: a rollback whose restored build fails its health check writes failed, exit code 1 and the first error. The file parses
       as a failed run, so no reader takes it for a run that is still going. */
    [Fact]
    public void AFailedRollbacksFile_ParsesAsAFailedRun()
    {
        var id = Guid.NewGuid();
        var started = new DateTime(2026, 11, 27, 0, 33, 5, DateTimeKind.Utc);

        Assert.True(UpgradeRunPickup.TryParse(RollbackRunJson(id, "failed", started), out var run, out var problem), problem);

        Assert.Equal(UpgradeRuns.Failed, run!.State);
        Assert.Equal(1, run.ExitCode);
        Assert.Equal(FailedRollbackError, run.FirstError);
        Assert.Equal(started.AddSeconds(33), run.EndedUtc);
        Assert.Equal("3.14.0", run.TargetBuild);
    }

    /* #9821: the service stores a failed rollback as a failed row, also when its first read is the finished file and it never
       saw the run as in_progress. */
    [Fact]
    public async Task AFailedRollbacksFile_IsStoredAsAFailedRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9821-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var rollback = Guid.NewGuid();
            var now = DateTime.UtcNow;

            File.WriteAllText(Path.Combine(folder, UpgradeRunPickup.FileName), RollbackRunJson(rollback, "failed", now.AddMinutes(-5)));
            var result = await pickup.PickUpAsync(connection, folder, now, ct);

            Assert.Equal(UpgradeRuns.Failed, result.State);
            Assert.Equal((1L, "failed"), await RowAsync(connection, rollback));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARunWrittenWhileTheStoreWasDown_IsRecordedAtTheNextPickup_AndAFailedRunsFileGoes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9432-").FullName;
        var bodySucceeded = false;
        try
        {
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var id = Guid.NewGuid();
            File.WriteAllText(file, RunJson(id, "failed", "the copy failed twice"));

            /* The store is down: the pickup's connection is not open, so it throws and the file stays for the next pickup. */
            await using (var down = new NpgsqlConnection(scratch.ConnectionString))
            {
                await Assert.ThrowsAnyAsync<Exception>(() => new UpgradeRunPickup(new CountingLogger()).PickUpAsync(down, folder, DateTime.UtcNow, ct));
            }

            Assert.True(File.Exists(file), "a pickup that could not reach the store leaves the file");

            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var result = await new UpgradeRunPickup(new CountingLogger()).PickUpAsync(connection, folder, DateTime.UtcNow, ct);
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, result.Outcome);
            Assert.Equal((1L, "failed"), await RowAsync(connection, id));
            Assert.False(File.Exists(file));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AMalformedFile_WarnsOncePerContent_AndStaysInPlace_AndAnOldInProgressFileGoes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9432-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var logger = new CountingLogger();
            var pickup = new UpgradeRunPickup(logger);
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);

            File.WriteAllText(file, "{ not json");
            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(UpgradeRunPickupOutcome.Malformed, (await pickup.PickUpAsync(connection, folder, DateTime.UtcNow, ct)).Outcome);
            }

            Assert.Equal(1, logger.Warnings);
            Assert.True(File.Exists(file), "a malformed file stays in place");

            File.WriteAllText(file, "{\"run_id\":\"not-a-guid\"}");
            await pickup.PickUpAsync(connection, folder, DateTime.UtcNow, ct);
            Assert.Equal(2, logger.Warnings);

            var oldId = Guid.NewGuid();
            File.WriteAllText(file, RunJson(oldId, "in_progress", started: DateTime.UtcNow.AddDays(-8)));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, DateTime.UtcNow, ct)).Outcome);
            Assert.Equal((1L, "in_progress"), await RowAsync(connection, oldId));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The review of #9493: an older copy of a run's file (the rollback backup copies the in_progress file, and a manual restore puts it back)
    /// must not move a finished row back to in_progress. The row keeps its state, and the stale file is removed.
    /// </summary>
    [Fact]
    public async Task AnOlderCopyOfTheRunFile_NeverMovesAFinishedRowBack_AndIsRemoved()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9432-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var now = DateTime.UtcNow;

            var verified = Guid.NewGuid();
            File.WriteAllText(file, RunJson(verified, "verified"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            File.WriteAllText(file, RunJson(verified, "in_progress"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(1), ct)).Outcome);
            Assert.Equal((1L, "verified"), await RowAsync(connection, verified));
            Assert.False(File.Exists(file), "a file behind the stored row is stale and goes");

            var failed = Guid.NewGuid();
            File.WriteAllText(file, RunJson(failed, "failed", "the copy failed twice"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(2), ct)).Outcome);
            File.WriteAllText(file, RunJson(failed, "in_progress"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(3), ct)).Outcome);
            Assert.Equal((2L, "failed"), await RowAsync(connection, failed));

            /* A run that moves forward is still recorded: succeeded after in_progress, with the file kept until it is verified. */
            var forward = Guid.NewGuid();
            File.WriteAllText(file, RunJson(forward, "in_progress"));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(4), ct)).Outcome);
            File.WriteAllText(file, RunJson(forward, "succeeded"));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            Assert.Equal((3L, "succeeded"), await RowAsync(connection, forward));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* #9852: the row as the store holds it: updated_at and the row version (xmin) say whether a pass wrote it. */
    private static async Task<(DateTime UpdatedAt, string Version, string? FirstError, int? ExitCode)> StoredAsync(NpgsqlConnection connection, Guid id)
    {
        await using var command = new NpgsqlCommand("SELECT updated_at, xmin::text, first_error, exit_code FROM collect.upgrade_runs WHERE run_id = $1", connection);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        return (reader.GetDateTime(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetInt32(3));
    }

    /// <summary>
    /// #9852: a file that equals the stored row writes nothing, so <c>updated_at</c> and the row version stay as the last change left them,
    /// and the pickup handles the file as it handles a current one: a run still going or not yet verified keeps its file, a failed or
    /// verified run's file is removed. The file is the same text on both passes (the pass that finds it unwritten is the one a file that
    /// could not be removed gets on every 5-minute pass).
    /// </summary>
    [Theory]
    [InlineData("in_progress", UpgradeRunPickupOutcome.Recorded, true)]
    [InlineData("succeeded", UpgradeRunPickupOutcome.Recorded, true)]
    [InlineData("failed", UpgradeRunPickupOutcome.RecordedAndRemoved, false)]
    [InlineData("verified", UpgradeRunPickupOutcome.RecordedAndRemoved, false)]
    public async Task TheSameFilePickedUpTwice_WritesNothingTheSecondTime_AndIsHandledAsACurrentFile(
        string state, UpgradeRunPickupOutcome outcome, bool fileStays)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9852-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var id = Guid.NewGuid();
            var now = DateTime.UtcNow;
            var text = RunJson(id, state, state == "failed" ? "the copy failed twice" : null);

            File.WriteAllText(file, text);
            Assert.Equal(outcome, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            var first = await StoredAsync(connection, id);
            Assert.Equal(fileStays, File.Exists(file));

            File.WriteAllText(file, text);
            var second = await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct);
            Assert.Equal(outcome, second.Outcome);
            Assert.Equal(id, second.RunId);
            Assert.Equal(state, second.State);
            Assert.Equal(fileStays, File.Exists(file));
            var stored = await StoredAsync(connection, id);
            Assert.Equal(first.UpdatedAt, stored.UpdatedAt);
            Assert.Equal(first.Version, stored.Version);
            Assert.Equal((1L, state), await RowAsync(connection, id));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9852: a file that differs from the stored row in any column still updates it, in the same state or a later one, and
    /// <c>updated_at</c> becomes the time of that pass. An unchanged pass after the change writes nothing again.
    /// </summary>
    [Fact]
    public async Task AChangedFile_UpdatesTheRow_AndTheNextUnchangedPassLeavesThatUpdateAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9852-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var id = Guid.NewGuid();
            var now = DateTime.UtcNow;

            File.WriteAllText(file, RunJson(id, "in_progress"));
            await pickup.PickUpAsync(connection, folder, now, ct);
            var started = await StoredAsync(connection, id);

            /* A state that moves forward is an update. */
            var succeeded = RunJson(id, "succeeded");
            File.WriteAllText(file, succeeded);
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            var moved = await StoredAsync(connection, id);
            Assert.NotEqual(started.UpdatedAt, moved.UpdatedAt);
            Assert.NotEqual(started.Version, moved.Version);
            Assert.Equal((1L, "succeeded"), await RowAsync(connection, id));

            /* The same file again writes nothing. */
            File.WriteAllText(file, succeeded);
            await pickup.PickUpAsync(connection, folder, now.AddMinutes(10), ct);
            var again = await StoredAsync(connection, id);
            Assert.Equal(moved.UpdatedAt, again.UpdatedAt);
            Assert.Equal(moved.Version, again.Version);

            /* The same state with another value in one column is an update too: the file moved to failed, with its error and exit code, and then only its error changes. */
            var failedText = RunJson(id, "failed", "the health check failed");
            File.WriteAllText(file, failedText);
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(15), ct)).Outcome);
            var failed = await StoredAsync(connection, id);
            Assert.NotEqual(again.UpdatedAt, failed.UpdatedAt);
            Assert.Equal("the health check failed", failed.FirstError);
            Assert.Equal(1, failed.ExitCode);

            File.WriteAllText(file, failedText.Replace("the health check failed", "the health check failed twice", StringComparison.Ordinal));
            await pickup.PickUpAsync(connection, folder, now.AddMinutes(20), ct);
            var reworded = await StoredAsync(connection, id);
            Assert.NotEqual(failed.UpdatedAt, reworded.UpdatedAt);
            Assert.Equal("the health check failed twice", reworded.FirstError);
            Assert.Equal((1L, "failed"), await RowAsync(connection, id));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9852: a file behind the stored row is still stale and is removed, and it writes nothing. A run that is still going keeps its file
    /// when it equals the row, and loses it when the row is already further along: the pickup keeps the two apart.
    /// </summary>
    [Fact]
    public async Task AFileBehindTheStoredRow_IsStillStale_WritesNothing_AndIsRemovedWhereAnEqualFileStays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9852-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var id = Guid.NewGuid();
            var now = DateTime.UtcNow;

            File.WriteAllText(file, RunJson(id, "succeeded"));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            var stored = await StoredAsync(connection, id);

            /* The run's first file, put back: behind the row, so stale and removed. */
            File.WriteAllText(file, RunJson(id, "in_progress"));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            Assert.False(File.Exists(file), "a file behind the stored row is stale and goes");
            var after = await StoredAsync(connection, id);
            Assert.Equal(stored.UpdatedAt, after.UpdatedAt);
            Assert.Equal(stored.Version, after.Version);
            Assert.Equal((1L, "succeeded"), await RowAsync(connection, id));
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* #9841: a succeeded run's file stays until it is verified, failed or a week old, so a pass records the same run in the same state
       every five minutes. The "recorded in the store" line is said once for that run and state. */
    [Theory]
    [InlineData(UpgradeRunPickupOutcome.Recorded)]
    [InlineData(UpgradeRunPickupOutcome.RecordedAndRemoved)]
    public void ThePassesThatRecordTheSameRunInTheSameState_SayItOnce(UpgradeRunPickupOutcome outcome)
    {
        var logger = new CountingLogger();
        var pickup = new UpgradeRunPickup(logger);
        var run = Guid.NewGuid();

        for (var pass = 0; pass < 5; pass++)
        {
            pickup.LogRecordedOnce(new UpgradeRunPickupResult(outcome, run, "succeeded"));
        }

        var line = Assert.Single(logger.Informations);
        Assert.Contains(run.ToString(), line, StringComparison.Ordinal);
        Assert.Contains("succeeded", line, StringComparison.Ordinal);
    }

    /* #9841: the line returns when the state moves (in_progress, succeeded, then verified, the order a run takes) and when a different run
       is recorded, such as a rollback's own run, and each of those is then said once as well. */
    [Fact]
    public void ANewStateOrANewRun_IsSaidAgain_AndOnlyOnce()
    {
        var logger = new CountingLogger();
        var pickup = new UpgradeRunPickup(logger);
        var upgrade = Guid.NewGuid();
        var rollback = Guid.NewGuid();

        foreach (var (run, state, outcome) in new[]
        {
            (upgrade, "in_progress", UpgradeRunPickupOutcome.Recorded),
            (upgrade, "in_progress", UpgradeRunPickupOutcome.Recorded),
            (upgrade, "succeeded", UpgradeRunPickupOutcome.Recorded),
            (upgrade, "succeeded", UpgradeRunPickupOutcome.Recorded),
            (upgrade, "verified", UpgradeRunPickupOutcome.RecordedAndRemoved),
            (upgrade, "verified", UpgradeRunPickupOutcome.RecordedAndRemoved),
            (rollback, "succeeded", UpgradeRunPickupOutcome.Recorded),
            (rollback, "succeeded", UpgradeRunPickupOutcome.Recorded),
        })
        {
            pickup.LogRecordedOnce(new UpgradeRunPickupResult(outcome, run, state));
        }

        Assert.Equal(4, logger.Informations.Count);
        Assert.Contains("in_progress", logger.Informations[0], StringComparison.Ordinal);
        Assert.Contains("succeeded", logger.Informations[1], StringComparison.Ordinal);
        Assert.Contains("verified", logger.Informations[2], StringComparison.Ordinal);
        Assert.Contains(rollback.ToString(), logger.Informations[3], StringComparison.Ordinal);
    }

    /* #9841: a pickup that recorded nothing says nothing here: no file, a file that does not parse, and a file that was recorded but could
       not be removed (its own warning says so). A later pass that does record is still said once. */
    [Fact]
    public void APickupThatRecordedNothing_SaysNothing_AndAPassWithNoFileBetweenTwoRecordsDoesNotRepeatTheLine()
    {
        var logger = new CountingLogger();
        var pickup = new UpgradeRunPickup(logger);
        var run = Guid.NewGuid();

        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.NoFile, null, null));
        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.Malformed, null, null));
        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.RecordedNotRemoved, run, "verified"));
        Assert.Empty(logger.Informations);

        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.RecordedAndRemoved, run, "verified"));
        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.NoFile, null, null));
        pickup.LogRecordedOnce(new UpgradeRunPickupResult(UpgradeRunPickupOutcome.RecordedAndRemoved, run, "verified"));
        Assert.Single(logger.Informations);
    }

    /* An exit_code that is present and is not a JSON number makes the file malformed, with the problem named. A string, an object, a
       boolean and a number too large for an integer are all refused the same way, and none of them throws. A JSON null is an exit code
       that is not given, as it is for a run in progress. */
    [Theory]
    [InlineData("\"1\"")]
    [InlineData("{}")]
    [InlineData("true")]
    [InlineData("[1]")]
    [InlineData("1.5")]
    [InlineData("4294967296")]
    public void AnExitCodeThatIsNotANumber_IsAMalformedFile_AndDoesNotThrow(string exitCodeJson)
    {
        var text = RunJson(Guid.NewGuid(), "succeeded").Replace("\"exit_code\":0,", "\"exit_code\":" + exitCodeJson + ",", StringComparison.Ordinal);
        Assert.Contains("\"exit_code\":" + exitCodeJson + ",", text, StringComparison.Ordinal);

        Assert.False(UpgradeRunPickup.TryParse(text, out var run, out var problem));

        Assert.Null(run);
        Assert.Equal("exit_code is not a number", problem);
    }

    [Fact]
    public void AnExitCodeOfNull_IsARunWithNoExitCode()
    {
        var id = Guid.NewGuid();
        var text = RunJson(id, "succeeded").Replace("\"exit_code\":0,", "\"exit_code\":null,", StringComparison.Ordinal);
        Assert.Contains("\"exit_code\":null,", text, StringComparison.Ordinal);

        Assert.True(UpgradeRunPickup.TryParse(text, out var run, out var problem), problem);

        Assert.Equal(id, run!.RunId);
        Assert.Equal(UpgradeRuns.Succeeded, run.State);
        Assert.Null(run.ExitCode);
    }

    /* A pickup of a file whose exit_code is not a number does what it does for any malformed file: the outcome is Malformed, the file
       stays where it is, one warning is logged for the content however many passes read it, and the store is not reached. The connection
       here is never opened, so a pickup that went on to the store would fail. */
    [Theory]
    [InlineData("\"1\"")]
    [InlineData("{}")]
    [InlineData("true")]
    public async Task APickupOfAFileWhoseExitCodeIsNotANumber_IsMalformed_WarnsOnce_KeepsTheFile_AndNeverReachesTheStore(string exitCodeJson)
    {
        var ct = TestContext.Current.CancellationToken;
        var folder = Directory.CreateTempSubdirectory("darling-9940-").FullName;
        try
        {
            var logger = new CountingLogger();
            var pickup = new UpgradeRunPickup(logger);
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var text = RunJson(Guid.NewGuid(), "succeeded").Replace("\"exit_code\":0,", "\"exit_code\":" + exitCodeJson + ",", StringComparison.Ordinal);
            File.WriteAllText(file, text);
            await using var connection = new NpgsqlConnection();

            for (var i = 0; i < 3; i++)
            {
                var result = await pickup.PickUpAsync(connection, folder, DateTime.UtcNow, ct);
                Assert.Equal(UpgradeRunPickupOutcome.Malformed, result.Outcome);
                Assert.Null(result.RunId);
            }

            Assert.Equal(1, logger.Warnings);
            Assert.Equal(text, File.ReadAllText(file));
            Assert.Equal(System.Data.ConnectionState.Closed, connection.State);
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
        }
    }

    /* #9940: the file upgrade-darling.sh --rollback writes when it could read the record of the run it steps back from, in the script's
       own layout: the rollback's run, then the four fields that name the other run. A null argument writes JSON null. */
    private static string SteppingBackRunJson(
        Guid id, string state, DateTime started, string steppedBackId, string? steppedBackState, DateTime? steppedBackEnded, int? steppedBackExit)
    {
        var record = RollbackRunJson(id, state, started);
        return record[..record.LastIndexOf("\n}", StringComparison.Ordinal)]
            + ",\n  \"stepped_back_run_id\": \"" + steppedBackId + "\",\n  \"stepped_back_state\": " + (steppedBackState is null ? "null" : "\"" + steppedBackState + "\"")
            + ",\n  \"stepped_back_ended_utc\": " + (steppedBackEnded is null ? "null" : "\"" + steppedBackEnded.Value.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + "\"")
            + ",\n  \"stepped_back_exit_code\": " + (steppedBackExit is null ? "null" : steppedBackExit.Value.ToString(CultureInfo.InvariantCulture)) + "\n}\n";
    }

    private static async Task<(string State, DateTime? EndedAt, int? ExitCode, DateTime UpdatedAt)> RunRowAsync(NpgsqlConnection connection, Guid id)
    {
        await using var command = new NpgsqlCommand("SELECT state, ended_at, exit_code, updated_at FROM collect.upgrade_runs WHERE run_id = $1", connection);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "the run has no row");
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1), reader.IsDBNull(2) ? null : reader.GetInt32(2), reader.GetDateTime(3));
    }

    /* #9940: the rollback's file names the run it steps back from and what that run's file last said. */
    [Fact]
    public void ARollbacksFile_ThatNamesTheRunItStepsBackFrom_ParsesWithThatRun()
    {
        var id = Guid.NewGuid();
        var upgrade = Guid.NewGuid();
        var started = new DateTime(2026, 11, 27, 0, 33, 5, DateTimeKind.Utc);
        var upgradeEnded = started.AddSeconds(-50);

        Assert.True(UpgradeRunPickup.TryParse(
            SteppingBackRunJson(id, "in_progress", started, upgrade.ToString(), "succeeded", upgradeEnded, 0), out var run, out var problem), problem);

        Assert.Equal(id, run!.RunId);
        Assert.Equal(UpgradeRuns.InProgress, run.State);
        Assert.Equal("/opt/darling.failed", run.RollbackFolder);
        Assert.Equal(new UpgradeRunSteppedBack(upgrade, UpgradeRuns.Succeeded, upgradeEnded, 0), run.SteppedBack);
    }

    /* #9940: the four fields are extra to a run. One that does not read is left out, and the file is still the rollback's own run: a run id
       that is no GUID, or the rollback's own, names no run. A state no file carries, an end that is no time and an exit code that is no
       number are read as not given. */
    [Fact]
    public void SteppedBackFieldsThatDoNotRead_LeaveTheRollbacksOwnRunWhole()
    {
        var id = Guid.NewGuid();
        var upgrade = Guid.NewGuid();
        var started = new DateTime(2026, 11, 27, 0, 33, 5, DateTimeKind.Utc);

        Assert.True(UpgradeRunPickup.TryParse(SteppingBackRunJson(id, "succeeded", started, "not-a-guid", "succeeded", started, 0), out var noGuid, out var problem), problem);
        Assert.Equal(id, noGuid!.RunId);
        Assert.Null(noGuid.SteppedBack);

        Assert.True(UpgradeRunPickup.TryParse(SteppingBackRunJson(id, "succeeded", started, id.ToString(), "succeeded", started, 0), out var ownId, out problem), problem);
        Assert.Null(ownId!.SteppedBack);

        var odd = SteppingBackRunJson(id, "succeeded", started, upgrade.ToString(), "rolled_sideways", null, null)
            .Replace("\"stepped_back_ended_utc\": null", "\"stepped_back_ended_utc\": \"yesterday\"", StringComparison.Ordinal)
            .Replace("\"stepped_back_exit_code\": null", "\"stepped_back_exit_code\": \"zero\"", StringComparison.Ordinal);
        Assert.True(UpgradeRunPickup.TryParse(odd, out var oddRun, out problem), problem);
        Assert.Equal(UpgradeRuns.Succeeded, oddRun!.State);
        Assert.Equal(new UpgradeRunSteppedBack(upgrade, null, null, null), oddRun.SteppedBack);

        Assert.True(UpgradeRunPickup.TryParse(RollbackRunJson(id, "succeeded", started), out var plain, out problem), problem);
        Assert.Null(plain!.SteppedBack);
    }

    /* #9940: a build reads a record that a newer script wrote. Fields it does not know, whatever their JSON kind, change nothing: the run
       parses to the same values as the record without them. */
    [Fact]
    public void ARecordWithFieldsThisBuildDoesNotKnow_ParsesAsTheSameRun()
    {
        var id = Guid.NewGuid();
        var started = new DateTime(2026, 11, 27, 0, 33, 5, DateTimeKind.Utc);
        var known = RollbackRunJson(id, "succeeded", started);
        var newer = known[..known.LastIndexOf("\n}", StringComparison.Ordinal)]
            + ",\n  \"a_later_text\": \"x\",\n  \"a_later_number\": 7,\n  \"a_later_object\": { \"run_id\": \"" + Guid.NewGuid() + "\", \"state\": \"failed\" },\n  \"a_later_list\": [1, \"two\", null]\n}\n";

        Assert.True(UpgradeRunPickup.TryParse(known, out var knownRun, out var problem), problem);
        Assert.True(UpgradeRunPickup.TryParse(newer, out var newerRun, out problem), problem);

        Assert.Equal(knownRun, newerRun);
    }

    /* #9940: what the named row is closed with. The run's own file is the witness when it said succeeded or failed. Anything else, a
       verified or in_progress state or none at all, gives stepped_back, ended at the rollback's start and with no exit code. */
    [Theory]
    [InlineData("succeeded", true, 0, "succeeded", true, 0)]
    [InlineData("failed", true, 1, "failed", true, 1)]
    [InlineData("succeeded", false, null, "succeeded", false, null)]
    [InlineData("in_progress", false, null, "stepped_back", false, null)]
    [InlineData("in_progress", true, 3, "stepped_back", false, null)]
    [InlineData("verified", true, 0, "stepped_back", false, null)]
    [InlineData(null, false, null, "stepped_back", false, null)]
    public void TheClose_TakesTheFilesFinalStateAndEnd_AndIsSteppedBackAtTheRollbacksStartOtherwise(
        string? fileState, bool fileHasEnd, int? fileExit, string expectedState, bool expectsFileEnd, int? expectedExit)
    {
        var rollbackStarted = new DateTime(2026, 11, 27, 0, 33, 5);
        var fileEnded = rollbackStarted.AddMinutes(-1);

        var (state, ended, exitCode) = UpgradeRuns.SteppedBackClose(
            new UpgradeRunSteppedBack(Guid.NewGuid(), fileState, fileHasEnd ? fileEnded : null, fileExit), rollbackStarted);

        Assert.Equal(expectedState, state);
        Assert.Equal(expectsFileEnd ? fileEnded : rollbackStarted, ended);
        Assert.Equal(expectedExit, exitCode);
    }

    /* #9940: an upgrade taken back before the service's next store pass. Its row is in_progress, and the file that says how it ended moved away
       with its build. The rollback's file names the run, so the pickup that stores the rollback's row closes the upgrade's: with the file's state,
       end and exit code when the file had them, and as stepped_back at the rollback's start when it did not. The passes after that write nothing
       to either row, and the read behind get_service_starts reports the closed row as stored. */
    [Theory]
    [InlineData("succeeded", 0, "succeeded")]
    [InlineData("failed", 1, "failed")]
    [InlineData("in_progress", null, "stepped_back")]
    [InlineData(null, null, "stepped_back")]
    public async Task ARollbackThatNamesARunStillInProgress_ClosesItsRow_AndTheNextPassesChangeNothing(string? fileState, int? fileExit, string expectedState)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9940-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var logger = new CountingLogger();
            var pickup = new UpgradeRunPickup(logger);
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var upgrade = Guid.NewGuid();
            var rollback = Guid.NewGuid();
            var now = new DateTime(2026, 11, 27, 12, 0, 0, DateTimeKind.Utc);
            var rollbackStarted = now.AddMinutes(-2);
            var final = fileState is "succeeded" or "failed";
            DateTime? fileEnded = final ? now.AddMinutes(-3) : null;

            File.WriteAllText(file, RunJson(upgrade, "in_progress", started: now.AddMinutes(-4)));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(-4), ct)).Outcome);
            Assert.Equal((1L, "in_progress"), await RowAsync(connection, upgrade));

            File.WriteAllText(file, SteppingBackRunJson(rollback, "in_progress", rollbackStarted, upgrade.ToString(), fileState, fileEnded, fileExit));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now, ct)).Outcome);
            Assert.Equal((2L, "in_progress"), await RowAsync(connection, rollback));
            var closed = await RunRowAsync(connection, upgrade);
            Assert.Equal(expectedState, closed.State);
            Assert.Equal(DateTime.SpecifyKind(fileEnded ?? rollbackStarted, DateTimeKind.Unspecified), closed.EndedAt);
            Assert.Equal(fileExit is null || !final ? null : fileExit, closed.ExitCode);
            Assert.Equal(DateTime.SpecifyKind(now, DateTimeKind.Unspecified), closed.UpdatedAt);
            Assert.Single(logger.Informations, line => line.Contains("closed in the store as " + expectedState, StringComparison.Ordinal));

            File.WriteAllText(file, SteppingBackRunJson(rollback, "succeeded", rollbackStarted, upgrade.ToString(), fileState, fileEnded, fileExit));
            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(5), ct)).Outcome);
            Assert.Equal((2L, "succeeded"), await RowAsync(connection, rollback));
            Assert.Equal(closed, await RunRowAsync(connection, upgrade));
            var rollbackRow = await RunRowAsync(connection, rollback);

            Assert.Equal(UpgradeRunPickupOutcome.Recorded, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(10), ct)).Outcome);
            Assert.Equal(closed, await RunRowAsync(connection, upgrade));
            Assert.Equal(rollbackRow, await RunRowAsync(connection, rollback));
            Assert.Single(logger.Informations, line => line.Contains("closed in the store", StringComparison.Ordinal));

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var runs = await UpgradeRuns.ReadAsync(dataSource, now.AddHours(1), ct);
            var read = Assert.Single(runs, r => r.RunId == upgrade);
            Assert.Equal(expectedState, read.State);
            Assert.Equal(expectedState, read.RecordedState);
            Assert.Equal(fileEnded ?? rollbackStarted, read.EndedUtc);
            Assert.Equal(UpgradeRuns.Succeeded, Assert.Single(runs, r => r.RunId == rollback).State);
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* #9940: the close matches only a row that is still in_progress. A run whose row already has its end keeps it, whatever the rollback's
       file says of that run, and a run the store never saw gets no row. A later file of the run itself is still stored: the placeholder
       stepped_back ranks with succeeded and failed, so the run's own word replaces it and an in_progress copy does not. */
    [Fact]
    public async Task ARowThatHasItsEnd_IsLeftAsItIs_ARunTheStoreNeverSawGetsNoRow_AndTheRunsOwnFileStillWins()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var folder = Directory.CreateTempSubdirectory("darling-9940-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var pickup = new UpgradeRunPickup(new CountingLogger());
            var file = Path.Combine(folder, UpgradeRunPickup.FileName);
            var now = new DateTime(2026, 11, 27, 12, 0, 0, DateTimeKind.Utc);
            var rollbackStarted = now.AddMinutes(-2);

            var ended = Guid.NewGuid();
            File.WriteAllText(file, RunJson(ended, "succeeded", started: now.AddMinutes(-9)));
            await pickup.PickUpAsync(connection, folder, now.AddMinutes(-8), ct);
            var before = await RunRowAsync(connection, ended);
            File.WriteAllText(file, SteppingBackRunJson(Guid.NewGuid(), "succeeded", rollbackStarted, ended.ToString(), "failed", now.AddMinutes(-3), 1));
            await pickup.PickUpAsync(connection, folder, now, ct);
            Assert.Equal(before, await RunRowAsync(connection, ended));

            var unseen = Guid.NewGuid();
            File.WriteAllText(file, SteppingBackRunJson(Guid.NewGuid(), "succeeded", rollbackStarted, unseen.ToString(), "succeeded", now.AddMinutes(-3), 0));
            await pickup.PickUpAsync(connection, folder, now, ct);
            Assert.Equal((3L, ""), await RowAsync(connection, unseen));

            var placeholder = Guid.NewGuid();
            File.WriteAllText(file, RunJson(placeholder, "in_progress", started: now.AddMinutes(-6)));
            await pickup.PickUpAsync(connection, folder, now.AddMinutes(-6), ct);
            File.WriteAllText(file, SteppingBackRunJson(Guid.NewGuid(), "succeeded", rollbackStarted, placeholder.ToString(), null, null, null));
            await pickup.PickUpAsync(connection, folder, now, ct);
            Assert.Equal("stepped_back", (await RunRowAsync(connection, placeholder)).State);

            File.WriteAllText(file, RunJson(placeholder, "in_progress", started: now.AddMinutes(-6)));
            Assert.Equal(UpgradeRunPickupOutcome.RecordedAndRemoved, (await pickup.PickUpAsync(connection, folder, now.AddMinutes(1), ct)).Outcome);
            Assert.Equal("stepped_back", (await RunRowAsync(connection, placeholder)).State);

            File.WriteAllText(file, RunJson(placeholder, "succeeded", started: now.AddMinutes(-6)));
            await pickup.PickUpAsync(connection, folder, now.AddMinutes(2), ct);
            var own = await RunRowAsync(connection, placeholder);
            Assert.Equal("succeeded", own.State);
            Assert.Equal(0, own.ExitCode);
            bodySucceeded = true;
        }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
