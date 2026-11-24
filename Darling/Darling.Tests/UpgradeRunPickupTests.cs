/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
    static UpgradeRunPickupTests() { AppDomain.CurrentDomain.FirstChanceException += (_, e) => { try { File.AppendAllText("C:/runner/logs/p9432-fc.txt", e.Exception.GetType().Name + ": " + e.Exception.Message.Replace(Environment.NewLine, " ") + Environment.NewLine); } catch (IOException) { } }; }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9432 tests (each mints its own scratch database).";

    private sealed class CountingLogger : ILogger
    {
        public int Warnings;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings++;
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
        catch (Exception ex) { File.AppendAllText("C:/runner/logs/p9432-err.txt", ex.ToString() + Environment.NewLine); throw; }
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
        catch (Exception ex) { File.AppendAllText("C:/runner/logs/p9432-err.txt", ex.ToString() + Environment.NewLine); throw; }
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
        catch (Exception ex) { File.AppendAllText("C:/runner/logs/p9432-err.txt", ex.ToString() + Environment.NewLine); throw; }
        finally
        {
            try { Directory.Delete(folder, recursive: true); } catch (IOException) { }
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
