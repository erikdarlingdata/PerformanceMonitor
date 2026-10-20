/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6247: a command nobody claimed within 15 minutes is failed, never run, and the Viewer cancels its own row when it stops
/// waiting. Every age is set from the STORE's clock (<c>now() AT TIME ZONE 'UTC'</c> minus an interval), the same clock the
/// claim tests it against. The expected error text is spelled out here rather than read from the product constant, so a
/// reworded message shows up as a failing test.
/// </summary>
public sealed class CommandExpiryLiveTests
{
    private const string ExpiredText = "expired: the service did not pick this up within 15 minutes of the request";
    private const string CancelledText = "cancelled: the Viewer stopped waiting";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Records whether the service ran a <c>purge_now</c>; nothing else may run.</summary>
    private sealed class PurgeRecordingHost : IDarlingCommandHost
    {
        public int PurgeCalls { get; private set; }

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
        {
            PurgeCalls++;
            return Task.FromResult(new CommandOutcome(true, "purge started", "{\"success\":true,\"started\":true}"));
        }

        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, IReadOnlyList<string>? collectors, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>Queues a <c>purge_now</c> row whose <c>created_at</c> is <paramref name="ageMinutes"/> old on the store's clock.</summary>
    private static async Task<long> QueueAsync(NpgsqlConnection connection, int ageMinutes, string commandType, CancellationToken ct, string? argsJson = null)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.config_command (requested_by, command_type, args_json, status, created_at) " +
            "VALUES ('expiry-test', $1, $2::jsonb, 'pending', (now() AT TIME ZONE 'UTC') - make_interval(mins => $3)) RETURNING command_id", connection);
        command.Parameters.AddWithValue(commandType);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = (object?)argsJson ?? DBNull.Value });
        command.Parameters.AddWithValue(ageMinutes);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private sealed record Row(string Status, string? ResultStatus, string? ErrorText, bool ArgsNull, bool Completed);

    private static async Task<Row> ReadAsync(NpgsqlConnection connection, long id, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT status, result_status, result_json->>'error', args_json IS NULL, completed_at IS NOT NULL FROM config.config_command WHERE command_id = $1", connection);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return new Row(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetBoolean(3), reader.GetBoolean(4));
    }

    private static async Task RunAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live command-expiry tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenMigratedAsync(scratch.ConnectionString, ct);
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task APendingRowOlderThanFifteenMinutes_IsFailedWithTheExpiryText_AndNotRun()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            var expired = await QueueAsync(connection, 16, "purge_now", ct, "{\"retention_days\": 3}");
            var host = new PurgeRecordingHost();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var executor = new DarlingCommandExecutor(postgres, host, "expiry-test", null);

            var ran = await executor.PollOnceAsync(ct);

            Assert.False(ran);
            Assert.Equal(0, host.PurgeCalls);
            var row = await ReadAsync(connection, expired, ct);
            Assert.Equal("failed", row.Status);
            Assert.Equal(ExpiredText, row.ResultStatus);
            Assert.Equal(ExpiredText, row.ErrorText);
            Assert.True(row.Completed);
            Assert.True(row.ArgsNull, "a row that is never run must not keep its args");
        });
    }

    [Fact]
    public async Task AFreshPendingRow_IsRun()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            var fresh = await QueueAsync(connection, 14, "purge_now", ct);
            var host = new PurgeRecordingHost();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var executor = new DarlingCommandExecutor(postgres, host, "expiry-test", null);

            Assert.True(await executor.PollOnceAsync(ct));

            Assert.Equal(1, host.PurgeCalls);
            Assert.Equal("succeeded", (await ReadAsync(connection, fresh, ct)).Status);
        });
    }

    [Fact]
    public async Task AnExpiredRowFollowedByAFreshOne_FailsTheOldOne_AndRunsTheFreshOne()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            var expired = await QueueAsync(connection, 40, "purge_now", ct);
            var alsoExpired = await QueueAsync(connection, 20, "purge_now", ct);
            var fresh = await QueueAsync(connection, 1, "purge_now", ct);
            var host = new PurgeRecordingHost();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var executor = new DarlingCommandExecutor(postgres, host, "expiry-test", null);

            Assert.True(await executor.PollOnceAsync(ct));

            Assert.Equal(1, host.PurgeCalls);
            Assert.Equal(ExpiredText, (await ReadAsync(connection, expired, ct)).ErrorText);
            Assert.Equal(ExpiredText, (await ReadAsync(connection, alsoExpired, ct)).ErrorText);
            Assert.Equal("succeeded", (await ReadAsync(connection, fresh, ct)).Status);
            Assert.False(await executor.PollOnceAsync(ct), "the queue is drained");
            Assert.Equal(1, host.PurgeCalls);
        });
    }

    [Theory]
    [InlineData("Pacific/Kiritimati")]
    [InlineData("Etc/GMT+12")]
    public async Task TheOutcomeDoesNotDependOnTheClockOrZoneOfTheServiceHost(string sessionTimeZone)
    {
        /* The service host's clock and zone never enter the claim: the age is the store's own now() against the store's own
           created_at. A session in a zone 14 hours ahead of UTC, or 12 behind, must reach the same outcome. */
        await RunAsync(async (scratch, connection, ct) =>
        {
            var expired = await QueueAsync(connection, 16, "purge_now", ct);
            var fresh = await QueueAsync(connection, 14, "purge_now", ct);
            var host = new PurgeRecordingHost();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString + ";Options=-c timezone=" + sessionTimeZone);
            var executor = new DarlingCommandExecutor(postgres, host, "expiry-test", null);

            Assert.True(await executor.PollOnceAsync(ct));

            Assert.Equal(1, host.PurgeCalls);
            Assert.Equal(ExpiredText, (await ReadAsync(connection, expired, ct)).ErrorText);
            Assert.Equal("succeeded", (await ReadAsync(connection, fresh, ct)).Status);
        });
    }

    [Fact]
    public async Task TheViewerCancelsItsOwnPendingRow_WhenItStopsWaiting()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* test_connect carries a credential blob in args_json: the cancel must drop it. */
            var id = await viewer.EnqueueCommandAsync("test_connect", null, "{\"name\":\"x\",\"encryptedPassword\":\"blob\"}", "tester", ct);

            var result = await viewer.PollCommandResultAsync(id, TimeSpan.FromMilliseconds(600), ct);

            Assert.NotNull(result);
            Assert.Equal("failed", result!.Status);
            Assert.Equal(CancelledText, result.ResultStatus);
            var row = await ReadAsync(connection, id, ct);
            Assert.Equal("failed", row.Status);
            Assert.Equal(CancelledText, row.ErrorText);
            Assert.True(row.ArgsNull);
        });
    }

    [Fact]
    public async Task TheViewerLeavesAnInProgressRowAlone_AndReportsItAsRunning()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var executor = new DarlingCommandExecutor(postgres, new PurgeRecordingHost(), "expiry-test", null);

            var id = await viewer.EnqueueCommandAsync("purge_now", null, null, "tester", ct);
            var claimed = await executor.ClaimNextAsync(ct);
            Assert.Equal(id, claimed!.CommandId);

            /* The service holds the claim and has not reported: the Viewer must not cancel it. */
            var result = await viewer.PollCommandResultAsync(id, TimeSpan.FromMilliseconds(600), ct);

            Assert.Null(result);
            Assert.False(await viewer.CancelPendingCommandAsync(id, ct));
            var row = await ReadAsync(connection, id, ct);
            Assert.Equal("in_progress", row.Status);
            Assert.Null(row.ErrorText);
        });
    }

    [Fact]
    public async Task TheViewerCancelsItsRow_WhenTheCallerStopsWaiting()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var id = await viewer.EnqueueCommandAsync("pause", null, null, "tester", ct);

            using var stop = new CancellationTokenSource();
            var waiting = viewer.PollCommandResultAsync(id, TimeSpan.FromSeconds(30), stop.Token);
            await stop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

            Assert.Equal(CancelledText, (await ReadAsync(connection, id, ct)).ErrorText);
        });
    }

    [Fact]
    public async Task RunCommandAsync_ForEveryCommandTheViewerQueues_LeavesNoPendingRowWhenItTimesOut()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            foreach (var type in new[] { "pause", "resume", "snapshot_now", "analyze_now", "purge_now", "undo_plan_change" })
            {
                var result = await viewer.RunCommandAsync(type, null, null, "tester", TimeSpan.FromMilliseconds(500), ct);
                Assert.Equal(CancelledText, result!.ResultStatus);
            }

            await using var count = new NpgsqlCommand("SELECT count(*) FROM config.config_command WHERE status = 'pending'", connection);
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }
}
