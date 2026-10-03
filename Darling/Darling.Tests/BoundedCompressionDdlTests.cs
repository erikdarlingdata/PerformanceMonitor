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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The compression-settings ALTERs on the hourly pass wait for their AccessExclusiveLock at most
/// <see cref="TimescaleSupport.HourlyDdlLockTimeout"/>, so a long reader cannot queue every collector write
/// behind them (#4970). Source pins for the wiring, a live lock-holder pin for the behaviour, and the rule for a
/// failed settings read.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
public sealed class BoundedCompressionDdlTests
{
    private static string Storage() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.cs").Replace("\r\n", "\n");

    private static string Body(string text, string signature)
    {
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"could not locate {signature}");
        var end = text.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"could not find the end of {signature}");
        return text.Substring(start, end - start);
    }

    [Fact]
    public void TheHelper_RunsInsideATransaction_WithASetLocalLockTimeout_AndCatchesLockNotAvailable()
    {
        var helper = Body(Storage(), "internal static async Task<BoundedDdlOutcome> TryRunBoundedDdlAsync(");
        Assert.Contains("BeginTransactionAsync", helper, StringComparison.Ordinal);
        Assert.Contains("SET LOCAL lock_timeout", helper, StringComparison.Ordinal);
        Assert.Contains("PostgresErrorCodes.LockNotAvailable", helper, StringComparison.Ordinal);
        Assert.Contains("CommitAsync", helper, StringComparison.Ordinal);
    }

    [Fact]
    public void BothCompressionAlters_GoThroughTheHelper_NotAnUnboundedCommand()
    {
        var text = Storage();
        var collector = Body(text, "public static async Task<int> ApplyCompressionPolicyAsync(");
        var aggregate = Body(text, "public static async Task<int> EnsureAggregateCompressionAsync(");

        Assert.Contains("TryRunBoundedDdlAsync(", collector, StringComparison.Ordinal);
        Assert.Contains("EnableCompressionSql(schema)", collector, StringComparison.Ordinal);
        Assert.DoesNotContain("new NpgsqlCommand(EnableCompressionSql(schema)", collector, StringComparison.Ordinal);

        Assert.Contains("TryRunBoundedDdlAsync(", aggregate, StringComparison.Ordinal);
        Assert.DoesNotContain("new NpgsqlCommand(EnableAggregateCompressionSql(view)", aggregate, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHourlyPass_SkipsTheEnableAlters_AfterAFailedRead_AndTheStartPathDoesNot()
    {
        var text = Storage();
        var collector = Body(text, "public static async Task<int> ApplyCompressionPolicyAsync(");
        Assert.Contains("bool hourly, CancellationToken cancellationToken", collector, StringComparison.Ordinal);
        Assert.Contains("var skipEnable = hourly && converged is null;", collector, StringComparison.Ordinal);
        Assert.Contains("if (skipEnable)", collector, StringComparison.Ordinal);

        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n");
        Assert.Contains("ApplyCompressionPolicyAsync(connection, logger, hourly: true, ct)", worker, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFailedSettingsRead_SkipsTheAltersOnTheHourlyPass_AndIssuesThemOnTheStartPath()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live failed-read pin.");
        var ct = TestContext.Current.CancellationToken;

        /* A scratch database with no TimescaleDB extension: the settings read fails (no timescaledb_information
           schema), which is the failed-read case without a mock. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecAsync(connection, "DROP EXTENSION IF EXISTS timescaledb CASCADE", ct);

        var hourlyLog = new CapturingTestLogger();
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, hourlyLog, hourly: true, ct);
        Assert.Equal(1, hourlyLog.Lines.Count(l => l.Contains("skipping the compression-enable ALTERs", StringComparison.Ordinal)));
        Assert.DoesNotContain(hourlyLog.Lines, l => l.Contains("compression settings on collect.", StringComparison.Ordinal));

        var startLog = new CapturingTestLogger();
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, startLog, ct);
        Assert.DoesNotContain(startLog.Lines, l => l.Contains("skipping the compression-enable ALTERs", StringComparison.Ordinal));
        Assert.Contains(startLog.Lines, l => l.Contains("compression settings on collect.", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ALongReader_CannotStallTheHourlyEnableAlter_OrTheWritesBehindIt_AndTheNextPassConverges()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live lock-holder pin.");
        var ct = TestContext.Current.CancellationToken;
        const string Table = "wait_stats";

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        Assert.DoesNotContain(Table, await TimescaleSupport.ReadTablesNeedingCompressionEnableAsync(connection, null, ct) ?? throw new InvalidOperationException("read failed"));

        using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        using var writer = new NpgsqlConnection(scratch.ConnectionString);
        await writer.OpenAsync(ct);

        var log = new CapturingTestLogger();
        var holding = true;
        try
        {
            await ExecAsync(holder, "BEGIN", ct);
            await ExecAsync(holder, $"SELECT 1 FROM collect.{Table} LIMIT 1", ct);

            var clock = Stopwatch.StartNew();
            var pass = Task.Run(() => TimescaleSupport.ApplyCompressionPolicyAsync(connection, log, hourly: true, ct), ct);

            /* The INSERT is timed while the ALTER is still queued for its lock. */
            using var observer = new NpgsqlConnection(scratch.ConnectionString);
            await observer.OpenAsync(ct);
            var waiting = false;
            var pollClock = Stopwatch.StartNew();
            while (!waiting && pollClock.Elapsed < TimeSpan.FromSeconds(5))
            {
                using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE l.mode = 'AccessExclusiveLock' AND NOT l.granted AND n.nspname = 'collect' AND c.relname = @t", observer);
                probe.Parameters.AddWithValue("t", Table);
                waiting = Convert.ToInt64(await probe.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0;
                if (!waiting)
                {
                    await Task.Delay(50, ct);
                }
            }

            Assert.True(waiting, $"the ALTER on collect.{Table} never showed an ungranted AccessExclusiveLock request within 5 s");
            var lockTimeout = TimeSpan.FromSeconds(int.Parse(TimescaleSupport.HourlyDdlLockTimeout.TrimEnd('s'), System.Globalization.CultureInfo.InvariantCulture));
            var insertClock = Stopwatch.StartNew();
            await ExecAsync(writer,
                $"INSERT INTO collect.{Table} (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds) VALUES (1, now(), 1, 's', 'W', 1, 1, 1)", ct);
            Assert.True(insertClock.Elapsed < lockTimeout + TimeSpan.FromSeconds(1.5), $"the insert must not queue behind the waiting ALTER, took {insertClock.Elapsed}");

            await pass;
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"the pass must return in a bounded time, took {clock.Elapsed}");

            Assert.DoesNotContain(Table, await TimescaleSupport.ReadTablesNeedingCompressionEnableAsync(connection, null, ct) ?? throw new InvalidOperationException("read failed"));
            Assert.Equal(1, log.Lines.Count(l => l.Contains($"collect.{Table}", StringComparison.Ordinal) && l.Contains("not changed this pass", StringComparison.Ordinal)));

            await ExecAsync(holder, "ROLLBACK", ct);
            holding = false;
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, log, hourly: true, ct);
            var after = await TimescaleSupport.ReadTablesNeedingCompressionEnableAsync(connection, null, ct) ?? throw new InvalidOperationException("read failed");
            Assert.Contains(Table, after);
        }
        finally
        {
            if (holding)
            {
                await ExecAsync(holder, "ROLLBACK", CancellationTokenNone());
            }
        }
    }

    private static System.Threading.CancellationToken CancellationTokenNone() => System.Threading.CancellationToken.None;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
