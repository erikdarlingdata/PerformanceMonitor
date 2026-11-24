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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The service start history (#9415) read back from a scratch store: the previous stop and the current flag across starts, a migration between
/// starts, a failed rung, the 10 and 30 minute snapshots, the log tally and the retention. Each fact seeds the source (a recorder driven by
/// its clock seam, a tally, the real migration runner over a small ladder), flushes it, and reads the rows back through
/// <see cref="ServiceStartHistory.ReadAsync"/>.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class ServiceStartHistoryLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the service start history tests.";

    private static readonly DateTime T0 = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServiceStartRecorder NewRecorder(
        ServiceLogTally tally, string host, DateTime processStartedUtc, Func<DateTime> clock, string version = "3.14.0-test.1") =>
        new(tally, Guid.NewGuid(), host, version, "0123abc", processStartedUtc, clock);

    private static Task<IReadOnlyList<ServiceStartRecord>> ReadAllAsync(NpgsqlDataSource source, CancellationToken ct) =>
        ServiceStartHistory.ReadAsync(source, 100, serviceHost: null, ct);

    /// <summary>
    /// A second start's previous stop is the first start's last flush, and only the newest start of each host is current. A start of another
    /// host neither lends its flush time as a previous stop nor takes the current flag from this host.
    /// </summary>
    [Fact]
    public async Task TheSecondStart_TakesTheFirstOnesLastFlushAsItsPreviousStop_AndOnlyTheNewestPerHostIsCurrent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = T0;
            var tally = new ServiceLogTally();

            /* Another host, flushed after the first start's last flush, so a lookup that ignored the host would pick it up. */
            using var first = NewRecorder(tally, "host-a", T0, () => now);
            using var other = NewRecorder(tally, "host-b", T0.AddMinutes(2), () => now);
            now = T0.AddMinutes(1);
            first.StoreReady();
            Assert.True(await first.FlushAsync(source, NullLogger.Instance, ct));
            now = T0.AddMinutes(7);
            Assert.True(await first.FlushAsync(source, NullLogger.Instance, ct));
            now = T0.AddMinutes(9);
            Assert.True(await other.FlushAsync(source, NullLogger.Instance, ct));

            /* The first start's last flush (T0 + 7 minutes) is when it stopped. */
            using var second = NewRecorder(tally, "host-a", T0.AddHours(1), () => now, version: "3.14.0-test.2");
            now = T0.AddHours(1).AddMinutes(1);
            second.StoreReady();
            Assert.True(await second.FlushAsync(source, NullLogger.Instance, ct));

            var rows = await ReadAllAsync(source, ct);
            Assert.Equal(3, rows.Count);
            var firstRow = rows.Single(r => r.Start.StartId == first.StartId);
            var secondRow = rows.Single(r => r.Start.StartId == second.StartId);
            var otherRow = rows.Single(r => r.Start.StartId == other.StartId);

            Assert.Null(firstRow.Start.PreviousStopUtc);
            Assert.Equal(T0.AddMinutes(7), secondRow.Start.PreviousStopUtc);
            Assert.Null(otherRow.Start.PreviousStopUtc);

            Assert.False(firstRow.Current);
            Assert.True(secondRow.Current);
            Assert.True(otherRow.Current);

            Assert.Equal("3.14.0-test.1", firstRow.Start.ProductVersion);
            Assert.Equal("3.14.0-test.2", secondRow.Start.ProductVersion);
            Assert.Equal(T0.AddMinutes(7), firstRow.Start.LastFlushUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A start that applies migrations writes one rung row per applied rung, with each rung's elapsed time, and the earlier start that applied
    /// none has no rung rows. The real runner drives a small ladder above the store's top rung.
    /// </summary>
    [Fact]
    public async Task AMigrationBetweenStarts_RecordsOneRungRowPerAppliedRungWithItsTiming()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = T0;
            var tally = new ServiceLogTally();

            using var first = NewRecorder(tally, "host-a", T0, () => now);
            now = T0.AddMinutes(1);
            first.StoreReady();
            Assert.True(await first.FlushAsync(source, NullLogger.Instance, ct));

            var top = PgMigrations.Scripts[^1].Version;
            var ladder = new[]
            {
                new PgMigrations.Migration(top + 1, "slow-rung", "SELECT pg_sleep(0.4);"),
                new PgMigrations.Migration(top + 2, "quick-rung", "SELECT 1;"),
            };

            using var second = NewRecorder(tally, "host-a", T0.AddHours(1), () => now);
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                var applied = await PgMigrations.ApplyLadderForTestsAsync(connection, null, ladder, 60, ct, second);
                Assert.Equal(2, applied);
            }

            now = T0.AddHours(1).AddMinutes(1);
            second.StoreReady();
            Assert.True(await second.FlushAsync(source, NullLogger.Instance, ct));

            var rows = await ReadAllAsync(source, ct);
            var firstRow = rows.Single(r => r.Start.StartId == first.StartId);
            var secondRow = rows.Single(r => r.Start.StartId == second.StartId);

            Assert.Empty(firstRow.Rungs);
            Assert.Equal(2, secondRow.Rungs.Count);
            Assert.Equal(new[] { top + 1, top + 2 }, secondRow.Rungs.Select(r => r.Version).ToArray());
            Assert.Equal(new[] { "slow-rung", "quick-rung" }, secondRow.Rungs.Select(r => r.Name).ToArray());
            Assert.All(secondRow.Rungs, r =>
            {
                Assert.Equal(MigrationRungOutcome.Applied, r.Outcome);
                Assert.Equal(1, r.Attempt);
                Assert.Null(r.ErrorText);
            });
            Assert.True(secondRow.Rungs[0].ElapsedMilliseconds >= 300, $"the slow rung slept 400 ms but is recorded as {secondRow.Rungs[0].ElapsedMilliseconds} ms");
            Assert.True(secondRow.Rungs[0].StartedUtc <= secondRow.Rungs[1].StartedUtc);
            Assert.Equal(top, secondRow.Start.SchemaVersionBefore);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// On a store with no tables yet the flush keeps what it holds and says so. The first rung makes the tables. A later rung fails, and the flush
    /// after that failed attempt writes the start row, the rung that made the tables as applied and the failed rung with its error text masked. A
    /// retry that fails again is a second attempt at the failed rung, kept beside the first.
    /// </summary>
    [Fact]
    public async Task AFailedRung_IsRecordedFailedWithItsMaskedError_AndTheStartRowSurvivesEachFailedAttempt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = T0.AddMinutes(1);
            using var recorder = NewRecorder(new ServiceLogTally(), "host-a", T0, () => now);

            /* No tables yet: nothing is written, and nothing is lost. */
            Assert.False(await recorder.FlushAsync(source, NullLogger.Instance, ct));

            var ladder = new[]
            {
                new PgMigrations.Migration(1, "make-history-tables", "CREATE SCHEMA IF NOT EXISTS collect; " + ServiceStartHistory.CreateSql + ServiceStartRungObjects.CreateSql),
                new PgMigrations.Migration(2, "broken-rung",
                    "DO $$ BEGIN RAISE EXCEPTION 'cannot reach the source with Host=db1;Password=hunter2;Port=5432'; END $$;"),
            };

            for (var attempt = 1; attempt <= 2; attempt++)
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await Assert.ThrowsAnyAsync<PostgresException>(
                    () => PgMigrations.ApplyLadderForTestsAsync(connection, null, ladder, 60, ct, recorder));
                Assert.True(await recorder.FlushAsync(source, NullLogger.Instance, ct));
            }

            var record = Assert.Single(await ReadAllAsync(source, ct));
            Assert.Equal(recorder.StartId, record.Start.StartId);
            Assert.Equal(0, record.Start.SchemaVersionBefore);
            Assert.Null(record.Start.SchemaVersionAfter);
            Assert.Null(record.Start.StoreReadyUtc);

            var made = record.Rungs.Where(r => r.Version == 1).ToList();
            Assert.NotEmpty(made);
            Assert.All(made, r => Assert.Equal(MigrationRungOutcome.Applied, r.Outcome));
            Assert.Contains(made, r => r.Attempt == 1);

            var failed = record.Rungs.Where(r => r.Version == 2).OrderBy(r => r.Attempt).ToList();
            Assert.Equal(new[] { 1, 2 }, failed.Select(r => r.Attempt).ToArray());
            Assert.All(failed, r =>
            {
                Assert.Equal(MigrationRungOutcome.Failed, r.Outcome);
                Assert.Contains("cannot reach the source", r.ErrorText, StringComparison.Ordinal);
                Assert.DoesNotContain("hunter2", r.ErrorText, StringComparison.Ordinal);
            });

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The 10 and 30 minute snapshots are taken by the first flush that finds each due and never overwritten by a later one, while the running
    /// counts keep moving.
    /// </summary>
    [Fact]
    public async Task TheTenAndThirtyMinuteSnapshots_AreWrittenOnceAndNeverOverwritten()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = T0;
            var tally = new ServiceLogTally();
            using var recorder = NewRecorder(tally, "host-a", T0, () => now);

            async Task<ServiceStartRow> FlushAtAsync(TimeSpan afterStart, int moreErrors, int moreWarns)
            {
                for (var i = 0; i < moreErrors; i++)
                {
                    tally.Record(LogLevel.Error, () => "an error");
                }

                for (var i = 0; i < moreWarns; i++)
                {
                    tally.Record(LogLevel.Warning, () => "a warning");
                }

                now = T0 + afterStart;
                Assert.True(await recorder.FlushAsync(source, NullLogger.Instance, ct));
                return (await ReadAllAsync(source, ct)).Single().Start;
            }

            var at9 = await FlushAtAsync(TimeSpan.FromMinutes(9), moreErrors: 2, moreWarns: 1);
            Assert.Null(at9.Errors10Minutes);
            Assert.Null(at9.Warns10Minutes);
            Assert.Null(at9.Errors30Minutes);

            var at11 = await FlushAtAsync(TimeSpan.FromMinutes(11), moreErrors: 3, moreWarns: 2);
            Assert.Equal(5, at11.Errors10Minutes);
            Assert.Equal(3, at11.Warns10Minutes);
            Assert.Null(at11.Errors30Minutes);
            Assert.Null(at11.Warns30Minutes);

            var at20 = await FlushAtAsync(TimeSpan.FromMinutes(20), moreErrors: 4, moreWarns: 4);
            Assert.Equal(5, at20.Errors10Minutes);
            Assert.Equal(3, at20.Warns10Minutes);
            Assert.Null(at20.Errors30Minutes);

            var at31 = await FlushAtAsync(TimeSpan.FromMinutes(31), moreErrors: 1, moreWarns: 0);
            Assert.Equal(5, at31.Errors10Minutes);
            Assert.Equal(3, at31.Warns10Minutes);
            Assert.Equal(10, at31.Errors30Minutes);
            Assert.Equal(7, at31.Warns30Minutes);

            var at45 = await FlushAtAsync(TimeSpan.FromMinutes(45), moreErrors: 6, moreWarns: 6);
            Assert.Equal(5, at45.Errors10Minutes);
            Assert.Equal(3, at45.Warns10Minutes);
            Assert.Equal(10, at45.Errors30Minutes);
            Assert.Equal(7, at45.Warns30Minutes);
            Assert.Equal(16, at45.ErrorCount);
            Assert.Equal(13, at45.WarnCount);
            Assert.Equal(T0.AddMinutes(45), at45.LastFlushUtc);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The tally keeps the first 10 ERROR and the first 10 WARN lines and counts every one, masks a connection-string password, and cuts a long
    /// line to the line length. Information lines are not counted and a critical line counts as an error. The kept lines read back from the store
    /// as they were kept.
    /// </summary>
    [Fact]
    public async Task TheLogTally_KeepsTheFirstTenOfEachLevelMaskedAndCut_AndTheStoreReturnsThem()
    {
        var tally = new ServiceLogTally();
        for (var i = 0; i < 15; i++)
        {
            var n = i;
            tally.Record(
                n == 3 ? LogLevel.Critical : LogLevel.Error,
                () => n == 3 ? "cannot connect: Host=db1;Password=hunter2;Port=5432"
                    : n == 4 ? "long " + new string('x', 800)
                    : "error " + n);
            tally.Record(
                LogLevel.Warning,
                () => n == 2 ? "retrying Host=db1;Password=hunter2;Port=5432" : "warn " + n);
            tally.Record(LogLevel.Information, () => "information " + n);
        }

        var counts = tally.Snapshot();
        Assert.Equal(15, counts.Errors);
        Assert.Equal(15, counts.Warns);
        Assert.Equal(ServiceStartHistory.FirstLinesKept, counts.FirstErrors.Count);
        Assert.Equal(ServiceStartHistory.FirstLinesKept, counts.FirstWarns.Count);
        Assert.Equal("error 0", counts.FirstErrors[0]);
        Assert.Equal("error 9", counts.FirstErrors[9]);
        Assert.Equal("warn 9", counts.FirstWarns[9]);
        Assert.DoesNotContain("error 10", counts.FirstErrors);
        Assert.DoesNotContain("warn 10", counts.FirstWarns);
        Assert.DoesNotContain("hunter2", counts.FirstErrors[3], StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", counts.FirstWarns[2], StringComparison.Ordinal);
        Assert.Equal(ServiceStartHistory.LineLength, counts.FirstErrors[4].Length);
        Assert.All(counts.FirstErrors.Concat(counts.FirstWarns), line => Assert.DoesNotContain("information", line, StringComparison.Ordinal));

        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var recorder = NewRecorder(tally, "host-a", T0, () => T0.AddMinutes(1));
            Assert.True(await recorder.FlushAsync(source, NullLogger.Instance, ct));

            var stored = Assert.Single(await ReadAllAsync(source, ct)).Start;
            Assert.Equal(15, stored.ErrorCount);
            Assert.Equal(15, stored.WarnCount);
            Assert.Equal(counts.FirstErrors, stored.FirstErrors);
            Assert.Equal(counts.FirstWarns, stored.FirstWarns);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Retention removes a start and its rungs once its last flush is older than 400 days, and keeps a start whose last flush is newer, even
    /// when the process started long before the cutoff.
    /// </summary>
    [Fact]
    public async Task Retention_RemovesStartsAndTheirRungsOlderThan400DaysFromTheLastFlush_AndKeepsTheRest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var now = new DateTime(2026, 11, 24, 12, 0, 0, DateTimeKind.Utc);
            Assert.Equal(400, ServiceStartHistory.RetentionDays);

            static ServiceStartRow Row(DateTime started, DateTime lastFlush) => new(
                Guid.NewGuid(), "host-a", "3.13.0", null, started, 300, 300, started, null, lastFlush, 0, 0,
                Array.Empty<string>(), Array.Empty<string>(), null, null, null, null);

            var gone = Row(now.AddDays(-402), now.AddDays(-401));
            var kept = Row(now.AddDays(-400), now.AddDays(-399));
            var longRunning = Row(now.AddDays(-500), now.AddDays(-1));

            foreach (var row in new[] { gone, kept, longRunning })
            {
                await ServiceStartHistory.UpsertStartAsync(source, row, ct);
                await ServiceStartHistory.InsertRungsAsync(
                    source,
                    [new ServiceStartRung(row.StartId, 300, "a-rung", 1, row.ProcessStartedUtc, 10, MigrationRungOutcome.Applied, null)],
                    ct);
            }

            await ServiceStartHistory.PurgeAsync(source, now, ct);

            var rows = await ReadAllAsync(source, ct);
            Assert.Equal(
                new[] { kept.StartId, longRunning.StartId }.OrderBy(id => id).ToArray(),
                rows.Select(r => r.Start.StartId).OrderBy(id => id).ToArray());
            Assert.All(rows, r => Assert.Single(r.Rungs));

            await using var count = source.CreateCommand("SELECT count(*) FROM collect.service_start_rungs WHERE start_id = $1");
            count.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = gone.StartId });
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
