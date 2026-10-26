/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ComposeStampLiveSupport;

namespace Darling.Tests;

/// <summary>
/// #5867: a Query Store compose build that throws is recorded per hour (<c>collect.query_store_compose_stamp_failures</c>), retried next
/// tick (under the extended timeout after a statement timeout), and after <see cref="QueryStoreComposeStamp.FailuresBeforeBackoff"/>
/// failures in a row, or a timeout under the extended timeout, retried once a day instead of every tick. A success clears the record. The
/// failing build is planted with a row trigger on the rollup that raises for one hour only; a <c>query_canceled</c> raise carries SQLSTATE
/// 57014, the code a <c>statement_timeout</c> cancel carries, so the timeout path runs without waiting a minute.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class QueryStoreComposeStampFailureLiveTests
{
    /// <summary>
    /// Plants a failing build of <paramref name="hour"/>: a row trigger on the rollup that raises for that hour's rows. With
    /// <paramref name="onlyUnderTheNormalTimeout"/> it raises only while the transaction's <c>statement_timeout</c> is the normal
    /// one (60 s, shown as <c>1min</c>), so the same build succeeds under the extended timeout: an hour that needs longer than 60 s.
    /// </summary>
    private static Task PlantFailureAsync(NpgsqlConnection connection, DateTime hour, string errcode, bool onlyUnderTheNormalTimeout, CancellationToken ct) =>
        ExecAsync(connection, $@"
CREATE OR REPLACE FUNCTION collect.fail_5867() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF {(onlyUnderTheNormalTimeout ? "current_setting('statement_timeout') = '1min'" : "true")} THEN
        RAISE EXCEPTION 'planted build failure 5867 (statement_timeout %)', current_setting('statement_timeout') USING ERRCODE = '{errcode}';
    END IF;
    RETURN NEW;
END
$f$;
DROP TRIGGER IF EXISTS trg_fail_5867 ON collect.query_store_compose_stamp;
CREATE TRIGGER trg_fail_5867 BEFORE INSERT ON collect.query_store_compose_stamp
FOR EACH ROW WHEN (NEW.collection_time >= TIMESTAMP '{At(hour)}' AND NEW.collection_time < TIMESTAMP '{At(hour)}' + interval '1 hour')
EXECUTE FUNCTION collect.fail_5867();", ct);

    private static Task DropFailureAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, "DROP TRIGGER IF EXISTS trg_fail_5867 ON collect.query_store_compose_stamp", ct);

    /// <summary>Makes <paramref name="hour"/> a never-built hour again, so the next plan picks it up.</summary>
    private static Task UnbuildAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct) =>
        ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(hour)}'", ct);

    private static Task<long> HoursBuiltAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct) => CountAsync(connection,
        $"SELECT count(*) FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{At(hour)}'", ct);

    /// <summary>A day passes for the hours in backoff: their next attempt moves back by <see cref="QueryStoreComposeStamp.BackoffInterval"/>.</summary>
    private static Task ADayPassesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, "UPDATE collect.query_store_compose_stamp_failures SET retry_at = retry_at - interval '1 day', last_failed_at = last_failed_at - interval '1 day'", ct);

    /* The tick plans and records failures on the arrange step's store clock (ClockWithin(hourNow)), not the real one: the plan's due hours
       and a failure's retry_at both read the store clock, so a test that crosses the top of the hour must not make one more hour due (#6769).
       The DeferralWatch measures elapsed time and stays on the real clock. */
    private static Task<QueryStoreComposeStamp.TickResult> TickAsync(NpgsqlDataSource source, ILogger logger, DateTime hourNow, CancellationToken ct) =>
        QueryStoreComposeStamp.RunTickAsync(source, ClockWithin(hourNow), RetentionDays, logger, () => TimeSpan.Zero, ct,
            new QueryStoreComposeStamp.TickOptions(new QueryStoreComposeStamp.DeferralWatch(() => DateTime.UtcNow), ClockWithin(hourNow)));

    [Fact]
    public async Task ABuildThatFailsForAnotherReason_IsRetriedEachTickThreeTimes_ThenOnceADay_IsReadable_AndASuccessClearsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var failing = hourNow.AddHours(-5);
            var normal = hourNow.AddHours(-6);
            await PlantFailureAsync(connection, failing, "40P01", onlyUnderTheNormalTimeout: false, ct);
            await UnbuildAsync(connection, failing, ct);
            await UnbuildAsync(connection, normal, ct);
            var logger = new CapturingTestLogger();

            /* Tick 1: the normal hour builds as before and leaves no record; the failing hour is recorded once and retried next tick. */
            var first = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(1, first.Failed);
            Assert.Equal(1, first.BuiltMissing);
            Assert.Equal(0, first.BackedOff);
            Assert.Equal(1, await HoursBuiltAsync(connection, normal, ct));
            var record = Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            Assert.Equal(failing, record.Hour);
            Assert.Equal(1, record.Failures);
            Assert.Equal("40P01", record.LastSqlState);
            Assert.Contains("planted build failure 5867", record.LastReason, StringComparison.Ordinal);
            Assert.Equal(QueryStoreComposeStamp.BuildStatementTimeoutSeconds, record.LastTimeoutSeconds);
            Assert.False(record.InBackoff);
            Assert.Contains(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("is retried next tick:", StringComparison.Ordinal));

            /* Not a timeout, so the retry runs under the normal timeout. */
            var planned = Assert.Single((await QueryStoreComposeStamp.PlanAsync(connection, ClockWithin(hourNow), RetentionDays, ct, ClockWithin(hourNow))).Builds);
            Assert.Equal(new QueryStoreComposeStamp.Build(failing, false, 1, false), planned);

            /* Ticks 2 and 3: the third failure in a row moves the hour to its daily attempt. */
            Assert.Equal(1, (await TickAsync(source, logger, hourNow, ct)).Failed);
            var third = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(1, third.Failed);
            Assert.Equal(1, third.BackedOff);
            record = Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            Assert.Equal(QueryStoreComposeStamp.FailuresBeforeBackoff, record.Failures);
            Assert.True(record.InBackoff);
            Assert.Equal(record.LastFailedAt + QueryStoreComposeStamp.BackoffInterval, record.NextAttemptAt);
            var backoffLine = logger.Lines.Last(l => l.StartsWith("Warning:", StringComparison.Ordinal));
            Assert.Contains("has failed 3 time(s) in a row; it is retried once a day", backoffLine, StringComparison.Ordinal);

            /* Tick 4, the next hour: the hour in backoff is not planned, not built, and logs nothing. */
            var warnings = logger.CountAtLevel(LogLevel.Warning);
            Assert.Empty((await QueryStoreComposeStamp.PlanAsync(connection, ClockWithin(hourNow), RetentionDays, ct, ClockWithin(hourNow))).Builds);
            var fourth = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(0, fourth.Failed);
            Assert.Equal(0, fourth.Built);
            Assert.Equal(warnings, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(QueryStoreComposeStamp.FailuresBeforeBackoff, Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct)).Failures);

            /* A day later it is tried once, fails once more, and goes back to waiting a day. */
            await ADayPassesAsync(connection, ct);
            var daily = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(1, daily.Failed);
            Assert.Equal(1, daily.BackedOff);
            Assert.Equal(QueryStoreComposeStamp.FailuresBeforeBackoff + 1, Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct)).Failures);
            Assert.Equal(0, (await TickAsync(source, logger, hourNow, ct)).Failed);

            /* The cause goes away: the next daily attempt builds the hour and clears its record. */
            await DropFailureAsync(connection, ct);
            await ADayPassesAsync(connection, ct);
            var healed = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(0, healed.Failed);
            Assert.Equal(1, healed.BuiltMissing);
            Assert.Equal(1, await HoursBuiltAsync(connection, failing, ct));
            Assert.Empty(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ABuildThatTimesOut_IsRetriedOnceUnderTheExtendedTimeout_AndBuildsThere_WhichClearsItsRecord()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var failing = hourNow.AddHours(-5);
            await PlantFailureAsync(connection, failing, "query_canceled", onlyUnderTheNormalTimeout: true, ct);
            await UnbuildAsync(connection, failing, ct);
            var logger = new CapturingTestLogger();

            var first = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(1, first.Failed);
            var record = Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            Assert.Equal(QueryStoreComposeStamp.StatementTimeoutSqlState, record.LastSqlState);
            Assert.False(record.InBackoff);
            Assert.Contains(logger.Lines, l => l.Contains($"is retried next tick under a {QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds} s statement timeout", StringComparison.Ordinal));
            Assert.Equal(new QueryStoreComposeStamp.Build(failing, false, 1, true),
                Assert.Single((await QueryStoreComposeStamp.PlanAsync(connection, ClockWithin(hourNow), RetentionDays, ct, ClockWithin(hourNow))).Builds));

            /* The next tick runs the build under the extended timeout, where the planted check passes: built, and the record is gone. */
            var second = await TickAsync(source, logger, hourNow, ct);
            Assert.Equal(0, second.Failed);
            Assert.Equal(1, second.BuiltMissing);
            Assert.Equal(1, await HoursBuiltAsync(connection, failing, ct));
            Assert.Empty(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task ABuildThatTimesOutUnderTheExtendedTimeoutToo_BacksOffAtOnce_AndIsNotPlannedOnTheNextTick()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var failing = hourNow.AddHours(-5);
            await PlantFailureAsync(connection, failing, "query_canceled", onlyUnderTheNormalTimeout: false, ct);
            await UnbuildAsync(connection, failing, ct);

            Assert.Equal(0, (await TickAsync(source, NullLogger.Instance, hourNow, ct)).BackedOff);
            var second = await TickAsync(source, NullLogger.Instance, hourNow, ct);
            Assert.Equal(1, second.Failed);
            Assert.Equal(1, second.BackedOff);
            var record = Assert.Single(await QueryStoreComposeStamp.ReadFailuresAsync(connection, ct));
            Assert.Equal(2, record.Failures);
            Assert.Equal(QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds, record.LastTimeoutSeconds);
            Assert.True(record.InBackoff);

            var third = await TickAsync(source, NullLogger.Instance, hourNow, ct);
            Assert.Equal(0, third.Failed);
            Assert.Equal(0, third.Built);
            Assert.Equal(0, await HoursBuiltAsync(connection, failing, ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    /// <summary>
    /// #5871: an attempt's client timeout is the same for EVERY command in it, because the server grants each statement the whole
    /// <c>statement_timeout</c>: a command left at the short client timeout would be cancelled by the client first, with no 57014 to
    /// key the failure record on. Seen through <c>commandCreated</c>, which lists each command's timeout as it is made.
    /// </summary>
    [Fact]
    public async Task EveryCommandOfABuildAttempt_CarriesTheAttemptsClientTimeout_ExtendedOrNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            var hour = hourNow.AddHours(-5);
            await UnbuildAsync(connection, hour, ct);

            var normal = new System.Collections.Generic.List<int>();
            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(connection, hour, DateTime.UtcNow, ct, commandCreated: c => normal.Add(c.CommandTimeout)));

            await UnbuildAsync(connection, hour, ct);
            var extended = new System.Collections.Generic.List<int>();
            Assert.NotNull(await QueryStoreComposeStamp.BuildHourAsync(connection, hour, DateTime.UtcNow, ct,
                statementTimeoutSeconds: QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds, commandCreated: c => extended.Add(c.CommandTimeout)));

            /* The two attempts must differ, or the check below could pass with one timeout for everything. */
            var normalTimeout = Assert.Single(normal.Distinct());
            var extendedTimeout = Assert.Single(extended.Distinct());
            Assert.True(extendedTimeout > normalTimeout);
            Assert.True(extendedTimeout >= QueryStoreComposeStamp.ExtendedBuildStatementTimeoutSeconds);
            Assert.Equal(normal.Count, extended.Count);
            Assert.True(extended.Count > 1);
            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
