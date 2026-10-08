/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #5594: BRIN autosummarize on the Query Store interval tables must not cancel autovacuum. A BRIN summarize work item waits for
/// the table's SHARE UPDATE EXCLUSIVE lock, and after deadlock_timeout PostgreSQL cancels the VACUUM that holds it. Every BRIN
/// leaf is off (an index an earlier build made with <c>on</c> included), and the hourly summarize that replaces the work items
/// never waits as long as deadlock_timeout for a lock.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalBrinLiveTests
{
    private const string Brin = "ix_query_store_interval_wide_collection_time_brin";

    /* BRIN leaf indexes (relkind i; the partitioned parent index is relkind I and cannot be altered) of the wide table's family. */
    private const string LeafBrinFrom =
        "FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_am am ON am.oid = i.relam "
        + "WHERE am.amname = 'brin' AND i.relkind = 'i' "
        + "AND (x.indrelid = 'collect.query_store_interval_wide'::regclass "
        + "OR x.indrelid IN (SELECT relid FROM pg_partition_tree('collect.query_store_interval_wide'::regclass)))";

    private const string LeafBrinCountSql = "SELECT count(*) " + LeafBrinFrom;

    private const string LeafBrinOnCountSql = "SELECT count(*) " + LeafBrinFrom + " AND coalesce(i.reloptions @> ARRAY['autosummarize=on'], false)";

    /* Enough rows for several COMPLETE 128-page ranges after the index exists: brin_summarize_new_values leaves the last, partial
       range alone, so a handful of rows summarizes nothing. */
    private static Task InsertManyAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(
            connection,
            "INSERT INTO collect.query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, "
            + "first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc) "
            + "SELECT t, 1, 'db', g, g, 'Regular', t, t, repeat('select 1 ', 20), 1, 1000, 1, t "
            + "FROM (SELECT g, timestamp '2026-10-08 12:00:00' + g * interval '1 second' AS t FROM generate_series(1, 30000) AS g) AS s",
            ct);

    [Theory]
    [InlineData(1000, 500)]
    [InlineData(200, 100)]
    [InlineData(199, 0)]
    [InlineData(1, 0)]
    [InlineData(5000, 2000)]
    [InlineData(60000, 2000)]
    public void TheSummarizeLockWait_IsAlwaysStrictlyBelowDeadlockTimeout_OrTheStepDoesNotRun(int deadlockMs, int expectedMs)
    {
        var lockMs = QueryStoreIntervalBrin.SummarizeLockTimeoutMs(deadlockMs);
        Assert.Equal(expectedMs, lockMs);
        Assert.True(lockMs == 0 || lockMs < deadlockMs);
    }

    [Fact]
    public async Task AnUpgradedStoreWithAutosummarizeOn_EndsUpOff_OnTheLegacyTable_EveryDayLeafAndDefault()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        /* The state a pre-#5594 store is in after V171: #4862's legacy index with autosummarize on, attached to a parent index that
           was made with it on. */
        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON collect.query_store_interval_wide_legacy USING brin (collection_time) WITH (autosummarize = on)", ct);
        await ExecAsync(connection, $"CREATE INDEX {Brin} ON ONLY collect.query_store_interval_wide USING brin (collection_time) WITH (autosummarize = on)", ct);
        await ExecAsync(connection, $"ALTER INDEX collect.{Brin} ATTACH PARTITION collect.{Brin}_legacy", ct);
        Assert.Equal(1L, await CountAsync(connection, LeafBrinOnCountSql, ct));

        /* The hourly step alone (the start path runs it before the collectors): the legacy leaf goes off. */
        var turnedOff = await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, turnedOff.Outcome);
        Assert.Equal(1, turnedOff.Count);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct)).Outcome);

        /* The partitioned parent index cannot be altered, so it keeps cloning "on" into every leaf the next steps make. */
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_class WHERE oid = 'collect.{Brin}'::regclass AND relkind = 'I' AND reloptions @> ARRAY['autosummarize=on']", ct));

        /* Promotion makes the day partitions and DEFAULT by PARTITION OF, each cloning the parent's option: all off. */
        await PromoteAsync(connection, Wide, Now, logger, ct);
        var leaves = await CountAsync(connection, LeafBrinCountSql, ct);
        Assert.True(leaves >= 4, $"expected the legacy table, DEFAULT and the day partitions to have a BRIN each; found {leaves}");
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));

        /* Create-ahead makes the next days by CREATE TABLE + ATTACH, which also clones: off, in the same transaction. */
        var ahead = await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, Now.AddDays(3), logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, ahead.Outcome);
        Assert.True(await CountAsync(connection, LeafBrinCountSql, ct) > leaves);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));

        /* An index somebody turned back on (or a leaf some other path made) is turned off by the next pass, and the pass counts it. */
        await ExecAsync(connection, $"ALTER INDEX collect.{Brin}_legacy SET (autosummarize = on)", ct);
        Assert.Equal(1L, await CountAsync(connection, LeafBrinOnCountSql, ct));
        var pass = await QueryStoreIntervalPartitions.RunMaintenancePassAsync(connection, Now.AddDays(3), logger, ct);
        Assert.Equal(0, pass.Failed);
        Assert.True(pass.Changed >= 1);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));
    }

    [Fact]
    public async Task AFreshStoresIndexBuild_AndItsLeaves_AreOffFromTheStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await QueryStoreIntervalWideBrinIndex.EnsureAsync(connection, logger, ct);
        await PromoteAsync(connection, Wide, Now, logger, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, Now.AddDays(3), logger, ct);

        Assert.True(await CountAsync(connection, LeafBrinCountSql, ct) >= 4);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM pg_class WHERE oid = 'collect.{Brin}'::regclass AND reloptions @> ARRAY['autosummarize=on']", ct));
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct)).Outcome);
    }

    [Fact]
    public async Task TheSummarize_UnderAHeldShareUpdateExclusiveLock_GivesUpBelowDeadlockTimeout_AndNeverShowsAWaiterThatLong()
    {
        /* A SHARE UPDATE EXCLUSIVE lock stands in for a running autovacuum. The summarize asks for the same lock, so if it waited
           deadlock_timeout PostgreSQL would cancel the real autovacuum. It must give up first, and say so. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var blocker = await OpenStoreAsync(scratch, ct);
        await using var watcher = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON collect.query_store_interval_wide_legacy USING brin (collection_time) WITH (autosummarize = off)", ct);
        await InsertManyAsync(connection, ct);

        var deadlockMs = (int)await CountAsync(connection, "SELECT setting::bigint FROM pg_settings WHERE name = 'deadlock_timeout'", ct);
        var longestWaiter = TimeSpan.Zero;
        var stop = false;
        QueryStoreIntervalPartitions.StepResult held;
        var elapsed = TimeSpan.Zero;
        Task poll;
        await using (var hold = await blocker.BeginTransactionAsync(ct))
        {
            await ExecAsync(blocker, "LOCK TABLE collect.query_store_interval_wide_legacy IN SHARE UPDATE EXCLUSIVE MODE", ct);
            poll = Task.Run(
                async () =>
                {
                    var since = (Stopwatch?)null;
                    while (!Volatile.Read(ref stop))
                    {
                        await using var command = new NpgsqlCommand(
                            "SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = 'collect.query_store_interval_wide_legacy'::regclass;", watcher);
                        var waiting = (long)(await command.ExecuteScalarAsync(ct))! > 0;
                        since = waiting ? since ?? Stopwatch.StartNew() : null;
                        if (since is not null && since.Elapsed > longestWaiter)
                        {
                            longestWaiter = since.Elapsed;
                        }

                        await Task.Delay(10, ct);
                    }
                },
                ct);

            var watch = Stopwatch.StartNew();
            held = await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, logger, ct);
            elapsed = watch.Elapsed;
            Volatile.Write(ref stop, true);
            await poll;
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, held.Outcome);
        Assert.Contains("without the lock", held.Detail);
        Assert.True(elapsed < TimeSpan.FromMilliseconds(deadlockMs), $"the summarize took {elapsed.TotalMilliseconds:F0} ms against deadlock_timeout {deadlockMs} ms");
        Assert.True(longestWaiter < TimeSpan.FromMilliseconds(deadlockMs), $"pg_locks showed a waiter for {longestWaiter.TotalMilliseconds:F0} ms against deadlock_timeout {deadlockMs} ms");

        /* Once the lock is gone the same call summarizes the range the insert left unsummarized, and the next call finds nothing. */
        var after = await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, logger, ct);
        Assert.True(after.Outcome == QueryStoreIntervalPartitions.StepOutcome.Done, $"{after.Outcome}: {after.Detail}");
        Assert.True(after.Count >= 1);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, (await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, logger, ct)).Outcome);
    }

    [Fact]
    public async Task TheSummarize_WithADeadlockTimeoutTooShortToWaitBelow_DoesNotRun()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON collect.query_store_interval_wide_legacy USING brin (collection_time) WITH (autosummarize = off)", ct);
        await InsertAsync(connection, Wide, Now.AddHours(-2), 1, ct);

        await ExecAsync(connection, "SET deadlock_timeout = '100ms'", ct);
        var step = await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, new ListLogger(), ct);

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NotReady, step.Outcome);
        Assert.Contains("deadlock_timeout is 100 ms", step.Detail);
    }
}
