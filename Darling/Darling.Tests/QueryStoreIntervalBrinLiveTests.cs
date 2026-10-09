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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
[Collection("timing")]
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

    /* Enough rows for several COMPLETE 128-page ranges after the index exists: a handful of rows fills less than one range, and
       the last, partial range is summarized too (brin_summarize_range passes include_partial), so the count would say little. */
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

    private const string LegacyTable = "collect.query_store_interval_wide_legacy";

    private const string LegacyBrin = "collect." + Brin + "_legacy";

    [Theory]
    [InlineData(1000, 2000)]
    [InlineData(200, 1200)]
    [InlineData(1, 1001)]
    [InlineData(0, 1001)]
    [InlineData(4000, 5000)]
    [InlineData(60000, 5000)]
    [InlineData(int.MaxValue, 5000)]
    public void TheTurnOffLockWait_IsDeadlockTimeoutPlusASecond_ButNeverMoreThanFiveSeconds(int deadlockMs, int expectedMs)
    {
        Assert.Equal(expectedMs, QueryStoreIntervalBrin.TurnOffLockTimeoutMs(deadlockMs));
    }

    /// <summary>The statement shapes #5594 review round 1 depends on: a text pin, so a rewrite back to the old shape fails here without a server.</summary>
    [Fact]
    public void TheStatementShapes_StopBetweenRanges_AndCountEveryVacuumThatCannotBeCancelled()
    {
        /* brin_summarize_new_values can only be cancelled, and a cancel strands a placeholder range that no later pass summarizes. */
        Assert.Contains("brin_summarize_range", QueryStoreIntervalBrin.SummarizeIndexSql);
        Assert.Contains("clock_timestamp()", QueryStoreIntervalBrin.SummarizeIndexSql);
        Assert.DoesNotContain("brin_summarize_new_values", QueryStoreIntervalBrin.SummarizeIndexSql);

        var holders = QueryStoreIntervalBrin.AutosummarizeOnSql;
        Assert.Contains("pg_stat_progress_vacuum", holders);
        Assert.Contains("autovacuum_freeze_max_age", holders);
        Assert.Contains("autovacuum_multixact_freeze_max_age", holders);
        Assert.Contains("autovacuum worker", holders);

        /* #5594 round 2 L1: the activity text autovacuum writes for an anti-wraparound vacuum is a signal of its own, and the age
           limit is the smaller of the table's setting and the server's (LEAST), never the table's alone (COALESCE). */
        Assert.Contains("(to prevent wraparound)", holders);
        Assert.Contains(QueryStoreIntervalBrin.AntiWraparoundAgeSql, holders);
        Assert.Contains("LEAST", QueryStoreIntervalBrin.AntiWraparoundAgeSql);
        Assert.DoesNotContain("COALESCE", QueryStoreIntervalBrin.AntiWraparoundAgeSql);
    }

    /// <summary>
    /// #5594 round 2 L2: brin_summarize_range raises XX000 for an index that is gone, so XX000 plus an index that no longer resolves
    /// is a drop; XX000 for an index that is still there, or any other error, is a fault that must surface.
    /// </summary>
    [Theory]
    [InlineData("XX000", false, true)]
    [InlineData("42P01", false, true)]
    [InlineData("XX000", true, false)]
    [InlineData("42P01", true, false)]
    [InlineData("55P03", false, false)]
    [InlineData("57014", false, false)]
    [InlineData(null, false, false)]
    public void ASummarizeError_CountsAsADroppedIndex_OnlyWhenTheIndexIsGone(string? sqlState, bool indexStillExists, bool expectedDropped)
    {
        Assert.Equal(expectedDropped, QueryStoreIntervalBrin.IsDroppedIndexError(sqlState, indexStillExists));
    }

    /// <summary>V171's parent BRIN says off, so a leaf made by PARTITION OF or ATTACH clones off (#5594 review round 1).</summary>
    [Fact]
    public void TheWideRung_CreatesTheParentBrinWithAutosummarizeOff()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == QueryStoreIntervalPartitionRungTests.WideRungVersion);
        Assert.Contains("USING brin (collection_time) WITH (autosummarize = off)", rung.Sql);
        Assert.DoesNotContain("autosummarize = on", rung.Sql);
    }

    /// <summary>A second session that watches pg_locks for a request on one relation that is not granted, and how long it stayed ungranted.</summary>
    private sealed class WaiterWatch : IAsyncDisposable
    {
        private readonly Task _poll;
        private bool _stop;

        public WaiterWatch(NpgsqlConnection watcher, string relation, CancellationToken ct)
        {
            _poll = Task.Run(
                async () =>
                {
                    Stopwatch? since = null;
                    while (!Volatile.Read(ref _stop))
                    {
                        await using var command = new NpgsqlCommand(
                            $"SELECT count(*) FROM pg_locks WHERE NOT granted AND relation = '{relation}'::regclass;", watcher);
                        var waiting = (long)(await command.ExecuteScalarAsync(ct))! > 0;
                        since = waiting ? since ?? Stopwatch.StartNew() : null;
                        if (since is not null)
                        {
                            Seen = true;
                            if (since.Elapsed > Longest)
                            {
                                Longest = since.Elapsed;
                            }
                        }

                        await Task.Delay(10, ct);
                    }
                },
                ct);
        }

        /// <summary>Whether a request on the relation was ever seen waiting.</summary>
        public bool Seen { get; private set; }

        public TimeSpan Longest { get; private set; }

        public async Task StopAsync()
        {
            Volatile.Write(ref _stop, true);
            await _poll;
        }

        public async ValueTask DisposeAsync() => await StopAsync();
    }

    /// <summary>
    /// A manual VACUUM in a second session, throttled to one page per 100 ms so it is still running when the test's step
    /// asks (a manual VACUUM is never cancelled by the deadlock check). Disposing cancels it and waits for it to end, which
    /// the scratch database's drop needs.
    /// </summary>
    private sealed class SlowVacuum : IAsyncDisposable
    {
        private readonly NpgsqlConnection _connection;
        private readonly CancellationTokenSource _cancel = new();
        private readonly Task _run;

        private SlowVacuum(NpgsqlConnection connection, string table)
        {
            _connection = connection;
            _run = Task.Run(
                async () =>
                {
                    try
                    {
                        await ExecAsync(connection, "SET vacuum_cost_delay = 100; SET vacuum_cost_limit = 1;", CancellationToken.None);
                        await using var vacuum = new NpgsqlCommand($"VACUUM {table}", connection) { CommandTimeout = 300 };
                        await vacuum.ExecuteNonQueryAsync(_cancel.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                    catch (PostgresException)
                    {
                    }
                });
        }

        public static async Task<SlowVacuum> StartAsync(ScratchPostgres scratch, NpgsqlConnection probe, string table, CancellationToken ct)
        {
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var vacuum = new SlowVacuum(connection, table);
            for (var attempt = 0; attempt < 300; attempt++)
            {
                if (await CountAsync(probe, $"SELECT count(*) FROM pg_stat_progress_vacuum WHERE relid = '{table}'::regclass", ct) > 0)
                {
                    return vacuum;
                }

                await Task.Delay(100, ct);
            }

            await vacuum.DisposeAsync();
            throw new InvalidOperationException($"the throttled VACUUM of {table} never showed in pg_stat_progress_vacuum");
        }

        public async ValueTask DisposeAsync()
        {
            await _cancel.CancelAsync();
            await _run;
            await _connection.DisposeAsync();
            _cancel.Dispose();
        }
    }

    [Fact]
    public async Task TheTurnOff_WhileAManualVacuumRunsOnTheTable_LeavesTheIndexOn_AndNeverAsksForTheLock()
    {
        /* A manual VACUUM is never cancelled by the deadlock check, so an ALTER (ACCESS EXCLUSIVE on the index, which the
           VACUUM holds ROW EXCLUSIVE) could only queue the leaf's writers and readers behind it for the whole lock wait. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var watcher = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON {LegacyTable} USING brin (collection_time) WITH (autosummarize = off)", ct);
        await InsertManyAsync(connection, ct);
        await ExecAsync(connection, $"ALTER INDEX {LegacyBrin} SET (autosummarize = on)", ct);

        QueryStoreIntervalPartitions.StepResult step;
        bool waiterSeen;
        await using (var vacuum = await SlowVacuum.StartAsync(scratch, connection, LegacyTable, ct))
        {
            var watch = new WaiterWatch(watcher, LegacyBrin, ct);
            step = await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct);
            await watch.StopAsync();
            waiterSeen = watch.Seen;
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, step.Outcome);
        Assert.Contains("left on", step.Detail);
        Assert.Equal(0, step.Count);
        Assert.False(waiterSeen, "the turn-off asked for the index's lock while a manual VACUUM was running");
        Assert.Equal(1L, await CountAsync(connection, LeafBrinOnCountSql, ct));

        /* Once the VACUUM is gone the next pass turns it off. */
        var after = await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, after.Outcome);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));
    }

    [Fact]
    public async Task TheTurnOff_AReaderHoldingTheIndex_TimesOutBelowFiveSeconds_AndTheTableIsStillConverged()
    {
        /* A reader's ACCESS SHARE on the index is a lock no check sees as uncancellable, so the ALTER does ask and its lock
           wait ends the request: rolled back, reported, and (L1) the table's NEXT index is still turned off. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var reader = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON collect.query_store_interval_wide_legacy USING brin (collection_time) WITH (autosummarize = on)", ct);
        await ExecAsync(connection, $"CREATE INDEX {Brin} ON ONLY collect.query_store_interval_wide USING brin (collection_time) WITH (autosummarize = off)", ct);
        await ExecAsync(connection, $"ALTER INDEX collect.{Brin} ATTACH PARTITION {LegacyBrin}", ct);
        await PromoteAsync(connection, Wide, Now, logger, ct);

        /* The legacy leaf sorts first by name; a day leaf is the one that must not be left behind. */
        var dayLeaf = (string)(await ScalarAsync(
            connection,
            "SELECT format('%I.%I', n.nspname, i.relname) FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_namespace n ON n.oid = i.relnamespace "
            + "JOIN pg_am am ON am.oid = i.relam WHERE am.amname = 'brin' AND i.relkind = 'i' AND i.relname <> '" + Brin + "_legacy' "
            + "AND x.indrelid IN (SELECT relid FROM pg_partition_tree('collect.query_store_interval_wide'::regclass)) ORDER BY 1 LIMIT 1",
            ct))!;
        await ExecAsync(connection, $"ALTER INDEX {LegacyBrin} SET (autosummarize = on)", ct);
        await ExecAsync(connection, $"ALTER INDEX {dayLeaf} SET (autosummarize = on)", ct);
        Assert.Equal(2L, await CountAsync(connection, LeafBrinOnCountSql, ct));

        var deadlockMs = (int)await CountAsync(connection, "SELECT setting::bigint FROM pg_settings WHERE name = 'deadlock_timeout'", ct);
        var lockMs = QueryStoreIntervalBrin.TurnOffLockTimeoutMs(deadlockMs);
        QueryStoreIntervalPartitions.StepResult step;
        var elapsed = TimeSpan.Zero;
        await using (var hold = await reader.BeginTransactionAsync(ct))
        {
            await ExecAsync(reader, $"SELECT count(*) FROM {LegacyTable}", ct);
            Assert.True(
                await CountAsync(connection, $"SELECT count(*) FROM pg_locks WHERE granted AND mode = 'AccessShareLock' AND relation = '{LegacyBrin}'::regclass", ct) > 0,
                "the reader's transaction must hold the legacy BRIN index");

            var watch = Stopwatch.StartNew();
            step = await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct);
            elapsed = watch.Elapsed;
            await hold.RollbackAsync(ct);
        }

        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.RetryLater, step.Outcome);
        Assert.Equal(1, step.Count);
        Assert.Contains("1 lock timeout", step.Detail);
        Assert.True(elapsed >= TimeSpan.FromMilliseconds(lockMs - 100), $"the legacy index's turn-off returned after {elapsed.TotalMilliseconds:F0} ms, before its {lockMs} ms lock timeout");
        Assert.True(elapsed < TimeSpan.FromMilliseconds(lockMs + 3000), $"the turn-off took {elapsed.TotalMilliseconds:F0} ms against a {lockMs} ms lock timeout");
        Assert.True(logger.Lines.Exists(l => l.Level == LogLevel.Warning && l.Message.Contains("lock timeout", StringComparison.Ordinal)), logger.Dump());

        /* The day leaf, which sorts after the legacy leaf, was altered; the legacy leaf was rolled back and is still on. */
        Assert.Equal(1L, await CountAsync(connection, LeafBrinOnCountSql, ct));
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM pg_class WHERE oid = '{LegacyBrin}'::regclass AND reloptions @> ARRAY['autosummarize=on']", ct));

        /* With the reader gone the next pass finishes. */
        var after = await QueryStoreIntervalBrin.TurnOffAutosummarizeAsync(connection, Wide, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, after.Outcome);
        Assert.Equal(0L, await CountAsync(connection, LeafBrinOnCountSql, ct));
    }

    [Fact]
    public async Task TheSummarize_WhileAManualVacuumRunsOnTheTable_SkipsTheIndex_AndNeverAsksForTheLock()
    {
        /* The pg_stat_progress_vacuum skip: a throttled manual VACUUM holds SHARE UPDATE EXCLUSIVE on the table, and the
           summarize must not queue for it. */
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await using var watcher = await OpenStoreAsync(scratch, ct);
        var logger = new ListLogger();

        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON {LegacyTable} USING brin (collection_time) WITH (autosummarize = off)", ct);
        await InsertManyAsync(connection, ct);

        QueryStoreIntervalPartitions.StepResult step;
        bool waiterSeen;
        await using (var vacuum = await SlowVacuum.StartAsync(scratch, connection, LegacyTable, ct))
        {
            var watch = new WaiterWatch(watcher, LegacyTable, ct);
            step = await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, logger, ct);
            await watch.StopAsync();
            waiterSeen = watch.Seen;
        }

        Assert.Contains("skipped 1 being vacuumed", step.Detail);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, step.Outcome);
        Assert.False(waiterSeen, "the summarize asked for the table's lock while a manual VACUUM was running");

        /* Nothing was summarized, and once the VACUUM is gone the next pass does it. */
        var after = await QueryStoreIntervalBrin.SummarizeNewRangesAsync(connection, Wide, logger, ct);
        Assert.Equal(QueryStoreIntervalPartitions.StepOutcome.Done, after.Outcome);
        Assert.True(after.Count >= 1);
    }

    [Fact]
    public async Task TheSummarizeStatement_StopsBetweenRanges_AndTheNextPassFinishesWithNothingLost()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        await ExecAsync(connection, $"CREATE INDEX {Brin}_legacy ON {LegacyTable} USING brin (collection_time) WITH (autosummarize = off)", ct);
        await InsertManyAsync(connection, ct);
        var blocks = await CountAsync(connection, $"SELECT pg_relation_size('{LegacyTable}') / 8192", ct);
        var fullRanges = blocks / 128;
        var rangesWalked = (blocks + 127) / 128;
        Assert.True(fullRanges >= 2, $"expected several complete 128-block ranges; the table has {blocks} blocks");

        /* A spent budget starts no range: nothing is summarized, every range is reported as not started. */
        var (done, notStarted) = await RunSummarizeAsync(connection, LegacyBrin, 0.0, ct);
        Assert.Equal(0L, done);
        Assert.Equal(rangesWalked, notStarted);

        /* The next pass, with a budget it will not spend, does all of it (the last, partial range is summarized too: brin_summarize_range passes include_partial). */
        (done, notStarted) = await RunSummarizeAsync(connection, LegacyBrin, 60.0, ct);
        Assert.InRange(done, fullRanges - 1, rangesWalked);
        Assert.Equal(0L, notStarted);
        (done, notStarted) = await RunSummarizeAsync(connection, LegacyBrin, 60.0, ct);
        Assert.Equal((0L, 0L), (done, notStarted));

        /* A leaf dropped after the target list was read reads no rows: (0, 0), not a NULL scalar that throws. */
        (done, notStarted) = await RunSummarizeAsync(connection, "collect.no_such_brin_leaf", 60.0, ct);
        Assert.Equal((0L, 0L), (done, notStarted));
    }

    private static async Task<bool> AntiWraparoundAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT {QueryStoreIntervalBrin.AntiWraparoundAgeSql} FROM pg_class AS t WHERE t.oid = $1::regclass", connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = table });
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// #5594 round 2 L3: the anti-wraparound age fragment reads false below the table's freeze limit and true past it, uses the
    /// smaller of the table's and the server's limit, and a table with no setting of its own follows the server's.
    /// </summary>
    [Fact]
    public async Task TheAntiWraparoundAge_ReadsTrueOnlyPastTheSmallerOfTheTablesAndTheServersFreezeLimit()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);

        /* 100000 is the smallest autovacuum_freeze_max_age a table may set; 2000000000 the largest, far above the server's 200 M. */
        await ExecAsync(connection, "CREATE TABLE collect.wrap_low (c int) WITH (autovacuum_freeze_max_age = 100000)", ct);
        await ExecAsync(connection, "CREATE TABLE collect.wrap_high (c int) WITH (autovacuum_freeze_max_age = 2000000000)", ct);
        await ExecAsync(connection, "CREATE TABLE collect.wrap_plain (c int)", ct);
        await ExecAsync(
            connection,
            "CREATE PROCEDURE collect.burn_xids(n int) LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..n LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END $$",
            ct);

        /* An open transaction that holds an XID keeps VACUUM from freezing past it, so no autovacuum (the 100000 table is due for
           one as soon as it is old enough) can reset the age this test is about to build. */
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await using var holderTransaction = await holder.BeginTransactionAsync(ct);
        await using (var xid = new NpgsqlCommand("SELECT pg_current_xact_id()::text", holder, holderTransaction))
        {
            await xid.ExecuteScalarAsync(ct);
        }

        Assert.False(await AntiWraparoundAsync(connection, "collect.wrap_low", ct));
        Assert.False(await AntiWraparoundAsync(connection, "collect.wrap_high", ct));
        Assert.False(await AntiWraparoundAsync(connection, "collect.wrap_plain", ct));

        /* Just over the 100000 minimum. Commits are not flushed one by one (synchronous_commit off), and each one burns an XID. */
        var burn = Stopwatch.StartNew();
        await ExecAsync(connection, "SET synchronous_commit = off", ct);
        await using (var call = new NpgsqlCommand("CALL collect.burn_xids(100200)", connection) { CommandTimeout = 600 })
        {
            await call.ExecuteNonQueryAsync(ct);
        }

        burn.Stop();
        TestContext.Current.SendDiagnosticMessage($"burned 100200 XIDs in {burn.Elapsed.TotalSeconds:F1} s");

        Assert.True(await AntiWraparoundAsync(connection, "collect.wrap_low", ct));
        Assert.False(await AntiWraparoundAsync(connection, "collect.wrap_high", ct), "a per-table limit above the server's 200 M must not count as past it");
        Assert.False(await AntiWraparoundAsync(connection, "collect.wrap_plain", ct));

        /* The server setting cannot change without a restart, so the smaller-limit leg lowers the setting the fragment reads: a
           function earlier in search_path answers for current_setting. The table's limit (2 billion) is now the larger of the two,
           and autovacuum would use the server's, so the fragment must too; the table's alone (the old COALESCE) reads false. */
        await ExecAsync(
            connection,
            "CREATE SCHEMA shadow; "
            + "CREATE FUNCTION shadow.current_setting(text) RETURNS text LANGUAGE sql STABLE AS "
            + "$$ SELECT CASE WHEN $1 = 'autovacuum_freeze_max_age' THEN '100000' ELSE pg_catalog.current_setting($1) END $$; "
            + "SET search_path = shadow, pg_catalog",
            ct);
        Assert.True(await AntiWraparoundAsync(connection, "collect.wrap_high", ct), "the smaller of the table's and the server's limit decides");
        Assert.True(await AntiWraparoundAsync(connection, "collect.wrap_plain", ct), "a table with no limit of its own follows the server's");
    }

    private static async Task<(long Done, long NotStarted)> RunSummarizeAsync(NpgsqlConnection connection, string index, double budgetSeconds, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStoreIntervalBrin.SummarizeIndexSql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = index });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double, Value = budgetSeconds });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
