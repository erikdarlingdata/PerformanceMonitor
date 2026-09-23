/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared tail's csvlog exclusion (#3997) against a REAL target where the stderr file and its csvlog
/// sibling both exist, both current, side by side — proving what a SQL-text pin cannot: run against a real
/// <c>pg_ls_logdir()</c> listing where both files carry the SAME modification instant (measured live on a
/// fresh 18.6 target: identical to the second), the fixed query reads the stderr file and all three log-tail
/// collectors still find their entries — a real plan, a real log event and a real deadlock, from the exact
/// production <c>BuildQuery()</c> SQL, not a fixture.
///
/// <para><b>Why this needs its own target rather than the shared <c>DARLING_TEST_PG</c> store.</b> Same
/// reason as <see cref="PgPlanCaptureLiveTests"/>: <c>logging_collector</c> and <c>log_destination</c> are
/// both settings the shared store must not carry — <c>logging_collector</c> is postmaster-context, and every
/// other live test that opens the shared store would have its own session chatter land in the csvlog file
/// this test reads. Gated on <c>DARLING_TEST_PG_CSVLOG_SIBLING</c>: a connection string to a target (any
/// Postgres 13+, self-hosted, with filesystem access) started with <c>logging_collector = on</c>,
/// <c>log_destination = 'stderr,csvlog'</c> and <c>log_line_prefix = '%m [%p] %Q '</c> in its
/// <c>postgresql.conf</c> from before its first start.</para>
/// </summary>
public sealed class PgLogTailStderrSiblingLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG_SIBLING");

    [Fact]
    public async Task TheSharedTail_ReadsTheStderrFile_AndAllThreeFamiliesStillFindTheirEntries()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG_CSVLOG_SIBLING to a Postgres connection string for a target started with " +
            "logging_collector = on, log_destination = 'stderr,csvlog' and log_line_prefix = '%m [%p] %Q ' " +
            "to run the live csvlog-sibling test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* Both siblings actually exist and are actually current on this target the moment logging_collector
           starts — every message this test provokes below is written to BOTH files, exactly the shape #3997
           describes and this test's own header measured. Nothing has to fabricate the sibling; connecting to
           a target started this way is enough for one to be there. */
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-rig-as-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            /* #4004, merged from dev after this branch started: PgLogEventsCollector.BuildQuery refuses
               without a key, since raw_line_hash and statement_fingerprint are keyed hashes now. */
            LogHashKey = TestLogHashKeys.Fixed,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 18,
                PostgresVersionNum = 180000,
            },
        };

        /* Family 1: a plan, via auto_explain — same technique as PgPlanCaptureLiveTests. */
        await using (var setup = new NpgsqlCommand(
            "LOAD 'auto_explain';" +
            "SET auto_explain.log_min_duration = 0;" +
            "SET auto_explain.log_analyze = true;" +
            "SET auto_explain.log_format = 'json';" +
            "SET compute_query_id = on;" +
            "DROP TABLE IF EXISTS log_tail_sibling_plan_3997;" +
            "CREATE TABLE log_tail_sibling_plan_3997 (id int);" +
            "INSERT INTO log_tail_sibling_plan_3997 VALUES (1);",
            connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        await using (var real = new NpgsqlCommand("SELECT * FROM log_tail_sibling_plan_3997;", connection))
        await using (var reader = await real.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
            }
        }

        /* Family 2: a log event — a duplicate-key violation the classifier recognises by SQLSTATE 23505. */
        await using (var errSetup = new NpgsqlCommand(
            "DROP TABLE IF EXISTS log_tail_sibling_unique_3997;" +
            "CREATE TABLE log_tail_sibling_unique_3997 (id int PRIMARY KEY);" +
            "INSERT INTO log_tail_sibling_unique_3997 VALUES (1);",
            connection))
        {
            await errSetup.ExecuteNonQueryAsync(ct);
        }

        await using (var dupe = new NpgsqlCommand("INSERT INTO log_tail_sibling_unique_3997 VALUES (1);", connection))
        {
            try
            {
                await dupe.ExecuteNonQueryAsync(ct);
                Assert.Fail("Expected a duplicate key violation (23505).");
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                /* Expected: this is what lands the error in the server log. */
            }
        }

        /* Family 3: a real deadlock — two sessions, reversed lock order, PostgreSQL's own detector finds it
           after deadlock_timeout (1s default; not lowered, since a live target's postgresql.conf is fixed
           before this test connects). */
        await using (var dlSetup = new NpgsqlCommand(
            "DROP TABLE IF EXISTS log_tail_sibling_dl_3997;" +
            "CREATE TABLE log_tail_sibling_dl_3997 (id int PRIMARY KEY, v int);" +
            "INSERT INTO log_tail_sibling_dl_3997 VALUES (1, 0), (2, 0);",
            connection))
        {
            await dlSetup.ExecuteNonQueryAsync(ct);
        }

        await ProvokeDeadlockAsync(cs!, ct);

        /* Now the real production SQL, against the SAME target, through the SAME connection this test used
           to provoke everything above — proving the fixed tail picks up the stderr file that this session's
           own backend just wrote to, not the csvlog sibling PostgreSQL wrote beside it. */
        var planSql = PgPlanCaptureCollector.Instance.BuildQuery(context).Text;
        var deadlockSql = PgDeadlocksCollector.Instance.BuildQuery(context).Text;
        var eventsSql = PgLogEventsCollector.Instance.BuildQuery(context).Text;

        List<PgPlanCaptureCollector.Row> planRows;
        await using (var read = new NpgsqlCommand(planSql, connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            /* Throws PgLoggingCollectorOffException or PgNoStderrLogFileException if the tail picked a
               sibling or found no stderr file — either would fail this test rather than silently pass. */
            planRows = await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, ct);
        }

        List<PgDeadlocksCollector.Row> deadlockRows;
        await using (var read = new NpgsqlCommand(deadlockSql, connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            deadlockRows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, ct);
        }

        List<PgLogEvent> eventRows;
        await using (var read = new NpgsqlCommand(eventsSql, connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            eventRows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, ct);
        }

        Assert.Contains(planRows, r => r.PlanJson.Contains("log_tail_sibling_plan_3997", StringComparison.Ordinal));
        Assert.Contains(deadlockRows, r => r.ParticipantCount == 2);
        /* Not SqlState: this target's log_line_prefix is '%m [%p] %Q ' (no %e), matching what
           PgPlanCaptureLiveTests requires for %Q attribution — so stderr-format lines carry no SQLSTATE
           field at all, only csvlog's own column does. The message text is what stderr actually carries. */
        Assert.Contains(eventRows, r => r.Message != null && r.Message.Contains("duplicate key value violates unique constraint", StringComparison.Ordinal));

        await using (var cleanup = new NpgsqlCommand(
            "DROP TABLE IF EXISTS log_tail_sibling_plan_3997;" +
            "DROP TABLE IF EXISTS log_tail_sibling_unique_3997;" +
            "DROP TABLE IF EXISTS log_tail_sibling_dl_3997;",
            connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// #4019, against a REAL target started with <c>logging_collector = on</c> and
    /// <c>log_destination = 'csvlog'</c> from its very first start. Its log directory still holds one small
    /// stderr-format file, the syslogger's own "ending log output to stderr" breadcrumb, so <c>newest</c> is
    /// not empty. Before #4019 the marker needed an empty <c>newest</c> and never fired: all three collectors
    /// read that stale file every cycle as a quiet target. The marker now reads <c>log_destination</c>, so
    /// every collector's production query must come back with it and its <c>ReadAsync</c> must refuse with
    /// <see cref="PgNoStderrLogFileException"/>. Gated on <c>DARLING_TEST_PG_CSVLOG_ONLY</c>, a connection
    /// string to such a target, which needs its own server for the same reason as the test above.
    /// </summary>
    [Fact]
    public async Task ACsvlogOnlyTarget_IsRefusedByName_EvenWithTheSysloggerStderrFileInItsLogDirectory()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG_ONLY");
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG_CSVLOG_ONLY to a Postgres connection string for a target started with " +
            "logging_collector = on, log_destination = 'csvlog' and log_timezone = 'UTC' from its first start " +
            "to run the live csvlog-only test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* #4019's precondition, measured rather than assumed: the listing really holds a stderr-format file, so
           an emptiness test alone could not have fired here. */
        await using (var listing = new NpgsqlCommand(
            @"SELECT count(*) FROM pg_catalog.pg_ls_logdir() WHERE name !~* '\.(csv|json)$'", connection))
        {
            Assert.True((long)(await listing.ExecuteScalarAsync(ct))! >= 1,
                "this csvlog-only target's log directory holds no stderr-format file, so it is not #4019's shape");
        }

        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-rig-as-target",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            LogHashKey = TestLogHashKeys.Fixed,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 18,
                PostgresVersionNum = 180000,
            },
        };

        await AssertRefusedAsync(connection, PgPlanCaptureCollector.Instance.BuildQuery(context).Text,
            async (reader, token) => await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, token), ct);
        await AssertRefusedAsync(connection, PgDeadlocksCollector.Instance.BuildQuery(context).Text,
            async (reader, token) => await PgDeadlocksCollector.Instance.ReadAsync(reader, context, token), ct);
        await AssertRefusedAsync(connection, PgLogEventsCollector.Instance.BuildQuery(context).Text,
            async (reader, token) => await PgLogEventsCollector.Instance.ReadAsync(reader, context, token), ct);
    }

    private static async Task AssertRefusedAsync(
        NpgsqlConnection connection, string sql,
        Func<NpgsqlDataReader, System.Threading.CancellationToken, Task> read, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        await Assert.ThrowsAsync<PgNoStderrLogFileException>(() => read(reader, ct));
    }

    /// <summary>Two sessions, reversed lock order on the two rows <c>log_tail_sibling_dl_3997</c> seeds.
    /// PostgreSQL's own deadlock detector aborts one side; that abort is expected and swallowed here, the
    /// same way the live plan-capture test swallows the syntax error it provokes on purpose.</summary>
    private static async Task ProvokeDeadlockAsync(string cs, System.Threading.CancellationToken ct)
    {
        await using var a = new NpgsqlConnection(cs);
        await using var b = new NpgsqlConnection(cs);
        await a.OpenAsync(ct);
        await b.OpenAsync(ct);

        await using (var cmd = new NpgsqlCommand("BEGIN;", a)) { await cmd.ExecuteNonQueryAsync(ct); }
        await using (var cmd = new NpgsqlCommand("BEGIN;", b)) { await cmd.ExecuteNonQueryAsync(ct); }

        /* A locks row 1, B locks row 2 — each holds the row the OTHER is about to ask for. */
        await using (var cmd = new NpgsqlCommand("UPDATE log_tail_sibling_dl_3997 SET v = v + 1 WHERE id = 1;", a)) { await cmd.ExecuteNonQueryAsync(ct); }
        await using (var cmd = new NpgsqlCommand("UPDATE log_tail_sibling_dl_3997 SET v = v + 1 WHERE id = 2;", b)) { await cmd.ExecuteNonQueryAsync(ct); }

        var crossedA = Task.Run(async () =>
        {
            await using var cmd = new NpgsqlCommand("UPDATE log_tail_sibling_dl_3997 SET v = v + 1 WHERE id = 2;", a);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);
        var crossedB = Task.Run(async () =>
        {
            await using var cmd = new NpgsqlCommand("UPDATE log_tail_sibling_dl_3997 SET v = v + 1 WHERE id = 1;", b);
            await cmd.ExecuteNonQueryAsync(ct);
        }, ct);

        try
        {
            await Task.WhenAll(crossedA, crossedB);
        }
        catch (PostgresException)
        {
            /* Expected: PostgreSQL's detector picks a victim and aborts its transaction. */
        }

        try
        {
            await using var rollbackA = new NpgsqlCommand("ROLLBACK;", a);
            await rollbackA.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException)
        {
        }

        try
        {
            await using var rollbackB = new NpgsqlCommand("ROLLBACK;", b);
            await rollbackB.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException)
        {
        }
    }
}
