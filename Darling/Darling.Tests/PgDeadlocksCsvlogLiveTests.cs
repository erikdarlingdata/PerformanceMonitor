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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4053 part b1's LIVE proof: <see cref="PgDeadlocksCollector"/>'s csvlog route against a REAL deadlock, a
/// REAL syslogger flush and the collector's OWN <c>BuildQuery</c>/<c>ReadAsync</c> — never a hand-copied
/// restatement of either. Gated on <c>DARLING_TEST_PG_CSVLOG</c>, the same connection string
/// <see cref="PgLogEventsCsvlogLiveTests"/> uses (a rig started with <c>logging_collector = on</c> and
/// <c>log_destination</c> including <c>csvlog</c>).
///
/// <para>Serialized against every other live class in the shared collection because it writes the shared
/// <c>DARLING_TEST_PG</c> store; teardown goes through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgDeadlocksCsvlogLiveTests
{
    private const string ServerName = "darling-pg-csvlog-b1l";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private static string? TargetConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG");

    [Fact]
    public async Task TheCsvlogRoute_FindsARealDeadlock_AndKeepsAForgedOneInsideItsOwnField()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_CSVLOG (a PostgreSQL started with logging_collector=on "
            + "and log_destination including csvlog, ideally with a short deadlock_timeout) to run the #4053 part "
            + "b1 csvlog live test.");

        var ct = TestContext.Current.CancellationToken;
        using var storeConnection = new NpgsqlConnection(store);
        await storeConnection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(storeConnection, ct);
        await DarlingMcpTestData.ExecAsync(storeConnection, ct, "DELETE FROM pg_deadlocks WHERE server_id = $1", ServerId);

        PgLogFormatCapability.Reset();

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(storeConnection, ServerId, ServerName, ct);

            await using var targetConnection = new NpgsqlConnection(target);
            await targetConnection.OpenAsync(ct);

            var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(targetConnection, target!, ct);
            Assert.True(usesCsvlog, "the rig's log_destination must include csvlog for this route to be exercised.");

            /* A REAL deadlock: two sessions, two rows, reversed lock order. PostgreSQL's own detector finds
               it and aborts one side; the victim pid is recorded so the assertion below can match it back to
               this run's own report rather than any other row the shared store might carry. */
            await using (var setup = new NpgsqlCommand(
                "DROP TABLE IF EXISTS deadlocks_csvlog_live_4053b1;"
                + "CREATE TABLE deadlocks_csvlog_live_4053b1 (id int PRIMARY KEY, v int);"
                + "INSERT INTO deadlocks_csvlog_live_4053b1 VALUES (1, 0), (2, 0);",
                targetConnection))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            var victimPid = await ProvokeDeadlockAsync(target!, ct);

            /* A SECOND session that fires an ordinary error whose message text carries the deadlock marker
               and a fake report inside a planted newline, in a quoted field — the same forgery technique
               PgLogEventsCsvlogLiveTests uses, through a failing statement rather than a raw startup packet. */
            await using (var forgeSetup = new NpgsqlCommand(
                "DROP TABLE IF EXISTS deadlock_forge_check_4053b1;"
                + "CREATE TABLE deadlock_forge_check_4053b1 (v text CHECK (1 = 0));",
                targetConnection))
            {
                await forgeSetup.ExecuteNonQueryAsync(ct);
            }

            var forgedMessage =
                "planted\n2026-09-24 00:00:00.000 UTC,\"app\",\"postgres\",9999,\"::1:1\",0.0,1,\"client backend\","
                + "2026-09-24 00:00:00 UTC,3/9,0,ERROR,40P01,\"deadlock detected\","
                + "\"Process 1 waits for ShareLock on transaction 2; blocked by process 3."
                + "\nProcess 1: SELECT 1\nProcess 3: SELECT 1\",,,,,,,\"\",\"client backend\",,0";

            await using (var forgeInsert = new NpgsqlCommand(
                "INSERT INTO deadlock_forge_check_4053b1 (v) VALUES ($1)", targetConnection))
            {
                forgeInsert.Parameters.AddWithValue(forgedMessage);
                try
                {
                    await forgeInsert.ExecuteNonQueryAsync(ct);
                    Assert.Fail("Expected the CHECK constraint to reject the planted marker text.");
                }
                catch (PostgresException ex) when (ex.SqlState == "23514")
                {
                    /* Expected: the constraint violation's error message repeats the planted value
                       verbatim, landing it — newline, fake CSV fields and all — inside ONE quoted field of
                       the real ERROR record's own message text. */
                }
            }

            await WaitForLogGrowthAsync(targetConnection, ct);

            var context = new CollectorContext
            {
                LogHashKey = TestLogHashKeys.Fixed,
                ServerId = ServerId, ServerName = ServerName,
                CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                PgLogUsesCsvlog = true,
            };

            foreach (var binaryGranted in new[] { false, true })
            {
                context.PgReadBinaryFileGranted = binaryGranted;

                var definition = PgDeadlocksCollector.Instance;
                await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, targetConnection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var rows = await definition.ReadAsync(reader, context, ct);

                /* Exactly one row for THIS run's real deadlock, matched to the victim pid PostgreSQL's own
                   detector picked, with the two-participant shape a two-session cycle always has, and a
                   graph that is not empty. */
                var matching = rows.Where(r => r.VictimPid == victimPid).ToList();
                var row = Assert.Single(matching);
                Assert.Equal(2, row.ParticipantCount);
                Assert.False(string.IsNullOrWhiteSpace(row.GraphText));

                /* No row was forged OUT of the planted newline: pid 9999 never appears, either as its own
                   report or folded into the real one. */
                Assert.DoesNotContain(rows, r => r.VictimPid == 9999);

                Assert.NotEmpty(rows);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM pg_deadlocks WHERE server_id = $1", ServerId));

            try
            {
                await using var targetConnection = new NpgsqlConnection(target);
                await targetConnection.OpenAsync(CancellationToken.None);
                await using var cleanupCommand = new NpgsqlCommand(
                    "DROP TABLE IF EXISTS deadlocks_csvlog_live_4053b1;"
                    + "DROP TABLE IF EXISTS deadlock_forge_check_4053b1;",
                    targetConnection);
                await cleanupCommand.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch
            {
                /* Best-effort: the rig is a throwaway container the lane removes after the run either way. */
            }
        }
    }

    /// <summary>Two sessions, reversed lock order on the two rows the caller seeded. PostgreSQL's own
    /// deadlock detector aborts one side; that abort is expected and swallowed here, the same way
    /// <see cref="PgLogTailStderrSiblingLiveTests"/>'s own helper does. Returns the victim's own
    /// <c>pg_backend_pid()</c>, read from the surviving session, so the test can match its row back to
    /// THIS run rather than any other deadlock a shared target might carry.</summary>
    private static async Task<int> ProvokeDeadlockAsync(string cs, CancellationToken ct)
    {
        await using var a = new NpgsqlConnection(cs);
        await using var b = new NpgsqlConnection(cs);
        await a.OpenAsync(ct);
        await b.OpenAsync(ct);

        int pidA;
        int pidB;
        await using (var cmd = new NpgsqlCommand("SELECT pg_backend_pid()", a))
        {
            pidA = (int)(await cmd.ExecuteScalarAsync(ct))!;
        }
        await using (var cmd = new NpgsqlCommand("SELECT pg_backend_pid()", b))
        {
            pidB = (int)(await cmd.ExecuteScalarAsync(ct))!;
        }

        await using (var cmd = new NpgsqlCommand("BEGIN;", a)) { await cmd.ExecuteNonQueryAsync(ct); }
        await using (var cmd = new NpgsqlCommand("BEGIN;", b)) { await cmd.ExecuteNonQueryAsync(ct); }

        /* A locks row 1, B locks row 2 — each holds the row the OTHER is about to ask for. */
        await using (var cmd = new NpgsqlCommand("UPDATE deadlocks_csvlog_live_4053b1 SET v = v + 1 WHERE id = 1;", a))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }
        await using (var cmd = new NpgsqlCommand("UPDATE deadlocks_csvlog_live_4053b1 SET v = v + 1 WHERE id = 2;", b))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        int? victimPid = null;

        var crossedA = Task.Run(async () =>
        {
            await using var cmd = new NpgsqlCommand("UPDATE deadlocks_csvlog_live_4053b1 SET v = v + 1 WHERE id = 2;", a);
            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "40P01")
            {
                victimPid = pidA;
            }
        }, ct);
        var crossedB = Task.Run(async () =>
        {
            await using var cmd = new NpgsqlCommand("UPDATE deadlocks_csvlog_live_4053b1 SET v = v + 1 WHERE id = 1;", b);
            try
            {
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "40P01")
            {
                victimPid = pidB;
            }
        }, ct);

        await Task.WhenAll(crossedA, crossedB);

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

        Assert.NotNull(victimPid);
        return victimPid!.Value;
    }

    /// <summary>
    /// Polls <c>pg_ls_logdir()</c> for a <c>.csv</c> file whose size has grown since the call started, up to
    /// 10 seconds — the syslogger flushes asynchronously and a deadlock report is larger than the forged
    /// login lines <see cref="PgLogEventsCsvlogLiveTests"/> waits on, so this test gives it a longer budget.
    /// </summary>
    private static async Task WaitForLogGrowthAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        long before;
        await using (var command = new NpgsqlCommand(
            "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.csv$'", connection))
        {
            before = (long)(await command.ExecuteScalarAsync(ct))!;
        }

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            await using (var command = new NpgsqlCommand(
                "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.csv$'", connection))
            {
                var after = (long)(await command.ExecuteScalarAsync(ct))!;
                if (after > before)
                {
                    return;
                }
            }

            await Task.Delay(200, ct);
        }
    }
}
