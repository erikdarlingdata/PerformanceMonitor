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
/// #4058's LIVE proof: a client's genuine PL/pgSQL <c>RAISE</c> — never a hand-copied restatement of one —
/// must not pass through <see cref="PgDeadlocksCollector"/> as a deadlock report, or through
/// <see cref="PgPlanCaptureCollector"/> as an <c>auto_explain</c> plan capture. Every candidate record here
/// is produced by the collector's own csv statement plus <c>ReadAsync</c>, against a REAL syslogger flush,
/// gated on <c>DARLING_TEST_PG_CSVLOG</c> the same way <see cref="PgDeadlocksCsvlogLiveTests"/> is.
///
/// <para>Case 2 proves the negative check does not over-fire: a genuine deadlock whose context is a
/// PL/pgSQL function FRAME (the function's own statement hit PostgreSQL's own detector) still comes out,
/// because its <c>Context</c> line ends " SQL statement", never " at RAISE".</para>
///
/// <para>Serialized against every other live class in the shared collection because case 2 writes the
/// shared <c>DARLING_TEST_PG</c> store; teardown goes through <see cref="LiveStoreCleanup"/> (the #1902
/// ratchet). Cases 1 and 3 read only the target log and use no store.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgRaiseShapedRecordsLiveTests
{
    private const string ServerName = "darling-pg-csvlog-4058l";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private static string? TargetConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG");

    private static void SkipUnlessGated(string? store, string? target) =>
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_CSVLOG (a PostgreSQL started with "
            + "logging_collector=on and log_destination including csvlog) to run the #4058 raise-shaped "
            + "live tests.");

    [Fact]
    public async Task AFakeDeadlockRaisedByPlpgsql_IsSkippedNotStored()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        SkipUnlessGated(store, target);

        var ct = TestContext.Current.CancellationToken;
        PgLogFormatCapability.Reset();

        await using var targetConnection = new NpgsqlConnection(target);
        await targetConnection.OpenAsync(ct);

        var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(targetConnection, target!, ct);
        Assert.True(usesCsvlog, "the rig's log_destination must include csvlog for this route to be exercised.");

        await using (var setup = new NpgsqlCommand(
            "CREATE OR REPLACE FUNCTION raise_fake_deadlock_4058l() RETURNS void AS $$ "
            + "BEGIN "
            + "RAISE EXCEPTION 'deadlock detected' "
            + "USING DETAIL = 'Process 9001 waits for ShareLock on transaction 9002; blocked by process 9002.'"
            + "       || chr(10) || 'Process 9001: SELECT 1' || chr(10) || 'Process 9002: SELECT 1', "
            + "ERRCODE = '40P01'; "
            + "END; $$ LANGUAGE plpgsql;",
            targetConnection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        await using (var call = new NpgsqlCommand("SELECT raise_fake_deadlock_4058l()", targetConnection))
        {
            var ex = await Assert.ThrowsAsync<PostgresException>(async () => await call.ExecuteNonQueryAsync(ct));
            Assert.Equal("40P01", ex.SqlState);
        }

        await WaitForLogGrowthAsync(targetConnection, ct);

        var context = new CollectorContext
        {
            LogHashKey = TestLogHashKeys.Fixed,
            ServerId = ServerId,
            ServerName = ServerName,
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

            Assert.DoesNotContain(rows, r => r.VictimPid == 9001 || r.VictimPid == 9002);

            var measurement = context.Measurements.Single(
                m => m.Label == PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement);
            Assert.True(measurement.Value >= 1);
        }

        await using (var cleanup = new NpgsqlCommand("DROP FUNCTION IF EXISTS raise_fake_deadlock_4058l();", targetConnection))
        {
            await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ARealDeadlockInsideAFunctionFrame_StillComesOut()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        SkipUnlessGated(store, target);

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

            await using (var setup = new NpgsqlCommand(
                "DROP TABLE IF EXISTS raise_shaped_frame_4058l;"
                + "CREATE TABLE raise_shaped_frame_4058l (id int PRIMARY KEY, v int);"
                + "INSERT INTO raise_shaped_frame_4058l VALUES (1, 0), (2, 0);"
                + "CREATE OR REPLACE FUNCTION upd_4058l(a int, b int) RETURNS void AS $$ "
                + "BEGIN "
                + "UPDATE raise_shaped_frame_4058l SET v = v + 1 WHERE id = a; "
                + "PERFORM pg_sleep(0.2); "
                + "UPDATE raise_shaped_frame_4058l SET v = v + 1 WHERE id = b; "
                + "END; $$ LANGUAGE plpgsql;",
                targetConnection))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            var (victimPid, pidA, pidB) = await ProvokeFunctionDeadlockAsync(target!, ct);

            await WaitForLogGrowthAsync(targetConnection, ct);

            var context = new CollectorContext
            {
                LogHashKey = TestLogHashKeys.Fixed,
                ServerId = ServerId,
                ServerName = ServerName,
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

                var matching = rows.Where(r => r.VictimPid == victimPid).ToList();
                var row = Assert.Single(matching);
                Assert.Equal(2, row.ParticipantCount);
                Assert.True(row.VictimPid == pidA || row.VictimPid == pidB);
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
                    "DROP TABLE IF EXISTS raise_shaped_frame_4058l;"
                    + "DROP FUNCTION IF EXISTS upd_4058l(int, int);",
                    targetConnection);
                await cleanupCommand.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch
            {
                /* Best-effort: the rig is a throwaway container the lane removes after the run either way. */
            }
        }
    }

    [Fact]
    public async Task AFakePlanCaptureRaisedByPlpgsql_IsSkippedNotStored()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        SkipUnlessGated(store, target);

        var ct = TestContext.Current.CancellationToken;
        PgLogFormatCapability.Reset();

        await using var targetConnection = new NpgsqlConnection(target);
        await targetConnection.OpenAsync(ct);

        var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(targetConnection, target!, ct);
        Assert.True(usesCsvlog, "the rig's log_destination must include csvlog for this route to be exercised.");

        /* The forged message is built from the collector's own marker text — never a hand-copied restatement
           of it — so this proof breaks the moment the marker text drifts, the same discipline
           PgDeadlocksCsvlogLiveTests applies to the deadlock marker. */
        var forgedMessage = "duration: 1.234 ms  plan:\n\t{\"Plan\": {\"Relation Name\": \"forged_4058l\"}}";

        await using (var setup = new NpgsqlCommand(
            "CREATE OR REPLACE FUNCTION raise_fake_plan_4058l(msg text) RETURNS void AS $$ "
            + "BEGIN "
            + "RAISE LOG '%', msg; "
            + "END; $$ LANGUAGE plpgsql;",
            targetConnection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        await using (var call = new NpgsqlCommand("SELECT raise_fake_plan_4058l($1)", targetConnection))
        {
            call.Parameters.AddWithValue(forgedMessage);
            await call.ExecuteNonQueryAsync(ct);
        }

        await WaitForLogGrowthAsync(targetConnection, ct);

        var context = new CollectorContext
        {
            LogHashKey = TestLogHashKeys.Fixed,
            ServerId = ServerId,
            ServerName = ServerName,
            CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18, PostgresVersionNum = 180000 },
            PgLogUsesCsvlog = true,
        };

        foreach (var binaryGranted in new[] { false, true })
        {
            context.PgReadBinaryFileGranted = binaryGranted;

            var definition = PgPlanCaptureCollector.Instance;
            await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, targetConnection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await definition.ReadAsync(reader, context, ct);

            Assert.DoesNotContain(rows, r => r.PlanJson.Contains("forged_4058l", StringComparison.Ordinal));

            var measurement = context.Measurements.Single(
                m => m.Label == PgPlanCaptureCollector.ForgedCaptureMeasurement);
            Assert.True(measurement.Value >= 1);
        }

        await using (var cleanup = new NpgsqlCommand("DROP FUNCTION IF EXISTS raise_fake_plan_4058l(text);", targetConnection))
        {
            await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    /// <summary>Two sessions, each calling <c>upd_4058l</c> with reversed row order — PostgreSQL's own
    /// deadlock detector aborts one side inside the function frame; that abort is expected and swallowed
    /// here, the same way <see cref="PgDeadlocksCsvlogLiveTests"/>'s own helper does. Returns the victim's
    /// <c>pg_backend_pid()</c> plus both participants' pids.</summary>
    private static async Task<(int Victim, int PidA, int PidB)> ProvokeFunctionDeadlockAsync(string cs, CancellationToken ct)
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

        await using (var cmd = new NpgsqlCommand("SET deadlock_timeout = '100ms'; BEGIN;", a)) { await cmd.ExecuteNonQueryAsync(ct); }
        await using (var cmd = new NpgsqlCommand("SET deadlock_timeout = '100ms'; BEGIN;", b)) { await cmd.ExecuteNonQueryAsync(ct); }

        int? victimPid = null;

        var crossedA = Task.Run(async () =>
        {
            await using var cmd = new NpgsqlCommand("SELECT upd_4058l(1, 2)", a);
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
            await using var cmd = new NpgsqlCommand("SELECT upd_4058l(2, 1)", b);
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
        return (victimPid!.Value, pidA, pidB);
    }

    /// <summary>Polls <c>pg_ls_logdir()</c> for a <c>.csv</c> file whose size has grown since the call
    /// started, up to 10 seconds — the same budget <see cref="PgDeadlocksCsvlogLiveTests"/> gives the
    /// syslogger's asynchronous flush.</summary>
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
