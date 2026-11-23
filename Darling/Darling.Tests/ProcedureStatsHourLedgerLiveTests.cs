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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9357 live pins, each on its own scratch store. The writer: a procedure_stats batch adds its non-restart row count to its hour
/// in the COPY's own transaction (driven through the product's <c>WriteBatchAsync</c> with rows whose interval is the fact's), and a
/// rolled-back batch leaves the ledger as it was. The guard: over rows the real writer stored and a refreshed
/// <c>procedure_stats_interval_hourly</c>, it passes when ledger and rollup agree per (server, hour), counts one mismatched hour,
/// and answers <see cref="IntervalRollupCountGuard.UncoveredResult"/> for hours before <c>counted_since</c>.
/// </summary>
public sealed class ProcedureStatsHourLedgerLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the procedure_stats hour ledger's live pins (each mints its own scratch database).";

    private const int ServerA = -945401;
    private const int ServerB = -945402;
    private const string NameA = "ProcLedgerA";
    private const string NameB = "ProcLedgerB";

    private static readonly DateTime H1 = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime H2 = H1.AddHours(3);

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    internal sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required CollectorDeltaCalculator Deltas { get; init; }

        public static async Task<Rig> OpenAsync(CancellationToken ct, bool timescale)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (timescale)
            {
                var enabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
                Assert.SkipWhen(!enabled, "The live count-guard test needs TimescaleDB.");
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
                await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
                await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            }

            var deltas = new CollectorDeltaCalculator();
            var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            return new Rig
            {
                Scratch = scratch,
                DataSource = dataSource,
                Connection = connection,
                Deltas = deltas,
                Runner = new DarlingCollectorRunner(dataSource, deltas),
            };
        }

        public static ServerRuntime Server(int serverId, string name) => new()
        {
            Config = new MonitoredServer { Name = name, Host = name },
            ConnectionString = "Server=" + name,
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = name,
            ServerId = serverId,
            EngineEdition = 3,
        };

        /// <summary>One batch through the product's <c>WriteBatchAsync</c>; a fault surfaces as the store's own exception.</summary>
        public async Task<int> WriteAsync(ServerRuntime server, DateTime collectionTime, List<ProcedureStatsCollector.Row> rows, CancellationToken ct)
        {
            var context = new CollectorContext
            {
                ServerId = server.ServerId,
                ServerName = server.StorageName,
                CollectionTime = collectionTime,
                Deltas = Deltas,
                Target = server.Target,
            };

            var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(ProcedureStatsCollector.Row)).Invoke(
                Runner,
                new object?[] { Connection, ProcedureStatsCollector.Instance, rows, server, collectionTime, context, ct })!;
            await task;
            return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One procedure row whose deltas are given, so the interval (0 = restart) is the fact's, not the calculator's.</summary>
    private static ProcedureStatsCollector.Row Row(string name, int interval) => new(
        "LedgerDb", "dbo", name, "P ", H1, H1, 1, 100, 100, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0,
        "0x" + name, "0xP" + name, null, null)
    {
        Precomputed = new ProcedureStatsCollector.ReadTimeDeltas(1, 100, 100, 1, 1, 1, 0, interval),
    };

    private static List<ProcedureStatsCollector.Row> Batch(int measured, int restart)
    {
        var rows = new List<ProcedureStatsCollector.Row>();
        for (var i = 0; i < measured; i++)
        {
            rows.Add(Row("m" + i.ToString(CultureInfo.InvariantCulture), 60));
        }

        for (var i = 0; i < restart; i++)
        {
            rows.Add(Row("r" + i.ToString(CultureInfo.InvariantCulture), 0));
        }

        return rows;
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? -1L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string LedgerN(int serverId, DateTime bucket) =>
        string.Create(CultureInfo.InvariantCulture,
            $"SELECT n FROM collect.procedure_stats_hour_ledger WHERE server_id = {serverId} AND bucket = '{bucket:yyyy-MM-dd HH:mm:ss}'");

    private static async Task<long> GuardAsync(NpgsqlConnection connection, string[]? servers, CancellationToken ct, DateTime? from = null)
    {
        await using var command = new NpgsqlCommand(IntervalRollupCountGuard.ProcedureStatsSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from ?? H1 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = H2 });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = servers is null ? DBNull.Value : servers,
        });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task SetCountedSinceAsync(NpgsqlConnection connection, DateTime since, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"UPDATE {ProcedureStatsHourLedger.StateTable} SET counted_since = $1", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = since });
        Assert.Equal(1, await command.ExecuteNonQueryAsync(ct));
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(H1);
        refresh.Parameters.AddWithValue(H2.AddHours(1));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// A batch adds its non-restart rows to its hour and restart rows add nothing: after every batch the ledger equals the raw rows the
    /// rollup would count (<c>sample_interval_seconds IS DISTINCT FROM 0</c>). A later batch in the hour adds, the next hour starts its
    /// own bucket, and a batch of restart rows alone leaves no ledger row.
    /// </summary>
    [Fact]
    public async Task ABatchAddsItsNonRestartRowCount_ToItsHour_AndRestartRowsAddNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(ct, timescale: false);
        var server = Rig.Server(ServerA, NameA);

        var bodySucceeded = false;
        try
        {
            var h10 = H1.AddHours(10);
            Assert.Equal(2, await rig.WriteAsync(server, h10.AddMinutes(1), Batch(0, 2), ct));
            Assert.Equal(-1L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            Assert.Equal(5, await rig.WriteAsync(server, h10.AddMinutes(2), Batch(3, 2), ct));
            Assert.Equal(3L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            Assert.Equal(4, await rig.WriteAsync(server, h10.AddMinutes(40), Batch(4, 0), ct));
            Assert.Equal(7L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            Assert.Equal(1, await rig.WriteAsync(server, h10.AddHours(1).AddMinutes(5), Batch(1, 0), ct));
            Assert.Equal(1L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10.AddHours(1)), ct));
            Assert.Equal(7L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            Assert.Equal(
                8L,
                await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats WHERE sample_interval_seconds IS DISTINCT FROM 0", ct));
            Assert.Equal(8L, await ScalarAsync(rig.Connection, "SELECT sum(n) FROM collect.procedure_stats_hour_ledger", ct));

            /* The query_stats ledger is not touched by a procedure_stats batch. */
            Assert.Equal(0L, await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.query_stats_hour_ledger", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A fault after the COPY (a trigger that refuses the totals write, which runs after the ledger upsert on the same transaction) rolls
    /// the whole batch back: the raw rows and the ledger are both as they were, and the next batch counts only its own rows.
    /// </summary>
    [Fact]
    public async Task ARolledBackBatch_LeavesNoLedgerChange()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(ct, timescale: false);
        var server = Rig.Server(ServerA, NameA);

        var bodySucceeded = false;
        try
        {
            var h10 = H1.AddHours(10);
            Assert.Equal(2, await rig.WriteAsync(server, h10.AddMinutes(1), Batch(2, 0), ct));
            Assert.Equal(2L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            await ExecAsync(
                rig.Connection,
                "CREATE FUNCTION collect.proc_ledger_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'totals write refused' USING ERRCODE = 'P0001'; END $$;"
                + "CREATE TRIGGER trg_proc_ledger_refuse BEFORE INSERT OR UPDATE ON collect.procedure_stats_totals FOR EACH ROW EXECUTE FUNCTION collect.proc_ledger_refuse();",
                ct);

            var fault = await Assert.ThrowsAsync<PostgresException>(() => rig.WriteAsync(server, h10.AddMinutes(2), Batch(5, 1), ct));
            Assert.Equal("P0001", fault.SqlState);
            Assert.Equal(2L, await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats", ct));
            Assert.Equal(2L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));

            await ExecAsync(rig.Connection, "DROP TRIGGER trg_proc_ledger_refuse ON collect.procedure_stats_totals; DROP FUNCTION collect.proc_ledger_refuse();", ct);
            Assert.Equal(1, await rig.WriteAsync(server, h10.AddMinutes(3), Batch(1, 0), ct));
            Assert.Equal(3L, await ScalarAsync(rig.Connection, LedgerN(ServerA, h10), ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The guard over rows the real writer stored (so the ledger is the writer's), against the refreshed hourly rollup: it passes
    /// when they agree for every (server, hour) of the window, with and without a scope; counts one mismatched hour whichever side is
    /// ahead; and answers the uncovered result for a window that starts before <c>counted_since</c> or when the state row is missing.
    /// </summary>
    [Fact]
    public async Task TheProcedureGuard_PassesWhenLedgerAndRollupAgree_RefusesAMismatchedHour_AndIsUncoveredBeforeCountedSince()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(ct, timescale: true);
        var a = Rig.Server(ServerA, NameA);
        var b = Rig.Server(ServerB, NameB);

        var bodySucceeded = false;
        try
        {
            for (var h = 0; h < 3; h++)
            {
                Assert.Equal(3, await rig.WriteAsync(a, H1.AddHours(h).AddMinutes(5), Batch(2, 1), ct));
                Assert.Equal(2, await rig.WriteAsync(b, H1.AddHours(h).AddMinutes(15), Batch(2, 0), ct));
            }

            await RefreshAsync(rig.Connection, ct);
            await SetCountedSinceAsync(rig.Connection, H1, ct);
            var scope = new[] { NameA, NameB };

            Assert.Equal(0L, await GuardAsync(rig.Connection, scope, ct));
            Assert.Equal(0L, await GuardAsync(rig.Connection, null, ct));

            /* A batch the rollup has not refreshed over: the ledger is ahead in one hour. */
            Assert.Equal(2, await rig.WriteAsync(a, H1.AddHours(1).AddMinutes(50), Batch(2, 0), ct));
            Assert.Equal(1L, await GuardAsync(rig.Connection, scope, ct));
            await RefreshAsync(rig.Connection, ct);
            Assert.Equal(0L, await GuardAsync(rig.Connection, scope, ct));

            /* A ledger count off by one in one hour, either way, is one mismatch. */
            await ExecAsync(rig.Connection, $"UPDATE collect.procedure_stats_hour_ledger SET n = n + 1 WHERE server_id = {ServerB} AND bucket = '2026-01-05 02:00:00'", ct);
            Assert.Equal(1L, await GuardAsync(rig.Connection, scope, ct));
            await ExecAsync(rig.Connection, $"UPDATE collect.procedure_stats_hour_ledger SET n = n - 2 WHERE server_id = {ServerB} AND bucket = '2026-01-05 02:00:00'", ct);
            Assert.Equal(1L, await GuardAsync(rig.Connection, scope, ct));
            await ExecAsync(rig.Connection, $"UPDATE collect.procedure_stats_hour_ledger SET n = n + 1 WHERE server_id = {ServerB} AND bucket = '2026-01-05 02:00:00'", ct);
            Assert.Equal(0L, await GuardAsync(rig.Connection, scope, ct));

            /* A ledger row for a pair the rollup lacks, and a rollup pair the ledger lacks, each fail. */
            await ExecAsync(rig.Connection, $"DELETE FROM collect.procedure_stats_hour_ledger WHERE server_id = {ServerA} AND bucket = '2026-01-05 00:00:00'", ct);
            Assert.Equal(1L, await GuardAsync(rig.Connection, scope, ct));
            await ExecAsync(rig.Connection, $"INSERT INTO collect.procedure_stats_hour_ledger (server_id, server_name, bucket, n) VALUES ({ServerA}, '{NameA}', '2026-01-05 00:00:00', 2)", ct);
            Assert.Equal(0L, await GuardAsync(rig.Connection, scope, ct));

            /* Hours before counted_since are uncovered: -1, never a pass, even with a mismatch inside the window. */
            await SetCountedSinceAsync(rig.Connection, H1.AddHours(1), ct);
            Assert.Equal(IntervalRollupCountGuard.UncoveredResult, await GuardAsync(rig.Connection, scope, ct));
            Assert.Equal(0L, await GuardAsync(rig.Connection, scope, ct, H1.AddHours(1)));
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_hour_ledger_state", ct);
            Assert.Equal(IntervalRollupCountGuard.UncoveredResult, await GuardAsync(rig.Connection, null, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt), CultureInfo.InvariantCulture));
            });
        }
    }
}
