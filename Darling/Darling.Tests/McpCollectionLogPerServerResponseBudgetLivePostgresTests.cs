/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198's own per-server pass: <c>get_collection_log</c>'s ONE-SERVER form (as opposed to the fleet form
/// #4199/#4205 already sized), at DEFAULT arguments, against a real store. Seeded to approximate a genuine
/// server's row mix rather than the optimistic all-columns-null minimum: most rows are one of two
/// server-scoped collectors (<c>sql_open_ms</c>/<c>sql_drain_ms</c>/<c>watermark_ms</c> plus the drain
/// block), a third are an ENUMERATED collector (<c>plan_fetch</c>/<c>text_fetch</c>, SQL Server target
/// only — PostgreSQL targets never run the deferred Query Store plan/text fetch this splits), and one in
/// ten is a failure carrying an <c>error_message</c>.
///
/// <para>Two shapes, not one, because the two target kinds write different rows (a SQL Server target's
/// <c>query_store</c> fills <c>plan_fetch</c>/<c>text_fetch</c>; a PostgreSQL target's collectors never
/// do) and #4198 sizes the shared default against the WIDER of the two.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class McpCollectionLogPerServerResponseBudgetLivePostgresTests
{
    /* NEGATIVE and tied to the issue number so a collision with any other live-postgres fixture's range is
       obvious on sight; nothing else in this test assembly uses -4198_7xxx (see McpFleetResponseBudgetLivePostgresTests
       for the sibling -994_ ranges this deliberately avoids). */
    private const int SqlServerTargetId = -4198_7001;
    private const int PostgresTargetId = -4198_7002;
    private const string SqlServerTargetName = "zz-4198-sqltarget";
    private const string PostgresTargetName = "zz-4198-pgtarget";
    private static readonly int[] AllIds = [SqlServerTargetId, PostgresTargetId];

    private const int RowsPerServer = 200;

    private const string ModerateError =
        "Timeout expired. The timeout period elapsed prior to completion of the operation or the monitored-server round trip is not responding.";

    [Fact]
    public async Task GetCollectionLog_PerServerDefaultCall_OnSqlServerTarget_StaysUnderTheResponseBudget()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP collection-log per-server budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, SqlServerTargetId, SqlServerTargetName, MonitoredEngineKind.SqlServer, ct);

            var now = DateTime.UtcNow;
            for (var r = 0; r < RowsPerServer; r++)
            {
                var when = now.AddMinutes(-(r + 1));
                if (r % 10 == 0)
                {
                    await InsertErrorRowAsync(connection, SqlServerTargetId, SqlServerTargetName, "query_store", when, ct);
                }
                else if (r % 3 == 0)
                {
                    await InsertEnumeratedRowAsync(connection, SqlServerTargetId, SqlServerTargetName, "query_store", when, ct);
                }
                else
                {
                    await InsertServerScopedRowAsync(connection, SqlServerTargetId, SqlServerTargetName, "wait_stats", when, ct);
                }
            }

            var defaultJson = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: SqlServerTargetName, hours_back: 24);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

            TestContext.Current.SendDiagnosticMessage(
                $"get_collection_log per-server default call, SQL Server target, {RowsPerServer} rows seeded: {defaultBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes.");

            Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
                $"Per-server default call on a SQL Server target was {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task GetCollectionLog_PerServerDefaultCall_OnPostgresTarget_StaysUnderTheResponseBudget()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP collection-log per-server budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await InsertServerAsync(connection, PostgresTargetId, PostgresTargetName, MonitoredEngineKind.Postgres, ct);

            var now = DateTime.UtcNow;
            for (var r = 0; r < RowsPerServer; r++)
            {
                var when = now.AddMinutes(-(r + 1));
                if (r % 10 == 0)
                {
                    await InsertErrorRowAsync(connection, PostgresTargetId, PostgresTargetName, "pg_stat_statements", when, ct);
                }
                else
                {
                    /* No enumerated (plan_fetch/text_fetch) rows on a PostgreSQL target: that split exists
                       only for SQL Server's deferred Query Store plan/text fetch (see this class's doc
                       comment), so every non-error row here is the server-scoped shape. */
                    var collector = r % 2 == 0 ? "pg_stat_statements" : "pg_locks";
                    await InsertServerScopedRowAsync(connection, PostgresTargetId, PostgresTargetName, collector, when, ct);
                }
            }

            var defaultJson = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: PostgresTargetName, hours_back: 24);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

            TestContext.Current.SendDiagnosticMessage(
                $"get_collection_log per-server default call, PostgreSQL target, {RowsPerServer} rows seeded: {defaultBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes.");

            Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
                $"Per-server default call on a PostgreSQL target was {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task InsertServerAsync(
        NpgsqlConnection connection, int serverId, string name, string engineKind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind, created_date)
VALUES ($1, $2, $2, TRUE, 2, $3, $4)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(engineKind);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The server-scoped shape: <c>sql_phases</c> (open/drain/watermark) plus <c>drain</c>, the
    /// columns V108/V109 added for the collectors that read one monitored server directly rather than
    /// fanning out per database.</summary>
    private static async Task InsertServerScopedRowAsync(
        NpgsqlConnection connection, int serverId, string name, string collectorName, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms,
     duckdb_duration_ms, rows_collected, sql_open_ms, sql_drain_ms, watermark_ms,
     drain_rows_read, drain_bytes_read, drain_last_read_ms, target_session_id, sweep_peer_max_ms)
VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 340, 260, 40, 5000, 180, 60, 12, 5000, 812345, 55, 771, 410)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The ENUMERATED shape: <c>plan_fetch</c> and <c>text_fetch</c>, V110's per-database deferred
    /// plan/statement-text fetch split — SQL Server's <c>query_store</c> only.</summary>
    private static async Task InsertEnumeratedRowAsync(
        NpgsqlConnection connection, int serverId, string name, string collectorName, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms,
     duckdb_duration_ms, rows_collected, sweep_peer_max_ms,
     plan_fetch_probe_ms, plan_fetch_target_ms, plan_fetch_write_ms, plan_fetch_ids_attempted, plan_fetch_probe_ids,
     text_fetch_probe_ms, text_fetch_target_ms, text_fetch_write_ms, text_fetch_ids_attempted, text_fetch_probe_ids)
VALUES ($1, $2, $3, $4, $5, 'SUCCESS', 5200, 4800, 300, 2500, 480,
        2650, 210, 80, 48, 22,
        1870, 140, 60, 36, 18)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertErrorRowAsync(
        NpgsqlConnection connection, int serverId, string name, string collectorName, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms,
     duckdb_duration_ms, rows_collected, error_message)
VALUES ($1, $2, $3, $4, $5, 'ERROR', 120, 100, 0, 0, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ModerateError);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var idList = string.Join(", ", AllIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));

        foreach (var table in new[] { "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({idList});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
