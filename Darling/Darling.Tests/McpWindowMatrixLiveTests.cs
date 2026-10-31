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
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one scratch database this fixture mints through ScratchPostgres holds one server, its Query Store rows and
   its collection log, and is dropped with the fixture. */
/// <summary>
/// The store behind <see cref="McpWindowMatrixLiveTests"/>: one registered server whose raw data starts three days back and whose
/// collection log starts earlier, seeded once for the whole class (#6984).
/// </summary>
public sealed class McpWindowMatrixStore : IAsyncLifetime
{
    public const int ServerId = -698401;
    public const string ServerName = McpReadToolBudgetLiveTests.ServerName;

    /// <summary>#7015: the PostgreSQL target the <c>get_pg_*</c> reads in <see cref="PgSeededTables"/> answer for, so each reaches the
    /// window-floor logic instead of the no-collection path an empty table takes.</summary>
    public const int PgServerId = -698402;
    public const string PgServerName = "mcp-window-matrix-pg";

    /// <summary>#7015: the Aurora PostgreSQL target <c>get_pg_cpu_utilization</c> answers for. That read collects Performance Insights, which only
    /// an Aurora target has, so on a stock PostgreSQL target it answers <c>not_collected</c> (an engine fact, no notice) over an empty window.</summary>
    public const int PgAuroraServerId = -698403;
    public const string PgAuroraServerName = "mcp-window-matrix-aurora";

    /// <summary>The server a <c>get_pg_*</c> read in <see cref="PgSeededTables"/> is bound to.</summary>
    public static string PgServerFor(string tool) => tool == "get_pg_cpu_utilization" ? PgAuroraServerName : PgServerName;

    /// <summary>Collectors whose runs, not table rows, the window probe reads (the read is SPARSE, see WebDataStartNote.CollectorRunsByRead): a
    /// collection_log run from the log's start on, for the PostgreSQL target.</summary>
    private static readonly string[] PgLoggedCollectors = ["pg_replication_stats"];

    /// <summary>The collector tables the PostgreSQL target is seeded for, by the read that windows on each (#7015). Every other
    /// <c>get_pg_*</c> read keeps answering for the SQL Server target, as before.</summary>
    public static readonly IReadOnlyDictionary<string, string> PgSeededTables = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_pg_cpu_utilization"] = "pg_cpu_utilization",
        ["get_pg_deadlocks"] = "pg_deadlocks",
        ["get_pg_index_usage"] = "pg_index_usage_stats",
        ["get_pg_io_stats"] = "pg_io_stats",
        ["get_pg_kernel_stats"] = "pg_kernel_stats",
        ["get_pg_lock_stats"] = "pg_lock_stats",
        ["get_pg_log_events"] = "pg_log_events",
        ["get_pg_plans"] = "pg_plan_capture",
        ["get_pg_predicate_stats"] = "pg_predicate_stats",
        ["get_pg_replication_stats"] = "pg_replication_stats",
        ["get_pg_server_config_changes"] = "pg_server_config",
        ["get_pg_wait_sampling"] = "pg_wait_sampling",
        ["get_pg_write_stats"] = "pg_write_stats",
    };

    /// <summary>Where the store's raw rows start (the Query Store baseline interval), and where the collection log starts, earlier.</summary>
    public static readonly TimeSpan RawFloorBack = TimeSpan.FromDays(3);
    public static readonly TimeSpan LogFloorBack = TimeSpan.FromHours(84);

    private ScratchPostgres? _scratch;

    public NpgsqlDataSource? DataSource { get; private set; }

    /// <summary>When the seed was written; every floor the tests compare against is measured from here, since a slow run moves "now" on.</summary>
    public DateTime SeededAt { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var ct = CancellationToken.None;
        var now = DateTime.UtcNow;
        SeededAt = now;
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct, plainPostgres: true);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* Registered when the collection log starts: the first collection the registry knows is not later than the log's first run. */
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* SQL Server 2022 (major 16): get_query_store_hints answers not_collected, bare, on an older server (the Query Store hints DMV starts there), so
               the matrix would never see its window notice on the helper's default of 15. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.servers SET created_date = $2, sql_major_version = 16 WHERE server_id = $1",
                ServerId, DarlingMcpTestData.Naive(now - LogFloorBack));

            /* Query Store: a baseline interval at the raw floor and a regressed one 30 minutes back (the 25.9 s of added CPU clears the
               regression gate), three queries each. */
            long collectionId = 0;
            foreach (var (back, executions, avgUs, interval) in new[] { (RawFloorBack, 50L, 1_000L, 1L), (TimeSpan.FromMinutes(30), 480L, 55_000L, 2L) })
            {
                for (var q = 0; q < 3; q++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                        -698400 - (++collectionId), DarlingMcpTestData.Naive(now - back), ServerId, ServerName, "TenantDb00", 1000L + q, 9L, "Regular",
                        executions, avgUs, avgUs, 100L, interval * 10 + q, "SELECT " + q, DarlingMcpTestData.Naive(now - back));
                }
            }

            /* #7010: system_health_events, the table the nine get_health_parser_* reads probe (get_query_heatmap reads the query_stats rows seeded below). A few
               rows at the raw floor and 30 minutes back, so the store holds them from three days back. The health rows carry an event type
               none of the nine reads asks for, so each read answers an empty window (its probe finds the table's coverage) instead of the
               "nothing ever collected" answer an unseeded table gives, which has no window notice by design. */
            long healthId = 0;
            foreach (var back in new[] { RawFloorBack, TimeSpan.FromMinutes(30) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    -698500 - (++healthId), DarlingMcpTestData.Naive(now - back), ServerId, ServerName, DarlingMcpTestData.Naive(now - back),
                    "matrix_seed_event", "<event name=\"matrix_seed_event\" />");
            }

            /* #7010: the raw tables the core data tools read (wait stats, CPU, tempdb, waiting tasks, query and procedure stats), a few rows
               each from LogFloorBack on, so a window past the store's data answers over rows that start there and not over an empty table. */
            long rawId = 0;
            foreach (var back in new[] { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) })
            {
                var at = DarlingMcpTestData.Naive(now - back);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, 'SOS_SCHEDULER_YIELD', 10, 5000, 100, 10, 5000, 100)", -698500 - (++rawId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, 30, 5)", -698500 - (++rawId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.tempdb_stats
    (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb,
     version_store_reserved_mb, total_reserved_mb, unallocated_mb)
VALUES ($1, $2, $3, $4, 10, 5, 2, 17, 900)", -698500 - (++rawId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.waiting_tasks
    (collection_id, collection_time, server_id, server_name, wait_type, wait_duration_ms, database_name)
VALUES ($1, $2, $3, $4, 'LCK_M_X', 250, 'TenantDb00')", -698500 - (++rawId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'TenantDb00', '0xAB01', '0xCD01', 500000, 600000, 5, 300)", -698500 - (++rawId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'TenantDb00', 'dbo', 'usp_Seeded', '0xEF01', 500000, 600000, 5, 300)", -698500 - (++rawId), at, ServerId, ServerName);
            }

            /* #7010: memory grants, client timeouts (attention events), and the two Query Store capture tables (forced plans, hints), a few rows
               each from LogFloorBack on. The two capture tables get the marker row a capture writes (the database and no plan or hint), so
               their reads answer "captured, nothing forced or hinted" over rows that start there, not the no-capture answer of an empty table. */
            long grantId = 0;
            foreach (var back in new[] { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) })
            {
                var at = DarlingMcpTestData.Naive(now - back);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, available_memory_mb, granted_memory_mb, waiter_count)
VALUES ($1, $2, $3, $4, 0, 1, 2048, 512, 0)", -698600 - (++grantId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
VALUES ($1, $2, $3, $4, 'count', 3)", -698600 - (++grantId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.query_store_forced_plans
    (collection_id, collection_time, server_id, server_name, database_name)
VALUES ($1, $2, $3, $4, 'TenantDb00')", -698600 - (++grantId), at, ServerId, ServerName);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.query_store_hints
    (collection_id, collection_time, server_id, server_name, database_name)
VALUES ($1, $2, $3, $4, 'TenantDb00')", -698600 - (++grantId), at, ServerId, ServerName);
            }

            /* #7010: the store's statement history (get_store_query_history), a few captures from LogFloorBack on with one statement each, so its
               earliest capture (the notice's floor) is where the store's history starts and the read answers rows, not the "no snapshot yet" precondition. */
            foreach (var back in new[] { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) })
            {
                var at = DarlingMcpTestData.Naive(now - back);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.store_statement_captures
    (capture_time, interval_seconds, statements_seen, statements_kept, hidden_statements, outcome)
VALUES ($1, 3600, 1, 1, 0, 'ok')", at);
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.store_statement_history
    (capture_time, interval_seconds, role_name, queryid, delta_calls, delta_total_exec_ms, delta_rows, delta_shared_blks_hit,
     delta_shared_blks_read, delta_temp_blks_written, max_exec_ms, first_seen, entry_restarted, reset_in_interval)
VALUES ($1, 3600, 'admin', 7010, 10, 250, 10, 100, 1, 0, 50, false, false, false)", at);
            }

            /* The collection log: every SQL Server collector from LogFloorBack on; the Query Store collector also ran 30 days back, long
               before the raw rows that are left (retention dropped them). */
            var collectors = CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer).Select(d => d.Name).ToArray();
            long logId = 0;
            foreach (var collector in collectors)
            {
                var backs = new List<TimeSpan> { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) };
                if (collector == "query_store") backs.Insert(0, TimeSpan.FromDays(30));
                foreach (var back in backs)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1,$2,$3,$4,$5,$6,'SUCCESS',$7)",
                        -698400 - (++logId), ServerId, ServerName, collector, DarlingMcpTestData.Naive(now - back), 100, 10);
                }
            }

            /* #7015: a registered PostgreSQL target with a few rows in each table PgSeededTables names, spread from the log's start
               (84 h back) to now so the store's data start holds. Only the columns a row cannot do without are filled (collection id, time,
               server, and any other NOT NULL column with no default, by type): the window-floor probe reads collection_time alone. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 'postgres', 16, $3, $3)",
                PgServerId, PgServerName, DarlingMcpTestData.Naive(now - LogFloorBack));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, engine_kind, postgres_major_version, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 'aurora-postgres', 16, $3, $3)",
                PgAuroraServerId, PgAuroraServerName, DarlingMcpTestData.Naive(now - LogFloorBack));
            long pgRowId = 0;
            foreach (var (tool, table) in PgSeededTables)
            {
                var (serverId, serverName) = tool == "get_pg_cpu_utilization" ? (PgAuroraServerId, PgAuroraServerName) : (PgServerId, PgServerName);
                foreach (var back in new[] { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) })
                {
                    await SeedPgRowAsync(connection, table, -698500 - (++pgRowId), DarlingMcpTestData.Naive(now - back), serverId, serverName, ct);
                }
            }

            /* The sparse reads probe their collector's logged runs (replication: a row exists only while a standby is attached), so the PostgreSQL
               target's log carries a run at each of the same times, which is what puts the store's start 84 h back for them. */
            long pgLogId = 0;
            foreach (var collector in PgLoggedCollectors)
            {
                foreach (var back in new[] { LogFloorBack, TimeSpan.FromDays(2), TimeSpan.FromDays(1), TimeSpan.FromMinutes(40), TimeSpan.FromMinutes(5) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1,$2,$3,$4,$5,$6,'SUCCESS',$7)",
                        -698600 - (++pgLogId), PgServerId, PgServerName, collector, DarlingMcpTestData.Naive(now - back), 100, 10);
                }
            }
        }

        DataSource = NpgsqlDataSource.Create(_scratch.ConnectionString);
    }

    private static async Task SeedPgRowAsync(NpgsqlConnection connection, string table, long id, DateTime time, int serverId, string serverName, CancellationToken ct)
    {
        var columns = new List<(string Name, string Type, bool Required)>();
        await using (var read = new NpgsqlCommand(
            "SELECT column_name, data_type, (is_nullable = 'NO' AND column_default IS NULL AND is_generated = 'NEVER' AND is_identity = 'NO') FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1", connection))
        {
            read.Parameters.AddWithValue(table);
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) columns.Add((reader.GetString(0), reader.GetString(1), reader.GetBoolean(2)));
        }

        var names = new List<string>();
        var values = new List<string>();
        foreach (var (name, type, required) in columns)
        {
            var value = name switch
            {
                "collection_id" => "$1",
                "collection_time" => "$2",
                "server_id" => "$3",
                "server_name" => "$4",
                _ when !required => null,
                _ => type switch
                {
                    "integer" or "bigint" or "smallint" or "numeric" or "double precision" or "real" => "0",
                    "text" or "character varying" => "'x'",
                    "boolean" => "FALSE",
                    "jsonb" or "json" => "'{}'",
                    var t when t.StartsWith("timestamp", StringComparison.Ordinal) => "$2",
                    _ => throw new InvalidOperationException($"collect.{table}.{name} is NOT NULL with no default and type {type}: teach SeedPgRowAsync how to fill it."),
                },
            };
            if (value is null) continue;
            names.Add(name);
            values.Add(value);
        }

        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"INSERT INTO collect.{table} ({string.Join(", ", names)}) VALUES ({string.Join(", ", values)})",
            id, time, serverId, serverName);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        if (_scratch is not null) await _scratch.DisposeAsync();
    }
}

/// <summary>
/// #6984: runs every MCP read tool across window sizes and <c>as_of</c> against a store whose raw data starts about three days back,
/// because #6829 then #6877 (Query Store regressions, a window past the data, then an <c>as_of</c> before it) each fixed one of two
/// cases the first report named, and no test ran a read over both. Darling's twin of Lite's <c>McpWindowMatrixTests</c>, reusing
/// <see cref="McpReadToolBudgetLiveTests"/>'s discovery and binding over one seed for the whole class.
/// <para>Contracts only, never the wording (#5833): when the requested window starts before the store's floor the answer carries
/// <c>window_truncated</c> true and, when it names one, an <c>effective_start</c> (top level or under <c>hints</c>) that is not earlier
/// than the requested start and not later than the window's end; and a read over these windows does not error. A tool that breaks the
/// contract is listed in <see cref="ExemptTools"/> with its reason; a listed tool that now passes fails the test, so a fix deletes its
/// row.</para>
/// </summary>
/* Shares the gap-cache collection with the other regressions tests, as QueryStoreRegressionsRetentionCauseLiveTests does: the
   gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class McpWindowMatrixLiveTests : IClassFixture<McpWindowMatrixStore>
{
    private readonly McpWindowMatrixStore _store;

    public McpWindowMatrixLiveTests(McpWindowMatrixStore store) => _store = store;

    private static string ServerName => McpWindowMatrixStore.ServerName;
    private static TimeSpan RawFloorBack => McpWindowMatrixStore.RawFloorBack;

    /// <summary>The tool carries no window_truncated notice over a window that starts before the store's data (the notice is not rolled out to it; the PostgreSQL reads and the fleet and store-level reads are in this group, and so are tools whose probe table this seed does not fill).</summary>
    private const string NoNotice = "no window_truncated notice over a window that starts before the store's data (not rolled out to this tool, or its probe table is not seeded)";

    /// <summary>#7010: the tool reads the newest snapshot and hours_back is only the span SEARCHED for it, not a span aggregated, so its
    /// answer is one row stamped with captured_at and age_seconds; a coverage notice would describe a window the tool never reads.</summary>
    private const string SnapshotSearch = "newest-snapshot read: hours_back is the span searched for the latest snapshot (captured_at and age_seconds say how old it is), not a data window (#7010)";

    /// <summary>The tool answers window_truncated, but false, for a window that starts before the store's data (a window that ends before it, or one the probe judged covered).</summary>
    private const string NotTrue ="window_truncated is not true over a window that starts before the store's data";

    /// <summary>The tool refuses the call because a parameter this matrix does not bind has no default (a required view or metric): a test binding gap, not a product defect.</summary>
    private const string NeedsArgument = "the generic binding leaves a required argument empty, so the tool answers invalid (test binding gap)";

    /// <summary>The tool throws or answers error because the generic binding leaves a required argument null (test binding gap), or reads a store the matrix does not seed.</summary>
    private const string BindFailure = "the generic binding leaves a required argument null, so the call errors (test binding gap)";

    /// <summary>The tools the window contract does not hold for yet, one row each with the reason. A fix deletes its row. The NoNotice and
    /// NotTrue rows (the product reasons, not the binding gaps) are tracked in #7010.</summary>
    private static readonly Dictionary<string, string> ExemptTools = new(StringComparer.Ordinal)
    {
        /* #7010: not a data window by design. One server: "always 7 days" of collector health read from the collection log (hours_back is ignored). Fleet mode (server_name "*") takes hours_back, but only to read one collector's liveness per server, not a windowed row set. */
        ["get_collection_health"] = NoNotice,
        ["get_cpu_scheduler_pressure"] = SnapshotSearch,
        ["get_database_protection"] = SnapshotSearch,
        /* #7010: not a data window by design. The reads are investigation REQUESTS opened in the last hours_back hours (hours_back 1-168, no as_of), the service's own work log of alert-triggered runs, not a span of collected data: a window with no request is a true negative, and the log is not floored by the collector tables. */
        ["get_investigation_reports"] = "investigation request log: hours_back is the age of requests opened, not a collected-data window (#7010)",
        ["get_pg_autovacuum_health"] = NoNotice,
        ["get_pg_blocking"] = NoNotice,
        ["get_pg_buffer_usage"] = NoNotice,
        ["get_pg_column_stats"] = NoNotice,
        ["get_pg_database_stats"] = NoNotice,
        ["get_pg_database_trend"] = NoNotice,
        ["get_pg_extensions"] = NoNotice,
        ["get_pg_index_bloat"] = NoNotice,
        ["get_pg_io_trend"] = NoNotice,
        ["get_pg_plan_capture_readiness"] = NoNotice,
        ["get_pg_query_duration_trend"] = NoNotice,
        ["get_pg_replication_slots"] = NoNotice,
        ["get_pg_session_states"] = NoNotice,
        ["get_pg_table_bloat"] = NoNotice,
        ["get_pg_top_queries"] = NoNotice,
        ["get_pg_wait_stats"] = NoNotice,
        ["get_pg_wait_trend"] = NoNotice,
        ["get_pg_wraparound_risk"] = NoNotice,
        ["get_pg_xmin_horizon"] = NoNotice,
        ["get_plan_cache_bloat"] = SnapshotSearch,
        /* #7010: not a data window by design. The window selects captures of the store's own PostgreSQL log, and every answer carries the capture
           denominator (captures against expected, bytes read and unread), which is how a window the store did not capture is told apart from a quiet one. */
        ["get_store_log"] = "store-log window: the answer's capture denominator says how much of the window was captured (#7010)",
        /* #7010: not a data window by design. The window picks the fleet sweeper's own run records for the timeline, the latest sweep is read outside
           it on purpose, and no collector table backs it for a coverage probe. */
        ["get_sweep_reports"] = "sweep timeline of the store's own run records; the latest sweep is read outside the window (#7010)",
    };

    /// <summary>The arguments a tool needs beyond the window that the generic binding cannot supply: a required view, metric or counter, the
    /// seeded query and plan, a definition to replay (no saved rule), and the investigation tool's two services (#6984).</summary>
    private static IEnumerable<KeyValuePair<string, object?>> ToolArguments(string tool, NpgsqlDataSource postgres)
    {
        switch (tool)
        {
            case "backtest_custom_alert_rule":
                /* One server: the replay is 288 steps a server at 168 h, and the PostgreSQL target this fixture seeds has no samples of the source.
                   The rule reads query_store_stats, the table this fixture already seeds for the SQL Server target (rows at the raw floor and
                   30 minutes back), so the backtest's coverage probe finds a floor and reaches IsTruncated (#7015). */
                yield return new("servers", ServerName);
                yield return new("definition",
                    "{\"metric\":{\"source\":\"query_store_stats\",\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\",\"hours\":0.25}," +
                    "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25},\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1},\"scope\":{\"mode\":\"all\"}}");
                break;
            case "get_finops": yield return new("view", "high_impact"); break;
            case "get_server_trend": yield return new("metric", "total_waits"); break;
            case "get_perfmon_trend": yield return new("counter_name", "Batch Requests/sec"); break;
            case "get_wait_trend": yield return new("wait_type", "SOS_SCHEDULER_YIELD"); break;
            case "get_plan_force_precheck":
                yield return new("database_name", "TenantDb00");
                yield return new("query_id", 1000L);
                yield return new("plan_id", 9L);
                break;
            case "get_query_store_query_history":
                yield return new("database_name", "TenantDb00");
                yield return new("query_id", 1000L);
                break;
            case "get_investigation_reports":
                yield return new("composer", new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(postgres),
                    new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools));
                yield return new("readPolicy", new InvestigationReadPolicy(false));
                break;
        }
    }

    public enum AsOfCase { None, PartlyInside, WhollyBefore }

    /// <summary>The three as_of cases as the offset back from now of the window's END: none is now, partly inside ends two days back
    /// (inside the three days of raw data), wholly before ends 4.5 days back (before the raw floor).</summary>
    private static TimeSpan EndBack(AsOfCase c) => c switch
    {
        AsOfCase.PartlyInside => TimeSpan.FromDays(2),
        AsOfCase.WhollyBefore => TimeSpan.FromHours(108),
        _ => TimeSpan.Zero,
    };

    private static string AsOfText(DateTime end) => end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    [Fact]
    public async Task EveryReadTool_AnswersTheWindowContract_AcrossHoursBackAndAsOf_UnlessListed()
    {
        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live #6984 window matrix.");
        var postgres = _store.DataSource!;
        var analysisService = new DarlingAnalysisService(postgres);
        var catalog = new McpToolGuideCatalog();

        var violations = new List<(string Tool, string Scenario, string Reason)>();
        var windowed = 0;

        foreach (var (name, method) in McpReadToolBudgetLiveTests.ReadToolMethods())
        {
            var hoursParam = method.GetParameters().FirstOrDefault(p => p.Name == "hours_back");
            var hasHours = hoursParam is not null && (Nullable.GetUnderlyingType(hoursParam.ParameterType) ?? hoursParam.ParameterType) == typeof(int);
            var hasAsOf = method.GetParameters().Any(p => p.Name == "as_of" && p.ParameterType == typeof(string));
            if (!hasHours && !hasAsOf) continue;
            /* #7015: the backtest's span is the double `hours`, not an int hours_back, so without its own binding the matrix passes it no
               requested start and CheckContract skips its truncation check. It takes the matrix's two sizes as hours. */
            var bindsDoubleHours = !hasHours && name == "backtest_custom_alert_rule";
            var bindsHours = hasHours || bindsDoubleHours;
            windowed++;

            var hourList = bindsHours ? new[] { 24, 168 } : new[] { 0 };
            var asOfList = hasAsOf ? new[] { AsOfCase.None, AsOfCase.PartlyInside, AsOfCase.WhollyBefore } : new[] { AsOfCase.None };
            foreach (var hours in hourList)
            {
                foreach (var asOfCase in asOfList)
                {
                    if (!bindsHours && asOfCase == AsOfCase.None) continue;
                    var scenario = (bindsHours ? $"hours_back {hours}" : "default window") + ", " + asOfCase;
                    var end = DateTime.UtcNow - EndBack(asOfCase);
                    var overrides = new Dictionary<string, object?>();
                    if (hasHours) overrides["hours_back"] = hours;
                    if (bindsDoubleHours) overrides["hours"] = (double)hours;
                    if (hasAsOf && asOfCase != AsOfCase.None) overrides["as_of"] = AsOfText(end);
                    foreach (var (argName, argValue) in ToolArguments(name, postgres)) overrides[argName] = argValue;
                    if (McpWindowMatrixStore.PgSeededTables.ContainsKey(name)) overrides["server_name"] = McpWindowMatrixStore.PgServerFor(name);

                    string json;
                    try
                    {
                        var args = McpReadToolBudgetLiveTests.BindArgs(method, postgres, analysisService, catalog, new List<string>(), name, overrides);
                        json = await McpReadToolBudgetLiveTests.InvokeAsync(method, args);
                    }
                    catch (Exception ex)
                    {
                        var inner = (ex as TargetInvocationException)?.InnerException ?? ex;
                        violations.Add((name, scenario, $"threw {inner.GetType().Name}: {inner.Message.Split('\n')[0]}"));
                        continue;
                    }

                    var requestedStart = bindsHours ? end - TimeSpan.FromHours(hours) : (DateTime?)null;
                    /* The regressions tool compares the 7 days BEFORE the window, so its effective_start is that baseline's start. */
                    var lowerBound = name == "get_query_store_regressions" && requestedStart is DateTime rsStart ? rsStart - TimeSpan.FromDays(7) : requestedStart;
                    var reason = CheckContract(json, requestedStart, lowerBound, end, _store.SeededAt);
                    if (reason is not null) violations.Add((name, scenario, reason));
                }
            }
        }

        var failing = violations.Select(v => v.Tool).ToHashSet(StringComparer.Ordinal);
        var report = string.Join(Environment.NewLine, violations.GroupBy(v => v.Tool).OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => $"{g.Key}: {g.First().Reason} [{g.First().Scenario}; {g.Count()} scenario(s)]"));
        TestContext.Current.TestOutputHelper?.WriteLine($"windowed tools {windowed}, failing {failing.Count}, exempt {ExemptTools.Count}" + Environment.NewLine + report);

        Assert.True(windowed > 100, $"only {windowed} read tools take hours_back or as_of -- reflection likely under-enumerated");
        var unlisted = report.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
            .Where(line => !ExemptTools.ContainsKey(line[..line.IndexOf(':')])).ToArray();
        Assert.True(unlisted.Length == 0, "tools that break the #6984 window contract and are not in ExemptTools:" + Environment.NewLine + string.Join(Environment.NewLine, unlisted));
        var stale = ExemptTools.Keys.Where(t => !failing.Contains(t)).ToArray();
        Assert.True(stale.Length == 0, "exempt tools that now pass -- remove their ExemptTools row:" + Environment.NewLine + string.Join(Environment.NewLine, stale));
    }

    /// <summary>The contract for one answer, null when it holds. <paramref name="requestedStart"/> is null for a tool with no
    /// hours_back (its default window is the tool's own), so only the bounds that need no start are checked.</summary>
    private static string? CheckContract(string json, DateTime? requestedStart, DateTime? lowerBound, DateTime windowEnd, DateTime seededAt)
    {
        JsonDocument doc;
        try { doc = JsonDocument.Parse(json); }
        catch (JsonException) { return "answer is not JSON"; }

        using (doc)
        {
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return null;

            if (root.TryGetProperty("status", out var status) && status.ValueKind == JsonValueKind.String
                && status.GetString() is "error" or "invalid")
            {
                var message = root.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : "";
                return $"answered status {status.GetString()}: {message?.Split('\n')[0]}";
            }

            var truncated = Find(root, "window_truncated");
            var effective = Find(root, "effective_start");

            if (effective is { ValueKind: JsonValueKind.String } e)
            {
                if (!DateTime.TryParse(e.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var start))
                    return $"effective_start '{e.GetString()}' is not a time";
                var slack = TimeSpan.FromMinutes(2);
                if (lowerBound is DateTime req && start < req - slack) return $"effective_start {start:o} is earlier than the requested start {req:o}";
                /* #6885: a window that ends before the store's first row answers effective_start = that first row (after the window end), so only that window skips the "not later than the window end" check. */
                var endsBeforeStore = windowEnd < seededAt - RawFloorBack;
                if (!endsBeforeStore && start > windowEnd + slack) return $"effective_start {start:o} is later than the window end {windowEnd:o}";
            }

            /* The store's floor: the collection log starts LogFloorBack back, so a window that starts before that (past the slack the
               tools allow) is one the store cannot cover. */
            if (requestedStart is DateTime rs && rs < seededAt - McpWindowMatrixStore.LogFloorBack - DurationTrendRouting.TruncationSlack - TimeSpan.FromMinutes(5))
            {
                if (truncated is not { ValueKind: JsonValueKind.True })
                    return truncated is null ? "no window_truncated key (top level or hints) over a window that starts before the store's data" : "window_truncated is not true over a window that starts before the store's data";
            }
            return null;
        }
    }

    /// <summary>The key at the top level, else under <c>hints</c>, else under <c>window</c>.</summary>
    private static JsonElement? Find(JsonElement root, string key)
    {
        if (root.TryGetProperty(key, out var top)) return top;
        if (root.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object && hints.TryGetProperty(key, out var inHints)) return inHints;
        /* #7010: get_query_store_clutter keeps the notice in its window block (the reach vocabulary, held beside a page cut by the payload census). */
        if (root.TryGetProperty("window", out var window) && window.ValueKind == JsonValueKind.Object && window.TryGetProperty(key, out var inWindow)) return inWindow;
        return null;
    }

    /* ═══════════════════════════ get_query_store_regressions: 2 x 3 ═══════════════════════════ */

    /// <summary>What the seed says each window of the regressions read is: a regression listed, a recent side with no baseline, or a
    /// window older than the raw data whose collector was running before it.</summary>
    public enum Cause { Regression, NoBaseline, AgedOut }

    [Theory]
    [InlineData(24, AsOfCase.None, Cause.Regression)]
    [InlineData(168, AsOfCase.None, Cause.NoBaseline)]
    [InlineData(24, AsOfCase.PartlyInside, Cause.NoBaseline)]
    [InlineData(168, AsOfCase.PartlyInside, Cause.NoBaseline)]
    [InlineData(24, AsOfCase.WhollyBefore, Cause.AgedOut)]
    [InlineData(168, AsOfCase.WhollyBefore, Cause.AgedOut)]
    public async Task GetQueryStoreRegressions_NamesTheCauseTheSeedHolds(int hours, AsOfCase asOfCase, Cause expected)
    {
        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live #6984 window matrix.");
        /* Seed (McpWindowMatrixStore): Query Store rows at 3 days back (the floor, 1 ms) and 30 minutes back (the 55 ms regression), the
           collector's logged runs from 30 days back. Recent window = hours before the end; baseline = the 7 days before that.
           End now: 24 h holds both rows (baseline = the floor row, recent = the regression); 168 h holds both in the recent
           window, so no baseline. End 2 days 6 h back (inside the raw data): the floor row is the only recent row, no baseline.
           End 4.5 days back (before the raw data): no row on either side, and the collector ran before, so the data aged out. */
        var asOf = asOfCase == AsOfCase.None ? null
            : AsOfText(DateTime.UtcNow - EndBack(asOfCase) - (asOfCase == AsOfCase.PartlyInside ? TimeSpan.FromHours(6) : TimeSpan.Zero));

        var json = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(_store.DataSource!, ServerName, hours, as_of: asOf);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var floor = _store.SeededAt - RawFloorBack;

        var truncated = Find(root, "window_truncated");
        Assert.True(truncated is { ValueKind: JsonValueKind.True }, "the baseline starts before the store's rows in every scenario, so window_truncated must be true: " + json);
        var effective = Find(root, "effective_start");
        Assert.True(effective is { ValueKind: JsonValueKind.String }, "effective_start must name where the store's rows start: " + json);
        var start = DateTime.Parse(effective!.Value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange((start - floor).TotalMinutes, -10, 10);

        switch (expected)
        {
            case Cause.Regression:
                Assert.False(root.TryGetProperty("status", out _), json);
                Assert.True(root.GetProperty("regression_count").GetInt32() > 0, json);
                break;
            case Cause.NoBaseline:
                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                Assert.Contains("no Query Store capture in the 7-day baseline window", root.GetProperty("message").GetString(), StringComparison.Ordinal);
                break;
            case Cause.AgedOut:
                Assert.Equal("unavailable", root.GetProperty("status").GetString());
                var message = root.GetProperty("message").GetString()!;
                Assert.Contains("is older than this server's raw Query Store data, which starts " + floor.ToString("MMM d", CultureInfo.InvariantCulture), message, StringComparison.Ordinal);
                Assert.DoesNotContain("No Query Store data has EVER been collected", message, StringComparison.Ordinal);
                break;
        }
    }
}
