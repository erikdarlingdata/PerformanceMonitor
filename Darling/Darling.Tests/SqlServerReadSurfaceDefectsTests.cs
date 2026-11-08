/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The blocked-process report writes <c>1900-01-01T00:00:00</c>, in the server's clock, for a session with no last batch or no
/// open transaction. The row read shifts every stamp to UTC, so the sentinel must be judged before the shift and answered as null,
/// not as a date near 1900 that reads as measured.
/// </summary>
public sealed class BlockedProcessReportSentinelTests
{
    /* The XE select is 38 columns wide: event_time first, the six server-local stamps at 25..30. */
    private static readonly int[] XeStamps = [25, 26, 27, 28, 29, 30];

    private static DataTable XeTable()
    {
        var table = new DataTable();
        for (var i = 0; i < 38; i++)
        {
            table.Columns.Add("c" + i, i == 0 || XeStamps.Contains(i) ? typeof(DateTime) : typeof(object));
        }
        return table;
    }

    private static DarlingBlockingReader.BlockedProcessReadRow ReadXe(DataTable table, ServerClock clock)
    {
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return DarlingBlockingReader.MapXeRow(reader, clock);
    }

    [Fact]
    public void TheReportsUnsetTimeSentinel_IsAnsweredAsNull_NotShiftedIntoADate()
    {
        var table = XeTable();
        var row = table.NewRow();
        var sentinel = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        row[0] = new DateTime(2026, 6, 1, 15, 0, 0, DateTimeKind.Unspecified);
        /* The blocker has a real transaction and batch; the blocked side has no batch and no transaction yet. */
        row[25] = sentinel;
        row[26] = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Unspecified);
        row[27] = sentinel;
        row[28] = new DateTime(2026, 6, 1, 10, 1, 0, DateTimeKind.Unspecified);
        row[29] = sentinel;
        row[30] = new DateTime(2026, 6, 1, 10, 2, 0, DateTimeKind.Unspecified);
        table.Rows.Add(row);

        var mapped = ReadXe(table, ServerClock.FixedOffset(-300));

        Assert.Null(mapped.BlockedLastTranStartedUtc);
        Assert.Null(mapped.BlockedLastBatchStartedUtc);
        Assert.Null(mapped.BlockedLastBatchCompletedUtc);
        Assert.Equal(new DateTime(2026, 6, 1, 15, 0, 0), mapped.BlockingLastTranStartedUtc);
        Assert.Equal(new DateTime(2026, 6, 1, 15, 1, 0), mapped.BlockingLastBatchStartedUtc);
        Assert.Equal(new DateTime(2026, 6, 1, 15, 2, 0), mapped.BlockingLastBatchCompletedUtc);
    }
}

/// <summary>
/// Five reads of the SQL Server surface, each against a seeded scratch store: a servers scope that names no registry name, the
/// fleet backlog answer with no response budget, an elapsed time at the engine's int ceiling, an Agent read on an engine the
/// agent_status collector does not run on, and a Query Store health snapshot that predates a role change to a readable secondary.
/// </summary>
/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing shared. */
[Collection("live-postgres")]
public sealed class SqlServerReadSurfaceDefectsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live read-surface tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string? StringOrNull(JsonElement e, string name) =>
        e.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String ? p.GetString() : null;

    [Fact]
    public async Task TestNow_OnAServersScopeOfDisplayNames_NamesEachUnmatchedEntryAndTheRegistryNameItPointsAt()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            const string registryName = "PRODSQL01\\CORE";
            var id = ServerIdHelper.GetDeterministicHashCode(registryName);
            await DarlingMcpTestData.RegisterServerAsync(c, id, registryName, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET display_name = 'Prod Core' WHERE server_id = $1", id);

            var definition =
                "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
                "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}," +
                "\"scope\":{\"mode\":\"servers\",\"servers\":[\"Prod Core\",\"nowhere\"]}}";

            var answer = Root(await DarlingMcpCustomAlertTools.TestCustomAlertRule(postgres, null, definition));
            Assert.Equal("no_in_scope_servers", StringOrNull(answer, "status"));

            Assert.True(answer.TryGetProperty("unmatched_servers", out var unmatched) && unmatched.ValueKind == JsonValueKind.Array,
                "the answer does not list the scope entries that matched nothing: " + answer);
            var entries = unmatched.EnumerateArray().ToDictionary(u => u.GetProperty("entry").GetString()!, u => StringOrNull(u, "registry_name"));
            Assert.Equal(registryName, entries["Prod Core"]);
            Assert.True(entries.ContainsKey("nowhere"));
            Assert.Null(entries["nowhere"]);
            Assert.Contains("Prod Core", StringOrNull(answer, "note"));
        });
    }

    [Fact]
    public async Task TheFleetBacklogAnswer_FitsTheResponseBudget_WorstFirst_AndSaysWhatItCut()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            for (var i = 1; i <= 60; i++)
            {
                var name = $"backlog-{i:00}";
                var id = ServerIdHelper.GetDeterministicHashCode(name);
                await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
                /* Server i holds i pending rows, so the worst server is backlog-60. */
                for (var p = 0; p < i; p++)
                {
                    await DarlingMcpTestData.ExecAsync(c, ct,
                        "INSERT INTO collect.oversized_plan_backlog (server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset, " +
                        "database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, plan_xml, captured_at, expired_at, last_attempt_at, attempt_count) " +
                        "VALUES ($1, 'query_stats', $2, $3, 0, 400, 'fixture_db', $4, 1000000, '2026-09-10 08:00:00', '2026-09-13 08:00:00', NULL, NULL, NULL, NULL, 0)",
                        id, $"0xPLAN{i:00}{p:000}", $"0xSQL{i:00}{p:000}", $"0xHASH{i:00}{p:000}");
                }
            }

            var json = await DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(postgres, null, false, DarlingMcpOversizedPlanBacklogTools.DefaultLimit, ct);
            var answer = Root(json);

            Assert.True(answer.TryGetProperty("truncated", out var truncated) && truncated.ValueKind == JsonValueKind.True,
                "60 servers do not fit the response budget, and the answer does not say it cut any: " + json.Length + " chars");
            Assert.True(json.Length <= McpResponseBudget.DefaultBytes, $"the answer is {json.Length} chars against a {McpResponseBudget.DefaultBytes}-byte budget");
            var returned = answer.GetProperty("servers_returned").GetInt32();
            var byServer = answer.GetProperty("by_server").EnumerateArray().ToList();
            Assert.Equal(returned, byServer.Count);
            Assert.InRange(returned, 1, 59);
            Assert.Equal(60, answer.GetProperty("servers_in_scope").GetInt32());
            Assert.Equal("backlog-60", byServer[0].GetProperty("server_name").GetString());
            Assert.Contains("TRUNCATED", StringOrNull(answer, "cut_note"));
            /* The census is the rollup over every server, cut or not. */
            Assert.Equal(60, answer.GetProperty("by_collector")[0].GetProperty("servers").GetInt32());
        });
    }

    [Fact]
    public async Task AnActiveQuery_AtTheEnginesElapsedTimeCeiling_AnswersNullTimes_WithANote()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            const string name = "ceiling-srv";
            var id = ServerIdHelper.GetDeterministicHashCode(name);
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
            var t = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-2));
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), t, id, name, 77, "master", "24  20:31:23.647", "EXEC sp_server_diagnostics", "running", 0, "SP_SERVER_DIAGNOSTICS_SLEEP", 500L, 5000L, (long)int.MaxValue, 100L, 0L, 2000L, 0.0m, "Read Committed", 1, 0, "sa", "HOST", "SQLAgent", 0, 0);

            var answer = Root(await DarlingMcpSessionTools.GetActiveQueries(postgres, server_name: name, cancellationToken: ct));
            var rows = answer.GetProperty("queries").EnumerateArray().ToList();
            var row = Assert.Single(rows);

            Assert.Equal(JsonValueKind.Null, row.GetProperty("elapsed_time_ms").ValueKind);
            Assert.Equal(JsonValueKind.Null, row.GetProperty("elapsed_time_formatted").ValueKind);
            Assert.Contains("2147483647", StringOrNull(row, "elapsed_time_note"));
        });
    }

    [Fact]
    public async Task JobHistory_OnAnEngineTheAgentStatusCollectorDoesNotRunOn_SaysNotCollected_NotStale()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            const string name = "rds-srv";
            var id = ServerIdHelper.GetDeterministicHashCode(name);
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
            await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET is_aws_rds = true WHERE server_id = $1", id);
            var run = DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-1));
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                                           category_name, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds,
                                           retries_attempted, message)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,$14,0,$15)",
                4_832_100_001L, run, id, name, 4_832_100_001L, "job-nightly", "Nightly", "Maintenance", 0, "(Job outcome)", 1, "The job succeeded.", run, 60, "ok");

            var answer = Root(await DarlingMcpSessionToolsHost.JobHistory(postgres, name, ct));
            var desc = StringOrNull(answer, "agent_status_desc");

            Assert.NotEqual(DarlingJobReader.AgentUnknownDescription, desc);
            Assert.Contains("RDS", desc, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(JsonValueKind.Null, answer.GetProperty("agent_running").ValueKind);
        });
    }

    [Fact]
    public async Task QueryStoreHealth_OnAReadableSecondary_WithASnapshotFromItsPrimaryDays_CarriesTheSecondaryAndStalenessNotes()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            const string name = "qs-secondary";
            var id = ServerIdHelper.GetDeterministicHashCode(name);
            await DarlingMcpTestData.RegisterServerAsync(c, id, name, ct);
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, ag_replica_role) " +
                "VALUES ($1, $2, $3, $4, 'Enterprise', '16.0.1000.6', 'RTM', 3, 'Secondary')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow), id, name);
            var nineteenDaysAgo = DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-19));
            foreach (var db in new[] { "tempdb", "Sales", "Billing" })
            {
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, interval_length_minutes) " +
                    "VALUES ($1, $2, $3, $4, $5, 'READ_ONLY', 'READ_WRITE', 8, 60)",
                    CollectionIdGenerator.Next(), nineteenDaysAgo, id, name, db);
            }

            var json = await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, name, null, ct);
            var answer = Root(json);

            Assert.Equal(3, answer.GetProperty("database_count").GetInt32());
            var note = StringOrNull(answer, "note");
            Assert.False(string.IsNullOrEmpty(note), "the answer serves a 19-day-old primary-role snapshot with no note: " + json);
            Assert.Contains("readable secondary", note, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("captured_at", note, StringComparison.Ordinal);
        });
    }
}

/// <summary>The job-history read with its window defaults, so the Agent assertion above reads as the tool's own answer.</summary>
internal static class DarlingMcpSessionToolsHost
{
    public static Task<string> JobHistory(NpgsqlDataSource postgres, string serverName, CancellationToken ct) =>
        DarlingMcpJobTools.GetJobHistory(postgres, server_name: serverName, cancellationToken: ct);
}
