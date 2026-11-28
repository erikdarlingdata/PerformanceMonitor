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
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: <c>get_analysis_facts</c> with a source reads only the fact families that source and its scoring need
/// (<see cref="DarlingAnalysisService.CollectAndScoreFactsAsync"/> with <c>sources</c>). For every source the engine accepts,
/// the facts of that source from the narrowed read are the facts of that source from the whole read, value, severities,
/// metadata, amplifier results and ranked objects alike, over one lab store seeded with facts of many sources.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>).</para>
/// </summary>
public sealed class FactSourceNarrowingLiveTests
{
    private const int ServerId = -857_003;
    private const string ServerName = "FactSourceNarrowSrv";

    private static readonly JsonSerializerOptions Json = new() { WriteIndented = false };

    /// <summary>A plan with a missing index and a warning, so MISSING_INDEX and PLAN_WARNING are emitted.</summary>
    private const string PlanXml =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"SELECT * FROM dbo.t WHERE UPPER(c) = 1\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<MissingIndexes><MissingIndexGroup Impact=\"88\"><MissingIndex Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\">"
        + "<ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[c]\" ColumnId=\"1\"/></ColumnGroup></MissingIndex></MissingIndexGroup></MissingIndexes>"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"upper([db].[dbo].[t].[c])=(1)\"><Const ConstValue=\"(1)\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>
    /// Waits (the grant, thread and scheduler waits the query facts' scoring reads among them), CPU samples, grant waiters,
    /// sessions, a long-running job, two settings, and query rows: parameter-sensitive plans, spills, a high DOP and plans.
    /// $1 server id, $2 server name, $3 window end.
    /// </summary>
    private const string SeedSql = @"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
SELECT 910000000 + w.n * 100 + s, $3::timestamp - make_interval(mins => 10 * s), $1, $2, w.wait_type, 400, w.wait_ms, w.wait_ms / 9
FROM (VALUES (1, 'RESOURCE_SEMAPHORE', 900000), (2, 'THREADPOOL', 400000), (3, 'SOS_SCHEDULER_YIELD', 2400000), (4, 'CXPACKET', 3000000),
             (5, 'PAGEIOLATCH_SH', 1500000), (6, 'LCK_M_X', 800000), (7, 'WRITELOG', 300000)) AS w (n, wait_type, wait_ms)
CROSS JOIN generate_series(0, 20) AS s;

INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT 920000000 + s, $3::timestamp - make_interval(mins => 10 * s), $1, $2, CASE WHEN s = 3 THEN 99 ELSE 85 END, 3
FROM generate_series(0, 20) AS s;

INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, waiter_count, grantee_count)
SELECT 930000000 + s, $3::timestamp - make_interval(mins => 10 * s), $1, $2, 0, 6, 20
FROM generate_series(0, 20) AS s;

INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count)
SELECT 940000000 + s, $3::timestamp - make_interval(mins => 10 * s), $1, $2, 'app', 300, 40, 250, 10
FROM generate_series(0, 2) AS s;

INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ($3::timestamp - interval '5 minutes', $1, $2, 'nightly', 'job-1', true, $3::timestamp - interval '3 hours', 10800, 1200, 1800, 40, true, 900.0);

INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_in_use)
VALUES (950000001, $3::timestamp - interval '1 hour', $1, $2, 'max degree of parallelism', 0),
       (950000002, $3::timestamp - interval '1 hour', $1, $2, 'cost threshold for parallelism', 5);

INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, creation_time, execution_count, delta_execution_count, delta_worker_time,
     delta_elapsed_time, delta_logical_reads, delta_spills, max_dop,
     min_worker_time, max_worker_time, min_grant_kb, max_grant_kb, min_spills, max_spills, query_plan_xml)
SELECT
    960000000 + k * 1000 + p * 100 + s,
    $3::timestamp - make_interval(mins => 10 * s),
    $1, $2,
    'Db' || (k % 3),
    '0xQ' || lpad(k::text, 3, '0'),
    '0xP' || lpad(k::text, 3, '0') || '_' || p,
    '0xS' || k, '0xH' || k || '_' || p, 'SELECT 1',
    $3::timestamp - interval '30 hours',
    5000 + s,
    50,
    60000000 * (1 + k % 4),
    90000000,
    4000000,
    CASE WHEN k % 3 = 0 THEN 2000 ELSE 0 END,
    CASE WHEN k % 4 = 0 THEN 16 ELSE 1 END,
    20000,
    20000 * (2 + k) + p,
    1024, 1024 * (1 + k % 4), 0, k % 2,
    CASE WHEN s = 1 THEN $4 END
FROM generate_series(1, 12) AS k
CROSS JOIN generate_series(0, 1) AS p
CROSS JOIN generate_series(1, 20) AS s;";

    /// <summary>
    /// Runs <see cref="SeedSql"/> one statement at a time (PostgreSQL prepares one statement per command), each with the
    /// parameters it reads: $1 server id, $2 server name, $3 window end, $4 plan XML.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        object[] values = [ServerId, ServerName, windowEnd, PlanXml];
        foreach (var statement in SeedSql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            await using var seed = new NpgsqlCommand(statement, connection);
            var count = Regex.Matches(statement, @"\$(\d+)").Max(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture));
            foreach (var value in values.Take(count))
                seed.Parameters.AddWithValue(value);
            await seed.ExecuteNonQueryAsync(ct);
        }
    }

    private static string Describe(IEnumerable<Fact> facts) =>
        JsonSerializer.Serialize(
            facts.Select(f => new
            {
                f.Source,
                f.Key,
                f.Value,
                f.BaseSeverity,
                f.Severity,
                f.ServerId,
                f.ObjectName,
                f.DatabaseName,
                Metadata = f.Metadata.OrderBy(m => m.Key, StringComparer.Ordinal).ToList(),
                Amplifiers = f.AmplifierResults.Select(a => new { a.Description, a.Matched, a.Boost }).ToList(),
                Ranked = f.Ranked?.Select(r => new { r.ObjectName, r.DatabaseName, r.Value, Figures = r.Figures?.OrderBy(x => x.Key, StringComparer.Ordinal).ToList() }).ToList(),
            }),
            Json);

    [Fact]
    public async Task EverySource_NarrowedRead_GivesTheFactsOfTheWholeRead()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fact source narrowing test.");
        var ct = TestContext.Current.CancellationToken;

        var windowEnd = LiveClock.Now();
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, windowEnd, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var service = new DarlingAnalysisService(postgres) { MinimumDataHours = 0 }.WithNoStageBudgets();

        var (whole, wholeCoverage, wholeCaveats) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, 4, windowEnd, ct);
        Assert.NotNull(wholeCoverage);
        Assert.False(wholeCaveats.SourcesNarrowed);

        /* The fixture gives the narrowed sources and the families their scoring reads facts to compare. */
        foreach (var source in new[] { "waits", "cpu", "memory", "config", "queries", "jobs", "sessions" })
            Assert.True(whole.Any(f => f.Source == source), $"the fixture gave no {source} fact");
        foreach (var key in new[] { "QUERY_SPILLS", "QUERY_HIGH_DOP", "PARAMETER_SENSITIVITY", "MISSING_INDEX", "PLAN_WARNING", "MEMORY_GRANT_PENDING" })
            Assert.True(whole.Any(f => f.Key == key), $"the fixture gave no {key} fact");
        /* A query fact whose score rests on another family's fact: the narrowed read must keep that family. */
        Assert.Contains(whole, f => f.Source == "queries" && f.AmplifierResults.Any(a => a.Matched && a.Description.Contains("grant", StringComparison.OrdinalIgnoreCase)));

        var narrowedAny = false;
        foreach (var source in FactScorer.KnownSources)
        {
            var asked = new HashSet<string>([source], StringComparer.OrdinalIgnoreCase);
            var (narrowed, coverage, caveats) = await service.CollectAndScoreFactsAsync(ServerId, ServerName, 4, windowEnd, asked, ct);

            Assert.Equal(
                Describe(whole.Where(f => string.Equals(f.Source, source, StringComparison.OrdinalIgnoreCase))),
                Describe(narrowed.Where(f => string.Equals(f.Source, source, StringComparison.OrdinalIgnoreCase))));
            Assert.Equal(JsonSerializer.Serialize(wholeCoverage!.ToPayload()), JsonSerializer.Serialize(coverage!.ToPayload()));
            Assert.Empty(caveats.Failures);

            if (caveats.SourcesNarrowed)
            {
                narrowedAny = true;
                Assert.True(caveats.FamiliesTotal < wholeCaveats.FamiliesTotal, $"{source}: a narrowed read counts the families it read");
            }
            else
            {
                Assert.Equal(wholeCaveats.FamiliesTotal, caveats.FamiliesTotal);
            }
        }

        Assert.True(narrowedAny, "no source narrowed the read");
    }

    /// <summary>The queries read skips the families no query fact needs, and keeps the ones its scoring reads.</summary>
    [Fact]
    public async Task QueriesRead_SkipsTheUnneededFamilies()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fact source narrowing test.");
        var ct = TestContext.Current.CancellationToken;

        var windowEnd = LiveClock.Now();
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, windowEnd, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var service = new DarlingAnalysisService(postgres) { MinimumDataHours = 0 }.WithNoStageBudgets();
        var (facts, _, caveats) = await service.CollectAndScoreFactsAsync(
            ServerId, ServerName, 4, windowEnd, new HashSet<string>(["queries"], StringComparer.OrdinalIgnoreCase), ct);

        Assert.True(caveats.SourcesNarrowed);
        Assert.Contains(facts, f => f.Source == "queries");
        Assert.Contains(facts, f => f.Source == "waits");
        Assert.Contains(facts, f => f.Key == "MEMORY_GRANT_PENDING");
        Assert.Contains(facts, f => f.Key == "CPU_SQL_PERCENT");
        foreach (var skipped in new[] { "jobs", "sessions", "config", "blocking", "database_config", "disk", "io", "tempdb", "bad_actor", "anomaly" })
            Assert.DoesNotContain(facts, f => f.Source == skipped);
    }

    /// <summary>
    /// A source <see cref="PgFactCollector.ScoringFamiliesBySource"/> does not list, and any source on a PostgreSQL target,
    /// reads every family: the facts, the coverage and the family total are the whole read's.
    /// </summary>
    [Fact]
    public async Task UnlistedSourceAndPostgresTarget_ReadEveryFamily()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fact source narrowing test.");
        var ct = TestContext.Current.CancellationToken;
        const int pgServerId = -857_005;
        const string pgServerName = "FactSourceNarrowPgSrv";

        var windowEnd = LiveClock.Now();
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, windowEnd, ct);

            await PgTargetFactCollectorTests.RegisterServerAsync(connection, pgServerId, pgServerName, MonitoredEngineKind.Postgres, 17, ct);
            await using var plant = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT 980000000 + n, $1::timestamp - make_interval(mins => n), $2, $3, 'appdb', 100000 - 50 * n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, 300) AS n", connection);
            plant.Parameters.AddWithValue(windowEnd);
            plant.Parameters.AddWithValue(pgServerId);
            plant.Parameters.AddWithValue(pgServerName);
            await plant.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var service = new DarlingAnalysisService(postgres) { MinimumDataHours = 0 }.WithNoStageBudgets();

        foreach (var (serverId, serverName, sources) in new[]
                 {
                     (ServerId, ServerName, new[] { "waits", "config", "memory", "cpu", "anomaly" }),
                     (pgServerId, pgServerName, new[] { "queries", "jobs", "pg_waits", "pg_queries" }),
                 })
        {
            var (whole, wholeCoverage, wholeCaveats) = await service.CollectAndScoreFactsAsync(serverId, serverName, 4, windowEnd, ct);
            Assert.NotEmpty(whole);
            foreach (var source in sources)
            {
                var asked = new HashSet<string>([source], StringComparer.OrdinalIgnoreCase);
                var (narrowed, coverage, caveats) = await service.CollectAndScoreFactsAsync(serverId, serverName, 4, windowEnd, asked, ct);

                Assert.False(caveats.SourcesNarrowed, $"{serverName} {source}");
                Assert.Equal(wholeCaveats.FamiliesTotal, caveats.FamiliesTotal);
                Assert.Equal(Describe(whole), Describe(narrowed));
                Assert.Equal(JsonSerializer.Serialize(wholeCoverage?.ToPayload()), JsonSerializer.Serialize(coverage?.ToPayload()));
            }
        }
    }
}
