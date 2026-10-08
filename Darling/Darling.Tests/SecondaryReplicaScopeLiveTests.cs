/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres for every test and never touches
   another test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5558 on a seeded PostgreSQL store, the twin of Lite's <c>SecondaryReplicaFactTests</c>: the replicated facts drop the
/// database this node holds as a secondary copy and the node-local ones keep it. Three databases: <c>SecDb</c> (the local
/// copy of group AG1 is SECONDARY), <c>PrimDb</c> (the local copy of group AG2 is PRIMARY) and <c>StandDb</c> (in no group).
/// Skipped when DARLING_TEST_PG is not set.
/// </summary>
public sealed class SecondaryReplicaScopeLiveTests
{
    private const int ServerId = -558_201;
    private const string ServerName = "darling-secondary-replica-scope";
    private static readonly string[] Databases = ["SecDb", "PrimDb", "StandDb"];

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Now() => DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

    /// <summary>Runs <paramref name="body"/> against a fresh migrated scratch store with the test server registered, then
    /// drops the store whether the body passed or not.</summary>
    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live secondary replica scope tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(connection, ct, sql, values);

    /// <summary>AG1: this node (NODE1) is <paramref name="localRole"/> for it and holds SecDb; AG2: this node is PRIMARY and
    /// holds PrimDb. NODE2 is the other replica of AG1 and holds SecDb too (not local).</summary>
    private static async Task SeedAgAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string localRole = "SECONDARY")
    {
        const string Replica = "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)";
        var when = DarlingMcpTestData.Naive(at);
        await ExecAsync(c, ct, Replica, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG1", "NODE1", localRole, true);
        await ExecAsync(c, ct, Replica, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG1", "NODE2", "PRIMARY", false);
        await ExecAsync(c, ct, Replica, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG2", "NODE1", "PRIMARY", true);
        const string Db = "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)";
        await ExecAsync(c, ct, Db, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG1", "SecDb", "NODE1", true);
        await ExecAsync(c, ct, Db, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG1", "SecDb", "NODE2", false);
        await ExecAsync(c, ct, Db, CollectionIdGenerator.Next(), when, ServerId, ServerName, "AG2", "PrimDb", "NODE1", true);
    }

    /// <summary>One sys.databases capture for each of the three databases: auto_shrink on and RCSI off, so every one is a
    /// DB_CONFIG offender, plus one 20 GB percent-growth data file each for FILE_AUTOGROWTH_PERCENT.</summary>
    private static async Task SeedDatabasesAsync(NpgsqlConnection c, CancellationToken ct, DateTime end)
    {
        foreach (var db in Databases)
        {
            await ExecAsync(c, ct, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     state_desc, compatibility_level, collation_name, recovery_model, is_read_only,
     is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_read_committed_snapshot_on, snapshot_isolation_state,
     is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
     target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on,
     is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5,
        'ONLINE', 160, 'SQL_Latin1_General_CP1_CI_AS', 'FULL', FALSE,
        FALSE, TRUE, TRUE, TRUE,
        FALSE, FALSE, 'OFF',
        FALSE, TRUE, FALSE, FALSE, FALSE,
        FALSE, FALSE, FALSE, 'NOTHING', 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-1)), ServerId, ServerName, db);

            await ExecAsync(c, ct, @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name,
     physical_name, total_size_mb, used_size_mb, is_percent_growth, growth_pct)
VALUES ($1, $2, $3, $4, $5, 7, 1, 'ROWS', $6, 'D:\data.mdf', 20480, 10000, TRUE, 10)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-30)), ServerId, ServerName, db, db + "_data");
        }
    }

    private static AnalysisContext Context(DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = DarlingMcpTestData.Naive(end.AddHours(-4)),
        TimeRangeEnd = DarlingMcpTestData.Naive(end),
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task<(List<Fact> Facts, AnalysisContext Context)> FactsAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct)
    {
        var context = Context(end);
        context.CancellationToken = ct;
        await PgSecondaryReplicaScope.EnsureAsync(postgres, context, logger: null);
        var facts = (await new PgFactCollector(postgres).CollectFactsAsync(context)).ToList();
        return (facts, context);
    }

    private static Fact One(List<Fact> facts, string key) => Assert.Single(facts, f => f.Key == key);

    private static async Task<string> DrillDownJsonAsync(NpgsqlDataSource postgres, AnalysisContext context, string rootFactKey, double severity = 0.3)
    {
        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = rootFactKey, StoryPath = rootFactKey, PathKeys = [rootFactKey], Severity = severity,
        };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
        return JsonSerializer.Serialize(finding.DrillDown);
    }

    [Fact]
    public async Task OnASecondary_DbConfigAndAutogrowthDropTheSecondaryDatabase_TheDrillDownsAgree_AndNodeLocalFactsKeepIt()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = Now();
            await SeedDatabasesAsync(c, ct, end);
            await SeedAgAsync(c, ct, end.AddMinutes(-1));

            var (facts, context) = await FactsAsync(postgres, end, ct);

            Assert.Equal(["SecDb"], context.SecondaryReplicaDatabases!.ToArray());

            var config = One(facts, "DB_CONFIG");
            Assert.Equal(2.0, config.Value);
            Assert.Equal(2.0, config.Metadata["database_count"]);
            Assert.Equal(2.0, config.Metadata["auto_shrink_on_count"]);
            Assert.Equal(2.0, config.Metadata["rcsi_off_count"]);

            var autogrowth = One(facts, "FILE_AUTOGROWTH_PERCENT");
            Assert.Equal(2.0, autogrowth.Value);
            Assert.Equal(2.0, autogrowth.Metadata["database_count"]);

            /* The drill-downs list the same databases the facts counted. */
            var configJson = await DrillDownJsonAsync(postgres, context, "DB_CONFIG");
            Assert.Contains("PrimDb", configJson);
            Assert.Contains("StandDb", configJson);
            Assert.DoesNotContain("SecDb", configJson);

            var growthJson = await DrillDownJsonAsync(postgres, context, "FILE_AUTOGROWTH_PERCENT");
            Assert.Contains("PrimDb", growthJson);
            Assert.Contains("StandDb", growthJson);
            Assert.DoesNotContain("SecDb", growthJson);
        });
    }

    [Fact]
    public async Task OnThePrimary_OrWithNoOrStaleAgRows_NothingIsSkipped()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = Now();
            await SeedDatabasesAsync(c, ct, end);

            /* No AG rows at all. */
            var (noRows, noRowsContext) = await FactsAsync(postgres, end, ct);
            Assert.Empty(noRowsContext.SecondaryReplicaDatabases!);
            Assert.Equal(3.0, One(noRows, "DB_CONFIG").Metadata["database_count"]);
            Assert.Equal(3.0, One(noRows, "FILE_AUTOGROWTH_PERCENT").Metadata["database_count"]);

            /* Rows exist, but the newest is three hours old: stale, so the role vouches for nothing. */
            await SeedAgAsync(c, ct, end.AddHours(-3));
            var (stale, staleContext) = await FactsAsync(postgres, end, ct);
            Assert.Empty(staleContext.SecondaryReplicaDatabases!);
            Assert.Equal(3.0, One(stale, "DB_CONFIG").Metadata["database_count"]);

            /* A fresh snapshot where the local role for the group is PRIMARY. */
            await SeedAgAsync(c, ct, end.AddMinutes(-1), localRole: "PRIMARY");
            var (primary, primaryContext) = await FactsAsync(postgres, end, ct);
            Assert.Empty(primaryContext.SecondaryReplicaDatabases!);
            Assert.Equal(3.0, One(primary, "DB_CONFIG").Metadata["database_count"]);
        });
    }

    [Fact]
    public async Task AnAsOfRead_UsesTheRoleAtItsOwnEnd_NotTheCurrentOne_AndTheNoteFollows()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var now = Now();
            /* Primary three hours ago, secondary a minute ago (a failover in between). */
            await SeedAgAsync(c, ct, now.AddHours(-3).AddMinutes(-1), localRole: "PRIMARY");
            await SeedAgAsync(c, ct, now.AddMinutes(-1));

            var current = await PgSecondaryReplicaScope.ReadAsync(postgres, ServerId, now, logger: null, ct);
            Assert.Equal(["SecDb"], current.ToArray());
            Assert.Equal(AgReplicaScope.SkippedNote(1), await PgSecondaryReplicaScope.NoteAsync(postgres, ServerId, logger: null, ct));

            var earlier = await PgSecondaryReplicaScope.ReadAsync(postgres, ServerId, now.AddHours(-3), logger: null, ct);
            Assert.Empty(earlier);
            Assert.Null(await PgSecondaryReplicaScope.NoteAsync(postgres, ServerId, logger: null, ct, asOfUtc: now.AddHours(-3)));

            /* An anchor before any snapshot knows nothing, so it skips nothing. */
            Assert.Empty(await PgSecondaryReplicaScope.ReadAsync(postgres, ServerId, now.AddDays(-30), logger: null, ct));
        });
    }

    private static Task SeedRegressionAsync(NpgsqlConnection c, CancellationToken ct, DateTime end, string db, long planId, string hash,
        long avgCpu, DateTime firstExec, DateTime lastExec, long queryId = 101) =>
        ExecAsync(c, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id)
VALUES ($1,$2,$3,$4,$5,$11,$6,'Regular',$7,$8,'SELECT 1','0xQH',100,$9,$9,$10,FALSE,0,$6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end), ServerId, ServerName, db, planId,
            DarlingMcpTestData.Naive(firstExec), DarlingMcpTestData.Naive(lastExec), avgCpu, hash, queryId);

    [Fact]
    public async Task PlanRegression_DropsTheSecondaryDatabase()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = Now();
            await SeedAgAsync(c, ct, end.AddMinutes(-1));
            foreach (var db in new[] { "SecDb", "StandDb" })
            {
                await SeedRegressionAsync(c, ct, end, db, planId: 1, hash: "0xGOOD", avgCpu: 100_000, firstExec: end.AddDays(-6), lastExec: end.AddDays(-5));
                await SeedRegressionAsync(c, ct, end, db, planId: 2, hash: "0xBAD", avgCpu: 1_200_000, firstExec: end.AddDays(-1), lastExec: end);
            }

            var (facts, context) = await FactsAsync(postgres, end, ct);

            Assert.Equal(1.0, One(facts, "PLAN_REGRESSION").Metadata["offender_count"]);
            Assert.NotEmpty(context.PlanRegressionOffenders!);
            Assert.All(context.PlanRegressionOffenders!, o => Assert.Equal("StandDb", o.DatabaseName));
        });
    }

    /// <summary>
    /// L1 (round 1): the regressed-queries drill-down filters the secondary database in its source CTE, BEFORE its LIMIT 5, as
    /// Lite's twin does. The pass where the fact did not run has no offender list, so every query is read; when the secondary
    /// database holds the five worst regressions, a filter applied after the LIMIT would leave nothing for the primary's own.
    /// </summary>
    [Fact]
    public async Task TheRegressedQueriesDrillDown_FiltersTheSecondaryDatabaseBeforeItsLimit()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = Now();
            await SeedAgAsync(c, ct, end.AddMinutes(-1));
            for (var q = 0; q < 5; q++)
            {
                var queryId = 201 + q;
                await SeedRegressionAsync(c, ct, end, "SecDb", planId: queryId * 10 + 1, hash: "0xGOOD", avgCpu: 100_000, firstExec: end.AddDays(-6), lastExec: end.AddDays(-5), queryId: queryId);
                await SeedRegressionAsync(c, ct, end, "SecDb", planId: queryId * 10 + 2, hash: "0xBAD", avgCpu: 5_000_000, firstExec: end.AddDays(-1), lastExec: end, queryId: queryId);
            }
            foreach (var queryId in new long[] { 101, 102 })
            {
                await SeedRegressionAsync(c, ct, end, "StandDb", planId: queryId * 10 + 1, hash: "0xGOOD", avgCpu: 100_000, firstExec: end.AddDays(-6), lastExec: end.AddDays(-5), queryId: queryId);
                await SeedRegressionAsync(c, ct, end, "StandDb", planId: queryId * 10 + 2, hash: "0xBAD", avgCpu: 1_200_000, firstExec: end.AddDays(-1), lastExec: end, queryId: queryId);
            }

            /* No fact ran, so PlanRegressionOffenders is null; the set itself is resolved as a pass would. */
            var context = Context(end);
            context.CancellationToken = ct;
            await PgSecondaryReplicaScope.EnsureAsync(postgres, context, logger: null);
            Assert.Null(context.PlanRegressionOffenders);

            var json = await DrillDownJsonAsync(postgres, context, "PLAN_REGRESSION", severity: 0.6);
            using var doc = JsonDocument.Parse(json);
            var rows = doc.RootElement.GetProperty("regressed_queries").EnumerateArray().ToList();
            Assert.Equal(2, rows.Count);
            Assert.All(rows, r => Assert.Equal("StandDb", r.GetProperty("database").GetString()));
        });
    }

    private static Task SeedObjectAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, decimal reservedMb) =>
        ExecAsync(c, ct, @"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name,
     index_id, index_name, reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1,$2,$3,$4,$5,7,'dbo',100,'Big',1,'PK_Big',$6,0,0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, db, reservedMb);

    [Fact]
    public async Task ObjectGrowth_DropsTheSecondaryDatabase()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var end = Now();
            await SeedAgAsync(c, ct, end.AddMinutes(-1));
            foreach (var (db, grownMb) in new[] { ("SecDb", 900m), ("StandDb", 500m) })
            {
                await SeedObjectAsync(c, ct, end.AddDays(-1), db, 100m);
                await SeedObjectAsync(c, ct, end.AddHours(-1), db, 100m + grownMb);
            }

            /* The baseline gate: anomaly detection sits out a server with no history to compare against. */
            await ExecAsync(c, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, 10, 5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddDays(-2)), ServerId, ServerName);

            var context = Context(end);
            context.CancellationToken = ct;
            await PgSecondaryReplicaScope.EnsureAsync(postgres, context, logger: null);
            var detector = new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres));
            var anomalies = await detector.DetectAnomaliesAsync(context);

            var growth = Assert.Single(anomalies, f => f.Key == "ANOMALY_OBJECT_GROWTH");
            Assert.Equal("StandDb", growth.DatabaseName);
        });
    }

    /// <summary>
    /// get_analysis_findings carries the shared one-sentence note beside a non-empty list and inside the empty answer when
    /// this node holds a secondary copy. Lite's twin is in <c>McpAnalysisFindingsCommandTests</c>.
    /// </summary>
    [Fact]
    public async Task GetAnalysisFindings_CarriesTheSecondaryReplicaNote_OnlyWhenThisNodeHoldsASecondaryCopy()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var now = Now();
            var service = new DarlingAnalysisService(postgres);

            /* Nothing stored, no AG rows: the plain empty answer, no note. */
            using (var plain = JsonDocument.Parse(await DarlingMcpTools.GetAnalysisFindings(service, postgres, ServerName)))
            {
                Assert.Equal("empty", plain.RootElement.GetProperty("status").GetString());
                Assert.DoesNotContain("secondary copy", plain.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            /* A secondary copy, still no findings: the empty answer names the skipped database. */
            await SeedAgAsync(c, ct, now.AddMinutes(-1));
            using (var empty = JsonDocument.Parse(await DarlingMcpTools.GetAnalysisFindings(service, postgres, ServerName)))
            {
                Assert.Contains(AgReplicaScope.SkippedNote(1)!, empty.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            /* With a finding stored the field rides on the payload. */
            var t = now.AddMinutes(-10);
            await ExecAsync(c, ct, @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name, time_range_start, time_range_end, severity, confidence,
     category, story_path, story_path_hash, story_text, root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count,
     incident_id, remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, $4, 'StandDb', $5, $2, 0.72, 0.55, 'config', 'DB_CONFIG', 'AG5558_HASH', '', 'DB_CONFIG', 2, 'DB_CONFIG', 2, 1,
        'AG5558_INCIDENT', NULL, NULL)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, DarlingMcpTestData.Naive(t.AddHours(-4)));

            using (var withNote = JsonDocument.Parse(await DarlingMcpTools.GetAnalysisFindings(service, postgres, ServerName)))
            {
                Assert.Equal(AgReplicaScope.SkippedNote(1), withNote.RootElement.GetProperty("secondary_replica_note").GetString());
            }
        });
    }

    private static Task PlantWaitAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, long waitMs) => ExecAsync(c, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'AN5558_WAIT', 50, $5)", CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, waitMs);

    /// <summary>The note from wherever the answer's shape puts it: the root of a data-bearing answer, or the hints of a miss.</summary>
    private static string? NoteOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var carrier = root.TryGetProperty("secondary_replica_note", out _) ? root : root.GetProperty("hints");
        var note = carrier.GetProperty("secondary_replica_note");
        return note.ValueKind == JsonValueKind.Null ? null : note.GetString();
    }

    /// <summary>
    /// M4 (round 1): analyze_server carries the same note get_analysis_findings does, under the same field name, always present
    /// and null when nothing is skipped; the service keeps the sentence the pass used. Lite's twin is in
    /// <c>SecondaryReplicaMcpAnalysisTests</c>.
    /// </summary>
    [Fact]
    public async Task AnalyzeServer_CarriesTheNote_OnlyWhenThisNodeHoldsASecondaryCopy()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var service = new DarlingAnalysisService(postgres) { MinimumDataHours = 0 };

            Assert.Null(NoteOf(await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4)));
            Assert.Null(service.LastSecondaryReplicaNote);

            await SeedAgAsync(c, ct, Now().AddMinutes(-1));
            var answer = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            Assert.Equal(AgReplicaScope.SkippedNote(1), NoteOf(answer));
            Assert.Equal(AgReplicaScope.SkippedNote(1), service.LastSecondaryReplicaNote);

            using var doc = JsonDocument.Parse(answer);
            if (doc.RootElement.GetProperty("status").GetString() == "empty")
                Assert.Contains(AgReplicaScope.SkippedNote(1)!, doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    /// <summary>M4 (round 1): compare_analysis carries one note per window, each from the role at that window's own end.</summary>
    [Fact]
    public async Task CompareAnalysis_CarriesOneNotePerWindow_EachFromItsOwnRole()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var now = Now();
            var baselineEnd = now.AddHours(-24);

            /* Secondary when the baseline window ended, primary after a failover by the time the comparison window ends. */
            await SeedAgAsync(c, ct, baselineEnd.AddMinutes(-1));
            await SeedAgAsync(c, ct, now.AddMinutes(-1), localRole: "PRIMARY");
            await PlantWaitAsync(c, ct, baselineEnd.AddHours(-1), 500_000L);
            await PlantWaitAsync(c, ct, now.AddHours(-1), 900_000L);

            var answer = await DarlingMcpTools.CompareAnalysis(
                new DarlingAnalysisService(postgres) { MinimumDataHours = 0 }, postgres, ServerName, 4, 28, cancellationToken: ct);

            using var doc = JsonDocument.Parse(answer);
            var root = doc.RootElement;
            var (baseline, comparison) = root.TryGetProperty("baseline", out var b)
                ? (b.GetProperty("secondary_replica_note"), root.GetProperty("comparison").GetProperty("secondary_replica_note"))
                : (root.GetProperty("hints").GetProperty("baseline_secondary_replica_note"), root.GetProperty("hints").GetProperty("comparison_secondary_replica_note"));
            Assert.Equal(AgReplicaScope.SkippedNote(1), baseline.GetString());
            Assert.Equal(JsonValueKind.Null, comparison.ValueKind);
        });
    }
}
