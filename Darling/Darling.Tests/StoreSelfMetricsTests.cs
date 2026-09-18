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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V53 <c>collect.store_metrics</c> store surface (#2068): the migration's identity, the viewer
/// schema gate a StorageVersion bump obligates, the sweep SQL's dialect and shape, and the invariant the
/// whole design leans on — the table that MEASURES the compression/retention machinery is not in the
/// collector catalog, so that machinery can never recurse onto it (no hypertable conversion, no
/// compression policy, no catalog-driven purge; its retention is the sweep's own bounded DELETE).
///
/// <para><b>#1776 own-store</b> — the live tests here mint their own scratch databases via
/// ScratchPostgres (they apply compression policies and drive run_job, which the shared fixture must
/// never inherit), so they cannot race the shared store and serializing them would be pure slowdown.</para>
/// </summary>
public sealed class StoreSelfMetricsTests
{
    [Fact]
    public void V53_MigrationIdentity_AndStorageVersionTracksTheNewestRung()
    {
        var v53 = PgMigrations.Scripts.Single(m => m.Version == 53);

        Assert.Equal("store-self-metrics", v53.Name);
        /* The invariant the test name states, with no literal to go stale: the build's schema version IS
           the newest registered rung. Three in-flight branches bumping versions made the literal form a
           recurring multi-test failure (#2210 round, again here at V62). */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);

        /* collect.-qualified like V44/V47/V49, and idempotent so a re-run is a no-op. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.store_metrics (", v53.Sql, StringComparison.Ordinal);
        Assert.Contains(
            "CREATE INDEX IF NOT EXISTS idx_store_metrics_time ON collect.store_metrics(metric_time);",
            v53.Sql,
            StringComparison.Ordinal);

        /* A PLAIN table by design — the compression/retention machinery this table measures must never
           recurse onto it, and a tiny hourly series needs neither chunks nor compression. */
        Assert.DoesNotContain("create_hypertable", v53.Sql, StringComparison.OrdinalIgnoreCase);

        /* Every column the sweep writes must exist in the migration, or the first hourly run after an
           upgrade fails on a column fresh code writes and the upgraded store lacks. */
        foreach (var column in new[]
        {
            "metric_time", "object_name", "object_kind", "total_bytes", "compressed_before_bytes",
            "compressed_after_bytes", "chunk_count", "row_count", "enabled_server_count",
        })
        {
            Assert.Contains($"    {column} ", v53.Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void V56_JobTelemetryColumns_MigrationAndSweepAgree_AndTheProbeKnowsTheRung()
    {
        /* #2136: every column the background-job sweep arm writes must exist in the V56 migration, or the
           first hourly run after an upgrade fails on a column fresh code writes and the store lacks —
           the exact failure class the V53 column pin above guards. */
        var v56 = PgMigrations.Scripts.Single(m => m.Version == 56);
        Assert.Equal("store-metrics-background-jobs", v56.Name);
        foreach (var column in new[]
        {
            "last_run_duration_ms", "schedule_interval_ms", "total_runs", "total_failures",
        })
        {
            Assert.Contains($"ADD COLUMN IF NOT EXISTS {column} bigint", v56.Sql, StringComparison.Ordinal);
            Assert.Contains(column, StoreSelfMetrics.BackgroundJobInsertSql, StringComparison.Ordinal);
        }

        /* The insert reads only TimescaleDB catalog surfaces, which is why the sweep gates it with the
           hypertable arm — a plain-PG store skips it silently. schedule_interval rides along so
           "duration vs cadence" — the tripwire that matters — is one division over the stored series. */
        Assert.Contains("FROM timescaledb_information.job_stats", StoreSelfMetrics.BackgroundJobInsertSql, StringComparison.Ordinal);
        Assert.Contains("JOIN timescaledb_information.jobs", StoreSelfMetrics.BackgroundJobInsertSql, StringComparison.Ordinal);
        Assert.Contains("'background_job'", StoreSelfMetrics.BackgroundJobInsertSql, StringComparison.Ordinal);

        /* The probe sentinel + arm: a fully-migrated V56 store maps to exactly the required version
           (the connect-time-gate trap), and a V55 store without the columns caps at 55. */
        Assert.Contains("column_name = 'last_run_duration_ms'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Equal(56, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: true));
        Assert.Equal(55, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: false));
    }

    [Fact]
    public void V57_CadenceKnob_MigrationSettingsAndProbeAgree()
    {
        /* #2136 (the alert half): the knob column the settings surfaces name must exist in the V57
           migration — the same first-run-after-upgrade failure class the V53/V56 column pins guard. */
        var v57 = PgMigrations.Scripts.Single(m => m.Version == 57);
        Assert.Equal("store-job-cadence-knob", v57.Name);
        Assert.Contains(
            "ADD COLUMN IF NOT EXISTS store_job_cadence_warn_percent integer NOT NULL DEFAULT 25",
            v57.Sql, StringComparison.Ordinal);

        /* The probe sentinel + arm: a fully-migrated V57 store maps to exactly the required version,
           and a V56 store without the knob caps at 56. */
        Assert.Contains("column_name = 'store_job_cadence_warn_percent'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Equal(57, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: true, hasJobCadenceKnob: true));
        Assert.Equal(56, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: true, hasJobCadenceKnob: false));
    }

    [Fact]
    public void StoreMetrics_IsNotACollectorTable_SoTheMachineryItMeasuresCannotReachIt()
    {
        /* TimescaleSupport's hypertable conversion + compression policies and DarlingRetention's purge both
           enumerate the collector catalog. store_metrics must stay OUT of it: a catalog entry would convert
           the self-metrics table into a hypertable (recursing the machinery onto its own measurement) and
           hand its retention to the policy path instead of the sweep's own 400-day DELETE. */
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, schema => schema.TargetTable == "store_metrics");
    }

    [Fact]
    public void ViewerSchemaGate_KnowsV53_SoAFullyMigratedStoreIsNotRefused()
    {
        /* The trap a StorageVersion bump sets: a probe that cannot SEE the newest migration maps every
           healthy store below RequiredStoreSchemaVersion and the connect-time gate refuses it permanently.
           Invariant form, no literal to go stale: the gate always requires exactly the build's version. */
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
        Assert.Contains("table_name = 'store_metrics'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        /* The V53 arm: store_metrics present (and everything below it, but NOT V54's gz column —
           hasPlanDimGzip defaults false) maps to exactly 53, the mid-ladder rung this feature added. */
        Assert.Equal(53, ViewerDataService.MapProbedSchemaVersion(
            hasConfigControlPlane: true, hasAlertDeliveryOverride: true, hasAnalysisState: true,
            hasAlertTuningKnobs: true, hasDefaultTraceEvents: true, hasIndexObjectStatsLatestIndex: true,
            hasCollectionLogHypertableOrPlainPg: true, hasJobHistory: true, hasAgentStatus: true,
            hasGenericWebhook: true, hasDeadlocksDatabaseName: true, hasQueryStoreReplicaRole: true,
            hasLongQueryCompletions: true, hasWebDashboardConfig: true, hasCustomViews: true,
            hasServerTags: true, hasConnectionRefireKnobs: true, hasAgCollectors: true,
            hasAgAlertKnobs: true, hasAgLatencyColumns: true, hasAgDisconnectRefire: true,
            hasPayloadDimensions: true, hasDimFloorIndexes: true, hasBlockingWaitThreshold: true,
            hasQueryStoreIntervalIdentity: true, hasPagerDutyWebhook: true, hasPagerDutyProxy: true,
            hasCollectorState: true, hasPlanCorrection: true, hasPvsStats: true,
            hasPvsPressureKnobs: true, hasDatabaseStateAlert: true, hasServerTagColour: true,
            hasQueryStatsHostObject: true, hasFindingDrillDown: true, hasStoreMetrics: true));
    }

    [Fact]
    public void HypertableInsertSql_ReadsTheThreeTimescaleCatalogSurfaces_TimescaleOnlyByConstruction()
    {
        var sql = StoreSelfMetrics.HypertableInsertSql;

        /* The three reads the issue names — the enumeration, the size, and the compression stats. All
           TimescaleDB-only objects, which is why the sweep gates this statement on the worker's cached
           TimescaleSupport detection and a plain-PG store skips it silently. */
        Assert.Contains("FROM timescaledb_information.hypertables", sql, StringComparison.Ordinal);
        Assert.Contains("hypertable_detailed_size", sql, StringComparison.Ordinal);
        Assert.Contains("chunk_compression_stats", sql, StringComparison.Ordinal);

        Assert.Contains("INSERT INTO collect.store_metrics", sql, StringComparison.Ordinal);
        Assert.Contains("'hypertable'", sql, StringComparison.Ordinal);
        /* The regclass is built from the catalog view's own rows via format('%I.%I', ...) — never input. */
        Assert.Contains("format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DimensionInsertSql_CoversBothPayloadDims_WithTotalRelationSizeAndExactRowCount()
    {
        var sql = StoreSelfMetrics.DimensionInsertSql;

        /* The dims are the store's dominant payloads (measured: query_plan_dim alone was 101 GB of a
           147 GB store) and invisible to every hypertable surface — this is the row that makes the single
           biggest forecasting term a stored series. pg_total_relation_size because the plan XML lives in
           TOAST, which per-table heap sizes do not count. */
        Assert.Contains(PayloadDimensions.QueryTextDimTable, sql, StringComparison.Ordinal);
        Assert.Contains(PayloadDimensions.QueryPlanDimTable, sql, StringComparison.Ordinal);
        Assert.Contains("pg_total_relation_size", sql, StringComparison.Ordinal);
        Assert.Contains("'dimension'", sql, StringComparison.Ordinal);
        Assert.Contains("count(*)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StoreInsertSql_WholeStoreSize_PlusTheEnabledServerDenominator()
    {
        var sql = StoreSelfMetrics.StoreInsertSql;

        /* pg_database_size is the same read the disk-pressure check and the Viewer status bar use, and
           is_enabled is the fleet reader's own registry predicate — so the per-server rate divides by
           exactly the servers the fleet surfaces count. */
        Assert.Contains("pg_database_size(current_database())", sql, StringComparison.Ordinal);
        Assert.Contains("'store'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.servers WHERE is_enabled", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Retention_IsTheSweepsOwnBoundedDelete_At400Days()
    {
        /* One DELETE inside the sweep — deliberately no policy machinery on a plain ~30-rows/hour table. */
        Assert.Equal(400, StoreSelfMetrics.RetentionDays);
        Assert.Contains("DELETE FROM collect.store_metrics", StoreSelfMetrics.RetentionDeleteSql, StringComparison.Ordinal);
        Assert.Contains("WHERE metric_time < $1", StoreSelfMetrics.RetentionDeleteSql, StringComparison.Ordinal);
    }

    /* ---------------- #3582: the rows the inventory was blind to, and the reconciliation ---------------- */

    /// <summary>
    /// #3582: the aggregate rows are sized through the MATERIALIZATION and named by the VIEW. Pinned
    /// because the hypertable arm cannot be made to see them — <c>timescaledb_information.hypertables</c>
    /// ends <c>AND ca.mat_hypertable_id IS NULL</c> on 2.28.1 — so a second enumeration over the
    /// aggregates view is the only route, and it has to size the internal hypertable while reporting the
    /// name an operator knows. The chunk count comes from the <c>chunks</c> view and NOT from a count over
    /// <c>chunk_compression_stats</c>, which returns zero rows for a hypertable whose compression is off
    /// (measured: a two-chunk materialization yielded no rows until compression was enabled).
    /// </summary>
    [Fact]
    public void ContinuousAggregateInsertSql_SizesTheMaterialization_UnderTheViewName()
    {
        var sql = StoreSelfMetrics.ContinuousAggregateInsertSql;

        Assert.Contains("FROM timescaledb_information.continuous_aggregates ca", sql, StringComparison.Ordinal);
        Assert.Contains("ca.view_name,", sql, StringComparison.Ordinal);
        Assert.Contains($"'{StoreSelfMetrics.ContinuousAggregateObjectKind}'", sql, StringComparison.Ordinal);
        Assert.Equal("continuous_aggregate", StoreSelfMetrics.ContinuousAggregateObjectKind);

        /* Both size functions take the MATERIALIZATION regclass, built from the view's own columns. */
        const string mat = "format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass";
        Assert.Contains($"hypertable_detailed_size({mat})", sql, StringComparison.Ordinal);
        Assert.Contains($"chunk_compression_stats({mat})", sql, StringComparison.Ordinal);

        /* The chunk count: from the chunks view, matched on the materialization's name, never inferred
           from the compression-stats row count. */
        Assert.Contains("FROM timescaledb_information.chunks ch", sql, StringComparison.Ordinal);
        Assert.Contains("ch.hypertable_name = ca.materialization_hypertable_name", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"count\(\*\)::integer AS chunk_count\s+FROM chunk_compression_stats", sql);

        /* And the hypertable arm is UNFILTERED — the omission is the view's, not a predicate of ours. */
        Assert.DoesNotContain("WHERE", StoreSelfMetrics.HypertableInsertSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3582: the three product-owned plain tables the walk used to lump are named, schema-qualified, sized
    /// with <c>pg_total_relation_size</c> and row-counted from the planner's estimate — and the census predicate that keeps them OUT
    /// of the catch-all names the same three by <c>(schema, relation)</c>. The two halves are pinned
    /// against each other: a table named here and not there would be counted twice, one named there and
    /// not here would vanish from both.
    /// </summary>
    [Fact]
    public void TableInsertSql_NamesTheThreeProductTables_AndTheCensusExcludesExactlyThose()
    {
        var sql = StoreSelfMetrics.TableInsertSql;
        Assert.Equal("table", StoreSelfMetrics.TableObjectKind);
        Assert.Contains($"'{StoreSelfMetrics.TableObjectKind}'", sql, StringComparison.Ordinal);

        var qualified = new[] { QueryStoreTextStore.TableName, QueryStorePlanMap.TableName, StoreSelfMetrics.AlertLogTable };
        Assert.Equal(new[] { "collect.query_store_text", "collect.query_store_plan_map", "config.config_alert_log" }, qualified);

        foreach (var table in qualified)
        {
            Assert.Contains($"'{table}',", sql, StringComparison.Ordinal);
            Assert.Contains($"pg_total_relation_size('{table}')", sql, StringComparison.Ordinal);
            /* The planner's estimate, NULL where it is -1 (never analysed) — never a 15 GiB count(*) an hour. */
            Assert.Contains($"(SELECT CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END FROM pg_class c WHERE c.oid = '{table}'::regclass)", sql, StringComparison.Ordinal);

            /* The census names the same table by the same compound constant, compared against the
               concatenated schema.relation — no hand-typed (schema, relation) tuple to drift (review catch). */
            Assert.Contains($"'{table}'", StoreSelfMetrics.NamedRelationPredicateSql, StringComparison.Ordinal);
        }

        Assert.Contains("(n.nspname || '.' || c.relname) IN (", StoreSelfMetrics.NamedRelationPredicateSql, StringComparison.Ordinal);
        /* The two payload dimensions are in the same predicate, so they leave the catch-all too. */
        Assert.Contains($"'collect.{PayloadDimensions.QueryTextDimTable}'", StoreSelfMetrics.NamedRelationPredicateSql, StringComparison.Ordinal);
        Assert.Contains($"'collect.{PayloadDimensions.QueryPlanDimTable}'", StoreSelfMetrics.NamedRelationPredicateSql, StringComparison.Ordinal);
        /* Exactly five names, and none of them typed by hand: every quoted name in the predicate is one of
           the five constants. */
        var quoted = System.Text.RegularExpressions.Regex.Matches(StoreSelfMetrics.NamedRelationPredicateSql, @"'([^']+)'").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(6, quoted.Length); /* five names plus the '.' separator literal */
        Assert.Equal(
            new[] { $"collect.{PayloadDimensions.QueryTextDimTable}", $"collect.{PayloadDimensions.QueryPlanDimTable}" }.Concat(qualified).OrderBy(x => x, StringComparer.Ordinal),
            quoted.Where(q => q != ".").OrderBy(x => x, StringComparer.Ordinal));
        Assert.Equal(3, sql.Split("UNION ALL").Length);
        Assert.DoesNotContain("count(*)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3582: the two catch-all rows, in both store shapes. The census is the same predicate three ways —
    /// which relations count, which are system, which are already named — and the TimescaleDB variant adds
    /// the three anti-joins that remove what the hypertable and aggregate rows already sized (their roots,
    /// and every chunk relation), while the plain variant names no TimescaleDB catalog at all, because on
    /// that store none exists and the statement would fail. <c>NOT c.relisshared</c> is pinned by itself:
    /// the shared catalogs are outside <c>pg_database_size</c>, and summing them made a rig census EXCEED
    /// the database — an over-100% reconciliation is wrong in the direction nobody checks.
    /// </summary>
    [Theory]
    [InlineData(nameof(StoreSelfMetrics.UnenumeratedInsertSql), true)]
    [InlineData(nameof(StoreSelfMetrics.UnenumeratedPlainInsertSql), false)]
    public void UnenumeratedInsertSql_WritesOtherAndSystem_OverTheSharedCensus(string sqlName, bool timescale)
    {
        var sql = (string)typeof(StoreSelfMetrics).GetField(sqlName, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!.GetValue(null)!;

        Assert.Equal("other", StoreSelfMetrics.OtherObjectKind);
        Assert.Equal("system", StoreSelfMetrics.SystemObjectKind);
        Assert.Contains($"'{StoreSelfMetrics.OtherObjectName}', '{StoreSelfMetrics.OtherObjectKind}'", sql, StringComparison.Ordinal);
        Assert.Contains($"'{StoreSelfMetrics.SystemObjectName}', '{StoreSelfMetrics.SystemObjectKind}'", sql, StringComparison.Ordinal);
        Assert.Contains("FROM census WHERE NOT is_system", sql, StringComparison.Ordinal);
        Assert.Contains("FROM census WHERE is_system", sql, StringComparison.Ordinal);

        /* The shared fragments, verbatim — the MCP reader's live top-N composes the same three. */
        Assert.Contains(StoreSelfMetrics.CensusRelationPredicateSql, sql, StringComparison.Ordinal);
        Assert.Contains(StoreSelfMetrics.SystemSchemaPredicateSql, sql, StringComparison.Ordinal);
        Assert.Contains("NOT " + StoreSelfMetrics.NamedRelationPredicateSql, sql, StringComparison.Ordinal);

        Assert.Contains("c.relkind IN ('r', 'm', 'p', 'S')", StoreSelfMetrics.CensusRelationPredicateSql, StringComparison.Ordinal);
        Assert.Contains("NOT c.relisshared", StoreSelfMetrics.CensusRelationPredicateSql, StringComparison.Ordinal);
        Assert.Contains("starts_with(n.nspname, '_timescaledb_')", StoreSelfMetrics.SystemSchemaPredicateSql, StringComparison.Ordinal);
        Assert.Contains("'pg_catalog', 'information_schema'", StoreSelfMetrics.SystemSchemaPredicateSql, StringComparison.Ordinal);

        /* An empty bucket is a ZERO row, never a missing one — the reconciliation reads absence as "the
           statement did not run". */
        Assert.Contains("coalesce(sum(pg_total_relation_size(oid)), 0)::bigint, count(*)::integer", sql, StringComparison.Ordinal);

        if (timescale)
        {
            Assert.Contains(StoreSelfMetrics.TimescaleInventoriedPredicateSql, sql, StringComparison.Ordinal);
            Assert.Contains("FROM timescaledb_information.hypertables h", sql, StringComparison.Ordinal);
            Assert.Contains("FROM timescaledb_information.continuous_aggregates ca", sql, StringComparison.Ordinal);
            Assert.Contains("FROM _timescaledb_catalog.chunk ch", sql, StringComparison.Ordinal);
            /* Name joins, never a regclass cast a vanished relation would make RAISE. */
            Assert.DoesNotContain("::regclass", StoreSelfMetrics.TimescaleInventoriedPredicateSql, StringComparison.Ordinal);
        }
        else
        {
            /* The plain variant READS no TimescaleDB catalog. It still NAMES timescaledb_information as a
               string literal inside the system-schema predicate, which is a different thing. */
            Assert.DoesNotContain("FROM timescaledb_information", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("FROM _timescaledb_catalog", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #3574 (managed-mode self-proof): the owner's evidence row embeds the SAME evidence SELECT the MCP
    /// reader runs — one string, two consumers — and maps its columns the way the class summary states.
    /// The count is written only where the view admits the sweep's role to every job's history (the two
    /// tests the reader's <c>Visibility</c> derives <c>All</c> from, in SQL), the newest row travels as its
    /// AGE at the sweep, and the window width is computed from the two binds rather than restated.
    /// </summary>
    [Fact]
    public void JobHistoryInsertSql_EmbedsTheSharedEvidenceRead_AndMapsTheOverloadedColumns()
    {
        var sql = StoreSelfMetrics.JobHistoryInsertSql;

        Assert.Equal("job_history", StoreSelfMetrics.JobHistoryObjectKind);
        Assert.Contains($"'{StoreSelfMetrics.JobHistoryObjectKind}'", sql, StringComparison.Ordinal);
        Assert.Contains(StoreSelfMetrics.JobHistoryEvidenceSql, sql, StringComparison.Ordinal);
        Assert.Contains("(metric_time, object_name, object_kind, row_count, total_runs, schedule_interval_ms, last_run_duration_ms)", sql, StringComparison.Ordinal);

        /* $2 is metric_time, $1 the window start the embedded SELECT already binds. */
        Assert.Matches(@"SELECT\s+\$2,\s+e\.reader_role,\s+'job_history',", sql);
        Assert.Contains("h.start_time >= $1", sql, StringComparison.Ordinal);

        /* The census-or-NULL rule for the count. */
        Assert.Matches(@"CASE\s+WHEN e\.reader_is_database_owner_member\s+OR \(e\.job_count > 0 AND e\.owner_member_job_count >= e\.job_count\)\s+THEN e\.rows_observed\s+END", sql);

        /* Population, window width from the binds, age of the newest row — in that column order. */
        Assert.Contains("e.jobs_run_in_window,", sql, StringComparison.Ordinal);
        Assert.Contains("(EXTRACT(EPOCH FROM (($2::timestamp AT TIME ZONE 'UTC') - $1)) * 1000)::bigint", sql, StringComparison.Ordinal);
        Assert.Contains("(EXTRACT(EPOCH FROM (($2::timestamp AT TIME ZONE 'UTC') - e.newest_row_at)) * 1000)::bigint", sql, StringComparison.Ordinal);
        Assert.Equal(24, StoreSelfMetrics.JobHistoryEvidenceWindowHours);
    }

    /// <summary>
    /// The kind vocabulary is the on-disk contract: every kind the sweep writes has a named constant, the
    /// constant appears quoted in the statement that writes it, and no two kinds share a spelling. A
    /// drifted kind returns zero rows to every reader rather than an error.
    /// </summary>
    [Fact]
    public void ObjectKinds_AreNamedOnce_DistinctAndWrittenByTheirArms()
    {
        var kinds = new (string Kind, string Sql)[]
        {
            (StoreSelfMetrics.HypertableObjectKind, StoreSelfMetrics.HypertableInsertSql),
            (StoreSelfMetrics.ContinuousAggregateObjectKind, StoreSelfMetrics.ContinuousAggregateInsertSql),
            (StoreSelfMetrics.BackgroundJobObjectKind, StoreSelfMetrics.BackgroundJobInsertSql),
            (StoreSelfMetrics.DimensionObjectKind, StoreSelfMetrics.DimensionInsertSql),
            (StoreSelfMetrics.TableObjectKind, StoreSelfMetrics.TableInsertSql),
            (StoreSelfMetrics.OtherObjectKind, StoreSelfMetrics.UnenumeratedInsertSql),
            (StoreSelfMetrics.SystemObjectKind, StoreSelfMetrics.UnenumeratedInsertSql),
            (StoreSelfMetrics.JobHistoryObjectKind, StoreSelfMetrics.JobHistoryInsertSql),
            (StoreSelfMetrics.StoreObjectKind, StoreSelfMetrics.StoreInsertSql),
        };

        Assert.Equal(kinds.Length, kinds.Select(k => k.Kind).Distinct(StringComparer.Ordinal).Count());
        Assert.All(kinds, k => Assert.Contains($"'{k.Kind}'", k.Sql, StringComparison.Ordinal));

        /* The three pre-#3582 spellings the shipped stores already hold 400 days of. */
        Assert.Equal("hypertable", StoreSelfMetrics.HypertableObjectKind);
        Assert.Equal("dimension", StoreSelfMetrics.DimensionObjectKind);
        Assert.Equal("background_job", StoreSelfMetrics.BackgroundJobObjectKind);
    }

    [Theory]
    [InlineData(nameof(StoreSelfMetrics.HypertableInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.ContinuousAggregateInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.DimensionInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.TableInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.UnenumeratedInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.UnenumeratedPlainInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.JobHistoryInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.StoreInsertSql))]
    [InlineData(nameof(StoreSelfMetrics.RetentionDeleteSql))]
    public void SweepSql_IsPostgresDialect_PositionalParams_NoBareNow(string sqlName)
    {
        var sql = (string)typeof(StoreSelfMetrics)
            .GetField(sqlName, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!
            .GetValue(null)!;

        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);
        /* No bare now(): every statement stamps the ONE caller-supplied $1 metric_time, which is what
           makes a run's rows join and keeps the timestamps naive UTC by the cross-store contract. */
        Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sweep END TO END against a real TimescaleDB (#2136) — the drift-catcher the SQL pins alone
    /// cannot be: every INSERT the sweep runs must agree with the columns the migrations created, and the
    /// failure class this guards (sweep writes a column the upgraded store lacks) only surfaces when the
    /// statements actually execute. Mints its own scratch store (the #1776 own-store idiom), migrates it,
    /// converts + applies compression policies so real background jobs exist, then asserts one run writes
    /// hypertable, dimension, store, AND background_job rows — the job rows carrying a schedule interval,
    /// because "duration vs cadence" is the series' whole point.
    ///
    /// <para><b>#3582 extends it to the kinds the inventory was blind to, and to the reconciliation.</b>
    /// The aggregates are created first (<see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/>), so
    /// one run must also write one <c>continuous_aggregate</c> row per rollup view, the three named
    /// <c>table</c> rows, exactly one <c>other</c> and one <c>system</c> row, and the owner's
    /// <c>job_history</c> row — and the rows of that one sweep, read back through the real MCP reader and
    /// its real reconciliation, must RECONCILE against the sweep's own <c>pg_database_size</c>: every byte
    /// attributed to some row, residual inside the bar, no object left over from an older sweep. On a rig
    /// the residual was exactly the database directory's non-relation files (161,471 bytes on 17 MiB);
    /// here only the bar is asserted, because refresh policies fire immediately on creation (#1788) and
    /// move the catalogs between the sweep's statements. The connection is the database owner, so the
    /// owner row must carry a COUNT (not the NULL a filtered role writes) and decode as <c>Observed</c>
    /// for the role that swept.</para>
    /// </summary>
    [Fact]
    public async Task Sweep_EndToEnd_WritesEveryObjectKind_IncludingBackgroundJobs_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live self-metrics sweep test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        /* #3582: the aggregates, so the materializations exist to be inventoried. */
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);

        var written = await StoreSelfMetrics.SweepAsync(
            connection, timescaleAvailable: true, DateTime.UtcNow, null, ct);
        Assert.True(written > 0, "the sweep wrote nothing");

        await using var kinds = new NpgsqlCommand($@"
SELECT
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.HypertableObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.DimensionObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.StoreObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.BackgroundJobObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.BackgroundJobObjectKind}' AND schedule_interval_ms > 0),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.ContinuousAggregateObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.ContinuousAggregateObjectKind}' AND total_bytes > 0 AND chunk_count IS NOT NULL),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.TableObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.OtherObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.SystemObjectKind}'),
    count(*) FILTER (WHERE object_kind = '{StoreSelfMetrics.JobHistoryObjectKind}'),
    (SELECT count(*) FROM timescaledb_information.continuous_aggregates)
FROM collect.store_metrics", connection);
        await using var reader = await kinds.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        Assert.True(reader.GetInt64(0) > 0, "no hypertable rows");
        Assert.True(reader.GetInt64(1) > 0, "no dimension rows");
        Assert.Equal(1, reader.GetInt64(2));
        Assert.True(reader.GetInt64(3) > 0, "no background_job rows — the compression policies just applied guarantee jobs exist");
        Assert.True(reader.GetInt64(4) > 0, "background_job rows carry no schedule interval — duration-vs-cadence needs it");

        /* #3582: one aggregate row per aggregate the catalog knows — the population the hypertable arm
           cannot see — each sized (a materialization's root alone is non-zero) and chunk-counted. */
        var aggregatesInCatalog = reader.GetInt64(11);
        Assert.True(aggregatesInCatalog > 0, "EnsureContinuousAggregatesAsync left no aggregates to inventory");
        Assert.Equal(aggregatesInCatalog, reader.GetInt64(5));
        Assert.Equal(aggregatesInCatalog, reader.GetInt64(6));
        Assert.Equal(3, reader.GetInt64(7));
        Assert.Equal(1, reader.GetInt64(8));
        Assert.Equal(1, reader.GetInt64(9));
        Assert.Equal(1, reader.GetInt64(10));
        await reader.CloseAsync();

        /* And the READ path carries the new fields end to end (the review catch: written but never read
           back would leave get_store_metrics returning job rows with null metrics). */
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var latest = await PerformanceMonitor.Darling.Service.Mcp.DarlingStoreMetricsReader.GetLatestAsync(dataSource, ct);
        var job = latest.FirstOrDefault(r => r.ObjectKind == StoreSelfMetrics.BackgroundJobObjectKind);
        Assert.NotNull(job);
        Assert.True(job!.ScheduleIntervalMs is > 0, "the reader dropped the job's schedule interval");

        /* #3582: the reconciliation, through the real reader over the real rows. One sweep, so no row is
           from an older one; both catch-all rows present; every byte attributed inside the bar; and the
           named rows are a non-trivial share even of an empty store (roots and indexes are real bytes). */
        var inventory = PerformanceMonitor.Darling.Service.Mcp.DarlingStoreMetricsReader.ComputeInventory(latest);
        Assert.NotNull(inventory);
        Assert.Equal(0, inventory!.StaleRowCount);
        Assert.True(inventory.CatchAllPresent, "a catch-all row is missing from the sweep");
        Assert.True(inventory.EnumeratedBytes > 0);
        Assert.True(inventory.DatabaseBytes > inventory.EnumeratedBytes);
        Assert.True(inventory.Reconciled,
            $"the inventory did not reconcile: database {inventory.DatabaseBytes}, attributed {inventory.AttributedBytes}, "
            + $"residual {inventory.ResidualBytes}, bar {inventory.ToleranceBytes}");
        Assert.Contains(StoreSelfMetrics.ContinuousAggregateObjectKind, inventory.BytesByKind.Keys);
        Assert.Contains(StoreSelfMetrics.TableObjectKind, inventory.BytesByKind.Keys);

        /* #3574: the owner's row. This connection is the database owner, so the sweep's role was admitted
           to every job's history and wrote a COUNT; decoded, that is an Observed reading for that role,
           over the 24-hour window, taken moments ago. */
        var history = Assert.Single(latest, r => r.ObjectKind == StoreSelfMetrics.JobHistoryObjectKind);
        Assert.NotNull(history.RowCount);
        Assert.NotNull(history.TotalRuns);
        Assert.Equal(StoreSelfMetrics.JobHistoryEvidenceWindowHours * 3_600_000L, history.ScheduleIntervalMs);

        var owner = PerformanceMonitor.Darling.Service.Mcp.DarlingStoreMetricsReader.OwnerJobHistoryEvidence.FromLatest(latest, DateTime.UtcNow);
        Assert.Equal(PerformanceMonitor.Darling.Service.Mcp.DarlingStoreMetricsReader.OwnerJobHistoryEvidenceStatus.Observed, owner.Status);
        Assert.Equal(history.ObjectName, owner.ReaderRole);
        Assert.Equal(history.RowCount, owner.RowsObserved);
        Assert.Equal(24.0, owner.WindowHours);
        Assert.True(owner.AgeHours is >= 0 and < 1);
        /* A row seen implies a newest-row instant that decodes to no later than the sweep itself. */
        if (owner.RowsObserved > 0)
        {
            Assert.NotNull(owner.NewestRowAt);
            Assert.True(owner.NewestRowAt <= owner.ObservedAt);
        }
    }

    /* ---------------- #2136 synthetic scale test ---------------- */

    /// <summary>
    /// The #2136 capacity claim, proven end to end rather than asserted from one production observation.
    /// One throwaway hypertable with a compression policy that is PARKED except when a measurement
    /// deliberately arms it (created parked in one transaction — the #1888 discipline — so no background
    /// tick ever races a measurement, the #2143 class), driven at 1x and then 10x row volume:
    /// <list type="number">
    /// <item>a scheduler-driven run at each scale (arm, poll last_successful_finish, park — foreground
    /// run_job does NOT update this accounting, CI-proved). Each run must COMPRESS THE CHUNK ITS SEED
    /// CREATED, and that chunk must hold exactly the seeded row count, so the escalation is real work at
    /// two genuinely different scales rather than two no-ops; and job_stats.last_run_duration must be
    /// measurable at BOTH scales — the premise the whole telemetry stands on;</item>
    /// <item>a self-metrics sweep after each run; the store_metrics series must carry both readings, in
    /// order — this is the series an operator (and the cadence alert's detail text) trends;</item>
    /// <item>alter_job shrinks the schedule interval to half the measured 10x duration, and the REAL
    /// evaluator, fed by the REAL <see cref="TimescaleSupport.ReadJobCadenceReadingsAsync"/> against this
    /// store, must fire the Critical tier under the storejob: key.</item>
    /// </list>
    /// Seeds are midday-anchored (#1972) so a run near midnight cannot split a chunk. Ordering the
    /// per-day counts ascending is safe across a midnight rollover too: the anchors are re-evaluated per
    /// seed, so warm-up stays the oldest day and 10x the newest whichever side of midnight each lands on.
    ///
    /// <para><b>#2266: there is deliberately NO assertion that the 10x run took LONGER than the 1x run,
    /// and one must not be reintroduced.</b> That assertion was the flake, and it is unfixable by tuning
    /// because it is a benchmark of TimescaleDB's compression throughput on shared CI hardware, not a
    /// claim about this product. Measured on a rig (TimescaleDB 2.29 / PG17, 15 consecutive runs of this
    /// exact sequence): d1 lands at 24–39 ms and d10 at 109–167 ms, so a 10x volume increase buys only
    /// ~3.2x the duration — about <b>85 ms</b> of absolute signal, because compression cost is largely
    /// fixed per run. CI's observed baseline for the same pair is 690–970 ms, i.e. roughly twenty times
    /// that fixed cost, so the volume-dependent component there is ~10% of the measurement's own
    /// magnitude and sits comfortably inside the run-to-run variance of launching a background worker on
    /// Windows. Both reported failures are exactly that: d1=970/d10=863 and then d1=689/d10=689. The
    /// earlier reading of the byte-identical pair as proof of a mechanism (both runs compressing nothing)
    /// is refuted by the rig — chunk counts go 1, 2, 3 and the per-day counts are exactly
    /// 2000/50000/500000 on all 15 runs — and it was never as improbable as it looked, because the pair
    /// is only ever read when the test FAILS, which selects for differences already near zero.
    /// Raising the volumes cannot rescue it either: at ~0.19 ms per thousand rows it would take millions
    /// of rows per chunk to clear a variance nobody has measured on the platform that actually fails.
    /// What the product owns is that a real duration is measured, recorded in order, and drives the
    /// cadence alert — all three asserted below, deterministically. What TimescaleDB owns is how long
    /// compressing a chunk takes, and this suite is not the place to police it. #2160's 4x-was-not-enough
    /// finding (d1=279ms, d4=217ms) was the same signal being read as a volume problem; 10x did not fix
    /// it and no multiple would have.</para>
    /// </summary>
    [Fact]
    public async Task ScaleTest_EachRunCompressesItsOwnChunk_TelemetryRecordsBothRuns_AndTheAlertFires_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #2136 scale test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        const string Table = "tick2136_scale";

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");

        /* Throwaway hypertable + compression, policy created PARKED in one transaction (#1888): the
           scheduler is a separate backend and must never see an armed job, or a background run races the
           deterministic run_job calls below and the durations stop being ours. */
        await ExecAsync(connection,
            $"CREATE TABLE collect.{Table} (collection_time timestamp NOT NULL, server_id integer NOT NULL, value bigint)", ct);
        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql($"collect.{Table}", "collection_time"), ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql($"collect.{Table}"), ct);
        await using (var tx = await connection.BeginTransactionAsync(ct))
        {
            await using (var create = new NpgsqlCommand(TimescaleSupport.AddCompressionPolicySql($"collect.{Table}"), connection, tx))
            {
                await create.ExecuteNonQueryAsync(ct);
            }
            await using (var park = new NpgsqlCommand($@"
SELECT alter_job(job_id, scheduled => false)
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection, tx))
            {
                await park.ExecuteNonQueryAsync(ct);
            }
            await tx.CommitAsync(ct);
        }

        var jobId = Convert.ToInt64((await new NpgsqlCommand($@"
SELECT job_id
FROM timescaledb_information.jobs
WHERE hypertable_schema = 'collect' AND hypertable_name = '{Table}'
AND   (proc_name LIKE '%compression%' OR proc_name LIKE '%columnstore%')", connection).ExecuteScalarAsync(ct))!);

        /* Warm-up: the first run of a policy pays one-time costs (worker spin-up, catalog warm-up) that
           would swamp d1. Run once on a token chunk and discard the measurement. Doubles as the canary
           that this scratch database HAS a scheduler: if it never runs, the arm-and-poll below fails with
           its own diagnosis rather than a mystery. It also establishes the compressed-chunk baseline the
           two measured runs are counted against, so a warm-up that silently compressed nothing shows up
           as a wrong count after 1x rather than as a mystery duration. */
        await SeedTickRowsAsync(connection, Table, daysBack: 12, rows: 2_000, ct);
        await RunJobViaSchedulerAsync(connection, jobId, ct);

        /* 1x: one closed chunk, 50k rows. */
        await SeedTickRowsAsync(connection, Table, daysBack: 10, rows: 50_000, ct);
        await RunJobViaSchedulerAsync(connection, jobId, ct);
        long d1 = await ReadJobDurationMsAsync(connection, jobId, ct);
        var work1 = await ReadCompressionWorkAsync(connection, Table, ct);
        /* Branch rather than pass the describe call into Assert.True's message: that argument is a plain
           string, so it is evaluated eagerly and would spend two live catalog queries on every PASSING run
           to build a message nobody reads (review catch). The helper exists to explain a failure, so it
           should only run when there is one. */
        if (d1 <= 0)
        {
            Assert.Fail(
                "a scheduler-driven run left job_stats.last_run_duration unmeasurable — the premise the " +
                "V56 telemetry and the #2141 alert both stand on. (Foreground run_job is already known " +
                "not to update this accounting — CI proved that on this test's first version — which is " +
                "why the runs go through the real scheduler.)" +
                $"\n  what the job did: {await DescribeJobWorkAsync(connection, Table, jobId, ct)}");
        }

        await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: true, DateTime.UtcNow, null, ct);

        /* 10x: one closed chunk, 500k rows. */
        await SeedTickRowsAsync(connection, Table, daysBack: 8, rows: 500_000, ct);
        await RunJobViaSchedulerAsync(connection, jobId, ct);
        long d10 = await ReadJobDurationMsAsync(connection, jobId, ct);
        var work10 = await ReadCompressionWorkAsync(connection, Table, ct);
        if (d10 <= 0)
        {
            /* Same eager-evaluation reason as the d1 branch above. */
            Assert.Fail(
                "the 10x run left job_stats.last_run_duration unmeasurable. Asserted separately from d1 " +
                "(#2266): ReadJobDurationMsAsync maps a NULL duration to 0, and the telemetry check below " +
                "compares the series against these same variables, so an unmeasurable 10x run used to " +
                "satisfy 0 == 0 and pass." +
                $"\n  what the job did: {await DescribeJobWorkAsync(connection, Table, jobId, ct)}");
        }

        await StoreSelfMetrics.SweepAsync(connection, timescaleAvailable: true, DateTime.UtcNow.AddSeconds(2), null, ct);

        /* 1. The escalation is REAL WORK at two different scales — asserted on rows and chunks, which are
           exact, instead of on the two durations, which are a benchmark of somebody else's compression
           engine (see the #2266 block in the summary for the measurements that settle that). Each measured
           run must have compressed the chunk its own seed created, and that chunk must hold exactly the
           seeded row count. Per-day counts double as the "one chunk per seed" check: 1-day chunks make day
           groups and chunks the same thing, so a seed that straddled midnight would show up as an extra
           group rather than as a quietly halved workload.

           This is the assertion the durations were standing in for, and it is strictly stronger: the
           hypothesis the intermittent failures raised — that both runs compressed nothing and the whole
           cost was fixed overhead — is a hard failure here, at the step where it happens, instead of
           being invisible behind a timing comparison that fails for two unrelated reasons. */
        Assert.Equal(new long[] { 2_000, 50_000 }, work1.RowsPerDay);
        Assert.Equal(2, work1.ChunksTotal);
        Assert.True(work1.ChunksCompressed == 2,
            $"the 1x run did not leave both chunks compressed ({work1}) — the 50k seed did not become " +
            "compressible work, so this test would be measuring fixed overhead twice rather than the " +
            $"#2136 capacity model (#2266). d1={d1}ms.");

        Assert.Equal(new long[] { 2_000, 50_000, 500_000 }, work10.RowsPerDay);
        Assert.Equal(3, work10.ChunksTotal);
        Assert.True(work10.ChunksCompressed == 3,
            $"the 10x run did not leave all three chunks compressed ({work10}) — the 500k seed did not " +
            $"become compressible work (#2266). d1={d1}ms, d10={d10}ms.");

        /* 2. The telemetry recorded both runs: two series points for this job, in order, carrying the
           durations the job actually reported. This is the product's half of #2136 — whatever duration
           TimescaleDB took, the V56 series has it, in order, ready for the cadence comparison in step 3. */
        await using (var series = new NpgsqlCommand(@"
SELECT last_run_duration_ms
FROM collect.store_metrics
WHERE object_kind = 'background_job' AND object_name LIKE '%' || $1 || '%'
ORDER BY metric_time", connection))
        {
            series.Parameters.AddWithValue(Table);
            var points = new List<long>();
            await using var reader = await series.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                points.Add(reader.GetInt64(0));
            }

            Assert.Equal(2, points.Count);
            Assert.Equal(d1, points[0]);
            Assert.Equal(d10, points[1]);
        }

        /* 3. The alert fires from REAL readings: shrink the schedule interval to half the measured 10x
           duration (percent ≈ 200), then run the real reader into the real evaluator. */
        await ExecAsync(connection, $@"
SELECT alter_job({jobId}::integer, schedule_interval => (
    SELECT last_run_duration / 2 FROM timescaledb_information.job_stats WHERE job_id = {jobId}))", ct);

        var readings = await TimescaleSupport.ReadJobCadenceReadingsAsync(connection, null, ct);
        var tickReading = Assert.Single(readings, r => r.JobId == jobId);
        Assert.True(tickReading.LastRunDurationMs is > 0 && tickReading.ScheduleIntervalMs > 0,
            "the cadence reader dropped the duration or interval for the tick job");

        var deliverer = new CadenceRecordingDeliverer();
        var evaluator = new DarlingSelfAlertEvaluator(
            new CadenceFakeSettings(), deliverer, new CadenceFakeHistoryStore(), _ => false);
        await evaluator.EvaluateStoreJobCadenceAsync(new[] { tickReading }, ct);

        var fired = Assert.Single(deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.JobCadenceMetric, fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Equal($"storejob:{jobId}", fired.ServerKey);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Seeds one closed, compression-eligible chunk: midday-anchored (#1972) N days back,
    /// spreading rows across seconds inside the day so they stay in ONE chunk.</summary>
    private static Task SeedTickRowsAsync(
        NpgsqlConnection connection, string table, int daysBack, int rows, CancellationToken ct) =>
        ExecAsync(connection, $@"
INSERT INTO collect.{table}
SELECT date_trunc('day', now()::timestamp) - INTERVAL '{daysBack} days' + INTERVAL '12 hours'
       + ((g % 40000) || ' milliseconds')::interval,
       {8850},
       g
FROM generate_series(1, {rows}) AS g", ct);

    /// <summary>
    /// Runs the job through the REAL scheduler — arm with <c>next_start => now()</c>, poll
    /// <c>total_runs</c> until it increments, park again. Foreground <c>run_job</c> deliberately NOT
    /// used: CI proved it does not update <c>job_stats.last_run_duration</c> (that accounting lives in
    /// the scheduler path), and the scheduler path is the one production's telemetry actually reads —
    /// so this is both the working mechanism and the honest one. Parking between measurements keeps
    /// each run's chunks OURS (the #1888 concern, inverted: armed on purpose, once, per measurement;
    /// the next background tick is an hour out, far beyond the test's lifetime).
    /// </summary>
    private static async Task RunJobViaSchedulerAsync(NpgsqlConnection connection, long jobId, CancellationToken ct)
    {
        /* Poll on last_successful_finish, NOT total_runs: total_runs increments when a run STARTS, and
           job_stats reports last_run_duration as NULL while the run is in flight — CI proved it, by
           catching the larger run mid-flight and reading 0ms (the 1x run had merely finished inside one
           poll tick). last_successful_finish only advances at COMPLETION, so a read after it moves is
           a read of a finished run's accounting. */
        var before = await ReadLastSuccessfulFinishAsync(connection, jobId, ct);
        await ExecAsync(connection, $"SELECT alter_job({jobId}::integer, scheduled => true, next_start => now())", ct);

        var deadline = DateTime.UtcNow.AddSeconds(90);
        while (await ReadLastSuccessfulFinishAsync(connection, jobId, ct) <= before)
        {
            Assert.True(DateTime.UtcNow < deadline,
                $"the scheduler did not COMPLETE a run of job {jobId} within 90s of next_start => now() — " +
                "either this scratch database has no scheduler, the cluster is out of background workers " +
                "(see CiClusterWorkerSizingTests for the sizing this suite depends on), or the run failed " +
                "(last_successful_finish never advances for a failed run — check job_stats.last_run_status)");
            await Task.Delay(500, ct);
        }

        await ExecAsync(connection, $"SELECT alter_job({jobId}::integer, scheduled => false)", ct);
    }

    /// <summary>
    /// What the compression job measurably ACHIEVED, as exact counts the scale test asserts on (#2266) —
    /// as opposed to <see cref="DescribeJobWorkAsync"/>, which is a best-effort string for explaining a
    /// failure and deliberately swallows its own faults. This one THROWS, because a Timescale view that
    /// stopped answering is a real failure of the thing being asserted rather than a cosmetic gap in a
    /// message.
    ///
    /// <para><c>RowsPerDay</c> is ordered by day ascending, which — with the 1-day chunk interval this
    /// hypertable is created at — makes it both the per-chunk row census and the "each seed produced
    /// exactly one chunk" check. Counting rows through the hypertable rather than reading a compression
    /// stats view is deliberate: compressed chunks stay transparently queryable, so a plain
    /// <c>count(*)</c> is exact and needs none of the pre/post-2.18 columnstore-vs-compression view
    /// vocabulary the rest of this file has to hedge on.</para>
    /// </summary>
    private sealed record CompressionWork(long ChunksTotal, long ChunksCompressed, long[] RowsPerDay)
    {
        public override string ToString() =>
            $"chunks={ChunksTotal} compressed={ChunksCompressed} rowsPerDay=[{string.Join(", ", RowsPerDay)}]";
    }

    private static async Task<CompressionWork> ReadCompressionWorkAsync(
        NpgsqlConnection connection, string table, CancellationToken ct)
    {
        long total;
        long compressed;
        await using (var chunks = new NpgsqlCommand(@"
SELECT
    count(*) AS chunks_total,
    count(*) FILTER (WHERE is_compressed) AS chunks_compressed
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect' AND hypertable_name = $1", connection))
        {
            chunks.Parameters.AddWithValue(table);
            await using var reader = await chunks.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "timescaledb_information.chunks returned no row");
            total = reader.GetInt64(0);
            compressed = reader.GetInt64(1);
        }

        var rowsPerDay = new List<long>();
        await using (var perDay = new NpgsqlCommand($@"
SELECT count(*) AS row_count
FROM collect.{table}
GROUP BY date_trunc('day', collection_time)
ORDER BY date_trunc('day', collection_time)", connection))
        {
            await using var reader = await perDay.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rowsPerDay.Add(reader.GetInt64(0));
            }
        }

        return new CompressionWork(total, compressed, rowsPerDay.ToArray());
    }

    private static async Task<DateTime> ReadLastSuccessfulFinishAsync(NpgsqlConnection connection, long jobId, CancellationToken ct)
    {
        /* -infinity (never finished) maps to DateTime.MinValue via Npgsql, which orders below every real
           finish — exactly the "before" baseline a first run needs. */
        await using var command = new NpgsqlCommand(
            "SELECT coalesce(last_successful_finish, '-infinity'::timestamptz) FROM timescaledb_information.job_stats WHERE job_id = $1",
            connection);
        command.Parameters.AddWithValue(jobId);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DateTime finish ? finish : DateTime.MinValue;
    }

    /// <summary>
    /// What the compression job actually DID, as one line for a failure message (#2266) — the job-side
    /// context (<c>total_runs</c>, <c>last_run_status</c>, <c>last_successful_finish</c>) that says whether a
    /// run happened at all and whether it succeeded. Attached to the two duration-measurability assertions,
    /// which are the ones where "did the run even complete" is the question a reader has next.
    ///
    /// <para>Distinct from <see cref="ReadCompressionWorkAsync"/>, which returns exact counts the test
    /// ASSERTS on. The split is the point: this one is prose for a human reading a failure, so it must never
    /// throw, and that same property makes it unfit to assert against.</para>
    ///
    /// <para>Deliberately best-effort and never throwing: it exists to explain a failure, so a fault here must
    /// not replace the assertion's own message with its own — that is the #1902 mistake in miniature. A missing
    /// Timescale view or a renamed column degrades to a note saying so.</para>
    /// </summary>
    private static async Task<string> DescribeJobWorkAsync(
        NpgsqlConnection connection, string table, long jobId, CancellationToken ct)
    {
        try
        {
            await using var command = new NpgsqlCommand(@"
SELECT
    (SELECT count(*) FROM timescaledb_information.chunks
     WHERE hypertable_schema = 'collect' AND hypertable_name = $1) AS chunks_total,
    (SELECT count(*) FROM timescaledb_information.chunks
     WHERE hypertable_schema = 'collect' AND hypertable_name = $1 AND is_compressed) AS chunks_compressed,
    (SELECT total_runs::bigint FROM timescaledb_information.job_stats WHERE job_id = $2) AS total_runs,
    (SELECT last_run_status::text FROM timescaledb_information.job_stats WHERE job_id = $2) AS last_run_status,
    (SELECT last_successful_finish::text FROM timescaledb_information.job_stats
     WHERE job_id = $2) AS last_successful_finish",
                connection);
            command.Parameters.AddWithValue(table);
            command.Parameters.AddWithValue(jobId);

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return "(job_stats returned no row)";
            }

            return $"chunks={reader.GetInt64(0)} compressed={reader.GetInt64(1)} " +
                   $"total_runs={(reader.IsDBNull(2) ? "?" : reader.GetInt64(2).ToString(CultureInfo.InvariantCulture))} " +
                   $"last_run_status={(reader.IsDBNull(3) ? "?" : reader.GetString(3))} " +
                   $"last_successful_finish={(reader.IsDBNull(4) ? "?" : reader.GetString(4))}";
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Broad on purpose: see the summary. An explanation that throws is worse than no explanation,
               because it replaces the failure being explained. */
            return $"(could not describe the job's work: {ex.GetType().Name}: {ex.Message})";
        }
    }

    private static async Task<long> ReadJobDurationMsAsync(NpgsqlConnection connection, long jobId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT (EXTRACT(EPOCH FROM last_run_duration) * 1000)::bigint FROM timescaledb_information.job_stats WHERE job_id = $1",
            connection);
        command.Parameters.AddWithValue(jobId);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? 0L : Convert.ToInt64(value);
    }

    /* Minimal local fakes: only AlertsEnabled + CooldownMinutes matter to the cadence path; the rest
       satisfy the interface at inert defaults. Local copies rather than sharing DarlingSelfAlertTests'
       private harness — the coupling worth having is the READING record, not the test scaffolding. */

    private sealed class CadenceRecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }
    }

    private sealed class CadenceFakeHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
    }

    private sealed class CadenceFakeSettings : IAlertEngineSettings
    {
        public bool AlertsEnabled { get; set; } = true;
        public bool CpuEnabled { get; set; }
        public bool BlockingEnabled { get; set; }
        public bool DeadlockEnabled { get; set; }
        public bool PoisonWaitEnabled { get; set; }
        public bool LongRunningQueryEnabled { get; set; }
        public bool TempDbSpaceEnabled { get; set; }
        public bool LowDiskEnabled { get; set; }
        public bool LongRunningJobEnabled { get; set; }
        public bool FailedJobEnabled { get; set; }
        public bool PvsEnabled { get; set; }
        public bool DatabaseStateEnabled { get; set; }
        public bool ForcePlanFailureEnabled { get; set; } = true;
        public int CpuThresholdPercent { get; set; } = 80;
        public int BlockingCountThreshold { get; set; } = 1;
        public int BlockingWaitSecondsThreshold { get; set; }
        public int DeadlockCountThreshold { get; set; } = 1;
        public int PoisonWaitThresholdMs { get; set; } = 500;
        public int LongRunningQueryThresholdMinutes { get; set; } = 30;
        public int LongRunningQueryMaxResults { get; set; } = 5;
        public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;
        public bool LongRunningQueryExcludeWaitFor { get; set; } = true;
        public bool LongRunningQueryExcludeBackups { get; set; } = true;
        public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;
        public bool LongRunningQueryExcludeCdc { get; set; } = true;
        public int TempDbSpaceThresholdPercent { get; set; } = 80;
        public int LowDiskThresholdPercent { get; set; } = 10;
        public int LowDiskThresholdGb { get; set; } = 5;
        public int DiskCriticalFreePercent { get; set; } = 3;
        public int DiskCriticalFreeGb { get; set; } = 2;
        public int SelfDiskFreeWarnPercent { get; set; } = 10;
        public int SelfDiskFreeWarnGb { get; set; } = 50;
        public int CollectionStaleMinutes { get; set; } = 30;
        public int CollectionFailureThreshold { get; set; } = 10;
        public int PvsThresholdPercent { get; set; } = 40;
        public int PvsFloorGb { get; set; } = 1;

        /* #2349: OFF in the fakes so existing expectations are untouched. */
        public bool FileGrowthEnabled { get; set; }
        public int FileGrowthRiseMb { get; set; } = 10240;
        public int FileGrowthVolumePercent { get; set; } = 60;
        public int FileGrowthLookbackMinutes { get; set; } = 60;
        public int LongRunningJobMultiplier { get; set; } = 3;
        public int FailedJobLookbackMinutes { get; set; } = 60;
        public int CooldownMinutes { get; set; } = 5;
        public IReadOnlyList<string> ExcludedDatabases { get; } = new List<string>();
        public CpuAlertMode CpuAlertMode { get; set; } = CpuAlertMode.TotalServer;
    }
}
