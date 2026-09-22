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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V137 (partial #3796, partial #3712, partial #3783): four column sets on three EXISTING
/// tables in one rung — (a) the two Query Store capture modes on <c>collect.query_store_health</c>, written by
/// the shared <see cref="QueryStoreHealthCollector"/> with ONE version gate (2017+ for
/// <c>wait_stats_capture_mode</c>); (b) <c>analysis_uncorroborated_route</c> on
/// <c>config.config_alert_settings</c>, the store-backed twin of the file-level knob #3732 shipped, nullable
/// tri-state with a CHECK; (c) <c>toast_bytes</c> and <c>toast_live_bytes</c> on <c>collect.store_metrics</c>,
/// the first written by the dimension-row sweep and the second written NULL until the maintainer picks the
/// instrument; (d) <c>checkpoint_write_ms</c>, <c>checkpoint_sync_ms</c> and <c>checkpoints_requested</c> on
/// <c>collect.store_metrics</c>, the checkpointer row's three counters — written by nothing at the rung, and since
/// #3783's code half written RAW (the server's cumulative figures) by <c>StoreSelfMetrics.CheckpointerInsertSql</c>
/// and differenced by <c>DarlingStoreMetricsReader.CheckpointerReading</c>, the reasoning on the writer. Eight
/// nullable columns, no DEFAULT, no backfill, no new table, no new hypertable, one
/// passthrough refreshed. The shape is <see cref="PerfmonCounterTypeRungTests"/>'s (V132, a column on a shared
/// collector's table plus its view) three times over, with <see cref="TimeHonestyRungTests"/>' (V134) mixed
/// view / no-view arms.
///
/// <para>This file carries the "I am the top rung" claims that moved off
/// <see cref="PgDatabaseSizeStatsAndHostMemoryRungTests"/> (V136) when this rung landed — a fully-migrated store
/// must map to EXACTLY this version, or the viewer's connect-time gate refuses a store that is actually
/// current. When the next rung lands, those claims move on and what stays is everything true of this rung
/// wherever it sits.</para>
///
/// <para>The collector's gate and reader (a 2016 body has ten ordinals and never names the 2017+ column; the
/// row stores NULL there) are <c>Lite.Tests.QueryStoreHealthCollectorDefinitionTests</c>; the DuckDB twin's
/// climb is <c>Lite.Tests.QsCaptureModeRungTests</c>. This file is the RUNG: the ladder, the DDL, the probe,
/// the two writers, and the live round trip.</para>
///
/// <para>It no longer carries the exit criterion's last clause. The rung merged (f2e1816f8) with every one of the
/// eight columns read by nothing, and <c>NoReaderNamesAnyOfTheEightColumnsYet</c> scanned the five product
/// projects, comments stripped, for the column names so each consumer lane would have to release the clause
/// deliberately. It was released the same hour it was pinned, and from four directions at once: #3797's clutter
/// reader (#3814, forty minutes behind the rung) named the <c>query_capture_mode</c> payload KEY — published
/// <c>null</c> beside <c>capture_mode_known: false</c>, a pointer and not a read — and the census could not tell
/// the two apart (dev red from 20215a40e, the first hit <c>DarlingMcpInstructions.cs</c>); #3821 exempted that
/// pair by name and bought one merge; #3822 fenced the clutter view's reader and judgment files; #3796's code
/// half (#3823) landed its readers, renamed the fact to six columns and inverted arm (a) to the positive form —
/// three PRs editing one method body inside an hour, each rebasing over the last, with #3712 (reading and
/// writing <c>analysis_uncorroborated_route</c> through <c>DarlingAlertSettings</c>, the MCP pair and the
/// Settings window) and #3783 (reading the TOAST and checkpointer columns through
/// <c>DarlingStoreMetricsReader</c>) next in the same queue. A "read by nothing" pin cannot outlive the readers
/// it was waiting for, so it was retired whole rather than exempted, renamed or inverted one consumer at a
/// time. The POSITIVE ownership pins — which reader names which column, and what it publishes — live in the
/// owning lanes' tests, where they already were and are stronger than a name scan: (a)'s reader ordinals, row
/// type, description, web catalogue and both grids in <c>DarlingMcpConfigHistoryToolsSurfaceAndSqlTests</c>
/// and <c>Lite.Tests.QueryStoreHealthCaptureModePublishTests</c>; the clutter view's null placeholder in
/// <c>QueryStoreClutterTests</c> and <c>QueryStoreClutterLivePostgresTests</c>; (b), (c) and (d) in #3712's and
/// #3783's tests when they land. This rung test owns the DDL shape, the ladder, the probe, the writers and the
/// Lite twin, not the consumer roster. One arm of that clause does survive, in
/// <see cref="TheDimensionSweepWritesToastBytes_AndNullLiveBytes_AndNoOtherKindTouchesEither"/>: (d) has no
/// WRITER in this rung, so the sweep names none of the three checkpointer columns — a writer pin on one file,
/// owned by exactly one lane (#3783), which retires it when it lands the row.</para>
/// </summary>
public sealed class QsCaptureModeRouteKnobToastRungTests
{
    private const int RungVersion = 137;
    private const int PreviousVersion = 136;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument — V138 (#3691,
    /// the pg_server_config scope columns) appended its own — so this is a position within the signature
    /// rather than its end, the handoff <see cref="PgDatabaseSizeStatsAndHostMemoryRungTests"/> made to this
    /// file one rung ago.</summary>
    private const int ProbeOrdinal = 112;

    private const string HealthTable = "query_store_health";
    private const string SettingsTable = "config_alert_settings";
    private const string MetricsTable = "store_metrics";

    private static readonly string[] CaptureModeColumns = { "query_capture_mode", "wait_stats_capture_mode" };
    private const string RouteColumn = "analysis_uncorroborated_route";
    private static readonly string[] ToastColumns = { "toast_bytes", "toast_live_bytes" };
    private static readonly string[] CheckpointerColumns = { "checkpoint_write_ms", "checkpoint_sync_ms", "checkpoints_requested" };

    private const string RouteCheckName = "config_alert_settings_analysis_uncorroborated_route_check";

    private static PgMigrations.Migration V137 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("qs-capture-mode-route-knob-toast-utilisation", V137.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V138 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Three ADD-COLUMN ALTERs (eight columns), one CHECK inside V62's DO guard, one passthrough refreshed —
    /// nullable, no DEFAULT, no backfill, no table, no index, no data movement — in the order (a), view, (b),
    /// CHECK, (c)+(d). The two collector columns are rendered from the collector's declaration so a type here
    /// that differed from <c>PayloadColumns</c> would fail rather than ship two populations.
    /// </summary>
    [Fact]
    public void TheRungAddsEightNullableColumns_OneCheck_RefreshesOnePassthrough_AndNothingElse()
    {
        var sql = V137.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        /* (a) — the two capture modes, in the collector's declared order and the generator's rendering. */
        var declared = QueryStoreHealthCollector.Instance.PayloadColumns.TakeLast(2).ToList();
        Assert.Equal(CaptureModeColumns, declared.Select(c => c.Name).ToArray());
        foreach (var column in declared)
        {
            Assert.Equal(CollectorColumnType.Varchar, column.Type);
            Assert.Equal("text", PgSchemaGenerator.TypeFor(column));
        }

        Assert.Contains(
            $"ALTER TABLE collect.{HealthTable}\n    ADD COLUMN IF NOT EXISTS query_capture_mode text,\n    ADD COLUMN IF NOT EXISTS wait_stats_capture_mode text;",
            sql, StringComparison.Ordinal);
        Assert.Contains($"CREATE OR REPLACE VIEW collect.v_{HealthTable} AS SELECT * FROM collect.{HealthTable};", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE OR REPLACE VIEW collect\\."));

        /* (b) — the route column, and the CHECK spelled from the router's own wire words. */
        Assert.Contains($"ALTER TABLE config.{SettingsTable}\n    ADD COLUMN IF NOT EXISTS {RouteColumn} text;", sql, StringComparison.Ordinal);
        Assert.Contains($"WHERE conname = '{RouteCheckName}'", sql, StringComparison.Ordinal);
        Assert.Contains($"ADD CONSTRAINT {RouteCheckName}", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"CHECK ({RouteColumn} IS NULL OR {RouteColumn} IN ('{FindingRouting.DigestText}', '{FindingRouting.PageText}'))",
            sql, StringComparison.Ordinal);
        Assert.Equal("digest", FindingRouting.DigestText);
        Assert.Equal("page", FindingRouting.PageText);
        /* One constraint statement (the rung's SQL comment quotes the clause by name to say why it is DO-guarded, so
           the count is on the statement form). */
        Assert.Single(Regex.Matches(sql, $"ADD CONSTRAINT {RouteCheckName}"));
        Assert.Single(Regex.Matches(sql, "DO \\$\\$"));

        /* (c) + (d) — the two TOAST columns and the three checkpointer columns, one ALTER, in this order. */
        Assert.Contains(
            $"ALTER TABLE collect.{MetricsTable}\n    ADD COLUMN IF NOT EXISTS toast_bytes bigint,\n    ADD COLUMN IF NOT EXISTS toast_live_bytes bigint,\n"
            + "    ADD COLUMN IF NOT EXISTS checkpoint_write_ms bigint,\n    ADD COLUMN IF NOT EXISTS checkpoint_sync_ms bigint,\n    ADD COLUMN IF NOT EXISTS checkpoints_requested bigint;",
            sql, StringComparison.Ordinal);

        /* The counts: four ALTER TABLE (three ADD COLUMN, one ADD CONSTRAINT), eight columns: 2 + 1 + 2 + 3. */
        Assert.Equal(4, Regex.Matches(sql, "ALTER TABLE").Count);
        Assert.Equal(8, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);
        Assert.Equal(8, CaptureModeColumns.Length + 1 + ToastColumns.Length + CheckpointerColumns.Length);

        /* In order: (a), its view, (b), its CHECK, (c). */
        var positions = new[]
        {
            sql.IndexOf($"ALTER TABLE collect.{HealthTable}", StringComparison.Ordinal),
            sql.IndexOf("CREATE OR REPLACE VIEW", StringComparison.Ordinal),
            sql.IndexOf($"ALTER TABLE config.{SettingsTable}", StringComparison.Ordinal),
            sql.IndexOf("DO $$", StringComparison.Ordinal),
            sql.IndexOf($"ALTER TABLE collect.{MetricsTable}", StringComparison.Ordinal),
        };
        Assert.All(positions, p => Assert.True(p >= 0));
        Assert.Equal(positions.OrderBy(p => p), positions);

        /* Nothing else. */
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("INSERT ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EXTENSION", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DROP ", sql, StringComparison.Ordinal);

        /* One view refreshed, two not: the health view IS a passthrough (refreshing it with SELECT * is the
           right idiom); the other two tables have no v_ passthrough, and a CREATE OR REPLACE VIEW for either
           would CREATE one the generator does not know about. */
        Assert.Contains($"v_{HealthTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{HealthTable}", PgSchemaGenerator.PayloadResolvingViews);
        Assert.DoesNotContain($"v_{MetricsTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{SettingsTable}", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain($"v_{MetricsTable}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain($"v_{SettingsTable}", sql, StringComparison.Ordinal);

        /* A fresh store gets the two collector columns from the generated CREATE TABLE, LAST — the positional
           COPY writer and an upgraded store's ALTER agree on where they sit. */
        AssertGeneratedTail(QueryStoreHealthCollector.Instance, CaptureModeColumns, "text", "interval_length_minutes ");
        Assert.EndsWith($", {string.Join(", ", CaptureModeColumns)}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(QueryStoreHealthCollector.Instance), StringComparison.Ordinal);

        /* The V101 rule: V76's CREATE carries the two for the fresh population, LAST and in the same order, so
           PgSchemaGeneratorTests' generator-identity pin on V76 holds and the fresh store and the upgraded
           store agree with the positional COPY. */
        var v76 = PgMigrations.Scripts.Single(m => m.Version == 76).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("    interval_length_minutes bigint,\n    query_capture_mode text,\n    wait_stats_capture_mode text\n);", v76, StringComparison.Ordinal);

        /* The rung doc carries the argument in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");
        var start = source.IndexOf("/// V137 —", StringComparison.Ordinal);
        var end = source.IndexOf("private const string V137Sql", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the V137 rung doc is missing or sits after its constant");
        var doc = source[start..end];
        foreach (var phrase in new[]
        {
            "#3796", "#3712", "#3783", "#3802", "#3745", "no new hypertable", "plan-churn factory", "755 k", "92–96 %",
            "<c>*_desc</c> spelling verbatim", "2017+ (v14)", "HasWaitStatsCaptureMode", "never as <c>OFF</c>",
            "TRI-STATE", "store non-NULL wins over file", "Why (b) carries a CHECK", "A route has no", "<c>pgae</c>",
            "V62 <c>plan_xml_compression</c> precedent", "analysis_uncorroborated_route</c> (<c>App.AnalysisUncorroboratedRoute</c>)",
            "40 % utilisation", "pg_relation_size(reltoastrelid)", "written NULL, deliberately, and the reason is a", "measurement.</b>",
            "100 %", "48.9 %", "48.5 %", "pg_freespacemap", "CREATE EXTENSION pg_freespacemap", "utilisation not measured",
            "object_kind = 'checkpointer'", "object_name = 'pg_stat_checkpointer'", "DELTAS since its previous run", "25.2 s and 14.0 s",
            "pg_stat_checkpointer</c> on PostgreSQL 17+", "pg_stat_bgwriter</c> before it", "the WRITER guards the version, not this rung",
            "NEW columns by ruling", "Filled by the inventory from #3783's code half; NULL",
            "Nullable, no DEFAULT, no backfill", "V101 rule", "What this rung deliberately does NOT do", "does not write the checkpointer row",
            "QsCaptureModeRouteKnobToastRungTests", "schema v64",
        })
        {
            Assert.Contains(phrase, doc, StringComparison.Ordinal);
        }

        /* No table, no collector: the censuses did not move; the health payload grew by exactly two. */
        Assert.Equal(72, TimescaleSupport.HypertableCount);
        Assert.Equal(71, CollectorCatalog.All.Count);
        Assert.Equal(12, QueryStoreHealthCollector.Instance.PayloadColumns.Count);
    }

    private static void AssertGeneratedTail(ICollectorSchemaInfo definition, string[] columns, string type, string expectedBefore)
    {
        var generated = PgSchemaGenerator.CreateTable(definition);
        var lines = generated.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, generated);
        for (var i = 0; i < columns.Length; i++)
        {
            Assert.Equal($"{columns[i]} {type}", lines[closing - columns.Length + i]);
        }

        Assert.StartsWith(expectedBefore, lines[closing - columns.Length - 1], StringComparison.Ordinal);
    }

    /* ---- the writers ------------------------------------------------------------------------------------ */

    /// <summary>
    /// (a)'s writer: the shared collector selects both modes on a 2017+ target and only the ungated one on a
    /// 2016 target, on BOTH execution shapes, from the one gate; the two Azure flavours always get both. The
    /// reader side (ten vs eleven ordinals, NULL by construction) is <c>Lite.Tests.QueryStoreHealthCollectorDefinitionTests</c>.
    /// </summary>
    [Fact]
    public void TheCollectorSelectsBothModes_AndGatesOnlyTheWaitStatsOne_On2016()
    {
        Assert.False(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 13 }));
        Assert.True(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 14 }));
        Assert.True(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 0 }));
        Assert.True(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 13, IsAzureSqlDb = true }));
        Assert.True(QueryStoreHealthCollector.HasWaitStatsCaptureMode(new CollectorTargetInfo { SqlMajorVersion = 13, IsAzureManagedInstance = true }));

        var onPrem2016 = QueryStoreHealthCollector.Instance.BuildPerItemQuery("db1", Context(13, isAzure: false)).Text;
        var onPrem2019 = QueryStoreHealthCollector.Instance.BuildPerItemQuery("db1", Context(15, isAzure: false)).Text;
        var azure = QueryStoreHealthCollector.Instance.BuildQuery(Context(13, isAzure: true)).Text;

        foreach (var text in new[] { onPrem2016, onPrem2019, azure })
        {
            Assert.Contains("query_capture_mode = qso.query_capture_mode_desc", text, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("wait_stats_capture_mode", onPrem2016, StringComparison.Ordinal);
        Assert.Contains("wait_stats_capture_mode = qso.wait_stats_capture_mode_desc", onPrem2019, StringComparison.Ordinal);
        Assert.Contains("wait_stats_capture_mode = qso.wait_stats_capture_mode_desc", azure, StringComparison.Ordinal);

        /* The collector says what it now gates, in the words a reader of the class doc will look for. */
        var collector = RepoFile.ReadRepoFile("PerformanceMonitor.Collectors", "QueryStoreHealthCollector.cs");
        Assert.DoesNotContain("so there are no per-column version gates", collector, StringComparison.Ordinal);
        foreach (var phrase in new[] { "ONE of them is gated", "COMPILE for the whole database", "never as OFF", "#3796", "V137", "DatabaseConfigCollector" })
        {
            Assert.Contains(phrase, collector, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// (c)'s writer: the dimension rows — and ONLY the dimension rows — fill the two columns; <c>toast_bytes</c>
    /// is the TOAST relation's main fork through <c>NULLIF(reltoastrelid, 0)</c> (NULL, never an error, for a
    /// table without one), and <c>toast_live_bytes</c> is the typed literal NULL in the INSERT itself — #3783's
    /// code half fills it from a SEPARATE, extension-fenced UPDATE (<c>StoreSelfMetrics.ToastLiveBytesUpdateSql</c>),
    /// so this INSERT still names no extension. Every other kind's INSERT leaves both columns alone, so they
    /// read NULL there by the table's own per-kind convention; (d)'s writer touches only its own three.
    /// </summary>
    [Fact]
    public void TheDimensionSweepWritesToastBytes_AndNullLiveBytes_AndNoOtherKindTouchesEither()
    {
        var sql = StoreSelfMetrics.DimensionInsertSql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("(metric_time, object_name, object_kind, total_bytes, row_count, toast_bytes, toast_live_bytes)", sql, StringComparison.Ordinal);
        foreach (var dim in new[] { PayloadDimensions.QueryTextDimTable, PayloadDimensions.QueryPlanDimTable })
        {
            Assert.Contains(
                $"pg_relation_size(NULLIF((SELECT c.reltoastrelid FROM pg_class AS c WHERE c.oid = 'collect.{dim}'::regclass), 0)),\n    NULL::bigint",
                sql, StringComparison.Ordinal);
        }

        Assert.Equal(2, Regex.Matches(sql, "reltoastrelid").Count);
        Assert.Equal(2, Regex.Matches(sql, "NULL::bigint").Count);
        /* pg_total_relation_size stays: total_bytes is unchanged, the TOAST figure rides beside it. */
        Assert.Equal(2, Regex.Matches(sql, "pg_total_relation_size").Count);
        /* No extension is consulted. */
        Assert.DoesNotContain("pg_freespace", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pgstattuple", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("n_live_tup", sql, StringComparison.Ordinal);

        foreach (var other in new[]
        {
            StoreSelfMetrics.HypertableInsertSql, StoreSelfMetrics.ContinuousAggregateInsertSql, StoreSelfMetrics.BackgroundJobInsertSql,
            StoreSelfMetrics.TableInsertSql, StoreSelfMetrics.UnenumeratedInsertSql, StoreSelfMetrics.UnenumeratedPlainInsertSql,
            StoreSelfMetrics.JobHistoryInsertSql, StoreSelfMetrics.StoreInsertSql,
        })
        {
            foreach (var column in ToastColumns.Concat(CheckpointerColumns))
            {
                Assert.DoesNotContain(column, other, StringComparison.Ordinal);
            }
        }

        /* (d)'s writer landed with #3783's code half and touches ONLY its three columns; the two TOAST columns
           stay the dimension arm's alone. The "no writer yet" arm that stood here was retired deliberately
           when the row landed, as the rung asked; StoreToastAndCheckpointerTests pins the writer's shape. */
        foreach (var checkpointerSql in new[] { StoreSelfMetrics.CheckpointerInsertSql, StoreSelfMetrics.CheckpointerBgwriterInsertSql })
        {
            foreach (var column in ToastColumns)
            {
                Assert.DoesNotContain(column, checkpointerSql, StringComparison.Ordinal);
            }

            foreach (var column in CheckpointerColumns)
            {
                Assert.Contains(column, checkpointerSql, StringComparison.Ordinal);
            }
        }

        /* And the live-bytes UPDATE touches only toast_live_bytes, keyed on the dimension kind. */
        Assert.Contains("SET    toast_live_bytes", StoreSelfMetrics.ToastLiveBytesUpdateSql, StringComparison.Ordinal);
        foreach (var column in CheckpointerColumns)
        {
            Assert.DoesNotContain(column, StoreSelfMetrics.ToastLiveBytesUpdateSql, StringComparison.Ordinal);
        }

        /* The sweep doc carries the measurement that decided NULL, in the words the next reader will look for. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs");
        foreach (var phrase in new[] { "V137, #3783", "40 % utilisation", "NULLIF(reltoastrelid, 0)", "reads 100 % after a vacuum", "1996", "48.9 %", "48.5 %", "pg_freespacemap", "NULL::bigint" })
        {
            Assert.Contains(phrase, source, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The Lite twin exists for (a) only, and the three facts that decide it are pinned rather than asserted in
    /// prose: Lite stores <c>query_store_health</c> (the collector is SQL Server-engine, so
    /// <c>DuckDbSchemaGenerator.StoredCollectors</c> emits it), Lite stores no <c>store_metrics</c> at all, and
    /// Lite's alert settings live in settings.json — under the SAME key this rung's column is spelled with — not
    /// in a table. The v64 block itself is pinned from this side by text (Lite is WPF and cannot be referenced
    /// here); its climb on a real DuckDB is <c>Lite.Tests.QsCaptureModeRungTests</c>.
    /// </summary>
    [Fact]
    public void LiteTwinsOnlyTheCaptureModes_AtSchemaV64()
    {
        Assert.Equal(CollectorTargetEngine.SqlServer, QueryStoreHealthCollector.Instance.TargetEngine);

        var initializer = RepoFile.ReadRepoFile("Lite", "Database", "DuckDbInitializer.cs");
        Assert.Contains("internal const int CurrentSchemaVersion = 64;", initializer, StringComparison.Ordinal);
        var start = initializer.IndexOf("if (fromVersion < 64)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v64 block");
        var block = initializer[start..];
        Assert.Contains("(\"query_store_health\", \"query_capture_mode\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("(\"query_store_health\", \"wait_stats_capture_mode\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("Running migration to v64", block, StringComparison.Ordinal);
        Assert.Contains("twinning Darling's V137", block, StringComparison.Ordinal);

        /* The frozen golden's query_store_health entry ends with the two, VARCHAR (the generator's Varchar
           rendering on DuckDB) — the tail-append that is the golden's one legal edit. Single-line anchors, so
           the raw (CRLF on the CI checkout) text is what is matched; the row-for-row equivalence itself is
           Lite.Tests.DuckDbSchemaEquivalenceTests. */
        var golden = RepoFile.ReadRepoFile("Lite.Tests", "GoldenCollectorSchema.cs");
        Assert.Contains("    query_capture_mode VARCHAR,", golden, StringComparison.Ordinal);
        Assert.Contains("    wait_stats_capture_mode VARCHAR", golden, StringComparison.Ordinal);
        Assert.Contains("the v64 Query Store capture-mode pair (#3796)", golden, StringComparison.Ordinal);

        /* No store_metrics on Lite, anywhere in its source; its alert settings are a file, keyed as this column is. */
        var liteFiles = System.IO.Directory.EnumerateFiles(RepoFile.PathTo("Lite"), "*.cs", System.IO.SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{System.IO.Path.DirectorySeparatorChar}obj{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{System.IO.Path.DirectorySeparatorChar}bin{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToList();
        Assert.DoesNotContain(liteFiles, f => System.IO.File.ReadAllText(f).Contains(MetricsTable, StringComparison.Ordinal));
        Assert.DoesNotContain(liteFiles, f => System.IO.File.ReadAllText(f).Contains(SettingsTable, StringComparison.Ordinal));
        var app = RepoFile.ReadRepoFile("Lite", "App.xaml.cs");
        Assert.Contains($"the settings file as {RouteColumn} ('digest' / 'page')", app, StringComparison.Ordinal);
        Assert.Contains($"read.TryGetProperty(\"{RouteColumn}\", out v)", app, StringComparison.Ordinal);
    }

    /* ---- the probe (three sites, top arm) ------------------------------------------------------------ */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm. The
    /// probe asks the question, the caller reads the answer, the map has the parameter — a sentinel present at
    /// only some of them shifts every LATER ordinal onto the wrong column, and a missing top arm maps a
    /// fully-migrated store one rung short, permanently.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            $"table_name = '{HealthTable}'\n                                                     AND   column_name = 'query_capture_mode'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        /* The "nothing past me" half of this claim moved to V138's test with the top ordinal; what stays is
           that this rung's sentinel is read at its OWN ordinal, which is what keeps every later one on the
           right column. */
        Assert.Contains("hasQsCaptureModeRouteKnobToast", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* A position within the signature, not its end: `ProbeOrdinal == arity - 1` asserted this rung is the
           NEWEST sentinel, which stopped being true the moment V138 appended its own. */
        Assert.True(ProbeOrdinal < arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* In the source, the arm sits ABOVE the previous rung's and returns this build's version. */
        var thisArm = viewer.IndexOf("if (hasQsCaptureModeRouteKnobToast)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPgDatabaseSizeStatsAndHostMemory)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V137 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V137 arm sits below the previous rung's, so a V137 store maps one rung low");
        /* This rung's own literal, not the build's version: the "returns StorageVersion.SchemaVersion" half of
           the top-arm claim moved to V138's test with the top. */
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables are named in the probe line and nowhere in the arm's prose. */
        foreach (var table in new[] { HealthTable, SettingsTable, MetricsTable })
        {
            Assert.DoesNotContain(table, viewer[thisArm..previousArm], StringComparison.Ordinal);
        }
    }

    private static string WithoutComments(string source)
    {
        var noBlocks = Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(noBlocks, @"^\s*//.*$", string.Empty, RegexOptions.Multiline);
    }

    private static CollectorContext Context(int majorVersion, bool isAzure) => new()
    {
        ServerId = -137137,
        ServerName = "v137-rung-pins",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        Target = new CollectorTargetInfo { SqlMajorVersion = majorVersion, IsAzureSqlDb = isAzure },
    };
}

/// <summary>
/// The rung against a real PostgreSQL + TimescaleDB store (<c>DARLING_TEST_PG</c>): the eight columns present
/// after <c>MigrateAsync</c>, nullable, default-less and LAST on each table; the CHECK present, refusing a
/// misspelling and admitting the two routes and NULL; a simulated climb from 136 through this rung applying
/// EXACTLY one rung and then zero — over a pre-rung <c>v_query_store_health</c> so the view refresh is the
/// appending shape the fleet will run; then a health row through the collector's real <c>WritePayload</c> over
/// a real binary COPY (<c>DarlingCollectorRunner</c>'s loop in shape), read back through the REFRESHED view
/// with both modes and a NULL wait-stats mode (the 2016 row); and one run of the dimension sweep's INSERT,
/// writing a non-NULL <c>toast_bytes</c>, a NULL <c>toast_live_bytes</c> and NULL checkpointer columns on both
/// dimension rows. Serialized against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class QsCaptureModeRouteKnobToastLivePostgresTests
{
    private const int ServerId = -137137;
    private const string ServerName = "qs-v137-rung-e2e";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheStoreClimbsToTheRung_TheCollectorWritesBothModes_AndTheSweepWritesToastBytes_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V137 round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        var seededSettingsRow = false;
        /* Naive, as the sweep stamps it (the cross-store contract), and far outside the sweep's 400-day retention
           and any real run, so the cleanup's equality delete finds exactly this run's two rows. */
        var metricTime = DarlingMcpTestData.Naive(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddYears(-50));
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* A store that stopped one rung short: the eight columns gone, the CHECK gone with its column, the
               health view re-frozen at its pre-rung column list, the stamp gone. MigrateAsync must apply EXACTLY
               this rung and put everything back — including re-expanding the view, which is the V14 lesson this
               rung's CREATE OR REPLACE exists for. query_store_health is a hypertable here (the fixture store
               converts them), so this is the ADD COLUMN the fleet will run. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DROP VIEW IF EXISTS collect.v_query_store_health");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.query_store_health DROP COLUMN IF EXISTS query_capture_mode, DROP COLUMN IF EXISTS wait_stats_capture_mode");
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE VIEW collect.v_query_store_health AS SELECT * FROM collect.query_store_health");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP CONSTRAINT IF EXISTS config_alert_settings_analysis_uncorroborated_route_check");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE config.config_alert_settings DROP COLUMN IF EXISTS analysis_uncorroborated_route");
            await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.store_metrics DROP COLUMN IF EXISTS toast_bytes, DROP COLUMN IF EXISTS toast_live_bytes, DROP COLUMN IF EXISTS checkpoint_write_ms, DROP COLUMN IF EXISTS checkpoint_sync_ms, DROP COLUMN IF EXISTS checkpoints_requested");

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 137");
            /* Every rung from this one up, not literally one: V138 (#3691) landed above it, and its two
               nullable columns are re-added by the same climb (IF NOT EXISTS, so the store keeps them). */
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= 137), await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            /* The eight, each LAST on its table, nullable, no default, the declared type. */
            await AssertTrailingColumnsAsync(connection, "collect", "query_store_health", new[] { ("query_capture_mode", "text"), ("wait_stats_capture_mode", "text") }, ct);
            await AssertTrailingColumnsAsync(connection, "config", "config_alert_settings", new[] { ("analysis_uncorroborated_route", "text") }, ct);
            await AssertTrailingColumnsAsync(connection, "collect", "store_metrics", new[] { ("toast_bytes", "bigint"), ("toast_live_bytes", "bigint"), ("checkpoint_write_ms", "bigint"), ("checkpoint_sync_ms", "bigint"), ("checkpoints_requested", "bigint") }, ct);

            /* The refreshed passthrough serves the two new columns (the frozen pre-rung view would not). */
            using (var viewColumns = new NpgsqlCommand(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'v_query_store_health' ORDER BY ordinal_position", connection))
            {
                using var reader = await viewColumns.ExecuteReaderAsync(ct);
                var names = new List<string>();
                while (await reader.ReadAsync(ct))
                {
                    names.Add(reader.GetString(0));
                }

                Assert.Equal(new[] { "query_capture_mode", "wait_stats_capture_mode" }, names.TakeLast(2).ToArray());
            }

            /* The CHECK: present under its name, refuses a misspelling, admits both routes and NULL. The singleton
               row is the SERVICE's to seed at start (every column has a default, so id alone suffices — the
               DarlingMcpAlertToolsTests idiom), so a freshly-migrated store has none and an UPDATE would touch
               nothing and prove nothing; seed it here if absent, remember which, and put the store back exactly
               as found. */
            using (var constraint = new NpgsqlCommand("SELECT count(*) FROM pg_constraint WHERE conname = 'config_alert_settings_analysis_uncorroborated_route_check'", connection))
            {
                Assert.Equal(1L, Convert.ToInt64(await constraint.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
            }

            bool settingsRowPreExisted;
            using (var existing = new NpgsqlCommand("SELECT count(*) FROM config.config_alert_settings WHERE id = 1", connection))
            {
                settingsRowPreExisted = Convert.ToInt64(await existing.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) == 1L;
            }

            seededSettingsRow = !settingsRowPreExisted;
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

            var rejected = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET analysis_uncorroborated_route = 'pgae' WHERE id = 1"));
            Assert.Equal("23514", rejected.SqlState); /* check_violation */
            /* Case matters at the store: the parser is case-insensitive, but the column holds the canonical wire
               spelling the write tool normalizes to, so a stored 'Page' would be a value no reader wrote. */
            var rejectedCase = await Assert.ThrowsAsync<PostgresException>(async () =>
                await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE config.config_alert_settings SET analysis_uncorroborated_route = 'Page' WHERE id = 1"));
            Assert.Equal("23514", rejectedCase.SqlState);

            foreach (var accepted in new[] { "'page'", "'digest'", "NULL" })
            {
                using var write = new NpgsqlCommand($"UPDATE config.config_alert_settings SET analysis_uncorroborated_route = {accepted} WHERE id = 1", connection);
                Assert.Equal(1, await write.ExecuteNonQueryAsync(ct));
            }

            using (var route = new NpgsqlCommand("SELECT analysis_uncorroborated_route FROM config.config_alert_settings WHERE id = 1", connection))
            {
                using var reader = await route.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.IsDBNull(0), "the route column must read NULL after the round trip — NULL is 'the file governs', the pre-rung state");
            }

            /* query_store_health through the REAL writer: a 2019 row with both modes, a 2016 row with the gated
               one NULL by construction, and an OFF database, read back verbatim through the refreshed view. */
            var captureTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            await WriteThroughTheCollectorAsync(connection, captureTime, new List<QueryStoreHealthCollector.Row>
            {
                new() { DbName = "churny", ActualState = "READ_WRITE", DesiredState = "READ_WRITE", ReadonlyReason = 0, CurrentStorageMb = 7_900, MaxStorageMb = 8_192, SizeBasedCleanupMode = "AUTO", StaleQueryThresholdDays = 21, MaxPlansPerQuery = 200, IntervalLengthMinutes = 60, QueryCaptureMode = "ALL", WaitStatsCaptureMode = "ON" },
                new() { DbName = "legacy2016", ActualState = "READ_WRITE", DesiredState = "READ_WRITE", ReadonlyReason = 0, CurrentStorageMb = 100, MaxStorageMb = 1_000, SizeBasedCleanupMode = "AUTO", StaleQueryThresholdDays = 30, MaxPlansPerQuery = 200, IntervalLengthMinutes = 60, QueryCaptureMode = "AUTO", WaitStatsCaptureMode = null },
                new() { DbName = "qsoff", ActualState = "OFF", DesiredState = "OFF", ReadonlyReason = 0, CurrentStorageMb = 0, MaxStorageMb = 100, SizeBasedCleanupMode = "AUTO", StaleQueryThresholdDays = 30, MaxPlansPerQuery = 200, IntervalLengthMinutes = 60, QueryCaptureMode = "NONE", WaitStatsCaptureMode = "OFF" },
            }, ct);

            using (var stored = new NpgsqlCommand(
                "SELECT database_name, actual_state, query_capture_mode, wait_stats_capture_mode FROM collect.v_query_store_health WHERE server_id = $1 ORDER BY database_name", connection))
            {
                stored.Parameters.AddWithValue(ServerId);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(string, string, string?, string?)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
                }

                Assert.Equal(new (string, string, string?, string?)[]
                {
                    ("churny", "READ_WRITE", "ALL", "ON"),
                    ("legacy2016", "READ_WRITE", "AUTO", null),
                    ("qsoff", "OFF", "NONE", "OFF"),
                }, rows);
            }

            /* The dimension sweep's INSERT, verbatim, against the migrated store: both dims exist from V38, both
               have a TOAST relation (a text / bytea column each), so toast_bytes is a real non-negative size and
               toast_live_bytes is NULL on both rows. */
            using (var sweep = new NpgsqlCommand(StoreSelfMetrics.DimensionInsertSql, connection))
            {
                sweep.Parameters.AddWithValue(metricTime);
                Assert.Equal(2, await sweep.ExecuteNonQueryAsync(ct));
            }

            using (var stored = new NpgsqlCommand(
                "SELECT object_name, total_bytes, toast_bytes, toast_live_bytes, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested FROM collect.store_metrics WHERE metric_time = $1 AND object_kind = 'dimension' ORDER BY object_name", connection))
            {
                stored.Parameters.AddWithValue(metricTime);
                using var reader = await stored.ExecuteReaderAsync(ct);
                var rows = new List<(string Name, long Total, long? Toast, long? Live, bool CheckpointerNull)>();
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetInt64(2), reader.IsDBNull(3) ? null : reader.GetInt64(3),
                        reader.IsDBNull(4) && reader.IsDBNull(5) && reader.IsDBNull(6)));
                }

                Assert.Equal(new[] { PayloadDimensions.QueryPlanDimTable, PayloadDimensions.QueryTextDimTable }, rows.Select(r => r.Name).ToArray());
                Assert.All(rows, r =>
                {
                    Assert.NotNull(r.Toast);
                    Assert.True(r.Toast >= 0, $"{r.Name}: toast_bytes must be a size, not {r.Toast}");
                    Assert.True(r.Toast <= r.Total, $"{r.Name}: the TOAST file ({r.Toast}) cannot exceed pg_total_relation_size ({r.Total}), which includes it");
                    Assert.Null(r.Live);
                    /* (d)'s three are a different kind's columns, NULL on a dimension row by the per-kind convention. */
                    Assert.True(r.CheckpointerNull, $"{r.Name}: a dimension row must leave the checkpointer columns NULL");
                });
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                if (seededSettingsRow)
                {
                    /* Only the row THIS test seeded goes; a row the service or a sibling test owns is left as it was. */
                    using var settings = new NpgsqlCommand("DELETE FROM config.config_alert_settings WHERE id = 1", cleanup);
                    await settings.ExecuteNonQueryAsync(cleanupCt);
                }

                using var metrics = new NpgsqlCommand("DELETE FROM collect.store_metrics WHERE metric_time = $1", cleanup);
                metrics.Parameters.AddWithValue(metricTime);
                await metrics.ExecuteNonQueryAsync(cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task AssertTrailingColumnsAsync(NpgsqlConnection connection, string schema, string table, (string Name, string Type)[] expected, System.Threading.CancellationToken ct)
    {
        using var columns = new NpgsqlCommand(
            "SELECT column_name, data_type, is_nullable, column_default IS NULL FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position", connection);
        columns.Parameters.AddWithValue(schema);
        columns.Parameters.AddWithValue(table);
        using var reader = await columns.ExecuteReaderAsync(ct);
        var rows = new List<(string Name, string Type, string Nullable, bool NoDefault)>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetBoolean(3)));
        }

        var tail = rows.TakeLast(expected.Length).ToList();
        Assert.Equal(expected.Select(e => e.Name).ToArray(), tail.Select(r => r.Name).ToArray());
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Type, tail[i].Type);
            Assert.Equal("YES", tail[i].Nullable);
            Assert.True(tail[i].NoDefault, $"{schema}.{table}.{tail[i].Name} has a DEFAULT");
        }
    }

    /// <summary>DarlingCollectorRunner's COPY loop, verbatim in shape: prefix columns through the writer, then
    /// BeginPayload / WritePayload / EndPayload per row, so the collector's positional contract is exercised
    /// against the real, migrated table.</summary>
    private static async Task WriteThroughTheCollectorAsync(NpgsqlConnection connection, DateTime captureTime, IReadOnlyList<QueryStoreHealthCollector.Row> rows, System.Threading.CancellationToken ct)
    {
        var definition = QueryStoreHealthCollector.Instance;
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = captureTime, Deltas = null! };
        var writer = new PgCollectorRowWriter();

        using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(DarlingMcpTestData.Naive(captureTime)).Value(ServerId).Value(ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM query_store_health WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
