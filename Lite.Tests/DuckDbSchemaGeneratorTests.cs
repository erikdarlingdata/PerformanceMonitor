/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins Lite's DuckDB collector schema against the shared collector catalog — the DuckDB twin of
/// Darling's <c>PgSchemaGeneratorTests</c>. The type map uses DuckDB spellings, the prefix / index
/// shapes reproduce Lite's historical schema exactly, and the FORCING-FUNCTION test asserts every
/// generated table's columns + types equal the catalog definition, so a catalog change necessarily
/// flows into Lite and no hand-edit can silently diverge. (Storage-equivalence to the former
/// hand-written tables is proven separately by <see cref="DuckDbSchemaEquivalenceTests"/>.)
/// </summary>
public sealed class DuckDbSchemaGeneratorTests
{
    [Fact]
    public void TypeFor_MapsEveryColumnTypeToItsDuckDbSpelling()
    {
        Assert.Equal("BIGINT", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.BigInt)));
        Assert.Equal("INTEGER", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Integer)));
        Assert.Equal("SMALLINT", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.SmallInt)));
        Assert.Equal("VARCHAR", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Varchar)));
        Assert.Equal("TIMESTAMP", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Timestamp)));
        Assert.Equal("DOUBLE", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Double)));
        Assert.Equal("DECIMAL(18,2)", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 18, 2)));
        Assert.Equal("DECIMAL(5,2)", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 5, 2)));
        Assert.Equal("DECIMAL(38,2)", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 38, 2)));
        Assert.Equal("DECIMAL(10,1)", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 10, 1)));
        Assert.Equal("BOOLEAN", DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Boolean)));
        Assert.Throws<InvalidOperationException>(
            () => DuckDbSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal)));
    }

    [Fact]
    public void CreateTable_WaitStats_FullDdlPinned()
    {
        var ddl = DuckDbSchemaGenerator.CreateTable(WaitStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS wait_stats (\n" +
            "    collection_id BIGINT PRIMARY KEY,\n" +
            "    collection_time TIMESTAMP NOT NULL,\n" +
            "    server_id INTEGER NOT NULL,\n" +
            "    server_name VARCHAR NOT NULL,\n" +
            "    wait_type VARCHAR NOT NULL,\n" +
            "    waiting_tasks_count BIGINT,\n" +
            "    wait_time_ms BIGINT,\n" +
            "    signal_wait_time_ms BIGINT,\n" +
            "    delta_waiting_tasks BIGINT,\n" +
            "    delta_wait_time_ms BIGINT,\n" +
            "    delta_signal_wait_time_ms BIGINT\n" +
            ")",
            ddl);
    }

    [Fact]
    public void CreateTable_RunningJobs_NoIdPrefix_WithNotNullOverlayAndDecimal()
    {
        /* No prefix id (running_jobs is keyed by (collection_time, server) alone), the NOT NULL overlay
           on the always-populated columns, and percent_of_average DECIMAL(10,1). */
        var ddl = DuckDbSchemaGenerator.CreateTable(RunningJobsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS running_jobs (\n" +
            "    collection_time TIMESTAMP NOT NULL,\n" +
            "    server_id INTEGER NOT NULL,\n" +
            "    server_name VARCHAR NOT NULL,\n" +
            "    job_name VARCHAR NOT NULL,\n" +
            "    job_id VARCHAR NOT NULL,\n" +
            "    job_enabled BOOLEAN NOT NULL,\n" +
            "    start_time TIMESTAMP NOT NULL,\n" +
            "    current_duration_seconds BIGINT NOT NULL,\n" +
            "    avg_duration_seconds BIGINT NOT NULL,\n" +
            "    p95_duration_seconds BIGINT NOT NULL,\n" +
            "    successful_run_count BIGINT NOT NULL,\n" +
            "    is_running_long BOOLEAN NOT NULL,\n" +
            "    percent_of_average DECIMAL(10,1)\n" +
            ")",
            ddl);
    }

    [Fact]
    public void CreateTable_ServerConfig_UsesConfigIdAndCaptureTimePrefix()
    {
        var ddl = DuckDbSchemaGenerator.CreateTable(ServerConfigCollector.Instance);

        Assert.StartsWith(
            "CREATE TABLE IF NOT EXISTS server_config (\n" +
            "    config_id BIGINT PRIMARY KEY,\n" +
            "    capture_time TIMESTAMP NOT NULL,\n" +
            "    server_id INTEGER NOT NULL,\n" +
            "    server_name VARCHAR NOT NULL,\n" +
            "    configuration_name VARCHAR NOT NULL,",
            ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_SpinlockStats_MapsDoubleAndKeepsNameNotNull()
    {
        var ddl = DuckDbSchemaGenerator.CreateTable(SpinlockStatsCollector.Instance);

        Assert.Contains("spinlock_name VARCHAR NOT NULL", ddl, StringComparison.Ordinal);
        Assert.Contains("spins_per_collision DOUBLE", ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_QuerySnapshots_PreservesIsCdcCaptureDefault()
    {
        var ddl = DuckDbSchemaGenerator.CreateTable(QuerySnapshotsCollector.Instance);

        Assert.Contains("is_cdc_capture BOOLEAN DEFAULT false", ddl, StringComparison.Ordinal);
        Assert.Contains("granted_query_memory_gb DECIMAL(18,2)", ddl, StringComparison.Ordinal);
        Assert.Contains("percent_complete DECIMAL(5,2)", ddl, StringComparison.Ordinal);
        Assert.Contains("requested_memory_mb DOUBLE", ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateIndex_MirrorsLitesIrregularNamesAndColumns()
    {
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_wait_stats_time ON wait_stats(server_id, collection_time)",
            DuckDbSchemaGenerator.CreateIndex(WaitStatsCollector.Instance));

        /* Irregular short-form names predating the uniform scheme. */
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_cpu_time ON cpu_utilization_stats(server_id, collection_time)",
            DuckDbSchemaGenerator.CreateIndex(CpuUtilizationCollector.Instance));
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_query_store_time ON query_store_stats(server_id, collection_time)",
            DuckDbSchemaGenerator.CreateIndex(QueryStoreCollector.Instance));

        /* Irregular COLUMN (sample_time, not the collection_time prefix). */
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_memory_pressure_events_time ON memory_pressure_events(server_id, sample_time)",
            DuckDbSchemaGenerator.CreateIndex(MemoryPressureEventsCollector.Instance));

        /* Composite object-drill index. */
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_index_object_stats_object ON index_object_stats(server_id, database_name, object_id, index_id, collection_time)",
            DuckDbSchemaGenerator.CreateIndex(IndexObjectStatsCollector.Instance));

        /* Config snapshots index on capture_time (handled by the default formula via PrefixTimeColumnName). */
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_trace_flags_time ON trace_flags(server_id, capture_time)",
            DuckDbSchemaGenerator.CreateIndex(TraceFlagsCollector.Instance));

        /* server_config / database_config have no index. */
        Assert.Null(DuckDbSchemaGenerator.CreateIndex(ServerConfigCollector.Instance));
        Assert.Null(DuckDbSchemaGenerator.CreateIndex(DatabaseConfigCollector.Instance));
    }

    [Fact]
    public void Generated_EmitsEveryCatalogTable_AndThirtyNineIndexes()
    {
        /* Counting the filtered sequence against itself could not fail. What matters is WHICH tables are
           emitted, so the names are compared as sets — and that no PostgreSQL table leaks into Lite's DuckDB,
           which is the actual invariant this file now guards. */
        var emitted = DuckDbSchemaGenerator.CreateTableStatements()
            .Select(s => Regex.Match(s, @"CREATE TABLE IF NOT EXISTS (\w+)").Groups[1].Value)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        var expected = DuckDbSchemaGenerator.StoredCollectors
            .Select(c => c.TargetTable)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(expected, emitted);
        Assert.DoesNotContain(emitted, n => n.StartsWith("pg_", StringComparison.Ordinal));
        Assert.All(
            CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql),
            c => Assert.DoesNotContain(c.TargetTable, emitted));

        /* The stored collectors minus the two index-less config tables (database_states is a
           time-series collector and gets the default retrieval index). */
        Assert.Equal(DuckDbSchemaGenerator.StoredCollectors.Count() - 2, DuckDbSchemaGenerator.CreateIndexStatements().Count());
    }

    /// <summary>
    /// THE forcing function: for every collector in the catalog, the generated DuckDB table's columns
    /// (in order) and their DuckDB types must equal the standard prefix followed by the definition's
    /// <see cref="ICollectorSchemaInfo.PayloadColumns"/>. A collector column added/renamed/retyped in
    /// the shared catalog therefore MUST appear in Lite's schema, and a generator change that dropped
    /// or reordered a catalog column fails here.
    /// </summary>
    [Fact]
    public void GeneratedColumnsAndTypes_MatchTheCatalog_ForEveryCollector()
    {
        var failures = new List<string>();

        foreach (var schema in DuckDbSchemaGenerator.StoredCollectors)
        {
            var expected = new List<(string Name, string Type)>();
            if (schema.IncludesCollectionId)
            {
                expected.Add((schema.PrefixIdColumnName, "BIGINT"));
            }
            expected.Add((schema.PrefixTimeColumnName, "TIMESTAMP"));
            expected.Add(("server_id", "INTEGER"));
            expected.Add(("server_name", "VARCHAR"));
            foreach (var column in schema.PayloadColumns)
            {
                expected.Add((column.Name, DuckDbSchemaGenerator.TypeFor(column)));
            }

            var actual = ParseColumns(DuckDbSchemaGenerator.CreateTable(schema));

            if (!expected.SequenceEqual(actual))
            {
                failures.Add($"{schema.TargetTable}:\n    expected: {Render(expected)}\n    actual  : {Render(actual)}");
            }
        }

        Assert.True(
            failures.Count == 0,
            "Generated Lite DDL no longer matches the collector catalog (columns/types/order):\n\n" +
            string.Join("\n\n", failures));
    }

    /// <summary>
    /// Pins the "three registrations" collapse: the archive-view list (DuckDbInitializer) and the
    /// archival/purge list (ArchiveService) are now BOTH derived from the catalog, so they cover
    /// exactly the 36 collector tables plus the two non-collector time-series tables, and can never
    /// again fall out of sync by hand.
    /// </summary>
    [Fact]
    public void ArchivableTables_AreCatalogDriven_AndMirrorEachOther()
    {
        var expected = DuckDbSchemaGenerator.StoredCollectors.Select(c => c.TargetTable)
            .Concat(new[] { "config_alert_log", "collection_log" })
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToArray();

        var initializerList = DuckDbInitializer.ArchivableTables.OrderBy(t => t, StringComparer.Ordinal).ToArray();
        var archiveServiceList = ArchiveService.ArchivableTables.Select(t => t.Table).OrderBy(t => t, StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, initializerList);
        Assert.Equal(expected, archiveServiceList);

        /* The time column for every archivable collector table is its catalog prefix-time column;
           the two non-collector tables carry their own. */
        var timeByTable = ArchiveService.ArchivableTables.ToDictionary(t => t.Table, t => t.TimeColumn);
        foreach (var schema in DuckDbSchemaGenerator.StoredCollectors)
        {
            Assert.Equal(schema.PrefixTimeColumnName, timeByTable[schema.TargetTable]);
        }
        Assert.Equal("alert_time", timeByTable["config_alert_log"]);
        Assert.Equal("collection_time", timeByTable["collection_log"]);
    }

    /// <summary>Splits a generated CREATE TABLE into ordered (columnName, typeToken) pairs, stripping constraints.</summary>
    private static List<(string Name, string Type)> ParseColumns(string ddl)
    {
        var columns = new List<(string, string)>();
        foreach (var rawLine in ddl.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'))
        {
            var line = rawLine.Trim().TrimEnd(',').Trim();
            if (line.Length == 0) continue;
            if (line.StartsWith("CREATE TABLE", StringComparison.Ordinal)) continue;
            if (line.StartsWith(")", StringComparison.Ordinal)) continue;

            var firstSpace = line.IndexOf(' ');
            var name = line.Substring(0, firstSpace);
            /* The type token is the first whitespace-delimited token after the name; DECIMAL(p,s)
               contains no space, and constraints (PRIMARY KEY / NOT NULL / DEFAULT …) follow a space. */
            var typeToken = line.Substring(firstSpace + 1).Trim().Split(' ')[0];
            columns.Add((name, typeToken));
        }
        return columns;
    }

    private static string Render(IEnumerable<(string Name, string Type)> cols) =>
        string.Join(", ", cols.Select(c => $"{c.Name}:{c.Type}"));
}
