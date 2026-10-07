/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the FinOps Index Analysis sub-tab's Darling-specific read/map/derive/project layer (Stage 3) against
/// the store contract, no live Postgres. The Common <see cref="IndexCleanupAnalyzer"/> itself is already
/// covered (53 tests); these cover ONLY the viewer glue Stage 3 adds: (1) the two reads — the load-bearing
/// SQL clauses + column parity against the generated collector DDL; (2) the <c>v_index_object_stats</c> row →
/// <see cref="IndexCleanupIndexInput"/> mapping (incl. NULL flag → false); (3) the
/// <see cref="IndexCleanupOptions"/> derivation from the collected <c>server_properties</c> facts
/// (edition → compression/online, start-time → uptime); and (4) the analyzer-result → grid-row projections
/// (the six-value action label, the "ALL DATABASES"-first rollup).
/// </summary>
public sealed class ViewerIndexAnalysisTests
{
    // ── Reads: SQL clause pins ──

    [Fact]
    public void IndexObjectStatsLatestSql_LatestPerIndex_ViaDistinctOn()
    {
        var sql = ViewerDataService.IndexObjectStatsLatestSql;

        Assert.Contains("FROM v_index_object_stats", sql, StringComparison.Ordinal);
        /* Each database's newest snapshot (#5072): a per-database max(collection_time) anchor, the rows joined on
           (database_id, anchor), lower-bounded by the oldest anchor so the join stays on the newest chunks. */
        Assert.Contains("max(collection_time) AS t", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY database_id", sql, StringComparison.Ordinal);
        Assert.Contains("JOIN anchor AS a ON a.database_id = s.database_id AND a.t = s.collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time >= (SELECT min(t) FROM anchor)", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (s.database_id, s.object_id, s.index_id)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE s.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY s.database_id, s.object_id, s.index_id, s.collection_time DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerCompressionInfoSql_ReadsServerPropertiesBaseTable_LatestRow()
    {
        var sql = ViewerDataService.ServerCompressionInfoSql;

        /* Darling has no v_server_properties view — the base table, bare-name resolved via the Search Path. */
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("engine_edition", sql, StringComparison.Ordinal);
        Assert.Contains("product_version", sql, StringComparison.Ordinal);
        Assert.Contains("sqlserver_start_time", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.IndexObjectStatsLatestSql))]
    [InlineData(nameof(ViewerDataService.ServerCompressionInfoSql))]
    public void IndexAnalysisReads_PgDialect_PositionalParams_NoTSqlNamedParams(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;

        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    // ── Reads: column parity against the generated collector DDL ──

    [Fact]
    public void IndexObjectStatsLatestSql_ReferencesColumnsThatExistInTheGeneratedTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(IndexObjectStatsCollector.Instance);
        Assert.Equal("index_object_stats", IndexObjectStatsCollector.Instance.TargetTable);

        foreach (var col in new[]
        {
            "database_name", "database_id", "schema_name", "object_id", "table_name", "index_id", "index_name",
            "index_type_desc", "key_columns", "included_columns", "filter_definition", "is_unique",
            "is_unique_constraint", "is_primary_key", "is_foreign_key", "is_foreign_key_reference", "is_disabled",
            "is_indexed_view", "data_compression_desc", "optimize_for_sequential_key", "fill_factor", "is_padded",
            "allow_page_locks", "allow_row_locks", "user_seeks", "user_scans", "user_lookups", "user_updates",
            "reserved_mb", "total_rows", "partition_count", "sqlserver_start_time", "collection_time", "server_id",
            "row_lock_wait_count", "row_lock_wait_in_ms", "page_lock_wait_count", "page_lock_wait_in_ms",
            "page_latch_wait_count", "page_latch_wait_in_ms", "page_io_latch_wait_count", "page_io_latch_wait_in_ms",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ServerCompressionInfoRead_ReferencesColumnsThatExistInTheGeneratedTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(ServerPropertiesCollector.Instance);
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.TargetTable);

        foreach (var col in new[] { "engine_edition", "product_version", "sqlserver_start_time", "collection_time", "server_id" })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Row → IndexCleanupIndexInput mapping ──

    private static ViewerDataService.IndexObjectStatsRow FullRow() => new()
    {
        DatabaseName = "AdventureWorks",
        DatabaseId = 7,
        SchemaName = "Sales",
        ObjectId = 12345,
        TableName = "Orders",
        IndexId = 3,
        IndexName = "IX_Orders_CustomerId",
        IndexTypeDesc = "NONCLUSTERED",
        KeyColumns = "[CustomerId], [OrderDate] DESC",
        IncludedColumns = "[Total]",
        FilterDefinition = "([Status]=(1))",
        IsUnique = true,
        IsUniqueConstraint = true,
        IsPrimaryKey = true,
        IsForeignKey = true,
        IsForeignKeyReference = true,
        IsDisabled = false,
        IsIndexedView = true,
        DataCompressionDesc = "NONE",
        OptimizeForSequentialKey = true,
        FillFactor = 90,
        IsPadded = true,
        AllowPageLocks = true,
        AllowRowLocks = true,
        UserSeeks = 10,
        UserScans = 20,
        UserLookups = 30,
        UserUpdates = 40,
        ReservedMb = 512.50m,
        TotalRows = 1_000_000,
        PartitionCount = 4,
        SqlServerStartTime = new DateTime(2026, 7, 1, 8, 0, 0),
        RowLockWaitCount = 100,
        RowLockWaitInMs = 500,
        PageLockWaitCount = 50,
        PageLockWaitInMs = 250,
        PageLatchWaitCount = 8,
        PageLatchWaitInMs = 40,
        PageIoLatchWaitCount = 2,
        PageIoLatchWaitInMs = 10,
    };

    [Fact]
    public void MapToIndexInput_CarriesEveryField()
    {
        var input = ViewerDataService.MapToIndexInput(FullRow());

        Assert.Equal("AdventureWorks", input.DatabaseName);
        Assert.Equal(7, input.DatabaseId);
        Assert.Equal("Sales", input.SchemaName);
        Assert.Equal(12345, input.ObjectId);
        Assert.Equal("Orders", input.TableName);
        Assert.Equal(3, input.IndexId);
        Assert.Equal("IX_Orders_CustomerId", input.IndexName);
        Assert.Equal("NONCLUSTERED", input.IndexTypeDesc);
        Assert.Equal("[CustomerId], [OrderDate] DESC", input.KeyColumns);
        Assert.Equal("[Total]", input.IncludedColumns);
        Assert.Equal("([Status]=(1))", input.FilterDefinition);
        Assert.True(input.IsUnique);
        Assert.True(input.IsUniqueConstraint);
        Assert.True(input.IsPrimaryKey);
        Assert.True(input.IsForeignKey);
        Assert.True(input.IsForeignKeyReference);
        Assert.False(input.IsDisabled);
        Assert.True(input.IsIndexedView);
        Assert.Equal("NONE", input.DataCompressionDesc);
        Assert.True(input.OptimizeForSequentialKey);
        Assert.Equal((short)90, input.FillFactor);
        Assert.True(input.IsPadded);
        Assert.True(input.AllowPageLocks);
        Assert.True(input.AllowRowLocks);
        Assert.Equal(10, input.UserSeeks);
        Assert.Equal(20, input.UserScans);
        Assert.Equal(30, input.UserLookups);
        Assert.Equal(40, input.UserUpdates);
        Assert.Equal(512.50m, input.ReservedMb);
        Assert.Equal(1_000_000, input.TotalRows);
        Assert.Equal(4, input.PartitionCount);
        Assert.Equal(new DateTime(2026, 7, 1, 8, 0, 0), input.SqlServerStartTime);
        Assert.Equal(100, input.RowLockWaitCount);
        Assert.Equal(500, input.RowLockWaitInMs);
        Assert.Equal(50, input.PageLockWaitCount);
        Assert.Equal(250, input.PageLockWaitInMs);
        Assert.Equal(8, input.PageLatchWaitCount);
        Assert.Equal(40, input.PageLatchWaitInMs);
        Assert.Equal(2, input.PageIoLatchWaitCount);
        Assert.Equal(10, input.PageIoLatchWaitInMs);
    }

    [Fact]
    public void MapToIndexInput_NullFlags_CollapseToFalse_AbsentGuardIsNotAGuard()
    {
        // A minimal row: every boolean flag NULL (the Postgres columns are nullable), counters NULL, sizes NULL.
        var input = ViewerDataService.MapToIndexInput(new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "db",
            SchemaName = "dbo",
            TableName = "t",
            IndexId = 2,
        });

        Assert.False(input.IsUnique);
        Assert.False(input.IsUniqueConstraint);
        Assert.False(input.IsPrimaryKey);
        Assert.False(input.IsForeignKey);
        Assert.False(input.IsForeignKeyReference);
        Assert.False(input.IsDisabled);
        Assert.False(input.IsIndexedView);
        Assert.False(input.OptimizeForSequentialKey);
        Assert.False(input.IsPadded);
        Assert.False(input.AllowPageLocks);
        Assert.False(input.AllowRowLocks);
        Assert.Equal(0, input.UserSeeks);
        Assert.Equal(0, input.UserScans);
        Assert.Equal(0, input.UserLookups);
        Assert.Equal(0, input.UserUpdates);
        // Lock/latch counters collapse NULL → 0 (an absent counter is a zero contribution to the rollup sum).
        Assert.Equal(0, input.RowLockWaitCount);
        Assert.Equal(0, input.RowLockWaitInMs);
        Assert.Equal(0, input.PageLockWaitCount);
        Assert.Equal(0, input.PageLockWaitInMs);
        Assert.Equal(0, input.PageLatchWaitCount);
        Assert.Equal(0, input.PageLatchWaitInMs);
        Assert.Equal(0, input.PageIoLatchWaitCount);
        Assert.Equal(0, input.PageIoLatchWaitInMs);
        // The genuinely-nullable sizing fields stay null (distinct from "collected as 0").
        Assert.Null(input.ReservedMb);
        Assert.Null(input.TotalRows);
        Assert.Null(input.PartitionCount);
        Assert.Null(input.FillFactor);
    }

    // ── Options derivation: compression / online by edition ──

    private static readonly DateTime Ref = new(2026, 7, 5, 12, 0, 0);

    [Theory]
    [InlineData(3, "16.0.1000.6", true, true)]   // Enterprise (also Developer/Evaluation) — both
    [InlineData(5, "12.0.2000.8", true, true)]   // Azure SQL Database — both (version irrelevant)
    [InlineData(8, "12.0.2000.8", true, true)]   // Azure SQL Managed Instance — both
    [InlineData(2, "16.0.1000.6", true, false)]  // Standard 2016+ — compression yes, online no
    [InlineData(2, "13.0.5026.0", true, false)]  // Standard 2016 RTM (major 13) — compression yes
    [InlineData(4, "15.0.4300.1", true, false)]  // Express 2019 — compression yes, online no
    [InlineData(2, "12.0.6024.0", false, false)] // Standard 2014 (pre-2016) — no compression, no online
    [InlineData(4, "11.0.7001.0", false, false)] // Express 2012 — neither
    public void DeriveOptions_CompressionAndOnline_ByEditionAndVersion(
        int engineEdition, string productVersion, bool expectCompression, bool expectOnline)
    {
        var options = ViewerDataService.DeriveIndexCleanupOptions(engineEdition, productVersion, null, Ref);

        Assert.Equal(expectCompression, options.ServerSupportsCompression);
        Assert.Equal(expectOnline, options.ServerSupportsOnline);
    }

    [Fact]
    public void DeriveOptions_UnknownEdition_NoCollectedRow_SafeDefaults()
    {
        // No server_properties row collected yet: assume compression available (2016+ product minimum), online off.
        var options = ViewerDataService.DeriveIndexCleanupOptions(null, null, null, Ref);

        Assert.True(options.ServerSupportsCompression);
        Assert.False(options.ServerSupportsOnline);
    }

    // ── Options derivation: uptime days from start time ──

    [Fact]
    public void DeriveOptions_UptimeDays_FromServerLocalStartVsReferenceNow()
    {
        var start = Ref.AddDays(-30);
        var options = ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", start, Ref);

        Assert.NotNull(options.ServerUptimeDays);
        Assert.Equal(30.0, options.ServerUptimeDays!.Value, precision: 3);
    }

    [Fact]
    public void DeriveOptions_UptimeUnderSevenDays_DrivesAnalyzerDedupeAndWarning()
    {
        // Uptime derivation feeds the analyzer's <=7-day auto-dedupe and <14-day caveat: prove the wiring end to end.
        var options = ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", Ref.AddDays(-3), Ref);
        Assert.Equal(3.0, options.ServerUptimeDays!.Value, precision: 3);

        var result = IndexCleanupAnalyzer.Analyze(Array.Empty<IndexCleanupIndexInput>(), options);
        Assert.True(result.DedupeOnlyApplied);
        Assert.True(result.UptimeWarning);
    }

    [Fact]
    public void DeriveOptions_NullStart_NoUptime_AndFutureStartIgnored()
    {
        Assert.Null(ViewerDataService.DeriveIndexCleanupOptions(3, "16.0", null, Ref).ServerUptimeDays);
        // A start clock ahead of the viewer's (timezone skew) yields a negative age — discarded, not applied.
        Assert.Null(ViewerDataService.DeriveIndexCleanupOptions(3, "16.0", Ref.AddDays(2), Ref).ServerUptimeDays);
    }

    [Fact]
    public void DeriveOptions_MinimumsAndFactors_MatchSpIndexCleanupParameterDefaults()
    {
        var options = ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", Ref.AddDays(-30), Ref);

        // sp_IndexCleanup @min_reads / @min_writes / @min_size_gb / @min_rows / @dedupe_only defaults (0 / off).
        Assert.False(options.DedupeOnly);
        Assert.Equal(0m, options.MinSizeGb);
        Assert.Equal(0, options.MinReads);
        Assert.Equal(0, options.MinWrites);
        Assert.Equal(0, options.MinRows);
        // The 0.20–0.60 general compression savings band (record defaults, unchanged).
        Assert.Equal(0.20m, options.CompressionMinSavingsFactor);
        Assert.Equal(0.60m, options.CompressionMaxSavingsFactor);
    }

    // ── ParseMajorVersion ──

    [Theory]
    [InlineData("16.0.1000.6", 16)]
    [InlineData("13.0.5026.0", 13)]
    [InlineData("9.00.5000.00", 9)]
    [InlineData("11.0.7001.0", 11)]
    [InlineData(null, 0)]
    [InlineData("", 0)]
    [InlineData("   ", 0)]
    [InlineData("garbage", 0)]
    public void ParseMajorVersion_LeadingInteger(string? productVersion, int expected) =>
        Assert.Equal(expected, ViewerDataService.ParseMajorVersion(productVersion));

    // ── Recommendation-row action label (the six-value filter set) ──

    [Theory]
    [InlineData(IndexCleanupAction.MakeUnique, IndexCleanupResultKind.Merge, "MAKE UNIQUE")]
    [InlineData(IndexCleanupAction.MakeUnique, IndexCleanupResultKind.Disable, "MAKE UNIQUE")] // Action wins over its bucket
    [InlineData(IndexCleanupAction.Disable, IndexCleanupResultKind.Disable, "DISABLE")]
    [InlineData(IndexCleanupAction.MergeIncludes, IndexCleanupResultKind.Merge, "MERGE")]
    [InlineData(IndexCleanupAction.Keep, IndexCleanupResultKind.Compress, "COMPRESS")]
    [InlineData(IndexCleanupAction.Keep, IndexCleanupResultKind.Review, "REVIEW")]
    [InlineData(IndexCleanupAction.Disable, IndexCleanupResultKind.DisableConstraint, "DROP CONSTRAINT")]
    public void ActionLabelFor_MapsToTheSixOperationLabels(
        IndexCleanupAction action, IndexCleanupResultKind resultKind, string expected) =>
        Assert.Equal(expected, IndexCleanupRecommendationRow.ActionLabelFor(action, resultKind));

    [Fact]
    public void RecommendationRow_ReviewRow_CarriesNoDestructiveScript()
    {
        var row = new IndexCleanupRecommendationRow(new IndexCleanupRecommendation
        {
            DatabaseName = "db",
            SchemaName = "dbo",
            TableName = "t",
            IndexName = "IX_review",
            Action = IndexCleanupAction.Keep,
            ResultKind = IndexCleanupResultKind.Review,
            ConsolidationRule = IndexCleanupRules.ReverseDuplicate,
            SupersededBy = "Related to IX_other",
            Script = "",
        });

        Assert.Equal("REVIEW", row.ActionDisplay);
        Assert.Equal("REVIEW", row.ResultKindDisplay);
        Assert.Equal(IndexCleanupRules.ReverseDuplicate, row.ConsolidationRule);
        Assert.Equal("", row.Script);
        Assert.Equal("Related to IX_other", row.SupersededBy);
    }

    // ── Rollup projection ──

    [Fact]
    public void ProjectRollups_OverallFirst_ThenPerDatabase()
    {
        var result = new IndexCleanupAnalysisResult
        {
            OverallRollup = new IndexCleanupRollup { DatabaseName = null, IndexCount = 42, TotalMaxSavingsGb = 9.5m },
            DatabaseRollups = new[]
            {
                new IndexCleanupRollup { DatabaseName = "Alpha", IndexCount = 20 },
                new IndexCleanupRollup { DatabaseName = "Beta", IndexCount = 22 },
            },
        };

        var rows = ViewerDataService.ProjectRollups(result);

        Assert.Equal(3, rows.Count);
        Assert.Equal("ALL DATABASES", rows[0].Scope);
        Assert.Equal(42, rows[0].IndexCount);
        Assert.Equal(9.5m, rows[0].TotalMaxSavingsGb);
        Assert.Equal("Alpha", rows[1].Scope);
        Assert.Equal("Beta", rows[2].Scope);
    }

    // ── End-to-end glue: map → analyze → project (NOT re-testing analyzer rules; just the Stage-3 wiring) ──

    [Fact]
    public void EndToEnd_MapAnalyzeProject_ExactDuplicate_YieldsADisableRecommendation()
    {
        // Two identical nonclustered indexes on one table = an Exact Duplicate; the analyzer disables the loser.
        ViewerDataService.IndexObjectStatsRow Idx(int indexId, string name) => new()
        {
            DatabaseName = "AdventureWorks",
            DatabaseId = 7,
            SchemaName = "Sales",
            ObjectId = 100,
            TableName = "Orders",
            IndexId = indexId,
            IndexName = name,
            IndexTypeDesc = "NONCLUSTERED",
            KeyColumns = "[CustomerId]",
            IncludedColumns = "[Total]",
            IsUnique = false,
            ReservedMb = 100m,
            TotalRows = 500_000,
            UserSeeks = 5,
        };

        var clustered = new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "AdventureWorks",
            DatabaseId = 7,
            SchemaName = "Sales",
            ObjectId = 100,
            TableName = "Orders",
            IndexId = 1,
            IndexName = "PK_Orders",
            IndexTypeDesc = "CLUSTERED",
            KeyColumns = "[OrderId]",
            IsUnique = true,
            IsPrimaryKey = true,
            ReservedMb = 400m,
            TotalRows = 500_000,
        };

        var inputs = new[] { clustered, Idx(2, "IX_A"), Idx(3, "IX_B") }
            .Select(ViewerDataService.MapToIndexInput)
            .ToList();

        var options = ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", Ref.AddDays(-60), Ref);
        var result = IndexCleanupAnalyzer.Analyze(inputs, options);

        var recRows = ViewerDataService.ProjectRecommendations(result);
        var rollupRows = ViewerDataService.ProjectRollups(result);

        // The Stage-3 glue produced rows the grids can bind: at least one DISABLE recommendation for the duplicate.
        Assert.Contains(recRows, r => r.ActionDisplay == "DISABLE" && r.ConsolidationRule == IndexCleanupRules.ExactDuplicate);
        // The rollup carries the overall row and reflects the disable.
        Assert.Equal("ALL DATABASES", rollupRows[0].Scope);
        Assert.True(rollupRows[0].IndexesToDisable >= 1);
    }

    // ── Workload-impact rollup columns (sp_IndexCleanup #index_reporting_stats parity) ──

    [Fact]
    public void IndexObjectStatsLatestSql_SelectsTheOperationalLockLatchCounters()
    {
        // The reclaimable-space rollup's workload columns are backed by the operational counters the collector
        // already captures — the read must pull them (the same columns the FinOps Locking sub-tab reads).
        var sql = ViewerDataService.IndexObjectStatsLatestSql;

        foreach (var col in new[]
        {
            "row_lock_wait_count", "row_lock_wait_in_ms", "page_lock_wait_count", "page_lock_wait_in_ms",
            "page_latch_wait_count", "page_latch_wait_in_ms", "page_io_latch_wait_count", "page_io_latch_wait_in_ms",
        })
        {
            Assert.Contains(col, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void WorkloadRollup_DatabaseRowAggregatesCountersAcrossIndexes_OverallRowIsNA()
    {
        // One database, three analyzed indexes on one table. The DATABASE rollup SUMs the workload counters over
        // every analyzed index (reads/writes + lock/latch); the overall ("ALL DATABASES") row mirrors
        // sp_IndexCleanup's SUMMARY level, which emits these columns as 'N/A'.
        var clustered = new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "AdventureWorks", DatabaseId = 7, SchemaName = "Sales", ObjectId = 100,
            TableName = "Orders", IndexId = 1, IndexName = "PK_Orders", IndexTypeDesc = "CLUSTERED",
            KeyColumns = "[OrderId]", IsUnique = true, IsPrimaryKey = true, ReservedMb = 400m, TotalRows = 500_000,
            // Clustered index carries no workload here so the expected sums stay clean (it is still counted).
        };
        var idxA = new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "AdventureWorks", DatabaseId = 7, SchemaName = "Sales", ObjectId = 100,
            TableName = "Orders", IndexId = 2, IndexName = "IX_A", IndexTypeDesc = "NONCLUSTERED",
            KeyColumns = "[CustomerId]", ReservedMb = 100m, TotalRows = 500_000,
            UserSeeks = 10, UserScans = 20, UserLookups = 30, UserUpdates = 40,
            RowLockWaitCount = 100, RowLockWaitInMs = 500, PageLockWaitCount = 50, PageLockWaitInMs = 250,
            PageLatchWaitCount = 8, PageLatchWaitInMs = 40, PageIoLatchWaitCount = 2, PageIoLatchWaitInMs = 10,
        };
        var idxB = new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "AdventureWorks", DatabaseId = 7, SchemaName = "Sales", ObjectId = 100,
            TableName = "Orders", IndexId = 3, IndexName = "IX_B", IndexTypeDesc = "NONCLUSTERED",
            KeyColumns = "[OrderDate]", ReservedMb = 100m, TotalRows = 500_000,
            UserSeeks = 5, UserScans = 5, UserLookups = 5, UserUpdates = 10,
            // No lock/latch waits — proves the SUM includes zero-wait indexes without skewing the average.
        };

        var inputs = new[] { clustered, idxA, idxB }.Select(ViewerDataService.MapToIndexInput).ToList();
        var options = ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", Ref.AddDays(-60), Ref);
        var result = IndexCleanupAnalyzer.Analyze(inputs, options);
        var rows = ViewerDataService.ProjectRollups(result);

        var overall = rows[0];
        var db = rows.Single(r => r.Scope == "AdventureWorks");

        // Database row: SUMs — reads 15/25/35 (=75), writes 50, lock waits 150 (avg 750/150 = 5), latch waits 10 (avg 50/10 = 5).
        Assert.Equal(50m, db.WritesSort);
        Assert.Equal(150m, db.LockWaitCountSort);
        Assert.Equal(5m, db.AvgLockWaitMsSort);
        Assert.Equal(10m, db.LatchWaitCountSort);
        Assert.Equal(5m, db.AvgLatchWaitMsSort);
        Assert.NotEqual("N/A", db.Writes);
        Assert.Contains("seeks", db.ReadsBreakdown, StringComparison.Ordinal);
        Assert.Contains("scans", db.ReadsBreakdown, StringComparison.Ordinal);
        Assert.Contains("lookups", db.ReadsBreakdown, StringComparison.Ordinal);

        // Overall ("ALL DATABASES") row: workload columns are 'N/A' (matches sp_IndexCleanup's SUMMARY level),
        // and their numeric sort keys sink below every real value.
        Assert.Equal("ALL DATABASES", overall.Scope);
        Assert.Equal("N/A", overall.ReadsBreakdown);
        Assert.Equal("N/A", overall.Writes);
        Assert.Equal("N/A", overall.LockWaitCount);
        Assert.Equal("N/A", overall.AvgLockWaitMs);
        Assert.Equal("N/A", overall.LatchWaitCount);
        Assert.Equal("N/A", overall.AvgLatchWaitMs);
        Assert.Equal(-1m, overall.WritesSort);
        Assert.Equal(-1m, overall.LockWaitCountSort);
        Assert.Equal(-1m, overall.AvgLatchWaitMsSort);
    }

    [Fact]
    public void WorkloadRollup_DatabaseRowWithNoWaits_ShowsZeroNotNA()
    {
        // A database whose indexes have zero lock/latch waits shows "0" (not 'N/A') on the database row —
        // 'N/A' is reserved for the overall row, exactly like sp_IndexCleanup's DATABASE-vs-SUMMARY split.
        var idx = new ViewerDataService.IndexObjectStatsRow
        {
            DatabaseName = "Db", DatabaseId = 9, SchemaName = "dbo", ObjectId = 1, TableName = "t",
            IndexId = 2, IndexName = "IX_quiet", IndexTypeDesc = "NONCLUSTERED", KeyColumns = "[c]",
            ReservedMb = 1m, TotalRows = 10, UserSeeks = 1,
        };

        var result = IndexCleanupAnalyzer.Analyze(
            new[] { idx }.Select(ViewerDataService.MapToIndexInput),
            ViewerDataService.DeriveIndexCleanupOptions(3, "16.0.1000.6", Ref.AddDays(-60), Ref));
        var db = ViewerDataService.ProjectRollups(result).Single(r => r.Scope == "Db");

        Assert.Equal("0", db.AvgLockWaitMs);   // no waits → "0", not a divide-by-zero or 'N/A'
        Assert.Equal("0", db.AvgLatchWaitMs);
        Assert.Equal(0m, db.LockWaitCountSort);
    }
}
