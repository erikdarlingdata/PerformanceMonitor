/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Size pin for the default <c>index_analysis</c> response, built from hand-made rows and no store: ten
/// recommendations whose script and definition both hit the 300-character cap, more databases than the cap, with long names (the cap keeps 13 serialized), both
/// caveat notes and the analyzer's full note list. The body must fit the default MCP response budget with headroom. A limit of 500 is
/// measured on three row shapes against the sizes the guide gives (#5238).
/// </summary>
public sealed class FinOpsIndexAnalysisViewSizeTests
{
    private const int HeadroomBytes = 30 * 1024;

    private static string Long(string stem, int length) => (stem + new string('x', length))[..length];

    private static IndexCleanupAnalysisResult WorstDefaultResult(int databases, int recommendations, int textLength = 400)
    {
        var rollups = Enumerable.Range(0, databases).Select(i => new IndexCleanupRollup
        {
            DatabaseName = Long($"tenant_database_{i:D2}_", 60),
            TablesAnalyzed = 123456, IndexCount = 1234567, TotalSizeGb = 1234567.8915m, TotalRows = 123456789012,
            IndexesToDisable = 123456, IndexesToMerge = 123456, CompressableIndexes = 123456, UnusedIndexes = 123456,
            UnusedSizeGb = 1234567.8915m, CompressionMinSavingsGb = 1234567.8915m, CompressionMaxSavingsGb = 1234567.8915m,
            TotalMinSavingsGb = 1234567.8915m, TotalMaxSavingsGb = 1234567.8915m,
            UserSeeks = 123456789012, UserScans = 123456789012, UserLookups = 123456789012, TotalWrites = 123456789012,
            LockWaitCount = 123456789, LockWaitInMs = 12345678901, LatchWaitCount = 123456789, LatchWaitInMs = 12345678901,
        }).ToList();
        var recs = Enumerable.Range(0, recommendations).Select(i => new IndexCleanupRecommendation
        {
            DatabaseName = Long($"tenant_database_{i:D2}_", 60), SchemaName = Long("schema_", 40), TableName = Long("table_", 60),
            IndexName = Long($"IX_index_{i:D2}_", 60), IndexId = 12,
            Action = IndexCleanupAction.Disable, ResultKind = IndexCleanupResultKind.Merge,
            ConsolidationRule = "Key Duplicate", TargetIndexName = Long("IX_target_", 60), SupersededBy = Long("IX_super_", 60),
            MissingIncludedColumns = Long("[col_a], [col_b], ", 120), AdditionalInfo = Long("Exact duplicate of ", 120),
            Script = Long("CREATE INDEX ", textLength), OriginalIndexDefinition = Long("CREATE NONCLUSTERED INDEX ", textLength),
            IndexSizeGb = 1234.5675m, IndexRows = 123456789012, IndexReads = 123456789012, IndexWrites = 123456789012,
            CanCompress = true, IsForeignKey = true, ScriptOmitsPartitionPlacement = true,
        }).ToList();
        return new IndexCleanupAnalysisResult
        {
            Recommendations = recs, DatabaseRollups = rollups, OverallRollup = rollups[0],
            UptimeWarning = true, DedupeOnlyApplied = true,
            Notes = [Long("Reconstructed CREATE/MERGE scripts omit ", 330), Long("Compression eligibility cannot ", 380),
                Long("Compression savings use ", 210), Long("COMPRESSION REBUILD uses ", 150), Long("Reverse Duplicate and ", 350),
                Long("Unique Constraint Replacement ", 700), Long("Out of scope of the ", 330)],
        };
    }

    private static int Bytes(string json) => Encoding.UTF8.GetByteCount(json);

    /* Recommendations of one row shape: every name nameLength characters, script and definition textLength, the free-text columns freeTextLength. */
    private static IndexCleanupAnalysisResult ShapedResult(int recommendations, int nameLength, int textLength, int freeTextLength)
    {
        var rollup = new IndexCleanupRollup { DatabaseName = Long("tenant_database_", nameLength), TotalMaxSavingsGb = 12.345m };
        var recs = Enumerable.Range(0, recommendations).Select(i => new IndexCleanupRecommendation
        {
            DatabaseName = Long($"tenant_database_{i:D3}_", nameLength), SchemaName = Long("schema_", nameLength), TableName = Long("table_", nameLength),
            IndexName = Long($"IX_index_{i:D3}_", nameLength), IndexId = 12,
            Action = IndexCleanupAction.Disable, ResultKind = IndexCleanupResultKind.Merge,
            ConsolidationRule = "Key Duplicate", TargetIndexName = Long("IX_target_", nameLength), SupersededBy = Long("IX_super_", nameLength),
            MissingIncludedColumns = Long("[col_a], [col_b], ", freeTextLength), AdditionalInfo = Long("Exact duplicate of ", freeTextLength),
            Script = Long("CREATE INDEX ", textLength), OriginalIndexDefinition = Long("CREATE NONCLUSTERED INDEX ", textLength),
            IndexSizeGb = 12.3456m, IndexRows = 1234567, IndexReads = 12345, IndexWrites = 123456,
            CanCompress = true, IsForeignKey = false, ScriptOmitsPartitionPlacement = false,
        }).ToList();
        return new IndexCleanupAnalysisResult { Recommendations = recs, DatabaseRollups = [rollup], OverallRollup = rollup, Notes = [] };
    }

    /* #5238: a limit of 500 is the largest answer this view gives and nothing trims it (#4198), so the guide tail says how big it is. Measured
       here with 1 MB = 1,048,576 bytes (the 30 KB above is 30 * 1024): 0.46 MB for typical rows, 0.92 MB for long names with script and
       definition at the 300-character cap, 3.49 MB for full_text on 3,000-character scripts. Each is asserted only within 15% of the figure the
       guide gives, and the guide must say that figure: the bound keeps the guide true when a field is added to a row; it is not a cap on the answer. */
    [Theory]
    [InlineData(28, 110, 30, false, 0.45)]
    [InlineData(100, 400, 100, false, 1.0)]
    [InlineData(100, 3000, 100, true, 3.5)]
    public void FiveHundredRecommendations_StayWithinFifteenPercentOfTheSizeTheGuideGives(int nameLength, int textLength, int freeTextLength, bool fullText, double guideMegabytes)
    {
        var json = DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("server", ShapedResult(500, nameLength, textLength, freeTextLength), 500, null, fullText, null);
        var megabytes = Bytes(json) / (1024.0 * 1024.0);

        Assert.InRange(megabytes, guideMegabytes * 0.85, guideMegabytes * 1.15);
        Assert.Contains(guideMegabytes.ToString(System.Globalization.CultureInfo.InvariantCulture) + " MB", DarlingMcpFinOpsTools.IndexAnalysisViewGuide, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultResponse_WithTenFullLengthRecommendationsAndTheDatabaseCap_FitsTheResponseBudget()
    {
        var json = DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("server", WorstDefaultResult(DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases + 37, 10), 10, null, false, null);

        Assert.True(Bytes(json) <= HeadroomBytes, $"default body is {Bytes(json)} bytes");
        Assert.True(HeadroomBytes <= McpResponseBudget.DefaultBytes);
    }

    [Fact]
    public void FullText_WithFiftyRecommendations_ReportsItsSizeAndAssertsNoBudget()
    {
        var result = WorstDefaultResult(3, 50, 4000);
        var json = DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("server", result, 50, null, true, null);
        using var doc = System.Text.Json.JsonDocument.Parse(json);

        /* full_text is documented as unbounded; the size is reported, not asserted against the budget. */
        Console.WriteLine($"full_text 50-recommendation body: {Bytes(json)} bytes");
        Assert.Equal(50, doc.RootElement.GetProperty("recommendations").GetArrayLength());
    }

    [Fact]
    public void DatabaseCap_KeepsTheListAtTheCap_AndSaysSo()
    {
        var json = DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("server", WorstDefaultResult(DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases + 5, 2), 10, null, false, null);
        using var doc = System.Text.Json.JsonDocument.Parse(json);

        Assert.Equal(DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases + 5, doc.RootElement.GetProperty("database_count").GetInt32());
        Assert.Equal(DarlingMcpFinOpsTools.MaxIndexAnalysisDatabases, doc.RootElement.GetProperty("databases").GetArrayLength());
        Assert.True(doc.RootElement.GetProperty("databases_truncated").GetBoolean());
    }
}
