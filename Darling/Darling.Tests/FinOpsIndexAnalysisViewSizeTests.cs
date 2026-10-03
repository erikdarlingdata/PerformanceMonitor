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
/// caveat notes and the analyzer's full note list. The body must fit the default MCP response budget with headroom.
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
