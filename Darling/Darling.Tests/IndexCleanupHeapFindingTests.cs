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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8880: the Heap finding in Index Analysis. A large, busy heap is a REVIEW row with no script, counted in the roll-up's Heaps
/// column and in none of its other figures; the Darling reader turns it on; MCP, the web page and the Viewer carry the count.
/// </summary>
public sealed class IndexCleanupHeapFindingTests
{
    private static readonly DateTime Start = new(2026, 10, 2, 6, 15, 0, DateTimeKind.Unspecified);

    private static IndexCleanupIndexInput Heap(
        long rows = 15_000_000, decimal reservedMb = 2457.6m, long scans = 12_400, long lookups = 35_810, long updates = 1_204_000,
        int objectId = 10, string table = "Events", string database = "Sales", int databaseId = 7, DateTime? start = null) => new()
    {
        DatabaseName = database,
        DatabaseId = databaseId,
        SchemaName = "dbo",
        ObjectId = objectId,
        TableName = table,
        IndexId = 0,
        IndexName = null,
        IndexTypeDesc = "HEAP",
        UserScans = scans,
        UserLookups = lookups,
        UserUpdates = updates,
        ReservedMb = reservedMb,
        TotalRows = rows,
        PartitionCount = 1,
        DataCompressionDesc = "NONE",
        SqlServerStartTime = start,
    };

    private static IndexCleanupIndexInput Nonclustered(
        string name, int indexId, int objectId = 10, string table = "Events", string database = "Sales", int databaseId = 7,
        decimal reservedMb = 300m, long seeks = 50, long updates = 900, bool disabled = false, DateTime? start = null) => new()
    {
        DatabaseName = database,
        DatabaseId = databaseId,
        SchemaName = "dbo",
        ObjectId = objectId,
        TableName = table,
        IndexId = indexId,
        IndexName = name,
        IndexTypeDesc = "NONCLUSTERED",
        KeyColumns = "[Col" + indexId + "]",
        UserSeeks = seeks,
        UserUpdates = updates,
        ReservedMb = reservedMb,
        TotalRows = 15_000_000,
        PartitionCount = 1,
        DataCompressionDesc = "PAGE",
        IsDisabled = disabled,
        SqlServerStartTime = start,
    };

    private static IndexCleanupOptions Opts(bool includeHeaps = true, double? uptimeDays = 46, DateTime? start = null) => new()
    {
        IncludeHeaps = includeHeaps,
        ServerUptimeDays = uptimeDays,
        ServerStartTime = start ?? (uptimeDays.HasValue ? Start : null),
    };

    private static IndexCleanupRecommendation HeapRow(IndexCleanupAnalysisResult r) =>
        Assert.Single(r.Recommendations, x => x.ConsolidationRule == IndexCleanupRules.Heap);

    // ── The finding ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheIssueExample_IsAReviewRowWithNoScript_AndTheAgreedWording()
    {
        var inputs = new[] { Heap(), Nonclustered("IX_A", 2), Nonclustered("IX_B", 3), Nonclustered("IX_C", 4) };

        var rec = HeapRow(IndexCleanupAnalyzer.Analyze(inputs, Opts()));

        Assert.Equal("Heap", IndexCleanupRules.Heap);
        Assert.Equal(("Sales", "dbo", "Events", "HEAP", 0), (rec.DatabaseName, rec.SchemaName, rec.TableName, rec.IndexName, rec.IndexId));
        Assert.Equal(IndexCleanupAction.None, rec.Action);
        Assert.Equal(IndexCleanupResultKind.Review, rec.ResultKind);
        Assert.Equal("REVIEW", rec.ScriptType);
        Assert.Equal("", rec.Script);
        Assert.Equal("", rec.OriginalIndexDefinition);
        Assert.False(rec.CanCompress);
        Assert.Equal(2.4m, rec.IndexSizeGb);
        Assert.Equal(15_000_000, rec.IndexRows);
        Assert.Equal(48_210, rec.IndexReads);
        Assert.Equal(1_204_000, rec.IndexWrites);
        /* The note as agreed on #8880, word for word. */
        Assert.Equal(
            "Heap with 2.4 GB and 15,000,000 rows, read 48,210 times (12,400 scans, 35,810 RID lookups) and written 1,204,000 times since 2026-10-02. "
            + "On a heap, an update that makes a row longer can leave a forwarded record that later reads must follow, and deletes can leave empty pages "
            + "that scans still read. A clustered index avoids both and lets queries seek on its key. Choose a narrow, unique, ever-increasing key from "
            + "the queries that use it. It has 3 nonclustered indexes. Their lookups reach the heap by row locator and follow any forwarded record.",
            rec.AdditionalInfo);
    }

    [Theory]
    [InlineData(1_000_000, 10, 1_000, 0, true)]      // the row floor, exactly
    [InlineData(999_999, 10, 1_000, 0, false)]       // a row short, and small
    [InlineData(10, 1024, 500, 500, true)]           // the size floor (1 GB), exactly; scans and lookups add up
    [InlineData(10, 1023.9, 1_000, 0, false)]        // just under 1 GB, and few rows
    [InlineData(1_000_000, 10, 999, 0, false)]       // a read short
    [InlineData(1_000_000, 10, 0, 1_000, true)]      // RID lookups alone
    [InlineData(5_000_000, 5000, 0, 0, false)]       // never read: a staging table, or a memory-optimized table (no usage rows)
    public void AHeap_IsAFinding_OnlyPastTheRowOrSizeFloor_AndTheReadFloor(long rows, double reservedMb, long scans, long lookups, bool listed)
    {
        var r = IndexCleanupAnalyzer.Analyze(new[] { Heap(rows, (decimal)reservedMb, scans, lookups) }, Opts());

        Assert.Equal(listed, r.Recommendations.Any(x => x.ConsolidationRule == IndexCleanupRules.Heap));
        Assert.Equal(listed ? 1 : 0, r.OverallRollup.Heaps);
    }

    [Fact]
    public void TheThresholds_AreTheAgreedDefaults_AndHeapsAreOffUnlessAskedFor()
    {
        var defaults = new IndexCleanupOptions();
        Assert.False(defaults.IncludeHeaps);
        Assert.Equal(1_000_000, defaults.HeapMinRows);
        Assert.Equal(1m, defaults.HeapMinSizeGb);
        Assert.Equal(1_000, defaults.HeapMinReads);

        /* Off: no heap row, and a database holding only a heap has no roll-up at all. */
        var off = IndexCleanupAnalyzer.Analyze(new[] { Heap() }, Opts(includeHeaps: false));
        Assert.Empty(off.Recommendations);
        Assert.Empty(off.DatabaseRollups);
        Assert.Equal(0, off.OverallRollup.Heaps);
    }

    [Fact]
    public void TheThresholds_ComeFromTheOptions()
    {
        var options = Opts() with { HeapMinRows = 100, HeapMinSizeGb = 50m, HeapMinReads = 5 };

        Assert.Single(IndexCleanupAnalyzer.Analyze(new[] { Heap(rows: 100, reservedMb: 1m, scans: 5, lookups: 0) }, options).Recommendations);
        Assert.Empty(IndexCleanupAnalyzer.Analyze(new[] { Heap(rows: 99, reservedMb: 1m, scans: 5, lookups: 0) }, options).Recommendations);
    }

    [Fact]
    public void AShortUptime_AndDedupeOnly_LeaveTheHeapRulePlain_AndOn()
    {
        /* #8880: a short uptime can only keep a heap under the read floor, never flag one wrongly, so no uptime variant, and the
           automatic dedupe-only mode at 7 days or less does not turn the rule off. */
        var shortUp = IndexCleanupAnalyzer.Analyze(new[] { Heap() }, Opts(uptimeDays: 3));
        Assert.True(shortUp.UptimeWarning);
        Assert.True(shortUp.DedupeOnlyApplied);
        Assert.Equal("Heap", HeapRow(shortUp).ConsolidationRule);

        var asked = IndexCleanupAnalyzer.Analyze(new[] { Heap() }, Opts() with { DedupeOnly = true });
        Assert.Equal("Heap", HeapRow(asked).ConsolidationRule);
    }

    // ── The roll-up ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void ADatabaseWhoseOnlyFindingIsAHeapWithNoIndexes_StillGetsItsRollup()
    {
        var inputs = new[]
        {
            Heap(database: "HeapsOnly", databaseId: 9),
            Heap(database: "HeapsOnly", databaseId: 9, objectId: 11, table: "Audit"),
            Heap(database: "Sales", databaseId: 7),
            Nonclustered("IX_A", 2),
        };

        var r = IndexCleanupAnalyzer.Analyze(inputs, Opts());

        Assert.Equal(new[] { "HeapsOnly", "Sales" }, r.DatabaseRollups.Select(d => d.DatabaseName).ToArray());
        var heapsOnly = r.DatabaseRollups[0];
        Assert.Equal(9, heapsOnly.DatabaseId);
        Assert.Equal(2, heapsOnly.Heaps);
        Assert.Equal(new IndexCleanupRollup { DatabaseName = "HeapsOnly", DatabaseId = 9, Heaps = 2 }, heapsOnly);
        Assert.Equal(1, r.DatabaseRollups[1].Heaps);
        Assert.Equal(3, r.OverallRollup.Heaps);
        Assert.Equal(3, r.Recommendations.Count(x => x.ConsolidationRule == IndexCleanupRules.Heap));
    }

    [Fact]
    public void Heaps_LeaveEveryExistingFigureWhereItWas()
    {
        /* The heap's own nonclustered indexes are analyzed the usual way (one unused, one a compression candidate); the heap adds a row and
           a count, and nothing to the table, index, size, row, reclaim or workload figures. */
        var inputs = new[]
        {
            Heap(),
            Nonclustered("IX_Unused", 2, seeks: 0),
            Nonclustered("IX_Plain", 3) with { DataCompressionDesc = "NONE" },
        };

        var without = IndexCleanupAnalyzer.Analyze(inputs, Opts(includeHeaps: false));
        var with = IndexCleanupAnalyzer.Analyze(inputs, Opts());

        Assert.Equal(without.DatabaseRollups.Single(), with.DatabaseRollups.Single() with { Heaps = 0 });
        Assert.Equal(without.OverallRollup, with.OverallRollup with { Heaps = 0 });
        Assert.Equal(1, with.OverallRollup.Heaps);
        Assert.Equal(without.Recommendations, with.Recommendations.Where(x => x.ConsolidationRule != IndexCleanupRules.Heap).ToList());
        Assert.True(without.OverallRollup.TotalMaxSavingsGb > 0);
    }

    // ── The note ─────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheNonclusteredSentence_CountsEnabledNonclusteredIndexes_OnThatHeapOnly()
    {
        var none = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(), Nonclustered("IX_Other", 2, objectId: 99, table: "Other") }, Opts()));
        Assert.EndsWith("from the queries that use it.", none.AdditionalInfo, StringComparison.Ordinal);

        var one = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(), Nonclustered("IX_A", 2), Nonclustered("IX_Off", 3, disabled: true) }, Opts()));
        Assert.EndsWith(
            " It has 1 nonclustered index. Its lookups reach the heap by row locator and follow any forwarded record.",
            one.AdditionalInfo, StringComparison.Ordinal);
    }

    [Fact]
    public void ANonclusteredColumnstoreIndex_IsNotCountedAsARowLocatorLookup()
    {
        /* A columnstore index does not reach the heap by row locator, so a heap whose only other index is one gets no lookup sentence. */
        var columnstore = Nonclustered("NCCI_Events", 2) with { IndexTypeDesc = "NONCLUSTERED COLUMNSTORE", KeyColumns = null };
        var rec = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(), columnstore }, Opts()));
        Assert.EndsWith("from the queries that use it.", rec.AdditionalInfo, StringComparison.Ordinal);
        Assert.DoesNotContain("nonclustered index", rec.AdditionalInfo, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNote_SaysOnceForASingleReadOrWrite_AndMbUnderAGigabyte()
    {
        var options = Opts() with { HeapMinReads = 1 };
        var rec = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(reservedMb: 812.4m, scans: 1, lookups: 0, updates: 1) }, options));

        Assert.StartsWith(
            "Heap with 812 MB and 15,000,000 rows, read once (1 scan, 0 RID lookups) and written once since 2026-10-02. ",
            rec.AdditionalInfo, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNoteDate_IsTheStartTheUptimeWasMeasuredFrom()
    {
        var rowStart = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

        /* The caller's start time wins over the rows' own, so the date and the uptime caveat never disagree. */
        var fromCaller = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(start: rowStart) }, Opts()));
        Assert.Contains(" since 2026-10-02.", fromCaller.AdditionalInfo, StringComparison.Ordinal);

        /* No caller uptime at all: both fall back to the newest collected start time. */
        var fromRows = IndexCleanupAnalyzer.Analyze(
            new[] { Heap(start: rowStart), Nonclustered("IX_A", 2, start: rowStart.AddDays(3)) },
            Opts(uptimeDays: null) with { ReferenceTimeUtc = rowStart.AddDays(40) });
        Assert.Contains(" since 2026-09-04.", HeapRow(fromRows).AdditionalInfo, StringComparison.Ordinal);

        /* An uptime with no start time to go with it: no date is made up. */
        var undated = HeapRow(IndexCleanupAnalyzer.Analyze(new[] { Heap(start: rowStart) }, new IndexCleanupOptions { IncludeHeaps = true, ServerUptimeDays = 30 }));
        Assert.Contains(" since the server last started.", undated.AdditionalInfo, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNotes_ExplainTheHeapRows_OnlyWhenThereAreAny()
    {
        var with = IndexCleanupAnalyzer.Analyze(new[] { Heap() }, Opts());
        var note = Assert.Single(with.Notes, n => n.StartsWith("Heap rows are REVIEW rows", StringComparison.Ordinal));
        Assert.Contains("at least 1,000,000 rows or 1 GB, and at least 1,000 reads since the server started", note, StringComparison.Ordinal);

        var without = IndexCleanupAnalyzer.Analyze(new[] { Heap(scans: 0, lookups: 0) }, Opts());
        Assert.DoesNotContain(without.Notes, n => n.StartsWith("Heap rows", StringComparison.Ordinal));
    }

    // ── Darling's reader ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheReader_TurnsHeapsOn_WithTheDefaults_AndTheStartItMeasuredTheUptimeFrom()
    {
        var now = new DateTime(2026, 11, 17, 9, 0, 0, DateTimeKind.Unspecified);
        var options = DarlingFinOpsIndexAnalysisReader.DeriveIndexCleanupOptions(3, "16.0.1000.6", Start, now);

        Assert.True(options.IncludeHeaps);
        Assert.Equal(Start, options.ServerStartTime);
        Assert.Equal((now - Start).TotalDays, options.ServerUptimeDays!.Value, precision: 6);
        Assert.Equal((1_000_000L, 1m, 1_000L), (options.HeapMinRows, options.HeapMinSizeGb, options.HeapMinReads));

        /* A start ahead of now is discarded for the uptime, so it dates nothing either. */
        var skewed = DarlingFinOpsIndexAnalysisReader.DeriveIndexCleanupOptions(3, "16.0.1000.6", now.AddDays(1), now);
        Assert.Null(skewed.ServerUptimeDays);
        Assert.Null(skewed.ServerStartTime);
        Assert.True(skewed.IncludeHeaps);
    }

    // ── MCP, the web page and the Viewer ─────────────────────────────────────────────────────────────

    private static JsonElement Payload(IndexCleanupAnalysisResult r) =>
        JsonDocument.Parse(DarlingMcpFinOpsTools.BuildIndexAnalysisPayload("srv", r, 50, null, false, null)).RootElement;

    [Fact]
    public void Mcp_CarriesHeapsOnEveryRollupRow_AndTheHeapAsAReviewRowWithNoScript()
    {
        var r = IndexCleanupAnalyzer.Analyze(new[] { Heap(database: "HeapsOnly", databaseId: 9), Heap(), Nonclustered("IX_A", 2) }, Opts());

        var root = Payload(r);

        Assert.Equal(2, root.GetProperty("overall").GetProperty("heaps").GetInt32());
        var dbs = root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database_name").GetString()!);
        Assert.Equal(1, dbs["HeapsOnly"].GetProperty("heaps").GetInt32());
        Assert.Equal(0, dbs["HeapsOnly"].GetProperty("index_count").GetInt32());
        Assert.Equal(1, dbs["Sales"].GetProperty("heaps").GetInt32());

        var heapRows = root.GetProperty("recommendations").EnumerateArray()
            .Where(x => x.GetProperty("consolidation_rule").GetString() == "Heap").ToList();
        Assert.Equal(2, heapRows.Count);
        Assert.All(heapRows, h =>
        {
            Assert.Equal("REVIEW", h.GetProperty("action").GetString());
            Assert.Equal("HEAP", h.GetProperty("index_name").GetString());
            Assert.Equal(0, h.GetProperty("index_id").GetInt32());
            Assert.Equal("", h.GetProperty("script").GetString());
        });
        Assert.Equal(2, root.GetProperty("counts_by_action").GetProperty("REVIEW").GetInt32());
    }

    [Fact]
    public void McpGuide_SaysReviewHoldsTheHeapFinding_AndWhatHeapsCounts()
    {
        var guide = DarlingMcpFinOpsTools.IndexAnalysisViewGuide;
        Assert.Contains("REVIEW also holds the Heap finding (consolidation_rule Heap, index_name HEAP, index_id 0)", guide, StringComparison.Ordinal);
        Assert.Contains("at least 1,000,000 rows or 1 GB reserved and at least 1,000 reads (scans + RID lookups) since the server started", guide, StringComparison.Ordinal);
        Assert.Contains("heaps counts them per database and overall", guide, StringComparison.Ordinal);
    }

    [Fact]
    public void Viewer_RollupRowsCarryTheHeapsCount()
    {
        var rows = ViewerDataService.ProjectRollups(new IndexCleanupAnalysisResult
        {
            OverallRollup = new IndexCleanupRollup { Heaps = 3 },
            DatabaseRollups = [new IndexCleanupRollup { DatabaseName = "Sales", Heaps = 3 }],
        });
        Assert.Equal(new[] { 3, 3 }, rows.Select(x => x.Heaps).ToArray());
    }
}
