/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Xunit;

namespace Darling.Tests;

/// <summary>#8570: the buffer figure the live buffer tests bound is the pages the scans read, not the root node's total. #9809: the rows figure holds on the
/// whole numbers PostgreSQL 17 and earlier print and on the hundredths of 18.</summary>
public sealed class ExplainBuffersTests
{
    /* The shape of a plan over a hypertable: a root that holds the catalog buffers of a cold backend (a CI run counted 4,430 there), a
       ChunkAppend that holds them too, and under it the scans that read the pages. The bitmap heap scan's 10 already include its index scan's 4. */
    private const string ColdBackendPlan = """
        [{"Plan": {"Node Type": "Limit", "Shared Hit Blocks": 4400, "Shared Read Blocks": 30,
          "Plans": [
            {"Node Type": "Custom Scan", "Custom Plan Provider": "ChunkAppend", "Parent Relationship": "InitPlan", "Shared Hit Blocks": 4300, "Shared Read Blocks": 20,
             "Plans": [
               {"Node Type": "Index Only Scan", "Parallel Aware": false, "Shared Hit Blocks": 2, "Shared Read Blocks": 1},
               {"Node Type": "Bitmap Heap Scan", "Shared Hit Blocks": 10, "Shared Read Blocks": 0,
                "Plans": [{"Node Type": "Bitmap Index Scan", "Shared Hit Blocks": 4, "Shared Read Blocks": 0}]},
               {"Node Type": "Index Scan", "Actual Loops": 0}
             ]},
            {"Node Type": "Seq Scan", "Parallel Aware": true, "Shared Hit Blocks": 80, "Shared Read Blocks": 8}
          ]}}]
        """;

    [Fact]
    public void DataPages_CountsTheScansPages_AndNotTheCatalogBuffersTheRootAndChunkAppendHold()
    {
        /* 3 for the index only scan, 10 for the bitmap heap scan (its bitmap index scan is inside it), 88 for the seq scan, 0 for the
           index scan that never ran. */
        Assert.Equal(101L, ExplainBuffers.DataPages(ColdBackendPlan));
    }

    [Fact]
    public void DataPages_IsZero_ForAPlanWithNoScan()
    {
        Assert.Equal(0L, ExplainBuffers.DataPages("""[{"Plan": {"Node Type": "Result", "Shared Hit Blocks": 5}}]"""));
    }

    private static readonly ISet<string> Chunks = new HashSet<string>(StringComparer.Ordinal) { "_hyper_30_7_chunk", "_hyper_30_8_chunk" };

    /* An index scan of one chunk run four times that returned 46 rows (11.5 a loop): PostgreSQL 17 prints the per-loop average as 12 and 18
       prints 11.50. Beside it a chunk scan that ran once and returned 10, and a scan of a table that is not in the set. */
    private static string RowsPlan(string perLoop) => """
        [{"Plan": {"Node Type": "Limit", "Actual Rows": 1, "Actual Loops": 1,
          "Plans": [
            {"Node Type": "Index Scan", "Relation Name": "_hyper_30_7_chunk", "Actual Rows": PERLOOP, "Actual Loops": 4},
            {"Node Type": "Seq Scan", "Relation Name": "_hyper_30_8_chunk", "Actual Rows": 10, "Actual Loops": 1},
            {"Node Type": "Seq Scan", "Relation Name": "other_table", "Actual Rows": 1000, "Actual Loops": 1}
          ]}}]
        """.Replace("PERLOOP", perLoop, StringComparison.Ordinal);

    [Fact]
    public void RowsRead_BoundsARoundedPlanOnBothSides_AndPinsAFractionalOne()
    {
        /* 12 a loop over four loops: the node returned 46 to 50 rows, so the plan read 56 to 60 and the printed figure is 58. */
        var rounded = RowsPlan("12");
        Assert.Equal(56L, ExplainBuffers.RowsReadAtLeast(rounded, Chunks));
        Assert.Equal(60L, ExplainBuffers.RowsReadAtMost(rounded, Chunks));
        Assert.Equal(58L, ExplainBuffers.RowsRead(rounded, Chunks));

        /* 11.50 a loop: 46 rows and no room either way, so the bound is as tight as on a count. */
        var fraction = RowsPlan("11.50");
        Assert.Equal(56L, ExplainBuffers.RowsReadAtLeast(fraction, Chunks));
        Assert.Equal(56L, ExplainBuffers.RowsReadAtMost(fraction, Chunks));
        Assert.Equal(56L, ExplainBuffers.RowsRead(fraction, Chunks));
    }

    [Fact]
    public void RowsRead_ALoopThatPrintedZeroReadsAtMostHalfARowAndNeverBelowZero_AndASingleLoopAllowsNothing()
    {
        /* 0 a loop over four loops: the node returned 0 to 2 rows. The chunk scan beside it adds its 10. */
        var none = RowsPlan("0");
        Assert.Equal(10L, ExplainBuffers.RowsReadAtLeast(none, Chunks));
        Assert.Equal(12L, ExplainBuffers.RowsReadAtMost(none, Chunks));

        /* A node that ran once prints its total, rounded or not. */
        const string once = """[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "_hyper_30_7_chunk", "Actual Rows": 12, "Actual Loops": 1}}]""";
        Assert.Equal(12L, ExplainBuffers.RowsReadAtLeast(once, Chunks));
        Assert.Equal(12L, ExplainBuffers.RowsReadAtMost(once, Chunks));
    }

    [Fact]
    public void RowsRead_IsZero_ForAPlanThatReadsNoneOfTheRelations_AndForANodeThatNeverRan()
    {
        const string elsewhere = """[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "other_table", "Actual Rows": 5, "Actual Loops": 3}}]""";
        Assert.Equal(0L, ExplainBuffers.RowsReadAtLeast(elsewhere, Chunks));
        Assert.Equal(0L, ExplainBuffers.RowsReadAtMost(elsewhere, Chunks));

        const string neverRan = """[{"Plan": {"Node Type": "Index Scan", "Relation Name": "_hyper_30_7_chunk", "Actual Rows": 0, "Actual Loops": 0}}]""";
        Assert.Equal(0L, ExplainBuffers.RowsReadAtLeast(neverRan, Chunks));
        Assert.Equal(0L, ExplainBuffers.RowsReadAtMost(neverRan, Chunks));
    }
}
