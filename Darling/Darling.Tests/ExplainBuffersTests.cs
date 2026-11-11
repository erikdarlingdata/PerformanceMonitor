/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>#8570: the buffer figure the live buffer tests bound is the pages the scans read, not the root node's total.</summary>
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
}
