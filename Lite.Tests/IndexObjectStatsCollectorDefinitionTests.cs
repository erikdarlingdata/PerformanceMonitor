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
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted index_object_stats definition: dual execution
/// (Azure per-database connections; on-prem enumeration + quote-doubled [db].sys.sp_executesql
/// body), the dedicated 300 s per-database budget (#1135), and the 57-column payload (44
/// size/usage/locking counters + 13 index-definition columns for monitor-side UNUSED/DUPLICATE
/// analysis, Stage 1).
/// </summary>
public sealed class IndexObjectStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void DualExecution_AzurePerDatabase_OnPremEnumeration()
    {
        Assert.True(IndexObjectStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(IndexObjectStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo()));

        Assert.Null(IndexObjectStatsCollector.Instance.BuildEnumerationQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)));
        var enumeration = IndexObjectStatsCollector.Instance.BuildEnumerationQuery(CollectorTestContext.Make(s_deltas));
        Assert.NotNull(enumeration);
        Assert.Contains("HAS_DBACCESS(d.name) = 1", enumeration!.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void TimeoutOverride_IsTheDedicated1135Budget()
    {
        Assert.Equal(300, IndexObjectStatsCollector.Instance.CommandTimeoutSecondsOverride);
    }

    [Fact]
    public void PerItemQuery_QuoteDoublesTheBody_AndEscapesBrackets()
    {
        var plan = IndexObjectStatsCollector.Instance.BuildPerItemQuery("we]rd db", CollectorTestContext.Make(s_deltas));

        Assert.StartsWith("EXECUTE [we]]rd db].sys.sp_executesql N'", plan.Text, StringComparison.Ordinal);
        /* The body's own literals must arrive quote-doubled inside the N'...' wrapper. */
        Assert.Contains("o.type IN (N''U'', N''V'')", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("o.type IN (N'U', N'V')", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureQuery_IsTheRawStagedBody()
    {
        var plan = IndexObjectStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("INTO #sizes", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL)", plan.Text, StringComparison.Ordinal);
        Assert.Contains("o.type IN (N'U', N'V')", plan.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3508: the per-index DEFINITION metadata (key/include lists, the FK flags, compression) is
    /// built from staged #temps and joined in, NOT rebuilt as correlated subqueries against the live
    /// catalog once per index row - which is what made a wide schema pay per index (~28 s on a
    /// 54K-index database). A revert to the correlated form would drop these markers.
    /// </summary>
    [Fact]
    public void DefinitionMetadata_IsStaged_NotCorrelatedPerIndex()
    {
        var plan = IndexObjectStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("INTO #index_columns", plan.Text, StringComparison.Ordinal);
        Assert.Contains("INTO #index_definitions", plan.Text, StringComparison.Ordinal);
        Assert.Contains("INTO #index_compression", plan.Text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN #index_definitions", plan.Text, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN #index_compression", plan.Text, StringComparison.Ordinal);
        /* The final projection reads the definition metadata from the temps - the key/include lists
           correlate to #index_columns during staging, never to the live catalog in the final SELECT. */
        Assert.Contains("key_columns = defs.key_columns", plan.Text, StringComparison.Ordinal);
        Assert.Contains("data_compression_desc = comp.data_compression_desc", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder_58Columns()
    {
        var names = IndexObjectStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Equal(58, names.Length);
        Assert.Equal("sqlserver_start_time", names[0]);
        Assert.Equal("page_io_latch_wait_in_ms", names[43]);
        /* The Stage-1 index-definition metadata is appended after the counters (ordinals 44..57). */
        Assert.Equal("key_columns", names[44]);
        Assert.Equal("included_columns", names[45]);
        Assert.Equal("filter_definition", names[46]);
        Assert.Equal("is_foreign_key", names[48]);
        Assert.Equal("is_foreign_key_reference", names[49]);
        Assert.Equal("data_compression_desc", names[51]);
        Assert.Equal("optimize_for_sequential_key", names[52]);
        Assert.Equal("fill_factor", names[53]);
        Assert.Equal("allow_row_locks", names[56]);
        Assert.Equal("is_indexed_view", names[57]);
    }

    [Fact]
    public async Task ReadItem_AndWrite_RoundTrip_WithBitConversions()
    {
        var start = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);
        var input = new object[58];
        input[0] = start; input[1] = "SO"; input[2] = 7; input[3] = "dbo"; input[4] = 12345;
        input[5] = "Posts"; input[6] = 1; input[7] = "PK_Posts"; input[8] = "CLUSTERED";
        input[9] = 1; input[10] = 1; input[11] = 0; input[12] = 4;
        input[13] = 1024.50m; input[14] = 1000.25m; input[15] = 900.00m; input[16] = 50.00m; input[17] = 25.00m;
        for (int i = 18; i < 44; i++) input[i] = (long)(i * 10);
        /* last_user_* are timestamps at 23..26 */
        input[23] = start; input[24] = DBNull.Value; input[25] = start; input[26] = DBNull.Value;
        /* Stage-1 index-definition metadata (ordinals 44..57): key/include lists, filter, the
           uniqueness/FK/disabled discriminators, the reconstruct-a-CREATE options, and is_indexed_view. */
        input[44] = "[OwnerUserId], [CreationDate] DESC"; input[45] = "[Score]"; input[46] = DBNull.Value;
        input[47] = 0; input[48] = 1; input[49] = 0; input[50] = 0;
        input[51] = "PAGE"; input[52] = 1; input[53] = 90; input[54] = 0; input[55] = 1; input[56] = 1; input[57] = 0;

        var rows = new List<IndexObjectStatsCollector.Row>();
        using (var reader = new FakeCollectorDataReader(input))
        {
            await IndexObjectStatsCollector.Instance.ReadItemAsync("SO", reader, rows, CollectorTestContext.Make(s_deltas), CancellationToken.None);
        }

        var row = Assert.Single(rows);
        Assert.True(row.IsUnique);
        Assert.False(row.IsFiltered);
        Assert.Null(row.LastUserScan);
        /* Index-definition metadata round-trips (incl. the FK-protection + option flags). */
        Assert.Equal("[OwnerUserId], [CreationDate] DESC", row.KeyColumns);
        Assert.Equal("[Score]", row.IncludedColumns);
        Assert.Null(row.FilterDefinition);
        Assert.False(row.IsUniqueConstraint);
        Assert.True(row.IsForeignKey);
        Assert.False(row.IsForeignKeyReference);
        Assert.False(row.IsDisabled);
        Assert.Equal("PAGE", row.DataCompressionDesc);
        Assert.True(row.OptimizeForSequentialKey);
        Assert.Equal((short)90, row.FillFactor);
        Assert.False(row.IsPadded);
        Assert.True(row.AllowPageLocks);
        Assert.True(row.AllowRowLocks);
        Assert.False(row.IsIndexedView);

        var writer = new RecordingCollectorRowWriter();
        IndexObjectStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(58, writer.Values.Count);
        Assert.Equal(start, writer.Values[0]);
        Assert.Equal(true, writer.Values[9]);
        Assert.Equal(430L, writer.Values[43]);
        Assert.Equal("[OwnerUserId], [CreationDate] DESC", writer.Values[44]);
        Assert.Equal((short)90, writer.Values[53]);
        Assert.Equal(true, writer.Values[56]);
        Assert.Equal(false, writer.Values[57]);
    }
}
