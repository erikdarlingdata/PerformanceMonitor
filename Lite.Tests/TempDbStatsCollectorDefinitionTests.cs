/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted tempdb_stats definition: two result sets collapse
/// to exactly one row (zeros when empty — matching the original collector), and the payload
/// order matches the tempdb_stats schema.
/// </summary>
public sealed class TempDbStatsCollectorDefinitionTests
{
    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        var names = TempDbStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "user_object_reserved_mb",
                "internal_object_reserved_mb",
                "version_store_reserved_mb",
                "total_reserved_mb",
                "unallocated_mb",
                "total_sessions_using_tempdb",
                "top_session_id",
                "top_session_tempdb_mb",
                /* #2515, APPENDED. Both stores generate their DDL from this list in order and both row
                   writers are positional, so the ceiling could only ever go last — inserting it beside
                   unallocated_mb, where it belongs semantically, would re-map every historical row. */
                "max_size_mb",
            },
            names);
    }

    [Fact]
    public void Query_TargetsBothTempDbDmvs()
    {
        var queryText = TempDbStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text;
        Assert.Contains("tempdb.sys.dm_db_file_space_usage", queryText, System.StringComparison.Ordinal);
        Assert.Contains("sys.dm_db_session_space_usage", queryText, System.StringComparison.Ordinal);
        Assert.Equal("tempdb_stats", TempDbStatsCollector.Instance.Name);
        Assert.Equal("tempdb_stats", TempDbStatsCollector.Instance.TargetTable);
    }

    /// <summary>
    /// #2515: the ceiling comes from tempdb's own catalog, and the two things that make it the RIGHT
    /// ceiling are both in the query rather than in the reader — so they can only be pinned here.
    ///
    /// <para>LOG files are excluded because <c>dm_db_file_space_usage</c>, which supplies every other
    /// column, reports DATA allocation: folding the log's cap into the same denominator would understate
    /// usage on every server, not just Azure. And <c>max_size</c> is an <c>int</c> of 8 KB pages that tops
    /// out at 16 TB per file, so a wide tempdb can overflow a plain <c>SUM</c> — the widen has to happen
    /// before the sum, not after it.</para>
    /// </summary>
    [Fact]
    public void Query_ReadsTheCeilingFromTheRowsFilesOnly_AndSumsItWideEnough()
    {
        var queryText = TempDbStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text;

        Assert.Contains("tempdb.sys.database_files AS df", queryText, System.StringComparison.Ordinal);
        Assert.Contains("WHERE df.type = 0 /*ROWS*/", queryText, System.StringComparison.Ordinal);
        Assert.Contains("SUM(CONVERT(bigint, df.max_size))", queryText, System.StringComparison.Ordinal);

        /* -1 on any one data file means tempdb as a whole grows without limit, and MIN is what finds it. */
        Assert.Contains("WHEN MIN(df.max_size) = -1", queryText, System.StringComparison.Ordinal);

        /* House convention, and it is load-bearing on a query that now carries a second aggregate. */
        Assert.Contains("OPTION(RECOMPILE)", queryText, System.StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_CombinesTwoResultSets_IntoOneRow()
    {
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { new object[] { 1.5m, 2.5m, 3.5m, 7.5m, 10.0m, 65536.0m } },
            new[] { new object[] { 55, 12.25m, 9L } });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await TempDbStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new TempDbStatsCollector.Row(1.5m, 2.5m, 3.5m, 7.5m, 10.0m, 9L, 55, 12.25m, 65536.0m), row);
    }

    /// <summary>
    /// #2515: a tempdb with no ROWS files visible makes the ceiling subquery return NULL, and NULL is not a
    /// ceiling of zero. It has to land on 0 — the "not measured" state every consumer answers by dividing by
    /// the allocation, exactly as it did before this column existed. A zero cap would divide the alert's
    /// percentage by nothing at all.
    /// </summary>
    [Fact]
    public async Task ReadAsync_NullCeiling_ReadsAsNotMeasured_NotAsAZeroCap()
    {
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { new object[] { 1.5m, 2.5m, 3.5m, 7.5m, 10.0m, System.DBNull.Value } },
            new[] { new object[] { 55, 12.25m, 9L } });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await TempDbStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(0m, Assert.Single(rows).MaxSizeMb);
    }

    /// <summary>
    /// And the unlimited answer survives the read AS -1 rather than being flattened to 0. They take the same
    /// denominator, but they are different facts — "this tempdb has no ceiling" versus "nobody looked" — and
    /// the alert detail says which.
    /// </summary>
    [Fact]
    public async Task ReadAsync_UnlimitedCeiling_StaysMinusOne()
    {
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { new object[] { 1.5m, 2.5m, 3.5m, 7.5m, 10.0m, -1m } },
            new[] { new object[] { 55, 12.25m, 9L } });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await TempDbStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(-1m, Assert.Single(rows).MaxSizeMb);
    }

    [Fact]
    public async Task ReadAsync_EmptyResultSets_StillYieldsOneZeroRow()
    {
        using var reader = FakeCollectorDataReader.WithResultSets(
            System.Array.Empty<object[]>(),
            System.Array.Empty<object[]>());

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await TempDbStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(default(TempDbStatsCollector.Row), row);
    }

    [Fact]
    public void WritePayload_EmitsSchemaOrder_NoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var writer = new RecordingCollectorRowWriter();
        var row = new TempDbStatsCollector.Row(1.5m, 2.5m, 3.5m, 7.5m, 10.0m, 9L, 55, 12.25m, 65536.0m);

        TempDbStatsCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(deltas));

        Assert.Equal(new object?[] { 1.5m, 2.5m, 3.5m, 7.5m, 10.0m, 9L, 55, 12.25m, 65536.0m }, writer.Values);
        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// #2512: the whole pipeline on an AZURE SQL DATABASE target, which this collector was gated off
    /// until the gate's stated reason was checked and found false.
    ///
    /// <para>Three things the gate meant nobody had ever exercised, and each fails differently:</para>
    /// <list type="number">
    /// <item><b>The query is target-independent.</b> If Azure needed a variant, <c>BuildQuery</c> would
    /// have to branch on the target — it does not, and the measurement says it does not need to.</item>
    /// <item><b>Column typing.</b> <c>sys.dm_db_session_space_usage.session_id</c> is <c>smallint</c>, so
    /// the driver hands back a <c>short</c>. The definition reads it through
    /// <c>Convert.ToInt32(GetValue(0))</c> rather than <c>GetInt32</c> precisely for that, and the payload
    /// column is <c>top_session_id INTEGER</c> — so the widening has to happen and has to be pinned. A
    /// fixture that feeds an <c>int</c> (as the parity pin above does) can never see this.</item>
    /// <item><b>The fan-out shape.</b> <c>RunsPerDatabase</c> is false, so on Azure SQL DB this takes the
    /// plain single-connection path rather than the per-database loop <c>file_io_stats</c> and
    /// <c>index_object_stats</c> take. Per #2220 a registration that names a database is scoped to that
    /// database, so this collects one tempdb snapshot per registration — not N of them.</item>
    /// </list>
    ///
    /// <para>Values are the ones actually measured on <c>GP_S_Gen5_2</c> (EngineEdition 5) on 2026-08-22,
    /// not invented ones, so the row this asserts is a row the platform really produced.</para>
    /// </summary>
    [Fact]
    public async Task AzureSqlDb_MeasuredValues_ComposeThroughToThePayload()
    {
        var azure = new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 };
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator(), isAzureSqlDb: true);

        /* One query for every target — no Azure variant, which is the claim the gate rested on. */
        Assert.Equal(
            TempDbStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text,
            TempDbStatsCollector.Instance.BuildQuery(context).Text,
            System.StringComparer.Ordinal);

        /* Plain path, not the Azure per-database loop. */
        Assert.False(TempDbStatsCollector.Instance.RunsPerDatabase(azure));
        Assert.Null(TempDbStatsCollector.Instance.BuildEnumerationQuery(context));

        using var reader = FakeCollectorDataReader.WithResultSets(
            /* result set 1: user 5.44 / internal 1.81 / version 0.00 / total 7.25 / unallocated 54.19 MB */
            new[] { new object[] { 5.44m, 1.81m, 0.00m, 7.25m, 54.19m, 65536.00m } },
            /* result set 2: session 74 as SMALLINT, 0.13 MB, 1 session over threshold as COUNT_BIG */
            new[] { new object[] { (short)74, 0.13m, 1L } });

        var rows = await TempDbStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        /*
            The ceiling is the ninth member and the reason this test exists on Azure at all: these five
            allocation figures are the real GP_S_Gen5_2 measurement, where 7.25 MB reserved inside 62.44 MB
            allocated reads 10% full -- and against the 65,536 MB ROWS ceiling the platform will actually
            grow to, 0.01%. Asserting the ceiling composes through is what stops the payload carrying the
            allocation alone and the alert dividing by the wrong number again (#2515).
        */
        Assert.Equal(
            new TempDbStatsCollector.Row(5.44m, 1.81m, 0.00m, 7.25m, 54.19m, 1L, 74, 0.13m, 65536.00m),
            row);

        var writer = new RecordingCollectorRowWriter();
        TempDbStatsCollector.Instance.WritePayload(row, writer, context);

        /* Positional AND typed: 74 must arrive as int, not short, or the INTEGER column takes a
           narrowed write on the Darling COPY path. */
        Assert.Equal(TempDbStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal(new object?[] { 5.44m, 1.81m, 0.00m, 7.25m, 54.19m, 1L, 74, 0.13m, 65536.00m }, writer.Values);
        Assert.IsType<int>(writer.Values[6]);
        Assert.IsType<long>(writer.Values[5]);
    }
}
