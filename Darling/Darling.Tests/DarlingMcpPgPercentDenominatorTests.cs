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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3541 A7: five PostgreSQL MCP tools published a share of the PAGE under a name that promised a share of
/// the WINDOW. Each divided every row by the sum of the rows it had fetched, so at <c>limit = 3</c> the three
/// <c>pct_of_total_*</c> figures summed to 100% by construction and read, to an agent holding only the JSON,
/// as "these three are everything". <c>get_pg_top_queries</c> compounded it with a hidden <c>LIMIT 50</c>
/// under a <c>Take(limit)</c>, so its denominator was whichever of the two caps bit — never the window.
///
/// <para>The fix is one rule: the denominator comes off the SAME statement as the rows, as a window
/// aggregate over the grouped result before <c>LIMIT</c>, and rides on a page record beside them. This file
/// pins the arithmetic through each tool's projection with no store: a window of three series whose
/// shares are 60 / 30 / 10, read at <c>limit = 1</c>, must report the top row at <b>60</b> and not 100, must
/// publish the window's total as <c>total_*</c> and the page's own sum as <c>returned_*</c>, and must observe
/// truncation from the sentinel row rather than infer it from the cap.</para>
///
/// <para><b>Every case is a PAIR</b> — the cut page and the whole page over the same three series — because
/// the whole-page case is where the old and new arithmetic AGREE (a page that is the window sums to 100
/// either way) and only the cut page can tell them apart. Asserting the whole page alone would pass under
/// the defect. The SQL that produces the window total is exercised against live PostgreSQL by
/// <see cref="McpPageContractLivePostgresTests"/>; here the page is constructed, so what is under test is
/// that the projection DIVIDES BY THE PAGE'S WINDOW TOTAL and not by anything it can compute from the rows.</para>
/// </summary>
public sealed class DarlingMcpPgPercentDenominatorTests
{
    /* The window: three series at 600 / 300 / 100, so the whole window is 1,000 and the shares are
       60 / 30 / 10 whatever the unit. A cut page at limit = 1 carries the 600 row plus the 300 row as the
       over-fetched sentinel; the projection must drop the sentinel and still divide by 1,000. */
    private const long WindowTotal = 1_000;
    private static readonly long[] Series = [600, 300, 100];

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>
    /// The envelope contract shared by all five: the named count is the array's length, <c>truncated</c> is
    /// what the sentinel said, <c>total_*</c> is the WINDOW's figure, <c>returned_*</c> is the page's own sum,
    /// and the headline ratio is page over window. Asserted by KEY NAME so a projection that quietly
    /// repointed <c>total_*</c> at the page sum again fails on the number rather than on a missing property.
    /// </summary>
    private static void AssertEnvelope(
        JsonElement root, string rowsKey, string returnedCountKey, int returned, bool truncated,
        string totalKey, double total, string returnedKey, double returnedSum, string ratioKey)
    {
        Assert.Equal(returned, root.GetProperty(rowsKey).GetArrayLength());
        Assert.Equal(returned, root.GetProperty(returnedCountKey).GetInt32());
        Assert.Equal(truncated, root.GetProperty("truncated").GetBoolean());
        Assert.Equal(total, root.GetProperty(totalKey).GetDouble());
        Assert.Equal(returnedSum, root.GetProperty(returnedKey).GetDouble());
        Assert.Equal(Math.Round(returnedSum / total * 100, 1), root.GetProperty(ratioKey).GetDouble());
        Assert.False(string.IsNullOrEmpty(root.GetProperty("order").GetString()));
    }

    /// <summary>The per-row shares divide by the window: the cut page's single row is 60, the whole page's
    /// rows are 60 / 30 / 10 and sum to 100 ONLY because that page is the whole window.</summary>
    private static void AssertShares(JsonElement rows, string shareKey, params double[] expected)
    {
        var actual = rows.EnumerateArray().Select(r => r.GetProperty(shareKey).GetDouble()).ToArray();
        Assert.Equal(expected, actual);
    }

    /* ───────────────────────── get_pg_top_queries ───────────────────────── */

    private static DarlingPgStatementReader.PgStatementRow Statement(int i) => new(
        QueryId: 1_000 + i, DatabaseId: 16384, Calls: 10, TotalExecTimeMs: Series[i], RowsReturned: 100,
        MaxExecTimeMs: 9.5, SharedBlocksHit: 1, SharedBlocksRead: 1, StorageBlocksRead: null, OrcacheBlocksHit: null,
        TempBlocksRead: 0, TempBlocksWritten: 0, WalBytes: 0, MaxPeakMemBytes: null, QueryText: null);

    [Fact]
    public void TopQueries_ShareIsOfTheWindow_NotOfThePage()
    {
        var cut = Parse(DarlingMcpPgStatementTools.BuildTopQueriesJson(
            "srv", 24, new DarlingPgStatementReader.PgTopQueriesPage([Statement(0), Statement(1)], WindowTotal), limit: 1));
        AssertEnvelope(cut, "queries", "queries_returned", returned: 1, truncated: true,
            "total_exec_time_ms", WindowTotal, "returned_exec_time_ms", 600, "returned_pct_of_total");
        AssertShares(cut.GetProperty("queries"), "pct_of_total_time", 60);
        Assert.Equal("total_exec_time_ms_desc", cut.GetProperty("order").GetString());

        var whole = Parse(DarlingMcpPgStatementTools.BuildTopQueriesJson(
            "srv", 24, new DarlingPgStatementReader.PgTopQueriesPage([Statement(0), Statement(1), Statement(2)], WindowTotal), limit: 3));
        AssertEnvelope(whole, "queries", "queries_returned", returned: 3, truncated: false,
            "total_exec_time_ms", WindowTotal, "returned_exec_time_ms", 1_000, "returned_pct_of_total");
        AssertShares(whole.GetProperty("queries"), "pct_of_total_time", 60, 30, 10);
    }

    /// <summary>
    /// The window total is NOT recomputed from the rows: a page whose rows sum to 600 against a window of
    /// 1,000 must say 1,000, and a page handed a window total SMALLER than its rows (impossible from the
    /// reader, but the discriminating input here) must still divide by what it was handed. A projection
    /// that summed the rows would pass the first and fail the second.
    /// </summary>
    [Fact]
    public void TopQueries_DividesByThePagesWindowTotal_NeverByASumOfTheRows()
    {
        var page = new DarlingPgStatementReader.PgTopQueriesPage([Statement(0)], WindowTotalExecTimeMs: 2_400);
        var root = Parse(DarlingMcpPgStatementTools.BuildTopQueriesJson("srv", 24, page, limit: 5));

        Assert.Equal(2_400, root.GetProperty("total_exec_time_ms").GetInt64());
        Assert.Equal(25.0, root.GetProperty("queries")[0].GetProperty("pct_of_total_time").GetDouble());
        Assert.Equal(25.0, root.GetProperty("returned_pct_of_total").GetDouble());
        Assert.False(root.GetProperty("truncated").GetBoolean());
    }

    /* ───────────────────────── get_pg_wait_stats ───────────────────────── */

    private static DarlingPgWaitReader.PgWaitRow Wait(int i) =>
        new("IO", "DataFileRead" + i, TotalWaits: 10, TotalWaitTimeMs: Series[i], AvgWaitTimeMs: Series[i] / 10.0);

    [Fact]
    public void WaitStats_ShareIsOfTheWindow_NotOfThePage()
    {
        var cut = Parse(DarlingMcpPgWaitTools.BuildWaitStatsJson(
            "srv", 24, new DarlingPgWaitReader.PgWaitStatsPage([Wait(0), Wait(1)], WindowTotal), limit: 1));
        AssertEnvelope(cut, "waits", "wait_events_returned", returned: 1, truncated: true,
            "total_wait_time_ms", WindowTotal, "returned_wait_time_ms", 600, "returned_pct_of_total");
        AssertShares(cut.GetProperty("waits"), "pct_of_total_wait", 60);
        Assert.Equal("total_wait_time_ms_desc", cut.GetProperty("order").GetString());

        var whole = Parse(DarlingMcpPgWaitTools.BuildWaitStatsJson(
            "srv", 24, new DarlingPgWaitReader.PgWaitStatsPage([Wait(0), Wait(1), Wait(2)], WindowTotal), limit: 3));
        AssertEnvelope(whole, "waits", "wait_events_returned", returned: 3, truncated: false,
            "total_wait_time_ms", WindowTotal, "returned_wait_time_ms", 1_000, "returned_pct_of_total");
        AssertShares(whole.GetProperty("waits"), "pct_of_total_wait", 60, 30, 10);
    }

    /* ───────────────────────── get_pg_wait_sampling ───────────────────────── */

    private static DarlingPgWaitSamplingReader.PgWaitSamplingRow Sample(int i) => new(
        EventType: i == 2 ? "CPU" : "IO", Event: i == 2 ? "Running" : "DataFileRead", QueryId: 1_000 + i,
        SampleCount: Series[i], EstimatedWaitMs: Series[i] * 10, BackendCount: 1, CounterReset: false,
        CaptureTime: new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void WaitSampling_ShareIsOfTheWindow_NotOfThePage()
    {
        var cut = Parse(DarlingMcpPgWaitSamplingTools.BuildWaitSamplingJson(
            "srv", 24, new DarlingPgWaitSamplingReader.PgWaitSamplingPage([Sample(0), Sample(1)], WindowTotal), limit: 1));
        AssertEnvelope(cut, "waits", "waits_returned", returned: 1, truncated: true,
            "total_samples", WindowTotal, "returned_samples", 600, "returned_pct_of_total");
        AssertShares(cut.GetProperty("waits"), "pct_of_samples", 60);
        Assert.Equal("samples_desc", cut.GetProperty("order").GetString());

        var whole = Parse(DarlingMcpPgWaitSamplingTools.BuildWaitSamplingJson(
            "srv", 24, new DarlingPgWaitSamplingReader.PgWaitSamplingPage([Sample(0), Sample(1), Sample(2)], WindowTotal), limit: 3));
        AssertEnvelope(whole, "waits", "waits_returned", returned: 3, truncated: false,
            "total_samples", WindowTotal, "returned_samples", 1_000, "returned_pct_of_total");
        AssertShares(whole.GetProperty("waits"), "pct_of_samples", 60, 30, 10);
    }

    /// <summary>
    /// The sentinel row must not decide anything the page reports: a reset flagged ONLY on the over-fetched
    /// row is not on the page and must not put the reset sentence in the page's note.
    /// </summary>
    [Fact]
    public void WaitSampling_TheSentinelRow_DoesNotSpeakForThePage()
    {
        var sentinelReset = Sample(1) with { CounterReset = true };
        var root = Parse(DarlingMcpPgWaitSamplingTools.BuildWaitSamplingJson(
            "srv", 24, new DarlingPgWaitSamplingReader.PgWaitSamplingPage([Sample(0), sentinelReset], WindowTotal), limit: 1));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.DoesNotContain("RESET", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    /* ───────────────────────── get_pg_kernel_stats ───────────────────────── */

    private static DarlingPgKernelStatsReader.PgKernelStatRow Kernel(int i) => new(
        DatabaseName: "app", QueryId: 1_000 + i, TotalCpuMs: Series[i], ExecUserTimeMs: Series[i] * 0.7, ExecSystemTimeMs: Series[i] * 0.3,
        ExecReadBytes: 8_192, ExecWriteBytes: 0, MajorFaults: 0, CounterReset: false,
        CaptureTime: new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void KernelStats_ShareIsOfTheWindow_NotOfThePage()
    {
        var cut = Parse(DarlingMcpPgKernelStatsTools.BuildKernelStatsJson(
            "srv", 24, new DarlingPgKernelStatsReader.PgKernelStatsPage([Kernel(0), Kernel(1)], WindowTotal), limit: 1));
        AssertEnvelope(cut, "queries", "queries_returned", returned: 1, truncated: true,
            "total_cpu_ms", WindowTotal, "returned_cpu_ms", 600, "returned_pct_of_total");
        AssertShares(cut.GetProperty("queries"), "pct_of_total_cpu", 60);
        Assert.Equal("cpu_ms_desc", cut.GetProperty("order").GetString());

        var whole = Parse(DarlingMcpPgKernelStatsTools.BuildKernelStatsJson(
            "srv", 24, new DarlingPgKernelStatsReader.PgKernelStatsPage([Kernel(0), Kernel(1), Kernel(2)], WindowTotal), limit: 3));
        AssertEnvelope(whole, "queries", "queries_returned", returned: 3, truncated: false,
            "total_cpu_ms", WindowTotal, "returned_cpu_ms", 1_000, "returned_pct_of_total");
        AssertShares(whole.GetProperty("queries"), "pct_of_total_cpu", 60, 30, 10);
    }

    /* ───────────────────────── get_pg_io_stats ───────────────────────── */

    private static DarlingPgIoReader.PgIoRow Io(int i, bool timed) => new(
        BackendType: "client backend", ObjectType: "relation", Context: i == 0 ? "normal" : i == 1 ? "vacuum" : "bulkread",
        Reads: Series[i], ReadTimeMs: timed ? Series[i] / 10.0 : 0, Hits: 0, Extends: 0, ExtendTimeMs: 0, Evictions: 0, Reuses: 0,
        Writes: 0, WriteTimeMs: 0, OpBytes: 8_192, WriteCountersTracked: true, StatsReset: null,
        ReadBytes: 0, WriteBytes: 0, ExtendBytes: 0, ByteCountersTracked: false);

    [Fact]
    public void IoStats_BothSharesAreOfTheWindow_NotOfThePage()
    {
        var cut = Parse(DarlingMcpPgIoTools.BuildIoJson(
            "srv", 24, new DarlingPgIoReader.PgIoPage([Io(0, true), Io(1, true)], WindowTotal, WindowTotal / 10.0), limit: 1, timingSetting: true));
        AssertEnvelope(cut, "combinations", "combination_count", returned: 1, truncated: true,
            "total_reads", WindowTotal, "returned_reads", 600, "returned_pct_of_total_reads");
        Assert.Equal(100.0, cut.GetProperty("total_read_time_ms").GetDouble());
        Assert.Equal(60.0, cut.GetProperty("returned_read_time_ms").GetDouble());
        Assert.Equal(60.0, cut.GetProperty("returned_pct_of_total_read_time").GetDouble());
        AssertShares(cut.GetProperty("combinations"), "pct_of_total_reads", 60);
        AssertShares(cut.GetProperty("combinations"), "pct_of_total_read_time", 60);
        Assert.Equal("read_time_ms_desc_then_reads_desc", cut.GetProperty("order").GetString());

        var whole = Parse(DarlingMcpPgIoTools.BuildIoJson(
            "srv", 24, new DarlingPgIoReader.PgIoPage([Io(0, true), Io(1, true), Io(2, true)], WindowTotal, WindowTotal / 10.0), limit: 3, timingSetting: true));
        AssertEnvelope(whole, "combinations", "combination_count", returned: 3, truncated: false,
            "total_reads", WindowTotal, "returned_reads", 1_000, "returned_pct_of_total_reads");
        AssertShares(whole.GetProperty("combinations"), "pct_of_total_reads", 60, 30, 10);
        AssertShares(whole.GetProperty("combinations"), "pct_of_total_read_time", 60, 30, 10);
    }

    /// <summary>
    /// The window's read-time total is a sum of stored zeros when <c>track_io_timing</c> is off, and it goes
    /// null with every other time figure (#3536) — the read-count side is unaffected, because operation
    /// counts are measured whatever the timing setting is.
    /// </summary>
    [Fact]
    public void IoStats_UntrackedTiming_NullsTheReadTimeTotalsAndKeepsTheReadCounts()
    {
        var root = Parse(DarlingMcpPgIoTools.BuildIoJson(
            "srv", 24, new DarlingPgIoReader.PgIoPage([Io(0, false), Io(1, false)], WindowTotal, 0), limit: 1, timingSetting: false));

        Assert.Equal(JsonValueKind.Null, root.GetProperty("total_read_time_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("returned_read_time_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("returned_pct_of_total_read_time").ValueKind);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("combinations")[0].GetProperty("pct_of_total_read_time").ValueKind);

        Assert.Equal(WindowTotal, root.GetProperty("total_reads").GetInt64());
        Assert.Equal(60.0, root.GetProperty("returned_pct_of_total_reads").GetDouble());
        Assert.Equal(60.0, root.GetProperty("combinations")[0].GetProperty("pct_of_total_reads").GetDouble());
    }

    /// <summary>
    /// The timing INFERENCE (used only when the server's configuration was never collected) runs over the
    /// cut page, not over the sentinel: a non-zero time that exists only on the over-fetched row is not
    /// evidence the page can cite.
    /// </summary>
    [Fact]
    public void IoStats_TheSentinelRow_DoesNotDecideTheTimingInference()
    {
        var root = Parse(DarlingMcpPgIoTools.BuildIoJson(
            "srv", 24, new DarlingPgIoReader.PgIoPage([Io(0, false), Io(1, true)], WindowTotal, 30), limit: 1, timingSetting: null));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.False(root.GetProperty("io_timing_tracked").GetBoolean());
    }

    /* ───────────────────────── the boundary, once for the dialect ───────────────────────── */

    /// <summary>
    /// <c>truncated</c> is observed from the sentinel, never inferred from the cap: a page holding EXACTLY
    /// <c>limit</c> rows is not truncated, and one holding <c>limit + 1</c> is. <c>count &gt;= limit</c> gets
    /// the first case wrong, which is why every pair above seeds the whole window at <c>limit = 3</c>; this
    /// states the rule once more at <c>limit = 2</c> so it is not mistaken for a property of the number 3.
    /// </summary>
    [Fact]
    public void Truncation_IsObservedFromTheSentinel_NotInferredFromTheCap()
    {
        var exactlyLimit = Parse(DarlingMcpPgWaitTools.BuildWaitStatsJson(
            "srv", 24, new DarlingPgWaitReader.PgWaitStatsPage([Wait(0), Wait(1)], WindowTotal), limit: 2));
        Assert.False(exactlyLimit.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, exactlyLimit.GetProperty("wait_events_returned").GetInt32());

        var onePast = Parse(DarlingMcpPgWaitTools.BuildWaitStatsJson(
            "srv", 24, new DarlingPgWaitReader.PgWaitStatsPage([Wait(0), Wait(1), Wait(2)], WindowTotal), limit: 2));
        Assert.True(onePast.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, onePast.GetProperty("wait_events_returned").GetInt32());
        /* And the sentinel's own figure is not on the page: returned is 900, not 1,000. */
        Assert.Equal(900.0, onePast.GetProperty("returned_wait_time_ms").GetDouble());
    }
}
