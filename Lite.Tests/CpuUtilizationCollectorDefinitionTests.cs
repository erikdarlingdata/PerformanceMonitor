/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted cpu_utilization definition: the three-way query
/// selection (ring buffer vs. Azure vs. Azure+watermark with its bound parameter), the
/// client-side ring-buffer dedup, the Linux NULL other-process rule (#1048), and — since Darling V134 /
/// Lite v63 (#3653 item 13, Q7) — the UTC twin: every arm projects <c>sample_time_utc</c> LAST, the
/// ring-buffer arm derives it by the identical two-step DATEADD off <c>SYSUTCDATETIME()</c> while
/// <c>sample_time</c> keeps <c>SYSDATETIME()</c> and the watermark, and the writer emits it as the fourth
/// payload value.
/// </summary>
public sealed class CpuUtilizationCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_RingBuffer_ForNonAzure_EvenWithWatermark()
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: DateTime.UtcNow));

        Assert.Contains("RING_BUFFER_SCHEDULER_MONITOR", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.dm_os_host_info", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_RingBuffer_SuppressesOtherCpu_OnlyWhenSystemIdleIsZero()
    {
        /* Some Linux/SQL Server version combos report a non-zero SystemIdle, so
           unconditionally nulling other-process CPU on @is_linux = 1 over-suppressed a
           derivable value. The guard must require BOTH @is_linux = 1 AND system_idle = 0. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("WHEN @is_linux = 1 AND x.system_idle = 0", plan.Text, StringComparison.Ordinal);

        /* Normalize line endings first: on a CRLF checkout the template contains "= 1\r\n", which
           the "\n"-suffixed needle would never match, making this guard vacuously pass. */
        Assert.DoesNotContain("WHEN @is_linux = 1\n", plan.Text.Replace("\r\n", "\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_RingBuffer_ComputesSampleTimeAtMillisecondPrecision_NotSecondTruncated()
    {
        /* #2749: DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), ...) discarded the sub-second
           remainder of the elapsed-ticks offset, and that remainder differs on every poll (SYSDATETIME()
           keeps moving; a given ring-buffer entry's own timestamp does not). Since this query re-reads
           the ring buffer's whole retained history every cycle and dedups client-side on this COMPUTED
           value, a later poll's recomputed sample_time for the SAME physical entry could drift forward by
           a fraction of a second, land just above the watermark, and re-insert a near-duplicate row a
           moment after the original -- which collapsed the CPU chart's TimeSeriesGaps median threshold to
           sub-second and broke the line at every genuine ~1-minute interval, leaving only dots. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("MILLISECOND, -((@ms_ticks - t.timestamp) % 1000)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_RingBuffer_SplitsSampleTimeMath_ToAvoidMillisecondDateaddOverflow()
    {
        /* #2755: a single DATEADD(MILLISECOND, -(@ms_ticks - t.timestamp), ...) -- the #2749 fix's shape --
           overflows once the elapsed-ticks offset exceeds int range (~24.8 days of milliseconds), which a
           long-uptime box hit in production within minutes of that fix shipping ("Arithmetic overflow error
           converting expression to data type int"). Splitting into a SECOND-scale DATEADD (safely inside int
           range for realistic uptimes, ~68 years) plus a MILLISECOND-scale DATEADD for the 0-999 remainder
           preserves the exact same instant -- verified bit-identical against a live SQL Server across the
           full delta range, including negative deltas and values past the int boundary -- without ever
           passing an out-of-range value to any single DATEADD call. */
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));

        Assert.Contains("DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME())", plan.Text, StringComparison.Ordinal);
        Assert.Contains(
            "DATEADD(\n        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),\n        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME())),",
            plan.Text.Replace("\r\n", "\n"),
            StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_Azure_NoWatermark_UsesTop60()
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("sys.dm_db_resource_stats", plan.Text, StringComparison.Ordinal);
        Assert.Contains("TOP (60)", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_Azure_WithWatermark_FiltersServerSide_AndBindsParameter()
    {
        var watermark = new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Utc);
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark));

        Assert.Contains("WHERE drs.end_time > @last_sample_time", plan.Text, StringComparison.Ordinal);
        var parameter = Assert.Single(plan.Parameters);
        Assert.Equal("@last_sample_time", parameter.Name);
        Assert.Equal(watermark, parameter.Value);
        Assert.Equal(CollectorParameterType.DateTime2, parameter.Type);
    }

    /// <summary>The watermark stays on the server-local <c>sample_time</c> — a watermark that changed frame
    /// would re-ingest or skip one UTC offset's worth of samples on the first poll after the upgrade — and the
    /// UTC twin is declared LAST, where the positional writers and an upgraded store's ALTER agree it sits.</summary>
    [Fact]
    public void WatermarkColumn_IsSampleTime_AndNamesMatchDispatchAndSchema()
    {
        Assert.Equal("sample_time", CpuUtilizationCollector.Instance.WatermarkColumn);
        Assert.Equal("cpu_utilization", CpuUtilizationCollector.Instance.Name);
        Assert.Equal("cpu_utilization_stats", CpuUtilizationCollector.Instance.TargetTable);
        Assert.Equal(
            new[] { "sample_time", "sqlserver_cpu_utilization", "other_process_cpu_utilization", "sample_time_utc" },
            CpuUtilizationCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
        Assert.Equal(CollectorColumnType.Timestamp, CpuUtilizationCollector.Instance.PayloadColumns[^1].Type);
    }

    /* ── #3653 item 13 (Q7): the UTC twin, per arm ── */

    /// <summary>
    /// The ring-buffer arm derives <c>sample_time_utc</c> by the SAME two-step DATEADD as <c>sample_time</c>
    /// (the #2749 millisecond precision and the #2755 overflow split both apply, because the age arithmetic is
    /// identical) anchored on <c>SYSUTCDATETIME()</c> instead of <c>SYSDATETIME()</c> — so the two columns differ
    /// by exactly the server's offset at the poll and by nothing else. <c>sample_time</c> itself keeps the local
    /// clock: converting it would break Lite's chart window and move the watermark's frame mid-series
    /// (<c>CollectorTimestampFrameTests</c> pins the same fact from the other side).
    /// </summary>
    [Fact]
    public void BuildQuery_RingBuffer_DerivesTheUtcTwin_ByTheSameArithmeticOffSysUtcDateTime()
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false));
        var text = plan.Text.Replace("\r\n", "\n");

        Assert.Contains(
            "sample_time_utc = DATEADD(\n        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),\n        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSUTCDATETIME()))",
            text, StringComparison.Ordinal);
        Assert.Contains(
            "sample_time = DATEADD(\n        MILLISECOND, -((@ms_ticks - t.timestamp) % 1000),\n        DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME())),",
            text, StringComparison.Ordinal);

        /* One local clock, one UTC clock, each on its own column (counted in the CODE, with the T-SQL's own
           block-comment reasoning stripped, since that reasoning names both functions): the local one must not
           have been converted and the UTC one must not have been derived from the local column plus a
           minute-quantised DATEDIFF. */
        var code = System.Text.RegularExpressions.Regex.Replace(text, @"/\*.*?\*/", " ", System.Text.RegularExpressions.RegexOptions.Singleline);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(code, @"\bSYSDATETIME\s*\(\s*\)"));
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(code, @"\bSYSUTCDATETIME\s*\(\s*\)"));
        Assert.DoesNotContain("DATEDIFF", code, StringComparison.Ordinal);

        /* Projected LAST, after the three pre-rung columns, so the reader's ordinal 3 is the twin. */
        Assert.True(
            text.IndexOf("sample_time_utc = ", StringComparison.Ordinal) > text.IndexOf("other_process_cpu_utilization =", StringComparison.Ordinal),
            "sample_time_utc must be the LAST projected column on the ring-buffer arm");
    }

    /// <summary>
    /// Both Azure arms project <c>sample_time_utc = drs.end_time</c> — the column <c>sample_time</c> already
    /// reads. That is a fact, not a shortcut: <c>sys.dm_db_resource_stats.end_time</c> is documented UTC and
    /// Azure SQL Database's own clock IS UTC, so the local and UTC frames are one value there and neither
    /// column lies. Projected last on both arms, like the ring buffer's.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BuildQuery_Azure_ProjectsEndTimeAsTheUtcTwin_Last(bool watermarked)
    {
        var plan = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermarked ? DateTime.UtcNow : null));
        var text = plan.Text.Replace("\r\n", "\n");

        Assert.Contains("    sample_time = drs.end_time,\n", text, StringComparison.Ordinal);
        Assert.Contains("    other_process_cpu_utilization = 0,\n    sample_time_utc = drs.end_time\nFROM sys.dm_db_resource_stats AS drs", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SYSDATETIME", text, StringComparison.Ordinal);
        Assert.DoesNotContain("SYSUTCDATETIME", text, StringComparison.Ordinal);
    }

    /// <summary>The fourth ordinal is the twin; it survives the ring-buffer dedup on the row it belongs to,
    /// and a NULL there (a shape no arm produces, guarded for the store's nullable column) reads as null
    /// rather than throwing.</summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_DedupsAtOrBelowWatermark()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var offset = TimeSpan.FromHours(-4);   /* the row's server sits at UTC-4: its UTC twin is four hours AHEAD of its local stamp */
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddSeconds(60), 50, 10, watermark.AddSeconds(60) - offset },
            new object[] { watermark, 40, 5, watermark - offset },
            new object[] { watermark.AddSeconds(-60), 30, 2, DBNull.Value });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermark),
            CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new CpuUtilizationCollector.Row(watermark.AddSeconds(60), 50, 10, watermark.AddSeconds(60) - offset), row);

        /* The dedup keys on the LOCAL stamp (the watermark's frame), never on the twin. */
        Assert.Equal(watermark.AddSeconds(60), row.SampleTime);
        Assert.Equal(row.SampleTime - offset, row.SampleTimeUtc);
    }

    [Fact]
    public async Task ReadAsync_Azure_DoesNotDedupClientSide()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark, 40, 0, watermark });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark),
            CancellationToken.None);

        var row = Assert.Single(rows);
        /* On Azure the two frames are one value (the clock is UTC; end_time is UTC). */
        Assert.Equal(row.SampleTime, row.SampleTimeUtc);
    }

    [Fact]
    public async Task ReadAsync_NullOtherProcess_StaysNull_LinuxRule()
    {
        var sample = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var sampleUtc = sample.AddHours(4);
        using var reader = new FakeCollectorDataReader(
            new object[] { sample, 55, DBNull.Value, sampleUtc });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.OtherProcessCpuUtilization);

        var writer = new RecordingCollectorRowWriter();
        CpuUtilizationCollector.Instance.WritePayload(row, writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { sample, 55, null, sampleUtc }, writer.Values);
    }

    /// <summary>The writer emits exactly one value per declared payload column, the twin LAST and null-or-value:
    /// the positional appender / binary COPY land it in the appended column, and a row without one writes NULL
    /// rather than a fabricated instant.</summary>
    [Fact]
    public void WritePayload_EmitsTheUtcTwinLast_AndNullStaysNull()
    {
        var local = new DateTime(2026, 11, 1, 1, 30, 0);   /* 01:30 on the fall-back morning: a local stamp that names TWO instants */
        var utc = new DateTime(2026, 11, 1, 5, 30, 0);     /* the twin says WHICH one: the first (EDT, UTC-4), not the second (EST, UTC-5) */

        var writer = new RecordingCollectorRowWriter();
        CpuUtilizationCollector.Instance.WritePayload(new CpuUtilizationCollector.Row(local, 42, 7, utc), writer, CollectorTestContext.Make(s_deltas));
        Assert.Equal(CpuUtilizationCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal(new object?[] { local, 42, 7, utc }, writer.Values);

        var nullTwin = new RecordingCollectorRowWriter();
        CpuUtilizationCollector.Instance.WritePayload(new CpuUtilizationCollector.Row(local, 42, 7, null), nullTwin, CollectorTestContext.Make(s_deltas));
        Assert.Equal(new object?[] { local, 42, 7, null }, nullTwin.Values);
    }
}
