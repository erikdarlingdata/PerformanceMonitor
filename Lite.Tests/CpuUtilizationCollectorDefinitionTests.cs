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
/// <c>sample_time</c> keeps <c>SYSDATETIME()</c>, and the writer emits it as the fourth payload value — and,
/// since #3778, the watermark's FRAME: the definition declares the twin as its <c>UtcWatermarkColumn</c>, the
/// host says which column it read the watermark from, and the ring-buffer dedup compares each row on that
/// same column. The three acceptance sequences #3778 named run through the REAL <c>ReadAsync</c> here: the
/// autumn fall-back hour landing under a UTC watermark, the upgrade day neither dropping nor duplicating
/// under the local one, and the Azure arm unchanged in either frame.
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

    /// <summary>The declared watermark column stays the server-local <c>sample_time</c> (what a store with no
    /// post-rung rows hands back, so the first post-upgrade run dedups exactly as before), the UTC twin is
    /// declared as the <c>UtcWatermarkColumn</c> the host prefers wherever the store has it (#3778), both are
    /// declared payload columns of timestamp type, and the twin is declared LAST, where the positional writers
    /// and an upgraded store's ALTER agree it sits.</summary>
    [Fact]
    public void WatermarkColumn_IsSampleTime_TheUtcWatermarkColumnIsTheTwin_AndNamesMatchDispatchAndSchema()
    {
        Assert.Equal("sample_time", CpuUtilizationCollector.Instance.WatermarkColumn);
        Assert.Equal("sample_time_utc", CpuUtilizationCollector.Instance.UtcWatermarkColumn);
        Assert.Null(CpuUtilizationCollector.Instance.PerDatabaseWatermarkColumn);
        foreach (var column in new[] { CpuUtilizationCollector.Instance.WatermarkColumn, CpuUtilizationCollector.Instance.UtcWatermarkColumn })
        {
            var declared = Assert.Single(CpuUtilizationCollector.Instance.PayloadColumns, c => c.Name == column);
            Assert.Equal(CollectorColumnType.Timestamp, declared.Type);
        }

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

    /// <summary>Under a LOCAL watermark — the host read it from <c>sample_time</c> because the store held no
    /// row with the twin, which is the first post-upgrade run and was every run before #3778 — the dedup keys
    /// on the LOCAL stamp, never on the twin. The fourth ordinal is the twin; it survives the dedup on the row
    /// it belongs to, and a NULL there (a shape no arm produces, guarded for the store's nullable column) reads
    /// as null rather than throwing.</summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_UnderALocalWatermark_DedupsAtOrBelowItOnTheLocalStamp()
    {
        var watermark = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var offset = TimeSpan.FromHours(-4);   /* the row's server sits at UTC-4: its UTC twin is four hours AHEAD of its local stamp */
        using var reader = new FakeCollectorDataReader(
            new object[] { watermark.AddSeconds(60), 50, 10, watermark.AddSeconds(60) - offset },
            new object[] { watermark, 40, 5, watermark - offset },
            new object[] { watermark.AddSeconds(-60), 30, 2, DBNull.Value });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermark, watermarkFromUtcColumn: false),
            CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new CpuUtilizationCollector.Row(watermark.AddSeconds(60), 50, 10, watermark.AddSeconds(60) - offset), row);

        /* The dedup keyed on the LOCAL stamp (the watermark's frame). Every twin here is four hours ABOVE the
           watermark, so a dedup that keyed on the twin under a local watermark would have kept all three. */
        Assert.Equal(watermark.AddSeconds(60), row.SampleTime);
        Assert.Equal(row.SampleTime - offset, row.SampleTimeUtc);
    }

    /// <summary>Under a UTC watermark — the host read it from <c>sample_time_utc</c>, every run after the first
    /// post-upgrade one — the dedup keys on the TWIN, never on the local stamp (#3778). The mirror of the pin
    /// above: the local stamps here are all four hours BELOW the watermark, so a dedup that still keyed on the
    /// local stamp would have dropped all three.</summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_UnderAUtcWatermark_DedupsAtOrBelowItOnTheTwin()
    {
        var watermarkUtc = new DateTime(2026, 7, 1, 16, 0, 0, DateTimeKind.Utc);
        var offset = TimeSpan.FromHours(-4);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermarkUtc.AddSeconds(60) + offset, 50, 10, watermarkUtc.AddSeconds(60) },
            new object[] { watermarkUtc + offset, 40, 5, watermarkUtc },
            new object[] { watermarkUtc.AddSeconds(-60) + offset, 30, 2, watermarkUtc.AddSeconds(-60) });

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermarkUtc, watermarkFromUtcColumn: true),
            CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(new CpuUtilizationCollector.Row(watermarkUtc.AddSeconds(60) + offset, 50, 10, watermarkUtc.AddSeconds(60)), row);
        Assert.True(row.SampleTime < watermarkUtc, "the fixture must put every LOCAL stamp below the UTC watermark, or this pin cannot tell the two frames apart");
    }

    /* ── #3778: the three acceptance sequences, through the REAL ReadAsync dedup rather than WritePayload ── */

    /// <summary>
    /// <b>(1) The autumn fall-back hour lands.</b> America/New_York, 2026-11-01: at 06:00 UTC the local clock
    /// goes from 01:59:59 EDT back to 01:00:00 EST and repeats the hour. The last sample stored before the
    /// transition was 05:59 UTC = 01:59 EDT. The next poll's ring buffer holds the sixty repeated-hour samples
    /// (06:00–06:59 UTC = 01:00–01:59 EST) plus the two already stored (05:59 and 05:58 UTC) — newest first,
    /// as the query orders them. EVERY repeated-hour sample carries a local stamp at or below 01:59, the
    /// pre-transition maximum.
    ///
    /// <para>Under a UTC watermark (the host read <c>MAX(sample_time_utc)</c> = 05:59), all sixty land and the
    /// two already stored are dropped: UTC never repeats an hour. The CONTROL is the same reader under the
    /// LOCAL watermark (01:59): zero rows — the pre-#3778 behaviour, and still the behaviour of the one run
    /// that is on the local rule because the store holds no twin yet (the class remarks say so under "what
    /// nobody promised"). The control is pinned rather than merely described because it is what proves the
    /// frame, not the value, is the mechanism.</para>
    /// </summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_FallBackHour_LandsEverySampleUnderAUtcWatermark_AndNoneUnderTheLocalControl()
    {
        var transitionUtc = new DateTime(2026, 11, 1, 6, 0, 0, DateTimeKind.Utc);
        var edt = TimeSpan.FromHours(-4);
        var est = TimeSpan.FromHours(-5);

        var lastStoredUtc = transitionUtc.AddMinutes(-1);          /* 05:59 UTC */
        var lastStoredLocal = lastStoredUtc + edt;                 /* 01:59 EDT */
        Assert.Equal(new DateTime(2026, 11, 1, 1, 59, 0), lastStoredLocal);

        static object[][] RingBufferAfterTheTransition(DateTime transitionUtc, TimeSpan edt, TimeSpan est)
        {
            var rows = new System.Collections.Generic.List<object[]>();
            for (var minute = 59; minute >= 0; minute--)      /* 06:59 down to 06:00 UTC: the repeated hour, EST side */
            {
                var utc = transitionUtc.AddMinutes(minute);
                rows.Add(new object[] { utc + est, 40 + (minute % 7), 3, utc });
            }

            rows.Add(new object[] { transitionUtc.AddMinutes(-1) + edt, 33, 2, transitionUtc.AddMinutes(-1) });   /* 05:59 UTC = 01:59 EDT, stored */
            rows.Add(new object[] { transitionUtc.AddMinutes(-2) + edt, 32, 2, transitionUtc.AddMinutes(-2) });   /* 05:58 UTC = 01:58 EDT, stored */
            return rows.ToArray();
        }

        /* Every repeated-hour local stamp is at or below the last stored local stamp: that is the fall-back. */
        var ring = RingBufferAfterTheTransition(transitionUtc, edt, est);
        Assert.Equal(62, ring.Length);
        Assert.All(ring, r => Assert.True((DateTime)r[0] <= lastStoredLocal));

        /* The fix: a UTC watermark. */
        using (var reader = new FakeCollectorDataReader(ring))
        {
            var landed = await CpuUtilizationCollector.Instance.ReadAsync(
                reader,
                CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: lastStoredUtc, watermarkFromUtcColumn: true),
                CancellationToken.None);

            Assert.Equal(60, landed.Count);
            Assert.Equal(
                Enumerable.Range(0, 60).Select(m => transitionUtc.AddMinutes(59 - m)).ToArray(),
                landed.Select(r => r.SampleTimeUtc!.Value).ToArray());
            Assert.All(landed, r => Assert.Equal(r.SampleTimeUtc!.Value + est, r.SampleTime));
            Assert.All(landed, r => Assert.True(r.SampleTime <= lastStoredLocal, "a landed row's LOCAL stamp is at or below the pre-transition maximum — that is the whole point"));
            Assert.DoesNotContain(landed, r => r.SampleTimeUtc <= lastStoredUtc);
        }

        /* The control: the LOCAL watermark drops the whole hour. */
        using (var reader = new FakeCollectorDataReader(ring))
        {
            var landed = await CpuUtilizationCollector.Instance.ReadAsync(
                reader,
                CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: lastStoredLocal, watermarkFromUtcColumn: false),
                CancellationToken.None);

            Assert.Empty(landed);
        }
    }

    /// <summary>
    /// <b>(2) The upgrade day neither drops nor duplicates.</b> A store the morning of the upgrade: eleven
    /// pre-rung rows, local stamps 12:00–12:10 at UTC−4, twin NULL. Each run below reads the store the way both
    /// hosts do (<c>MAX(sample_time_utc)</c> preferred, <c>MAX(sample_time)</c> otherwise, the frame stated —
    /// the real reads are pinned against a real DuckDB and a real PostgreSQL in the two
    /// <c>TimeHonestyRungTests</c>), runs the REAL dedup over a ring buffer that re-reads its whole history
    /// newest-first, and stores what landed.
    ///
    /// <para>Run 1 finds no twin, gets the LOCAL watermark 12:10 and dedups local-to-local: 12:11 and 12:12
    /// land, nothing at or below 12:10 re-lands. Run 2 finds the twins run 1 stored, gets the UTC watermark
    /// 16:12 and dedups UTC-to-UTC: 12:13 and 12:14 land. Across the sequence every sample after the pre-rung
    /// maximum landed exactly once and no pre-rung sample landed again — the bound the issue set was "at
    /// most one"; the stated frame's is zero, and that zero is what this pins.</para>
    /// </summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_UpgradeDay_NeitherDropsNorDuplicates_AndTheSecondRunIsOnUtc()
    {
        var offset = TimeSpan.FromHours(-4);
        var noonUtc = new DateTime(2026, 7, 1, 16, 0, 0, DateTimeKind.Utc);   /* 12:00 local */

        /* The store, as (local, twin) pairs. Pre-rung: local only. */
        var store = Enumerable.Range(0, 11)
            .Select(m => (Local: noonUtc.AddMinutes(m) + offset, Utc: (DateTime?)null))
            .ToList();

        /* What both hosts' pair read returns for this store: the twin's maximum when any row has one, else the
           local maximum, with the frame. */
        static (DateTime? Value, bool FromUtcColumn) HostWatermarkRead(System.Collections.Generic.List<(DateTime Local, DateTime? Utc)> store)
        {
            var twins = store.Where(r => r.Utc.HasValue).Select(r => r.Utc!.Value).ToList();
            if (twins.Count > 0) return (twins.Max(), true);
            return store.Count > 0 ? (store.Max(r => r.Local), false) : (null, false);
        }

        /* The ring buffer at a poll: every sample from 12:00 up to `newestMinute`, newest first, both columns. */
        static object[][] RingBufferAt(DateTime noonUtc, TimeSpan offset, int newestMinute) =>
            Enumerable.Range(0, newestMinute + 1).Reverse()
                .Select(m => new object[] { noonUtc.AddMinutes(m) + offset, 50, 5, noonUtc.AddMinutes(m) })
                .ToArray();

        async Task<System.Collections.Generic.List<CpuUtilizationCollector.Row>> RunAsync(int newestMinute)
        {
            var (watermark, fromUtc) = HostWatermarkRead(store);
            using var reader = new FakeCollectorDataReader(RingBufferAt(noonUtc, offset, newestMinute));
            var landed = await CpuUtilizationCollector.Instance.ReadAsync(
                reader,
                CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermark, watermarkFromUtcColumn: fromUtc),
                CancellationToken.None);
            store.AddRange(landed.Select(r => (r.SampleTime, r.SampleTimeUtc)));
            return landed;
        }

        /* Run 1: the first post-upgrade run, on the LOCAL rule. */
        Assert.Equal(((DateTime?)(noonUtc.AddMinutes(10) + offset), false), HostWatermarkRead(store));
        var first = await RunAsync(newestMinute: 12);
        Assert.Equal(new[] { 12, 11 }, first.Select(r => r.SampleTime.Minute).ToArray());

        /* Run 2: the store now holds twins, so the read is UTC and so is the comparison. */
        Assert.Equal(((DateTime?)noonUtc.AddMinutes(12), true), HostWatermarkRead(store));
        var second = await RunAsync(newestMinute: 14);
        Assert.Equal(new[] { 14, 13 }, second.Select(r => r.SampleTime.Minute).ToArray());

        /* Across the sequence: no duplicate, no hole. Fifteen distinct samples, 12:00 through 12:14, each once. */
        Assert.Equal(15, store.Count);
        Assert.Equal(15, store.Select(r => r.Local).Distinct().Count());
        Assert.Equal(Enumerable.Range(0, 15).Select(m => noonUtc.AddMinutes(m) + offset), store.Select(r => r.Local).OrderBy(l => l));
        Assert.Equal(11, store.Count(r => r.Utc is null));      /* the pre-rung rows, untouched */
        Assert.Equal(4, store.Count(r => r.Utc is not null));   /* the four that landed, each with its twin */
    }

    /// <summary>
    /// <b>(3) The Azure arm is unchanged.</b> Azure filters server-side — <c>@last_sample_time</c> against
    /// <c>drs.end_time</c>, which is UTC and is the value of BOTH stored columns there — so the frame flag must
    /// move nothing: the same query text, the same single parameter (name, value, type) in either frame, and
    /// no client-side dedup in either. Pinned by comparing the two plans field by field rather than each to a
    /// literal, so the claim is "identical", not "both plausible".
    /// </summary>
    [Fact]
    public async Task Azure_WatermarkedQueryAndBinding_AreIdenticalInEitherFrame_AndNeverDedupClientSide()
    {
        var watermark = new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Utc);
        var local = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark, watermarkFromUtcColumn: false));
        var utc = CpuUtilizationCollector.Instance.BuildQuery(
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark, watermarkFromUtcColumn: true));

        Assert.Equal(local.Text, utc.Text);
        Assert.Contains("WHERE drs.end_time > @last_sample_time", utc.Text, StringComparison.Ordinal);
        var localParameter = Assert.Single(local.Parameters);
        var utcParameter = Assert.Single(utc.Parameters);
        Assert.Equal(localParameter.Name, utcParameter.Name);
        Assert.Equal(localParameter.Value, utcParameter.Value);
        Assert.Equal(localParameter.Type, utcParameter.Type);
        Assert.Equal(watermark, utcParameter.Value);

        /* And the unwatermarked Azure arm does not consult the flag either. */
        Assert.Equal(
            CpuUtilizationCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text,
            CpuUtilizationCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermarkFromUtcColumn: true)).Text);

        /* A row AT the watermark, under a UTC-frame watermark, on Azure: kept, because Azure never dedups
           client-side — the server already filtered. Same as the local-frame pin below it. */
        foreach (var fromUtc in new[] { false, true })
        {
            using var reader = new FakeCollectorDataReader(new object[] { watermark, 40, 0, watermark });
            var rows = await CpuUtilizationCollector.Instance.ReadAsync(
                reader,
                CollectorTestContext.Make(s_deltas, isAzureSqlDb: true, watermark: watermark, watermarkFromUtcColumn: fromUtc),
                CancellationToken.None);
            Assert.Single(rows);
        }
    }

    /// <summary>
    /// The never-path, stated: under a UTC watermark a row whose twin is NULL cannot be placed in the
    /// watermark's frame, and it is KEPT — not compared across frames on its local stamp, not dropped on a
    /// guess. No arm produces such a row (<c>BuildQuery_RingBuffer_DerivesTheUtcTwin_ByTheSameArithmeticOffSysUtcDateTime</c>
    /// and <c>BuildQuery_Azure_ProjectsEndTimeAsTheUtcTwin_Last</c> pin the projections); if one ever did, a
    /// visible duplicate is the failure that gets noticed, and a silent hole is the one this issue is about.
    /// </summary>
    [Fact]
    public async Task ReadAsync_RingBuffer_UnderAUtcWatermark_KeepsARowWithNoTwin_RatherThanComparingAcrossFrames()
    {
        var watermarkUtc = new DateTime(2026, 7, 1, 16, 0, 0, DateTimeKind.Utc);
        using var reader = new FakeCollectorDataReader(
            new object[] { watermarkUtc.AddHours(-4).AddMinutes(-30), 30, 2, DBNull.Value });   /* local stamp far below the watermark; no twin */

        var rows = await CpuUtilizationCollector.Instance.ReadAsync(
            reader,
            CollectorTestContext.Make(s_deltas, isAzureSqlDb: false, watermark: watermarkUtc, watermarkFromUtcColumn: true),
            CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.SampleTimeUtc);
    }

    /// <summary>
    /// The generic mechanism's census (#3778): <c>cpu_utilization</c> is the ONLY definition that declares a
    /// <c>UtcWatermarkColumn</c>, so every other definition's watermark read is the byte-identical SQL it was
    /// (both hosts branch on the declaration, and the two <c>TimeHonestyRungTests</c> pin both branches' SQL);
    /// and no definition declares a UTC twin TOGETHER with a <c>PerDatabaseWatermarkColumn</c>, because the
    /// per-database and per-item refreshes read the declared column alone and would leave the frame flag stale
    /// — <c>CollectorContext.WatermarkFromUtcColumn</c>'s remarks state that boundary; this is what holds it.
    /// <c>CollectorCatalog.All</c> is typed as the non-generic schema surface, so the two members are reached by
    /// reflection, the way <c>ServerWatermarkDispatchGateTests</c> reaches its watermark columns.
    /// </summary>
    [Fact]
    public void OnlyCpuUtilization_DeclaresAUtcWatermarkColumn_AndNoneDeclaresItBesideAPerDatabaseWatermark()
    {
        static string? Column(ICollectorSchemaInfo definition, string property) =>
            (string?)definition.GetType().GetProperty(property)?.GetValue(definition);

        var declaring = CollectorCatalog.All.Where(d => Column(d, "UtcWatermarkColumn") is not null).ToList();
        var only = Assert.Single(declaring);
        Assert.Same(CpuUtilizationCollector.Instance, only);

        foreach (var definition in CollectorCatalog.All)
        {
            Assert.NotNull(definition.GetType().GetProperty("UtcWatermarkColumn"));   /* the member exists on every definition, null or not */
            if (Column(definition, "UtcWatermarkColumn") is not null)
            {
                Assert.NotNull(Column(definition, "WatermarkColumn"));
                Assert.Null(Column(definition, "PerDatabaseWatermarkColumn"));
            }
        }
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
