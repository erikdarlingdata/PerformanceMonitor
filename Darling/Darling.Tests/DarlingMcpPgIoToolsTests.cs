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
/// Wire shape for <c>get_pg_io_stats</c>, asserted against the real projection (#3536).
///
/// <para>The subject is the timing contract the trend sibling already shipped and this read lacked:
/// <c>track_io_timing</c> is OFF by default in PostgreSQL, so on a stock server every time counter in the
/// store is zero — and <c>read_time_ms / reads</c> over that is 0.000 ms, a latency that reads as an
/// impossibly fast disk rather than an unmeasured one. The zero is a fact about the configuration; printed
/// as a measurement it is the most reassuring wrong number on the surface.</para>
/// </summary>
public class DarlingMcpPgIoToolsTests
{
    /// <summary>
    /// Two combinations the way the reader hands them over: ordered by read time then by read count, so
    /// over a store of zero times (timing off) the count IS the ordering. The busiest-by-reads row leads.
    /// </summary>
    private static List<DarlingPgIoReader.PgIoRow> Rows(bool timed) => new()
    {
        new DarlingPgIoReader.PgIoRow(
            BackendType: "client backend", ObjectType: "relation", Context: "normal",
            Reads: 5_000, ReadTimeMs: timed ? 2_500 : 0,
            Hits: 95_000, Extends: 10, ExtendTimeMs: timed ? 40 : 0,
            Evictions: 5, Reuses: 0,
            Writes: 200, WriteTimeMs: timed ? 90 : 0,
            OpBytes: 8_192, WriteCountersTracked: true, StatsReset: null,
            ReadBytes: 0, WriteBytes: 0, ExtendBytes: 0, ByteCountersTracked: false),
        new DarlingPgIoReader.PgIoRow(
            BackendType: "autovacuum worker", ObjectType: "relation", Context: "vacuum",
            Reads: 1_000, ReadTimeMs: timed ? 700 : 0,
            Hits: 3_000, Extends: 0, ExtendTimeMs: 0,
            Evictions: 0, Reuses: 40,
            Writes: 50, WriteTimeMs: timed ? 25 : 0,
            OpBytes: 8_192, WriteCountersTracked: true, StatsReset: null,
            ReadBytes: 0, WriteBytes: 0, ExtendBytes: 0, ByteCountersTracked: false),
    };

    /// <summary>
    /// The page the projection takes since #3541 A7: the rows plus the WINDOW's totals. Built here as the rows'
    /// own sums, which is the "page is the whole window" case every assertion in this file was written
    /// against; the shares-of-a-larger-window arithmetic is <see cref="DarlingMcpPgPercentDenominatorTests"/>'
    /// subject.
    /// </summary>
    private static DarlingPgIoReader.PgIoPage Page(List<DarlingPgIoReader.PgIoRow> rows) =>
        new(rows, rows.Sum(r => r.Reads), rows.Sum(r => r.ReadTimeMs));

    private static JsonElement Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    /// <summary>
    /// <b>The point of #3536.</b> With <c>track_io_timing</c> off — PostgreSQL's DEFAULT — every time field
    /// is null rather than the 0.0 the arithmetic produces, top to bottom: the per-row times, the per-read
    /// latency, the read-time shares, and the window total. The counters beside them survive untouched,
    /// because the operation counts are real measurements whatever the timing setting is.
    /// </summary>
    [Fact]
    public void TimingUntracked_NullsEveryTimeField_AndKeepsTheCounts()
    {
        var root = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: false)), 20, timingSetting: false));

        Assert.False(root.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("total_read_time_ms").ValueKind);

        var row = root.GetProperty("combinations")[0];
        Assert.Equal(JsonValueKind.Null, row.GetProperty("read_time_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("avg_read_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("write_time_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("extend_time_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("pct_of_total_read_time").ValueKind);

        /* The counts are measured regardless of the timing setting and must not be dragged down with it. */
        Assert.Equal(5_000, row.GetProperty("reads").GetInt64());
        Assert.Equal(95_000, row.GetProperty("hits").GetInt64());
        Assert.Equal(95.0, row.GetProperty("hit_pct").GetDouble());
        Assert.Equal(200, row.GetProperty("writes").GetInt64());

        var note = root.GetProperty("timing_note").GetString()!;
        Assert.Contains("off by DEFAULT", note, StringComparison.Ordinal);
        Assert.Contains("does not measure I/O time", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The busiest field keeps its key — the web tile reads it by name — and <c>busiest_basis</c> beside it
    /// says what the ranking actually used: read time when the server measures it, the read COUNT when it
    /// does not. The reader's ORDER BY carries the count as its second key, so over a store of zeros the
    /// count is the entire ordering rather than a tiebreak, and the payload has to say so.
    /// </summary>
    [Fact]
    public void TheBusiestBasis_IsReadsWhenUntracked_AndReadTimeWhenTracked()
    {
        var untracked = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: false)), 20, timingSetting: false));
        Assert.Equal("client backend/relation/normal", untracked.GetProperty("busiest_by_read_time").GetString());
        Assert.Contains("read count", untracked.GetProperty("busiest_basis").GetString(), StringComparison.Ordinal);
        Assert.Contains("does not measure I/O time", untracked.GetProperty("busiest_basis").GetString(), StringComparison.Ordinal);

        var tracked = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: true)), 20, timingSetting: true));
        Assert.Equal("client backend/relation/normal", tracked.GetProperty("busiest_by_read_time").GetString());
        Assert.Contains("read time", tracked.GetProperty("busiest_basis").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// With timing tracked the measured figures flow through unchanged — the untracked arm must not cost
    /// the measuring server anything: per-read latency is time over reads, the shares add up, and the
    /// window total is the sum of the rows.
    /// </summary>
    [Fact]
    public void TimingTracked_KeepsTheMeasuredFigures()
    {
        var root = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: true)), 20, timingSetting: true));

        Assert.True(root.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Equal(3_200.0, root.GetProperty("total_read_time_ms").GetDouble());
        Assert.Contains("pg_server_config", root.GetProperty("io_timing_source").GetString(), StringComparison.Ordinal);
        Assert.Contains("track_io_timing on", root.GetProperty("timing_note").GetString(), StringComparison.Ordinal);

        var row = root.GetProperty("combinations")[0];
        Assert.Equal(2_500.0, row.GetProperty("read_time_ms").GetDouble());
        Assert.Equal(0.5, row.GetProperty("avg_read_ms").GetDouble());
        Assert.Equal(78.1, row.GetProperty("pct_of_total_read_time").GetDouble());
        Assert.Equal(90.0, row.GetProperty("write_time_ms").GetDouble());
    }

    /// <summary>
    /// A store without the server's configuration answers from the only evidence left — whether any
    /// non-zero time appears in the window — and SAYS it is inferring, exactly as the trend sibling does.
    /// "We do not know" and "the server does not measure it" license different readings of a zero.
    /// </summary>
    [Fact]
    public void AnUncollectedSetting_IsInferredFromTheData_AndSaysSo()
    {
        var quiet = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: false)), 20, timingSetting: null));
        Assert.False(quiet.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Contains("inferred from the data", quiet.GetProperty("io_timing_source").GetString(), StringComparison.Ordinal);

        var timed = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: true)), 20, timingSetting: null));
        Assert.True(timed.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Contains("inferred from the data", timed.GetProperty("io_timing_source").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The setting wins over the observation in BOTH directions, mirroring the trend sibling: collected
    /// configuration is the authority, and inference is only for a store that never collected it.
    /// </summary>
    [Fact]
    public void TheCollectedSetting_OverridesTheObservation()
    {
        /* Setting says on, window happens to be all zeros: tracked, with honest zeros, not nulls. */
        var on = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: false)), 20, timingSetting: true));
        Assert.True(on.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Equal(0.0, on.GetProperty("combinations")[0].GetProperty("read_time_ms").GetDouble());

        /* Setting says off, stale non-zero times in the window: untracked wins and the times are null. */
        var off = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(Rows(timed: true)), 20, timingSetting: false));
        Assert.False(off.GetProperty("io_timing_tracked").GetBoolean());
        Assert.Equal(JsonValueKind.Null, off.GetProperty("combinations")[0].GetProperty("read_time_ms").ValueKind);
    }

    /// <summary>
    /// The Aurora contract is untouched by the timing one: write COUNTERS tracked/untracked is a different
    /// axis from time measured/unmeasured, and a timing-on Aurora still reports null writes. The two flags
    /// travel separately because their remedies are different — one is a platform fact, one is a setting.
    /// </summary>
    [Fact]
    public void AuroraNullWrites_SurviveTheTimingGate()
    {
        var aurora = Rows(timed: true)
            .Select(r => r with { Writes = 0, WriteTimeMs = 0, WriteCountersTracked = false })
            .ToList();

        var root = Parse(DarlingMcpPgIoTools.BuildIoJson("srv", 24, Page(aurora), 20, timingSetting: true));

        Assert.True(root.GetProperty("io_timing_tracked").GetBoolean());
        Assert.False(root.GetProperty("write_counters_tracked_anywhere").GetBoolean());
        Assert.Contains("Aurora", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        var row = root.GetProperty("combinations")[0];
        Assert.Equal(JsonValueKind.Null, row.GetProperty("writes").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("write_time_ms").ValueKind);
        /* The read side still reports: timing is on and reads are tracked everywhere. */
        Assert.Equal(2_500.0, row.GetProperty("read_time_ms").GetDouble());
    }
}
