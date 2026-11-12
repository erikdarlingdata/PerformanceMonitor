using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7464: an amplifier or a relationship edge that corroborates on a CONTINUOUSLY scored peer (a wait fraction, a
/// latency, CPU %, TEMPDB_USAGE) must see the peer at a level that could root a story, not at any trace. The
/// scorer grades a trace of such a peer as a tiny positive base, so "BaseSeverity > 0" matched 15 ms of LCK_M_S and
/// 2.75 MB of tempdb on an idle server, and the boost lifted a 0.3 settings fact past the 0.5 root bar.
///
/// Every row of each table has the same two halves: the figures from the issue's idle server (a trace) must NOT
/// match, and a real level of the same peer MUST still match.
/// </summary>
public class AmplifierRootBarTests
{
    /* ── fact builders (Source decides which scoring arm grades the fact) ── */

    private static Fact Wait(string key, double fraction) =>
        new() { Source = "waits", Key = key, Value = fraction, Metadata = new() };

    private static Fact Io(string key, double ms) =>
        new() { Source = "io", Key = key, Value = ms, Metadata = new() };

    private static Fact Cpu(string key, double percent) =>
        new() { Source = "cpu", Key = key, Value = percent, Metadata = new() };

    private static Fact TempDb(double fraction, double maxReservedMb) =>
        new()
        {
            Source = "tempdb",
            Key = "TEMPDB_USAGE",
            Value = fraction,
            Metadata = new() { ["max_reserved_mb"] = maxReservedMb }
        };

    private static Fact Blocking(string key, double perHour) =>
        new() { Source = "blocking", Key = key, Value = perHour, Metadata = new() };

    private static Fact Query(string key, double value) =>
        new() { Source = "queries", Key = key, Value = value, Metadata = new() };

    private static Fact Memory(string key, double value) =>
        new() { Source = "memory", Key = key, Value = value, Metadata = new() };

    private static Fact MemoryAnomaly() =>
        new()
        {
            Source = "anomaly",
            Key = "ANOMALY_MEMORY_PRESSURE",
            Value = 95,
            Metadata = new() { ["deviation_sigma"] = 3.0 }
        };

    private static Fact DbConfig(double rcsiOff, double autoShrink) =>
        new()
        {
            Source = "database_config",
            Key = "DB_CONFIG",
            Value = 1,
            Metadata = new() { ["rcsi_off_count"] = rcsiOff, ["auto_shrink_on_count"] = autoShrink }
        };

    private static Fact[] Scored(params Fact[] facts)
    {
        new FactScorer().ScoreAll(facts.ToList());
        return facts;
    }

    private static Fact Root(Fact[] scored) => scored[0];

    private static bool Matched(Fact root, string descriptionStart) =>
        root.AmplifierResults.Single(a => a.Description.StartsWith(descriptionStart, StringComparison.Ordinal)).Matched;

    /* ── Layer 2: amplifiers ── */

    private sealed record AmplifierCase(
        Func<Fact> Root, string Amplifier, Func<Fact[]> Trace, Func<Fact[]> Real);

    // The trace figures are the issue's idle-server read: IO_READ 4.85 ms, LCK_M_S 15 ms of waiting (value 1E-06),
    // TEMPDB_USAGE 0.000673 with 2.75 MB reserved. Real levels sit past each peer's own concerning bar.
    private static readonly Dictionary<string, AmplifierCase> Amplifiers = new()
    {
        ["auto_shrink_io"] = new(
            () => DbConfig(rcsiOff: 0, autoShrink: 1),
            "I/O latency elevated",
            () => [Io("IO_READ_LATENCY_MS", 4.85)],
            () => [Io("IO_READ_LATENCY_MS", 25)]),
        ["rcsi_lck_m_s"] = new(
            () => DbConfig(rcsiOff: 1, autoShrink: 0),
            "LCK_M_S waits",
            () => [Wait("LCK_M_S", 1E-06)],
            () => [Wait("LCK_M_S", 0.10)]),
        ["rcsi_lck_m_is"] = new(
            () => DbConfig(rcsiOff: 1, autoShrink: 0),
            "LCK_M_IS waits",
            () => [Wait("LCK_M_IS", 1E-06)],
            () => [Wait("LCK_M_IS", 0.10)]),
        ["rcsi_deadlocks_with_reader_waits"] = new(
            () => DbConfig(rcsiOff: 1, autoShrink: 0),
            "Deadlocks with reader/writer lock waits",
            () => [Blocking("DEADLOCKS", 8), Wait("LCK_M_S", 1E-06)],
            () => [Blocking("DEADLOCKS", 8), Wait("LCK_M_S", 0.10)]),
        ["deadlocks_reader_writer_waits"] = new(
            () => Blocking("DEADLOCKS", 8),
            "Reader/writer lock waits present",
            () => [Wait("LCK_M_S", 1E-06), Wait("LCK_M_IS", 1E-06)],
            () => [Wait("LCK_M_S", 0.10)]),
        ["latch_tempdb"] = new(
            () => Wait("LATCH_EX", 0.30),
            "TempDB usage elevated: latch contention",
            () => [TempDb(0.000673, 2.75)],
            () => [TempDb(0.80, 4000)]),
        ["disk_space_tempdb"] = new(
            () => new Fact { Source = "disk", Key = "DISK_SPACE", Value = 0.07, Metadata = new() },
            "TempDB usage elevated: growing TempDB",
            () => [TempDb(0.000673, 2.75)],
            () => [TempDb(0.80, 4000)]),
        ["plan_regression_cpu_percent"] = new(
            () => Query("PLAN_REGRESSION", 4),
            "SQL Server CPU elevated",
            () => [Cpu("CPU_SQL_PERCENT", 1)],
            () => [Cpu("CPU_SQL_PERCENT", 80)]),
        ["plan_regression_cpu_spike"] = new(
            () => Query("PLAN_REGRESSION", 4),
            "CPU spike present",
            // The trace half is defensive: neither collector emits CPU_SPIKE below max CPU 80, so no live run emits this fact at this level.
            () => [Cpu("CPU_SPIKE", 10)],
            () => [Cpu("CPU_SPIKE", 90)]),
        ["cpu_spike_sos"] = new(
            () => Cpu("CPU_SPIKE", 90),
            "SOS_SCHEDULER_YIELD elevated: scheduler pressure during CPU spike",
            () => [Wait("SOS_SCHEDULER_YIELD", 0.01)],
            () => [Wait("SOS_SCHEDULER_YIELD", 0.40)]),
        ["memory_grant_semaphore"] = new(
            () => Memory("MEMORY_GRANT_PENDING", 3),
            "RESOURCE_SEMAPHORE waits present: memory grant pressure",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)],
            () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
        ["memory_anomaly_semaphore"] = new(
            MemoryAnomaly,
            "RESOURCE_SEMAPHORE waits present: grant pressure visible in wait stats",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)],
            () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
        ["spills_semaphore"] = new(
            () => Query("QUERY_SPILLS", 300),
            "RESOURCE_SEMAPHORE waits: grant pressure",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)],
            () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
        ["compile_semaphore"] = new(
            () => Wait("RESOURCE_SEMAPHORE_QUERY_COMPILE", 0.05),
            "RESOURCE_SEMAPHORE also present",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)],
            () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
    };

    public static IEnumerable<object[]> AmplifierNames() => Amplifiers.Keys.Select(k => new object[] { k });

    [Theory]
    [MemberData(nameof(AmplifierNames))]
    public void Amplifier_DoesNotMatch_OnTheIdleServerTrace(string name)
    {
        var c = Amplifiers[name];

        var scored = Scored([c.Root(), .. c.Trace()]);

        Assert.False(Matched(Root(scored), c.Amplifier),
            $"{name}: '{c.Amplifier}' matched on trace values {string.Join(", ", scored.Skip(1).Select(f => $"{f.Key}={f.Value}"))}");
    }

    [Theory]
    [MemberData(nameof(AmplifierNames))]
    public void Amplifier_StillMatches_AtARealLevel(string name)
    {
        var c = Amplifiers[name];

        var scored = Scored([c.Root(), .. c.Real()]);

        Assert.True(Matched(Root(scored), c.Amplifier),
            $"{name}: '{c.Amplifier}' did not match at real values {string.Join(", ", scored.Skip(1).Select(f => $"{f.Key}={f.Value}"))}");
    }

    [Fact]
    public void AutoShrinkAmplifier_NeedsAutoShrinkOn_EvenWithRealReadLatency()
    {
        // The amplifier's own text is about auto_shrink: a slow disk says nothing about a database that never shrinks.
        var scored = Scored(DbConfig(rcsiOff: 1, autoShrink: 0), Io("IO_READ_LATENCY_MS", 40));

        Assert.False(Matched(Root(scored), "I/O latency elevated"));
    }

    [Fact]
    public void RcsiOffSettingsFact_OnAnIdleServer_StaysUnderTheRootBar()
    {
        // The issue's read: DB_CONFIG base 0.3 (RCSI off) was 0.54 after amplifiers on 15 ms of lock waits, a 4.85 ms
        // read and 2.75 MB of tempdb.
        var scored = Scored(
            DbConfig(rcsiOff: 1, autoShrink: 0),
            Io("IO_READ_LATENCY_MS", 4.85),
            Wait("LCK_M_S", 1E-06),
            Wait("LATCH_EX", 0.0001),
            Wait("LATCH_SH", 0.0001),
            TempDb(0.000673, 2.75));

        var config = Root(scored);
        Assert.Equal(0.3, config.BaseSeverity, 3);
        Assert.True(config.Severity < 0.5, $"DB_CONFIG reached {config.Severity} on trace values");
        Assert.All(config.AmplifierResults, a => Assert.False(a.Matched, a.Description));
    }

    /* ── relationship edges ── */

    private sealed record EdgeCase(string Source, string Destination, Func<Fact[]> Trace, Func<Fact[]> Real);

    private static readonly Dictionary<string, EdgeCase> Edges = new()
    {
        ["spike_to_sos"] = new("CPU_SPIKE", "SOS_SCHEDULER_YIELD",
            () => [Wait("SOS_SCHEDULER_YIELD", 0.01)], () => [Wait("SOS_SCHEDULER_YIELD", 0.40)]),
        ["pageiolatch_to_semaphore"] = new("PAGEIOLATCH_SH", "RESOURCE_SEMAPHORE",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)], () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
        ["grant_pending_to_semaphore"] = new("MEMORY_GRANT_PENDING", "RESOURCE_SEMAPHORE",
            () => [Wait("RESOURCE_SEMAPHORE", 0.0001)], () => [Wait("RESOURCE_SEMAPHORE", 0.02)]),
        ["pageiolatch_to_read_latency"] = new("PAGEIOLATCH_SH", "IO_READ_LATENCY_MS",
            () => [Io("IO_READ_LATENCY_MS", 4.85)], () => [Io("IO_READ_LATENCY_MS", 25)]),
        ["sos_to_compile_semaphore"] = new("SOS_SCHEDULER_YIELD", "RESOURCE_SEMAPHORE_QUERY_COMPILE",
            () => [Wait("RESOURCE_SEMAPHORE_QUERY_COMPILE", 0.0001)], () => [Wait("RESOURCE_SEMAPHORE_QUERY_COMPILE", 0.05)]),
        ["deadlocks_to_lck_m_s"] = new("DEADLOCKS", "LCK_M_S",
            () => [Wait("LCK_M_S", 1E-06)], () => [Wait("LCK_M_S", 0.10)]),
        ["db_config_to_lck_m_s"] = new("DB_CONFIG", "LCK_M_S",
            () => [DbConfig(rcsiOff: 1, autoShrink: 0), Wait("LCK_M_S", 1E-06)],
            () => [DbConfig(rcsiOff: 1, autoShrink: 0), Wait("LCK_M_S", 0.10)]),
        ["io_write_to_writelog"] = new("IO_WRITE_LATENCY_MS", "WRITELOG",
            () => [Wait("WRITELOG", 0.001)], () => [Wait("WRITELOG", 0.10)]),
        ["writelog_to_write_latency"] = new("WRITELOG", "IO_WRITE_LATENCY_MS",
            () => [Io("IO_WRITE_LATENCY_MS", 1)], () => [Io("IO_WRITE_LATENCY_MS", 25)]),
        ["latch_to_tempdb"] = new("LATCH_EX", "TEMPDB_USAGE",
            () => [TempDb(0.000673, 2.75)], () => [TempDb(0.80, 4000)]),
        ["spills_to_tempdb"] = new("QUERY_SPILLS", "TEMPDB_USAGE",
            () => [TempDb(0.000673, 2.75)], () => [TempDb(0.80, 4000)]),
        ["regression_to_cpu_percent"] = new("PLAN_REGRESSION", "CPU_SQL_PERCENT",
            () => [Cpu("CPU_SQL_PERCENT", 1)], () => [Cpu("CPU_SQL_PERCENT", 80)]),
        // The trace half is defensive: neither collector emits CPU_SPIKE below max CPU 80, so no live run emits this fact at this level.
        ["regression_to_cpu_spike"] = new("PLAN_REGRESSION", "CPU_SPIKE",
            () => [Cpu("CPU_SPIKE", 10)], () => [Cpu("CPU_SPIKE", 90)]),
    };

    public static IEnumerable<object[]> EdgeNames() => Edges.Keys.Select(k => new object[] { k });

    private static bool EdgeActive(EdgeCase c, Fact[] peers)
    {
        var scored = Scored(peers);
        var lookup = scored.ToFactLookup();
        return new RelationshipGraph().GetActiveEdges(c.Source, lookup).Any(e => e.Destination == c.Destination);
    }

    [Theory]
    [MemberData(nameof(EdgeNames))]
    public void Edge_IsNotDrawn_ToATraceOfItsDestination(string name)
    {
        var c = Edges[name];

        Assert.False(EdgeActive(c, c.Trace()), $"{name}: {c.Source} -> {c.Destination} drawn on a trace");
    }

    [Theory]
    [MemberData(nameof(EdgeNames))]
    public void Edge_IsStillDrawn_ToARealLevelOfItsDestination(string name)
    {
        var c = Edges[name];

        Assert.True(EdgeActive(c, c.Real()), $"{name}: {c.Source} -> {c.Destination} not drawn at a real level");
    }

    /* ── the story the issue reported, end to end ── */

    private static List<AnalysisStory> Stories(params Fact[] facts)
    {
        var scored = Scored(facts);
        return new InferenceEngine(new RelationshipGraph()).BuildStories(scored.ToList());
    }

    [Fact]
    public void IdleServer_RcsiOffSettingsFact_IsNotDrawnToLockWaits()
    {
        var stories = Stories(
            DbConfig(rcsiOff: 1, autoShrink: 0),
            Io("IO_READ_LATENCY_MS", 4.85),
            Wait("LCK_M_S", 1E-06),
            TempDb(0.000673, 2.75));

        var story = Assert.Single(stories);
        Assert.Equal("DB_CONFIG", story.RootFactKey);
        Assert.Equal(["DB_CONFIG"], story.Path);
        Assert.True(story.Severity < 0.5, $"root severity {story.Severity}");
    }

    [Fact]
    public void RealReaderWriterBlocking_StillRootsAtLockWaits_AndNamesTheRcsiSetting()
    {
        var stories = Stories(
            DbConfig(rcsiOff: 1, autoShrink: 0),
            Wait("LCK_M_S", 0.10));

        var story = Assert.Single(stories);
        Assert.Equal("LCK_M_S", story.RootFactKey);
        Assert.Equal(["LCK_M_S", "DB_CONFIG"], story.Path);
    }
}
