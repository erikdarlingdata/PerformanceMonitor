using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9340: a thread-exhaustion story is labelled "parallelism and blocking" only when the parallelism fact is notable on
/// its own (it clears the bar that roots a story, <see cref="FactScorer.ClearsRootBar"/>), not when it scored a trace above
/// zero. Each case runs the real scorer and engine over hand-built facts. The blocking half keeps its behaviour: any
/// positive blocking fact still counts.
/// </summary>
public class ThreadpoolFindingClassificationTests
{
    private static Fact Threadpool() =>
        new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,
            Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } };

    private static Fact Cx(double fraction) =>
        new() { Source = "waits", Key = "CXPACKET", Value = fraction, Metadata = new() };

    private static Fact BlockingEvents() =>
        new() { Source = "blocking", Key = "BLOCKING_EVENTS", Value = 30, Metadata = new() };

    private static string RootKeyOf(params Fact[] facts)
    {
        var list = facts.ToList();
        new FactScorer().ScoreAll(list);
        var stories = new InferenceEngine(new RelationshipGraph()).BuildStories(list);
        return stories.Single(s => s.RootFactKey.StartsWith("THREADPOOL", StringComparison.Ordinal)).RootFactKey;
    }

    [Fact]
    public void TraceOfParallelism_WithBlocking_IsBlockingDriven_NotMixed()
    {
        var cx = Cx(0.0005);
        var key = RootKeyOf(Threadpool(), cx, BlockingEvents());

        /* The trace is above zero but far under the root bar: the fixture is only a test of the fix if both hold. */
        Assert.InRange(cx.BaseSeverity, 0.0001, 0.4999);
        Assert.Equal("THREADPOOL_BLOCKING", key);
    }

    [Fact]
    public void TraceOfParallelism_WithoutBlocking_IsUnattributed()
        => Assert.Equal("THREADPOOL", RootKeyOf(Threadpool(), Cx(0.0005)));

    [Fact]
    public void NotableParallelism_WithBlocking_IsMixed()
    {
        var cx = Cx(0.8);
        var key = RootKeyOf(Threadpool(), cx, BlockingEvents());

        Assert.True(FactScorer.ClearsRootBar(cx));
        Assert.Equal("THREADPOOL_MIXED", key);
    }

    [Fact]
    public void NotableParallelism_WithoutBlocking_IsParallelDriven()
        => Assert.Equal("THREADPOOL_PARALLEL", RootKeyOf(Threadpool(), Cx(0.8)));
}
