using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests the InferenceEngine and RelationshipGraph against seeded scenarios.
/// Validates that stories are built with correct paths and severity ordering.
/// </summary>
public class InferenceEngineTests
{
    /* ── Unit tests: graph edge evaluation ── */

    [Fact]
    public void Graph_NoEdgesForUnknownFact()
    {
        var graph = new RelationshipGraph();
        var facts = new Dictionary<string, Fact>();

        var edges = graph.GetActiveEdges("UNKNOWN_THING", facts);
        Assert.Empty(edges);
    }

    [Fact]
    public void Graph_CxPacketEdgeFires_WhenSosIsHigh()
    {
        var graph = new RelationshipGraph();
        var facts = new Dictionary<string, Fact>
        {
            ["SOS_SCHEDULER_YIELD"] = new() { Key = "SOS_SCHEDULER_YIELD", Value = 0.50, Severity = 0.67 }
        };

        var edges = graph.GetActiveEdges("CXPACKET", facts);
        Assert.Contains(edges, e => e.Destination == "SOS_SCHEDULER_YIELD");
    }

    [Fact]
    public void Graph_CxPacketEdgeDoesNotFire_WhenSosIsLow()
    {
        var graph = new RelationshipGraph();
        var facts = new Dictionary<string, Fact>
        {
            ["SOS_SCHEDULER_YIELD"] = new() { Key = "SOS_SCHEDULER_YIELD", Value = 0.10, Severity = 0.13 }
        };

        var edges = graph.GetActiveEdges("CXPACKET", facts);
        Assert.DoesNotContain(edges, e => e.Destination == "SOS_SCHEDULER_YIELD");
    }

    // WS3: a config-advisory fact (DB_CONFIG/SERVER_CONFIG) roots a standalone recommendation
    // at its base severity (e.g. RCSI-off = 0.3), below the 0.5 incident threshold — so a
    // standing misconfig surfaces on a quiet, healthy server. An incident fact at the same
    // severity does NOT root.
    [Fact]
    public void ConfigFact_RootsStandalone_BelowMinimumSeverity()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "DB_CONFIG", Source = "config", Value = 1, Severity = 0.3,
                    Metadata = new Dictionary<string, double> { ["rcsi_off_count"] = 9 } }
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == "DB_CONFIG");
    }

    [Fact]
    public void IncidentFact_BelowMinimumSeverity_DoesNotRoot()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "CPU_SQL_PERCENT", Source = "cpu", Value = 60, Severity = 0.3 }
        };

        var stories = engine.BuildStories(facts);

        Assert.DoesNotContain(stories, s => s.RootFactKey == "CPU_SQL_PERCENT");
    }

    // WS3: a FILE_AUTOGROWTH_PERCENT fact at its 0.3 advisory base roots a standalone
    // recommendation, below the 0.5 incident threshold — because it is a config-advisory root
    // key. Mirrors ConfigFact_RootsStandalone_BelowMinimumSeverity for the new key.
    [Fact]
    public void FileAutogrowthPercentFact_RootsStandalone_BelowMinimumSeverity()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "FILE_AUTOGROWTH_PERCENT", Source = "config", Value = 2, Severity = 0.3,
                    Metadata = new Dictionary<string, double> { ["file_count"] = 2 } }
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == "FILE_AUTOGROWTH_PERCENT");
    }

    // WS3: each bad server-level config fact at its 0.4 advisory base roots a standalone
    // recommendation, below the 0.5 incident threshold — because each is a config-advisory root
    // key. One finding per CONFIG_* fact (they are leaves with no edges between them).
    [Theory]
    [InlineData("CONFIG_MAXDOP")]
    [InlineData("CONFIG_CTFP")]
    [InlineData("CONFIG_MAX_MEMORY_MB")]
    [InlineData("CONFIG_MIN_MAX_MEMORY_NARROW")]
    public void ServerConfigFact_RootsStandalone_BelowMinimumSeverity(string key)
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = key, Source = "config", Value = 0, Severity = 0.4 }
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == key);
    }

    // WS4: each plan-XML advisory (MISSING_INDEX / PLAN_WARNING) roots a standalone recommendation
    // below the 0.5 incident threshold — they are config-advisory root keys, like the server facts.
    [Theory]
    [InlineData("MISSING_INDEX")]
    [InlineData("PLAN_WARNING")]
    public void PlanAdvisoryFact_RootsStandalone_BelowMinimumSeverity(string key)
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = key, Source = "queries", Value = 3, Severity = 0.4 }
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == key);
    }

    // All four bad server-config facts together root four distinct standalone findings (no edges
    // between them, so none consumes another).
    [Fact]
    public void ServerConfigFacts_AllFour_RootFourDistinctFindings()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "CONFIG_MAXDOP", Source = "config", Value = 0, Severity = 0.4 },
            new() { Key = "CONFIG_CTFP", Source = "config", Value = 5, Severity = 0.4 },
            new() { Key = "CONFIG_MAX_MEMORY_MB", Source = "config", Value = 2147483647, Severity = 0.4 },
            new() { Key = "CONFIG_MIN_MAX_MEMORY_NARROW", Source = "config", Value = 24000, Severity = 0.4 },
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_MAXDOP");
        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_CTFP");
        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_MAX_MEMORY_MB");
        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_MIN_MAX_MEMORY_NARROW");
    }

    // WS5: each server-health advisory fact at its 0.4 advisory base roots a standalone
    // recommendation, below the 0.5 incident threshold — because each is a config-advisory root
    // key. One finding per fact (they are leaves with no edges between them). Mirrors the WS3
    // ServerConfigFact_RootsStandalone_BelowMinimumSeverity test.
    [Theory]
    [InlineData("CONFIG_IFI_DISABLED")]
    [InlineData("CONFIG_LPIM_DISABLED")]
    [InlineData("SERVER_MEMORY_DUMPS")]
    public void ServerHealthFact_RootsStandalone_BelowMinimumSeverity(string key)
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = key, Source = "config", Value = 0, Severity = 0.4 }
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == key);
    }

    // All three server-health facts together root three distinct standalone findings (no edges
    // between them, so none consumes another).
    [Fact]
    public void ServerHealthFacts_AllThree_RootThreeDistinctFindings()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "CONFIG_IFI_DISABLED", Source = "config", Value = 0, Severity = 0.4 },
            new() { Key = "CONFIG_LPIM_DISABLED", Source = "config", Value = 0, Severity = 0.4 },
            new() { Key = "SERVER_MEMORY_DUMPS", Source = "config", Value = 3, Severity = 0.4 },
        };

        var stories = engine.BuildStories(facts);

        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_IFI_DISABLED");
        Assert.Contains(stories, s => s.RootFactKey == "CONFIG_LPIM_DISABLED");
        Assert.Contains(stories, s => s.RootFactKey == "SERVER_MEMORY_DUMPS");
    }

    // Regression: a duplicated fact key (e.g. a collector returning two rows for the same
    // setting) once aborted the entire analysis — BuildStories built its lookup with a raw
    // ToDictionary(f => f.Key), which throws on a duplicate. It must now dedupe and survive.
    [Fact]
    public void BuildStories_WithDuplicateFactKeys_DoesNotThrow()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "CONFIG_CTFP", Source = "config", Value = 5, Severity = 0.4 },
            new() { Key = "CONFIG_CTFP", Source = "config", Value = 5, Severity = 0.4 },  // duplicate key
        };

        var ex = Record.Exception(() => engine.BuildStories(facts));

        Assert.Null(ex);
    }

    // C (workload-aware MAXDOP/CTFP): a THREADPOOL root is relabeled by its co-elevated cause so the
    // persisted root key carries parallel-vs-blocking — only the parallel flavor is a MAXDOP/CTFP
    // problem. CXPACKET/high-DOP co-fired -> _PARALLEL; blocking co-fired -> _BLOCKING; both ->
    // _MIXED; neither -> generic THREADPOOL. Co-causes are seeded below 0.5 so only THREADPOOL roots.
    [Fact]
    public void Threadpool_RootIsAttributedByCoElevatedCause()
    {
        var engine = new InferenceEngine(new RelationshipGraph());

        string ThreadpoolRoot(params Fact[] facts) =>
            engine.BuildStories(facts.ToList())
                  .First(s => s.RootFactKey.StartsWith("THREADPOOL"))
                  .RootFactKey;

        Fact Tp() => new() { Key = "THREADPOOL", Source = "waits", Value = 0.5, Severity = 0.9 };
        Fact Cx() => new() { Key = "CXPACKET", Source = "waits", Value = 0.3, Severity = 0.3 };
        Fact Dop() => new() { Key = "QUERY_HIGH_DOP", Source = "queries", Value = 6, Severity = 0.3 };
        Fact Blk() => new() { Key = "BLOCKING_EVENTS", Source = "blocking", Value = 20, Severity = 0.3 };

        Assert.Equal("THREADPOOL_PARALLEL", ThreadpoolRoot(Tp(), Cx()));
        Assert.Equal("THREADPOOL_PARALLEL", ThreadpoolRoot(Tp(), Dop()));
        Assert.Equal("THREADPOOL_BLOCKING", ThreadpoolRoot(Tp(), Blk()));
        Assert.Equal("THREADPOOL_MIXED", ThreadpoolRoot(Tp(), Cx(), Blk()));
        Assert.Equal("THREADPOOL", ThreadpoolRoot(Tp()));
    }

    // Regression: RootFactValue/LeafFactValue must carry the fact's RAW collected VALUE, not its
    // severity. They were both set to .Severity, so a CONFIG_CTFP finding (value 5, severity 0.4)
    // reported RootFactValue 0.4 — which the MCP root_fact.value contract and the notification
    // headline surface as "the value", contradicting the value-stated advice ("CTFP is 5").
    [Fact]
    public void BuildStory_RootFactValue_IsFactValue_NotSeverity()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var facts = new List<Fact>
        {
            new() { Key = "CONFIG_CTFP", Source = "config", Value = 5, Severity = 0.4 }
        };

        var story = engine.BuildStories(facts).First(s => s.RootFactKey == "CONFIG_CTFP");

        Assert.Equal(5, story.RootFactValue);   // the collected value, NOT 0.4
        Assert.Equal(0.4, story.Severity);      // severity still on its own field
    }

    [Fact]
    public void BuildStory_LeafFactValue_IsFactValue_NotSeverity()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        // CXPACKET roots (severity >= 0.5) and traverses to SOS_SCHEDULER_YIELD (edge fires when SOS
        // severity is high), so the story has a leaf whose value must be SOS's value, not its severity.
        var facts = new List<Fact>
        {
            new() { Key = "CXPACKET", Source = "waits", Value = 0.6, Severity = 0.9 },
            new() { Key = "SOS_SCHEDULER_YIELD", Source = "waits", Value = 0.5, Severity = 0.67 }
        };

        var story = engine.BuildStories(facts).First(s => s.RootFactKey == "CXPACKET");

        Assert.Equal(0.6, story.RootFactValue);              // CXPACKET value, not 0.9
        Assert.Equal("SOS_SCHEDULER_YIELD", story.LeafFactKey);
        Assert.Equal(0.5, story.LeafFactValue);              // SOS value, not 0.67
    }

    // ── correlate-and-focus: graph-connectivity incident clustering ──

    [Fact]
    public void ClusterIntoIncidents_MergesGraphConnectedStories()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var stories = new List<AnalysisStory>
        {
            new() { RootFactKey = "CPU_SQL_PERCENT", Severity = 1.6, Path = ["CPU_SQL_PERCENT"] },
            new() { RootFactKey = "PLAN_REGRESSION", Severity = 0.6, Path = ["PLAN_REGRESSION"] },
        };
        var facts = new List<Fact>
        {
            new() { Key = "CPU_SQL_PERCENT", Source = "cpu", Value = 90, Severity = 1.6 },
            new() { Key = "PLAN_REGRESSION", Source = "queries", Value = 1, Severity = 0.6, BaseSeverity = 0.6 },
        };

        // CPU_SQL_PERCENT -> PLAN_REGRESSION is an active edge (PLAN_REGRESSION.BaseSeverity > 0),
        // so the two stories are one incident even though the greedy traversal left them separate.
        Assert.Single(engine.ClusterIntoIncidents(stories, facts));
    }

    [Fact]
    public void ClusterIntoIncidents_KeepsIndependentStoriesSeparate()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var stories = new List<AnalysisStory>
        {
            new() { RootFactKey = "CPU_SQL_PERCENT", Severity = 1.6, Path = ["CPU_SQL_PERCENT"] },
            new() { RootFactKey = "DISK_SPACE", Severity = 1.6, Path = ["DISK_SPACE"] },
        };
        var facts = new List<Fact>
        {
            new() { Key = "CPU_SQL_PERCENT", Source = "cpu", Value = 90, Severity = 1.6 },
            new() { Key = "DISK_SPACE", Source = "disk", Value = 5, Severity = 1.6 },
        };

        // No graph edge between CPU and disk space -> two separate incidents.
        Assert.Equal(2, engine.ClusterIntoIncidents(stories, facts).Count);
    }

    [Fact]
    public void ClusterIntoIncidents_NormalizesThreadpoolRelabel_AndBridgesToBlocking()
    {
        var engine = new InferenceEngine(new RelationshipGraph());
        var stories = new List<AnalysisStory>
        {
            new() { RootFactKey = "THREADPOOL_BLOCKING", Severity = 0.9, Path = ["THREADPOOL_BLOCKING"] },
            new() { RootFactKey = "LCK", Severity = 0.9, Path = ["LCK"] },
        };
        var facts = new List<Fact>
        {
            new() { Key = "THREADPOOL", Source = "waits", Value = 0.5, Severity = 0.9 },
            new() { Key = "LCK", Source = "waits", Value = 0.5, Severity = 0.9 },
        };

        // The relabeled THREADPOOL_BLOCKING path key normalizes to THREADPOOL, whose active
        // THREADPOOL->LCK bridge (LCK severity >= 0.5) merges it with the blocking story.
        Assert.Single(engine.ClusterIntoIncidents(stories, facts));
    }
}
