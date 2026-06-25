using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests FactScorer Layer 1 (base severity) and Layer 2 (amplifiers).
/// Validates threshold formulas, amplifier firing, and severity capping.
/// </summary>
public class FactScorerTests
{
    /* ── Threshold formula unit tests ── */

    [Theory]
    [InlineData(0.0, 0.25, null, 0.0)]        // Zero → 0.0
    [InlineData(0.125, 0.25, null, 0.5)]       // Half of concerning → 0.5
    [InlineData(0.25, 0.25, null, 1.0)]        // At concerning (no critical) → 1.0
    [InlineData(0.50, 0.25, null, 1.0)]        // Above concerning (no critical) → capped at 1.0
    [InlineData(0.0, 0.25, 0.75, 0.0)]         // Zero → 0.0
    [InlineData(0.125, 0.25, 0.75, 0.25)]      // Half of concerning → 0.25
    [InlineData(0.25, 0.25, 0.75, 0.5)]        // At concerning → 0.5
    [InlineData(0.50, 0.25, 0.75, 0.75)]       // Midway → 0.75
    [InlineData(0.75, 0.25, 0.75, 1.0)]        // At critical → 1.0
    [InlineData(1.00, 0.25, 0.75, 1.0)]        // Above critical → 1.0
    public void ApplyThresholdFormula_ReturnsExpected(
        double value, double concerning, double? critical, double expected)
    {
        var result = FactScorer.ApplyThresholdFormula(value, concerning, critical);
        Assert.Equal(expected, result, precision: 4);
    }

    /* ── Unknown wait types ── */

    [Fact]
    public void Score_UnknownWaitType_GetsSeverityZero()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "UNKNOWN_WAIT_XYZ", Value = 0.50 }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity);
    }

    /* ── Layer 2: Amplifier tests ── */

    [Fact]
    public void Amplifier_SeverityCappedAt2()
    {
        // Synthetic: create a fact set where amplifiers would push past 2.0
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },           // base = 1.0
            new() { Source = "waits", Key = "SOS_SCHEDULER_YIELD", Value = 0.50 }, // > 25% threshold
            new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,           // real thread exhaustion
                Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } },  // 2h total, 3.6s avg
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },          // bad CTFP
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },        // bad MAXDOP
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");

        // base 1.0 * (1.0 + 0.3 SOS + 0.4 THREADPOOL + 0.3 CTFP + 0.2 MAXDOP) = 2.2 → capped at 2.0
        Assert.True(cx.Severity <= 2.0, "Severity should never exceed 2.0");
        Assert.Equal(2.0, cx.Severity);
    }

    /* ── Regression: duplicate fact keys must not crash scoring ── */

    // A duplicated fact key (e.g. a config collector returning two rows for the same setting)
    // once aborted the ENTIRE analysis for a server: ScoreAll built its amplifier lookup with a
    // raw ToDictionary(f => f.Key), which throws on a duplicate. The exception was swallowed
    // upstream, silently blanking every finding. ScoreAll must now dedupe and survive.
    [Fact]
    public void ScoreAll_WithDuplicateFactKeys_DoesNotThrow()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },   // duplicate key
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },
        };

        var scorer = new FactScorer();
        var ex = Record.Exception(() => scorer.ScoreAll(facts));

        Assert.Null(ex);
    }

    // The backstop helper keeps the first fact per key and never throws on duplicates.
    [Fact]
    public void ToFactLookup_DedupesByKey_KeepsFirst()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 99 },
        };

        var lookup = facts.ToFactLookup();

        Assert.Single(lookup);
        Assert.Equal(5, lookup["CONFIG_CTFP"].Value);   // first wins
    }

    /* ── WS3: percent-autogrowth-on-large-files config fact ── */

    // A FILE_AUTOGROWTH_PERCENT fact scores the 0.3 advisory base when at least one large
    // percent-growth file was found (file_count > 0) — mirroring DB_CONFIG's single-misconfig
    // base. It is deliberately below the 0.5 incident threshold; it surfaces only because it
    // is a config-advisory root key (see the InferenceEngine rooting test).
    [Fact]
    public void FileAutogrowthPercent_ScoresAdvisoryBase_WhenFilesPresent()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "FILE_AUTOGROWTH_PERCENT", Value = 2,
                    Metadata = new() { ["file_count"] = 2, ["database_count"] = 1 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.3, facts[0].BaseSeverity, precision: 4);
    }

    // No offending files (file_count == 0) → no severity. Defends the collector contract that
    // the fact is emitted only when file_count > 0, and the scorer's own guard.
    [Fact]
    public void FileAutogrowthPercent_ScoresZero_WhenNoFiles()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "FILE_AUTOGROWTH_PERCENT", Value = 0,
                    Metadata = new() { ["file_count"] = 0, ["database_count"] = 0 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for the fact key (a missing AdviceBlock is the P1 dead-fact bug class —
    // a fact that roots but renders no advice). Headline must be the authored copy.
    [Fact]
    public void FileAutogrowthPercent_HasAdviceBlock()
    {
        var advice = FactAdvice.GetForFactKey("FILE_AUTOGROWTH_PERCENT");

        Assert.NotNull(advice);
        Assert.Equal("Large file(s) growing in percentage steps", advice!.Headline);
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }

    /* ── WS3: server-level config facts (MAXDOP / CTFP / max & min memory) ── */

    // Each per-setting CONFIG_* fact scores the 0.4 advisory base ONLY when the value is bad, and
    // 0 otherwise — so audit_config still sees every CONFIG_* fact (it reads the raw value) but
    // only a BAD one roots a recommendation card.
    [Theory]
    // MAXDOP: 0 is bad (unlimited); any other value is operator-chosen.
    [InlineData("CONFIG_MAXDOP", 0, 0.4)]
    [InlineData("CONFIG_MAXDOP", 4, 0.0)]
    [InlineData("CONFIG_MAXDOP", 8, 0.0)]
    // CTFP: <= 5 is bad (default-and-below); above is fine.
    [InlineData("CONFIG_CTFP", 5, 0.4)]
    [InlineData("CONFIG_CTFP", 1, 0.4)]
    [InlineData("CONFIG_CTFP", 50, 0.0)]
    [InlineData("CONFIG_CTFP", 6, 0.0)]
    // max server memory: the 2 PB default (2147483647) is bad; a real cap is fine.
    [InlineData("CONFIG_MAX_MEMORY_MB", 2147483647, 0.4)]
    [InlineData("CONFIG_MAX_MEMORY_MB", 28672, 0.0)]
    public void ServerConfigFact_ScoresBadValueOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = value,
                    Metadata = new() { ["value_in_use"] = value } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // CONFIG_MIN_MAX_MEMORY_NARROW is emitted by the collector ONLY when bad, so any presence of
    // the fact scores 0.4 (the fact's existence is the flag).
    [Fact]
    public void NarrowMemoryFact_AlwaysScoresAdvisoryBase()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_MIN_MAX_MEMORY_NARROW", Value = 24000,
                    Metadata = new() { ["min_memory_mb"] = 24000, ["max_memory_mb"] = 28672 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.4, facts[0].BaseSeverity, precision: 4);
    }

    // CONFIG_MIN_MEMORY_MB and CONFIG_MAX_WORKER_THREADS are leaf/context facts — never scored
    // (they feed audit_config / the narrow-memory derivation, not a card of their own).
    [Theory]
    [InlineData("CONFIG_MIN_MEMORY_MB")]
    [InlineData("CONFIG_MAX_WORKER_THREADS")]
    public void ServerConfigLeafFacts_ScoreZero(string key)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = 1024, Metadata = new() { ["value_in_use"] = 1024 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each of the four server-config root keys (dead-fact guard).
    [Theory]
    [InlineData("CONFIG_MAXDOP")]
    [InlineData("CONFIG_CTFP")]
    [InlineData("CONFIG_MAX_MEMORY_MB")]
    [InlineData("CONFIG_MIN_MAX_MEMORY_NARROW")]
    public void ServerConfigKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }

    // The shared narrow-memory builder: emitted only when max is CONFIGURED and min >= 80% of max.
    [Theory]
    [InlineData(28672, 24000, true)]    // min 83.7% of max -> narrow
    [InlineData(28672, 22938, true)]    // just over 80% (threshold 22937.6) -> narrow
    [InlineData(28672, 22000, false)]   // ~76.7% -> just under 80%, not narrow
    [InlineData(28672, 14000, false)]   // min ~49% -> not narrow
    [InlineData(2147483647, 2000000000, false)] // max unconfigured -> never narrow
    [InlineData(0, 0, false)]           // max 0 -> guard
    public void BuildNarrowMemoryFact_FollowsThreshold(long maxMb, long minMb, bool emitted)
    {
        var fact = FactRemediation.BuildNarrowMemoryFact(1, maxMb, minMb);

        if (!emitted)
        {
            Assert.Null(fact);
            return;
        }

        Assert.NotNull(fact);
        Assert.Equal("CONFIG_MIN_MAX_MEMORY_NARROW", fact!.Key);
        Assert.Equal(minMb, fact.Value);
        Assert.Equal(minMb, fact.Metadata["min_memory_mb"]);
        Assert.Equal(maxMb, fact.Metadata["max_memory_mb"]);
    }

    [Fact]
    public void BuildNarrowMemoryFact_NullInputs_ReturnsNull()
    {
        Assert.Null(FactRemediation.BuildNarrowMemoryFact(1, null, 24000));
        Assert.Null(FactRemediation.BuildNarrowMemoryFact(1, 28672, null));
    }

    /* ── WS5: server-health advisory facts (LPIM / IFI / memory dumps) ── */

    // Each WS5 server-health fact scores its 0.4 advisory base ONLY when the value is bad, and 0
    // otherwise — mirroring the WS3 server-config keys. The collectors gate emission (Express /
    // small-RAM / dumps>0) so a fact that would score 0 is normally never emitted, but the scorer
    // is still independently bad-only so it can be unit-tested in isolation.
    [Theory]
    // IFI: disabled (Value 0) is bad; enabled (Value 1) is fine.
    [InlineData("CONFIG_IFI_DISABLED", 0, 0.4)]
    [InlineData("CONFIG_IFI_DISABLED", 1, 0.0)]
    // LPIM: disabled (Value 0) is bad; enabled (Value 1) is fine.
    [InlineData("CONFIG_LPIM_DISABLED", 0, 0.4)]
    [InlineData("CONFIG_LPIM_DISABLED", 1, 0.0)]
    // Memory dumps: any count > 0 is bad; 0 is fine.
    [InlineData("SERVER_MEMORY_DUMPS", 1, 0.4)]
    [InlineData("SERVER_MEMORY_DUMPS", 7, 0.4)]
    [InlineData("SERVER_MEMORY_DUMPS", 0, 0.0)]
    public void ServerHealthFact_ScoresBadValueOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = value }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each WS5 server-health root key (dead-fact guard — a fact that roots but
    // renders no advice is the P1 dead-fact bug class).
    [Theory]
    [InlineData("CONFIG_IFI_DISABLED")]
    [InlineData("CONFIG_LPIM_DISABLED")]
    [InlineData("SERVER_MEMORY_DUMPS")]
    public void ServerHealthKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
        // Advise-only: no generated Apply T-SQL is attached to the bare advice block.
        Assert.Null(advice.RemediationTsql);
    }

    // WS4: plan-XML advisories. MISSING_INDEX / PLAN_WARNING (Source "queries", Value = count)
    // score the 0.4 advisory base only when the count is > 0, and 0 otherwise. Advise-only.
    [Theory]
    [InlineData("MISSING_INDEX", 1, 0.4)]
    [InlineData("MISSING_INDEX", 5, 0.4)]
    [InlineData("MISSING_INDEX", 0, 0.0)]
    [InlineData("PLAN_WARNING", 1, 0.4)]
    [InlineData("PLAN_WARNING", 0, 0.0)]
    public void PlanAdvisoryFact_ScoresWhenPresentOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "queries", Key = key, Value = value }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each WS4 plan-advisory root key (dead-fact guard); advise-only (no Apply T-SQL).
    [Theory]
    [InlineData("MISSING_INDEX")]
    [InlineData("PLAN_WARNING")]
    public void PlanAdvisoryKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
        Assert.Null(advice.RemediationTsql);
    }

    // C (workload-aware MAXDOP/CTFP): every THREADPOOL attribution variant InferenceEngine can root
    // on (parallel/blocking/mixed) plus the generic fallback has an advice block — a relabeled root
    // with no advice would render an empty card (the dead-fact bug class).
    [Theory]
    [InlineData("THREADPOOL")]
    [InlineData("THREADPOOL_PARALLEL")]
    [InlineData("THREADPOOL_BLOCKING")]
    [InlineData("THREADPOOL_MIXED")]
    public void ThreadpoolVariants_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }
}
