using System.Collections.Generic;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for FinOpsHealthCalculator and HighImpactScorer pure functions.
/// Covers happy path, boundary values, and adversarial inputs.
/// </summary>
public class HealthCalculatorTests
{
    // ============================================
    // CpuScore
    // ============================================

    [Fact]
    public void CpuScore_ZeroCpu_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.CpuScore(0));
    }

    [Fact]
    public void CpuScore_IdleCpu_HighScore()
    {
        var score = FinOpsHealthCalculator.CpuScore(10);
        Assert.True(score > 85, $"10% CPU should score >85, got {score}");
    }

    [Fact]
    public void CpuScore_ModerateCpu_MidRange()
    {
        var score = FinOpsHealthCalculator.CpuScore(50);
        Assert.InRange(score, 55, 75);
    }

    [Fact]
    public void CpuScore_At70Boundary_Returns50()
    {
        // 70% is the inflection point between the two formulas
        Assert.Equal(50, FinOpsHealthCalculator.CpuScore(70));
    }

    [Fact]
    public void CpuScore_HighCpu_LowScore()
    {
        var score = FinOpsHealthCalculator.CpuScore(90);
        Assert.True(score < 20, $"90% CPU should score <20, got {score}");
    }

    [Fact]
    public void CpuScore_100Percent_ZeroOrNear()
    {
        var score = FinOpsHealthCalculator.CpuScore(100);
        Assert.InRange(score, 0, 5);
    }

    [Fact]
    public void CpuScore_NegativeInput_DoesNotCrash()
    {
        // Garbage in — should not throw
        var score = FinOpsHealthCalculator.CpuScore(-10);
        Assert.True(score >= 0, "Negative input should not produce negative score");
    }

    [Fact]
    public void CpuScore_Over100_DoesNotCrash()
    {
        var score = FinOpsHealthCalculator.CpuScore(150);
        Assert.True(score >= 0, $"150% CPU should clamp to 0, got {score}");
    }

    [Fact]
    public void CpuScore_MonotonicallyDecreasing()
    {
        // Score should never increase as CPU increases
        int prev = FinOpsHealthCalculator.CpuScore(0);
        for (int cpu = 1; cpu <= 100; cpu++)
        {
            int current = FinOpsHealthCalculator.CpuScore(cpu);
            Assert.True(current <= prev,
                $"Score increased from {prev} at {cpu - 1}% to {current} at {cpu}%");
            prev = current;
        }
    }

    // ============================================
    // MemoryScore
    // ============================================

    [Fact]
    public void MemoryScore_ZeroRatio_Returns60()
    {
        // Over-provisioned — returns 60 (not 100)
        Assert.Equal(60, FinOpsHealthCalculator.MemoryScore(0));
    }

    [Fact]
    public void MemoryScore_VeryLowRatio_Returns60()
    {
        // 10% buffer pool ratio — over-provisioned
        Assert.Equal(60, FinOpsHealthCalculator.MemoryScore(0.10m));
    }

    [Fact]
    public void MemoryScore_SweetSpot_Returns100()
    {
        // 50% ratio — healthy range
        Assert.Equal(100, FinOpsHealthCalculator.MemoryScore(0.50m));
    }

    [Fact]
    public void MemoryScore_UpperSweetSpot_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.MemoryScore(0.85m));
    }

    [Fact]
    public void MemoryScore_At30Boundary_Returns60()
    {
        // Boundary between over-provisioned and sweet spot
        Assert.Equal(60, FinOpsHealthCalculator.MemoryScore(0.30m));
    }

    [Fact]
    public void MemoryScore_JustAbove30_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.MemoryScore(0.31m));
    }

    [Fact]
    public void MemoryScore_HighPressure_LowScore()
    {
        var score = FinOpsHealthCalculator.MemoryScore(0.98m);
        Assert.True(score < 20, $"98% ratio should score <20, got {score}");
    }

    [Fact]
    public void MemoryScore_FullyExhausted_NearZero()
    {
        var score = FinOpsHealthCalculator.MemoryScore(1.0m);
        Assert.InRange(score, 0, 5);
    }

    [Fact]
    public void MemoryScore_NegativeRatio_DoesNotCrash()
    {
        var score = FinOpsHealthCalculator.MemoryScore(-0.5m);
        Assert.True(score >= 0);
    }

    [Fact]
    public void MemoryScore_Over1_DoesNotCrash()
    {
        // Buffer pool > physical memory shouldn't happen but shouldn't crash
        var score = FinOpsHealthCalculator.MemoryScore(1.5m);
        Assert.True(score >= 0, $"Ratio > 1.0 should not produce negative score, got {score}");
    }

    // ============================================
    // StorageScore
    // ============================================

    [Fact]
    public void StorageScore_PlentyOfSpace_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.StorageScore(50));
    }

    [Fact]
    public void StorageScore_At30Boundary_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.StorageScore(30));
    }

    [Fact]
    public void StorageScore_20Percent_Returns75()
    {
        Assert.Equal(75, FinOpsHealthCalculator.StorageScore(20));
    }

    [Fact]
    public void StorageScore_10Percent_Returns50()
    {
        Assert.Equal(50, FinOpsHealthCalculator.StorageScore(10));
    }

    [Fact]
    public void StorageScore_5Percent_Returns25()
    {
        Assert.Equal(25, FinOpsHealthCalculator.StorageScore(5));
    }

    [Fact]
    public void StorageScore_ZeroFreeSpace_ReturnsZero()
    {
        Assert.Equal(0, FinOpsHealthCalculator.StorageScore(0));
    }

    [Fact]
    public void StorageScore_NegativeFreeSpace_DoesNotCrash()
    {
        var score = FinOpsHealthCalculator.StorageScore(-10);
        Assert.True(score >= -50, $"Negative free space produced {score}");
        // Note: formula allows negative output for negative input — that's fine,
        // callers should never pass negative
    }

    [Fact]
    public void StorageScore_MonotonicallyIncreasing()
    {
        int prev = FinOpsHealthCalculator.StorageScore(0);
        for (int pct = 1; pct <= 100; pct++)
        {
            int current = FinOpsHealthCalculator.StorageScore(pct);
            Assert.True(current >= prev,
                $"Score decreased from {prev} at {pct - 1}% to {current} at {pct}%");
            prev = current;
        }
    }

    // ============================================
    // Overall
    // ============================================

    [Fact]
    public void Overall_AllPerfect_Returns100()
    {
        Assert.Equal(100, FinOpsHealthCalculator.Overall(100, 100, 100));
    }

    [Fact]
    public void Overall_AllZero_ReturnsZero()
    {
        Assert.Equal(0, FinOpsHealthCalculator.Overall(0, 0, 0));
    }

    [Fact]
    public void Overall_CpuWeightedAt40Percent()
    {
        // CPU=100, rest=0 → should be 40
        Assert.Equal(40, FinOpsHealthCalculator.Overall(100, 0, 0));
    }

    [Fact]
    public void Overall_MemoryWeightedAt30Percent()
    {
        Assert.Equal(30, FinOpsHealthCalculator.Overall(0, 100, 0));
    }

    [Fact]
    public void Overall_StorageWeightedAt30Percent()
    {
        Assert.Equal(30, FinOpsHealthCalculator.Overall(0, 0, 100));
    }

    [Fact]
    public void Overall_MixedScores()
    {
        // CPU=80 (32), Memory=60 (18), Storage=40 (12) → 62
        Assert.Equal(62, FinOpsHealthCalculator.Overall(80, 60, 40));
    }

    [Fact]
    public void Overall_NegativeInputs_DoesNotCrash()
    {
        var score = FinOpsHealthCalculator.Overall(-50, -50, -50);
        // Garbage in, but should not throw
        Assert.True(score < 0);
    }

    // ============================================
    // ScoreColor
    // ============================================

    [Fact]
    public void ScoreColor_80_Green()
    {
        Assert.Equal("#27AE60", FinOpsHealthCalculator.ScoreColor(80));
    }

    [Fact]
    public void ScoreColor_100_Green()
    {
        Assert.Equal("#27AE60", FinOpsHealthCalculator.ScoreColor(100));
    }

    [Fact]
    public void ScoreColor_79_Orange()
    {
        Assert.Equal("#F39C12", FinOpsHealthCalculator.ScoreColor(79));
    }

    [Fact]
    public void ScoreColor_60_Orange()
    {
        Assert.Equal("#F39C12", FinOpsHealthCalculator.ScoreColor(60));
    }

    [Fact]
    public void ScoreColor_59_Red()
    {
        Assert.Equal("#E74C3C", FinOpsHealthCalculator.ScoreColor(59));
    }

    [Fact]
    public void ScoreColor_0_Red()
    {
        Assert.Equal("#E74C3C", FinOpsHealthCalculator.ScoreColor(0));
    }

    [Fact]
    public void ScoreColor_NegativeScore_Red()
    {
        Assert.Equal("#E74C3C", FinOpsHealthCalculator.ScoreColor(-10));
    }

    [Fact]
    public void ScoreColor_LargeScore_Green()
    {
        Assert.Equal("#27AE60", FinOpsHealthCalculator.ScoreColor(999));
    }

    // ============================================
    // PercentRank
    // ============================================

    [Fact]
    public void PercentRank_SingleValue_ReturnsZero()
    {
        var values = new List<decimal> { 100m };
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 100m));
    }

    [Fact]
    public void PercentRank_EmptyList_ReturnsZero()
    {
        var values = new List<decimal>();
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 50m));
    }

    [Fact]
    public void PercentRank_HighestValue_Returns1()
    {
        var values = new List<decimal> { 10m, 20m, 30m, 40m, 50m };
        Assert.Equal(1.0m, HighImpactScorer.PercentRank(values, 50m));
    }

    [Fact]
    public void PercentRank_LowestValue_ReturnsZero()
    {
        var values = new List<decimal> { 10m, 20m, 30m, 40m, 50m };
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 10m));
    }

    [Fact]
    public void PercentRank_MiddleValue_Returns50Pct()
    {
        var values = new List<decimal> { 10m, 20m, 30m, 40m, 50m };
        Assert.Equal(0.5m, HighImpactScorer.PercentRank(values, 30m));
    }

    [Fact]
    public void PercentRank_AllSameValues_ReturnsZero()
    {
        var values = new List<decimal> { 50m, 50m, 50m, 50m };
        // No values are strictly less than 50, so rank = 0
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 50m));
    }

    [Fact]
    public void PercentRank_ValueNotInList_StillWorks()
    {
        var values = new List<decimal> { 10m, 20m, 30m };
        // 25 is greater than 10 and 20, so rank = 2 out of (3-1) = 1.0
        Assert.Equal(1.0m, HighImpactScorer.PercentRank(values, 25m));
    }

    [Fact]
    public void PercentRank_ValueBelowAll_ReturnsZero()
    {
        var values = new List<decimal> { 10m, 20m, 30m };
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 5m));
    }

    [Fact]
    public void PercentRank_ValueAboveAll_Returns1()
    {
        var values = new List<decimal> { 10m, 20m, 30m };
        Assert.Equal(1.0m, HighImpactScorer.PercentRank(values, 100m));
    }

    [Fact]
    public void PercentRank_TwoValues_BinaryOutcome()
    {
        var values = new List<decimal> { 10m, 20m };
        Assert.Equal(0m, HighImpactScorer.PercentRank(values, 10m));
        Assert.Equal(1.0m, HighImpactScorer.PercentRank(values, 20m));
    }

    // ============================================
    // HighImpactScorer.Score — adversarial
    // ============================================

    [Fact]
    public void HighImpactScorer_SingleRow_ScoresIt()
    {
        var rows = new List<HighImpactQueryRow>
        {
            new() { QueryHash = "SOLO", TotalCpuMs = 1000, TotalDurationMs = 500, TotalReads = 100, TotalWrites = 10, PeakMemoryMb = 50, TotalExecutions = 100 }
        };
        var scored = HighImpactScorer.Score(rows, topN: 10);
        Assert.Single(scored);
        Assert.Equal(100m, scored[0].CpuShare); // Only query = 100% share
    }

    [Fact]
    public void HighImpactScorer_AllIdenticalRows_EqualShares()
    {
        var rows = new List<HighImpactQueryRow>();
        for (int i = 0; i < 5; i++)
        {
            rows.Add(new HighImpactQueryRow
            {
                QueryHash = $"Q{i}",
                TotalCpuMs = 100,
                TotalDurationMs = 100,
                TotalReads = 100,
                TotalWrites = 100,
                PeakMemoryMb = 100,
                TotalExecutions = 100
            });
        }

        var scored = HighImpactScorer.Score(rows, topN: 10);
        Assert.Equal(5, scored.Count);
        // All should have 20% share
        foreach (var row in scored)
        {
            Assert.Equal(20.0m, row.CpuShare);
        }
    }

    [Fact]
    public void HighImpactScorer_TopNFiltering_LimitsResults()
    {
        var rows = new List<HighImpactQueryRow>();
        for (int i = 0; i < 50; i++)
        {
            rows.Add(new HighImpactQueryRow
            {
                QueryHash = $"Q{i:D3}",
                TotalCpuMs = i * 10,
                TotalDurationMs = i * 5,
                TotalReads = i * 100,
                TotalWrites = i * 10,
                PeakMemoryMb = i,
                TotalExecutions = 50 - i // Inverse correlation to test UNION dedup
            });
        }

        var scored = HighImpactScorer.Score(rows, topN: 3);
        // Top 3 per 6 dimensions via UNION — should be more than 3 but less than 50
        Assert.True(scored.Count <= 18, $"Should be limited by topN union, got {scored.Count}");
        Assert.True(scored.Count >= 3, $"Should have at least topN results, got {scored.Count}");
    }

    [Fact]
    public void HighImpactScorer_ZeroValues_DoesNotDivideByZero()
    {
        var rows = new List<HighImpactQueryRow>
        {
            new() { QueryHash = "ZERO", TotalCpuMs = 0, TotalDurationMs = 0, TotalReads = 0, TotalWrites = 0, PeakMemoryMb = 0, TotalExecutions = 0 }
        };
        // Should not throw
        var scored = HighImpactScorer.Score(rows, topN: 10);
        Assert.Single(scored);
    }

    [Fact]
    public void HighImpactScorer_SortedByImpactScoreDescending()
    {
        var rows = new List<HighImpactQueryRow>
        {
            new() { QueryHash = "LOW", TotalCpuMs = 10, TotalDurationMs = 10, TotalReads = 10, TotalWrites = 10, PeakMemoryMb = 1, TotalExecutions = 10 },
            new() { QueryHash = "HIGH", TotalCpuMs = 10000, TotalDurationMs = 10000, TotalReads = 1000000, TotalWrites = 10000, PeakMemoryMb = 1000, TotalExecutions = 10000 },
            new() { QueryHash = "MID", TotalCpuMs = 500, TotalDurationMs = 500, TotalReads = 50000, TotalWrites = 500, PeakMemoryMb = 50, TotalExecutions = 500 },
        };
        var scored = HighImpactScorer.Score(rows, topN: 10);
        Assert.Equal("HIGH", scored[0].QueryHash);
        for (int i = 1; i < scored.Count; i++)
        {
            Assert.True(scored[i].ImpactScore <= scored[i - 1].ImpactScore,
                $"Not sorted descending: {scored[i - 1].ImpactScore} then {scored[i].ImpactScore}");
        }
    }
}
