/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The custom-alert rule definition parser/validator (#3285). The metric is validated by the compose
/// <c>TryParsePanel</c> authority (so these use a real catalog measure — wait_stats/wait_time_ms) and pinned
/// to Scalar; the predicate/hysteresis/scope/window rules are validated here.
/// </summary>
public class CustomAlertRuleDefinitionTests
{
    /// <summary>A valid Scalar metric over a real catalog measure.</summary>
    private const string Metric = "\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}";

    [Fact]
    public void MinimalRule_Parses_WithDefaults()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}");

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(CustomAlertOp.GreaterOrEqual, def!.Op);
        Assert.Equal(1000d, def.WarnThreshold);
        Assert.Null(def.CriticalThreshold);
        Assert.Null(def.LowerBound);   // a scalar rule carries no band
        Assert.Null(def.UpperBound);
        Assert.False(def.IsRange);
        Assert.Equal(1, def.BreachSamples);
        Assert.Equal(1, def.ClearSamples);
        Assert.Equal(CustomAlertScopeMode.All, def.ScopeMode);
        Assert.Equal(1.0, def.WindowHours);
    }

    [Fact]
    public void FullRule_Parses_AllFields()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}," +
            "\"scope\":{\"mode\":\"servers\",\"servers\":[\"PROD-A\",\"PROD-B\"]}," +
            "\"evaluationIntervalSeconds\":120}");

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(40, def!.CriticalThreshold);
        Assert.Equal(3, def.BreachSamples);
        Assert.Equal(2, def.ClearSamples);
        Assert.Equal(CustomAlertScopeMode.Servers, def.ScopeMode);
        Assert.Equal(120, def.EvaluationIntervalSeconds);
        Assert.True(def.AppliesTo("prod-a"));   // case-insensitive
        Assert.False(def.AppliesTo("PROD-C"));
    }

    [Fact]
    public void ScopeAll_AppliesToEveryServer()
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"gt\",\"warnThreshold\":1}}");
        Assert.True(def!.AppliesTo("anything"));
    }

    [Theory]
    [InlineData(20, false, AlertSeverityLevel.Warning)] // below warn
    [InlineData(30, true, AlertSeverityLevel.Warning)]  // over warn, under critical
    [InlineData(45, true, AlertSeverityLevel.Critical)] // over critical
    public void PredicateAndSeverity_GreaterOrEqual(double value, bool breaching, AlertSeverityLevel severity)
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}}");

        Assert.Equal(breaching, def!.IsBreaching(value));
        if (breaching)
        {
            Assert.Equal(severity, def.SeverityFor(value));
        }
    }

    [Fact]
    public void MissingMetric_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse("{\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void MissingPredicate_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse("{" + Metric + "}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void UnknownOp_IsError()
    {
        // 'between'/'outside' are now real range ops (#3351), so an unknown op is a genuinely unrecognized token.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"nope\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void NonScalarMetric_IsError()
    {
        // A timeBucket makes the compose panel a time series, not a single value.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void CriticalNotMoreExtremeThanWarn_IsError()
    {
        // op ge means higher is worse, so critical must be >= warn.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":40,\"criticalThreshold\":25}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void WindowBeyondCeiling_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":100}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void TagScope_Parses_WithStableTagId()
    {
        // #3350: a tag-scoped rule stores the tag's STABLE integer id (scope.tagId), so a rename never
        // re-scopes it; the evaluator resolves the id -> the tag's server set at sweep time.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}," +
            "\"scope\":{\"mode\":\"tag\",\"tagId\":7}}");

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(CustomAlertScopeMode.Tag, def!.ScopeMode);
        Assert.Equal(7, def.ScopeTagId);
        // A tag rule cannot be decided from a storage name alone — AppliesTo (All/Servers only) returns false.
        Assert.False(def.AppliesTo("anything"));
    }

    [Fact]
    public void TagScope_MissingTagId_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"scope\":{\"mode\":\"tag\"}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void TagScope_NonIntegerTagId_IsError()
    {
        // A tag is stored by its stable integer id, never its name — a string tagId is rejected.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}," +
            "\"scope\":{\"mode\":\"tag\",\"tagId\":\"prod\"}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void TagScope_NonPositiveTagId_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}," +
            "\"scope\":{\"mode\":\"tag\",\"tagId\":0}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void HysteresisBelowOne_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"hysteresis\":{\"breachSamples\":0}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Fact]
    public void ServersScopeWithEmptyList_IsError()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1},\"scope\":{\"mode\":\"servers\",\"servers\":[]}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    // ─────────────────────────── range ops (#3351): between / outside ───────────────────────────

    /// <summary>A valid range predicate over the shared metric: op + a two-sided band [lower, upper].</summary>
    private static string RangePredicate(string op, double lower, double upper) =>
        "{" + Metric + ",\"predicate\":{\"op\":\"" + op + "\",\"lowerBound\":" +
        lower.ToString(System.Globalization.CultureInfo.InvariantCulture) + ",\"upperBound\":" +
        upper.ToString(System.Globalization.CultureInfo.InvariantCulture) + "}}";

    [Theory]
    [InlineData("between", CustomAlertOp.Between)]
    [InlineData("outside", CustomAlertOp.Outside)]
    public void RangeRule_Parses_WithBounds_AndNoScalarThresholds(string op, CustomAlertOp expected)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(RangePredicate(op, 10, 100));

        Assert.Null(error);
        Assert.NotNull(def);
        Assert.Equal(expected, def!.Op);
        Assert.True(def.IsRange);
        Assert.Equal(10d, def.LowerBound);
        Assert.Equal(100d, def.UpperBound);
        // A range op carries the band ONLY — never the scalar warn/critical bars (mutual exclusivity).
        Assert.Null(def.WarnThreshold);
        Assert.Null(def.CriticalThreshold);
    }

    [Theory]
    [InlineData("between")]
    [InlineData("outside")]
    public void RangeOp_MissingLowerBound_IsError(string op)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"" + op + "\",\"upperBound\":100}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData("between")]
    [InlineData("outside")]
    public void RangeOp_MissingUpperBound_IsError(string op)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"" + op + "\",\"lowerBound\":10}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData(100, 100)] // equal bounds are not a band
    [InlineData(100, 10)]  // inverted bounds
    public void RangeOp_LowerNotLessThanUpper_IsError(double lower, double upper)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(RangePredicate("outside", lower, upper));
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData("\"warnThreshold\":50")]
    [InlineData("\"criticalThreshold\":50")]
    public void RangeOp_CarryingScalarThreshold_IsError(string scalarKey)
    {
        // Mutual exclusivity: a between/outside predicate must NOT carry warn/critical.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"outside\",\"lowerBound\":10,\"upperBound\":100," + scalarKey + "}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData("\"lowerBound\":10")]
    [InlineData("\"upperBound\":100")]
    public void ScalarOp_CarryingBound_IsError(string boundKey)
    {
        // Mutual exclusivity: a gt/ge/lt/le predicate must NOT carry lowerBound/upperBound.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25," + boundKey + "}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData(5, false)]   // below the band
    [InlineData(10, true)]   // on the lower bound (inclusive)
    [InlineData(55, true)]   // inside
    [InlineData(100, true)]  // on the upper bound (inclusive)
    [InlineData(150, false)] // above the band
    public void Between_BreachesInsideTheInclusiveBand(double value, bool breaching)
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(RangePredicate("between", 10, 100));
        Assert.Equal(breaching, def!.IsBreaching(value));
        if (breaching)
        {
            // A range op is Warning-only in v1 — no critical tier.
            Assert.Equal(AlertSeverityLevel.Warning, def.SeverityFor(value));
        }
    }

    [Theory]
    [InlineData(5, true)]    // below the band
    [InlineData(10, false)]  // on the lower bound (inclusive band => not outside)
    [InlineData(55, false)]  // inside
    [InlineData(100, false)] // on the upper bound (inclusive band => not outside)
    [InlineData(150, true)]  // above the band
    public void Outside_BreachesBeyondTheInclusiveBand(double value, bool breaching)
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(RangePredicate("outside", 10, 100));
        Assert.Equal(breaching, def!.IsBreaching(value));
        if (breaching)
        {
            Assert.Equal(AlertSeverityLevel.Warning, def.SeverityFor(value));
        }
    }

    [Fact]
    public void FiredThreshold_RendersBand_ForRangeOp_WithNoNumericTwin()
    {
        // Round-trip through the delivered-alert render authority: a range op renders its band as ASCII
        // "outside <lo> - <hi>" and has no single numeric threshold value (NumericThresholdValue is null).
        var (def, _) = CustomAlertRuleDefinition.TryParse(RangePredicate("outside", 10, 100));
        var (text, numeric) = def!.FiredThreshold(AlertSeverityLevel.Warning);
        Assert.Equal("outside 10 - 100", text);
        Assert.Null(numeric);
    }

    [Fact]
    public void FiredThreshold_RendersScalarBar_ForScalarOp_WithTheCrossedValue()
    {
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}}");
        var (warnText, warnNum) = def!.FiredThreshold(AlertSeverityLevel.Warning);
        Assert.Equal(">= 25", warnText);
        Assert.Equal(25d, warnNum);

        var (critText, critNum) = def.FiredThreshold(AlertSeverityLevel.Critical);
        Assert.Equal(">= 40", critText);
        Assert.Equal(40d, critNum);
    }

    // ─────────────── #3373: the no-data (stalled-collector) count guard — scalar AND range ───────────────

    /// <summary>A COUNT-aggregate Scalar metric. deadlocks/deadlock_count is a count-only per-event measure, so
    /// the metric compiles to <c>COUNT(*)</c>, which reads 0 over an empty (stalled) window — never the NULL the
    /// evaluator's no-data freeze catches. A '&lt;'/'&lt;=' scalar, or a range band that fires at 0, therefore
    /// cannot tell "no events happened" from "the collector is dead".</summary>
    private const string CountMetric =
        "\"metric\":{\"source\":\"deadlocks\",\"measure\":\"deadlock_count\",\"aggregate\":\"count\",\"hours\":1}";

    /// <summary>A range predicate over the COUNT metric above.</summary>
    private static string CountRangePredicate(string op, double lower, double upper) =>
        "{" + CountMetric + ",\"predicate\":{\"op\":\"" + op + "\",\"lowerBound\":" +
        lower.ToString(System.Globalization.CultureInfo.InvariantCulture) + ",\"upperBound\":" +
        upper.ToString(System.Globalization.CultureInfo.InvariantCulture) + "}}";

    /// <summary>A range predicate over the shared NON-count metric (wait_stats/wait_time_ms/SUM).</summary>
    private static string SumRangePredicate(string op, double lower, double upper) =>
        "{" + Metric + ",\"predicate\":{\"op\":\"" + op + "\",\"lowerBound\":" +
        lower.ToString(System.Globalization.CultureInfo.InvariantCulture) + ",\"upperBound\":" +
        upper.ToString(System.Globalization.CultureInfo.InvariantCulture) + "}}";

    [Theory]
    [InlineData("lt")]
    [InlineData("le")]
    public void ScalarCountWithLessThanOrEqual_IsError_StalledCollectorTrap(string op)
    {
        // The scalar half of the count no-data guard (the range half is below): a stalled collector reads COUNT 0,
        // indistinguishable from zero events, so a '<'/'<=' count alert would false-fire on a data gap.
        var (def, error) = CustomAlertRuleDefinition.TryParse(
            "{" + CountMetric + ",\"predicate\":{\"op\":\"" + op + "\",\"warnThreshold\":5}}");
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    // A COUNT range band that FIRES on a stalled-collector 0 is rejected (the #3373 range half of the guard).
    [InlineData("outside", 1, 100)] // 0 < lower  => outside fires on 0
    [InlineData("between", 0, 100)] // lower <= 0 <= upper => between fires on 0
    public void CountRangeBandThatFiresOnZero_IsError_StalledCollectorTrap(string op, double lower, double upper)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(CountRangePredicate(op, lower, upper));
        Assert.Null(def);
        Assert.NotNull(error);
    }

    [Theory]
    // A COUNT range band that does NOT fire on 0 is fine: a stalled 0 simply does not breach, so there is no
    // ambiguity to guard against and the rule parses normally.
    [InlineData("outside", 0, 100)] // 0 is the inclusive lower edge => not outside => safe
    [InlineData("between", 1, 100)] // 0 < lower => not between => safe
    public void CountRangeBandThatDoesNotFireOnZero_Parses(string op, double lower, double upper)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(CountRangePredicate(op, lower, upper));
        Assert.Null(error);
        Assert.NotNull(def);
        Assert.True(def!.IsRange);
        Assert.False(def.BreachesOnZero()); // the guard's predicate agrees the band is safe on a stalled 0
        Assert.False(def.IsBreaching(0));   // and a stalled 0 genuinely does not breach
    }

    [Theory]
    // The guard is COUNT-specific: a non-count aggregate (SUM) whose band fires on 0 is NOT rejected, because a
    // stalled collector returns NULL for SUM/AVG (caught by the evaluator's no-data freeze), not a real 0.
    [InlineData("outside", 1, 100)]
    [InlineData("between", 0, 100)]
    public void NonCountRangeBandThatFiresOnZero_Parses(string op, double lower, double upper)
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(SumRangePredicate(op, lower, upper));
        Assert.Null(error);
        Assert.NotNull(def);
        Assert.True(def!.BreachesOnZero()); // the band WOULD fire on 0, but the metric is not a COUNT...
        Assert.True(def.IsBreaching(0));    // ...so the guard leaves it alone.
    }

    [Theory]
    // BreachesOnZero is the band-at-0 verdict for a range op, and it IS IsBreaching(0) — the single authority.
    [InlineData("outside", 1, 100, true)]
    [InlineData("outside", 0, 100, false)]
    [InlineData("between", 0, 100, true)]
    [InlineData("between", 1, 100, false)]
    public void BreachesOnZero_EqualsIsBreachingAtZero_ForRangeOps(string op, double lower, double upper, bool expected)
    {
        // Use the non-count metric so a fires-on-0 band still PARSES (the count guard would reject it otherwise).
        var (def, _) = CustomAlertRuleDefinition.TryParse(SumRangePredicate(op, lower, upper));
        Assert.Equal(expected, def!.BreachesOnZero());
        Assert.Equal(def.IsBreaching(0), def.BreachesOnZero());
    }

    [Fact]
    public void BreachesOnZero_IsFalse_ForScalarOp_EvenWhenItBreachesAtZero()
    {
        // BreachesOnZero is a range-only concept; a scalar op's count trap is the separate '<'/'<=' rejection.
        // 'le 0' on a SUM metric DOES breach at 0, yet BreachesOnZero is false because the op is not a range.
        var (def, _) = CustomAlertRuleDefinition.TryParse(
            "{" + Metric + ",\"predicate\":{\"op\":\"le\",\"warnThreshold\":0}}");
        Assert.True(def!.IsBreaching(0));
        Assert.False(def.BreachesOnZero());
    }
}
