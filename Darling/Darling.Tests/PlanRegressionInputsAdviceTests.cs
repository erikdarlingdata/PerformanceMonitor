/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5630/#5631: the PLAN_REGRESSION advice reads <c>worst_input_basis</c> (0 not checked, 1 both plans compiled for the
/// same values, 2 the statistics changed between the compiles) and claims only what that value shows. When
/// PARAMETER_SENSITIVITY fired beside it, the advice must not tell the reader to force. Rendered through the public
/// <see cref="FactAdvice.Compose"/> entry point, as a reader sees it.
/// </summary>
public sealed class PlanRegressionInputsAdviceTests
{
    private static Fact PlanRegression(Dictionary<string, double>? extra = null)
    {
        var meta = new Dictionary<string, double>
        {
            ["worst_regression_factor"] = 12,
            ["offender_count"] = 2,
            ["latest_cpu_per_exec_us"] = 120_000,
            ["best_cpu_per_exec_us"] = 10_000,
        };
        if (extra is not null)
        {
            foreach (var pair in extra)
                meta[pair.Key] = pair.Value;
        }

        return new Fact { Key = "PLAN_REGRESSION", Severity = 1, Metadata = meta };
    }

    private static Fact PlanRegressionWithBasis(int basis, Dictionary<string, double>? extra = null)
    {
        var meta = new Dictionary<string, double> { ["worst_input_basis"] = basis };
        if (extra is not null)
        {
            foreach (var pair in extra)
                meta[pair.Key] = pair.Value;
        }

        return PlanRegression(meta);
    }

    private static Fact ParameterSensitivity() => new()
    {
        Key = "PARAMETER_SENSITIVITY",
        Severity = 1,
        Metadata = new Dictionary<string, double> { ["worst_ratio"] = 40, ["offender_count"] = 1 },
    };

    private static Fact CpuSpike() => new()
    {
        Key = "CPU_SPIKE",
        Severity = 1,
        Metadata = new Dictionary<string, double> { ["max_sql_cpu"] = 95, ["avg_sql_cpu"] = 30 },
    };

    private static Fact CpuSqlPercent() => new()
    {
        Key = "CPU_SQL_PERCENT",
        Severity = 1,
        Metadata = new Dictionary<string, double> { ["avg_sql_cpu"] = 80, ["max_sql_cpu"] = 95 },
    };

    private static Dictionary<string, Fact> Facts(params Fact[] facts)
    {
        var map = new Dictionary<string, Fact>();
        foreach (var f in facts)
            map[f.Key] = f;
        return map;
    }

    private static AdviceBlock Compose(string key, params Fact[] facts)
    {
        var advice = FactAdvice.Compose(key, Facts(facts));
        Assert.NotNull(advice);
        return advice!;
    }

    // ---- basis 0, 1 and 2 on PLAN_REGRESSION ----

    [Fact]
    public void Basis1_KeepsTheForceAsTheFastFix_AndSaysBothPlansUsedTheSameValues()
    {
        var advice = Compose("PLAN_REGRESSION", PlanRegressionWithBasis(1));

        Assert.Contains("forcing it stops the bleeding immediately", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("both plans were compiled for the same parameter values", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("a plan choice that got worse", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("a plan choice got worse", advice.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void Basis2_PointsAtTheStatistics_AndAsksForOneSlowCallBeforeForcing()
    {
        var advice = Compose("PLAN_REGRESSION", PlanRegressionWithBasis(2));

        Assert.StartsWith("Look at the statistics first", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("confirm it on one slow call first", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("stops the bleeding", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("different parameter values", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("statistics they used changed between the compiles", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("a plan choice that got worse", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("got worse", advice.Headline, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Basis0_SaysTheInputsCouldNotBeCompared_WhetherTheBasisIsZeroOrAbsent(bool present)
    {
        var advice = Compose("PLAN_REGRESSION", present ? PlanRegressionWithBasis(0) : PlanRegression());

        Assert.Contains("could not be compared", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("check them in both plans first", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("forcing the cheaper plan can make the slow calls slower", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("stops the bleeding", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("the cause is not established", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("got worse", advice.Headline, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void NoBasisTextEverSaysToForceOnlyWhenTheValuesMatch(int basis)
    {
        foreach (var facts in new[]
        {
            Facts(PlanRegressionWithBasis(basis)),
            Facts(PlanRegressionWithBasis(basis), ParameterSensitivity()),
            Facts(PlanRegressionWithBasis(basis, new Dictionary<string, double> { ["latest_is_forced"] = 1, ["force_failure_count"] = 3 })),
            Facts(PlanRegressionWithBasis(basis, new Dictionary<string, double> { ["cross_input_excluded_count"] = 2, ["inputs_unverified_count"] = 1 })),
        })
        {
            var all = FactAdvice.Compose("PLAN_REGRESSION", facts);
            Assert.NotNull(all);
            var text = all!.Headline + " " + all.Investigation + " " + all.Remediation;
            Assert.DoesNotContain("only when the values match", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("only when they match", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("until the compiled parameter values match", text, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Theory]
    [InlineData("CPU_SQL_PERCENT")]
    [InlineData("CPU_SPIKE")]
    public void NoCpuTextEverSaysToForceOnlyWhenTheValuesMatch(string rootKey)
    {
        var root = rootKey == "CPU_SPIKE" ? CpuSpike() : CpuSqlPercent();
        var staticRoot = new Fact { Key = rootKey, Severity = 1, Metadata = [] };
        foreach (var facts in new[]
        {
            Facts(root, PlanRegressionWithBasis(1)),
            Facts(root, PlanRegressionWithBasis(1), ParameterSensitivity()),
            Facts(staticRoot, PlanRegressionWithBasis(1)),
            Facts(staticRoot, PlanRegressionWithBasis(1), ParameterSensitivity()),
        })
        {
            var advice = FactAdvice.Compose(rootKey, facts);
            Assert.NotNull(advice);
            var text = advice!.Headline + " " + advice.Investigation + " " + advice.Remediation;
            Assert.DoesNotContain("only when the values match", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("only when they match", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("compare the two plans' compiled parameter values", text, StringComparison.Ordinal);
            Assert.DoesNotContain("compare the compiled parameter values", text, StringComparison.Ordinal);
        }
    }

    // ---- the excluded and unverified counts ----

    [Fact]
    public void TheExcludedCountNamesAllThreeReasons()
    {
        var advice = Compose("PLAN_REGRESSION", PlanRegressionWithBasis(1, new Dictionary<string, double>
        {
            ["cross_input_excluded_count"] = 3,
            ["inputs_unverified_count"] = 1,
        }));

        Assert.Contains("3 other queries were left out", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("recompiles on every call", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("returns many times the rows", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("different parameter values over unchanged statistics", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("1 reported query could not be compared", advice.Remediation, StringComparison.Ordinal);

        var one = Compose("PLAN_REGRESSION", PlanRegressionWithBasis(1, new Dictionary<string, double> { ["cross_input_excluded_count"] = 1 }));
        Assert.Contains("1 other query was left out", one.Investigation, StringComparison.Ordinal);
    }

    // ---- PARAMETER_SENSITIVITY beside PLAN_REGRESSION ----

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void PlanRegressionWithParameterSensitivity_SaysNotToForce_WhateverTheBasis(int basis)
    {
        var advice = Compose("PLAN_REGRESSION", PlanRegressionWithBasis(basis), ParameterSensitivity());

        Assert.Contains("do not force a query that also shows that signature", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("OPTION (RECOMPILE)", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("stops the bleeding", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("so this is a plan choice that got worse", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("parameter sensitivity fired as well", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("got worse", advice.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailingForceStillGetsItsGuidance_WhenParameterSensitivityFiredToo()
    {
        var advice = Compose("PLAN_REGRESSION",
            PlanRegressionWithBasis(1, new Dictionary<string, double> { ["latest_is_forced"] = 1, ["force_failure_count"] = 3 }),
            ParameterSensitivity());

        Assert.Contains("do not force a query that also shows that signature", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("clear the broken force", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("failing to apply", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailingForceWithoutParameterSensitivity_KeepsItsOwnGuidance()
    {
        var advice = Compose("PLAN_REGRESSION",
            PlanRegressionWithBasis(1, new Dictionary<string, double> { ["latest_is_forced"] = 1, ["force_failure_count"] = 3 }));

        Assert.StartsWith("Fix the failing force first", advice.Remediation, StringComparison.Ordinal);
    }

    // ---- the CPU texts ----

    [Fact]
    public void ACpuSpikeWithBothFacts_LeadsWithTheDoNotForceGuidance_AndDoesNotAdviseAForce()
    {
        var advice = Compose("CPU_SPIKE", CpuSpike(), PlanRegressionWithBasis(1), ParameterSensitivity());

        Assert.StartsWith("Parameter sensitivity co-fired — do NOT force a plan", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("forcing it is the fast fix", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("do not force a query that also shows that signature", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ACpuSpikeWithOnlyAPlanRegression_StillAdvisesTheForce()
    {
        var advice = Compose("CPU_SPIKE", CpuSpike(), PlanRegressionWithBasis(1));

        Assert.Contains("forcing it is the fast fix", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ACpuSpikeWithOnlyParameterSensitivity_IsUnchanged()
    {
        var advice = Compose("CPU_SPIKE", CpuSpike(), ParameterSensitivity());

        Assert.StartsWith("Parameter sensitivity co-fired — do NOT force a plan", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("plan regression", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCoFiredCauseList_DoesNotSayToForceWhenParameterSensitivityFired()
    {
        var both = Compose("CPU_SQL_PERCENT", CpuSqlPercent(), PlanRegressionWithBasis(1), ParameterSensitivity());
        var only = Compose("CPU_SQL_PERCENT", CpuSqlPercent(), PlanRegressionWithBasis(1));

        Assert.DoesNotContain("force the historically faster plan", both.Remediation, StringComparison.Ordinal);
        Assert.Contains("do not force a query that also shows the parameter-sensitivity signature", both.Remediation, StringComparison.Ordinal);
        Assert.Contains("force the historically faster plan", only.Remediation, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CPU_SQL_PERCENT")]
    [InlineData("CPU_SPIKE")]
    public void TheCoFiredCauseList_NamesParameterSensitivityBeforeThePlanRegression(string rootKey)
    {
        var root = rootKey == "CPU_SPIKE" ? CpuSpike() : CpuSqlPercent();
        var text = Compose(rootKey, root, PlanRegressionWithBasis(1), ParameterSensitivity()).Remediation;
        var sensitivity = text.IndexOf("parameter sensitivity —", StringComparison.Ordinal);
        var regression = text.IndexOf("a plan regression —", StringComparison.Ordinal);
        if (rootKey == "CPU_SQL_PERCENT")
        {
            Assert.True(sensitivity >= 0, "the cause list names parameter sensitivity");
            Assert.True(regression > sensitivity, "the cause list names the plan regression after it");
        }
        else
        {
            /* The spike's own sentence leads with the sensitivity guidance and does not name a plan regression first. */
            Assert.StartsWith("Parameter sensitivity co-fired", text, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("CPU_SQL_PERCENT")]
    [InlineData("CPU_SPIKE")]
    public void TheStaticCpuText_OnlyAdvisesAForceForAPlanRegressionWithoutParameterSensitivity(string rootKey)
    {
        /* No CPU metadata, so the composer falls back to the fixed text. */
        var text = Compose(rootKey, new Fact { Key = rootKey, Severity = 1, Metadata = [] }, PlanRegressionWithBasis(1), ParameterSensitivity()).Remediation;

        var force = text.IndexOf("force the historically faster plan", StringComparison.Ordinal);
        var doNot = text.IndexOf("do NOT force", StringComparison.OrdinalIgnoreCase);
        var without = text.IndexOf("without it", StringComparison.Ordinal);
        Assert.True(doNot >= 0, "the text says not to force when parameter sensitivity fired");
        Assert.True(force > doNot, "the force sentence comes after the do-not-force sentence");
        Assert.True(without >= 0 && without < force, "the force sentence is for a plan regression without parameter sensitivity");
    }
}
