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
/// #5630: when PARAMETER_SENSITIVITY fired beside PLAN_REGRESSION, the advice must not tell the reader to force the
/// "best" plan. That plan may only have served small inputs, and forcing it made the slow case slower in a scratch test.
/// Rendered through the public <see cref="FactAdvice.Compose"/> entry point, as a reader sees it.
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

    [Fact]
    public void ACpuSpikeWithBothFacts_LeadsWithTheDoNotForceGuidance_AndDoesNotAdviseAForce()
    {
        var advice = FactAdvice.Compose("CPU_SPIKE", Facts(CpuSpike(), PlanRegression(), ParameterSensitivity()));

        Assert.NotNull(advice);
        Assert.StartsWith("Parameter sensitivity co-fired — do NOT force a plan", advice!.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("forcing it is the fast fix", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("compare the two plans' compiled parameter values", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ACpuSpikeWithOnlyAPlanRegression_StillAdvisesTheForce()
    {
        var advice = FactAdvice.Compose("CPU_SPIKE", Facts(CpuSpike(), PlanRegression()));

        Assert.NotNull(advice);
        Assert.Contains("forcing it is the fast fix", advice!.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ACpuSpikeWithOnlyParameterSensitivity_IsUnchanged()
    {
        var advice = FactAdvice.Compose("CPU_SPIKE", Facts(CpuSpike(), ParameterSensitivity()));

        Assert.NotNull(advice);
        Assert.StartsWith("Parameter sensitivity co-fired — do NOT force a plan", advice!.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("compiled parameter values", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCoFiredCauseList_DoesNotSayToForceWhenParameterSensitivityFired()
    {
        var both = FactAdvice.Compose("CPU_SQL_PERCENT", Facts(CpuSqlPercent(), PlanRegression(), ParameterSensitivity()));
        var only = FactAdvice.Compose("CPU_SQL_PERCENT", Facts(CpuSqlPercent(), PlanRegression()));

        Assert.NotNull(both);
        Assert.NotNull(only);
        Assert.DoesNotContain("force the historically faster plan", both!.Remediation, StringComparison.Ordinal);
        Assert.Contains("compare the two plans' compiled parameter values", both.Remediation, StringComparison.Ordinal);
        Assert.Contains("force the historically faster plan", only!.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanRegressionWithParameterSensitivity_SaysToCompareCompiledValues_NotThatForcingStopsTheCost()
    {
        var advice = FactAdvice.Compose("PLAN_REGRESSION", Facts(PlanRegression(), ParameterSensitivity()));

        Assert.NotNull(advice);
        Assert.Contains("do NOT force the cheaper plan yet", advice!.Remediation, StringComparison.Ordinal);
        Assert.Contains("compare the compiled parameter values of the two plans", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("stops the bleeding", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("so this is a plan choice that got worse", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("may have been compiled for different parameter values", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanRegressionWithoutParameterSensitivity_KeepsItsForceAdvice()
    {
        var advice = FactAdvice.Compose("PLAN_REGRESSION", Facts(PlanRegression()));

        Assert.NotNull(advice);
        Assert.Contains("forcing it stops the bleeding immediately", advice!.Remediation, StringComparison.Ordinal);
        Assert.Contains("so this is a plan choice that got worse", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailingForceStillGetsItsGuidance_WhenParameterSensitivityFiredToo()
    {
        var advice = FactAdvice.Compose("PLAN_REGRESSION", Facts(
            PlanRegression(new Dictionary<string, double> { ["latest_is_forced"] = 1, ["force_failure_count"] = 3 }),
            ParameterSensitivity()));

        Assert.NotNull(advice);
        Assert.Contains("do NOT force the cheaper plan yet", advice!.Remediation, StringComparison.Ordinal);
        Assert.Contains("clear the broken force", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("failing to apply", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailingForceWithoutParameterSensitivity_KeepsItsOwnGuidance()
    {
        var advice = FactAdvice.Compose("PLAN_REGRESSION", Facts(
            PlanRegression(new Dictionary<string, double> { ["latest_is_forced"] = 1, ["force_failure_count"] = 3 })));

        Assert.NotNull(advice);
        Assert.StartsWith("Fix the failing force first", advice!.Remediation, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("CPU_SQL_PERCENT")]
    [InlineData("CPU_SPIKE")]
    public void TheCoFiredCauseList_NamesParameterSensitivityBeforeThePlanRegression(string rootKey)
    {
        var root = rootKey == "CPU_SPIKE" ? CpuSpike() : CpuSqlPercent();
        var advice = FactAdvice.Compose(rootKey, Facts(root, PlanRegression(), ParameterSensitivity()));

        Assert.NotNull(advice);
        var text = advice!.Remediation;
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
    public void TheStaticCpuText_NeverSaysToForceBeforeComparingCompiledValues(string rootKey)
    {
        /* No CPU metadata, so the composer falls back to the fixed text. */
        var advice = FactAdvice.Compose(rootKey, Facts(new Fact { Key = rootKey, Severity = 1, Metadata = [] }, PlanRegression(), ParameterSensitivity()));

        Assert.NotNull(advice);
        var text = advice!.Remediation;
        var force = text.IndexOf("force the historically faster plan", StringComparison.Ordinal);
        var doNot = text.IndexOf("do NOT force", StringComparison.OrdinalIgnoreCase);
        var compare = text.IndexOf("compare the compiled parameter values", StringComparison.Ordinal);
        Assert.True(doNot >= 0, "the text says not to force when parameter sensitivity fired");
        Assert.True(compare >= 0, "the text says to compare the compiled values");
        Assert.True(force > compare, "the force sentence comes only after the compare sentence");
        Assert.True(force > doNot, "the force sentence comes only after the do-not-force sentence");
    }

    [Fact]
    public void ThePlanRegressionAdviceCountsWhatTheInputCheckLeftOutAndWhatItCouldNotCheck()
    {
        var advice = FactAdvice.Compose("PLAN_REGRESSION", Facts(PlanRegression(new Dictionary<string, double>
        {
            ["cross_input_excluded_count"] = 3,
            ["inputs_unverified_count"] = 1,
        })));

        Assert.NotNull(advice);
        Assert.Contains("3 other queries compared plans compiled for different parameter values", advice!.Investigation, StringComparison.Ordinal);
        Assert.Contains("1 reported query could not be checked", advice.Remediation, StringComparison.Ordinal);
    }
}
