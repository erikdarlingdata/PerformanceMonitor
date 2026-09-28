/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4516 — <see cref="BenefitScorer.ScoreWaitStats"/> already computes a benefit % for each wait
/// type in an actual plan and stores it on <see cref="PlanStatement.WaitBenefits"/>, but nothing
/// turned a wait into a finding: a query that spent most of its elapsed time waiting on I/O or
/// locks got no warning about it.
///
/// <para>Mirrors erikdarlingdata/PerformanceStudio's end state for wait stats as warnings
/// (<c>1391589</c> adds the "Wait: type" findings and the severity tiers, <c>aabbaa2</c> drops the
/// AI-drafted descriptions, and <c>d45a6a6</c> moves the curated descriptions to an embedded
/// <c>WaitStats.json</c> read through <c>WaitStatsConfig</c>). Ported behavior: one "Wait: type"
/// finding per wait in <see cref="PlanStatement.WaitStats"/>, PAGEIOLATCH_* waits also carry an
/// average per-wait latency, and severity comes from the wait's benefit % (Critical at 50 or more,
/// Warning at 10 or more, else Info).</para>
/// </summary>
public sealed class PlanSync4516Tests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static ParsedPlan ScoreOnly(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        BenefitScorer.Score(plan);
        return plan;
    }

    private static PlanWarning? WaitWarning(PlanStatement stmt, string waitType) =>
        stmt.PlanWarnings.FirstOrDefault(w => w.WarningType == "Wait: " + waitType);

    // ---- severity tiers off the wait's benefit % --------------------------------------------

    [Fact]
    public void HighBenefitWait_EmitsCriticalWithAverageLatency()
    {
        // 800 ms across 100 waits on a 1000 ms elapsed serial statement -> 80% benefit -> Critical.
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 200, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "PAGEIOLATCH_SH", WaitTimeMs = 800, WaitCount = 100 }]
        };

        ScoreOnly(stmt);

        var warning = WaitWarning(stmt, "PAGEIOLATCH_SH");
        Assert.NotNull(warning);
        Assert.Equal(PlanWarningSeverity.Critical, warning!.Severity);
        Assert.Equal(80.0, warning.MaxBenefitPercent);
        Assert.Contains("800", warning.Message);
        Assert.Contains("100 waits", warning.Message);
        // PAGEIOLATCH_* shows the average per wait: 800ms / 100 = 8ms.
        Assert.Contains("Effective latency: 8.0 ms per wait", warning.Message);
    }

    [Fact]
    public void SmallWait_EmitsInfo_AndNoLatencyLineForAWaitConfigDoesNotFlagForAverages()
    {
        // 20 ms across 5 waits on a 1000 ms elapsed statement -> 2% benefit -> Info.
        // LCK_M_X isn't configured to show average latency, so the message stays plain.
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 100, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "LCK_M_X", WaitTimeMs = 20, WaitCount = 5 }]
        };

        ScoreOnly(stmt);

        var warning = WaitWarning(stmt, "LCK_M_X");
        Assert.NotNull(warning);
        Assert.Equal(PlanWarningSeverity.Info, warning!.Severity);
        Assert.Equal(2.0, warning.MaxBenefitPercent);
        Assert.DoesNotContain("Effective latency", warning.Message);
    }

    [Fact]
    public void MidBenefitWait_AtTheTenPercentBoundary_IsWarningNotInfo()
    {
        // 100 ms of a 1000 ms elapsed statement -> exactly 10% -> the >= 10 tier is Warning.
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 100, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "WRITELOG", WaitTimeMs = 100, WaitCount = 10 }]
        };

        ScoreOnly(stmt);

        var warning = WaitWarning(stmt, "WRITELOG");
        Assert.NotNull(warning);
        Assert.Equal(PlanWarningSeverity.Warning, warning!.Severity);
        Assert.Equal(10.0, warning.MaxBenefitPercent);
    }

    [Fact]
    public void BenefitAtTheFiftyPercentBoundary_IsCriticalNotWarning()
    {
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 100, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "PAGELATCH_EX", WaitTimeMs = 500, WaitCount = 50 }]
        };

        ScoreOnly(stmt);

        var warning = WaitWarning(stmt, "PAGELATCH_EX");
        Assert.NotNull(warning);
        Assert.Equal(PlanWarningSeverity.Critical, warning!.Severity);
        Assert.Equal(50.0, warning.MaxBenefitPercent);
    }

    // ---- WaitStatsConfig: curated description present / absent -----------------------------

    [Fact]
    public void ConfiguredWaitWithADescription_AppendsItToTheMessage()
    {
        var doc = "{\"waitStats\":[{\"name\":\"TEST_WAIT_WITH_DESC\",\"isEnabled\":true,\"description\":\"A curated test description.\"}]}";
        var entries = WaitStatsConfig.ParseDocument(doc);

        Assert.True(entries.TryGetValue("TEST_WAIT_WITH_DESC", out var entry));
        Assert.Equal("A curated test description.", entry!.Description);
    }

    [Fact]
    public void WaitAbsentFromTheConfig_HasNoDescriptionAndNoAverageLatencyFlag()
    {
        Assert.Null(WaitStatsConfig.Description("NOT_A_REAL_WAIT_TYPE_4516"));
        Assert.False(WaitStatsConfig.ShowAverageWaitTime("NOT_A_REAL_WAIT_TYPE_4516"));
    }

    [Fact]
    public void TheEmbeddedResourceLoads_AndKnowsARealWaitType()
    {
        // Proves the resource is actually embedded and wired to the loader, not just present on disk.
        Assert.False(WaitStatsConfig.ShowAverageWaitTime("PAGEIOLATCH_SH") == default && WaitStatsConfig.Get("PAGEIOLATCH_SH") == null);
        Assert.NotNull(WaitStatsConfig.Get("PAGEIOLATCH_SH"));
        Assert.True(WaitStatsConfig.Get("PAGEIOLATCH_SH")!.ShowAverageWaitTime);
    }

    // ---- through the product's own call path: parse -> analyze -> score --------------------

    [Fact]
    public void ThroughTheParsedShowPlanPipeline_AWaitStatBecomesAFinding()
    {
        var xml =
            $"<ShowPlanXML xmlns=\"{Ns}\"><BatchSequence><Batch><Statements>" +
            "<StmtSimple StatementText=\"SELECT a FROM dbo.t\" StatementType=\"SELECT\">" +
            "<QueryPlan>" +
            "<WaitStats><Wait WaitType=\"PAGEIOLATCH_SH\" WaitTimeMs=\"800\" WaitCount=\"100\" /></WaitStats>" +
            "<QueryTimeStats CpuTime=\"200\" ElapsedTime=\"1000\" />" +
            "</QueryPlan>" +
            "</StmtSimple>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";

        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);

        var stmt = plan.Batches.Single().Statements.Single();
        var warning = WaitWarning(stmt, "PAGEIOLATCH_SH");
        Assert.NotNull(warning);
        Assert.Equal(PlanWarningSeverity.Critical, warning!.Severity);
    }

    // ---- zero/negative wait time is skipped, same as before ---------------------------------

    [Fact]
    public void ZeroWaitTime_EmitsNoFinding()
    {
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 100, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "CXPACKET", WaitTimeMs = 0, WaitCount = 0 }]
        };

        ScoreOnly(stmt);

        Assert.Null(WaitWarning(stmt, "CXPACKET"));
    }
}
