/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7894: the parallelism advice (CXPACKET, SOS_SCHEDULER_YIELD with CXPACKET co-fired, QUERY_HIGH_DOP, the generic
/// THREADPOOL and the THREADPOOL_PARALLEL guard) reads the server's collected MAXDOP and cost threshold for parallelism, and
/// recommends only a change from what is set. A server already at or past guidance is told so, never "raise cost threshold for
/// parallelism to 50 and cap MAXDOP at 8". When the config facts were not collected the static text stands.
/// <see cref="FactAdvice.Compose"/> is the shared composer both apps run, so the pins live once, here.
/// </summary>
public sealed class ParallelismAdviceSettingsTests
{
    private static Dictionary<string, Fact> Facts(params Fact[] facts) => facts.ToDictionary(f => f.Key, f => f);

    private static Fact Config(string key, double value) => new() { Source = "config", Key = key, Value = value };

    private static Fact Hardware(int coresPerSocket) => new()
    {
        Source = "config",
        Key = "SERVER_HARDWARE",
        Value = coresPerSocket * 2,
        Metadata = new Dictionary<string, double>
        {
            ["cpu_count"] = coresPerSocket * 2,
            ["socket_count"] = 2,
            ["cores_per_socket"] = coresPerSocket,
        }
    };

    private static Fact Fired(string key, Dictionary<string, double>? metadata = null) => new()
    {
        Source = "waits",
        Key = key,
        Value = 1,
        BaseSeverity = 0.8,
        Severity = 0.8,
        Metadata = metadata ?? new Dictionary<string, double>()
    };

    private static Fact Sos() => Fired("SOS_SCHEDULER_YIELD", new Dictionary<string, double>
    {
        ["wait_time_ms"] = 600_000,
        ["avg_ms_per_wait"] = 4,
        ["signal_wait_time_ms"] = 300_000,
    });

    private const string StaticRaise = "raise `cost threshold for parallelism` to 50";
    private const string StaticCap = "cap `MAXDOP` at the logical processors in a single NUMA node";

    /// <summary>CTFP 80 and MAXDOP 4 on 4 cores per socket are past guidance: the CXPACKET card states them and recommends no
    /// change, and the fixed "raise to 50, cap at 8" sentence is gone.</summary>
    [Fact]
    public void Cxpacket_AtCtfp80Maxdop4_StatesTheSettingsAndRecommendsNoChange()
    {
        var advice = FactAdvice.Compose("CXPACKET", Facts(Hardware(4), Config("CONFIG_MAXDOP", 4), Config("CONFIG_CTFP", 80)))!;

        Assert.Contains("MAXDOP is 4", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("cost threshold for parallelism is 80", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("within topology guidance", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticRaise, advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticCap, advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("to 50", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>At the shipped defaults (MAXDOP 0, CTFP 5) the CXPACKET card recommends the change from those values, once, with
    /// the cap read off the server's cores per socket. The static sentence does not repeat it.</summary>
    [Fact]
    public void Cxpacket_AtDefaults_RecommendsTheChangeFromTheValuesOnce()
    {
        var advice = FactAdvice.Compose("CXPACKET", Facts(Hardware(4), Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 5)))!;

        Assert.Contains("raise cost threshold for parallelism to 50", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("cap MAXDOP at 4", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticRaise, advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticCap, advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>With neither config fact collected the static CXPACKET block is what ships, unchanged.</summary>
    [Fact]
    public void Cxpacket_WithoutConfigFacts_KeepsTheStaticText()
    {
        var advice = FactAdvice.Compose("CXPACKET", Facts(Hardware(4)))!;

        Assert.Equal(FactAdvice.GetForFactKey("CXPACKET")!.Remediation, advice.Remediation);
    }

    /// <summary>SOS_SCHEDULER_YIELD with CXPACKET co-fired on a server at CTFP 80 / MAXDOP 4: the parallelism sentence states the
    /// settings and says they are within guidance instead of telling the operator to lower the threshold to 50.</summary>
    [Fact]
    public void Sos_WithCxpacketCoFired_AtCtfp80Maxdop4_RecommendsNoSettingChange()
    {
        var advice = FactAdvice.Compose("SOS_SCHEDULER_YIELD",
            Facts(Sos(), Fired("CXPACKET"), Hardware(4), Config("CONFIG_MAXDOP", 4), Config("CONFIG_CTFP", 80)))!;

        Assert.Contains("CXPACKET co-fired", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("MAXDOP is 4", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("cost threshold for parallelism is 80", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("to 50", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("no more than 8", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>The same card at the shipped defaults recommends the change from those values, with the cap read off the
    /// server's cores per socket rather than a fixed 8.</summary>
    [Fact]
    public void Sos_WithCxpacketCoFired_AtDefaults_RecommendsTheChangeFromTheValues()
    {
        var advice = FactAdvice.Compose("SOS_SCHEDULER_YIELD",
            Facts(Sos(), Fired("CXPACKET"), Hardware(4), Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 5)))!;

        Assert.Contains("MAXDOP is 0", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("raise cost threshold for parallelism to 50", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("cap MAXDOP at 4", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("no more than 8", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>With the config facts absent the SOS card keeps its static parallelism sentence.</summary>
    [Fact]
    public void Sos_WithCxpacketCoFired_WithoutConfigFacts_KeepsTheStaticSentence()
    {
        var advice = FactAdvice.Compose("SOS_SCHEDULER_YIELD", Facts(Sos(), Fired("CXPACKET")))!;

        Assert.Contains("raise cost threshold for parallelism to 50 and cap MAXDOP at the per-NUMA-node processor count (no more than 8)",
            advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>QUERY_HIGH_DOP on a server at guidance (MAXDOP 8, CTFP 50, 8 cores per socket): no cap was recommended, so the card
    /// does not refer to "the cap above". It says the settings are within guidance and points at where else the degree comes from.</summary>
    [Fact]
    public void QueryHighDop_AtGuidance_DoesNotReferToACapItDidNotRecommend()
    {
        var highDop = Fired("QUERY_HIGH_DOP", new Dictionary<string, double> { ["high_dop_query_count"] = 3 });
        var advice = FactAdvice.Compose("QUERY_HIGH_DOP", Facts(highDop, Hardware(8), Config("CONFIG_MAXDOP", 8), Config("CONFIG_CTFP", 50)))!;

        Assert.Contains("3 queries ran", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("within topology guidance", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("The cap above", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("OPTION (MAXDOP", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>QUERY_HIGH_DOP at the defaults keeps the cap sentence, because a cap was recommended.</summary>
    [Fact]
    public void QueryHighDop_AtDefaults_KeepsTheCapSentence()
    {
        var highDop = Fired("QUERY_HIGH_DOP", new Dictionary<string, double> { ["high_dop_query_count"] = 3 });
        var advice = FactAdvice.Compose("QUERY_HIGH_DOP", Facts(highDop, Hardware(8), Config("CONFIG_MAXDOP", 0), Config("CONFIG_CTFP", 5)))!;

        Assert.Contains("cap MAXDOP at 8", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("The cap above", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>The generic THREADPOOL card at CTFP 80 / MAXDOP 4 keeps its blocking half and its collection-gap note, and drops the
    /// fixed parallelism parenthetical in favour of the settings it read.</summary>
    [Fact]
    public void Threadpool_AtCtfp80Maxdop4_DoesNotSayRaiseTo50()
    {
        var advice = FactAdvice.Compose("THREADPOOL", Facts(Hardware(4), Config("CONFIG_MAXDOP", 4), Config("CONFIG_CTFP", 80)))!;

        Assert.Contains("MAXDOP is 4", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("within topology guidance", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticRaise, advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain(StaticCap, advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("Do NOT raise `max worker threads`", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("Note on the data", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>THREADPOOL_PARALLEL with MAXDOP 2 and CTFP 50 on 4 cores per socket: the harder guard would also be 2, so the card
    /// does not say "lowering MAXDOP from 2 to 2". It says MAXDOP is already as low as a harder guard would set it and points at the
    /// threshold and the queries. MAXDOP 1 is the same case: never "from 1 to 2".</summary>
    [Fact]
    public void ThreadpoolParallel_AtMaxdop2Ctfp50_DoesNotSayLoweringFrom2To2()
    {
        var two = FactAdvice.Compose("THREADPOOL_PARALLEL", Facts(Hardware(4), Config("CONFIG_MAXDOP", 2), Config("CONFIG_CTFP", 50)))!;
        Assert.Contains("MAXDOP is 2", two.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("lowering MAXDOP from 2 to 2", two.Remediation, StringComparison.Ordinal);
        Assert.Contains("raising cost threshold for parallelism above 50", two.Remediation, StringComparison.Ordinal);

        var one = FactAdvice.Compose("THREADPOOL_PARALLEL", Facts(Hardware(4), Config("CONFIG_MAXDOP", 1), Config("CONFIG_CTFP", 50)))!;
        Assert.DoesNotContain("lowering MAXDOP from 1 to 2", one.Remediation, StringComparison.Ordinal);
    }

    /// <summary>THREADPOOL_PARALLEL with MAXDOP 8 and CTFP 50 on 8 cores per socket still recommends the harder guard, from 8 to 4.</summary>
    [Fact]
    public void ThreadpoolParallel_AtMaxdop8Ctfp50_RecommendsTheHarderGuard()
    {
        var advice = FactAdvice.Compose("THREADPOOL_PARALLEL", Facts(Hardware(8), Config("CONFIG_MAXDOP", 8), Config("CONFIG_CTFP", 50)))!;

        Assert.Contains("lowering MAXDOP from 8 to 4", advice.Remediation, StringComparison.Ordinal);
    }
}
