/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Darling half of #3540's measurement contract census (#3653). <b>The ten rules, their numbers and their
/// statuses live ONCE, in <c>Lite.Tests/MeasurementContractCensusTests</c>'s summary</b>; this file does not
/// restate the list. It exists because two product trees are out of the Lite census's reach: the <c>lite</c> CI
/// filter does not run <c>Lite.Tests</c> for a change under <c>Darling/PerformanceMonitor.Darling.Storage</c> or
/// <c>Darling/PerformanceMonitor.Darling.Analysis</c>, and <c>CrossAppGuardCiGateTests</c> fails a Lite guard
/// whose read PR CI cannot run (#2839). So the sweeps for rules 1, 2 and 6 run here over exactly those two trees
/// with the same regexes (duplicated deliberately — a cross-project equality pin would itself be a read the gate
/// has to reach), and the one Darling-only artifact the contract governs — the continuous aggregates — is
/// censused here against the real <see cref="TimescaleSupport"/> constants rather than against source text.
///
/// <para><b>What this file holds that the Lite half does not.</b></para>
/// <list type="bullet">
/// <item><description><b>Rule 1's allowance roster.</b> Two <c>PgBaselineProvider</c> arms define their
/// <c>interval_sec</c> alias by passing the stored interval through without <c>NULLIF</c> — and are honest
/// anyway, because they read an interval-honest successor aggregate (#3698) whose own <c>WHERE</c> has
/// already dropped every interval-0 collection. The allowance names the source it rests on, and
/// <see cref="EveryDerivedIntervalAlias_RoutesTheStoredValueThroughNullif_OrNamesTheAggregateThatAlreadyDid"/>
/// asserts that source is a registered successor whose CREATE carries the predicate — so the allowance dies
/// with the fact.</description></item>
/// <item><description><b>Rule 1's zero-fallback roster</b> for these trees, EMPTY and asserted empty: the
/// nine PostgreSQL I/O, database and wait trend rates over a LAG-derived interval and the two
/// <c>PgBaselineProvider</c> wait arms all ended at <c>ELSE 0</c> when this census was written and were
/// rostered as dead text under their statements' <c>IS NOT NULL</c> guards; #3653 retired every one to
/// <c>END</c> (NULL) in the PR after this file landed. The roster is kept declared, in the
/// <c>DeltaFamilyIntervalColumnTests.StillNaked</c> idiom, so a rate arm that ships with <c>ELSE 0</c> again
/// has to name itself here to pass and the diff says so. Two of the eleven were not dead text after all:
/// the baseline arms' LAG fallback for a pre-column collection derives 0 when two collections
/// <c>date_trunc</c> to the same second, and on a PG18 rig that row was rated 0 ms/sec INTO the sample set
/// (three planted collections: mean 66.7 where the rated two say 100). The fix also took Lite's
/// <c>interval_sec &gt; 0</c> into <c>with_rate</c>'s WHERE, because <c>END</c> alone would have carried a
/// NULL rate into <c>clean</c> and <c>COUNT(*)</c>ed it as a sample of nothing (same rig: count 3, mean
/// 100); <c>DarlingAnomalyBaselineTests</c> pins both halves on both texts.</description></item>
/// <item><description><b>Rule 5's roster.</b> Every continuous aggregate that aggregates a delta family's
/// rows either carries <c>sample_interval_seconds IS DISTINCT FROM 0</c>, carries a predicate equivalent to
/// it, or is named in <see cref="RollupsThatAggregateUnknowableRows"/>. Tonight that roster holds the
/// superseded legacy pair (retiring by <see cref="TimescaleSupport.SupersededBaselineRelations"/>'s coverage
/// condition, ~day five) and the three hourly rollups over <c>query_stats</c> / <c>procedure_stats</c>, which
/// were built before the interval reached those tables and cannot be altered in place; a successor under a
/// new name, <c>WITH NO DATA</c> + backfill + coverage-gated retirement, is the #3698 shape and a lane of its
/// own. What the contamination IS on those three is stated in the roster's remarks — not the sums, which a 0
/// leaves alone, but <c>min()</c> and <c>count(*) AS sample_count</c>, which read a restart's (0, 0) row as a
/// quiet sample.</description></item>
/// <item><description><b>Rule 6's C# half, for all four Darling trees.</b> Every C# payload key carrying a
/// per-second name on Service, Viewer, Storage and Analysis is populated from a reader field of the same rate
/// name, from a C# quotient over a measured span, from a rate helper, or from an aggregate over a reader's rate
/// field — or is on <see cref="PayloadKeysUnderAnotherNameThanTheirAlias"/> with the alias or model quotient it
/// rests on (<see cref="EveryPerSecondPayloadKey_IsAReaderRateOrACSharpQuotient_OrIsNamed"/>). The Lite half
/// sweeps Service and Viewer for the SQL rules but NOT for this one: a Service tool's key copies a reader field
/// that a <c>Darling.Storage</c> alias populated (<c>DarlingMcpPgTrendTools</c> over <c>DarlingPgTrendReader</c>),
/// and only a census that reads both trees can check the copy against its alias. The <c>const</c> keys those
/// stamps write through (<c>PgTargetScorer.IoOpsPerSecKey</c>) are declared in the shared
/// <c>PerformanceMonitor.Analysis</c>, which this half reads for the constants alone — the <c>core</c> filter
/// runs this suite on a change there.</description></item>
/// </list>
/// </summary>
public sealed class MeasurementContractCensusTests
{
    /// <summary>The two trees the Lite census cannot reach; everything else is swept there.</summary>
    private static readonly string[] SweptTrees =
    {
        "Darling/PerformanceMonitor.Darling.Analysis",
        "Darling/PerformanceMonitor.Darling.Storage",
    };

    /// <summary>The trees rule 6's C# half censuses HERE: all four Darling product trees, because a Service tool's
    /// payload key copies a reader field a Storage alias populated and the copy has to be checked against the
    /// alias. The Lite half censuses Lite and the shared libraries, whose aliases are Lite's own.</summary>
    private static readonly string[] PayloadKeyTrees =
    {
        "Darling/PerformanceMonitor.Darling.Analysis",
        "Darling/PerformanceMonitor.Darling.Service",
        "Darling/PerformanceMonitor.Darling.Storage",
        "Darling/PerformanceMonitor.Darling.Viewer",
    };

    /// <summary>Where the <c>const string</c> keys the Darling stamps write through are DECLARED: the four trees
    /// plus the shared <c>PerformanceMonitor.Analysis</c> (<c>PgTargetScorer.*PerSecKey</c>, <c>MetricNames</c>).
    /// Read for constant definitions only; no offender can come from the shared tree here, the Lite half
    /// sweeps its keys.</summary>
    private static readonly string[] PerSecondKeyConstantTrees =
    {
        "Darling/PerformanceMonitor.Darling.Analysis",
        "Darling/PerformanceMonitor.Darling.Service",
        "Darling/PerformanceMonitor.Darling.Storage",
        "Darling/PerformanceMonitor.Darling.Viewer",
        "PerformanceMonitor.Analysis",
    };

    /* ---------------- rule 1 (the same regexes as the Lite half) ---------------- */

    private static readonly Regex BareIntervalDenominator = new(
        @"/(?:\s*(?:\(|(?!NULLIF\s*\()[A-Za-z_]\w*\s*\())*\s*(?:\w+\.)?sample_interval_seconds\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex NullifIntervalDenominator = new(
        @"NULLIF\(\s*(?:MAX\(\s*)?(?:\w+\.)?sample_interval_seconds\s*\)?\s*,\s*0\s*\)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex DerivedIntervalAlias = new(
        @"\bAS\s+(?:\w+\.)?interval_sec(?:onds)?\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex IntervalIsNullTest = new(
        @"(?:MAX\(\s*)?(?:\w+\.)?sample_interval_seconds\s*\)?\s+IS\s+NULL\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex StoredIntervalMention = new(
        @"\bsample_interval_seconds\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex DivisionByDerivedInterval = new(
        @"/\s*(?:\w+\.)?interval_sec(?:onds)?\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex DerivedIntervalPositiveGuard = new(
        @"\b(?:\w+\.)?interval_sec(?:onds)?\s*>\s*0\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex ZeroFallbackTail = new(
        @"ELSE\s+0\s+END\s+AS\s+(?<alias>\w+)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex GuardedRateArmOpener = new(
        @"CASE\s+WHEN\s+(?:\w+\.)?interval_sec(?:onds)?\s*>\s*0\s+THEN",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The table a select item reads, for naming an allowance by its source rather than by a line.</summary>
    private static readonly Regex FromTable = new(
        @"\bFROM\s+(?:collect\.)?(\w+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Rule 1's allowance: derived interval aliases whose definition passes the stored value through WITHOUT
    /// <c>NULLIF</c>, named by the file and the source relation the definition reads. Both are
    /// <c>PgBaselineProvider</c> arms re-pointed by #3698 at the interval-honest successors: the BatchRequests
    /// arm (<c>COALESCE(sample_interval_seconds::DOUBLE PRECISION, LAG…)</c> over <c>perfmon_interval_baseline</c>)
    /// and the WaitMsPerSec arm (<c>CASE … ELSE sample_interval_seconds END</c> over
    /// <c>wait_stats_interval_baseline</c>). The successor's CREATE carries
    /// <c>sample_interval_seconds IS DISTINCT FROM 0</c>, so the value the arm reads is NULL (pre-column) or
    /// positive and the passthrough cannot see a 0 — the census asserts that predicate on the named source
    /// below, so re-pointing either arm at raw would red this list rather than silently reopen the hole.
    /// Shrink-only: routing the two through <c>NULLIF</c> anyway (belt over the aggregate's braces) is a
    /// one-line product edit that empties this.
    /// </summary>
    private static readonly (string File, string Source)[] IntervalAliasPassthroughsDefendedByTheAggregatePredicate =
    {
        ("Darling/PerformanceMonitor.Darling.Analysis/PgBaselineProvider.cs", TimescaleSupport.PerfmonIntervalBaselineView),
        ("Darling/PerformanceMonitor.Darling.Analysis/PgBaselineProvider.cs", TimescaleSupport.WaitStatsIntervalBaselineView),
    };

    /// <summary>
    /// Rate arms on these trees whose CASE falls back to <c>ELSE 0</c>, as <c>file: alias</c>, duplicates
    /// kept. EMPTY since the #3653 rate-arm PR; until then it named eleven: the two <c>PgBaselineProvider</c>
    /// <c>ms_per_sec</c> arms (the successor-backed and the legacy text of one metric) and
    /// <c>DarlingPgTrendReader</c>'s <c>estimated_wait_ms_per_second</c>, <c>extends_per_second</c>,
    /// <c>hits_per_second</c>, <c>read_bytes_per_second</c>, <c>reads_per_second</c>,
    /// <c>temp_bytes_per_second</c>, <c>transactions_per_second</c>, <c>write_bytes_per_second</c> and
    /// <c>writes_per_second</c> — each now <c>END</c>. Kept declared, and asserted empty below, so a new one
    /// has to name itself here and the diff says so; the honest end state is this list staying empty.
    /// </summary>
    private static readonly string[] RateArmsThatFallBackToZero = Array.Empty<string>();

    [Fact]
    public void NoReaderDividesByTheStoredIntervalBare()
    {
        Assert.Matches(BareIntervalDenominator, "delta_cntr_value * 1.0 / sample_interval_seconds");
        Assert.Matches(BareIntervalDenominator, "x / MAX(sample_interval_seconds)");
        Assert.DoesNotMatch(BareIntervalDenominator, "x / NULLIF(MAX(sample_interval_seconds), 0)");

        var offenders = new List<string>();
        var sanctioned = 0;
        var mentions = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            mentions += StoredIntervalMention.Matches(sql).Count;
            sanctioned += NullifIntervalDenominator.Matches(sql).Count;
            foreach (Match m in BareIntervalDenominator.Matches(sql))
            {
                offenders.Add($"{file}:{line + LineOffset(sql, m.Index)}: {Snippet(sql, m.Index)}");
            }
        }

        Assert.True(mentions >= 40, $"the sweep found only {mentions} mentions of sample_interval_seconds in these trees' SQL — it is not reading the readers");
        Assert.True(sanctioned >= 5, $"only {sanctioned} NULLIF(sample_interval_seconds, 0) denominators found — the floor that makes an empty offender list mean something");
        Assert.Empty(offenders);
    }

    [Fact]
    public void EveryDerivedIntervalAlias_RoutesTheStoredValueThroughNullif_OrNamesTheAggregateThatAlreadyDid()
    {
        var passthroughs = new List<(string File, string Source)>();
        var offenders = new List<string>();
        var definitions = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            foreach (Match alias in DerivedIntervalAlias.Matches(sql))
            {
                definitions++;
                var expression = SelectItemBefore(sql, alias.Index);
                if (!StoredIntervalMention.IsMatch(expression))
                {
                    continue;
                }

                var valueMentions = IntervalIsNullTest.Replace(expression, string.Empty);
                var total = StoredIntervalMention.Matches(valueMentions).Count;
                var routed = NullifIntervalDenominator.Matches(valueMentions).Count;
                if (total == routed)
                {
                    continue;
                }

                var from = FromTable.Match(sql, alias.Index);
                if (!from.Success)
                {
                    offenders.Add($"{file}:{line + LineOffset(sql, alias.Index)}: passthrough with no FROM to name: {Collapse(expression)}");
                    continue;
                }

                passthroughs.Add((file, from.Groups[1].Value));
            }
        }

        Assert.True(definitions >= 12, $"only {definitions} derived interval aliases found — the sweep is not reading the readers");
        Assert.Empty(offenders);

        /* Both directions: the allowance describes exactly the passthroughs that exist. */
        Assert.Equal(
            IntervalAliasPassthroughsDefendedByTheAggregatePredicate.OrderBy(p => p.File, StringComparer.Ordinal).ThenBy(p => p.Source, StringComparer.Ordinal),
            passthroughs.OrderBy(p => p.File, StringComparer.Ordinal).ThenBy(p => p.Source, StringComparer.Ordinal));

        /* And each allowance rests on a fact this asserts: its source is a registered successor whose CREATE
           excludes the interval-0 collection. Re-point the arm at raw, or drop the predicate from the
           aggregate, and the allowance goes red with it. */
        var registered = TimescaleSupport.BaselineAggregates.ToDictionary(a => a.View, a => a.CreateSql, StringComparer.Ordinal);
        foreach (var (_, source) in IntervalAliasPassthroughsDefendedByTheAggregatePredicate)
        {
            Assert.Contains(source, TimescaleSupport.SupersededBaselineRelations.Select(s => s.Successor));
            Assert.True(registered.TryGetValue(source, out var createSql), $"{source} is not a registered baseline aggregate");
            Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", createSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryDivisionByADerivedInterval_IsGuardedAbovePositive_AndEveryZeroFallbackIsNamed()
    {
        var unguarded = new List<string>();
        var zeroFallbacks = new List<string>();
        var divisions = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            foreach (Match division in DivisionByDerivedInterval.Matches(sql))
            {
                divisions++;
                var item = SelectItemBefore(sql, division.Index);
                if (!GuardedRateArmOpener.IsMatch(item) && !DerivedIntervalPositiveGuard.IsMatch(sql))
                {
                    unguarded.Add($"{file}:{line + LineOffset(sql, division.Index)}: {Snippet(sql, division.Index)}");
                }
            }

            foreach (Match tail in ZeroFallbackTail.Matches(sql))
            {
                var item = SelectItemBefore(sql, tail.Index);
                if (GuardedRateArmOpener.IsMatch(item) && DivisionByDerivedInterval.IsMatch(item))
                {
                    zeroFallbacks.Add($"{file}: {tail.Groups["alias"].Value}");
                }
            }
        }

        Assert.True(divisions >= 15, $"only {divisions} divisions by a derived interval alias found — the sweep is not reading the readers");
        Assert.Empty(unguarded);

        /* Rule 1's quotient arm is complete on these trees: nothing falls back to 0. A rate arm added to the
           roster has to be a deliberate diff, and the both-directions equality below is what makes forgetting
           to remove a retired one loud. */
        Assert.Empty(RateArmsThatFallBackToZero);
        Assert.Equal(
            RateArmsThatFallBackToZero.OrderBy(s => s, StringComparer.Ordinal),
            zeroFallbacks.OrderBy(s => s, StringComparer.Ordinal));
    }

    /* ---------------- rule 2 ---------------- */

    private const string CadenceLiterals = @"(?:60|300|900|1800|3600|86400)(?:\.0+)?";

    private static readonly Regex SqlDeltaOverCadence = new(
        @"\bdelta_\w+\b[^,;\n/]*?/\s*" + CadenceLiterals + @"\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex CSharpDeltaOverCadence = new(
        @"\b\w*[Dd]elta\w*\b\s*\)?\s*/\s*" + CadenceLiterals + @"[dDmMfF]?\b",
        RegexOptions.Compiled);

    private static readonly Regex SqlDeltaOverMeasured = new(
        @"\bdelta_\w+\b[^,;]*?/\s*(?:NULLIF\(|(?:\w+\.)?interval_sec(?:onds)?\b)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    [Fact]
    public void NoRate_DividesADeltaByAnAssumedCadence()
    {
        Assert.Matches(SqlDeltaOverCadence, "SUM(delta_wait_time_ms) / 60.0 AS ms_per_min");
        Assert.Matches(CSharpDeltaOverCadence, "var rate = row.DeltaValue / 60.0;");
        Assert.DoesNotMatch(SqlDeltaOverCadence, "SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms");

        var offenders = new List<string>();
        var measured = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            measured += SqlDeltaOverMeasured.Matches(sql).Count;
            foreach (Match m in SqlDeltaOverCadence.Matches(sql))
            {
                offenders.Add($"{file}:{line + LineOffset(sql, m.Index)}: SQL: {Snippet(sql, m.Index)}");
            }
        }

        foreach (var (file, code) in CodeBodies())
        {
            foreach (Match m in CSharpDeltaOverCadence.Matches(code))
            {
                offenders.Add($"{file}:{1 + LineOffset(code, m.Index)}: C#: {Snippet(code, m.Index)}");
            }
        }

        Assert.True(measured >= 6, $"only {measured} deltas divided by a measured interval found — the sweep is not reading the readers");
        Assert.Empty(offenders);
    }

    /* ---------------- rule 6 ---------------- */

    private static readonly Regex PerSecondAlias = new(
        @"\bAS\s+(\w+_per_sec(?:ond)?)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex PerSecondName = new(
        @"\b\w+_per_sec(?:ond)?\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    [Fact]
    public void EveryPerSecondAlias_IsAQuotient()
    {
        var offenders = new List<string>();
        var aliases = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            foreach (Match alias in PerSecondAlias.Matches(sql))
            {
                aliases++;
                var item = SelectItemBefore(sql, alias.Index);
                var divides = item.Contains('/', StringComparison.Ordinal);
                var windowsOverARate = PerSecondName.Matches(item).Any(n => !string.Equals(n.Value, alias.Groups[1].Value, StringComparison.OrdinalIgnoreCase));
                if (!divides && !windowsOverARate)
                {
                    offenders.Add($"{file}:{line + LineOffset(sql, alias.Index)}: {alias.Groups[1].Value} <= {Collapse(item)}");
                }
            }
        }

        Assert.True(aliases >= 15, $"only {aliases} per-second aliases found — the sweep is not reading the readers");
        Assert.Empty(offenders);
    }

    /* ---------------- rule 6, the C# half (the Lite half's regexes, verbatim) ---------------- */

    private static readonly Regex IdentifierPayloadKey = new(
        @"(?<![\w.])(?<key>\w+_per_sec(?:ond)?)\s*=(?![=>])", RegexOptions.Compiled);

    private static readonly Regex QuotedPayloadKey = new(
        @"\[\s*""(?<key>\w+_per_sec(?:ond)?)""\s*\]\s*=(?![=>])", RegexOptions.Compiled);

    private static readonly Regex AddedPayloadKey = new(
        @"\.(?:Try)?Add\(\s*""(?<key>\w+_per_sec(?:ond)?)""\s*,", RegexOptions.Compiled);

    private static readonly Regex PerSecondKeyConstant = new(
        @"\bconst\s+string\s+(?<name>\w+)\s*=\s*""(?<key>\w+_per_sec(?:ond)?)""\s*;", RegexOptions.Compiled);

    private static readonly Regex ConstantIndexerKey = new(
        @"\[\s*(?:\w+\s*\.\s*)*(?<name>\w+)\s*\]\s*=(?![=>])", RegexOptions.Compiled);

    private static readonly Regex ConstantAddedKey = new(
        @"\.(?:Try)?Add\(\s*(?:\w+\s*\.\s*)*(?<name>\w+)\s*,", RegexOptions.Compiled);

    private static readonly Regex DivisionByAMeasuredSpan = new(
        @"/\s*\(?\s*(?:\(\s*(?:double|decimal|float|long|int)\??\s*\)\s*)?(?:\w+\s*\.\s*)*\w*(?:[Ii]nterval|[Ee]lapsed|[Ss]econds|[Oo]bserved)\w*",
        RegexOptions.Compiled);

    private static readonly Regex DivisionByACadenceLiteral = new(
        @"/\s*" + CadenceLiterals + @"[dDmMfF]?\b", RegexOptions.Compiled);

    private static readonly Regex RateHelperCall = new(
        @"\b(?<helper>\w*PerSec(?:ond)?)\s*\((?<args>[^()]*(?:\([^()]*\)[^()]*)*)\)", RegexOptions.Compiled);

    private static readonly Regex RateHelperDefinition = new(
        @"\bstatic\s+(?:double|decimal|float)\??\s+(?<helper>\w*PerSec(?:ond)?)\s*\((?<parameters>[^)]*)\)\s*(?<arrow>=>)?",
        RegexOptions.Compiled);

    private static readonly Regex StoredDeltaName = new(
        @"\b(?:\w+\s*\.\s*)?(?:[Dd]elta\w*|\w+[Dd]elta)\b|\bdelta_\w+", RegexOptions.Compiled);

    private static readonly Regex RateNamedMember = new(
        @"\.\s*(?<member>\w+PerSec(?:ond)?)\b(?!\s*\()", RegexOptions.Compiled);

    private static readonly Regex AnyMember = new(
        @"\.\s*(?<member>[A-Za-z_]\w*)\b(?!\s*\()", RegexOptions.Compiled);

    private static readonly Regex AggregateOverAMember = new(
        @"\.(?:Max|Min|Average)\(\s*\w+\s*=>\s*\w+\s*\.\s*(?<member>\w+PerSec(?:ond)?)\b", RegexOptions.Compiled);

    /// <summary>
    /// Payload keys on <see cref="PayloadKeyTrees"/> whose value is NOT the reader field of their own name and
    /// NOT a C# quotient in the stamping expression — <c>file: key</c>, each with the one fact it rests on, so
    /// the list can only shrink deliberately and a new key of this shape has to name itself and its fact.
    /// <list type="bullet">
    /// <item><description><c>current_ms_per_sec</c> on the three wait-profile detectors (<c>PgAnomalyDetector</c>,
    /// <c>PgTargetAnomalyDetector</c>, <c>PgTargetAnomalyDetector.WaitsSampled</c>): the story's name for the
    /// window's PEAK, which each detector's SQL aliases <c>peak_ms_per_sec</c> and reads into the local the stamp
    /// copies; #3741 kept <c>current</c> as the peak when it put the window mean (<c>avg_ms_per_sec</c> /
    /// <c>mean_ms_per_sec</c>, which ARE their aliases' names) beside it. Rests on <c>peak_ms_per_sec</c>,
    /// asserted present in each file's SQL.</description></item>
    /// <item><description><c>DarlingMcpTrendTools.cs: elapsed_ms_per_second</c> — <c>p.Value</c>: the duration
    /// trend point's <c>Value</c> IS the elapsed-ms-per-second alias (#3541, in the tool's own remarks; the payload
    /// carries <c>value</c> beside it as the same quantity). Rests on <c>elapsed_ms_per_second</c>, asserted
    /// present on the trees' SQL (<c>DurationTrendRouting</c>, both tiers).</description></item>
    /// <item><description><c>DarlingMcpStallProbeTools.cs: trigger_mb_per_second</c> — <c>p.TriggerMbPerSecond</c>,
    /// a member the reader model COMPUTES (<c>DarlingStallProbeReader</c>: bytes over <c>TriggerElapsedMs</c>,
    /// "derived here rather than stored, so the stored row keeps only measurements"). No alias: the quotient is
    /// one hop from the key, in the model, and the census asserts that member's body divides by a measured
    /// span.</description></item>
    /// </list>
    /// </summary>
    private static readonly (string File, string Key, string? Alias, string? QuotientIn)[] PayloadKeysUnderAnotherNameThanTheirAlias =
    {
        ("Darling/PerformanceMonitor.Darling.Analysis/PgAnomalyDetector.cs", "current_ms_per_sec", "peak_ms_per_sec", null),
        ("Darling/PerformanceMonitor.Darling.Analysis/PgTargetAnomalyDetector.WaitsSampled.cs", "current_ms_per_sec", "peak_ms_per_sec", null),
        ("Darling/PerformanceMonitor.Darling.Analysis/PgTargetAnomalyDetector.cs", "current_ms_per_sec", "peak_ms_per_sec", null),
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStallProbeTools.cs", "trigger_mb_per_second", null, "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingStallProbeReader.cs: TriggerMbPerSecond"),
        ("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs", "elapsed_ms_per_second", "elapsed_ms_per_second", null),
    };

    /// <summary>
    /// Rule 6, the C# half, over all four Darling trees — the Lite half's census with the Lite half's bounds
    /// (a reader field is judged by NAME, a bare local is followed ONE assignment up), stated once there. What
    /// this half adds is the reach: the Service tools' keys are checked against the Storage aliases that
    /// populate their reader fields, which is the copy the Lite half's header called "not censused here" until
    /// this landed.
    /// </summary>
    [Fact]
    public void EveryPerSecondPayloadKey_IsAReaderRateOrACSharpQuotient_OrIsNamed()
    {
        var sources = ProductSources(PayloadKeyTrees, PayloadKeyTreesFileFloor).ToList();
        var vocabulary = PerSecondAliasVocabulary(sources);
        Assert.True(vocabulary.Count >= 20, $"only {vocabulary.Count} distinct per-second aliases on the four trees' SQL — the vocabulary is not reading the readers");

        var constants = PerSecondKeyConstants(ProductSources(PerSecondKeyConstantTrees, PayloadKeyTreesFileFloor));
        Assert.True(constants.Count >= 4, $"only {constants.Count} per-second key constants found — PgTargetScorer alone declares four");
        Assert.Equal("ops_per_sec", constants["IoOpsPerSecKey"]);

        var sites = new List<PayloadKeySite>();
        foreach (var (file, text) in sources)
        {
            sites.AddRange(PayloadKeySites(file, text, constants, vocabulary));
        }

        Assert.True(sites.Count >= 35, $"only {sites.Count} per-second payload keys found on {PayloadKeyTrees.Length} trees (42 when this landed) — the sweep is not reading the tools");

        var offenders = sites.Where(s => s.Verdict.StartsWith("OFFENDER", StringComparison.Ordinal)).Select(s => s.ToString()).ToList();
        Assert.True(offenders.Count == 0, "per-second payload key(s) whose value is a lie:\n  " + string.Join("\n  ", offenders));

        /* Both directions: the roster is exactly the keys whose provenance is not the alias of their own name —
           the ones it names (Rostered) and any it does not yet (UnderAnotherName), so a new key of the shape
           fails here naming itself, and a retired one fails here until its row leaves. */
        var underAnotherName = sites.Where(s => s.Verdict is UnderAnotherName or Rostered).Select(s => (s.File, s.Key)).OrderBy(s => s.File, StringComparer.Ordinal).ThenBy(s => s.Key, StringComparer.Ordinal).ToList();
        Assert.Equal(
            PayloadKeysUnderAnotherNameThanTheirAlias.Select(r => (r.File, r.Key)).OrderBy(s => s.File, StringComparer.Ordinal).ThenBy(s => s.Key, StringComparer.Ordinal),
            underAnotherName);

        foreach (var (file, key, alias, quotientIn) in PayloadKeysUnderAnotherNameThanTheirAlias)
        {
            Assert.True((alias is null) != (quotientIn is null), $"{file}: {key} must rest on exactly one of an alias or a model quotient");
            if (alias is not null)
            {
                Assert.Contains(alias, vocabulary);
                if (alias != key)
                {
                    Assert.Contains(alias, PerSecondAliasVocabulary(new[] { (file, RepoFile.ReadRepoFile(file)) }));
                }
            }
            else
            {
                var colon = quotientIn!.IndexOf(':', StringComparison.Ordinal);
                var (modelFile, member) = (quotientIn[..colon], quotientIn[(colon + 1)..].Trim());
                var modelCode = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(modelFile));
                var definition = Regex.Match(modelCode, @"\b" + Regex.Escape(member) + @"\s*=>");
                Assert.True(definition.Success, $"{modelFile}: no computed member {member} for {file}: {key} to rest on");
                var body = modelCode[(definition.Index + definition.Length)..(modelCode.IndexOf(';', definition.Index) + 1)];
                Assert.True(DivisionByAMeasuredSpan.IsMatch(body) && !DivisionByACadenceLiteral.IsMatch(body), $"{modelFile}: {member} does not divide by a measured span: {Collapse(body)}");
            }
        }

        /* The verdicts that DID fire, per kind, so a census where every key came out one way cannot pass — and
           the cross-tree copy this half exists for really was judged: a Service tool's key resolved against a
           Storage alias. */
        Assert.Contains(sites, s => s.Verdict == ReaderAlias && s.File.Contains("/Mcp/DarlingMcpPgTrendTools.cs", StringComparison.Ordinal));
        Assert.Contains(sites, s => s.Verdict == Quotient && s.File.Contains("Darling.Analysis/", StringComparison.Ordinal));
        Assert.Contains(sites, s => s.Verdict == AggregateOverARate);
        Assert.Contains(sites, s => s.Verdict == Rostered);
        Assert.Contains(sites, s => s.Key == "ops_per_sec" && s.Verdict == Quotient);
    }

    /// <summary>The Lite half's fixture control, verbatim: every shape the census judges with the verdict it must
    /// draw, the planted <c>planted_per_sec = row.StoredDelta</c> among them. Duplicated with the regexes: the
    /// two halves are the same instrument and have to fail the same way.</summary>
    [Fact]
    public void ThePayloadKeyVerdicts_FireOnEveryShape_AndThePlantedPassthroughFails()
    {
        const string Fixture = """
            internal static class Keys { public const string ConstPerSecKey = "constant_per_sec"; }
            static double? PerSecond(long? delta, int? seconds) => delta is long d && seconds > 0 ? d / (double)seconds : null;
            var peak = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var bytesPerSec = tempBytes / observedSeconds;
            var copied = row.DeltaBytes;
            var stamped = new Dictionary<string, double>
            {
                ["honest_per_sec"] = peak,
                ["hopped_per_sec"] = bytesPerSec,
                ["copied_per_sec"] = copied,
                ["planted_per_sec"] = row.StoredDelta,
                ["assumed_per_sec"] = row.DeltaValue / 60.0,
                [Keys.ConstPerSecKey] = ops / observedSeconds,
                ["unknown_per_sec"] = something,
            };
            stamped.Add("added_per_sec", total / elapsed.TotalSeconds);
            var page = new
            {
                divided_per_second = Math.Round(p.DeltaWaitMs / (double)p.IntervalSeconds, 2),
                helped_per_second = PerSecond(r.DeltaSpins, r.SampleIntervalSeconds),
                aliased_per_second = p.AliasedPerSecond is { } a ? Math.Round(a, 3) : (double?)null,
                swapped_per_second = p.OtherPerSecond,
                peak_aliased_per_second = points.Max(p => p.AliasedPerSecond) is { } m ? Math.Round(m, 3) : (double?)null,
                halved_per_second = total / count,
                renamed_per_second = p.Value,
                mapped_per_second = "a_table",
                compared = ms_per_sec == 0,
            };
            """;

        var vocabulary = new HashSet<string>(StringComparer.Ordinal) { "honest_per_sec", "aliased_per_second", "renamed_per_second", "swapped_per_second" };
        var constants = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match m in PerSecondKeyConstant.Matches(BlankComments(Fixture)))
        {
            constants[m.Groups["name"].Value] = m.Groups["key"].Value;
        }

        Assert.Equal(new Dictionary<string, string> { ["ConstPerSecKey"] = "constant_per_sec" }, constants);

        var verdicts = PayloadKeySites("Fixture.cs", Fixture, constants, vocabulary)
            .Select(s => (s.Key, Kind: s.Verdict.Split(':')[0]))
            .ToList();

        Assert.Equal(
            new[]
            {
                ("honest_per_sec", ReaderAlias),
                ("hopped_per_sec", Quotient),
                ("copied_per_sec", "OFFENDER"),
                ("planted_per_sec", "OFFENDER"),
                ("assumed_per_sec", "OFFENDER"),
                ("constant_per_sec", Quotient),
                ("unknown_per_sec", UnderAnotherName),
                ("added_per_sec", Quotient),
                ("divided_per_second", Quotient),
                ("helped_per_second", RateHelper),
                ("aliased_per_second", ReaderAlias),
                ("swapped_per_second", UnderAnotherName),
                ("peak_aliased_per_second", AggregateOverARate),
                ("halved_per_second", "OFFENDER"),
                ("renamed_per_second", UnderAnotherName),
                ("mapped_per_second", MapEntry),
            },
            verdicts);

        Assert.DoesNotContain(verdicts, v => v.Key == "ms_per_sec");
        Assert.Empty(PayloadKeySites("Sql.cs", "var sql = @\"SELECT 1 FROM t WHERE ms_per_sec = 0 AND x_per_second = 2\";", constants, vocabulary));

        /* The helper DEFINITION regex, on the shape Lite writes — these trees carry no helper today, so the pin
           below has no population of its own and this is what keeps its regex alive. */
        var definition = RateHelperDefinition.Match("private static double? PerSecond(long? delta, int? sampleIntervalSeconds) =>");
        Assert.True(definition.Success && definition.Groups["arrow"].Success);
    }

    /// <summary>Rule 6's C# half, the helper pin: every rate helper on the four trees divides by a parameter naming
    /// a measured span. None exists here today (Darling's latch and spinlock tools read their rates off the
    /// reader's SQL aliases; Lite's compute them in <c>McpLatchSpinlockTools.PerSecond</c>) — the fixture
    /// control above keeps the definition regex alive, and a helper that arrives here is judged on arrival.</summary>
    [Fact]
    public void EveryRateHelper_DividesByTheIntervalItIsHanded()
    {
        var offenders = new List<string>();
        foreach (var (file, text) in ProductSources(PayloadKeyTrees, PayloadKeyTreesFileFloor))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);
            foreach (Match m in RateHelperDefinition.Matches(code))
            {
                var body = m.Groups["arrow"].Success
                    ? code[(m.Index + m.Length)..(code.IndexOf(';', m.Index + m.Length) + 1)]
                    : CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', m.Index + m.Length));
                if (!DivisionByAMeasuredSpan.IsMatch(body) || DivisionByACadenceLiteral.IsMatch(body))
                {
                    offenders.Add($"{file}:{1 + LineOffset(code, m.Index)}: {m.Groups["helper"].Value} does not divide by a measured span: {Collapse(body)}");
                }
            }
        }

        Assert.Empty(offenders);
    }

    private const string Quotient = "quotient";
    private const string RateHelper = "rate-helper";
    private const string ReaderAlias = "reader-alias";
    private const string AggregateOverARate = "aggregate-over-a-rate";
    private const string MapEntry = "map-entry";
    private const string UnderAnotherName = "under-another-name";
    private const string Rostered = "rostered";

    private sealed record PayloadKeySite(string File, int Line, string Key, string Verdict, string Value)
    {
        public override string ToString() => $"{File}:{Line}: {Key} = {Collapse(Value)} — {Verdict}";
    }

    private static IEnumerable<PayloadKeySite> PayloadKeySites(string file, string text, IReadOnlyDictionary<string, string> constants, ISet<string> vocabulary)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var kept = BlankComments(text);
        var sites = new List<(int Index, string Key, int ValueStart)>();

        foreach (Match m in IdentifierPayloadKey.Matches(code))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length));
        }

        foreach (Match m in QuotedPayloadKey.Matches(kept))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length));
        }

        foreach (Match m in AddedPayloadKey.Matches(kept))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length));
        }

        foreach (Match m in ConstantIndexerKey.Matches(code))
        {
            if (constants.TryGetValue(m.Groups["name"].Value, out var key))
            {
                sites.Add((m.Index, key, m.Index + m.Length));
            }
        }

        foreach (Match m in ConstantAddedKey.Matches(code))
        {
            if (constants.TryGetValue(m.Groups["name"].Value, out var key))
            {
                sites.Add((m.Index, key, m.Index + m.Length));
            }
        }

        foreach (var (index, key, valueStart) in sites.OrderBy(s => s.Index))
        {
            var value = ValueExpression(code, valueStart);
            var verdict = Verdict(key, value, code, index, vocabulary, hops: 1);
            if (verdict == UnderAnotherName && PayloadKeysUnderAnotherNameThanTheirAlias.Any(r => r.File == file && r.Key == key))
            {
                verdict = Rostered;
            }

            yield return new PayloadKeySite(file, 1 + LineOffset(code, index), key.ToLowerInvariant(), verdict, value);
        }
    }

    private static string Verdict(string key, string value, string code, int site, ISet<string> vocabulary, int hops)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
        {
            return MapEntry;
        }

        if (DivisionByACadenceLiteral.IsMatch(trimmed))
        {
            return "OFFENDER: divides by a cadence literal (rule 2)";
        }

        var helper = RateHelperCall.Match(trimmed);
        if (helper.Success && Regex.IsMatch(helper.Groups["args"].Value, @"[Ii]nterval|[Ee]lapsed|[Ss]econds"))
        {
            return RateHelper;
        }

        if (trimmed.Contains('/', StringComparison.Ordinal))
        {
            return DivisionByAMeasuredSpan.IsMatch(trimmed)
                ? Quotient
                : "OFFENDER: divides by something that is not a measured span";
        }

        if (StoredDeltaName.IsMatch(trimmed))
        {
            return "OFFENDER: a stored delta passed through under a per-second name";
        }

        var aggregate = AggregateOverAMember.Match(trimmed);
        if (aggregate.Success)
        {
            return vocabulary.Contains(Snake(aggregate.Groups["member"].Value))
                ? AggregateOverARate
                : UnderAnotherName;
        }

        if (hops > 0 && Regex.IsMatch(trimmed, @"^[A-Za-z_]\w*$"))
        {
            var assignment = Regex.Matches(code[..site], @"(?<![\w.])" + Regex.Escape(trimmed) + @"\s*=(?![=>])").LastOrDefault();
            if (assignment is not null)
            {
                var made = ValueExpression(code, assignment.Index + assignment.Length);
                var upstream = Verdict(key, made, code, assignment.Index, vocabulary, hops - 1);
                if (upstream != UnderAnotherName && upstream != ReaderAlias)
                {
                    return upstream;
                }
            }

            return vocabulary.Contains(key.ToLowerInvariant()) ? ReaderAlias : UnderAnotherName;
        }

        var members = AnyMember.Matches(trimmed).Select(m => m.Groups["member"].Value).ToList();
        if (members.Count == 0)
        {
            return vocabulary.Contains(key.ToLowerInvariant()) ? ReaderAlias : UnderAnotherName;
        }

        var spellsTheKey = RateNamedMember.Matches(trimmed).Any(m => Snake(m.Groups["member"].Value) == key.ToLowerInvariant());
        return spellsTheKey && vocabulary.Contains(key.ToLowerInvariant()) ? ReaderAlias : UnderAnotherName;
    }

    private static string ValueExpression(string code, int start)
    {
        var depth = 0;
        for (var i = start; i < code.Length; i++)
        {
            var c = code[i];
            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                if (depth == 0)
                {
                    return code[start..i];
                }

                depth--;
            }
            else if ((c == ',' || c == ';') && depth == 0)
            {
                return code[start..i];
            }
        }

        return code[start..];
    }

    private static HashSet<string> PerSecondAliasVocabulary(IEnumerable<(string File, string Text)> sources)
    {
        var vocabulary = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, text) in sources)
        {
            foreach (var (_, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                if (!LooksLikeSql(body))
                {
                    continue;
                }

                foreach (Match alias in PerSecondAlias.Matches(BlankSqlComments(body)))
                {
                    vocabulary.Add(alias.Groups[1].Value.ToLowerInvariant());
                }
            }
        }

        return vocabulary;
    }

    private static Dictionary<string, string> PerSecondKeyConstants(IEnumerable<(string File, string Text)> sources)
    {
        var constants = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, text) in sources)
        {
            foreach (Match m in PerSecondKeyConstant.Matches(BlankComments(text)))
            {
                constants[m.Groups["name"].Value] = m.Groups["key"].Value;
            }
        }

        return constants;
    }

    private static string BlankComments(string text)
    {
        var keep = CSharpSourceWalker.CodeMask(text);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = start; i < start + body.Length; i++)
            {
                keep[i] = true;
            }

            if (start > 0 && text[start - 1] == '"')
            {
                keep[start - 1] = true;
            }

            if (start + body.Length < text.Length && text[start + body.Length] == '"')
            {
                keep[start + body.Length] = true;
            }
        }

        var sb = new StringBuilder(text.Length);
        for (var i = 0; i < text.Length; i++)
        {
            sb.Append(keep[i] ? text[i] : text[i] == '\n' ? '\n' : ' ');
        }

        return sb.ToString();
    }

    private static string Snake(string pascal) =>
        Regex.Replace(pascal, @"(?<=[a-z0-9])(?=[A-Z])", "_").ToLowerInvariant();

    /* ---------------- rule 5: rollups aggregate interval > 0 only ---------------- */

    /// <summary>The collector's knowability verdict, baked into an aggregate's WHERE.</summary>
    private const string IntervalPredicate = "sample_interval_seconds IS DISTINCT FROM 0";

    /// <summary>
    /// Aggregates over a delta family that admit the unknowable (0, 0) row, named so the list can only shrink
    /// deliberately — and, since #3653 Q12, EVERY member is SUPERSEDED: each has a registered interval-honest
    /// successor carrying the predicate, which the test below asserts pair by pair, so the roster is no longer a
    /// list of open defects but the record of which legacy text still exists and why. Two kinds, stated apart
    /// because they retire differently:
    /// <list type="bullet">
    /// <item><description><c>perfmon_baseline</c>, <c>wait_stats_baseline</c> — the SUPERSEDED pair (#3698).
    /// Not registered (nothing creates them any more), read by <c>PgBaselineProvider</c>'s legacy arm for the
    /// window the successor does not yet cover, dropped by <see cref="TimescaleSupport.DropRetiredBaselineAggregatesAsync"/>
    /// once the successor's <c>min(bucket)</c> reaches the 35-day tier. They leave this list by moving to
    /// <see cref="TimescaleSupport.RetiredBaselineRelations"/> — the text is kept until then because the live
    /// retirement test builds its fixture from it.</description></item>
    /// <item><description><c>query_stats_hourly</c>, <c>procedure_stats_hourly</c>, <c>query_stats_db_hourly</c>
    /// — the REGISTERED hourly rollups over two delta families, built (#1849 era) before those tables carried
    /// an interval, SUPERSEDED for the hourly-tier read by <c>query_stats_interval_hourly</c>,
    /// <c>procedure_stats_interval_hourly</c> and <c>query_stats_db_interval_hourly</c> (#3653, Q12;
    /// <see cref="TimescaleSupport.SupersededHourlyRollups"/>). A restart's (0, 0) row costs their <c>sum()</c>s
    /// nothing, but <c>count(*) AS sample_count</c> counts it as a sample on all three, and <c>min(delta_*)</c>
    /// reads it as a real minimum on the two that carry a min (<c>query_stats_db_hourly</c> carries sums and the
    /// sample count only). Unlike the baseline pair they STAY registered — in FrozenRollupAggregates since
    /// #3653 LC's freeze, not HourlyAggregates — but no longer refreshing: the daily tier is hierarchical from
    /// them and a continuous aggregate's source is fixed at CREATE, so LC froze the pair in place (no refresh
    /// policy, watermark never advances again) rather than dropping or cascading it. Every hourly-tier reader
    /// takes the successor through <c>RollupCoverage.HourlyRelationFor</c> where it reaches as far as the
    /// legacy; the daily tier inherits the legacy's contamination at the day grain — LB's three interval-honest
    /// successor dailies now exist, but only LA's stitched reads route to them past the legacy's floor. These
    /// three leave this list only when the legacy CREATE text itself is finally retired.
    /// </description></item>
    /// </list>
    /// </summary>
    private static readonly string[] RollupsThatAggregateUnknowableRows =
    {
        TimescaleSupport.LegacyPerfmonBaselineView,
        TimescaleSupport.LegacyWaitStatsBaselineView,
        TimescaleSupport.ProcedureStatsHourlyView,
        TimescaleSupport.QueryStatsDbHourlyView,
        TimescaleSupport.QueryStatsHourlyView,
    };

    /// <summary>
    /// Aggregates whose WHERE excludes the unknowable row by a predicate other than the interval's — with the
    /// bound each rests on. <c>query_stats_baseline</c> keeps <c>delta_execution_count &gt; 0</c>: the calculator
    /// reports a positive execution delta only from a subtraction it could make (first sighting, reset and
    /// gap all report 0; the series-age rescue credits a whole counter only over a measured positive gap), so
    /// a positive count implies a knowable interval and the predicate is the interval predicate in another
    /// column. #3698 judged it needs no successor on that reasoning; this row states the reasoning so the
    /// judgement is a bound and not a memory.
    /// </summary>
    private static readonly (string View, string Predicate)[] EquivalentPredicates =
    {
        (TimescaleSupport.QueryStatsBaselineView, "delta_execution_count > 0"),
    };

    /// <summary>One continuous aggregate's CREATE, as read off <see cref="TimescaleSupport"/>.</summary>
    private sealed record Aggregate(string Constant, string View, string From, bool AggregatesADelta, bool CarriesIntervalPredicate, string Where);

    [Fact]
    public void EveryContinuousAggregateOverADeltaFamily_ExcludesTheUnknowableRow_OrIsNamed()
    {
        var aggregates = Aggregates();
        Assert.True(aggregates.Count >= 20, $"only {aggregates.Count} CREATE MATERIALIZED VIEW constants found on TimescaleSupport — the reflection read is not reading it");

        var deltaTables = CollectorDeltaCalculator.DeltaFamilyCollectors
            .Select(f => CollectorCatalog.Find(f)!.TargetTable)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        Assert.Equal(10, deltaTables.Count);

        var governed = aggregates.Where(a => deltaTables.Contains(a.From) && a.AggregatesADelta).ToList();
        Assert.True(governed.Count >= 11, $"only {governed.Count} aggregates read a delta family — expected the five successors, the legacy pair, query_stats_baseline and the three legacy hourly rollups at least");

        var carrying = governed.Where(a => a.CarriesIntervalPredicate).Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal).ToList();
        var equivalent = governed.Where(a => !a.CarriesIntervalPredicate && EquivalentPredicates.Any(e => e.View == a.View)).ToList();
        var admitting = governed.Where(a => !a.CarriesIntervalPredicate && EquivalentPredicates.All(e => e.View != a.View)).Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal).ToList();

        /* The successors carry it — the whole point of #3698 and of #3653 Q12 — and every one is REGISTERED:
           the baseline pair in the legacy pair's positions, the hourly trio appended to HourlyAggregates. */
        Assert.Equal(
            new[]
            {
                TimescaleSupport.PerfmonIntervalBaselineView,
                TimescaleSupport.ProcedureStatsIntervalHourlyView,
                TimescaleSupport.QueryStatsDbIntervalHourlyView,
                TimescaleSupport.QueryStatsIntervalHourlyView,
                TimescaleSupport.WaitStatsIntervalBaselineView,
            }.OrderBy(v => v, StringComparer.Ordinal),
            carrying);
        var registeredHourlyOrBaseline = TimescaleSupport.HourlyAggregates.Concat(TimescaleSupport.BaselineAggregates).Select(a => a.View).ToHashSet(StringComparer.Ordinal);
        Assert.All(carrying, view => Assert.Contains(view, registeredHourlyOrBaseline));

        /* THE INVARIANT SINCE Q12: every aggregate that still admits the row is SUPERSEDED — named in one of the
           two supersession lists with a successor that is registered and carries the predicate. The roster
           cannot empty while the legacy CREATE text exists (the hourly trio is registered for its daily tier;
           the baseline pair's text is the live retirement fixture), so "empty" is not the honest assertion;
           "every member has a carrying successor" is, and it is the stronger one: a new admitting aggregate
           cannot pass by joining the roster, it has to come with its successor. */
        var supersessions = TimescaleSupport.SupersededBaselineRelations.Select(s => (s.Legacy, s.Successor))
            .Concat(TimescaleSupport.SupersededHourlyRollups.Select(s => (s.Legacy, s.Successor)))
            .ToDictionary(s => s.Legacy, s => s.Successor, StringComparer.Ordinal);
        Assert.All(admitting, legacy => Assert.True(supersessions.ContainsKey(legacy), $"{legacy} admits the unknowable row and has no registered successor — rule 5 is open again"));
        Assert.All(admitting, legacy => Assert.Contains(supersessions[legacy], carrying));
        Assert.Equal(admitting.Count, supersessions.Count);

        /* Every equivalent predicate is really in the WHERE it claims to be in. */
        foreach (var (view, predicate) in EquivalentPredicates)
        {
            var aggregate = Assert.Single(equivalent, a => a.View == view);
            Assert.Contains(predicate, aggregate.Where, StringComparison.Ordinal);
        }

        /* Both directions: the roster is exactly the aggregates that admit the row. */
        Assert.Equal(RollupsThatAggregateUnknowableRows.OrderBy(v => v, StringComparer.Ordinal), admitting);

        /* The legacy pair is superseded, not registered; the three rollups are registered hourlies, and the
           contamination named on the roster is really in their text. */
        var registeredViews = TimescaleSupport.HourlyAggregates.Concat(TimescaleSupport.BaselineAggregates).Concat(TimescaleSupport.DailyAggregates).Select(a => a.View).ToHashSet(StringComparer.Ordinal);
        foreach (var (legacy, _) in TimescaleSupport.SupersededBaselineRelations)
        {
            Assert.Contains(legacy, admitting);
            Assert.DoesNotContain(legacy, registeredViews);
        }

        var contamination = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsHourlyView] = new[] { "count(*) AS sample_count", "min(delta_" },
            [TimescaleSupport.ProcedureStatsHourlyView] = new[] { "count(*) AS sample_count", "min(delta_" },
            [TimescaleSupport.QueryStatsDbHourlyView] = new[] { "count(*) AS sample_count" },
        };
        var registeredAdmitting = admitting.Except(TimescaleSupport.SupersededBaselineRelations.Select(s => s.Legacy)).ToList();
        Assert.Equal(contamination.Keys.OrderBy(v => v, StringComparer.Ordinal), registeredAdmitting);
        Assert.Equal(TimescaleSupport.SupersededHourlyRollups.Select(s => s.Legacy).OrderBy(v => v, StringComparer.Ordinal), registeredAdmitting);
        foreach (var view in registeredAdmitting)
        {
            /* #3653 LC: the trio froze out of HourlyAggregates into FrozenRollupAggregates — still registered,
               just on the other list, so the membership check has to look at both. */
            Assert.Contains(view, TimescaleSupport.HourlyAggregates.Concat(TimescaleSupport.FrozenRollupAggregates).Select(a => a.View));
            var text = Text(aggregates.Single(a => a.View == view).Constant);
            foreach (var shape in contamination[view])
            {
                Assert.Contains(shape, text, StringComparison.Ordinal);
            }

            /* The db-hourly carries no min — the roster's remark says so; keep it true. */
            Assert.Equal(contamination[view].Contains("min(delta_"), Regex.IsMatch(text, @"\bmin\(delta_\w+\)"));

            /* THE SUCCESSOR IS THE LEGACY'S SHAPE PLUS THE VERDICT (#3653, Q12): same FROM, same GROUP BY, every
               legacy select item present under the same alias, the interval predicate in the WHERE, and the
               measured interval carried as a per-bucket sum. Asserted on the shipped text so a successor that
               drifted from its legacy's dimensions — which is what would make a reader's relation swap change
               its answer's shape — is red here rather than at a 42703 on a store. */
            var (_, successor, dependentDaily) = TimescaleSupport.SupersededHourlyRollups.Single(s => s.Legacy == view);
            var successorText = Text(aggregates.Single(a => a.View == successor).Constant);
            Assert.Contains(IntervalPredicate, successorText, StringComparison.Ordinal);
            Assert.Contains("sum(sample_interval_seconds) AS sample_interval_seconds_sum", successorText, StringComparison.Ordinal);
            Assert.Equal(Regex.Match(text, @"\bFROM\s+collect\.(\w+)").Groups[1].Value, Regex.Match(successorText, @"\bFROM\s+collect\.(\w+)").Groups[1].Value);
            Assert.Equal(TimescaleSupport.RefreshGroupingTermsFor(text), TimescaleSupport.RefreshGroupingTermsFor(successorText));
            foreach (Match alias in Regex.Matches(text, @"\b(\w+\([^)]*\)) AS (\w+)"))
            {
                Assert.Contains($"{alias.Groups[1].Value} AS {alias.Groups[2].Value}", successorText, StringComparison.Ordinal);
            }

            /* And the daily the legacy is kept for really is hierarchical from the LEGACY, not the successor —
               the structural fact the whole "stays registered" reasoning rests on. */
            var dailyText = Text(aggregates.Single(a => a.View == dependentDaily).Constant);
            Assert.Contains($"FROM collect.{view}", dailyText, StringComparison.Ordinal);
            /* #3653 LC: the dependent daily froze right alongside its hourly — same list move, same reason. */
            Assert.Contains(dependentDaily, TimescaleSupport.DailyAggregates.Concat(TimescaleSupport.FrozenRollupAggregates).Select(a => a.View));
        }
    }

    /// <summary>
    /// The aggregates outside rule 5's reach, stated so the governed set above is a decision and not a
    /// coincidence: every non-governed CREATE either reads a table that is not a delta family (session_stats,
    /// blocked_process_reports, deadlocks, memory_stats, query_store_stats — a cumulative-snapshot source with
    /// no stored interval, #3540's QS reference) or reads another aggregate (the hierarchical dailies), whose
    /// honesty is its parent's.
    /// </summary>
    [Fact]
    public void EveryAggregateOutsideTheRule_ReadsANonDeltaTable_OrAnotherAggregate()
    {
        var aggregates = Aggregates();
        var deltaTables = CollectorDeltaCalculator.DeltaFamilyCollectors.Select(f => CollectorCatalog.Find(f)!.TargetTable).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var views = aggregates.Select(a => a.View).ToHashSet(StringComparer.Ordinal);

        foreach (var aggregate in aggregates.Where(a => !(deltaTables.Contains(a.From) && a.AggregatesADelta)))
        {
            var readsAnotherAggregate = views.Contains(aggregate.From);
            var readsANonDeltaTable = !deltaTables.Contains(aggregate.From);
            Assert.True(readsAnotherAggregate || readsANonDeltaTable,
                $"{aggregate.View} reads delta family table {aggregate.From} without aggregating a delta_* column — decide whether rule 5 governs it and say so here");
        }
    }

    /// <summary>Every <c>CREATE MATERIALIZED VIEW</c> constant on <see cref="TimescaleSupport"/>, parsed just far
    /// enough: the view name, the FROM table, whether any aggregate wraps a <c>delta_*</c> column, and whether
    /// the WHERE carries the interval predicate.</summary>
    private static List<Aggregate> Aggregates()
    {
        var result = new List<Aggregate>();
        foreach (var field in typeof(TimescaleSupport).GetFields(BindingFlags.Public | BindingFlags.Static).Where(f => f.IsLiteral && f.FieldType == typeof(string)).OrderBy(f => f.Name, StringComparer.Ordinal))
        {
            var text = (string)field.GetRawConstantValue()!;
            var head = Regex.Match(text, @"^\s*CREATE MATERIALIZED VIEW IF NOT EXISTS collect\.(\w+)");
            if (!head.Success)
            {
                continue;
            }

            var from = Regex.Match(text, @"\bFROM\s+collect\.(\w+)");
            Assert.True(from.Success, $"{field.Name}: no FROM collect.<table>");
            var where = Regex.Match(text, @"\bWHERE\b(.*?)\bGROUP BY\b", RegexOptions.Singleline);

            result.Add(new Aggregate(
                field.Name,
                head.Groups[1].Value,
                from.Groups[1].Value,
                Regex.IsMatch(text, @"\b(?:sum|avg|min|max|count)\(\s*delta_\w+", RegexOptions.IgnoreCase),
                text.Contains(IntervalPredicate, StringComparison.Ordinal),
                where.Success ? where.Groups[1].Value : string.Empty));
        }

        return result;
    }

    private static string Text(string constant) =>
        (string)typeof(TimescaleSupport).GetField(constant, BindingFlags.Public | BindingFlags.Static)!.GetRawConstantValue()!;

    /* ---------------- helpers (the Lite half's, verbatim) ---------------- */

    private static IEnumerable<(string File, int Line, string Sql)> SqlBodies()
    {
        foreach (var (file, text) in ProductSources(SweptTrees, SweptTreesFileFloor))
        {
            foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
            {
                if (!LooksLikeSql(body))
                {
                    continue;
                }

                yield return (file, 1 + LineOffset(text, start), BlankSqlComments(body));
            }
        }
    }

    private static IEnumerable<(string File, string Code)> CodeBodies()
    {
        foreach (var (file, text) in ProductSources(SweptTrees, SweptTreesFileFloor))
        {
            yield return (file, CSharpSourceWalker.StripCommentsAndStrings(text));
        }
    }

    /// <summary>The two trees' file floor (106 when this landed).</summary>
    private const int SweptTreesFileFloor = 80;

    /// <summary>The four Darling trees' file floor (521 when the C# half landed).</summary>
    private const int PayloadKeyTreesFileFloor = 400;

    private static IEnumerable<(string File, string Text)> ProductSources(string[] trees, int fileFloor)
    {
        var root = RepoFile.Root;
        var files = 0;

        foreach (var tree in trees)
        {
            var dir = Path.Combine(root, tree.Replace('/', Path.DirectorySeparatorChar));
            Assert.True(Directory.Exists(dir), $"swept tree missing: {tree}");

            foreach (var path in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
                .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .Where(p => !p.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .OrderBy(p => p, StringComparer.Ordinal))
            {
                files++;
                var relative = Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/');
                yield return (relative, File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal));
            }
        }

        Assert.True(files >= fileFloor, $"the sweep read only {files} files across {trees.Length} trees (floor {fileFloor})");
    }

    /// <summary>The literals the SQL sweeps read: anything carrying a SELECT, or naming an interval (a fragment a
    /// reader assembles into a statement). Everything else — descriptions, log lines, JSON keys — is out, so
    /// prose about a rate cannot be swept as a rate.</summary>
    private static bool LooksLikeSql(string body) =>
        body.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase) >= 0 || body.IndexOf("interval", StringComparison.OrdinalIgnoreCase) >= 0;

    private static string BlankSqlComments(string sql)
    {
        sql = Regex.Replace(sql, @"/\*.*?\*/", m => Regex.Replace(m.Value, @"[^\n]", " "), RegexOptions.Singleline);
        return Regex.Replace(sql, @"--[^\n]*", m => new string(' ', m.Length));
    }

    private static string SelectItemBefore(string sql, int end)
    {
        var depth = 0;
        var i = end - 1;
        for (; i >= 0; i--)
        {
            var c = sql[i];
            if (c == ')')
            {
                depth++;
            }
            else if (c == '(')
            {
                if (depth == 0)
                {
                    break;
                }

                depth--;
            }
            else if (c == ',' && depth == 0)
            {
                break;
            }
            else if (depth == 0 && i >= 5 && string.Compare(sql, i - 5, "SELECT", 0, 6, StringComparison.OrdinalIgnoreCase) == 0)
            {
                break;
            }
        }

        return sql[(i + 1)..end];
    }

    private static int LineOffset(string text, int index) => text.AsSpan(0, index).Count('\n');

    private static string Snippet(string text, int index)
    {
        var start = Math.Max(0, index - 60);
        var end = Math.Min(text.Length, index + 60);
        return Collapse(text[start..end]);
    }

    private static string Collapse(string text) => Regex.Replace(text, @"\s+", " ").Trim();
}
