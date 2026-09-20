/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Darling.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3540's measurement contract as a census (#3653): <b>the ten rules, numbered once, each with the bound it
/// rests on and the test that holds it</b> — so "the contract has ten rules" is a list in a file rather than a
/// sentence in an issue, and a rule that is pinned, rostered, or not census-able says which, here.
///
/// <para><b>The record, and what it does not hold.</b> #3540's body states the contract in one sentence —
/// "every delta family persists interval; readers NULL-not-0 on unknowable; rates divide by measured
/// elapsed; epoch change → ClearServer + marker; rollups aggregate interval&gt;0 only; per-sec names divided
/// at comparison; gauges never delta'd" — seven rules, and #3614 added the eighth ("a host restart seeds
/// EVERY delta family, keys and pass window"). The review that produced the issue numbered TEN, and two of
/// its numbers reached the tree: <see cref="DeltaFamilyIntervalColumnTests"/> calls the interval rule
/// <b>rule 4</b> and <see cref="DeltaFamilySeedingCensusTests"/> calls the seeding rule <b>rule 7</b>. The
/// review's own enumeration was never written into the issue or its comments, and the body's order cannot be
/// the review's (the interval rule is FIRST in the body and fourth in the review; the epoch rule is fourth
/// in the body and cannot be seventh). So: 4 and 7 keep their numbers, the six remaining named rules take the
/// free slots in the body's order, and <b>slots 9 and 10 are left empty rather than invented</b> — the
/// record does not say what they were, and a census of a rule nobody can state is not a census. Nothing
/// below keys on a number; re-numbering is an edit to this comment.</para>
///
/// <para><b>The ten rules.</b></para>
/// <list type="number">
/// <item><description><b>Readers NULL-not-0 on unknowable.</b> A stored <c>sample_interval_seconds</c> of 0
/// is the calculator's "no delta knowable" marker (first sighting, counter reset, gap past the policy), so a
/// reader never divides by it bare: the denominator is <c>NULLIF(sample_interval_seconds, 0)</c> — or a
/// derived <c>interval_seconds</c> alias built through that NULLIF, divided under <c>CASE WHEN … &gt; 0</c>
/// — and the quotient is NULL, never 0.00. <b>PINNED here</b> (<see cref="NoReaderDividesByTheStoredIntervalBare"/>,
/// <see cref="EveryDerivedIntervalAlias_RoutesTheStoredValueThroughNullif"/>,
/// <see cref="EveryDivisionByADerivedInterval_IsGuardedAbovePositive_AndEveryZeroFallbackIsNamed"/>) over
/// the trees the <c>lite</c> and <c>core</c> CI filters reach; the Darling twin
/// (<c>Darling.Tests/MeasurementContractCensusTests</c>) sweeps <c>Darling.Storage</c> and <c>Darling.Analysis</c>
/// with the same regexes and holds the one allowance roster (two baseline arms whose source aggregate has
/// already excluded the interval-0 collection). Rate arms whose CASE falls back to <c>ELSE 0</c> are
/// rostered shrink-only in <see cref="RateArmsThatFallBackToZero"/>, EMPTY and asserted empty since the
/// #3653 rate-arm PR retired the fifteen it named (four here, eleven in the Darling twin) to <c>END</c>;
/// a new one has to name itself.</description></item>
/// <item><description><b>Rates divide by measured elapsed.</b> No reader turns a delta into a rate over an
/// ASSUMED cadence — no <c>delta_x / 60</c>, <c>/ 300</c>, <c>/ 3600</c> in SQL, no <c>Delta… / 60.0</c> in
/// C# — because the fleet's measured gap runs p50 299 s, p99 830 s (<see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>'s
/// lineage), so any literal is wrong by whatever the jitter was. #3540's credits say there are none;
/// <b>PINNED here</b> as zero-and-stays-zero (<see cref="NoRate_DividesADeltaByAnAssumedCadence"/>), with a
/// positive control on a fixture so an empty result cannot be a dead regex, and a population floor on the
/// divisions that DO exist (by the stored interval or its alias).</description></item>
/// <item><description><b>Epoch change → ClearServer + marker.</b> When the counters behind a
/// <c>server_id</c> belong to a different instance than the baselines were read from — a new
/// <c>sqlserver_start_time</c> or <c>@@SERVERNAME</c>, a moved <c>pg_stat_statements_info.stats_reset</c>,
/// a same-id reconnect with a changed connection — the host forgets every affected baseline and pass
/// window (<see cref="ICollectorDeltaCalculator.ClearServer"/> / <c>ClearGroups</c>) BEFORE it subtracts,
/// and leaves a marker the read layer can find (<c>identity_epoch_changes=1</c> on the carrier run's
/// <c>collection_log</c> note; the replaced pair in <c>collector_state</c>). Landed in #3694 as
/// <see cref="ServerEpoch"/>; <b>PINNED here as a call-site census</b>: exactly the four carriers observe
/// (two SQL Server instance carriers, the statements carrier, the Aurora wait carrier), every forget site
/// in the product is an epoch or a remove path
/// (<see cref="EveryForgetSite_IsTheEpochComparatorOrAHostRemovePath"/>), both hosts drain the account and
/// carry the marker (<see cref="BothHosts_DrainTheDiscontinuityAccount_AndCarryTheMarkerOntoTheCollectionLog"/>).
/// The two residuals #3694 left were rostered shrink-only and are now EMPTY, asserted empty from the product:
/// no SQL Server delta family is scheduled before the first instance carrier (<c>wait_stats</c> carries the
/// pair on both hosts, first in the order — <see cref="FamiliesThatSubtractOnceBeforeTheCarrier"/>), and
/// no delta family lacks a carrier (<c>pg_wait_stats</c> carries <c>pg_postmaster_start_time()</c> —
/// <see cref="FamiliesWithoutAnEpochCarrier"/>).</description></item>
/// <item><description><b>Every delta family persists the interval its deltas accrued over.</b> COMPLETE
/// and <b>PINNED</b> by <see cref="DeltaFamilyIntervalColumnTests"/> (Darling V127/V128, Lite v60/v61):
/// all ten members of <see cref="CollectorDeltaCalculator.DeltaFamilyCollectors"/> carry
/// <c>sample_interval_seconds</c>, the still-naked list is empty and asserted empty. Left alone here;
/// <see cref="TheTwoPinnedRules_KeepTheNumbersTheirTestsGaveThem"/> only keeps this list's numbering honest
/// against those files.</description></item>
/// <item><description><b>Rollups aggregate interval &gt; 0 only.</b> A continuous aggregate or rollup that
/// <c>sum()</c>s / <c>min()</c>s / <c>count()</c>s a delta family's rows carries
/// <c>sample_interval_seconds IS DISTINCT FROM 0</c>, so a restart collection produces no sample rather
/// than a quiet one. <b>ROSTERED</b> in the Darling twin, which reads <c>TimescaleSupport</c>: the
/// five interval-honest successors carry it — #3698's baseline pair (<c>perfmon_interval_baseline</c>,
/// <c>wait_stats_interval_baseline</c>) and #3653 Q12's hourly trio (<c>query_stats_interval_hourly</c>,
/// <c>procedure_stats_interval_hourly</c>, <c>query_stats_db_interval_hourly</c>); <c>query_stats_baseline</c>
/// carries the equivalent <c>delta_execution_count &gt; 0</c>; the superseded legacy pair (<c>perfmon_baseline</c>,
/// <c>wait_stats_baseline</c> — retiring through <c>SupersededBaselineRelations</c> once the successor covers
/// the tier) and the three legacy hourly rollups (<c>query_stats_hourly</c>, <c>procedure_stats_hourly</c>,
/// <c>query_stats_db_hourly</c> — superseded for every hourly-tier read through
/// <c>RollupCoverage.HourlyRelationFor</c>, but REGISTERED for good, because the indefinite daily tier is
/// hierarchical from them; <c>SupersededHourlyRollups</c>) do NOT, and are the roster — every member of which
/// now has a registered successor, which the Darling twin asserts pair by pair. The Compose raw tier's
/// <c>FILTER (WHERE sample_interval_seconds IS DISTINCT FROM 0)</c> is
/// #3695's and pinned in its own tests. Lite has no rollup tier; its baseline reads are raw and fall under
/// rule 1's sweep.</description></item>
/// <item><description><b>Per-second names are divided at comparison, not stored.</b> A column or key named
/// <c>*_per_second</c> / <c>*_per_sec</c> is a quotient computed at read time over the measured interval;
/// the store holds raw per-interval deltas, and the perfmon counters whose NAMES end in <c>/sec</c>
/// (<c>Batch Requests/sec</c> and its siblings) are stored as <c>delta_cntr_value</c> — a per-interval
/// delta, not a rate — and divided by every reader. <b>PINNED here</b> for the SQL half
/// (<see cref="EveryPerSecondAlias_IsAQuotient_AndNoStoredColumnCarriesTheName"/>): every per-second alias in
/// product SQL is a division or a window over one, and no collector payload column carries the name. And
/// <b>PINNED here for the C# half</b> (<see cref="EveryPerSecondPayloadKey_IsAReaderRateOrACSharpQuotient_OrIsNamed"/>):
/// every C# payload key carrying the name — an MCP tool's anonymous-object member, a fact's
/// <c>Metadata["…"]</c> entry, a key reached through a <c>const</c> — is populated from a reader field of
/// the SAME rate name (so the SQL half above is what proves it divides), from a C# division by a measured
/// interval or elapsed in the same expression or one local up, from a rate helper handed the interval, or
/// from an aggregate over a reader's rate field; never a stored delta passed through under the name, never
/// a division by a cadence literal. The keys whose provenance is not the alias of their own name are
/// rostered shrink-only in <see cref="PayloadKeysUnderAnotherNameThanTheirAlias"/>, each with the alias or
/// quotient it rests on asserted. This file's C# half sweeps Lite and the shared libraries; the Darling
/// twin sweeps all four Darling trees, because a Service tool's reader field is populated by a
/// <c>Darling.Storage</c> alias the <c>lite</c> filter cannot reach.</description></item>
/// <item><description><b>A host restart seeds EVERY delta family, keys and pass window.</b> <b>PINNED</b> by
/// <see cref="DeltaFamilySeedingCensusTests"/> (#3614, #3630): both hosts' seeders against the calculator's
/// family list, byte-identical shared reads, per-family guards, <c>ClearServer</c> on both remove paths.
/// Left alone here.</description></item>
/// <item><description><b>Gauges are never delta'd.</b> A perfmon counter whose <c>cntr_type</c> is a gauge
/// (Total Server Memory (KB), Memory Grants Pending, Processes blocked…) is a level, and differencing it turns a
/// falling level into a fake counter reset. <b>PINNED here</b> since Darling V132 / Lite v62 stored the type
/// (<see cref="TheGaugeRule_TheCollectorWritesNoDeltaForAGaugeType_AndEveryReaderClassifiesByTheStoredType"/>):
/// the collector's payload carries <c>cntr_type</c>, its write branches on the one gauge set the read-side
/// vocabulary declares (pinned equal across the assembly boundary in <c>PerfmonCounterTypeTests</c>), the
/// delta call sits inside the non-gauge branch only, and every perfmon read that sums or projects a delta
/// also selects the type. Until the rung this row pinned the ABSENCE of the column so the rung would red it
/// and owe the census; the census is what replaced it. The <c>/sec</c>-suffix proxy is now the NULL-type
/// fallback only, pinned as such in <c>DeltaSeriesShapingTests</c>.</description></item>
/// <item><description><i>Not recoverable from the record.</i> The review numbered ten; the body names eight.
/// Whoever holds the review's enumeration writes this slot; nothing is invented for it.</description></item>
/// <item><description><i>Not recoverable from the record</i> — as above.</description></item>
/// </list>
///
/// <para><b>How the sweeps read.</b> SQL is read out of string literals with
/// <see cref="CSharpSourceWalker.StringLiteralBodies"/> (interpolation holes blanked to spaces, so an
/// assembled query is still a query), then SQL comments (<c>/* … */</c>, <c>-- …</c>) are blanked with their
/// newlines kept, so prose that QUOTES the forbidden shape — and this repository's SQL comments quote it
/// often — cannot count as the shape. C# is read off <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>
/// for the same reason in the other direction. Rule 6's C# half reads a third text — comments blanked,
/// code AND literal text kept (<see cref="BlankComments"/>) — because a payload key can be a C# identifier
/// (<c>waits_per_second = …</c>) or a string (<c>["current_ms_per_sec"] = …</c>) and the value it is given
/// is always code. Every negative census here carries two controls, because a
/// negative result agrees with a dead filter: a POSITIVE control (a fixture the regex must fire on) and a
/// POPULATION floor (the swept text must contain at least N of the thing the rule governs). Offenders are
/// reported as <c>path:line</c>.</para>
///
/// <para><b>Why two files.</b> The <c>lite</c> CI filter reaches <c>Lite</c>, the shared
/// <c>PerformanceMonitor.*</c> libraries (through <c>core</c>), the Darling Service and the Darling Viewer —
/// and not <c>Darling.Storage</c> or <c>Darling.Analysis</c>. <c>CrossAppGuardCiGateTests</c> fails any Lite
/// guard whose source read PR CI cannot run (#2839), so this file sweeps exactly the trees named in
/// <see cref="SweptTrees"/>, and the Darling twin sweeps the other two with the same regexes and carries the
/// Darling-only artifacts (the continuous aggregates). The regexes are duplicated deliberately rather than
/// linked: a cross-project pin on their equality would itself be a read the gate has to reach. Rule 6's C#
/// half splits differently, by where a key's ALIAS lives rather than by what the filter reaches: this file
/// censuses the payload keys of <see cref="PayloadKeyTrees"/> (Lite and the shared libraries, whose
/// aliases are Lite's own SQL) and the twin censuses all four Darling trees, because a Darling Service tool
/// copies a reader field that a <c>Darling.Storage</c> alias populated, and only the twin can see both
/// ends of that copy.</para>
/// </summary>
public sealed class MeasurementContractCensusTests
{
    /* ---------------- the swept population ---------------- */

    /// <summary>The product trees this file reads, every one reachable by the <c>lite</c> or <c>core</c>
    /// filter in build.yml. <c>Darling.Storage</c> and <c>Darling.Analysis</c> are the Darling twin's.</summary>
    private static readonly string[] SweptTrees =
    {
        "Lite",
        "PerformanceMonitor.Alerting",
        "PerformanceMonitor.Analysis",
        "PerformanceMonitor.Collectors",
        "PerformanceMonitor.Common",
        "PerformanceMonitor.Notifications",
        "PerformanceMonitor.PlanAnalysis",
        "PerformanceMonitor.Ui",
        "Darling/PerformanceMonitor.Darling.Service",
        "Darling/PerformanceMonitor.Darling.Viewer",
    };

    /// <summary>The trees whose C# payload keys rule 6's C# half censuses HERE: Lite and the shared libraries —
    /// <see cref="SweptTrees"/> less the two Darling trees, which the Darling twin sweeps for that half together
    /// with Storage and Analysis. The split is by alias, not by filter: a key on these trees copies a reader field
    /// that Lite's own SQL populated, so the vocabulary this half checks a key against is complete on these trees;
    /// a Service tool's key copies a field a <c>Darling.Storage</c> alias populated, and the <c>lite</c> filter
    /// cannot reach that alias to check it. The SQL rules above still sweep all ten.</summary>
    private static readonly string[] PayloadKeyTrees = SweptTrees
        .Where(t => !t.StartsWith("Darling/", StringComparison.Ordinal))
        .ToArray();

    /* ---------------- rule 1: readers NULL-not-0 ---------------- */

    /// <summary>
    /// A denominator that reaches the stored interval through anything but <c>NULLIF(</c>: the <c>/</c>, then
    /// any run of parentheses and function openers that are NOT <c>NULLIF(</c> (<c>CAST(</c>, <c>MAX(</c>,
    /// <c>COALESCE(</c>, a bare <c>(</c>), then the column with an optional alias prefix. A denominator reached
    /// some other way — <c>/ (1.0 * sample_interval_seconds)</c> — is outside this regex, and that is the
    /// stated bound: there is no such site in either SKU, and the control below proves the shapes that exist
    /// are the shapes it fires on.
    /// </summary>
    private static readonly Regex BareIntervalDenominator = new(
        @"/(?:\s*(?:\(|(?!NULLIF\s*\()[A-Za-z_]\w*\s*\())*\s*(?:\w+\.)?sample_interval_seconds\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The sanctioned denominator, for the population floor: <c>NULLIF(sample_interval_seconds, 0)</c>
    /// and its <c>MAX(…)</c>-over-the-collection and alias-prefixed spellings.</summary>
    private static readonly Regex NullifIntervalDenominator = new(
        @"NULLIF\(\s*(?:MAX\(\s*)?(?:\w+\.)?sample_interval_seconds\s*\)?\s*,\s*0\s*\)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>A derived interval alias's definition: <c>… AS interval_seconds</c> / <c>AS interval_sec</c>.</summary>
    private static readonly Regex DerivedIntervalAlias = new(
        @"\bAS\s+(?:\w+\.)?interval_sec(?:onds)?\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The three-state test that is allowed to name the column bare: <c>sample_interval_seconds IS NULL</c>
    /// (or <c>MAX(…) IS NULL</c>) chooses the LAG fallback for a pre-column row and divides by nothing.</summary>
    private static readonly Regex IntervalIsNullTest = new(
        @"(?:MAX\(\s*)?(?:\w+\.)?sample_interval_seconds\s*\)?\s+IS\s+NULL\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex StoredIntervalMention = new(
        @"\bsample_interval_seconds\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>A division by the derived alias.</summary>
    private static readonly Regex DivisionByDerivedInterval = new(
        @"/\s*(?:\w+\.)?interval_sec(?:onds)?\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The guard a division by the derived alias sits under.</summary>
    private static readonly Regex DerivedIntervalPositiveGuard = new(
        @"\b(?:\w+\.)?interval_sec(?:onds)?\s*>\s*0\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The tail of a CASE that falls back to ZERO and names its column: <c>ELSE 0 END AS name</c>. Whether the
    /// CASE is a RATE arm (a division by the derived interval under an <c>interval &gt; 0</c> guard) is decided on
    /// the whole select item walked back from here, so a nested CASE inside the THEN cannot hide the arm.</summary>
    private static readonly Regex ZeroFallbackTail = new(
        @"ELSE\s+0\s+END\s+AS\s+(?<alias>\w+)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>The guard a rate arm opens with.</summary>
    private static readonly Regex GuardedRateArmOpener = new(
        @"CASE\s+WHEN\s+(?:\w+\.)?interval_sec(?:onds)?\s*>\s*0\s+THEN",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Rate arms whose CASE falls back to <c>ELSE 0</c> rather than to NULL, named as <c>file: alias</c> so the
    /// list can only shrink deliberately. EMPTY since the #3653 rate-arm PR; until then it named four, the
    /// file-I/O throughput trends' <c>read_mb_per_sec</c> / <c>write_mb_per_sec</c> on both SKUs
    /// (<c>Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.FileIo.cs</c>,
    /// <c>Lite/Services/LocalDataService.FileIo.cs</c>). Each was dead text — the statement's own
    /// <c>WHERE interval_seconds IS NOT NULL AND interval_seconds &gt; 0</c> one CTE down kept the ELSE from
    /// running — and was rostered because it is the shape rule 1 forbids: a reader that would answer 0 for
    /// "unknowable" the moment the WHERE moved. Each now ends at <c>END</c>. Kept declared, in
    /// <see cref="DeltaFamilyIntervalColumnTests"/>' still-naked idiom, and asserted empty below, so a rate
    /// arm that ships with <c>ELSE 0</c> again has to name itself here to pass and the diff says so. The
    /// Darling twin's list (the <c>Darling.Storage</c> / <c>Darling.Analysis</c> members) emptied in the
    /// same PR.
    /// </summary>
    private static readonly string[] RateArmsThatFallBackToZero = Array.Empty<string>();

    /// <summary>
    /// Rule 1, the direct arm: no product SQL on the swept trees divides by the stored interval except through
    /// <c>NULLIF(…, 0)</c>. Zero offenders, and the two controls that make zero mean something: the regex fires
    /// on every bare shape a reader could write, and the swept SQL carries the sanctioned shape at least
    /// twenty times (it carries far more — the floor is a dead-sweep detector, not a count pin).
    /// </summary>
    [Fact]
    public void NoReaderDividesByTheStoredIntervalBare()
    {
        /* Positive control: the shapes a reader could write, every one a violation. */
        foreach (var bare in new[]
        {
            "delta_wait_time_ms * 1.0 / sample_interval_seconds AS ms_per_sec",
            "SUM(delta_cntr_value) / w.sample_interval_seconds",
            "x / CAST(sample_interval_seconds AS DOUBLE PRECISION)",
            "x / MAX(sample_interval_seconds)",
            "x / COALESCE(sample_interval_seconds, 60)",
            "x / (sample_interval_seconds)",
            "x /\n    (f.sample_interval_seconds)",
        })
        {
            Assert.True(BareIntervalDenominator.IsMatch(bare), $"the rule-1 regex does not fire on the bare shape: {bare}");
        }

        /* Negative control: the sanctioned shapes, none a violation. */
        foreach (var sanctioned in new[]
        {
            "x / NULLIF(sample_interval_seconds, 0)",
            "x / NULLIF(MAX(w.sample_interval_seconds), 0)",
            "x / (NULLIF(sample_interval_seconds, 0))",
            "x / nullif(sample_interval_seconds, 0)",
        })
        {
            Assert.False(BareIntervalDenominator.IsMatch(sanctioned), $"the rule-1 regex fires on the sanctioned shape: {sanctioned}");
        }

        var offenders = new List<string>();
        var sanctionedCount = 0;
        var mentions = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            mentions += StoredIntervalMention.Matches(sql).Count;
            sanctionedCount += NullifIntervalDenominator.Matches(sql).Count;
            foreach (Match m in BareIntervalDenominator.Matches(sql))
            {
                offenders.Add($"{file}:{line + LineOffset(sql, m.Index)}: {Snippet(sql, m.Index)}");
            }
        }

        Assert.True(mentions >= 60, $"the sweep found only {mentions} mentions of sample_interval_seconds in product SQL — it is not reading the readers");
        Assert.True(sanctionedCount >= 20, $"only {sanctionedCount} NULLIF(sample_interval_seconds, 0) denominators found — the population floor that makes an empty offender list mean something");
        Assert.Empty(offenders);
    }

    /// <summary>
    /// Rule 1, the derived arm: every <c>interval_seconds</c> / <c>interval_sec</c> alias whose definition
    /// reads the stored column routes the value through <c>NULLIF(…, 0)</c>. The definition may NAME the column
    /// bare in its three-state test (<c>sample_interval_seconds IS NULL → LAG</c>); every other mention is the
    /// value, and the value goes through NULLIF. On these trees there is no allowance; the Darling twin holds
    /// the two arms that read a successor aggregate which has already dropped the interval-0 collections.
    /// </summary>
    [Fact]
    public void EveryDerivedIntervalAlias_RoutesTheStoredValueThroughNullif()
    {
        var offenders = new List<string>();
        var definitions = 0;
        var storedBacked = 0;

        foreach (var (file, line, sql) in SqlBodies())
        {
            foreach (Match alias in DerivedIntervalAlias.Matches(sql))
            {
                definitions++;
                var expression = SelectItemBefore(sql, alias.Index);
                if (!StoredIntervalMention.IsMatch(expression))
                {
                    continue; /* a LAG-only interval (pre-column sources, Query Store): nothing stored to route */
                }

                storedBacked++;
                var valueMentions = IntervalIsNullTest.Replace(expression, string.Empty);
                var total = StoredIntervalMention.Matches(valueMentions).Count;
                var routed = NullifIntervalDenominator.Matches(valueMentions).Count;
                if (total != routed)
                {
                    offenders.Add($"{file}:{line + LineOffset(sql, alias.Index)}: {total - routed} value mention(s) of sample_interval_seconds outside NULLIF in: {Collapse(expression)}");
                }
            }
        }

        Assert.True(definitions >= 20, $"only {definitions} derived interval aliases found — the sweep is not reading the readers");
        Assert.True(storedBacked >= 15, $"only {storedBacked} of them read the stored column — the three-state idiom should be the majority");
        Assert.Empty(offenders);
    }

    /// <summary>
    /// Rule 1, the quotient arm: every division by a derived interval alias sits under a positive guard —
    /// <c>CASE WHEN interval &gt; 0 THEN</c> in its own select item, or an <c>interval &gt; 0</c> predicate in
    /// the same statement — and every guarded arm that falls back to <c>ELSE 0</c> is on the shrink-only
    /// roster, both directions. The guard is belt-and-braces (a NULLIF-built alias is NULL or positive by
    /// construction; a LAG-built one is 0 only for two rows sharing a collection time, which the guard
    /// turns into NULL) and the census says so rather than claiming the guard is what makes the quotient
    /// honest. Bound: "the same statement" is the whole literal, not the enclosing CTE — a guard in a
    /// sibling CTE would satisfy it. The alias itself is what carries the honesty; this arm catches the
    /// division written with no guard at all.
    /// </summary>
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

        Assert.True(divisions >= 25, $"only {divisions} divisions by a derived interval alias found — the sweep is not reading the readers");
        Assert.Empty(unguarded);

        /* Rule 1's quotient arm is complete on these trees: nothing falls back to 0. A rate arm added to the
           roster has to be a deliberate diff. Both directions: a new ELSE 0 arm must name itself here, and a
           retired one must leave. */
        Assert.Empty(RateArmsThatFallBackToZero);
        Assert.Equal(
            RateArmsThatFallBackToZero.OrderBy(s => s, StringComparer.Ordinal),
            zeroFallbacks.OrderBy(s => s, StringComparer.Ordinal));

        /* Positive control for the roster scan, on the exact shape the file-I/O trends USED before #3653
           retired it, and on a nested CASE inside the THEN (the shape the PostgreSQL wait trend used, in the
           Darling twin's trees). With the roster empty this control is what proves the scan is alive. */
        Assert.Equal(new[] { "read_mb_per_sec", "estimated_wait_ms_per_second" }, ZeroFallbackRateArms(
            "SELECT collection_time,\n"
            + "    CASE WHEN interval_seconds > 0\n     THEN CAST(delta_read_bytes AS DOUBLE PRECISION) / interval_seconds / 1048576.0\n     ELSE 0 END AS read_mb_per_sec,\n"
            + "    CASE WHEN interval_seconds > 0 THEN (CASE WHEN samples < prev THEN samples ELSE samples - prev END) * 1.0 / interval_seconds ELSE 0 END AS estimated_wait_ms_per_second,\n"
            + "    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second,\n"
            + "    CASE WHEN reads > 0 THEN read_time_ms / reads ELSE 0 END AS avg_read_ms\n"
            + "FROM t"));
    }

    /// <summary>The aliases of every zero-fallback RATE arm in one SQL text, in source order.</summary>
    private static List<string> ZeroFallbackRateArms(string sql)
    {
        var arms = new List<string>();
        foreach (Match tail in ZeroFallbackTail.Matches(sql))
        {
            var item = SelectItemBefore(sql, tail.Index);
            if (GuardedRateArmOpener.IsMatch(item) && DivisionByDerivedInterval.IsMatch(item))
            {
                arms.Add(tail.Groups["alias"].Value);
            }
        }

        return arms;
    }

    /* ---------------- rule 2: rates divide by measured elapsed ---------------- */

    /// <summary>The cadence literals a reader could assume: a minute, five, fifteen, thirty, an hour, a day.</summary>
    private const string CadenceLiterals = @"(?:60|300|900|1800|3600|86400)(?:\.0+)?";

    /// <summary>A delta column, then — with no other division between them — a division by a cadence literal on
    /// the same line. The no-intervening-slash bound keeps a unit chain (<c>/ 1000.0 / 60</c>, microseconds to
    /// minutes of TOTAL time) out: that is a conversion of a total, not a rate over an assumed cadence.</summary>
    private static readonly Regex SqlDeltaOverCadence = new(
        @"\bdelta_\w+\b[^,;\n/]*?/\s*" + CadenceLiterals + @"\b",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>A C# identifier carrying <c>Delta</c> as the DIVIDEND — at most one closing paren between it and the
    /// slash — over a cadence literal. Anchored on the dividend so <c>MaxDeltaFrequencyMinutes = DefaultMaxGapSeconds / 60</c>
    /// (a constant's seconds-to-minutes, with the delta-named identifier on the LEFT of the assignment) is outside it.</summary>
    private static readonly Regex CSharpDeltaOverCadence = new(
        @"\b\w*[Dd]elta\w*\b\s*\)?\s*/\s*" + CadenceLiterals + @"[dDmMfF]?\b",
        RegexOptions.Compiled);

    /// <summary>The rates that DO exist — a delta divided by the stored interval or its derived alias — for the
    /// population floor: rule 2 is "by measured elapsed", and the floor proves the measured divisions are
    /// what the sweep is reading.</summary>
    private static readonly Regex SqlDeltaOverMeasured = new(
        @"\bdelta_\w+\b[^,;]*?/\s*(?:NULLIF\(|(?:\w+\.)?interval_sec(?:onds)?\b)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Rule 2: zero divisions of a delta by an assumed cadence, in SQL and in C#, on every swept tree — and
    /// it stays zero, because a new one is a red build naming its line. Unit conversions (<c>/ 1000.0</c>,
    /// <c>/ 1048576.0</c>) are not cadences and are outside the literal set on purpose; a seconds-to-minutes
    /// conversion on a CONSTANT (<c>DefaultMaxGapSeconds / 60</c>) has no delta in its dividend and is outside
    /// the C# regex by construction.
    /// </summary>
    [Fact]
    public void NoRate_DividesADeltaByAnAssumedCadence()
    {
        /* Positive controls: the lie, in both languages. */
        Assert.Matches(SqlDeltaOverCadence, "SUM(delta_wait_time_ms) / 60.0 AS ms_per_min");
        Assert.Matches(SqlDeltaOverCadence, "delta_cntr_value * 1.0 / 60 AS per_second");
        Assert.Matches(SqlDeltaOverCadence, "CAST(delta_execution_count AS DOUBLE PRECISION) / 3600");
        Assert.Matches(CSharpDeltaOverCadence, "var rate = row.DeltaValue / 60.0;");
        Assert.Matches(CSharpDeltaOverCadence, "perSecond = deltaCntrValue / 300d;");
        Assert.Matches(CSharpDeltaOverCadence, "Math.Round((double)deltaWaitMs) / 3600, 2)");

        /* Negative controls: a unit conversion, a unit chain, and a constant's seconds-to-minutes. */
        Assert.DoesNotMatch(SqlDeltaOverCadence, "SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms");
        Assert.DoesNotMatch(SqlDeltaOverCadence, "SUM(delta_elapsed_time) / 1000000.0 / 60 AS elapsed_minutes");
        Assert.DoesNotMatch(CSharpDeltaOverCadence, "public const int MaxDeltaFrequencyMinutes = DefaultMaxGapSeconds / 60 / 2;");

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

        Assert.True(measured >= 20, $"only {measured} deltas divided by a measured interval found — the sweep is not reading the readers");
        Assert.Empty(offenders);
    }

    /* ---------------- rule 3: epoch change → ClearServer + marker ---------------- */

    private const string ComparatorFile = "PerformanceMonitor.Collectors/ServerEpoch.cs";
    private const string LiteHostLoop = "Lite/Services/RemoteCollectorService.cs";
    private const string DarlingHostLoop = "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs";
    private const string DarlingRunner = "Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs";
    private const string LiteRemovePaths = "Lite/MainWindow.xaml.cs";

    /// <summary>The definitions that observe an epoch, and which observation each makes (#3694, and the two
    /// residues closed after it). SQL Server's carriers are the wait-stats collector's and the CPU collector's
    /// second result sets (<c>sqlserver_start_time</c>, <c>@@SERVERNAME</c>) — the whole server's baselines ride
    /// on either; wait_stats is first in the order and does the forgetting, the CPU carrier is kept for the
    /// operator who disables wait_stats and finds the calculator already forgotten (ServerEpoch's "two carriers,
    /// one forget"). The PostgreSQL statements family carries its own <c>stats_reset</c> and Aurora's wait family
    /// its <c>pg_postmaster_start_time()</c>; each says nothing about any other counter and forgets only its own
    /// groups.</summary>
    private static readonly (string File, string Observation)[] Carriers =
    {
        ("PerformanceMonitor.Collectors/CpuUtilizationCollector.cs", "ObserveInstance"),
        ("PerformanceMonitor.Collectors/PgStatementStatsCollector.cs", "ObserveStatements"),
        ("PerformanceMonitor.Collectors/PgWaitStatsCollector.cs", "ObservePostmaster"),
        ("PerformanceMonitor.Collectors/WaitStatsCollector.cs", "ObserveInstance"),
    };

    /// <summary>
    /// Every site in the product that forgets a server's baselines, with its count: the comparator's three
    /// (one per observation — the instance observation's single <c>ClearServer</c> serves both SQL Server
    /// carriers; the statements and postmaster observations each <c>ClearGroups</c> their own family), Lite's
    /// two remove paths (a closed tab, a removed server) and Darling's two (reconcile-remove, and the same-id
    /// reconnect #3653 A5 added). Stated as an exact set so a forget that appears anywhere else — or one of
    /// these that disappears — is a diff to this list.
    /// </summary>
    private static readonly (string File, string Method, int Count)[] ForgetSites =
    {
        (ComparatorFile, "ClearGroups", 2),
        (ComparatorFile, "ClearServer", 1),
        (DarlingHostLoop, "ClearServer", 2),
        (LiteRemovePaths, "ClearServer", 2),
    };

    /// <summary>
    /// The SQL Server delta families scheduled BEFORE the first instance carrier, which would therefore
    /// subtract once against the dead baseline on the pass that sees the epoch (#3694's stated residual: five
    /// families, when the only carrier was <c>cpu_utilization</c>, tenth in the order). EMPTY since the pair
    /// moved onto <c>wait_stats</c>' batch, first on both hosts — and asserted empty from BOTH hosts' schedule
    /// order below, so a reorder that puts a delta family ahead of every carrier, or a carrier that stops
    /// carrying, reappears here as a diff rather than as fabricated intervals. The first carrier's OWN family
    /// is honest on the pass because it observes before its first subtraction; that ordering is pinned on the
    /// recording double in <c>ServerEpochTests</c>, not here.
    /// </summary>
    private static readonly string[] FamiliesThatSubtractOnceBeforeTheCarrier = Array.Empty<string>();

    /// <summary>Delta families no epoch observation covers. EMPTY since <c>pg_wait_stats</c> (Aurora's cumulative
    /// wait counters, which restart with the instance) carries <c>pg_postmaster_start_time()</c> on its own
    /// query — the SQL Server pair is read off a SQL Server DMV and <c>stats_reset</c> speaks for the statements
    /// view alone, so neither could have covered it. Asserted empty from the catalog and the carrier list, so an
    /// eleventh delta family arrives here before it arrives unwatched.</summary>
    private static readonly string[] FamiliesWithoutAnEpochCarrier = Array.Empty<string>();

    /// <summary>The SQL Server collectors whose observation covers every SQL Server delta family; whichever
    /// both hosts schedule first is the one whose position bounds <see cref="FamiliesThatSubtractOnceBeforeTheCarrier"/>.</summary>
    private static readonly string[] InstanceCarriers = { "wait_stats", "cpu_utilization" };

    /// <summary>
    /// Rule 3, the comparator's call sites: exactly the two carriers observe (one <c>ObserveInstance</c>, one
    /// <c>ObserveStatements</c>), no host calls the comparator directly, and every forget in the product is the
    /// comparator's or a remove path's — both directions, read off comment-stripped code so the remarks that
    /// discuss these calls cannot count as them.
    /// </summary>
    [Fact]
    public void EveryForgetSite_IsTheEpochComparatorOrAHostRemovePath()
    {
        var observers = new List<(string File, string Observation)>();
        var forgets = new List<(string File, string Method, int Count)>();

        foreach (var (file, code) in CodeBodies())
        {
            foreach (Match m in Regex.Matches(code, @"\bServerEpoch\.(Observe\w+)\("))
            {
                observers.Add((file, m.Groups[1].Value));
            }

            foreach (var method in new[] { "ClearGroups", "ClearServer" })
            {
                /* The CALL, not the declaration: a member access, so the calculator's own `public void
                   ClearServer(` and the interface's declaration do not count themselves. */
                var count = Regex.Matches(code, @"[\w?)]\s*\.\s*" + method + @"\(").Count;
                if (count > 0)
                {
                    forgets.Add((file, method, count));
                }
            }
        }

        Assert.Equal(
            Carriers.OrderBy(c => c.File, StringComparer.Ordinal),
            observers.OrderBy(c => c.File, StringComparer.Ordinal));

        Assert.Equal(
            ForgetSites.OrderBy(f => f.File, StringComparer.Ordinal).ThenBy(f => f.Method, StringComparer.Ordinal),
            forgets.OrderBy(f => f.File, StringComparer.Ordinal).ThenBy(f => f.Method, StringComparer.Ordinal));

        /* The comparator forgets BEFORE it records: ClearServer/ClearGroups precede the Measure and the
           PendingState write inside each observation, so the marker is never written for a forget that did
           not happen and the forget never happens without the marker. (The instance observation's forget is
           conditional on the per-calculator memo — a second carrier finding the calculator already on the new
           identity skips it — but the order of the three sites is what this pins, and the memo branch sits
           inside the same `if (changed)` the marker does.) */
        var comparator = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(ComparatorFile));
        foreach (var (observation, forget, measurement) in new[]
        {
            ("ObserveInstance", "ClearServer", "IdentityChangesMeasurement"),
            ("ObserveStatements", "ClearGroups", "StatementsChangesMeasurement"),
            ("ObservePostmaster", "ClearGroups", "PostmasterChangesMeasurement"),
        })
        {
            var body = comparator[comparator.IndexOf("public static bool " + observation + "(", StringComparison.Ordinal)..];
            body = body[..body.IndexOf("\n    }", StringComparison.Ordinal)];
            var forgetAt = body.IndexOf("context.Deltas." + forget + "(", StringComparison.Ordinal);
            var measureAt = body.IndexOf("context.Measure(" + measurement, StringComparison.Ordinal);
            var previousAt = body.IndexOf("PreviousStateKey] = Serialize(replaced)", StringComparison.Ordinal);
            Assert.True(forgetAt >= 0 && measureAt > forgetAt && previousAt > measureAt,
                $"{observation}: expected forget → measure → persist-the-replaced-pair, in that order");
        }
    }

    /// <summary>
    /// Rule 3, the marker's route on both hosts: each host loop drains the comparator's account after every
    /// collector run (so the sentence is logged beside the run that observed it, or by the next run if the
    /// carrier failed at its write), each host composes the run's measurements onto the
    /// <c>collection_log</c> note (<c>CollectorMeasurementNote.Compose(HostNote, Measurements)</c> — the
    /// #3161 seam, so <c>identity_epoch_changes=1</c> is a row with a collection_time in a table both stores
    /// already have), and the two labels are the strings a reader keys on. Nothing else in the product drains
    /// the account: a second drain would steal the sentence from the loop that logs it.
    /// </summary>
    [Fact]
    public void BothHosts_DrainTheDiscontinuityAccount_AndCarryTheMarkerOntoTheCollectionLog()
    {
        var drains = CodeBodies()
            .Where(b => Regex.IsMatch(b.Code, @"\.DrainDiscontinuities\("))
            .Select(b => b.File)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(new[] { DarlingHostLoop, LiteHostLoop }, drains);

        var notes = CodeBodies()
            .Where(b => b.Code.Contains("CollectorMeasurementNote.Compose(HostNote, Measurements)", StringComparison.Ordinal))
            .Select(b => b.File)
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(new[] { DarlingRunner, LiteHostLoop }, notes);

        /* Lite writes the composed note onto the collection_log row's error_message (the #1837 seam). */
        var lite = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(LiteHostLoop));
        Assert.Contains("errorMessage = telemetry.Note;", lite, StringComparison.Ordinal);

        /* The labels, as the constants the carriers measure under — a reader greps collection_log for these. */
        Assert.Equal("identity_epoch_changes", ServerEpoch.IdentityChangesMeasurement);
        Assert.Equal("statements_epoch_changes", ServerEpoch.StatementsChangesMeasurement);
        Assert.Equal("postmaster_epoch_changes", ServerEpoch.PostmasterChangesMeasurement);

        /* And the persisted pair, old beside new, under the carriers' declared state keys — the other half
           of the marker, in collector_state, needing no rung. Both SQL Server carriers declare the SAME two
           keys: the store keys state by (server_id, collector_name), so each holds its own prior under its
           own name. */
        Assert.Equal(new[] { ServerEpoch.IdentityStateKey, ServerEpoch.IdentityPreviousStateKey }, WaitStatsCollector.Instance.StateKeys);
        Assert.Equal(new[] { ServerEpoch.IdentityStateKey, ServerEpoch.IdentityPreviousStateKey }, CpuUtilizationCollector.Instance.StateKeys);
        Assert.Equal(new[] { ServerEpoch.StatementsStateKey, ServerEpoch.StatementsPreviousStateKey }, PgStatementStatsCollector.Instance.StateKeys);
        Assert.Equal(new[] { ServerEpoch.PostmasterStateKey, ServerEpoch.PostmasterPreviousStateKey }, PgWaitStatsCollector.Instance.StateKeys);
    }

    /// <summary>
    /// Rule 3's two residuals, read from the product rather than restated: the delta families both hosts
    /// schedule before the FIRST instance carrier (Darling iterates <see cref="CollectorScheduleDefaults.All"/> in
    /// declaration order; Lite's default schedule is <c>ScheduleManager.GetDefaultSchedules()</c>, pinned
    /// equal to the shared defaults elsewhere), and the family no observation covers. Each is asserted equal
    /// to its roster — both empty — so the roster describes the product and cannot describe anything else.
    /// Both instance carriers must be present in both orders: the CPU carrier is the one that stays when an
    /// operator disables wait_stats, and a schedule that dropped either would silently narrow the cover.
    /// </summary>
    [Fact]
    public void TheEpochResiduals_AreExactlyTheRosteredFamilies()
    {
        var families = CollectorDeltaCalculator.DeltaFamilyCollectors;

        var darlingOrder = CollectorScheduleDefaults.All.Keys.ToList();
        var liteOrder = ScheduleManager.GetDefaultSchedules().Select(s => s.Name).ToList();
        foreach (var carrier in InstanceCarriers)
        {
            Assert.Contains(carrier, darlingOrder);
            Assert.Contains(carrier, liteOrder);
            Assert.Contains(Carriers, c => c.Observation == "ObserveInstance" && c.File.EndsWith("/" + Pascal(carrier) + "Collector.cs", StringComparison.Ordinal));
        }

        foreach (var (host, order) in new[] { ("Darling", darlingOrder), ("Lite", liteOrder) })
        {
            var before = order
                .TakeWhile(name => !InstanceCarriers.Contains(name, StringComparer.OrdinalIgnoreCase))
                .Where(families.Contains)
                .OrderBy(n => n, StringComparer.Ordinal)
                .ToList();
            Assert.True(
                FamiliesThatSubtractOnceBeforeTheCarrier.SequenceEqual(before, StringComparer.Ordinal),
                $"{host}: the delta families scheduled before the first instance carrier are [{string.Join(", ", before)}], the roster says [{string.Join(", ", FamiliesThatSubtractOnceBeforeTheCarrier)}] — move the roster with the schedule, or the carrier");

            /* And the first carrier in the order is wait_stats on both hosts — the fact that makes the roster
               empty, and the carrier whose forget-before-first-subtraction ServerEpochTests pins. Stated by
               name so a reorder that promotes the CPU carrier ahead of it is a conscious diff here, not a
               quiet change of which collector's ordering the whole cover rests on. */
            Assert.Equal("wait_stats", order.First(name => InstanceCarriers.Contains(name, StringComparer.OrdinalIgnoreCase)));
        }

        /* The families the SQL Server carrier covers are every SQL Server delta family; the PostgreSQL
           statements family covers itself; what is left is the roster. Engine read off the catalog. */
        var uncovered = families
            .Select(f => CollectorCatalog.Find(f)!)
            .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql)
            .Select(d => d.Name)
            .Where(name => !Carriers.Any(c => c.File.EndsWith("/" + Pascal(name) + "Collector.cs", StringComparison.Ordinal)))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(FamiliesWithoutAnEpochCarrier.OrderBy(n => n, StringComparer.Ordinal), uncovered);
    }

    /* ---------------- rule 6: per-second names divided at comparison ---------------- */

    private static readonly Regex PerSecondAlias = new(
        @"\bAS\s+(\w+_per_sec(?:ond)?)\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex PerSecondName = new(
        @"\b\w+_per_sec(?:ond)?\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Rule 6: every per-second alias in product SQL is a quotient — its select item contains a division —
    /// or a window over one (<c>LAG(ms_per_sec)</c>, a prior rate for the restart heuristic); none passes a
    /// stored column through under the name. And no collector payload column carries the name at all: the
    /// store holds deltas, and the perfmon counters whose names end in <c>/sec</c> are stored as
    /// <c>delta_cntr_value</c> beside the interval they accrued over.
    /// </summary>
    [Fact]
    public void EveryPerSecondAlias_IsAQuotient_AndNoStoredColumnCarriesTheName()
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

        Assert.True(aliases >= 30, $"only {aliases} per-second aliases found — the sweep is not reading the readers");
        Assert.Empty(offenders);

        /* The store side: no payload column is a rate. Positive control on the population: the catalog is
           the real one (dozens of definitions) and perfmon's delta column is where the /sec counters land. */
        Assert.True(CollectorCatalog.All.Count >= 40);
        var rateColumns = CollectorCatalog.All
            .SelectMany(d => d.PayloadColumns.Select(c => $"{d.Name}.{c.Name}"))
            .Where(c => Regex.IsMatch(c, @"per_sec(?:ond)?", RegexOptions.IgnoreCase))
            .ToList();
        Assert.Empty(rateColumns);
        Assert.Contains(CollectorCatalog.Find("perfmon_stats")!.PayloadColumns, c => c.Name == "delta_cntr_value");
        Assert.True(PerfmonStatsCollector.DefaultCounters.Count(c => c.EndsWith("/sec", StringComparison.Ordinal)) >= 20,
            "the /sec-named counters are the population rule 6 speaks about; fewer than twenty means the list moved");
    }

    /* ---------------- rule 6, the C# half: every payload key that carries the name ---------------- */

    /// <summary>A payload key spelled as a C# identifier and given a value: an anonymous-object member or an
    /// object-initializer member (<c>waits_per_second = PerSecond(…)</c>). Read off CODE, so a SQL predicate
    /// inside a literal (<c>WHERE ms_per_sec = 0</c>) is blank where this looks. Not <c>==</c>, not
    /// <c>=&gt;</c>.</summary>
    private static readonly Regex IdentifierPayloadKey = new(
        @"(?<![\w.])(?<key>\w+_per_sec(?:ond)?)\s*=(?![=>])", RegexOptions.Compiled);

    /// <summary>A payload key spelled as a string and given a value through an indexer:
    /// <c>["current_ms_per_sec"] = peakRate</c>. Read off the comments-blanked text, where the literal is
    /// still legible and the brackets and the <c>=</c> are code.</summary>
    private static readonly Regex QuotedPayloadKey = new(
        @"\[\s*""(?<key>\w+_per_sec(?:ond)?)""\s*\]\s*=(?![=>])", RegexOptions.Compiled);

    /// <summary>The same key given a value through <c>Add("…", value)</c> / <c>TryAdd</c>. No product site writes
    /// this shape today; it is swept so the day one does it is a site and not a gap.</summary>
    private static readonly Regex AddedPayloadKey = new(
        @"\.(?:Try)?Add\(\s*""(?<key>\w+_per_sec(?:ond)?)""\s*,", RegexOptions.Compiled);

    /// <summary>A <c>const string</c> whose VALUE is a per-second name (<c>IoOpsPerSecKey = "ops_per_sec"</c>), so a
    /// key written through the constant (<c>[PgTargetScorer.IoOpsPerSecKey] = …</c>) is swept under the name it
    /// really writes.</summary>
    private static readonly Regex PerSecondKeyConstant = new(
        @"\bconst\s+string\s+(?<name>\w+)\s*=\s*""(?<key>\w+_per_sec(?:ond)?)""\s*;", RegexOptions.Compiled);

    /// <summary>An indexer keyed by an identifier (<c>[X.Y] = …</c>), a candidate constant-keyed site; it counts only
    /// when the last identifier is one of <see cref="PerSecondKeyConstant"/>'s names.</summary>
    private static readonly Regex ConstantIndexerKey = new(
        @"\[\s*(?:\w+\s*\.\s*)*(?<name>\w+)\s*\]\s*=(?![=>])", RegexOptions.Compiled);

    /// <summary><c>Add(X.Y, value)</c>, the constant twin of <see cref="AddedPayloadKey"/>.</summary>
    private static readonly Regex ConstantAddedKey = new(
        @"\.(?:Try)?Add\(\s*(?:\w+\s*\.\s*)*(?<name>\w+)\s*,", RegexOptions.Compiled);

    /// <summary>A division whose divisor names a measured span — an interval, an elapsed, a seconds, an observed
    /// duration — through any member path and an optional cast: <c>/ observedSeconds</c>, <c>/ p.IntervalSeconds</c>,
    /// <c>/ (double)seconds</c>, <c>/ TriggerElapsedMs</c>. What the divisor is NAMED is the bound: a measured
    /// span stored in a variable called <c>n</c> is outside this regex and reads as a division by nothing
    /// measured, which fails loud rather than quiet.</summary>
    private static readonly Regex DivisionByAMeasuredSpan = new(
        @"/\s*\(?\s*(?:\(\s*(?:double|decimal|float|long|int)\??\s*\)\s*)?(?:\w+\s*\.\s*)*\w*(?:[Ii]nterval|[Ee]lapsed|[Ss]econds|[Oo]bserved)\w*",
        RegexOptions.Compiled);

    /// <summary>Rule 2's literal set, as a divisor in a per-second key's value: <c>/ 60.0</c>, <c>/ 3600</c>. Built
    /// from the same <see cref="CadenceLiterals"/> the rule-2 pin uses, so the two cannot disagree about what a
    /// cadence is; rule 2's own regex anchors on a Delta-named dividend and stays where it is.</summary>
    private static readonly Regex DivisionByACadenceLiteral = new(
        @"/\s*" + CadenceLiterals + @"[dDmMfF]?\b", RegexOptions.Compiled);

    /// <summary>A call to a rate helper — a method whose name ends in <c>PerSecond</c> / <c>PerSec</c> — handed an
    /// interval or elapsed among its arguments: <c>PerSecond(r.DeltaSpins, r.SampleIntervalSeconds)</c>. The helper's
    /// body is pinned separately (<see cref="EveryRateHelper_DividesByTheIntervalItIsHanded"/>) to divide by
    /// that argument.</summary>
    private static readonly Regex RateHelperCall = new(
        @"\b(?<helper>\w*PerSec(?:ond)?)\s*\((?<args>[^()]*(?:\([^()]*\)[^()]*)*)\)", RegexOptions.Compiled);

    /// <summary>A rate helper's DEFINITION: a static method returning a double whose name ends in <c>PerSecond</c> /
    /// <c>PerSec</c>, with its parameter list.</summary>
    private static readonly Regex RateHelperDefinition = new(
        @"\bstatic\s+(?:double|decimal|float)\??\s+(?<helper>\w*PerSec(?:ond)?)\s*\((?<parameters>[^)]*)\)\s*(?<arrow>=>)?",
        RegexOptions.Compiled);

    /// <summary>A stored delta named in a value: the calculator's <c>Delta…</c> model fields (<c>r.DeltaWaitTimeMs</c>,
    /// <c>row.StoredDelta</c>) or a <c>delta_</c> column. Under a per-second key WITHOUT a division this is the
    /// exact lie rule 6 forbids — a per-interval delta labelled as a rate.</summary>
    private static readonly Regex StoredDeltaName = new(
        @"\b(?:\w+\s*\.\s*)?(?:[Dd]elta\w*|\w+[Dd]elta)\b|\bdelta_\w+", RegexOptions.Compiled);

    /// <summary>A member access whose member is rate-named (<c>p.WaitTimeMsPerSecond</c>) and is not a call.</summary>
    private static readonly Regex RateNamedMember = new(
        @"\.\s*(?<member>\w+PerSec(?:ond)?)\b(?!\s*\()", RegexOptions.Compiled);

    /// <summary>Any member access that is not a call — the population a value's provenance is read from.</summary>
    private static readonly Regex AnyMember = new(
        @"\.\s*(?<member>[A-Za-z_]\w*)\b(?!\s*\()", RegexOptions.Compiled);

    /// <summary>An aggregate over a reader's rate field — <c>points.Max(p =&gt; p.ReadsPerSecond)</c> — the C# twin
    /// of the SQL half's "a window over a rate" (<c>LAG(ms_per_sec)</c>): a max, min or mean of rates is a rate,
    /// and the field it aggregates is the one whose alias the SQL half proved.</summary>
    private static readonly Regex AggregateOverAMember = new(
        @"\.(?:Max|Min|Average)\(\s*\w+\s*=>\s*\w+\s*\.\s*(?<member>\w+PerSec(?:ond)?)\b", RegexOptions.Compiled);

    /// <summary>
    /// Payload keys on <see cref="PayloadKeyTrees"/> whose value is NOT the reader field of their own name and
    /// NOT a C# quotient — named as <c>file: key</c>, with the alias each really rests on, so the list can only
    /// shrink deliberately and a new key of this shape has to name itself and its alias to pass.
    /// <list type="bullet">
    /// <item><description><c>Lite/Analysis/AnomalyDetector.cs: current_ms_per_sec</c> — the wait-profile story's
    /// name for the window's PEAK, which the SQL aliases <c>peak_ms_per_sec</c> (the local it copies is read off
    /// that column); #3741 kept <c>current</c> as the peak when it put <c>avg_ms_per_sec</c> beside it. Rests on
    /// <c>peak_ms_per_sec</c>, asserted present in the same file's SQL.</description></item>
    /// <item><description><c>Lite/Mcp/McpQueryTools.cs: elapsed_ms_per_second</c> — <c>p.Value</c>: the duration
    /// trend point's <c>Value</c> IS the elapsed-ms-per-second alias (#3541 says so in the tool's own remarks,
    /// and the same payload carries <c>value</c> beside it as the same quantity). Rests on
    /// <c>elapsed_ms_per_second</c>, asserted present in Lite's SQL.</description></item>
    /// </list>
    /// Each entry rests on exactly one of two facts, and the census asserts whichever is named: <c>Alias</c>, a
    /// per-second alias the SQL half divides for (in the key's own file when the key is a story's name for a
    /// column that file reads); or <c>QuotientIn</c>, a <c>file: member</c> whose computed body divides by a
    /// measured span — the Darling twin's stall-probe rate is that shape; nothing on these trees is.
    /// </summary>
    private static readonly (string File, string Key, string? Alias, string? QuotientIn)[] PayloadKeysUnderAnotherNameThanTheirAlias =
    {
        ("Lite/Analysis/AnomalyDetector.cs", "current_ms_per_sec", "peak_ms_per_sec", null),
        ("Lite/Mcp/McpQueryTools.cs", "elapsed_ms_per_second", "elapsed_ms_per_second", null),
    };

    /// <summary>
    /// Rule 6, the C# half: every payload key on <see cref="PayloadKeyTrees"/> that carries a per-second name is
    /// given a value the census can trace to a measurement — a reader field of the same rate name (the SQL half
    /// proved that alias divides), a division by a measured span in the value or one local up, a rate helper
    /// handed the interval, or an aggregate over a reader's rate field — or is on the roster with the alias it
    /// rests on. Never a stored delta under the name, never a cadence literal, never a division by something
    /// that is not a span. Both directions on the roster; a population floor on the keys found, read off the
    /// real count so an empty sweep cannot pass; and the fixture control below is what proves each verdict
    /// fires on the shape it is for.
    ///
    /// <para><b>Bounds, stated.</b> "Reader field" is judged by NAME: the member the value copies must spell the
    /// key (<c>WaitTimeMsPerSecond</c> for <c>wait_time_ms_per_second</c>) and the key must be an alias some
    /// swept SQL divides for; the ordinal wiring from that alias to that member is positional and outside a
    /// text census. A bare local is followed ONE assignment up (<c>var bytesPerSec = tempBytes / observedSeconds</c>
    /// two lines before the stamp), assignments only — a local bound by a pattern (<c>callsPerSec is { } rate</c>)
    /// is accepted on the key's name alone when that name is an alias.</para>
    /// </summary>
    [Fact]
    public void EveryPerSecondPayloadKey_IsAReaderRateOrACSharpQuotient_OrIsNamed()
    {
        var sources = ProductSources(PayloadKeyTrees, PayloadKeyTreesFileFloor).ToList();
        var vocabulary = PerSecondAliasVocabulary(sources);
        Assert.True(vocabulary.Count >= 10, $"only {vocabulary.Count} distinct per-second aliases on the payload trees' SQL — the vocabulary is not reading the readers");

        var constants = PerSecondKeyConstants(sources);
        Assert.True(constants.Count >= 4, $"only {constants.Count} per-second key constants found — PgTargetScorer alone declares four");

        var sites = new List<PayloadKeySite>();
        foreach (var (file, text) in sources)
        {
            sites.AddRange(PayloadKeySites(file, text, constants, vocabulary));
        }

        Assert.True(sites.Count >= 10, $"only {sites.Count} per-second payload keys found on {PayloadKeyTrees.Length} trees (12 when this landed, the metric-to-table map entry among them) — the sweep is not reading the tools");

        var offenders = sites.Where(s => s.Verdict.StartsWith("OFFENDER", StringComparison.Ordinal)).Select(s => s.ToString()).ToList();
        Assert.True(offenders.Count == 0, "per-second payload key(s) whose value is a lie:\n  " + string.Join("\n  ", offenders));

        /* Both directions: the roster is exactly the keys whose provenance is not the alias of their own name —
           the ones it names (Rostered) and any it does not yet (UnderAnotherName), so a new key of the shape
           fails here naming itself, and a retired one fails here until its row leaves. */
        var underAnotherName = sites.Where(s => s.Verdict is UnderAnotherName or Rostered).Select(s => (s.File, s.Key)).OrderBy(s => s.File, StringComparer.Ordinal).ThenBy(s => s.Key, StringComparer.Ordinal).ToList();
        Assert.Equal(
            PayloadKeysUnderAnotherNameThanTheirAlias.Select(r => (r.File, r.Key)).OrderBy(s => s.File, StringComparer.Ordinal).ThenBy(s => s.Key, StringComparer.Ordinal),
            underAnotherName);

        /* And each roster entry rests on the one fact it names: an alias the SQL half divides for — in its own
           file when the key is the story's name for a column that file reads, on the trees when the model's
           field carries it — or a model member whose computed body is a quotient over a measured span. */
        foreach (var (file, key, alias, quotientIn) in PayloadKeysUnderAnotherNameThanTheirAlias)
        {
            Assert.True((alias is null) != (quotientIn is null), $"{file}: {key} must rest on exactly one of an alias or a model quotient");
            if (alias is not null)
            {
                Assert.Contains(alias, vocabulary);
                if (alias != key)
                {
                    Assert.Contains(alias, PerSecondAliasVocabulary(new[] { (file, ReadRepoFile(file)) }));
                }
            }
            else
            {
                var colon = quotientIn!.IndexOf(':', StringComparison.Ordinal);
                var (modelFile, member) = (quotientIn[..colon], quotientIn[(colon + 1)..].Trim());
                var modelCode = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(modelFile));
                var definition = Regex.Match(modelCode, @"\b" + Regex.Escape(member) + @"\s*=>");
                Assert.True(definition.Success, $"{modelFile}: no computed member {member} for {file}: {key} to rest on");
                var body = modelCode[(definition.Index + definition.Length)..(modelCode.IndexOf(';', definition.Index) + 1)];
                Assert.True(DivisionByAMeasuredSpan.IsMatch(body) && !DivisionByACadenceLiteral.IsMatch(body), $"{modelFile}: {member} does not divide by a measured span: {Collapse(body)}");
            }
        }

        /* The verdicts that DID fire, as a floor per kind: a census where every key came out one way is a census
           reading one file. */
        Assert.Contains(sites, s => s.Verdict == ReaderAlias);
        Assert.Contains(sites, s => s.Verdict == RateHelper);
        Assert.Contains(sites, s => s.Verdict == Rostered);
    }

    /// <summary>
    /// Rule 6's C# half, the fixture control: one synthetic file carrying every shape the census judges, with the
    /// verdict each must draw — including the planted <c>planted_per_sec = row.StoredDelta</c>, which MUST fail.
    /// Asserted as the whole ordered list, so a regex that stopped firing on one shape cannot pass by the others.
    /// </summary>
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
                ("honest_per_sec", ReaderAlias),            /* a local read off the reader, under a name the SQL divides for */
                ("hopped_per_sec", Quotient),               /* a local that divided by observedSeconds one assignment up */
                ("copied_per_sec", "OFFENDER"),             /* a local that copied a stored delta one assignment up */
                ("planted_per_sec", "OFFENDER"),            /* THE planted passthrough: a stored delta under a rate name */
                ("assumed_per_sec", "OFFENDER"),            /* a delta over a cadence literal */
                ("constant_per_sec", Quotient),             /* through a const, divided by observedSeconds */
                ("unknown_per_sec", UnderAnotherName),      /* a local nothing explains, under a name no SQL divides for */
                ("added_per_sec", Quotient),                /* Add(key, total / elapsed.TotalSeconds) */
                ("divided_per_second", Quotient),
                ("helped_per_second", RateHelper),
                ("aliased_per_second", ReaderAlias),        /* the member spells the key and the key is an alias */
                ("swapped_per_second", UnderAnotherName),   /* a rate-named member that spells a DIFFERENT key */
                ("peak_aliased_per_second", AggregateOverARate),
                ("halved_per_second", "OFFENDER"),          /* divides by a count, not a span */
                ("renamed_per_second", UnderAnotherName),   /* p.Value: not the field of its own name */
                ("mapped_per_second", MapEntry),            /* a name mapped to a name — outside the rule */
            },
            verdicts);

        /* The comparison inside the fixture (`compared = ms_per_sec == 0`) is not a key site: `==`. And a SQL
           predicate of that shape inside a literal is blank where the identifier regex looks. */
        Assert.DoesNotContain(verdicts, v => v.Key == "ms_per_sec");
        Assert.Empty(PayloadKeySites("Sql.cs", "var sql = @\"SELECT 1 FROM t WHERE ms_per_sec = 0 AND x_per_second = 2\";", constants, vocabulary));
    }

    /// <summary>
    /// Rule 6's C# half, the helper pin: every rate helper on the payload trees — a static method returning a
    /// double whose name ends in <c>PerSecond</c> / <c>PerSec</c> — divides by a parameter that names a measured
    /// span, so a call site the census accepted as <see cref="RateHelper"/> really did divide. Floored on the one
    /// Lite writes (<c>McpLatchSpinlockTools.PerSecond</c>); the Darling twin's trees carry none today and its
    /// copy of this pin says so.
    /// </summary>
    [Fact]
    public void EveryRateHelper_DividesByTheIntervalItIsHanded()
    {
        var helpers = new List<string>();
        var offenders = new List<string>();

        foreach (var (file, text) in ProductSources(PayloadKeyTrees, PayloadKeyTreesFileFloor))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);
            foreach (Match m in RateHelperDefinition.Matches(code))
            {
                helpers.Add($"{file}: {m.Groups["helper"].Value}");
                var body = m.Groups["arrow"].Success
                    ? code[(m.Index + m.Length)..(code.IndexOf(';', m.Index + m.Length) + 1)]
                    : CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', m.Index + m.Length));
                if (!DivisionByAMeasuredSpan.IsMatch(body) || DivisionByACadenceLiteral.IsMatch(body))
                {
                    offenders.Add($"{file}:{1 + LineOffset(code, m.Index)}: {m.Groups["helper"].Value} does not divide by a measured span: {Collapse(body)}");
                }
            }
        }

        Assert.Contains("Lite/Mcp/McpLatchSpinlockTools.cs: PerSecond", helpers);
        Assert.Empty(offenders);
    }

    private const string Quotient = "quotient";
    private const string RateHelper = "rate-helper";
    private const string ReaderAlias = "reader-alias";
    private const string AggregateOverARate = "aggregate-over-a-rate";
    private const string MapEntry = "map-entry";
    private const string UnderAnotherName = "under-another-name";
    private const string Rostered = "rostered";

    /// <summary>One payload key site and its verdict: <see cref="Quotient"/>, <see cref="RateHelper"/>,
    /// <see cref="ReaderAlias"/>, <see cref="AggregateOverARate"/>, <see cref="MapEntry"/>,
    /// <see cref="UnderAnotherName"/> (rosterable), <see cref="Rostered"/>, or <c>OFFENDER: reason</c>.</summary>
    private sealed record PayloadKeySite(string File, int Line, string Key, string Verdict, string Value)
    {
        public override string ToString() => $"{File}:{Line}: {Key} = {Collapse(Value)} — {Verdict}";
    }

    /// <summary>Every per-second payload key in one file, with its verdict. Keys are found on the comments-blanked
    /// text (so a string key is legible) and on the code (so an identifier key is), the value is read off the code
    /// (so a literal value is blank and a SQL predicate cannot be a value), and locals are followed one assignment
    /// up through the code before the site.</summary>
    private static IEnumerable<PayloadKeySite> PayloadKeySites(string file, string text, IReadOnlyDictionary<string, string> constants, ISet<string> vocabulary)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var kept = BlankComments(text);
        var sites = new List<(int Index, string Key, int ValueStart, char Closer)>();

        foreach (Match m in IdentifierPayloadKey.Matches(code))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length, '\0'));
        }

        foreach (Match m in QuotedPayloadKey.Matches(kept))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length, '\0'));
        }

        foreach (Match m in AddedPayloadKey.Matches(kept))
        {
            sites.Add((m.Index, m.Groups["key"].Value, m.Index + m.Length, ')'));
        }

        foreach (Match m in ConstantIndexerKey.Matches(code))
        {
            if (constants.TryGetValue(m.Groups["name"].Value, out var key))
            {
                sites.Add((m.Index, key, m.Index + m.Length, '\0'));
            }
        }

        foreach (Match m in ConstantAddedKey.Matches(code))
        {
            if (constants.TryGetValue(m.Groups["name"].Value, out var key))
            {
                sites.Add((m.Index, key, m.Index + m.Length, ')'));
            }
        }

        foreach (var (index, key, valueStart, _) in sites.OrderBy(s => s.Index))
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

    /// <summary>The verdict on one value expression — see <see cref="PayloadKeySite"/>. Order matters: a division is
    /// judged before a delta name (a delta divided by its interval is the honest shape), and a bare local is
    /// followed one assignment up before its name is judged.</summary>
    private static string Verdict(string key, string value, string code, int site, ISet<string> vocabulary, int hops)
    {
        var trimmed = value.Trim();
        if (trimmed.Length == 0)
        {
            return MapEntry; /* the whole value was a literal: a name mapped to a name, not a measurement */
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
            /* A bare local: its nearest preceding assignment in this file is where the value was made. */
            var assignment = Regex.Matches(code[..site], @"(?<![\w.])" + Regex.Escape(trimmed) + @"\s*=(?![=>])").LastOrDefault();
            if (assignment is not null)
            {
                var made = ValueExpression(code, assignment.Index + assignment.Length);
                var upstream = Verdict(key, made, code, assignment.Index, vocabulary, hops - 1);
                if (upstream != UnderAnotherName && upstream != ReaderAlias)
                {
                    return upstream; /* a quotient, a helper, an aggregate or an offender, one assignment up */
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

    /// <summary>The value expression starting at <paramref name="start"/>: to the first <c>,</c> or <c>;</c> at
    /// depth zero, or the closer of whatever the site sits inside. Read off CODE, so a brace in a literal cannot
    /// unbalance it.</summary>
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

    /// <summary>Every distinct per-second alias in the SQL of <paramref name="sources"/>, lower-cased — the names
    /// rule 6's SQL half has already proved are quotients.</summary>
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

    /// <summary>Every <c>const string</c> on <paramref name="trees"/> whose value is a per-second name, by the
    /// constant's simple name.</summary>
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

    /// <summary>The source with comments blanked and everything else — code and literal text — kept, newlines
    /// preserved. The third text beside <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> (code only) and
    /// <see cref="CSharpSourceWalker.StringLiteralBodies"/> (literals only), built on the same walk so a delimiter
    /// one understands cannot desynchronise this one.</summary>
    private static string BlankComments(string text)
    {
        var keep = CSharpSourceWalker.CodeMask(text);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = start; i < start + body.Length; i++)
            {
                keep[i] = true;
            }

            /* The literal's own quote delimiters are neither code nor body; a quoted key needs them legible. A raw
               string's triple quotes are not restored — no key is written inside one, and its body is kept. */
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

    /// <summary><c>WaitTimeMsPerSecond</c> → <c>wait_time_ms_per_second</c>.</summary>
    private static string Snake(string pascal) =>
        Regex.Replace(pascal, @"(?<=[a-z0-9])(?=[A-Z])", "_").ToLowerInvariant();

    /* ---------------- rule 8: gauges never delta'd (the census, since the rung) ---------------- */

    /// <summary>
    /// Rule 8, censused (V132 / v62, #3653 A7). Three facts, each of which the rule rests on: the perfmon payload
    /// carries <c>cntr_type</c> as its LAST column (positional writers; an ALTER lands at the end); the collector's
    /// one delta call sits inside the branch guarded by <c>IsGauge</c> and nowhere else, so a gauge row can
    /// never reach the calculator (the value-level proof — a gauge row writes NULL/NULL and records no delta
    /// call — is <c>PerfmonStatsCollectorDefinitionTests</c>); and every product SQL that sums or projects
    /// <c>delta_cntr_value</c> off the perfmon family also selects <c>cntr_type</c>, so no reader is left
    /// differencing a level it cannot recognise. The default list still carries both kinds by name — that is
    /// why the type, not the name, is the classifier. Population floors keep a dead filter from passing.
    /// </summary>
    [Fact]
    public void TheGaugeRule_TheCollectorWritesNoDeltaForAGaugeType_AndEveryReaderClassifiesByTheStoredType()
    {
        var perfmon = CollectorCatalog.Find("perfmon_stats")!;
        Assert.Equal("cntr_type", perfmon.PayloadColumns[^1].Name);
        Assert.Equal(CollectorColumnType.Integer, perfmon.PayloadColumns[^1].Type);

        /* Both kinds are still collected under one counter list — rates by name, and levels the name does not
           mark — which is exactly why a name cannot be the classifier. */
        var counters = PerfmonStatsCollector.DefaultCounters;
        Assert.True(counters.Count(c => c.EndsWith("/sec", StringComparison.Ordinal)) >= 20);
        Assert.True(counters.Count(c => !c.EndsWith("/sec", StringComparison.Ordinal)) >= 10);
        Assert.Contains("Total Server Memory (KB)", counters);
        Assert.Contains("Memory Grants Pending", counters);
        Assert.Contains("Batch Requests/sec", counters);

        /* The write: one delta call, inside the non-gauge branch. The branch is read structurally off the
           stripped source — the `if (!IsGauge(` guard opens before the call and its block closes after it. */
        var writer = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile("PerformanceMonitor.Collectors/PerfmonStatsCollector.cs"));
        var payload = writer[writer.IndexOf("public override void WritePayload(", StringComparison.Ordinal)..];
        Assert.Single(Regex.Matches(payload, @"CalculateDeltaWithInterval\("));
        var guard = payload.IndexOf("if (!IsGauge(row.CntrType))", StringComparison.Ordinal);
        var call = payload.IndexOf("CalculateDeltaWithInterval(", StringComparison.Ordinal);
        Assert.True(guard >= 0, "the collector's WritePayload has no IsGauge guard — gauges are being differenced");
        var guardBlock = CSharpSourceWalker.BraceBalanced(payload, payload.IndexOf('{', guard));
        Assert.True(guard < call && call < guard + guardBlock.Length, "the delta call sits outside the IsGauge guard");
        Assert.NotEmpty(PerfmonStatsCollector.GaugeCounterTypes);

        /* The reads: every product SQL literal that aggregates or projects the perfmon delta selects the type
           beside it. Population floor: the two trend reads per SKU plus the two latest-snapshot reads. */
        var perfmonReads = SqlBodies()
            .Where(b => b.Sql.Contains("perfmon_stats", StringComparison.Ordinal)
                        && Regex.IsMatch(b.Sql, @"\bdelta_cntr_value\b")
                        && Regex.IsMatch(b.Sql, @"^\s*SELECT", RegexOptions.Multiline)
                        && !b.Sql.Contains("Batch Requests/sec", StringComparison.Ordinal)
                        && !b.Sql.Contains("SQL Compilations/sec", StringComparison.Ordinal))
            .ToList();
        Assert.True(perfmonReads.Count >= 5, "fewer than five perfmon delta reads swept: " + perfmonReads.Count);
        var blind = perfmonReads.Where(b => !b.Sql.Contains("cntr_type", StringComparison.Ordinal)).Select(b => $"{b.File}:{b.Line}").ToList();
        Assert.True(blind.Count == 0, "perfmon delta read(s) that do not select cntr_type:\n  " + string.Join("\n  ", blind));
    }

    /* ---------------- rules 4 and 7: the numbers on the record ---------------- */

    /// <summary>The two rule numbers the record holds, in the tests that hold them — so the list above cannot
    /// quietly renumber either.</summary>
    [Fact]
    public void TheTwoPinnedRules_KeepTheNumbersTheirTestsGaveThem()
    {
        Assert.Contains("the measurement contract's rule 4", ReadRepoFile("Lite.Tests/DeltaFamilyIntervalColumnTests.cs"), StringComparison.Ordinal);
        Assert.Contains("the measurement contract's rule 7", ReadRepoFile("Lite.Tests/DeltaFamilySeedingCensusTests.cs"), StringComparison.Ordinal);
        Assert.Equal(10, CollectorDeltaCalculator.DeltaFamilyCollectors.Count);
    }

    /* ---------------- helpers ---------------- */

    /// <summary>Every SQL-bearing string literal on the swept trees, with its file (repo-relative, forward
    /// slashes) and the 1-based line the literal starts on. SQL comments are blanked with newlines kept, so
    /// offsets inside the body still map to lines.</summary>
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

    /// <summary>Every product file's CODE — comments and literal text blanked, newlines kept.</summary>
    private static IEnumerable<(string File, string Code)> CodeBodies()
    {
        foreach (var (file, text) in ProductSources(SweptTrees, SweptTreesFileFloor))
        {
            yield return (file, CSharpSourceWalker.StripCommentsAndStrings(text));
        }
    }

    /// <summary>The floor that makes every empty offender list above mean something: the ten trees hold well over
    /// a thousand source files; a sweep that read a hundred read the wrong checkout (1,089 when this landed).</summary>
    private const int SweptTreesFileFloor = 900;

    /// <summary>The same floor for <see cref="PayloadKeyTrees"/>, the ten less the two Darling trees.</summary>
    private const int PayloadKeyTreesFileFloor = 600;

    /// <summary>Every <c>*.cs</c> under <paramref name="trees"/> (bin/ and obj/ excluded), as repo-relative
    /// forward-slash path and LF-normalised text, floored at <paramref name="fileFloor"/> files.</summary>
    private static IEnumerable<(string File, string Text)> ProductSources(string[] trees, int fileFloor)
    {
        var root = RepoRoot();
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

    /// <summary>Blanks <c>/* … */</c> and <c>-- …</c> inside SQL text, keeping every newline so line arithmetic
    /// over the result matches the source.</summary>
    private static string BlankSqlComments(string sql)
    {
        sql = Regex.Replace(sql, @"/\*.*?\*/", m => Regex.Replace(m.Value, @"[^\n]", " "), RegexOptions.Singleline);
        return Regex.Replace(sql, @"--[^\n]*", m => new string(' ', m.Length));
    }

    /// <summary>The select-item expression that ends at <paramref name="end"/>: walks back over balanced
    /// parentheses to the previous top-level comma, the <c>SELECT</c> keyword, or an opening paren.</summary>
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

    /// <summary><c>pg_wait_stats</c> → <c>PgWaitStats</c>.</summary>
    private static string Pascal(string snake) =>
        string.Concat(snake.Split('_').Select(p => char.ToUpperInvariant(p[0]) + p[1..]));

    private static string ReadRepoFile(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar))).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The tracked solution file marks the root — a marker that survives a git worktree, whose
    /// <c>.git</c> is a file (the RepoFile idiom).</summary>
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, $"PerformanceMonitor.sln not found walking up from {thisFile}");
        return dir!;
    }
}
