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

    /* ---------------- rule 5: rollups aggregate interval > 0 only ---------------- */

    /// <summary>The collector's knowability verdict, baked into an aggregate's WHERE.</summary>
    private const string IntervalPredicate = "sample_interval_seconds IS DISTINCT FROM 0";

    /// <summary>
    /// Aggregates over a delta family that admit the unknowable (0, 0) row, named so the list can only shrink
    /// deliberately. Two kinds, stated apart because they retire differently:
    /// <list type="bullet">
    /// <item><description><c>perfmon_baseline</c>, <c>wait_stats_baseline</c> — the SUPERSEDED pair (#3698).
    /// Not registered (nothing creates them any more), read by <c>PgBaselineProvider</c>'s legacy arm for the
    /// window the successor does not yet cover, dropped by <see cref="TimescaleSupport.DropRetiredBaselineAggregatesAsync"/>
    /// once the successor's <c>min(bucket)</c> reaches the 35-day tier. They leave this list by moving to
    /// <see cref="TimescaleSupport.RetiredBaselineRelations"/> — the text is kept until then because the live
    /// retirement test builds its fixture from it.</description></item>
    /// <item><description><c>query_stats_hourly</c>, <c>procedure_stats_hourly</c>, <c>query_stats_db_hourly</c>
    /// — the REGISTERED hourly rollups over two delta families, built (#1849 era) before those tables carried
    /// an interval. A restart's (0, 0) row costs their <c>sum()</c>s nothing, but <c>count(*) AS sample_count</c>
    /// counts it as a sample on all three, and <c>min(delta_*)</c> reads it as a real minimum on the two that
    /// carry a min (<c>query_stats_db_hourly</c> carries sums and the sample count only); the daily tier is
    /// hierarchical from these and inherits both. A continuous aggregate cannot be altered in place, so the
    /// fix is #3698's shape — a successor under a new name, <c>WITH NO DATA</c>, backfilled, coverage-gated
    /// retirement — and it moves the hourly refresh phase grid, which is why it is a lane and not a line.
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
        Assert.True(governed.Count >= 8, $"only {governed.Count} aggregates read a delta family — expected the two successors, the legacy pair, query_stats_baseline and the three hourly rollups at least");

        var carrying = governed.Where(a => a.CarriesIntervalPredicate).Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal).ToList();
        var equivalent = governed.Where(a => !a.CarriesIntervalPredicate && EquivalentPredicates.Any(e => e.View == a.View)).ToList();
        var admitting = governed.Where(a => !a.CarriesIntervalPredicate && EquivalentPredicates.All(e => e.View != a.View)).Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal).ToList();

        /* The successors carry it — the whole point of #3698 — and are the two registered in the pair's
           positions. */
        Assert.Equal(new[] { TimescaleSupport.PerfmonIntervalBaselineView, TimescaleSupport.WaitStatsIntervalBaselineView }, carrying);
        Assert.All(carrying, view => Assert.Contains(view, TimescaleSupport.BaselineAggregates.Select(a => a.View)));

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
        foreach (var view in registeredAdmitting)
        {
            Assert.Contains(view, TimescaleSupport.HourlyAggregates.Select(a => a.View));
            var text = Text(aggregates.Single(a => a.View == view).Constant);
            foreach (var shape in contamination[view])
            {
                Assert.Contains(shape, text, StringComparison.Ordinal);
            }

            /* The db-hourly carries no min — the roster's remark says so; keep it true. */
            Assert.Equal(contamination[view].Contains("min(delta_"), Regex.IsMatch(text, @"\bmin\(delta_\w+\)"));
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
        foreach (var (file, text) in ProductSources())
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
        foreach (var (file, text) in ProductSources())
        {
            yield return (file, CSharpSourceWalker.StripCommentsAndStrings(text));
        }
    }

    private static IEnumerable<(string File, string Text)> ProductSources()
    {
        var root = RepoFile.Root;
        var files = 0;

        foreach (var tree in SweptTrees)
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

        Assert.True(files >= 80, $"the sweep read only {files} files across {SweptTrees.Length} trees (106 when this landed)");
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
