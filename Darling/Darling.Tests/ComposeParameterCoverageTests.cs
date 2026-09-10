/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3211: <c>ComposeCompiler</c> keeps the correspondence between the SQL text it appends and the positional
/// parameters <c>ParamList</c> allocates BY HAND, at each site. A site that binds a value it already bound
/// gets a fresh ordinal, writes valid SQL that still cites the original, and shifts every ordinal allocated
/// after it by one. Nothing checked that, and the mutation survived 273 tests.
///
/// <para><b>Why the existing ordinal assertions cannot close it.</b> They are per-statement, and each asserts
/// the ordinals of the statement it was written for.
/// <c>DarlingComposeTests.CompileAnnotations_EmitsSchemaQualifiedCollect_WindowAndServerBound_Capped</c>
/// counts parameters, but on a UTC-framed source that never takes the offset join the double bind lived in;
/// <c>CompileAnnotations_EachSource_SelectsItsCatalogTimeAndLabelColumns</c> reaches the affected statement
/// but counts nothing. So the realistic regression — a NEW site, written by someone who binds a value they
/// already had — is what neither shape can see, and adding a third per-site assertion would not change
/// that.</para>
///
/// <para><b>A duplicate bind has two spellings, and they need two different instruments — measured, not
/// reasoned.</b> Coverage of the placeholders alone catches only one of them:
/// <list type="bullet">
/// <item>The extra bind's placeholder is DISCARDED and the text keeps citing the variable holding the
/// original. The extra ordinal is then bound and never cited, which is what
/// <see cref="CoverageViolation"/> sees.</item>
/// <item>The extra bind's placeholder is USED, at the site that prompted the second bind, while an earlier
/// site keeps citing the original. Every ordinal from 1 to <c>Parameters.Count</c> then appears in the SQL
/// and coverage is SATISFIED — as is any max-based comparison. This is the spelling of the #3202
/// alternative (a second <c>p.AddTextArray(context.Servers!)</c> for the offset join): applied to the
/// compiler it produces four bound parameters and a statement citing <c>$1</c> through <c>$4</c>, and
/// neither check fires.</item>
/// </list>
/// So the check that closes the defect class is a COUNT the plan predicts —
/// <see cref="PredictedParameterCount"/>, two for the window plus a server scope plus one per filter plus
/// one for a ranked mode's <c>topN</c> — compared against what the compiler bound. An extra bind raises the
/// actual and leaves the prediction where it was, whichever spelling it took.
/// <see cref="TheThreeChecks_SeeDifferentHalvesOfADuplicateBind"/> pins all three verdicts against both
/// spellings side by side, so the reasoning cannot be re-derived wrongly from either one alone.</para>
///
/// <para><b>Both instruments stay,</b> because each catches something the other cannot: the count sees an
/// extra bind, coverage sees a bind whose placeholder never reached the text (an unemitted branch, a
/// dropped return value), and the range arm sees an ordinal written past the end of the bound set. None of
/// the three is a superset of another.</para>
///
/// <para><b>The prediction cannot be silently outgrown.</b> It restates the compiler's binding rule, so
/// <see cref="ThePredictionCoversEveryBindingSite_InTheCompiler"/> counts the <c>ParamList</c> call sites in
/// <c>ComposeCompiler.cs</c> itself: a new one reds here and has to be folded into the rule deliberately,
/// which is the "next site someone adds" case a per-statement ordinal assertion cannot reach.</para>
///
/// <para><b>The corpus is derived, and its reach is asserted.</b> A sweep that compiles nothing reports a
/// clean bill of health, so <see cref="EveryCompiledStatement_BindsWhatItsPlanPredicts_AndCitesAllOfIt"/>
/// requires that EVERY measure in the catalog and EVERY annotation source contributed at least one
/// statement, and that each panel mode is represented — a floor derived from the catalog rather than a
/// number typed here, which a new measure raises on its own. Same discipline as
/// <c>ServerLocalReadFrameDisciplineTests</c> and <c>CreationTimeClockFrameDisciplineTests</c>.</para>
/// </summary>
public sealed class ComposeParameterCoverageTests
{
    /// <summary>A positional placeholder in composed SQL. Matched as a whole number, not a substring: a
    /// <c>Contains("$1")</c> style check is satisfied by <c>$10</c>, so it would count an ordinal that was
    /// never cited as cited on any statement that got past nine parameters.</summary>
    private static readonly Regex Placeholder = new(@"\$(\d+)");

    private static readonly DateTime WindowStart = new(2026, 7, 18, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = new(2026, 7, 18, 6, 0, 0, DateTimeKind.Utc);
    private static readonly string[] TwoServers = ["SERVER-A", "SERVER-B"];

    /* ───────────────────────── the invariant ───────────────────────── */

    /// <summary>
    /// The property, in both directions, over one compiled statement. Returns null when the statement is
    /// sound, or the reason it is not.
    ///
    /// <para>The FIRST direction — every cited ordinal is bound — is what stops an off-by-one from reaching
    /// Postgres as a bind failure. The SECOND — every bound parameter is cited — is the one that sees a
    /// duplicate bind, because the duplicate is what gets allocated while the SQL keeps citing the original.
    /// A statement can satisfy either alone.</para>
    /// </summary>
    internal static string? CoverageViolation(string label, string sql, IReadOnlyList<NpgsqlParameter> parameters)
    {
        var cited = Placeholder.Matches(sql)
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .ToHashSet();

        var outOfRange = cited.Where(n => n < 1 || n > parameters.Count).OrderBy(n => n).ToArray();
        if (outOfRange.Length > 0)
        {
            return $"{label}: SQL cites ${string.Join(", $", outOfRange)} but only {parameters.Count} "
                + "parameter(s) are bound — the statement would fail to bind.";
        }

        var uncited = Enumerable.Range(1, parameters.Count).Where(n => !cited.Contains(n)).ToArray();
        if (uncited.Length > 0)
        {
            return $"{label}: ${string.Join(", $", uncited)} is bound but never cited in the SQL — the "
                + "signature of a value bound twice, which shifts every ordinal allocated after it.";
        }

        return null;
    }

    /// <summary>
    /// The cheapest alternative: the highest ordinal cited must not exceed the number bound. It is here ONLY
    /// as a control in <see cref="TheThreeChecks_SeeDifferentHalvesOfADuplicateBind"/> — an extra bind raises
    /// the count and leaves the maximum at or below it, so this form is satisfied by both spellings of the
    /// defect. It is never applied to the corpus, because everything it would catch the range arm of
    /// <see cref="CoverageViolation"/> catches too.
    /// </summary>
    internal static string? MaxBasedViolation(string label, string sql, IReadOnlyList<NpgsqlParameter> parameters)
    {
        var highest = Placeholder.Matches(sql)
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .DefaultIfEmpty(0)
            .Max();

        return highest > parameters.Count
            ? $"{label}: SQL cites ${highest} but only {parameters.Count} parameter(s) are bound."
            : null;
    }

    /// <summary>
    /// How many parameters a measure panel's plan and its run context PREDICT — the compiler's binding rule
    /// restated as a function of the author's intent rather than read back off the compiler's own output.
    /// Two for the naive-UTC window, one more when the run names servers, one per filter (every
    /// <c>BuildFilterClause</c> arm binds exactly one value, whether a text array or a scalar), and one for
    /// <c>topN</c> in the two ranked modes.
    ///
    /// <para>An extra bind anywhere raises what the compiler produced and leaves this where it was, which is
    /// what makes it see the spelling coverage cannot. It is deliberately NOT derived from
    /// <c>Parameters.Count</c> — a prediction taken from the thing it is checking agrees with it always.</para>
    /// </summary>
    internal static int PredictedParameterCount(PanelPlan plan, bool serverScoped) =>
        2
        + (serverScoped ? 1 : 0)
        + plan.Filters.Count
        + (plan.Mode is PanelMode.Ranked or PanelMode.RankedTimeSeries ? 1 : 0);

    /// <summary>
    /// The same prediction for an annotation query, whose whole parameter set is the window plus the
    /// optional server scope: the catalog supplies its table, time column and label column as compiler
    /// constants, and the per-server offset join reuses the array the outer predicate already bound rather
    /// than binding a second one.
    /// </summary>
    internal static int PredictedAnnotationParameterCount(bool serverScoped) => 2 + (serverScoped ? 1 : 0);

    /* ───────────────────────── the corpus sweep ───────────────────────── */

    [Fact]
    public void EveryCompiledStatement_BindsWhatItsPlanPredicts_AndCitesAllOfIt()
    {
        var corpus = Corpus();

        var overBound = corpus
            .Where(c => c.Compiled.Parameters.Count != c.Predicted)
            .Select(c => $"{c.Label}: bound {c.Compiled.Parameters.Count}, its plan predicts {c.Predicted}")
            .ToArray();
        Assert.True(
            overBound.Length == 0,
            "compiled statement(s) bound a different number of parameters than their plan predicts — a "
            + "count above the prediction is a value bound twice:\n" + string.Join("\n", overBound));

        var violations = corpus
            .Select(c => CoverageViolation(c.Label, c.Compiled.Sql, c.Compiled.Parameters))
            .Where(v => v is not null)
            .ToArray();

        Assert.True(violations.Length == 0, string.Join("\n", violations));

        /* The prediction is not a constant the compiler happens to match: it has to take several values
           across the corpus, or a predictor stuck at one number would agree with a compiler stuck at the
           same one. Window-only through window + scope + two filters + topN is five distinct counts. */
        Assert.True(
            corpus.Select(c => c.Predicted).Distinct().Count() >= 5,
            "the prediction took fewer than five distinct values across the corpus, so it is not "
            + "discriminating between statement shapes");

        /* The reach, derived rather than declared. Each of these fails toward "the sweep examined less than
           it claims to", which is the failure a bare total cannot report. */
        var measuresCovered = corpus.Where(c => c.Measure is not null).Select(c => c.Measure!).ToHashSet(StringComparer.Ordinal);
        var missingMeasures = MeasureCatalog.Measures
            .Select(m => m.Key)
            .Where(k => !measuresCovered.Contains(k))
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.True(
            missingMeasures.Length == 0,
            $"the corpus compiled no statement for {missingMeasures.Length} measure(s): {string.Join(", ", missingMeasures)}");

        var annotationsCovered = corpus.Where(c => c.Annotation is not null).Select(c => c.Annotation!).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(
            MeasureCatalog.AnnotationSources.Select(a => a.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray(),
            annotationsCovered.OrderBy(k => k, StringComparer.Ordinal).ToArray());

        /* Every panel mode, and both server scopes within each: a corpus that lost the RankedTimeSeries arm
           would still cover every measure, and that arm is the only one with a rank CTE to bind inside.
           Asserted as a named gap rather than with Assert.Contains, whose failure output is the whole
           corpus — every compiled statement in it — which is unreadable at exactly the moment it matters. */
        var missingShapes = Enum.GetValues<PanelMode>()
            .SelectMany(mode => new[] { true, false }.Select(scoped => (mode, scoped)))
            .Where(s => !corpus.Any(c => c.Mode == s.mode && c.ServerScoped == s.scoped))
            .Select(s => $"{s.mode}/{(s.scoped ? "scoped" : "fleet")}")
            .ToArray();
        Assert.True(
            missingShapes.Length == 0,
            $"the corpus compiled no statement for: {string.Join(", ", missingShapes)}");

        /* Every filter operator reaches a compiled predicate, since each binds through its own
           BuildFilterClause arm, and a variable-resolved filter as well as a literal one. */
        var missingOps = MeasureCatalog.FilterOpWireNames
            .Where(op => !corpus.Any(c => string.Equals(c.FilterOp, op, StringComparison.Ordinal)))
            .ToArray();
        Assert.True(
            missingOps.Length == 0,
            $"the corpus compiled no filtered statement for operator(s): {string.Join(", ", missingOps)}");

        Assert.True(corpus.Any(c => c.UsesVariable), "the corpus compiled no variable-resolved filter");

        /* Parameter-free statements cannot exhibit the defect, so a corpus that had quietly become all
           fleet-wide-scalar would pass the sweep having asserted nothing about binding at all. */
        Assert.True(
            corpus.Count(c => c.Compiled.Parameters.Count >= 3) >= MeasureCatalog.Measures.Count,
            $"only {corpus.Count(c => c.Compiled.Parameters.Count >= 3)} statement(s) bound three or more "
            + "parameters — the corpus is not exercising the sites past the window prelude.");
    }

    /* ───────────────────────── the discriminator ───────────────────────── */

    /// <summary>
    /// The three checks against both spellings of one defect, in one table, because the verdicts are not
    /// what the reasoning in #3211 predicted and the difference is only visible side by side.
    ///
    /// <para>Both statements below bind the panel's server array TWICE. They differ only in whether the
    /// second bind's placeholder reaches the text. Where it does — the spelling a contributor actually
    /// writes, because they added the bind in order to use it — every ordinal appears and BOTH text-shaped
    /// checks are satisfied. Only the predicted count sees it.</para>
    ///
    /// <para>Built from hand-written pairs rather than from a mutated compiler, deliberately: the compiler's
    /// current text is not the subject — the DISCRIMINATION is. A future rewrite of
    /// <c>CompileAnnotation</c> must not be able to retire this by making the mutation unreachable, and a
    /// contributor who drops the count check in favour of a text-shaped one has to delete a test that says
    /// in one line why that does not work.</para>
    /// </summary>
    [Fact]
    public void TheThreeChecks_SeeDifferentHalvesOfADuplicateBind()
    {
        const string prelude =
            "SELECT f.event_time AS ts\n"
            + "FROM collect.default_trace_events AS f\n"
            + "      LEFT JOIN (\n"
            + "        SELECT DISTINCT ON (server_name) server_name, utc_offset_minutes\n"
            + "        FROM collect.server_properties\n";

        const string coda =
            "      ) AS o ON o.server_name = f.server_name\n"
            + "WHERE f.event_time >= $1\n"
            + "  AND f.event_time <= $2\n"
            + "  AND f.server_name = ANY($3)\n";

        /* Spelling 1 — the second bind's placeholder is USED by the join that prompted it, while the outer
           predicate keeps citing the first. Verified against the compiler under this mutation: four
           parameters, and the SQL cites $1 through $4. */
        var placeholderUsed = prelude + "        AND   server_name = ANY($4)\n" + coda;

        /* Spelling 2 — the second bind's placeholder is DISCARDED and the join cites the original. */
        var placeholderDiscarded = prelude + "        AND   server_name = ANY($3)\n" + coda;

        var fourBound = Bound(4);

        /* The spelling that matters, and the finding: neither text-shaped check fires. */
        Assert.Null(MaxBasedViolation("placeholder used", placeholderUsed, fourBound));
        Assert.Null(CoverageViolation("placeholder used", placeholderUsed, fourBound));

        /* The other spelling: coverage fires, the max form still does not. */
        Assert.Null(MaxBasedViolation("placeholder discarded", placeholderDiscarded, fourBound));
        var discardedCoverage = CoverageViolation("placeholder discarded", placeholderDiscarded, fourBound);
        Assert.NotNull(discardedCoverage);
        Assert.Contains("$4 is bound but never cited", discardedCoverage, StringComparison.Ordinal);

        /* The count sees both, because it never reads the text at all. An annotation query on a
           server-scoped run binds the window and the array: three, not four. */
        Assert.Equal(3, PredictedAnnotationParameterCount(serverScoped: true));
        Assert.NotEqual(PredictedAnnotationParameterCount(serverScoped: true), fourBound.Length);

        /* And the control, which every one of the three must pass: the SHIPPED form, one array bound once
           and cited twice. A check that flagged this would make reusing an ordinal — the discipline the
           compiler is built on — look like the defect. */
        Assert.Null(MaxBasedViolation("shipped", placeholderDiscarded, Bound(3)));
        Assert.Null(CoverageViolation("shipped", placeholderDiscarded, Bound(3)));
        Assert.Equal(PredictedAnnotationParameterCount(serverScoped: true), Bound(3).Length);
    }

    /// <summary>
    /// <see cref="PredictedParameterCount"/> restates a rule that lives in another file, so it can be
    /// outgrown. This counts the <c>ParamList</c> call sites in <c>ComposeCompiler.cs</c> and pins the
    /// total: fifteen, which is the three window/scope binds and one <c>topN</c> per ranked arm in
    /// <c>Compile</c>, the seven <c>BuildFilterClause</c> operator arms, and the three window/scope binds in
    /// <c>CompileAnnotation</c>.
    ///
    /// <para>A sixteenth is the "next site someone adds" case, and it reds HERE — where the fix is to decide
    /// whether the prediction grows with it — rather than in the sweep, where it would read as a compiler
    /// bug. Comments and string literals are stripped first, because this file's reasoning names
    /// <c>p.AddTextArray</c> in prose.</para>
    /// </summary>
    [Fact]
    public void ThePredictionCoversEveryBindingSite_InTheCompiler()
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            "PerformanceMonitor.Darling.Service",
            "Compose",
            "ComposeCompiler.cs");
        Assert.True(File.Exists(path), $"ComposeCompiler.cs was not found at {path} — this pin needs re-anchoring.");

        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
        var sites = Regex.Matches(code, @"\bp\.Add[A-Za-z]+\s*\(").Count;

        Assert.Equal(15, sites);
    }

    /// <summary>
    /// The other direction, and the one the max form does see: an ordinal cited past the end of the bound
    /// set. Kept so the coverage check is pinned as a SUPERSET of the max check rather than an alternative
    /// to it — a coverage check that had lost its range arm would still catch the duplicate bind above and
    /// would silently stop catching this.
    /// </summary>
    [Fact]
    public void TheCoverageCheck_AlsoCatchesAnOrdinalPastTheEndOfTheBoundSet()
    {
        const string offByOne = "SELECT 1 WHERE a >= $1 AND b <= $2 AND c = ANY($3)\n";

        Assert.NotNull(MaxBasedViolation("off by one", offByOne, Bound(2)));

        var coverage = CoverageViolation("off by one", offByOne, Bound(2));
        Assert.NotNull(coverage);
        Assert.Contains("would fail to bind", coverage, StringComparison.Ordinal);
    }

    /// <summary>
    /// The ordinal reader reads ORDINALS, not digits. <c>$1</c> is a prefix of <c>$10</c>, so a
    /// substring-shaped scan would report <c>$1</c> as cited on any statement that reached ten parameters —
    /// and the statement whose <c>$1</c> really had gone uncited is exactly the one with enough binds to
    /// have a <c>$10</c>.
    /// </summary>
    [Fact]
    public void TheOrdinalReader_DoesNotReadTenAsOne()
    {
        var elevenBound = Bound(11);
        var citesTenNotOne = new System.Text.StringBuilder("SELECT 1 WHERE x = $10 AND y = $11");
        for (var n = 2; n <= 9; n++)
        {
            citesTenNotOne.Append(CultureInfo.InvariantCulture, $" AND c{n} = ${n}");
        }

        var coverage = CoverageViolation("cites $10 but not $1", citesTenNotOne.ToString(), elevenBound);
        Assert.NotNull(coverage);
        Assert.Contains("$1 is bound but never cited", coverage, StringComparison.Ordinal);
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>One compiled statement, the count its plan predicted, and the facets the reach assertions
    /// count over.</summary>
    private sealed record Statement(
        string Label,
        ComposeCompiled Compiled,
        int Predicted,
        PanelMode Mode,
        bool ServerScoped,
        string? Measure,
        string? Annotation,
        string? FilterOp,
        bool UsesVariable);

    /// <summary>
    /// Every measure in every mode its archetype and dimensions allow, fleet-wide and server-scoped; every
    /// filter operator on a measure that has a dimension to filter; a variable-resolved filter; and every
    /// annotation source both ways.
    ///
    /// <para>A shape a measure cannot legally take (a gauge has no summable column, an undimensioned measure
    /// cannot be ranked) is skipped rather than forced — but the reach assertions above are what make that
    /// safe, because a skip that swallowed a whole measure or a whole mode is reported as a missing member
    /// rather than as a smaller total.</para>
    /// </summary>
    private static List<Statement> Corpus()
    {
        var corpus = new List<Statement>();

        foreach (var measure in MeasureCatalog.Measures)
        {
            var keyword = measure.Kind == MeasureKind.Ratio ? "ratio" : "measure";
            var aggregate = measure.Kind == MeasureKind.Ratio
                ? null
                : MeasureCatalog.WireName(measure.ValidAggs[0]);
            var aggregateJson = aggregate is null ? "" : $",\"aggregate\":\"{aggregate}\"";
            var head = $"{{\"source\":\"{measure.SourceTable}\",\"{keyword}\":\"{measure.Key}\"{aggregateJson}";

            /* server is universal, so it is always available as a group-by even on an undimensioned
               server-grain measure. A declared dimension is preferred when there is one, because that is the
               path a filter's ColumnRef takes. */
            var dimension = measure.AllowedDimensions.Count > 0 ? measure.AllowedDimensions[0] : "server";

            foreach (var servers in new[] { (IReadOnlyList<string>?)null, TwoServers })
            {
                Add(corpus, $"{head},\"timeBucket\":\"hour\",\"viz\":\"line\"}}", servers, measure.Key);
                Add(corpus, $"{head},\"viz\":\"stat\"}}", servers, measure.Key);
                Add(corpus, $"{head},\"topN\":5,\"groupBy\":[\"{dimension}\"],\"viz\":\"bar\"}}", servers, measure.Key);
                Add(
                    corpus,
                    $"{head},\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"{dimension}\"],\"includeOther\":true,\"viz\":\"line\"}}",
                    servers,
                    measure.Key);
            }
        }

        /* Every filter operator, each on a dimension that legally accepts it — LIKE is gated to a Likeable
           dimension, and the ordered comparisons need a real column to compare. */
        foreach (var op in MeasureCatalog.FilterOpWireNames)
        {
            var likeOnly = string.Equals(op, "like", StringComparison.Ordinal);
            foreach (var measure in MeasureCatalog.Measures.Where(m => m.Kind == MeasureKind.Scalar && m.AllowedDimensions.Count > 0))
            {
                var dimension = MeasureCatalog.Dimensions.FirstOrDefault(d =>
                    string.Equals(d.SourceTable, measure.SourceTable, StringComparison.Ordinal)
                    && measure.AllowedDimensions.Contains(d.Name, StringComparer.Ordinal)
                    && (!likeOnly || d.Likeable));

                if (dimension is null)
                {
                    continue;
                }

                var aggregate = MeasureCatalog.WireName(measure.ValidAggs[0]);

                /* eq/neq take a list (multi-select); LIKE and the four ordered comparisons take a single
                   value and parse rejects a list. Spelled from the parser's own rule rather than uniformly,
                   because a uniform list would leave five of the seven operators silently uncompiled — which
                   is what the missing-operator assertion above reports rather than tolerates. */
                var singleValued = !(string.Equals(op, "eq", StringComparison.Ordinal)
                    || string.Equals(op, "neq", StringComparison.Ordinal));
                var value = singleValued ? "\"alpha%\"" : "[\"alpha\",\"beta\"]";
                var oneValue = singleValued ? "\"alpha%\"" : "[\"alpha\"]";

                var json =
                    $"{{\"source\":\"{measure.SourceTable}\",\"measure\":\"{measure.Key}\",\"aggregate\":\"{aggregate}\","
                    + "\"timeBucket\":\"hour\",\"viz\":\"line\","
                    + $"\"filters\":[{{\"dimension\":\"{dimension.Name}\",\"op\":\"{op}\",\"value\":{value}}}]}}";

                /* Two filters as well as one, because a second predicate is where an ordinal shift shows: a
                   duplicate bind in the first clause moves the second clause's placeholder. */
                var twoFilters =
                    $"{{\"source\":\"{measure.SourceTable}\",\"measure\":\"{measure.Key}\",\"aggregate\":\"{aggregate}\","
                    + "\"timeBucket\":\"hour\",\"viz\":\"line\","
                    + $"\"filters\":[{{\"dimension\":\"{dimension.Name}\",\"op\":\"{op}\",\"value\":{oneValue}}},"
                    + $"{{\"dimension\":\"server\",\"op\":\"eq\",\"value\":[\"SERVER-A\"]}}]}}";

                /* A filter AND a topN in one statement, which is where an ordinal shift between the two
                   would show: the filter binds first, so an extra bind in its arm moves the LIMIT's
                   placeholder and nothing about the filter itself looks wrong. */
                var filteredRanked =
                    $"{{\"source\":\"{measure.SourceTable}\",\"measure\":\"{measure.Key}\",\"aggregate\":\"{aggregate}\","
                    + $"\"topN\":5,\"groupBy\":[\"{dimension.Name}\"],\"viz\":\"bar\","
                    + $"\"filters\":[{{\"dimension\":\"{dimension.Name}\",\"op\":\"{op}\",\"value\":{oneValue}}},"
                    + $"{{\"dimension\":\"server\",\"op\":\"eq\",\"value\":[\"SERVER-A\"]}}]}}";

                var before = corpus.Count;
                Add(corpus, json, TwoServers, measure.Key, filterOp: op);
                Add(corpus, twoFilters, null, measure.Key, filterOp: op);
                Add(corpus, filteredRanked, TwoServers, measure.Key, filterOp: op);
                Add(corpus, filteredRanked, null, measure.Key, filterOp: op);

                if (corpus.Count > before)
                {
                    break;
                }
            }
        }

        /* A variable-resolved filter value: the same bind, reached through the $var path rather than a
           literal, so a future divergence between the two resolutions is in the population. */
        Add(
            corpus,
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\","
            + "\"viz\":\"line\",\"filters\":[{\"dimension\":\"wait_type\",\"op\":\"eq\",\"value\":\"$waits\"}]}",
            TwoServers,
            "wait_time_ms",
            filterOp: "eq",
            variables: new Dictionary<string, string?>(StringComparer.Ordinal) { ["waits"] = "PAGEIOLATCH_SH" },
            declaredVariables: ["waits"]);

        foreach (var source in MeasureCatalog.AnnotationSources)
        {
            foreach (var servers in new[] { (IReadOnlyList<string>?)null, TwoServers })
            {
                var json =
                    "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\","
                    + $"\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"{source.Key}\"]}}";

                var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, []);
                Assert.True(error is null, error);

                foreach (var (key, compiled) in ComposeCompiler.CompileAnnotations(plan!, Context(servers, null)))
                {
                    corpus.Add(new Statement(
                        $"annotation {key} ({(servers is null ? "fleet" : "scoped")})",
                        compiled,
                        PredictedAnnotationParameterCount(servers is not null),
                        PanelMode.TimeSeries,
                        servers is not null,
                        Measure: null,
                        Annotation: key,
                        FilterOp: null,
                        UsesVariable: false));
                }
            }
        }

        return corpus;
    }

    private static void Add(
        List<Statement> corpus,
        string json,
        IReadOnlyList<string>? servers,
        string measureKey,
        string? filterOp = null,
        IReadOnlyDictionary<string, string?>? variables = null,
        string[]? declaredVariables = null)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel(
            (JsonObject)JsonNode.Parse(json)!,
            declaredVariables ?? []);

        if (parseError is not null)
        {
            /* An illegal shape for this measure (a gauge cannot be summed, an undimensioned measure cannot
               be ranked). The reach assertions in the sweep are what stop this swallowing a whole member. */
            return;
        }

        var (compiled, compileError) = ComposeCompiler.Compile(plan!, Context(servers, variables));
        if (compileError is not null)
        {
            return;
        }

        corpus.Add(new Statement(
            $"{measureKey} {plan!.Mode} ({(servers is null ? "fleet" : "scoped")})"
            + (filterOp is null ? "" : $" filter:{filterOp}"),
            compiled!,
            PredictedParameterCount(plan, servers is not null),
            plan.Mode,
            servers is not null,
            measureKey,
            Annotation: null,
            filterOp,
            variables is not null));
    }

    private static ComposeRunContext Context(
        IReadOnlyList<string>? servers,
        IReadOnlyDictionary<string, string?>? variables) =>
        new(
            servers,
            WindowStart,
            WindowEnd,
            variables ?? ComposeRunContext.NoVariables,
            RollupAvailability.All,
            WindowEnd,
            RollupCoverage.Unknown);

    /// <summary>The repository root, from this file's own compile-time path — the same anchor
    /// <c>StartupCommandTimeoutTests</c> and <c>ServerLocalReadFrameDisciplineTests</c> use, so the source
    /// scan reads the checkout rather than whatever landed beside the test binary.</summary>
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        /* This file lives at <repo>/Darling/Darling.Tests/, so the repo root is two levels up. */
        return Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
    }

    /// <summary>A bound set of the given size. Only its COUNT is read by the checks under test, so the
    /// values are deliberately uniform — a distinguishing value here would suggest the property depends on
    /// what was bound, which it does not.</summary>
    private static NpgsqlParameter[] Bound(int count) =>
        [.. Enumerable.Range(1, count).Select(_ => new NpgsqlParameter { Value = "x" })];
}
