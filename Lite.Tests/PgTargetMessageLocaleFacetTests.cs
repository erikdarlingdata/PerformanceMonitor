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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3061: the <c>message_locale</c> facet on <c>pg_plan_capture_readiness</c>.
///
/// <para><b>What this is defending.</b> Four target-side readers match PostgreSQL's ENGLISH message text in
/// a customer-owned instance's server log. PostgreSQL translates its own messages under <c>lc_messages</c>,
/// the severity label included, so on a target running any other locale they go blind — and for
/// <c>pg_deadlocks</c> the blind state and the healthy state are the same empty result. The facet turns that
/// silent zero into a named unmet precondition. Making the parsers locale-aware was explicitly NOT the
/// scope; saying the precondition is unmet is the honest alternative to pretending to read.</para>
///
/// <para><b>Why the assertions look like this.</b> The issue's own standard: a pin that merely checks the
/// English tokens are still present cannot see this class of defect at all. So the locale predicate is
/// EXTRACTED FROM THE SHIPPED QUERY and evaluated against real locale names, rather than retyped here —
/// a retyped copy proves the transcription works, which is not the claim. PostgreSQL cannot be run on the
/// development host, so <see cref="ThePredicateUsesOnlySyntaxThatMeansTheSameThingInBothEngines"/> earns
/// the right to evaluate those patterns with .NET's engine by first pinning that they use only syntax
/// whose meaning is identical in PostgreSQL's ARE and in .NET. That is the honest limit of what is provable
/// here, and it is stated rather than implied.</para>
/// </summary>
public sealed class PgTargetMessageLocaleFacetTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string Sql()
        => PgPlanCaptureReadinessCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 9, 5, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 16,
            },
            ExcludedDatabases = Array.Empty<string>(),
        }).Text;

    /* Both halves of the locale test, lifted out of the SHIPPED query with the operator that carries their
       case sensitivity: `~` is case-sensitive and `~*` is not, and which one each pattern gets is
       load-bearing rather than incidental. Anchored on the coalesce over message_locale so this cannot
       accidentally pick up the shared_preload_libraries regex, which is the only other one in the file. */
    private static List<(string Pattern, bool CaseSensitive)> ExtractLocalePredicate(string sql)
        => Regex.Matches(sql, @"message_locale, ''\)\s*(~\*?)\s*'([^']*)'")
                .Select(m => (m.Groups[2].Value, m.Groups[1].Value == "~"))
                .ToList();

    private static bool IsSatisfied(string sql, string localeValue)
        => ExtractLocalePredicate(sql)
           .Any(p => Regex.IsMatch(
                         localeValue,
                         p.Pattern,
                         p.CaseSensitive ? RegexOptions.None : RegexOptions.IgnoreCase));

    /// <summary>
    /// The facet exists, is its own row, and judges <c>lc_messages</c> — read through the shared settings
    /// CTE in the two-argument MISSING_OK form like every other GUC this collector consumes.
    /// </summary>
    [Fact]
    public void TheMessageLocaleFacet_IsASeparateRow_AndReadsLcMessagesThroughTheSharedCte()
    {
        var sql = Sql();

        Assert.Contains("'message_locale'::text", sql, StringComparison.Ordinal);

        var reads = Regex.Matches(sql, @"current_setting\(\s*'lc_messages'\s*,\s*true\s*\)");
        Assert.True(
            reads.Count == 1,
            $"lc_messages is read {reads.Count} times in the MISSING_OK form. It must be read exactly once, "
            + "in the settings CTE, for the reason shared_preload_libraries is: one fact derived twice is "
            + "how two facets come to disagree about it.");

        Assert.True(
            sql.LastIndexOf("current_setting('lc_messages'", StringComparison.Ordinal)
                < sql.IndexOf("UNION ALL", StringComparison.Ordinal),
            "lc_messages is read inside a facet branch rather than the shared settings CTE.");
    }

    /// <summary>
    /// NOT gated on whether auto_explain is loaded, and the reason is stronger than
    /// <c>plan_attribution</c>'s: the deadlock read depends on this locale with no auto_explain in the
    /// picture at all, so a target that will never load the library still needs the answer.
    /// </summary>
    [Fact]
    public void TheFacet_IsNotGatedOnTheLibraryBeingLoaded()
    {
        var sql = Sql();

        var branchStart = sql.IndexOf("'message_locale'::text", StringComparison.Ordinal);
        Assert.True(branchStart >= 0, "the message_locale facet is missing");

        var satisfiedAt = sql.IndexOf("p.english_messages", branchStart, StringComparison.Ordinal);
        Assert.True(satisfiedAt > branchStart, "message_locale's is_satisfied column moved; rescope this test");

        var satisfiedExpression = sql[branchStart..(satisfiedAt + "p.english_messages".Length)];

        Assert.DoesNotContain("p.loaded", satisfiedExpression, StringComparison.Ordinal);
    }

    /// <summary>
    /// The locale test is derived ONCE and consulted by both the <c>is_satisfied</c> column and the detail
    /// arms. This is the #2605 shape — one fact derived in five places, where the fifth forgot and asserted
    /// the opposite of the other four — and a facet whose verdict and whose prose disagree is worse than no
    /// facet, because the prose is what a reader acts on.
    /// </summary>
    [Fact]
    public void TheLocaleTest_IsDerivedOnce_AndBothTheVerdictAndTheProseReadThatOneCopy()
    {
        var sql = Sql();

        var predicate = ExtractLocalePredicate(sql);
        Assert.Equal(2, predicate.Count);

        foreach (var (pattern, _) in predicate)
        {
            var occurrences = Regex.Matches(sql, Regex.Escape(pattern)).Count;
            Assert.True(
                occurrences == 1,
                $"the locale pattern {pattern} appears {occurrences} times. It must appear once, as a "
                + "derived column, or the verdict and the remedy text can drift apart.");
        }

        /* And the facet branch reaches it by name rather than recomputing it. */
        var branch = sql[sql.IndexOf("'message_locale'::text", StringComparison.Ordinal)..];
        Assert.Contains("p.english_messages", branch, StringComparison.Ordinal);
        Assert.Contains("p.locale_unset", branch, StringComparison.Ordinal);
        Assert.DoesNotContain("current_setting", branch, StringComparison.Ordinal);
    }

    /// <summary>
    /// The licence to evaluate PostgreSQL's regexes with .NET's engine, which is what
    /// <see cref="TheSatisfiedSet_IsOnlyLocalesPostgresLeavesUntranslated"/> does. No live PostgreSQL is
    /// reachable from the development host, so the patterns have to be run somewhere else — and that is only
    /// honest if they use nothing whose meaning differs between the two engines. This pins that: bare
    /// alternation, anchors, literal characters and a single escaped dot. No class shorthands
    /// (<c>\d</c>, <c>\w</c>, <c>\s</c>, <c>\b</c>), no POSIX bracket classes, no quantifiers, no
    /// lookaround, no back-references, no embedded-option prefixes.
    /// </summary>
    [Fact]
    public void ThePredicateUsesOnlySyntaxThatMeansTheSameThingInBothEngines()
    {
        foreach (var (pattern, _) in ExtractLocalePredicate(Sql()))
        {
            Assert.Matches(new Regex(@"^[A-Za-z0-9_^$()|.\\@-]+$"), pattern);

            foreach (var forbidden in new[] { "(?", "[[:", "\\d", "\\w", "\\s", "\\b", "{", "*", "+", "?" })
            {
                Assert.DoesNotContain(forbidden, pattern, StringComparison.Ordinal);
            }

            /* The only escape used is \. — anything else escaped is a divergence risk this has not checked. */
            foreach (Match escape in Regex.Matches(pattern, @"\\(.)"))
            {
                Assert.Equal(".", escape.Groups[1].Value);
            }
        }
    }

    /// <summary>
    /// The assertion the issue asked for, stated as the RELATION rather than as either side: the locales the
    /// facet calls satisfied are exactly the ones under which PostgreSQL leaves its message catalogue
    /// untranslated, so a target running anything else FAILS TO BE SATISFIED rather than silently missing.
    ///
    /// <para><b>The adversarial values are the point of this test</b>, and one of them was found by
    /// mutating the shipped predicate rather than by reasoning about it. The mistake that matters is
    /// DROPPING THE ANCHOR: <c>Czech_Czech Republic.1250</c> and <c>Chinese (Simplified)_China.936</c> are
    /// real Windows locale names that begin with a literal upper-case <c>C</c>, so an unanchored
    /// <c>^C</c> reports both as the C locale — a translated catalogue declared readable, which is the exact
    /// answer this facet exists to prevent. Neither is visible to a test built only from plausible values.
    /// </para>
    ///
    /// <para><b>What is NOT load-bearing, established by mutation and recorded so nobody re-adds it:</b>
    /// case sensitivity on the C family. <c>ca_ES</c> (Catalan) and <c>cs_CZ</c> (Czech) are kept below
    /// because they are the obvious near-misses, but the ANCHOR rejects them however case is treated — a
    /// case-insensitive C test was mutated in and every assertion here still passed. So the predicate
    /// matches case-insensitively on purpose: no language has the ISO code <c>c</c>, and
    /// <c>posix</c>/<c>POSIX</c> spelling variation is real. <c>posix</c> below is what fails if someone
    /// tightens it back.</para>
    /// </summary>
    [Theory]
    // The C family: no catalogue to load, so nothing to translate.
    [InlineData("C", true)]
    [InlineData("C.UTF-8", true)]
    [InlineData("C.utf8", true)]
    [InlineData("POSIX", true)]
    // Case-insensitively, on purpose - see the type comment. This is what a tightened C test breaks.
    [InlineData("posix", true)]
    [InlineData("POSIX.UTF-8", true)]
    // The catalogue's own source language: gettext hands back the msgid unchanged.
    [InlineData("en_US.UTF-8", true)]
    [InlineData("en_US.utf8", true)]
    [InlineData("en_GB.UTF-8", true)]
    [InlineData("en_AU", true)]
    [InlineData("en", true)]
    // The Windows form of the same.
    [InlineData("English_United States.1252", true)]
    // Translated catalogues, which is the whole exposure.
    [InlineData("de_DE.UTF-8", false)]
    [InlineData("de_DE", false)]
    [InlineData("fr_FR.UTF-8", false)]
    [InlineData("ja_JP.UTF-8", false)]
    [InlineData("ru_RU.UTF-8", false)]
    [InlineData("zh_CN.UTF-8", false)]
    [InlineData("pt_BR.UTF-8", false)]
    [InlineData("es_ES.UTF-8", false)]
    // ADVERSARIAL - the case-sensitivity mistake.
    [InlineData("ca_ES.UTF-8", false)]
    [InlineData("ca_ES@valencia", false)]
    [InlineData("cs_CZ.UTF-8", false)]
    // ADVERSARIAL - the anchoring mistake, in the Windows naming form.
    [InlineData("Czech_Czech Republic.1250", false)]
    [InlineData("Chinese (Simplified)_China.936", false)]
    public void TheSatisfiedSet_IsOnlyLocalesPostgresLeavesUntranslated(string locale, bool expected)
    {
        var actual = IsSatisfied(Sql(), locale);

        Assert.True(
            actual == expected,
            expected
                ? $"lc_messages = '{locale}' leaves PostgreSQL's messages in English, but the facet reports "
                  + "it UNSATISFIED - so a target that this product can read is being told it cannot be."
                : $"lc_messages = '{locale}' makes PostgreSQL TRANSLATE its own messages, severity label "
                  + "included, but the facet reports it SATISFIED - so every log read on this target finds "
                  + "nothing while the facet says the precondition is met. For pg_deadlocks that is "
                  + "indistinguishable from a server that did not deadlock, which is the #3030 failure "
                  + "shape this facet exists to prevent.");
    }

    /// <summary>
    /// An EMPTY <c>lc_messages</c> is a third state, and it resolves to UNSATISFIED. PostgreSQL then takes
    /// its message language from the server process's own environment, which no SQL read can see — so this
    /// is UNKNOWN, not English. Unsatisfied is the direction the reader already takes for a NULL
    /// (<c>DarlingPgPlanCaptureReadinessReader</c>: claiming readiness we cannot prove is the failure that
    /// matters), and the detail arm carries the difference so the row does not overclaim the other way
    /// either — the <c>extension_available</c> convention, where a negative is inconclusive and says so.
    /// </summary>
    [Fact]
    public void AnEmptyLocale_IsUnsatisfied_ButItsDetailSaysUnknownRatherThanTranslated()
    {
        var sql = Sql();

        Assert.False(
            IsSatisfied(sql, string.Empty),
            "an empty lc_messages reports SATISFIED. The server takes its message language from its own "
            + "environment there, which no query can see, so this is a precondition that cannot be proven "
            + "and must not read as met.");

        /* And the is_satisfied COLUMN is that predicate and nothing else. The check above evaluates the
           extracted patterns, so on its own it cannot see the facet's verdict being widened downstream -
           `p.english_messages OR p.locale_unset` would leave every pattern untouched and turn unknown into
           satisfied, which is the precise inversion this facet exists to prevent. */
        var branchStart = sql.IndexOf("'message_locale'::text", StringComparison.Ordinal);
        var satisfiedAt = sql.IndexOf("p.english_messages", branchStart, StringComparison.Ordinal);
        var afterSatisfied = satisfiedAt + "p.english_messages".Length;
        var verdictLine = sql[satisfiedAt..sql.IndexOf("CASE", afterSatisfied, StringComparison.Ordinal)];

        Assert.True(
            verdictLine.Trim() == "p.english_messages,",
            "the message_locale verdict is not the derived predicate alone - it reads "
            + $"'{verdictLine.Trim()}'. Anything ORed or ANDed on here changes what SATISFIED means without "
            + "touching the predicate the rest of this class checks.");

        /* Its own flag, so it gets its own arm rather than being folded into the translated case. */
        Assert.Contains("AS locale_unset", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN p.locale_unset", sql, StringComparison.Ordinal);

        /* Anchored on the arm's own opening text rather than on a newline: the query is a verbatim string,
           so its line endings are the source file's CRLF on every platform and Environment.NewLine would
           disagree with them on the Linux build. */
        var branch = sql[sql.IndexOf("'message_locale'::text", StringComparison.Ordinal)..];
        var unsetArm = branch[branch.IndexOf("THEN 'lc_messages is EMPTY", StringComparison.Ordinal)..];
        unsetArm = unsetArm[..unsetArm.IndexOf("WHEN p.english_messages", StringComparison.Ordinal)];

        /* The distinction a reader acts on: unknown, not wrong. */
        Assert.Contains("UNKNOWN", unsetArm, StringComparison.Ordinal);
        Assert.DoesNotContain("TRANSLATES", unsetArm, StringComparison.Ordinal);

        /* And it must still say the empty result means nothing, or "unknown" reads as "probably fine". */
        Assert.Contains("unknown meaning", unsetArm, StringComparison.Ordinal);
    }

    /// <summary>
    /// The unsatisfied remedy has to name the deadlock trap specifically, because that is the only one of
    /// the affected reads whose blind state is also its healthy state. A generic "the log may not be
    /// readable" sends the reader nowhere: what they need is that zero deadlocks means nothing here, and
    /// that <c>pg_stat_database</c>'s counter is a NUMBER and therefore locale-proof.
    /// </summary>
    [Fact]
    public void TheTranslatedRemedy_NamesTheDeadlockTrap_AndTheLocaleProofIndependentCheck()
    {
        var sql = Sql();
        var branch = sql[sql.IndexOf("'message_locale'::text", StringComparison.Ordinal)..];
        var translatedArm = branch[branch.IndexOf("ELSE 'PostgreSQL TRANSLATES", StringComparison.Ordinal)..];

        /* The concrete demonstration, not an abstraction about catalogues. */
        Assert.Contains("FEHLER:", translatedArm, StringComparison.Ordinal);
        Assert.Contains("ERROR:", translatedArm, StringComparison.Ordinal);

        /* The indistinguishability, which is the actual defect. */
        Assert.Contains("indistinguishable", translatedArm, StringComparison.Ordinal);

        /* The one instrument that survives a translated catalogue, because it is a counter. */
        Assert.Contains("pg_stat_database", translatedArm, StringComparison.Ordinal);

        /* auto_explain has no severity-independent anchor to retreat to, so the remedy must not imply one. */
        Assert.Contains("00000", translatedArm, StringComparison.Ordinal);

        /* The remedy itself, and that it is a reload rather than a reboot. */
        Assert.Contains("lc_messages = ''C''", translatedArm, StringComparison.Ordinal);
        Assert.Contains("reload", translatedArm, StringComparison.Ordinal);
    }

    /// <summary>
    /// The RELATION between the parsers and the facet, which is what makes this more than a spelling pin:
    /// every target-side reader that anchors on PostgreSQL's own message text is discovered FROM SOURCE, and
    /// each anchored severity label must be one of the untranslated upper-case tokens the satisfied locale
    /// set guarantees. A fifth reader anchored on a token outside that vocabulary fails here; so does
    /// widening the facet to a locale that would translate one.
    ///
    /// <para>Discovery is derived rather than listed, and the discovered set is asserted non-empty in its
    /// own right — a source walk that silently finds nothing would make this pass on an empty set, which is
    /// the failure mode that turns a guard into false confidence.</para>
    ///
    /// <para><b>Why raw text and not <c>CSharpSourceWalker</c>,</b> which #2913 made the shared authority
    /// and #3058 used for the store-side half of this: that walker strips comments, and here comments are
    /// part of what is being guarded. These readers document their anchors in prose beside the regex, so a
    /// comment still claiming <c>ERROR:</c> after the literal moved is documentation drift worth failing on.
    /// The walker answers "what does the compiler see"; this asks "does anything in this file rest on an
    /// English log label", and the second question wants the wider read. Being comment-inclusive only ever
    /// adds tokens to the set every one of which is then asserted, so it cannot mask a change — a literal
    /// mutated to <c>FEHLER:</c> is discovered and fails whether or not its comment moved with it.</para>
    /// </summary>
    [Fact]
    public void EveryTargetSideLogReader_AnchorsOnATokenTheSatisfiedLocaleSetGuarantees()
    {
        var sql = Sql();
        var collectorDir = Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors");

        /* PostgreSQL's untranslated severity vocabulary at the head of a log entry. Upper-case ASCII on
           purpose: under any locale the facet calls satisfied these are what error_severity() writes, and
           under one it does not they are precisely what it does NOT write. */
        var untranslated = new[] { "DEBUG", "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC", "DETAIL", "STATEMENT", "HINT", "CONTEXT" };

        var anchored = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(collectorDir, "Pg*.cs", SearchOption.TopDirectoryOnly))
        {
            var text = File.ReadAllText(file);

            /* A log-message anchor is an upper-case label followed by the DOUBLE SPACE PostgreSQL writes
               after it. That double space is what makes this specific enough not to collect the product's
               own status vocabulary, which is why it is not relaxed to a single one. Nothing is filtered
               here on purpose: everything found is asserted below, so a token outside the vocabulary fails
               rather than being quietly dropped by the discovery step. */
            var labels = Regex.Matches(text, @"([A-Z]{3,}):  ")
                              .Select(m => m.Groups[1].Value)
                              .ToList();

            if (labels.Count > 0)
            {
                anchored[Path.GetFileName(file)] = new SortedSet<string>(labels, StringComparer.Ordinal);
            }
        }

        /* #3061 enumerated four: PgDeadlockLogParser, PgDeadlocksCollector, PgPlanLogParser and
           PgPlanCaptureCollector. The walk rediscovers them rather than being handed the list, so a fifth
           reader is covered automatically - but a walk that came back short would make this pass on a set
           it never examined, which is the shape that turns a guard into false confidence. */
        Assert.True(
            anchored.Count >= 4,
            $"the source walk over {collectorDir} found {anchored.Count} target-side log reader(s), fewer "
            + "than the four #3061 enumerated. Either one was removed, or the discovery pattern stopped "
            + "matching and this test is now guarding nothing.");

        foreach (var (file, labels) in anchored)
        {
            foreach (var label in labels)
            {
                Assert.True(
                    untranslated.Contains(label, StringComparer.Ordinal),
                    $"{file} anchors on the log label '{label}', which is not one of the untranslated "
                    + "tokens PostgreSQL writes under the locales pg_plan_capture_readiness calls "
                    + "satisfied. Either the token is wrong or the facet's satisfied set no longer "
                    + "guarantees it.");
            }
        }

        /* And the facet those readers now rest on must actually be there. Deleting it silently returns
           every file above to the state #3061 was filed on. */
        Assert.True(
            sql.Contains("'message_locale'::text", StringComparison.Ordinal),
            "the message_locale facet is gone, so these target-side log readers have no precondition "
            + $"reporting their locale dependency again: {string.Join(", ", anchored.Keys)}");
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repository root not found above this test file.");
    }
}
