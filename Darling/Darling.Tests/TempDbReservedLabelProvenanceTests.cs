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
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para><b>The property.</b> Every label an operator reads for the tempdb-space percentage — the
/// snapshot member's own name, and every string the alert path renders it into — names the same quantity
/// the percentage's numerator actually measures.</para>
///
/// <para><b>Why a word-presence check would not do (#3144).</b> The shipped shape was
/// <c>TempDbSpaceInfo.UsedPercent</c> over a numerator summing three <c>*_reserved_page_count</c> columns,
/// rendered as "% used" in the alert body, the toast and the detail heading, while
/// <see cref="IAlertEngineSettings.TempDbSpaceThresholdPercent"/> — the one surface an operator never reads —
/// correctly said "reserved". A pin asserting that the word "reserved" is PRESENT cannot fail in the
/// direction that matters: it stays green when someone swaps the numerator and leaves the label alone, which
/// is the same disagreement pointing the other way. So the basis is DERIVED from the numerator, per run, and
/// the labels are judged against what was derived.</para>
///
/// <para><b>How the basis is derived, and why not from source.</b> A fixture gives every member of the
/// snapshot a value that makes its share of the denominator unique, the real
/// <see cref="AlertContextBuilders.BuildTempDbSpaceContext"/> renders it, and the percentage is read back
/// OUT of the rendered heading. The member whose share equals that percentage is the numerator, and the
/// member whose VALUE equals it is the percentage. Both fall out of a real render, so no member name is
/// written down anywhere in the derivation — rename either one and the derivation still finds it, which is
/// what lets the name itself be treated as a label and checked.</para>
///
/// <para><b>What the judgement is.</b> Two rules, and neither carries an exemption list.
/// <see cref="ForbiddenWords"/> is every rival basis noun the numerator does NOT measure, plus the
/// occupancy words ("used", "usage", …) that claim the space HOLDS DATA — a claim no member of this
/// snapshot measures, which is the whole finding. No in-scope label may contain one. And every file that
/// renders the percentage into a literal must name the basis in at least one of its in-scope labels, so
/// stripping the word rather than replacing it is caught too.</para>
///
/// <para><b>The scope is derived, not listed.</b> The files are every non-test file whose CODE references
/// the percentage member, read through <see cref="CSharpSourceWalker"/> so a mention in a comment is not
/// one; the labels are every literal in those files that either mentions tempdb or carries a hole rendering
/// the percentage. A new consumer assembly, or a sixth label added to an existing one, is therefore in scope
/// without this pin being edited. It is deliberately the BROAD reading: a field name like "Max Size" would
/// be pulled in if it ever named tempdb, and failing a green build on correct prose costs a one-line
/// decision, where a scan narrowed to today's five literals would pass vacuously on a sixth.</para>
///
/// <para><b>Two bounds worth knowing.</b> A label assembled in a DIFFERENT file from the render is not seen
/// — the two-literal split at the resolution branch works only because both halves sit in the same file.
/// And a mislabel on some OTHER tempdb number (a field naming reserved MB as "used") is outside the
/// property: this pin is about the percentage.</para>
/// </summary>
public class TempDbReservedLabelProvenanceTests
{
    /// <summary>
    /// The basis nouns an operator could reasonably read as the answer to "a percentage of WHAT". Every one
    /// of them is forbidden in a label except the ones the derived numerator requires, which is what makes
    /// the check symmetric: it reds on "reserved" just as readily when the numerator is the allocation.
    ///
    /// <para>Matched on WORD boundaries, which is load-bearing twice over: "allocated" must not match
    /// inside "unallocated" (they are different members with opposite meanings), and "reserved" must not
    /// match inside "Resolved", which is a word the resolution title actually uses.</para>
    /// </summary>
    private static readonly string[] RivalBasisWords =
    [
        "reserved", "allocated", "unallocated", "free", "capacity", "user", "internal", "version",
        "consumer", "max",
    ];

    /// <summary>
    /// Words that claim the space HOLDS DATA. Forbidden regardless of which member is the numerator, unless
    /// that member's own required vocabulary contains the word — because no member of this snapshot measures
    /// occupancy, and none cheaply can:
    /// <c>dm_db_file_space_usage</c> exposes the reserved page counts, <c>unallocated_extent_page_count</c>
    /// and the extent totals, and no used-pages column. Should a genuine used numerator ever arrive, its
    /// entry in <see cref="RequiredWords"/> carries "used" and these stop being forbidden for it — which is
    /// the case where label and numerator AGREE and this pin must not fire.
    /// </summary>
    private static readonly string[] OccupancyWords =
    [
        "used", "use", "uses", "using", "usage", "consumed", "full", "occupied", "utilization",
        "utilisation",
    ];

    /// <summary>
    /// The words a label must carry to name each member, keyed by member name.
    ///
    /// <para>Asserted COMPLETE against the snapshot's reflected members by
    /// <see cref="TheVocabularyNamesEveryMemberTheSnapshotExposes"/>, so a new member arrives as a failure
    /// saying this table cannot judge it, rather than as a member the derivation silently cannot name. That
    /// is the difference between a pin that reds when it stops being able to answer and one that reports
    /// clean because it read nothing.</para>
    /// </summary>
    private static readonly Dictionary<string, string[]> RequiredWords = new(StringComparer.Ordinal)
    {
        ["TotalReservedMb"] = ["reserved"],
        ["AllocatedMb"] = ["allocated"],
        ["UnallocatedMb"] = ["unallocated"],
        ["UserObjectReservedMb"] = ["user", "object"],
        ["InternalObjectReservedMb"] = ["internal", "object"],
        ["VersionStoreReservedMb"] = ["version", "store"],
        ["TopConsumerMb"] = ["top", "consumer"],
        ["MaxSizeMb"] = ["max", "size"],
        ["CapacityMb"] = ["capacity"],
        ["ReservedPercent"] = ["reserved"],
    };

    /// <summary>
    /// The fixture the basis is derived from. Every member's share of <see cref="TempDbSpaceInfo.CapacityMb"/>
    /// is a different number, and the percentage's own value collides with no member's value, so the two
    /// look-ups below have exactly one answer each — asserted rather than assumed, because a fixture that
    /// stopped discriminating would let the derivation pick a member at random and the pin would then be
    /// judging labels against a basis it invented.
    ///
    /// <para>The three component members sum to the total, as the collector's own query makes them: a
    /// fixture where they did not would be describing a snapshot the product cannot produce.</para>
    ///
    /// <para><b>Every value is a multiple of 16 against the 1,600 MB ceiling</b>, so every share is a whole
    /// number and survives the alert's own <c>:F0</c> unchanged. Found by mutation: with a share of 13.75
    /// the render says 14%, no member's exact share is 14, and the derivation reported that it could not
    /// attribute the numerator — a red, but one blaming the fixture for what was a numerator swap. Reading
    /// the rendered integer and rounding the candidates to match is the other half of that fix.</para>
    /// </summary>
    private static TempDbSpaceInfo Fixture() => new()
    {
        TotalReservedMb = 640,
        UnallocatedMb = 160,
        UserObjectReservedMb = 224,
        InternalObjectReservedMb = 288,
        VersionStoreReservedMb = 128,
        TopConsumerSessionId = 57,
        TopConsumerMb = 176,
        MaxSizeMb = 1600,
    };

    private static readonly Regex RenderedPercent = new(@"(\d+(?:\.\d+)?)\s*%", RegexOptions.Compiled);

    /// <summary>
    /// Whether <paramref name="text"/> names <paramref name="word"/> as a whole word, plural included.
    ///
    /// <para><b>Word boundaries</b> are what keep "allocated" out of "unallocated" — two members with
    /// opposite meanings — and "reserved" out of "Resolved", which the resolution title really uses. Both
    /// near-misses are pinned in <see cref="TheJudgementRedsInBothDirections"/>.</para>
    ///
    /// <para><b>The optional plural</b> is here because labels are prose and prose pluralises: "user
    /// objects" names <c>UserObjectReservedMb</c>, and a strict word boundary does not see it. Found by a
    /// theory row rather than by reading, which is the argument for having the rows.</para>
    /// </summary>
    private static bool Mentions(string text, string word) =>
        Regex.IsMatch(text, @"\b" + Regex.Escape(word) + @"s?\b", RegexOptions.IgnoreCase);

    /// <summary>Every rival and occupancy word the numerator does not require.</summary>
    private static IReadOnlyList<string> ForbiddenWords(string numerator)
    {
        var required = RequiredWords.TryGetValue(numerator, out var r) ? r : [];

        return RivalBasisWords.Concat(OccupancyWords)
            .Where(w => !required.Contains(w, StringComparer.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    /// <summary>
    /// The forbidden words a label contains — the whole judgement, as a pure function, so
    /// <see cref="TheJudgementRedsInBothDirections"/> can drive it over pairs the tree does not contain.
    /// </summary>
    private static IReadOnlyList<string> OffendingWords(string numerator, string label) =>
        ForbiddenWords(numerator).Where(w => Mentions(label, w)).ToList();

    /// <summary>Whether a label names the basis the numerator measures.</summary>
    private static bool NamesTheBasis(string numerator, string label) =>
        RequiredWords.TryGetValue(numerator, out var required)
        && required.All(w => Mentions(label, w));

    /// <summary>
    /// Whether a label says anything an operator reads as WORDS, rather than being a bare number.
    ///
    /// <para>This is what scopes the per-label basis rule. <c>$"{x.Percent:F0}%"</c> is a fragment that
    /// gets composed into a sentence elsewhere and cannot sensibly be made to name a basis on its own;
    /// <c>$"tempdb {x.Percent:F0}%"</c> is a toast body that must. Interpolation holes are already blanked
    /// out of the text by the walk, so what is left is the literal's own prose — but the FORMAT SPECIFIER
    /// survives, so a two-letter-or-longer specifier would read as prose and pull a bare fragment into the
    /// rule. That direction demands a basis word on a label that may not need one, which fails a green
    /// build rather than passing a mislabel.</para>
    /// </summary>
    private static bool CarriesProse(string label) => Regex.IsMatch(label, "[A-Za-z]{2,}");

    /* ───────────────── deriving the basis from a real render ───────────────── */

    /// <summary>One derivation: the numerator member, the percentage member, and what was rendered.</summary>
    private sealed record Provenance(string Numerator, string Percentage, double Percent, string Heading);

    /// <summary>
    /// Reads the numerator and the percentage member back out of a real
    /// <see cref="AlertContextBuilders.BuildTempDbSpaceContext"/> render.
    ///
    /// <para>The look-ups are asserted UNIQUE. A fixture that let two members share a share of the
    /// denominator would hand the label rules an arbitrary basis, and every assertion downstream would then
    /// be judging against a number this pin made up — a pass for the wrong reason, which is worth less than
    /// a failure.</para>
    /// </summary>
    private static Provenance Derive()
    {
        var info = Fixture();
        var context = AlertContextBuilders.BuildTempDbSpaceContext(info);
        Assert.NotNull(context);
        var item = Assert.Single(context!.Details);

        var match = RenderedPercent.Match(item.Heading ?? string.Empty);
        Assert.True(
            match.Success,
            "The tempdb detail heading rendered no percentage at all, so there is no rendered artifact to "
            + $"derive a basis from and every label rule below would pass vacuously. Heading was: '{item.Heading}'.");

        var percent = double.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);

        /* Fields label their own values. One of them carrying the percentage would put it outside the
           heading the rules read, so that is a failure rather than something to widen the scan for. */
        foreach (var (name, value) in item.Fields ?? [])
        {
            Assert.False(
                RenderedPercent.IsMatch(value ?? string.Empty)
                && Math.Abs(ParsedPercentOr(value, double.NaN) - percent) < 0.01,
                $"Alert detail field '{name}' renders the tempdb percentage as well as the heading. The "
                + "label rules read the heading, so a percentage that has moved into a field is a label "
                + "this pin no longer judges.");
        }

        var members = ReadableDoubles();

        var numerators = members
            .Where(m => Math.Abs(Share(info, m) - percent) < 0.005)
            .Select(m => m.Name)
            .ToList();

        Assert.True(
            numerators.Count == 1,
            $"The rendered {percent}% is the share of {numerators.Count} snapshot members "
            + $"({string.Join(", ", numerators)}), so the numerator cannot be attributed. The fixture's "
            + "whole job is to make one member's share unique; fix the fixture before trusting anything "
            + "this class asserts.");

        var percentages = members
            .Where(m => Math.Abs(Value(info, m) - percent) < 0.005)
            .Select(m => m.Name)
            .ToList();

        Assert.True(
            percentages.Count == 1,
            $"{percentages.Count} snapshot members have the value {percent} "
            + $"({string.Join(", ", percentages)}), so the percentage member cannot be attributed by value. "
            + "The fixture must keep it distinct from every raw MB figure.");

        return new(numerators[0], percentages[0], percent, item.Heading!);
    }

    private static double ParsedPercentOr(string? text, double fallback)
    {
        var m = RenderedPercent.Match(text ?? string.Empty);

        return m.Success ? double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture) : fallback;
    }

    private static PropertyInfo[] ReadableDoubles() =>
        typeof(TempDbSpaceInfo)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(double) && p.CanRead)
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToArray();

    private static double Value(TempDbSpaceInfo info, PropertyInfo p) => (double)p.GetValue(info)!;

    /// <summary>
    /// A member's share of the denominator, rounded the way the alert rounds it.
    ///
    /// <para>The percentage is read back out of a <c>:F0</c> render, so a candidate has to be compared at
    /// the same precision or a member whose share is 13.75 can never match the 14% an operator was shown.
    /// Two members rounding onto the same integer would tie, which the uniqueness assertion in
    /// <see cref="Derive"/> reports rather than resolves.</para>
    /// </summary>
    private static double Share(TempDbSpaceInfo info, PropertyInfo p) =>
        info.CapacityMb > 0
            ? Math.Round(Value(info, p) / info.CapacityMb * 100, MidpointRounding.AwayFromZero)
            : double.NaN;

    /* ───────────────── the label population, derived from the tree ───────────────── */

    /// <summary>One label, with where it was read from.</summary>
    private sealed record Label(string File, int Line, string Text, bool RendersThePercentage);

    private static bool HasBuildOutputSegment(string path) =>
        path.Split('/', '\\')
            .Any(s => string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
                      || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>A test project's own sources, which carry expected-value strings rather than labels.</summary>
    private static bool IsTestSource(string relative) =>
        relative.Split('/', '\\').Any(s => s.EndsWith(".Tests", StringComparison.OrdinalIgnoreCase));

    private static IEnumerable<string> NonTestSources()
    {
        var root = RepoFile.Root;

        foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(root, file);

            if (HasBuildOutputSegment(relative) || IsTestSource(relative))
            {
                continue;
            }

            yield return file;
        }
    }

    /// <summary>
    /// Every non-test file whose CODE references <paramref name="member"/>, with that file's text. Read
    /// through <see cref="CSharpSourceWalker"/>, so the member named in a doc comment (as
    /// <c>IAlertReadAdapter</c> names it) is not a consumer and does not have to carry a label.
    /// </summary>
    private static List<(string Relative, string Text, int CodeReferences)> FilesReferencing(string member)
    {
        var name = new Regex(@"\b" + Regex.Escape(member) + @"\b", RegexOptions.Compiled);
        var found = new List<(string, string, int)>();

        foreach (var file in NonTestSources())
        {
            var text = File.ReadAllText(file);

            if (!text.Contains(member, StringComparison.Ordinal))
            {
                continue;
            }

            var references = name.Matches(CSharpSourceWalker.StripCommentsAndStrings(text)).Count;

            if (references > 0)
            {
                found.Add((Path.GetRelativePath(RepoFile.Root, file), text, references));
            }
        }

        return found.OrderBy(f => f.Item1, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// Every literal in <paramref name="text"/> that is in scope: it mentions tempdb, or it carries an
    /// interpolation hole rendering <paramref name="member"/>.
    ///
    /// <para>The hole test reads the CODE stream over the literal's own span, which is what
    /// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/> preserves holes for. Blanking a hole with
    /// the rest of the literal — which every copy of the walk did before #2913 — would make
    /// <c>$"tempdb {x.Percent:F0}% used"</c> a literal with no percentage in it, and the whole scan would
    /// then read as clean.</para>
    /// </summary>
    private static List<Label> LabelsIn(string relative, string text, string member)
    {
        var name = new Regex(@"\b" + Regex.Escape(member) + @"\b", RegexOptions.Compiled);
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        var bodies = CSharpSourceWalker.StringLiteralBodies(text).ToList();
        var labels = new List<Label>();

        foreach (var (start, body) in bodies)
        {
            if (body.Trim().Length == 0)
            {
                continue;
            }

            /* The literal's span ends where its own text does; the holes inside it are code at the same
               offsets, so the code stream over that span is exactly this literal's holes. */
            var span = code.Substring(start, body.Length);
            var renders = name.IsMatch(span);
            var mentionsTempDb = Mentions(body, "tempdb");

            if (!renders && !mentionsTempDb)
            {
                continue;
            }

            labels.Add(new(relative, CSharpMemberMap.LineOf(text, start), body, renders));
        }

        return labels;
    }

    /// <summary>
    /// An identifier as the words it is made of, so the word-boundary matching the labels get applies to a
    /// member NAME too. <c>ReservedPercent</c> holds no <c>\breserved\b</c> until the camel hump is a space.
    /// </summary>
    private static string Spaced(string identifier) =>
        Regex.Replace(identifier, "(?<=[a-z0-9])(?=[A-Z])", " ");

    /* ───────────────── the rules ───────────────── */

    /// <summary>
    /// <see cref="RequiredWords"/> can name every member the snapshot exposes, so the derivation cannot
    /// land on one this class has no vocabulary for.
    ///
    /// <para>The failure direction is why this is asserted rather than trusted. An unnamed member reached
    /// through <see cref="ForbiddenWords"/>'s empty-required fallback would forbid every rival word
    /// including the right one, and reach <see cref="NamesTheBasis"/> as an unconditional false — so the
    /// main pin would red with a message about labels while the actual fault was a table nobody
    /// extended.</para>
    /// </summary>
    [Fact]
    public void TheVocabularyNamesEveryMemberTheSnapshotExposes()
    {
        var missing = ReadableDoubles()
            .Select(p => p.Name)
            .Where(n => !RequiredWords.ContainsKey(n))
            .ToList();

        Assert.True(
            missing.Count == 0,
            $"{nameof(TempDbSpaceInfo)} exposes member(s) this pin has no basis vocabulary for: "
            + $"{string.Join(", ", missing)}. Add each to {nameof(RequiredWords)} with the words a label "
            + "must carry to name it. Until then the derivation can attribute the numerator to a member "
            + "whose labels cannot be judged, and this class reports on labels when the fault is here.");
    }

    /// <summary>
    /// The numerator is the same member whichever of #2515's three ceiling states the snapshot is in — a
    /// real cap, unlimited growth, and a snapshot taken before the column existed.
    ///
    /// <para>Those states change the DENOMINATOR, so they change every share. If the derivation only found
    /// the numerator under one of them it would be reading a coincidence of the fixture rather than the
    /// arithmetic, and the label rules would be judged against a basis that moved with the test data.</para>
    ///
    /// <para><b>Attribution is up to VALUE, not up to name, and it has to be.</b>
    /// <see cref="TempDbSpaceInfo.CapacityMb"/> is by construction equal to one of the other members —
    /// <see cref="TempDbSpaceInfo.MaxSizeMb"/> where the ceiling is real, <see cref="TempDbSpaceInfo.AllocatedMb"/>
    /// where it is not — so under some state two members always hold the same number and no arithmetic can
    /// tell them apart. Demanding a single NAME would therefore red on a legitimate change of numerator
    /// rather than on a mislabel. What is asserted instead is that every member the share attributes to
    /// holds the derived numerator's value: the ambiguity may be two names for one number, never two
    /// different quantities coinciding.</para>
    /// </summary>
    [Theory]
    [InlineData(1600d)]
    [InlineData(-1d)]
    [InlineData(0d)]
    public void TheNumeratorIsAttributedUnderEveryCeilingState(double maxSizeMb)
    {
        var provenance = Derive();
        var members = ReadableDoubles();
        var info = Fixture();
        info.MaxSizeMb = maxSizeMb;

        /* Read the percentage through the DERIVED member, so this pin holds no member name of its own -
           a name written down here would turn a rename into a compile error in place of a verdict. */
        var percent = Value(info, members.Single(m => m.Name == provenance.Percentage));
        var derived = Value(info, members.Single(m => m.Name == provenance.Numerator));

        var attributed = members
            .Where(m => Math.Abs(Share(info, m) - percent) < 0.005)
            .ToList();

        Assert.True(
            attributed.Count > 0
            && attributed.Any(m => m.Name == provenance.Numerator)
            && attributed.All(m => Math.Abs(Value(info, m) - derived) < 0.005),
            $"With MaxSizeMb = {maxSizeMb} the percentage attributes to "
            + $"[{string.Join(", ", attributed.Select(m => $"{m.Name}={Value(info, m)}"))}] rather than to "
            + $"'{provenance.Numerator}'={derived}, which the rendered alert attributes it to. The "
            + "derivation has to reach the same quantity under all three ceiling states, or the basis this "
            + "pin judges labels against is an artifact of one of them.");
    }

    /// <summary>
    /// <b>The pin.</b> Every label in the alert path names the basis the numerator measures, and none of
    /// them names a rival basis or claims occupancy.
    ///
    /// <para>Reds when the property is renamed to something that claims occupancy, when a label is edited
    /// back to "used", when a sixth label arrives carrying the wrong word, and — the case a word-presence
    /// check cannot see — when the numerator is swapped and every label is left alone, because the basis is
    /// re-derived from the numerator on every run.</para>
    /// </summary>
    [Fact]
    public void EveryLabelInTheAlertPathNamesTheBasisTheNumeratorMeasures()
    {
        var provenance = Derive();
        var forbidden = ForbiddenWords(provenance.Numerator);

        /* The one true render this pin performs. The source scan below covers the engine's strings and the
           deprecated app's; this covers the detail heading through the real builder. */
        var offendingHeading = OffendingWords(provenance.Numerator, provenance.Heading);

        Assert.True(
            offendingHeading.Count == 0,
            $"The RENDERED tempdb alert heading '{provenance.Heading}' claims "
            + $"[{string.Join(", ", offendingHeading)}] while the numerator is "
            + $"'{provenance.Numerator}'. Either the heading names the wrong basis, or the numerator moved "
            + "and the heading did not.");

        Assert.True(
            NamesTheBasis(provenance.Numerator, provenance.Heading),
            $"The RENDERED tempdb alert heading '{provenance.Heading}' does not name the basis its "
            + $"numerator '{provenance.Numerator}' measures "
            + $"({string.Join(" + ", RequiredWords[provenance.Numerator])}). An operator reading it cannot "
            + "tell what the percentage is a percentage of.");

        var files = FilesReferencing(provenance.Percentage);
        var offenders = new List<string>();
        var unnamed = new List<string>();

        foreach (var (relative, text, _) in files)
        {
            var labels = LabelsIn(relative, text, provenance.Percentage);

            foreach (var label in labels)
            {
                var words = OffendingWords(provenance.Numerator, label.Text);

                if (words.Count > 0)
                {
                    offenders.Add($"{relative}:{label.Line} claims [{string.Join(", ", words)}] in "
                        + $"\"{label.Text.Trim()}\"");
                }

                /* Stripping the basis word rather than replacing it has to fail too, and it has to fail
                   PER LABEL. Asking only that the file name the basis somewhere is too coarse: the
                   engine's resolution body says "reserved" whatever the fire strings say, so both of the
                   strings an operator is actually paged with could lose the word and the file would still
                   read clean. Found by mutation, not by reading these assertions. */
                if (label.RendersThePercentage
                    && CarriesProse(label.Text)
                    && !NamesTheBasis(provenance.Numerator, label.Text))
                {
                    unnamed.Add($"{relative}:{label.Line} \"{label.Text.Trim()}\"");
                }
            }

            /* And the file-level form, for the case every rendering label IS a bare fragment: the basis
               then has to be named by one of the file's other labels, because it is named nowhere else. */
            if (labels.Any(l => l.RendersThePercentage)
                && !labels.Any(l => NamesTheBasis(provenance.Numerator, l.Text)))
            {
                unnamed.Add($"{relative} (no label in the file names it)");
            }
        }

        Assert.True(
            offenders.Count == 0,
            $"The tempdb-space percentage is computed from '{provenance.Numerator}', so a label may not "
            + $"claim [{string.Join(", ", forbidden)}]. Offending label(s):\n  "
            + string.Join("\n  ", offenders)
            + "\n\nIf the NUMERATOR is what changed, move every label with it — the mismatch this reports "
            + "is symmetric and does not say which side is wrong.");

        Assert.True(
            unnamed.Count == 0,
            $"Label(s) render the tempdb percentage without naming its basis "
            + $"({string.Join(" + ", RequiredWords[provenance.Numerator])}):\n  "
            + string.Join("\n  ", unnamed)
            + "\n\nA percentage with prose around it and no basis word tells an operator nothing about "
            + "what it is a percentage of.");
    }

    /// <summary>
    /// The scan reached real files and real labels. Without this, a glob that stopped matching, a walker
    /// that blanked interpolation holes, or a member rename the derivation mis-attributed would all leave
    /// the pin above reporting a clean tree having read nothing — the one failure mode a guard cannot
    /// afford, because it is indistinguishable from correctness.
    ///
    /// <para>The file set is cross-checked against a RAW text search, which fails differently: raw counts
    /// prose the walker correctly drops, so the walker's set must be a subset and every file only raw can
    /// see must genuinely have no code reference. Two instruments, one assertion.</para>
    /// </summary>
    [Fact]
    public void TheLabelScanIsNotVacuous()
    {
        var provenance = Derive();
        var files = FilesReferencing(provenance.Percentage);

        Assert.True(
            files.Count >= 2,
            $"Only {files.Count} non-test file(s) reference '{provenance.Percentage}' in code. The alert "
            + "engine and the context builder are both consumers, so a set this small means the scan is "
            + "not reading the tree and every label rule passes vacuously.");

        var withLabels = files
            .Where(f => LabelsIn(f.Relative, f.Text, provenance.Percentage).Count > 0)
            .Select(f => f.Relative)
            .ToList();

        Assert.True(
            withLabels.Count >= 2,
            $"Only {withLabels.Count} of {files.Count} referencing file(s) yielded a single in-scope label. "
            + "The labels are read out of interpolation holes and tempdb mentions; none found means the "
            + "literal walk is not seeing them and the pin is asserting nothing.");

        var rendering = files
            .Where(f => LabelsIn(f.Relative, f.Text, provenance.Percentage).Any(l => l.RendersThePercentage))
            .Select(f => f.Relative)
            .ToList();

        Assert.True(
            rendering.Count >= 2,
            $"Only {rendering.Count} file(s) were found rendering '{provenance.Percentage}' INSIDE a string "
            + "literal. If holes are being blanked with their literal, an interpolated label is invisible "
            + "to this scan and \"% used\" could sit in the tree unseen.");

        /* Second instrument: raw text, which cannot tell code from prose. */
        var raw = new List<string>();

        foreach (var file in NonTestSources())
        {
            if (Regex.IsMatch(File.ReadAllText(file), @"\b" + Regex.Escape(provenance.Percentage) + @"\b"))
            {
                raw.Add(Path.GetRelativePath(RepoFile.Root, file));
            }
        }

        var onlyWalker = files.Select(f => f.Relative).Except(raw, StringComparer.Ordinal).ToList();

        Assert.True(
            onlyWalker.Count == 0,
            $"The walker attributed a code reference to file(s) a raw text search cannot even find the "
            + $"member in: {string.Join(", ", onlyWalker)}. The two instruments disagree in the impossible "
            + "direction, so one of them is not reading what it claims to.");

        foreach (var relative in raw.Except(files.Select(f => f.Relative), StringComparer.Ordinal))
        {
            var text = File.ReadAllText(Path.Combine(RepoFile.Root, relative));
            var stripped = CSharpSourceWalker.StripCommentsAndStrings(text);

            Assert.False(
                Regex.IsMatch(stripped, @"\b" + Regex.Escape(provenance.Percentage) + @"\b"),
                $"{relative} was dropped from the consumer set as prose-only, but the code stream still "
                + "holds the member. The walk disagrees with itself.");
        }
    }

    /// <summary>
    /// The snapshot member's own NAME is a label, and it is held to the same rule as the rendered strings.
    ///
    /// <para>This is the assertion that reds BY NAME on a rename back to <c>UsedPercent</c>. The rename
    /// would also break this project's compile, but a compile error names a file and a line rather than the
    /// reason, and the reason is the whole point: <c>Used Percent</c> claims occupancy over a numerator that
    /// measures none.</para>
    /// </summary>
    [Fact]
    public void TheSnapshotMemberNameIsItselfALabel()
    {
        var provenance = Derive();
        var spelled = Spaced(provenance.Percentage);
        var offending = OffendingWords(provenance.Numerator, spelled);

        Assert.True(
            offending.Count == 0,
            $"'{provenance.Percentage}' claims [{string.Join(", ", offending)}] as its basis while being "
            + $"computed from '{provenance.Numerator}'. The member name is the label every consumer reads "
            + "first; two names for one number is worse than one wrong name, so rename it rather than "
            + "leaving the strings to disagree with it.");

        Assert.True(
            NamesTheBasis(provenance.Numerator, spelled),
            $"'{provenance.Percentage}' does not name the basis '{provenance.Numerator}' measures "
            + $"({string.Join(" + ", RequiredWords[provenance.Numerator])}), so a caller cannot tell from "
            + "the member what the percentage is a percentage of.");
    }

    /// <summary>
    /// The provenance the whole class rests on: the numerator column is the SUM of exactly the three
    /// reserved page counts, read off the collector's SHIPPED query rather than a copy of it.
    ///
    /// <para>Reserved is the right basis for an exhaustion alarm and that is why the name moved rather than
    /// the arithmetic — a reserved page is committed to an allocation unit and cannot be handed to anything
    /// else, and a spill reserves before it fills, so the number rises while there is still time to act.
    /// This pin is what makes that claim checkable: swap in <c>unallocated_extent_page_count</c>, or narrow
    /// the sum to user objects, and the derived basis stops matching what every label says.</para>
    /// </summary>
    [Fact]
    public void TheCollectorsNumeratorIsTheThreeReservedPageCounts()
    {
        var query = TempDbStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo(),
        }).Text;

        var start = query.IndexOf("total_reserved_mb", StringComparison.Ordinal);

        Assert.True(
            start >= 0,
            "The tempdb_stats query no longer projects total_reserved_mb, so the column the alert's "
            + "numerator is read from has been renamed or removed and this pin can no longer establish its "
            + "provenance.");

        var end = query.IndexOf('\n', start);
        var expression = end < 0 ? query[start..] : query[start..end];

        var counts = Regex.Matches(expression, @"\b\w+_page_count\b")
            .Select(m => m.Value)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(
            new[]
            {
                "internal_object_reserved_page_count",
                "user_object_reserved_page_count",
                "version_store_reserved_page_count",
            },
            counts);
    }

    /* ───────────────── the judgement's own falsifiers ───────────────── */

    /// <summary>
    /// The judgement reds in BOTH directions and on either side moving alone, and passes when the numerator
    /// and the label move together.
    ///
    /// <para>Driven over pairs the tree does not contain, because the tree only ever holds the state that
    /// passes. A rule exercised solely against correct source is an assertion that happens to hold: these
    /// rows are what distinguish it from a check that cannot fail. The two word-boundary rows are here for
    /// specific near-misses — <c>allocated</c> inside <c>unallocated</c> reverses a member's meaning, and
    /// <c>reserved</c> inside <c>Resolved</c> is a word the resolution title really uses.</para>
    /// </summary>
    [Theory]
    /* Reserved numerator: the shipped state, and the three ways to break it. */
    [InlineData("TotalReservedMb", "tempdb 40% reserved", true)]
    [InlineData("TotalReservedMb", "tempdb 40% used", false)]
    [InlineData("TotalReservedMb", "SRV-A: tempdb usage back to 40%", false)]
    [InlineData("TotalReservedMb", "tempdb 40% allocated", false)]
    /* The numerator moved and the label did not — the direction a word-presence check cannot see. */
    [InlineData("AllocatedMb", "tempdb 40% reserved", false)]
    /* Both moved together, so the surfaces agree and nothing is wrong. */
    [InlineData("AllocatedMb", "tempdb 40% allocated", true)]
    /* Word boundaries: two words that contain another word each. */
    [InlineData("UnallocatedMb", "tempdb 40% unallocated", true)]
    [InlineData("UnallocatedMb", "tempdb 40% allocated", false)]
    [InlineData("TotalReservedMb", "tempdb Space Resolved", true)]
    /* Case is not a defence, and neither is a plural. */
    [InlineData("TotalReservedMb", "tempdb — 40% Used", false)]
    [InlineData("TotalReservedMb", "tempdb 40% in use by user objects", false)]
    public void TheJudgementRedsInBothDirections(string numerator, string label, bool clean)
    {
        var offending = OffendingWords(numerator, label);

        Assert.True(
            (offending.Count == 0) == clean,
            $"With numerator '{numerator}', the label \"{label}\" should be "
            + $"{(clean ? "clean" : "reported")} and was {(offending.Count == 0 ? "clean" : "reported")} "
            + $"[{string.Join(", ", offending)}].");
    }

    /// <summary>
    /// Stripping the basis word is caught, not just replacing it: a file whose labels render the percentage
    /// and say nothing about what it measures fails the same rule.
    /// </summary>
    [Theory]
    [InlineData("TotalReservedMb", "tempdb 40% reserved", true)]
    [InlineData("TotalReservedMb", "tempdb 40%", false)]
    [InlineData("UserObjectReservedMb", "tempdb 40% user object reserved", true)]
    /* The plural names the member too; a strict word boundary would not have seen it. */
    [InlineData("UserObjectReservedMb", "tempdb 40% user objects reserved", true)]
    /* Half the required vocabulary is not the basis: "reserved" alone names the whole pool, not this part. */
    [InlineData("UserObjectReservedMb", "tempdb 40% reserved", false)]
    public void TheBasisMustBeNamedInFull(string numerator, string label, bool names)
    {
        Assert.True(
            NamesTheBasis(numerator, label) == names,
            $"With numerator '{numerator}', the label \"{label}\" should "
            + $"{(names ? "" : "NOT ")}name the basis and did {(NamesTheBasis(numerator, label) ? "" : "not")}.");
    }

    /// <summary>
    /// Which labels the per-label basis rule applies to: the ones carrying words, not the bare fragments
    /// that get composed into a sentence elsewhere.
    ///
    /// <para>The two <c>false</c> rows are the fragment the engine's resolution branch really builds, with
    /// and without a format specifier — if either read as prose, the rule would demand a basis word on a
    /// string that has nowhere to put one.</para>
    /// </summary>
    [Theory]
    [InlineData("{                  :F0}%", false)]
    [InlineData("40%", false)]
    [InlineData("tempdb {                  :F0}%", true)]
    [InlineData("{                  :F0}% ({                      :F0} MB)", true)]
    [InlineData("{                  :F0}% reserved", true)]
    public void OnlyLabelsCarryingWordsMustNameTheBasis(string label, bool prose)
    {
        Assert.True(
            CarriesProse(label) == prose,
            $"\"{label}\" should {(prose ? "" : "NOT ")}count as carrying prose and did "
            + $"{(CarriesProse(label) ? "" : "not")}.");
    }
}
