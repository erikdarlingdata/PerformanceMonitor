// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 — a census pin over <see cref="PgSettingScrub.CandidateLikeTerms"/>: every corpus case in
/// <see cref="PgSettingRedactorTests.RedactionCases"/> whose expected output differs from its input (a case
/// the redactor actually masks) must have at least one term in the term list that would have selected it as
/// a candidate row. If a future rule joins the redactor corpus without a matching term, this fails — it is
/// so the coarse filter and the fine-grained redactor cannot drift out of rule-for-rule sync (#4348).
/// </summary>
public sealed class PgSettingScrubCandidateCensusTests
{
    public static IEnumerable<object[]> MaskedRedactionCases()
    {
        foreach (var candidate in PgSettingRedactorTests.RedactionCases())
        {
            var value = (string)candidate[1];
            var expected = (string)candidate[2];

            if (value != expected)
            {
                yield return candidate;
            }
        }
    }

    [Theory]
    [MemberData(nameof(MaskedRedactionCases))]
    public void EveryMaskedCorpusCase_MatchesAtLeastOneCandidateLikeTerm(string name, string value, string _)
    {
        var matched = false;

        foreach (var term in PgSettingScrub.CandidateLikeTerms)
        {
            if (LikePatternToRegex(term).IsMatch(value))
            {
                matched = true;
                break;
            }
        }

        // The whole-value extension-setting shape (a dotted name, e.g. "anon.salt") is selected by NAME,
        // not by the value's own text — mirror that half of CandidateSql here too, same as PgSettingRedactor's
        // own whole-value-mask decision for a dotted name.
        if (!matched && name.Contains('.'))
        {
            foreach (var term in PgSettingScrub.CandidateNameTerms)
            {
                if (LikePatternToRegex(term).IsMatch(name))
                {
                    matched = true;
                    break;
                }
            }
        }

        // ssl_passphrase_command is selected by exact name, whole value masked.
        if (!matched && name == "ssl_passphrase_command")
        {
            matched = true;
        }

        Assert.True(
            matched,
            $"Value {value!.Replace("\n", "\\n")} for setting {name} is masked by the redactor " +
            "but does not match any PgSettingScrub.CandidateLikeTerms/CandidateNameTerms entry — the " +
            "coarse filter would never select this row.");
    }

    /// <summary>
    /// Translates a Postgres ILIKE pattern (case-insensitive, '%' = any run of characters, '_' = any single
    /// character, no ESCAPE clause needed for any entry in <see cref="PgSettingScrub.CandidateLikeTerms"/>
    /// today, since none of them contain a literal '%' or '_') into an equivalent case-insensitive .NET
    /// regex, anchored to match the whole string.
    /// </summary>
    private static Regex LikePatternToRegex(string likePattern)
    {
        var builder = new System.Text.StringBuilder("^");

        foreach (var c in likePattern)
        {
            switch (c)
            {
                case '%':
                    builder.Append(".*");
                    break;
                case '_':
                    builder.Append('.');
                    break;
                default:
                    builder.Append(Regex.Escape(c.ToString()));
                    break;
            }
        }

        builder.Append('$');

        return new Regex(builder.ToString(), RegexOptions.IgnoreCase | RegexOptions.Singleline);
    }
}
