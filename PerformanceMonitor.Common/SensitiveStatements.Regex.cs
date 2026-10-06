/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Common;

/// <summary>
/// The .NET evaluation of the shared statement filter (#4348): the same pattern PostgreSQL judges with
/// <c>~*</c>, translated token by token into a .NET regular expression. The translation is derived from
/// <see cref="Pattern"/> at first use and never copied, so the two engines cannot drift apart on what the
/// pattern says. Nothing calls it yet.
/// </summary>
public static partial class SensitiveStatements
{
    /// <summary>The per-value match timeout. A value that runs past it is treated as named.</summary>
    internal static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(250);

    /// <summary>The outcome of judging one value. <see cref="TimedOut"/> counts as named everywhere.</summary>
    internal enum Verdict
    {
        Clean,
        Named,
        TimedOut,
    }

    private const string WordChars = "[0-9A-Za-z_]";

    // Lazily built, once. Null after a failed guard: every value is then named (fail closed).
    private static readonly Lazy<Func<string, Verdict>> s_judge =
        new(() => CreateJudge(Pattern, MatchTimeout));

    /// <summary>The pattern as .NET reads it, or null when a guard failed. Exposed so a test can pin the
    /// translation as a literal.</summary>
    internal static string? TranslatedPattern => TryTranslate(Pattern, out var translated) ? translated : null;

    /// <summary>
    /// Translates a POSIX ARE source into the .NET equivalent, or returns false when a guard fails. Guards:
    /// the source has no backslash and no <c>^</c> or <c>$</c> outside a bracket expression (a <c>]</c> that
    /// closes no open bracket is a literal), and no <c>[:</c> remains after translating.
    /// </summary>
    internal static bool TryTranslate(string source, out string translated) =>
        TryTranslate(source, factor: true, dropAssertions: false, out translated);

    /// <summary>
    /// The translation with two switches for the pre-check and the offline pins. <paramref name="factor"/> false
    /// leaves the word-start assertion on every alternative (the unfactored form a test compares against).
    /// <paramref name="dropAssertions"/> true removes every word-boundary assertion instead of translating it, so
    /// the result matches a superset of what the full translation matches and uses no lookaround, which lets
    /// <see cref="RegexOptions.NonBacktracking"/> run it in linear time.
    /// </summary>
    internal static bool TryTranslate(string source, bool factor, bool dropAssertions, out string translated)
    {
        translated = string.Empty;
        if (source.Length == 0 || source.Contains('\\', StringComparison.Ordinal) || !AnchorsOnlyInBrackets(source))
        {
            return false;
        }

        // The word-boundary tokens first, then the classes (a class sits inside a bracket expression).
        var wordStart = dropAssertions ? string.Empty : "(?<!" + WordChars + ")(?=" + WordChars + ")";
        var wordEnd = dropAssertions ? string.Empty : "(?<=" + WordChars + ")(?!" + WordChars + ")";
        var result = source
            .Replace("[[:<:]]", wordStart, StringComparison.Ordinal)
            .Replace("[[:>:]]", wordEnd, StringComparison.Ordinal)
            .Replace("[:space:]", "\\s",StringComparison.Ordinal)
            .Replace("[:cntrl:]", "\\p{Cc}",StringComparison.Ordinal);

        if (result.Contains("[:", StringComparison.Ordinal))
        {
            return false;
        }

        translated = factor && !dropAssertions ? FactorWordStart(result) : result;
        return true;
    }

    private const string WordStart = "(?<!" + WordChars + ")(?=" + WordChars + ")";

    /// <summary>
    /// Hoists the word-start assertion that most top-level alternatives begin with into one group:
    /// <c>S a|S b|c</c> becomes <c>(?:S (?:a|b))|c</c>. The same strings match; only the engine's search
    /// differs. With the assertion repeated at the head of every alternative .NET tries all of them at every
    /// position and a 1,000,000-character statement takes about 300 ms (past the 250 ms match timeout, so it
    /// would be withheld); hoisted it takes about 80 ms.
    /// </summary>
    private static string FactorWordStart(string translated)
    {
        var grouped = new List<string>();
        var others = new List<string>();
        var start = 0;
        var depth = 0;
        for (var i = 0; i <= translated.Length; i++)
        {
            if (i == translated.Length || (translated[i] == '|' && depth == 0))
            {
                var branch = translated.Substring(start, i - start);
                if (branch.StartsWith(WordStart, StringComparison.Ordinal))
                {
                    grouped.Add(branch.Substring(WordStart.Length));
                }
                else
                {
                    others.Add(branch);
                }

                start = i + 1;
                continue;
            }

            switch (translated[i])
            {
                case '[':
                    // A bracket expression: a leading ^ and then a leading ] are part of it.
                    i++;
                    if (i < translated.Length && translated[i] == '^')
                    {
                        i++;
                    }

                    if (i < translated.Length && translated[i] == ']')
                    {
                        i++;
                    }

                    while (i < translated.Length && translated[i] != ']')
                    {
                        i++;
                    }

                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
            }
        }

        if (grouped.Count < 2 || depth != 0)
        {
            return translated;
        }

        var factored = "(?:" + WordStart + "(?:" + string.Join("|", grouped) + "))";
        return others.Count == 0 ? factored : factored + "|" + string.Join("|", others);
    }

    /// <summary>True when no <c>^</c> or <c>$</c> sits outside a bracket expression. Inside a bracket a
    /// <c>[:name:]</c> class is skipped whole; a first <c>]</c> (after an optional <c>^</c>) is a literal;
    /// a <c>]</c> outside any bracket is a literal.</summary>
    private static bool AnchorsOnlyInBrackets(string source)
    {
        var i = 0;
        while (i < source.Length)
        {
            var c = source[i];
            if (c == '^' || c == '$')
            {
                return false;
            }

            if (c != '[')
            {
                i++;
                continue;
            }

            // A bracket expression opens.
            i++;
            if (i < source.Length && source[i] == '^')
            {
                i++;
            }

            if (i < source.Length && source[i] == ']')
            {
                i++;
            }

            var closed = false;
            while (i < source.Length)
            {
                if (source[i] == '[' && i + 1 < source.Length && source[i + 1] == ':')
                {
                    var end = source.IndexOf(":]", i + 2, StringComparison.Ordinal);
                    if (end < 0)
                    {
                        return false;
                    }

                    i = end + 2;
                    continue;
                }

                if (source[i] == ']')
                {
                    i++;
                    closed = true;
                    break;
                }

                i++;
            }

            if (!closed)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Builds the linear-time pre-check for a pattern source: the same translation with every word-boundary
    /// assertion removed, run with <see cref="RegexOptions.NonBacktracking"/>. It matches a superset of the
    /// full judge, so no hit means the full judge cannot match either; a hit proves nothing and the full judge
    /// decides. Null when the guards or the build fail, and the caller then always runs the full judge (fail
    /// closed: a missing pre-check only costs time).
    /// </summary>
    internal static Regex? CreatePrefilter(string source, TimeSpan timeout)
    {
        if (!TryTranslate(source, factor: false, dropAssertions: true, out var superset))
        {
            return null;
        }

        try
        {
            return new Regex(
                superset,
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.NonBacktracking,
                timeout);
        }
#pragma warning disable CA1031 // fail closed: no pre-check means the full judge runs on every value
        catch (Exception)
#pragma warning restore CA1031
        {
            return null;
        }
    }

    /// <summary>Builds a judge for a pattern source. A failed guard, or a translation .NET cannot compile,
    /// gives a judge that names every value. <paramref name="factor"/> and <paramref name="prefilter"/> exist so
    /// a test can build the unfactored form and the judge without the pre-check.</summary>
    internal static Func<string, Verdict> CreateJudge(string source, TimeSpan timeout, bool factor = true, bool prefilter = true)
    {
        if (!TryTranslate(source, factor, dropAssertions: false, out var translated))
        {
            return static _ => Verdict.Named;
        }

        Regex regex;
        try
        {
            regex = new Regex(
                translated,
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled,
                timeout);
        }
        catch (ArgumentException)
        {
            return static _ => Verdict.Named;
        }

        var precheck = prefilter ? CreatePrefilter(source, timeout) : null;

        return text =>
        {
            try
            {
                // #5320 M1: no hit in the linear-time superset means the exact pattern cannot match either.
                if (precheck is not null && !precheck.IsMatch(text))
                {
                    return Verdict.Clean;
                }

                return regex.IsMatch(text) ? Verdict.Named : Verdict.Clean;
            }
            catch (RegexMatchTimeoutException)
            {
                return Verdict.TimedOut;
            }
#pragma warning disable CA1031 // fail closed: any other failure counts as named
            catch (Exception)
#pragma warning restore CA1031
            {
                return Verdict.Named;
            }
        };
    }

    /// <summary>Judges one value against <see cref="Pattern"/>. Null or empty is clean; an exception is named.</summary>
    internal static Verdict Judge(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return Verdict.Clean;
        }

        try
        {
            return s_judge.Value(text);
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return Verdict.Named;
        }
    }

    /// <summary>True when <paramref name="text"/> is named by the filter, including a timed-out judgement.
    /// Null or empty is false.</summary>
    public static bool Names(string? text) => !string.IsNullOrEmpty(text) && Judge(text) != Verdict.Clean;

    /// <summary>
    /// <see cref="PlaceholderText"/> when <paramref name="text"/> is named, or when it contains <c>&amp;</c>
    /// and its HTML-decoded form is named; otherwise the same instance. Never throws.
    /// </summary>
    public static string? Text(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        try
        {
            if (Judge(text) != Verdict.Clean)
            {
                return PlaceholderText;
            }

            if (text.Contains('&', StringComparison.Ordinal) && Judge(WebUtility.HtmlDecode(text)) != Verdict.Clean)
            {
                return PlaceholderText;
            }

            return text;
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            return PlaceholderText;
        }
    }

    /// <summary>
    /// Runs a judge under an elapsed-time budget. Once <see cref="Elapsed"/> reaches the limit
    /// (<see cref="Spent"/>), every later value is returned as named without running the judge and is counted
    /// unjudged.
    /// </summary>
    internal sealed class JudgeBudget(TimeSpan limit, Func<string, Verdict>? judge = null)
    {
        private readonly Func<string, Verdict> _judge = judge ?? SensitiveStatements.Judge;
        private TimeSpan _elapsed;

        public TimeSpan Limit { get; } = limit;

        public TimeSpan Elapsed => _elapsed;

        public bool Spent => _elapsed >= Limit;

        /// <summary>Values the judge named.</summary>
        public int Named { get; private set; }

        /// <summary>Values whose match timed out (they count as named).</summary>
        public int TimedOut { get; private set; }

        /// <summary>Values returned as named because the budget was already spent.</summary>
        public int Unjudged { get; private set; }

        /// <summary>Charges time the caller spent on this budget's behalf (for example an XML parse).</summary>
        public void AddElapsed(TimeSpan elapsed)
        {
            if (elapsed > TimeSpan.Zero)
            {
                _elapsed += elapsed;
            }
        }

        public Verdict Judge(string text)
        {
            if (Spent)
            {
                Unjudged++;
                return Verdict.Named;
            }

            var started = Stopwatch.GetTimestamp();
            Verdict verdict;
            try
            {
                verdict = _judge(text);
            }
#pragma warning disable CA1031 // fail closed
            catch (Exception)
#pragma warning restore CA1031
            {
                verdict = Verdict.Named;
            }

            _elapsed += Stopwatch.GetElapsedTime(started);
            if (verdict == Verdict.Named)
            {
                Named++;
            }
            else if (verdict == Verdict.TimedOut)
            {
                TimedOut++;
            }

            return verdict;
        }
    }
}
