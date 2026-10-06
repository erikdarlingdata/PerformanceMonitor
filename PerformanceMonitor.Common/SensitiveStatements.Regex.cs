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

    // Case-sensitive on purpose (#5320 L1): under IgnoreCase .NET also reads U+212A (the Kelvin sign) as a member
    // of [A-Za-z], which a C-locale PostgreSQL word-boundary check does not.
    private const string WordChars = "(?-i:[0-9A-Za-z_])";

    // Lazily built, once. Null after a failed guard: every value is then named (fail closed).
    private static readonly Lazy<Func<string, Verdict>> s_judge =
        new(() => CreateJudge(Pattern, MatchTimeout, headAlternatives: JudgeHeadAlternatives));

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
    /// would be withheld); hoisted it takes about 80 ms. The judge factors each of its two parts (see
    /// <see cref="JudgeHeadAlternatives"/>) on its own; a part with fewer than two such alternatives is left as it is.
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

            i = EndOfBracket(source, i);
            if (i < 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The index just past the bracket expression that opens at <paramref name="open"/>, or -1 when it
    /// never closes. A <c>^</c> and then a <c>]</c> right after the opening bracket belong to it, and a
    /// <c>[:name:]</c> class inside is skipped whole.</summary>
    private static int EndOfBracket(string source, int open)
    {
        var i = open + 1;
        if (i < source.Length && source[i] == '^')
        {
            i++;
        }

        if (i < source.Length && source[i] == ']')
        {
            i++;
        }

        while (i < source.Length)
        {
            if (source[i] == '[' && i + 1 < source.Length && source[i + 1] == ':')
            {
                var end = source.IndexOf(":]", i + 2, StringComparison.Ordinal);
                if (end < 0)
                {
                    return -1;
                }

                i = end + 2;
                continue;
            }

            if (source[i] == ']')
            {
                return i + 1;
            }

            i++;
        }

        return -1;
    }

    /// <summary>
    /// Cuts a pattern source at the top-level <c>|</c> that ends its first <paramref name="headAlternatives"/>
    /// alternatives (#5320 M1): <paramref name="head"/> is those alternatives, <paramref name="tail"/> is the rest,
    /// and <c>head + "|" + tail</c> is exactly <paramref name="source"/>. A <c>|</c> inside a group or a bracket
    /// expression is not top level. False, with the whole source in <paramref name="head"/>, when the source has
    /// no alternative after the first <paramref name="headAlternatives"/> (or the count is below one).
    /// </summary>
    internal static bool TrySplitAlternatives(string source, int headAlternatives, out string head, out string tail)
    {
        head = source;
        tail = string.Empty;
        if (headAlternatives < 1)
        {
            return false;
        }

        var seen = 0;
        var depth = 0;
        var i = 0;
        while (i < source.Length)
        {
            switch (source[i])
            {
                case '[':
                    i = EndOfBracket(source, i);
                    if (i < 0)
                    {
                        return false;
                    }

                    continue;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case '|' when depth == 0:
                    seen++;
                    if (seen == headAlternatives)
                    {
                        if (i + 1 >= source.Length)
                        {
                            return false;
                        }

                        head = source.Substring(0, i);
                        tail = source.Substring(i + 1);
                        return true;
                    }

                    break;
            }

            i++;
        }

        return false;
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

    private const string WarmUpText = "create user warm_up password 'x'";

    /// <summary>
    /// A compiled regex built at several match timeouts, each half the one above, so one value can be matched
    /// against what is left of a shared time budget (#5320 M1). A <see cref="Regex"/> takes its timeout when it is
    /// built and has no per-call limit, so the rung that fits the time left is the one that runs: the largest whose
    /// limit is no more than the time left, and no rung at all (a timeout) when less than the smallest is left.
    /// The two largest rungs are compiled (a compiled copy is about four times faster, and one of them runs for
    /// every value that backs a long first part); the two smallest are interpreted, which costs no compile or JIT
    /// time at build, and only runs when most of the budget is already gone. The compile and the warm-up of every rung
    /// happen at build, outside any budget.
    /// </summary>
    internal sealed class BudgetedRegex
    {
        private const int Rungs = 4;
        private const int CompiledRungs = 2;
        private readonly TimeSpan[] _limits = new TimeSpan[Rungs];
        private readonly Regex[] _regexes = new Regex[Rungs];

        public BudgetedRegex(string translated, TimeSpan timeout)
        {
            // The top rung is three quarters of the budget: after any real matching work less than the whole budget
            // is left, so a rung equal to it would never be used.
            var limitMs = Math.Max(1, (long)(timeout.TotalMilliseconds * 3 / 4));
            for (var i = 0; i < Rungs; i++)
            {
                _limits[i] = TimeSpan.FromMilliseconds(Math.Max(1, limitMs >> i));
                _regexes[i] = NewRegex(translated, _limits[i], compiled: i < CompiledRungs);
            }
        }

        public void WarmUp(string text)
        {
            foreach (var regex in _regexes)
            {
                regex.IsMatch(text);
            }
        }

        /// <summary>True when the regex matches; <see cref="RegexMatchTimeoutException"/> when it ran out of
        /// <paramref name="remaining"/> time, or when too little was left to try.</summary>
        public bool IsMatch(string text, TimeSpan remaining)
        {
            for (var i = 0; i < Rungs; i++)
            {
                if (_limits[i] <= remaining)
                {
                    return _regexes[i].IsMatch(text);
                }
            }

            throw new RegexMatchTimeoutException(string.Empty, string.Empty, remaining);
        }
    }

    private static Regex NewRegex(string translated, TimeSpan timeout, bool compiled = true) =>
        new(
            translated,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline
                | (compiled ? RegexOptions.Compiled : RegexOptions.None),
            timeout);

    /// <summary>Builds a judge for a pattern source. A failed guard, or a translation .NET cannot compile,
    /// gives a judge that names every value. <paramref name="factor"/> and <paramref name="prefilter"/> exist so
    /// a test can build the unfactored form and the judge without the pre-check.
    /// <para><paramref name="headAlternatives"/> above zero compiles the source as two regexes (#5320 M1): its
    /// first <paramref name="headAlternatives"/> top-level alternatives, then the rest (see
    /// <see cref="JudgeHeadAlternatives"/>). A value is named when either matches. Both run inside the one
    /// <paramref name="timeout"/>: the first gets all of it, the second only what the first left, and a value that
    /// the pair cannot finish in that time is <see cref="Verdict.TimedOut"/>, so the pair never runs longer than
    /// the timeout plus one regex's own overshoot (the linear-time pre-check is outside it, as before). Zero
    /// compiles one regex. <paramref name="clock"/> reads the elapsed time the budget is charged with, and exists so a
    /// test can say how long the first part took.</para></summary>
    internal static Func<string, Verdict> CreateJudge(
        string source,
        TimeSpan timeout,
        bool factor = true,
        bool prefilter = true,
        int headAlternatives = 0,
        Func<TimeSpan>? clock = null)
    {
        var now = clock ?? (static () => Stopwatch.GetElapsedTime(0));
        var headSource = source;
        string? tailSource = null;
        if (TrySplitAlternatives(source, headAlternatives, out var splitHead, out var splitTail))
        {
            headSource = splitHead;
            tailSource = splitTail;
        }

        string? tailTranslated = null;
        if (!TryTranslate(headSource, factor, dropAssertions: false, out var headTranslated)
            || (tailSource is not null && !TryTranslate(tailSource, factor, dropAssertions: false, out tailTranslated)))
        {
            return static _ => Verdict.Named;
        }

        Regex head;
        BudgetedRegex? tail = null;
        try
        {
            head = NewRegex(headTranslated, timeout);
            if (tailTranslated is not null)
            {
                tail = new BudgetedRegex(tailTranslated, timeout);
            }
        }
        catch (ArgumentException)
        {
            return static _ => Verdict.Named;
        }

        // The pre-check is always built from the whole source: one linear-time pass over the superset, however
        // many regexes follow it.
        var precheck = prefilter ? CreatePrefilter(source, timeout) : null;

        // #5320 L3: the build stays outside any budget. The regexes are compiled above, and the first match still
        // pays the one-time JIT of the compiled code (inside the match timeout and the caller's clock), so run
        // each engine once on a short value here, where a budget does not time it.
        try
        {
            precheck?.IsMatch(WarmUpText);
            head.IsMatch(WarmUpText);
            tail?.WarmUp(WarmUpText);
        }
#pragma warning disable CA1031 // a failed warm-up only costs the first caller its time
        catch (Exception)
#pragma warning restore CA1031
        {
        }

        return text =>
        {
            try
            {
                // #5320 M1: no hit in the linear-time superset means the exact pattern cannot match either.
                if (precheck is not null && !precheck.IsMatch(text))
                {
                    return Verdict.Clean;
                }

                var started = now();
                if (head.IsMatch(text))
                {
                    return Verdict.Named;
                }

                if (tail is null)
                {
                    return Verdict.Clean;
                }

                return tail.IsMatch(text, timeout - (now() - started)) ? Verdict.Named : Verdict.Clean;
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
    internal static Verdict Judge(string text) => JudgeShared(s_judge, text);

    private static Verdict JudgeShared(Lazy<Func<string, Verdict>> shared, string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return Verdict.Clean;
        }

        try
        {
            return shared.Value(text);
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
    internal sealed class JudgeBudget(
        TimeSpan limit,
        Func<string, Verdict>? judge = null,
        Func<TimeSpan>? clock = null,
        Lazy<Func<string, Verdict>>? shared = null)
    {
        private readonly Func<string, Verdict>? _judge = judge;
        private readonly Lazy<Func<string, Verdict>> _shared = shared ?? s_judge;
        private readonly Func<TimeSpan> _now = clock ?? (static () => Stopwatch.GetElapsedTime(0));
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

            if (_judge is null)
            {
                // #5320 L3: build the shared judge before the clock starts, so the budget is charged matching only.
                try
                {
                    _ = _shared.Value;
                }
#pragma warning disable CA1031 // fail closed: a build that throws names the value below
                catch (Exception)
#pragma warning restore CA1031
                {
                }
            }

            var started = _now();
            Verdict verdict;
            try
            {
                verdict = _judge is not null ? _judge(text) : JudgeShared(_shared, text);
            }
#pragma warning disable CA1031 // fail closed
            catch (Exception)
#pragma warning restore CA1031
            {
                verdict = Verdict.Named;
            }

            _elapsed += _now() - started;
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
