using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Every regular expression the plan-analysis project runs on plan text goes through here. Statement text and
/// predicate strings come from plan XML that a person or an MCP client may have written, and a pattern
/// such as <c>\s*AND\s+PROBE\s*\(...</c> on a long run of spaces backtracks for minutes. Two limits apply:
/// <list type="bullet">
/// <item><description>each call has a match timeout (<see cref="TimeoutMilliseconds"/>);</description></item>
/// <item><description>inside <see cref="BeginPlan()"/>, one plan has a wall-clock deadline
/// (<see cref="PlanBudgetMilliseconds"/>) that starts when the analysis starts and counts all the time, in
/// regex calls and out of them. The regex helpers check it, and so do the loops that run without a regex
/// (<see cref="TimeIsUp"/>). Once it has passed, every further check is skipped.</description></item>
/// </list>
/// A timeout or a skipped call never throws and never quietly changes the answer: the check is recorded
/// (<see cref="TakeSkipped"/>), and the analyzer adds one note to the statement that names the checks it
/// skipped. A skipped check is a missing result and never an invented one: a warning that states a fact about
/// the text is raised only when the text shows it. Each helper takes the check's name for that note, and each
/// call site picks the answer a timeout gives:
/// <list type="bullet">
/// <item><description><c>IsMatch</c> gives the caller's <c>onTimeout</c> value, or, in the overloads that take a
/// linear fallback, the answer of a plain text search that needs no regex and is not recorded as skipped;</description></item>
/// <item><description><c>Match</c> and <c>Replace</c> report through <c>completed</c> that they did not finish;</description></item>
/// <item><description><c>Matches</c> yields lazily, so a caller's early exit still ends the pass, and its
/// <see cref="MatchRun.Incomplete"/> says the pass stopped before the end.</description></item>
/// </list>
/// The overloads that take a <see cref="TimeSpan"/> exist so a test can force a timeout in a few milliseconds.
/// The <c>[GeneratedRegex]</c> fields carry the same timeout in their attribute, and every use of one goes
/// through the <see cref="Regex"/> overloads here.
/// </summary>
internal static class PlanRegex
{
    /// <summary>The match timeout in milliseconds: the default of every helper and of every generated regex.</summary>
    internal const int TimeoutMilliseconds = 250;

    /// <summary>
    /// The wall-clock time one plan's analysis may run before its remaining checks are skipped, in
    /// milliseconds, counted from the start of the analysis. Analyzing a normal plan takes a few milliseconds,
    /// so 2 seconds is far above any real plan, while the worst case per plan stays near 2 seconds plus one
    /// regex call's timeout or one pass over a predicate, no matter how much of the time goes to loops that
    /// run without a regex. The Darling service and Lite analyze the top plans of every cycle, so this
    /// bounds the cycle's cost.
    /// </summary>
    internal const int PlanBudgetMilliseconds = 2000;

    internal static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(TimeoutMilliseconds);

    /// <summary>The note's wording for the checks <see cref="TakeSkipped"/> returned.</summary>
    internal static string SkippedNote(IReadOnlyList<string> checks) =>
        "Some checks on this statement were skipped because the plan was too large or complex to finish within the analysis time limit: "
        + string.Join(", ", checks) + ". Warnings that depend on those checks may be missing or less specific.";

    // ---- The per-plan budget ----

    private sealed class Budget
    {
        private readonly TimeSpan _limit;
        private readonly long _startStamp = Stopwatch.GetTimestamp();
        private readonly Func<TimeSpan>? _clock = ClockForTest;
        private readonly TimeSpan _clockStart;
        private readonly List<string> _skipped = new();

        internal Budget(TimeSpan limit)
        {
            _limit = limit;
            _clockStart = _clock?.Invoke() ?? TimeSpan.Zero;
        }

        private TimeSpan Elapsed => _clock != null ? _clock() - _clockStart : Stopwatch.GetElapsedTime(_startStamp);

        internal bool Exhausted => Elapsed >= _limit;

        internal TimeSpan Remaining
        {
            get
            {
                var left = _limit - Elapsed;
                return left > TimeSpan.Zero ? left : TimeSpan.Zero;
            }
        }

        internal void Skip(string check)
        {
            if (!_skipped.Contains(check))
                _skipped.Add(check);
        }

        internal string[] Take()
        {
            var taken = _skipped.ToArray();
            _skipped.Clear();
            return taken;
        }
    }

    /// <summary>
    /// A test's own clock, read as the time since an origin of the test's choosing, on this thread only. A plan
    /// that begins while it is set measures its deadline against it, so a test can spend the time limit
    /// part-way through a loop without waiting.
    /// </summary>
    [ThreadStatic]
    internal static Func<TimeSpan>? ClockForTest;

    // Analysis is synchronous, so the plan's budget lives on the thread that runs it.
    [ThreadStatic]
    private static Budget? t_budget;

    /// <summary>Ends the plan's budget when disposed. A scope opened inside another one shares the outer budget.</summary>
    internal sealed class Scope : IDisposable
    {
        private readonly object? _owned;

        internal Scope(object? owned) => _owned = owned;

        public void Dispose()
        {
            if (_owned != null && ReferenceEquals(t_budget, _owned))
                t_budget = null;
        }
    }

    /// <summary>Starts one plan's time budget on this thread.</summary>
    internal static Scope BeginPlan() => BeginPlan(TimeSpan.FromMilliseconds(PlanBudgetMilliseconds));

    internal static Scope BeginPlan(TimeSpan budget)
    {
        if (t_budget != null)
            return new Scope(null);

        var created = new Budget(budget);
        t_budget = created;
        return new Scope(created);
    }

    /// <summary>The checks skipped (timeout or spent budget) since the last call, without repeats. Empty outside a plan.</summary>
    internal static IReadOnlyList<string> TakeSkipped() => t_budget?.Take() ?? Array.Empty<string>();

    /// <summary>False, with the check recorded as skipped when <paramref name="record"/>, once the plan's deadline has passed.</summary>
    private static bool CanRun(string check, bool record = true)
    {
        var budget = t_budget;
        if (budget == null || !budget.Exhausted)
            return true;

        if (record)
            budget.Skip(check);
        return false;
    }

    /// <summary>
    /// For a loop that runs without a regex: true, with <paramref name="check"/> recorded as skipped, once the
    /// plan's deadline has passed, and the loop must stop. Always false outside a plan.
    /// </summary>
    internal static bool TimeIsUp(string check) => !CanRun(check);

    private static void RecordTimeout(string check) => t_budget?.Skip(check);

    /// <summary>The timeout for one call: the given one, or what is left of the plan's budget when that is less.</summary>
    private static TimeSpan Limit(TimeSpan timeout)
    {
        var remaining = t_budget?.Remaining;
        if (remaining == null || remaining.Value >= timeout)
            return timeout;

        return remaining.Value < TimeSpan.FromMilliseconds(1) ? TimeSpan.FromMilliseconds(1) : remaining.Value;
    }

    /// <summary>Runs one regex call under the budget. False when it was skipped or timed out; the check is recorded.</summary>
    private static bool Run<T>(string check, Func<T> call, out T result, bool record = true)
    {
        result = default!;
        if (!CanRun(check, record))
            return false;

        try
        {
            result = call();
            return true;
        }
        catch (RegexMatchTimeoutException)
        {
            if (record)
                RecordTimeout(check);
            return false;
        }
    }

    // ---- Pattern strings ----

    internal static bool IsMatch(string input, string pattern, RegexOptions options, string check, bool onTimeout = false) =>
        IsMatch(input, pattern, options, DefaultTimeout, check, onTimeout);

    internal static bool IsMatch(string input, string pattern, RegexOptions options, TimeSpan timeout, string check, bool onTimeout = false) =>
        Run(check, () => Regex.IsMatch(input, pattern, options, Limit(timeout)), out var found) ? found : onTimeout;

    /// <summary>
    /// The regex's answer, or, when the call timed out or was skipped, <paramref name="withoutRegex"/>'s answer
    /// on the same text: a linear text search that decides the same question. Nothing is recorded as skipped,
    /// because the question was answered.
    /// </summary>
    internal static bool IsMatch(string input, string pattern, RegexOptions options, string check, Func<string, bool> withoutRegex) =>
        Run(check, () => Regex.IsMatch(input, pattern, options, Limit(DefaultTimeout)), out var found, record: false) ? found : withoutRegex(input);

    /// <summary>The match, or <see cref="System.Text.RegularExpressions.Match.Empty"/> with <paramref name="completed"/> false.</summary>
    internal static Match Match(string input, string pattern, RegexOptions options, string check, out bool completed) =>
        Match(input, pattern, options, DefaultTimeout, check, out completed);

    internal static Match Match(string input, string pattern, RegexOptions options, TimeSpan timeout, string check, out bool completed)
    {
        completed = Run(check, () => Regex.Match(input, pattern, options, Limit(timeout)), out var match);
        return completed ? match : System.Text.RegularExpressions.Match.Empty;
    }

    /// <summary>The replaced text, or the input unchanged with <paramref name="completed"/> false.</summary>
    /// <param name="recordSkip">False when the caller answers the question another way if the call does not finish, so nothing is recorded as skipped.</param>
    internal static string Replace(string input, string pattern, string replacement, RegexOptions options, string check, out bool completed, bool recordSkip = true) =>
        Replace(input, pattern, replacement, options, DefaultTimeout, check, out completed, recordSkip);

    internal static string Replace(string input, string pattern, string replacement, RegexOptions options, TimeSpan timeout, string check, out bool completed, bool recordSkip = true)
    {
        completed = Run(check, () => Regex.Replace(input, pattern, replacement, options, Limit(timeout)), out var replaced, recordSkip);
        return completed ? replaced : input;
    }

    // ---- Regex instances ([GeneratedRegex] and compiled fields): their own timeout applies ----

    /// <summary>The answer when the call finished; <paramref name="onTimeout"/> when it timed out or was skipped.</summary>
    internal static bool IsMatch(Regex regex, string input, string check, bool onTimeout = false) =>
        TryIsMatch(regex, input, check, out var found) ? found : onTimeout;

    /// <summary>The answer of <paramref name="regex"/>, or <paramref name="withoutRegex"/>'s when the call timed out or was skipped (nothing recorded).</summary>
    internal static bool IsMatch(Regex regex, string input, string check, Func<string, bool> withoutRegex) =>
        Run(check, () => regex.IsMatch(input), out var found, record: false) ? found : withoutRegex(input);

    /// <summary>
    /// <see cref="TryIsMatch(Regex, string, string, out bool)"/> on <c>input[start..(start + length)]</c>, read in
    /// place with no copy. The range is the whole input as far as the pattern's anchors go.
    /// </summary>
    internal static bool TryIsMatch(Regex regex, string input, int start, int length, string check, out bool matched) =>
        Run(check, () => regex.Match(input, start, length).Success, out matched);

    /// <summary>False when the call timed out or was skipped; <paramref name="matched"/> is then false.</summary>
    internal static bool TryIsMatch(Regex regex, string input, string check, out bool matched) =>
        Run(check, () => regex.IsMatch(input), out matched);

    /// <summary>The match, or <see cref="System.Text.RegularExpressions.Match.Empty"/> with <paramref name="completed"/> false.</summary>
    internal static Match Match(Regex regex, string input, string check, out bool completed)
    {
        completed = Run(check, () => regex.Match(input), out var match);
        return completed ? match : System.Text.RegularExpressions.Match.Empty;
    }

    /// <summary>
    /// The matches of <paramref name="regex"/>, found lazily one at a time, so a caller that leaves its
    /// <c>foreach</c> early stops the pass. Each step is timed against the plan's budget. A timeout or a
    /// spent budget ends the list at the matches found so far and sets <see cref="MatchRun.Incomplete"/>.
    /// </summary>
    internal static MatchRun Matches(Regex regex, string input, string check) => new(regex, input, check, 0, input.Length);

    /// <summary><see cref="Matches(Regex, string, string)"/> over <c>input[start..(start + length)]</c>, read in place with no copy.</summary>
    internal static MatchRun Matches(Regex regex, string input, int start, int length, string check) => new(regex, input, check, start, length);

    /// <summary>One lazy pass over a regex's matches. Enumerate it once; read <see cref="Incomplete"/> afterwards.</summary>
    internal sealed class MatchRun : IEnumerable<Match>
    {
        private readonly Regex _regex;
        private readonly string _input;
        private readonly string _check;
        private readonly int _start;
        private readonly int _length;

        internal MatchRun(Regex regex, string input, string check, int start, int length)
        {
            _regex = regex;
            _input = input;
            _check = check;
            _start = start;
            _length = length;
        }

        /// <summary>True when the pass stopped before the end of the input because of a timeout or the spent budget.</summary>
        internal bool Incomplete { get; private set; }

        public IEnumerator<Match> GetEnumerator()
        {
            var current = Step(null);
            while (current != null && current.Success)
            {
                yield return current;
                current = Step(current);
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private Match? Step(Match? previous)
        {
            if (!Run(_check, () => previous == null ? _regex.Match(_input, _start, _length) : previous.NextMatch(), out var next))
            {
                Incomplete = true;
                return null;
            }

            return next;
        }
    }
}
