using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Every regular expression the plan analyzer runs on plan text goes through here. Statement text and
/// predicate strings come from plan XML that a person or an MCP client may have written, and a pattern
/// such as <c>\s*AND\s+PROBE\s*\(...</c> on a long run of spaces backtracks for minutes. Each call has a
/// match timeout, and a timeout gives the answer "nothing found" instead of an exception, so one hostile
/// string costs the analyzer about a quarter of a second and never a crash or a hang:
/// <list type="bullet">
/// <item><description><c>IsMatch</c> gives false;</description></item>
/// <item><description><c>Match</c> gives no match (<see cref="System.Text.RegularExpressions.Match.Empty"/>);</description></item>
/// <item><description><c>Matches</c> gives an empty list, never the part found before the timeout;</description></item>
/// <item><description><c>Replace</c> returns the input unchanged.</description></item>
/// </list>
/// The overloads that take a <see cref="TimeSpan"/> exist so a test can force a timeout in a few milliseconds.
/// The <c>[GeneratedRegex]</c> fields carry the same timeout (<see cref="TimeoutMilliseconds"/>) in their
/// attribute, and every use of one goes through the <see cref="Regex"/> overloads here.
/// </summary>
internal static class PlanRegex
{
    /// <summary>The match timeout in milliseconds: the default of every helper and of every generated regex.</summary>
    internal const int TimeoutMilliseconds = 250;

    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(TimeoutMilliseconds);

    internal static bool IsMatch(string input, string pattern, RegexOptions options = RegexOptions.None) =>
        IsMatch(input, pattern, options, DefaultTimeout);

    internal static bool IsMatch(string input, string pattern, RegexOptions options, TimeSpan timeout)
    {
        try
        {
            return Regex.IsMatch(input, pattern, options, timeout);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    internal static Match Match(string input, string pattern, RegexOptions options = RegexOptions.None) =>
        Match(input, pattern, options, DefaultTimeout);

    internal static Match Match(string input, string pattern, RegexOptions options, TimeSpan timeout)
    {
        try
        {
            return Regex.Match(input, pattern, options, timeout);
        }
        catch (RegexMatchTimeoutException)
        {
            return System.Text.RegularExpressions.Match.Empty;
        }
    }

    internal static string Replace(string input, string pattern, string replacement, RegexOptions options = RegexOptions.None) =>
        Replace(input, pattern, replacement, options, DefaultTimeout);

    internal static string Replace(string input, string pattern, string replacement, RegexOptions options, TimeSpan timeout)
    {
        try
        {
            return Regex.Replace(input, pattern, replacement, options, timeout);
        }
        catch (RegexMatchTimeoutException)
        {
            return input;
        }
    }

    /// <summary>For a <c>[GeneratedRegex]</c> field: its own timeout applies, and a timeout gives false.</summary>
    internal static bool IsMatch(Regex regex, string input)
    {
        try
        {
            return regex.IsMatch(input);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    /// <summary>For a <c>[GeneratedRegex]</c> field: its own timeout applies, and a timeout gives no match.</summary>
    internal static Match Match(Regex regex, string input)
    {
        try
        {
            return regex.Match(input);
        }
        catch (RegexMatchTimeoutException)
        {
            return System.Text.RegularExpressions.Match.Empty;
        }
    }

    /// <summary>
    /// For a <c>[GeneratedRegex]</c> field. A <c>MatchCollection</c> runs the regex lazily, while the
    /// caller enumerates it, so the timeout would surface inside the caller's <c>foreach</c>. This runs the whole
    /// pass here and hands back a list; a timeout gives an empty list.
    /// </summary>
    internal static IReadOnlyList<Match> Matches(Regex regex, string input)
    {
        try
        {
            var found = new List<Match>();
            foreach (Match match in regex.Matches(input))
                found.Add(match);
            return found;
        }
        catch (RegexMatchTimeoutException)
        {
            return Array.Empty<Match>();
        }
    }
}
