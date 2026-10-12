using System;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Plain text searches that answer the same yes-or-no questions as a few of the analyzer's regexes, in one linear
/// pass and with no regex. When a regex is skipped (the plan's time limit has passed, or the call timed out), the
/// call site asks one of these instead, so a warning that states a fact about the statement text is raised only
/// when the text shows it. A word here is delimited as a regex's <c>\b</c> would: by a character that is not a
/// letter, a digit or an underscore. A gap between words is one or more whitespace characters, as <c>\s+</c>.
/// </summary>
internal static class PlanTextSearch
{
    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    /// <summary>
    /// True when <paramref name="text"/> holds <paramref name="words"/> in order, each pair separated by
    /// whitespace, compared without regard to case. <paramref name="wordStart"/> asks for a word boundary before the
    /// first word, and <paramref name="wordEnd"/> for one after the last.
    /// </summary>
    internal static bool ContainsWords(string text, bool wordStart, bool wordEnd, params string[] words)
    {
        var span = text.AsSpan();
        var from = 0;
        while (from <= span.Length - words[0].Length)
        {
            var found = span[from..].IndexOf(words[0], StringComparison.OrdinalIgnoreCase);
            if (found < 0)
                return false;

            var at = from + found;
            from = at + 1;
            if (wordStart && at > 0 && IsWordChar(span[at - 1]))
                continue;

            var position = at + words[0].Length;
            var matched = true;
            for (var w = 1; w < words.Length && matched; w++)
            {
                var gapStart = position;
                while (position < span.Length && char.IsWhiteSpace(span[position]))
                    position++;

                matched = position > gapStart
                    && span[position..].StartsWith(words[w], StringComparison.OrdinalIgnoreCase);
                if (matched)
                    position += words[w].Length;
            }

            if (!matched)
                continue;

            if (wordEnd && position < span.Length && IsWordChar(span[position]))
                continue;

            return true;
        }

        return false;
    }

    /// <summary>
    /// True when a predicate holds nothing but PROBE(...) bitmap filters joined by AND or OR, as the regex pair that
    /// strips them does. The arguments of a PROBE may nest parentheses to any depth here; an unbalanced one is not
    /// PROBE-only.
    /// </summary>
    internal static bool IsProbeOnly(string predicate)
    {
        var position = 0;
        while (true)
        {
            while (position < predicate.Length && char.IsWhiteSpace(predicate[position]))
                position++;

            if (position >= predicate.Length)
                return true;

            var rest = predicate.AsSpan(position);
            if (StartsWithWord(rest, "AND"))
            {
                position += 3;
            }
            else if (StartsWithWord(rest, "OR"))
            {
                position += 2;
            }
            else if (rest.StartsWith("PROBE", StringComparison.OrdinalIgnoreCase))
            {
                position += 5;
                while (position < predicate.Length && char.IsWhiteSpace(predicate[position]))
                    position++;

                if (position >= predicate.Length || predicate[position] != '(')
                    return false;

                var depth = 0;
                for (; position < predicate.Length; position++)
                {
                    if (predicate[position] == '(')
                    {
                        depth++;
                    }
                    else if (predicate[position] == ')' && --depth == 0)
                    {
                        break;
                    }
                }

                if (depth != 0)
                    return false;

                position++; // past the closing parenthesis
            }
            else
            {
                return false;
            }
        }
    }

    private static bool StartsWithWord(ReadOnlySpan<char> rest, string word) =>
        rest.StartsWith(word, StringComparison.OrdinalIgnoreCase)
        && (rest.Length == word.Length || !IsWordChar(rest[word.Length]));
}
