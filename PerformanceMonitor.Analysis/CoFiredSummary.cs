using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Builds the "what else fired in this analysis window" cross-reference shared by both apps'
/// recommendation surfaces and both MCP tools. This is the first, deliberately small slice of the
/// correlate-and-focus work (review §1d): it does NOT group or persist anything — it just lets each
/// finding point at the others from the same run, so the operator stops hunting across cards to see
/// that they are related. The content is a flat factual list (no synthesis / reasoning — that is the
/// MCP/LLM consumer's job), capped, highest-severity first.
/// </summary>
public static class CoFiredSummary
{
    /// <summary>How many sibling titles to name before collapsing the rest into "(+N more)".</summary>
    public const int DefaultCap = 3;

    /// <summary>
    /// The distinct OTHER finding titles in the same analysis window, highest-severity first,
    /// excluding the caller's own title. <paramref name="windowTitles"/> is every finding/card title in
    /// the window paired with its raw severity; the caller passes its own title as <paramref name="ownTitle"/>.
    /// </summary>
    public static IReadOnlyList<string> OtherTitles(
        string? ownTitle, IEnumerable<(string Title, double Severity)> windowTitles)
    {
        if (windowTitles is null)
            return Array.Empty<string>();

        return windowTitles
            .Where(t => !string.IsNullOrWhiteSpace(t.Title)
                     && !string.Equals(t.Title, ownTitle, StringComparison.Ordinal))
            .OrderByDescending(t => t.Severity)
            .Select(t => t.Title)
            .Distinct(StringComparer.Ordinal) // LINQ-to-objects Distinct keeps first (highest-severity) occurrence
            .ToList();
    }

    /// <summary>
    /// A demarcated one-line cross-reference ("Also surfaced in this analysis window: A; B; C (+N more).")
    /// for appending to a card's advice text, or null when nothing else fired in the window.
    /// </summary>
    public static string? Line(IReadOnlyList<string> others, int cap = DefaultCap)
    {
        if (others is null || others.Count == 0)
            return null;

        var shown = others.Take(cap).ToList();
        var extra = others.Count - shown.Count;
        var list = string.Join("; ", shown);
        if (extra > 0)
            list += $" (+{extra} more)";
        return "Also surfaced in this analysis window: " + list + ".";
    }
}
