using System;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The limits on plan XML that a person or an MCP client supplies: a file opened or dropped in a viewer,
/// pasted text, or the <c>plan_xml</c> argument of <c>analyze_plan_xml</c>. The check is one pass over the
/// characters and runs BEFORE any parse, so a refused input costs milliseconds instead of the parse.
/// <para>It is deliberately not part of <see cref="ShowPlanParser.Parse"/> or the viewer's <c>LoadPlan</c>:
/// plans the collectors stored go through both, and those keep their own limits (16M characters, depth
/// 1,000). Only the entry points that take text from a caller call this.</para>
/// </summary>
public static class CallerPlanXml
{
    /// <summary>
    /// The most attributes one element may carry. Measured on the shipped parser (Release build, this
    /// machine): the cost of one element grows with the SQUARE of its attribute count, 0.27 s at 100,000
    /// attributes, 0.9 s at 200,000, 6.3 s at 400,000 and 27 s at 1,000,000 (11.9M characters, inside the 16M
    /// cap). The same attributes spread over many elements cost almost nothing: 1,000,000 elements with one
    /// attribute each parse in 0.8 s, and 15M characters of elements with 10,000 attributes each in 0.5 s.
    /// Size alone is linear, so the 16M-character cap stays as it is. The widest element in the largest
    /// real plans of the test fixtures has 20 attributes, so 1,000 leaves 50x of room; at 1,000 attributes per
    /// element the worst 16M-character input parses in about half a second.
    /// </summary>
    internal const int MaxAttributesPerElement = 1_000;

    /// <summary>
    /// Returns why <paramref name="xml"/> is refused, or null when it may go on to the parser. The sentence
    /// names the limit, so it can go to the caller as it is (an MCP refusal or a viewer dialog).
    /// </summary>
    public static string? RefusalReason(string? xml)
    {
        if (string.IsNullOrEmpty(xml)) return null;

        // Showplan XML never has a DTD. Refusing one closes entity expansion (up to 10M characters from
        // a few hundred) without asking anyone to hand-craft a plan that needs it.
        if (xml.Contains("<!DOCTYPE", StringComparison.Ordinal))
            return "The plan XML has a DOCTYPE declaration. Showplan XML never has one, so it is not read.";

        // Count the attributes of each tag: the '=' signs between '<' and '>' that are OUTSIDE a quoted
        // value. A '>' or an '=' inside a value is text, not markup: a predicate such as [a]=[b] holds
        // '=' signs, and a hostile tag can hide behind a '>' in a value (a plain '<'..'>' count resets
        // there, so 1,000,000 attributes each holding a '>' would count as one). '<' always starts a new
        // tag and clears the quote, because an attribute value cannot hold '<' and the parser rejects one
        // that does, so a stray quote in a comment or CDATA section cannot carry over into the next tag.
        var inTag = false;
        var quote = '\0';
        var attributes = 0;
        foreach (var c in xml)
        {
            if (c == '<')
            {
                inTag = true;
                quote = '\0';
                attributes = 0;
            }
            else if (!inTag)
            {
                continue;
            }
            else if (quote != '\0')
            {
                if (c == quote) quote = '\0';
            }
            else if (c == '"' || c == '\'')
            {
                quote = c;
            }
            else if (c == '=')
            {
                if (++attributes > MaxAttributesPerElement)
                    return $"An element in the plan XML has more than {MaxAttributesPerElement:N0} attributes. A real showplan element has fewer than 50, and the parser slows down sharply as one element grows past that, so the plan is not read.";
            }
            else if (c == '>')
            {
                inTag = false;
            }
        }

        return null;
    }
}
