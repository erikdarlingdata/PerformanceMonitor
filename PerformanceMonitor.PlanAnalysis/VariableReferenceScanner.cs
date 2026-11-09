/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Finds the <c>@name</c> references in a statement's code, in order, repeats included. A reference is an
/// <c>@</c> followed by one or more word characters: letters, decimal digits, non-spacing marks and connector
/// punctuation, the set .NET's <c>\w</c> matches, so a name with a combining mark or a <c>‿</c> is one token. String literals (<c>'...'</c>, including an
/// <c>N</c>-prefixed one, with <c>''</c> for a quote inside), <c>--</c> comments to the end of the line, nested
/// <c>/* */</c> comments, <c>"quoted"</c> identifiers (<c>""</c> inside) and <c>[bracketed]</c> names (<c>]]</c>
/// inside) are stepped over whole, since an <c>@</c> inside them is text, not a variable. A reference starts only
/// at an <c>@</c> whose next character is a word character, so at a <c>@@</c> function the token found is the name
/// after its second <c>@</c>; each caller keeps its own handling of that token.
/// </summary>
public static class VariableReferenceScanner
{
    public static List<string> Scan(string? text)
    {
        var names = new List<string>();
        if (string.IsNullOrEmpty(text))
            return names;

        var i = 0;
        while (i < text.Length)
        {
            var c = text[i];
            if (c == '\'' || c == '"' || c == '[')
            {
                i = SkipDelimited(text, i, c == '[' ? ']' : c);
            }
            else if (c == '-' && i + 1 < text.Length && text[i + 1] == '-')
            {
                while (i < text.Length && text[i] != '\n' && text[i] != '\r')
                    i++;
            }
            else if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                i = SkipBlockComment(text, i);
            }
            else if (c == '@' && i + 1 < text.Length && IsWordCharacter(text[i + 1]))
            {
                var start = i;
                i += 2;
                while (i < text.Length && IsWordCharacter(text[i]))
                    i++;
                names.Add(text[start..i]);
            }
            else
            {
                i++;
            }
        }

        return names;
    }

    /// <summary>
    /// The index past the delimited run that opens at <paramref name="open"/>, where a doubled closer is one
    /// closer inside the run; a run with no closer reaches the end of the text.
    /// </summary>
    private static int SkipDelimited(string text, int open, char close)
    {
        var i = open + 1;
        while (i < text.Length)
        {
            if (text[i] == close)
            {
                if (i + 1 < text.Length && text[i + 1] == close)
                {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return text.Length;
    }

    /// <summary>
    /// The index past the block comment that opens at <paramref name="open"/>, counting nested openers; a comment
    /// with no closer reaches the end of the text.
    /// </summary>
    private static int SkipBlockComment(string text, int open)
    {
        var depth = 0;
        var i = open;
        while (i < text.Length)
        {
            if (text[i] == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                depth++;
                i += 2;
            }
            else if (text[i] == '*' && i + 1 < text.Length && text[i + 1] == '/')
            {
                depth--;
                i += 2;
                if (depth == 0)
                    return i;
            }
            else
            {
                i++;
            }
        }
        return text.Length;
    }

    private static bool IsWordCharacter(char c) => char.GetUnicodeCategory(c) switch
    {
        UnicodeCategory.UppercaseLetter or UnicodeCategory.LowercaseLetter or UnicodeCategory.TitlecaseLetter
            or UnicodeCategory.ModifierLetter or UnicodeCategory.OtherLetter
            or UnicodeCategory.DecimalDigitNumber or UnicodeCategory.NonSpacingMark or UnicodeCategory.ConnectorPunctuation => true,
        _ => false,
    };
}
