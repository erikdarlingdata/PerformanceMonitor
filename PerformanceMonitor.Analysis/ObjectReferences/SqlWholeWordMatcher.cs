/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * Ported from SQLExtended's SearchTextMatcher (MIT, Copyright (c) 2026 James Noonan).
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis.ObjectReferences;

/// <summary>
/// The whole-word text search the view usage read falls back to when a statement does not parse (#8784 part 2).
///
/// <para>Word characters are T-SQL identifier characters: letters, digits, <c>_</c>, <c>@</c>, <c>#</c> and <c>$</c>. So <c>person</c>
/// matches <c>dbo.person</c>, <c>[person]</c> and <c>FROM person WHERE</c>, but not <c>person_id</c>, <c>@person</c> or <c>#person</c>, each
/// of which is a different name. A boundary is only needed on a side where the name itself starts or ends with a word character.</para>
///
/// <para>Comparison ignores case, which can match more than the engine would on a case-sensitive database: the safe way to be wrong here,
/// since a match only ever makes a view look used.</para>
/// </summary>
public static class SqlWholeWordMatcher
{
    /// <summary>Whether <paramref name="c"/> is part of a T-SQL identifier.</summary>
    public static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c is '_' or '@' or '#' or '$';

    /// <summary>Whether every character of <paramref name="term"/> is a word character, so a whole-word match of it is exactly one of
    /// <see cref="Words"/>.</summary>
    public static bool IsWord(string term)
    {
        ArgumentNullException.ThrowIfNull(term);
        if (term.Length == 0)
        {
            return false;
        }

        foreach (var c in term)
        {
            if (!IsWordChar(c))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Every maximal run of word characters in <paramref name="text"/>, compared ignoring case (#8784 review). A term made only of word
    /// characters occurs as a whole word exactly when it is one of these runs, so an unparsed statement is split once, in one pass, and each
    /// view is then one set lookup, where <see cref="Contains"/> per view searched the whole text once per view.
    /// </summary>
    public static IReadOnlySet<string> Words(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var words = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var start = -1;
        for (var i = 0; i <= text.Length; i++)
        {
            var word = i < text.Length && IsWordChar(text[i]);
            if (word && start < 0)
            {
                start = i;
            }
            else if (!word && start >= 0)
            {
                words.Add(text[start..i]);
                start = -1;
            }
        }

        return words;
    }

    /// <summary>Whether <paramref name="term"/> occurs in <paramref name="text"/> as a whole word.</summary>
    public static bool Contains(string? text, string? term) => IndexOf(text, term, 0) >= 0;

    /// <summary>The index of the first whole-word match at or after <paramref name="startIndex"/>, or -1.</summary>
    public static int IndexOf(string? text, string? term, int startIndex)
    {
        if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty(term))
        {
            return -1;
        }

        var checkStart = IsWordChar(term[0]);
        var checkEnd = IsWordChar(term[^1]);

        var index = Math.Max(0, startIndex);
        while (index <= text.Length - term.Length && (index = text.IndexOf(term, index, StringComparison.OrdinalIgnoreCase)) >= 0)
        {
            var end = index + term.Length;
            var startOk = !checkStart || index == 0 || !IsWordChar(text[index - 1]);
            var endOk = !checkEnd || end == text.Length || !IsWordChar(text[end]);
            if (startOk && endOk)
            {
                return index;
            }

            index++;
        }

        return -1;
    }
}
