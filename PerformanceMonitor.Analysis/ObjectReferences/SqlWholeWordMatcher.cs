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
