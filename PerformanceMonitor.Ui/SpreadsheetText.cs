/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Keeps exported and copied text from being read as a spreadsheet formula. Collected text (query text, object
/// and login names, wait resources) can start with <c>=</c>, <c>+</c>, <c>-</c> or <c>@</c>, or with a tab or a
/// carriage return, and a spreadsheet that opens the CSV or receives the pasted cells evaluates such a cell
/// instead of showing it. The fix is the usual one: a text cell that starts with one of those characters gets a
/// leading apostrophe.
///
/// <para>Text only. A number stays a number: a negative number starts with <c>-</c> too, and an apostrophe in
/// front of it would turn a value into text. Numbers are told apart two ways. A typed value that is not a string
/// (an int, a double, a date) is left alone by <see cref="NeutralizeCell"/>. A string that is nothing but a plain
/// number (<c>-5</c>, <c>-1,234.5</c>, <c>-1.5E-05</c>) is left alone by <see cref="Neutralize"/>, because
/// digits, separators and an exponent cannot form a formula.</para>
///
/// <para>Every grid copy, grid CSV export and chart CSV export goes through this one class, so the rule cannot
/// drift between them. It is idempotent: the apostrophe it adds is not itself a trigger character.</para>
/// </summary>
public static class SpreadsheetText
{
    /// <summary>
    /// Returns <paramref name="text"/> with a leading apostrophe when it starts with a tab or a carriage return,
    /// or when its first character that is not white space or a control character is <c>=</c>, <c>+</c>,
    /// <c>-</c> or <c>@</c>, and the text is not a plain number; otherwise returns it unchanged. A spreadsheet
    /// (and the HTML form of a paste) drops leading white space, so the rule looks past it. Null becomes the
    /// empty string.
    /// </summary>
    public static string Neutralize(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        if (text[0] is '\t' or '\r')
        {
            return "'" + text;
        }

        var start = 0;
        while (start < text.Length && IsLeadingSkipped(text[start]))
        {
            start++;
        }

        if (start == text.Length || !IsFormulaStart(text[start]))
        {
            return text;
        }

        var lead = start == 0 ? text : text.Substring(start);
        return IsPlainNumber(lead) ? text : "'" + text;
    }

    /// <summary>White space and control characters that a spreadsheet ignores in front of a cell's text.</summary>
    private static bool IsLeadingSkipped(char c) => char.IsWhiteSpace(c) || char.IsControl(c) || c == '\uFEFF';

    /// <summary>
    /// A text cell as the tab-separated copies (Copy Row, Copy All Rows, the keyboard copy) write it: every run of
    /// tabs and line breaks becomes one space, then the cell goes through <see cref="Neutralize"/>, which looks
    /// past leading white space. A tab or a line
    /// break inside a cell would otherwise start a new cell or a new row when the text is pasted into a
    /// spreadsheet, and the new piece would not carry the leading apostrophe. Mirrors the web grid's Copy row.
    /// </summary>
    public static string NeutralizeClipboardCell(string? text) => Neutralize(CollapseLineBreaks(text));

    /// <summary>
    /// Replaces every run of tabs, carriage returns and line feeds with one space. Text with none of them comes
    /// back as the same instance; null becomes the empty string.
    /// </summary>
    public static string CollapseLineBreaks(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        if (text.AsSpan().IndexOfAny('\t', '\r', '\n') < 0)
        {
            return text;
        }

        var sb = new System.Text.StringBuilder(text.Length);
        var inRun = false;
        foreach (var c in text)
        {
            if (c is '\t' or '\r' or '\n')
            {
                if (!inRun)
                {
                    sb.Append(' ');
                    inRun = true;
                }

                continue;
            }

            inRun = false;
            sb.Append(c);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Neutralizes a typed cell value: a string is passed through <see cref="Neutralize"/>; anything else (a
    /// number, a date, a null) is returned as it is, so a numeric cell is never turned into text.
    /// </summary>
    public static object? NeutralizeCell(object? value) => value is string text ? Neutralize(text) : value;

    private static bool IsFormulaStart(char c) => c is '=' or '+' or '-' or '@';

    /// <summary>
    /// True for an optional sign followed by digits with optional <c>.</c> and <c>,</c> separators and an
    /// optional exponent (<c>e</c> or <c>E</c>, an optional sign, digits), and nothing else.
    /// </summary>
    internal static bool IsPlainNumber(string text)
    {
        var i = 0;
        if (text.Length > 0 && text[0] is '+' or '-')
        {
            i++;
        }

        var digits = 0;
        for (; i < text.Length && (char.IsAsciiDigit(text[i]) || text[i] is '.' or ','); i++)
        {
            if (char.IsAsciiDigit(text[i]))
            {
                digits++;
            }
        }

        if (digits == 0)
        {
            return false;
        }

        if (i < text.Length && text[i] is 'e' or 'E')
        {
            i++;
            if (i < text.Length && text[i] is '+' or '-')
            {
                i++;
            }

            var exponentDigits = 0;
            for (; i < text.Length && char.IsAsciiDigit(text[i]); i++)
            {
                exponentDigits++;
            }

            if (exponentDigits == 0)
            {
                return false;
            }
        }

        return i == text.Length;
    }
}
