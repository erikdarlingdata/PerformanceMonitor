/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Reads one SQL Server error log line of message 8957 ("DBCC CHECKDB (db) WITH options executed by
/// login found N errors and repaired M errors"). #8865: an Availability Group replica is judged from its own
/// error log, and this line is the only record there of which CHECKDB options ran and how many errors it found.
/// A line is about one database: the longest known name that the log closes, because a name may hold a shorter
/// database's name followed by what the log writes after a name (<c>) WITH </c>, <c>) executed by </c>, <c>, noindex</c>),
/// while the options are engine keywords and never hold <c>)</c>. Only the text after that name is parsed, and the
/// error count is the last <c>found N errors and repaired M errors</c>, since the name and the login can hold the same
/// words. The parse is English only: the message text is localised by the instance, and a CHECKDB completion line in
/// another language (<see cref="IsUnreadableCompletion"/>) is counted as not parsed rather than guessed at.
/// </summary>
public static class CheckDbErrorLogLine
{
    public const string PhysicalOnly = "physical_only";
    public const string Full = "full";

    /// <summary>Added to the options when the check ran with NOINDEX, which skips the nonclustered indexes.</summary>
    public const string NoIndex = "noindex";

    private const string Command = "DBCC CHECKDB";
    private const string Prefix = Command + " (";
    private const string WithMarker = ") WITH ";
    private const string ExecutedByMarker = " executed by ";
    private const string AbnormalEnd = " terminated abnormally due to error state ";

    private static readonly Regex FoundErrors = new(
        @"found (\d+) errors and repaired \d+ errors",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.RightToLeft);

    /// <summary>
    /// The database among <paramref name="names"/> a line is about, or null. #8865: the name follows <c>DBCC CHECKDB (</c> and ends at the
    /// <c>)</c> that closes it, or at <c>, </c> when the check ran with a second argument inside the parentheses (<c>NOINDEX</c> or a
    /// repair option: <c>DBCC CHECKDB (db, noindex) WITH ...</c>). A name may itself hold either, so an end counts only where the log
    /// puts one: a <c>)</c> followed by <c> WITH </c> or <c> executed by </c>, and a <c>, </c> followed by one of those options and
    /// then such a <c>)</c>. Otherwise <c>Sales, Inc</c> would also credit <c>Sales</c> with its check and its error count. When several
    /// known names end that way the longest is the one: <c>Sales) WITH physical_only</c> holds an end after <c>Sales</c>, and a line
    /// about it is not about <c>Sales</c>. Names are compared exactly. A line of message 5235 (the check ended abnormally) is about no
    /// database: it says nothing about the check's result and must not shadow an earlier good line.
    /// </summary>
    public static string? DatabaseName(string text, IReadOnlySet<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);
        if (string.IsNullOrEmpty(text) || !text.StartsWith(Prefix, StringComparison.Ordinal)
            || text.Contains(AbnormalEnd, StringComparison.Ordinal))
        {
            return null;
        }

        string? longest = null;
        for (var i = Prefix.Length; i < text.Length; i++)
        {
            if (ClosesTheName(text, i) || EndsBeforeASecondArgument(text, i))
            {
                var name = text.Substring(Prefix.Length, i - Prefix.Length);
                if (names.Contains(name) && (longest is null || name.Length > longest.Length))
                {
                    longest = name;
                }
            }
        }

        return longest;
    }

    /// <summary>
    /// #8865: a CHECKDB completion line that came back from the log but cannot be read here, which is what an instance whose error log is in
    /// another language writes. Message 8957 keeps the command (<c>DBCC CHECKDB</c>, never translated) and the database in parentheses
    /// after it in every language, but not always at the start: some put a word or the login first, Korean writes no space before the
    /// parenthesis, and Turkish puts the login between the command and the parenthesised name. So the line counts when it holds the
    /// command, then a database of <paramref name="names"/> in parentheses after it (closed by <c>)</c>, or by <c>, </c> before a second
    /// argument), and has neither the English <c> executed by </c> of message 8957 nor the English wording of message 5235. An English
    /// corruption advisory that tells the reader to run DBCC CHECKDB names no database in parentheses after the command, and is not
    /// counted; nor is a line about a database the collector does not list.
    /// </summary>
    public static bool IsUnreadableCompletion(string text, IReadOnlySet<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);
        if (string.IsNullOrEmpty(text)
            || text.Contains(ExecutedByMarker, StringComparison.Ordinal)
            || text.Contains(AbnormalEnd, StringComparison.Ordinal))
        {
            return false;
        }

        var command = text.IndexOf(Command, StringComparison.Ordinal);
        if (command < 0)
        {
            return false;
        }

        for (var open = text.IndexOf('(', command + Command.Length); open >= 0; open = text.IndexOf('(', open + 1))
        {
            for (var end = open + 1; end < text.Length; end++)
            {
                var closes = text[end] == ')' || (text[end] == ',' && end + 1 < text.Length && text[end + 1] == ' ');
                if (closes && names.Contains(text.Substring(open + 1, end - open - 1)))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /* #8865: the arguments CHECKDB takes after the name inside the parentheses. */
    private static readonly string[] SecondArguments = { NoIndex, "repair_allow_data_loss", "repair_fast", "repair_rebuild" };

    /* #8865: the ")" that closes the parentheses is followed by the WITH options or by " executed by "; a ")" inside a name is not. */
    private static bool ClosesTheName(string text, int i) =>
        text[i] == ')'
        && (text.AsSpan(i).StartsWith(WithMarker, StringComparison.Ordinal)
            || text.AsSpan(i + 1).StartsWith(ExecutedByMarker, StringComparison.Ordinal));

    /* #8865: ", " ends the name only when a CHECKDB argument follows it and closes the parentheses: "(Sales, noindex)" is Sales,
       "(Sales, Inc)" is not. */
    private static bool EndsBeforeASecondArgument(string text, int i)
    {
        if (text[i] != ',' || i + 1 >= text.Length || text[i + 1] != ' ')
        {
            return false;
        }

        foreach (var argument in SecondArguments)
        {
            var close = i + 2 + argument.Length;
            if (close < text.Length
                && text.AsSpan(i + 2, argument.Length).Equals(argument, StringComparison.OrdinalIgnoreCase)
                && ClosesTheName(text, close))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Parses the line about <paramref name="database"/> (the name <see cref="DatabaseName"/> gave). <paramref name="options"/> is
    /// <c>physical_only</c> or <c>full</c>, then <c>,data_purity</c>, <c>,extended_logical_checks</c> and <c>,noindex</c> in that order
    /// when present. Returns false when the line is not about that name or the error count cannot be read. #8865: only the text after
    /// <c>DBCC CHECKDB (</c> and the name is read, so nothing in the name can pass for an option or a count. NOINDEX is given inside the
    /// parentheses, after the name (<c>DBCC CHECKDB (db, noindex)</c>), not after <c>WITH</c>, and it is reported because a check that
    /// skipped the nonclustered indexes is a lesser check; a repair option there changes nothing about what was checked and is not reported.
    /// </summary>
    public static bool TryParse(string text, string database, out string options, out int errors)
    {
        options = Full;
        errors = 0;
        if (string.IsNullOrEmpty(text) || database is null || !text.StartsWith(Prefix + database, StringComparison.Ordinal))
        {
            return false;
        }

        var rest = text.Substring(Prefix.Length + database.Length);
        var noIndex = false;
        if (rest.StartsWith(", ", StringComparison.Ordinal))
        {
            var close = rest.IndexOf(')', StringComparison.Ordinal);
            if (close < 0)
            {
                return false;
            }

            noIndex = rest.AsSpan(2, close - 2).Equals(NoIndex, StringComparison.OrdinalIgnoreCase);
            rest = rest.Substring(close);
        }

        /* The options are engine keywords, so the first " executed by " after the closing ")" is the log's own. */
        var executedBy = rest.IndexOf(ExecutedByMarker, StringComparison.Ordinal);
        if (!rest.StartsWith(')') || executedBy < 0)
        {
            return false;
        }

        var tail = rest.Substring(executedBy);
        var match = FoundErrors.Match(tail);
        if (!match.Success
            || !int.TryParse(match.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out errors))
        {
            errors = 0;
            return false;
        }

        var optionText = rest.StartsWith(WithMarker, StringComparison.Ordinal) && executedBy > WithMarker.Length
            ? rest.Substring(WithMarker.Length, executedBy - WithMarker.Length)
            : string.Empty;
        var words = optionText.ToLowerInvariant().Split(
            new[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries);
        var physicalOnly = Array.IndexOf(words, PhysicalOnly) >= 0;
        var dataPurity = Array.IndexOf(words, "data_purity") >= 0;
        var extendedLogical = Array.IndexOf(words, "extended_logical_checks") >= 0;

        options = (physicalOnly ? PhysicalOnly : Full)
            + (dataPurity ? ",data_purity" : string.Empty)
            + (extendedLogical ? ",extended_logical_checks" : string.Empty)
            + (noIndex ? "," + NoIndex : string.Empty);
        return true;
    }
}
