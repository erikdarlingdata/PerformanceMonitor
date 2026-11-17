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
/// The parse is English only: the message text is localised by the instance, so a line in another language is
/// reported as not parsed rather than guessed at.
/// </summary>
public static class CheckDbErrorLogLine
{
    public const string PhysicalOnly = "physical_only";
    public const string Full = "full";

    /// <summary>Added to the options when the check ran with NOINDEX, which skips the nonclustered indexes.</summary>
    public const string NoIndex = "noindex";

    private const string Prefix = "DBCC CHECKDB (";
    private const string WithMarker = ") WITH ";
    private const string ExecutedByMarker = " executed by ";
    private const string AbnormalEnd = " terminated abnormally due to error state ";

    private static readonly Regex FoundErrors = new(
        @"found (\d+) errors", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The databases among <paramref name="names"/> a line is about. #8865: the name follows <c>DBCC CHECKDB (</c> and ends at the
    /// <c>)</c> that closes it, or at <c>, </c> when the check ran with a second argument inside the parentheses (<c>NOINDEX</c> or a
    /// repair option: <c>DBCC CHECKDB (db, noindex) WITH ...</c>). A name may itself hold either, so every such end is tried against the
    /// names, compared exactly. A line of message 5235 (the check ended abnormally) is about no database: it says nothing about the
    /// check's result and must not shadow an earlier good line.
    /// </summary>
    public static IEnumerable<string> DatabaseNames(string text, IReadOnlySet<string> names)
    {
        ArgumentNullException.ThrowIfNull(names);
        if (string.IsNullOrEmpty(text) || !text.StartsWith(Prefix, StringComparison.Ordinal)
            || text.Contains(AbnormalEnd, StringComparison.Ordinal))
        {
            yield break;
        }

        var executedBy = text.IndexOf(ExecutedByMarker, StringComparison.Ordinal);
        var end = executedBy > 0 ? executedBy : text.Length;
        for (var i = Prefix.Length; i < end; i++)
        {
            if (text[i] == ')' || (text[i] == ',' && i + 1 < end && text[i + 1] == ' '))
            {
                var name = text.Substring(Prefix.Length, i - Prefix.Length);
                if (names.Contains(name))
                {
                    yield return name;
                }
            }
        }
    }

    /// <summary>
    /// <paramref name="options"/> is <c>physical_only</c> or <c>full</c>, then <c>,data_purity</c>, <c>,extended_logical_checks</c>
    /// and <c>,noindex</c> in that order when present. Returns false when the error count cannot be read. #8865: NOINDEX is given
    /// inside the parentheses, after the name (<c>DBCC CHECKDB (db, noindex)</c>), not after <c>WITH</c>, and it is reported because a
    /// check that skipped the nonclustered indexes is a lesser check; a repair option there changes nothing about what was checked
    /// and is not reported.
    /// </summary>
    public static bool TryParse(string text, out string options, out int errors)
    {
        options = Full;
        errors = 0;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var match = FoundErrors.Match(text);
        if (!match.Success
            || !int.TryParse(match.Groups[1].Value, NumberStyles.None, CultureInfo.InvariantCulture, out errors))
        {
            errors = 0;
            return false;
        }

        // The database name may itself contain ") WITH ", so the options start at the last marker before "executed by".
        var executedBy = text.IndexOf(ExecutedByMarker, StringComparison.Ordinal);
        var optionText = string.Empty;
        var noIndex = false;
        if (executedBy > 0)
        {
            var with = text.LastIndexOf(WithMarker, executedBy, StringComparison.Ordinal);
            if (with >= 0)
            {
                var start = with + WithMarker.Length;
                optionText = text.Substring(start, executedBy - start);
            }

            // The parenthesised part closes at the WITH marker, or right before "executed by" when there is no WITH.
            var close = with >= 0 ? with : executedBy - 1;
            noIndex = close >= 0 && text[close] == ')'
                && text.AsSpan(0, close).EndsWith(", " + NoIndex, StringComparison.OrdinalIgnoreCase);
        }

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
