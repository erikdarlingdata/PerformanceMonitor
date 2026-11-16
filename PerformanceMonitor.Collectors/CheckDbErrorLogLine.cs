/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    private const string WithMarker = ") WITH ";
    private const string ExecutedByMarker = " executed by ";

    private static readonly Regex FoundErrors = new(
        @"found (\d+) errors", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// <paramref name="options"/> is <c>physical_only</c> or <c>full</c>, then <c>,data_purity</c> and/or
    /// <c>,extended_logical_checks</c> in that order when present. Returns false when the error count cannot be read.
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
        if (executedBy > 0)
        {
            var with = text.LastIndexOf(WithMarker, executedBy, StringComparison.Ordinal);
            if (with >= 0)
            {
                var start = with + WithMarker.Length;
                optionText = text.Substring(start, executedBy - start);
            }
        }

        var words = optionText.ToLowerInvariant().Split(
            new[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries);
        var physicalOnly = Array.IndexOf(words, PhysicalOnly) >= 0;
        var dataPurity = Array.IndexOf(words, "data_purity") >= 0;
        var extendedLogical = Array.IndexOf(words, "extended_logical_checks") >= 0;

        options = (physicalOnly ? PhysicalOnly : Full)
            + (dataPurity ? ",data_purity" : string.Empty)
            + (extendedLogical ? ",extended_logical_checks" : string.Empty);
        return true;
    }
}
