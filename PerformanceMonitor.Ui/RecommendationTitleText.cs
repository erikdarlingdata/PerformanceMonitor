/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The words a recommendation card shows for a finding that has no headline and so is named by its fact token. A wait type keeps its
/// name, since that is what a reader looks up, and says that it is a wait ("SOS_SCHEDULER_YIELD waits"); a wait is one the wait
/// drill-down classifies (<see cref="WaitDrillDownHelper.Classify"/>), so a key that merely looks like one ("CONFIG_MAXDOP_UNSET") reads as
/// its words. A resource monitor notification reads as what it means ("Low physical memory"). A headline that is already prose is
/// unchanged. Lite's and the Darling Viewer's recommendation cards both read their titles through this.
/// </summary>
public static class RecommendationTitleText
{
    private static readonly Dictionary<string, string> ResourceNotifications = new(StringComparer.Ordinal)
    {
        ["RESOURCE_MEMPHYSICAL_LOW"] = "Low physical memory",
        ["RESOURCE_MEMPHYSICAL_HIGH"] = "Physical memory recovered",
        ["RESOURCE_MEMVIRTUAL_LOW"] = "Low virtual memory",
        ["RESOURCE_MEM_STEADY"] = "Memory steady",
    };

    public static string Readable(string title)
    {
        if (string.IsNullOrWhiteSpace(title) || !IsToken(title))
        {
            return title;
        }

        if (IsWaitType(title))
        {
            return title + " waits";
        }

        if (ResourceNotifications.TryGetValue(title, out var words))
        {
            return words;
        }

        var rest = title.StartsWith("RESOURCE_", StringComparison.Ordinal) ? title["RESOURCE_".Length..] : title;
        var sentence = string.Join(' ', rest.Split('_', StringSplitOptions.RemoveEmptyEntries).Select(w => w.ToLowerInvariant()));
        return sentence.Length == 0 ? title : char.ToUpperInvariant(sentence[0]) + sentence[1..];
    }

    /// <summary>A wait type the wait drill-down knows how to follow; every other token is a fact key that is not a wait.</summary>
    private static bool IsWaitType(string token) => WaitDrillDownHelper.Classify(token).Category != WaitDrillDownHelper.WaitCategory.Filtered;

    /// <summary>A fact token: letters in capitals, digits and underscores only, four characters or more.</summary>
    private static bool IsToken(string text) =>
        text.Length >= 4 && text.All(c => char.IsUpper(c) || char.IsDigit(c) || c == '_') && text.Any(char.IsLetter);
}
