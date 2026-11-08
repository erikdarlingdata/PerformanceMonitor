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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The words a recommendation card shows for a finding that has no headline and so is named by its fact token. A resource
/// monitor notification reads as what it means ("Low physical memory"); a wait type keeps its name, since that is what a
/// reader looks up, and says that it is a wait ("SOS_SCHEDULER_YIELD waits"). A headline that is already prose is unchanged.
/// Lite's recommendation cards read the same words from their own copy of this class.
/// </summary>
internal static class RecommendationTitleText
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

        if (ResourceNotifications.TryGetValue(title, out var words))
        {
            return words;
        }

        if (title.StartsWith("RESOURCE_", StringComparison.Ordinal))
        {
            var rest = string.Join(' ', title["RESOURCE_".Length..].Split('_', StringSplitOptions.RemoveEmptyEntries).Select(w => w.ToLowerInvariant()));
            return rest.Length == 0 ? title : char.ToUpperInvariant(rest[0]) + rest[1..];
        }

        return title + " waits";
    }

    /// <summary>A fact token: letters in capitals, digits and underscores only, four characters or more.</summary>
    private static bool IsToken(string text) =>
        text.Length >= 4 && text.All(c => char.IsUpper(c) || char.IsDigit(c) || c == '_') && text.Any(char.IsLetter);
}
