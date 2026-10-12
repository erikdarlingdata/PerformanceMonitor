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
/// Single source of truth for the server-search match rule, shared by Lite's Overview and the Darling
/// viewer's sidebar tree so both surfaces filter identically (the web viewer mirrors the same rule in
/// JavaScript). Pure and deterministic. Parameterised by the caller's own fields rather than a shared
/// row type — the viewer matches display name, instance name AND a server's tag names; Lite matches
/// name only — exactly like <see cref="ServerOverviewSort"/> takes selectors instead of an interface.
/// </summary>
public static class ServerOverviewFilter
{
    /// <summary>
    /// The line both apps show when a search matched no Overview card (#5352). One sentence in one place so
    /// Lite and the viewer cannot drift, and so the viewer's "all N servers are healthy" all-clear is never
    /// the thing on screen when the search is what emptied the grid.
    /// </summary>
    public const string NoMatchText = "No server matches the search.";

    /// <summary>
    /// Normalises a raw search-box value: trimmed, with empty/whitespace collapsed to null — the "no
    /// filter, everything matches" sentinel. Callers store the result so an empty box is cheap (no
    /// per-item work) and never accidentally filters everything out.
    /// </summary>
    public static string? Normalize(string? query) =>
        string.IsNullOrWhiteSpace(query) ? null : query.Trim();

    /// <summary>
    /// True when <paramref name="query"/> matches: an empty/whitespace query matches EVERYTHING, otherwise
    /// a case-insensitive substring match against ANY of the supplied fields (null/empty fields skipped).
    /// The query is trimmed here too, so passing a raw box value or a <see cref="Normalize"/>d one behaves
    /// the same. Substring rather than prefix, so "prod" finds "sql-prod-01".
    /// </summary>
    public static bool Matches(string? query, params string?[] fields)
    {
        var q = query?.Trim();
        if (string.IsNullOrEmpty(q))
        {
            return true;
        }

        if (fields is null)
        {
            return false;
        }

        foreach (var field in fields)
        {
            if (!string.IsNullOrEmpty(field) &&
                field.Contains(q, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
