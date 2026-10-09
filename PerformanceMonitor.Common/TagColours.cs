/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The fixed server-tag colour palette (#2008 stage 2a) and the deterministic tag → colour assignment.
/// Pure and WPF-free (hex strings only; each surface turns a hex into its own brush), so the viewer, the
/// service and the MCP tools share one copy. The swatches are mid-tone saturated colours chosen to read under
/// a constant light pill label (#E4E6EB) in BOTH the light and dark viewer themes, so a tag's pill looks the
/// same whichever theme is active — that is the "theme-safe" requirement, and it is why the palette is a
/// fixed set rather than theme-keyed brushes (which would have to diverge per theme and trip the parity guard).
/// <para><see cref="ForTagId"/> rotates by tag id, NOT randomly: re-creating the same tags in the same order
/// yields the same colours, and a tag keeps its colour for life because its id never changes.</para>
/// </summary>
public static class TagColours
{
    /// <summary>The palette, as <c>#RRGGBB</c> strings. Order is the rotation order for <see cref="ForTagId"/>
    /// and the order the colour picker presents the swatches.</summary>
    public static readonly IReadOnlyList<string> Swatches = new[]
    {
        "#B5484A", // red
        "#BE6C3C", // orange
        "#8F7A2E", // gold
        "#4B8B57", // green
        "#2F8080", // teal
        "#416FA6", // blue
        "#7C5299", // purple
        "#A85286", // magenta
    };

    /// <summary>
    /// The palette colour a newly-created tag is assigned, rotated by its id so the choice is stable and
    /// reproducible (id 1 → the second swatch, wrapping by the palette length). Ids are the store's positive
    /// IDENTITY values; the modulo is written to stay in range for any non-negative id.
    /// </summary>
    public static string ForTagId(int id)
    {
        var count = Swatches.Count;
        var index = ((id % count) + count) % count;
        return Swatches[index];
    }

    /// <summary>
    /// Whether <paramref name="colour"/> is a value this palette layer would store — a 7-char <c>#RRGGBB</c>
    /// hex. Null/empty is valid too (the "no colour, neutral pill" state); this rejects only malformed text,
    /// so a hand-edited store value can't reach a brush parser as garbage.
    /// </summary>
    public static bool IsValidStoredColour(string? colour)
    {
        if (string.IsNullOrEmpty(colour))
        {
            return true;
        }

        if (colour.Length != 7 || colour[0] != '#')
        {
            return false;
        }

        for (var i = 1; i < colour.Length; i++)
        {
            if (!Uri.IsHexDigit(colour[i]))
            {
                return false;
            }
        }

        return true;
    }
}
