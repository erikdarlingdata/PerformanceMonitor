/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The fixed server-tag colour palette (#2008 stage 2a) as the WPF surfaces know it. The palette and the
/// tag → colour rule live in <see cref="TagColours"/> (Common), shared with the service and the MCP tools;
/// this class forwards each member so the viewer's call sites and the existing pins are unchanged.
/// </summary>
public static class TagColorPalette
{
    /// <summary>The palette, as <c>#RRGGBB</c> strings. See <see cref="TagColours.Swatches"/>.</summary>
    public static IReadOnlyList<string> Swatches => TagColours.Swatches;

    /// <summary>The palette colour a newly-created tag is assigned. See <see cref="TagColours.ForTagId"/>.</summary>
    public static string ForTagId(int id) => TagColours.ForTagId(id);

    /// <summary>Whether a stored colour is a <c>#RRGGBB</c> hex (null/empty is valid). See
    /// <see cref="TagColours.IsValidStoredColour"/>.</summary>
    public static bool IsValidStoredColour(string? colour) => TagColours.IsValidStoredColour(colour);
}
