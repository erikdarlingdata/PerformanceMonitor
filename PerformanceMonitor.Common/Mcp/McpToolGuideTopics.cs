/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The cross-tool reading guides <c>get_tool_guide</c> serves by name (#3898 D1). A topic is guidance that
/// belongs to several tools at once, stated once here instead of once per description. The texts are
/// <c>const</c> so a converted tool's tail can include one verbatim (attribute arguments must be constants),
/// which saves the caller a second call without a second copy of the sentence. Both SKUs serve the same set
/// (D6 lockstep).
/// </summary>
public static class McpToolGuideTopics
{
    /// <summary>The <see cref="SystemHealthEmptyWindows"/> topic's name.</summary>
    public const string SystemHealthEmptyWindowsName = "system_health_empty_windows";

    /// <summary>
    /// What zero rows means for a <c>get_health_parser_*</c> read: the four-rung ladder both SKUs'
    /// <c>EmptyAsync</c> implement (#3541 A12). It was the same 400-character sentence in all nine
    /// descriptions on both SKUs; the heads keep the guardrail (an empty answer is not a clean bill; read the
    /// witness keys) and this carries the rungs.
    /// </summary>
    public const string SystemHealthEmptyWindows =
        "Every get_health_parser_* answer carries source_observed (whether this server's system_health session has EVER " +
        "been read into the store) and last_captured_at (the collector's newest capture), beside status on data and " +
        "empty answers alike. Zero rows is one of four things. (1) Captured and gated out: events of the category were " +
        "stored in the window and the significance gate kept none; healthy, and events_in_window says how many were " +
        "captured. (2) Captured before, not in this window: a quiet window; last_captured_of_type_at says when the last " +
        "one was stored, so move as_of or widen hours_back to reach it. (3) Never recorded, session alive: other " +
        "categories are stored, so the ring buffer is being read and the engine has never recorded one of these; for a " +
        "memory-node OOM or a severe error that is the healthy measurement. Those three answer status empty. (4) Nothing " +
        "of any type, ever: a dead system_health session or a collector that never ran; status unavailable with " +
        "source_observed false. That is not a clean bill of health; it is no evidence either way. An engine with no " +
        "system_health session to read (Azure SQL Database) says so instead.";

    /// <summary>Every topic, in the order the index lists them.</summary>
    public static IReadOnlyList<McpToolGuideTopic> All { get; } =
    [
        new(
            SystemHealthEmptyWindowsName,
            "What an empty get_health_parser_* answer means: four different nothings, one of them no evidence at all.",
            SystemHealthEmptyWindows),
    ];
}
