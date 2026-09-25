/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared response-size target every MCP read tool's DEFAULT arguments are sized against (#4198). #4198
/// measured 12 tools over 50 KB and 3 over 100 KB at default arguments on one server — the worst,
/// <c>get_query_store_regressions</c>, at 211 KB — and Claude Code refused <c>get_fleet_overview</c> (67-81 KB)
/// inline outright, spilling the result to a file instead.
///
/// <para>An earlier pass of this work trimmed any oversized reply after the fact, in one call-tool filter shared
/// by every tool. #4198 ruled that out: a trim after the fact makes the tool's own <c>truncated</c> /
/// <c>*_returned</c> fields wrong, cuts calls that explicitly asked for more rows, and — for anything sorted
/// oldest-first — drops the newest rows instead of the oldest. So this is a SIZE TARGET for each tool's own
/// default <c>limit</c>/<c>top</c> and field widths, not a filter: every tool sizes its own defaults so that a
/// default call stays near this many bytes, the same way <see cref="TrendBuckets.McpPointBudget"/> is the point
/// count trend reads size their own default bucketing against.</para>
/// </summary>
public static class McpResponseBudget
{
    /// <summary>
    /// The size target: 32 KB, roughly 8k tokens at a common 4-bytes-per-token estimate. Each read tool's
    /// default arguments (row limits, field widths) are sized so a default call on a large production store
    /// stays under this — #4198 measured Claude Code refusing three 65,860-80,816 CHARACTER
    /// <c>get_fleet_overview</c> results outright ("result exceeds maximum allowed tokens") and writing them to
    /// files instead.
    /// </summary>
    public const int DefaultBytes = 32 * 1024;

    /// <summary>
    /// <c>get_collection_log</c>'s fleet-wide form (#4199, both products) default row limit, shared so a caller
    /// who omits <c>server_name</c> (or passes <c>"*"</c>) gets the same page size from Darling and Lite. The
    /// per-server form keeps its own default (200) — sizing it is #4198's per-tool pass, out of scope here.
    /// #4198 measured a per-server default call at 85-88 KB for 200 rows (about 435-440 bytes/row); the fleet
    /// form's row is that same shape plus one added <c>server_name</c> field, so a page this size stays under
    /// <see cref="DefaultBytes"/> with room for the envelope fields around the array. See the fleet-form
    /// byte-budget tests in Darling.Tests / Lite.Tests for the measured number this was set from.
    /// </summary>
    public const int CollectionLogFleetDefaultLimit = 60;
}
