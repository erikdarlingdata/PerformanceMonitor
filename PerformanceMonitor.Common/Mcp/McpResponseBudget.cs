/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared response-size budget every MCP read tool's DEFAULT call is sized to stay under (#4198): one
/// server, default arguments, still under one shared ceiling, so a client never has to spill a reply to disk
/// or refuse it outright. <see cref="McpReadToolBudgetLiveTests"/> (Darling) and its Lite twin pin every read
/// tool against this constant; #4205 sizes the two central-store-only reads it names directly.
///
/// <para>NOTE for whoever merges this beside #4205: that PR adds a file at this same path
/// (<c>PerformanceMonitor.Common/Mcp/McpResponseBudget.cs</c>) with a differently-named constant
/// (<c>DefaultMaxBytes</c>) and a runtime call-tool-filter trimmer. The two need to be reconciled by hand;
/// this PR's own pin only reads the tools' raw method output (below any filter layer), so it does not depend
/// on which shape wins.</para>
/// </summary>
public static class McpResponseBudget
{
    /// <summary>32 KB (~8k tokens): what one MCP read tool's DEFAULT call may weigh, UTF-8, on a busy store.</summary>
    public const int DefaultBytes = 32 * 1024;
}
