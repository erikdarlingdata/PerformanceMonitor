/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// A single call-tool filter that applies <see cref="McpResponseBudget"/> to every tool's result — #4198's
/// "one shared default budget every read tool applies", covering every tool with NO per-tool changes. Shared
/// between Darling and Lite (registered once in each host: <c>DarlingMcpHostService</c> and
/// <c>PerformanceMonitorLite.Mcp.McpHostService</c>) so the two products cannot drift on what the budget is or
/// when it applies — the same reason <c>McpHelpers</c> lives here instead of being copied twice.
///
/// <para>Applies unconditionally to every tool, read and write alike, following the same reasoning
/// Darling's <c>GcfCallToolFilter</c> already uses for its own always-on transform: a write tool's response is
/// a small confirmation that is never within reach of the budget, so this costs those calls nothing.</para>
///
/// <para><b>Ordering, where Darling also registers <c>GcfCallToolFilter</c>:</b> this filter MUST be
/// registered BEFORE it (earlier in the <c>AddCallToolFilter</c> chain), so the budget trims plain JSON before
/// GCF re-encodes it — GCF's re-encode is conservative (never larger) but operates on whatever text it is
/// handed, and re-encoding an untrimmed payload would carry the untrimmed size straight through.</para>
/// </summary>
public static class McpResponseBudgetCallToolFilter
{
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) => Transform(await next(request, cancellationToken));

    /// <summary>Applies the budget to a single lone text-content result, matching GCF's own guard: a
    /// multi-block or image result is left untouched rather than risk dropping a block this filter does not
    /// understand. Exposed for testing.</summary>
    public static CallToolResult Transform(CallToolResult result)
    {
        if (result.Content == null || result.IsError == true)
            return result;

        if (result.Content.Count != 1 || result.Content[0] is not TextContentBlock text)
            return result;

        var (budgeted, truncated) = McpResponseBudget.Apply(text.Text);
        if (!truncated)
            return result;

        return new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = budgeted } },
            StructuredContent = result.StructuredContent,
            IsError = result.IsError,
            Meta = result.Meta,
        };
    }
}
