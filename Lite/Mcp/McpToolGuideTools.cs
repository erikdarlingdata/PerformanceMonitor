/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// <c>get_tool_guide</c> (#3898 D1): the long-form reading guides that <c>tools/list</c> leaves out. A converted
/// tool's description is split at <see cref="McpToolGuide.Marker"/> where every tool is registered
/// (<c>McpSchemaCompat.WithGeminiCompatibleTools</c>); this serves the tails and the named cross-tool topics.
/// Darling's <c>DarlingMcpToolGuideTools</c> is the twin, same name, same description, same topics (D6
/// lockstep). It reads no collected data and no monitored server: the catalog is built at registration from the
/// tools this host serves.
/// </summary>
[McpServerToolType]
public sealed class McpToolGuideTools
{
    [McpServerTool(Name = "get_tool_guide"), Description("Returns the reading guides tools/list leaves out. A tool whose description ends \"Reading guide: get_tool_guide.\" has one: pass its name in tools for what its fields mean and its edge cases. Pass topic names in topics for guides that span tools. Call with neither to list the tools with guides and the topics. Unknown names are reported in the answer, not as errors; an answer over its size budget lists the rest under deferred. Read-only: reads no collected data and no monitored server.")]
    public static string GetToolGuide(
        McpToolGuideCatalog catalog,
        [Description(McpToolGuideToolText.ToolsParameter)] string[]? tools = null,
        [Description(McpToolGuideToolText.TopicsParameter)] string[]? topics = null)
        => McpToolGuide.Render(catalog, McpToolGuideTopics.All, tools, topics);
}
