/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one shape of an undo call on a write tool's answer (#6403): <c>{tool, arguments}</c>, the same shape
/// <c>enable_server</c>/<c>disable_server</c> use. Written once so every write tool that answers with
/// <c>previous</c> and <c>undo</c> builds it the same way.
/// </summary>
internal static class DarlingMcpUndoShape
{
    internal static JsonObject Call(string tool, JsonObject arguments) => new() { ["tool"] = tool, ["arguments"] = arguments };
}
