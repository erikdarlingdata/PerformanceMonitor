/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// The front door class of the MCP tool this method implements (#6840), set beside its <c>[McpServerTool]</c> attribute. A tool that takes
/// <c>server_name</c> needs none, and neither does a write, an investigation tool or a server admin tool: the write policy's sets put those
/// in the store-required class. Every other tool carries one, and <see cref="DarlingFrontDoorRouting"/> reads them all once.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
internal sealed class FrontDoorClassAttribute : Attribute
{
    internal FrontDoorClassAttribute(FrontDoorToolClass toolClass) => ToolClass = toolClass;

    /// <summary>How the front door sends a call to the tool that names no server and no store.</summary>
    internal FrontDoorToolClass ToolClass { get; }
}
