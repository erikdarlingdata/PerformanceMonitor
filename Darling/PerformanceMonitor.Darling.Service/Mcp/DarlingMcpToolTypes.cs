/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP tool classes this service registers: every class in this assembly that carries <see cref="McpServerToolTypeAttribute"/>, in
/// ordinal order of the full type name. The host registers them all (<see cref="WithDarlingTools"/>), so a new tool class is served by
/// carrying the attribute and needs no line in the host. Which tools a caller is served is the write policy's decision
/// (<see cref="DarlingMcpWritePolicy"/>), made by tool name after registration.
/// </summary>
internal static class DarlingMcpToolTypes
{
    private static readonly Lazy<IReadOnlyList<Type>> AllTypes = new(
        () => typeof(DarlingMcpToolTypes).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly Lazy<IReadOnlyList<(string Name, MethodInfo Method)>> AllTools = new(
        () => AllTypes.Value
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
            .Where(x => x.Attribute is not null)
            .Select(x => (x.Attribute!.Name ?? x.Method.Name, x.Method))
            .OrderBy(x => x.Item1, StringComparer.Ordinal)
            .ToList(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>Every tool class, in ordinal order of the full type name.</summary>
    internal static IReadOnlyList<Type> All => AllTypes.Value;

    /// <summary>Every registered tool by its served name, with the method that implements it, in ordinal order of the name.</summary>
    internal static IReadOnlyList<(string Name, MethodInfo Method)> Tools => AllTools.Value;

    /// <summary>Registers every tool class in <see cref="All"/> with <see cref="McpSchemaCompat.WithGeminiCompatibleTools(IMcpServerBuilder, Type)"/>.</summary>
    internal static IMcpServerBuilder WithDarlingTools(this IMcpServerBuilder builder)
    {
        foreach (var type in All)
        {
            builder.WithGeminiCompatibleTools(type);
        }

        return builder;
    }
}
