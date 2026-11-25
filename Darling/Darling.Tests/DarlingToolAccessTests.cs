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
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every method in the service assembly that carries <c>[McpServerTool]</c>, registered or not, declares exactly one
/// <see cref="DarlingToolAccessAttribute"/>, and the declarations agree with the write policy's sets both ways: the Write-marked tools are
/// exactly <see cref="DarlingMcpWritePolicy.WriteTools"/> and the Investigation-marked tools are exactly
/// <see cref="DarlingMcpWritePolicy.InvestigationTools"/>. The write policy is a denylist (<see cref="DarlingMcpWritePolicy.IsWrite"/>), so a
/// new write that nobody added to the set would be served under the default policy with <c>readOnlyHint</c>; the attribute makes the decision
/// explicit on every tool and these tests hold it to the set.
/// </summary>
public sealed class DarlingToolAccessTests
{
    /// <summary>Every tool method in the assembly, found by walking every type rather than the registry, so a method on a class the host does not register is checked too.</summary>
    private static List<(string Name, MethodInfo Method)> AssemblyToolMethods() => typeof(DarlingMcpToolTypes).Assembly.GetTypes()
        .SelectMany(t => t.GetMethods(DarlingMcpToolClassification.AnyMethod))
        .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
        .Where(x => x.Attribute is not null)
        .Select(x => (x.Attribute!.Name ?? x.Method.Name, x.Method))
        .ToList();

    private static string Describe((string Name, MethodInfo Method) tool) => $"{tool.Name} ({tool.Method.DeclaringType!.Name}.{tool.Method.Name})";

    private static string[] Sorted(IEnumerable<string> names) => names.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    private static HashSet<string> MarkedAs(ToolAccess access) => AssemblyToolMethods()
        .Where(t => DarlingMcpToolClassification.AccessOf(t.Method) == access)
        .Select(t => t.Name)
        .ToHashSet(StringComparer.Ordinal);

    [Fact]
    public void EveryToolMethod_CarriesExactlyOneAccess()
    {
        var tools = AssemblyToolMethods();
        Assert.True(tools.Count > 200, $"the assembly walk found only {tools.Count} tool methods");

        var unmarked = Sorted(tools
            .Where(t => t.Method.GetCustomAttributes<DarlingToolAccessAttribute>(inherit: false).Count() != 1)
            .Select(Describe));

        Assert.True(
            unmarked.Length == 0,
            "These tool methods do not carry exactly one [DarlingToolAccess(ToolAccess.Read|Write|Investigation)] above their [McpServerTool]. "
            + $"Decide what the call does, and for a Write or Investigation also add the name to the write policy's set: [{string.Join(", ", unmarked)}]");
    }

    [Fact]
    public void EveryToolMethod_IsRegistered_AndNoNameIsServedTwice()
    {
        var tools = AssemblyToolMethods();
        var registered = DarlingMcpToolTypes.Tools.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

        var unregistered = Sorted(tools.Where(t => !registered.Contains(t.Name)).Select(Describe));
        var twice = Sorted(tools.GroupBy(t => t.Name, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key));

        Assert.True(
            unregistered.Length == 0,
            $"These tool methods are on a class the host does not register (the class needs [McpServerToolType]): [{string.Join(", ", unregistered)}]");
        Assert.True(twice.Length == 0, $"These tool names are declared by more than one method: [{string.Join(", ", twice)}]");
        Assert.Equal(tools.Count, registered.Count);
    }

    [Fact]
    public void TheWriteMarkedTools_AreExactlyTheWriteSet()
    {
        var marked = MarkedAs(ToolAccess.Write);

        var unlisted = Sorted(marked.Except(DarlingMcpWritePolicy.WriteTools));
        var unmarked = Sorted(DarlingMcpWritePolicy.WriteTools.Except(marked));

        Assert.True(
            unlisted.Length == 0,
            $"These tools are marked ToolAccess.Write and are not in DarlingMcpWritePolicy.WriteTools, so the default policy would serve them: [{string.Join(", ", unlisted)}]");
        Assert.True(
            unmarked.Length == 0,
            "These names are in DarlingMcpWritePolicy.WriteTools and no tool method is marked ToolAccess.Write for them "
            + $"(a read marked Read, or a name that is no tool): [{string.Join(", ", unmarked)}]");
    }

    [Fact]
    public void TheInvestigationMarkedTools_AreExactlyTheInvestigationSet()
    {
        var marked = MarkedAs(ToolAccess.Investigation);

        var unlisted = Sorted(marked.Except(DarlingMcpWritePolicy.InvestigationTools));
        var unmarked = Sorted(DarlingMcpWritePolicy.InvestigationTools.Except(marked));

        Assert.True(
            unlisted.Length == 0,
            $"These tools are marked ToolAccess.Investigation and are not in DarlingMcpWritePolicy.InvestigationTools: [{string.Join(", ", unlisted)}]");
        Assert.True(
            unmarked.Length == 0,
            "These names are in DarlingMcpWritePolicy.InvestigationTools and no tool method is marked ToolAccess.Investigation for them "
            + $"(a tool marked another way, or a name that is no tool): [{string.Join(", ", unmarked)}]");
    }

    [Fact]
    public void TheReadOnlyList_IsTheReadMarkedTools_AndNothingTheWritePolicyGates()
    {
        var readMarked = MarkedAs(ToolAccess.Read);

        Assert.True(readMarked.SetEquals(DarlingMcpToolClassification.ReadOnlyTools), "the read-only list is not the Read-marked registered tools");
        Assert.DoesNotContain(readMarked, DarlingMcpWritePolicy.IsWrite);
        Assert.DoesNotContain(readMarked, DarlingMcpWritePolicy.IsInvestigation);
        Assert.Contains("analyze_server", readMarked);
    }
}
