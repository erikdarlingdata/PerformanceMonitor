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
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7109: the agent never approves. No MCP tool decides a proposal: no tool class calls the press API or the store methods that record a press,
/// approve a proposal or run an approved action, and no tool takes an approver.
/// </summary>
public sealed class ApprovalPressToolSurfaceTests
{
    /// <summary>The methods that decide a proposal. A call to any of them from a tool would let an agent decide one.</summary>
    private static readonly HashSet<string> StoreDecisions = new(StringComparer.Ordinal)
    {
        nameof(InvestigationRequestStore.CheckProposalPressAsync),
        nameof(InvestigationRequestStore.ApproveProposalForRunAsync),
        nameof(InvestigationRequestStore.RunApprovedActionAsync),
        nameof(InvestigationRequestStore.RecordFirstPressAsync),
        nameof(InvestigationRequestStore.ApproveProposalAsync),
    };

    private static bool IsDecision(MethodBase method) =>
        method.DeclaringType == typeof(ActionProposalPress)
        || (method.DeclaringType == typeof(InvestigationRequestStore) && StoreDecisions.Contains(method.Name));

    /// <summary>Every tool class, with its nested types: an async tool's body is a nested state machine, and a lambda is a nested closure.</summary>
    private static IEnumerable<Type> ToolTypes()
    {
        var tools = typeof(DarlingMcpPlanForceTools).Assembly.GetTypes().Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null).ToArray();
        Assert.NotEmpty(tools);
        foreach (var tool in tools)
        {
            var pending = new Stack<Type>();
            pending.Push(tool);
            while (pending.Count > 0)
            {
                var type = pending.Pop();
                yield return type;
                foreach (var nested in type.GetNestedTypes(BindingFlags.Public | BindingFlags.NonPublic))
                    pending.Push(nested);
            }
        }
    }

    /// <summary>The methods a body calls or constructs: each <c>call</c>, <c>callvirt</c> and <c>newobj</c> operand resolved through the module.
    /// A byte that only looks like one of those opcodes resolves to nothing or to an unrelated member, which the check ignores.</summary>
    private static IEnumerable<MethodBase> Callees(MethodBase method)
    {
        var il = method.GetMethodBody()?.GetILAsByteArray();
        if (il is null)
            yield break;
        for (var i = 0; i + 4 < il.Length; i++)
        {
            if (il[i] is not (0x28 or 0x6F or 0x73))
                continue;
            var token = BitConverter.ToInt32(il, i + 1);
            MethodBase? callee = null;
            try
            {
                callee = method.Module.ResolveMethod(token, method.DeclaringType?.GetGenericArguments(),
                    method.IsGenericMethod ? method.GetGenericArguments() : null);
            }
            catch (ArgumentException)
            {
            }
            catch (BadImageFormatException)
            {
            }

            if (callee is not null)
                yield return callee;
        }
    }

    [Fact]
    public void NoMcpToolCallsThePressApi_OrTheStoreMethodsThatDecideAProposal()
    {
        const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly;
        var reached = ToolTypes()
            .SelectMany(t => t.GetMethods(all).Cast<MethodBase>().Concat(t.GetConstructors(all)))
            .SelectMany(m => Callees(m).Where(IsDecision).Select(c => $"{m.DeclaringType!.FullName}.{m.Name} -> {c.DeclaringType!.Name}.{c.Name}"))
            .Distinct()
            .ToArray();
        Assert.Empty(reached);
    }

    [Fact]
    public void NoMcpToolTakesAnApprover_AndThePressApiIsNotAToolClass()
    {
        var tools = typeof(DarlingMcpPlanForceTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToArray();
        Assert.NotEmpty(tools);
        Assert.DoesNotContain(tools, m => m.GetParameters().Any(p => p.ParameterType == typeof(ProposalApprover) || p.ParameterType == typeof(ActionProposalPress)));
        Assert.Null(typeof(ActionProposalPress).GetCustomAttribute<McpServerToolTypeAttribute>());
    }

    /// <summary>The check above must see a call when there is one: the press class's own confirm reaches the store's approval.</summary>
    [Fact]
    public void TheCallScan_SeesTheConfirmReachingTheStoresApproval()
    {
        var confirm = typeof(ActionProposalPress).GetNestedTypes(BindingFlags.NonPublic)
            .Where(t => t.Name.Contains(nameof(ActionProposalPress.ConfirmAsync), StringComparison.Ordinal))
            .SelectMany(t => t.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly));
        Assert.Contains(confirm.SelectMany(Callees), c => c.Name == nameof(InvestigationRequestStore.ApproveProposalForRunAsync));
    }
}
