/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The call-tool filter that holds every MCP tool call until the store's migrations are done (#9577). The first start after an upgrade
/// serves MCP requests while the worker is still applying the ladder, and a tool whose read names a column a rung has not added yet
/// failed with the store's own error. While the store is not ready the filter answers a refusal in the shape the collector schedule tools
/// use (<c>status</c> refused, <c>reason</c> store_not_ready) and never calls the tool, so no read reaches the store.
///
/// <para><b>One filter, every tool.</b> It is registered once, right after the <c>/lean</c> filter, so a <c>call_tool</c> call has already
/// been rewritten to its inner tool and is held by the inner tool's name. It sits outside the argument guard, the latency record and the
/// database-name filter, the last of which reads the store's database lists.</para>
///
/// <para><b>The signal.</b> <see cref="StoreHealthWarmGate"/>, the moment the worker's migrations are done, which is the same moment the
/// store table health passes wait on. <see cref="StoreReadyFor"/> says which processes never wait: a front door (it holds no store and
/// forwards each call to one), a host built without the process services (no worker runs beside it, so nothing would ever mark the gate),
/// and a process whose worker stood down for good (it publishes <see cref="CollectorRuntimeState.CollectorPhase.Stopped"/> and never
/// migrates, so waiting changes nothing and a call gets the store's own answer, as it did before the gate).</para>
///
/// <para><b>Tools that read no store.</b> <c>find_tools</c> searches the served tool set and <c>get_tool_guide</c> serves the reading
/// guides. Neither opens a connection, so they answer during an upgrade.</para>
/// </summary>
internal static class McpStoreReadyFilter
{
    /// <summary>The refusal's <c>reason</c>, the word <see cref="DarlingMcpCommandQueue"/> already uses for a store that cannot take the call yet.</summary>
    internal const string Reason = "store_not_ready";

    /// <summary>
    /// The two discovery tools, which never open a store connection. The list is deliberately limited to them: another tool that
    /// could run without the store (<c>analyze_plan_xml</c> reads only the caller's XML) waits out the migration like the rest, so a
    /// tool that later gains a store read cannot pass the hold unnoticed.
    /// </summary>
    internal static readonly IReadOnlySet<string> StorelessTools = new HashSet<string>(StringComparer.Ordinal) { "find_tools", "get_tool_guide" };

    /// <summary>Whether this process's MCP calls may reach the store now. Read on every call, so a start that finishes its migrations opens the gate at once.</summary>
    internal static Func<bool> StoreReadyFor(bool isFrontDoor, CollectorRuntimeState? collector, StoreHealthWarmGate gate)
    {
        ArgumentNullException.ThrowIfNull(gate);
        if (isFrontDoor || collector is null)
        {
            return static () => true;
        }

        return () => gate.IsStoreReady || collector.Read() is { Phase: CollectorRuntimeState.CollectorPhase.Stopped };
    }

    /// <summary>The filter over <paramref name="storeReady"/>: a call passes when it is true or the tool reads no store, and is refused otherwise.</summary>
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> Over(Func<bool> storeReady)
    {
        ArgumentNullException.ThrowIfNull(storeReady);
        return next => async (request, cancellationToken) =>
        {
            var name = request.Params?.Name;
            if (name is null || StorelessTools.Contains(name) || storeReady())
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            return NotReady(name);
        };
    }

    /// <summary>The answer for a call held back. A tool that writes is held too, so the message says nothing was run.</summary>
    internal static CallToolResult NotReady(string tool) => new()
    {
        IsError = true,
        Content = [new TextContentBlock { Text = NotReadyJson(tool) }],
    };

    internal static string NotReadyJson(string tool) =>
        new JsonObject
        {
            ["status"] = "refused",
            ["tool"] = tool,
            ["reason"] = Reason,
            ["message"] = "The service has not finished starting its store (reaching it and applying any schema upgrade), so this call was not run. Nothing was changed. Retry in a few seconds. If this answer persists, the service log says what the start is waiting on.",
            ["changed"] = false,
        }.ToJsonString();
}
