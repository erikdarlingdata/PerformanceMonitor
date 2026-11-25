/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The <c>/lean</c> URL path (#9388): a short <c>tools/list</c> for clients that load every listed tool into the model's context.
/// It lists the four entry tools, <c>find_tools</c> and <c>call_tool</c>. The session still SERVES the caller's whole tool set
/// (after the token and write policy), so <c>find_tools</c> can search every tool the caller can call, a direct <c>tools/call</c> of an
/// unlisted tool works as it does on <c>/</c> (the same token can call it there, so this path adds no reach), and
/// <c>call_tool</c> can dispatch.
///
/// <para><c>call_tool</c> is served on <c>/lean</c> only. It runs no tool itself. <see cref="CallToolFilter"/>, the OUTERMOST call-tool
/// filter, checks the rules, rewrites the request to the inner tool's name, arguments and matched tool, and calls next, so every later
/// filter (the unknown-argument guard, the latency record, GCF, the database-name filter, the data-detail mask, the statement filter)
/// and the SDK's dispatch see a direct call. A handler that called the inner tool's own <c>InvokeAsync</c> would skip them all.</para>
/// </summary>
internal static class DarlingLeanToolProfile
{
    internal const string CallToolName = "call_tool";

    /// <summary>The search tool part A of #9388 registers on Darling and Lite, named here by its string.</summary>
    internal const string FindToolsName = "find_tools";

    /// <summary>The tools <c>/lean</c>'s <c>tools/list</c> offers, before the intersection with what the caller is served.</summary>
    internal static readonly IReadOnlySet<string> ListedTools = new HashSet<string>(ListedToolNames(), StringComparer.Ordinal);

    private static string[] ListedToolNames() => [.. DarlingCoreToolProfile.EntryTools, FindToolsName, CallToolName];

    internal static bool IsLeanPath(PathString path) => path.StartsWithSegments("/lean");

    /// <summary>The note a <c>/lean</c> session's instructions lead with.</summary>
    internal static readonly string LeanNote =
        "## This is the /lean endpoint\n\nIt lists a few tools: " + NamesInSentence(ListedToolNames()) +
        ". Use find_tools to find any other tool by what it does. Use call_tool with the tool's name and its arguments to run a read tool. " +
        "call_tool runs read tools only. A tool that writes is not run here: use the / endpoint for writes. " +
        "The tool count below describes the full set.";

    /// <summary>"a, b and c": the names as a sentence, so the note is built from the listed tools and cannot drift from them.</summary>
    private static string NamesInSentence(string[] names) =>
        names.Length < 2 ? string.Join("", names) : string.Join(", ", names[..^1]) + " and " + names[^1];

    internal static string LeanInstructions(string? fullInstructions) =>
        string.IsNullOrEmpty(fullInstructions) ? LeanNote : LeanNote + "\n\n" + fullInstructions;

    /// <summary>A fresh collection without <c>call_tool</c>, for every path but <c>/lean</c>: there it is absent, so a call to it gets
    /// the SDK's unknown-tool answer. The input is untouched (it is shared by every request).</summary>
    internal static McpServerPrimitiveCollection<McpServerTool> WithoutCallTool(McpServerPrimitiveCollection<McpServerTool>? allTools)
    {
        var served = new McpServerPrimitiveCollection<McpServerTool>();
        if (allTools is not null)
        {
            foreach (var tool in allTools)
            {
                if (!string.Equals(tool.ProtocolTool.Name, CallToolName, StringComparison.Ordinal))
                    served.Add(tool);
            }
        }

        return served;
    }

    /// <summary>The list-tools filter: on <c>/lean</c>, narrows the listing to <see cref="ListedTools"/>. The served collection
    /// is untouched, so the listing is the only thing it changes.</summary>
    internal static McpRequestFilter<ListToolsRequestParams, ListToolsResult> ListFilter { get; } = next => async (request, cancellationToken) =>
    {
        var result = await next(request, cancellationToken).ConfigureAwait(false);
        var path = request.Services?.GetService<IHttpContextAccessor>()?.HttpContext?.Request.Path ?? default;
        if (IsLeanPath(path))
        {
            var kept = result.Tools.Where(t => ListedTools.Contains(t.Name)).ToList();
            result.Tools.Clear();
            foreach (var tool in kept)
                result.Tools.Add(tool);
        }

        return result;
    };

    /// <summary>True when a tool may run through <c>call_tool</c>: its stamped read-only hint is set and it is in none of the write
    /// policy's sets.</summary>
    internal static bool IsReadOnly(McpServerTool tool)
    {
        var name = tool.ProtocolTool.Name;
        return tool.ProtocolTool.Annotations?.ReadOnlyHint == true
            && !DarlingMcpWritePolicy.IsWrite(name)
            && !DarlingMcpWritePolicy.IsInvestigation(name)
            && !DarlingMcpWritePolicy.IsServerAdmin(name)
            && !DarlingMcpWritePolicy.AgentActionTools.Contains(name);
    }

    /// <summary>The outermost call-tool filter. For anything but a call to <c>call_tool</c> it does nothing. For a call to it, it checks the
    /// rules (the name is served on this request, is not <c>call_tool</c> and is a read tool, the arguments are an object or absent), then
    /// rewrites the request to a direct call of the inner tool and calls next.</summary>
    internal static McpRequestFilter<CallToolRequestParams, CallToolResult> CallToolFilter { get; } = next => async (request, cancellationToken) =>
    {
        if (request.MatchedPrimitive is not McpServerTool { ProtocolTool.Name: CallToolName } || request.Params is not { } outer)
            return await next(request, cancellationToken).ConfigureAwait(false);

        string? name = null;
        JsonElement? argumentsElement = null;
        if (outer.Arguments is { } given)
        {
            if (given.TryGetValue("name", out var nameElement) && nameElement.ValueKind == JsonValueKind.String)
                name = nameElement.GetString();
            if (given.TryGetValue("arguments", out var found))
                argumentsElement = found;
        }

        if (string.IsNullOrWhiteSpace(name))
            return Refusal("call_tool needs the name of the tool to run, as a string in name.");

        /* A name this request is not served gets the SDK's own unknown-tool answer, so a tool the token's policy drops (or the write
           switches leave out) is not told apart from one that does not exist. */
        var served = request.Server?.ServerOptions?.ToolCollection;
        if (served is null || !served.TryGetPrimitive(name, out var inner))
            throw new McpProtocolException("Unknown tool: '" + name + "'", McpErrorCode.InvalidParams);

        if (string.Equals(name, CallToolName, StringComparison.Ordinal))
            return Refusal("call_tool does not run itself. Name the tool to run.");

        if (!IsReadOnly(inner))
            return Refusal("call_tool runs read tools only, and " + name + " is not one. Call " + name + " on the full path, the / endpoint, where the client shows its own confirmation before a write.");

        Dictionary<string, JsonElement>? innerArguments = null;
        if (argumentsElement is { } rawArguments && rawArguments.ValueKind != JsonValueKind.Null && rawArguments.ValueKind != JsonValueKind.Undefined)
        {
            if (rawArguments.ValueKind != JsonValueKind.Object)
                return Refusal("call_tool takes arguments as a JSON object, or none.");

            innerArguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
            foreach (var property in rawArguments.EnumerateObject())
                innerArguments[property.Name] = property.Value;
        }

        /* The rewrite: every later filter reads Params.Name and the served collection, and the dispatch reads MatchedPrimitive. */
        outer.Name = name;
        outer.Arguments = innerArguments;
        request.MatchedPrimitive = inner;
        return await next(request, cancellationToken).ConfigureAwait(false);
    };

    private static CallToolResult Refusal(string text) => new()
    {
        IsError = true,
        Content = [new TextContentBlock { Text = text }],
    };
}

/// <summary>
/// <c>call_tool</c>, served on <c>/lean</c> only (#9388). Its method is never reached for a valid call:
/// <see cref="DarlingLeanToolProfile.CallToolFilter"/> rewrites the request to the inner tool before dispatch. It refuses if it is.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpLeanTools
{
    [FrontDoorClass(FrontDoorToolClass.Local)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "call_tool"), Description("Runs one read tool by name, with its arguments, as if it had been called directly. Find the tool and its arguments with find_tools. It runs read tools only: a tool that writes must be called on the full path. Where a tool takes a store argument, put it inside arguments: call_tool has no store of its own.")]
    public static string CallTool(
        [Description("The name of the tool to run.")] string name,
        [Description("The tool's arguments, as a JSON object. Omit for a tool that takes none.")] JsonElement? arguments = null) =>
        throw new McpException("call_tool did not reach the tool " + name + ". Call it directly.");
}
