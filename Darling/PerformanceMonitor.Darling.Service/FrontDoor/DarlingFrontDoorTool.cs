/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// A tool as the front door serves it (#6840): the same name, description and parameters the store's tool has, plus an optional
/// <c>store</c> argument, and a call that goes to a store's MCP endpoint through <see cref="DarlingFrontDoorRouter"/>. The inner tool
/// is what the host registered, kept only so the tool's schema is the build's own. It never runs.
/// </summary>
internal sealed class DarlingFrontDoorTool : DelegatingMcpServerTool
{
    private readonly DarlingFrontDoorRouter _router;
    private readonly Func<IServiceProvider?, bool> _agentRoleOf;
    private readonly Tool _protocolTool;

    /// <param name="inner">The registered tool, whose schema this one copies.</param>
    /// <param name="router">Where a call goes.</param>
    /// <param name="agentRoleOf">True when the request's caller holds the agent role (#6542), from the request's services.</param>
    internal DarlingFrontDoorTool(McpServerTool inner, DarlingFrontDoorRouter router, Func<IServiceProvider?, bool> agentRoleOf)
        : base(inner)
    {
        _router = router;
        _agentRoleOf = agentRoleOf;
        _protocolTool = WithStoreArgument(inner.ProtocolTool, router.StoreNames);
    }

    /// <inheritdoc />
    public override Tool ProtocolTool => _protocolTool;

    /// <inheritdoc />
    public override async ValueTask<CallToolResult> InvokeAsync(RequestContext<CallToolRequestParams> request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var agentRole = _agentRoleOf(request.Services);
        return await _router.CallAsync(_protocolTool.Name, request.Params?.Arguments, agentRole, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>A copy of <paramref name="source"/> whose input schema also has the optional <c>store</c> string, an enum of the
    /// configured store names (no enum while none is configured). Every other part of the tool is the source's.</summary>
    internal static Tool WithStoreArgument(Tool source, IReadOnlyList<string> storeNames)
    {
        var schema = JsonNode.Parse(source.InputSchema.GetRawText()) as JsonObject ?? new JsonObject { ["type"] = "object" };
        if (schema["properties"] is not JsonObject properties)
        {
            properties = new JsonObject();
            schema["properties"] = properties;
        }

        var store = new JsonObject
        {
            ["type"] = "string",
            ["description"] = "The store to ask. Optional: a server_name finds its store, and a read that names no server asks every store. Required for writes.",
        };
        if (storeNames.Count > 0)
        {
            var names = new JsonArray();
            foreach (var name in storeNames)
            {
                names.Add(name);
            }

            store["enum"] = names;
        }

        properties[DarlingFrontDoorRouting.StoreArgument] = store;

        using var document = JsonDocument.Parse(schema.ToJsonString());
        return new Tool
        {
            Name = source.Name,
            Title = source.Title,
            Description = source.Description,
            InputSchema = document.RootElement.Clone(),
            OutputSchema = source.OutputSchema,
            Annotations = source.Annotations,
            Icons = source.Icons,
            Meta = source.Meta,
        };
    }
}
