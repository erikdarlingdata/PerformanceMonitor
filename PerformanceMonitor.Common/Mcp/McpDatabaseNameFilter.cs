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
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// Reads, for one call, the stored database names of the server the call names that equal the given name ignoring letter case.
/// Null when there is nothing to resolve against (the server does not resolve, or the store holds no database list for it): the call
/// then runs with the name as given and says nothing about it. An empty list means the server has a list and no such name is on it.
/// </summary>
/// <param name="services">The request's services.</param>
/// <param name="serverName">The call's <c>server_name</c>, or null when it sent none.</param>
/// <param name="databaseName">The call's <c>database_name</c>, never blank.</param>
/// <param name="cancellationToken">The request's token.</param>
public delegate Task<IReadOnlyList<string>?> McpDatabaseNameSource(
    IServiceProvider? services, string? serverName, string databaseName, CancellationToken cancellationToken);

/// <summary>
/// #9065: the call-tool filter that resolves a call's <c>database_name</c> before the tool runs, by
/// <see cref="McpDatabaseNameResolver"/>. One filter per host covers every tool that takes <c>database_name</c> and
/// <c>server_name</c> (<see cref="McpDatabaseNameResolver.Resolves"/>), so a tool's read never sees a name it would compare
/// case-sensitively against a differently cased stored one, and no tool resolves it for itself.
///
/// <para>An exact match, a blank name and a server with no database list all pass the call through untouched. A single
/// case-insensitive match replaces the argument with the stored spelling and adds
/// <see cref="McpDatabaseNameResolver.ResolvedField"/> to the answer. Two or more refuse the call with the list. No match runs the
/// call as given and adds <see cref="McpDatabaseNameResolver.NoteField"/>. A lookup that fails is "nothing to resolve against": a read
/// never fails on it.</para>
///
/// <para>The answer is edited as the tool's own JSON, so a host registers this outside its masking filters and inside any
/// re-encoding one.</para>
/// </summary>
public static class McpDatabaseNameFilter
{
    /// <summary>The filter over <paramref name="source"/>, the host's read of a server's database names.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Using(McpDatabaseNameSource source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return next => async (request, cancellationToken) =>
        {
            if (request.Params?.Arguments is not { } arguments
                || !arguments.TryGetValue(McpDatabaseNameResolver.Parameter, out var sent)
                || sent.ValueKind != JsonValueKind.String
                || sent.GetString() is not { } given
                || string.IsNullOrWhiteSpace(given)
                || !McpDatabaseNameResolver.Resolves(ToolOf(request)))
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var serverName = arguments.TryGetValue(McpDatabaseNameResolver.ServerParameter, out var server)
                && server.ValueKind == JsonValueKind.String ? server.GetString() : null;

            IReadOnlyList<string>? known;
            try
            {
                known = await source(request.Services, serverName, given, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                known = null;
            }

            if (known is null)
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var resolution = McpDatabaseNameResolver.Resolve(given, known);
            switch (resolution.Match)
            {
                case McpDatabaseNameMatch.Ambiguous:
                    return new CallToolResult
                    {
                        Content = new List<ContentBlock>
                        {
                            new TextContentBlock
                            {
                                Text = McpHelpers.Refusal(
                                    McpDatabaseNameResolver.Parameter,
                                    McpDatabaseNameResolver.AmbiguousMessage(given, resolution.Candidates)),
                            },
                        },
                        IsError = true,
                    };

                case McpDatabaseNameMatch.Resolved:
                    request.Params.Arguments = new Dictionary<string, JsonElement>(arguments)
                    {
                        [McpDatabaseNameResolver.Parameter] = JsonSerializer.SerializeToElement(resolution.Name),
                    };
                    return WithField(
                        await next(request, cancellationToken).ConfigureAwait(false),
                        McpDatabaseNameResolver.ResolvedField,
                        resolution.Name!);

                case McpDatabaseNameMatch.NotSeen:
                    return WithField(
                        await next(request, cancellationToken).ConfigureAwait(false),
                        McpDatabaseNameResolver.NoteField,
                        McpDatabaseNameResolver.NotSeenMessage(given));

                default:
                    return await next(request, cancellationToken).ConfigureAwait(false);
            }
        };
    }

    /// <summary>
    /// <paramref name="result"/> with <paramref name="field"/> added to its answer when that answer is one JSON object in one text
    /// block; the same instance otherwise (an error result, another content shape, text that is not a JSON object).
    /// </summary>
    public static CallToolResult WithField(CallToolResult result, string field, string value)
    {
        if (result is null || result.IsError == true || result.Content is not { Count: 1 } content
            || content[0] is not TextContentBlock text
            || McpDatabaseNameResolver.WithField(text.Text, field, value) is not { } edited)
        {
            return result!;
        }

        return new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = edited } },
            StructuredContent = result.StructuredContent,
            IsError = result.IsError,
            Meta = result.Meta,
        };
    }

    private static Tool? ToolOf(RequestContext<CallToolRequestParams> request)
    {
        var name = request.Params?.Name;
        var tools = request.Server?.ServerOptions?.ToolCollection;
        return !string.IsNullOrEmpty(name) && tools is not null && tools.TryGetPrimitive(name!, out var tool) ? tool.ProtocolTool : null;
    }
}
