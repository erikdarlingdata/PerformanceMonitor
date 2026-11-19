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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// Reads, for one call, the stored database names of the server the call names that equal the given name ignoring letter case: the
/// newest list's, plus the given spelling itself when the host finds it on an earlier list in its look-back. Null when there is nothing to
/// resolve against (the server does not resolve, or the store holds no database list for it): the call then runs with the name as
/// given and says nothing about it. An empty list means the server has a list and no such name is on it. The answer carries the
/// time the newest list was taken, so what the filter tells the caller about that list is dated.
/// </summary>
/// <param name="services">The request's services.</param>
/// <param name="serverName">The call's <c>server_name</c>, or null when it sent none.</param>
/// <param name="databaseName">The call's <c>database_name</c>, never blank.</param>
/// <param name="changesState">True when the tool changes state, so the server is resolved the way such a tool resolves it.</param>
/// <param name="cancellationToken">The request's token, cut off at <see cref="McpDatabaseNameFilter.LookupDeadline"/>.</param>
public delegate Task<McpDatabaseNameLookup?> McpDatabaseNameSource(
    IServiceProvider? services, string? serverName, string databaseName, bool changesState, CancellationToken cancellationToken);

/// <summary>What one lookup found on a server's database lists.</summary>
/// <param name="Names">The stored names that equal the given one ignoring letter case; empty when the newest list holds none.</param>
/// <param name="ListTakenUtc">When the newest list was taken, UTC; null when the host does not know.</param>
public sealed record McpDatabaseNameLookup(IReadOnlyList<string> Names, DateTime? ListTakenUtc = null);

/// <summary>
/// #9065: the call-tool filter that resolves a call's <c>database_name</c> before the tool runs, by
/// <see cref="McpDatabaseNameResolver"/>. One filter per host covers every tool that takes <c>database_name</c> and
/// <c>server_name</c> (<see cref="McpDatabaseNameResolver.Resolves"/>), so a tool's read never sees a name it would compare
/// case-sensitively against a differently cased stored one, and no tool resolves it for itself.
///
/// <para>An exact match, a blank name and a server with no database list all pass the call through untouched. A single
/// case-insensitive match replaces the argument with the stored spelling and adds
/// <see cref="McpDatabaseNameResolver.ResolvedField"/> to the answer, unless the tool changes state
/// (<see cref="McpDatabaseNameResolver.ChangesState"/>): that call is refused with the stored spelling, never rewritten. Two or more
/// refuse the call with the list. No match runs the call as given and adds <see cref="McpDatabaseNameResolver.NoteField"/>. Each of
/// those says when the newest list was taken (<see cref="McpDatabaseNameResolver.ResolvedAsOfField"/> beside the resolved name, a
/// sentence in the note and in both refusals), so a list that is days old is not read as the server's present state.</para>
///
/// <para>The lookup has its own deadline, <see cref="LookupDeadline"/>. A lookup that times out or fails is logged as a warning (the
/// tool's name, never the caller's server or database name) and is "nothing to resolve against": the call runs as given.</para>
///
/// <para>The answer is edited as the tool's own JSON, so a host registers this outside its masking filters and inside any
/// re-encoding one.</para>
/// </summary>
public static class McpDatabaseNameFilter
{
    /// <summary>
    /// How long one call waits for its name lookup. The lookup is one to three small index reads that measure in single
    /// milliseconds, so three seconds is hundreds of times its cost and still well under the five seconds at which an MCP read is
    /// slow; past it the store is stalled and the call is better run as given than held.
    /// </summary>
    public static readonly TimeSpan LookupDeadline = TimeSpan.FromSeconds(3);

    /// <summary>The filter over <paramref name="source"/>, the host's read of a server's database names.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Using(McpDatabaseNameSource source) =>
        Using(source, LookupDeadline);

    /// <summary>The filter over <paramref name="source"/>, giving each lookup <paramref name="lookupDeadline"/> before the call runs as given.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Using(McpDatabaseNameSource source, TimeSpan lookupDeadline)
    {
        ArgumentNullException.ThrowIfNull(source);
        return next => async (request, cancellationToken) =>
        {
            if (request.Params?.Arguments is not { } arguments
                || !arguments.TryGetValue(McpDatabaseNameResolver.Parameter, out var sent)
                || sent.ValueKind != JsonValueKind.String
                || sent.GetString() is not { } given
                || string.IsNullOrWhiteSpace(given)
                || ToolOf(request) is not { } tool
                || !McpDatabaseNameResolver.Resolves(tool))
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var serverName = arguments.TryGetValue(McpDatabaseNameResolver.ServerParameter, out var server)
                && server.ValueKind == JsonValueKind.String ? server.GetString() : null;

            var changesState = McpDatabaseNameResolver.ChangesState(tool);

            McpDatabaseNameLookup? known;
            using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                deadline.CancelAfter(lookupDeadline);
                try
                {
                    known = await source(request.Services, serverName, given, changesState, deadline.Token).ConfigureAwait(false);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    /* The request itself is not cancelled, so this is the lookup's own deadline or its own failure. */
                    Warn(request.Services, tool.Name, ex, deadline.IsCancellationRequested ? lookupDeadline : null);
                    known = null;
                }
            }

            if (known is null)
            {
                return await next(request, cancellationToken).ConfigureAwait(false);
            }

            var resolution = McpDatabaseNameResolver.Resolve(given, known.Names);
            var taken = known.ListTakenUtc;
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
                                    McpDatabaseNameResolver.AmbiguousMessage(given, resolution.Candidates, taken)),
                            },
                        },
                        IsError = true,
                    };

                case McpDatabaseNameMatch.Resolved when changesState:
                    return new CallToolResult
                    {
                        Content = new List<ContentBlock>
                        {
                            new TextContentBlock
                            {
                                Text = McpHelpers.Refusal(
                                    McpDatabaseNameResolver.Parameter,
                                    McpDatabaseNameResolver.ExactRequiredMessage(given, resolution.Name!, taken)),
                            },
                        },
                        IsError = true,
                    };

                case McpDatabaseNameMatch.Resolved:
                    request.Params.Arguments = new Dictionary<string, JsonElement>(arguments)
                    {
                        [McpDatabaseNameResolver.Parameter] = JsonSerializer.SerializeToElement(resolution.Name),
                    };
                    var answered = WithField(
                        await next(request, cancellationToken).ConfigureAwait(false),
                        McpDatabaseNameResolver.ResolvedField,
                        resolution.Name!);
                    return taken is { } listTaken
                        ? WithField(answered, McpDatabaseNameResolver.ResolvedAsOfField, McpDatabaseNameResolver.AsOf(listTaken))
                        : answered;

                case McpDatabaseNameMatch.NotSeen:
                    return WithField(
                        await next(request, cancellationToken).ConfigureAwait(false),
                        McpDatabaseNameResolver.NoteField,
                        McpDatabaseNameResolver.NotSeenMessage(given, taken));

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

    private static void Warn(IServiceProvider? services, string? toolName, Exception ex, TimeSpan? timedOutAfter)
    {
        if (services?.GetService<ILoggerFactory>() is not { } loggerFactory)
        {
            return;
        }

        var logger = loggerFactory.CreateLogger(typeof(McpDatabaseNameFilter).FullName!);
        if (timedOutAfter is { } waited)
        {
            logger.LogWarning(
                "The database_name lookup for {Tool} did not answer within {Seconds} s, so the call ran with the name as given.",
                toolName, waited.TotalSeconds);
        }
        else
        {
            logger.LogWarning(ex, "The database_name lookup for {Tool} failed, so the call ran with the name as given.", toolName);
        }
    }

    private static Tool? ToolOf(RequestContext<CallToolRequestParams> request)
    {
        var name = request.Params?.Name;
        var tools = request.Server?.ServerOptions?.ToolCollection;
        return !string.IsNullOrEmpty(name) && tools is not null && tools.TryGetPrimitive(name!, out var tool) ? tool.ProtocolTool : null;
    }
}
