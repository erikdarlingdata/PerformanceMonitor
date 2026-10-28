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
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// The front door's answer to a read that names no server and no store (#6840): the read is asked of every store that is not held,
/// in parallel, and the answers are put in one text block with each store's own answer kept apart. Nothing is merged, so each
/// store's <c>status</c> words (<c>empty</c>, <c>not_collected</c>, <c>busy</c>) stay what that store said.
/// </summary>
internal static class DarlingFrontDoorFanOut
{
    /// <summary>
    /// Asks <paramref name="askStore"/> for each store whose health is not <see cref="FrontDoorStoreHealth.AuthFailed"/> (a held store is
    /// not called, so a wrong token is not sent again) and builds the answer:
    /// <c>{"front_door":true,"tool":...,"stores":[{"store":...,"is_error":false,"result":...},{"store":...,"status":"unavailable","message":...}]}</c>.
    /// <c>IsError</c> is set only when every store failed, a failure being an unavailable store or a result the store marked as an error.
    /// </summary>
    internal static async Task<CallToolResult> RunAsync(
        string tool,
        IReadOnlyList<FrontDoorStoreState> stores,
        Func<string, Task<DarlingFrontDoorRouter.StoreOutcome>> askStore,
        CancellationToken ct)
    {
        /* No store to ask is not "asked every store, nothing to report": answer what the one-store path answers when it has no store. */
        if (stores.Count == 0)
        {
            return new CallToolResult
            {
                Content = new List<ContentBlock>
                {
                    new TextContentBlock { Text = McpHelpers.Status("unavailable", "No store is reachable right now, so the front door cannot answer.") },
                },
                IsError = true,
            };
        }

        var tasks = stores
            .Select(state => state.Health == FrontDoorStoreHealth.AuthFailed
                ? Task.FromResult(new DarlingFrontDoorRouter.StoreOutcome(
                    state.Store.Name, null, $"Store '{state.Store.Name}' refused the front door's token, so it is not asked until the hold ends."))
                : askStore(state.Store.Name))
            .ToList();

        var outcomes = await Task.WhenAll(tasks).WaitAsync(ct).ConfigureAwait(false);

        var entries = new JsonArray();
        var allFailed = outcomes.Length > 0;
        foreach (var outcome in outcomes)
        {
            if (outcome.Result is null)
            {
                entries.Add(new JsonObject
                {
                    ["store"] = outcome.Store,
                    ["status"] = "unavailable",
                    ["message"] = outcome.Message,
                });
                continue;
            }

            var isError = outcome.Result.IsError == true;
            allFailed &= isError;
            entries.Add(new JsonObject
            {
                ["store"] = outcome.Store,
                ["is_error"] = isError,
                ["result"] = ResultNode(outcome.Result),
            });
        }

        var root = new JsonObject
        {
            ["front_door"] = true,
            ["tool"] = tool,
            ["stores"] = entries,
        };

        return new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = root.ToJsonString(McpHelpers.JsonOptions) } },
            IsError = allFailed,
        };
    }

    /// <summary>A store's result as a JSON value: its text parsed when it is JSON (every Darling tool answers JSON), else the text itself.</summary>
    internal static JsonNode? ResultNode(CallToolResult result)
    {
        var text = TextOf(result);
        try
        {
            return JsonNode.Parse(text);
        }
        catch (JsonException)
        {
            return JsonValue.Create(text);
        }
    }

    /// <summary>The text of every text block of a result, joined with a line feed.</summary>
    internal static string TextOf(CallToolResult result)
    {
        var builder = new StringBuilder();
        foreach (var block in result.Content.OfType<TextContentBlock>())
        {
            if (builder.Length > 0)
            {
                builder.Append('\n');
            }

            builder.Append(block.Text);
        }

        return builder.ToString();
    }
}
