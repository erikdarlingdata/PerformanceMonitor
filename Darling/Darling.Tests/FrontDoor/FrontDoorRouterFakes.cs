/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests.FrontDoor;

/// <summary>A directory of named stores whose answers a test sets (#6840 part P3). It does no network and no refresh of its own.</summary>
internal sealed class FakeFrontDoorDirectory : IFrontDoorDirectory
{
    private readonly List<FrontDoorStoreState> _stores;

    internal FakeFrontDoorDirectory(params string[] names)
        : this(names.Select(n => (n, FrontDoorStoreHealth.Ok)).ToArray())
    {
    }

    internal FakeFrontDoorDirectory(params (string Name, FrontDoorStoreHealth Health)[] stores)
    {
        _stores = stores
            .Select(s => new FrontDoorStoreState(
                new FrontDoorStore(s.Name, new Uri($"http://{s.Name}.example.invalid:5152/"), null, null),
                s.Health, Array.Empty<RegisteredServer>(), null, null))
            .ToList();
    }

    /// <summary>What <see cref="ResolveServer"/> answers for a name. The default refuses every name.</summary>
    internal Func<string, FrontDoorRoute> Resolver { get; set; } = name => new FrontDoorRoute.Refused($"'{name}' matches no server.");

    /// <summary>Runs inside <see cref="RefreshOnMissAsync"/>, so a test can make a server appear on the refresh.</summary>
    internal Action? OnRefresh { get; set; }

    internal int RefreshCalls { get; private set; }

    internal int ResolveCalls { get; private set; }

    public IReadOnlyList<FrontDoorStoreState> Stores => _stores;

    public FrontDoorStore? Find(string name) =>
        _stores.Select(s => s.Store).FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));

    public FrontDoorRoute ResolveServer(string serverName)
    {
        ResolveCalls++;
        return Resolver(serverName);
    }

    public Task RefreshOnMissAsync(CancellationToken ct)
    {
        RefreshCalls++;
        OnRefresh?.Invoke();
        return Task.CompletedTask;
    }

    public void Hold(string store, TimeSpan span, string why)
    {
    }
}

/// <summary>One call the fake peers received.</summary>
internal sealed record FakePeerCall(string Store, CallToolRequestParams Params, bool AgentRole, CancellationToken Token);

/// <summary>The MCP peer client with the answer to each call set by a test, recording every call it gets.</summary>
internal sealed class FakeFrontDoorPeers : IFrontDoorMcpPeers
{
    private readonly ConcurrentQueue<FakePeerCall> _calls = new();

    /// <summary>The answer to a call. The default is the text <c>{"status":"ok","store":"&lt;store&gt;"}</c>.</summary>
    internal Func<FakePeerCall, Task<CallToolResult>> Handler { get; set; } =
        call => Task.FromResult(Text($"{{\"status\":\"ok\",\"store\":\"{call.Store}\"}}"));

    internal IReadOnlyList<FakePeerCall> Calls => _calls.ToArray();

    public Task<CallToolResult> CallAsync(string store, CallToolRequestParams call, bool agentRole, CancellationToken ct)
    {
        var record = new FakePeerCall(store, call, agentRole, ct);
        _calls.Enqueue(record);
        return Handler(record);
    }

    internal static CallToolResult Text(string text, bool isError = false) =>
        new() { Content = new List<ContentBlock> { new TextContentBlock { Text = text } }, IsError = isError };

    /// <summary>The call's arguments as JSON text, for a comparison.</summary>
    internal static string ArgumentsOf(FakePeerCall call) =>
        JsonSerializer.Serialize(call.Params.Arguments ?? new Dictionary<string, JsonElement>());

    /// <summary>The text of a result's one text block.</summary>
    internal static string TextOf(CallToolResult result) =>
        Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text;

    /// <summary>The arguments of a call from name and JSON-text pairs: <c>("server_name", "\"a\"")</c>.</summary>
    internal static Dictionary<string, JsonElement> Args(params (string Name, string Json)[] pairs)
    {
        var args = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var (name, json) in pairs)
        {
            using var document = JsonDocument.Parse(json);
            args[name] = document.RootElement.Clone();
        }

        return args;
    }
}
