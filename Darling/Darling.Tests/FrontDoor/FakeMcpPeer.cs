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
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// A store's MCP endpoint for the front door's tests (#6840): a real Kestrel app on 127.0.0.1 with a free port, running the SDK's
/// stateless Streamable HTTP transport, as a store does. It checks the bearer token the way a store does (an operator token and
/// an agent token, 401 for anything else), can answer 401, 403 or 429 on demand, and serves a few tools under real names:
/// <c>list_servers</c> (the rows in <see cref="AddServer"/>), <c>whoami</c> (which token role the call carried),
/// <c>get_wait_stats</c> (answers at once, or with <see cref="BlockWaitStats"/> waits until the request is cancelled and
/// records that it saw it) and <c>get_busy</c> (a busy answer, which is a tool result and not an HTTP status).
/// Part P3 reuses it for the router's tests.
/// </summary>
internal sealed class FakeMcpPeer : IAsyncDisposable
{
    public const string OperatorToken = "fake-operator-token";
    public const string AgentToken = "fake-agent-token";

    private readonly WebApplication _app;
    private readonly ConcurrentDictionary<string, int> _toolCalls = new();
    private readonly object _serversGate = new();
    private readonly List<string> _servers;
    private int _httpRequests;
    private int _refuseStatus;
    private int _retryAfterSeconds;

    private FakeMcpPeer(WebApplication app, Uri url, List<string> servers)
    {
        _app = app;
        Url = url;
        _servers = servers;
    }

    /// <summary>The endpoint to put in a store's <c>mcpUrl</c>.</summary>
    public Uri Url { get; }

    /// <summary>Set to make <c>get_wait_stats</c> wait until its request is cancelled.</summary>
    public bool BlockWaitStats { get; set; }

    /// <summary>Completes when a blocked <c>get_wait_stats</c> call has started.</summary>
    public TaskCompletionSource WaitStatsStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes when a blocked <c>get_wait_stats</c> call saw its request cancelled.</summary>
    public TaskCompletionSource WaitStatsCancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Every HTTP request that reached the app, refused or not.</summary>
    public int HttpRequests => Volatile.Read(ref _httpRequests);

    /// <summary>Every Authorization header value received, in order.</summary>
    public ConcurrentQueue<string> Authorizations { get; } = new();

    /// <summary>How many times the tool ran.</summary>
    public int ToolCalls(string tool) => _toolCalls.TryGetValue(tool, out var n) ? n : 0;

    /// <summary>Answers every request with this status (401, 403 or 429) from now on, or stops when 0.</summary>
    public void Refuse(int status, int retryAfterSeconds = 0)
    {
        Volatile.Write(ref _retryAfterSeconds, retryAfterSeconds);
        Volatile.Write(ref _refuseStatus, status);
    }

    /// <summary>Adds a server to the list <c>list_servers</c> answers.</summary>
    public void AddServer(string name)
    {
        lock (_serversGate)
        {
            _servers.Add(name);
        }
    }

    /// <param name="servers">The <c>server_name</c> values <c>list_servers</c> answers.</param>
    /// <param name="operatorToken">The operator-role token, or null for a store that has none.</param>
    /// <param name="agentToken">The agent-role token, or null for a store that has none.</param>
    /// <param name="port">A port to take again (a store that came back), or 0 for any free one.</param>
    public static async Task<FakeMcpPeer> StartAsync(
        IEnumerable<string> servers, string? operatorToken = OperatorToken, string? agentToken = AgentToken, int port = 0)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        builder.Services.AddHttpContextAccessor();

        FakeMcpPeer? self = null;
        var list = servers.ToList();

        void Count(string tool) => self!._toolCalls.AddOrUpdate(tool, 1, (_, n) => n + 1);

        var tools = new List<McpServerTool>
        {
            McpServerTool.Create(
                () =>
                {
                    Count("list_servers");
                    string[] snapshot;
                    lock (self!._serversGate)
                    {
                        snapshot = self._servers.ToArray();
                    }

                    return JsonSerializer.Serialize(new
                    {
                        server_count = snapshot.Length,
                        servers = snapshot.Select(n => new
                        {
                            server_name = n,
                            display_name = n,
                            engine_kind = "sqlserver",
                            read_only = n.EndsWith(":RO", StringComparison.Ordinal),
                        }),
                    });
                },
                new McpServerToolCreateOptions { Name = "list_servers" }),
            McpServerTool.Create(
                (IHttpContextAccessor http) =>
                {
                    Count("whoami");
                    return http.HttpContext?.Items["role"] as string ?? "none";
                },
                new McpServerToolCreateOptions { Name = "whoami" }),
            McpServerTool.Create(
                async (IHttpContextAccessor http, CancellationToken ct) =>
                {
                    Count("get_wait_stats");
                    if (!self!.BlockWaitStats)
                    {
                        return "{\"status\":\"ok\"}";
                    }

                    self.WaitStatsStarted.TrySetResult();
                    using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                        ct, http.HttpContext?.RequestAborted ?? CancellationToken.None);
                    try
                    {
                        await Task.Delay(Timeout.Infinite, linked.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        self.WaitStatsCancelled.TrySetResult();
                        throw;
                    }

                    return "{\"status\":\"ok\"}";
                },
                new McpServerToolCreateOptions { Name = "get_wait_stats" }),
            McpServerTool.Create(
                () =>
                {
                    Count("get_busy");
                    return "{\"status\":\"busy\",\"retry_after_seconds\":5}";
                },
                new McpServerToolCreateOptions { Name = "get_busy" }),
        };

        builder.Services.AddMcpServer().WithHttpTransport(o => o.Stateless = true).WithTools(tools);
        var app = builder.Build();

        app.Use(async (http, next) =>
        {
            Interlocked.Increment(ref self!._httpRequests);
            var authorization = http.Request.Headers.Authorization.ToString();
            self.Authorizations.Enqueue(authorization);

            var refuse = Volatile.Read(ref self._refuseStatus);
            if (refuse != 0)
            {
                http.Response.StatusCode = refuse;
                var seconds = Volatile.Read(ref self._retryAfterSeconds);
                if (seconds > 0)
                {
                    http.Response.Headers.RetryAfter = seconds.ToString(CultureInfo.InvariantCulture);
                }

                return;
            }

            string? role = null;
            if (operatorToken is not null && authorization == "Bearer " + operatorToken)
            {
                role = "operator";
            }
            else if (agentToken is not null && authorization == "Bearer " + agentToken)
            {
                role = "agent";
            }

            if (role is null)
            {
                http.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return;
            }

            http.Items["role"] = role;
            await next(http);
        });
        app.MapMcp();

        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        self = new FakeMcpPeer(app, new Uri(address.TrimEnd('/') + "/"), list);
        return self;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
