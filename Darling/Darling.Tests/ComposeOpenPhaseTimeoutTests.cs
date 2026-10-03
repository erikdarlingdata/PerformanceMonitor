/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605 review round 2: drives the OPEN phase of a compose run for real. A listener that accepts a TCP
/// connection and never answers makes Npgsql's connect/startup time out, which the runner must answer as a
/// store-connection failure (a server error), never as the author-facing statement-timeout text. No
/// PostgreSQL is needed.
/// </summary>
public sealed class ComposeOpenPhaseTimeoutTests
{
    private const string PanelJson = "{\"panel\":{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}}";

    private sealed class SilentListener : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly List<TcpClient> _clients = [];
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _loop;

        public SilentListener()
        {
            _listener.Start();
            _loop = Task.Run(async () =>
            {
                try
                {
                    while (!_stop.IsCancellationRequested)
                    {
                        var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                        lock (_clients)
                        {
                            _clients.Add(client);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    /* Disposal. */
                }
                catch (ObjectDisposedException)
                {
                    /* Disposal. */
                }
            });
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        public string ConnectionString => $"Host=127.0.0.1;Port={Port};Username=x;Password=x;Database=x;Timeout=1;Pooling=false";

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            try
            {
                _loop.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                /* Accept loop ended by disposal. */
            }

            lock (_clients)
            {
                foreach (var client in _clients)
                {
                    client.Dispose();
                }
            }

            _stop.Dispose();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ASilentStore_IsAServerError_NotTheStatementTimeoutText(bool remapClientTimeout)
    {
        using var listener = new SilentListener();
        await using var store = NpgsqlDataSource.Create(listener.ConnectionString);
        var body = (JsonObject)JsonNode.Parse(PanelJson)!;

        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(
            store, body, TestContext.Current.CancellationToken, null, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: remapClientTimeout);

        Assert.True(outcome.IsServerError, outcome.Error);
        /* The same prefix assertion, with the whole text as the failure message: xunit cuts a StartsWith failure after about
           fifty characters, which hid the cause when this failed on a loaded runner. */
        Assert.True(outcome.Error!.StartsWith("Error running query: could not get a store connection in time: ", StringComparison.Ordinal), outcome.Error);
        Assert.NotEqual(DarlingWebEndpoints.StatementTimeoutText, outcome.Error);
        Assert.NotEqual("57014", outcome.AuthorSqlState);
    }

    [Fact]
    public async Task ASilentStore_AnswersTheComposeRoute_WithA500_NotA400CarryingTheTimeoutText()
    {
        using var listener = new SilentListener();
        await using var store = NpgsqlDataSource.Create(listener.ConnectionString);
        var ct = TestContext.Current.CancellationToken;

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions());
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(store);

        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, store, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        using var server = app.GetTestServer();

        var httpContext = await server.SendAsync(request =>
        {
            request.Request.Method = "POST";
            request.Request.Path = "/api/compose/run";
            request.Request.Headers.Host = "localhost";
            request.Request.ContentType = "application/json";
            request.Request.Body = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(PanelJson));
        }, ct);

        var text = await new System.IO.StreamReader(httpContext.Response.Body).ReadToEndAsync(ct);
        Assert.Equal(StatusCodes.Status500InternalServerError, httpContext.Response.StatusCode);
        Assert.DoesNotContain("statement timeout", text, StringComparison.OrdinalIgnoreCase);
        await app.StopAsync(ct);
    }
}
