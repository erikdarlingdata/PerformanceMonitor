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
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8184: the health host's routes, its start order, and its behaviour for a missing, broken or unreadable config and a taken port.
/// Touches no process-wide state (no statics, no environment variables, no files): every collaborator is a per-test instance and the
/// real-port tests bind loopback on a port the test obtained, so the class does not join the statics census.
/// </summary>
public sealed class DarlingHealthHostTests
{
    private sealed class EmptyDirectory : IFrontDoorDirectory
    {
        public IReadOnlyList<FrontDoorStoreState> Stores { get; } = Array.Empty<FrontDoorStoreState>();
        public FrontDoorStore? Find(string name) => throw new NotSupportedException();
        public FrontDoorRoute ResolveServer(string serverName) => throw new NotSupportedException();
        public Task RefreshOnMissAsync(CancellationToken ct) => throw new NotSupportedException();
        public void Hold(string store, TimeSpan span, string why) => throw new NotSupportedException();
    }

    private sealed class CapturingLogger : ILogger<DarlingHealthHostService>
    {
        public List<(LogLevel Level, string Text)> Lines { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add((logLevel, formatter(state, exception)));
    }

    private static async Task<WebApplication> StartRoutesAsync(CollectorRuntimeState collector, IPAddress? remote = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        if (remote is not null)
        {
            app.Use((context, next) =>
            {
                context.Connection.RemoteIpAddress = remote;
                return next(context);
            });
        }

        DarlingHealthHostService.MapRoutes(app, probe => DarlingHealthEndpoints.Answer(probe, collector.Read()));
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    private static int FreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        try
        {
            return ((IPEndPoint)probe.LocalEndpoint).Port;
        }
        finally
        {
            probe.Stop();
        }
    }

    private static DarlingConfig EnabledOn(int port)
    {
        var config = new DarlingConfig();
        config.Health.Enabled = true;
        config.Health.Port = port;
        config.Health.Listen = "127.0.0.1";
        return config;
    }

    [Fact]
    public async Task Livez_And_Readyz_AnswerTheWordOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        var collector = new CollectorRuntimeState();
        collector.PublishCollecting();
        await using var app = await StartRoutesAsync(collector);
        using var client = app.GetTestClient();

        foreach (var path in new[] { "/livez", "/readyz" })
        {
            using var response = await client.GetAsync(path, ct);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("ok", await response.Content.ReadAsStringAsync(ct));
            Assert.Equal("text/plain", response.Content.Headers.ContentType?.MediaType);
            Assert.False(response.Headers.Contains("Set-Cookie"));
        }
    }

    [Fact]
    public async Task StoppedStart_WebAndMcpDown_HealthStillAnswers()
    {
        var ct = TestContext.Current.CancellationToken;
        var collector = new CollectorRuntimeState();
        collector.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        await using var app = await StartRoutesAsync(collector);
        using var client = app.GetTestClient();

        using var live = await client.GetAsync("/livez", ct);
        using var ready = await client.GetAsync("/readyz", ct);

        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal("stopped", await live.Content.ReadAsStringAsync(ct));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
        Assert.Equal("stopped", await ready.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public async Task NoToken_NoAllowFrom()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartRoutesAsync(new CollectorRuntimeState(), IPAddress.Parse("203.0.113.9"));
        using var client = app.GetTestClient();

        using var live = await client.GetAsync("/livez", ct);
        using var ready = await client.GetAsync("/readyz", ct);

        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
    }

    [Fact]
    public async Task UnknownPath_404_WrongMethod_405()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartRoutesAsync(new CollectorRuntimeState());
        using var client = app.GetTestClient();

        foreach (var path in new[] { "/", "/api/ping" })
        {
            using var response = await client.GetAsync(path, ct);
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
        }

        using var post = await client.PostAsync("/livez", new StringContent(""), ct);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
        Assert.Equal("", await post.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public void RegisteredBeforeTheWorker()
    {
        var services = new ServiceCollection();
        DarlingProcessServices.Add(services, null);

        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).ToList();
        var health = hosted.FindIndex(d => d.ImplementationType == typeof(DarlingHealthHostService));
        var worker = hosted.FindIndex(d => d.ImplementationType == typeof(DarlingWorker));

        Assert.True(health >= 0, "the health host is registered");
        Assert.True(worker >= 0, "the worker is registered");
        Assert.True(health < worker, "the health host must start before the worker");
    }

    [Fact]
    public async Task Disabled_StartsNothing()
    {
        var config = new DarlingConfig();
        config.Health.Enabled = false;
        await using var host = new DarlingHealthHostService(new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => config);

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
    }

    [Fact]
    public async Task ConfigWithProblems_StillServesHealth()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        var collector = new CollectorRuntimeState();
        var config = EnabledOn(port);
        collector.PublishConfigurationProblems(config);
        await using var host = new DarlingHealthHostService(collector, new EmptyDirectory(), new CapturingLogger(), () => config);

        await host.StartAsync(ct);

        Assert.True(host.IsListening);
        using var client = new HttpClient();
        using var live = await client.GetAsync($"http://127.0.0.1:{port}/livez", ct);
        using var ready = await client.GetAsync($"http://127.0.0.1:{port}/readyz", ct);
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal("stopped", await live.Content.ReadAsStringAsync(ct));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
        await host.StopAsync(ct);
    }

    [Fact]
    public async Task UnreadableConfig_DoesNotStart_AndLogsWhy()
    {
        var logger = new CapturingLogger();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), logger, () => throw new JsonException("bad json"));

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        var line = Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
        Assert.Contains("health", line.Text);
        Assert.Contains(nameof(JsonException), line.Text);
    }

    [Fact]
    public async Task BindFailure_LogsAndDoesNotThrow()
    {
        var taken = new TcpListener(IPAddress.Loopback, 0);
        taken.Start();
        try
        {
            var port = ((IPEndPoint)taken.LocalEndpoint).Port;
            var logger = new CapturingLogger();
            await using var host = new DarlingHealthHostService(
                new CollectorRuntimeState(), new EmptyDirectory(), logger, () => EnabledOn(port));

            await host.StartAsync(TestContext.Current.CancellationToken);

            Assert.False(host.IsListening);
            var line = Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
            Assert.Contains("127.0.0.1", line.Text);
            Assert.Contains(port.ToString(), line.Text);
        }
        finally
        {
            taken.Stop();
        }
    }
}
