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
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8184: the health host's routes, its start order, and its behaviour for a missing, broken or unreadable config, a bad health
/// block, a store secret that does not resolve, and a taken port. Touches no process-wide state (no statics, no environment variables;
/// the one file is a per-test temp file): every collaborator is a per-test instance and the real-port tests bind loopback on a port
/// the test obtained, so the class does not join the statics census. The ambient-configuration test, which sets an environment
/// variable, is <see cref="DarlingHealthHostAmbientConfigTests"/>.
/// </summary>
public sealed class DarlingHealthHostTests
{
    internal sealed class EmptyDirectory : IFrontDoorDirectory
    {
        public IReadOnlyList<FrontDoorStoreState> Stores { get; } = Array.Empty<FrontDoorStoreState>();
        public FrontDoorStore? Find(string name) => throw new NotSupportedException();
        public FrontDoorRoute ResolveServer(string serverName) => throw new NotSupportedException();
        public Task RefreshOnMissAsync(CancellationToken ct) => throw new NotSupportedException();
        public void Hold(string store, TimeSpan span, string why) => throw new NotSupportedException();
    }

    internal sealed class CapturingLogger : ILogger<DarlingHealthHostService>
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

    internal static int FreePort()
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

    internal static string EnabledOn(int port, string listen = "127.0.0.1") =>
        $"{{\"health\":{{\"enabled\":true,\"port\":{port},\"listen\":\"{listen}\"}}}}";

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
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => "{\"health\":{\"enabled\":false}}");

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
    }

    [Fact]
    public async Task ConfigWithProblems_StillServesHealth()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        var collector = new CollectorRuntimeState();
        collector.PublishConfigurationProblems(new DarlingConfig());
        var logger = new CapturingLogger();
        await using var host = new DarlingHealthHostService(collector, new EmptyDirectory(), logger, () => EnabledOn(port));

        await host.StartAsync(ct);

        Assert.True(host.IsListening);
        /* M1: the line names the address Kestrel bound, not only the configured one. */
        Assert.Contains(logger.Lines, l => l.Level == LogLevel.Information && l.Text.Contains($"127.0.0.1:{port}", StringComparison.Ordinal));
        using var client = new HttpClient();
        using var live = await client.GetAsync($"http://127.0.0.1:{port}/livez", ct);
        using var ready = await client.GetAsync($"http://127.0.0.1:{port}/readyz", ct);
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal("stopped", await live.Content.ReadAsStringAsync(ct));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
        await host.StopAsync(ct);
    }

    private static async Task AssertConfigFailureDoesNotStart(Exception failure)
    {
        var logger = new CapturingLogger();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), logger, () => throw failure);

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        var line = Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
        Assert.Contains("health", line.Text);
        Assert.Contains(failure.GetType().Name, line.Text);
    }

    [Fact]
    public Task UnauthorizedConfig_DoesNotStart_AndLogsWhy() =>
        AssertConfigFailureDoesNotStart(new UnauthorizedAccessException("darling.json is restricted"));

    [Fact]
    public Task EmptyConfig_DoesNotStart_AndLogsWhy() =>
        AssertConfigFailureDoesNotStart(new System.IO.InvalidDataException("darling.json is empty"));

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

    [Fact]
    public Task AnyStartFailure_DoesNotThrow() =>
        AssertConfigFailureDoesNotStart(new Exception("anything at all"));

    /// <summary>#8184 I1: the host reads only its own blocks and resolves no secret, so a store secret that does not resolve (the
    /// worker stops on it) leaves the port answering.</summary>
    [Fact]
    public async Task UnresolvableStoreSecret_StillServesHealth()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        var unset = $"DARLING_8184_UNSET_{Guid.NewGuid():N}";
        Assert.Null(Environment.GetEnvironmentVariable(unset));
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"darling-health-{Guid.NewGuid():N}.json");
        System.IO.File.WriteAllText(path, $$"""
            {
              // a comment and a trailing comma, as darling.json allows
              "postgres": { "connectionString": "env:{{unset}}" },
              "health": { "enabled": true, "port": {{port}}, "listen": "127.0.0.1" },
            }
            """);
        try
        {
            Assert.ThrowsAny<Exception>(() => DarlingConfig.Parse(System.IO.File.ReadAllText(path)));

            var logger = new CapturingLogger();
            await using var host = new DarlingHealthHostService(
                new CollectorRuntimeState(), new EmptyDirectory(), logger, () => System.IO.File.ReadAllText(path));

            await host.StartAsync(ct);

            Assert.True(host.IsListening);
            Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Error);
            using var client = new HttpClient();
            using var live = await client.GetAsync($"http://127.0.0.1:{port}/livez", ct);
            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
            Assert.Equal("starting", await live.Content.ReadAsStringAsync(ct));
            await host.StopAsync(ct);
        }
        finally
        {
            System.IO.File.Delete(path);
        }
    }

    /// <summary>#8184 I2: <c>"health": null</c> is off, the same as an absent block, and never stops the start.</summary>
    [Fact]
    public async Task NullHealth_IsOff_AndDoesNotThrow()
    {
        var logger = new CapturingLogger();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), logger, () => "{\"health\": null}");

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        Assert.Empty(logger.Lines);
    }

    private static async Task AssertRefused(string json, params string[] named)
    {
        var logger = new CapturingLogger();
        await using var host = new DarlingHealthHostService(new CollectorRuntimeState(), new EmptyDirectory(), logger, () => json);

        await host.StartAsync(TestContext.Current.CancellationToken);

        Assert.False(host.IsListening);
        var line = Assert.Single(logger.Lines);
        Assert.Equal(LogLevel.Critical, line.Level);
        foreach (var setting in named)
        {
            Assert.Contains(setting, line.Text, StringComparison.Ordinal);
        }
    }

    /// <summary>#8184 I5 (ruling R9): a health port equal to web.port or mcp.port, set in the file or left at its default, is refused by
    /// the host with one Critical line naming both settings; <c>Validate()</c> does not see it.</summary>
    [Theory]
    [InlineData("web", "{\"web\":{\"port\":6100},\"health\":{\"enabled\":true,\"port\":6100,\"listen\":\"127.0.0.1\"}}")]
    [InlineData("mcp", "{\"mcp\":{\"port\":6101},\"health\":{\"enabled\":true,\"port\":6101,\"listen\":\"127.0.0.1\"}}")]
    [InlineData("web", "{\"health\":{\"enabled\":true,\"port\":5153,\"listen\":\"127.0.0.1\"}}")]
    [InlineData("mcp", "{\"health\":{\"enabled\":true,\"port\":5152,\"listen\":\"127.0.0.1\"}}")]
    public Task HealthPortClash_IsRefusedByTheHost(string surface, string json) =>
        AssertRefused(json, "health.port", $"{surface}.port");

    /// <summary>#8184 I5 and M1: a port outside 1..65535 is refused, port 0 included, so the host never binds an ephemeral port.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65536)]
    public Task HealthPortOutOfRange_IsRefusedByTheHost(int port) =>
        AssertRefused(EnabledOn(port), "health.port", "1 and 65535");

    [Fact]
    public Task HealthListenUnparsable_IsRefusedByTheHost() =>
        AssertRefused(EnabledOn(6102, "nope"), "health.listen");

    /// <summary>#8184 review 1, item 1: a second Kestrel failure while a failed start is cleaned up must not leave StartAsync, or the
    /// service start stops and the worker never runs. The port is taken, so the start fails after the app is built; the injected dispose
    /// really disposes it and then throws.</summary>
    [Fact]
    public async Task FailedStart_WhoseTeardownThrows_StillDoesNotThrow()
    {
        var taken = new TcpListener(IPAddress.Loopback, 0);
        taken.Start();
        try
        {
            var port = ((IPEndPoint)taken.LocalEndpoint).Port;
            var logger = new CapturingLogger();
            var disposeCalls = 0;
            await using var host = new DarlingHealthHostService(
                new CollectorRuntimeState(), new EmptyDirectory(), logger, () => EnabledOn(port),
                disposeApp: async app =>
                {
                    disposeCalls++;
                    await app.DisposeAsync();
                    throw new InvalidOperationException("Kestrel failed again on teardown");
                });

            await host.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(1, disposeCalls);
            Assert.False(host.IsListening);
            Assert.Single(logger.Lines, l => l.Level == LogLevel.Error);
        }
        finally
        {
            taken.Stop();
        }
    }

    [Fact]
    public async Task DisposeAsync_WhoseTeardownThrows_DoesNotThrow()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => EnabledOn(port),
            disposeApp: async app =>
            {
                await app.DisposeAsync();
                throw new InvalidOperationException("Kestrel failed on teardown");
            });
        await host.StartAsync(ct);
        Assert.True(host.IsListening);

        await host.DisposeAsync();

        Assert.False(host.IsListening);
    }

    /// <summary>#8184 review 1, item 2: the limits the built server holds are the ones in the table, read back through the options.</summary>
    [Fact]
    public async Task KestrelLimits_AreTheDocumentedOnes()
    {
        var port = FreePort();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => EnabledOn(port));
        await host.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(host.IsListening);

        var options = host.Services!.GetRequiredService<IOptions<KestrelServerOptions>>().Value;

        Assert.False(options.AddServerHeader);
        Assert.Equal(64, options.Limits.MaxConcurrentConnections);
        Assert.Equal(TimeSpan.FromSeconds(15), options.Limits.KeepAliveTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), options.Limits.RequestHeadersTimeout);
        Assert.Equal(8 * 1024, options.Limits.MaxRequestHeadersTotalSize);
        Assert.Equal(0, options.Limits.MaxRequestBodySize);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>#8184 review 1, item 2: a real response carries no Server header, and the listener speaks HTTP/1 only (an HTTP/2
    /// prior-knowledge request is refused).</summary>
    [Fact]
    public async Task RealResponse_HasNoServerHeader_AndHttp2IsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => EnabledOn(port));
        await host.StartAsync(ct);
        Assert.True(host.IsListening);

        using var client = new HttpClient();
        using var live = await client.GetAsync($"http://127.0.0.1:{port}/livez", ct);
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal(new Version(1, 1), live.Version);
        Assert.False(live.Headers.Contains("Server"));

        using var h2 = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{port}/livez")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        await Assert.ThrowsAnyAsync<HttpRequestException>(() => client.SendAsync(h2, ct));
        await host.StopAsync(ct);
    }

    private static async Task<DarlingHealthHostService> StartWithAsync(CapturingLogger logger, string listen, bool inContainer)
    {
        var port = FreePort();
        var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), logger, () => EnabledOn(port, listen), inContainer: inContainer);
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    /// <summary>#8184 review 1, item 3: an empty listen that resolved wide through the container default gets one Warning naming the
    /// container variable, and still exactly one Information line for the bound address.</summary>
    [Fact]
    public async Task EmptyListen_InContainer_WarnsNamingTheContainerVariable()
    {
        var logger = new CapturingLogger();
        await using var host = await StartWithAsync(logger, "", inContainer: true);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("DOTNET_RUNNING_IN_CONTAINER", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("health.listen", warning.Text, StringComparison.Ordinal);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    [Theory]
    [InlineData("0.0.0.0")]
    [InlineData("::")]
    public async Task WideListen_WarnsNamingHealthListen(string listen)
    {
        var logger = new CapturingLogger();
        await using var host = await StartWithAsync(logger, listen, inContainer: false);

        Assert.True(host.IsListening);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("health.listen", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("DOTNET_RUNNING_IN_CONTAINER", warning.Text, StringComparison.Ordinal);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    [Theory]
    [InlineData("", false)]
    [InlineData("127.0.0.1", false)]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", false)]
    public async Task LoopbackListen_GivesNoWarning(string listen, bool inContainer)
    {
        var logger = new CapturingLogger();
        await using var host = await StartWithAsync(logger, listen, inContainer);

        Assert.True(host.IsListening);
        Assert.DoesNotContain(logger.Lines, l => l.Level >= LogLevel.Warning);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information);
    }

    /// <summary>#8184 review 1, item 4: every other method on either route is a 405 with an empty body.</summary>
    [Theory]
    [InlineData("HEAD", "/livez")]
    [InlineData("OPTIONS", "/livez")]
    [InlineData("PUT", "/livez")]
    [InlineData("DELETE", "/livez")]
    [InlineData("PATCH", "/livez")]
    [InlineData("HEAD", "/readyz")]
    [InlineData("OPTIONS", "/readyz")]
    [InlineData("PUT", "/readyz")]
    [InlineData("DELETE", "/readyz")]
    [InlineData("PATCH", "/readyz")]
    public async Task OtherMethods_Are405WithAnEmptyBody(string method, string path)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartRoutesAsync(new CollectorRuntimeState());
        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(new HttpMethod(method), path);

        using var response = await client.SendAsync(request, ct);

        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
    }

    /// <summary>One raw HTTP/1.0 request (no chunked body to decode) to the real listener; the target goes out exactly as given, so Kestrel is what sees it.</summary>
    private static async Task<(int Status, string Body)> RawGetAsync(int port, string target, CancellationToken ct)
    {
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, ct);
        var stream = tcp.GetStream();
        var request = System.Text.Encoding.ASCII.GetBytes($"GET {target} HTTP/1.0\r\n\r\n");
        await stream.WriteAsync(request, ct);
        using var reader = new System.IO.StreamReader(stream, System.Text.Encoding.ASCII);
        var all = await reader.ReadToEndAsync(ct);
        var split = all.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        var head = all[..split];
        var body = all[(split + 4)..];
        var status = int.Parse(head.Split(' ')[1]);
        return (status, body);
    }

    /// <summary>#8184 review 1, item 4: what each spelling of the path gets from the real Kestrel listener today, sent as a raw request
    /// target so no client resolves a dot segment or decodes the path first. The service has not published a state, so the word is
    /// <c>starting</c>.</summary>
    [Theory]
    [InlineData("/LIVEZ", 200, "starting")]
    [InlineData("/livez/", 200, "starting")]
    [InlineData("/livez?x=1", 200, "starting")]
    [InlineData("/%6civez", 200, "starting")]
    [InlineData("/x/../livez", 200, "starting")]
    [InlineData("/livez/../readyz", 503, "starting")]
    public async Task PathSpellings_GetTheAnswerPinnedToday(string target, int status, string body)
    {
        var ct = TestContext.Current.CancellationToken;
        var port = FreePort();
        await using var host = new DarlingHealthHostService(
            new CollectorRuntimeState(), new EmptyDirectory(), new CapturingLogger(), () => EnabledOn(port));
        await host.StartAsync(ct);
        Assert.True(host.IsListening);

        var answer = await RawGetAsync(port, target, ct);

        Assert.Equal(status, answer.Status);
        Assert.Equal(body, answer.Body);
        await host.StopAsync(ct);
    }
}
