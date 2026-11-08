/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// #8184: the opt-in health port for Kubernetes probes. It reads its own <c>health</c> block (file-only, a change needs a restart),
/// runs a slim Kestrel app on its own port and serves <c>GET /livez</c> and <c>GET /readyz</c>, one word each, with no token, no
/// <c>allowFrom</c> and no TLS. Registered before the worker so it listens before migrations and before any ownership check can stop
/// the start. It does not depend on the config validating: a <c>darling.json</c> that loads but has problems still serves
/// (<c>stopped</c>); only a file that cannot be parsed at all leaves the port off, with one Error line saying why.
/// </summary>
internal sealed class DarlingHealthHostService : IHostedService, IAsyncDisposable
{
    private readonly CollectorRuntimeState _collector;
    private readonly IFrontDoorDirectory _frontDoor;
    private readonly ILogger<DarlingHealthHostService> _logger;
    private readonly Func<DarlingConfig> _loadConfig;
    private WebApplication? _app;

    public DarlingHealthHostService(
        CollectorRuntimeState collector,
        IFrontDoorDirectory frontDoor,
        ILogger<DarlingHealthHostService> logger,
        Func<DarlingConfig>? loadConfig = null)
    {
        _collector = collector;
        _frontDoor = frontDoor;
        _logger = logger;
        _loadConfig = loadConfig ?? (() => DarlingConfig.Load());
    }

    /// <summary>True once Kestrel has started; false when health is disabled or after a config-load or bind failure.</summary>
    internal bool IsListening { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = _loadConfig();
        }
        catch (Exception ex) when (ConfigLoadFailures(ex))
        {
            _logger.LogError(ex, "Health port not started: darling.json could not be read, so the health block is unknown ({ExceptionType}) (#8184)", ex.GetType().Name);
            return;
        }

        var health = config.Health;
        if (!health.Enabled)
        {
            return;
        }

        var address = HealthConfig.ResolveListenAddress(health.Listen, DarlingHostBinding.IsRunningInContainer);
        if (address is null)
        {
            _logger.LogError("Health port not started: health.listen '{Listen}' is not an IP address (#8184)", health.Listen);
            return;
        }

        var isFrontDoor = config.IsFrontDoor;
        WebApplication? app = null;
        try
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(o => o.Listen(address, health.Port));
            app = builder.Build();
            MapRoutes(app, probe => isFrontDoor
                ? DarlingHealthEndpoints.AnswerFrontDoor(probe, _collector.Read(), _frontDoor.Stores)
                : DarlingHealthEndpoints.Answer(probe, _collector.Read()));
            await app.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (BindFailures(ex))
        {
            _logger.LogError(ex, "Health port could not listen on {Address}:{Port} ({ExceptionType}); the service keeps running without it (#8184)", address, health.Port, ex.GetType().Name);
            if (app is not null)
            {
                await app.DisposeAsync().ConfigureAwait(false);
            }

            return;
        }

        _app = app;
        IsListening = true;
        _logger.LogInformation("Health port listening on {Address}:{Port} (/livez, /readyz; no token, no allowFrom) (#8184)", address, health.Port);
    }

    /// <summary>#8184: every failure <c>DarlingConfig.Load</c> and <c>Parse</c> raise for a file the host cannot use: a missing or locked file
    /// (<see cref="IOException"/>, <see cref="UnauthorizedAccessException"/>, <see cref="System.Security.SecurityException"/>), malformed JSON
    /// (<see cref="JsonException"/>), an empty or <c>null</c> file (<see cref="InvalidDataException"/>), and a value or type the deserializer or
    /// the secret resolution refuses (<see cref="NotSupportedException"/>, <see cref="ArgumentException"/>, <see cref="FormatException"/>,
    /// <see cref="InvalidOperationException"/>). The worker treats these as terminal-but-survivable, so the health host must not stop the start.</summary>
    private static bool ConfigLoadFailures(Exception ex) =>
        ex is JsonException or IOException or UnauthorizedAccessException or InvalidDataException or System.Security.SecurityException
            or NotSupportedException or ArgumentException or FormatException or InvalidOperationException;

    /// <summary>#8184: every failure Kestrel's build and start raise for a taken or unusable address: address in use
    /// (<see cref="IOException"/>), <see cref="SocketException"/>, no permission to bind (<see cref="UnauthorizedAccessException"/>), a port out
    /// of range (<see cref="ArgumentException"/>) and Kestrel's own <see cref="InvalidOperationException"/>. A cancelled start is not a failure.</summary>
    private static bool BindFailures(Exception ex) =>
        ex is IOException or SocketException or UnauthorizedAccessException or ArgumentException or InvalidOperationException or NotSupportedException;

    internal static void MapRoutes(IEndpointRouteBuilder app, Func<HealthProbe, HealthAnswer> answer)
    {
        app.MapGet("/livez", context => Write(context, answer(HealthProbe.Live)));
        app.MapGet("/readyz", context => Write(context, answer(HealthProbe.Ready)));
    }

    private static Task Write(HttpContext context, HealthAnswer answer)
    {
        context.Response.StatusCode = answer.StatusCode;
        context.Response.ContentType = "text/plain; charset=utf-8";
        return context.Response.WriteAsync(answer.Word);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var app = _app;
        if (app is null)
        {
            return;
        }

        IsListening = false;
        await app.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        var app = Interlocked.Exchange(ref _app, null);
        IsListening = false;
        if (app is not null)
        {
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }
}
