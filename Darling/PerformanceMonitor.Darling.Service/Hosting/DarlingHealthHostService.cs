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
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// #8184: the opt-in health port for Kubernetes probes. It reads its own <c>health</c> block (file-only, a change needs a restart),
/// runs a slim Kestrel app on its own port and serves <c>GET /livez</c> and <c>GET /readyz</c>, one word each, with no token, no
/// <c>allowFrom</c> and no TLS. Registered before the worker so it listens before migrations and before any ownership check can stop
/// the start. It reads only the blocks it needs and resolves no secret, so a <c>darling.json</c> that fails <c>Validate()</c>, or
/// names a store secret that cannot be resolved, still serves (<c>stopped</c>). A bad <c>health</c> block turns off only the port,
/// with one Critical line, and collection keeps running (D-validate). Only a file that cannot be read or parsed at all leaves the
/// port off with an Error line. No failure here stops the service start.
/// </summary>
internal sealed class DarlingHealthHostService : IHostedService, IAsyncDisposable
{
    private readonly CollectorRuntimeState _collector;
    private readonly IFrontDoorDirectory _frontDoor;
    private readonly ILogger<DarlingHealthHostService> _logger;
    private readonly Func<string> _readConfigText;
    private WebApplication? _app;

    public DarlingHealthHostService(
        CollectorRuntimeState collector,
        IFrontDoorDirectory frontDoor,
        ILogger<DarlingHealthHostService> logger,
        Func<string>? readConfigText = null)
    {
        _collector = collector;
        _frontDoor = frontDoor;
        _logger = logger;
        _readConfigText = readConfigText ?? (() => File.ReadAllText(DarlingConfig.ResolveConfigPath()));
    }

    /// <summary>True once Kestrel has started; false when health is disabled or refused, or after any start failure.</summary>
    internal bool IsListening { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        /* #8184: one catch-all, the sibling hosts' shape (DarlingMcpHostService, DarlingWebHostService): a cancelled start is rethrown,
           any other failure is one Error line, the app is disposed if it was built, and the service start goes on. `reading` picks the
           Error line: while the file is being read the host does not know its own address yet. */
        var reading = true;
        IPAddress? address = null;
        var port = 0;
        WebApplication? app = null;
        try
        {
            var file = HealthFile.Parse(_readConfigText());
            reading = false;

            var health = file.Health;
            if (!health.Enabled)
            {
                return;
            }

            var problems = HealthConfig.Problems(health, file.WebPort, file.McpPort);
            address = HealthConfig.ResolveListenAddress(health.Listen, DarlingHostBinding.IsRunningInContainer);
            if (problems.Count > 0 || address is null)
            {
                _logger.LogCritical(
                    "Health port not started: {Problems} Fix the health block in darling.json and restart; collection keeps running without the port (#8184)",
                    string.Join(" ", problems));
                return;
            }

            port = health.Port;
            var isFrontDoor = file.FrontDoor?.Enabled ?? false;

            /* #8184: the sibling hosts' pins. EnvironmentName: an ambient ASPNETCORE_ENVIRONMENT=Development must not put the developer
               exception page on an unauthenticated listener (#4281 review, finding 4). The empty Kestrel configuration: a
               Kestrel__Endpoints__* variable or an appsettings file must not add a second listener or fail the start (#5288). */
            var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
            builder.Logging.ClearProviders();
            var listenAddress = address;
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Configure(new ConfigurationBuilder().Build());
                options.Listen(listenAddress, port);
            });
            app = builder.Build();
            MapRoutes(app, probe => isFrontDoor
                ? DarlingHealthEndpoints.AnswerFrontDoor(probe, _collector.Read(), _frontDoor.Stores)
                : DarlingHealthEndpoints.Answer(probe, _collector.Read()));
            await app.StartAsync(cancellationToken).ConfigureAwait(false);

            _app = app;
            IsListening = true;
            _logger.LogInformation("Health port listening on {Urls} (/livez, /readyz; no token, no allowFrom) (#8184)", string.Join(", ", app.Urls));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await DisposeFailedStartAsync(app).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            if (reading)
            {
                _logger.LogError(ex, "Health port not started: darling.json could not be read or parsed, so the health block is unknown ({ExceptionType}) (#8184)", ex.GetType().Name);
            }
            else
            {
                _logger.LogError(ex, "Health port could not start on {Address}:{Port} ({ExceptionType}); the service keeps running without it (#8184)", address, port, ex.GetType().Name);
            }

            await DisposeFailedStartAsync(app).ConfigureAwait(false);
        }
    }

    private async Task DisposeFailedStartAsync(WebApplication? app)
    {
        IsListening = false;
        _app = null;
        if (app is not null)
        {
            await app.DisposeAsync().ConfigureAwait(false);
        }
    }

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

    /// <summary>
    /// #8184: the only part of darling.json the health host reads: <c>health</c>, <c>frontDoor.enabled</c>, and <c>web.port</c> and
    /// <c>mcp.port</c> for the clash check. Read with <see cref="DarlingConfig.JsonOptions"/> (the same comments, trailing commas and
    /// case-insensitive names as <see cref="DarlingConfig.Parse"/>), and nothing else is resolved: no <c>env:</c>/<c>file:</c>
    /// secret, no owned-secret set. So a store secret that does not resolve leaves the worker stopped and this port answering.
    /// </summary>
    private sealed class HealthFile
    {
        private HealthConfig _health = new();

        [JsonPropertyName("health")]
        public HealthConfig Health
        {
            get => _health;

            /* "health": null means off, the same rule as DarlingConfig.Health (#5460). */
            set => _health = value ?? new();
        }

        [JsonPropertyName("frontDoor")]
        public EnabledBlock? FrontDoor { get; set; }

        [JsonPropertyName("web")]
        public PortBlock? Web { get; set; } = new();

        [JsonPropertyName("mcp")]
        public PortBlock? Mcp { get; set; } = new();

        /// <summary><c>web.port</c> as the file gives it (the web default when the block or the key is absent), or null for <c>"web": null</c>,
        /// which <see cref="HealthConfig.Problems"/> skips as the old check did.</summary>
        public int? WebPort => Web is null ? null : Web.Port ?? new WebConfig().Port;

        /// <summary><c>mcp.port</c> the same way as <see cref="WebPort"/>.</summary>
        public int? McpPort => Mcp is null ? null : Mcp.Port ?? new McpConfig().Port;

        public static HealthFile Parse(string json) =>
            JsonSerializer.Deserialize<HealthFile>(json, DarlingConfig.JsonOptions)
            ?? throw new InvalidDataException("darling.json is empty or contains only 'null'.");
    }

    private sealed class EnabledBlock
    {
        [JsonPropertyName("enabled")]
        public bool Enabled { get; set; }
    }

    private sealed class PortBlock
    {
        [JsonPropertyName("port")]
        public int? Port { get; set; }
    }
}
