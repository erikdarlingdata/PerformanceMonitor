/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// #9013: the opt-in Prometheus port. It reads its own <c>metrics</c> block (file-only, a change needs a restart), runs a slim
/// Kestrel app on its own port and serves <c>GET /metrics</c> from an in-memory snapshot that one background refresh rebuilds at
/// most once a minute, so a scrape never reads the store. A bad block turns off only the port, with one Critical line, and
/// collection, web and MCP keep running. No failure here stops the service start. The store login is resolved on the first
/// refresh, not at start: the worker may still be provisioning it.
/// </summary>
internal sealed class DarlingMetricsHostService : IHostedService, IAsyncDisposable
{
    internal const string ApplicationName = "PerformanceMonitor Darling metrics";

    private readonly CollectorRuntimeState _collector;
    private readonly ReadAdmission _admission;
    private readonly MonitoredServerRegistryState _registry;
    private readonly ILogger<DarlingMetricsHostService> _logger;
    private readonly Func<DarlingConfig> _loadConfig;
    private readonly bool? _inContainer;
    private readonly Func<WebApplication, ValueTask> _disposeApp;
    private readonly Func<CancellationToken, Task<NpgsqlDataSource?>>? _dataSourceOverride;
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _stopping = new();
    private readonly SemaphoreSlim _dataSourceGate = new(1, 1);
    private WebApplication? _app;
    private NpgsqlDataSource? _dataSource;
    private volatile bool _dataSourceClosed;
    private bool _reportedUnusableLogin;
    private int _disposed;

    public DarlingMetricsHostService(
        CollectorRuntimeState collector,
        ReadAdmission admission,
        MonitoredServerRegistryState registry,
        ILogger<DarlingMetricsHostService> logger,
        Func<DarlingConfig> loadConfig,
        bool? inContainer = null,
        Func<WebApplication, ValueTask>? disposeApp = null,
        Func<CancellationToken, Task<NpgsqlDataSource?>>? dataSourceOverride = null,
        TimeProvider? time = null)
    {
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        _admission = admission ?? throw new ArgumentNullException(nameof(admission));
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _loadConfig = loadConfig ?? throw new ArgumentNullException(nameof(loadConfig));
        _inContainer = inContainer;
        _disposeApp = disposeApp ?? (app => app.DisposeAsync());
        _dataSourceOverride = dataSourceOverride;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>The running app's services while the port is up, so a test reads back the Kestrel options the built server holds.</summary>
    internal IServiceProvider? Services => _app?.Services;

    /// <summary>True once Kestrel has started; false when metrics is disabled or refused, or after any start failure.</summary>
    internal bool IsListening { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        /* #9013: the health host's catch-all (DarlingHealthHostService.StartAsync): a cancelled start is rethrown, any other failure
           is one Error line, the app is disposed if it was built, and the service start goes on. */
        var reading = true;
        IPAddress? address = null;
        var port = 0;
        WebApplication? app = null;
        try
        {
            var config = _loadConfig();
            reading = false;

            var metrics = config.Metrics;
            if (!metrics.Enabled)
            {
                return;
            }

            if (config.FrontDoor?.Enabled == true)
            {
                /* #9013: a front door holds no store of its own, and the snapshot is a store read. */
                _logger.LogCritical("Metrics port not started: metrics needs a store; not served in front-door mode. Remove the metrics block, or turn it on at each store (#9013)");
                return;
            }

            var check = MetricsConfig.Check(
                metrics,
                config.Web?.Port,
                config.Mcp?.Port,
                config.Health.Enabled ? config.Health.Port : null);
            var inContainer = _inContainer ?? DarlingHostBinding.IsRunningInContainer;
            address = HealthConfig.ResolveListenAddress(metrics.Listen, inContainer);
            if (check.Problems.Count > 0 || address is null)
            {
                _logger.LogCritical(
                    "Metrics port not started: {Problems} Fix the metrics block in darling.json and restart; collection, web and MCP keep running without the port (#9013)",
                    string.Join(" ", check.Problems));
                return;
            }

            if (check.TokenIsLiteral)
            {
                _logger.LogWarning(
                    "metrics.token is set in plaintext in darling.json: prefer env:NAME or file:PATH. This token gates every scrape of the metrics port (#9013)");
            }

            port = metrics.Port;
            var refresher = new DarlingMetricsRefresher(
                _dataSourceOverride ?? (ct => GetDataSourceAsync(config, ct)), _admission, _registry, _collector, _time, _logger);
            var cache = new DarlingMetricsSnapshotCache(refresher.RefreshAsync, refresher.CanRefresh, _time, _logger, _stopping.Token);

            /* #9013: the health host's pins (#4281, #5288): Production, so an ambient ASPNETCORE_ENVIRONMENT=Development cannot put the
               developer exception page on this listener, and an empty Kestrel configuration, so a Kestrel__Endpoints__* variable or an
               appsettings file cannot add a second listener. The limits are the health port's: a scrape is a short GET with no body. */
            var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
            builder.Logging.ClearProviders();
            var listenAddress = address;
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Configure(new ConfigurationBuilder().Build());
                DarlingHealthHostService.ApplyLimits(options);
                options.Listen(listenAddress, port, l => l.Protocols = HttpProtocols.Http1);
            });
            app = builder.Build();
            MapRoutes(app, context =>
            {
                var body = PrometheusText.Write(cache.Scrape(), _time.GetUtcNow().UtcDateTime);
                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = PrometheusText.ContentType;
                return context.Response.WriteAsync(body);
            });
            await app.StartAsync(cancellationToken).ConfigureAwait(false);

            _app = app;
            IsListening = true;
            _logger.LogInformation("Metrics port listening on {Urls} (/metrics) (#9013)", string.Join(", ", app.Urls));

            if (!IPAddress.IsLoopback(address) && check.AllowList.Count == 0)
            {
                /* #9013: after the bind, so the log never says bound and then fails. Without allowFrom every host that can reach this
                   address can try the token, and there is no TLS, so the token crosses the network in clear. */
                _logger.LogWarning(
                    "Metrics port is bound to {Address}, not loopback, and metrics.allowFrom is empty: it is guarded by the token alone, over plain HTTP. Set metrics.allowFrom to the scraper's addresses (#9013)",
                    address);
            }
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
                _logger.LogError(ex, "Metrics port not started: darling.json could not be read or parsed, so the metrics block is unknown ({ExceptionType}) (#9013)", ex.GetType().Name);
            }
            else
            {
                _logger.LogError(ex, "Metrics port could not start on {Address}:{Port} ({ExceptionType}); the service keeps running without it (#9013)", address, port, ex.GetType().Name);
            }

            await DisposeFailedStartAsync(app).ConfigureAwait(false);
        }
    }

    internal static void MapRoutes(IEndpointRouteBuilder app, Func<HttpContext, Task> answer)
    {
        /* #9013: GET only. Routing answers any other method on this path with an empty 405, and any other path with an empty 404. */
        app.MapGet("/metrics", context => answer(context));
    }

    /// <summary>
    /// #9013: the store data source, built at most once, on the first refresh that needs it. A login that is not there yet (the
    /// worker has not provisioned it, or a managed credential is not stored) gives null, which fails that refresh; the next
    /// refresh, a minute later, tries again.
    /// </summary>
    private async Task<NpgsqlDataSource?> GetDataSourceAsync(DarlingConfig config, CancellationToken ct)
    {
        var existing = Volatile.Read(ref _dataSource);
        if (existing is not null)
        {
            return existing;
        }

        await _dataSourceGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_dataSourceClosed)
            {
                return null;
            }

            if (_dataSource is not null)
            {
                return _dataSource;
            }

            string? connectionString;
            if (config.Postgres.Managed)
            {
                connectionString = OperatingSystem.IsWindows()
                    ? DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential(config.Postgres)
                    : null;
            }
            else
            {
                connectionString = await DarlingStoreLogins.ResolveUnmanagedAsync(
                    DarlingStoreLogins.Surface.Web, config.Postgres, _logger, ct).ConfigureAwait(false);
            }

            if (connectionString is null)
            {
                return null;
            }

            /* #9013: the web host's guard (DarlingWebHostService): Npgsql's own parse error can quote part of the string, the
               password included, so prove the string first and log the formatter's text instead. Once, not every minute. */
            if (DarlingStoreConnection.TryDescribeParseFailure(connectionString) is { } unusable)
            {
                if (!_reportedUnusableLogin)
                {
                    _reportedUnusableLogin = true;
                    _logger.LogError(
                        "Metrics refresh cannot open the store: its connection string (postgres.webConnectionString, or postgres.connectionString when that is not set) is unusable: {Detail} (#9013)",
                        unusable);
                }

                return null;
            }

            var created = DarlingWorker.CreateHostStoreDataSource(connectionString, ApplicationName);
            Volatile.Write(ref _dataSource, created);

            /* #9013: a stop that ran while the login resolved has already closed the source it saw; close this one too. */
            if (_dataSourceClosed)
            {
                await CloseDataSourceAsync().ConfigureAwait(false);
                return null;
            }

            return created;
        }
        finally
        {
            _dataSourceGate.Release();
        }
    }

    private async Task CloseDataSourceAsync()
    {
        _dataSourceClosed = true;
        var dataSource = Interlocked.Exchange(ref _dataSource, null);
        if (dataSource is not null)
        {
            /* #9013: best-effort, as the app's teardown below: a pool that fails to close must not fail the service stop. */
            try { await dataSource.DisposeAsync().ConfigureAwait(false); } catch { /* best-effort */ }
        }
    }

    private void CancelRefresh()
    {
        if (Volatile.Read(ref _disposed) == 0)
        {
            _stopping.Cancel();
        }
    }

    private async Task DisposeFailedStartAsync(WebApplication? app)
    {
        IsListening = false;
        _app = null;
        if (app is not null)
        {
            /* #9013: the health host's guard: a second Kestrel failure on teardown must not leave StartAsync, or the service start
               stops and the worker never runs. */
            try { await _disposeApp(app).ConfigureAwait(false); } catch { /* best-effort */ }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        CancelRefresh();
        var app = _app;
        if (app is not null)
        {
            IsListening = false;
            await app.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        await CloseDataSourceAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        CancelRefresh();
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var app = Interlocked.Exchange(ref _app, null);
        IsListening = false;
        if (app is not null)
        {
            try { await _disposeApp(app).ConfigureAwait(false); } catch { /* best-effort */ }
        }

        await CloseDataSourceAsync().ConfigureAwait(false);
        _stopping.Dispose();
    }
}
