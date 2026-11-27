/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
using Microsoft.Extensions.Logging.Abstractions;
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
    private const string RefusalSurface = "Metrics";

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
    private bool _loginResolveFailing;
    private volatile DarlingConfig? _config;
    private DarlingWebTls.LoadedCertificate? _certificate;
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

            /* #9326: the web certificate, when web.network.tls is set, so the token in the Authorization header is not sent in the
               clear. Loaded through the web and MCP listeners' own path (DarlingListenerTls.Resolve), with a state of its own so the
               web host's certificate alert is neither published nor cleared from here. Read at start, as the web listener reads it:
               a renewed certificate is picked up on the next restart. A block that is set but unusable keeps the port off: the
               operator asked for TLS, and a plain-HTTP port is not what they asked for. No block set keeps plain HTTP. */
            var webTls = config.Web?.Network?.Tls;
            if (DarlingWebTls.Describe(webTls, ListenerTlsLabels.Metrics.Section).Shape != DarlingWebTls.TlsShape.NotConfigured)
            {
                var tlsOutcome = DarlingListenerTls.Resolve(
                    _logger, new MetricsTlsCertificateState(), ListenerTlsLabels.Metrics, webTls, address, port, null);
                _certificate = tlsOutcome.Certificate;
                if (!tlsOutcome.Expose || tlsOutcome.ExposesWithoutItsCertificate)
                {
                    ReleaseCertificate();
                    /* A refusal already logged its one Critical line inside Resolve, ending "the metrics port stays off". Only the broken
                       invariant (exposing with no certificate), which Resolve never returns, has logged nothing yet. */
                    if (tlsOutcome.Expose)
                    {
                        _logger.LogCritical(
                            "Metrics port TLS returned no certificate for web.network.tls: the metrics port stays off (#9326)");
                    }

                    return;
                }
            }

            var listenerCertificate = _certificate;
            _config = config;
            var refresher = new DarlingMetricsRefresher(
                _dataSourceOverride ?? StoreDataSourceAsync, _admission, _registry, _collector, _time, _logger);
            var cache = new DarlingMetricsSnapshotCache(refresher.RefreshAsync, refresher.CanRefresh, _time, _logger, _stopping.Token);

            /* #9013: the health host's pins (#4281, #5288): Production, so an ambient ASPNETCORE_ENVIRONMENT=Development cannot put the
               developer exception page on this listener, and an empty Kestrel configuration, so a Kestrel__Endpoints__* variable or an
               appsettings file cannot add a second listener. The limits are the health port's: a scrape is a short GET with no body. */
            var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
            {
                EnvironmentName = Environments.Production,
                /* #9837: not the working folder, which the builder would watch with every folder under it. */
                ContentRootPath = DarlingHostBinding.ContentRoot,
            });
            /* #9013: the slim builder still registers the framework's forwarded-headers filter, which ASPNETCORE_FORWARDEDHEADERS_ENABLED
               turns on; pinned off as on the web and MCP hosts (#5288), so metrics.allowFrom and the token throttle key on the TCP peer. */
            DarlingMcpHostService.PinForwardedHeadersOff(builder.Services);
            builder.Logging.ClearProviders();
            var listenAddress = address;
            builder.WebHost.ConfigureKestrel(options =>
            {
                options.Configure(new ConfigurationBuilder().Build());
                DarlingHealthHostService.ApplyLimits(options);
                options.Listen(listenAddress, port, l =>
                {
                    l.Protocols = HttpProtocols.Http1;
                    if (listenerCertificate is not null)
                    {
                        /* #9326: the leaf and its intermediates, through the body the web and MCP listeners share. */
                        l.UseHttps(https => DarlingListenerTls.ConfigureHttps(https, listenerCertificate.Value));
                    }
                });
            });
            app = builder.Build();
            /* #9013: one refusal log for this host, so its per-source rate limit spans every request the port answers. */
            var refusals = new DarlingHttpRefusalLog();
            /* #9013: the bearer gate's per-source wait after repeated wrong tokens, as on MCP: one per started port, so a restart starts
               clean and no static outlives the listener that earned the failures. */
            var tokenThrottle = new DarlingSignInThrottle();
            MapRoutes(app, context => Answer(context, check, cache, refusals, tokenThrottle, _logger, _time));
            await app.StartAsync(cancellationToken).ConfigureAwait(false);

            _app = app;
            IsListening = true;
            _logger.LogInformation("Metrics port listening on {Urls} (/metrics) (#9013)", string.Join(", ", app.Urls));

            if (!IPAddress.IsLoopback(address) && (listenerCertificate is null || check.AllowList.Count == 0))
            {
                /* #9013, #9326: after the bind, so the log never says bound and then fails. Without TLS the token in the Authorization
                   header crosses the network in clear, whatever allowFrom holds; without allowFrom every host that can reach this
                   address can try the token. One line names whichever of the two apply. */
                _logger.LogWarning("{Warning} (#9013, #9326)", BindWarning(address, listenerCertificate is null, check.AllowList.Count == 0));
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

    /// <summary>
    /// #9013: one <c>GET /metrics</c>, gated in the MCP host's order (#5288): an address outside <c>metrics.allowFrom</c> is a 403
    /// whatever token it sends, then a missing or wrong bearer token is a 401, both with an empty body; only then the scrape. The
    /// address is the TCP peer: forwarded headers are never read, and loopback is always admitted, as on web and MCP. An empty
    /// <c>allowFrom</c> means no address filter, so the list is not asked at all (an empty list admits nobody). A presented bearer token
    /// goes through <paramref name="tokenThrottle"/> first, as on MCP: a source held after repeated wrong tokens gets 429 with
    /// <c>Retry-After</c> and nothing is compared.
    /// </summary>
    internal static async Task Answer(
        HttpContext context, MetricsCheck check, DarlingMetricsSnapshotCache cache, DarlingHttpRefusalLog refusals,
        DarlingSignInThrottle tokenThrottle, ILogger logger, TimeProvider time)
    {
        var remote = context.Connection.RemoteIpAddress;
        if (check.AllowList.Count > 0 && !DarlingHostBinding.IsRemoteAddressAllowed(remote, check.AllowList))
        {
            refusals.Report(
                logger, RefusalSurface, DarlingRefusalGate.SourceCidr, StatusCodes.Status403Forbidden, remote,
                $"its address is outside metrics.allowFrom ({check.AllowList})",
                time.GetUtcNow().UtcDateTime);
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        var authorization = context.Request.Headers.Authorization.ToString();
        var presented = DarlingMcpHostService.ExtractBearerToken(authorization);

        /* #9013: the MCP bearer gate's wait (DarlingSignInThrottle): the port is reachable before a credential, and in a container it
           listens on every address with no allowFrom by default, so without it a token could be tried at line rate. Only a presented
           Bearer is an attempt; it is counted in one atomic step before the comparison, and a held source compares nothing, a right
           token included. The wait is per source address and kept in memory: a restart clears it. */
        var attempt = default(SignInAttempt);
        if (presented is not null)
        {
            Func<DateTime> clock = () => time.GetUtcNow().UtcDateTime;
            attempt = await tokenThrottle.BeginAttemptAsync(
                DarlingSignInThrottle.SourceKey(remote), clock, context.RequestAborted).ConfigureAwait(false);
            if (attempt.IsHeld)
            {
                var seconds = (int)Math.Ceiling(attempt.RetryAfter.TotalSeconds);
                var totalHold = attempt.Hold == SignInHold.Total;
                refusals.Report(
                    logger, RefusalSurface, DarlingRefusalGate.Token, StatusCodes.Status429TooManyRequests, remote,
                    totalHold
                        ? $"requests are held for {seconds} more second(s): {DarlingSignInThrottle.GlobalMaxAttempts} wrong bearer tokens arrived within the hour from several addresses, so no token is compared from an address that has not presented a correct one recently; restarting the service clears it, and the failing client is in this log with its 401 lines (fix its token or take it out of metrics.allowFrom)"
                        : $"requests from this source are held for {seconds} more second(s) after repeated wrong bearer tokens; no token is compared during the wait, and restarting the service clears it",
                    clock(),
                    state: totalHold ? "held-total" : "held-source");
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                context.Response.Headers.RetryAfter = seconds.ToString(CultureInfo.InvariantCulture);
                context.Response.ContentType = "text/plain; charset=utf-8";
                await context.Response.WriteAsync(
                    totalHold
                        ? $"Bearer tokens are held for {seconds} seconds: {DarlingSignInThrottle.GlobalMaxAttempts} wrong tokens arrived in the last hour from several addresses, "
                          + "not necessarily this one. No token is compared from an address that has not presented a correct one recently. "
                          + "The hold is kept in memory: restarting the service clears it. Find the client that sends the wrong token and fix it or take it out of metrics.allowFrom, or the hold returns."
                        : $"Too many wrong bearer tokens from this address. Try again in {seconds} seconds. "
                          + "No token is compared during the wait. The wait is kept in memory: restarting the service clears it.").ConfigureAwait(false);
                return;
            }
        }

        if (presented is null || !DarlingHostBinding.FixedTimeTokenEquals(presented, check.Token))
        {
            /* The reserved attempt's miss is recorded now (a missing Bearer reserved nothing, so this does nothing for it). */
            tokenThrottle.Missed(attempt);
            /* #9013: the MCP host's three states, read from the header's shape only, so the line never holds what was presented. */
            var refusal = presented is null
                ? DarlingMcpHostService.DescribeBearerRefusal(authorization)
                : "the presented bearer token does not match metrics.token";
            refusals.Report(
                logger, RefusalSurface, DarlingRefusalGate.Token, StatusCodes.Status401Unauthorized, remote,
                refusal,
                time.GetUtcNow().UtcDateTime,
                state: refusal);
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.Headers.WWWAuthenticate = "Bearer";
            return;
        }

        tokenThrottle.Accepted(attempt);
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = PrometheusText.ContentType;
        await context.Response.WriteAsync(PrometheusText.Write(cache.Scrape(), time.GetUtcNow().UtcDateTime)).ConfigureAwait(false);
    }

    internal static void MapRoutes(IEndpointRouteBuilder app, Func<HttpContext, Task> answer)
    {
        /* #9013: GET only. Routing answers any other method on this path with an empty 405, and any other path with an empty 404. */
        app.MapGet("/metrics", context => answer(context));
    }

    /// <summary>
    /// #9013: the store data source, built at most once, on the first refresh that needs it. A login that is not there yet (the
    /// worker has not provisioned it, or a managed credential is not stored) gives null, which fails that refresh; the next
    /// refresh, a minute later, tries again. Null before a start that read an enabled block.
    /// </summary>
    internal async Task<NpgsqlDataSource?> StoreDataSourceAsync(CancellationToken ct)
    {
        var config = _config;
        if (config is null)
        {
            return null;
        }

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
                /* #9013: a null login is not cached, so the resolver runs again on every refresh, about once a minute, and its
                   refusal lines (an unresolvable webConnectionString, a store that refuses the role) would repeat for as long as the
                   cause lasts. After the first null it gets a silent logger; the first login that resolves lets it speak again. */
                var resolverLogger = _loginResolveFailing ? NullLogger.Instance : (ILogger)_logger;
                connectionString = await DarlingStoreLogins.ResolveUnmanagedAsync(
                    DarlingStoreLogins.Surface.Metrics, config.Postgres, resolverLogger, ct).ConfigureAwait(false);
                _loginResolveFailing = connectionString is null;
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
            Interlocked.Exchange(ref _dataSource, created);

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

    /// <summary>
    /// #9013, #9326: the warning for a bind off loopback. <paramref name="plainHttp"/> is true when no certificate is served, which
    /// puts the token in the Authorization header on the network unencrypted whatever <c>metrics.allowFrom</c> holds;
    /// <paramref name="noAllowFrom"/> is true when <c>metrics.allowFrom</c> is empty, which leaves the token as the only guard.
    /// Called only when at least one is true.
    /// </summary>
    internal static string BindWarning(IPAddress address, bool plainHttp, bool noAllowFrom)
    {
        var text = $"Metrics port is bound to {address}, not loopback";
        if (noAllowFrom)
        {
            text += plainHttp
                ? ", and metrics.allowFrom is empty: it is guarded by the token alone, over plain HTTP, so the token in the Authorization header crosses the network unencrypted. Set metrics.allowFrom to the scraper's addresses, and set web.network.tls to serve HTTPS with the web certificate"
                : ", and metrics.allowFrom is empty: it is guarded by the token alone. Set metrics.allowFrom to the scraper's addresses";
        }
        else
        {
            text += ", and serves plain HTTP: the token in the Authorization header crosses the network unencrypted. Set web.network.tls to serve HTTPS with the web certificate";
        }

        return text;
    }

    /// <summary>#9326: a certificate this host adopted is released once, on every path that ends the port.</summary>
    private void ReleaseCertificate()
    {
        var certificate = _certificate;
        _certificate = null;
        try { certificate?.Dispose(); } catch { /* best-effort */ }
    }

    /// <summary>#9326: this host's own certificate state, so its TLS load never touches the web host's published certificate facts.</summary>
    private sealed class MetricsTlsCertificateState : ListenerTlsCertificateState
    {
    }

    private async Task DisposeFailedStartAsync(WebApplication? app)
    {
        IsListening = false;
        _app = null;
        ReleaseCertificate();
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
            try
            {
                await app.StopAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReleaseCertificate();
                await CloseDataSourceAsync().ConfigureAwait(false);
            }

            return;
        }

        await CloseDataSourceAsync().ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _stopping.Cancel();

        var app = Interlocked.Exchange(ref _app, null);
        IsListening = false;
        if (app is not null)
        {
            try { await _disposeApp(app).ConfigureAwait(false); } catch { /* best-effort */ }
        }

        ReleaseCertificate();
        await CloseDataSourceAsync().ConfigureAwait(false);
        _stopping.Dispose();
    }
}
