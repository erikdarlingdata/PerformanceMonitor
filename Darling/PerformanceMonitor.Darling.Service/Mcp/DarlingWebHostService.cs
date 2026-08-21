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
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Hosting;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Optional hosted service serving Darling's read-only WEB DASHBOARD over HTTP (#1562) — a SECOND, independent
/// Kestrel host beside the MCP host (<see cref="DarlingMcpHostService"/>), with its own port (default 5153),
/// its own kill switch (<c>web.enabled</c> / config_service.web_enabled), its own LAN-exposure block, and its
/// own firewall rule. Not co-hosted with MCP because MCP owns the root path on its port, the two need
/// independent enable switches (web must not require mcp_enabled), and they gate different blast radii (the MCP
/// token guards analyze_server's live outbound SQL connections; the web surface is read-only over the store).
///
/// <para><b>Shape mirrors the MCP host</b> (5s supervisor poll, a pure <see cref="DecideWebAction"/> decision
/// table, 30s failed-start backoff, <c>StartAsync</c> not <c>RunAsync</c> so the supervisor owns the wait) and
/// reuses the shared <see cref="DarlingHostBinding"/> for the LAN-exposure ladder, the in-app CIDR check, the
/// loopback-listener guard, and the constant-time token compare. The live
/// enable/port state arrives via <see cref="WebRuntimeState"/> (the worker publishes it), so the viewer's
/// Settings toggle starts/stops/rebinds the dashboard with no service restart.</para>
///
/// <para><b>Browser auth (network mode only):</b> in network mode EVERY request authenticates, loopback
/// included — the MCP host's exposed-mode loopback-token SSRF guard, now mirrored here (#1649). Loopback is
/// exempt from the CIDR test only (127.0.0.1 is not in a LAN CIDR), never from the credential. A
/// loopback-only dashboard registers no auth middleware at all and remains tokenless. A request needs either a valid session
/// cookie or a valid <c>?token=</c> (constant-time), which is exchanged for an HMAC-signed HttpOnly
/// SameSite=Strict cookie and 302-redirected to strip the token from the URL; out-of-CIDR is 403; no
/// cookie/token gets a minimal inline login form. The cookie signing key is a per-process 32-byte RNG value,
/// so a restart invalidates sessions (acceptable). No TLS (the same reverse-proxy story as MCP).</para>
///
/// <para>The dashboard connects to the store as the least-privilege VIEWER role (not owner, not mcp) — a
/// read-only pool. Static assets ship from <c>wwwroot</c> (a csproj Content copy) with the content root AND
/// web root pinned to <see cref="AppContext.BaseDirectory"/>: a Windows service's CWD is System32, so the
/// parameterless builder would 404 every static file in production only.</para>
/// </summary>
public sealed class DarlingWebHostService : BackgroundService
{
    private readonly ILogger<DarlingWebHostService> _logger;
    private readonly WebRuntimeState _state;
    private WebApplication? _app;
    private NpgsqlDataSource? _appDataSource;
    private int _runningPort;

    /// <summary>How often the supervisor re-reads the live control-plane state (#1562, mirrors the MCP host).</summary>
    internal static readonly TimeSpan SupervisorPollInterval = TimeSpan.FromSeconds(5);

    /// <summary>Backoff after a FAILED start attempt (port in use, credential not ready) so a persistent
    /// failure logs on a calm cadence instead of every poll tick.</summary>
    internal static readonly TimeSpan FailedStartBackoff = TimeSpan.FromSeconds(30);

    /// <summary>The session cookie name and lifetime for the network-mode token→cookie exchange.</summary>
    internal const string SessionCookieName = "darling_web_session";
    internal static readonly TimeSpan SessionLifetime = TimeSpan.FromHours(12);
    private const int SigningKeyBytes = 32;

    public DarlingWebHostService(ILogger<DarlingWebHostService> logger, WebRuntimeState state)
    {
        _logger = logger;
        _state = state;
    }

    /// <summary>The supervisor's per-tick verdict — pure over (running, runningPort, enabled, desiredPort) so a
    /// unit test pins the whole decision table without a server (#1562, mirrors the MCP host).</summary>
    public enum WebSupervisorAction { None, Start, Stop, Restart }

    internal static WebSupervisorAction DecideWebAction(bool running, int runningPort, bool enabled, int desiredPort)
    {
        if (!running)
        {
            return enabled ? WebSupervisorAction.Start : WebSupervisorAction.None;
        }

        if (!enabled)
        {
            return WebSupervisorAction.Stop;
        }

        return runningPort == desiredPort ? WebSupervisorAction.None : WebSupervisorAction.Restart;
    }

    /// <summary>
    /// The supervisor loop (#1562): the viewer's Settings toggle writes config_service.web_enabled /
    /// web_port, the worker's reload beacon publishes the live values to <see cref="WebRuntimeState"/>, and
    /// this loop starts / stops / rebinds the inner web app to match — no service restart. Until the worker's
    /// first publish the FILE values apply. The web.network exposure block stays file-defined and restart-only
    /// by design; the store toggle still stops an exposed dashboard instantly (a kill switch).
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        /* Config load lives INSIDE the supervisor loop, on the failed-start backoff (#2038). It used to be a
           single front-loaded Load() whose failure stood this host down for the process LIFETIME, logged only
           at Debug — so one transient darling.json read failure at boot (an AV pass over the file mid service
           restart is enough) silently killed the dashboard until the next manual restart while the worker kept
           collecting, and the default-level log said nothing. Once loaded, the config is held for the process
           lifetime exactly as before (the network exposure block is restart-only by design). */
        DarlingConfig? config = null;
        var lastFailedStartUtc = DateTime.MinValue;
        /* #2389: the last control-plane-override report emitted, so a steady disagreement is stated once per
           distinct state instead of on every 5s poll tick. */
        string? lastOverrideReport = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (config is null && DateTime.UtcNow - lastFailedStartUtc >= FailedStartBackoff)
            {
                try
                {
                    config = DarlingConfig.Load();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        "Web dashboard configuration could not be loaded ({Message}) — retrying in {Backoff}s. The worker logs a missing/broken config as critical; a transient read failure self-heals here.",
                        ex.Message, (int)FailedStartBackoff.TotalSeconds);
                    lastFailedStartUtc = DateTime.UtcNow;
                }
            }

            if (config is null)
            {
                try
                {
                    await Task.Delay(SupervisorPollInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            var published = _state.Read();

            /* #2389: the store still wins whenever the worker has published (unchanged), but the resolution
               now carries WHICH plane supplied each value — the MCP host's twin, sharing one resolver so the
               two surfaces cannot drift. */
            var toggle = DarlingHostBinding.ResolveEndpointToggle(
                published is null ? null : (published.Enabled, published.Port), config.Web.Enabled, config.Web.Port);

            /* Report the DISAGREEMENT at the point of override, once per distinct state, so a file edit that
               the control plane is quietly ignoring says so instead of presenting as a successful start. */
            var overrideReport = DarlingHostBinding.DescribeToggleOverride(
                toggle, "web", "Web dashboard", config.Web.Enabled, config.Web.Port);
            if (overrideReport is not null && !string.Equals(overrideReport, lastOverrideReport, StringComparison.Ordinal))
            {
                _logger.LogWarning("{Report}", overrideReport);
            }

            lastOverrideReport = overrideReport;

            switch (DecideWebAction(_app is not null, _runningPort, toggle.Enabled, toggle.Port))
            {
                case WebSupervisorAction.Start when DateTime.UtcNow - lastFailedStartUtc >= FailedStartBackoff:
                    if (!await TryStartServerAsync(config, toggle, stoppingToken))
                    {
                        lastFailedStartUtc = DateTime.UtcNow;
                    }
                    break;

                case WebSupervisorAction.Stop:
                    _logger.LogInformation("Web dashboard disabled via the control plane — stopping (no restart needed)");
                    await StopServerAsync(stoppingToken);
                    break;

                case WebSupervisorAction.Restart:
                    _logger.LogInformation(
                        "Web dashboard port changed via the control plane ({Old} -> {New}) — rebinding", _runningPort, toggle.Port);
                    await StopServerAsync(stoppingToken);
                    if (!await TryStartServerAsync(config, toggle, stoppingToken))
                    {
                        lastFailedStartUtc = DateTime.UtcNow;
                    }
                    break;
            }

            try
            {
                await Task.Delay(SupervisorPollInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>Stops and disposes the running app + its data source. Safe to call when nothing is running.
    /// <para>It used to also remove the firewall rule here, mirroring the start-side reconcile. That write
    /// could never succeed from this account (#1771) and the rule is now install-managed, so a stop leaves it
    /// alone: it is scoped to a port nothing is listening on, an admin removes it with --configure-firewall or
    /// uninstall-darling.ps1, and the start-side check reports it as stale until then.</para></summary>
    private async Task StopServerAsync(CancellationToken cancellationToken)
    {
        if (_app is null)
        {
            return;
        }

        try
        {
            await _app.StopAsync(cancellationToken);
            await _app.DisposeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Web dashboard stop reported an error (continuing): {Message}", ex.Message);
        }

        _app = null;

        if (_appDataSource is not null)
        {
            await _appDataSource.DisposeAsync();
            _appDataSource = null;
        }

        _runningPort = 0;
    }

    /// <summary>Failed-start cleanup: a partially built app / data source must not leak between attempts.</summary>
    private async Task DisposeFailedStartAsync()
    {
        if (_app is not null)
        {
            try { await _app.DisposeAsync(); } catch { /* best-effort */ }
            _app = null;
        }

        if (_appDataSource is not null)
        {
            try { await _appDataSource.DisposeAsync(); } catch { /* best-effort */ }
            _appDataSource = null;
        }
    }

    /// <summary>
    /// The effective web-dashboard bind — the web twin of <see cref="DarlingMcpHostService.ResolveMcpBind"/>:
    /// projects the web network block onto the shared <see cref="DarlingHostBinding.ResolveBind"/> ladder
    /// (darling-network-endpoints anti-drift). The web host was built on the shared enums from the start, so
    /// unlike MCP there is no nested-enum mapping — the shared decision is returned as-is. PURE; extracted so
    /// the --configure-network wizard validates candidate web blocks through the SAME resolver the host
    /// fail-closes on (#1617), exactly as the wizard's MCP path calls ResolveMcpBind.
    /// </summary>
    internal static DarlingHostBinding.BindDecision ResolveWebBind(WebConfig web, bool managed, bool? inContainer = null)
    {
        var network = web.Network;
        return DarlingHostBinding.ResolveBind(
            network?.Listen,
            network?.AllowFrom,
            tokenPresent: network is not null
                && (!string.IsNullOrWhiteSpace(network.EncryptedToken) || !string.IsNullOrWhiteSpace(network.Token)),
            networkConfigured: network is { IsConfigured: true },
            managed: managed,
            /* #1804: tests pass this explicitly; the running host takes the ambient container marker. */
            inContainer: inContainer ?? DarlingHostBinding.IsRunningInContainer);
    }

    /// <summary>
    /// One start ATTEMPT of the inner web app at <paramref name="toggle"/>'s port: the port comes from the live
    /// control-plane value, and every bail path returns false so the supervisor retries with backoff instead of
    /// standing down for the process lifetime. The bind/network/token decisions come from the FILE-loaded config
    /// (network exposure is deliberately restart-only); returns true when the app is started and listening.
    /// <para>#2389: the toggle carries the enable/port PROVENANCE, not just the port, so the start line names
    /// the plane each half of the bind came from.</para>
    /// </summary>
    private async Task<bool> TryStartServerAsync(
        DarlingConfig config, DarlingHostBinding.EndpointToggle toggle, CancellationToken stoppingToken)
    {
        var effectivePort = toggle.Port;

        var web = config.Web;
        var network = web.Network;

        /* Decide the effective bind PURELY (shared ladder), then map the reason -> severity here. */
        var bind = ResolveWebBind(web, config.Postgres.Managed);
        LogBindReason(web, bind.Reason);

        try
        {
            var networkMode = bind.Mode == DarlingHostBinding.BindMode.NetworkAndLoopback;

            /* In network mode ResolveBind has already validated the listen IP, the allowFrom CIDR, AND their
               address-family agreement, so these two parses cannot throw; only resolving the token can still
               fail (a corrupt DPAPI blob), which fail-closes to loopback-only rather than exposing tokenless. */
            IPAddress? networkListenIp = null;
            IPNetwork allowedCidr = default;
            string accessToken = "";
            if (networkMode)
            {
                networkListenIp = IPAddress.Parse(network!.Listen!.Trim());
                allowedCidr = IPNetwork.Parse(network.AllowFrom!.Trim());

                try
                {
                    var token = network.ResolveToken(out var usedPlaintext);
                    if (string.IsNullOrWhiteSpace(token))
                    {
                        _logger.LogCritical(
                            "Web dashboard token resolved to empty after decryption — refusing to expose; binding loopback-only.");
                        networkMode = false;
                    }
                    else
                    {
                        accessToken = token;
                        if (usedPlaintext)
                        {
                            _logger.LogWarning(
                                "web.network.token is set in plaintext (dev convenience) — prefer web.network.encryptedToken " +
                                "(produced by --encrypt-password). This token gates web dashboard network access.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(
                        "Web dashboard token could not be decrypted ({Message}) — refusing to expose; binding loopback-only.",
                        ex.Message);
                    networkMode = false;
                }
            }

            /* The REAL primary bind address (network IP when exposed, else loopback): both the port precheck and
               the Kestrel bind use it, so the precheck probes the actual address, not always loopback. */
            var primaryBind = networkMode ? networkListenIp! : IPAddress.Loopback;

            /* Port-in-use pre-check via the shared utility against the REAL bind address. Done before the
               firewall check so a bail here reports nothing about the firewall. */
            if (await PortUtilityService.IsTcpPortListeningAsync(effectivePort, primaryBind, stoppingToken))
            {
                _logger.LogError("Port {Port} is already in use — web dashboard not started this attempt; will retry", effectivePort);
                return false;
            }

            /* Firewall CHECK (managed mode only; read-only, never fatal) — #1771. Reports a missing rule when
               exposed and a stale one when not, naming the elevated command either way; the rule itself is
               created by the installer, because this process cannot create it. The in-app CIDR is the
               boundary; the firewall is defense-in-depth. */
            if (config.Postgres.Managed && OperatingSystem.IsWindows())
            {
                await CheckWebFirewallAsync(
                    effectivePort, networkMode, networkMode ? allowedCidr.ToString() : null, stoppingToken);
            }

            /* Managed mode: the WORKER owns the bundled server's lifecycle; the web host only derives the
               least-privilege VIEWER-role connection string from the stored DPAPI credential. */
            string? storeConnectionString;
            if (config.Postgres.Managed)
            {
                if (!OperatingSystem.IsWindows())
                {
                    _logger.LogError("Web dashboard not started: postgres.managed = true requires Windows");
                    return false;
                }

                storeConnectionString = await WaitForManagedConnectionStringAsync(config.Postgres, stoppingToken);
                if (storeConnectionString is null)
                {
                    return false;
                }
            }
            else
            {
                storeConnectionString = config.Postgres.ConnectionString;
            }

            /* Lifetime tied to the running app: disposed by StopServerAsync, not this method's scope. */
            var postgres = NpgsqlDataSource.Create(storeConnectionString);
            _appDataSource = postgres;

            /* FOOTGUN (load-bearing): pin BOTH the content root AND the web root to the binary's directory. A
               Windows service's CWD is System32, so the parameterless CreateBuilder would 404 every static file
               in production only. WebRootPath is resolved against ContentRootPath -> AppContext.BaseDirectory/wwwroot. */
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = AppContext.BaseDirectory,
                WebRootPath = "wwwroot",
            });

            builder.WebHost.ConfigureKestrel(options =>
            {
                if (networkMode)
                {
                    /* Bind the specific family, then ALSO both loopback families (unless the listen is itself
                       loopback/wildcard, which would collide) so a local client resolving "localhost" still
                       reaches the dashboard (loopback stays tokenless). */
                    options.Listen(primaryBind, effectivePort);
                    if (DarlingHostBinding.ShouldAddLoopbackListeners(primaryBind))
                    {
                        options.Listen(IPAddress.Loopback, effectivePort);
                        options.Listen(IPAddress.IPv6Loopback, effectivePort);
                    }
                }
                else
                {
                    /* The default/degraded loopback-only server — both families. */
                    options.ListenLocalhost(effectivePort);
                }
            });

            /* Suppress ASP.NET Core console logging — the service's own logger reports lifecycle. */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* The VIEWER-role store pool the read endpoints query (registered for DI + passed to MapAll). */
            builder.Services.AddSingleton<NpgsqlDataSource>(postgres);

            _app = builder.Build();

            /* Pipeline order: the Host-allowlist middleware runs FIRST on EVERY request (both modes) as the
               DNS-rebinding guard, then (network mode only) the auth middleware, then DarlingWebEndpoints.MapAll
               -> UseDefaultFiles -> UseStaticFiles. WebApplication auto-inserts UseRouting at the head and
               UseEndpoints at the tail, so the static-file middleware sits behind these gates and serves the SPA
               for non-API paths. */

            /* DNS-rebinding guard — runs in BOTH modes (the #1576 fix: it previously guarded network mode only,
               leaving the tokenless loopback write path reachable cross-origin via a DNS rebind). The loopback
               surface is tokenless, so a browser ON the host that loads attacker content could be rebound to
               127.0.0.1:5153 and read/write the whole surface same-origin. Require the Host header to name an
               address we actually bind — a loopback name/IP (localhost / 127.0.0.1 / [::1]) or, in network mode,
               the configured listen IP. networkListenIp is null in loopback mode, so ONLY loopback Hosts pass
               there; a rebound foreign hostname pointed at 127.0.0.1:5153 is rejected 400 before any auth
               decision, route handler, or static file. */
            _app.Use(async (context, next) =>
            {
                if (!IsAllowedHost(context.Request.Host.Host, networkListenIp))
                {
                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return;
                }

                await next(context);
            });

            if (networkMode)
            {
                var cidr = allowedCidr;
                var token = accessToken;
                /* Per-process signing key: a restart regenerates it and thereby invalidates every session
                   cookie (acceptable — the operator re-presents the token once). */
                var signingKey = RandomNumberGenerator.GetBytes(SigningKeyBytes);

                _app.Use(async (context, next) =>
                {
                    /* The Host-allowlist / DNS-rebinding guard already ran above (both modes); this gate owns
                       only the network auth decision (session cookie / ?token= / in-CIDR). */
                    var remote = context.Connection.RemoteIpAddress;
                    var hasValidCookie = TryValidateSessionCookie(
                        context.Request.Cookies[SessionCookieName], signingKey, DateTimeOffset.UtcNow);
                    var hasValidToken = DarlingHostBinding.FixedTimeTokenEquals(
                        context.Request.Query["token"].ToString(), token);

                    switch (DecideWebAuth(remote, cidr, hasValidCookie, hasValidToken))
                    {
                        case WebAuthAction.Allow:
                            await next(context);
                            return;

                        case WebAuthAction.SetCookieAndRedirect:
                            AppendSessionCookie(context, signingKey);
                            /* 302 (default) to the same path with ?token= stripped, so the token never lingers
                               in browser history, bookmarks, or a Referer header. */
                            context.Response.Redirect(BuildPathWithoutToken(context.Request));
                            return;

                        case WebAuthAction.Forbid:
                            context.Response.StatusCode = StatusCodes.Status403Forbidden;
                            return;

                        default: /* ShowLogin */
                            await WriteLoginPageAsync(context);
                            return;
                    }
                });
            }

            DarlingWebEndpoints.MapAll(_app, postgres);
            _app.UseDefaultFiles();
            _app.UseStaticFiles();

            /* #2389: name the authority for each half of what is being started — enabled/port from whichever
               plane the supervisor resolved, listen/allowFrom/token always from darling.json. */
            var origin = DarlingHostBinding.DescribeToggleOrigin(toggle);
            if (networkMode)
            {
                _logger.LogInformation(
                    "Starting web dashboard on http://{Listen}:{Port} (LAN-exposed to {Cidr} behind a token->cookie gate + in-app CIDR; loopback also bound, tokenless) — "
                    + "enabled/port from {Origin}; listen/allowFrom/token from darling.json web.network (file-only, restart-only)",
                    primaryBind, effectivePort, allowedCidr, origin);
            }
            else
            {
                _logger.LogInformation(
                    "Starting web dashboard on http://localhost:{Port} (loopback only) — enabled/port from {Origin}",
                    effectivePort, origin);
            }

            /* #2479 item 6: the network block is read ONCE and held for the process lifetime by design.
               Say so at every start, in BOTH modes - the loopback line above never mentioned the block at
               all, and loopback-when-you-expected-LAN is exactly the state being diagnosed. */
            _logger.LogInformation(
                "{Report}",
                DarlingHostBinding.DescribeNetworkBlockLifetime(
                    "web", "Web dashboard", config.Web.Network?.IsConfigured ?? false, networkMode,
                    DarlingNetwork.IsExposedListenAddress(config.Web.Network?.Listen),
                    networkMode ? primaryBind.ToString() : null,
                    networkMode ? allowedCidr.ToString() : null));

            /* StartAsync, not RunAsync: the supervisor loop owns the wait — the app keeps serving until
               StopServerAsync (toggle-off, port change, or shutdown). */
            await _app.StartAsync(stoppingToken);
            _runningPort = effectivePort;
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Normal shutdown mid-start. */
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError("Web dashboard failed to start: {Message}", ex.Message);
            await DisposeFailedStartAsync();
            return false;
        }
    }

    /* ---------------------------------------------------------------------------------------------------
       Browser auth (network mode only). The decision is a PURE function over the resolved facts (loopback,
       in-CIDR, cookie-valid, token-valid) so a unit test pins the whole matrix without a server; the
       middleware above owns only the HTTP mechanics (read cookie/token, then act on the verdict).
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The verdict for one network-mode request.</summary>
    internal enum WebAuthAction { Allow, Forbid, SetCookieAndRedirect, ShowLogin }

    /// <summary>
    /// PURE Host-header allowlist — the DNS-rebinding guard. A request is served only when its Host names an
    /// address this host actually binds: a loopback name/IP (<c>localhost</c>, <c>127.0.0.1</c>, <c>::1</c>) or,
    /// when LAN-exposed, the configured listen IP (<paramref name="networkListenIp"/>). Any port suffix is
    /// ignored (already split into <see cref="HostString.Host"/>), and an empty Host is allowed (HTTP/1.0 / some
    /// health probes). This rejects a rebound foreign hostname pointed at 127.0.0.1:5153 before the tokenless
    /// loopback allow, while a direct IP request from the real operator passes.
    ///
    /// <para>#1648 lifted the decision itself into the shared
    /// <see cref="PerformanceMonitor.Common.HostHeaderGuard"/> (via <see cref="DarlingHostBinding"/>) so the two
    /// MCP hosts — Darling's and Lite's — install the SAME guard instead of going without one. This forwarder
    /// stays so this host's behavior and its existing tests are byte-for-byte unchanged.</para>
    /// </summary>
    internal static bool IsAllowedHost(string? host, IPAddress? networkListenIp)
        => DarlingHostBinding.IsAllowedHost(host, networkListenIp);

    /// <summary>
    /// PURE route-auth decision. This method is only ever reached in NETWORK mode — the caller registers the
    /// auth middleware inside <c>if (networkMode)</c> — so a loopback-only dashboard is unaffected by every
    /// rule here and stays tokenless.
    ///
    /// <para>Loopback skips the CIDR check (127.0.0.1 is not in a LAN CIDR, so testing it there would 403 the
    /// operator's own browser) but still needs a session cookie or a valid <c>?token=</c>, exactly like any
    /// other remote. It previously passed tokenless on the grounds that the web surface was read-only; that
    /// stopped being true when Custom Views v2 added view create/update/delete and <c>/api/compose/run</c>, so
    /// any local process — a scheduled task, SSRF'd code, another user's session — could read the whole store
    /// and mutate views with no credential while the host was LAN-exposed (#1649). This now mirrors the MCP
    /// host's rule verbatim: in exposed mode even a local client presents the token, which IS the loopback
    /// guard against SSRF and sandboxed sockets.</para>
    ///
    /// <para>A non-loopback remote must additionally be inside <paramref name="allowedCidr"/> (a
    /// null/unverifiable remote fails closed to <see cref="WebAuthAction.Forbid"/>). Then a valid session
    /// cookie passes, a valid <c>?token=</c> is exchanged for one
    /// (<see cref="WebAuthAction.SetCookieAndRedirect"/>), and anything else gets the login form.</para>
    /// </summary>
    internal static WebAuthAction DecideWebAuth(IPAddress? remoteIp, IPNetwork allowedCidr, bool hasValidCookie, bool hasValidToken)
    {
        /* Loopback determination lives in DarlingWebEndpoints.IsLoopbackRemote so every caller unwraps
           IPv4-mapped-IPv6 (::ffff:127.0.0.1) identically. Loopback is exempt from the CIDR test ONLY — it
           still has to authenticate below. */
        var isLoopback = DarlingWebEndpoints.IsLoopbackRemote(remoteIp);

        if (!isLoopback && !DarlingHostBinding.IsRemoteAddressAllowed(remoteIp, allowedCidr))
        {
            return WebAuthAction.Forbid;
        }

        if (hasValidCookie)
        {
            return WebAuthAction.Allow;
        }

        if (hasValidToken)
        {
            return WebAuthAction.SetCookieAndRedirect;
        }

        return WebAuthAction.ShowLogin;
    }

    /// <summary>
    /// Builds a session cookie value <c>{expiryUnix}.{base64url(HMAC-SHA256(key, expiryUnix))}</c>. PURE — the
    /// HMAC is over the exact expiry string that is stored, so <see cref="TryValidateSessionCookie"/> verifies
    /// the same bytes it signs (a tampered expiry fails the HMAC).
    /// </summary>
    internal static string BuildSessionCookieValue(byte[] signingKey, DateTimeOffset expiry)
    {
        var expiryUnix = expiry.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var signature = HMACSHA256.HashData(signingKey, Encoding.ASCII.GetBytes(expiryUnix));
        return $"{expiryUnix}.{Base64UrlEncode(signature)}";
    }

    /// <summary>
    /// PURE verify of a session cookie against <paramref name="signingKey"/> and the current time: false on a
    /// malformed value, an unparsable/expired expiry, a bad base64url signature, or an HMAC mismatch (tamper).
    /// The signature compare is constant-time.
    /// </summary>
    internal static bool TryValidateSessionCookie(string? cookieValue, byte[] signingKey, DateTimeOffset now)
    {
        if (string.IsNullOrEmpty(cookieValue))
        {
            return false;
        }

        var dot = cookieValue.IndexOf('.');
        if (dot <= 0 || dot >= cookieValue.Length - 1)
        {
            return false;
        }

        var expiryPart = cookieValue.Substring(0, dot);
        var signaturePart = cookieValue.Substring(dot + 1);

        if (!long.TryParse(expiryPart, NumberStyles.None, CultureInfo.InvariantCulture, out var expiryUnix))
        {
            return false;
        }

        if (now.ToUnixTimeSeconds() > expiryUnix)
        {
            return false;
        }

        byte[] presented;
        try
        {
            presented = Base64UrlDecode(signaturePart);
        }
        catch (FormatException)
        {
            return false;
        }

        var expected = HMACSHA256.HashData(signingKey, Encoding.ASCII.GetBytes(expiryPart));
        return CryptographicOperations.FixedTimeEquals(presented, expected);
    }

    private static void AppendSessionCookie(HttpContext context, byte[] signingKey)
    {
        var value = BuildSessionCookieValue(signingKey, DateTimeOffset.UtcNow.Add(SessionLifetime));
        context.Response.Cookies.Append(SessionCookieName, value, new CookieOptions
        {
            HttpOnly = true,
            SameSite = SameSiteMode.Strict,
            /* The dashboard endpoint is plain HTTP (no TLS on this surface); a Secure cookie would never be
               sent, so it must be false here. The reverse-proxy story is the same as MCP for on-wire secrecy. */
            Secure = false,
            Path = "/",
            MaxAge = SessionLifetime,
        });
    }

    /// <summary>Rebuilds the current request's path + query with the <c>token</c> parameter removed, for the
    /// post-exchange 302. Preserves any other query parameters.</summary>
    private static string BuildPathWithoutToken(HttpRequest request)
    {
        var query = QueryString.Empty;
        foreach (var pair in request.Query)
        {
            if (string.Equals(pair.Key, "token", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var value in pair.Value)
            {
                query = query.Add(pair.Key, value ?? string.Empty);
            }
        }

        var location = request.PathBase.Add(request.Path).ToUriComponent() + query.ToUriComponent();
        return SanitizeRedirectPath(location);
    }

    /// <summary>
    /// PURE open-redirect guard for the token-strip 302 target: forces a single-slash site-relative path. A
    /// request path beginning <c>//</c> (or <c>/\</c>) is a protocol-relative URL the browser follows off-site,
    /// so a crafted <c>//evil.com/?token=...</c> would 302 the operator away; any leading slash/backslash run is
    /// collapsed to one <c>/</c>. Empty maps to <c>/</c>.
    /// </summary>
    internal static string SanitizeRedirectPath(string location)
    {
        if (string.IsNullOrEmpty(location))
        {
            return "/";
        }

        var i = 0;
        while (i < location.Length && (location[i] == '/' || location[i] == '\\'))
        {
            i++;
        }

        return "/" + location.Substring(i);
    }

    private static Task WriteLoginPageAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        return context.Response.WriteAsync(LoginPageHtml);
    }

    /* A minimal, fully self-contained login form (no external references) — a GET form whose only field is the
       access token, so submitting it re-requests the same URL with ?token=, which the middleware exchanges for
       a session cookie. It renders BEFORE auth, so it cannot link the gated /css/theme.css (the static files
       sit behind this same auth gate); instead it INLINES the same custom-property token block as theme.css
       (keep in sync) and carries the same wordmark/subtitle as index.html, plus an input focus style and the
       host being accessed. Single-quoted HTML attributes so the C# verbatim string needs no quote-doubling. */
    private const string LoginPageHtml = @"<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Darling Web</title>
<style>
  /* Mirror of css/theme.css tokens — this page renders before auth, so it cannot link the gated stylesheet. */
  :root {
    --accent: #2eaef1; --bg: #181b1f; --bg-dark: #111217; --card: #22252b;
    --fg: #E4E6EB; --dim: #C7CBD4; --muted: #A8AEBA; --border: #2a2d35;
    --radius: 6px; --radius-sm: 4px;
    --font: system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  body { font-family: var(--font); background: var(--bg); color: var(--fg); margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  form { background: var(--card); padding: 2rem; border: 1px solid var(--border); border-radius: var(--radius); display: flex; flex-direction: column; gap: 0.75rem; min-width: 300px; }
  .brand h1 { font-size: 1.15rem; margin: 0; color: var(--accent); letter-spacing: 0.2px; }
  .brand .sub { font-size: 0.75rem; color: var(--muted); margin: 2px 0 0.5rem; }
  label { font-size: 0.85rem; color: var(--dim); }
  input { padding: 0.55rem; border-radius: var(--radius-sm); border: 1px solid var(--border); background: var(--bg-dark); color: var(--fg); font-size: 0.9rem; }
  input:focus { outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px rgba(46, 174, 241, 0.25); }
  button { padding: 0.55rem; border-radius: var(--radius-sm); border: 0; background: var(--accent); color: var(--bg-dark); font-weight: 600; cursor: pointer; font-size: 0.9rem; }
  button:hover { filter: brightness(1.08); }
  .host { font-size: 0.72rem; color: var(--muted); text-align: center; margin-top: 0.25rem; }
</style>
</head>
<body>
<form method='get' action=''>
  <div class='brand'>
    <h1>Darling Web</h1>
    <div class='sub'>SQL Server fleet monitor</div>
  </div>
  <label for='token'>Access token</label>
  <input id='token' name='token' type='password' autocomplete='off' autofocus>
  <button type='submit'>Enter</button>
  <div class='host' id='host'></div>
</form>
<script>document.getElementById('host').textContent = 'Accessing ' + location.host;</script>
</body>
</html>";

    private static string Base64UrlEncode(byte[] bytes)
        => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static byte[] Base64UrlDecode(string value)
    {
        var b64 = value.Replace('-', '+').Replace('_', '/');
        b64 = (b64.Length % 4) switch
        {
            2 => b64 + "==",
            3 => b64 + "=",
            _ => b64,
        };
        return Convert.FromBase64String(b64);
    }

    /* ---------------------------------------------------------------------------------------------------
       Bind-reason logging + managed-store credential wait + firewall check (mirrors the MCP host).
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Emits the <see cref="DarlingHostBinding.ResolveBind"/> reason at its mapped severity. Silent for
    /// the non-degrade reasons (the network-exposed line is logged at start with the real bind).</summary>
    private void LogBindReason(WebConfig web, DarlingHostBinding.BindReason reason)
    {
        var level = DarlingHostBinding.MapBindReasonSeverity(reason);
        if (level is null)
        {
            return;
        }

        switch (reason)
        {
            case DarlingHostBinding.BindReason.ListenInvalid:
                _logger.Log(level.Value,
                    "Web dashboard network exposure requested but web.network.listen '{Listen}' is not a valid IP address — " +
                    "refusing to expose; binding loopback-only. Use a specific IP (e.g. 192.168.1.205), or 0.0.0.0 for all interfaces.",
                    web.Network?.Listen);
                break;

            case DarlingHostBinding.BindReason.TokenMissing:
                _logger.Log(level.Value,
                    "Web dashboard network exposure requested (web.network.listen is non-loopback) but no access token is set — " +
                    "refusing to expose; binding loopback-only. Set web.network.encryptedToken (via --encrypt-password) or web.network.token.");
                break;

            case DarlingHostBinding.BindReason.AllowFromInvalid:
                _logger.Log(level.Value,
                    "Web dashboard network exposure requested but web.network.allowFrom '{AllowFrom}' is not a valid CIDR or its " +
                    "address family does not match web.network.listen — refusing to expose; binding loopback-only. " +
                    "Use e.g. 192.168.1.0/24 (host bits zeroed, same family as listen).",
                    web.Network?.AllowFrom);
                break;

            case DarlingHostBinding.BindReason.ManagedModeRequired:
                _logger.Log(level.Value,
                    "web.network.* is set but postgres.managed = false — web dashboard network exposure is managed-mode (or container, " +
                    "#1804) only and is ignored; your own reverse proxy governs uncontained BYO exposure. Binding loopback-only.");
                break;

            default:
                break;
        }
    }

    /// <summary>The scoped web firewall rule name (idempotent by DisplayName), port-specific and distinct from
    /// the store's and MCP's rules so the endpoints are managed independently. <c>internal</c> so the headless
    /// endpoint-toggle CLI verbs (--enable-web/--disable-web) and --configure-firewall act on the SAME rule by
    /// DisplayName.</summary>
    internal static string WebFirewallRuleName(int port) => $"PerformanceMonitor Darling Web (port {port})";

    /// <summary>Last (rule, verdict) this host reported, so a supervisor retry loop restates a steady
    /// firewall state at most once (<see cref="DarlingFirewallCheck.ShouldReport"/>).</summary>
    private string? _lastFirewallRule;
    private FirewallRuleVerdict? _lastFirewallVerdict;

    /// <summary>Read-only firewall verification (#1771) — see <see cref="DarlingFirewallCheck"/> for why this
    /// no longer writes the rule.</summary>
    [SupportedOSPlatform("windows")]
    private async Task CheckWebFirewallAsync(int port, bool exposed, string? cidr, CancellationToken cancellationToken)
        => (_lastFirewallRule, _lastFirewallVerdict) = await DarlingFirewallCheck.CheckAsync(
            WebFirewallRuleName(port), port, exposed, cidr, _lastFirewallRule, _lastFirewallVerdict, _logger, cancellationToken);

    /// <summary>
    /// Managed mode's first-boot race, handled: the VIEWER-role credential appears only after the worker's
    /// initdb + migration + role provisioning finish, so poll up to ~5 minutes (60 x 5s) instead of racing it,
    /// then stand down with a pointer at the worker log — fail-closed (the dashboard just does not start; it
    /// self-heals on the next attempt once the credential exists).
    /// </summary>
    [SupportedOSPlatform("windows")]
    private async Task<string?> WaitForManagedConnectionStringAsync(PostgresConfig config, CancellationToken stoppingToken)
    {
        for (var attempt = 0; attempt < 60; attempt++)
        {
            var connectionString = DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential(config);
            if (connectionString is not null)
            {
                return connectionString;
            }

            if (attempt == 0)
            {
                _logger.LogInformation("Waiting for the managed Postgres viewer-role credential (first-run initialization) before starting the web dashboard");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
        }

        _logger.LogError("Web dashboard not started: the managed Postgres viewer-role credential never appeared — see the worker log for the bootstrap failure");
        return null;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app != null)
        {
            _logger.LogInformation("Stopping web dashboard");
            await StopServerAsync(cancellationToken);
        }

        await base.StopAsync(cancellationToken);
    }
}
