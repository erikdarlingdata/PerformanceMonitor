/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.ResponseCompression;
using ForwardedHeaders = Microsoft.AspNetCore.HttpOverrides.ForwardedHeaders;
using ForwardedHeadersOptions = Microsoft.AspNetCore.Builder.ForwardedHeadersOptions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Optional hosted service serving Darling's WEB DASHBOARD over HTTP (#1562) — a SECOND, independent
/// Kestrel host beside the MCP host (<see cref="DarlingMcpHostService"/>), with its own port (default 5153),
/// its own kill switch (<c>web.enabled</c> / config_service.web_enabled), its own LAN-exposure block, and its
/// own firewall rule. Not co-hosted with MCP because MCP owns the root path on its port, the two need
/// independent enable switches (web must not require mcp_enabled), and they gate different blast radii (the MCP
/// token guards analyze_server's live outbound SQL connections; the web surface reads the store, and its edit seat writes only through the host's gates).
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
/// loopback-only dashboard with no token configured does not start unless <c>web.allowTokenlessLoopback</c> is on, which
/// serves local browsers with no token, for reads only, and refuses every change; one
/// with a configured <c>web.network.encryptedToken</c> / <c>token</c> (or that network mode fell back to after its token
/// resolved, a refused TLS certificate) has this same token-to-cookie gate, because the operator's config says every
/// client presents the token (<see cref="ResolveLoopbackOnlyToken"/>). A request needs either a valid session
/// cookie or a token posted from the sign-in form (constant-time), which is exchanged for an HMAC-signed HttpOnly
/// SameSite=Strict cookie and 302-redirected to the page being opened; out-of-CIDR is 403; no
/// cookie/token gets a minimal inline login form. The cookie signing key is a per-process 32-byte RNG value,
/// so a restart invalidates sessions (acceptable).</para>
///
/// <para><b>OIDC sign-in (#2550, opt-in via <c>web.network.oidc</c>):</b> authorization code + PKCE beside
/// the token — the login page grows an SSO link, the callback mints the SAME session cookie with the
/// authenticated subject and role encoded in its signed subject slot (#2583), and a viewer-role seat is
/// refused every mutating request by a group-level gate in the auth middleware. The token path is unchanged
/// and remains the scripted-caller/break-glass credential; the CIDR stays outermost, OIDC endpoints
/// included. See <see cref="Hosting.DarlingWebOidc"/> (the protocol) and
/// <see cref="Hosting.DarlingWebSeat"/> (the seat model).</para>
///
/// <para><b>TLS (#2562):</b> opt-in via <c>web.network.tls</c> and applied to the NETWORK listener only — see
/// <see cref="Hosting.DarlingWebTls"/> for the certificate rules and the Kestrel bind below for why the
/// loopback listeners stay plain HTTP. Without it the exposed listener is plain HTTP and every start warns
/// that the token and its cookie cross the segment in the clear. A certificate that is missing, unreadable,
/// ambiguous or expired falls back to loopback-only (the token stays resolved and keeps gating it); it never
/// downgrades to serving the LAN over HTTP. A token that is blank or cannot be decrypted stops the start in every mode.</para>
///
/// <para>The dashboard connects to the store as the least-privilege VIEWER role (not owner, not mcp) — a
/// read-only pool — on a managed store and on the compose distribution's own store; on any other store as
/// <c>postgres.webConnectionString</c>, or as the owner with a startup warning when that is unset
/// (<see cref="DarlingStoreLogins"/>, #3914). Static assets ship from <c>wwwroot</c> (a csproj Content copy) with the content root AND
/// web root pinned to <see cref="AppContext.BaseDirectory"/>: a Windows service's CWD is System32, so the
/// parameterless builder would 404 every static file in production only.</para>
/// </summary>
public sealed class DarlingWebHostService : BackgroundService
{
    private readonly ILogger<DarlingWebHostService> _logger;
    private readonly WebRuntimeState _state;

    /// <summary>#3514: the served TLS certificate's expiry facts, published here on load so the worker's alert
    /// sweep can raise a self-alert as it approaches (the certificate is loaded once and only a log line ever
    /// reported its expiry). Null-by-default when there is no LAN TLS certificate.</summary>
    private readonly WebTlsCertificateState _certState;

    /// <summary>#2953: the collector's startup verdict, published by the worker and reported by
    /// <c>/api/ping</c>. Held rather than resolved per request so the route stays a field read — the whole
    /// point of that endpoint is that it answers without depending on anything that can be down.</summary>
    private readonly CollectorRuntimeState _collectorState;
    private WebApplication? _app;
    private NpgsqlDataSource? _appDataSource;

    /// <summary>#6600: the second, small pool the Admin writes use on the managed store (null on a store you bring, or with web.allowAdminWrites off).</summary>
    private WebAdminArm? _adminArm;

    /// <summary>The TLS certificate the current network listener presents — the leaf AND the intermediates
    /// that travel with it (#2562) — held for its lifetime. Disposal is not bookkeeping: on Windows the
    /// private key is loaded with <c>MachineKeySet</c> and no <c>PersistKeySet</c>, so disposing is what
    /// REMOVES the key material from the machine key store. A rebind that leaked it would accumulate a key
    /// per restart.</summary>
    private DarlingWebTls.LoadedCertificate? _serverCertificate;

    /// <summary>The OIDC client for the current network listener (#2550) — null unless web.network.oidc is
    /// configured AND valid. Holds the HttpClient and the cached discovery document, so its lifetime is the
    /// listener's, exactly like the certificate above.</summary>
    private DarlingWebOidcClient? _oidcClient;

    private int _runningPort;

    /// <summary>Whether this process has logged <see cref="MissingTokenMessage"/>: a start with no token fails the same
    /// way on every retry until the config changes, which needs a restart, so the line is logged once.</summary>
    private bool _missingTokenReported;

    /// <summary>How often the supervisor re-reads the live control-plane state (#1562, mirrors the MCP host).</summary>
    internal static readonly TimeSpan SupervisorPollInterval = TimeSpan.FromSeconds(5);

    /// <summary>Backoff after a FAILED start attempt (port in use, credential not ready) so a persistent
    /// failure logs on a calm cadence instead of every poll tick.</summary>
    internal static readonly TimeSpan FailedStartBackoff = TimeSpan.FromSeconds(30);

    /// <summary>The session cookie name and lifetime for the network-mode token→cookie exchange.</summary>
    internal const string SessionCookieName = "darling_web_session";
    internal static readonly TimeSpan SessionLifetime = TimeSpan.FromHours(12);
    private const int SigningKeyBytes = 32;

    /* #3941: the process's shared baseline tier, for the analysis reads MapAll serves (compare_analysis' banding).
       Optional so a host built outside the service's DI (a test) keeps a private one. */
    private readonly BaselineCache _baselineCache;

    /// <summary>The shared baseline cache this host holds (#5950): a test compares it with the container's one instance.</summary>
    internal BaselineCache SharedBaselineCache => _baselineCache;

    /// <summary>#4442 scope 2: the process-wide read-latency histogram, passed through to <see
    /// cref="DarlingWebEndpoints.MapAll"/>. Optional so a host built outside the service's DI (a test) simply
    /// records nothing rather than needing its own instance.</summary>
    private readonly ReadLatencyAccumulator? _readLatency;

    /* #5097: where the web recorders offer a slow or failed read; null in a test-constructed host. */
    private readonly SlowReadLog? _slowReads;

    /* #5506 part 3: the process's one read admission, shared with the MCP host; null in a test-constructed host. */
    private readonly ReadAdmission? _readAdmission;

    /// <summary>The live monitored-server registry, so the read tools scope an Azure master target's blocking and deadlock facts per call.</summary>
    private readonly MonitoredServerRegistryState? _registryState;

    /* #5632 step 9: the worker's store owner data source, for the plan undo route only (it reads one server's remediation slot). */
    private readonly StoreOwnerSource? _ownerSource;

    /* #6236: the store's pseudonym key, so the dashboard's investigation export numbers a login as the command line does. */
    private readonly PseudonymKeySource? _pseudonymKeys;

    /* #6840: the service's container, null in a host built outside it. The front door's directory and secrets are internal types, so they
       are looked up here, and only when darling.json has frontDoor.enabled (which replaces the dashboard's endpoint map with
       DarlingFrontDoorWebProxy). */
    private readonly IServiceProvider? _services;

    public DarlingWebHostService(ILogger<DarlingWebHostService> logger, WebRuntimeState state, CollectorRuntimeState collectorState, WebTlsCertificateState certState, BaselineCache? baselineCache = null, ReadLatencyAccumulator? readLatency = null, MonitoredServerRegistryState? registryState = null, SlowReadLog? slowReads = null, ReadAdmission? readAdmission = null, StoreOwnerSource? ownerSource = null, PseudonymKeySource? pseudonymKeys = null, IServiceProvider? services = null)
    {
        _services = services;
        _pseudonymKeys = pseudonymKeys;
        _ownerSource = ownerSource;
        _slowReads = slowReads;
        _readAdmission = readAdmission;
        _registryState = registryState;
        _logger = logger;
        _state = state;
        _collectorState = collectorState;
        _certState = certState;
        _baselineCache = baselineCache ?? new BaselineCache();
        _readLatency = readLatency;
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
        /* #5288: the last tick's enabled flag (null before the first tick), so the certificate release for a listener
           that is disabled and not running below runs once per transition to disabled, not on every poll tick - the
           same last-state shape as lastOverrideReport. */
        bool? lastEnabled = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (config is null && CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff))
            {
                try
                {
                    config = DarlingConfig.Load();

                    /* #5506 part 3: reads.maxHeavyReads is file-only and read when the host starts. */
                    _readAdmission?.Configure(config.Reads.MaxHeavyReads);
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
                case WebSupervisorAction.Start when CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff):
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

                /* #5288: not running and disabled. A failed start keeps a certificate refusal or an expired verdict
                   published (ClearUnlessRefusal), and a runtime disable that comes after it finds no app to stop, so
                   no stop runs and the worker's alert would stay open until the next start or a service restart.
                   Release the state with the call StopServerAsync makes, once per transition to disabled. */
                case WebSupervisorAction.None
                    when ListenerTlsCertificateState.ReleasesWhenDisabled(_app is not null, toggle.Enabled, lastEnabled):
                    ReleaseServerCertificate();
                    break;
            }

            lastEnabled = toggle.Enabled;

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
            /* Nothing is serving, but a start that adopted a certificate and stopped short of building the app
               must not leave its key held or its verdict published: released BEFORE this return, the way the MCP
               host's stop does, so no path through this method skips it. */
            ReleaseServerCertificate();
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

        _adminArm?.Dispose();
        _adminArm = null;

        /* #3514 follow-up: the served certificate is gone, so stop advertising its expiry to the worker's
           alert sweep. A runtime disable of the dashboard (a no-restart op) reaches here; without the clear
           the worker keeps firing the expiry alert about a dashboard the operator turned off. A port-change
           rebind runs Stop→Start in the same supervisor tick, so the next start re-publishes before the
           hourly sweep can observe this null. */
        ReleaseServerCertificate();

        _oidcClient?.Dispose();
        _oidcClient = null;

        _runningPort = 0;
    }

    /// <summary>Disposes the held certificate (its private key) and updates the published certificate state, the way
    /// the MCP host's helper of the same name does. <paramref name="failedStart"/> keeps a published refusal or
    /// expired verdict (<see cref="ListenerTlsCertificateState.ClearUnlessRefusal"/>); a stop clears it whatever it is.</summary>
    private void ReleaseServerCertificate(bool failedStart = false)
    {
        try { _serverCertificate?.Dispose(); } catch { /* best-effort */ }
        _serverCertificate = null;
        if (failedStart)
        {
            _certState.ClearUnlessRefusal(DateTimeOffset.UtcNow);
        }
        else
        {
            _certState.Clear();
        }
    }

    /// <summary>Failed-start cleanup: a partially built app / data source must not leak between attempts.
    /// Internal so a test drives the real cleanup against the published certificate state.</summary>
    internal async Task DisposeFailedStartAsync()
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

        try { _adminArm?.Dispose(); } catch { /* best-effort */ }
        _adminArm = null;

        /* #3514 follow-up: a start that published its certificate (before the port-in-use / credential
           bail below it) but never served TLS must not leave the worker alerting on a non-served cert.
           #5288: a refusal or an expired certificate is the exception - the listener is loopback-only on that
           verdict whether or not the start failed, so it stays published until a successful load replaces it. */
        ReleaseServerCertificate(failedStart: true);

        try { _oidcClient?.Dispose(); } catch { /* best-effort */ }
        _oidcClient = null;
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

    /// <summary>The answer of <see cref="ResolveLoopbackOnlyToken"/>: the token the loopback-only dashboard's
    /// token-to-cookie gate compares against (empty only when none is configured and <c>web.allowTokenlessLoopback</c>
    /// is on, which leaves the dashboard open to local browsers for reads only), whether the start must stop, whether it
    /// stops because no token is configured at all, and why a resolved token is under the length floor
    /// (<see cref="DarlingHostBinding.DescribeTokenProblem"/>; null when it clears it). A token under the floor still gates
    /// sign-in, and the dashboard then serves reads only.</summary>
    internal readonly record struct LoopbackTokenResolution(bool Refuse, string Token, bool MissingToken = false, string? TokenProblem = null);

    /// <summary>The line a start with no token logs when it does not serve the dashboard: the settings to add, how a
    /// browser presents the token, and the opt-out.</summary>
    internal const string MissingTokenMessage =
        "The web dashboard is enabled but has no token, so it is not started. Set web.network.encryptedToken (from "
        + "--encrypt-password) or web.network.token (the token, env:NAME or file:PATH) in darling.json and restart the "
        + "service; the dashboard's sign-in page then asks for it. To serve local browsers without a token as before, set "
        + "web.allowTokenlessLoopback to true instead.";

    /// <summary>The line every start logs while the opt-out serves the dashboard with no token.</summary>
    internal const string TokenlessLoopbackMessage =
        "web.allowTokenlessLoopback is on: the web dashboard serves http://localhost with no token, for reads only. Set "
        + "web.network.encryptedToken or web.network.token to require one and to make changes.";

    /// <summary>The answer to a write the opt-out refuses: the dashboard it serves with no token makes no changes.</summary>
    internal const string TokenlessWriteRefusedMessage =
        "This dashboard serves reads only: web.allowTokenlessLoopback serves it with no token. To make changes, set "
        + "web.network.encryptedToken (or web.network.token) in darling.json, restart the service and sign in with the token.";

    /// <summary>The answer to a write refused because the dashboard's token is under the length floor.</summary>
    internal static readonly string ShortTokenWriteRefusedMessage =
        "This dashboard serves reads only: its token (web.network.encryptedToken or web.network.token) needs at least "
        + DarlingHostBinding.MinimumTokenLength + " characters and " + DarlingHostBinding.MinimumDistinctTokenCharacters
        + " different ones before it can make changes. Set a longer token in darling.json and restart the service.";

    /// <summary>The host slot of the OIDC redirect-URI line a start logs (#6609): the URI the operator registers at the IdP,
    /// so an IPv6 listen is bracketed the way the redirect_uri the service sends (built from the request's Host header) is.</summary>
    internal static string DescribeOidcRedirectHosts(string? listen) =>
        $"{DarlingListenUrl.FormatHost(listen)} or localhost";

    /// <summary>The line a start logs when its token is under the length floor: <paramref name="problem"/> is
    /// <see cref="DarlingHostBinding.DescribeTokenProblem"/>'s text, which never holds the token.</summary>
    internal static string ShortTokenReadsOnlyLine(string problem) =>
        $"Web dashboard token (web.network.encryptedToken or web.network.token) {problem}. Browsers still sign in with it, but the "
        + "dashboard serves reads only until a longer token is set.";

    /// <summary>
    /// The access token for a loopback-only start (no <c>web.network.listen</c>, or one the bind ladder refused). A
    /// token the operator configured gates the loopback dashboard whatever the bind mode, through the same
    /// token-to-cookie gate a network-exposed start uses. No token configured refuses the start
    /// (<c>MissingToken</c>), and logs <see cref="MissingTokenMessage"/> when <paramref name="reportMissingToken"/> is
    /// set, unless <paramref name="allowTokenlessLoopback"/> is on: then it is <c>(false, "")</c>, the dashboard serves
    /// local browsers without a token for reads only, and the start logs <see cref="TokenlessLoopbackMessage"/>. A token
    /// under the length floor is returned with its <c>TokenProblem</c>, after one Critical line
    /// (<see cref="ShortTokenReadsOnlyLine"/>): it still gates sign-in, and the dashboard serves reads only. A configured token that
    /// resolves to nothing or cannot be decrypted returns <c>Refuse</c> with one Critical line shaped like the
    /// network-mode token lines, because serving the dashboard open would drop the credential the operator asked every
    /// client to present. The caller owns what a refusal does (it fails the start). Never logs the token.
    /// </summary>
    internal static LoopbackTokenResolution ResolveLoopbackOnlyToken(
        WebNetworkConfig? network, bool allowTokenlessLoopback, ILogger logger, bool reportMissingToken = true)
    {
        if (network is null
            || (string.IsNullOrWhiteSpace(network.EncryptedToken) && string.IsNullOrWhiteSpace(network.Token)))
        {
            if (allowTokenlessLoopback)
            {
                logger.LogWarning("{Message}", TokenlessLoopbackMessage);
                return new LoopbackTokenResolution(false, "");
            }

            if (reportMissingToken)
            {
                logger.LogCritical("{Message}", MissingTokenMessage);
            }

            return new LoopbackTokenResolution(true, "", MissingToken: true);
        }

        try
        {
            var token = network.ResolveToken(out var usedPlaintext);
            if (string.IsNullOrWhiteSpace(token))
            {
                logger.LogCritical(
                    "Web dashboard token resolved to empty after decryption — refusing to serve the loopback listener without it; web dashboard not started.");
                return new LoopbackTokenResolution(true, "");
            }

            if (usedPlaintext)
            {
                logger.LogWarning(
                    "web.network.token is set in plaintext (dev convenience) — prefer web.network.encryptedToken " +
                    "(produced by --encrypt-password). This token gates web dashboard access, loopback included.");
            }

            var problem = DarlingHostBinding.DescribeTokenProblem(token);
            if (problem is not null)
            {
                logger.LogCritical("{Message}", ShortTokenReadsOnlyLine(problem));
            }

            return new LoopbackTokenResolution(false, token, TokenProblem: problem);
        }
        catch (Exception ex)
        {
            logger.LogCritical(
                "Web dashboard token could not be decrypted ({Message}) — refusing to serve the loopback listener without it; web dashboard not started.",
                ex.Message);
            return new LoopbackTokenResolution(true, "");
        }
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

            /* In network mode ResolveBind has already validated the listen IP, the allowFrom CIDR list (#5288),
               AND every entry's address-family agreement with the listen, so these two parses cannot throw; only
               resolving the token can still fail (a corrupt DPAPI blob), and a token that is blank or cannot be
               decrypted stops the start, as it does in loopback-only mode. The list type's default admits nobody,
               so the value the loopback mode never reads fails closed too. */
            IPAddress? networkListenIp = null;
            CidrAllowList allowedCidr = default;
            string accessToken = "";

            /* Why the resolved token is under the length floor, or null. A token under it still gates sign-in, but the
               shared-token seat then serves reads only (ConfigurePipeline's sharedTokenReadsOnlyProblem). */
            string? writeTokenProblem = null;
            if (networkMode)
            {
                networkListenIp = IPAddress.Parse(network!.Listen!.Trim());
                allowedCidr = CidrAllowList.Parse(network.AllowFrom!);

                try
                {
                    var token = network.ResolveToken(out var usedPlaintext);
                    if (string.IsNullOrWhiteSpace(token))
                    {
                        _logger.LogCritical(
                            "Web dashboard token resolved to empty after decryption; web dashboard not started.");
                        await DisposeFailedStartAsync();
                        return false;
                    }

                    /* Shorter than the floor. The wizard generates 32 characters; a short one comes from an
                       operator-typed value (a file:/env: reference in the compose distribution, or a plaintext
                       "token"). It is not used to gate the network: Critical, then loopback-only, with the token
                       still resolved so it keeps gating the loopback listener, the way a refused TLS
                       certificate degrades. */
                    var floor = DarlingHostBinding.ApplyTokenFloor(token, networkMode);
                    if (floor.Problem is not null)
                    {
                        _logger.LogCritical(
                            "Web dashboard token {Problem} — refusing to expose; binding loopback-only.",
                            floor.Problem);
                        _logger.LogCritical("{Message}", ShortTokenReadsOnlyLine(floor.Problem));
                        networkMode = floor.NetworkMode;
                        accessToken = token;
                        writeTokenProblem = floor.Problem;
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
                        "Web dashboard token could not be decrypted ({Message}); web dashboard not started.",
                        ex.Message);
                    await DisposeFailedStartAsync();
                    return false;
                }
            }
            else
            {
                /* A loopback-only start (no web.network.listen, or one the ladder refused) resolves a token the
                   operator configured too, and the pipeline then gates the loopback dashboard on it through the
                   token-to-cookie gate, as it gates one that network mode fell back to. No token configured stops this
                   start unless web.allowTokenlessLoopback is on, which leaves accessToken empty and the dashboard serving
                   local browsers with no token, for reads only. The no-token line is logged once per process: the
                   config is held for the process lifetime, so the 30s retries would only repeat it. A configured token
                   that cannot be used stops this start, because serving the dashboard open would drop the credential
                   the operator asked every client to present. */
                var loopbackToken = ResolveLoopbackOnlyToken(
                    network, web.AllowTokenlessLoopback, _logger, reportMissingToken: !_missingTokenReported);
                if (loopbackToken.Refuse)
                {
                    _missingTokenReported |= loopbackToken.MissingToken;
                    await DisposeFailedStartAsync();
                    return false;
                }

                accessToken = loopbackToken.Token;
                writeTokenProblem = loopbackToken.TokenProblem;
            }

            /* OIDC sign-in (#2550) — resolved once per start, like the token and the certificate. A
               misconfiguration DISABLES OIDC and says so at Critical, but does NOT degrade the bind: OIDC is
               additive — the dashboard without it is exactly the pre-#2550 dashboard, still behind the
               token→cookie gate — so taking the whole surface down over it would punish the operator for
               attempting more security than they had yesterday. */
            DarlingWebOidcClient? oidcClient = null;
            if (networkMode && network!.Oidc is { IsConfigured: true } oidcConfig)
            {
                var problem = DarlingWebOidc.ValidateConfig(oidcConfig);
                string? clientSecret = null;
                if (problem is null)
                {
                    try
                    {
                        clientSecret = oidcConfig.ResolveClientSecret(out var usedPlaintextSecret);
                        if (usedPlaintextSecret)
                        {
                            _logger.LogWarning(
                                "web.network.oidc.clientSecret is set in plaintext (dev convenience) — prefer encryptedClientSecret (produced by --encrypt-password).");
                        }
                    }
                    catch (Exception ex)
                    {
                        problem = $"the client secret could not be resolved: {ex.Message}";
                    }
                }

                if (problem is not null)
                {
                    _logger.LogCritical(
                        "web.network.oidc is misconfigured ({Problem}) — OIDC sign-in is DISABLED this start; the shared token remains the only web credential.",
                        problem);
                }
                else
                {
                    oidcClient = new DarlingWebOidcClient(new DarlingWebOidcClient.ResolvedOptions(
                        oidcConfig.Authority!.Trim(),
                        oidcConfig.ClientId!.Trim(),
                        clientSecret,
                        DarlingWebOidc.EffectiveScopes(oidcConfig.Scopes),
                        string.IsNullOrWhiteSpace(oidcConfig.SubjectClaim) ? null : oidcConfig.SubjectClaim.Trim(),
                        string.IsNullOrWhiteSpace(oidcConfig.RoleClaim) ? null : oidcConfig.RoleClaim.Trim(),
                        oidcConfig.AdminRoles ?? Array.Empty<string>(),
                        oidcConfig.ViewerRoles ?? Array.Empty<string>()));
                    _oidcClient = oidcClient;

                    if (clientSecret is null)
                    {
                        _logger.LogWarning(
                            "web.network.oidc has no client secret — the code exchange will run as a PUBLIC client on PKCE alone. Some providers refuse this for web apps; register a confidential client and set encryptedClientSecret if sign-ins fail at the exchange.");
                    }

                    /* Name the redirect-URI shape at start, because it is the one thing the operator has to
                       register at the IdP and the one thing some IdPs (Okta) refuse for a bare IP. */
                    _logger.LogInformation(
                        "OIDC sign-in enabled — authority {Authority}, clientId {ClientId}, role mapping {RoleMapping}. Register the redirect URI(s) your browsers will actually use: {{scheme}}://{{host}}:{Port}/auth/oidc/callback (host = {Hosts}). The shared token keeps working beside OIDC as the scripted-caller/break-glass path.",
                        oidcClient.Options.Authority,
                        oidcClient.Options.ClientId,
                        oidcClient.Options.RoleClaim is null
                            ? "none (every signed-in user gets the edit seat, the shared token's reach)"
                            : $"claim '{oidcClient.Options.RoleClaim}' -> {oidcClient.Options.AdminRoles.Count} admin / {oidcClient.Options.ViewerRoles.Count} viewer value(s), unmatched users refused",
                        effectivePort,
                        DescribeOidcRedirectHosts(network.Listen));
                }
            }

            /* Reverse proxies whose forwarded headers are honored — resolved here like the token, BEFORE the certificate is
               loaded (so a refusal never leaves a certificate published for a listener that is not exposed), and an entry that does not parse fails closed the same way: Critical, then
               loopback-only. Assuming a proxy list would either trust a header from nobody (the setting
               silently doing nothing) or from everybody (any client's X-Forwarded-Proto marks a cookie Secure). */
            IReadOnlyList<IPNetwork>? trustedProxies = null;
            if (networkMode
                && !DarlingHostBinding.TryParseTrustedProxies(network!.TrustedProxies, out trustedProxies, out var proxyProblem))
            {
                _logger.LogCritical(
                    "web.network.trustedProxies is misconfigured ({Problem}) — refusing to expose; binding loopback-only.",
                    proxyProblem);
                networkMode = false;
                trustedProxies = null;
            }

            /* #4220: web.publicBaseUrl's host, admitted as one extra allowed Host header value — see
               ConfigurePipeline's publicBaseUrlHost param and TriageLink.TryGetHost. Pure, so it is computed
               BEFORE the TLS call (#5288 review F11): the certificate's name check needs the same value, since a
               certificate that names this host is not mismatched even with no iPAddress SAN. */
            var publicBaseUrlHost = TriageLink.TryGetHost(web.PublicBaseUrl);

            /* TLS for the network listener (#2562). Resolved HERE rather than in the pure ResolveBind ladder for
               the same reason the token is: loading a certificate reads files and a clock, and the ladder is
               kept free of both. It also means a certificate failure degrades to loopback-only after a Critical line
               (the token stays resolved and keeps gating it) instead of needing its own BindReason, which the MCP host's
               parallel enum would have had to grow a member it can never use.

               #5288: the block itself lives in DarlingListenerTls.Resolve now, shared with the MCP host, so
               there is one fail-closed path and not two copies of it. What stays here is what only this host
               can do: adopt the certificate, and refuse to expose when TLS was asked for and no certificate
               came back. */
            DarlingWebTls.LoadedCertificate? serverCertificate = null;
            if (networkMode)
            {
                var tlsOutcome = DarlingListenerTls.Resolve(
                    _logger, _certState, ListenerTlsLabels.Web, network!.Tls, networkListenIp!, effectivePort, publicBaseUrlHost);
                networkMode = tlsOutcome.Expose;

                /* Adopted by the field IMMEDIATELY, before any of the bail paths below it (port in
                   use, store credential not ready), so every one of them releases the key. Resolve owned the
                   certificate until it returned; from this line the field does. */
                serverCertificate = tlsOutcome.Certificate;
                _serverCertificate = tlsOutcome.Certificate;

                /* #5288 review F1: Kestrel decides HTTPS from "the listener was handed a certificate", not
                   from "TLS was configured", so TLS asked for + network mode + no certificate would bind the
                   LAN address in plain HTTP. Resolve never returns that. This refuses it anyway, with its own
                   Critical line, because the cost of being wrong is cleartext on the segment. */
                if (tlsOutcome.ExposesWithoutItsCertificate)
                {
                    _logger.LogCritical(
                        "Web dashboard TLS is configured ({Shape}) but no certificate came back — refusing to expose; binding loopback-only.",
                        tlsOutcome.Shape);
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
                await DisposeFailedStartAsync();
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

            /* #6840: a front door holds no store of its own, so it skips the store wait and everything built on the store connection: the
               viewer-role pool, the Admin arm and the endpoint map. The web host's sign-in, role gate, Host guard and session cookie are
               the same ones; only the routes behind them change (DarlingFrontDoorWebProxy replaces DarlingWebEndpoints.MapAll). */
            NpgsqlDataSource? postgres = null;
            DarlingFrontDoorWebProxy? frontDoorProxy = null;
            if (config.IsFrontDoor)
            {
                var frontDoorDirectory = _services?.GetService<IFrontDoorDirectory>();
                var frontDoorSecrets = _services?.GetService<IFrontDoorSecrets>();
                if (frontDoorDirectory is null || frontDoorSecrets is null)
                {
                    _logger.LogError("Web dashboard not started: frontDoor.enabled is set but this host has no front-door directory to route with");
                    await DisposeFailedStartAsync();
                    return false;
                }

                frontDoorProxy = DarlingFrontDoorWebProxy.Create(frontDoorDirectory, frontDoorSecrets, config.FrontDoor, _logger, _collectorState.Read);

                /* Every user who signs in with no role mapping holds the edit seat, and through the front door a change they make runs on
                   whichever store it is sent to. */
                if (_oidcClient is { } frontDoorOidc && frontDoorOidc.Options.RoleClaim is null)
                {
                    _logger.LogWarning(
                        "web.network.oidc has no role mapping, so every user who signs in has the edit seat at the front door, and a change they make runs on the store it is sent to. "
                        + "Set web.network.oidc.roleClaim with adminRoles and viewerRoles to limit who can edit.");
                }
            }
            else
            {
                /* Managed mode: the WORKER owns the bundled server's lifecycle; the web host only derives the
                   least-privilege VIEWER-role connection string from the stored DPAPI credential. */
                string? storeConnectionString;
                if (config.Postgres.Managed)
                {
                    if (!OperatingSystem.IsWindows())
                    {
                        _logger.LogError("Web dashboard not started: postgres.managed = true requires Windows");
                        await DisposeFailedStartAsync();
                        return false;
                    }

                    storeConnectionString = await WaitForManagedConnectionStringAsync(config.Postgres, stoppingToken);
                    if (storeConnectionString is null)
                    {
                        await DisposeFailedStartAsync();
                        return false;
                    }
                }
                else
                {
                    /* #3914: not the owner by default any more — postgres.webConnectionString, the viewer role the
                       service provisioned on the compose store, or the owner with a warning, in that order. */
                    storeConnectionString = await DarlingStoreLogins.ResolveUnmanagedAsync(
                        DarlingStoreLogins.Surface.Web, config.Postgres, _logger, stoppingToken);
                    if (storeConnectionString is null)
                    {
                        await DisposeFailedStartAsync();
                        return false;
                    }
                }

                /* A string Npgsql cannot use throws from the Create below into the catch at the end of this method,
                   which logs the exception's message, and Npgsql's text for that can quote part of the string (a password
                   with an unquoted ';' makes the text after it the "keyword", and a password tail that becomes a Host
                   value with a ':' in it makes the text after the ':' a bad port). So prove it first, with Npgsql's own
                   construction step (the guard builds and disposes a data source, and opens nothing), and log the
                   formatter's text instead. The keywords added below (application name, session time zone) take part in
                   none of Npgsql's construction checks, so a string that passes here does not throw from the Create below
                   for a parse or a validation reason. */
                if (DarlingStoreConnection.TryDescribeParseFailure(storeConnectionString) is { } unusableStoreString)
                {
                    _logger.LogError(
                        "Web dashboard not started this attempt: its store connection string (postgres.webConnectionString, or postgres.connectionString when that is not set) is unusable: {Detail}",
                        unusableStoreString);
                    await DisposeFailedStartAsync();
                    return false;
                }

                /* Lifetime tied to the running app: disposed by StopServerAsync, not this method's scope. #4479:
                   the viewer-role connection string built by DarlingManagedPostgres already carries
                   WebApplicationName, but a CONFIGURED (postgres.webConnectionString) or owner-fallback login
                   never runs through that builder — set-if-absent here so every path this string can take still
                   names the surface. */
                /* #6630: a store the operator brings gets the worker's collect/config search path the same way (set-if-absent), because the
                   bare names the reads use (servers, config_service, ...) only resolve through it, and the database default is best effort. */
                postgres = DarlingWorker.CreateHostStoreDataSource(storeConnectionString, DarlingManagedPostgres.WebApplicationName);
                _appDataSource = postgres;

                /* #6600: on the managed store, a signed-in admin's Admin writes use a second, small pool on the stored admin login. It opens
                   nothing until the first such request. A store you bring yourself, or web.allowAdminWrites off, builds none. */
                if (OperatingSystem.IsWindows())
                {
                    _adminArm = WebAdminArm.ForHost(config.Postgres, web.AllowAdminWrites, _logger);
                }

                /* The MCP host warns the same way while its twin setting is on: the state of this gate is otherwise visible nowhere at start. */
                if (web.AllowWindowsAuthOverUnverifiedTls)
                {
                    _logger.LogWarning(
                        "{Setting} is on: the dashboard's server add and edit accept Windows authentication with trust_server_certificate or " +
                        "optional encryption, so a signed-in editor can point this service's own account at a server it cannot verify.",
                        DarlingUnverifiedWindowsAuthScreen.WebSetting);
                }

                /* Off Windows there is no managed store and no admin pool, but OIDC without a role mapping still gives every signed-in user the edit seat. */
                WebAdminArm.WarnWhenOidcHasNoRoleMapping(_oidcClient, DarlingWebStoreWrites.IsManaged(config.Postgres), _adminArm, _logger);
            }

            /* FOOTGUN (load-bearing): pin BOTH the content root AND the web root to the binary's directory. A
               Windows service's CWD is System32, so the parameterless CreateBuilder would 404 every static file
               in production only. WebRootPath is resolved against ContentRootPath -> AppContext.BaseDirectory/wwwroot. */
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = AppContext.BaseDirectory,
                WebRootPath = "wwwroot",
                /* #4281 review, finding 4: with no EnvironmentName set here, an ASPNETCORE_ENVIRONMENT or
                   DOTNET_ENVIRONMENT of "Development" left set anywhere on the machine (a leftover from testing
                   something unrelated) would add the developer exception page ahead of the Host guard — a
                   caller with NO credentials then gets the exception message and stack trace for any throw
                   that escapes. Pinned so the ambient variable can never reach this host. */
                EnvironmentName = Environments.Production,
            });

            /* #5288: the same kind of pin for the framework's forwarded-headers switch, on the very next line so no
               service is registered between the builder and the pin. The helper lives with the MCP host, which
               pins its own builder with it; see DarlingMcpHostService.PinForwardedHeadersOff. */
            DarlingMcpHostService.PinForwardedHeadersOff(builder.Services);

            var listenerCertificate = serverCertificate;
            builder.WebHost.ConfigureKestrel(options =>
            {
                /* #5288: the listeners below are the ONLY ones this host binds. Kestrel adds the endpoints written in
                   the "Kestrel:Endpoints" section of configuration (an appsettings file in the content root, or a
                   Kestrel__Endpoints__* variable in the service environment) to whatever the code listens on, with
                   none of the scheme or certificate decisions made here. Handing it an empty configuration replaces
                   that loader, so the layout below is the whole layout. The MCP host's ConfigureListeners starts
                   with the same line. */
                options.Configure(new ConfigurationBuilder().Build());

                if (networkMode)
                {
                    /* Bind the specific family, then ALSO both loopback families (unless the listen is itself
                       loopback/wildcard, which would collide) so a local client resolving "localhost" still
                       reaches the dashboard. Loopback is exempt from the CIDR test only — since #1649 it
                       authenticates like any other remote. */
                    options.Listen(primaryBind, effectivePort, listen =>
                    {
                        if (listenerCertificate is not null)
                        {
                            /* The leaf AND its intermediates, through the body both hosts share (#5288):
                               Kestrel presents ONLY what it is handed, so see DarlingListenerTls.ConfigureHttps
                               for why the chain travels with the leaf. This is the one HTTPS call in the file,
                               and it sits on the network listener alone. */
                            listen.UseHttps(https => DarlingListenerTls.ConfigureHttps(https, listenerCertificate.Value));
                        }
                    });

                    if (DarlingHostBinding.ShouldAddLoopbackListeners(primaryBind))
                    {
                        /* The loopback listeners stay PLAIN HTTP even when the network listener is TLS, and
                           that is a decision rather than an oversight. The certificate names the LAN address
                           the operator exposes; it almost never also names "localhost", so serving TLS here
                           would hand the local browser a name-mismatch warning on the one surface that never
                           leaves the machine. Nothing is lost: loopback traffic is not on the segment this
                           feature protects. When the listen IS a wildcard, ShouldAddLoopbackListeners is false
                           and the single listener is TLS for everyone — the operator asked for all interfaces.

                           This is also the answer to "redirect or refuse" for the exposed address: one port
                           cannot speak both schemes, so a plain-HTTP client hitting the TLS listener fails at
                           the handshake. Adding a second HTTP port to redirect would re-open, on a new port,
                           the cleartext surface this exists to close. */
                        options.Listen(IPAddress.Loopback, effectivePort);
                        options.Listen(IPAddress.IPv6Loopback, effectivePort);
                    }
                }
                else
                {
                    /* The default/degraded loopback-only server — both families, always plain HTTP. */
                    options.ListenLocalhost(effectivePort);
                }
            });

            /* The logging split, both halves decided here: ASP.NET framework/request noise is deliberately
               SILENCED (ClearProviders — the service's own logger narrates lifecycle, and Kestrel's
               per-request chatter has no seat in the service log), and the APP-LEVEL log-and-degrade lines
               are deliberately ROUTED to the service's real logger, by handing _logger to MapAll's endpoint
               seats below. The two are one design: clearing providers makes app.Logger a logger with nowhere
               to write, so an endpoint that logged through it would degrade with no trace — the exact gap
               the MCP host closed for its tools with AddSingleton<ILogger>(_logger) (#3473 review), closed
               here at the wiring seam instead because this host maps routes directly rather than resolving
               tool parameters through DI. */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* The VIEWER-role store pool the read endpoints query (registered for DI + passed to MapAll). */
            if (postgres is not null)
            {
                builder.Services.AddSingleton<NpgsqlDataSource>(postgres);
            }

            /* #4188: Brotli/gzip for the JSON API and the static JS/CSS/HTML. A services-collection step, so it
               has to run on the BUILDER, ahead of Build() - see ConfigureResponseCompression's own doc for why
               this is a method the live-HTTP test calls too rather than a block copied into it. */
            ConfigureResponseCompression(builder.Services);

            _app = builder.Build();
            /* #4214 part 2 / round-1 review Low 3: trimmed copy, not config.Postgres itself — see the
               matching comment at DarlingMcpHostService.cs's AddSingleton(PostgresConfig) registration. */
            var storeHostPostgresConfig = new PostgresConfig { Managed = config.Postgres.Managed, DataDirectory = config.Postgres.DataDirectory };

            /* #5288: a start that was asked to expose and fell back to loopback-only AFTER its token resolved (the
               TLS block above refused) keeps the token-to-cookie gate on the loopback server, because the operator's
               config said every client presents the token. accessToken holds the resolved token in both branches above
               (the network branch, and the loopback-only branch via ResolveLoopbackOnlyToken), so "not network mode,
               token set" covers that state and a loopback-only start whose configured token resolved. A start
               with no token configured gets here only with web.allowTokenlessLoopback on, leaves it empty and serves
               reads only with no token; a configured token that cannot be used never gets here, the start stops in every mode. */
            var requireTokenWhenLoopbackOnly = !networkMode && accessToken.Length > 0;

            /* #6265: a web token with the MCP token's value is refused for web sign-in (the pipeline checks each posted token). The
               service still starts, so an upgrade of such a setup keeps running; this says why the token form answers 403. */
            var mcpToken = DarlingAgentDispatchChecks.ResolveToken(config.Mcp).Token;
            if (DarlingAgentDispatchChecks.WebTokenIsMcpToken(accessToken, mcpToken))
            {
                _logger.LogWarning("{Message}", DarlingAgentDispatchChecks.WebTokenSameAsMcpMessage);
            }

            /* #5288: the Host guard admits the listen address only while the server is exposed on it. networkListenIp
               was parsed before any degrade and still holds it afterwards, so it goes through the MCP host's
               ResolveHostGuardListenIp (the one rule both hosts share) with the final mode: a start that fell back to
               loopback-only admits loopback names only, whatever it parsed. */
            ConfigurePipeline(_app, postgres, networkMode, DarlingMcpHostService.ResolveHostGuardListenIp(networkMode, networkListenIp), allowedCidr, accessToken, oidcClient, publicBaseUrlHost, storeHostPostgresConfig, config.Analyzer, requireTokenWhenLoopbackOnly, trustedProxies: trustedProxies, sendHsts: network?.Hsts ?? false, dataDetail: McpDataDetailModes.Resolve(config.Mcp.DataDetail), mcpToken: mcpToken, sharedTokenReadsOnlyProblem: writeTokenProblem, adminArm: _adminArm, allowWindowsAuthOverUnverifiedTls: web.AllowWindowsAuthOverUnverifiedTls, frontDoorProxy: frontDoorProxy);

            /* #2389: name the authority for each half of what is being started — enabled/port from whichever
               plane the supervisor resolved, listen/allowFrom/token always from darling.json. */
            var origin = DarlingHostBinding.DescribeToggleOrigin(toggle);
            if (networkMode)
            {
                /* #2562: name the SCHEME the exposed listener actually speaks. An operator reading this line is
                   deciding whether the token they are about to paste crosses the wire in the clear, and the
                   line used to say "http://" unconditionally because that was the only thing it could be.

                   #5288 review F11: and name loopback as plain HTTP only when it is. With TLS on and a wildcard
                   listen there is ONE listener, HTTPS for loopback too, and saying otherwise sends a local
                   client to the wrong scheme. With no TLS the text is unchanged. Text only: no listener
                   decision moves. */
                // install-darling.ps1 waits for this text (#6470): reword both together.
                _logger.LogInformation(
                    "Starting web dashboard on {Scheme}://{Listen}:{Port} (LAN-exposed to {Cidr} behind a token->cookie gate + in-app CIDR; "
                    + "{Loopback}, and since #1649 it authenticates too) — "
                    + "enabled/port from {Origin}; listen/allowFrom/token/tls from darling.json web.network (file-only, restart-only)",
                    serverCertificate is null ? "http" : "https", DarlingListenUrl.FormatHost(primaryBind), effectivePort, allowedCidr,
                    DarlingListenerTls.DescribeLoopbackListener(
                        "loopback also bound over plain HTTP", serverCertificate is not null, primaryBind),
                    origin);
            }
            else
            {
                // install-darling.ps1 waits for this text (#6470): reword both together.
                _logger.LogInformation(
                    "Starting web dashboard on http://localhost:{Port} (loopback only){TokenNote} — enabled/port from {Origin}",
                    effectivePort, requireTokenWhenLoopbackOnly ? ", token required" : "", origin);
                if (requireTokenWhenLoopbackOnly)
                {
                    _logger.LogInformation(
                        "The web dashboard loopback listener requires the web.network token: local browsers and clients present it too.");
                }
            }

            /* #2479 item 6: the network block is read ONCE and held for the process lifetime by design.
               Say so at every start, in BOTH modes - the loopback line above never mentioned the block at
               all, and loopback-when-you-expected-LAN is exactly the state being diagnosed. */
            _logger.LogInformation(
                "{Report}",
                DarlingHostBinding.DescribeNetworkBlockLifetime(
                    "web", "Web dashboard", config.Web.Network?.IsConfigured ?? false, networkMode,
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
            /* Normal shutdown mid-start — still release anything already acquired (#2562: the certificate's
               private key is held in the machine key store until it is disposed). */
            await DisposeFailedStartAsync();
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
    internal enum WebAuthAction { Allow, Forbid, ShowLogin }

    /// <summary>
    /// PURE Host-header allowlist — the DNS-rebinding guard. A request is served only when its Host names an
    /// address this host actually binds: a loopback name/IP (<c>localhost</c>, <c>127.0.0.1</c>, <c>::1</c>) or,
    /// when LAN-exposed, the configured listen IP (<paramref name="networkListenIp"/>). Any port suffix is
    /// ignored (already split into <see cref="HostString.Host"/>), and an empty Host is allowed (HTTP/1.0 / some
    /// health probes). This rejects a rebound foreign hostname pointed at 127.0.0.1:5153 before any auth
    /// decision, while a direct IP request from the real operator passes.
    ///
    /// <para>#1648 lifted the decision itself into the shared
    /// <see cref="PerformanceMonitor.Common.HostHeaderGuard"/> (via <see cref="DarlingHostBinding"/>) so the two
    /// MCP hosts — Darling's and Lite's — install the SAME guard instead of going without one. This forwarder
    /// stays so this host's behavior and its existing tests are byte-for-byte unchanged for every 2-arg caller.</para>
    ///
    /// <para><paramref name="extraAllowedHost"/> (#4220): this web host, and only this web host, also admits
    /// <c>web.publicBaseUrl</c>'s host — see <see cref="DarlingHostBinding.IsAllowedHost"/> for why that is
    /// safe. Defaults to null.</para>
    /// </summary>
    internal static bool IsAllowedHost(string? host, IPAddress? networkListenIp, string? extraAllowedHost = null)
        => DarlingHostBinding.IsAllowedHost(host, networkListenIp, extraAllowedHost);

    /// <summary>
    /// PURE route-auth decision. This method is only ever reached in NETWORK mode, or on a loopback-only server
    /// that has a configured token (#5288) — the caller registers the auth middleware only for those. A
    /// loopback-only dashboard with no token runs only under <c>web.allowTokenlessLoopback</c>, which registers a
    /// gate that serves reads only instead of this one.
    ///
    /// <para>Loopback skips the CIDR check (127.0.0.1 is not in a LAN CIDR, so testing it there would 403 the
    /// operator's own browser) but still needs a session cookie or a signed-in token, exactly like any
    /// other remote. It previously passed with no credential on the grounds that the web surface was read-only; that
    /// stopped being true when Custom Views v2 added view create/update/delete and <c>/api/compose/run</c>, so
    /// any local process could read the whole store and change views with no credential while the host was
    /// LAN-exposed (#1649). This now mirrors the MCP host's rule verbatim: in exposed mode even a local client
    /// presents the token.</para>
    ///
    /// <para>A non-loopback remote must additionally be inside <paramref name="allowedCidr"/> (a
    /// null/unverifiable remote fails closed to <see cref="WebAuthAction.Forbid"/>). Then a valid session
    /// cookie passes and anything else gets the login form. A token is never a verdict input: the sign-in form's
    /// POST to <see cref="LoginPath"/> is handled as a flow route, before this decision.</para>
    /// </summary>
    internal static WebAuthAction DecideWebAuth(
        IPAddress? remoteIp, CidrAllowList allowedCidr, bool hasValidCookie, IPAddress? connectionIp = null)
    {
        /* Loopback determination lives in DarlingWebEndpoints.IsLoopbackRemote so every caller unwraps
           IPv4-mapped-IPv6 (::ffff:127.0.0.1) identically. Loopback is exempt from the CIDR test ONLY — it
           still has to authenticate below. */
        var isLoopback = IsLoopbackConnection(remoteIp, connectionIp);

        if (!isLoopback && !IsInAllowFrom(remoteIp, allowedCidr))
        {
            return WebAuthAction.Forbid;
        }

        if (hasValidCookie)
        {
            return WebAuthAction.Allow;
        }

        return WebAuthAction.ShowLogin;
    }

    /// <summary>
    /// Registers Brotli + gzip response compression (#4188) for the JSON API and the static JS/CSS/HTML the
    /// dashboard ships. A services-collection step — <c>AddResponseCompression</c> has to run on the
    /// <c>WebApplicationBuilder</c>, before <c>Build()</c>, unlike <see cref="ConfigurePipeline"/>'s <c>Use*</c>
    /// calls which run on the built <c>app</c> — so it is its own static method, called from the production
    /// builder in <c>TryStartServerAsync</c> right before <c>builder.Build()</c> and from the live-HTTP test
    /// that builds the SAME pipeline (<c>DarlingWebResponseCompressionTests</c>), so that test proves the real
    /// registration rather than a hand-copied one that could drift from it, exactly the reasoning #4128 gives
    /// for extracting <see cref="ConfigurePipeline"/> itself.
    ///
    /// <para><b>BREACH.</b> Compressing a response lets an attacker who controls part of the request and can
    /// observe the compressed length recover a FIXED secret carried in the SAME response, when the two sit
    /// side by side (the classic case: a reflected query string next to a CSRF token). Nothing on this host
    /// does that: the session identity lives in the HttpOnly cookie, never in a response body, the mutation
    /// routes are gated by an <c>application/json</c> content-type check rather than a body-embedded token
    /// (see <see cref="DarlingWebEndpoints.MapAll"/>'s CSRF notes), and the one page that renders before auth
    /// — the login form — is a static template that echoes no query string, header, or path segment (its
    /// <c>#host</c> div is filled by client-side script, not server-rendered). <c>EnableForHttps</c> is
    /// therefore safe with what this host serves today; an endpoint added later that echoes request input
    /// beside a secret in one body would need to opt out.</para>
    ///
    /// <para>MIME types: the framework defaults (<c>text/html</c>/<c>text/css</c>/JS/etc.) minus
    /// <c>application/json</c> and <c>text/json</c>. Both providers run at
    /// <see cref="CompressionLevel.Fastest"/>. JSON is excluded explicitly because
    /// <see cref="ResponseCompressionDefaults.MimeTypes"/> already includes it — just omitting the
    /// <c>Append</c> call is not enough. The <c>/api/*</c> routes carry the session cookie alongside
    /// attacker-readable paths, so compressing JSON beside a fixed secret would open a BREACH oracle.
    /// The <c>/api/*</c> responses also carry <c>Cache-Control: no-store</c>, which further limits
    /// exposure.</para>
    /// </summary>
    internal static void ConfigureResponseCompression(IServiceCollection services)
    {
        services.AddResponseCompression(options =>
        {
            options.EnableForHttps = true;
            options.Providers.Add<BrotliCompressionProvider>();
            options.Providers.Add<GzipCompressionProvider>();
            // Exclude JSON — BREACH: the /api/* routes reflect attacker-controlled paths alongside the session
            // cookie. ResponseCompressionDefaults.MimeTypes includes application/json and text/json, so we
            // must explicitly override the list rather than simply omitting the Append call.
            options.MimeTypes = ResponseCompressionDefaults.MimeTypes
                .Except(new[] { "application/json", "text/json" }, StringComparer.OrdinalIgnoreCase);
        });

        services.Configure<BrotliCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);
        services.Configure<GzipCompressionProviderOptions>(o => o.Level = CompressionLevel.Fastest);
    }

    /* ---------------------------------------------------------------------------------------------------
       OIDC sign-in (#2550). The flow endpoints, the per-request decision that routes to them, and the
       handler. DecideWebAuth above is untouched — its matrix is the pinned pre-OIDC behavior, and
       DecideWebRequest composes AROUND it rather than growing it, so the token path cannot regress by
       construction.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// Everything AFTER <c>builder.Build()</c>: response compression (#4188), the Host-allowlist/DNS-rebinding
    /// guard (both modes), the network-mode auth middleware, an <c>/api/*</c> no-store stamp, then
    /// <see cref="DarlingWebEndpoints.MapAll"/> -> <c>UseDefaultFiles</c> -> <c>UseStaticFiles</c> (no-cache).
    /// Extracted (#4128) so a live-HTTP test can build the SAME pipeline against a
    /// <c>TestServer</c> instead of a second, hand-copied one that could silently drift from production. The
    /// production call site passes exactly these values, in exactly this order — see <c>TryStartServerAsync</c>.
    /// Instance method, not static: the gates call back into <see cref="HandleAuthFlowAsync"/> and
    /// <see cref="ReportSignInRefusal"/>, and read <c>_logger</c>/<c>_collectorState</c>/<c>_baselineCache</c>,
    /// exactly as before the extraction — only the receiver (this vs. a test-constructed instance) changes.
    /// </summary>
    /// <param name="publicBaseUrlHost">#4220: <c>web.publicBaseUrl</c>'s host, or null when unset/unparseable
    /// — the one extra Host value the DNS-rebinding guard admits beside the loopback names and
    /// <paramref name="networkListenIp"/>. See <see cref="DarlingHostBinding.IsAllowedHost"/>. #5288: the caller
    /// passes the raw host (<c>Uri.Host</c>, in whichever spelling the operator wrote); the guard converts its own
    /// copy once to the decoded form <c>HttpRequest.Host</c> gives the middleware, so a punycode and a Unicode
    /// spelling both admit the punycode Host a browser sends.</param>
    /// <param name="requireTokenWhenLoopbackOnly">#5288: keeps the token-to-cookie gate on a loopback-only server. The
    /// host passes true when network mode was configured and its token resolved, and the start then fell back to
    /// loopback-only (a refused TLS certificate): the operator's config said every client presents the token, so a
    /// lapsed certificate does not leave the local listener open. It is also true for a loopback-only start with a
    /// configured token (<see cref="ResolveLoopbackOnlyToken"/>). A loopback-only server with no token configured
    /// passes false and registers the reads-only gate instead of the auth middleware. The
    /// CIDR check inside the gate is a no-op for loopback, the only peers such a server has. Optional, so every
    /// other caller is unchanged.</param>
    /// <param name="sendHsts"><c>web.network.hsts</c>: send <c>Strict-Transport-Security</c> on a response to an
    /// HTTPS request. Off by default (see <see cref="DarlingWebSecurityHeaders.StrictTransportSecurityValue"/>).</param>
    /// <param name="trustedProxies">The reverse proxies whose <c>X-Forwarded-For</c> / <c>-Proto</c> /
    /// <c>-Host</c> headers are honored (<c>web.network.trustedProxies</c>, parsed by
    /// <see cref="DarlingHostBinding.TryParseTrustedProxies"/>). Null or empty (the default) means the headers
    /// are ignored from every source, so the scheme, the client address and the host are always the
    /// connection's own.</param>
    /// <param name="dataDetail">The <c>mcp.dataDetail</c> level the dashboard's investigation download masks at (#6236), read from the file when the
    /// host starts. The default is the strictest level, so a caller that does not say never serves more than it was told to.</param>
    /// <param name="utcNow">The clock the sign-in gate's per-source wait reads (null is the system clock). A test
    /// passes a settable one so the wait can be crossed without sleeping.</param>
    /// <param name="sharedTokenReadsOnlyProblem">Why the access token is under the length floor
    /// (<see cref="DarlingHostBinding.DescribeTokenProblem"/>), or null. When set, a session signed in with the shared token
    /// serves reads only and a write is answered with <see cref="ShortTokenWriteRefusedMessage"/>; OIDC seats are unchanged.</param>
    internal void ConfigurePipeline(
        WebApplication app,
        NpgsqlDataSource? postgres,
        bool networkMode,
        IPAddress? networkListenIp,
        CidrAllowList allowedCidr,
        string accessToken,
        DarlingWebOidcClient? oidcClient,
        string? publicBaseUrlHost = null,
        PostgresConfig? postgresConfig = null,
        PerformanceMonitor.PlanAnalysis.AnalyzerConfig? analyzerConfig = null,
        bool requireTokenWhenLoopbackOnly = false,
        byte[]? sessionSigningKeyForTests = null,
        IReadOnlyList<IPNetwork>? trustedProxies = null,
        bool sendHsts = false,
        Func<DateTime>? utcNow = null,
        McpDataDetail dataDetail = McpDataDetail.MetadataOnly,
        string? mcpToken = null,
        string? sharedTokenReadsOnlyProblem = null,
        WebAdminArm? adminArm = null,
        bool allowWindowsAuthOverUnverifiedTls = false,
        DarlingFrontDoorWebProxy? frontDoorProxy = null)
    {
        /* A caller that does not say whether the token is under the length floor gets the same answer the start path
           computes: a short shared token serves reads only. Without this a forgotten argument meant writes. */
        sharedTokenReadsOnlyProblem ??= accessToken.Length > 0 ? DarlingHostBinding.DescribeTokenProblem(accessToken) : null;

        Func<DateTime> clock = utcNow ?? (() => DateTime.UtcNow);

        /* #2479 item 5: the gates below used to refuse silently. Rate-limited per (gate, source),
           because this port is LAN-exposed on purpose - see DarlingHttpRefusalLog. Created per
           started server so a rebind starts with a clean budget. */
        var refusals = new DarlingHttpRefusalLog();

        /* Forwarded headers, from the CONFIGURED proxies only, and ahead of every gate so the Host guard sees
           the forwarded Host, the CIDR gate and the sign-in throttle see the real client address, and
           Request.IsHttps (the cookie's Secure flag, HSTS, the OIDC redirect_uri) sees the proxy's scheme.
           Behind a TLS-terminating proxy the connection to this host is plain HTTP, so without this the cookie
           lost Secure and the OIDC redirect_uri became http://, which the IdP refuses. The framework's default
           trusts loopback proxies; cleared, so that only the operator's list is trusted. ForwardLimit stays 1:
           the rightmost X-Forwarded-For entry is the one the trusted proxy itself appended. It sits ahead of the
           failure observer too, so the observer's throttled lines carry the client address, not the proxy's. */
        if (trustedProxies is { Count: > 0 })
        {
            var forwarded = new ForwardedHeadersOptions
            {
                ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto | ForwardedHeaders.XForwardedHost,
                ForwardLimit = 1,
            };
            forwarded.KnownProxies.Clear();
            forwarded.KnownIPNetworks.Clear();
            foreach (var proxy in trustedProxies)
            {
                forwarded.KnownIPNetworks.Add(proxy);
            }

            /* The connection's own address, kept before the middleware below replaces RemoteIpAddress with the
               forwarded client: only the connection can be loopback for the allowFrom exemption (L2). */
            app.UseMiddleware<DarlingConnectionAddressStamp>();

            app.UseForwardedHeaders(forwarded);
        }

        /* Pipeline order: the failure observer (W12, which folded in the #4276 backstop), then response compression (#4188), then the
           Host-allowlist/DNS-rebinding guard (both modes), then (network mode, or a loopback-only server that keeps a configured token,
           see requireTokenWhenLoopbackOnly) the auth middleware, then a marker that the request has passed the gate, then the
           no-store stamp on /api/* responses, then DarlingWebEndpoints.MapAll -> UseDefaultFiles -> UseStaticFiles.
           WebApplication auto-inserts UseRouting at the head and UseEndpoints at the tail, so the static-file middleware sits
           behind these gates and serves the SPA for non-API paths.

           The observer is outermost so a throw from compression, the Host guard or the auth gate is logged too. It decides
           nothing about a request before next() runs (no refusal, no write, no read of the body), so it adds no unauthenticated
           surface ahead of the Host guard, which stays the first app.Use (HostHeaderGuardTests, #1648: that guard is the fix for
           the #1576 rebinding gap, and a handler that DECIDED anything ahead of it would sit ahead of that guard on the loopback
           bind). Compression only transforms an OUTGOING body (Content-Encoding), never a routing or auth decision, so it costs
           nothing to wrap the gates' own refusal bodies and the login page in it. A throw before the marker below comes from a
           request nobody has authenticated, so the observer's line for it goes through the refusals throttle (round 2, L1). */
        app.UseMiddleware<DarlingWebFailureObserver>(_logger, refusals);

        /* The browser-facing response headers (Content-Security-Policy, X-Frame-Options, X-Content-Type-Options,
           Referrer-Policy) ride on EVERY response: the shell, static assets, API replies and every gate's refusal
           or sign-in page. It registers its stamp with OnStarting and decides nothing about the request, so
           it adds no surface ahead of the Host guard; a class middleware, so the Host guard stays the first
           app.Use lambda. */
        app.UseMiddleware<DarlingWebSecurityHeaders>(sendHsts);

        app.UseResponseCompression();

        /* #5288, #4220: HttpRequest.Host hands the guard below the DECODED form of a Host header
           (HostString.FromUriComponent turns a punycode xn-- name into Unicode), and a browser always sends
           punycode. So a web.publicBaseUrl written in punycode (https://xn--bcher-kva.example/) was refused for
           its own Host: its raw name could never equal the decoded one. The name goes through the same
           conversion once, here, so both sides are in the form the framework gives the middleware; a Unicode
           name and an ASCII one come out unchanged. Only the guard's copy changes: the link builder and the TLS
           name check keep the raw host. A malformed punycode label (xn--a) makes the conversion throw
           ArgumentException; the raw value is kept then, so start-up never fails on it. */
        var admittedHostName = publicBaseUrlHost;
        if (publicBaseUrlHost is not null)
        {
            try
            {
                admittedHostName = HostString.FromUriComponent(publicBaseUrlHost).Host;
            }
            catch (ArgumentException)
            {
                /* Keep the raw value, as above. */
            }
        }

        /* DNS-rebinding guard — runs in BOTH modes (the #1576 fix: it previously guarded network mode only,
           leaving the loopback surface reachable cross-origin via a DNS rebind). The loopback surface has no token
           only under web.allowTokenlessLoopback (which serves reads only), and a page in a browser ON the host could
           otherwise be rebound to 127.0.0.1:5153 and read the whole surface same-origin. Require the Host header to name an
           address we actually bind — a loopback name/IP (localhost / 127.0.0.1 / [::1]) or, in network mode,
           the configured listen IP. networkListenIp is null in loopback mode, so ONLY loopback Hosts pass
           there; a rebound foreign hostname pointed at 127.0.0.1:5153 is rejected 400 before any auth
           decision, route handler, or static file. publicBaseUrlHost (#4220) is also admitted, in BOTH
           modes, because it is operator config on this box, not something a request chooses — see
           DarlingHostBinding.IsAllowedHost. */
        app.Use(async (context, next) =>
        {
            if (!IsAllowedHost(context.Request.Host.Host, networkListenIp, admittedHostName))
            {
                refusals.Report(
                    _logger, "Web dashboard", DarlingRefusalGate.HostAllowlist, StatusCodes.Status400BadRequest,
                    context.Connection.RemoteIpAddress,
                    $"the Host header '{DarlingHttpRefusalLog.Sanitize(context.Request.Host.Host)}' is not an address this endpoint binds"
                    + " (a loopback name/IP, web.network.listen when LAN-exposed, or web.publicBaseUrl's host)",
                    DateTime.UtcNow);
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            await next(context);
        });

        /* The auth gate installs in network mode and, on a loopback-only server that has a configured token
           (requireTokenWhenLoopbackOnly, #5288), too: the token-to-cookie gate is the
           one credential check this host has, and the CIDR test inside it exempts loopback, so on a server that
           only binds loopback it is the token that gates. A loopback-only server with no token configured gets here
           only under web.allowTokenlessLoopback, and registers the reads-only gate in the else branch instead. */
        if (networkMode || requireTokenWhenLoopbackOnly)
        {
            var cidr = allowedCidr;
            var token = accessToken;
            /* Per-process signing key: a restart regenerates it and thereby invalidates every session
               cookie (acceptable — the operator re-presents the token once). The OIDC transaction key is
               DERIVED from it rather than shared — see DeriveTransactionKey for why sharing the raw key
               across the two cookie shapes would let a pre-auth transaction cookie impersonate a session. */
            /* sessionSigningKeyForTests (#5240): a live-pipeline test mints a signed viewer or admin cookie with a key it
               chose; production never passes one, so the key stays a per-process random value. */
            var signingKey = sessionSigningKeyForTests ?? RandomNumberGenerator.GetBytes(SigningKeyBytes);
            var transactionKey = DarlingWebOidc.DeriveTransactionKey(signingKey);

            /* Per-source sign-in throttle, one per started server like the refusal log: a source that keeps
               failing the token waits longer each time (DarlingSignInThrottle). */
            var signInThrottle = new DarlingSignInThrottle();

            app.Use(async (context, next) =>
            {
                /* The Host-allowlist / DNS-rebinding guard already ran above (both modes); this gate owns
                   the network auth decision (session cookie / in-CIDR / the sign-in flow). */
                var remote = context.Connection.RemoteIpAddress;
                var hasValidCookie = TryValidateSessionCookie(
                    context.Request.Cookies[SessionCookieName], signingKey, DateTimeOffset.UtcNow, out var cookieSubject);

                /* Resolve WHO holds the cookie (#2550). A cryptographically valid cookie whose subject
                   slot encodes a seat we do not recognize is treated as NO cookie: refusing is
                   recoverable (re-login), serving an unresolvable identity is not. */
                var seat = DarlingWebSeat.SharedToken;
                if (hasValidCookie && !DarlingWebSeat.TryResolveSeat(cookieSubject, out seat))
                {
                    hasValidCookie = false;
                    seat = DarlingWebSeat.SharedToken;
                }

                /* The token is accepted from the sign-in form's POST body only (HandleTokenSignInAsync, on
                   LoginPath). One riding the URL is never compared, whatever its value: a token in the URL lands in
                   browser history, bookmarks, proxy logs and the Referer of every asset the page loads, so
                   exchanging it would encourage a habit that stores the token in those places. Its presence is only noted for the
                   refusal line below, so an operator with an old bookmark learns why it stopped working. */
                var tokenInUrl = context.Request.Query.ContainsKey("token");
                var isAuthFlowRoute = IsAuthFlowPath(context.Request.Path.Value ?? "/", oidcClient is not null);

                var connectionIp = context.Items.TryGetValue(ConnectionIpItemKey, out var stashedIp) ? stashedIp as IPAddress : null;
                switch (DecideWebRequest(remote, cidr, isAuthFlowRoute, hasValidCookie, connectionIp))
                {
                    case WebRequestAction.Allow:
                        /* A token under the length floor still signs a browser in, but the shared-token seat it grants
                           serves reads only: the same rule that keeps the MCP write tools off with such a token. */
                        var shortTokenSeat = sharedTokenReadsOnlyProblem is not null && seat.Subject is null && seat.CanEdit;
                        if (shortTokenSeat)
                        {
                            seat = DarlingWebSeat.ReadsOnly;
                        }

                        /* #6840: a front door forwarding for a viewer lowers its shared-token session to the reads-only seat for
                           this one request, so every handler below answers the viewer as it answers any reads-only caller. */
                        seat = DarlingWebSeat.ApplyFrontDoorRole(context, seat);

                        /* The seat rides HttpContext.Items so EVERY downstream consumer — /api/session,
                           the updated_by stamps, endpoints added on any parallel branch — resolves the
                           same identity without per-endpoint wiring. */
                        context.Items[DarlingWebSeat.HttpContextItemKey] = seat;

                        /* The group-level write gate (#2550): a read-only seat is refused every
                           mutating request HERE, before routing, so a write endpoint added tomorrow is
                           born gated instead of born exposed. */
                        if (shortTokenSeat && !DarlingWebSeat.IsRequestAllowed(seat, context.Request.Method, context.Request.Path.Value ?? "/"))
                        {
                            refusals.Report(
                                _logger, "Web dashboard", DarlingRefusalGate.ReadsOnlyDashboard, StatusCodes.Status403Forbidden,
                                remote,
                                $"the token is under the length floor, so the dashboard serves reads only and {context.Request.Method} is a change",
                                DateTime.UtcNow);
                            context.Response.StatusCode = StatusCodes.Status403Forbidden;
                            await WriteReadsOnlyRefusalAsync(context, ShortTokenWriteRefusedMessage);
                            return;
                        }

                        if (!DarlingWebSeat.IsRequestAllowed(seat, context.Request.Method, context.Request.Path.Value ?? "/"))
                        {
                            refusals.Report(
                                _logger, "Web dashboard", DarlingRefusalGate.ReadOnlySeat, StatusCodes.Status403Forbidden,
                                remote,
                                $"the signed-in seat is read-only (viewer role) and {context.Request.Method} is a mutation",
                                DateTime.UtcNow);
                            context.Response.StatusCode = StatusCodes.Status403Forbidden;
                            context.Response.ContentType = "application/json; charset=utf-8";
                            await context.Response.WriteAsync("{\"error\": \"This account has read-only access.\"}");
                            return;
                        }

                        await next(context);
                        return;

                    case WebRequestAction.HandleAuthFlow:
                        /* The flow endpoints are reachable WITHOUT a credential by design (a sign-in has
                           to start somewhere), and this pipeline registers no exception-handling
                           middleware — so anything that throws in here would answer an anonymous caller
                           with an unhandled 500 and a stack-shaped failure instead of a refusal. The
                           #2744 review found one such throw for real (an out-of-range exp reaching
                           DateTimeOffset); that root cause is fixed in DarlingWebOidc, and this boundary
                           exists so the NEXT one is also a clean refusal rather than a 500. A client
                           abort still propagates — that is not a failure to report. */
                        try
                        {
                            await HandleAuthFlowAsync(context, oidcClient, signingKey, transactionKey, seat, refusals, token, signInThrottle, admittedHostName, clock, mcpToken);
                        }
                        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            ReportSignInRefusal(refusals, remote, StatusCodes.Status500InternalServerError,
                                $"the sign-in flow failed unexpectedly: {DarlingHttpRefusalLog.Sanitize(ex.Message, 128)}");
                            if (!context.Response.HasStarted)
                            {
                                await WriteSignInErrorAsync(context, StatusCodes.Status500InternalServerError,
                                    "The sign-in could not be completed. The service log names what failed.");
                            }
                        }

                        return;

                    case WebRequestAction.Forbid:
                        /* An out-of-CIDR remote, or one whose address ASP.NET Core could not report
                           (which fails closed). A wrong credential from inside the CIDR is ShowLogin,
                           not this - see below. The CIDR stays OUTERMOST: it wins over a valid cookie,
                           a valid token, AND the OIDC endpoints (#2550). */
                        refusals.Report(
                            _logger, "Web dashboard", DarlingRefusalGate.SourceCidr, StatusCodes.Status403Forbidden,
                            remote,
                            remote is null
                                ? "its source address could not be determined, which fails closed"
                                : $"its address is outside web.network.allowFrom ({cidr})",
                            DateTime.UtcNow);
                        context.Response.StatusCode = StatusCodes.Status403Forbidden;
                        return;

                    default: /* ShowLogin */
                        /* A page route gets 200 rather than 401, so it is not a "rejected request" by status -
                           and it is exactly the state an operator asks about when a token they pasted did not
                           work. An /api/* call gets 401 instead (#4187): the SAME unauthenticated state, but a
                           fetch caller branches on status rather than parsing a body, and the SPA must never
                           mistake the login form for empty data. This arm is reached with no valid cookie AND
                           no valid token regardless of which of the three modes (shared token, cookie session,
                           OIDC) the caller was trying, so the split below covers all three by construction.
                           Logged ONLY when a token was actually presented and did not match: a first visit (or
                           an open tab polling with no cookie after a restart - #4187's own trigger) is the
                           normal path here and logging THAT would make every bookmark, and every stale tab,
                           a warning. */
                        var isApiCall = IsApiPath(context.Request.Path.Value ?? "/");
                        var shownStatus = isApiCall ? StatusCodes.Status401Unauthorized : StatusCodes.Status200OK;

                        if (tokenInUrl)
                        {
                            refusals.Report(
                                _logger, "Web dashboard", DarlingRefusalGate.Token, shownStatus,
                                remote,
                                isApiCall
                                    ? "a token was presented in the URL (?token=), which is not accepted; scripted callers sign in with a form POST to " + LoginPath + " and then present the session cookie"
                                    : "a token was presented in the URL (?token=), which is not accepted; the login page was served instead, and its form posts the token",
                                DateTime.UtcNow);
                        }

                        if (isApiCall)
                        {
                            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                            context.Response.ContentType = "application/json; charset=utf-8";
                            await context.Response.WriteAsync("{\"error\":\"session expired\",\"login\":\"/\"}");
                            return;
                        }

                        /* #6286: a page link followed from another site (Teams, mail, a work item) is a cross-site top-level
                           navigation, so the browser withholds the SameSite=Strict session cookie and a live session looks
                           signed out. Answer it once with a page that re-navigates to the same address from this site; that
                           request is same-origin and carries the cookie. With no session still, it gets the sign-in page below. */
                        if (IsCrossSitePageNavigation(context.Request))
                        {
                            await WriteReNavigationPageAsync(context);
                            return;
                        }

                        await WriteLoginPageAsync(context, oidcClient is not null);
                        return;
                }
            });
        }
        else
        {
            /* No gate: the only start that gets here is web.allowTokenlessLoopback. It serves reads only. The seat is
               stored, so /api/session reports can_edit false and every consumer reads the same seat, and a write is
               refused here, before routing, so a write route added later is refused too. POST /api/compose/run
               stays served: it only reads (DarlingWebSeat.IsRequestAllowed). */
            app.Use(async (context, next) =>
            {
                context.Items[DarlingWebSeat.HttpContextItemKey] = DarlingWebSeat.ReadsOnly;
                if (!DarlingWebSeat.IsRequestAllowed(DarlingWebSeat.ReadsOnly, context.Request.Method, context.Request.Path.Value ?? "/"))
                {
                    refusals.Report(
                        _logger, "Web dashboard", DarlingRefusalGate.ReadsOnlyDashboard, StatusCodes.Status403Forbidden,
                        context.Connection.RemoteIpAddress,
                        $"web.allowTokenlessLoopback serves reads only and {context.Request.Method} is a change",
                        DateTime.UtcNow);
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    await WriteReadsOnlyRefusalAsync(context, TokenlessWriteRefusedMessage);
                    return;
                }

                await next(context);
            });
        }

        /* Round 2, L1: past this point a request has passed the auth gate (or, on a server with no gate, the Host guard), so a throw
           from here on is a failure of a caller the service let in and the observer logs it unthrottled, as the #4276 backstop did. */
        app.Use(async (context, next) =>
        {
            DarlingWebFailureLog.NotePassedAuth();
            await next(context);
        });

        /* #4276, W12: the failure backstop is no longer here. It is DarlingWebFailureObserver, registered FIRST (before response
           compression), so a throw from the Host guard, the auth gate or compression is logged too, and still once per request. */

        /* API responses never cache (#4188): the store mutates continuously, so a stale GET is a stale
           dashboard. no-store rather than no-cache/must-revalidate — these bodies carry no ETag, so "cache but
           always revalidate" would have nothing to revalidate against and a client that honored only the
           weaker no-cache could still serve a stale body from disk on its next launch. Set ahead of MapAll so
           it covers every route MapAll adds (including ones added on a parallel branch) without being
           duplicated at each one; a handler is free to override it, none does. */
        app.Use(async (context, next) =>
        {
            if (context.Request.Path.StartsWithSegments("/api"))
            {
                context.Response.Headers.CacheControl = "no-store";
            }

            await next(context);
        });

        /* #5506 part 3: a successful web write (a 2xx answer to a non-GET /api/* request, except the compose run, the rule
           validator and the rule test) moves the read cache's write generation, so no cached answer from before it is
           served again. Round 2, L2: moved as the 2xx starts, before its first byte leaves, as the MCP surface moves it
           before answering, so a browser that refreshes the moment the write answers never reads the entry from before
           it. A response always starts (an empty one at the end of the pipeline), so the callback always runs. */
        if (_readAdmission is { } readAdmission)
        {
            app.Use(async (context, next) =>
            {
                context.Response.OnStarting(() =>
                {
                    if (ReadResultCache.IsWebWrite(context.Request.Method, context.Request.Path.Value ?? string.Empty, context.Response.StatusCode))
                    {
                        readAdmission.Cache.BumpGeneration();
                    }

                    return Task.CompletedTask;
                });
                await next(context);
            });
        }

        if (frontDoorProxy is not null)
        {
            /* #6840: a front door answers the four local routes and forwards the rest of /api/* to a store. It sits exactly where MapAll
               does, behind the gates above, so a request reaches it only after sign-in, the CIDR test and the role gate. */
            frontDoorProxy.AddRoutes(app);
        }
        else
        {
            DarlingWebEndpoints.MapAll(app, postgres ?? throw new ArgumentNullException(nameof(postgres)), _collectorState, _logger, _baselineCache, postgresConfig, _readLatency, analyzerConfig, _registryState, _slowReads, _readAdmission, _ownerSource, dataDetail, _pseudonymKeys, adminArm: adminArm, allowWindowsAuthOverUnverifiedTls: allowWindowsAuthOverUnverifiedTls);
        }

        app.UseDefaultFiles();

        /* Static assets carry an ETag/Last-Modified already (the framework default); no-cache (#4188) makes
           revalidation MANDATORY instead of left to the browser's heuristic guess, which is the minimum fix
           for "never serve a stale app after an upgrade" — the shell does not version its <script>/<link> URLs
           (no ?v= / content hash), so a longer max-age would risk a browser skipping the revalidation entirely
           and running old JS against a new API. Revalidation itself stays cheap (a 304, no body). */
        app.UseStaticFiles(new StaticFileOptions
        {
            OnPrepareResponse = ctx => ctx.Context.Response.Headers.CacheControl = "no-cache",
        });
    }

    /// <summary>The verdict for one network-mode request, with the sign-in flow added (#2550). The first four
    /// members are <see cref="WebAuthAction"/>'s, mapped one-to-one.</summary>
    internal enum WebRequestAction { Allow, Forbid, ShowLogin, HandleAuthFlow }

    internal const string OidcLoginPath = "/auth/oidc/login";
    internal const string OidcCallbackPath = "/auth/oidc/callback";
    internal const string LogoutPath = "/auth/logout";

    /// <summary>The shared-token sign-in: the login form POSTs the token here (<c>token</c> and <c>return</c>
    /// fields, form-encoded). A GET renders the form. The token is accepted from this POST body and nowhere
    /// else — see the auth gate's note on a token in the URL.</summary>
    internal const string LoginPath = "/auth/login";

    /// <summary>The <see cref="HttpContext.Items"/> key holding the address of the connection itself, stored
    /// before the forwarded-headers middleware rewrites <c>RemoteIpAddress</c> to the forwarded client.</summary>
    internal const string ConnectionIpItemKey = "darling.connectionIp";

    /// <summary>PURE: is <paramref name="remoteIp"/> inside <c>allowFrom</c>? Unlike
    /// <see cref="DarlingHostBinding.IsRemoteAddressAllowed"/> it has no loopback exemption of its own: the
    /// exemption is decided once, on the connection's own address (<see cref="IsLoopbackConnection"/>), so a
    /// forwarded loopback address is tested against the list like any other.</summary>
    internal static bool IsInAllowFrom(IPAddress? remoteIp, CidrAllowList allowedCidr)
    {
        if (remoteIp is null)
        {
            return false;
        }

        return allowedCidr.Contains(remoteIp.IsIPv4MappedToIPv6 ? remoteIp.MapToIPv4() : remoteIp);
    }

    /// <summary>
    /// PURE: does this request count as loopback for the <c>allowFrom</c> exemption? Only when the
    /// connection ITSELF is loopback: a forwarded <c>127.0.0.1</c> / <c>::1</c> (a header a proxy relayed from
    /// its client) never is, so it is judged against <c>allowFrom</c> like any other address. When a trusted
    /// proxy on this host forwards a client, the request is loopback only if that client is too, which keeps
    /// <c>allowFrom</c> in force for clients that reach a local proxy from elsewhere. With no forwarded
    /// rewrite, <paramref name="connectionIp"/> is null and the remote address decides, as before.
    /// </summary>
    internal static bool IsLoopbackConnection(IPAddress? remoteIp, IPAddress? connectionIp)
        => DarlingWebEndpoints.IsLoopbackRemote(remoteIp)
           && (connectionIp is null || DarlingWebEndpoints.IsLoopbackRemote(connectionIp));

    /// <summary>The most a sign-in POST body may declare. The form carries a token and a path; anything
    /// larger is not the form, and this leg is reachable before any credential.</summary>
    internal const int MaxLoginFormBytes = 4096;

    /// <summary>
    /// PURE: is this path one the auth-flow handler owns? The two OIDC legs exist only while OIDC is
    /// enabled (disabled, they fall through to normal auth and render the login page — no route squatting);
    /// logout is ALWAYS a flow path, because the shared-token seat holds a session cookie too and deserves a
    /// way to drop it, and so is the token sign-in (<see cref="LoginPath"/>), which exists in every mode.
    /// </summary>
    internal static bool IsAuthFlowPath(string path, bool oidcEnabled)
    {
        if (string.Equals(path, LogoutPath, StringComparison.OrdinalIgnoreCase)
            || string.Equals(path, LoginPath, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return oidcEnabled
            && (string.Equals(path, OidcLoginPath, StringComparison.OrdinalIgnoreCase)
                || string.Equals(path, OidcCallbackPath, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// PURE: is this an <c>/api/*</c> call rather than a page navigation? #4187 — the unauthenticated arm of
    /// the auth gate (below) used to answer BOTH the same way: a 200 <c>text/html</c> login form. That is the
    /// right answer for a browser loading a page, but the SPA's own fetch layer parsed the form as a failed
    /// JSON.parse, got <c>body: null</c>, and rendered an expired/rotated session as an empty "nothing here"
    /// card rather than "sign in again" (<c>classifyResponse</c>'s own comment says a failure must never do
    /// that). Every route this host serves under <c>/api/</c> always answers JSON, so the split is a path
    /// prefix, not a content-negotiation guess — a hand-curled request with no <c>Accept</c> header gets the
    /// same answer as the SPA's.
    /// </summary>
    internal static bool IsApiPath(string path)
        => path.Equals("/api", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("/api/", StringComparison.OrdinalIgnoreCase);

    /// <summary>Writes the body of a write a reads-only dashboard refuses (the caller sets the 403): <paramref name="message"/> as the JSON error.</summary>
    private static async Task WriteReadsOnlyRefusalAsync(HttpContext context, string message)
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        await context.Response.WriteAsync("{\"error\": " + System.Text.Json.JsonSerializer.Serialize(message) + "}");
    }

    /// <summary>
    /// PURE per-request decision, composing the sign-in flow around <see cref="DecideWebAuth"/>: the CIDR
    /// stays OUTERMOST (an out-of-CIDR caller is 403 even on the OIDC endpoints — sign-in is not a way past
    /// the network boundary), then a flow path is handled regardless of credential state (a signed-in user
    /// re-visiting /auth/oidc/login is starting a new sign-in, not asking for the dashboard), then the
    /// original matrix decides exactly as before.
    /// </summary>
    internal static WebRequestAction DecideWebRequest(
        IPAddress? remoteIp, CidrAllowList allowedCidr, bool isAuthFlowRoute, bool hasValidCookie,
        IPAddress? connectionIp = null)
    {
        var isLoopback = IsLoopbackConnection(remoteIp, connectionIp);
        if (!isLoopback && !IsInAllowFrom(remoteIp, allowedCidr))
        {
            return WebRequestAction.Forbid;
        }

        if (isAuthFlowRoute)
        {
            return WebRequestAction.HandleAuthFlow;
        }

        return DecideWebAuth(remoteIp, allowedCidr, hasValidCookie, connectionIp) switch
        {
            WebAuthAction.Allow => WebRequestAction.Allow,
            WebAuthAction.Forbid => WebRequestAction.Forbid,
            _ => WebRequestAction.ShowLogin,
        };
    }

    /// <summary>
    /// The longest <paramref name="subject"/> <see cref="BuildSessionCookieValue"/> will mint into a cookie.
    ///
    /// <para>Generous for an OIDC <c>sub</c> — the identifiers real providers issue are GUIDs, opaque
    /// 40-character strings, or an email — and a hard REFUSAL rather than a truncation, deliberately.
    /// Truncating an identity is the one failure mode that would be worse than not having one: two subjects
    /// sharing a prefix would collapse to the same seat, so the surface would report the wrong person as the
    /// author of a change and every downstream authorization decision would be made about somebody else.
    /// Failing the sign-in loudly is recoverable; silently merging two identities is not.</para>
    /// </summary>
    internal const int MaxSessionSubjectLength = 256;

    /// <summary>
    /// Builds a session cookie value. PURE.
    ///
    /// <para>Two shapes, and the difference is whether the seat has a NAME:
    /// <c>{expiryUnix}.{base64url(HMAC)}</c> for the shared-token seat, which has no identity to carry, and
    /// <c>{expiryUnix}.{base64url(subject)}.{base64url(HMAC)}</c> once a per-user sign-in has established
    /// who is holding it (#2550).</para>
    ///
    /// <para><b>The signature covers the whole prefix before the FINAL dot</b>, which is what makes the two
    /// shapes one construction rather than two. Signing only the expiry and appending the subject beside it
    /// would leave the subject unauthenticated — anyone holding a valid cookie could rewrite it to any other
    /// subject and be served as that person, which is a worse position than the shared token this exists to
    /// improve on, because it would look like identity while providing none. Extending the signed region
    /// instead means expiry and subject are tamper-evident together.</para>
    ///
    /// <para>The two shapes cannot be confused for each other even though they share a separator: the
    /// signature is always the last segment and the signed payload is always everything before it, so
    /// re-presenting a 3-segment cookie as a 2-segment one requires the subject to be a valid HMAC over the
    /// expiry, and the reverse requires forging an HMAC. Neither is available without the key.</para>
    ///
    /// <para>The subjectless form is byte-for-byte what this method produced before per-user identity existed,
    /// so cookies already in browsers keep validating across the upgrade instead of signing everyone out.</para>
    /// </summary>
    /// <param name="subject">The authenticated principal, or null/empty for the shared-token seat.</param>
    /// <exception cref="ArgumentException"><paramref name="subject"/> exceeds
    /// <see cref="MaxSessionSubjectLength"/> — see the note there on why this refuses rather than truncates.
    /// </exception>
    internal static string BuildSessionCookieValue(byte[] signingKey, DateTimeOffset expiry, string? subject = null)
    {
        if (subject is { Length: > MaxSessionSubjectLength })
        {
            throw new ArgumentException(
                $"Session subject is {subject.Length} characters, which exceeds the {MaxSessionSubjectLength}-character "
                + "limit. It is refused rather than truncated because two subjects sharing a prefix would become the "
                + "same seat.",
                nameof(subject));
        }

        var expiryUnix = expiry.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
        var payload = string.IsNullOrEmpty(subject)
            ? expiryUnix
            : $"{expiryUnix}.{Base64UrlEncode(Encoding.UTF8.GetBytes(subject))}";

        /* ASCII, as before: the payload is an integer and base64url by construction, so every byte is in
           range, and keeping the encoding means the subjectless cookie is identical to the one this minted
           before the subject existed. */
        var signature = HMACSHA256.HashData(signingKey, Encoding.ASCII.GetBytes(payload));
        return $"{payload}.{Base64UrlEncode(signature)}";
    }

    /// <summary>
    /// PURE verify of a session cookie against <paramref name="signingKey"/> and the current time: false on a
    /// malformed value, an unparsable/expired expiry, a bad base64url signature, or an HMAC mismatch (tamper).
    /// The signature compare is constant-time.
    /// </summary>
    internal static bool TryValidateSessionCookie(string? cookieValue, byte[] signingKey, DateTimeOffset now)
        => TryValidateSessionCookie(cookieValue, signingKey, now, out _);

    /// <summary>
    /// PURE verify, additionally reporting WHO the cookie says is holding it (#2550).
    ///
    /// <para><paramref name="subject"/> is null for the shared-token seat, which genuinely has no identity —
    /// distinct from an empty string, which would read as an authenticated principal with a blank name. Every
    /// caller that stamps provenance has to keep those apart, because "the shared token did this" and "a
    /// signed-in person we failed to name did this" are different facts.</para>
    ///
    /// <para>The subject is decoded only AFTER the HMAC verifies, so nothing derived from an unauthenticated
    /// cookie ever reaches a caller.</para>
    /// </summary>
    internal static bool TryValidateSessionCookie(string? cookieValue, byte[] signingKey, DateTimeOffset now, out string? subject)
    {
        subject = null;

        if (string.IsNullOrEmpty(cookieValue))
        {
            return false;
        }

        /* LAST dot, not the first: the signature is the final segment and the signed payload is everything
           before it, which is the rule that lets the subjectless and subject-bearing shapes share one parse.
           For a subjectless cookie the last dot IS the first, so this is the original behavior unchanged. */
        var lastDot = cookieValue.LastIndexOf('.');
        if (lastDot <= 0 || lastDot >= cookieValue.Length - 1)
        {
            return false;
        }

        var payload = cookieValue.Substring(0, lastDot);
        var signaturePart = cookieValue.Substring(lastDot + 1);

        var firstDot = payload.IndexOf('.');
        var expiryPart = firstDot < 0 ? payload : payload.Substring(0, firstDot);
        var subjectPart = firstDot < 0 ? null : payload.Substring(firstDot + 1);

        /* Exactly two or three segments. A fourth would leave a dot inside the subject segment, and accepting
           it would mean two different cookies could parse to the same subject. */
        if (subjectPart is not null && (subjectPart.Length == 0 || subjectPart.Contains('.')))
        {
            return false;
        }

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

        var expected = HMACSHA256.HashData(signingKey, Encoding.ASCII.GetBytes(payload));
        if (!CryptographicOperations.FixedTimeEquals(presented, expected))
        {
            return false;
        }

        if (subjectPart is not null)
        {
            try
            {
                subject = Encoding.UTF8.GetString(Base64UrlDecode(subjectPart));
            }
            catch (FormatException)
            {
                /* Signed by us and still undecodable means we minted it wrong, not that a caller tampered.
                   Refusing is still right: serving a session whose identity we cannot read would put an
                   unnamed principal behind the write paths this exists to attribute. */
                return false;
            }
        }

        return true;
    }

    private static void AppendSessionCookie(HttpContext context, byte[] signingKey, string? subject = null)
    {
        var value = BuildSessionCookieValue(signingKey, DateTimeOffset.UtcNow.Add(SessionLifetime), subject);
        context.Response.Cookies.Append(SessionCookieName, value, new CookieOptions
        {
            HttpOnly = true,
            SameSite = SameSiteMode.Strict,
            /* PER-REQUEST, not a fixed value, because since #2562 this ONE host can serve both schemes at once:
               the network listener is TLS when web.network.tls is configured while the loopback listeners
               deliberately stay plain HTTP. A hardcoded true would mint a cookie the loopback browser then
               refuses to send back (the login would loop forever); a hardcoded false would let a cookie issued
               over TLS be replayed on any http:// downgrade. IsHttps is the connection's own answer, so each
               cookie is marked for the transport it was actually issued on. */
            Secure = context.Request.IsHttps,
            Path = "/",
            MaxAge = SessionLifetime,
        });
    }

    /// <summary>
    /// PURE open-redirect guard for the token-strip 302 target and the OIDC <c>?return=</c> landing (#2550):
    /// forces a single-slash site-relative path. A request path beginning <c>//</c> (or <c>/\</c>) is a
    /// protocol-relative URL the browser follows off-site, so a path such as <c>//other.example/</c> would
    /// 302 the operator away; any leading slash/backslash run is collapsed to one <c>/</c>. Empty maps to
    /// <c>/</c>.
    ///
    /// <para><b>ASCII control characters are stripped from the WHOLE string first</b> (review catch on
    /// #2730): the WHATWG URL parser removes tab/CR/LF from anywhere in a URL before parsing, and that
    /// stripping applies to <c>location.replace()</c>, <c>&lt;a href&gt;</c>, and <c>Location</c> headers
    /// alike — so <c>\t//evil.com</c> would sail past a leading-slash-only guard and become protocol-relative
    /// IN THE BROWSER. Stripping before collapsing means the guard sees the same string the browser will.
    /// All C0 controls go, not just the three the URL spec names: none has any business in a path, and
    /// enumerating exactly the browser's list is how the next variant gets through.</para>
    /// </summary>
    internal static string SanitizeRedirectPath(string location)
    {
        if (string.IsNullOrEmpty(location))
        {
            return "/";
        }

        Span<char> cleaned = location.Length <= 512 ? stackalloc char[location.Length] : new char[location.Length];
        var length = 0;
        foreach (var c in location)
        {
            if (c >= ' ' && c != '\x7f')
            {
                cleaned[length++] = c;
            }
        }

        var i = 0;
        while (i < length && (cleaned[i] == '/' || cleaned[i] == '\\'))
        {
            i++;
        }

        return "/" + new string(cleaned.Slice(i, length - i));
    }

    /* ---------------------------------------------------------------------------------------------------
       The sign-in flow handler (#2550): /auth/logout always; /auth/oidc/login and /auth/oidc/callback when
       OIDC is enabled. Reached only AFTER the CIDR gate (DecideWebRequest puts Forbid first), so everything
       here is talking to a caller the network boundary already admitted.
       --------------------------------------------------------------------------------------------------- */

    private async Task HandleAuthFlowAsync(
        HttpContext context,
        DarlingWebOidcClient? oidcClient,
        byte[] signingKey,
        byte[] transactionKey,
        DarlingWebSeat currentSeat,
        DarlingHttpRefusalLog refusals,
        string accessToken,
        DarlingSignInThrottle signInThrottle,
        string? publicHostName,
        Func<DateTime> clock,
        string? mcpToken)
    {
        var path = context.Request.Path.Value ?? "/";
        var remote = context.Connection.RemoteIpAddress;

        if (string.Equals(path, LoginPath, StringComparison.OrdinalIgnoreCase))
        {
            await HandleTokenSignInAsync(context, accessToken, signingKey, signInThrottle, refusals, oidcClient is not null, publicHostName, clock, mcpToken);
            return;
        }

        if (string.Equals(path, LogoutPath, StringComparison.OrdinalIgnoreCase))
        {
            /* Local sign-out only — the cookie dies, the IdP session (if any) lives on. RP-initiated logout
               at the IdP would sign the user out of everything ELSE their org runs, which is not this
               button's mandate. A GET with no CSRF token is deliberate too: the worst a forged logout does
               is show someone the login page. */
            if (currentSeat.Subject is not null)
            {
                _logger.LogInformation("Web dashboard sign-out: {Subject}",
                    DarlingHttpRefusalLog.Sanitize(currentSeat.Subject, DarlingWebSeat.MaxSubjectLength));
            }

            context.Response.Cookies.Delete(SessionCookieName, new CookieOptions { Path = "/" });
            context.Response.Redirect("/");
            return;
        }

        if (oidcClient is null)
        {
            /* Unreachable through IsAuthFlowPath (the OIDC legs only qualify while a client exists), kept
               as a backstop rather than a NullReference away from a 500. The answer is the login page —
               exactly what the non-flow pipeline serves an unauthenticated caller — not a bare 4xx, which
               the #2479 report-before-refusing pin rightly refuses to allow silently. */
            await WriteLoginPageAsync(context, oidcEnabled: false);
            return;
        }

        if (string.Equals(path, OidcLoginPath, StringComparison.OrdinalIgnoreCase))
        {
            /* A repeated ?return= is refused (#5245), before the discovery fetch: the joined "a,b" is not a path
               anyone asked to land on. */
            if (!DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "return", out var requestedReturn))
            {
                await RefuseRepeatedSignInKeyAsync(context, refusals, remote);
                return;
            }

            var discovery = await oidcClient.GetDiscoveryAsync(context.RequestAborted);
            if (discovery is null)
            {
                /* Through the rate-limited refusal log, not a bare LogWarning: this leg is reachable
                   pre-credential by design, so during an IdP outage every hit would otherwise write a
                   line — the #2479 flood, reintroduced on a new path. */
                refusals.Report(
                    _logger, "Web dashboard", DarlingRefusalGate.SignIn, StatusCodes.Status502BadGateway, remote,
                    $"the OIDC discovery document could not be fetched/parsed from {oidcClient.Options.Authority} — the next sign-in attempt retries",
                    DateTime.UtcNow);
                await WriteSignInErrorAsync(context, StatusCodes.Status502BadGateway,
                    "Single sign-on is temporarily unavailable (the identity provider did not answer). Use the access token, or try again.");
                return;
            }

            var (verifier, challenge) = DarlingWebOidc.CreatePkce();
            var state = DarlingWebOidc.CreateFlowValue();
            var nonce = DarlingWebOidc.CreateFlowValue();

            /* Where to land after the sign-in — the SPA path the login page was covering, run through the
               same open-redirect guard as the token-strip 302. */
            var returnPath = SanitizeRedirectPath(requestedReturn);

            /* The redirect URI is derived from THIS request's scheme+Host (which the allowlist has already
               vetted), then FROZEN into the transaction: the token endpoint requires the exact value the
               authorization request used, and this host answers on several names at once. */
            var redirectUri = $"{context.Request.Scheme}://{context.Request.Host.Value}{OidcCallbackPath}";

            var startedTransaction = new DarlingWebOidc.OidcTransaction(state, nonce, verifier, redirectUri, returnPath);
            context.Response.Cookies.Append(
                DarlingWebOidc.TransactionCookieName,
                DarlingWebOidc.BuildTransactionCookie(
                    transactionKey, startedTransaction, DateTimeOffset.UtcNow.Add(DarlingWebOidc.TransactionLifetime)),
                new CookieOptions
                {
                    HttpOnly = true,
                    /* Lax, NOT Strict, and it is load-bearing: the callback arrives as a top-level
                       navigation INITIATED BY THE IDP's ORIGIN, and a Strict cookie is simply not sent on
                       cross-site navigations — the flow would fail on every sign-in with "expired or
                       tampered", only in real browsers, never in a test harness. Lax sends it exactly on
                       that top-level GET and still never on cross-site subresource/POST requests. */
                    SameSite = SameSiteMode.Lax,
                    Secure = context.Request.IsHttps,
                    Path = "/auth/oidc",
                    MaxAge = DarlingWebOidc.TransactionLifetime,
                });

            context.Response.Redirect(DarlingWebOidc.BuildAuthorizationUrl(
                discovery.AuthorizationEndpoint,
                oidcClient.Options.ClientId,
                redirectUri,
                oidcClient.Options.Scopes,
                state,
                nonce,
                challenge));
            return;
        }

        /* ------- the callback: the IdP sent the browser back with ?code=&state= (or ?error=). ------- */

        /* Each callback parameter is single-valued (RFC 6749 section 3.1; #5245): a key sent twice is refused
           as a malformed callback, and the transaction is spent like any other failed callback. */
        if (!DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "error", out var providerError)
            || !DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "error_description", out var providerErrorDescription)
            || !DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "code", out var code)
            || !DarlingWebEndpoints.TrySingleQueryValue(context.Request.Query, "state", out var presentedState))
        {
            context.Response.Cookies.Delete(DarlingWebOidc.TransactionCookieName, new CookieOptions { Path = "/auth/oidc" });
            await RefuseRepeatedSignInKeyAsync(context, refusals, remote);
            return;
        }

        if (!string.IsNullOrEmpty(providerError))
        {
            /* IdP-authored in the honest case, but it rode a browser querystring here — anyone can put an
               ?error= on this URL, which is exactly why it goes through the rate-limited refusal log,
               sanitized like any other echoed input. */
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.SignIn, StatusCodes.Status403Forbidden, remote,
                $"the provider (or the querystring) reported '{DarlingHttpRefusalLog.Sanitize(providerError)}"
                + $" {DarlingHttpRefusalLog.Sanitize(providerErrorDescription)}'",
                DateTime.UtcNow);
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "The identity provider refused the sign-in. Ask your administrator, or use the access token.");
            return;
        }

        var hasTransaction = DarlingWebOidc.TryValidateTransactionCookie(
            context.Request.Cookies[DarlingWebOidc.TransactionCookieName], transactionKey, DateTimeOffset.UtcNow,
            out var transaction);

        /* One-shot either way: success or failure, this transaction is spent. */
        context.Response.Cookies.Delete(DarlingWebOidc.TransactionCookieName, new CookieOptions { Path = "/auth/oidc" });

        if (string.IsNullOrEmpty(code) || !hasTransaction
            || !string.Equals(presentedState, transaction!.State, StringComparison.Ordinal))
        {
            ReportSignInRefusal(refusals, remote, StatusCodes.Status400BadRequest,
                "the callback carried no code, or its state did not match the sign-in this host started (expired/tampered transaction, or a cross-site forgery)");
            await WriteSignInErrorAsync(context, StatusCodes.Status400BadRequest,
                "This sign-in attempt is stale or was not started by this dashboard. Start again from the login page.");
            return;
        }

        var discoveryForExchange = await oidcClient.GetDiscoveryAsync(context.RequestAborted);
        if (discoveryForExchange is null)
        {
            ReportSignInRefusal(refusals, remote, StatusCodes.Status502BadGateway,
                $"discovery is no longer available from {oidcClient.Options.Authority}, so the code exchange could not run");
            await WriteSignInErrorAsync(context, StatusCodes.Status502BadGateway,
                "Single sign-on is temporarily unavailable (the identity provider did not answer). Use the access token, or try again.");
            return;
        }

        var exchange = await oidcClient.ExchangeCodeAsync(
            discoveryForExchange.TokenEndpoint, code, transaction.RedirectUri, transaction.CodeVerifier, context.RequestAborted);
        if (exchange.IdToken is null)
        {
            /* exchange.Error is IdP/transport-authored, never secret material (the client guarantees it),
               but it still gets the sanitizer before a log line. */
            ReportSignInRefusal(refusals, remote, StatusCodes.Status502BadGateway,
                $"the code exchange failed: {DarlingHttpRefusalLog.Sanitize(exchange.Error, 128)}");
            await WriteSignInErrorAsync(context, StatusCodes.Status502BadGateway,
                "The identity provider rejected the sign-in exchange. Try again; if it persists, the service log names the provider's error.");
            return;
        }

        var payload = DarlingWebOidc.TryParseJwtPayload(exchange.IdToken);
        var claimProblem = payload is null
            ? "the id_token is not a parsable JWT"
            : DarlingWebOidc.ValidateIdTokenClaims(
                payload, discoveryForExchange.Issuer, oidcClient.Options.ClientId, transaction.Nonce, DateTimeOffset.UtcNow);
        if (claimProblem is not null)
        {
            ReportSignInRefusal(refusals, remote, StatusCodes.Status403Forbidden, $"the ID token failed validation: {claimProblem}");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "The identity provider's answer did not validate. The service log has the specific check that failed.");
            return;
        }

        var subject = DarlingWebOidc.ResolveSubject(payload!, oidcClient.Options.SubjectClaim);
        if (subject is null)
        {
            ReportSignInRefusal(refusals, remote, StatusCodes.Status403Forbidden, oidcClient.Options.SubjectClaim is null
                ? "the ID token carries none of the subject claims (preferred_username/email/sub)"
                : $"the ID token does not carry the configured subjectClaim '{oidcClient.Options.SubjectClaim}' — refusing rather than falling back to a claim the operator did not choose");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Signed in at the provider, but the token carries no usable identity for this dashboard. The service log names the missing claim.");
            return;
        }

        if (subject.Length > DarlingWebSeat.MaxSubjectLength)
        {
            /* The cookie's own rule (#2583): refuse, never truncate — truncation merges identities. */
            ReportSignInRefusal(refusals, remote, StatusCodes.Status403Forbidden, $"the resolved subject is {subject.Length} characters (limit {DarlingWebSeat.MaxSubjectLength})");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Signed in at the provider, but the account identifier is too long for this dashboard's session. Configure a shorter subjectClaim.");
            return;
        }

        if (DarlingWebOidc.SubjectCarriesControlCharacters(subject))
        {
            /* Same shape as the length cap (review catch on #2730): a subject with CR/LF in it could forge
               lines in the audit trail these sign-ins feed, and no legitimate identifier carries controls —
               refuse the seat rather than launder the name and attribute writes to the laundered form. */
            ReportSignInRefusal(refusals, remote, StatusCodes.Status403Forbidden,
                "the resolved subject contains ASCII control characters, which no legitimate identifier does");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Signed in at the provider, but the account identifier is not usable by this dashboard. Configure a different subjectClaim.");
            return;
        }

        var role = DarlingWebOidc.MapRole(
            oidcClient.Options.RoleClaim is null
                ? Array.Empty<string>()
                : DarlingWebOidc.ExtractClaimValues(payload!, oidcClient.Options.RoleClaim),
            oidcClient.Options.AdminRoles,
            oidcClient.Options.ViewerRoles);
        if (role == WebOidcRole.Denied)
        {
            /* Authenticated at the IdP, unknown to the role mapping: refused, and the LOG carries who —
               this line is the audit trail for "someone signed in who shouldn't reach the dashboard". */
            _logger.LogWarning(
                "OIDC sign-in refused: {Subject} authenticated at the provider but matches neither adminRoles nor viewerRoles in claim '{RoleClaim}'.",
                DarlingHttpRefusalLog.Sanitize(subject, DarlingWebSeat.MaxSubjectLength), oidcClient.Options.RoleClaim);
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Your account is not granted a seat on this dashboard. Ask your administrator to add you to a mapped role.");
            return;
        }

        /* #6214: the owner key (issuer + sub, hashed) rides in the signed cookie, so a personal view follows the person's provider
           identity and not the display name, which can change and can be reissued. */
        AppendSessionCookie(context, signingKey, DarlingWebSeat.EncodeCookieSubject(subject, role, DarlingWebOidc.ResolveOwnerKey(payload!)));

        /* Per-user identity in the service log — the #2550 counterpart of the updated_by stamp. */
        _logger.LogInformation(
            "OIDC sign-in: {Subject} as {Role} from {Remote}",
            DarlingHttpRefusalLog.Sanitize(subject, DarlingWebSeat.MaxSubjectLength),
            role == WebOidcRole.Admin ? "admin (edit)" : "viewer (read-only)", remote);

        await WriteSignedInLandingAsync(context, transaction.ReturnPath);
    }

    /// <summary>
    /// The shared-token sign-in leg (<see cref="LoginPath"/>). A GET renders the form. A POST carries the
    /// token in its form body, which keeps it out of the URL, the browser history and every Referer the
    /// dashboard's assets would otherwise send. The POST must come from the dashboard's own page
    /// (<see cref="IsSameSiteSignIn"/>: Origin, or Referer when Origin is missing, names the host it serves),
    /// and that is checked before anything counts as an attempt. The body is read as bytes up to
    /// <see cref="MaxLoginFormBytes"/> whatever its framing (chunked bodies included), and only a
    /// url-encoded form is read at all. Per source, a failed attempt costs a growing wait before the
    /// next one is even compared (<see cref="DarlingSignInThrottle"/>): the attempt is checked against the wait
    /// and counted in one atomic step after the body is in, so a staged burst is counted one by one. A wait in
    /// force answers 429 with <c>Retry-After</c> and compares nothing, a correct token presented during it
    /// included, or the wait would tell the caller which attempt was right. A success gives back only its own
    /// reservation and answers with the session cookie and a 302 to
    /// the page the form was covering (its <c>return</c> field, through the open-redirect guard).
    /// </summary>
    private async Task HandleTokenSignInAsync(
        HttpContext context,
        string accessToken,
        byte[] signingKey,
        DarlingSignInThrottle signInThrottle,
        DarlingHttpRefusalLog refusals,
        bool oidcEnabled,
        string? publicHostName,
        Func<DateTime> clock,
        string? mcpToken)
    {
        var remote = context.Connection.RemoteIpAddress;
        var now = clock();

        if (!HttpMethods.IsPost(context.Request.Method))
        {
            if (context.Request.Query.ContainsKey("token"))
            {
                refusals.Report(
                    _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status200OK, remote,
                    "a token was presented in the URL (?token=), which is not accepted; the login page was served instead, and its form posts the token",
                    now);
            }

            await WriteLoginPageAsync(context, oidcEnabled);
            return;
        }

        /* #6265: the web token is the credential for a person's writes (approving a note, closing a request), and an MCP agent holds the
           MCP token. A web token with the same value would let that agent sign in as a person, so the token form refuses to sign in
           at all (before the form is read or counted); OIDC sign-in is unaffected. The service logs a warning at start. */
        if (DarlingAgentDispatchChecks.WebTokenIsMcpToken(accessToken, mcpToken))
        {
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status403Forbidden, remote,
                "the web token is the same value as the MCP token, so token sign-in is refused; set a different web.network token",
                now);
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden, DarlingAgentDispatchChecks.WebTokenSameAsMcpMessage);
            return;
        }

        /* A POST from another site is refused before it is read or counted: a page the operator browses can
           submit a form to this address from the operator's own browser, and every such post would otherwise
           count against the operator's source. */
        if (!IsSameSiteSignIn(context.Request, publicHostName))
        {
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.TokenSignInSite, StatusCodes.Status403Forbidden, remote,
                "the sign-in POST came from another site (its Sec-Fetch-Site says cross-site, or its Origin or Referer does not name the host this dashboard serves; behind a reverse proxy that rewrites Host, set web.publicBaseUrl), so it was not read or counted",
                now);
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "The sign-in request did not come from this dashboard's own page. Open the dashboard and use the form. "
                + "Behind a reverse proxy that rewrites the Host header, set web.publicBaseUrl to the address the browser uses.");
            return;
        }

        var form = await ReadLoginFormAsync(context);
        if (form is null)
        {
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status400BadRequest, remote,
                $"the sign-in POST did not carry a url-encoded form of at most {MaxLoginFormBytes} bytes, so it was not used",
                now);
            await WriteSignInErrorAsync(context, StatusCodes.Status400BadRequest,
                "The sign-in request was not the login form. Open the dashboard and use the form.");
            return;
        }

        /* Checked against the wait and counted in ONE step, after the body is in and with a fresh clock, and
           before the token is compared: attempts staged on open connections and released together are
           counted one by one, and only the allowance reaches the comparison. */
        var source = DarlingSignInThrottle.SourceKey(remote);
        var attempt = await signInThrottle.BeginAttemptAsync(source, clock, context.RequestAborted);
        now = clock();
        if (attempt.IsHeld)
        {
            var seconds = (int)Math.Ceiling(attempt.RetryAfter.TotalSeconds);
            var totalHold = attempt.Hold == SignInHold.Total;
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status429TooManyRequests, remote,
                totalHold
                    ? $"sign-in attempts are held for {seconds} more second(s): {DarlingSignInThrottle.GlobalMaxAttempts} wrong tokens arrived within the hour from several addresses, so no token is compared from an address that has not signed in recently; restarting the service clears it, and the failing client is in this log (fix its token or take it out of allowFrom)"
                    : $"sign-in attempts from this source are held for {seconds} more second(s) after repeated failures; no token is compared during the wait, and restarting the service clears it",
                now);
            context.Response.Headers.RetryAfter = seconds.ToString(CultureInfo.InvariantCulture);
            await WriteSignInErrorAsync(context, StatusCodes.Status429TooManyRequests,
                totalHold
                    ? $"Sign-in is held for {seconds} seconds: {DarlingSignInThrottle.GlobalMaxAttempts} wrong tokens arrived in the last hour from several addresses, not necessarily this one. "
                      + "No token is compared from an address that has not signed in recently. "
                      + "The hold is kept in memory: restarting the service clears it. Find the client that sends the wrong token and fix it or take it out of allowFrom, or the hold returns."
                    : $"Too many failed sign-in attempts. Try again in {seconds} seconds. No token is compared during the wait. "
                      + "The wait is kept in memory: restarting the service clears it.");
            return;
        }

        var returnPath = SignInLandingPath(form.GetValueOrDefault("return", string.Empty));
        if (!DarlingHostBinding.FixedTimeTokenEquals(form.GetValueOrDefault("token", string.Empty), accessToken))
        {
            signInThrottle.Missed(attempt);
            refusals.Report(
                _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status200OK, remote,
                "the posted token does not match web.network.encryptedToken, so the login page was served instead",
                now);
            /* The form is served again as the answer to this POST, so its address is /auth/login: the page names
               the target it was covering in the form itself, or the retry would post the form's own address. */
            await WriteLoginPageAsync(context, oidcEnabled, returnPath);
            return;
        }

        signInThrottle.Accepted(attempt);
        AppendSessionCookie(context, signingKey);
        context.Response.Redirect(returnPath);
    }

    /// <summary>
    /// PURE: where a signed-in operator lands. The requested page through <see cref="SanitizeRedirectPath"/> (a local
    /// path only), except that a sign-in or sign-out page itself maps to <c>/</c>: those paths are served without
    /// looking at the session cookie, so landing on one would show the form again to someone who has just signed in.
    /// </summary>
    internal static string SignInLandingPath(string? requested)
    {
        var safe = SanitizeRedirectPath(requested ?? string.Empty);
        var end = safe.IndexOfAny(['?', '#']);
        var pathOnly = end < 0 ? safe : safe[..end];
        return IsAuthFlowPath(pathOnly, oidcEnabled: true) ? "/" : safe;
    }

    /// <summary>
    /// PURE (reads headers only): did this sign-in POST come from the dashboard's own page? A browser states
    /// it in <c>Sec-Fetch-Site</c>, which page script cannot set and which the browser works out from the URL
    /// it actually used, so it is right behind a reverse proxy that rewrites <c>Host</c>: <c>same-origin</c>
    /// passes and <c>cross-site</c> is refused. A browser that sends no <c>Sec-Fetch-Site</c> (or says
    /// <c>same-site</c>) is judged by <c>Origin</c>, or <c>Referer</c> when Origin is missing: whichever is
    /// there must name the host this request was addressed to, port included, or <c>web.publicBaseUrl</c>'s
    /// host. Behind a proxy listed in <c>trustedProxies</c> the request's host is already the one the proxy
    /// forwarded in <c>X-Forwarded-Host</c> (the forwarded-headers middleware replaces it), so that name is
    /// compared too. An Origin that is not a URL (the string <c>null</c>) is refused. A request with none of
    /// these headers is not a browser post from another page and passes, which keeps command-line and script
    /// sign-ins working. Host names compare in their ASCII (punycode) form on both sides.
    /// </summary>
    internal static bool IsSameSiteSignIn(HttpRequest request, string? publicHostName)
    {
        var fetchSite = request.Headers["Sec-Fetch-Site"].ToString();
        if (string.Equals(fetchSite, "cross-site", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.Equals(fetchSite, "same-origin", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var claimed = request.Headers.Origin.ToString();
        if (claimed.Length == 0)
        {
            claimed = request.Headers.Referer.ToString();
        }

        if (claimed.Length == 0)
        {
            return true;
        }

        if (!Uri.TryCreate(claimed, UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            return false;
        }

        if (publicHostName is not null && SameHostName(uri.IdnHost, publicHostName))
        {
            return true;
        }

        var host = request.Host;
        if (!host.HasValue)
        {
            return false;
        }

        var portMatches = host.Port is int port ? uri.Port == port : uri.IsDefaultPort;
        return portMatches && SameHostName(uri.IdnHost, host.Host);
    }

    /// <summary>
    /// PURE: two host names are the same name when their ASCII forms match (case-insensitive), so a Unicode
    /// name and its punycode form compare equal. A name the IDN mapping rejects (an IPv6 literal, say)
    /// is compared as written.
    /// </summary>
    internal static bool SameHostName(string left, string right)
    {
        return string.Equals(ToAsciiHostName(left), ToAsciiHostName(right), StringComparison.OrdinalIgnoreCase);
    }

    private static string ToAsciiHostName(string name)
    {
        try
        {
            return new System.Globalization.IdnMapping().GetAscii(name);
        }
        catch (ArgumentException)
        {
            return name;
        }
    }

    /// <summary>
    /// The sign-in form's fields, or null when the body is not a url-encoded form of at most
    /// <see cref="MaxLoginFormBytes"/> bytes. The cap is enforced on the bytes actually read, so a chunked
    /// body (no Content-Length) is held to it too; a multipart body is not read at all (the form posts
    /// <c>application/x-www-form-urlencoded</c>, and the framework's multipart reader would spool file parts to
    /// disk). The server's own per-request body limit is lowered to the cap where the server exposes one.
    /// </summary>
    internal static async Task<Dictionary<string, string>?> ReadLoginFormAsync(HttpContext context)
    {
        if (!IsUrlEncodedForm(context.Request.ContentType) || context.Request.ContentLength is > MaxLoginFormBytes)
        {
            return null;
        }

        var limit = context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>();
        if (limit is { IsReadOnly: false })
        {
            limit.MaxRequestBodySize = MaxLoginFormBytes;
        }

        var buffer = new byte[MaxLoginFormBytes + 1];
        var total = 0;
        try
        {
            while (total < buffer.Length)
            {
                var read = await context.Request.Body.ReadAsync(buffer.AsMemory(total), context.RequestAborted);
                if (read == 0)
                {
                    break;
                }

                total += read;
            }
        }
        catch (BadHttpRequestException)
        {
            /* The server's own limit tripped at the cap: too large, as above. */
            return null;
        }

        if (total > MaxLoginFormBytes)
        {
            return null;
        }

        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in Microsoft.AspNetCore.WebUtilities.QueryHelpers.ParseQuery(Encoding.UTF8.GetString(buffer, 0, total)))
        {
            fields[pair.Key] = pair.Value.ToString();
        }

        return fields;
    }

    private static bool IsUrlEncodedForm(string? contentType)
    {
        const string FormType = "application/x-www-form-urlencoded";
        if (string.IsNullOrEmpty(contentType)
            || !contentType.StartsWith(FormType, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return contentType.Length == FormType.Length || contentType[FormType.Length] == ';' || contentType[FormType.Length] == ' ';
    }

    /// <summary>Sign-in refusals go through the rate-limited refusal log (#2479): the flow endpoints are
    /// reachable pre-credential by design, so an unthrottled warning per failed callback would hand a
    /// scanner the log-flood the limiter exists to bound. The one deliberate exception is the
    /// role-mapping denial, which stays a direct line naming the subject — it is the audit trail, and
    /// reaching it costs a completed IdP handshake.</summary>
    private void ReportSignInRefusal(DarlingHttpRefusalLog refusals, IPAddress? remote, int statusCode, string reason)
        => refusals.Report(_logger, "Web dashboard", DarlingRefusalGate.SignIn, statusCode, remote, reason, DateTime.UtcNow);

    /// <summary>
    /// The post-sign-in landing page. An HTML page with a script navigation instead of a 302, and the
    /// difference is load-bearing: the session cookie is SameSite=Strict, and a 302 here would be the tail
    /// of a navigation chain the IDP'S origin initiated — cross-site, so the browser would withhold the
    /// just-set cookie from the redirect target and the user would land back on the login page having
    /// genuinely signed in. A navigation started by a script on THIS page is same-site, and Strict cookies
    /// travel with it.
    /// </summary>
    internal static Task WriteSignedInLandingAsync(HttpContext context, string returnPath)
    {
        var safePath = SanitizeRedirectPath(returnPath);
        /* HTML-encode once for the fallback link and for the data- attribute the script reads its target from. */
        var linkTarget = System.Text.Encodings.Web.HtmlEncoder.Default.Encode(safePath);

        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        var html =
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Darling Web</title>" + IconLinkHtml + "</head>"
            + "<body style='background:#181b1f;color:#E4E6EB;font-family:system-ui'>"
            + $"<p>Signed in — continuing to <a id='go' style='color:#2eaef1' href='{linkTarget}' data-target='{linkTarget}'>the dashboard</a>…</p>"
            + LandingScriptBlock
            + "</body></html>";
        /* The page's one inline script is constant text (the return path travels in the data-target attribute),
           so its policy is computed once and never from this rendering. */
        context.Response.Headers[DarlingWebSecurityHeaders.ContentSecurityPolicyHeader] = LandingPagePolicy;
        return context.Response.WriteAsync(html);
    }

    /// <summary>
    /// PURE (reads headers only): is this a GET page request that the browser says came from another site, so the
    /// SameSite=Strict session cookie was not sent (#6286)? True only when <c>Sec-Fetch-Site</c> says
    /// <c>cross-site</c>. A request without the header (an older browser, a command-line client), with
    /// <c>none</c> (typed address, bookmark) or with <c>same-origin</c> / <c>same-site</c> is false, so it gets the
    /// sign-in page as before: the same-origin answer to the re-navigation below is what ends the loop. When the
    /// browser also says what the request is for, only a top-level navigation counts (<c>Sec-Fetch-Mode</c>
    /// <c>navigate</c>, <c>Sec-Fetch-Dest</c> <c>document</c>), so a cross-site image or script request is not
    /// answered with a page. It must also be a navigation the person started (<c>Sec-Fetch-User</c> is <c>?1</c>, which a
    /// browser sends for a click or a typed address and not for a script redirect, a meta refresh or a popup nobody
    /// clicked), so such a navigation gets the sign-in form as it did before. Never true for a non-GET request or an
    /// /api (or /api/*) call.
    /// </summary>
    internal static bool IsCrossSitePageNavigation(HttpRequest request)
    {
        if (!HttpMethods.IsGet(request.Method) || IsApiPath(request.Path.Value ?? "/"))
        {
            return false;
        }

        if (!string.Equals(request.Headers["Sec-Fetch-Site"].ToString(), "cross-site", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!string.Equals(request.Headers["Sec-Fetch-User"].ToString(), "?1", StringComparison.Ordinal))
        {
            return false;
        }

        var mode = request.Headers["Sec-Fetch-Mode"].ToString();
        var dest = request.Headers["Sec-Fetch-Dest"].ToString();
        return (mode.Length == 0 || string.Equals(mode, "navigate", StringComparison.OrdinalIgnoreCase))
            && (dest.Length == 0 || string.Equals(dest, "document", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The page a cross-site page request with no session cookie is answered with (#6286): it reloads itself, which
    /// asks for the SAME address again, path, query and <c>#/...</c> fragment included. A navigation started by a
    /// script on this site is same-site, so the Strict session cookie travels with it, as on the signed-in landing
    /// page. The script reloads rather than calling <c>location.replace(location.href)</c> because an address that
    /// carries a fragment is a same-document navigation to a script: the browser does nothing and the page stays.
    /// The script names no address at all, so the page cannot send anyone elsewhere. It reloads only when the page
    /// came from an ordinary navigation; in every other case (it is itself the result of a reload, the browser gives no
    /// navigation entry, or the script fails) it shows a message instead, so a browser that still marked the request
    /// cross-site cannot loop. A static link to the sign-in page is always in the markup.
    /// </summary>
    internal static Task WriteReNavigationPageAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        var html =
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Darling Web</title>" + IconLinkHtml + "</head>"
            + "<body style='background:#181b1f;color:#E4E6EB;font-family:system-ui'>"
            + "<p id='m'>Opening the dashboard…</p>"
            + "<p><a href='/' style='color:#2eaef1'>Go to the sign-in page</a></p>"
            + ReNavigationScriptBlock
            + "</body></html>";
        context.Response.Headers[DarlingWebSecurityHeaders.ContentSecurityPolicyHeader] = ReNavigationPagePolicy;
        return context.Response.WriteAsync(html);
    }

    /// <summary>The re-navigation page's only inline script: constant text, so its hash does not depend on the request.</summary>
    internal const string ReNavigationScriptBlock = "<script>try{var n=performance.getEntriesByType('navigation')[0];if(n&&n.type==='navigate'){location.reload();}else{document.getElementById('m').textContent='Open the address again to continue.';}}catch(e){document.getElementById('m').textContent='Open the address again to continue.';}</script>";

    /// <summary>The re-navigation page's policy: the default plus the hash of <see cref="ReNavigationScriptBlock"/>.</summary>
    internal static readonly string ReNavigationPagePolicy = DarlingWebSecurityHeaders.PolicyForInlinePage(ReNavigationScriptBlock);

    /// <summary>
    /// The tab-icon link every page this host writes by hand carries in its own head (#5701), the same <c>data:</c>
    /// SVG as <c>wwwroot/index.html</c>. Without a link, a browser asks for <c>/favicon.ico</c> on its first load and
    /// gets a 404, which the UI smoke check treats as a failed request. The default policy allows
    /// <c>img-src data:</c>, so no page needs a file or a round trip for it.
    /// </summary>
    internal const string IconLinkHtml =
        "<link rel=\"icon\" href=\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Crect width='16' height='16' rx='3' fill='%231f6feb'/%3E%3Cpath d='M2 11l3-4 3 2 3-5 3 3' fill='none' stroke='white' stroke-width='1.6' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E\" />";

    /// <summary>The landing page's only inline script: constant text that reads the sanitized return path from the
    /// link's data-target attribute, so its hash does not depend on the request.</summary>
    internal const string LandingScriptBlock = "<script>location.replace(document.getElementById('go').dataset.target);</script>";

    /// <summary>The landing page's policy: the default plus the hash of <see cref="LandingScriptBlock"/>.</summary>
    internal static readonly string LandingPagePolicy = DarlingWebSecurityHeaders.PolicyForInlinePage(LandingScriptBlock);

    /// <summary>The sign-in flow's answer to a single-valued query key sent more than once (#5245): the same
    /// rate-limited refusal log and error page every other failed sign-in uses, status 400.</summary>
    private async Task RefuseRepeatedSignInKeyAsync(HttpContext context, DarlingHttpRefusalLog refusals, IPAddress? remote)
    {
        ReportSignInRefusal(refusals, remote, StatusCodes.Status400BadRequest,
            "a sign-in query parameter (return, error, error_description, code or state) was sent more than once");
        await WriteSignInErrorAsync(context, StatusCodes.Status400BadRequest,
            "The sign-in request repeated a parameter. Start again from the login page.");
    }

    /// <summary>A minimal self-contained sign-in error page — same reasoning as the login form: it renders
    /// before auth, so it can reference nothing gated.</summary>
    internal static Task WriteSignInErrorAsync(HttpContext context, int statusCode, string message)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        return context.Response.WriteAsync(
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Darling Web</title>" + IconLinkHtml + "</head>"
            + "<body style='background:#181b1f;color:#E4E6EB;font-family:system-ui'>"
            + $"<p>{System.Text.Encodings.Web.HtmlEncoder.Default.Encode(message)}</p>"
            + "<p><a style='color:#2eaef1' href='/'>Back to the login page</a></p>"
            + "</body></html>");
    }

    /// <summary>
    /// Serves the login form. <paramref name="returnPath"/> (already through <see cref="SignInLandingPath"/>) is the
    /// page the form was covering, carried back in the form's hidden <c>return</c> field when this page answers a
    /// POST to <see cref="LoginPath"/> (a wrong token), where the address bar no longer names that page. A hidden
    /// field's value is not a script or style block, so the page's policy does not change with it.
    /// </summary>
    internal static Task WriteLoginPageAsync(HttpContext context, bool oidcEnabled, string? returnPath = null)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.Headers.CacheControl = "no-store";
        var html = BuildLoginPageHtml(oidcEnabled);
        if (!string.IsNullOrEmpty(returnPath))
        {
            html = html.Replace(
                ReturnFieldHtml,
                ReturnFieldHtml.Replace("value=''", $"value='{System.Text.Encodings.Web.HtmlEncoder.Default.Encode(returnPath)}'", StringComparison.Ordinal),
                StringComparison.Ordinal);
        }

        /* The login page carries one inline style block and one or two small inline scripts (it renders before the
           gated stylesheet and scripts are reachable); each is allowed by its hash. The page is constant text
           (the two renderings differ only by the SSO fragment), so each policy is computed once from the
           LF-normalized text and never from a rendering. */
        context.Response.Headers[DarlingWebSecurityHeaders.ContentSecurityPolicyHeader] = LoginPagePolicy(oidcEnabled);
        return context.Response.WriteAsync(html);
    }

    private static readonly string s_loginPageHtmlPlain = ComposeLoginPage(oidcEnabled: false);
    private static readonly string s_loginPageHtmlSso = ComposeLoginPage(oidcEnabled: true);
    private static readonly string s_loginPolicyPlain = DarlingWebSecurityHeaders.PolicyForInlinePage(s_loginPageHtmlPlain);
    private static readonly string s_loginPolicySso = DarlingWebSecurityHeaders.PolicyForInlinePage(s_loginPageHtmlSso);

    /// <summary>The Content-Security-Policy of the login page, fixed per rendering (SSO link or not).</summary>
    internal static string LoginPagePolicy(bool oidcEnabled) => oidcEnabled ? s_loginPolicySso : s_loginPolicyPlain;

    /* The source file is checked out with CRLF, and a browser hashes the LF text, so the page is served in LF. */
    private static string ComposeLoginPage(bool oidcEnabled)
        => DarlingWebSecurityHeaders.NormalizeLineEndings(
            LoginPageHtml.Replace("<!--SSO-->", oidcEnabled ? SsoFragmentHtml : string.Empty, StringComparison.Ordinal)
                .Replace("<!--ICON-->", IconLinkHtml, StringComparison.Ordinal));

    /// <summary>The login page, with the SSO affordance included exactly when OIDC is enabled — the token
    /// form is never removed (#2550 keeps the shared token as the scripted-caller/break-glass path).</summary>
    internal static string BuildLoginPageHtml(bool oidcEnabled)
        => oidcEnabled ? s_loginPageHtmlSso : s_loginPageHtmlPlain;

    /* A minimal, fully self-contained login form (no external references) — a POST form to LoginPath whose
       fields are the access token and the page to return to, so the token travels in the request body and
       never in a URL; HandleTokenSignInAsync exchanges it for a session cookie and 302s back to `return`. It
       renders BEFORE auth, so it cannot link the gated /css/theme.css (the static files sit behind this same
       auth gate); instead it INLINES the same custom-property token block as theme.css (keep in sync) and
       carries the same wordmark/subtitle as index.html, plus an input focus style and the host being
       accessed. Single-quoted HTML attributes so the C# verbatim string needs no quote-doubling. */
    private const string LoginPageHtml = @"<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'>
<!-- A same-origin referrer policy for this page: the response header says no-referrer, and under that policy a browser sends 'Origin: null' with this form's POST, which the sign-in cannot tell from a post made by another site. same-origin keeps the real Origin on the post to this host and sends nothing to any other site. -->
<meta name='referrer' content='same-origin'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Darling Web</title>
<!--ICON-->
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
  .sso { border-top: 1px solid var(--border); margin-top: 0.5rem; padding-top: 0.75rem; text-align: center; }
  .sso a { display: block; padding: 0.55rem; border-radius: var(--radius-sm); border: 1px solid var(--accent); color: var(--accent); text-decoration: none; font-weight: 600; font-size: 0.9rem; }
  .sso a:hover { background: rgba(46, 174, 241, 0.12); }
</style>
</head>
<body>
<form method='post' action='/auth/login'>
  <div class='brand'>
    <h1>Darling Web</h1>
    <div class='sub'>SQL Server and PostgreSQL monitor</div>
  </div>
  <label for='token'>Access token</label>
  <input id='token' name='token' type='password' autocomplete='off' autofocus>
  <!-- #4221: the page this form is covering (pathname+search+hash, the fragment included) rides the POST as
       a field, and the sign-in leg 302s back to it, so an alert link survives sign-in. -->
  <input type='hidden' name='return' id='return' value=''>
  <button type='submit'>Enter</button>
<!--SSO-->
  <div class='host' id='host'></div>
</form>
<script>document.getElementById('host').textContent = 'Accessing ' + location.host;
if (location.pathname.toLowerCase() !== '/auth/login') { document.getElementById('return').value = location.pathname + location.search + location.hash; }</script>
</body>
</html>";

    /* Spliced over the <!--SSO--> placeholder when OIDC is enabled (#2550). The link carries the page the
       user was actually trying to reach as ?return=, so sign-in lands them there rather than on the root;
       the server runs it through SanitizeRedirectPath before trusting it. Sits INSIDE the form for layout
       only — it is an anchor, not a submit. */
    private const string SsoFragmentHtml = @"  <div class='sso'><a id='sso' href='/auth/oidc/login'>Sign in with SSO</a></div>
  <script>document.getElementById('sso').href = '/auth/oidc/login?return=' + encodeURIComponent(location.pathname.toLowerCase() === '/auth/login' ? document.getElementById('return').value : location.pathname + location.search + location.hash);</script>";

    /* The form's hidden return field exactly as the page carries it: the text WriteLoginPageAsync fills in. */
    private const string ReturnFieldHtml = "<input type='hidden' name='return' id='return' value=''>";

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
                    "Web dashboard network exposure requested but web.network.allowFrom '{AllowFrom}' is not a valid CIDR list or an " +
                    "entry's address family does not match web.network.listen — refusing to expose; binding loopback-only. " +
                    "Use one CIDR (e.g. 192.168.1.0/24) or several, separated by commas " +
                    "(e.g. 10.8.0.0/16,192.168.1.5/32): every entry in CIDR form (/32 for one address), with each IPv4 " +
                    "address written as four plain decimal numbers (no leading zeros) and no IPv6 zone index, and of the same " +
                    "family as listen (a :: listen takes IPv6 entries only). Host bits are masked (192.168.1.5/24 means 192.168.1.0/24).",
                    web.Network?.AllowFrom);
                break;

            case DarlingHostBinding.BindReason.ManagedModeRequired:
                _logger.Log(level.Value,
                    "web.network.* is set but postgres.managed = false — web dashboard network exposure is managed-mode (or container, " +
                    "#1804) only, so listen and allowFrom are ignored; your own reverse proxy governs uncontained BYO exposure. " +
                    "Binding loopback-only. A token in the block (web.network.encryptedToken / token) still gates the loopback dashboard.");
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
