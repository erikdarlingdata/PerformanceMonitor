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
using System.Security.Cryptography.X509Certificates;
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
/// ambiguous or expired fail-closes to loopback-only exactly as an undecryptable token does; it never
/// downgrades to serving the LAN over HTTP.</para>
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

        _serverCertificate?.Dispose();
        _serverCertificate = null;

        _oidcClient?.Dispose();
        _oidcClient = null;

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

        try { _serverCertificate?.Dispose(); } catch { /* best-effort */ }
        _serverCertificate = null;

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

            /* The optional extra allowed Host name (#2550) — resolved before TLS because the SAN advice
               below reads it. Invalid shapes are dropped with a warning rather than degrading the bind:
               unlike a broken token this cannot expose anything, it can only fail to admit a Host. */
            string? publicHost = null;
            if (networkMode && !string.IsNullOrWhiteSpace(network!.PublicHost))
            {
                publicHost = NormalizePublicHost(network.PublicHost);
                if (publicHost is null)
                {
                    _logger.LogWarning(
                        "web.network.publicHost '{PublicHost}' is not a bare host name (no scheme, no port, no path) — ignoring it; the Host allowlist admits only loopback and the listen IP.",
                        DarlingHttpRefusalLog.Sanitize(network.PublicHost));
                }
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
                        publicHost is null ? $"{network.Listen} or localhost" : $"{publicHost}, {network.Listen} or localhost");
                }
            }

            /* TLS for the network listener (#2562). Resolved HERE rather than in the pure ResolveBind ladder for
               the same reason the token is: loading a certificate reads files and a clock, and the ladder is
               kept free of both. It also means a certificate failure degrades exactly the way a token failure
               does — Critical, then loopback-only — instead of needing its own BindReason, which the MCP host's
               parallel enum would have had to grow a member it can never use. */
            DarlingWebTls.LoadedCertificate? serverCertificate = null;
            if (networkMode)
            {
                var plan = DarlingWebTls.Describe(network!.Tls);
                switch (plan.Shape)
                {
                    case DarlingWebTls.TlsShape.NotConfigured:
                        /* The pre-#2562 behaviour, still the default, and still the only thing a plain-HTTP
                           reverse proxy in front of the port needs. Warn every start: the token and the cookie
                           it mints are readable by anything on the segment, and that is easy not to notice
                           precisely because the dashboard works perfectly. */
                        _logger.LogWarning(
                            "Web dashboard is LAN-exposed WITHOUT TLS — the access token and its session cookie cross the "
                            + "segment in the clear, and web.network.allowFrom bounds only who can route to the port. "
                            + "Configure web.network.tls (a PKCS#12 bundle or a PEM pair), or front the port with a "
                            + "TLS-terminating reverse proxy.");
                        break;

                    case DarlingWebTls.TlsShape.Invalid:
                        _logger.LogCritical(
                            "Web dashboard TLS is misconfigured ({Problem}) — refusing to expose; binding loopback-only.",
                            plan.Problem);
                        networkMode = false;
                        break;

                    default:
                        try
                        {
                            var loaded = DarlingWebTls.Load(network.Tls!, plan.Shape);
                            var certificate = loaded.Leaf;

                            if (plan.Warning is not null)
                            {
                                _logger.LogWarning("Web dashboard TLS: {Warning}", plan.Warning);
                            }

                            /* Lifetime is checked BEFORE the listener is built, not left to the handshake: an
                               expired certificate takes the dashboard down either way, and this is the only
                               place the reason reaches an operator's log.

                               ToUniversalTime() is not decoration: X509Certificate2.NotBefore/NotAfter return
                               LOCAL DateTimes, and while the implicit DateTime->DateTimeOffset conversion does
                               carry the local offset and would compare correctly, it reads as a UTC value to
                               everyone who follows. Convert where the trap is, not where it detonates. */
                            var refusal = DarlingWebTls.LifetimeRefusal(
                                certificate.NotBefore.ToUniversalTime(),
                                certificate.NotAfter.ToUniversalTime(),
                                DateTimeOffset.UtcNow);
                            if (refusal is not null)
                            {
                                loaded.Dispose();
                                _logger.LogCritical(
                                    "Web dashboard TLS certificate cannot be used ({Refusal}) — refusing to expose; binding loopback-only.",
                                    refusal);
                                networkMode = false;
                                break;
                            }

                            /* Adopted by the field IMMEDIATELY, before any of the bail paths below it (port in
                               use, store credential not ready), so every one of them releases the key. */
                            serverCertificate = loaded;
                            _serverCertificate = loaded;

                            /* The SAN has to name the IP, not a hostname, and that is a consequence of a
                               control that lives two files away: the anti-DNS-rebind Host allowlist accepts
                               only `localhost`, a loopback literal, or the configured listen IP, so a LAN
                               client CANNOT reach this dashboard by DNS name — it is refused 400 before any
                               route runs. An internal CA asked for "a certificate for darling.corp.local"
                               issues exactly the certificate that can never match here, and the operator
                               would learn that from a browser warning rather than from us.

                               A warning, not a refusal: the dashboard genuinely works after a click-through,
                               and taking it down over a name mismatch would be a worse outcome than saying so.
                               Skipped on a wildcard bind, where there is no single address to match against.
                               Reuses the store's own SAN reader rather than growing a second one. */
                            if (!networkListenIp!.Equals(IPAddress.Any)
                                && !networkListenIp.Equals(IPAddress.IPv6Any)
                                && !DarlingManagedPostgres.CertificateSanCoversIp(certificate, networkListenIp)
                                /* #2550: with a publicHost the certificate may legitimately be a DNS-name
                                   certificate — browsers reach the dashboard by that name, the allowlist
                                   admits it, and an iPAddress SAN stops being the only way to match. Only a
                                   certificate matching NEITHER earns the warning. */
                                && !(publicHost is not null && certificate.MatchesHostname(publicHost)))
                            {
                                /* One placeholder per argument, in order. A repeated {Listen} would read
                                   naturally and throw at format time, which the surrounding catch would
                                   report as "web dashboard failed to start". */
                                _logger.LogWarning(
                                    "Web dashboard TLS certificate carries no iPAddress SAN for {Listen} — every browser "
                                    + "will report a name mismatch. The anti-DNS-rebind Host allowlist accepts only that "
                                    + "literal IP or loopback in the Host header, so a LAN client has to browse to it by IP "
                                    + "on port {Port} and a DNS-name-only certificate can never match. Reissue the "
                                    + "certificate with an iPAddress SAN.",
                                    networkListenIp, effectivePort);
                            }

                            var expiry = DarlingWebTls.ExpiryWarning(
                                certificate.NotAfter.ToUniversalTime(), DateTimeOffset.UtcNow);
                            if (expiry is not null)
                            {
                                _logger.LogWarning(
                                    "Web dashboard TLS certificate {Expiry} — subject {Subject}, thumbprint {Thumbprint}. "
                                    + "The dashboard stops serving when it lapses; renew it before then.",
                                    expiry, certificate.Subject, certificate.Thumbprint);
                            }
                            else
                            {
                                _logger.LogInformation(
                                    "Web dashboard TLS certificate loaded — subject {Subject}, thumbprint {Thumbprint}, expires {NotAfter:u}.",
                                    certificate.Subject, certificate.Thumbprint, certificate.NotAfter.ToUniversalTime());
                            }
                        }
                        catch (Exception ex)
                        {
                            /* Release whatever was already adopted. Adoption happens BEFORE the SAN check and
                               the expiry logging, and both of those still run inside this try — the SAN reader
                               re-materializes an extension from raw DER and can throw on a malformed one. A
                               throw there lands here holding a certificate the listener will never use, and
                               this degrade RETURNS TRUE (loopback-only started fine), so the method's outer
                               catch and DisposeFailedStartAsync never see it. Without these two lines the
                               "every bail path releases the key" claim on _serverCertificate is false. */
                            _serverCertificate?.Dispose();
                            _serverCertificate = null;
                            serverCertificate = null;

                            _logger.LogCritical(
                                "Web dashboard TLS certificate could not be loaded ({Message}) — refusing to expose; binding loopback-only.",
                                ex.Message);
                            networkMode = false;
                        }

                        break;
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

            var listenerCertificate = serverCertificate;
            builder.WebHost.ConfigureKestrel(options =>
            {
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
                            /* ServerCertificateChain, not just ServerCertificate: Kestrel presents ONLY what
                               it is handed, so an intermediate left out here is an incomplete chain and a
                               failed handshake on every client that has not independently cached it. Measured
                               against a real leaf+intermediate PEM before this was wired: the server sent one
                               certificate. */
                            listen.UseHttps(https =>
                            {
                                https.ServerCertificate = listenerCertificate.Value.Leaf;
                                if (listenerCertificate.Value.Chain.Count > 0)
                                {
                                    https.ServerCertificateChain = listenerCertificate.Value.Chain;
                                }
                            });
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

            /* Suppress ASP.NET Core console logging — the service's own logger reports lifecycle. */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* The VIEWER-role store pool the read endpoints query (registered for DI + passed to MapAll). */
            builder.Services.AddSingleton<NpgsqlDataSource>(postgres);

            _app = builder.Build();

            /* #2479 item 5: the gates below used to refuse silently. Rate-limited per (gate, source),
               because this port is LAN-exposed on purpose - see DarlingHttpRefusalLog. Created per
               started server so a rebind starts with a clean budget. */
            var refusals = new DarlingHttpRefusalLog();

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
                if (!IsAllowedHost(context.Request.Host.Host, networkListenIp, publicHost))
                {
                    refusals.Report(
                        _logger, "Web dashboard", DarlingRefusalGate.HostAllowlist, StatusCodes.Status400BadRequest,
                        context.Connection.RemoteIpAddress,
                        $"the Host header '{DarlingHttpRefusalLog.Sanitize(context.Request.Host.Host)}' is not an address this endpoint binds"
                        + " (a loopback name/IP, web.network.listen, or web.network.publicHost when LAN-exposed)",
                        DateTime.UtcNow);
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
                   cookie (acceptable — the operator re-presents the token once). The OIDC transaction key is
                   DERIVED from it rather than shared — see DeriveTransactionKey for why sharing the raw key
                   across the two cookie shapes would let a pre-auth transaction cookie impersonate a session. */
                var signingKey = RandomNumberGenerator.GetBytes(SigningKeyBytes);
                var transactionKey = DarlingWebOidc.DeriveTransactionKey(signingKey);

                _app.Use(async (context, next) =>
                {
                    /* The Host-allowlist / DNS-rebinding guard already ran above (both modes); this gate owns
                       the network auth decision (session cookie / ?token= / in-CIDR / the sign-in flow). */
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

                    var presentedToken = context.Request.Query["token"].ToString();
                    var hasValidToken = DarlingHostBinding.FixedTimeTokenEquals(presentedToken, token);
                    var isAuthFlowRoute = IsAuthFlowPath(context.Request.Path.Value ?? "/", oidcClient is not null);

                    switch (DecideWebRequest(remote, cidr, isAuthFlowRoute, hasValidCookie, hasValidToken))
                    {
                        case WebRequestAction.Allow:
                            /* The seat rides HttpContext.Items so EVERY downstream consumer — /api/session,
                               the updated_by stamps, endpoints added on any parallel branch — resolves the
                               same identity without per-endpoint wiring. */
                            context.Items[DarlingWebSeat.HttpContextItemKey] = seat;

                            /* The group-level write gate (#2550): a read-only seat is refused every
                               mutating request HERE, before routing, so a write endpoint added tomorrow is
                               born gated instead of born exposed. */
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
                            await HandleAuthFlowAsync(context, oidcClient, signingKey, transactionKey, seat);
                            return;

                        case WebRequestAction.SetCookieAndRedirect:
                            AppendSessionCookie(context, signingKey);
                            /* 302 (default) to the same path with ?token= stripped, so the token never lingers
                               in browser history, bookmarks, or a Referer header. */
                            context.Response.Redirect(BuildPathWithoutToken(context.Request));
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
                            /* A 200 rather than a 401, so this is not a "rejected request" by status - and
                               it is exactly the state an operator asks about when a ?token= they pasted did
                               not work. Logged ONLY when a token was actually presented and did not match:
                               a first visit with no token is the normal path to the login page and logging
                               it would make every bookmark a warning. */
                            if (!string.IsNullOrEmpty(presentedToken))
                            {
                                refusals.Report(
                                    _logger, "Web dashboard", DarlingRefusalGate.Token, StatusCodes.Status200OK,
                                    remote,
                                    "the presented ?token= does not match web.network.encryptedToken, so the login page was served instead",
                                    DateTime.UtcNow);
                            }

                            await WriteLoginPageAsync(context, oidcClient is not null);
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
                /* #2562: name the SCHEME the exposed listener actually speaks. An operator reading this line is
                   deciding whether the token they are about to paste crosses the wire in the clear, and the
                   line used to say "http://" unconditionally because that was the only thing it could be. */
                _logger.LogInformation(
                    "Starting web dashboard on {Scheme}://{Listen}:{Port} (LAN-exposed to {Cidr} behind a token->cookie gate + in-app CIDR; "
                    + "loopback also bound over plain HTTP, and since #1649 it authenticates too) — "
                    + "enabled/port from {Origin}; listen/allowFrom/token/tls from darling.json web.network (file-only, restart-only)",
                    serverCertificate is null ? "http" : "https", primaryBind, effectivePort, allowedCidr, origin);
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
    /// The web host's #2550 extension of the shared allowlist: everything the shared guard admits, plus AT
    /// MOST ONE configured DNS name (<c>web.network.publicHost</c>, already normalized). One name, compared
    /// case-insensitively as a whole — never a suffix/subdomain match, which is how allowlists rot into
    /// rebind holes. Null <paramref name="publicHost"/> is byte-for-byte the shared behavior, so every
    /// existing deployment is unchanged.
    /// </summary>
    internal static bool IsAllowedHost(string? host, IPAddress? networkListenIp, string? publicHost)
        => DarlingHostBinding.IsAllowedHost(host, networkListenIp)
           || (publicHost is not null && string.Equals(host, publicHost, StringComparison.OrdinalIgnoreCase));

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

    /* ---------------------------------------------------------------------------------------------------
       OIDC sign-in (#2550). The flow endpoints, the per-request decision that routes to them, and the
       handler. DecideWebAuth above is untouched — its matrix is the pinned pre-OIDC behavior, and
       DecideWebRequest composes AROUND it rather than growing it, so the token path cannot regress by
       construction.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The verdict for one network-mode request, with the sign-in flow added (#2550). The first four
    /// members are <see cref="WebAuthAction"/>'s, mapped one-to-one.</summary>
    internal enum WebRequestAction { Allow, Forbid, SetCookieAndRedirect, ShowLogin, HandleAuthFlow }

    internal const string OidcLoginPath = "/auth/oidc/login";
    internal const string OidcCallbackPath = "/auth/oidc/callback";
    internal const string LogoutPath = "/auth/logout";

    /// <summary>
    /// PURE: is this path one the auth-flow handler owns? The two OIDC legs exist only while OIDC is
    /// enabled (disabled, they fall through to normal auth and render the login page — no route squatting);
    /// logout is ALWAYS a flow path, because the shared-token seat holds a session cookie too and deserves a
    /// way to drop it.
    /// </summary>
    internal static bool IsAuthFlowPath(string path, bool oidcEnabled)
    {
        if (string.Equals(path, LogoutPath, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return oidcEnabled
            && (string.Equals(path, OidcLoginPath, StringComparison.OrdinalIgnoreCase)
                || string.Equals(path, OidcCallbackPath, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// PURE per-request decision, composing the sign-in flow around <see cref="DecideWebAuth"/>: the CIDR
    /// stays OUTERMOST (an out-of-CIDR caller is 403 even on the OIDC endpoints — sign-in is not a way past
    /// the network boundary), then a flow path is handled regardless of credential state (a signed-in user
    /// re-visiting /auth/oidc/login is starting a new sign-in, not asking for the dashboard), then the
    /// original matrix decides exactly as before.
    /// </summary>
    internal static WebRequestAction DecideWebRequest(
        IPAddress? remoteIp, IPNetwork allowedCidr, bool isAuthFlowRoute, bool hasValidCookie, bool hasValidToken)
    {
        var isLoopback = DarlingWebEndpoints.IsLoopbackRemote(remoteIp);
        if (!isLoopback && !DarlingHostBinding.IsRemoteAddressAllowed(remoteIp, allowedCidr))
        {
            return WebRequestAction.Forbid;
        }

        if (isAuthFlowRoute)
        {
            return WebRequestAction.HandleAuthFlow;
        }

        return DecideWebAuth(remoteIp, allowedCidr, hasValidCookie, hasValidToken) switch
        {
            WebAuthAction.Allow => WebRequestAction.Allow,
            WebAuthAction.Forbid => WebRequestAction.Forbid,
            WebAuthAction.SetCookieAndRedirect => WebRequestAction.SetCookieAndRedirect,
            _ => WebRequestAction.ShowLogin,
        };
    }

    /// <summary>
    /// PURE: normalizes <c>web.network.publicHost</c> to a bare, lowercase host name, or null when the value
    /// is not one (a scheme, a port, a path, whitespace — the operator pasted a URL where a name goes). An
    /// IP literal is accepted too (a NAT'd or proxied dashboard may be reached by an address that is not the
    /// listen IP), but the design center is the DNS name that lets an Okta-class IdP accept the redirect URI.
    /// </summary>
    internal static string? NormalizePublicHost(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var candidate = raw.Trim();
        return Uri.CheckHostName(candidate) == UriHostNameType.Unknown
            ? null
            : candidate.ToLowerInvariant();
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
        DarlingWebSeat currentSeat)
    {
        var path = context.Request.Path.Value ?? "/";

        if (string.Equals(path, LogoutPath, StringComparison.OrdinalIgnoreCase))
        {
            /* Local sign-out only — the cookie dies, the IdP session (if any) lives on. RP-initiated logout
               at the IdP would sign the user out of everything ELSE their org runs, which is not this
               button's mandate. A GET with no CSRF token is deliberate too: the worst a forged logout does
               is show someone the login page. */
            if (currentSeat.Subject is not null)
            {
                _logger.LogInformation("Web dashboard sign-out: {Subject}", currentSeat.Subject);
            }

            context.Response.Cookies.Delete(SessionCookieName, new CookieOptions { Path = "/" });
            context.Response.Redirect("/");
            return;
        }

        if (oidcClient is null)
        {
            /* Unreachable through IsAuthFlowPath (the OIDC legs only qualify while a client exists), kept
               as a fail-closed backstop rather than a NullReference away from a 500. */
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        if (string.Equals(path, OidcLoginPath, StringComparison.OrdinalIgnoreCase))
        {
            var discovery = await oidcClient.GetDiscoveryAsync(context.RequestAborted);
            if (discovery is null)
            {
                _logger.LogWarning(
                    "OIDC sign-in unavailable: the discovery document could not be fetched/parsed from {Authority} — serving the token login instead. The next sign-in attempt retries.",
                    oidcClient.Options.Authority);
                await WriteSignInErrorAsync(context, StatusCodes.Status502BadGateway,
                    "Single sign-on is temporarily unavailable (the identity provider did not answer). Use the access token, or try again.");
                return;
            }

            var (verifier, challenge) = DarlingWebOidc.CreatePkce();
            var state = DarlingWebOidc.CreateFlowValue();
            var nonce = DarlingWebOidc.CreateFlowValue();

            /* Where to land after the sign-in — the SPA path the login page was covering, run through the
               same open-redirect guard as the token-strip 302. */
            var returnPath = SanitizeRedirectPath(context.Request.Query["return"].ToString());

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

        var remote = context.Connection.RemoteIpAddress;

        var providerError = context.Request.Query["error"].ToString();
        if (!string.IsNullOrEmpty(providerError))
        {
            /* IdP-authored, but it rode a browser querystring here — sanitize like any other echoed input. */
            _logger.LogWarning(
                "OIDC sign-in refused by the provider: {Error} {Description}",
                DarlingHttpRefusalLog.Sanitize(providerError),
                DarlingHttpRefusalLog.Sanitize(context.Request.Query["error_description"].ToString(), 128));
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "The identity provider refused the sign-in. Ask your administrator, or use the access token.");
            return;
        }

        var code = context.Request.Query["code"].ToString();
        var presentedState = context.Request.Query["state"].ToString();
        var hasTransaction = DarlingWebOidc.TryValidateTransactionCookie(
            context.Request.Cookies[DarlingWebOidc.TransactionCookieName], transactionKey, DateTimeOffset.UtcNow,
            out var transaction);

        /* One-shot either way: success or failure, this transaction is spent. */
        context.Response.Cookies.Delete(DarlingWebOidc.TransactionCookieName, new CookieOptions { Path = "/auth/oidc" });

        if (string.IsNullOrEmpty(code) || !hasTransaction
            || !string.Equals(presentedState, transaction!.State, StringComparison.Ordinal))
        {
            ReportSignInRefusal(remote, "the callback carried no code, or its state did not match the sign-in this host started (expired/tampered transaction, or a cross-site forgery)");
            await WriteSignInErrorAsync(context, StatusCodes.Status400BadRequest,
                "This sign-in attempt is stale or was not started by this dashboard. Start again from the login page.");
            return;
        }

        var discoveryForExchange = await oidcClient.GetDiscoveryAsync(context.RequestAborted);
        if (discoveryForExchange is null)
        {
            _logger.LogWarning("OIDC sign-in failed at the exchange: discovery is no longer available from {Authority}.", oidcClient.Options.Authority);
            await WriteSignInErrorAsync(context, StatusCodes.Status502BadGateway,
                "Single sign-on is temporarily unavailable (the identity provider did not answer). Use the access token, or try again.");
            return;
        }

        var exchange = await oidcClient.ExchangeCodeAsync(
            discoveryForExchange.TokenEndpoint, code, transaction.RedirectUri, transaction.CodeVerifier, context.RequestAborted);
        if (exchange.IdToken is null)
        {
            _logger.LogWarning("OIDC code exchange failed: {Error}", exchange.Error);
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
            ReportSignInRefusal(remote, $"the ID token failed validation: {claimProblem}");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "The identity provider's answer did not validate. The service log has the specific check that failed.");
            return;
        }

        var subject = DarlingWebOidc.ResolveSubject(payload!, oidcClient.Options.SubjectClaim);
        if (subject is null)
        {
            ReportSignInRefusal(remote, oidcClient.Options.SubjectClaim is null
                ? "the ID token carries none of the subject claims (preferred_username/email/sub)"
                : $"the ID token does not carry the configured subjectClaim '{oidcClient.Options.SubjectClaim}' — refusing rather than falling back to a claim the operator did not choose");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Signed in at the provider, but the token carries no usable identity for this dashboard. The service log names the missing claim.");
            return;
        }

        if (subject.Length > DarlingWebSeat.MaxSubjectLength)
        {
            /* The cookie's own rule (#2583): refuse, never truncate — truncation merges identities. */
            ReportSignInRefusal(remote, $"the resolved subject is {subject.Length} characters (limit {DarlingWebSeat.MaxSubjectLength})");
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Signed in at the provider, but the account identifier is too long for this dashboard's session. Configure a shorter subjectClaim.");
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
                subject, oidcClient.Options.RoleClaim);
            await WriteSignInErrorAsync(context, StatusCodes.Status403Forbidden,
                "Your account is not granted a seat on this dashboard. Ask your administrator to add you to a mapped role.");
            return;
        }

        AppendSessionCookie(context, signingKey, DarlingWebSeat.EncodeCookieSubject(subject, role));

        /* Per-user identity in the service log — the #2550 counterpart of the updated_by stamp. */
        _logger.LogInformation(
            "OIDC sign-in: {Subject} as {Role} from {Remote}",
            subject, role == WebOidcRole.Admin ? "admin (edit)" : "viewer (read-only)", remote);

        await WriteSignedInLandingAsync(context, transaction.ReturnPath);
    }

    /// <summary>Sign-in refusals share the refusal log's shape but not its object (they are per-flow, not
    /// per-request-flood); a plain warning keeps the who/why in one greppable line.</summary>
    private void ReportSignInRefusal(IPAddress? remote, string reason)
        => _logger.LogWarning("OIDC sign-in refused for {Remote}: {Reason}", remote, reason);

    /// <summary>
    /// The post-sign-in landing page. An HTML page with a script navigation instead of a 302, and the
    /// difference is load-bearing: the session cookie is SameSite=Strict, and a 302 here would be the tail
    /// of a navigation chain the IDP'S origin initiated — cross-site, so the browser would withhold the
    /// just-set cookie from the redirect target and the user would land back on the login page having
    /// genuinely signed in. A navigation started by a script on THIS page is same-site, and Strict cookies
    /// travel with it.
    /// </summary>
    private static Task WriteSignedInLandingAsync(HttpContext context, string returnPath)
    {
        var safePath = SanitizeRedirectPath(returnPath);
        /* JSON-encode for the script (handles quotes/backslashes), HTML-encode for the fallback link. */
        var scriptTarget = System.Text.Json.JsonSerializer.Serialize(safePath);
        var linkTarget = System.Text.Encodings.Web.HtmlEncoder.Default.Encode(safePath);

        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        return context.Response.WriteAsync(
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Darling Web</title></head>"
            + "<body style='background:#181b1f;color:#E4E6EB;font-family:system-ui'>"
            + $"<p>Signed in — continuing to <a style='color:#2eaef1' href='{linkTarget}'>the dashboard</a>…</p>"
            + $"<script>location.replace({scriptTarget});</script>"
            + "</body></html>");
    }

    /// <summary>A minimal self-contained sign-in error page — same reasoning as the login form: it renders
    /// before auth, so it can reference nothing gated.</summary>
    private static Task WriteSignInErrorAsync(HttpContext context, int statusCode, string message)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "text/html; charset=utf-8";
        return context.Response.WriteAsync(
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Darling Web</title></head>"
            + "<body style='background:#181b1f;color:#E4E6EB;font-family:system-ui'>"
            + $"<p>{System.Text.Encodings.Web.HtmlEncoder.Default.Encode(message)}</p>"
            + "<p><a style='color:#2eaef1' href='/'>Back to the login page</a></p>"
            + "</body></html>");
    }

    private static Task WriteLoginPageAsync(HttpContext context, bool oidcEnabled)
    {
        context.Response.StatusCode = StatusCodes.Status200OK;
        context.Response.ContentType = "text/html; charset=utf-8";
        return context.Response.WriteAsync(BuildLoginPageHtml(oidcEnabled));
    }

    /// <summary>The login page, with the SSO affordance included exactly when OIDC is enabled — the token
    /// form is never removed (#2550 keeps the shared token as the scripted-caller/break-glass path).</summary>
    internal static string BuildLoginPageHtml(bool oidcEnabled)
        => LoginPageHtml.Replace("<!--SSO-->", oidcEnabled ? SsoFragmentHtml : string.Empty);

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
  .sso { border-top: 1px solid var(--border); margin-top: 0.5rem; padding-top: 0.75rem; text-align: center; }
  .sso a { display: block; padding: 0.55rem; border-radius: var(--radius-sm); border: 1px solid var(--accent); color: var(--accent); text-decoration: none; font-weight: 600; font-size: 0.9rem; }
  .sso a:hover { background: rgba(46, 174, 241, 0.12); }
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
<!--SSO-->
  <div class='host' id='host'></div>
</form>
<script>document.getElementById('host').textContent = 'Accessing ' + location.host;</script>
</body>
</html>";

    /* Spliced over the <!--SSO--> placeholder when OIDC is enabled (#2550). The link carries the page the
       user was actually trying to reach as ?return=, so sign-in lands them there rather than on the root;
       the server runs it through SanitizeRedirectPath before trusting it. Sits INSIDE the form for layout
       only — it is an anchor, not a submit. */
    private const string SsoFragmentHtml = @"  <div class='sso'><a id='sso' href='/auth/oidc/login'>Sign in with SSO</a></div>
  <script>document.getElementById('sso').href = '/auth/oidc/login?return=' + encodeURIComponent(location.pathname + location.search);</script>";

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
