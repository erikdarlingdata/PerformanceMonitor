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
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Optional hosted service exposing Darling's full MCP tool surface over Streamable HTTP — the analysis
/// class (6 tools) plus the plan-analysis tools and the ~60 STORED data-read tools (resource metrics,
/// query performance, blocking/deadlocks, sessions, config history + current-config snapshots, index/object,
/// latch/spinlock/memory-grant/plan-cache/scheduler/jobs, windowed trends, the system_health parse-on-read
/// family, and the fleet-triage alerts + health-overview reads)
/// — the same names Lite and the Dashboard expose, all reading Darling's Postgres store (no live
/// monitored-server hit except <c>analyze_server</c>'s plan fetch). Same transport/hosting model as
/// Lite's <c>McpHostService</c> (ModelContextProtocol.AspNetCore, Kestrel, stateless HTTP,
/// Gemini-compatible tool registration for #1074); both reasons for HTTP-over-stdio (the server outlives
/// any one client and serves concurrent clients) apply MORE to a 24/7 headless service.
///
/// <para><b>Network exposure — off by default, secure by default (darling-network-endpoints, D3):</b>
/// with no <c>mcp.network</c> block the server binds loopback only and is TOKENLESS — byte-for-byte
/// today's local MCP, so existing local clients are unaffected. An opt-in <c>mcp.network</c> block
/// (MANAGED MODE ONLY) binds the specified LAN interface (plus both loopback families) behind two
/// middlewares installed FIRST in the pipeline, before any MCP handler/handshake: an in-app CIDR check on
/// <c>RemoteIpAddress</c> (loopback always allowed, Round-4 #2) and an unconditional constant-time bearer
/// token (NO loopback exemption — the loopback guard). The effective bind is decided by the pure
/// <see cref="ResolveMcpBind"/>; the caller maps its reason to a severity — LogCritical on a missing
/// precondition (token / valid allowFrom CIDR) and LogWarning in BYO mode — and degrades to loopback-only
/// either way. Fail-closed, enforced HERE (the MCP host), NEVER in the all-fatal
/// <see cref="DarlingConfig.Validate"/> (the worker's abort would not stop this host). The scoped, idempotent
/// firewall rule is created by the ELEVATED installer (#1771 — this account cannot); here it is only CHECKED
/// and reported (defense-in-depth; the token + CIDR are the boundary, not the firewall).</para>
///
/// <para><b>TLS on the network listener: opt-in (#5288).</b> With an <c>mcp.network.tls</c> block (a PKCS#12
/// bundle or a PEM pair, the shapes <c>web.network.tls</c> takes) the LAN listener serves HTTPS, intermediates
/// included. With no block it serves plain HTTP exactly as before, and the bearer token is readable on the
/// segment (<see cref="DarlingListenerTls.Resolve"/> warns about that at every start; a TLS-terminating reverse
/// proxy in front of the endpoint is the other answer). The two loopback listeners stay plain HTTP either way,
/// because the certificate names the LAN address and that surface never leaves the machine; on a wildcard listen
/// there is ONE listener and it serves HTTPS to loopback too. A certificate that cannot be used (unreadable,
/// invalid, expired, not yet valid) degrades to loopback-only with a Critical line, as an unreadable token does,
/// never to plain HTTP on the LAN. The certificate's expiry reaches the worker's alert sweep through
/// <see cref="McpTlsCertificateState"/>. Like the rest of <c>mcp.network</c>, the block is read once at start
/// and a change takes a service restart.</para>
///
/// <para>Gated by darling.json's <c>mcp.enabled</c> (default OFF — a headless service should not open a
/// port unless the operator asks); when disabled or when the config cannot load (the worker already logs
/// that as critical), this service stands down without affecting collection. Registered always in
/// Program.cs and self-gating here, because config loading/validation is the worker's job and Program.cs
/// stays config-free.</para>
///
/// <para>The MCP surface gets its OWN <see cref="NpgsqlDataSource"/> over the store, connecting as the
/// dedicated least-privilege <c>mcp</c> role (D3-role) on a managed store and on the compose distribution's own
/// store; on any other store as <c>postgres.mcpConnectionString</c>, or as the owner with a startup warning when
/// that is unset (<see cref="DarlingStoreLogins"/>, #3914). As mcp it is NOT the superuser owner, so a token-holder (or a
/// future/buggy tool) reaches only the viewer read surface plus the <c>analysis_findings</c> /
/// <c>analysis_muted</c> INSERTs the tools persist, never the <c>config_command</c> service-credential
/// pivot or the carved secret columns. It also gets its own <see cref="DarlingAnalysisService"/>. Store
/// migration + role provisioning are the WORKER's job; the <c>mcp</c>-role credential is written AFTER
/// migration (later than the owner's), so the first-boot poll budget tolerates the delay. The plan fetcher
/// resolves a finding's serverId to a live connection string from the worker-published registry
/// (<see cref="MonitoredServerRegistryState"/>, #2298 — darling.json only before the worker's first
/// publish; DPAPI resolution lazy per fetch; any resolution/connection failure degrades the fetch to null
/// inside <see cref="PgPlanFetcher"/>). On a brand-new store, tool calls before the first migration/connect
/// simply return their error/miss envelopes.</para>
/// </summary>
public sealed class DarlingMcpHostService : BackgroundService
{
    private readonly ILogger<DarlingMcpHostService> _logger;
    private readonly McpRuntimeState _state;

    /* #2298: the worker-published monitored-server registry the plan-fetch resolver reads per fetch —
       this host never re-reads config_monitored_servers itself (the mcp role's encrypted_password
       SELECT-carve fails that whole read by design). */
    private readonly MonitoredServerRegistryState _registryState;

    private WebApplication? _app;
    private NpgsqlDataSource? _appDataSource;
    private int _runningPort;

    /// <summary>How often the supervisor re-reads the live control-plane state (#1560).</summary>
    internal static readonly TimeSpan SupervisorPollInterval = TimeSpan.FromSeconds(5);

    /// <summary>Backoff after a FAILED start attempt (port in use, credential not ready) so a persistent
    /// failure logs on a calm cadence instead of every poll tick.</summary>
    internal static readonly TimeSpan FailedStartBackoff = TimeSpan.FromSeconds(30);

    /* #3941: the process's shared baseline tier (the worker's passes fill it), so analyze_server and compare_analysis
       inside an analysis hour a scheduled pass already computed read no 30-day baseline. Optional so a host built
       outside the service's DI (a test) keeps a private one. */
    private readonly BaselineCache _baselineCache;

    /* #4442 scope 2: the process-wide read-latency accumulator, the SAME singleton Program.cs hands the web
       host and the worker's flush (DI, one AddSingleton<ReadLatencyAccumulator> registration) -- optional so a
       test-constructed host (which passes none) keeps a private, throwaway accumulator rather than a
       null-reference, exactly like BaselineCache above. */
    private readonly ReadLatencyAccumulator _readLatency;

    /* #5097: where the tool filter offers a slow or failed call; null in a test-constructed host. */
    private readonly SlowReadLog? _slowReads;

    /* #5288: where this host publishes the served certificate's expiry facts for the worker's alert sweep. It is
       the SAME McpTlsCertificateState singleton DI hands the worker (Program.cs registers it once), so a Publish
       or Clear here is what the "MCP TLS Certificate Expiring" self-alert reads. Optional so a test-constructed
       host (which passes none) keeps a private, throwaway state rather than a null-reference, exactly like
       BaselineCache above. */
    private readonly McpTlsCertificateState _mcpTlsCertState;

    /// <summary>The TLS certificate the current network listener presents (#5288): the leaf AND the intermediates
    /// that travel with it, held for the listener's lifetime. Disposal is not bookkeeping: on Windows the private
    /// key is loaded with <c>MachineKeySet</c> and no <c>PersistKeySet</c>, so disposing is what REMOVES the key
    /// material from the machine key store, and a rebind or a failed start that leaked it would accumulate a key
    /// per attempt. Adopted the moment <see cref="DarlingListenerTls.Resolve"/> returns, so every later bail path
    /// releases it; released by <see cref="ReleaseServerCertificate"/>.</summary>
    private DarlingWebTls.LoadedCertificate? _serverCertificate;

    public DarlingMcpHostService(ILogger<DarlingMcpHostService> logger, McpRuntimeState state, MonitoredServerRegistryState registryState, BaselineCache? baselineCache = null, ReadLatencyAccumulator? readLatency = null, SlowReadLog? slowReads = null, McpTlsCertificateState? mcpTlsCertState = null)
    {
        _slowReads = slowReads;
        _logger = logger;
        _state = state;
        _registryState = registryState;
        _baselineCache = baselineCache ?? new BaselineCache();
        _readLatency = readLatency ?? new ReadLatencyAccumulator();
        _mcpTlsCertState = mcpTlsCertState ?? new McpTlsCertificateState();
    }

    /// <summary>The supervisor's per-tick verdict — pure over (running, runningPort, enabled, desiredPort)
    /// so a unit test pins the whole decision table without a server (#1560).</summary>
    public enum McpSupervisorAction { None, Start, Stop, Restart }

    internal static McpSupervisorAction DecideMcpAction(bool running, int runningPort, bool enabled, int desiredPort)
    {
        if (!running)
        {
            return enabled ? McpSupervisorAction.Start : McpSupervisorAction.None;
        }

        if (!enabled)
        {
            return McpSupervisorAction.Stop;
        }

        return runningPort == desiredPort ? McpSupervisorAction.None : McpSupervisorAction.Restart;
    }

    /// <summary>
    /// The supervisor loop (#1560): the viewer's Settings toggle writes config_service.mcp_enabled /
    /// mcp_port, the worker's reload beacon publishes the live values to <see cref="McpRuntimeState"/>,
    /// and this loop starts / stops / rebinds the inner web app to match — no service restart. Until the
    /// worker's first publish the FILE values apply (byte-for-byte the old behavior, and the only signal
    /// available when the store is unreachable). The mcp.network exposure block stays file-defined and
    /// restart-only by design; the store toggle still stops an exposed server instantly (a kill switch).
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        /* Config load lives INSIDE the supervisor loop, on the failed-start backoff (#2038) — the web host's
           twin fix. A front-loaded Load() failure used to stand this host down for the process LIFETIME at
           Debug level, so one transient darling.json read failure at boot silently killed MCP until the next
           manual restart. Once loaded, the config is held for the process lifetime exactly as before (the
           network exposure block is restart-only by design). */
        DarlingConfig? config = null;
        var lastFailedStartUtc = DateTime.MinValue;
        /* #2389: the last control-plane-override report emitted, so a steady disagreement is stated once per
           distinct state instead of on every 5s poll tick. */
        string? lastOverrideReport = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            if (config is null && CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff))
            {
                try
                {
                    config = DarlingConfig.Load();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        "MCP server configuration could not be loaded ({Message}) — retrying in {Backoff}s. The worker logs a missing/broken config as critical; a transient read failure self-heals here.",
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
               now carries WHICH plane supplied each value, so neither the start line nor a disagreement has to
               be inferred from two INFO lines five seconds apart. */
            var toggle = DarlingHostBinding.ResolveEndpointToggle(
                published is null ? null : (published.Enabled, published.Port), config.Mcp.Enabled, config.Mcp.Port);

            /* Report the DISAGREEMENT at the point of override, not the outcome. Once per distinct state (the
               last-reported string, the same shape as the firewall check's ShouldReport) so a steady mismatch
               says its piece once per service start rather than every poll tick, while a LATER re-divergence —
               someone toggling the store after boot — is still reported. */
            var overrideReport = DarlingHostBinding.DescribeToggleOverride(toggle, "mcp", "MCP", config.Mcp.Enabled, config.Mcp.Port);
            if (overrideReport is not null && !string.Equals(overrideReport, lastOverrideReport, StringComparison.Ordinal))
            {
                _logger.LogWarning("{Report}", overrideReport);
            }

            lastOverrideReport = overrideReport;

            switch (DecideMcpAction(_app is not null, _runningPort, toggle.Enabled, toggle.Port))
            {
                case McpSupervisorAction.Start when CollectorCadence.IntervalElapsed(lastFailedStartUtc, DateTime.UtcNow, FailedStartBackoff):
                    if (!await TryStartServerAsync(config, toggle, stoppingToken))
                    {
                        lastFailedStartUtc = DateTime.UtcNow;
                    }
                    break;

                case McpSupervisorAction.Stop:
                    _logger.LogInformation("MCP server disabled via the control plane — stopping (no restart needed)");
                    await StopServerAsync(stoppingToken);
                    break;

                case McpSupervisorAction.Restart:
                    _logger.LogInformation(
                        "MCP port changed via the control plane ({Old} -> {New}) — rebinding", _runningPort, toggle.Port);
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
            /* #5288: nothing is serving, but a start that adopted a certificate and then stopped short of building
               the app (shutdown caught it mid-start, say) must not leave its key held or its expiry published.
               Released BEFORE this return, so no path through this method skips it. */
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
            _logger.LogWarning("MCP server stop reported an error (continuing): {Message}", ex.Message);
        }

        _app = null;

        if (_appDataSource is not null)
        {
            await _appDataSource.DisposeAsync();
            _appDataSource = null;
        }

        /* #5288: the certificate goes only after the listener that served it has stopped, so a handshake can never
           reach a key already pulled from the machine key store, and its published expiry goes with it, so the
           worker stops alerting on a certificate nothing serves. A runtime disable of MCP (a no-restart op)
           reaches here; a port-change rebind runs Stop then Start in the same supervisor tick, so the next start
           re-publishes before the hourly sweep can observe the empty state. */
        ReleaseServerCertificate();

        _runningPort = 0;
    }

    /// <summary>Failed-start cleanup: a partially built app / data source / certificate must not leak between attempts.</summary>
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

        /* #5288: a start that adopted its certificate (before the port-in-use / credential bails after it) but never
           served TLS must not hold the key until the next full stop, nor leave the worker alerting on a certificate
           nothing serves. */
        ReleaseServerCertificate();
    }

    /// <summary>
    /// Disposes the served certificate, forgets it, and withdraws its published expiry facts (#5288). Best-effort,
    /// so a throw from releasing a key cannot stop the rest of a stop or a failed-start cleanup. Safe to call when
    /// there is no certificate.
    /// </summary>
    private void ReleaseServerCertificate()
    {
        try { _serverCertificate?.Dispose(); } catch { /* best-effort */ }
        _serverCertificate = null;
        _mcpTlsCertState.Clear();
    }

    /// <summary>
    /// One start ATTEMPT of the inner MCP web app at <paramref name="toggle"/>'s port (#1560): the whole
    /// pre-supervisor startup body, with two changes — the port comes from the live control-plane value
    /// rather than the file, and every bail path returns false so the supervisor can retry with backoff
    /// instead of standing down for the process lifetime. The bind/network/token decisions still come from
    /// the FILE-loaded config (network exposure is deliberately restart-only); returns true when the app
    /// is started and listening.
    /// <para>#2389: the toggle carries the enable/port PROVENANCE, not just the port, so the start line names
    /// the plane each half of the bind came from — the operator greps that line and stops reading, so it has
    /// to admit when it is starting on file values the control plane may be about to contradict.</para>
    /// </summary>
    private async Task<bool> TryStartServerAsync(
        DarlingConfig config, DarlingHostBinding.EndpointToggle toggle, CancellationToken stoppingToken)
    {
        var effectivePort = toggle.Port;

        /* Decide the effective bind PURELY, then map the reason -> severity here (Round-4 #7: the caller,
           not the pure fn, chooses LogCritical vs LogWarning; tests assert (Mode, Reason) without a logger). */
        var bind = ResolveMcpBind(config.Mcp, config.Postgres.Managed);
        LogBindReason(config.Mcp, bind.Reason);

        try
        {
            var networkMode = bind.Mode == McpBindMode.NetworkAndLoopback;

            /* In network mode ResolveMcpBind has already validated the listen IP, the allowFrom CIDR list (#5288),
               AND every entry's address-family agreement with the listen, so these two parses cannot throw; only
               resolving the token can still fail (a corrupt DPAPI blob), which fail-closes to loopback-only
               rather than exposing tokenless. The list type's default admits nobody, so the value the loopback
               mode never reads fails closed too. */
            IPAddress? networkListenIp = null;
            CidrAllowList allowedCidr = default;
            string bearerToken = "";
            if (networkMode)
            {
                networkListenIp = IPAddress.Parse(config.Mcp.Network!.Listen!.Trim());
                allowedCidr = CidrAllowList.Parse(config.Mcp.Network.AllowFrom!);

                try
                {
                    var token = config.Mcp.Network.ResolveToken(out var usedPlaintext);
                    if (string.IsNullOrWhiteSpace(token))
                    {
                        _logger.LogCritical(
                            "MCP network token resolved to empty after decryption — refusing to expose; binding loopback-only.");
                        networkMode = false;
                    }
                    else
                    {
                        bearerToken = token;
                        if (usedPlaintext)
                        {
                            _logger.LogWarning(
                                "mcp.network.token is set in plaintext (dev convenience) — prefer mcp.network.encryptedToken " +
                                "(produced by --encrypt-password). This token gates ALL MCP network access.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(
                        "MCP network token could not be decrypted ({Message}) — refusing to expose; binding loopback-only.",
                        ex.Message);
                    networkMode = false;
                }
            }

            /* TLS for the network listener (#5288). Resolved HERE, in network mode only, for the reason the web host
               resolves its own here: loading a certificate reads files and a clock, and the pure bind ladder
               (ResolveMcpBind) is kept free of both. A certificate failure therefore degrades exactly as the token
               failure above does, Critical and then loopback-only, rather than needing a bind reason of its own. It
               sits BEFORE primaryBind and before the Host-name decision further down, because a refusal changes the
               final mode and both read it.

               The block itself lives in DarlingListenerTls.Resolve, shared with the web host, so there is one
               fail-closed path and not two copies of it. What stays here is what only this host can do: adopt the
               certificate, and refuse to expose when TLS was asked for and no certificate came back. */
            DarlingWebTls.LoadedCertificate? serverCertificate = null;
            if (networkMode)
            {
                var network = config.Mcp.Network!;
                var tlsOutcome = DarlingListenerTls.Resolve(
                    _logger, _mcpTlsCertState, ListenerTlsLabels.Mcp, network.Tls, networkListenIp!, effectivePort,
                    NormalizedHostName(network.HostName));
                networkMode = tlsOutcome.Expose;

                /* Adopted by the field IMMEDIATELY, before any of the bail paths below it (port in use, store
                   credential not ready, shutdown mid-start), so every one of them releases the key through
                   DisposeFailedStartAsync. Resolve owned the certificate until it returned; from this line the
                   field does. */
                serverCertificate = tlsOutcome.Certificate;
                _serverCertificate = tlsOutcome.Certificate;

                /* #5288 review F1: Kestrel decides HTTPS from "the listener was handed a certificate", not from
                   "TLS was configured", so TLS asked for + network mode + no certificate would bind the LAN address
                   in plain HTTP. Resolve never returns that. This refuses it anyway, with its own Critical line,
                   because the cost of being wrong is cleartext on the segment. */
                if (tlsOutcome.ExposesWithoutItsCertificate)
                {
                    _logger.LogCritical(
                        "MCP server TLS is configured ({Shape}) but no certificate came back; refusing to expose, binding loopback-only.",
                        tlsOutcome.Shape);
                    networkMode = false;
                }
            }

            /* The REAL primary bind address (network IP when exposed, else loopback): both the port precheck
               and the Kestrel bind use it, so the precheck probes the actual address, not always loopback. */
            var primaryBind = networkMode ? networkListenIp! : IPAddress.Loopback;

            /* Port-in-use pre-check — Lite's StartMcpServerAsync guard, via the shared utility, against the
               REAL bind address (D3-e: not always IPAddress.Loopback). Done before the firewall check so a
               bail here reports nothing about the firewall. */
            if (await PortUtilityService.IsTcpPortListeningAsync(effectivePort, primaryBind, stoppingToken))
            {
                _logger.LogError("Port {Port} is already in use — MCP server not started this attempt; will retry", effectivePort);
                await DisposeFailedStartAsync();
                return false;
            }

            /* Firewall CHECK (managed mode only; read-only, never fatal) — #1771. Reports a missing rule when
               exposed and a stale one when not, naming the elevated command either way; the rule itself is
               created by the installer, because this process cannot create it. The token + in-app CIDR are the
               boundary; the firewall is defense-in-depth. */
            if (config.Postgres.Managed && OperatingSystem.IsWindows())
            {
                await CheckMcpFirewallAsync(
                    effectivePort, networkMode, networkMode ? allowedCidr.ToString() : null, stoppingToken);
            }

            /* Managed mode: the WORKER owns the bundled server's lifecycle; the MCP host only derives the
               least-privilege mcp-role connection string from the stored DPAPI credential (D3-role). */
            string? storeConnectionString;
            if (config.Postgres.Managed)
            {
                if (!OperatingSystem.IsWindows())
                {
                    _logger.LogError("MCP server not started: postgres.managed = true requires Windows");
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
                /* #3914: not the owner by default any more — postgres.mcpConnectionString, the mcp role the
                   service provisioned on the compose store, or the owner with a warning, in that order. */
                storeConnectionString = await DarlingStoreLogins.ResolveUnmanagedAsync(
                    DarlingStoreLogins.Surface.Mcp, config.Postgres, _logger, stoppingToken);
                if (storeConnectionString is null)
                {
                    await DisposeFailedStartAsync();
                    return false;
                }
            }

            /* Lifetime tied to the running app (#1560): disposed by StopServerAsync, not this method's
               scope — the supervisor may keep the app running across many poll ticks. #4479: the mcp-role
               connection string built by DarlingManagedPostgres already carries McpApplicationName, but a
               CONFIGURED (postgres.mcpConnectionString) or owner-fallback login never runs through that
               builder — set-if-absent here so every path this string can take still names the surface. */
            var postgres = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(storeConnectionString, DarlingManagedPostgres.McpApplicationName)));
            _appDataSource = postgres;

            /* serverId → connection string, keyed by the STORE's identity (review catch on #2218).
               Resolution is lazy so DPAPI decrypt runs only when a plan fetch actually needs the
               connection; first entry wins on a duplicate storage name, mirroring the worker's
               FirstOrDefault over runtimes.

               The server set comes from the WORKER's published registry (#2298), not a read of our own.
               This host used to re-read config_monitored_servers over its mcp-role connection, and that
               read selects encrypted_password — a column the section-6 secret ACL deliberately
               SELECT-carves from mcp (DarlingManagedRoles: mcp can WRITE a credential blob but never READ
               one back). The 42501 failed the whole config view read, so live plan fetch silently fell
               back to darling.json — on a seeded box, exactly the set of servers the file does not know
               about (#2254/#2256). The worker already loads the same rows over its privileged connection
               (it must, or it could not collect), so the process already holds everything this host was
               failing to re-read; a second, deliberately-restricted read of it was the defect. The mcp
               DATABASE role keeps its carve untouched — this state feeds only the in-process resolver,
               and no MCP tool exposes it, so a token-holder still cannot obtain a stored credential.

               Resolution reads the live snapshot PER FETCH rather than copying it once at host start:
               before the worker's first publish it falls back to darling.json (this host's documented
               store-down posture), and it heals on the next resolve after the publish — which also means
               a server added later through add_servers or the Viewer reaches this resolver on the
               worker's next reload, with no MCP restart. */
            var fileFallbackById = new Dictionary<int, MonitoredServer>();
            foreach (var server in config.Servers)
            {
                fileFallbackById.TryAdd(server.ServerId, server);
            }

            /* Review note on #2298: the old permanent-failure WARN is gone with the failing read, but the
               transient pre-publish window deserves a breadcrumb — once per inner-server (re)start (the
               supervisor's Start and port-rebind Restart both come through here), at Debug, because it is
               self-healing by design and a per-fetch log would just be noise. */
            if (_registryState.Read() is null)
            {
                _logger.LogDebug(
                    "MCP starting before the worker's first registry publish — live plan fetch resolves from darling.json until it arrives (self-healing; store-registered servers reach the resolver on the worker's next reload).");
            }

            var planFetcher = new PgPlanFetcher(
                serverId =>
                {
                    var byId = _registryState.Read()?.ById ?? fileFallbackById;
                    return byId.TryGetValue(serverId, out var server)
                        ? DarlingServerConnector.ResolveConnectionString(server, _logger)
                        : null;
                },
                _logger);

            /* #4286 review, Low 1: with no EnvironmentName set here, an ASPNETCORE_ENVIRONMENT or
               DOTNET_ENVIRONMENT of "Development" left set anywhere on the machine would add the developer
               exception page ahead of the Host guard and the bearer check -- a throw that escapes then answers
               with the exception message and stack trace on the loopback bind, which has no token. Same pin as
               the web host (DarlingWebHostService, #4281 review finding 4). */
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                EnvironmentName = Environments.Production,
            });

            /* The listener layout lives in ConfigureListeners (#5288), so a live-HTTP test binds the SAME listeners
               production does: the network listener carries the certificate, the loopback ones stay plain. */
            builder.WebHost.ConfigureKestrel(options =>
                ConfigureListeners(options, networkMode, primaryBind, effectivePort, serverCertificate));

            /* Suppress ASP.NET Core console logging — the service's own logger reports lifecycle. */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* Register services that MCP tools need via dependency injection. */
            builder.Services.AddSingleton<NpgsqlDataSource>(postgres);
            /* get_fleet_overview scopes an Azure master's counts by the live registry, the way the analysis service is. */
            builder.Services.AddSingleton<MonitoredServerRegistryState>(_registryState);
            /* #4214 part 2: get_store_host's config seat — the same config this host loaded to reach this
               point, so it cannot disagree with what actually connected. Read-only: GatherAsync only ever
               reads dataDirectory/Managed off it, never writes.
               Trimmed copy, not config.Postgres itself (round-1 review, Low 3): the full PostgresConfig
               also carries the owner connection string. Nothing serializes this DI registration today,
               but injecting only the two fields GatherAsync reads means a future [McpServerTool] that
               takes a PostgresConfig parameter cannot receive the owner secret through this seat. */
            builder.Services.AddSingleton(new PostgresConfig { Managed = config.Postgres.Managed, DataDirectory = config.Postgres.DataDirectory });
            /* #4535: the plan analyzer's per-rule config, read from darling.json's optional "analyzer"
               section. Null (section omitted) is AnalyzerConfig.Default; DarlingMcpPlanTools takes this
               as an [McpServerTool] method parameter the same way it takes NpgsqlDataSource above. */
            builder.Services.AddSingleton<AnalyzerConfig>(config.Analyzer ?? AnalyzerConfig.Default);
            /* #4214 round-1 review, Medium 2: get_store_host's 5-minute shared cache — the process-wide
               Shared instance, not a fresh one per request, so every caller (MCP and the direct-call web
               path below) actually shares the one cache window. Typed-generic AddSingleton<T>, not the
               untyped AddSingleton(instance) overload: McpServiceParameterDiSeatCensusTests greps this
               file's source text for AddSingleton<StoreHostProfileCache> specifically. */
            builder.Services.AddSingleton<StoreHostProfileCache>(StoreHostProfileCache.Shared);
            /* #4602: the same analyzer config just registered above, so MCP's plan advisories (get_analysis_facts,
               analyze_server, drill-down) honor a user's disabled/overridden rules the same way the worker
               (DarlingWorker.cs) and the web endpoints (DarlingWebEndpoints.cs) already do. Before this fix the
               MCP path silently fell back to AnalyzerConfig.Default. */
            /* #4726: registered PER CALL, through the method a test also calls (see RegisterAnalysisService). */
            RegisterAnalysisService(builder.Services, postgres, planFetcher, _logger, _baselineCache, config.Analyzer, _registryState);
            /* The HOST's logger, registered as the bare ILogger a tool method can take as a DI parameter
               (the postgres pattern one line up — service-typed params are resolved per request and never
               reach the advertised schema). Deliberately NOT the web app's own ILogger<T>: this builder
               clears its logging providers two blocks up, so anything resolved from the app's logging
               would be a logger with nowhere to write — the host's is the one wired to the service's
               real providers, the same instance DarlingAnalysisService already receives. Closes the
               #3473 review's observation: get_sweep_reports' child reads throw on a store fault
               (#4315) and the tool's own catch logs the exception once, where before this they
               degraded with no log trace anywhere on the MCP path. */
            builder.Services.AddSingleton<ILogger>(_logger);

            /* #2339: publish the declared peer stores before the instructions are rendered, so the same
               snapshot feeds the instructions section, list_servers' peer_fleets block, and the
               server-resolution miss message. Publishing here as well as in the worker is deliberate: either
               may reach its config first, the value is identical (both read darling.json), and the disclosure
               should not depend on which one won. An empty declaration is Snapshot.Empty, which leaves every
               one of those three surfaces exactly as it was.

               THIS host never calls DarlingConfig.Validate (see the class doc: its fail-closed checks are
               host-local, because the worker's abort is a return from the worker and would not stop this
               server). Publish therefore validates the peers block itself and refuses the whole thing on any
               problem — the same host-local fail-closed posture as ResolveMcpBind, and the reason a
               credential pasted into a peer description cannot reach a client from here. Reported at CRITICAL
               because it is a configuration defect that silently costs the operator their disclosure. */
            var peerPublish = DarlingPeerDirectory.Publish(config.Peers);
            var declaredPeers = peerPublish.Snapshot;

            /* #3712: the FILE half of the analysis routing knob (the store half is a settings-row column since
               V137, read off the row by the tool itself) — the peers precedent: either host may load its config
               first, and get_alert_settings must have the file's value from whichever did, to say what a NULL
               store column defers to. */
            DarlingFileLevelAlertSettings.Publish(config.Analysis);

            if (peerPublish.Refused)
            {
                foreach (var problem in peerPublish.RefusedProblems)
                {
                    _logger.LogCritical(
                        "MCP peer disclosure REFUSED (nothing published; peers are not disclosed until this is fixed): {Problem}",
                        problem);
                }
            }
            else if (!declaredPeers.IsEmpty)
            {
                _logger.LogInformation(
                    "MCP peer disclosure active: {PeerCount} declared peer store(s){Coverage}. Disclosure only — this service never contacts a peer.",
                    declaredPeers.Peers.Count,
                    declaredPeers.ThisStoreCovers.Length > 0 ? $"; this store covers {declaredPeers.ThisStoreCovers}" : "");
            }

            /* Register MCP server with the analysis tool class. */
            ConfigureMcpServices(builder.Services, declaredPeers, _readLatency, _logger, _slowReads);

            _app = builder.Build();

            /* The Host guard also admits mcp.network.hostName (#5288), in NETWORK mode only (review F2). networkMode
               is final by here (an unreadable token above has already made it loopback-only), and a loopback-only
               server admits no extra name. This deliberately differs from the web host (#4220), which admits
               web.publicBaseUrl's host in both modes: MCP has no link builder that needs the name, and its
               loopback surface is tokenless, so one more admitted name there would widen the very surface the
               guard exists to protect and buy nothing. A name that is set but refused logs one Warning inside
               ResolveAllowedHostName and is not admitted (fail closed). */
            var allowedHostName = ResolveAllowedHostName(config.Mcp.Network?.HostName, networkMode, _logger);

            ConfigurePipeline(_app, networkMode, networkListenIp, allowedCidr, bearerToken, allowedHostName);

            /* #2389: name the authority for each half of what is being started. enabled/port come from
               whichever plane the supervisor resolved; listen/allowFrom/token are always darling.json. */
            var origin = DarlingHostBinding.DescribeToggleOrigin(toggle);
            if (networkMode)
            {
                /* #2562/#5288 review F11: name the SCHEME the exposed listener actually speaks, and name loopback as
                   plain HTTP only when it is (see DescribeNetworkStart). With no TLS the text is the line this host
                   has always logged. Text only: no listener decision moves here. */
                _logger.LogInformation(
                    "{Line}", DescribeNetworkStart(serverCertificate is not null, primaryBind, effectivePort, allowedCidr, origin));
            }
            else
            {
                _logger.LogInformation(
                    "Starting MCP server on http://localhost:{Port} (loopback only) — enabled/port from {Origin}",
                    effectivePort, origin);
            }

            /* #2479 item 6: the network block is read ONCE and held for the process lifetime by design.
               Say so at every start, in BOTH modes - the loopback line above never mentioned the block at
               all, and loopback-when-you-expected-LAN is exactly the state being diagnosed.

               The null-conditional is load-bearing, not defensive. config.Mcp.Network is McpNetworkConfig?
               and is NULL on the default secure config - no mcp.network block at all - which is exactly the
               state this line exists to describe. The dereferences at 313/317 are safe because they sit
               inside if (networkMode), where the bind resolution has already proven a block exists; this
               one runs unconditionally, so a bare .IsConfigured throws on every start of an un-exposed
               server, gets swallowed by the catch below, and retry-fails forever because the config never
               changes. Review catch on #2479. */
            _logger.LogInformation(
                "{Report}",
                DarlingHostBinding.DescribeNetworkBlockLifetime(
                    "mcp", "MCP", config.Mcp.Network?.IsConfigured ?? false, networkMode,
                    networkMode ? primaryBind.ToString() : null,
                    networkMode ? allowedCidr.ToString() : null));

            /* StartAsync, not RunAsync (#1560): the supervisor loop owns the wait — the app keeps
               serving until StopServerAsync (toggle-off, port change, or shutdown). */
            await _app.StartAsync(stoppingToken);
            _runningPort = effectivePort;
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Normal shutdown mid-start, still releasing anything already acquired (#5288: the certificate's
               private key is held in the machine key store until it is disposed). */
            await DisposeFailedStartAsync();
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError("MCP server failed to start: {Message}", ex.Message);
            await DisposeFailedStartAsync();
            return false;
        }
    }

    /// <summary>
    /// The Kestrel listener layout for one start (#5288), extracted from <c>TryStartServerAsync</c> so a live-HTTP
    /// test binds the SAME listeners production does instead of a hand-copied set that could drift. In network mode
    /// the network listener binds <paramref name="primaryBind"/> and, when <paramref name="certificate"/> is not
    /// null, is the ONE listener that serves HTTPS; both loopback families are added beside it as PLAIN HTTP unless
    /// the listen is itself loopback or a wildcard (<see cref="ShouldAddLoopbackListeners"/>), which would collide
    /// on the port. In loopback-only mode there is one plain server on both families.
    ///
    /// <para><b>Why loopback stays plain HTTP.</b> The certificate names the LAN address the operator exposes; it
    /// almost never also names <c>localhost</c>, so serving TLS there would hand every local client a name-mismatch
    /// failure on the one surface that never leaves the machine, and nothing is lost: loopback traffic is not on the
    /// segment this protects. When the listen IS a wildcard there is a single listener and it is HTTPS for
    /// everyone, loopback included, because the operator asked for all interfaces. A client that speaks plain HTTP
    /// to the TLS listener fails at the handshake: one port cannot speak both schemes, and a second HTTP port to
    /// redirect from would re-open, on a new port, the cleartext surface this exists to close.</para>
    ///
    /// <para>There is exactly ONE HTTPS call in this file, on the network listener, handing Kestrel the leaf and its
    /// intermediates through the body both hosts share (<see cref="DarlingListenerTls.ConfigureHttps"/>). The
    /// caller owns the certificate and disposes it after the listener stops.</para>
    /// </summary>
    internal static void ConfigureListeners(
        KestrelServerOptions options,
        bool networkMode,
        IPAddress primaryBind,
        int effectivePort,
        DarlingWebTls.LoadedCertificate? certificate)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(primaryBind);

        if (networkMode)
        {
            /* Bind the specific family (not ListenAnyIP), then ALSO both loopback families so a local
               client resolving "localhost" -> ::1 still works — skipping the loopback Listen(s) when the
               listen value is itself loopback or a wildcard (0.0.0.0/::), which would collide on the port. */
            options.Listen(primaryBind, effectivePort, listen =>
            {
                if (certificate is not null)
                {
                    /* The leaf AND its intermediates, through the body both hosts share: Kestrel presents ONLY
                       what it is handed, so see DarlingListenerTls.ConfigureHttps for why the chain travels with
                       the leaf. This is the one HTTPS call in the file, and it sits on the network listener alone. */
                    listen.UseHttps(https => DarlingListenerTls.ConfigureHttps(https, certificate.Value));
                }
            });

            if (ShouldAddLoopbackListeners(primaryBind))
            {
                options.Listen(IPAddress.Loopback, effectivePort);
                options.Listen(IPAddress.IPv6Loopback, effectivePort);
            }
        }
        else
        {
            /* The default/degraded loopback-only server — byte-for-byte today's bind (both families), plain HTTP. */
            options.ListenLocalhost(effectivePort);
        }
    }

    /// <summary>
    /// The start line for the LAN-exposed listener (#2389, #5288 review F11), as text so a test can render it. It
    /// names the SCHEME the listener actually speaks, because an operator reading it is deciding whether the token
    /// they are about to paste crosses the wire in the clear, and it names loopback as plain HTTP only when it is:
    /// with TLS on and a wildcard listen there is ONE listener, HTTPS for loopback too, and saying otherwise would
    /// send a local client to the wrong scheme (<see cref="DarlingListenerTls.DescribeLoopbackListener"/>). With no
    /// TLS the text is exactly the line this host has always logged. PURE; text only, no listener decision moves here.
    /// </summary>
    /// <param name="tlsServed">Whether the network listener was handed a certificate.</param>
    /// <param name="primaryBind">The address the network listener binds.</param>
    /// <param name="port">The port both listeners serve.</param>
    /// <param name="allowedCidr">The in-app allowFrom list.</param>
    /// <param name="origin">Which plane supplied the enabled/port halves (<c>DescribeToggleOrigin</c>).</param>
    internal static string DescribeNetworkStart(
        bool tlsServed, IPAddress primaryBind, int port, CidrAllowList allowedCidr, string origin)
    {
        ArgumentNullException.ThrowIfNull(primaryBind);
        ArgumentNullException.ThrowIfNull(origin);

        var scheme = tlsServed ? "https" : "http";
        var fileFields = tlsServed ? "listen/allowFrom/token/tls" : "listen/allowFrom/token";
        var loopback = DarlingListenerTls.DescribeLoopbackListener(
            tlsServed ? "loopback also bound over plain HTTP" : "loopback also bound", tlsServed, primaryBind);

        return $"Starting MCP server on {scheme}://{primaryBind}:{port} "
            + $"(LAN-exposed to {allowedCidr.ToString()} behind a bearer token + in-app CIDR; {loopback}) — "
            + $"enabled/port from {origin}; {fileFields} from darling.json mcp.network (file-only, restart-only)";
    }

    /// <summary>
    /// #4726: registers the analysis service TRANSIENT, so every MCP call gets its own instance, the way the worker
    /// builds one per pass. A single shared instance answers a second, overlapping analyze_server call with an empty
    /// list (its busy check) and the tool then reads the FIRST call's running state, so the second server got
    /// "No significant findings" for a server it never analyzed. The ONE shared <paramref name="baselineCache"/> is
    /// still handed to every instance (#3941), so baselines are not recomputed per call. Extracted so a test resolves
    /// the service from the production registration instead of a hand-copied one that could drift from it.
    /// </summary>
    internal static void RegisterAnalysisService(
        IServiceCollection services,
        NpgsqlDataSource postgres,
        PerformanceMonitor.Analysis.IPlanFetcher? planFetcher,
        ILogger? logger,
        BaselineCache baselineCache,
        AnalyzerConfig? analyzer,
        MonitoredServerRegistryState? registryState = null)
    {
        services.AddTransient<DarlingAnalysisService>(_ => new DarlingAnalysisService(postgres, planFetcher, logger, baselineCache, analyzer ?? AnalyzerConfig.Default)
        {
            /* Resolved per call from the live registry, the way the worker fills its per-pass instance, so an
               analyze_server run (which persists) agrees with the scheduled pass for an Azure master target. */
            SeparatelyMonitoredResolver = registryState is null
                ? null
                : (serverId, ct) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(serverId, registryState.Read(), postgres, ct)
        });
    }

    /// <summary>
    /// Registers the MCP server, its stateless HTTP transport, every tool class, and the two call-tool
    /// filters — the same registration <c>TryStartServerAsync</c> used to build inline. Extracted (#4128)
    /// so a live-HTTP test builds the SAME server (including <c>/core</c>'s <c>ConfigureSessionOptions</c>
    /// narrowing and its tools/list) against a test-constructed <see cref="IServiceCollection"/>, instead of
    /// a hand-copied second registration that could silently drift from production. The production call
    /// site passes exactly this <paramref name="declaredPeers"/> snapshot — see <c>TryStartServerAsync</c>.
    /// Every line below is identical to before the extraction; only the receiver (<c>builder.Services</c>
    /// there vs. the parameter here) changes.
    /// </summary>
    internal static void ConfigureMcpServices(IServiceCollection services, DarlingPeerDirectory.Snapshot declaredPeers, ReadLatencyAccumulator? readLatency = null, ILogger? readLatencyLogger = null, SlowReadLog? slowReads = null)
    {
        /* #4442 scope 2: the per-tool latency filter needs the SAME accumulator singleton the web host and
           worker flush use (Program.cs registers ONE ReadLatencyAccumulator for the whole process) -- an
           optional parameter, defaulted to a private instance, so every existing test-constructed call site
           (which passes none) keeps building and running with its own throwaway accumulator instead of a
           null-reference. Production's one real call site (TryStartServerAsync) resolves the DI singleton and
           passes it here explicitly. */
        var hostReadLatency = readLatency ?? new ReadLatencyAccumulator();
        var toolLatency = new McpToolLatencyFilter(hostReadLatency, readLatencyLogger, slowReads);

        /* #4782: run_custom_view_panel records its composed-panel run through the shared runner, which used to
           find the accumulator in a process-wide static that only the web host set -- so the run was dropped
           whenever the web host was off, and was tied to whichever web server was mapped last when several
           were set up in one process. The tool now takes this seat as a DI service parameter, over the SAME
           accumulator the filter above records into. Typed-generic AddSingleton<T>, as the seat census
           (McpServiceParameterDiSeatCensusTests) greps this file's source text for it. */
        services.AddSingleton<ReadLatencyRecorder>(new ReadLatencyRecorder(hostReadLatency, readLatencyLogger, slowReads));

        services
            .AddMcpServer(options =>
            {
                options.ServerInfo = new()
                {
                    Name = "PerformanceMonitorDarling",
                    Version = "1.0.0"
                };
                options.ServerInstructions = DarlingMcpInstructions.Build(declaredPeers);
            })
            /* Stateless mode: each request is self-contained (no Mcp-Session-Id round-trip).
               Required for clients like Google Antigravity that don't echo the session id,
               which otherwise connect but list zero tools (issue #1074).

               ConfigureSessionOptions is the /core profile's ONLY hook (#3898 D7): in Stateless mode the
               SDK invokes it on every request with that request's HttpContext, after ToolCollection is
               already populated from every tool class registered below. A /core request gets
               ToolCollection replaced with DarlingCoreToolProfile's closure; every other path (in
               practice, just /) is untouched and keeps the full set. This is a REAL subset, not a listing
               filter: the SDK's tools/call dispatch checks the same ToolCollection before falling back to
               any handler, and Darling registers no fallback CallToolHandler, so a tools/call for a tool
               outside the closure gets the SDK's own "unknown tool" error on /core. Security posture is
               inherited for free: this callback runs from inside the MCP transport, AFTER every _app.Use
               middleware below (Host-header guard, bearer token, CIDR) — a /core request is refused there
               exactly as a / request would be, before this callback, or MapMcp, ever runs.
               The subset holds only in Stateless mode, where this callback runs on every request. A stateful
               session is looked up by its id alone, not by route, so one opened on / could call any tool on
               /core. HostHeaderGuardTests pins Stateless while /core is mapped. The instructions change too:
               the shared text counts and describes every tool on /, so /core leads with a note saying what
               it serves. */
            .WithHttpTransport(options =>
            {
                options.Stateless = true;
                options.ConfigureSessionOptions = (context, sessionOptions, _) =>
                {
                    if (DarlingCoreToolProfile.IsCorePath(context.Request.Path))
                    {
                        sessionOptions.ToolCollection = DarlingCoreToolProfile.FilterToolCollection(sessionOptions.ToolCollection);
                        sessionOptions.ServerInstructions = DarlingCoreToolProfile.CoreInstructions(sessionOptions.ServerInstructions);
                    }

                    return Task.CompletedTask;
                };
            })
            /* WithGeminiCompatibleTools (not the SDK's WithTools) rewrites parameter schemas into
               the subset Gemini/Antigravity accepts — collapsing nullable type unions and
               dropping the default keyword. The companion to stateless transport for issue #1074. */
            .WithGeminiCompatibleTools<DarlingMcpTools>()
            /* The five plan-analysis tools (analyze_query_plan / analyze_procedure_plan /
               analyze_query_store_plan / analyze_plan_xml / get_plan_xml) — the same names the
               Dashboard and Lite expose, fetching the collectors' STORED plan XML from Postgres
               (no live monitored-server hit) and running the SHARED PlanAnalysis engine — plus the
               three raw-plan reads behind the web grids' plan buttons (#5228: get_query_store_plan_xml /
               get_procedure_plan_xml / get_active_query_plan_xml), which are Darling-only. */
            .WithGeminiCompatibleTools<DarlingMcpPlanTools>()
            /* The core data-read tools (resource metrics, query performance, discovery/health —
               get_cpu_utilization / get_wait_stats / get_wait_trend / get_memory_stats /
               get_memory_clerks / get_file_io_stats / get_tempdb_trend / get_perfmon_stats /
               get_top_queries_by_cpu / get_top_procedures_by_cpu / get_query_store_top /
               list_servers / get_collection_health / get_server_properties), the same names Lite
               and the Dashboard expose, over Darling's Postgres store (STORED reads, no live hit).
               These are the tools the analysis findings' next_tools recommendations point at. */
            .WithGeminiCompatibleTools<DarlingMcpDataTools>()
            /* get_query_store_regressions (#2484) — the viewer's Query Store Regressions tab. Every
               other Query Store read answers what is EXPENSIVE; this answers what got WORSE, which is
               not derivable from the first (the costliest query is usually the one that always was).
               A STORED read over the same query_store_stats the tools above read. */
            .WithGeminiCompatibleTools<DarlingMcpQueryStoreRegressionTools>()
            /* get_query_store_clutter (#3797) — the Query Store CLUTTER view: per-database read cost
               (the collection_log fan-out rollup), plan churn (raw query_store_stats plan identities) and
               configuration (query_store_health), plus ONE per-server overhead block (the non-sleep QDS_*
               wait deltas and MEMORYCLERK_QUERYDISKSTORE). Composed from rows the collectors already
               write — not a new query against the target. Darling-only for now: every input exists on
               Lite too, so the twin is a port, not a SKU boundary (CrossAppMcpToolInventoryPinTests). */
            .WithGeminiCompatibleTools<DarlingMcpQueryStoreClutterTools>()
            /* get_query_heatmap (#2484) — the viewer's Query Heatmap tab. The interactive plot is
               desktop-only by design; the READ behind it is not, and a bucketed table is the same
               answer. It is the only query read with a TIME axis: the rankings above cannot show that
               a window had a quiet half and a bad half. A STORED read over the same query_stats. */
            .WithGeminiCompatibleTools<DarlingMcpQueryHeatmapTools>()
            /* The diagnostic-depth data-read tools (blocking/deadlocks, sessions, config-history,
               index/object) — get_blocking / get_deadlocks / get_deadlock_detail /
               get_blocked_process_xml, get_session_stats / get_active_queries / get_waiting_tasks,
               get_server_config_changes / get_database_config_changes / get_trace_flag_changes /
               get_database_scoped_config, get_table_index_sizes / get_index_usage / get_object_locking /
               get_database_sizes — the same names Lite and the Dashboard expose, over Darling's Postgres
               store (STORED reads, no live hit). Result shapes follow Lite where the two SKUs diverge. */
            .WithGeminiCompatibleTools<DarlingMcpBlockingTools>()
            /* #2028 get_plan_corrections — automatic plan correction activity + per-database
               FORCE_LAST_GOOD_PLAN enablement, the one collected table that previously had no
               agent-readable path at all. Twin registered in Lite's host. */
            .WithGeminiCompatibleTools<DarlingMcpPlanCorrectionTools>()
            /* #2029 get_pvs_stats — the ADR persistent version store, previously reachable only
               indirectly (alert knobs + compose measures). Twin registered in Lite's host. */
            .WithGeminiCompatibleTools<DarlingMcpPvsTools>()
            /* #2068 get_store_metrics — the monitoring store's OWN hourly self-metrics series
               (per-hypertable size/compression, payload-dimension sizes + row counts, whole-store
               size + enabled-server count) for capacity forecasting. Darling-only: a single-server
               edition has no central store to measure, so no Lite twin. */
            .WithGeminiCompatibleTools<DarlingMcpStoreMetricsTools>()
            /* #3021 get_store_log — the store's OWN server-log census, the second self-monitoring
               surface beside get_store_metrics. */
            .WithGeminiCompatibleTools<DarlingMcpStoreLogTools>()
            .WithGeminiCompatibleTools<DarlingMcpReadLatencyTools>()
            .WithGeminiCompatibleTools<DarlingMcpSlowReadTools>()
            /* #4214 part 2 get_store_host — the store HOST's profile (platform/RAM/data volume, store
               facts, per-setting verdicts), the read side of part 1's --check-settings verb. Darling-only:
               Lite has no managed PostgreSQL store to profile. */
            .WithGeminiCompatibleTools<DarlingMcpStoreHostTools>()
            .WithGeminiCompatibleTools<DarlingMcpStoreQueryStatsTools>()
            .WithGeminiCompatibleTools<DarlingMcpStoreQueryHistoryTools>()
            .WithGeminiCompatibleTools<DarlingMcpCollectorCostTools>()
            /* #2880 get_collector_stall_probes - the out-of-band server-wide wait samples taken
               while one of OUR collectors was stalled mid-read. Darling-only: the arm is installed by
               DarlingCollectorRunner's server-scoped path, which Lite's runner does not have. */
            .WithGeminiCompatibleTools<DarlingMcpStallProbeTools>()
            /* #3398 get_oversized_plan_backlog - the V121 worklist of cached plans the capture cap
               declined, and what the out-of-band sweep has done about each one. Darling-only: the
               sweep is a fleet-level errand on the headless worker's own cadence, which Lite's
               single-instance runner has no counterpart of. */
            .WithGeminiCompatibleTools<DarlingMcpOversizedPlanBacklogTools>()
            /* #1496 get_long_query_completions — the opt-in long-query completion trace (rpc/batch over
               the duration threshold + attentions), over Darling's Postgres store (STORED read). */
            .WithGeminiCompatibleTools<DarlingMcpLongQueryTools>()
            .WithGeminiCompatibleTools<DarlingMcpSessionTools>()
            .WithGeminiCompatibleTools<DarlingMcpConfigHistoryTools>()
            .WithGeminiCompatibleTools<DarlingMcpObjectStatsTools>()
            /* The resource-contention + jobs data-read tools — get_latch_stats / get_spinlock_stats,
               get_resource_semaphore / get_memory_grants, get_plan_cache_bloat / get_cpu_scheduler_pressure,
               get_running_jobs — the same names Lite and the Dashboard expose, over Darling's Postgres store
               (STORED reads of the collected latch/spinlock/memory-grant/plan-cache/cpu-scheduler/running-job
               snapshots, no live hit). The Dashboard-only CASE enrichment (latch severity/description/
               recommendation, spinlock description) and the #1410 client-side classifications (plan-cache
               bloat_level, cpu-scheduler pressure_level) are reproduced service-side so the full result shape
               is served. Per-second rates divide by each row's stored sample_interval_seconds (V127, #3540),
               falling back to the LAG interval only for pre-V127 rows, and are null when the latest interval
               was unknowable rather than 0. */
            .WithGeminiCompatibleTools<DarlingMcpLatchSpinlockTools>()
            /* get_pg_wait_stats — PostgreSQL wait events for an Aurora target, paired with the
               pg_wait_stats collector. A separate tool from get_wait_stats rather than a widened
               one: PostgreSQL's waits are a two-level type/event taxonomy with no signal-wait
               concept, reported in microseconds, so the two engines cannot share a result shape
               without lying about a unit or emitting mostly-null columns. */
            .WithGeminiCompatibleTools<DarlingMcpPgWaitTools>()
            /* get_pg_cpu_utilization — instance-level CPU for a PostgreSQL/Aurora target (#2719),
               paired with the pg_cpu_utilization collector. Sourced from AWS Performance Insights
               rather than a database connection, so it sits beside the wait tools rather than the
               activity ones: a gauge over time, like SQL Server's own CPU read, not a ranked list. */
            .WithGeminiCompatibleTools<DarlingMcpPgCpuUtilizationTools>()
            /* get_pg_top_queries — PostgreSQL query shapes by total time, paired with the
               pg_statement_stats collector. Carries Aurora's I/O source split and per-statement
               peak memory, neither of which the SQL Server tools have an equivalent for. */
            .WithGeminiCompatibleTools<DarlingMcpPgStatementTools>()
            /* get_pg_plans — the plan itself, not a pointer to one (#2567). Registered beside the
               statement tools because that is the join: a plan is read alongside the statement it
               belongs to, on query_id. Carries get_pg_plan_capture_readiness too (#3070): whether the
               target can capture a plan at all, facet by facet with the remedy for each, which is the
               read somebody needs the moment the plans one comes back empty. */
            .WithGeminiCompatibleTools<DarlingMcpPgPlanTools>()
            /* get_pg_logging_audit (#3607) - the rest of the logging surface, in readiness's shape:
               log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints,
               log_connections / log_disconnections and log_min_duration_statement, each judged from
               the stored pg_server_config snapshot with what it unlocks, the recommended value and its
               cost, and the remedy in the hosting flavour's syntax. Registered beside the plan tools
               because it is the other half of one onboarding question - is this target telling us
               everything it could - and lists plan capture's own settings with a pointer to the
               readiness read rather than judging them twice. */
            .WithGeminiCompatibleTools<DarlingMcpPgLoggingAuditTools>()
            /* get_pg_wraparound_risk — XID/MultiXact freeze headroom, the highest-consequence
               PostgreSQL signal and one with no SQL Server counterpart. Not Aurora-gated. */
            .WithGeminiCompatibleTools<DarlingMcpPgWraparoundTools>()
            /* get_pg_xmin_horizon — why vacuum reclaims nothing, attributed to one of four causes
               that are indistinguishable by symptom and need different fixes. */
            .WithGeminiCompatibleTools<DarlingMcpPgXminTools>()
            /* get_pg_replication_slots — the other half of the abandoned-slot story. The xmin tool
               reports a slot pinning the horizon; this one reports the WAL it is retaining, which is
               unbounded by default and fills the volume regardless of what vacuum is doing. */
            .WithGeminiCompatibleTools<DarlingMcpPgSlotTools>()
            /* get_pg_autovacuum_health — which tables autovacuum is not keeping up with, ranked by
               how far past each table's OWN threshold it is. The ratio is the whole tool: a
               dead-tuple count is not comparable between a 50-million-row table and a 10,000-row
               one, and the threshold is what makes it so. */
            .WithGeminiCompatibleTools<DarlingMcpPgAutovacuumTools>()
            /* get_pg_io_stats — I/O attributed to who/what/why rather than to a file. The context
               dimension has no SQL Server counterpart and is what separates a buffer-pool miss that
               more memory would fix from a ring-buffered sequential scan that it would not. */
            .WithGeminiCompatibleTools<DarlingMcpPgIoTools>()
            /* get_pg_blocking — who is blocked by whom, assembled from the stored edge list into chains
               with the ROOT attributed. The one PostgreSQL read whose caveat has to travel WITH the
               answer: SQL Server's blocked-process report is engine-recorded, this is periodically
               sampled, so "no blocking" here means "none was sampled" and the tool reports its own
               capture count so that distinction cannot be lost. */
            .WithGeminiCompatibleTools<DarlingMcpPgBlockingTools>()
            /* get_pg_database_stats — four questions off one cluster-wide view: temp-file spills (the
               PostgreSQL answer to "why is this query slow" that no other read here can give on a stock
               target), the buffer-cache hit ratio, a server-recorded deadlock count, and the
               commit/rollback split. The one read whose reset handling is part of its contract: a
               statistics reset is reported as a reset rather than surfacing as a negative rate. */
            .WithGeminiCompatibleTools<DarlingMcpPgDatabaseTools>()
            /* get_pg_index_usage — per-index scan counts with the catalog facts that decide whether an
               index can actually go. The half that is not in pg_stat_user_indexes is the point: a
               unique index backing a constraint enforces it without ever registering a scan, so advice
               derived from the counter alone tells somebody to drop their primary key. */
            .WithGeminiCompatibleTools<DarlingMcpPgIndexUsageTools>()
            /* get_pg_table_bloat — the damage the vacuum reads above measure the cause of. The only
               read here whose headline number is an ESTIMATE, and the one whose contract is that it
               suppresses that number rather than captioning it when its inputs cannot be trusted. */
            .WithGeminiCompatibleTools<DarlingMcpPgTableBloatTools>()
            /* get_pg_session_states — the session side of the xmin horizon, and the one read here whose
               job includes REFUSING a causal claim. get_pg_xmin_horizon says a session is holding the
               horizon; this says which one, and — measured on a live instance — says when an
               idle-in-transaction session that looks identical is holding nothing at all, because a
               READ COMMITTED transaction that only read has already released its snapshot. */
            .WithGeminiCompatibleTools<DarlingMcpPgSessionStatesTools>()
            /* #2659: these six shipped REGISTERED NOWHERE. They were implemented, documented, dispatched
               by the web API and counted in the instructions census, and an agent could not call one of
               them — the web dashboard could, which is why it went unnoticed. Registration here is
               per-class and explicit, with no assembly scan, so a tools class is reachable only if
               someone remembers this line and nothing failed when they did not.
               McpToolTypeRegistrationTests now derives the check by reflection instead of trusting it. */
            .WithGeminiCompatibleTools<DarlingMcpPgServerStateTools>()
            .WithGeminiCompatibleTools<DarlingMcpPgIndexTools>()
            .WithGeminiCompatibleTools<DarlingMcpPgKernelStatsTools>()
            .WithGeminiCompatibleTools<DarlingMcpPgPredicateTools>()
            .WithGeminiCompatibleTools<DarlingMcpPgReplicationStatsTools>()
            .WithGeminiCompatibleTools<DarlingMcpPgWaitSamplingTools>()
            /* get_pg_deadlocks / get_pg_deadlock_detail (#2661) - the reports themselves, out of the
               server log, rather than pg_stat_database's count. */
            .WithGeminiCompatibleTools<DarlingMcpPgDeadlockTools>()
            /* get_pg_log_events (#3601) - the classified log-event pipeline's read: errors, connections,
               lock waits and the recognised-only families, out of the same server log. */
            .WithGeminiCompatibleTools<DarlingMcpPgLogEventTools>()
            /* get_pg_wait_trend / get_pg_query_duration_trend / get_pg_io_trend /
               get_pg_database_trend (#2663) - the PostgreSQL time series. Fourteen trend reads shipped
               and none worked on this engine. All four live on one tools class, so this line covers
               the later two as well - which is the only reason adding them needed no edit here. */
            .WithGeminiCompatibleTools<DarlingMcpPgTrendTools>()
            .WithGeminiCompatibleTools<DarlingMcpMemoryGrantTools>()
            .WithGeminiCompatibleTools<DarlingMcpPlanCacheSchedulerTools>()
            .WithGeminiCompatibleTools<DarlingMcpJobTools>()
            /* The windowed-trend siblings of the core data-read tools — get_memory_trend /
               get_perfmon_trend / get_file_io_trend / get_query_trend / get_query_duration_trend — the
               same names Lite and the Dashboard expose, over Darling's Postgres store (STORED reads of
               the collected memory / perfmon / file-io / query-stats series, no live hit). Each mirrors
               the viewer's proven chart read; the shape follows Lite where the SKUs diverge. */
            .WithGeminiCompatibleTools<DarlingMcpTrendTools>()
            .WithGeminiCompatibleTools<DarlingMcpServerTrendTools>()
            /* The fleet-triage quick-win reads the fleet edition previously lacked — the alerts family
               (get_alert_history over config_alert_log, get_alert_settings over config_alert_settings,
               get_mute_rules via the service-side PgMuteRuleStore), the CURRENT-config snapshot trio
               (get_server_config / get_database_config / get_trace_flags — latest capture, the companion to
               the *_changes diff tools), and the health overview (get_server_summary + the daily rollup
               get_daily_summary and its #2484 range sibling get_daily_summary_range — the Performance
               Calendar's month grid — both folded through the shared DailyHealthBandCalculator). Same
               names Lite and the Dashboard expose, all STORED reads over Darling's Postgres store (no live
               hit). The blocking-trend / deadlock-trend / lock-wait-trend, memory-pressure-event, and
               wait-type siblings ride along on the existing blocking / memory-grant / core data-read
               classes above. */
            .WithGeminiCompatibleTools<DarlingMcpAlertTools>()
            .WithGeminiCompatibleTools<DarlingMcpConfigTools>()
            .WithGeminiCompatibleTools<DarlingMcpHealthTools>()
            /* The cross-server fleet overview — get_fleet_overview (#1562) — the roll-up only the central
               store can serve, over the SHARED DarlingFleetReader that also powers the web /api/fleet and the
               WPF viewer's Overview (one reader, one banding). ADDITIVE alongside get_server_summary. */
            .WithGeminiCompatibleTools<DarlingMcpFleetTools>()
            /* The Availability Group topology — get_ag_health (#991) — every monitored server's view of the
               AGs it hosts, replicas plus per-database secondary state, over the SHARED DarlingAgReader that
               also powers the web /api/ag and the Availability Groups page (one reader, one banding). Like
               get_fleet_overview this is a cross-server read the central store makes possible. */
            .WithGeminiCompatibleTools<DarlingMcpAgTools>()
            /* The fleet sweep reports read — get_sweep_reports (#3466) — the third cross-server read, and
               the one WITH MEMORY: the sweep timeline for a window, the newest sweep in full (mute header,
               would-have-paged ledger, instrument liveness), and the watch-item worklist, over the SAME
               FleetSweepStore presentation reads and FleetSweepPresentation builders the web /api/sweeps
               routes serve — one reader, one shape, the zero-drift rule get_fleet_overview and /api/fleet
               established, applied to the sweep rows. */
            .WithGeminiCompatibleTools<DarlingMcpFleetSweepTools>()
            /* The system_health parse-on-read family — get_health_parser_cpu_tasks / _io_issues /
               _memory_broker / _memory_conditions / _memory_node_oom / _scheduler_issues /
               _severe_errors / _significant_waits / _system_health — the same names the Dashboard
               exposes. Where the Dashboard reads its server-side-parsed collect.HealthParser_*
               tables, these shred the raw
               system_health_events on read via the shared SystemHealthParser (Common) and gate with the
               service-side twin of the viewer's SystemEventSignificance, exactly as the viewer's System
               Events tab does — the same SIGNIFICANT warning set, no live hit. */
            .WithGeminiCompatibleTools<DarlingMcpHealthParserTools>()
            /* The Default Trace tool — get_default_trace_events — the same name the Dashboard exposes.
               Reads Darling's collected default_trace_events (the base table, no v_* view — like
               server_properties) and returns the SIGNIFICANT set via the shared
               DefaultTraceEventSignificance, the same significant-set gate the viewer's System Events
               surface uses; config-change events are excluded (the config-snapshot diff tools own them). */
            .WithGeminiCompatibleTools<DarlingMcpDefaultTraceTools>()
            /* The Custom Views v2 MANAGEMENT tools (#1563) — the one WRITE surface on this server:
               list_custom_views / get_custom_view / validate_custom_view / create_custom_view /
               update_custom_view / delete_custom_view / run_custom_view_panel. They CRUD the user-authored
               views in config.custom_views through the SAME CustomViewStore + ValidateDefinition + compose
               runner the web viewer's editor uses (no divergent second impl), and run back a composed panel's
               data for a self-test loop. The mcp role carries the narrow INSERT/UPDATE/DELETE grant on ONLY
               config.custom_views (mirroring viewer's) — never the config pivot or the secret columns. */
            .WithGeminiCompatibleTools<DarlingMcpCustomViewTools>()
            /* The custom-alert-rule MANAGEMENT tools (#3285) - the second WRITE surface: list_custom_alert_rules
               / get_custom_alert_rule / validate_custom_alert_rule / create_custom_alert_rule /
               update_custom_alert_rule / delete_custom_alert_rule. They CRUD the user-authored threshold-alert
               rules in config.custom_alert_rules through the SAME CustomAlertRuleStore the web editor uses and the
               SAME CustomAlertRuleDefinition.TryParse the CustomAlertEvaluator applies when it loads a rule (no
               divergent second impl), validating every definition before it stores. The mcp role carries the
               narrow INSERT/UPDATE/DELETE grant on ONLY config.custom_alert_rules (granted under V116), never the
               config pivot or the secret columns. */
            .WithGeminiCompatibleTools<DarlingMcpCustomAlertTools>()
            /* The fleet server-tag WRITE tools (#5085) - create_server_tag / update_server_tag / delete_server_tag /
               assign_server_tag / unassign_server_tag. They write config.server_tags and config.server_tag_map
               through the SAME ServerTagStore the Viewer uses, with the depth, cycle, duplicate, name and colour
               rules in ServerTagRules, and every result names the custom alert rules the change affects. The mcp
               role carries the narrow INSERT/UPDATE/DELETE grant on ONLY those two tables (the admin gate), never
               the config pivot or the secret columns. */
            .WithGeminiCompatibleTools<DarlingMcpServerTagTools>()
            /* The server-onboarding WRITE tools — add_servers (BULK) / remove_server: an MCP client can stand up
               or tear down FLEET monitoring conversationally. The service-side twin of the Viewer's Add / Add-
               Multiple dialogs: add_servers validates each entry, probes the connection IN-PROCESS (the service
               holds the network path + credentials, so no test_connect command plane is needed), skips
               case-folded duplicates via the shared ServerIdHelper identity, DPAPI-encrypts the SQL password
               (the service identity, so it round-trips at collection time), and INSERTs config.config_monitored_
               servers mirroring StoreConfigProvider.SeedMonitoredServersAsync; remove_server DELETEs by the same
               resolver the read tools use. The mcp role carries the narrow INSERT/UPDATE/DELETE grant on ONLY
               config.config_monitored_servers (the encrypted_password column stays SELECT-carved) — never the
               config pivot or a schema-wide write. */
            .WithGeminiCompatibleTools<DarlingMcpServerAdminTools>()
            /* #3898 D1: get_tool_guide serves the reading guides tools/list leaves out (the tails split off
               at McpToolGuide.Marker in WithGeminiCompatibleTools) and the cross-tool topics. Lite twin:
               McpToolGuideTools. */
            .WithGeminiCompatibleTools<DarlingMcpToolGuideTools>()
            // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
            .WithGeminiCompatibleTools<DarlingMcpFinOpsInventoryTools>()
            .WithGeminiCompatibleTools<DarlingMcpFinOpsRecommendationsTools>()
            // FinOps web parity (#4843), set A ends.
            // Each set belongs to one series of changes. Append to your own set only,
            // so the two series never edit the same lines of this registration list.
            // Entries keep the registration list's existing order and form.
            // Set A and set B are separated on purpose: keep this gap.
            //
            //
            //
            // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
            .WithGeminiCompatibleTools<DarlingMcpFinOpsTools>()
            // FinOps web parity (#4843), set B ends.
            /* Three call-tool filters, each registered ONCE and each covering every tool with no
               per-tool change — the seam that exists precisely so a decision about all ~147 reads
               is made in one place.

               The unknown-argument guard (#3870) runs FIRST and can refuse before dispatch: a call
               carrying an argument no tool parameter declares is answered with the refusal envelope
               naming the key and listing what the tool accepts, instead of being run with the key
               silently dropped. The SDK binds arguments by name and ignores the rest, so
               get_collection_log with a hallucinated status_filter returned two hundred unfiltered
               rows and nothing anywhere said a knob had been discarded — for a surface whose callers
               are language models, a silently dropped key is a confidently wrong answer. Shared with
               Lite from PerformanceMonitor.Common so both SKUs refuse identically.

               The read-latency filter (#4442 scope 2) times every call and records it into the shared
               ReadLatencyAccumulator under ReadSurface.Mcp — the same seam the guard above uses (a call-tool
               filter, registered once, covering every tool without a per-tool change), chosen because the SDK
               offers exactly this hook (McpServerOptions.AddCallToolFilter) and Darling registers no fallback
               CallToolHandler, so a filter here sees every dispatched call on BOTH / and /core — a /core
               request that never reaches dispatch (a name outside the narrowed ToolCollection) never reaches
               this filter either, which is correct: nothing ran to time. run_custom_view_panel is skipped
               inside the filter (see McpToolLatencyFilter's own doc): it already records one Compose sample
               through RunComposedPanelAsync, so recording it again here would double-count every MCP
               custom-view run.

               Optional GCF (Graph Compact Format) output runs LAST: when DARLING_OUTPUT_FORMAT=gcf
               it re-encodes each tool's JSON result as a GCF generic wire. Opt-in, lossless, and
               never larger than the JSON (see GcfCallToolFilter / GcfOutput). #4198 ruled out a
               post-hoc trim-to-budget filter here (it would make a tool's own truncated/*_returned
               fields wrong, cut calls that explicitly asked for more rows, and drop the newest rows
               of anything sorted oldest-first) in favor of sizing each tool's own defaults to fit —
               see McpResponseBudget.DefaultBytes, which stays as the one shared size target. */
            .WithRequestFilters(filters => filters
                .AddCallToolFilter(McpUnknownArgumentGuard.Instance)
                .AddCallToolFilter(toolLatency.AsFilter())
                .AddCallToolFilter(GcfCallToolFilter.Instance));
    }

    /// <summary>
    /// The bare DNS name <c>mcp.network.hostName</c> stands for, or null when none is set or the value is not one
    /// (#5288). The ONE place the raw value is normalized in this file, so the two things that read it cannot
    /// disagree about which name the operator meant: the Host guard's admitted name (<see cref="ResolveAllowedHostName"/>,
    /// network mode only) and the certificate's name check, which compares the same name against the certificate's
    /// dNSName entries. PURE and silent: the Warning for a value that is set but refused belongs to
    /// <see cref="ResolveAllowedHostName"/> and is written once, so reading the name for the certificate does not
    /// log it a second time.
    /// </summary>
    internal static string? NormalizedHostName(string? configuredHostName)
        => McpNetworkConfig.NormalizeHostName(configuredHostName);

    /// <summary>
    /// The ONE extra Host the guard admits beside the names it always admits (#5288): the normalized
    /// <c>mcp.network.hostName</c>, or null for none. NETWORK mode only (review F2): in loopback-only mode, and in
    /// every mode that degraded to it (an unreadable token, a refused certificate), it returns null even for a
    /// valid name, because that surface is tokenless and the name exists for the network listener's clients. This
    /// deliberately differs from the web host (#4220), which admits <c>web.publicBaseUrl</c>'s host in both
    /// modes: MCP has no link builder, and its loopback surface is tokenless.
    ///
    /// <para>A value that is SET but is not a bare DNS name (<see cref="McpNetworkConfig.NormalizeHostName"/>
    /// returns null for it while the raw value is not blank) logs ONE Warning and admits nothing: fail closed,
    /// never an error and never a reason to degrade the listener. The warning is about the config value, so it is
    /// written in every mode. An unset (blank) value is silent. PURE but for that one log line; it takes the
    /// logger so a test can read the line, and <paramref name="networkMode"/> must be the FINAL mode, after every
    /// degrade, because that is the whole rule.</para>
    /// </summary>
    internal static string? ResolveAllowedHostName(string? configuredHostName, bool networkMode, ILogger logger)
    {
        var hostName = NormalizedHostName(configuredHostName);
        if (hostName is null)
        {
            if (!string.IsNullOrWhiteSpace(configuredHostName))
            {
                logger.LogWarning(
                    "mcp.network.hostName '{HostName}' is not a bare DNS name (no scheme, port, path, wildcard or IP address; "
                    + "a non-ASCII name must be a valid internationalized domain name, and an xn-- label must decode). "
                    + "Ignoring it: the Host-header guard "
                    + "admits no name beyond the ones it always admits. Write the name alone, e.g. mcp.corp.example.",
                    DarlingHttpRefusalLog.Sanitize(configuredHostName));
            }

            return null;
        }

        return networkMode ? hostName : null;
    }

    /// <summary>
    /// Everything AFTER <c>builder.Build()</c>: the Host-allowlist/DNS-rebinding guard (both modes), the
    /// network-mode bearer-token + CIDR middleware, then <c>MapMcp()</c> and <c>MapMcp("/core")</c>.
    /// Extracted (#4128) so a live-HTTP test can build the SAME pipeline against a <c>TestServer</c> instead
    /// of a second, hand-copied one that could silently drift from production. The production call site
    /// passes exactly these values, in exactly this order — see <c>TryStartServerAsync</c>. Instance method,
    /// not static: the gates read <c>_logger</c>, exactly as before the extraction — only the receiver
    /// (this vs. a test-constructed instance) changes.
    ///
    /// <para><paramref name="allowedHostName"/> (#5288) is the ONE extra Host the guard admits beside the names it
    /// always admits: the already-normalized <c>mcp.network.hostName</c>, or null for none. The pipeline admits
    /// whatever it is given, so the caller owns the rule: <see cref="ResolveAllowedHostName"/> returns null in
    /// loopback-only and degraded modes (review F2), and a test builds this pipeline through it for the same
    /// reason it builds it through this method. Optional, so a caller with no name is unchanged.</para>
    /// </summary>
    internal void ConfigurePipeline(
        WebApplication app,
        bool networkMode,
        IPAddress? networkListenIp,
        CidrAllowList allowedCidr,
        string bearerToken,
        string? allowedHostName = null)
    {
        /* #2479 item 5: every gate below used to refuse silently, so "is my token wrong or my CIDR
           wrong" was answerable only from the client, which sees one opaque status code. One log per
           refusal is rate-limited per (gate, source) because this port is LAN-exposed on purpose and
           an exposed port meets a scanner eventually - see DarlingHttpRefusalLog for the shape and
           what it deliberately never writes. Created here, per started server, so a rebind starts
           with a clean budget rather than inheriting the previous listener's scan. */
        var refusals = new DarlingHttpRefusalLog();

        /* #5288: HttpRequest.Host hands the guard the DECODED form of a Host header (HostString.FromUriComponent
           turns a punycode xn-- name into Unicode), so a client that sends xn--bcher-kva.example is seen as the
           Unicode name. The configured name goes through the same conversion before the guard compares it, so
           both sides are in the form the framework gives the middleware; an ASCII name comes out unchanged. A
           malformed punycode label (xn--a) makes the conversion throw ArgumentException; the raw value is kept
           then, so start-up never fails on it. McpNetworkConfig.NormalizeHostName already refuses such a name, so
           the production caller never reaches the catch; this method admits whatever it is given, and the web
           host's web.publicBaseUrl name takes the same block. */
        var admittedHostName = allowedHostName;
        if (allowedHostName is not null)
        {
            try
            {
                admittedHostName = HostString.FromUriComponent(allowedHostName).Host;
            }
            catch (ArgumentException)
            {
                /* Keep the raw value, as above. */
            }
        }

        /* DNS-rebinding guard (#1648) — the FIRST middleware, in BOTH modes, mirroring the web host's
           #1576 fix. The loopback bind is tokenless by design (the network gates below install only in
           network mode), so a browser ON this host that loads attacker content could be rebound to
           127.0.0.1:5152 and reach the MCP surface same-origin — and that surface is no longer read-only
           (custom-view CRUD, add_servers/remove_server, alert-config writes). The application/json content
           type does NOT save us: under a rebind the browser treats the request as same-origin, so no CORS
           preflight applies. Require the Host header to name an address we actually bind — a loopback
           name/IP or, in network mode, the configured listen IP. networkListenIp is null in loopback mode,
           so ONLY loopback Hosts pass there; a rebound foreign hostname is rejected 400 before the bearer
           check, the CIDR check, MapMcp, or any tool handler.

           #5288: allowedHostName admits ONE more exact name (mcp.network.hostName, case-insensitive, no port),
           the standard AllowedHosts pattern: a rebind needs a hostname the ATTACKER chooses, and this admits
           only the one the OPERATOR wrote. The host passes it in network mode only, where the bearer token
           still gates every request, so the name adds no credential-free way in; in loopback mode it is null
           and only loopback names pass, exactly as before. */
        app.Use(async (context, next) =>
        {
            if (!DarlingHostBinding.IsAllowedHost(context.Request.Host.Host, networkListenIp, admittedHostName))
            {
                refusals.Report(
                    _logger, "MCP", DarlingRefusalGate.HostAllowlist, StatusCodes.Status400BadRequest,
                    context.Connection.RemoteIpAddress,
                    $"the Host header '{DarlingHttpRefusalLog.Sanitize(context.Request.Host.Host)}' is not an address this endpoint binds"
                    + " (a loopback name/IP, or mcp.network.listen when LAN-exposed)",
                    DateTime.UtcNow);
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            await next(context);
        });

        /* Access-control middleware — installed ONLY in network mode (Round-4 #6). The default/degraded
           loopback-only server stays byte-for-byte today's tokenless local MCP, so existing local clients
           keep working. Both run BEFORE MapMcp (D3-b: "first ... before any handler/handshake"): the
           unconditional constant-time bearer token FIRST (NO loopback exemption — in exposed mode even a
           local client must present the token; that IS the loopback guard against SSRF/sandboxed sockets),
           then the in-app CIDR check (loopback-exempt so the loopback bind's local clients are not 403'd,
           Round-4 #2 — it bounds WHO can route to the port, independent of the best-effort firewall). */
        if (networkMode)
        {
            var cidr = allowedCidr;
            var token = bearerToken;

            app.Use(async (context, next) =>
            {
                /* Materialized once: StringValues.ToString() allocates, and the refusal path below needs
                   the same header again to tell "no credential" from "wrong credential" (review catch on
                   #2479). IsBearerTokenAuthorized keeps taking the raw header rather than returning what
                   it parsed - its signature is pinned by DarlingMcpHostTests and DarlingHostBindingTests,
                   and threading a result type through it to save one parse on an ALREADY-REFUSED request
                   is not a trade worth making. */
                var authorization = context.Request.Headers.Authorization.ToString();

                if (!IsBearerTokenAuthorized(authorization, token))
                {
                    /* THREE client states, never the token's value. Each one is a different next step
                       for the operator, which is the whole point of logging this at all:

                         no header            -> a client that was never configured with a token
                         header, not a Bearer -> a client configured wrong (Basic, a bare token, an
                                                 empty "Bearer ") - it IS sending something
                         a Bearer that misses -> a token that does not match this endpoint's

                       Review catch on #2479: ExtractBearerToken returns null for the first TWO, so
                       testing only it reported "nothing was presented" about a client that presented
                       a malformed header - collapsing precisely the ambiguity this exists to resolve.
                       None of the three says anything about what the token IS. */
                    refusals.Report(
                        _logger, "MCP", DarlingRefusalGate.Token, StatusCodes.Status401Unauthorized,
                        context.Connection.RemoteIpAddress,
                        DescribeBearerRefusal(authorization),
                        DateTime.UtcNow);
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    context.Response.Headers.WWWAuthenticate = "Bearer";
                    return;
                }

                await next(context);
            });

            app.Use(async (context, next) =>
            {
                if (!IsRemoteAddressAllowed(context.Connection.RemoteIpAddress, cidr))
                {
                    refusals.Report(
                        _logger, "MCP", DarlingRefusalGate.SourceCidr, StatusCodes.Status403Forbidden,
                        context.Connection.RemoteIpAddress,
                        $"its address is outside mcp.network.allowFrom ({cidr})",
                        DateTime.UtcNow);
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return;
                }

                await next(context);
            });
        }

        app.MapMcp();

        /* #3898 D7: /core, the same endpoint narrowed to DarlingCoreToolProfile's closure via
           ConfigureSessionOptions above. Mapped on the SAME _app after the SAME middleware (the Host-header
           guard, and in network mode the bearer-token and CIDR checks) — nothing about this route is
           exempt from any gate above. / is unchanged and keeps serving every tool, for good. */
        app.MapMcp("/core");
    }

    /* ---------------------------------------------------------------------------------------------------
       Pure decision functions (darling-network-endpoints, D3). Factored out of the Kestrel/middleware
       wiring so they are unit-testable without a running server or a logger; the caller maps the reason to
       a log severity and installs the middleware only in network mode.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The effective MCP bind. <see cref="McpBindMode.LoopbackOnly"/> is the secure default;
    /// <see cref="McpBindMode.NetworkAndLoopback"/> binds the LAN interface behind the token + CIDR.</summary>
    internal enum McpBindMode
    {
        LoopbackOnly,
        NetworkAndLoopback,
    }

    /// <summary>WHY the bind resolved as it did — the caller maps this to a severity (Round-4 #7):
    /// <see cref="LoopbackByDefault"/>/<see cref="NetworkExposed"/> are non-degrade (no critical log),
    /// <see cref="TokenMissing"/>/<see cref="AllowFromInvalid"/> are fail-closed degrades (LogCritical),
    /// and <see cref="ManagedModeRequired"/> is the BYO "ignored" notice (LogWarning, D-BYO).</summary>
    internal enum McpBindReason
    {
        /// <summary>No network block, or a loopback/absent listen — the byte-for-byte-today loopback server.</summary>
        LoopbackByDefault,

        /// <summary>All preconditions met: non-loopback listen + managed + token present + valid allowFrom CIDR.</summary>
        NetworkExposed,

        /// <summary>network.* is set but postgres.managed = false — network exposure is managed-mode only (D-BYO warning).</summary>
        ManagedModeRequired,

        /// <summary>Exposed + managed but the listen value is not a parseable IP (localhost/hostname/"*") — fail-closed to loopback (LogCritical).</summary>
        ListenInvalid,

        /// <summary>Exposed + managed but no bearer token — fail-closed to loopback (LogCritical).</summary>
        TokenMissing,

        /// <summary>Exposed + managed + token but allowFrom is missing, is not a valid CIDR list (one CIDR, or CIDRs separated by commas, #5288), or an entry's family does not match the listen — fail-closed to loopback (LogCritical).</summary>
        AllowFromInvalid,
    }

    /// <summary>The (mode, reason) pair returned by <see cref="ResolveMcpBind"/>.</summary>
    internal readonly record struct McpBindDecision(McpBindMode Mode, McpBindReason Reason);

    /// <summary>
    /// The effective MCP bind — a thin adapter over the shared <see cref="DarlingHostBinding.ResolveBind"/>
    /// ladder (darling-network-endpoints anti-drift): projects the MCP network block onto the surface-agnostic
    /// inputs and maps the shared decision back to the nested (Mode, Reason) the MCP host + its tests use. The
    /// LADDER itself (exposed classifier, managed gate, listen/token/allowFrom validation, family match) now
    /// lives ONCE in <see cref="DarlingHostBinding"/>; this method's contract is byte-for-byte what it was.
    /// </summary>
    internal static McpBindDecision ResolveMcpBind(McpConfig mcp, bool managed, bool? inContainer = null)
    {
        var network = mcp.Network;
        var decision = DarlingHostBinding.ResolveBind(
            network?.Listen,
            network?.AllowFrom,
            tokenPresent: network is not null
                && (!string.IsNullOrWhiteSpace(network.EncryptedToken) || !string.IsNullOrWhiteSpace(network.Token)),
            networkConfigured: network is { IsConfigured: true },
            managed: managed,
            /* #1804: tests pass this explicitly; the running host takes the ambient container marker. */
            inContainer: inContainer ?? DarlingHostBinding.IsRunningInContainer);

        /* The nested McpBind* enums mirror DarlingHostBinding's BindMode/BindReason 1:1 (same member order,
           pinned equal by DarlingHostBindingTests), so a numeric cast maps them without a per-value switch. */
        return new McpBindDecision((McpBindMode)(int)decision.Mode, (McpBindReason)(int)decision.Reason);
    }

    /// <summary>
    /// Whether to ALSO bind the two loopback families beside the network listener (D3-e). Skipped when the
    /// listen value is itself a loopback address (already covered) or a wildcard (<c>0.0.0.0</c> covers IPv4
    /// loopback, <c>::</c> the IPv6) — binding an explicit loopback on the same port then would collide
    /// (WSAEADDRINUSE). For a specific LAN IP the loopback binds are added so a local client resolving
    /// "localhost" still reaches the server (which, in network mode, now also requires the token).
    /// </summary>
    internal static bool ShouldAddLoopbackListeners(IPAddress listenIp)
        => DarlingHostBinding.ShouldAddLoopbackListeners(listenIp);

    /// <summary>
    /// PURE in-app CIDR check (D3-c, Round-4 #2): is <paramref name="remoteIp"/> allowed? Loopback
    /// (<c>127.0.0.0/8</c> or <c>::1</c>, incl. an IPv4-mapped-IPv6 form) is ALWAYS allowed — it is not in
    /// <paramref name="allowedCidr"/>, so otherwise the loopback bind's local clients would get 403. Everything
    /// else must fall inside ANY entry of the CIDR list (#5288). A null remote (unverifiable origin) fails closed.
    /// </summary>
    internal static bool IsRemoteAddressAllowed(IPAddress? remoteIp, CidrAllowList allowedCidr)
        => DarlingHostBinding.IsRemoteAddressAllowed(remoteIp, allowedCidr);

    /// <summary>
    /// PURE bearer-token check (D3-b): true only when <paramref name="authorizationHeaderValue"/> carries a
    /// <c>Bearer</c> token that matches <paramref name="expectedToken"/>. The compare is constant-time over
    /// SHA-256 digests, so it leaks neither the token nor its length; empty/missing/mismatch all return false,
    /// and an empty <paramref name="expectedToken"/> never authorizes. Has NO notion of the remote address —
    /// so there is structurally no loopback exemption (the loopback guard).
    /// </summary>
    internal static bool IsBearerTokenAuthorized(string? authorizationHeaderValue, string expectedToken)
    {
        if (string.IsNullOrEmpty(expectedToken))
        {
            return false;
        }

        var presented = ExtractBearerToken(authorizationHeaderValue);
        if (string.IsNullOrEmpty(presented))
        {
            return false;
        }

        /* The constant-time, length-hiding compare lives once in the shared host-binding helper (used by the
           web host's ?token= check too); this method keeps the MCP-specific Bearer-header parsing above it. */
        return DarlingHostBinding.FixedTimeTokenEquals(presented, expectedToken);
    }

    /// <summary>Extracts the token from a <c>Bearer &lt;token&gt;</c> Authorization header (scheme
    /// case-insensitive); null when absent, malformed, or the token part is blank. PURE.</summary>
    internal static string? ExtractBearerToken(string? authorizationHeaderValue)
    {
        if (string.IsNullOrWhiteSpace(authorizationHeaderValue))
        {
            return null;
        }

        const string prefix = "Bearer ";
        var value = authorizationHeaderValue.Trim();
        if (!value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var token = value.Substring(prefix.Length).Trim();
        return string.IsNullOrEmpty(token) ? null : token;
    }

    /// <summary>
    /// PURE: why a bearer check refused, in the operator's terms — three states, not two (#2479).
    ///
    /// <para><see cref="ExtractBearerToken"/> answers null for BOTH "no header" and "a header that is not a
    /// well-formed Bearer", so a refusal line built on it alone tells an operator nothing was presented
    /// while their client is sending <c>Authorization: Basic …</c> every second. Those are different
    /// faults with different fixes — one client has no token configured, the other has it configured
    /// wrong — and telling them apart is the reason this line exists.</para>
    ///
    /// <para>Says nothing about the token's value, and cannot: it reads only the header's SHAPE.</para>
    /// </summary>
    internal static string DescribeBearerRefusal(string? authorizationHeaderValue)
    {
        if (string.IsNullOrWhiteSpace(authorizationHeaderValue))
        {
            return "no 'Authorization: Bearer <token>' header was presented";
        }

        if (ExtractBearerToken(authorizationHeaderValue) is null)
        {
            return "an Authorization header WAS presented but is not a 'Bearer <token>' "
                + "(wrong scheme, or an empty token after 'Bearer')";
        }

        return "the presented bearer token does not match mcp.network.encryptedToken";
    }

    /// <summary>
    /// PURE severity map for a <see cref="ResolveMcpBind"/> reason (Round-4 #7): the fail-closed degrades
    /// (<see cref="McpBindReason.ListenInvalid"/>/<see cref="McpBindReason.TokenMissing"/>/
    /// <see cref="McpBindReason.AllowFromInvalid"/>) are <see cref="LogLevel.Critical"/>, the BYO "ignored"
    /// notice (<see cref="McpBindReason.ManagedModeRequired"/>) is <see cref="LogLevel.Warning"/>, and the
    /// non-degrade reasons (<see cref="McpBindReason.NetworkExposed"/>/<see cref="McpBindReason.LoopbackByDefault"/>)
    /// are silent (null). <see cref="LogBindReason"/> drives its emit level off this, so the level and the
    /// message can never diverge.
    /// </summary>
    internal static LogLevel? MapBindReasonSeverity(McpBindReason reason)
        => DarlingHostBinding.MapBindReasonSeverity((DarlingHostBinding.BindReason)(int)reason);

    /// <summary>Emits the <see cref="ResolveMcpBind"/> reason at its mapped severity (Round-4 #7). Silent for
    /// the non-degrade reasons (the network-exposed line is logged at start with the real bind).</summary>
    private void LogBindReason(McpConfig mcp, McpBindReason reason)
    {
        var level = MapBindReasonSeverity(reason);
        if (level is null)
        {
            /* NetworkExposed is announced at start with the real address; LoopbackByDefault is the silent,
               byte-for-byte-today path. */
            return;
        }

        switch (reason)
        {
            case McpBindReason.ListenInvalid:
                _logger.Log(level.Value,
                    "MCP network exposure requested but mcp.network.listen '{Listen}' is not a valid IP address — " +
                    "refusing to expose; binding loopback-only. Use a specific IP (e.g. 192.168.1.205), or 0.0.0.0 for all interfaces.",
                    mcp.Network?.Listen);
                break;

            case McpBindReason.TokenMissing:
                _logger.Log(level.Value,
                    "MCP network exposure requested (mcp.network.listen is non-loopback) but no bearer token is set — " +
                    "refusing to expose; binding loopback-only. Set mcp.network.encryptedToken (via --encrypt-password) or mcp.network.token.");
                break;

            case McpBindReason.AllowFromInvalid:
                _logger.Log(level.Value,
                    "MCP network exposure requested but mcp.network.allowFrom '{AllowFrom}' is not a valid CIDR list or an " +
                    "entry's address family does not match mcp.network.listen — refusing to expose; binding loopback-only. " +
                    "Use one CIDR (e.g. 192.168.1.0/24) or several, separated by commas or as a JSON array " +
                    "(e.g. 10.8.0.0/16,192.168.1.5/32): every entry in CIDR form (/32 for one address) and of the same " +
                    "family as listen (a :: listen takes IPv6 entries only). Host bits are masked (192.168.1.5/24 means 192.168.1.0/24).",
                    mcp.Network?.AllowFrom);
                break;

            case McpBindReason.ManagedModeRequired:
                _logger.Log(level.Value,
                    "mcp.network.* is set but postgres.managed = false — MCP network exposure is managed-mode (or container, #1804) " +
                    "only and is ignored; your own PostgreSQL/reverse proxy governs uncontained BYO exposure. Binding loopback-only.");
                break;

            default:
                break;
        }
    }

    /* ---------------------------------------------------------------------------------------------------
       MCP firewall VERIFICATION (#1771). This used to reconcile the rule itself, which could never work: the
       service runs as an unprivileged virtual account, so both halves failed "Access is denied" on every
       start of every hardened install, and a fresh networked install got no rule at all. The rule is now
       created by the elevated installer (--configure-firewall); this only reads and reports.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>The scoped MCP firewall rule name (idempotent by DisplayName), port-specific and distinct
    /// from the store's rule so the two endpoints are managed independently. <c>internal</c> so the headless
    /// endpoint-toggle CLI verbs (--enable-mcp/--disable-mcp) and --configure-firewall act on the SAME rule
    /// by DisplayName.</summary>
    internal static string McpFirewallRuleName(int port) => $"PerformanceMonitor Darling MCP (port {port})";

    /// <summary>Last (rule, verdict) this host reported, so a supervisor retry loop restates a steady
    /// firewall state at most once (<see cref="DarlingFirewallCheck.ShouldReport"/>).</summary>
    private string? _lastFirewallRule;
    private FirewallRuleVerdict? _lastFirewallVerdict;

    [SupportedOSPlatform("windows")]
    private async Task CheckMcpFirewallAsync(int port, bool exposed, string? cidr, CancellationToken cancellationToken)
        => (_lastFirewallRule, _lastFirewallVerdict) = await DarlingFirewallCheck.CheckAsync(
            McpFirewallRuleName(port), port, exposed, cidr, _lastFirewallRule, _lastFirewallVerdict, _logger, cancellationToken);

    /// <summary>
    /// Managed mode's first-boot race, handled: the dedicated <c>mcp</c>-role credential appears only after
    /// the worker's initdb + migration + role provisioning finish (LATER than the owner credential), so poll
    /// up to ~5 minutes (60 × 5s) instead of racing it, then stand down with a pointer at the worker log —
    /// fail-closed (MCP just does not start; it self-heals on the next restart once the credential exists).
    /// The 5-minute budget tolerates a cold first boot (unpack + initdb + start + migrate + provision) that
    /// can exceed the owner credential's shorter window (Round-4 #8).
    /// </summary>
    [SupportedOSPlatform("windows")]
    private async Task<string?> WaitForManagedConnectionStringAsync(PostgresConfig config, CancellationToken stoppingToken)
    {
        for (var attempt = 0; attempt < 60; attempt++)
        {
            var connectionString = DarlingManagedPostgres.TryBuildMcpConnectionStringFromStoredCredential(config);
            if (connectionString is not null)
            {
                return connectionString;
            }

            if (attempt == 0)
            {
                _logger.LogInformation("Waiting for the managed Postgres mcp-role credential (first-run initialization) before starting the MCP server");
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

        _logger.LogError("MCP server not started: the managed Postgres mcp-role credential never appeared — see the worker log for the bootstrap failure");
        return null;
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app != null)
        {
            _logger.LogInformation("Stopping MCP server");
            await StopServerAsync(cancellationToken);
        }

        await base.StopAsync(cancellationToken);
    }
}
