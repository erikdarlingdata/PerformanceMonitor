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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;

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
/// <see cref="DarlingConfig.Validate"/> (the worker's abort would not stop this host). No TLS on MCP (a
/// self-signed cert breaks real clients; the named MITM control is a TLS reverse proxy in front of the
/// endpoint) — the token travels cleartext on-segment, so own that residual with the reverse proxy. The
/// scoped, idempotent firewall rule is created by the ELEVATED installer (#1771 — this account cannot);
/// here it is only CHECKED and reported (defense-in-depth; the token + CIDR are the boundary, not the
/// firewall).</para>
///
/// <para>Gated by darling.json's <c>mcp.enabled</c> (default OFF — a headless service should not open a
/// port unless the operator asks); when disabled or when the config cannot load (the worker already logs
/// that as critical), this service stands down without affecting collection. Registered always in
/// Program.cs and self-gating here, because config loading/validation is the worker's job and Program.cs
/// stays config-free.</para>
///
/// <para>The MCP surface gets its OWN <see cref="NpgsqlDataSource"/> over the store, connecting as the
/// dedicated least-privilege <c>mcp</c> role (D3-role) — NOT the superuser owner — so a token-holder (or a
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

    public DarlingMcpHostService(ILogger<DarlingMcpHostService> logger, McpRuntimeState state, MonitoredServerRegistryState registryState)
    {
        _logger = logger;
        _state = state;
        _registryState = registryState;
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
            if (config is null && DateTime.UtcNow - lastFailedStartUtc >= FailedStartBackoff)
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
                case McpSupervisorAction.Start when DateTime.UtcNow - lastFailedStartUtc >= FailedStartBackoff:
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

            /* In network mode ResolveMcpBind has already validated the listen IP, the allowFrom CIDR, AND their
               address-family agreement, so these two parses cannot throw; only resolving the token can still
               fail (a corrupt DPAPI blob), which fail-closes to loopback-only rather than exposing tokenless. */
            IPAddress? networkListenIp = null;
            IPNetwork allowedCidr = default;
            string bearerToken = "";
            if (networkMode)
            {
                networkListenIp = IPAddress.Parse(config.Mcp.Network!.Listen!.Trim());
                allowedCidr = IPNetwork.Parse(config.Mcp.Network.AllowFrom!.Trim());

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

            /* The REAL primary bind address (network IP when exposed, else loopback): both the port precheck
               and the Kestrel bind use it, so the precheck probes the actual address, not always loopback. */
            var primaryBind = networkMode ? networkListenIp! : IPAddress.Loopback;

            /* Port-in-use pre-check — Lite's StartMcpServerAsync guard, via the shared utility, against the
               REAL bind address (D3-e: not always IPAddress.Loopback). Done before the firewall check so a
               bail here reports nothing about the firewall. */
            if (await PortUtilityService.IsTcpPortListeningAsync(effectivePort, primaryBind, stoppingToken))
            {
                _logger.LogError("Port {Port} is already in use — MCP server not started this attempt; will retry", effectivePort);
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

            /* Lifetime tied to the running app (#1560): disposed by StopServerAsync, not this method's
               scope — the supervisor may keep the app running across many poll ticks. */
            var postgres = NpgsqlDataSource.Create(storeConnectionString);
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

            var builder = WebApplication.CreateBuilder();

            builder.WebHost.ConfigureKestrel(options =>
            {
                if (networkMode)
                {
                    /* Bind the specific family (not ListenAnyIP), then ALSO both loopback families so a local
                       client resolving "localhost" -> ::1 still works — skipping the loopback Listen(s) when the
                       listen value is itself loopback or a wildcard (0.0.0.0/::), which would collide on the port. */
                    options.Listen(primaryBind, effectivePort);
                    if (ShouldAddLoopbackListeners(primaryBind))
                    {
                        options.Listen(IPAddress.Loopback, effectivePort);
                        options.Listen(IPAddress.IPv6Loopback, effectivePort);
                    }
                }
                else
                {
                    /* The default/degraded loopback-only server — byte-for-byte today's bind (both families). */
                    options.ListenLocalhost(effectivePort);
                }
            });

            /* Suppress ASP.NET Core console logging — the service's own logger reports lifecycle. */
            builder.Logging.ClearProviders();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);

            /* Register services that MCP tools need via dependency injection. */
            builder.Services.AddSingleton<NpgsqlDataSource>(postgres);
            builder.Services.AddSingleton(new DarlingAnalysisService(postgres, planFetcher, _logger));

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
            builder.Services
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
                   which otherwise connect but list zero tools (issue #1074). */
                .WithHttpTransport(options => options.Stateless = true)
                /* WithGeminiCompatibleTools (not the SDK's WithTools) rewrites parameter schemas into
                   the subset Gemini/Antigravity accepts — collapsing nullable type unions and
                   dropping the default keyword. The companion to stateless transport for issue #1074. */
                .WithGeminiCompatibleTools<DarlingMcpTools>()
                /* The five plan-analysis tools (analyze_query_plan / analyze_procedure_plan /
                   analyze_query_store_plan / analyze_plan_xml / get_plan_xml) — the same names the
                   Dashboard and Lite expose, fetching the collectors' STORED plan XML from Postgres
                   (no live monitored-server hit) and running the SHARED PlanAnalysis engine. */
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
                   is served; Darling's delta collectors store no sample_interval_seconds, so per-second rates are
                   derived from the LAG interval. */
                .WithGeminiCompatibleTools<DarlingMcpLatchSpinlockTools>()
                /* get_pg_wait_stats — PostgreSQL wait events for an Aurora target, paired with the
                   pg_wait_stats collector. A separate tool from get_wait_stats rather than a widened
                   one: PostgreSQL's waits are a two-level type/event taxonomy with no signal-wait
                   concept, reported in microseconds, so the two engines cannot share a result shape
                   without lying about a unit or emitting mostly-null columns. */
                .WithGeminiCompatibleTools<DarlingMcpPgWaitTools>()
                /* get_pg_top_queries — PostgreSQL query shapes by total time, paired with the
                   pg_statement_stats collector. Carries Aurora's I/O source split and per-statement
                   peak memory, neither of which the SQL Server tools have an equivalent for. */
                .WithGeminiCompatibleTools<DarlingMcpPgStatementTools>()
                /* get_pg_plans — the plan itself, not a pointer to one (#2567). Registered beside the
                   statement tools because that is the join: a plan is read alongside the statement it
                   belongs to, on query_id. */
                .WithGeminiCompatibleTools<DarlingMcpPgPlanTools>()
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
                /* get_pg_wait_trend / get_pg_query_duration_trend (#2663) - the first PostgreSQL time
                   series. Fourteen trend reads shipped and none worked on this engine. */
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
                .WithGeminiCompatibleTools<DarlingMcpServerAdminTools>();

            _app = builder.Build();

            /* #2479 item 5: every gate below used to refuse silently, so "is my token wrong or my CIDR
               wrong" was answerable only from the client, which sees one opaque status code. One log per
               refusal is rate-limited per (gate, source) because this port is LAN-exposed on purpose and
               an exposed port meets a scanner eventually - see DarlingHttpRefusalLog for the shape and
               what it deliberately never writes. Created here, per started server, so a rebind starts
               with a clean budget rather than inheriting the previous listener's scan. */
            var refusals = new DarlingHttpRefusalLog();

            /* DNS-rebinding guard (#1648) — the FIRST middleware, in BOTH modes, mirroring the web host's
               #1576 fix. The loopback bind is tokenless by design (the network gates below install only in
               network mode), so a browser ON this host that loads attacker content could be rebound to
               127.0.0.1:5152 and reach the MCP surface same-origin — and that surface is no longer read-only
               (custom-view CRUD, add_servers/remove_server, alert-config writes). The application/json content
               type does NOT save us: under a rebind the browser treats the request as same-origin, so no CORS
               preflight applies. Require the Host header to name an address we actually bind — a loopback
               name/IP or, in network mode, the configured listen IP. networkListenIp is null in loopback mode,
               so ONLY loopback Hosts pass there; a rebound foreign hostname is rejected 400 before the bearer
               check, the CIDR check, MapMcp, or any tool handler. */
            _app.Use(async (context, next) =>
            {
                if (!DarlingHostBinding.IsAllowedHost(context.Request.Host.Host, networkListenIp))
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

                _app.Use(async (context, next) =>
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

                _app.Use(async (context, next) =>
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

            _app.MapMcp();

            /* #2389: name the authority for each half of what is being started. enabled/port come from
               whichever plane the supervisor resolved; listen/allowFrom/token are always darling.json. */
            var origin = DarlingHostBinding.DescribeToggleOrigin(toggle);
            if (networkMode)
            {
                _logger.LogInformation(
                    "Starting MCP server on http://{Listen}:{Port} (LAN-exposed to {Cidr} behind a bearer token + in-app CIDR; loopback also bound) — "
                    + "enabled/port from {Origin}; listen/allowFrom/token from darling.json mcp.network (file-only, restart-only)",
                    primaryBind, effectivePort, allowedCidr, origin);
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
            /* Normal shutdown mid-start. */
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError("MCP server failed to start: {Message}", ex.Message);
            await DisposeFailedStartAsync();
            return false;
        }
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

        /// <summary>Exposed + managed + token but allowFrom is missing/not a valid CIDR or its family does not match the listen — fail-closed to loopback (LogCritical).</summary>
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
    /// else must fall inside the CIDR. A null remote (unverifiable origin) fails closed.
    /// </summary>
    internal static bool IsRemoteAddressAllowed(IPAddress? remoteIp, IPNetwork allowedCidr)
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
                    "MCP network exposure requested but mcp.network.allowFrom '{AllowFrom}' is not a valid CIDR or its " +
                    "address family does not match mcp.network.listen — refusing to expose; binding loopback-only. " +
                    "Use e.g. 192.168.1.0/24 (host bits zeroed, same family as listen).",
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
