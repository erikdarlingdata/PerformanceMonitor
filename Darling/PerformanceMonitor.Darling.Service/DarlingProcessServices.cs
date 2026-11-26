/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The process-wide singletons and the three hosted services Program.cs registers (#5950). Extracted from the
/// top-level statements so a test builds the SAME container production does and resolves from it (the shared
/// baseline cache a host receives is the instance the others receive), instead of searching Program.cs for text.
/// </summary>
internal static class DarlingProcessServices
{
    /// <param name="services">The service collection to register into.</param>
    /// <param name="stoppedMarkerPath">DARLING_STOPPED_MARKER, read once by the caller; null leaves the marker off.</param>
    internal static void Add(IServiceCollection services, string? stoppedMarkerPath)
    {
    /* #1560/#1562: the live MCP + web enable/port seams — the worker publishes the control-plane values on
       every reload; each host's supervisor observes and starts/stops/rebinds without a service restart. */
    services.AddSingleton<McpRuntimeState>();
    services.AddSingleton<WebRuntimeState>();

    /* #3514: the host->worker seam carrying the served web-dashboard TLS certificate's expiry, so the worker's
       alert sweep can raise a self-alert as it approaches (the web host loads the certificate once and only a
       log line ever reported its expiry). */
    services.AddSingleton<WebTlsCertificateState>();

    /* #5288: the MCP listener's twin of the seam above, a SEPARATE singleton so each listener publishes and
       clears only its own certificate: a web restart can never clear or resolve an MCP alert, and a certificate
       both listeners serve is published to both and alerts (and resolves) twice, independently. */
    services.AddSingleton<McpTlsCertificateState>();

    /* #2298: the worker-published monitored-server registry the MCP host's plan-fetch resolver reads,
       replacing its own mcp-role re-read of rows whose encrypted_password column that role is
       deliberately denied. */
    services.AddSingleton<MonitoredServerRegistryState>();

    /* #5632 step 7: the worker's store owner data source, published for the one MCP tool that reads a remediation slot. */
    services.AddSingleton<StoreOwnerSource>();

    /* #9644: the startup config's password key facts, published by the worker once and read by the password key MCP tools. */
    services.AddSingleton<PerformanceMonitor.Darling.Service.Mcp.PasswordKeyStartupFacts>();

    /* #8737: every personal MCP token row, refreshed over the owner connection; the MCP bearer gate checks it. */
    services.AddSingleton<DarlingPersonalTokenSnapshot>(provider => new DarlingPersonalTokenSnapshot
    {
        Pending = provider.GetRequiredService<PersonalTokenPendingRevokes>(),
    });

    /* #9325: the sign-in revokes the store has not taken yet. One queue for the process: the snapshot's token check refuses the owners in it at
       once, the web supervisor drains it, and it is saved in the folder that holds the service's keys so a restart keeps it. Built before either
       host serves, because the snapshot takes it. A config that cannot be loaded leaves it in memory only, with one warning. */
    services.AddSingleton(provider =>
    {
        var logger = provider.GetService<ILoggerFactory>()?.CreateLogger<PersonalTokenPendingRevokes>();
        try
        {
            return PersonalTokenPendingRevokes.ForConfig(DarlingConfig.Load(), DarlingConfig.ResolveConfigPath(), logger);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "The MCP personal token revoke queue is kept in memory only, so a restart would lose it: darling.json could not be loaded.");
            return new PersonalTokenPendingRevokes(logger: logger);
        }
    });

    /* #7041: the worker publishes its alert evaluator here for the MCP host's send_daily_health_report_now. */
    services.AddSingleton<DailyHealthReportHub>();

    /* #6066: the store's pseudonym key, read once over the owner connection the worker publishes above and kept in memory. Taken by the MCP
       host (its data-detail filter and the brief composer) and the worker (the webhook dispatcher); no MCP tool method takes it. */
    services.AddSingleton<PseudonymKeySource>(provider => new PseudonymKeySource(
        provider.GetRequiredService<StoreOwnerSource>(), provider.GetRequiredService<ILogger<PseudonymKeySource>>()));

    /* #2953: the collector's startup verdict — the worker publishes it, the web host's /api/ping reads it, and
       neither touches the store to do so. A singleton for the same reason the three above are: the publisher and
       the reader are separate hosted services in one process, and the answer has to survive being asked at any
       moment rather than being computed on demand from something that might be down.

       #4733: DARLING_STOPPED_MARKER, read once here, is the path of a file the state keeps present while collection
       is stopped for good. Only the compose file sets it (its `darling` service's healthcheck tests for the file);
       the Windows service and every other deployment leave it unset and nothing is written. */
    services.AddSingleton(provider => new CollectorRuntimeState(
        stoppedMarkerPath,
        provider.GetRequiredService<ILogger<CollectorRuntimeState>>()));

    /* #3941: the process's shared baseline tier. The worker's per-pass analysis services, the MCP host's and the web
       host's each read the store as a different role (#3914), so they cannot share providers; they share the computed
       30-day buckets instead, and a series is read from the store once per analysis hour between them. */
    services.AddSingleton<BaselineCache>();

    /* #4442 scope 2: the read-latency histogram accumulator, shared between the web host's /api/read/* dispatch
       loop and RunComposedPanelAsync -- a singleton for the same reason CollectorCostAccumulator is: the worker's
       flush and the web host's recording are separate hosted services in the one process. */
    services.AddSingleton<ReadLatencyAccumulator>();

    /* #5506 part 3: the read admission, one per process: the MCP and web hosts admit through the same slots, each host sets
       the limit from reads.maxHeavyReads when it starts, and the worker builds its long-read table. */
    services.AddSingleton<ReadAdmission>();

    /* #5097: the slow-read record's queue. The three recorders offer to it; the worker runs its one writer once the store
       is migrated. */
    services.AddSingleton<SlowReadLog>();

    /* #8184: the opt-in health port (/livez, /readyz). Registered BEFORE the worker on purpose: hosted services start in order, so it
       listens before migrations and before #8141's ownership check can stop the start, and a probe sees `starting` or `stopped`
       rather than a refused connection. Self-gating on darling.json's health.enabled (default OFF). */
    services.AddHostedService<PerformanceMonitor.Darling.Service.Hosting.DarlingHealthHostService>();

    services.AddHostedService<DarlingWorker>();

    /* #9013: the opt-in Prometheus port (/metrics). Registered AFTER the worker because its snapshot is a store read: the worker
       migrates the store and provisions the login it uses, and a scrape answers the meta series only until the worker collects. The
       config loader is passed in so the host's own source never loads darling.json itself. Self-gating on metrics.enabled (default OFF). */
    services.AddHostedService(provider => new PerformanceMonitor.Darling.Service.Hosting.DarlingMetricsHostService(
        provider.GetRequiredService<CollectorRuntimeState>(),
        provider.GetRequiredService<ReadAdmission>(),
        provider.GetRequiredService<MonitoredServerRegistryState>(),
        provider.GetRequiredService<ILogger<PerformanceMonitor.Darling.Service.Hosting.DarlingMetricsHostService>>(),
        loadConfig: () => DarlingConfig.Load()));

    /* AN4: the analysis MCP tools over Streamable HTTP — registered always, self-gating on
       darling.json's mcp.enabled (default OFF), so Program.cs stays config-free like the worker. */
    services.AddHostedService<DarlingMcpHostService>();

    /* #1562: the read-only web dashboard on its own port — registered always, self-gating on
       darling.json's web.enabled (default OFF), same config-free posture as the worker and MCP host. */
    services.AddHostedService<DarlingWebHostService>();

    /* #7657: the Slack Socket Mode listener. Registered always and self-gating on the opened app-level token the worker publishes:
       a token set means listen, blank means off. The state is the seam the worker publishes the token and route readers through. */
    services.AddSingleton<PerformanceMonitor.Darling.Service.Chat.SlackListenerRuntimeState>();
    services.AddHostedService<PerformanceMonitor.Darling.Service.Chat.SlackSocketModeHostService>();

    /* #6840: the front door. The directory, its secrets and the hosted service that refreshes it are registered always and
       cost nothing unless darling.json's frontDoor.enabled is set (the service gates itself, like the hosts above). The block
       is read from the file on first use, and a file that cannot be read gives the disabled block: the worker reports the
       load failure itself. Every store's MCP peer client is registered by the part that adds it. */
    services.AddSingleton<FrontDoorConfig>(_ =>
    {
        try
        {
            return DarlingConfig.Load().FrontDoor;
        }
        catch (Exception)
        {
            return new FrontDoorConfig();
        }
    });
    services.AddSingleton<IFrontDoorSecrets>(provider => new DarlingFrontDoorSecrets(
        provider.GetRequiredService<FrontDoorConfig>(), provider.GetRequiredService<ILogger<DarlingFrontDoorSecrets>>()));
    services.AddSingleton<DarlingFrontDoorDirectory>();
    services.AddSingleton<IFrontDoorDirectory>(provider => provider.GetRequiredService<DarlingFrontDoorDirectory>());
    services.AddHostedService<DarlingFrontDoorDirectoryService>();

    /* #6840 P2: the MCP peer client. It takes the directory lazily because the directory calls it for list_servers and it calls
       the directory back to hold a store that refused its token. */
    services.AddSingleton<IFrontDoorMcpPeers>(provider => new DarlingFrontDoorMcpPeers(
        provider.GetRequiredService<FrontDoorConfig>(),
        provider.GetRequiredService<IFrontDoorSecrets>(),
        () => provider.GetRequiredService<IFrontDoorDirectory>(),
        provider.GetRequiredService<ILogger<DarlingFrontDoorMcpPeers>>()));
    }

    /// <summary>
    /// Puts the rolling file log on the host's loggers (#9747). The provider buffers lines for up to five seconds and writes them on a timer,
    /// and its Dispose writes what is left. The container disposes what a factory returns and never an instance it was handed, so the
    /// provider is registered through a factory: an instance registration would lose the lines logged in the last seconds before an exit,
    /// the <c>collection loop stopped</c> line among them. The container is disposed after the hosted services have stopped, so their
    /// last lines are in the buffer by then.
    /// </summary>
    internal static void AddFileLog(ILoggingBuilder logging, DarlingFileLoggerProvider provider) =>
        logging.Services.AddSingleton<ILoggerProvider>(_ => provider);
}
