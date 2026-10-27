/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.FrontDoor;
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

    services.AddHostedService<DarlingWorker>();

    /* AN4: the analysis MCP tools over Streamable HTTP — registered always, self-gating on
       darling.json's mcp.enabled (default OFF), so Program.cs stays config-free like the worker. */
    services.AddHostedService<DarlingMcpHostService>();

    /* #1562: the read-only web dashboard on its own port — registered always, self-gating on
       darling.json's web.enabled (default OFF), same config-free posture as the worker and MCP host. */
    services.AddHostedService<DarlingWebHostService>();

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
}
