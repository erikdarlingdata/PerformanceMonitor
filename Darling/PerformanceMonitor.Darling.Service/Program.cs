/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;

/* #1581: classify the FIRST argument before anything else. Only a no-arg invocation (StartHost — how the
   Windows Service Control Manager starts the exe) or a RECOGNIZED verb reaches the host; --version/--help
   print and exit 0; an UNKNOWN option prints usage to stderr and exits NON-ZERO without starting the host.
   The incident: `Service.exe --version` used to fall through into a real service startup and spawn a second
   instance that fought the first over the bundled PostgreSQL and ports — the outage. The recognized-verb
   dispatch below is unchanged; it only runs for the RunKnownVerb / StartHost fall-through. */
switch (DarlingCliCommands.ClassifyStartupArgs(args))
{
    case StartupAction.PrintVersion:
        Console.WriteLine(DarlingCliCommands.ProductVersion());
        return 0;

    case StartupAction.PrintHelp:
        Console.WriteLine(DarlingCliCommands.UsageText());
        return 0;

    case StartupAction.UnknownOption:
        Console.Error.WriteLine($"Unknown option: {args[0]}");
        Console.Error.WriteLine();
        Console.Error.WriteLine(DarlingCliCommands.UsageText());
        return 1;

    case StartupAction.RunKnownVerb:
    case StartupAction.StartHost:
    default:
        /* Fall through to the recognized-verb dispatch (RunKnownVerb) or the host build (StartHost). */
        break;
}

/* CLI verb: encrypt a SQL-auth password for darling.json. Reads from stdin (not an argument,
   so the plaintext never lands in shell history) and prints the DPAPI-LocalMachine blob. */
if (args.Length > 0 && DarlingCliCommands.IsEncryptPasswordVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--encrypt-password requires Windows (DPAPI).");
        return 1;
    }

    Console.Error.Write("Password: ");
    var plaintext = Console.ReadLine();
    if (string.IsNullOrEmpty(plaintext))
    {
        /* #2097: on a non-interactive host (ISE/remote/redirected stdin) ReadLine returns null instantly
           and stderr is invisible — the guidance must ride STDOUT, the stream every host shows. */
        Console.Error.WriteLine("No password read from stdin.");
        DarlingCliCommands.WriteNonInteractiveGuidance(Console.Out);
        return 1;
    }

    Console.WriteLine(DarlingSecrets.Protect(plaintext));
    Console.Error.WriteLine("Paste the line above into the server's \"encryptedPassword\" in darling.json.");
    return 0;
}

/* CLI verb: validate darling.json and probe every configured server (reachability + permission pre-flight),
   reusing the same DarlingServerConnector probe the test_connect command runs. Optional second arg = an
   explicit config path (else the usual DARLING_CONFIG / next-to-binary resolution). Exit 0 iff all pass. */
if (args.Length > 0 && DarlingCliCommands.IsValidateConfigVerb(args[0]))
{
    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.ValidateConfigAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: print a paste-ready remote-viewer connection string + the server TLS cert for the opt-in store
   network endpoint (darling-network-endpoints D8). It DPAPI-decrypts the network role's credential, so it is
   Windows-only (same guard shape as --encrypt-password). Optional second arg = an explicit config path. */
if (args.Length > 0 && DarlingCliCommands.IsPrintViewerConnectionVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--print-viewer-connection requires Windows (DPAPI).");
        return 1;
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.PrintViewerConnectionAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verbs: --print-mcp-token / --print-web-token (#2479 item 2) — reprint an endpoint's access token from
   darling.json. --configure-network shows each generated token once; losing it used to leave regeneration as
   the only path, which invalidates every client already configured against it. The verbs refuse when not
   elevated, and both are Windows-only for the same reason --print-viewer-connection is: the token is a DPAPI
   blob. The reasoning about what this does and does NOT disclose lives with the implementation. */
if (args.Length > 0 && DarlingCliCommands.IsPrintMcpTokenVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--print-mcp-token requires Windows (DPAPI).");
        return 1;
    }

    return DarlingCliCommands.PrintMcpToken(args.Length > 1 ? args[1] : null, Console.Out, Console.Error);
}

if (args.Length > 0 && DarlingCliCommands.IsPrintWebTokenVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--print-web-token requires Windows (DPAPI).");
        return 1;
    }

    return DarlingCliCommands.PrintWebToken(args.Length > 1 ? args[1] : null, Console.Out, Console.Error);
}

/* CLI verb: --export-viewer-config (#1953) — write the viewer machine's whole handoff folder (a complete
   darling.json with "managed": false and the resolved connection string, the store's server.crt beside it, and
   a README.txt documenting every field) instead of making the operator hand-merge --print-viewer-connection's
   output into JSON copied out of the docs. Same DPAPI/TLS material as that verb, so the same Windows-only
   guard. First non-flag arg = the output DIRECTORY (default: viewer-config beside darling.json); an explicit
   config path is passed as --config <path> (this verb's positional is the destination, unlike its siblings). */
if (args.Length > 0 && DarlingCliCommands.IsExportViewerConfigVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--export-viewer-config requires Windows (DPAPI).");
        return 1;
    }

    /* Parsed STRICTLY by the pure TryParseExportViewerConfigArgs (never guess at an argument — #1581's
       posture), so the rules are pinned by tests rather than living inline here. */
    if (!DarlingCliCommands.TryParseExportViewerConfigArgs(
            args[1..], out var exportConfigPath, out var exportDirectory, out var exportArgError))
    {
        Console.Error.WriteLine(exportArgError);
        return 1;
    }

    return await DarlingCliCommands.ExportViewerConfigAsync(
        exportConfigPath, exportDirectory, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: the interactive --configure-network wizard (#1561; web surface #1617) — guides the operator
   through the opt-in store / MCP / web-dashboard LAN exposure, validating every input by delegation to the
   SAME resolvers the running service fail-closes on, then splicing a comment-preserving edit into
   darling.json behind a timestamped backup. It generates + DPAPI-protects the MCP bearer / web access
   tokens, so it is Windows-only (same guard shape as the two verbs above). Optional second arg = an
   explicit config path. Console.In is the scripted-input testability lever. */
if (args.Length > 0 && DarlingCliCommands.IsConfigureNetworkVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--configure-network requires Windows (DPAPI + service control).");
        return 1;
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.ConfigureNetworkAsync(
        configPath, Console.In, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: --configure-firewall (#1771) — create/remove every scoped Darling firewall rule so the live
   firewall matches darling.json. The service account cannot write firewall rules by design, so this ELEVATED
   verb owns them: install-darling.ps1 runs it after the service is up, uninstall-darling.ps1 removes them, and
   the running service only verifies. Reads darling.json only (no store, no credentials), so it works at
   install time before the store has ever booted. Optional second arg = an explicit config path. */
if (args.Length > 0 && DarlingCliCommands.IsConfigureFirewallVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--configure-firewall requires Windows (Windows Firewall).");
        return 1;
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.ConfigureFirewallAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: re-apply the secret-file ACLs, elevated (#2352). The running service computes the correct DACL and
   detects when the real one is wrong, but cannot apply it — re-ACLing a file it does not own needs WRITE_DAC and
   taking ownership needs a privilege a virtual service account is not granted — so it logs the remedy and carries
   on. This is the actor with the authority. Verifies every target after the attempt and exits non-zero if
   anything is still readable, so it is usable in a provisioning script. Windows-only: ACLs.
   Optional second arg = an explicit config path. */
if (args.Length > 0 && DarlingCliCommands.IsHardenFilesVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--harden-files requires Windows (NTFS ACLs).");
        return 1;
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return DarlingCliCommands.HardenFiles(configPath, Console.Out, Console.Error);
}

/* CLI verbs: enable/disable the embedded MCP + web-dashboard endpoints on a HEADLESS managed deployment. Each
   flips the live switch in config.config_service (mcp_enabled/web_enabled — the store is authoritative after the
   first run; darling.json's enabled is only the seed) via a targeted UPDATE whose self-bump trigger makes the
   worker hot-reload within one sweep, and — when the endpoint opts into LAN exposure — opens/removes the matching
   scoped firewall rule if elevated (else prints it as an elevated handoff). They decrypt the managed owner
   credential + touch the firewall, so they are Windows-only (same guard shape as --print-viewer-connection).
   Optional second arg = an explicit config path. The IsKnownVerb allow-list mirrors this dispatch (they cannot drift). */
if (args.Length > 0 && DarlingCliCommands.IsEnableMcpVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        /* #2626: the refusal names the path that WORKS on this host, not just the platform it is not. */
        return DarlingCliCommands.WriteEndpointVerbPlatformRefusal(
            isMcp: true, enable: true, args.Length > 1 ? args[1] : null, Console.Error);
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.EnableMcpAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

if (args.Length > 0 && DarlingCliCommands.IsDisableMcpVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        /* #2626: the refusal names the path that WORKS on this host, not just the platform it is not. */
        return DarlingCliCommands.WriteEndpointVerbPlatformRefusal(
            isMcp: true, enable: false, args.Length > 1 ? args[1] : null, Console.Error);
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.DisableMcpAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

if (args.Length > 0 && DarlingCliCommands.IsEnableWebVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        /* #2626: the refusal names the path that WORKS on this host, not just the platform it is not. */
        return DarlingCliCommands.WriteEndpointVerbPlatformRefusal(
            isMcp: false, enable: true, args.Length > 1 ? args[1] : null, Console.Error);
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.EnableWebAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

if (args.Length > 0 && DarlingCliCommands.IsDisableWebVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        /* #2626: the refusal names the path that WORKS on this host, not just the platform it is not. */
        return DarlingCliCommands.WriteEndpointVerbPlatformRefusal(
            isMcp: false, enable: false, args.Length > 1 ? args[1] : null, Console.Error);
    }

    var configPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.DisableWebAsync(configPath, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: --add-server / --add-servers (#2256) — register monitored server(s) in the store from a JSON
   array on STDIN. The store is authoritative after the first seed, so darling.json edits are ignored, and the
   web surface deliberately excludes the write tools; a headless host (the field report ran Windows Server 2012,
   which cannot run the Viewer) had no supported path at all. Goes through the same AddServers path the MCP tool
   uses, so validation, dedupe, the connection probe, password encryption and the server_id computation are
   shared rather than reimplemented. NO Windows guard here on purpose: the verb needs Windows only for a MANAGED
   store credential (DPAPI), which it checks itself, so a Linux host with bring-your-own Postgres can use it.
   Reads stdin rather than argv so a password never lands in the process list or shell history. */
if (args.Length > 0 && DarlingCliCommands.IsAddServerVerb(args[0]))
{
    var addServerConfigPath = args.Length > 1 ? args[1] : null;
    return await DarlingCliCommands.AddServerAsync(
        addServerConfigPath, Console.In, Console.Out, Console.Error, CancellationToken.None);
}

/* CLI verb: --backfill-rollups (#1759 Phase 2) — materialize the query-acceleration rollups back over
   pre-existing history so the #1680 arming gate can release the held raw retention policies by itself. An
   OPERATOR verb, deliberately not a startup step: the gate is all-or-nothing, so a store with a year of raw
   must materialize the whole history before the first purge reclaims anything, which puts peak disk BEFORE
   any relief. It preflights free space and refuses with numbers rather than filling the volume. Runs while the
   service is up, is resumable, and arms nothing itself. --dry-run prints the plan + estimate and stops.
   Windows-only in managed mode (the DPAPI owner credential), same guard shape as the verbs above. */
if (args.Length > 0 && DarlingCliCommands.IsBackfillRollupsVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--backfill-rollups requires Windows (DPAPI store credential).");
        return 1;
    }

    var rest = args.AsSpan(1);
    var dryRun = false;
    string? backfillConfigPath = null;
    foreach (var arg in rest)
    {
        if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
        {
            dryRun = true;
        }
        else
        {
            backfillConfigPath = arg;
        }
    }

    return await DarlingCliCommands.BackfillRollupsAsync(
        backfillConfigPath, dryRun, Console.Out, Console.Error, CancellationToken.None);
}

/* #1912 — repair the pre-#1907 Query Store split slices still in stored rows, then re-materialize what they
   fed. Same Windows-only managed-credential guard and same --dry-run shape as --backfill-rollups above. */
if (args.Length > 0 && DarlingCliCommands.IsCollapseLegacySlicesVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--collapse-legacy-slices requires Windows (DPAPI store credential).");
        return 1;
    }

    var rest = args.AsSpan(1);
    var dryRun = false;
    string? collapseConfigPath = null;
    foreach (var arg in rest)
    {
        if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
        {
            dryRun = true;
        }
        else
        {
            collapseConfigPath = arg;
        }
    }

    return await DarlingCliCommands.CollapseLegacySlicesAsync(
        collapseConfigPath, dryRun, Console.Out, Console.Error, CancellationToken.None);
}

/* #2076 — convert the plan dimension's pre-V54 text rows to gzip (#2069's write form), in batches, while
   the service runs. Same Windows-only managed-credential guard and same --dry-run shape as the verbs above. */
if (args.Length > 0 && DarlingCliCommands.IsRecompressPlanDimVerb(args[0]))
{
    if (!OperatingSystem.IsWindows())
    {
        Console.Error.WriteLine("--recompress-plan-dim requires Windows (DPAPI store credential).");
        return 1;
    }

    var (recompressConfigPath, dryRun, vacuumMode, parseError) = DarlingCliCommands.ParseRecompressArgs(args.AsSpan(1));
    if (parseError is not null)
    {
        Console.Error.WriteLine(parseError);
        return 1;
    }

    return await DarlingCliCommands.RecompressPlanDimAsync(
        recompressConfigPath, dryRun, vacuumMode, Console.Out, Console.Error, CancellationToken.None);
}

var builder = Host.CreateApplicationBuilder(args);

/* Windows-service lifetime is a no-op when run from a console, so the same exe
   serves interactive debugging and `sc create` installation (plan HP7/Phase 8). */
builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "PerformanceMonitor Darling";
});

/* The rolling file log under %ProgramData%\PerformanceMonitorDarling\logs — the service's PRIMARY
   diagnostic surface (see DarlingFileLoggerProvider remarks). Registered unconditionally: console
   runs get the same file, which is exactly what an operator collecting a bug report wants. */
builder.Logging.AddProvider(new DarlingFileLoggerProvider());

if (OperatingSystem.IsWindows())
{
    /* Pin the Event Log source to the SERVICE NAME so operators find events where the docs say to
       look ("PerformanceMonitor Darling"), not under the assembly name AddWindowsService defaults
       to. The source itself can only be REGISTERED by an elevated principal: the recommended
       NT SERVICE virtual account cannot, so the attempt below is best-effort (it succeeds on an
       elevated console run or an install script that pre-created it; see the README install step).
       When the source does not exist, the Event Log provider silently drops events — which is why
       the file log above, not this, is the primary surface. */
    builder.Logging.AddEventLog(ConfigureWindowsEventLogSource);
    try
    {
        if (!System.Diagnostics.EventLog.SourceExists("PerformanceMonitor Darling"))
        {
            System.Diagnostics.EventLog.CreateEventSource("PerformanceMonitor Darling", "Application");
        }
    }
    catch
    {
        /* SourceExists itself throws for a non-elevated caller when the source is missing (it
           probes the Security log). Degrade: the file log carries the diagnostics regardless. */
    }
}

/* #1560/#1562: the live MCP + web enable/port seams — the worker publishes the control-plane values on
   every reload; each host's supervisor observes and starts/stops/rebinds without a service restart. */
builder.Services.AddSingleton<McpRuntimeState>();
builder.Services.AddSingleton<WebRuntimeState>();

/* #2298: the worker-published monitored-server registry the MCP host's plan-fetch resolver reads,
   replacing its own mcp-role re-read of rows whose encrypted_password column that role is
   deliberately denied. */
builder.Services.AddSingleton<MonitoredServerRegistryState>();

/* #2953: the collector's startup verdict — the worker publishes it, the web host's /api/ping reads it, and
   neither touches the store to do so. A singleton for the same reason the three above are: the publisher and
   the reader are separate hosted services in one process, and the answer has to survive being asked at any
   moment rather than being computed on demand from something that might be down. */
builder.Services.AddSingleton<CollectorRuntimeState>();

builder.Services.AddHostedService<DarlingWorker>();

/* AN4: the analysis MCP tools over Streamable HTTP — registered always, self-gating on
   darling.json's mcp.enabled (default OFF), so Program.cs stays config-free like the worker. */
builder.Services.AddHostedService<DarlingMcpHostService>();

/* #1562: the read-only web dashboard on its own port — registered always, self-gating on
   darling.json's web.enabled (default OFF), same config-free posture as the worker and MCP host. */
builder.Services.AddHostedService<DarlingWebHostService>();

var host = builder.Build();

/* #1581 single-instance guard: acquire a system-wide named mutex BEFORE running the host (Build() only
   constructs the DI graph; Run() is what starts the worker and touches Postgres). If another instance
   already holds it, log and exit non-zero WITHOUT starting — a second instance would fight the first over
   the bundled PostgreSQL shared-memory segment and the MCP/web ports (the ~7.5h outage). Windows-only: the
   Global\ namespace and the shared-memory fight are Windows concerns, and the managed-Postgres bootstrap
   already fail-closes on non-Windows — matching how the codebase gates Windows behavior. The guard is
   acquired and released on THIS (main) thread, around the blocking host.Run(), honoring the mutex's thread
   affinity. */
SingleInstanceGuard? instanceGuard = null;
if (OperatingSystem.IsWindows())
{
    var startupLogger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("SingleInstance");
    instanceGuard = SingleInstanceGuard.TryAcquire(SingleInstanceGuard.ServiceMutexName);
    if (!instanceGuard.Owns)
    {
        startupLogger.LogCritical(
            "Another PerformanceMonitor Darling service instance already holds the single-instance lock ({Mutex}) — exiting without starting a second instance (a second instance would fight the first over the bundled PostgreSQL and the MCP/web ports).",
            SingleInstanceGuard.ServiceMutexName);
        instanceGuard.Dispose();
        /* Run() disposes the host on the normal path; we are skipping Run(), so dispose it ourselves. */
        host.Dispose();
        return 4;
    }

    if (instanceGuard.WasAbandoned)
    {
        startupLogger.LogWarning(
            "Acquired the single-instance lock ({Mutex}) left abandoned by a previous instance that exited without a clean shutdown — continuing startup.",
            SingleInstanceGuard.ServiceMutexName);
    }
}

try
{
    host.Run();
    return 0;
}
finally
{
    instanceGuard?.Dispose();
}

/* The delegate is registered inside the OperatingSystem.IsWindows() block above, but the platform
   analyzer does not carry that guard into a delegate body — an explicit in-body guard keeps
   EventLogSettings.SourceName provably Windows-only. */
static void ConfigureWindowsEventLogSource(Microsoft.Extensions.Logging.EventLog.EventLogSettings settings)
{
    if (!OperatingSystem.IsWindows())
    {
        return;
    }

    settings.SourceName = "PerformanceMonitor Darling";
}
