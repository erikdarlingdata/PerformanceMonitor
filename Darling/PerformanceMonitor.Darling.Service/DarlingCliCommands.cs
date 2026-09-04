/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Versioning;
using System.Security.Principal;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the exe should do for its command-line, decided from the FIRST argument by
/// <see cref="DarlingCliCommands.ClassifyStartupArgs"/> (#1581). Before this, an UNRECOGNIZED flag
/// (the incident's <c>Service.exe --version</c>) fell through into a real service startup, spawning a
/// second instance — the outage. Now only <see cref="StartHost"/> (no args) or a recognized verb reaches
/// the host; anything else prints and exits.
/// </summary>
public enum StartupAction
{
    /// <summary>No arguments — run the service host (also how the SCM starts it).</summary>
    StartHost,

    /// <summary><c>--version</c>/<c>-v</c> — print the product version and exit 0.</summary>
    PrintVersion,

    /// <summary><c>--help</c>/<c>-h</c> — print usage and exit 0.</summary>
    PrintHelp,

    /// <summary>A recognized one-shot verb (encrypt-password, test-connection, …) — dispatched by Program.</summary>
    RunKnownVerb,

    /// <summary>An unrecognized argument — print "unknown option" + usage to stderr and exit non-zero.</summary>
    UnknownOption,
}

/// <summary>
/// One-shot CLI verbs the service exe supports alongside the Windows-service host — currently the
/// <c>--test-connection</c> / <c>--validate-config</c> pre-flight (Stage 2). It loads darling.json,
/// validates its shape, and probes EVERY configured server for reachability + permissions, reusing the SAME
/// <see cref="DarlingServerConnector.ProbeAsync"/> path the <c>test_connect</c> command runs — so a config
/// that validates from the CLI connects identically under the running service. Pure output formatting
/// (<see cref="FormatProbeLine"/>) is split out so it is unit-testable without live SQL.
/// </summary>
public static class DarlingCliCommands
{
    /// <summary>The verb that encrypts a SQL-auth password for darling.json (reads stdin).</summary>
    public static bool IsEncryptPasswordVerb(string arg) =>
        string.Equals(arg, "--encrypt-password", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb aliases handled by <see cref="TryGetValidateConfigVerb"/>.</summary>
    public static bool IsValidateConfigVerb(string arg) =>
        string.Equals(arg, "--test-connection", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "--validate-config", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="PrintViewerConnectionAsync"/> handles (darling-network-endpoints D8).</summary>
    public static bool IsPrintViewerConnectionVerb(string arg) =>
        string.Equals(arg, "--print-viewer-connection", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="PrintMcpToken"/> handles — reprint the MCP bearer token (#2479, item 2).</summary>
    public static bool IsPrintMcpTokenVerb(string arg) =>
        string.Equals(arg, "--print-mcp-token", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="PrintWebToken"/> handles — reprint the web dashboard access token.
    /// The issue asked for the MCP half; both tokens have the identical unrecoverable-loss problem and the
    /// identical DPAPI shape, so shipping one and not the other would leave half a fix behind.</summary>
    public static bool IsPrintWebTokenVerb(string arg) =>
        string.Equals(arg, "--print-web-token", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="ExportViewerConfigAsync"/> handles — write a COMPLETE viewer darling.json +
    /// server.crt + README.txt an operator copies to the viewer machine as-is (#1953).</summary>
    public static bool IsExportViewerConfigVerb(string arg) =>
        string.Equals(arg, "--export-viewer-config", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="ConfigureNetworkAsync"/> handles — the interactive exposure wizard (#1561).</summary>
    public static bool IsConfigureNetworkVerb(string arg) =>
        string.Equals(arg, "--configure-network", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="ConfigureFirewallAsync"/> handles — reconcile every scoped Darling firewall
    /// rule from darling.json, elevated (#1771). This is the ONLY place rules are created; the service itself
    /// only verifies them.</summary>
    public static bool IsConfigureFirewallVerb(string arg) =>
        string.Equals(arg, "--configure-firewall", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="HardenFiles"/> handles — re-apply the secret-file ACLs from an elevated
    /// prompt (#2352). The running service knows the correct ACL and cannot apply it: re-ACLing a file it does
    /// not own needs WRITE_DAC, and taking ownership needs a privilege a virtual service account is not
    /// granted, so it can only log the remedy. This is the actor that carries it out.</summary>
    public static bool IsHardenFilesVerb(string arg) =>
        string.Equals(arg, "--harden-files", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="EnableMcpAsync"/> handles — enable the MCP endpoint in the store (+ firewall).</summary>
    public static bool IsEnableMcpVerb(string arg) =>
        string.Equals(arg, "--enable-mcp", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="DisableMcpAsync"/> handles — disable the MCP endpoint in the store (+ firewall).</summary>
    public static bool IsDisableMcpVerb(string arg) =>
        string.Equals(arg, "--disable-mcp", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="EnableWebAsync"/> handles — enable the web-dashboard endpoint in the store (+ firewall).</summary>
    public static bool IsEnableWebVerb(string arg) =>
        string.Equals(arg, "--enable-web", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="DisableWebAsync"/> handles — disable the web-dashboard endpoint in the store (+ firewall).</summary>
    public static bool IsDisableWebVerb(string arg) =>
        string.Equals(arg, "--disable-web", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="BackfillRollupsAsync"/> handles — materialize the query-acceleration rollups
    /// back to raw's oldest row, behind a disk preflight (#1759 Phase 2).</summary>
    public static bool IsBackfillRollupsVerb(string arg) =>
        string.Equals(arg, "--backfill-rollups", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="CollapseLegacySlicesAsync"/> handles — repair the pre-#1907 Query Store
    /// split slices still sitting in stored rows, then re-materialize what they fed (#1912).</summary>
    public static bool IsCollapseLegacySlicesVerb(string arg) =>
        string.Equals(arg, "--collapse-legacy-slices", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="RecompressPlanDimAsync"/> handles — convert the plan dimension's
    /// pre-V54 text rows to the gzip form V54's write path produces (#2076).</summary>
    public static bool IsRecompressPlanDimVerb(string arg) =>
        string.Equals(arg, "--recompress-plan-dim", StringComparison.OrdinalIgnoreCase);

    /// <summary>The verb <see cref="AddServerAsync"/> handles — register monitored server(s) in the store from a
    /// JSON array on STDIN (#2256). The store is authoritative after the first seed, so on a headless host with no
    /// GUI and no MCP client this was previously impossible: the web surface excludes the write tools by design
    /// and darling.json is a one-time bootstrap.</summary>
    public static bool IsAddServerVerb(string arg) =>
        string.Equals(arg, "--add-server", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "--add-servers", StringComparison.OrdinalIgnoreCase);

    /// <summary><c>--version</c>/<c>-v</c> — print the product version and exit.</summary>
    public static bool IsVersionVerb(string arg) =>
        string.Equals(arg, "--version", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "-v", StringComparison.OrdinalIgnoreCase);

    /// <summary><c>--help</c>/<c>-h</c>/<c>-?</c>/<c>/?</c> — print usage and exit.</summary>
    public static bool IsHelpVerb(string arg) =>
        string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "-h", StringComparison.OrdinalIgnoreCase)
        || string.Equals(arg, "-?", StringComparison.Ordinal)
        || string.Equals(arg, "/?", StringComparison.Ordinal);

    /// <summary>
    /// Every one-shot CLI verb the exe recognizes as its FIRST argument — the allow-list
    /// <see cref="ClassifyStartupArgs"/> uses and the single source of truth Program's dispatch mirrors, so the
    /// two can never drift. Excludes <c>--version</c>/<c>--help</c> (those are their own classifications).
    /// </summary>
    public static bool IsKnownVerb(string arg) =>
        IsEncryptPasswordVerb(arg)
        || IsValidateConfigVerb(arg)
        || IsPrintViewerConnectionVerb(arg)
        || IsPrintMcpTokenVerb(arg)
        || IsPrintWebTokenVerb(arg)
        || IsExportViewerConfigVerb(arg)
        || IsConfigureNetworkVerb(arg)
        || IsConfigureFirewallVerb(arg)
        || IsHardenFilesVerb(arg)
        || IsEnableMcpVerb(arg)
        || IsDisableMcpVerb(arg)
        || IsEnableWebVerb(arg)
        || IsDisableWebVerb(arg)
        || IsBackfillRollupsVerb(arg)
        || IsCollapseLegacySlicesVerb(arg)
        || IsRecompressPlanDimVerb(arg)
        || IsAddServerVerb(arg);

    /// <summary>
    /// Classifies the exe's command line from its FIRST argument (#1581): no args → run the host; a recognized
    /// verb → dispatch it; <c>--version</c>/<c>--help</c> → print + exit; ANYTHING else → an unknown option that
    /// must NOT start the host (the incident: <c>Service.exe --version</c> used to fall through into a real
    /// startup and spawn a second instance). Pure so it pins directly.
    /// </summary>
    public static StartupAction ClassifyStartupArgs(string[]? args)
    {
        if (args is null || args.Length == 0)
        {
            return StartupAction.StartHost;
        }

        var first = args[0];
        if (IsVersionVerb(first))
        {
            return StartupAction.PrintVersion;
        }

        if (IsHelpVerb(first))
        {
            return StartupAction.PrintHelp;
        }

        if (IsKnownVerb(first))
        {
            return StartupAction.RunKnownVerb;
        }

        return StartupAction.UnknownOption;
    }

    /// <summary>
    /// The product version string for <c>--version</c> — the assembly's informational version (the csproj
    /// <c>&lt;Version&gt;</c>), with any SemVer <c>+build</c> metadata suffix stripped, falling back to the
    /// assembly version. Pure (reads this assembly's own attributes), so it pins directly.
    /// </summary>
    public static string ProductVersion()
    {
        var assembly = typeof(DarlingCliCommands).Assembly;
        var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrWhiteSpace(informational))
        {
            var plus = informational.IndexOf('+', StringComparison.Ordinal);
            return plus >= 0 ? informational[..plus] : informational;
        }

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }

    /// <summary>The usage text for <c>--help</c> and the unknown-option error. Pure ASCII, one verb per line.</summary>
    public static string UsageText() =>
        "PerformanceMonitor Darling service." + Environment.NewLine +
        Environment.NewLine +
        "Usage:" + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe                     Run the service (also how the Windows Service Control Manager starts it)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --version, -v       Print the product version and exit." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --help, -h          Print this help and exit." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --test-connection   Validate darling.json and probe every configured server." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --encrypt-password  Encrypt a SQL-auth password for darling.json (reads stdin)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --print-viewer-connection   Print a remote-viewer connection string (managed store)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --print-mcp-token   Reprint the MCP bearer token from darling.json (run elevated; writes a LIVE token to stdout)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --print-web-token   Reprint the web dashboard access token from darling.json (run elevated; writes a LIVE token to stdout)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --export-viewer-config [dir] [--config <path>]  Write a ready-to-copy viewer folder (darling.json + server.crt + README.txt)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --configure-network Interactive LAN-exposure wizard." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --configure-firewall  Create/remove the scoped firewall rules to match darling.json (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --harden-files      Re-apply the ACLs on darling.json and the store credentials (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --enable-mcp        Enable the MCP endpoint in the store and open its firewall (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --disable-mcp       Disable the MCP endpoint in the store and remove its firewall rule (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --enable-web        Enable the web dashboard in the store and open its firewall (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --disable-web       Disable the web dashboard in the store and remove its firewall rule (run elevated)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --backfill-rollups  Materialize the retention rollups back over existing history, after a disk preflight." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --collapse-legacy-slices  Repair Query Store rows collected before the split-slice fix, then re-materialize the rollups they fed." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --recompress-plan-dim  Convert the plan dimension's pre-V54 text rows to gzip in batches while the service runs, then VACUUM FULL to return the space to the volume (--no-vacuum-full to skip; --vacuum-full to compact an already-converted store)." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --add-server, --add-servers   Register monitored server(s) from a JSON array on stdin (the add_servers shape); the running service picks them up without a restart." + Environment.NewLine +
        "  PerformanceMonitor.Darling.Service.exe --backfill-rollups --dry-run   Show the plan, the disk estimate and the time budget, and change nothing.";

    /// <summary>
    /// Loads + validates darling.json, then probes every server. Prints one PASS/FAIL line per server and a
    /// summary. Returns 0 only when the config is valid AND every server is reachable; 1 otherwise (so it is
    /// usable as a deployment gate). Store/collection are never touched — this is a pure config pre-flight.
    /// </summary>
    public static async Task<int> ValidateConfigAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var problems = config.Validate();
        if (problems.Count > 0)
        {
            error.WriteLine("Configuration is invalid:");
            foreach (var problem in problems)
            {
                error.WriteLine("  - " + problem);
            }

            return 1;
        }

        output.WriteLine($"Validating connectivity to {config.Servers.Count} server(s)...");

        var allReachable = true;
        foreach (var server in config.Servers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var probe = await DarlingServerConnector.ProbeAsync(server, null, cancellationToken);
            output.WriteLine(FormatProbeLine(server.DisplayName, probe));
            if (!probe.Success)
            {
                allReachable = false;
            }
        }

        output.WriteLine(allReachable
            ? "All servers reachable."
            : "One or more servers failed the connection pre-flight (see above).");
        return allReachable ? 0 : 1;
    }

    /// <summary>Formats one server's probe outcome as a PASS/FAIL line (pure — unit-testable).</summary>
    public static string FormatProbeLine(string serverName, ConnectionProbeResult probe)
    {
        if (!probe.Success)
        {
            return $"  [FAIL] {serverName}: {probe.Error}";
        }

        return $"  [PASS] {serverName}: {DarlingServerConnector.DescribeProbeFacts(probe)}";
    }

    /// <summary>
    /// Prints a paste-ready remote-viewer connection string PER ADMITTED ROLE (#2665) and the server TLS
    /// certificate for the opt-in store network endpoint (darling-network-endpoints D8). It DPAPI-decrypts the
    /// credential of every role <c>postgres.network.role</c> names (default <c>viewer</c>, read-only) and reads
    /// the generated <c>server.crt</c>, so it must run ON the managed store's host under an account that can decrypt them —
    /// hence Windows-only (the caller is <c>OperatingSystem.IsWindows()</c>-guarded, mirroring
    /// <c>--encrypt-password</c>). The operator pastes the string into the VIEWER machine's darling.json
    /// (<c>postgres.managed = false</c>, into <c>postgres.connectionString</c>, consumed verbatim — no viewer
    /// code change) and saves the emitted PEM where <c>Root Certificate</c> points. Returns 0 on success; 1 on a
    /// mode/role/credential error. Managed-mode only (BYO governs its own exposure, D-BYO); network config lives
    /// out of the all-fatal <see cref="DarlingConfig.Validate"/>, so this verb never calls it.
    /// <para><b>STDOUT carries a LIVE SECRET</b> (the role password) — the verb warns (on STDERR) to redirect it
    /// to an ACL'd file or the clipboard, never scrollback / CI / a screenshare.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> PrintViewerConnectionAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var handoffs = await ResolveViewerHandoffsAsync(
            config, "--print-viewer-connection", "print", error, cancellationToken);
        if (handoffs is null)
        {
            return 1;
        }

        /* The client-side Root Certificate placeholder: the operator saves the PEM below at this path on the
           VIEWER machine (a bare filename resolves against the folder holding the viewer's darling.json —
           #1970; an absolute path also works). Kept as a literal so the printed string is paste-ready. */
        const string clientCertificatePath = ViewerClientCertificateFileName;

        /* Read the cert BEFORE anything reaches STDOUT: every STDERR line — including the missing-cert NOTE —
           must be emitted ahead of the payload (#1953 item 3). The field report watched the live password scroll
           past and only THEN saw the redirect advice, which is exactly backwards for a warning. */
        /* #2117: prefer the distributable ROOT (the CA that signed the served leaf) when the store
           carries the fixed chain shape — that is what verify-full's Root Certificate must anchor
           on. A legacy store has no root.crt, and its single self-signed server.crt remains the
           right (if Windows-hostile) thing to print. */
        var certificateSource = handoffs[0].CertificatePath;
        var distributableCertPath = DarlingManagedPostgres.RootCertificatePathFor(certificateSource);
        if (!File.Exists(distributableCertPath))
        {
            distributableCertPath = certificateSource;
        }

        var certificate = File.Exists(distributableCertPath)
            ? (await File.ReadAllTextAsync(distributableCertPath, cancellationToken)).Trim()
            : null;

        var admitsAdmin = handoffs.Any(h => string.Equals(h.Role, "admin", StringComparison.Ordinal));
        var roleList = string.Join("', '", handoffs.Select(h => h.Role));
        var many = handoffs.Count > 1;

        /* Guidance + the live-secret warning go to STDERR, so redirecting STDOUT to a file or the clipboard
           captures the connection string + cert WITHOUT swallowing the warning (D8). ALL of it is emitted
           before the first STDOUT byte, including for a multi-role exposure — interleaving a warning between
           two printed strings would put half the advice after a password had already scrolled past (#1953
           item 3). */
        error.WriteLine();
        error.WriteLine(
            $"WARNING: the connection {(many ? "strings" : "string")} below {(many ? "contain" : "contains")} a LIVE " +
            $"database password (the '{roleList}' {(many ? "roles" : "role")}), written " +
            "to STDOUT. Redirect it to an ACL'd file or pipe it to the clipboard; do not leave it in shell " +
            "scrollback, CI logs, or a screenshare.");
        error.WriteLine("  Example (file):      PerformanceMonitor.Darling.Service.exe --print-viewer-connection > viewer-connection.txt");
        error.WriteLine("  Example (clipboard): PerformanceMonitor.Darling.Service.exe --print-viewer-connection | clip");
        error.WriteLine("  Example (no paste):  PerformanceMonitor.Darling.Service.exe --export-viewer-config   (writes the whole viewer folder for you)");
        if (many)
        {
            error.WriteLine(
                $"  NOTE: postgres.network.role admits {handoffs.Count} roles (#2665), so one string is printed per role, " +
                "each a DIFFERENT credential. Give each seat only the one it needs — they are not interchangeable.");
        }

        if (admitsAdmin)
        {
            error.WriteLine(
                "  NOTE: 'admin' is a WRITE credential holding the config-table pivot surface. Prefer the default " +
                "'viewer' (read-only) for a remote seat; if you must use 'admin', NTFS-ACL the laptop file too.");
        }

        if (certificate is not null)
        {
            error.WriteLine(
                $"Save the certificate block below as '{clientCertificatePath}' on the viewer machine (beside its " +
                "darling.json) and point \"Root Certificate\" at it — the store uses SSL Mode=VerifyFull, so the cert must match.");
        }
        else
        {
            error.WriteLine(
                $"NOTE: the server TLS certificate ({certificateSource}) does not exist yet — the service generates it " +
                "on its first managed start with postgres.network exposed. Enable postgres.network, restart the " +
                "service, then re-run this command to emit the cert for verify-full.");
        }

        error.WriteLine();

        /* One paste-ready string per admitted role, each labelled with the role it authenticates as — the
           strings differ only in Username= and Password=, so an unlabelled pair is impossible to tell apart
           after the fact and the reader would have to guess which seat to hand where. */
        foreach (var handoff in handoffs)
        {
            output.WriteLine(
                $"# Paste into the viewer machine's darling.json -> postgres.connectionString (with postgres.managed = false) — the '{handoff.Role}' seat:");
            output.WriteLine(BuildViewerConnectionString(
                handoff.Host, handoff.Port, handoff.Role, handoff.Password, clientCertificatePath));
            output.WriteLine();
        }

        /* Emit the server cert PEM so the operator can copy it to the viewer machine. */
        if (certificate is not null)
        {
            /* #2117 review catch: name the file whose CONTENT is actually below — root.crt on a
               chain-shaped store, server.crt only on a legacy one. */
            output.WriteLine($"# Server TLS certificate ({Path.GetFileName(distributableCertPath)}) — save as '{clientCertificatePath}' on the viewer machine:");
            output.WriteLine(certificate);
        }

        return 0;
    }

    /* ---------------------------------------------------------------------------------------------------
       REPRINTING AN ENDPOINT TOKEN (#2479, item 2).

       --configure-network shows each generated token's plaintext exactly once. Lose it and the only path
       was regeneration, which invalidates every client already configured against it - so one mislaid
       token during UAT meant re-onboarding every consumer of that endpoint. That is an unrecoverable
       mistake made out of a recoverable one, and the recovery costs nothing to provide.

       WHAT THIS DISCLOSES, checked rather than assumed. Nothing new. darling.json stores the token as a
       DarlingSecrets blob: DPAPI, DataProtectionScope.LocalMachine, with the entropy constant
       "PerformanceMonitor.Darling.v1" compiled into an OPEN-SOURCE binary. LocalMachine scope means any
       process on this box can Unprotect it, and the entropy is not a secret because it is published in
       this repository. So the whole of this verb is four lines of PowerShell to anyone holding the file.

       WHICH IS WHY THE ELEVATION GATE IS NOT A CONFIDENTIALITY BOUNDARY, and this comment says so rather
       than letting the code imply otherwise. install-darling.ps1 and DarlingFileSecurity deliberately grant
       INTERACTIVE *read* on darling.json - the Viewer and these very CLI verbs are run by the interactive
       operator - so an ordinary logged-on user who is NOT an administrator can already read the blob and
       decrypt it. The gate is worth having for what it actually does: it keeps the verb consistent with the
       other endpoint verbs that carry "(run elevated)", it stops a script or a shared shell picking a live
       credential up in passing, and it makes reprinting a deliberate act. The token's real protection is
       the file's ACL, and it always was.

       Emission follows --print-viewer-connection's posture exactly (D8): every warning on STDERR, ahead of
       any STDOUT payload, so redirecting STDOUT to an ACL'd file or the clipboard captures the token
       WITHOUT swallowing the warning. The field report on that verb watched a live password scroll past
       and only then saw the redirect advice, which is precisely backwards.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Reprints the MCP bearer token from <c>mcp.network</c>. See the block above for the
    /// disclosure analysis. Returns 0 when a token was printed, 1 otherwise.
    ///
    /// <para>No <c>Async</c> suffix, deliberately: this reads one file and decrypts one blob, with nothing
    /// to await. It follows <c>HardenFiles</c> rather than the <c>PrintViewerConnectionAsync</c> directly
    /// above, which really is async - naming a synchronous method <c>…Async</c> invites a caller to await
    /// something that never yields (review catch on #2479).</para></summary>
    [SupportedOSPlatform("windows")]
    public static int PrintMcpToken(string? configPath, TextWriter output, TextWriter error)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var network = config.Mcp.Network;
        return PrintEndpointToken(
            "mcp",
            "MCP bearer",
            "--print-mcp-token",
            "Remote MCP clients send it as the header:  Authorization: Bearer <token>",
            IsElevated(),
            network is not null && network.IsConfigured,
            () => network!.ResolveToken(out _),
            output,
            error);
    }

    /// <summary>Reprints the web dashboard access token from <c>web.network</c>. Returns 0 when a token was
    /// printed, 1 otherwise.</summary>
    [SupportedOSPlatform("windows")]
    public static int PrintWebToken(string? configPath, TextWriter output, TextWriter error)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var network = config.Web.Network;
        return PrintEndpointToken(
            "web",
            "web dashboard access",
            "--print-web-token",
            "A remote browser presents it once via ?token=... and gets a session cookie back.",
            IsElevated(),
            network is not null && network.IsConfigured,
            () => network!.ResolveToken(out _),
            output,
            error);
    }

    /// <summary>
    /// The shared body. Split out so the elevation refusal, the disclosure warning and the redirect advice
    /// are written once and cannot drift between the two endpoints - the same reason
    /// <c>DescribeToggleOverride</c> takes a section name rather than existing twice.
    ///
    /// <para><paramref name="elevated"/> is passed IN rather than measured here, the same way the pure alert
    /// gates take <c>nowUtc</c>: it makes both branches - the refusal and the disclosure - drivable in a test
    /// on any runner, elevated or not. A verb whose refusal path is only exercised when the CI agent happens
    /// to be unprivileged is a refusal nobody has checked.</para>
    /// </summary>
    internal static int PrintEndpointToken(
        string section,
        string tokenName,
        string verb,
        string howClientsPresentIt,
        bool elevated,
        bool configured,
        Func<string?> resolveToken,
        TextWriter output,
        TextWriter error)
    {
        if (!elevated)
        {
            error.WriteLine($"{verb} needs an ELEVATED PowerShell.");
            error.WriteLine();
            error.WriteLine("It reprints a live credential, so it is a deliberate act rather than something a script or a");
            error.WriteLine("shared shell picks up in passing.");
            error.WriteLine();
            error.WriteLine("Be clear about what this gate is NOT, though: darling.json grants INTERACTIVE read on purpose");
            error.WriteLine("(the Viewer and these CLI verbs are run by the interactive operator), and the token is DPAPI-");
            error.WriteLine("protected at LocalMachine scope with an entropy constant published in this project's source.");
            error.WriteLine("Anyone who can log on to this box interactively can already decrypt it without this verb. The");
            error.WriteLine("token's protection is the FILE'S ACL - keep darling.json restricted, and run --harden-files if");
            error.WriteLine("you are not sure it still is.");
            return 1;
        }

        if (!configured)
        {
            error.WriteLine($"darling.json has no {section}.network block, so there is no {tokenName} token to print.");
            error.WriteLine($"Run --configure-network to create one (it generates the token and shows it once).");
            return 1;
        }

        string? token;
        try
        {
            token = resolveToken();
        }
        catch (System.Security.Cryptography.CryptographicException ex)
        {
            /* The DPAPI cause, and ONLY the DPAPI cause. ResolveToken also resolves env: and file: secret
               references, and those fail with an InvalidOperationException whose message already names the
               missing variable or the unreadable path (review catch on #2479). Appending "a DPAPI blob only
               decrypts on the machine that produced it - regenerate it" to THOSE would be a false diagnosis
               pointing at a destructive fix: regenerating invalidates every configured client, when the
               actual repair is to set the environment variable or correct the path. So the advice is gated
               on the exception that earns it, and the arm below reports what it actually knows. */
            error.WriteLine($"Could not read the {tokenName} token: {ex.Message}");
            error.WriteLine(
                $"A DPAPI blob only decrypts on the machine that produced it, so a {section}.network.encryptedToken "
                + "copied from another host can never be read here - regenerate it with --configure-network.");
            return 1;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not read the {tokenName} token: {ex.Message}");
            error.WriteLine(
                $"That is what {section}.network resolved to. Fix the source it names (an env: reference needs "
                + "the variable set in THIS shell; a file: reference needs a path this account can read), then "
                + "re-run - the token itself is fine and does not need regenerating.");
            return 1;
        }

        if (string.IsNullOrEmpty(token))
        {
            error.WriteLine($"{section}.network is configured but carries no token, so this endpoint is not LAN-exposed.");
            error.WriteLine("Run --configure-network to generate one.");
            return 1;
        }

        /* Every stderr line before the payload (D8), so a STDOUT redirect keeps the token and still shows
           the warning. */
        error.WriteLine();
        error.WriteLine(
            $"WARNING: the {tokenName} token below is written to STDOUT as PLAINTEXT. It gates ALL network access to "
            + $"this endpoint. Redirect it to an ACL'd file or pipe it to the clipboard; do not leave it in shell "
            + "scrollback, CI logs, or a screenshare.");
        error.WriteLine($"  Example (file):      PerformanceMonitor.Darling.Service.exe {verb} > token.txt");
        error.WriteLine($"  Example (clipboard): PerformanceMonitor.Darling.Service.exe {verb} | clip");
        error.WriteLine($"  {howClientsPresentIt}");
        error.WriteLine(
            "  If this token has actually LEAKED, reprinting it is not the fix: --configure-network generates a new "
            + "one, which invalidates every client already configured against the old one.");
        error.WriteLine();

        output.WriteLine(token);
        return 0;
    }

    /// <summary>
    /// The name the exported/pasted client-side certificate takes on the VIEWER machine, and therefore the
    /// <c>Root Certificate=</c> value both viewer verbs emit. Deliberately the same file name the store
    /// generates, so an operator who copies the folder never has to re-point anything.
    /// </summary>
    public const string ViewerClientCertificateFileName = "server.crt";

    /// <summary>Everything a remote viewer seat needs, resolved from darling.json + the managed store's
    /// on-disk material: where to dial, as whom, with which password, and the server cert to pin. Shared by
    /// <see cref="PrintViewerConnectionAsync"/> and <see cref="ExportViewerConfigAsync"/> so the two verbs
    /// cannot disagree about any of it.</summary>
    /// <param name="CertificatePath">The store-side <c>server.crt</c>; may not exist yet (each verb decides
    /// whether that is fatal — the print verb still has a useful string, the export does not).</param>
    private sealed record ViewerHandoff(
        string Host, int Port, string Role, string Password, string CertificatePath);

    /// <summary>
    /// Resolves the remote-viewer handoff material (D8) for EVERY role the exposure admits (#2665), in
    /// <see cref="DarlingNetwork.NormalizeNetworkRoles"/>' order, writing every refusal + warning to
    /// <paramref name="error"/> and returning null when the caller must exit 1. Managed-mode only: the DPAPI
    /// credential files and the generated TLS cert it reads exist only there — in BYO the operator's own
    /// PostgreSQL governs exposure + credentials (D-BYO). Windows-only (DPAPI-LocalMachine). The two string
    /// parameters shape message WORDING only — never the logic, so both verbs resolve identically.
    /// <para>Fail-closed on the FIRST role whose credential is missing or will not decrypt, rather than
    /// returning the roles that worked. Both credentials are provisioned in the same act by
    /// <c>DarlingManagedRoles</c>, so one missing means the store's bootstrap did not complete — which is
    /// what the shared missing-credential diagnostic below actually explains. A partial handoff would bury
    /// that under an export that looks like it succeeded.</para>
    /// </summary>
    /// <param name="verb">The CLI verb to name in refusals, e.g. <c>--export-viewer-config</c>.</param>
    /// <param name="action">What the verb would have produced, e.g. "print" / "export", for the no-remote-connection refusal.</param>
    [SupportedOSPlatform("windows")]
    private static async Task<IReadOnlyList<ViewerHandoff>?> ResolveViewerHandoffsAsync(
        DarlingConfig config, string verb, string action, TextWriter error, CancellationToken cancellationToken)
    {
        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return null;
        }

        if (!postgres.Managed)
        {
            error.WriteLine(
                $"{verb} is for the managed store only. In bring-your-own mode " +
                "(postgres.connectionString), your own PostgreSQL governs network exposure and credentials — " +
                "build the remote viewer's connection string from your own role + TLS setup.");
            return null;
        }

        /* Every pg_hba login role the network exposure names — default viewer (read-only, the secure default).
           An explicitly-invalid value is a hard error: the store degrades to loopback for it, so no remote
           connection exists at all. */
        var network = postgres.Network;
        var roles = DarlingNetwork.NormalizeNetworkRoles(network?.Role);
        if (roles is null || roles.Count == 0)
        {
            error.WriteLine(
                $"postgres.network.role '{network?.Role}' is invalid — it must be \"viewer\" (default, read-only), " +
                $"\"admin\", or both (e.g. \"admin,viewer\"). The store degrades to loopback for an unknown role, so " +
                $"there is no remote connection to {action}.");
            return null;
        }

        /* Warn (not fail) when the store is not actually network-exposed: the operator still gets a template,
           but the endpoint will not accept it until postgres.network.listen is set and the service restarted. */
        if (!DarlingNetwork.IsExposedListenAddress(network?.Listen))
        {
            error.WriteLine(
                "WARNING: postgres.network.listen is not a network address, so the managed store is loopback-only " +
                "right now. Set postgres.network (listen + allowFrom) and restart the service to expose it (which " +
                "also generates the TLS cert), then re-run this command.");
        }

        var host = ResolveViewerHost(network?.Listen);

        /* Decrypt each role's DPAPI-LocalMachine credential (Windows-only; the caller is IsWindows-guarded).
           Every credential and the cert live in the same directory (ParentOf(dataDirectory)), so the cert
           path is role-independent. */
        var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(postgres);
        var certificatePath = Path.Combine(
            Path.GetDirectoryName(DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory))!,
            DarlingManagedPostgres.ServerCertFileName);

        var handoffs = new List<ViewerHandoff>(roles.Count);
        foreach (var role in roles)
        {
            var credentialPath = string.Equals(role, "admin", StringComparison.Ordinal)
                ? DarlingManagedPostgres.AdminCredentialPathFor(dataDirectory)
                : DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory);

            if (!File.Exists(credentialPath))
            {
                /* #2197: which of the two things this means is decided from the store's own files, not assumed.
                   A bootstrap that has already failed produces this same absence, and telling THAT operator to
                   start the service again is the dead end the field report walked into. */
                error.WriteLine(DarlingStoreBootstrapEvidence.MissingCredentialMessage(
                    $"The '{role}' role credential ({credentialPath})",
                    "provisions the least-privilege roles and their credentials",
                    dataDirectory));
                return null;
            }

            string password;
            try
            {
                password = DarlingSecrets.Unprotect((await File.ReadAllTextAsync(credentialPath, cancellationToken)).Trim());
            }
            catch (Exception ex)
            {
                error.WriteLine(
                    $"Could not decrypt the '{role}' credential at {credentialPath}: {ex.Message} (DPAPI-LocalMachine — " +
                    "run this on the same machine as the service, under an account that can read the credential).");
                return null;
            }

            handoffs.Add(new ViewerHandoff(host, postgres.Port, role, password, certificatePath));
        }

        return handoffs;
    }

    /// <summary>
    /// The single seat <c>--export-viewer-config</c> writes when the exposure admits both roles (#2665): the
    /// LEAST-PRIVILEGE one. The verb produces one folder holding one live credential, and the folder is what
    /// gets handed to somebody else — so where there is a choice it must be the read-only seat, the same call
    /// D7 makes for the default. The admin string stays one <c>--print-viewer-connection</c> away, and the
    /// verb says so rather than leaving the choice invisible. Pure.
    /// </summary>
    private static ViewerHandoff SelectExportSeat(IReadOnlyList<ViewerHandoff> handoffs)
    {
        foreach (var handoff in handoffs)
        {
            if (string.Equals(handoff.Role, "viewer", StringComparison.Ordinal))
            {
                return handoff;
            }
        }

        return handoffs[0];
    }

    /// <summary>
    /// #1953 — writes the viewer machine's COMPLETE handoff folder: a ready-to-use <c>darling.json</c> (the
    /// resolved connection string, <c>"managed": false</c> already set, every field commented in place), the
    /// store's <c>server.crt</c> beside it, and a <c>README.txt</c> that documents the fields — including the
    /// valid <c>Root Certificate=</c> values — for an operator who never opens the JSON. The field report that
    /// motivated this had to hand-merge <c>--print-viewer-connection</c>'s output into JSON copied out of the
    /// docs and discover the <c>managed</c> flip by trial; nobody writes that file by hand anymore.
    /// <para>Managed-mode + Windows only (same DPAPI/TLS material as <see cref="PrintViewerConnectionAsync"/>,
    /// resolved by the same helper). The cert is REQUIRED here, unlike the print verb: a folder whose
    /// verify-full connection cannot be completed is exactly the half-finished handoff this verb exists to
    /// kill, so a missing cert fails with the reason instead of exporting something broken.</para>
    /// <para><b>The written darling.json holds a LIVE credential</b> — the verb says so on STDERR, naming the
    /// file, before writing it, then ACLs it to SYSTEM + Administrators + this account + INTERACTIVE and
    /// CONFIRMS that (returning 2, not 0, when the secret is still readable — "exported" must not read as
    /// "protected" to a script). The password value itself is never echoed.</para>
    /// <para>It also refuses destinations rather than clobbering them, all decided BEFORE any config load or
    /// credential decrypt because they are pure path questions whose failure cannot be undone: the service's
    /// OWN config directory (exporting there replaced darling.json with the viewer's, destroying its servers,
    /// encrypted passwords and tokens — reproduced, and one keystroke from a legitimate command); a
    /// darling.json this verb did not write (an operator's file, or one pre-created by a local user who would
    /// keep OWNERSHIP through the harden — a Windows owner keeps WRITE_DAC); and a junction/symlink
    /// destination (creating one needs no privilege, and it redirects the cleartext credential). Re-exporting
    /// over its OWN output is silent — that is the documented step after a rotation.</para>
    /// </summary>
    /// <param name="outputDirectory">Where to write; null = a <c>viewer-config</c> folder beside darling.json.</param>
    /// <returns>0 exported and protected; 1 refused or failed; 2 exported but the secret is NOT protected.</returns>
    [SupportedOSPlatform("windows")]
    public static async Task<int> ExportViewerConfigAsync(
        string? configPath, string? outputDirectory, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        var servicePath = DarlingConfig.ResolveConfigPath(configPath);
        var targetDirectory = ResolveViewerExportDirectory(servicePath, outputDirectory);

        /* Destination guards run FIRST — before any config load or credential decrypt — because they are pure
           path questions and because the failure they prevent is unrecoverable. Nothing below has touched
           DPAPI yet, so a bad argument costs nothing. */
        if (File.Exists(targetDirectory)
            || Path.TrimEndingDirectorySeparator(targetDirectory).EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            /* This verb's positional argument is the DESTINATION, while every sibling verb's is a config path,
               so an operator with sibling muscle memory types darling.json here. */
            error.WriteLine(
                $"'{targetDirectory}' is a file, not a directory. This verb's argument is the DESTINATION folder " +
                "for the exported viewer files; to point it at a different darling.json, use --config <path> " +
                "(e.g. --export-viewer-config D:\\handoff --config C:\\Darling\\darling.json).");
            return 1;
        }

        var exportedConfigPath = Path.Combine(targetDirectory, ViewerConfigFileName);

        /* The unrecoverable one: exporting INTO the service's own config directory would overwrite the
           service's darling.json with the viewer's, destroying every monitored server, every DPAPI
           encryptedPassword, and the MCP/web tokens — none of which exist anywhere else. One keystroke from
           a legitimate command (the install directory is the obvious place to put a handoff folder).
           #1973 — this compare is LEXICAL: Path.GetFullPath normalizes text and does not resolve links, so a
           junction on an ANCESTOR of the destination (say C:\Data\handoff, where C:\Data links into the
           service's own directory) reads as a different path here and slips past — as it does past the leaf
           junction check below, whose attributes describe only the last component. Deliberately NOT chased
           with a chain-resolving walk, because the destructive case is already refused one layer down: the
           per-FILE TryCheckExportTarget goes through the OS's REAL path resolution (File.GetAttributes and
           File.ReadAllText follow the whole junction chain to the actual file), and the service's own
           darling.json carries no export marker, so such a run stops with "not written by
           --export-viewer-config" before anything is deleted. What survives the gap is only "the files land
           in a directory the junction-planter chose" — and planting a junction in the ancestor chain of the
           operator's destination already means controlling that destination's fate. */
        if (string.Equals(
                Path.GetFullPath(exportedConfigPath), Path.GetFullPath(servicePath), StringComparison.OrdinalIgnoreCase))
        {
            error.WriteLine(
                $"Refusing to export into {targetDirectory}: that would overwrite the SERVICE's own {ViewerConfigFileName} " +
                $"({servicePath}) with the viewer's, destroying its monitored servers, encrypted passwords and tokens. " +
                "Name a different destination — the default (no argument) is a viewer-config subfolder beside it.");
            return 1;
        }

        /* A destination that is a junction/symlink is refused: creating one needs no privilege on Windows, so
           it is how an unprivileged local user redirects a cleartext credential into a directory they control.
           #1973 — leaf-only, and knowingly so: DirectoryInfo.Attributes describes THIS directory, never the
           ones above it, so this shares the ancestor-junction blind spot recorded on the guard above, and the
           same per-file marker check in TryCheckExportTarget is the backstop that actually holds. */
        if (Directory.Exists(targetDirectory)
            && new DirectoryInfo(targetDirectory).Attributes.HasFlag(FileAttributes.ReparsePoint))
        {
            error.WriteLine(
                $"Refusing to export into {targetDirectory}: it is a junction or symbolic link, so the real " +
                "destination is somewhere else. Export to a real directory.");
            return 1;
        }

        /* Overwrite only what THIS verb wrote — for ALL THREE files, not just the secret-bearing one. They
           land in the same operator-nameable directory, so each is equally a place a local user can plant a
           symlink to redirect the write, or pre-create a file to keep OWNERSHIP of (a Windows owner retains
           WRITE_DAC no matter what DACL is applied afterwards). Re-exporting after a credential or
           certificate rotation is a documented workflow, so an export of ours is replaced silently; anything
           else stops the run. Checked here, before the config load and the credential decrypt, because these
           are pure filesystem questions — and check-only: nothing is deleted until the write itself, so a run
           that fails later has not thrown away the previous export. */
        foreach (var (path, marker) in new[]
        {
            (exportedConfigPath, ViewerConfigMarker),
            (Path.Combine(targetDirectory, ViewerClientCertificateFileName), CertificatePemMarker),
            (Path.Combine(targetDirectory, ViewerReadmeFileName), ViewerReadmeMarker),
        })
        {
            if (!TryCheckExportTarget(path, marker, error))
            {
                return 1;
            }
        }

        if (targetDirectory.StartsWith(@"\\", StringComparison.Ordinal))
        {
            error.WriteLine(
                $"WARNING: {targetDirectory} is a network path. The exported {ViewerConfigFileName} holds a live " +
                "password, and the file ACL this verb applies locally does not necessarily hold on a remote share — " +
                "check the share's permissions yourself.");
        }

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var handoffs = await ResolveViewerHandoffsAsync(
            config, "--export-viewer-config", "export", error, cancellationToken);
        if (handoffs is null)
        {
            return 1;
        }

        /* One folder holds one live credential, so a multi-role exposure has to CHOOSE — and the folder is
           what gets handed to somebody else, so the choice is the read-only seat (see SelectExportSeat).
           Saying which, and where the other one is, because an operator who set "admin,viewer" and got a
           folder back would otherwise have no way to know a seat was picked for them. */
        var handoff = SelectExportSeat(handoffs);
        if (handoffs.Count > 1)
        {
            error.WriteLine(
                $"NOTE: postgres.network.role admits '{string.Join("', '", handoffs.Select(h => h.Role))}'. This folder is the " +
                $"'{handoff.Role}' seat (the least-privileged of them). For another role's connection string, run " +
                "--print-viewer-connection, which prints one per admitted role.");
        }

        string certificate;
        if (!File.Exists(handoff.CertificatePath))
        {
            error.WriteLine(
                $"Cannot export: the server TLS certificate ({handoff.CertificatePath}) does not exist yet, and the " +
                "exported connection uses SSL Mode=VerifyFull — without the cert the viewer could not connect. The " +
                "service generates it on its first managed start with postgres.network exposed: set postgres.network " +
                "(listen + allowFrom), restart the service, then re-run this command.");
            return 1;
        }

        /* #2117: prefer the distributable ROOT on chain-shaped stores — the print verb's rule. */
        var exportCertPath = DarlingManagedPostgres.RootCertificatePathFor(handoff.CertificatePath);
        if (!File.Exists(exportCertPath))
        {
            exportCertPath = handoff.CertificatePath;
        }

        try
        {
            certificate = (await File.ReadAllTextAsync(exportCertPath, cancellationToken)).Trim();
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not read the server TLS certificate ({exportCertPath}): {ex.Message}");
            return 1;
        }

        var configFile = exportedConfigPath;
        var certificateFile = Path.Combine(targetDirectory, ViewerClientCertificateFileName);
        var readmeFile = Path.Combine(targetDirectory, ViewerReadmeFileName);

        /* The live-secret warning goes out BEFORE the write, naming the file — so an operator who walks away
           mid-run has still been told a credential-bearing file is landing there (#1953 item 3's ordering rule,
           applied to the verb that writes the secret rather than printing it). */
        error.WriteLine();
        error.WriteLine(
            $"WARNING: {configFile} will contain a LIVE database password (the '{handoff.Role}' role) in cleartext, " +
            "because that is what the viewer connects with. Copy the folder over a channel you trust, and keep the " +
            "file ACL'd to the operator account on the viewer machine.");

        /* One timestamp for the whole export: the two files date the same act, and sampling the clock twice
           can straddle a second boundary and print two. */
        var generatedUtc = DateTimeOffset.UtcNow;

        bool secretProtected;
        try
        {
            Directory.CreateDirectory(targetDirectory);

            await WriteExportFileAsync(
                configFile,
                BuildViewerConfigJson(
                    BuildViewerConnectionString(
                        handoff.Host, handoff.Port, handoff.Role, handoff.Password, ViewerClientCertificateFileName),
                    generatedUtc),
                cancellationToken);

            /* Only this file gets an ACL: it is the only one carrying a secret. The certificate is public by
               construction and the README has nothing in it — but all three go through the same
               replace-safely path above, because the ownership and symlink problems are about the PATH, not
               about what the file contains. */
            secretProtected = TryHardenExportedSecret(configFile, error);

            await WriteExportFileAsync(certificateFile, certificate + Environment.NewLine, cancellationToken);
            await WriteExportFileAsync(
                readmeFile,
                BuildViewerConfigReadme(handoff.Host, handoff.Port, handoff.Role, generatedUtc),
                cancellationToken);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not write the viewer config to {targetDirectory}: {ex.Message}");

            /* A write that died part-way can leave a file holding the start of a live password. Remove it, and
               say so either way — "the export failed" must not leave an unmentioned secret on disk.
               #1973 — and sweep the OTHER TWO as well, not just the secret-bearing one: each write DELETES the
               previous export's copy before recreating it, so a failure part-way through leaves a mix of new
               and stale files with no config to go with them — the half-finished handoff this verb exists to
               refuse. The cert and the README are always safe to remove (neither carries a secret and neither
               is usable without the config), so the folder is emptied of this verb's files rather than left
               looking exportable. Every file still on disk is NAMED; each removal stands alone, so one that
               fails masks neither the original error nor the other removals. */
            TryRemovePartialExport(
                configFile,
                "it held a live password — the whole of one if the write that failed was a later file",
                "it holds a live password",
                error);
            TryRemovePartialExport(
                certificateFile,
                "cleanup — no secret in it, but it belonged to an export that did not finish",
                "it is a leftover of the failed export, not a secret",
                error);
            TryRemovePartialExport(
                readmeFile,
                "cleanup — no secret in it, but it belonged to an export that did not finish",
                "it is a leftover of the failed export, not a secret",
                error);

            return 1;
        }

        if (string.Equals(handoff.Role, "admin", StringComparison.Ordinal))
        {
            error.WriteLine(
                "NOTE: 'admin' is a WRITE credential holding the config-table pivot surface. Prefer the default " +
                "'viewer' (read-only) for a remote seat; if you must use 'admin', NTFS-ACL the exported file on the " +
                "viewer machine too.");
        }

        error.WriteLine();
        error.WriteLine("Copy the whole folder to the viewer machine and put the three files NEXT TO the Viewer");
        error.WriteLine("executable — that works unedited. To keep them elsewhere, point DARLING_CONFIG at the");
        error.WriteLine($"{ViewerConfigFileName} — also unedited, because a bare \"Root Certificate\" resolves against the");
        error.WriteLine($"folder holding {ViewerConfigFileName}, wherever that is. {ViewerReadmeFileName} explains every field.");
        error.WriteLine("Re-run this command after the store's certificate is regenerated (a changed bind IP rotates it).");
        error.WriteLine();

        output.WriteLine(targetDirectory);
        output.WriteLine(configFile);
        output.WriteLine(certificateFile);
        output.WriteLine(readmeFile);

        /* The files exist and work, but an unprotected cleartext credential is not a success an automation
           should read as one — exit non-zero so a script stops, with the manifest already printed so the
           operator can act on the exact file. */
        return secretProtected ? 0 : 2;
    }

    /// <summary>The exported viewer folder's file names — the JSON the Viewer resolves by name, the cert its
    /// <c>Root Certificate=</c> points at, and the operator-facing field reference (#1953 item 4).</summary>
    public const string ViewerConfigFileName = "darling.json";
    public const string ViewerReadmeFileName = "README.txt";

    /// <summary>
    /// The first comment line of an exported viewer config, and the way a re-export tells ITS OWN previous
    /// output apart from a file it must not clobber (an operator's real darling.json, or a pre-created one an
    /// attacker wants to keep ownership of). Re-exporting after a rotation is a documented workflow, so our
    /// own file is replaced silently; anything lacking this line stops the run.
    /// </summary>
    public const string ViewerConfigMarker = "PerformanceMonitor Darling - VIEWER configuration.";

    /// <summary>The README's own first line — its half of the same identity check.</summary>
    public const string ViewerReadmeMarker = "PerformanceMonitor Darling - remote Viewer setup";

    /// <summary>What a <c>server.crt</c> at the destination must look like to be replaceable: a PEM
    /// certificate block. The exported cert is a copy of the store's, so unlike the other two it carries no
    /// marker of ours — but a file at that path that is not a certificate at all is still someone else's,
    /// and replacing it silently is the behavior being prevented.</summary>
    public const string CertificatePemMarker = "-----BEGIN CERTIFICATE-----";

    /// <summary>
    /// Parses <c>--export-viewer-config</c>'s arguments STRICTLY, in the spirit of #1581's classifier: never
    /// guess. A last-wins loop like the sibling <c>--dry-run</c> verbs' would take an unrecognized flag as the
    /// DESTINATION — and a bare <c>--config</c> with no value would write a live cleartext password to a
    /// folder literally named <c>--config</c>, under whatever the working directory is (for the elevated
    /// prompt the docs prescribe, <c>C:\Windows\System32</c>). Returns false with a ready-to-print
    /// <paramref name="errorMessage"/> on anything unrecognized. Pure — unit-testable, which is why it lives
    /// here rather than inline in Program.
    /// </summary>
    /// <param name="rest">The arguments AFTER the verb itself.</param>
    public static bool TryParseExportViewerConfigArgs(
        string[] rest, out string? configPath, out string? outputDirectory, out string? errorMessage)
    {
        configPath = null;
        outputDirectory = null;
        errorMessage = null;

        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            if (string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= rest.Length)
                {
                    errorMessage = "--config needs a path: --export-viewer-config [directory] --config <path to darling.json>";
                    return false;
                }

                configPath = rest[++i];
                continue;
            }

            if (arg.StartsWith('-'))
            {
                errorMessage =
                    $"Unknown option for --export-viewer-config: {arg}" + Environment.NewLine +
                    "Usage: --export-viewer-config [destination directory] [--config <path to darling.json>]";
                return false;
            }

            if (outputDirectory is not null)
            {
                errorMessage = $"--export-viewer-config takes ONE destination directory; got '{outputDirectory}' and '{arg}'.";
                return false;
            }

            outputDirectory = arg;
        }

        return true;
    }

    /// <summary>
    /// Where <see cref="ExportViewerConfigAsync"/> writes: the operator's directory when they named one,
    /// else a <c>viewer-config</c> folder beside the service's darling.json — next to the file the operator
    /// already knows, not buried under ProgramData. Pure (no I/O), so it pins directly.
    /// </summary>
    public static string ResolveViewerExportDirectory(string configPath, string? outputDirectory)
    {
        if (!string.IsNullOrWhiteSpace(outputDirectory))
        {
            return Path.GetFullPath(outputDirectory.Trim());
        }

        var configDirectory = Path.GetDirectoryName(Path.GetFullPath(configPath));
        return Path.Combine(
            string.IsNullOrEmpty(configDirectory) ? AppContext.BaseDirectory : configDirectory,
            "viewer-config");
    }

    /// <summary>
    /// The exported viewer <c>darling.json</c> (#1953 items 1 + 4): the two fields the Viewer actually reads —
    /// <c>managed: false</c> and the verbatim connection string — with every field, and the valid
    /// <c>Root Certificate=</c> values, documented in <c>//</c> comments IN the file. The Viewer parses with
    /// <c>JsonCommentHandling.Skip</c> + <c>AllowTrailingCommas</c> (the same tolerance darling.sample.json
    /// relies on), so the commented file is consumed as-is — <see cref="!:ViewerSettings"/> is pinned against
    /// this text by <c>DarlingExportViewerConfigTests</c>. Hand-composed rather than serialized precisely
    /// because <c>System.Text.Json</c> cannot WRITE comments, and the comments are the point.
    /// Pure — unit-testable.
    /// </summary>
    public static string BuildViewerConfigJson(string connectionString, DateTimeOffset generatedUtc) =>
        "{" + Environment.NewLine +
        "  // " + ViewerConfigMarker + Environment.NewLine +
        $"  // Generated by --export-viewer-config at {generatedUtc.ToUniversalTime():yyyy-MM-dd HH:mm:ss}Z. Nothing here needs hand-editing." + Environment.NewLine +
        "  //" + Environment.NewLine +
        "  // THIS FILE CONTAINS A LIVE DATABASE PASSWORD. Keep it ACL'd to the operator account." + Environment.NewLine +
        "  //" + Environment.NewLine +
        "  // WHERE TO PUT THESE FILES on the viewer machine:" + Environment.NewLine +
        "  //   Keep the three together, anywhere. Nothing here needs editing either way, because a" + Environment.NewLine +
        "  //   bare Root Certificate name resolves against the folder holding darling.json - not" + Environment.NewLine +
        "  //   against the folder the Viewer happens to be launched from." + Environment.NewLine +
        "  //" + Environment.NewLine +
        "  //   Copy all three next to the Viewer executable and it finds darling.json by itself." + Environment.NewLine +
        "  //   To keep the folder somewhere else instead, point the DARLING_CONFIG environment" + Environment.NewLine +
        "  //   variable at this file - the server.crt beside it still resolves, nothing to edit." + Environment.NewLine +
        "  //   (The Viewer also looks one folder up from itself, for the release zip's viewer\\ layout.)" + Environment.NewLine +
        "  \"postgres\": {" + Environment.NewLine +
        "    // false = this machine does not RUN a store, it connects to one. The service host's own" + Environment.NewLine +
        "    // darling.json says true; that flag is about who OWNS the PostgreSQL, not who is connecting." + Environment.NewLine +
        "    // A viewer left on true would hunt for a bundled local PostgreSQL that is not there." + Environment.NewLine +
        "    \"managed\": false," + Environment.NewLine +
        Environment.NewLine +
        "    // Consumed VERBATIM by the Viewer (Npgsql keywords):" + Environment.NewLine +
        "    //   Host / Port      the service host's exposed store endpoint (its postgres.network.listen + postgres.port)" + Environment.NewLine +
        "    //   Username         the least-privilege store role: viewer = read-only, admin = read + config writes" + Environment.NewLine +
        "    //   Password         that role's live password (rotate by re-running the export after a rotation)" + Environment.NewLine +
        "    //   Database         always darling" + Environment.NewLine +
        "    //   Search Path      collect,config,public - resolves the bare table names the Viewer queries" + Environment.NewLine +
        "    //   SSL Mode         VerifyFull - encrypted AND the server certificate must match Host. Do not" + Environment.NewLine +
        "    //                    downgrade it to Require: that keeps the encryption but stops verifying the" + Environment.NewLine +
        "    //                    certificate, so Root Certificate below would be ignored entirely." + Environment.NewLine +
        "    //   Root Certificate the server.crt exported beside this file. A relative value resolves" + Environment.NewLine +
        "    //                    against the folder holding darling.json, so the name below is correct" + Environment.NewLine +
        "    //                    wherever you keep these files and however the Viewer is launched." + Environment.NewLine +
        "    //                    Valid values:" + Environment.NewLine +
        "    //                      server.crt              a bare name - the certificate exported" + Environment.NewLine +
        "    //                                              beside this file" + Environment.NewLine +
        "    //                      certs\\server.crt        a relative subpath - same anchor" + Environment.NewLine +
        "    //                      C:\\Darling\\server.crt   an absolute path - used exactly as written," + Environment.NewLine +
        "    //                                              for a certificate kept elsewhere" + Environment.NewLine +
        "    //                    It must be the PEM the SERVICE generated (exported beside this file);" + Environment.NewLine +
        "    //                    VerifyFull rejects any other certificate, including a re-issued one." + Environment.NewLine +
        $"    \"connectionString\": {JsonSerializer.Serialize(connectionString)}" + Environment.NewLine +
        "  }" + Environment.NewLine +
        "}" + Environment.NewLine;

    /// <summary>
    /// The exported <c>README.txt</c> (#1953 item 4): the same field reference as the JSON's comments, for an
    /// operator who never opens the JSON, plus the one-line "copy this folder" instruction. Takes no password
    /// — the credential lives in exactly one exported file and is never echoed anywhere else. Pure —
    /// unit-testable.
    /// </summary>
    public static string BuildViewerConfigReadme(string host, int port, string role, DateTimeOffset generatedUtc) =>
        "PerformanceMonitor Darling - remote Viewer setup" + Environment.NewLine +
        "================================================" + Environment.NewLine +
        Environment.NewLine +
        $"Generated by --export-viewer-config at {generatedUtc.ToUniversalTime():yyyy-MM-dd HH:mm:ss}Z on the service host." + Environment.NewLine +
        Environment.NewLine +
        "TO USE IT" + Environment.NewLine +
        "  Copy these three files next to the Viewer executable on the viewer machine, then start the" + Environment.NewLine +
        "  Viewer. Nothing to edit." + Environment.NewLine +
        Environment.NewLine +
        "  To keep the folder somewhere else instead, set the DARLING_CONFIG environment variable to the" + Environment.NewLine +
        "  darling.json inside it. Still nothing to edit: the Viewer resolves a bare Root Certificate" + Environment.NewLine +
        "  name against the folder holding darling.json, so the server.crt beside it is found wherever" + Environment.NewLine +
        "  you keep the folder. See THE FIELDS below." + Environment.NewLine +
        Environment.NewLine +
        "WHAT IS IN HERE" + Environment.NewLine +
        $"  {ViewerConfigFileName}      the Viewer's configuration. CONTAINS A LIVE DATABASE PASSWORD - treat it as a secret," + Environment.NewLine +
        "                    ACL it to your account, and do not commit it or mail it around." + Environment.NewLine +
        $"  {ViewerClientCertificateFileName}        the store's TLS certificate. The connection pins THIS file; the Viewer will" + Environment.NewLine +
        "                    refuse any other certificate." + Environment.NewLine +
        $"  {ViewerReadmeFileName}        this file." + Environment.NewLine +
        Environment.NewLine +
        "THE FIELDS" + Environment.NewLine +
        "  managed = false   This machine does not RUN a store, it connects to one over the network. The" + Environment.NewLine +
        "                    SERVICE host's own darling.json says managed = true, which is about who OWNS" + Environment.NewLine +
        "                    the PostgreSQL, not who connects. A viewer left on true looks for a bundled" + Environment.NewLine +
        "                    local PostgreSQL that does not exist there and fails." + Environment.NewLine +
        "  connectionString  Handed to Npgsql verbatim. This export's values:" + Environment.NewLine +
        Environment.NewLine +
        $"      Host={host} / Port={port}" + Environment.NewLine +
        "          the exposed store endpoint on the service host." + Environment.NewLine +
        $"      Username={role}" + Environment.NewLine +
        "          the least-privilege store role: viewer = read-only, admin = read plus the" + Environment.NewLine +
        "          Viewer's config writes." + Environment.NewLine +
        "      Password=<that role's live password>" + Environment.NewLine +
        "          filled in for you. It is in darling.json and nowhere else, including this file." + Environment.NewLine +
        "      Database=darling" + Environment.NewLine +
        "          always." + Environment.NewLine +
        "      Search Path=collect,config,public" + Environment.NewLine +
        "          resolves the bare table names the Viewer queries." + Environment.NewLine +
        "      SSL Mode=VerifyFull" + Environment.NewLine +
        "          encrypted AND the server certificate must match Host. Do NOT downgrade this to" + Environment.NewLine +
        "          Require: the encryption stays, the verification stops, and Root Certificate is" + Environment.NewLine +
        "          then ignored entirely." + Environment.NewLine +
        $"      Root Certificate={ViewerClientCertificateFileName}" + Environment.NewLine +
        "          the certificate exported beside darling.json. A relative value resolves against" + Environment.NewLine +
        "          the folder holding darling.json, so this name is correct wherever you keep these" + Environment.NewLine +
        "          three files and however the Viewer is launched. Valid values:" + Environment.NewLine +
        "            server.crt              a bare name - the certificate exported beside this file." + Environment.NewLine +
        "            certs\\server.crt        a relative subpath - same anchor." + Environment.NewLine +
        "            C:\\Darling\\server.crt   an absolute path - used exactly as written, for a" + Environment.NewLine +
        "                                    certificate kept elsewhere." + Environment.NewLine +
        "          It must be the PEM the SERVICE generated; VerifyFull rejects any other" + Environment.NewLine +
        "          certificate, including a re-issued one." + Environment.NewLine +
        Environment.NewLine +
        "IF IT DOES NOT CONNECT" + Environment.NewLine +
        "  - Certificate errors: keep server.crt in the same folder as darling.json (that is what a bare" + Environment.NewLine +
        "    Root Certificate name resolves to), or point Root Certificate at wherever you moved it." + Environment.NewLine +
        "    The Viewer's startup log and its connection-failure window print the absolute path it opened." + Environment.NewLine +
        "  - The store's certificate is regenerated when its bind IP changes. Re-run --export-viewer-config" + Environment.NewLine +
        "    on the service host and replace this folder." + Environment.NewLine +
        $"  - The service host must be reachable on port {port} (its firewall rule is scoped to the allowed CIDR)," + Environment.NewLine +
        "    and its postgres.network block must still name this network." + Environment.NewLine;

    /// <summary>
    /// Decides whether one export target can be written, WITHOUT touching it: refuses a symlink/junction at
    /// the path (it redirects the write to wherever the link points, and creating one needs no privilege on
    /// Windows), and refuses an existing file that is not recognizably a previous export's. Applied to all
    /// three exported files — the ownership and redirection problems belong to the PATH, not to what the file
    /// happens to contain, so the certificate and README get the same treatment as the secret.
    /// <para>The link check reads the path's ATTRIBUTES, which describe the LINK; every other file API here
    /// follows it to the target. Without this check a planted link is not merely written through — a link to
    /// a target that does not exist yet still reports as existing on Windows, so the identity check below
    /// would try to read it, fail, and refuse with a puzzling "could not read" instead of naming the link.</para>
    /// </summary>
    private static bool TryCheckExportTarget(string path, string ourMarker, TextWriter error)
    {
        FileAttributes attributes;
        try
        {
            attributes = File.GetAttributes(path);
        }
        catch (FileNotFoundException)
        {
            return true;
        }
        catch (DirectoryNotFoundException)
        {
            /* The destination folder does not exist yet — the normal first-export case. */
            return true;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Refusing to write {path}: could not inspect it ({ex.Message}).");
            return false;
        }

        if (attributes.HasFlag(FileAttributes.ReparsePoint))
        {
            var target = TryDescribeLinkTarget(path);
            error.WriteLine(
                $"Refusing to write {path}: it is a symbolic link{target}, so the real destination is somewhere " +
                "else. Export to a directory that does not contain one.");
            return false;
        }

        if (!File.Exists(path))
        {
            return true;
        }

        string existing;
        try
        {
            existing = File.ReadAllText(path);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not read the existing {path} to check whether it is ours: {ex.Message}");
            return false;
        }

        if (!existing.Contains(ourMarker, StringComparison.Ordinal))
        {
            error.WriteLine(
                $"Refusing to overwrite {path}: it was not written by --export-viewer-config. " +
                "Export to an empty directory, or move that file aside first.");
            return false;
        }

        return true;
    }

    /// <summary>Best-effort " (to X)" for the refusal message; naming the target is a convenience, so a
    /// failure to resolve it must not change the refusal itself.</summary>
    private static string TryDescribeLinkTarget(string path)
    {
        try
        {
            return File.ResolveLinkTarget(path, returnFinalTarget: false) is { } link
                ? $" (to {link.FullName})"
                : string.Empty;
        }
        catch (Exception)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Best-effort removal of ONE file left by a failed export, naming the outcome either way (#1973). The
    /// verb's premise is that it refuses to hand over a partial setup, so a failure that leaves anything on
    /// disk has to say WHICH file — and a removal that itself fails must neither mask the original error nor
    /// stop the other removals, which is why every path out of here is a message rather than a throw.
    /// </summary>
    /// <param name="path">The exported file to remove.</param>
    /// <param name="whyRemoved">Parenthetical for the "Removed" line: for darling.json, that it held the secret.</param>
    /// <param name="whatItIs">What a survivor is, for the "left behind" line — the operator has to decide what to do about it.</param>
    private static void TryRemovePartialExport(string path, string whyRemoved, string whatItIs, TextWriter error)
    {
        try
        {
            /* File.GetAttributes rather than File.Exists, and for the same reason TryCheckExportTarget uses
               it: Exists answers FALSE for a dangling symlink, which would leave a planted link unmentioned
               by the one sweep whose whole job is to account for what is on disk. */
            _ = File.GetAttributes(path);
        }
        catch (FileNotFoundException)
        {
            /* The run never got this far — nothing on disk, so nothing to name. */
            return;
        }
        catch (DirectoryNotFoundException)
        {
            return;
        }
        catch (Exception inspectFailure)
        {
            error.WriteLine(
                $"WARNING: could not check whether {path} was left behind ({inspectFailure.Message}) — " +
                $"if it is there, {whatItIs}. Check it yourself.");
            return;
        }

        try
        {
            File.Delete(path);
            error.WriteLine($"Removed {path} ({whyRemoved}).");
        }
        catch (Exception cleanupFailure)
        {
            error.WriteLine(
                $"WARNING: {path} was left behind ({cleanupFailure.Message}) — {whatItIs}. Delete it yourself.");
        }
    }

    /// <summary>
    /// Writes one exported file so that THIS process owns it. Delete-then-<see cref="FileMode.CreateNew"/>
    /// rather than a truncating overwrite: an overwrite keeps the existing file's OWNER, and a Windows owner
    /// keeps <c>WRITE_DAC</c> — so writing over a file someone else created would leave them able to undo the
    /// ACL applied afterwards. <c>CreateNew</c> also closes the gap between
    /// <see cref="TryCheckExportTarget"/> and this write: anything that reappears at the path in between
    /// makes the create FAIL rather than be written through.
    /// </summary>
    private static async Task WriteExportFileAsync(string path, string content, CancellationToken cancellationToken)
    {
        File.Delete(path);

        var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        await using (stream.ConfigureAwait(false))
        {
            var writer = new StreamWriter(stream);
            await using (writer.ConfigureAwait(false))
            {
                await writer.WriteAsync(content.AsMemory(), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// ACLs the exported darling.json down to SYSTEM + Administrators + this account + INTERACTIVE (the
    /// admin/viewer-credential posture — the Viewer reads it interactively), so it does not sit in a folder
    /// that inherited BUILTIN\Users read, then CONFIRMS the result. Returns whether the file is protected —
    /// "we tried" is not the same claim as "the secret is not readable", which is why the credential-writing
    /// harden sites pair the call with <see cref="DarlingFileSecurity.IsReadableByOrdinaryUsers"/> and why
    /// the caller's exit code depends on this answer. Never throws: the export's value is the file, so a
    /// failure is reported with the icacls that fixes it rather than throwing the folder away.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static bool TryHardenExportedSecret(string path, TextWriter error)
    {
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);
        }
        catch (Exception ex)
        {
            error.WriteLine(
                $"WARNING: could not restrict the ACL on {path} ({ex.Message}){DarlingFileSecurity.DescribeOwnerAndExposure(path)}. " +
                "It holds a LIVE password in cleartext. Delete it, or restrict it by hand before leaving this machine: " +
                $"icacls \"{path}\" /inheritance:r /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}\":F");
            return false;
        }

        try
        {
            if (DarlingFileSecurity.IsReadableByOrdinaryUsers(path))
            {
                error.WriteLine(
                    $"WARNING: {path} is STILL readable by ordinary users after hardening" +
                    $"{DarlingFileSecurity.DescribeOwnerAndExposure(path)}. It holds a LIVE password in cleartext. " +
                    "Delete it, or restrict it by hand before leaving this machine: " +
                    $"icacls \"{path}\" /inheritance:r /grant \"{DarlingFileSecurity.ServiceAccountDisplayName}\":F");
                return false;
            }
        }
        catch (Exception ex)
        {
            error.WriteLine(
                $"WARNING: could not confirm the ACL on {path} ({ex.Message}). It holds a LIVE password in " +
                "cleartext — check its permissions before leaving this machine.");
            return false;
        }

        return true;
    }

    /// <summary>
    /// The <c>Host=</c> value for the remote viewer connection (D8 / Round 4 #12): the bind IP itself when
    /// <paramref name="listen"/> is a concrete IP (not IPv4 loopback, not a <c>0.0.0.0</c>/<c>::</c> wildcard) —
    /// verify-full then validates it against the cert's iPAddress SAN — otherwise the machine's hostname, which
    /// the cert also carries as a dnsName SAN (the fallback for a wildcard bind, a hostname listen, or an unset
    /// listen). Pure — unit-testable.
    /// </summary>
    public static string ResolveViewerHost(string? listen)
    {
        var trimmed = listen?.Trim();
        if (!string.IsNullOrEmpty(trimmed)
            && IPAddress.TryParse(trimmed, out var ip)
            && !(ip.AddressFamily == AddressFamily.InterNetwork && ip.GetAddressBytes()[0] == 127)
            && !ip.Equals(IPAddress.Any)
            && !ip.Equals(IPAddress.IPv6Any))
        {
            return trimmed;
        }

        return Environment.MachineName;
    }

    /// <summary>
    /// The remote viewer's paste-ready Npgsql connection string (D8): the resolved host, the network role,
    /// verify-full TLS against the pinned server cert, the <c>darling</c> database, and the collect/config
    /// search path — the exact string the operator drops into the viewer machine's darling.json
    /// <c>postgres.connectionString</c> (<c>managed = false</c>, consumed verbatim). The managed role password
    /// is service-generated alphanumeric (no connection-string metacharacters), so a hand-built string is safe
    /// and yields the exact documented shape. Pure — unit-testable.
    /// </summary>
    public static string BuildViewerConnectionString(
        string host, int port, string role, string password, string rootCertificatePath) =>
        $"Host={host};Port={port};Username={role};Password={password};Database=darling;" +
        $"Search Path=collect,config,public;SSL Mode=VerifyFull;Root Certificate={rootCertificatePath}";

    /* ================================================================================================
       --configure-network: the interactive opt-in exposure wizard (#1561).

       Design invariants (the whole reason this is safe):
         - Validation is DELEGATED. Every candidate value is checked by building the SAME config object
           the service reads and running the SAME resolver it fail-closes on (the store's
           DarlingManagedPostgres.ResolveNetworkExposure, the MCP host's DarlingMcpHostService.ResolveMcpBind,
           the web host's DarlingWebHostService.ResolveWebBind).
           The wizard never re-implements CIDR / family / role / token rules — it re-prompts with the
           resolver's own degrade reason, so the wizard can never write what the service would reject.
         - The edit is comment-preserving TEXT SURGERY (DarlingNetworkConfigEditor) — the sample's
           heavily-commented documentation survives verbatim.
         - Nothing is written until the new text passes DarlingConfig.Parse AND the resolver re-check on
           the REPARSED result, and only then behind a timestamped backup. An edit never leaves an
           unparseable or fail-closed darling.json.
         - The MCP bearer / web access tokens are generated + DPAPI-protected; each plaintext is printed to
           STDOUT exactly once with the save-this warning on STDERR (the --print-viewer-connection
           secret-split posture).
         - mcp.enabled / mcp.port are control-plane after the first run (the Viewer's Settings toggle owns
           them live), so the wizard WARNS and points at Settings — it never edits them. The network block
           it writes is deliberately file-defined + restart-only.
       ================================================================================================ */

    private const string ServiceName = "PerformanceMonitor Darling";

    /// <summary>
    /// Interactive wizard that guides the operator through the opt-in store / MCP / web-dashboard LAN exposure and writes
    /// a comment-preserving, resolver-validated edit to darling.json behind a timestamped backup. Managed
    /// mode only (BYO exposure is governed by the operator's own PostgreSQL). Windows-only (it generates a
    /// DPAPI-protected token and controls the Windows service). <paramref name="input"/> is the scripted-
    /// input testability lever — the tests drive the whole flow with a <see cref="StringReader"/>. Returns
    /// 0 on a completed run (including a clean quit) and 1 on a load/parse/write error or a BYO refusal.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> ConfigureNetworkAsync(
        string? configPath, TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        var resolvedPath = DarlingConfig.ResolveConfigPath(configPath);

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        string originalText;
        try
        {
            originalText = await File.ReadAllTextAsync(resolvedPath, cancellationToken);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not read {resolvedPath}: {ex.Message}");
            return 1;
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling — opt-in network exposure wizard (--configure-network)");
        output.WriteLine($"Config: {resolvedPath}");
        output.WriteLine();

        /* Current verdicts, straight from the resolvers the running service uses — so the operator sees the
           service's own truth (including any fail-closed degrade), not the wizard's guess. */
        var (storeCertPath, storeKeyPath) = ResolveStoreCertPaths(postgres);
        var storeNow = DarlingManagedPostgres.ResolveNetworkExposure(postgres.Network, storeCertPath, storeKeyPath);
        var mcpNow = DarlingMcpHostService.ResolveMcpBind(config.Mcp, postgres.Managed);
        var mcpNowExposed = mcpNow.Mode == DarlingMcpHostService.McpBindMode.NetworkAndLoopback;
        var mcpNowDegrade =
            mcpNow.Reason is DarlingMcpHostService.McpBindReason.NetworkExposed or DarlingMcpHostService.McpBindReason.LoopbackByDefault
                ? null
                : McpDegradeText(mcpNow.Reason, config.Mcp);
        var webNow = DarlingWebHostService.ResolveWebBind(config.Web, postgres.Managed);
        var webNowExposed = webNow.Mode == DarlingHostBinding.BindMode.NetworkAndLoopback;
        var webNowDegrade =
            webNow.Reason is DarlingHostBinding.BindReason.NetworkExposed or DarlingHostBinding.BindReason.LoopbackByDefault
                ? null
                : WebDegradeText(webNow.Reason, config.Web);

        output.WriteLine("Current exposure:");
        output.WriteLine(DarlingNetworkConfigEditor.FormatExposureState(
            "Store", storeNow.Exposed, storeNow.ListenIp, storeNow.Cidr,
            storeNow.Roles is null ? null : string.Join(", ", storeNow.Roles), storeNow.DegradeReason));
        output.WriteLine(DarlingNetworkConfigEditor.FormatExposureState(
            "MCP  ", mcpNowExposed, config.Mcp.Network?.Listen, config.Mcp.Network?.AllowFrom, null, mcpNowDegrade));
        output.WriteLine(DarlingNetworkConfigEditor.FormatExposureState(
            "Web  ", webNowExposed, config.Web.Network?.Listen, config.Web.Network?.AllowFrom, null, webNowDegrade));

        /* #2562: whether the exposed dashboard is ENCRYPTED belongs in the exposure summary — this verb is
           what an operator runs to answer "what is open", and "open on the LAN" reads very differently with
           and without TLS. Only when exposed: TLS is meaningless on a loopback-only dashboard, and a line
           saying "OFF" there would be a warning about nothing. The wizard does not PROMPT for a certificate
           (web.network.tls is file-defined and restart-only, like the rest of the block); it reports it. */
        if (webNowExposed)
        {
            var tls = DarlingWebTls.Describe(config.Web.Network?.Tls);
            output.WriteLine(tls.Shape switch
            {
                DarlingWebTls.TlsShape.NotConfigured =>
                    "         TLS: off — the access token and its session cookie cross the segment in the clear. "
                    + "Set web.network.tls to serve HTTPS.",
                DarlingWebTls.TlsShape.Invalid =>
                    $"         TLS: MISCONFIGURED — {tls.Problem} The dashboard will bind loopback-only.",
                /* The warning rides along: this verb is the one an operator runs to see what is open, and a
                   stale PKCS#12 password beside a working PEM pair is exactly the "I thought the bundle was
                   being served" state they came here to resolve. The service logs it at every start; nobody
                   reading this summary should have to go find that line. */
                DarlingWebTls.TlsShape.Pem =>
                    $"         TLS: on (PEM pair, {config.Web.Network!.Tls!.CertPath})."
                    + (tls.Warning is null ? string.Empty : $" NOTE: {tls.Warning}"),
                _ => $"         TLS: on (PKCS#12, {config.Web.Network!.Tls!.PfxPath})."
                     + (tls.Warning is null ? string.Empty : $" NOTE: {tls.Warning}"),
            });
        }

        output.WriteLine($"  Service: {await DescribeServiceStateAsync(cancellationToken)}");
        output.WriteLine();

        /* BYO guard — exposure is managed-mode only (same refusal shape as PrintViewerConnectionAsync). */
        if (!postgres.Managed)
        {
            output.WriteLine("Network exposure is MANAGED-MODE ONLY.");
            output.WriteLine("This darling.json uses bring-your-own PostgreSQL (postgres.connectionString), so your own");
            output.WriteLine("PostgreSQL / reverse proxy governs network exposure — the wizard cannot open the endpoints here.");

            var hasBlocks = (postgres.Network?.IsConfigured ?? false)
                || (config.Mcp.Network?.IsConfigured ?? false)
                || (config.Web.Network?.IsConfigured ?? false);
            if (hasBlocks && AskYesNo(input, output, "A network block is present but IGNORED in this mode. Remove it from darling.json?", defaultYes: false))
            {
                return await DisableExposureAsync(resolvedPath, originalText, input, output, error, cancellationToken);
            }

            return 1;
        }

        /* Surface selection — one surface, a comma combination (e.g. "1,3"), all three, or disable. */
        output.WriteLine("What would you like to configure?");
        output.WriteLine("  [1] Store   — a remote Viewer over TLS (verify-full)");
        output.WriteLine("  [2] MCP     — a LAN assistant/client behind a bearer token");
        output.WriteLine("  [3] Web     — the browser dashboard behind a token->cookie login");
        output.WriteLine("  [4] All     — every surface above (or pick a combination, e.g. 1,3)");
        output.WriteLine("  [5] Disable — remove all exposure (back to loopback-only)");
        output.WriteLine("  [q] Quit without changes");
        var choice = Prompt(input, output, "Choice", "q");

        /* #2097: null is EOF — a NON-INTERACTIVE host, not a human pressing q. Saying "No changes made."
           there reads as the wizard shrugging for no reason; name the actual cause on stdout and exit
           nonzero so scripts notice. An empty line or explicit q remains the deliberate quit. */
        if (choice is null)
        {
            WriteNonInteractiveGuidance(output);
            return 1;
        }

        if (choice.Length == 0 || string.Equals(choice, "q", StringComparison.OrdinalIgnoreCase))
        {
            output.WriteLine("No changes made.");
            return 0;
        }

        if (choice == "5")
        {
            return await DisableExposureAsync(resolvedPath, originalText, input, output, error, cancellationToken);
        }

        if (!TryParseSurfaceChoice(choice, out var doStore, out var doMcp, out var doWeb))
        {
            output.WriteLine("Unrecognized choice; no changes made.");
            return 0;
        }

        /* Gather all inputs (delegated validation) BEFORE writing anything, so a cancel leaves the file
           untouched and a multi-surface run is all-or-nothing. */
        (string Listen, string AllowFrom, string Role)? store = null;
        if (doStore)
        {
            output.WriteLine();
            output.WriteLine("== Store exposure ==");
            store = GatherStoreInputs(input, output, error, storeCertPath, storeKeyPath);
            if (store is null)
            {
                return 1;
            }
        }

        (string Listen, string AllowFrom, string? EncryptedToken, string? PlainToken, string? GeneratedPlain)? mcp = null;
        if (doMcp)
        {
            output.WriteLine();
            output.WriteLine("== MCP exposure ==");
            /* #2389: unconditional, and about the PRECEDENCE rather than the file value. This used to print
               only when mcp.enabled was FALSE in the file, which is exactly backwards — a file that says true
               while config_service.mcp_enabled says false is the combination that misleads, and it printed
               nothing there. The wizard holds no store connection, so it cannot report the effective value;
               what it can do honestly is say which plane decides and where the file value stops mattering. */
            output.WriteLine($"NOTE: darling.json has mcp.enabled = {(config.Mcp.Enabled ? "true" : "false")}, but after the first run that is only");
            output.WriteLine("      the SEED — config.config_service.mcp_enabled decides whether the endpoint actually runs, and");
            output.WriteLine("      this wizard neither reads nor writes it. It writes the network block only (which IS file-only");
            output.WriteLine("      and restart-only); use --enable-mcp / --disable-mcp or the Viewer's Settings to turn MCP on.");

            mcp = GatherMcpInputs(input, output, error, config.Mcp);
            if (mcp is null)
            {
                return 1;
            }
        }

        (string Listen, string AllowFrom, string? EncryptedToken, string? PlainToken, string? GeneratedPlain)? web = null;
        if (doWeb)
        {
            output.WriteLine();
            output.WriteLine("== Web dashboard exposure ==");
            /* #2389: the MCP note's twin — unconditional, and about which plane decides. */
            output.WriteLine($"NOTE: darling.json has web.enabled = {(config.Web.Enabled ? "true" : "false")}, but after the first run that is only");
            output.WriteLine("      the SEED — config.config_service.web_enabled decides whether the dashboard actually runs, and");
            output.WriteLine("      this wizard neither reads nor writes it. It writes the network block only (which IS file-only");
            output.WriteLine("      and restart-only); use --enable-web / --disable-web or the Viewer's Settings to turn it on.");

            web = GatherWebInputs(input, output, error, config.Web);
            if (web is null)
            {
                return 1;
            }
        }

        /* Build the edit through the comment-preserving surgeon. */
        var newText = originalText;
        if (store is not null)
        {
            newText = DarlingNetworkConfigEditor.UpsertNetworkBlock(
                newText, "postgres",
                DarlingNetworkConfigEditor.BuildStoreNetworkBlock(store.Value.Listen, store.Value.AllowFrom, store.Value.Role));
        }

        if (mcp is not null)
        {
            newText = DarlingNetworkConfigEditor.UpsertNetworkBlock(
                newText, "mcp",
                DarlingNetworkConfigEditor.BuildMcpNetworkBlock(mcp.Value.Listen, mcp.Value.AllowFrom, mcp.Value.EncryptedToken, mcp.Value.PlainToken));
        }

        if (web is not null)
        {
            newText = DarlingNetworkConfigEditor.UpsertNetworkBlock(
                newText, "web",
                DarlingNetworkConfigEditor.BuildWebNetworkBlock(web.Value.Listen, web.Value.AllowFrom, web.Value.EncryptedToken, web.Value.PlainToken));
        }

        /* Guard 1: the edited text must PARSE (comments/trailing-commas tolerated). */
        DarlingConfig reparsed;
        try
        {
            reparsed = DarlingConfig.Parse(newText);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Internal error: the edited darling.json did not parse ({ex.Message}). No changes were written.");
            return 1;
        }

        /* Guard 2: the resolvers must ACCEPT the reparsed result — never write a file the service would
           fail-close on. This is the same delegation the input loop used, re-run on the FINAL text. */
        if (store is not null)
        {
            var check = DarlingManagedPostgres.ResolveNetworkExposure(reparsed.Postgres.Network, storeCertPath, storeKeyPath);
            if (!check.Exposed)
            {
                error.WriteLine($"Internal error: the store block would fail-close ({check.DegradeReason}). No changes were written.");
                return 1;
            }
        }

        if (mcp is not null)
        {
            var check = DarlingMcpHostService.ResolveMcpBind(reparsed.Mcp, reparsed.Postgres.Managed);
            if (check.Mode != DarlingMcpHostService.McpBindMode.NetworkAndLoopback)
            {
                error.WriteLine($"Internal error: the MCP block would fail-close ({McpDegradeText(check.Reason, reparsed.Mcp)}). No changes were written.");
                return 1;
            }
        }

        if (web is not null)
        {
            var check = DarlingWebHostService.ResolveWebBind(reparsed.Web, reparsed.Postgres.Managed);
            if (check.Mode != DarlingHostBinding.BindMode.NetworkAndLoopback)
            {
                error.WriteLine($"Internal error: the web block would fail-close ({WebDegradeText(check.Reason, reparsed.Web)}). No changes were written.");
                return 1;
            }
        }

        /* Only now: timestamped backup + write. */
        if (!await WriteWithBackupAsync(resolvedPath, newText, output, error, cancellationToken))
        {
            return 1;
        }

        /* The generated token plaintexts — STDOUT exactly once each; the save-this warning on STDERR so a
           STDOUT redirect keeps the token without swallowing the warning (MCP first, then web, so a
           two-token capture is unambiguous by order). */
        if (mcp is not null && mcp.Value.GeneratedPlain is not null)
        {
            error.WriteLine();
            error.WriteLine("SAVE THIS NOW — your new MCP bearer token is shown ONCE (darling.json stores only its DPAPI blob).");
            error.WriteLine("Remote MCP clients send it as the header:  Authorization: Bearer <token>");
            output.WriteLine(mcp.Value.GeneratedPlain);
        }

        if (web is not null && web.Value.GeneratedPlain is not null)
        {
            error.WriteLine();
            error.WriteLine("SAVE THIS NOW — your new web dashboard access token is shown ONCE (darling.json stores only its DPAPI blob).");
            error.WriteLine("A remote browser presents it once via ?token=... and gets a session cookie back.");
            output.WriteLine(web.Value.GeneratedPlain);
        }

        PrintNextSteps(
            output,
            store is not null, postgres.Port, store?.AllowFrom,
            mcp is not null, config.Mcp.Port, mcp?.AllowFrom, config.Mcp.Enabled,
            web is not null, config.Web.Port, web?.AllowFrom, config.Web.Enabled, web?.Listen);

        await OfferRestartAsync(input, output, error, cancellationToken);
        return 0;
    }

    /// <summary>
    /// Parses the surface-selection choice: "1"/"2"/"3" (store/MCP/web), "4" = all three, or a comma
    /// combination like "1,3". STRICT — every token must be a known surface digit, so a typo ("1,shop")
    /// rejects the whole input instead of silently configuring a subset. False = unrecognized (nothing
    /// selected). Pure.
    /// </summary>
    internal static bool TryParseSurfaceChoice(string choice, out bool doStore, out bool doMcp, out bool doWeb)
    {
        doStore = false;
        doMcp = false;
        doWeb = false;

        foreach (var raw in choice.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            switch (raw)
            {
                case "1": doStore = true; break;
                case "2": doMcp = true; break;
                case "3": doWeb = true; break;
                case "4": doStore = true; doMcp = true; doWeb = true; break;
                default:
                    doStore = false;
                    doMcp = false;
                    doWeb = false;
                    return false;
            }
        }

        return doStore || doMcp || doWeb;
    }

    /// <summary>
    /// The store's generated TLS cert/key paths (beside the data directory), passed to
    /// <c>ResolveNetworkExposure</c> so the whitespace-in-path degrade (a spaced path the pg_ctl -o
    /// override cannot pass to postgres) is caught pre-write, exactly as the service would. Mirrors the
    /// path idiom in <see cref="PrintViewerConnectionAsync"/>.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static (string CertPath, string KeyPath) ResolveStoreCertPaths(PostgresConfig postgres)
    {
        var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(postgres);
        var credentialDirectory = Path.GetDirectoryName(DarlingManagedPostgres.ViewerCredentialPathFor(dataDirectory))!;
        return (
            Path.Combine(credentialDirectory, DarlingManagedPostgres.ServerCertFileName),
            Path.Combine(credentialDirectory, DarlingManagedPostgres.ServerKeyFileName));
    }

    /// <summary>Best-effort Windows-service status line via Get-Service; never throws (falls back to a plain note).</summary>
    [SupportedOSPlatform("windows")]
    private static async Task<string> DescribeServiceStateAsync(CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, psOutput) = await DarlingManagedPostgres.RunPowerShellAsync(
                $"(Get-Service -Name '{ServiceName}' -ErrorAction SilentlyContinue).Status", cancellationToken);
            var status = psOutput.Trim();
            return exitCode == 0 && status.Length > 0 ? status : "not installed (or status unavailable)";
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return "status unavailable";
        }
    }

    /// <summary>The machine's non-loopback IPv4 unicast addresses (interface name + address). Impure (queries the OS); the pure menu formatter takes its output.</summary>
    private static List<(string Name, string Address)> EnumerateLocalIPv4()
    {
        var addresses = new List<(string Name, string Address)>();
        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (nic.OperationalStatus != OperationalStatus.Up || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
            {
                continue;
            }

            foreach (var unicast in nic.GetIPProperties().UnicastAddresses)
            {
                if (unicast.Address.AddressFamily != AddressFamily.InterNetwork)
                {
                    continue;
                }

                var ip = unicast.Address.ToString();
                if (!ip.StartsWith("127.", StringComparison.Ordinal))
                {
                    addresses.Add((nic.Name, ip));
                }
            }
        }

        return addresses;
    }

    /// <summary>Prompts for the bind IP: pick a listed adapter, choose 0.0.0.0, or type any IP (the resolver validates it). Returns null on EOF/cancel.</summary>
    private static string? SelectListenAddress(TextReader input, TextWriter output, string surface)
    {
        var adapters = EnumerateLocalIPv4();
        output.WriteLine($"Select the {surface} bind IP (the address remote clients connect to):");
        output.WriteLine(DarlingNetworkConfigEditor.FormatAdapterMenu(adapters));
        var allInterfacesChoice = adapters.Count + 1;
        output.WriteLine($"  [{allInterfacesChoice}] 0.0.0.0  (all interfaces — connect by a cert SAN name)");
        output.WriteLine("  [c] type a custom IP");

        while (true)
        {
            var pick = Prompt(input, output, "Bind IP");
            if (pick is null)
            {
                return null;
            }

            if (pick.Length == 0)
            {
                output.WriteLine("  A bind IP is required.");
                continue;
            }

            if (int.TryParse(pick, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
            {
                if (n >= 1 && n <= adapters.Count)
                {
                    return adapters[n - 1].Address;
                }

                if (n == allInterfacesChoice)
                {
                    return "0.0.0.0";
                }

                output.WriteLine("  Not a listed number — enter a menu number or an IP.");
                continue;
            }

            if (string.Equals(pick, "c", StringComparison.OrdinalIgnoreCase))
            {
                var custom = Prompt(input, output, "Enter the bind IP");
                if (custom is null)
                {
                    return null;
                }

                if (custom.Length == 0)
                {
                    output.WriteLine("  A bind IP is required.");
                    continue;
                }

                return custom;
            }

            /* Anything else is treated as a directly-typed IP; the resolver is the arbiter of validity. */
            return pick;
        }
    }

    /// <summary>
    /// Gathers listen / allowFrom / role(s) for the store, RE-PROMPTING with the store resolver's own degrade
    /// reason until it accepts them (or the operator cancels). The whitespace-in-path degrade is a config
    /// problem the loop cannot fix, so it is reported and the surface is abandoned. Returns null on cancel.
    /// <para>The returned Role is the resolver's CANONICAL joined list (#2665): <c>"viewer + admin"</c> comes
    /// back as <c>"admin,viewer"</c>, so darling.json holds the text the service would itself compute and a
    /// re-run of the wizard is a no-op rather than a reorder.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static (string Listen, string AllowFrom, string Role)? GatherStoreInputs(
        TextReader input, TextWriter output, TextWriter error, string certPath, string keyPath)
    {
        while (true)
        {
            var listen = SelectListenAddress(input, output, "store");
            if (listen is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            var allowFrom = Prompt(input, output, "Allowed remote CIDR (e.g. 192.168.1.0/24)");
            if (allowFrom is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            output.WriteLine("Remote pg_hba role(s): 'viewer' (read-only, the secure default), 'admin' (remote WRITES),");
            output.WriteLine("or BOTH as 'admin,viewer' — that writes one hostssl rule per role inside the managed block, so");
            output.WriteLine("an admin seat and read-only seats can reach the same store and a later CIDR narrowing tightens");
            output.WriteLine("all of them together (#2665).");
            var role = Prompt(input, output, "Role(s)", "viewer");
            if (role is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            /* Warn off the NORMALIZED value, not the typed text. A check for the string being exactly "admin"
               goes silent for every list form ("admin,viewer", "viewer+admin") — which is the case where the
               operator most needs to read it, because they may be thinking about the viewer half. */
            if (string.Equals(DarlingNetwork.NormalizeNetworkRole(role), "admin", StringComparison.Ordinal))
            {
                output.WriteLine("  WARNING: 'admin' is a remote WRITE credential holding the config-table service-credential pivot.");
                output.WriteLine("           Prefer 'viewer' unless you specifically need remote writes.");
            }

            var candidate = new PostgresNetworkConfig { Listen = listen, AllowFrom = allowFrom, Role = role };
            var decision = DarlingManagedPostgres.ResolveNetworkExposure(candidate, certPath, keyPath);
            if (decision.Exposed)
            {
                /* Write the resolver's canonical values (parsed IP, host-bits-zeroed CIDR, normalized role)
                   so the file matches what the service would compute. */
                return (decision.ListenIp!, decision.Cidr!, string.Join(",", decision.Roles!));
            }

            var reason = decision.DegradeReason ?? "the store resolver rejected these values";
            output.WriteLine($"  Not accepted: {reason}");
            if (reason.Contains("whitespace", StringComparison.OrdinalIgnoreCase))
            {
                error.WriteLine("  This is a path problem, not an input problem — move postgres.dataDirectory to a space-free path, then re-run.");
                return null;
            }

            output.WriteLine("  Let us try again.");
        }
    }

    /// <summary>
    /// Gathers the MCP token (default KEEP an existing one; else generate a fresh 32-char token and
    /// DPAPI-protect it) plus listen / allowFrom, RE-PROMPTING with the MCP resolver's degrade reason
    /// until it accepts them. Returns the fields to write plus the generated plaintext (non-null only when
    /// a token was generated, so the caller prints it once). Returns null on cancel.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static (string Listen, string AllowFrom, string? EncryptedToken, string? PlainToken, string? GeneratedPlain)? GatherMcpInputs(
        TextReader input, TextWriter output, TextWriter error, McpConfig currentMcp)
    {
        string? encryptedToken = null;
        string? plainToken = null;
        string? generatedPlain = null;

        var existing = currentMcp.Network;
        var hasExistingToken = existing is not null
            && (!string.IsNullOrWhiteSpace(existing.EncryptedToken) || !string.IsNullOrWhiteSpace(existing.Token));

        if (hasExistingToken && AskYesNo(input, output, "An MCP bearer token already exists. Keep it?", defaultYes: true))
        {
            if (!string.IsNullOrWhiteSpace(existing!.EncryptedToken))
            {
                encryptedToken = existing.EncryptedToken;
            }
            else
            {
                plainToken = existing!.Token;
                output.WriteLine("  Keeping the existing PLAINTEXT token (consider regenerating to store it DPAPI-encrypted instead).");
            }
        }
        else
        {
            generatedPlain = DarlingManagedPostgres.GeneratePassword();
            encryptedToken = DarlingSecrets.Protect(generatedPlain);
        }

        while (true)
        {
            var listen = SelectListenAddress(input, output, "MCP");
            if (listen is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            var allowFrom = Prompt(input, output, "Allowed remote CIDR (e.g. 192.168.1.0/24)");
            if (allowFrom is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            var candidate = new McpConfig
            {
                Enabled = currentMcp.Enabled,
                Port = currentMcp.Port,
                Network = new McpNetworkConfig
                {
                    Listen = listen,
                    AllowFrom = allowFrom,
                    EncryptedToken = encryptedToken,
                    Token = plainToken,
                },
            };

            var decision = DarlingMcpHostService.ResolveMcpBind(candidate, managed: true);
            if (decision.Mode == DarlingMcpHostService.McpBindMode.NetworkAndLoopback)
            {
                return (listen, allowFrom, encryptedToken, plainToken, generatedPlain);
            }

            output.WriteLine($"  Not accepted: {McpDegradeText(decision.Reason, candidate)}");
            output.WriteLine("  Let us try again.");
        }
    }

    /// <summary>
    /// Gathers the web dashboard access token (default KEEP an existing one; else generate a fresh 32-char
    /// token and DPAPI-protect it) plus listen / allowFrom, RE-PROMPTING with the web bind resolver's degrade
    /// reason until it accepts them — the web twin of <see cref="GatherMcpInputs"/> (#1617). Returns the
    /// fields to write plus the generated plaintext (non-null only when a token was generated, so the caller
    /// prints it once). Returns null on cancel.
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static (string Listen, string AllowFrom, string? EncryptedToken, string? PlainToken, string? GeneratedPlain)? GatherWebInputs(
        TextReader input, TextWriter output, TextWriter error, WebConfig currentWeb)
    {
        string? encryptedToken = null;
        string? plainToken = null;
        string? generatedPlain = null;

        var existing = currentWeb.Network;
        var hasExistingToken = existing is not null
            && (!string.IsNullOrWhiteSpace(existing.EncryptedToken) || !string.IsNullOrWhiteSpace(existing.Token));

        if (hasExistingToken && AskYesNo(input, output, "A web access token already exists. Keep it?", defaultYes: true))
        {
            if (!string.IsNullOrWhiteSpace(existing!.EncryptedToken))
            {
                encryptedToken = existing.EncryptedToken;
            }
            else
            {
                plainToken = existing!.Token;
                output.WriteLine("  Keeping the existing PLAINTEXT token (consider regenerating to store it DPAPI-encrypted instead).");
            }
        }
        else
        {
            generatedPlain = DarlingManagedPostgres.GeneratePassword();
            encryptedToken = DarlingSecrets.Protect(generatedPlain);
        }

        while (true)
        {
            var listen = SelectListenAddress(input, output, "web");
            if (listen is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            var allowFrom = Prompt(input, output, "Allowed remote CIDR (e.g. 192.168.1.0/24)");
            if (allowFrom is null)
            {
                output.WriteLine("Cancelled — no changes made.");
                return null;
            }

            var candidate = new WebConfig
            {
                Enabled = currentWeb.Enabled,
                Port = currentWeb.Port,
                Network = new WebNetworkConfig
                {
                    Listen = listen,
                    AllowFrom = allowFrom,
                    EncryptedToken = encryptedToken,
                    Token = plainToken,
                },
            };

            var decision = DarlingWebHostService.ResolveWebBind(candidate, managed: true);
            if (decision.Mode == DarlingHostBinding.BindMode.NetworkAndLoopback)
            {
                return (listen, allowFrom, encryptedToken, plainToken, generatedPlain);
            }

            output.WriteLine($"  Not accepted: {WebDegradeText(decision.Reason, candidate)}");
            output.WriteLine("  Let us try again.");
        }
    }

    /// <summary>Human text for a web bind degrade reason (presentation only — the resolver decides; this narrates).</summary>
    private static string WebDegradeText(DarlingHostBinding.BindReason reason, WebConfig web) => reason switch
    {
        DarlingHostBinding.BindReason.ListenInvalid =>
            $"web.network.listen '{web.Network?.Listen}' is not a valid IP address (use a specific IP, or 0.0.0.0 for all interfaces).",
        DarlingHostBinding.BindReason.TokenMissing =>
            "no access token is set (the wizard should have supplied one — this is unexpected).",
        DarlingHostBinding.BindReason.AllowFromInvalid =>
            $"web.network.allowFrom '{web.Network?.AllowFrom}' is not a valid CIDR or its address family does not match listen (e.g. 192.168.1.0/24, host bits zeroed).",
        DarlingHostBinding.BindReason.ManagedModeRequired =>
            "network exposure is managed-mode only.",
        _ => "the web bind resolver rejected these values.",
    };

    /// <summary>Human text for an MCP bind degrade reason (presentation only — the resolver decides; this narrates).</summary>
    private static string McpDegradeText(DarlingMcpHostService.McpBindReason reason, McpConfig mcp) => reason switch
    {
        DarlingMcpHostService.McpBindReason.ListenInvalid =>
            $"mcp.network.listen '{mcp.Network?.Listen}' is not a valid IP address (use a specific IP, or 0.0.0.0 for all interfaces).",
        DarlingMcpHostService.McpBindReason.TokenMissing =>
            "no bearer token is set (the wizard should have supplied one — this is unexpected).",
        DarlingMcpHostService.McpBindReason.AllowFromInvalid =>
            $"mcp.network.allowFrom '{mcp.Network?.AllowFrom}' is not a valid CIDR or its address family does not match listen (e.g. 192.168.1.0/24, host bits zeroed).",
        DarlingMcpHostService.McpBindReason.ManagedModeRequired =>
            "network exposure is managed-mode only.",
        _ => "the MCP resolver rejected these values.",
    };

    /// <summary>Removes all three network blocks (symmetric with the reconcilers), validating parse + loopback-only before the timestamped write, then offers a restart. Shared by the managed Disable choice and the BYO cleanup.</summary>
    [SupportedOSPlatform("windows")]
    private static async Task<int> DisableExposureAsync(
        string resolvedPath, string originalText, TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        var newText = DarlingNetworkConfigEditor.RemoveNetworkBlock(originalText, "postgres");
        newText = DarlingNetworkConfigEditor.RemoveNetworkBlock(newText, "mcp");
        newText = DarlingNetworkConfigEditor.RemoveNetworkBlock(newText, "web");

        if (string.Equals(newText, originalText, StringComparison.Ordinal))
        {
            output.WriteLine("No live network block found — already loopback-only. Nothing to change.");
            return 0;
        }

        DarlingConfig reparsed;
        try
        {
            reparsed = DarlingConfig.Parse(newText);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Internal error: the disabled darling.json did not parse ({ex.Message}). No changes were written.");
            return 1;
        }

        var (certPath, keyPath) = ResolveStoreCertPaths(reparsed.Postgres);
        var storeStillExposed = DarlingManagedPostgres.ResolveNetworkExposure(reparsed.Postgres.Network, certPath, keyPath).Exposed;
        var mcpStillExposed = DarlingMcpHostService.ResolveMcpBind(reparsed.Mcp, reparsed.Postgres.Managed).Mode
            == DarlingMcpHostService.McpBindMode.NetworkAndLoopback;
        var webStillExposed = DarlingWebHostService.ResolveWebBind(reparsed.Web, reparsed.Postgres.Managed).Mode
            == DarlingHostBinding.BindMode.NetworkAndLoopback;
        if (storeStillExposed || mcpStillExposed || webStillExposed)
        {
            error.WriteLine("Internal error: exposure is still present after removal. No changes were written.");
            return 1;
        }

        if (!await WriteWithBackupAsync(resolvedPath, newText, output, error, cancellationToken))
        {
            return 1;
        }

        output.WriteLine("Network exposure removed. The service reconciles the endpoints OFF (pg_hba rule, firewall, ssl) on its next start.");
        await OfferRestartAsync(input, output, error, cancellationToken);
        return 0;
    }

    /// <summary>Offers to restart the service so the edit applies. Elevated: does it via a 90s-budget Restart-Service + WaitForStatus; otherwise prints the exact manual commands (non-fatal, guidance-first).</summary>
    [SupportedOSPlatform("windows")]
    private static async Task OfferRestartAsync(TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        output.WriteLine();
        if (!AskYesNo(input, output, $"Restart the '{ServiceName}' service now to apply?", defaultYes: false))
        {
            output.WriteLine("Apply later by restarting the service:");
            PrintManualRestartCommands(output);
            return;
        }

        if (!IsElevated())
        {
            output.WriteLine("This shell is not elevated, so it cannot control the service. Run these in an ELEVATED PowerShell:");
            PrintManualRestartCommands(output);
            return;
        }

        output.WriteLine($"Restarting '{ServiceName}' (this can take up to ~90 seconds)...");
        var command =
            $"Restart-Service -Name '{ServiceName}' -Force; " +
            $"(Get-Service -Name '{ServiceName}').WaitForStatus('Running', [TimeSpan]::FromSeconds(75))";
        try
        {
            var (exitCode, psOutput) = await DarlingManagedPostgres.RunPowerShellAsync(command, cancellationToken, TimeSpan.FromSeconds(90));
            if (exitCode == 0)
            {
                output.WriteLine($"Service '{ServiceName}' restarted and is Running.");
            }
            else
            {
                error.WriteLine($"Restart did not confirm Running (exit {exitCode}): {psOutput}");
                output.WriteLine("Restart it manually:");
                PrintManualRestartCommands(output);
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Restart attempt failed ({ex.Message}).");
            output.WriteLine("Restart it manually:");
            PrintManualRestartCommands(output);
        }
    }

    private static void PrintManualRestartCommands(TextWriter output)
    {
        output.WriteLine($"  Restart-Service -Name '{ServiceName}' -Force");
        output.WriteLine($"  (or:  sc.exe stop \"{ServiceName}\"   then   sc.exe start \"{ServiceName}\")");
    }

    /// <summary>Prints the handoff reminders: the scoped firewall command(s), the store's --export-viewer-config step, and the web dashboard's browser login hint.</summary>
    [SupportedOSPlatform("windows")]
    private static void PrintNextSteps(
        TextWriter output,
        bool storeConfigured, int storePort, string? storeCidr,
        bool mcpConfigured, int mcpPort, string? mcpCidr, bool mcpEnabled,
        bool webConfigured, int webPort, string? webCidr, bool webEnabled, string? webListen)
    {
        output.WriteLine();
        output.WriteLine("Next steps:");
        if (storeConfigured)
        {
            output.WriteLine("  Store firewall rule (run ELEVATED; scoped to the port + CIDR):");
            output.WriteLine("    " + DarlingManagedPostgres.BuildFirewallEnableCommand(
                $"PerformanceMonitor Darling store (port {storePort})", storePort, storeCidr!));
            output.WriteLine("  After the service restarts (which generates the TLS cert), write the remote viewer's");
            output.WriteLine("  whole config folder — darling.json + server.crt + README — with:");
            output.WriteLine("    PerformanceMonitor.Darling.Service.exe --export-viewer-config");
            output.WriteLine("  (or --print-viewer-connection for just the paste-ready string + certificate).");
        }

        if (mcpConfigured)
        {
            output.WriteLine("  MCP firewall rule (run ELEVATED; scoped to the port + CIDR):");
            output.WriteLine("    " + DarlingManagedPostgres.BuildFirewallEnableCommand(
                DarlingMcpHostService.McpFirewallRuleName(mcpPort), mcpPort, mcpCidr!));
            /* #2414: this wizard holds no store connection, so the only port it can name is darling.json's, and
               the port is part of the rule's NAME. Say that, and point at the verb that resolves the effective
               one — pasting the line below on a box whose port has been moved opens the wrong port. */
            output.WriteLine($"  (that rule is named for darling.json's mcp.port = {mcpPort}. If the port was ever changed in the");
            output.WriteLine("   Viewer's Settings, config.config_service.mcp_port is what the endpoint binds — run");
            output.WriteLine("   --configure-firewall ELEVATED instead and it resolves the effective port and moves the rule.)");
            /* #2389: unconditional and about the PLANE, not the file value. This printed only when the FILE
               said false, which left the actually-misleading combination — file true, store false — with no
               note at all; and mcpEnabled here is the file's value, which on a seeded box decides nothing. */
            output.WriteLine("  NOTE: the network block you just wrote is FILE-authoritative and applies on restart, but whether");
            output.WriteLine("        MCP runs at all is config.config_service.mcp_enabled — darling.json's mcp.enabled is only the");
            output.WriteLine($"        first-run seed, and it currently reads {(mcpEnabled ? "true" : "false")}. Enable with --enable-mcp or the Viewer's Settings.");
        }

        if (webConfigured)
        {
            output.WriteLine("  Web dashboard firewall rule (run ELEVATED; scoped to the port + CIDR — --enable-web also");
            output.WriteLine("  reconciles this rule for you):");
            output.WriteLine("    " + DarlingManagedPostgres.BuildFirewallEnableCommand(
                DarlingWebHostService.WebFirewallRuleName(webPort), webPort, webCidr!));
            /* #2414: the MCP caveat's twin. */
            output.WriteLine($"  (that rule is named for darling.json's web.port = {webPort}. If the port was ever changed in the");
            output.WriteLine("   Viewer's Settings, config.config_service.web_port is what the endpoint binds — run");
            output.WriteLine("   --configure-firewall ELEVATED instead and it resolves the effective port and moves the rule.)");

            /* The one login step a human does differently for Web: a remote browser presents the access token
               once via ?token= and is 302'd back with a session cookie. A 0.0.0.0 bind has no single address
               to print, so fall back to a placeholder. */
            var webHost = webListen == "0.0.0.0" ? "<a-LAN-IP-of-this-machine>" : webListen;
            output.WriteLine("  Remote browser login (after the service restarts):");
            output.WriteLine($"    http://{webHost}:{webPort}/?token=<your-access-token>");
            output.WriteLine("  (the token is exchanged for a session cookie and stripped from the URL; loopback needs no token)");
            /* #2389: the MCP note's twin — unconditional, and about which plane decides. */
            output.WriteLine("  NOTE: the network block you just wrote is FILE-authoritative and applies on restart, but whether");
            output.WriteLine("        the dashboard runs at all is config.config_service.web_enabled — darling.json's web.enabled is");
            output.WriteLine($"        only the first-run seed, and it currently reads {(webEnabled ? "true" : "false")}. Enable with --enable-web or Settings.");
        }
    }

    /// <summary>
    /// Backs up darling.json to a timestamped sibling, then writes the new text. Returns false (with a
    /// message) on any I/O failure.
    ///
    /// <para><b>The backup is hardened, because it is a second copy of the secret.</b> Every edit here
    /// copies a file holding each monitored server's encrypted password and the MCP/web tokens, and
    /// <c>File.Copy</c> does NOT carry the source's DACL — the new file takes the DIRECTORY's inheritable
    /// ACEs instead. Measured, not assumed: copying a file whose DACL is protected with one ACE produces a
    /// backup that is unprotected with three inherited ones. On the documented install location, a folder
    /// created directly under <c>C:\</c>, those inherited ACEs include <c>BUILTIN\Users: Read</c> — so
    /// without this every <c>--rotate-token</c> or <c>--disable</c> would drop a world-readable copy of
    /// every credential beside a correctly hardened <c>darling.json</c>, defeating the ACL that is the
    /// whole protection boundary for LocalMachine-scope DPAPI blobs (#1721).</para>
    ///
    /// <para>Copy-then-harden leaves a sub-millisecond window where the backup exists with inherited
    /// access. That is stated rather than hidden: closing it would mean creating the file with an explicit
    /// security descriptor, which is a bigger change than the exposure warrants for an operator-initiated,
    /// elevated, interactive command — but it is the reason this is a mitigation of a leak rather than a
    /// proof of its absence.</para>
    /// </summary>
    internal static async Task<bool> WriteWithBackupAsync(
        string resolvedPath, string newText, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        try
        {
            var stamp = DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
            var backupPath = resolvedPath + ".bak-" + stamp;
            for (var suffix = 2; File.Exists(backupPath); suffix++)
            {
                backupPath = $"{resolvedPath}.bak-{stamp}-{suffix}";
            }

            File.Copy(resolvedPath, backupPath, overwrite: false);
            if (OperatingSystem.IsWindows())
            {
                HardenConfigBackup(backupPath, error);
            }

            /* WriteAllText TRUNCATES an existing file rather than recreating it, so darling.json keeps
               whatever DACL it already had — only the new backup needs hardening. */
            await File.WriteAllTextAsync(resolvedPath, newText, cancellationToken);
            output.WriteLine($"Wrote {resolvedPath}");
            output.WriteLine($"Backup saved: {backupPath}");
            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not write the configuration: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Locks a freshly written config backup to SYSTEM, Administrators and the service account — and,
    /// unlike <c>darling.json</c> itself, NOT to <c>NT AUTHORITY\INTERACTIVE</c> (#1769).
    ///
    /// <para>A backup is a byte-for-byte copy of the config: every monitored server's
    /// <c>encryptedPassword</c>, the MCP bearer token, the web access token. Those are DPAPI
    /// <b>LocalMachine</b> blobs with an entropy constant that ships in an open-source repo, so READ access
    /// IS the secret. The live config grants INTERACTIVE read because things genuinely read it as the
    /// interactive operator — the Viewer (<c>ViewerSettings.TryLoad</c>) and the CLI verbs. <b>Nothing reads
    /// a backup.</b> The only references to the <c>.bak-</c> name in non-test code are the two lines above
    /// that CONSTRUCT it, and restoring one is a hand operation that already requires elevation, because
    /// writing <c>darling.json</c> does — INTERACTIVE only ever had Read here, never Write. So the grant
    /// bought nothing and cost a second copy of every secret, readable by any interactively-logged-on user.</para>
    ///
    /// <para>Never fatal — the edit that produced the backup has already been decided on and refusing to
    /// finish it over a permissions problem would leave the operator worse off. But it is reported LOUDLY
    /// and names the file, because a silent best-effort ACL failure is exactly how #1721 persisted
    /// unnoticed across months of service starts: the failure was logged once per start and read by nobody
    /// until a deploy check happened to look. An operator who just ran a command is the one person
    /// guaranteed to be watching, so tell them while they are there.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static void HardenConfigBackup(string backupPath, TextWriter error)
    {
        try
        {
            DarlingFileSecurity.HardenFile(backupPath, allowInteractiveRead: false);
        }
        catch (Exception ex)
        {
            error.WriteLine($"WARNING: could not restrict permissions on the backup {backupPath} ({ex.Message}).");
            error.WriteLine("         It is a full copy of your encrypted passwords and access tokens. Delete it, or");
            error.WriteLine("         restrict it by hand, before leaving this machine.");
            return;
        }

        if (DarlingFileSecurity.IsReadableByOrdinaryUsers(backupPath))
        {
            error.WriteLine($"WARNING: {backupPath} is still readable by ordinary local users after hardening.");
            error.WriteLine("         It is a full copy of your encrypted passwords and access tokens. Delete it, or");
            error.WriteLine("         move the install out of a world-readable folder.");
        }
    }

    /// <summary>True when the current process is running elevated (Administrators role) — required to control the service.</summary>
    [SupportedOSPlatform("windows")]
    private static bool IsElevated()
    {
        using var identity = WindowsIdentity.GetCurrent();
        return new WindowsPrincipal(identity).IsInRole(WindowsBuiltInRole.Administrator);
    }

    /// <summary>
    /// The actionable message for a verb that read EOF where it expected a human (#2097, gotqn): in the
    /// PowerShell ISE, remote/PSRemoting sessions, and some integrated terminals, stdin is not an
    /// interactive console — <c>ReadLine()</c> returns null immediately — and STDERR (where the prompts
    /// and errors went) is not surfaced at all, so the verb read as a silent no-op. This goes to STDOUT,
    /// the one stream every host shows, and names both the cause and the ways forward.
    /// </summary>
    internal static void WriteNonInteractiveGuidance(TextWriter output)
    {
        output.WriteLine();
        output.WriteLine("No interactive input received. This console appears to be non-interactive (PowerShell ISE,");
        output.WriteLine("a remote/PSRemoting session, or redirected stdin) - in these hosts, prompts written to");
        output.WriteLine("stderr may not be shown at all, so the verb looks like it did nothing.");
        output.WriteLine("Run this verb from a regular interactive console (Windows Terminal, powershell.exe, cmd),");
        output.WriteLine("or, for single-value verbs, pipe the value in, e.g.:");
        output.WriteLine("  Read-Host -Prompt 'password' | PerformanceMonitor.Darling.Service.exe --encrypt-password");
    }

    /// <summary>Writes a prompt and reads a trimmed line. Returns null on EOF (input exhausted); an empty line yields <paramref name="defaultValue"/> (or "").</summary>
    private static string? Prompt(TextReader input, TextWriter output, string label, string? defaultValue = null)
    {
        output.Write(defaultValue is null ? $"{label}: " : $"{label} [{defaultValue}]: ");
        var line = input.ReadLine();
        if (line is null)
        {
            return null;
        }

        line = line.Trim();
        return line.Length == 0 ? defaultValue ?? "" : line;
    }

    /// <summary>Yes/no prompt; EOF or an empty line yields <paramref name="defaultYes"/>. A leading 'y' (any case) is yes.</summary>
    private static bool AskYesNo(TextReader input, TextWriter output, string label, bool defaultYes)
    {
        output.Write($"{label} [{(defaultYes ? "Y/n" : "y/N")}]: ");
        var line = input.ReadLine();
        if (line is null)
        {
            return defaultYes;
        }

        line = line.Trim();
        return line.Length == 0 ? defaultYes : line.StartsWith("y", StringComparison.OrdinalIgnoreCase);
    }

    /* ================================================================================================
       --enable-mcp / --disable-mcp / --enable-web / --disable-web: headless endpoint bring-up.

       Two gaps these close on a headless box (no WPF Viewer, and the service runs as a virtual service
       account that CANNOT modify Windows Firewall, so the running service only VERIFIES its rules):
         (a) ENABLE/DISABLE an endpoint. After the first run mcp.enabled/web.enabled in darling.json are only
             a SEED; the store (config.config_service.mcp_enabled/web_enabled) is authoritative and is normally
             toggled only by the Viewer's Settings. These verbs write the store directly — a TARGETED UPDATE
             whose BEFORE-UPDATE self-bump trigger increments config_version, so the worker hot-reloads within
             one sweep (no restart). We NEVER set config_version ourselves (the trigger owns it), and never
             touch paused or the OTHER endpoint's flag.
         (b) OPEN/CLOSE the endpoint's firewall, but only when its darling.json network block opts into LAN
             exposure. Elevated -> run the SAME scoped, idempotent-by-DisplayName rule the host reconciles;
             not elevated -> print the exact elevated command as a handoff (the store toggle already
             succeeded, so a non-elevated shell is never a failure).

       Managed-mode only (the owner credential + firewall are managed concerns; BYO governs its own
       config_service + exposure). Windows-only: DPAPI credential decrypt + WindowsPrincipal + firewall — the
       Program dispatch is OperatingSystem.IsWindows()-guarded, mirroring --print-viewer-connection.
       ================================================================================================ */

    /// <summary>The CLI's targeted store write that ENABLES the MCP endpoint on the single config_service row
    /// (id=1). Sets only <c>mcp_enabled</c> + the audit columns; the BEFORE-UPDATE self-bump trigger fires
    /// <c>config_version</c> (deliberately NOT set here) so the worker hot-reloads. Pure — Darling.Tests pin the shape.
    /// <para>#2414: it also RETURNS <c>mcp_port</c>. The firewall half of this verb has to name its rule for the
    /// port the endpoint BINDS, which is this column and not darling.json's seed, and returning it from the write
    /// itself makes that read free, atomic with the toggle, and impossible to skip — a separate SELECT is a second
    /// resolution path, which is exactly how the two ports drifted apart in the first place.</para></summary>
    public const string EnableMcpStoreSql =
        "UPDATE config.config_service SET mcp_enabled = TRUE, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = 'cli' WHERE id = 1 RETURNING mcp_port";

    /// <summary>The CLI's targeted store write that DISABLES the MCP endpoint (twin of <see cref="EnableMcpStoreSql"/>).</summary>
    public const string DisableMcpStoreSql =
        "UPDATE config.config_service SET mcp_enabled = FALSE, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = 'cli' WHERE id = 1 RETURNING mcp_port";

    /// <summary>The CLI's targeted store write that ENABLES the read-only web dashboard endpoint (twin of <see cref="EnableMcpStoreSql"/>).</summary>
    public const string EnableWebStoreSql =
        "UPDATE config.config_service SET web_enabled = TRUE, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = 'cli' WHERE id = 1 RETURNING web_port";

    /// <summary>The CLI's targeted store write that DISABLES the web dashboard endpoint (twin of <see cref="EnableMcpStoreSql"/>).</summary>
    public const string DisableWebStoreSql =
        "UPDATE config.config_service SET web_enabled = FALSE, updated_at = (now() AT TIME ZONE 'UTC'), updated_by = 'cli' WHERE id = 1 RETURNING web_port";

    /// <summary>The elevated firewall verb's best-effort read of the CONTROL PLANE's effective endpoint toggles
    /// (#2414). Read-only, and the only store contact <c>--configure-firewall</c> makes — see
    /// <see cref="TryReadEndpointTogglesAsync"/> for why it is best-effort rather than required.</summary>
    public const string ReadEndpointTogglesSql =
        "SELECT mcp_enabled, mcp_port, web_enabled, web_port FROM config.config_service WHERE id = 1";

    /// <summary>Which optional endpoint a toggle verb targets — selects the store column, firewall rule name,
    /// darling.json network block, and seed-key note.</summary>
    private enum EndpointKind
    {
        Mcp,
        Web,
    }

    /// <summary>The firewall step a toggle verb takes, from the pure (exposed, elevated) inputs (pin-tested via
    /// <see cref="ClassifyFirewallPlan"/>).</summary>
    public enum EndpointFirewallPlan
    {
        /// <summary>Loopback-only (no LAN-exposure block) — no firewall change is needed.</summary>
        LoopbackNoAction,

        /// <summary>Exposed + elevated — run the scoped enable/disable command directly.</summary>
        RunElevated,

        /// <summary>Exposed + NOT elevated — print the exact elevated command for the operator to run by hand.</summary>
        Handoff,
    }

    /// <summary>PURE firewall-step decision for a toggle verb (unit-tested): a loopback-only endpoint needs no
    /// rule; an exposed endpoint runs the rule when elevated, otherwise hands the exact command off. Shared by
    /// enable and disable alike (disable just runs/hands-off the removal command instead of the open command).</summary>
    public static EndpointFirewallPlan ClassifyFirewallPlan(bool exposed, bool elevated) =>
        !exposed ? EndpointFirewallPlan.LoopbackNoAction
        : elevated ? EndpointFirewallPlan.RunElevated
        : EndpointFirewallPlan.Handoff;

    /// <summary>Whether an ENABLE toggle's <c>allowFrom</c> can be used as a firewall <c>-RemoteAddress</c> (#1646).</summary>
    public enum EndpointAllowFromVerdict
    {
        /// <summary>Absent/blank — the service would fail-close this endpoint to loopback, so there is nothing to open.</summary>
        Missing,

        /// <summary>Present but not a CIDR — REFUSE. Never build a firewall command from it.</summary>
        Invalid,

        /// <summary>A valid CIDR; the canonical <c>IPNetwork.ToString()</c> form is what reaches the command.</summary>
        Valid,
    }

    /// <summary>
    /// PURE <c>allowFrom</c> gate for a toggle verb (#1646). <c>darling.json</c> is operator-supplied text that
    /// <see cref="DarlingConfig.Load"/> only deserializes — it never calls <see cref="DarlingConfig.Validate"/> —
    /// so this was the ONE <see cref="DarlingManagedPostgres.BuildFirewallEnableCommand"/> caller that reached
    /// the PowerShell <c>-Command</c> string with an unparsed value, where a blank-check was the only gate.
    /// Every other call site passes a canonicalized <c>IPNetwork.ToString()</c>; this makes that universal.
    /// Parsing is the security property, not the formatting: <see cref="IPNetwork.TryParse"/> accepts ONLY a
    /// single <c>address/prefix</c> pair, so no shell metacharacter, statement separator, or second CIDR can
    /// survive it — and <paramref name="canonicalCidr"/> is the PARSER'S output, never the caller's string, so
    /// nothing unvalidated is carried through even on the valid path. That last point is load-bearing rather
    /// than belt-and-braces: <c>TryParse</c> MASKS host bits instead of rejecting them (<c>192.168.1.5/24</c>
    /// parses, as <c>192.168.1.0/24</c>), so "validate, then use the original" would forward a string the
    /// parser had already decided meant something else.
    /// </summary>
    public static EndpointAllowFromVerdict ClassifyAllowFrom(string? allowFrom, out string canonicalCidr)
    {
        canonicalCidr = "";

        if (string.IsNullOrWhiteSpace(allowFrom))
        {
            return EndpointAllowFromVerdict.Missing;
        }

        if (!IPNetwork.TryParse(allowFrom.Trim(), out var cidr))
        {
            return EndpointAllowFromVerdict.Invalid;
        }

        canonicalCidr = cidr.ToString();
        return EndpointAllowFromVerdict.Valid;
    }

    /// <summary>
    /// Enables the embedded MCP endpoint on a headless managed deployment: flips
    /// <c>config.config_service.mcp_enabled</c> TRUE (the live switch — the worker hot-reloads within one sweep
    /// via the self-bump trigger, no restart) and, when mcp.network opts into LAN exposure, opens the scoped
    /// firewall rule if elevated (else prints it as an elevated handoff — the toggle still succeeded). Managed-
    /// mode only; Windows-only (the caller is <c>OperatingSystem.IsWindows()</c>-guarded, mirroring
    /// <see cref="PrintViewerConnectionAsync"/>). Returns 0 on a successful toggle; 1 on a load/mode/credential/unseeded error.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static Task<int> EnableMcpAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken) =>
        ToggleEndpointAsync(EndpointKind.Mcp, enable: true, configPath, output, error, cancellationToken);

    /// <summary>Disables the embedded MCP endpoint (twin of <see cref="EnableMcpAsync"/>): flips
    /// <c>mcp_enabled</c> FALSE live and, when exposed, best-effort removes the scoped firewall rule (elevated) or
    /// prints the removal as a handoff. A firewall-removal failure is non-fatal.</summary>
    [SupportedOSPlatform("windows")]
    public static Task<int> DisableMcpAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken) =>
        ToggleEndpointAsync(EndpointKind.Mcp, enable: false, configPath, output, error, cancellationToken);

    /// <summary>Enables the embedded read-only web dashboard endpoint (twin of <see cref="EnableMcpAsync"/> for
    /// <c>web_enabled</c> + the web firewall rule).</summary>
    [SupportedOSPlatform("windows")]
    public static Task<int> EnableWebAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken) =>
        ToggleEndpointAsync(EndpointKind.Web, enable: true, configPath, output, error, cancellationToken);

    /// <summary>Disables the embedded web dashboard endpoint (twin of <see cref="DisableMcpAsync"/> for
    /// <c>web_enabled</c> + the web firewall rule).</summary>
    [SupportedOSPlatform("windows")]
    public static Task<int> DisableWebAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken) =>
        ToggleEndpointAsync(EndpointKind.Web, enable: false, configPath, output, error, cancellationToken);

    /// <summary>The shared body of the four endpoint-toggle verbs: load + managed-mode guard + owner-credential
    /// build (mirroring <see cref="PrintViewerConnectionAsync"/>), a targeted <c>config_service</c> UPDATE (0
    /// rows = an unseeded store), the live-apply note, the firewall step, and the "darling.json enabled is only
    /// the seed" UX note. Never touches config_version (the self-bump trigger owns it) or the OTHER endpoint's flag.</summary>
    [SupportedOSPlatform("windows")]
    private static async Task<int> ToggleEndpointAsync(
        EndpointKind endpoint, bool enable, string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var verb = VerbName(endpoint, enable);
        var endpointLabel = endpoint == EndpointKind.Mcp ? "MCP" : "web dashboard";
        var column = endpoint == EndpointKind.Mcp ? "mcp_enabled" : "web_enabled";
        var seedKey = endpoint == EndpointKind.Mcp ? "mcp.enabled" : "web.enabled";

        /* Managed-mode guard (mirrors PrintViewerConnectionAsync): the owner credential + firewall reconcile are
           managed concerns. In BYO the operator's own PostgreSQL holds config_service — toggle it there. */
        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        if (!postgres.Managed)
        {
            error.WriteLine(ByoEndpointToggleMessage(verb, column, enable));
            return 1;
        }

        /* The OWNER connection (the service's own superuser credential) — null until the worker's first run has
           written the DPAPI-protected credential (i.e. the service has never initialized the store). */
        var connectionString = DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres);
        if (connectionString is null)
        {
            error.WriteLine(DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres));
            return 1;
        }

        /* The TARGETED store write. A DIRECT config_service UPDATE self-bumps config_version via the BEFORE-UPDATE
           trigger, so the worker hot-reloads within one sweep — we never touch config_version ourselves.

           #2414: the statement RETURNS the endpoint's port, so the firewall step below scopes its rule to the port
           the endpoint actually BINDS instead of to darling.json's first-run seed. It is a scalar read of the row
           this verb just wrote, in the same round trip, under the same lock — and it doubles as the seeded check,
           since a RETURNING that yields no row is the 0-rows-updated case. Note what this makes structurally
           impossible: the store is unreachable, and the verb has already returned 1 before any firewall command is
           built, so these two verbs can never open a port on a guessed number. */
        var sql = EndpointToggleSql(endpoint, enable);
        object? returnedPort;
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds };
            returnedPort = await command.ExecuteScalarAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not update the control-plane store: {ex.Message}");
            return 1;
        }

        if (returnedPort is null or DBNull)
        {
            error.WriteLine(
                "The control-plane store is not seeded yet (config.config_service has no id=1 row) — start the " +
                "PerformanceMonitor Darling service once so it seeds the store, then re-run this command.");
            return 1;
        }

        var storePort = Convert.ToInt32(returnedPort, CultureInfo.InvariantCulture);

        output.WriteLine(
            $"{endpointLabel} endpoint {(enable ? "ENABLED" : "DISABLED")} in the control-plane store " +
            $"(config.config_service.{column} = {(enable ? "true" : "false")}).");
        output.WriteLine(
            "The running service applies this LIVE within one collection sweep (the write self-bumps the reload " +
            "beacon) — no restart needed.");

        /* #2414: the SAME resolver the two host supervisors bind on, fed the row this verb just wrote. It carries
           the provenance as well as the value, so the firewall step can state on whose authority it picked a port
           and flag a file/store disagreement instead of silently acting on one. */
        var toggle = DarlingHostBinding.ResolveEndpointToggle(
            (enable, storePort),
            endpoint == EndpointKind.Mcp ? config.Mcp.Enabled : config.Web.Enabled,
            endpoint == EndpointKind.Mcp ? config.Mcp.Port : config.Web.Port);

        await ReconcileEndpointFirewallAsync(endpoint, enable, config, toggle, output, error, cancellationToken);

        output.WriteLine();
        output.WriteLine(
            $"NOTE: '{seedKey}' in darling.json is only the FIRST-RUN seed, not the live switch. After the first run " +
            $"the store (config.config_service.{column}) is authoritative — which is exactly what this command changed.");

        return 0;
    }

    /// <summary>The four endpoint-toggle SQL strings, selected by (endpoint, enable) — the routing the public verbs share.</summary>
    private static string EndpointToggleSql(EndpointKind endpoint, bool enable) => (endpoint, enable) switch
    {
        (EndpointKind.Mcp, true) => EnableMcpStoreSql,
        (EndpointKind.Mcp, false) => DisableMcpStoreSql,
        (EndpointKind.Web, true) => EnableWebStoreSql,
        (EndpointKind.Web, false) => DisableWebStoreSql,
        _ => throw new ArgumentOutOfRangeException(nameof(endpoint)),
    };

    /// <summary>
    /// What a bring-your-own deployment is told instead of a toggle: the flags live in the operator's own
    /// PostgreSQL, and the UPDATE is the whole procedure. Composed here rather than inline because #2626
    /// needs the SAME sentence from a second caller — the platform guard, which is the one a non-Windows
    /// operator actually reaches.
    /// </summary>
    private static string ByoEndpointToggleMessage(string verb, string column, bool enable) =>
        $"{verb} applies to the managed store only. In bring-your-own mode (postgres.connectionString), the "
        + $"endpoint enable flags live in YOUR PostgreSQL's config.config_service ({column}) — toggle them "
        + $"there:\n    UPDATE config.config_service SET {column} = {(enable ? "true" : "false")};\n"
        + "The service picks that up within one sweep; no restart is needed.";

    /// <summary>
    /// The message for an endpoint-toggle verb invoked on a non-Windows host (#2626).
    ///
    /// <para>
    /// "requires Windows" alone is true of the verb and MISLEADING about the situation. The two Windows
    /// dependencies — the DPAPI-protected owner credential and the firewall reconcile — are both MANAGED-mode
    /// concerns, and a non-Windows deployment is necessarily bring-your-own, where the verb would refuse
    /// anyway with the message that actually helps. Reading the platform message on macOS or Linux, an
    /// operator concludes the DASHBOARD is Windows-only; it is not, and one UPDATE turns it on.
    /// </para>
    ///
    /// <para>
    /// So the config is loaded here, best effort, purely to decide which sentence to print. A config that
    /// cannot be loaded, or a managed one (which on a non-Windows host should not exist), gets the platform
    /// sentence — the honest answer when we cannot tell.
    /// </para>
    /// </summary>
    public static int WriteEndpointVerbPlatformRefusal(
        bool isMcp, bool enable, string? configPath, TextWriter error)
    {
        var endpoint = isMcp ? EndpointKind.Mcp : EndpointKind.Web;
        var verb = VerbName(endpoint, enable);
        var column = isMcp ? "mcp_enabled" : "web_enabled";

        var managed = true;
        try
        {
            managed = DarlingConfig.Load(configPath).Postgres?.Managed ?? true;
        }
        catch
        {
            /* Deliberately swallowed: this method exists to pick a sentence, and a config we cannot read is
               not a reason to fail differently than we already are. */
        }

        error.WriteLine(managed
            ? $"{verb} requires Windows (DPAPI + firewall)."
            : ByoEndpointToggleMessage(verb, column, enable));

        return 1;
    }

    /// <summary>The verb spelling for a toggle (for error + handoff text).</summary>
    private static string VerbName(EndpointKind endpoint, bool enable) => (endpoint, enable) switch
    {
        (EndpointKind.Mcp, true) => "--enable-mcp",
        (EndpointKind.Mcp, false) => "--disable-mcp",
        (EndpointKind.Web, true) => "--enable-web",
        (EndpointKind.Web, false) => "--disable-web",
        _ => throw new ArgumentOutOfRangeException(nameof(endpoint)),
    };

    /// <summary>
    /// The firewall half of a toggle (defense-in-depth, never the boundary — pg_hba/token + the in-app CIDR
    /// check are). Only acts when the endpoint's darling.json network block opts into LAN exposure (a non-loopback
    /// listen, via the shared <see cref="DarlingNetwork.IsExposedListenAddress"/>). Uses the SAME scoped,
    /// idempotent-by-DisplayName rule name the host's start-up check looks for
    /// (<see cref="DarlingMcpHostService.McpFirewallRuleName"/> / <see cref="DarlingWebHostService.WebFirewallRuleName"/>)
    /// and the SAME pure command builders. Elevated -> runs the rule; otherwise prints the exact elevated command
    /// — the store toggle already succeeded, so a non-elevated shell is a handoff, never a failure. A firewall
    /// failure is likewise non-fatal.
    ///
    /// <para>#2414: the port comes from <paramref name="toggle"/>, i.e. from the control plane, NOT from
    /// <c>config.Mcp.Port</c>/<c>config.Web.Port</c>. Those are the first-run seed; the supervisor binds the store's
    /// value, so naming the rule from the file opened one port while the endpoint served another. The caller
    /// resolves the toggle from the row it just wrote, so this method cannot be reached with a guessed port.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static async Task ReconcileEndpointFirewallAsync(
        EndpointKind endpoint, bool enable, DarlingConfig config, DarlingHostBinding.EndpointToggle toggle,
        TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        var (section, surface, filePort, listen, allowFrom) = endpoint == EndpointKind.Mcp
            ? ("mcp", "MCP", config.Mcp.Port, config.Mcp.Network?.Listen, config.Mcp.Network?.AllowFrom)
            : ("web", "web dashboard", config.Web.Port, config.Web.Network?.Listen, config.Web.Network?.AllowFrom);

        var port = toggle.Port;
        var ruleName = endpoint == EndpointKind.Mcp
            ? DarlingMcpHostService.McpFirewallRuleName(port)
            : DarlingWebHostService.WebFirewallRuleName(port);

        var exposed = DarlingNetwork.IsExposedListenAddress(listen);
        var plan = ClassifyFirewallPlan(exposed, IsElevated());

        output.WriteLine();
        if (plan == EndpointFirewallPlan.LoopbackNoAction)
        {
            output.WriteLine(enable
                ? "Firewall: this endpoint has no LAN-exposure block, so it binds LOOPBACK ONLY — no firewall change is " +
                  "needed. To expose it on the LAN, run --configure-network (which writes the listen/allowFrom/token block)."
                : "Firewall: this endpoint is loopback-only — there is no scoped firewall rule to remove.");
            return;
        }

        /* #1646: parse allowFrom as a CIDR BEFORE it can reach a PowerShell -Command string, and pass the
           parser's canonical form — the posture every other BuildFirewallEnableCommand caller already had.
           An unparseable value is refused outright: the firewall is NOT touched and nothing is printed for an
           operator to paste into an elevated shell, because the injected text would run either way (this verb
           runs the command itself when elevated, and hands it to a human to run elevated when it is not). */
        var canonicalCidr = "";
        if (enable)
        {
            switch (ClassifyAllowFrom(allowFrom, out canonicalCidr))
            {
                case EndpointAllowFromVerdict.Missing:
                    /* Non-loopback listen but no allowFrom CIDR: the service itself would fail-close this to loopback, so
                       there is nothing to open. Point at the wizard rather than emit a malformed New-NetFirewallRule. */
                    output.WriteLine(
                        $"Firewall: the network block sets listen '{listen}' but no allowFrom CIDR, so the service will bind " +
                        "loopback-only until it is completed. Run --configure-network to finish the block; not opening the firewall.");
                    return;

                case EndpointAllowFromVerdict.Invalid:
                    error.WriteLine(
                        $"Firewall: allowFrom in darling.json is not a valid CIDR, so NO firewall change was made and no " +
                        "command is being printed to run by hand. The endpoint toggle itself already succeeded; the service " +
                        "will bind loopback-only until allowFrom is fixed. Expected an address/prefix with the host bits " +
                        "zeroed, e.g. 192.168.1.0/24 or 2001:db8::/32. Run --configure-network to rewrite the block.");
                    return;
            }
        }

        /* #2414: state which plane named the port BEFORE doing anything with it. Here the control plane always
           answered — the caller could not have got this far otherwise — so this is either a confirmation or the
           report of a file/store disagreement the operator has never been shown. Only on ENABLE: a disable sweeps
           every port of the surface, so which one the store named decides nothing and claiming otherwise would be
           a line that describes work this verb is not doing. */
        if (enable)
        {
            output.WriteLine(DarlingHostBinding.DescribeFirewallPortAuthority(toggle, section, surface, filePort, null));
        }

        /* #2414, the security half: sweep EVERY port of this surface before ensuring the desired rule, rather than
           reconciling the one exact DisplayName. The port lives in the rule's name, so moving a port does not
           update a rule — it creates a second one and strands the first as an inbound allow rule on a port nothing
           serves. Exact-name reconciliation can by construction never reach that rule; it is not even aware of it.
           The wildcard comes from DarlingFirewallCheck.SurfaceRuleWildcard, which returns the name UNCHANGED when
           it does not parse, so the widening is provably confined to other ports of THIS surface and can never
           reach a rule this product did not create. --configure-firewall has swept this way since #1771; the
           toggle verbs, which are the ones an operator reaches for after changing a port, did not. */
        var wildcard = DarlingFirewallCheck.SurfaceRuleWildcard(ruleName);
        var sweepCommand = DarlingManagedPostgres.BuildFirewallSweepCommand(wildcard);
        var openCommand = enable
            ? DarlingManagedPostgres.BuildFirewallEnableCommand(ruleName, port, canonicalCidr)
            : null;

        if (plan == EndpointFirewallPlan.RunElevated)
        {
            /* Its own step, never concatenated ahead of the open: the sweep command ends in `exit 0` and would
               otherwise terminate the shell before the rule was created. A failure in either step is reported with
               the exact command and is non-fatal — the store toggle already succeeded. */
            await TryRunFirewallStepAsync(
                sweepCommand,
                enable
                    ? $"Firewall: cleared any previous {surface} rule matching '{wildcard}'."
                    : $"Firewall: removed every {surface} rule matching '{wildcard}'.",
                $"Firewall: could not remove the rule(s) matching '{wildcard}'",
                output, error, cancellationToken);

            if (openCommand is not null)
            {
                await TryRunFirewallStepAsync(
                    openCommand,
                    $"Firewall rule '{ruleName}' opened (TCP {port}, inbound, from {canonicalCidr}).",
                    $"Firewall rule open did not confirm for '{ruleName}'",
                    output, error, cancellationToken);
            }

            return;
        }

        /* Handoff (not elevated) — the store toggle already succeeded; print the exact commands to run elevated. */
        output.WriteLine(enable
            ? "Firewall: this shell is not elevated, so the endpoint was enabled but its firewall rule was NOT opened. " +
              "Run these in an ELEVATED PowerShell — the first clears any rule left on a previously configured port, " +
              "the second opens this one (scoped to the port + CIDR):"
            : "Firewall: this shell is not elevated, so the firewall rule was NOT removed. Run this in an ELEVATED " +
              "PowerShell to close the port (it covers every port this surface has ever been configured on):");
        output.WriteLine("  " + sweepCommand);
        if (openCommand is not null)
        {
            output.WriteLine("  " + openCommand);
        }
    }

    /* ================================================================================================
       --configure-firewall: the ELEVATED owner of every scoped Darling firewall rule (#1771).

       The service runs as an unprivileged virtual account and CANNOT create firewall rules. It used to try
       on every start anyway, failing "Access is denied" each time; on a fresh networked install — the normal
       deployment mode — that meant the rule was never created at all and remote clients were simply blocked.
       Rule management therefore belongs to the elevated context that already exists: install-darling.ps1
       calls this verb, uninstall-darling.ps1 removes the rules, and the running service only VERIFIES
       (DarlingFirewallCheck).

       It reconciles all THREE surfaces (store, MCP, web) in one pass. #2414 gave it ONE optional store read:
       the MCP and web PORTS are control-plane-authoritative (config_service.mcp_port/web_port), and naming a
       rule from darling.json's seed opened one port while the endpoint served another. The read is best-effort
       and never a precondition — this verb still has to work at install time, before the store exists, where
       the file's port is the value the store will be seeded WITH. When the read fails, the verb says which port
       it used and why, and it never falls back silently: see TryReadEndpointTogglesAsync.

       #2436 followed the same read one step further, into what the verb is entitled to DO with it. The store
       row carries an enable flag as well as a port, so a surface the control plane has switched off gets its
       rule removed rather than opened — the posture --disable-mcp already had, which this verb used to undo on
       every upgrade (see DescribeDisabledSurface). And when the read FAILS, the verb stops removing a
       LAN-exposed surface's existing rules at all — neither the port nor the enable flag is knowable then, and
       both are ways to end up deleting the rule the endpoint is actually being served on. It still creates the
       file's rule, which is what a fresh install needs (see FirewallRulePlan.SweepOtherPorts).
       ================================================================================================ */

    /// <summary>What <c>--configure-firewall</c> will do to one surface's rule.</summary>
    public enum FirewallRuleAction
    {
        /// <summary>The config exposes this surface on the LAN — create/refresh the scoped allow rule.</summary>
        Open,

        /// <summary>The surface is loopback-only (by config, or fail-closed by its own resolver) — the desired
        /// state is NO rule. Removal is idempotent, so this is also the no-op for a rule that never existed.</summary>
        Remove,
    }

    /// <summary>One surface's desired firewall state. <paramref name="Note"/> is a human explanation printed
    /// alongside, non-null only where the reason is not self-evident (a fail-closed exposure, or a surface the
    /// control plane has switched off).
    /// <paramref name="PortNote"/> (#2414) says which PLANE supplied <paramref name="Port"/> and, when the store
    /// could not be read, what would make the chosen port wrong — non-null only on an Open plan, since a Remove
    /// sweeps every port of the surface and does not depend on picking the right one.
    /// <para><paramref name="SweepOtherPorts"/> (#2436) is permission to remove this surface's rules on ports
    /// OTHER than <paramref name="Port"/>. That sweep is how a rule stranded by a port change gets collected,
    /// and its whole justification is that the sweeper knows what this surface is actually doing — so it is
    /// withheld on exactly one combination: a surface darling.json exposes on the LAN, on a run that could not
    /// read the control plane. There, BOTH store-backed values are a guess. The port may have moved, so another
    /// port's rule may be the live one; and <c>mcp_enabled</c> may have been turned on with <c>--enable-mcp</c>
    /// or in the Viewer, which never writes back to darling.json — so the file's <c>enabled = false</c> may be
    /// years stale and the surface may be serving right now. Either way the wildcard would delete the rule the
    /// endpoint is being served on, which is the outage this whole change exists to prevent. Nothing is exposed
    /// by deferring: the store is unreadable because the service is stopped, so no Darling endpoint is
    /// listening on any port until a run that CAN read the control plane is possible again.</para>
    /// <para>The other half is certain and always sweeps. Whether a surface is network-exposed at all is
    /// decided by <c>mcp.network</c> / <c>web.network</c>, which are FILE-ONLY and have no config_service
    /// equivalent by design (#2389) — the control plane can switch an exposed surface off, never switch an
    /// unexposed one on. So a bind that resolves loopback-only is loopback-only whatever the store would have
    /// said, no rule belongs on any port, and collecting them needs no knowledge this run is missing. Same for
    /// the store surface, whose <c>postgres.port</c> has no config_service column to disagree with it.</para></summary>
    public readonly record struct FirewallRulePlan(
        string Surface, string RuleName, int Port, FirewallRuleAction Action, string? Cidr, string? Note, string? PortNote,
        bool SweepOtherPorts = true);

    /// <summary>
    /// PURE desired-state for all three rules, so the whole decision pins without a live firewall.
    /// <para>Every surface is resolved by the SAME resolver the RUNNING service fail-closes on
    /// (<see cref="DarlingManagedPostgres.ResolveNetworkExposure"/>,
    /// <see cref="DarlingMcpHostService.ResolveMcpBind"/>, <see cref="DarlingWebHostService.ResolveWebBind"/>)
    /// rather than by re-reading <c>listen</c>/<c>allowFrom</c> here. That is the load-bearing choice: a config
    /// the service degrades to loopback (an unparseable listen, a mismatched address family, a missing CIDR)
    /// must NOT get an open port, and the service's own start-up check must reach the same verdict this did —
    /// otherwise every start would report a rule this verb had just deliberately created.</para>
    /// <para>BYO mode resolves loopback for MCP/web (their resolvers take <c>managed</c> and refuse exposure
    /// without it) and skips the store entirely, whose exposure the operator's own PostgreSQL governs.</para>
    /// <para>#2414: the MCP and web PORTS come from <paramref name="mcpStore"/>/<paramref name="webStore"/> when
    /// the caller could read <c>config.config_service</c>, because that is the port the supervisor binds; they fall
    /// back to darling.json's seed when it could not, which is the normal state at install time and carries a
    /// <see cref="FirewallRulePlan.PortNote"/> saying so. The store surface has no such split — <c>postgres.port</c>
    /// is file-only, with no config_service column to disagree with it — so it is resolved from the file, full
    /// stop, and gets no note.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static IReadOnlyList<FirewallRulePlan> PlanFirewallRules(
        DarlingConfig config,
        (bool Enabled, int Port)? mcpStore = null,
        (bool Enabled, int Port)? webStore = null,
        string? storeUnavailableReason = null)
    {
        var plans = new List<FirewallRulePlan>();
        var managed = config.Postgres?.Managed ?? false;

        if (managed && config.Postgres is not null)
        {
            var (certPath, keyPath) = ResolveStoreCertPaths(config.Postgres);
            var store = DarlingManagedPostgres.ResolveNetworkExposure(config.Postgres.Network, certPath, keyPath);
            plans.Add(new FirewallRulePlan(
                "store",
                DarlingManagedPostgres.StoreFirewallRuleName(config.Postgres.Port),
                config.Postgres.Port,
                store.Exposed ? FirewallRuleAction.Open : FirewallRuleAction.Remove,
                store.Exposed ? store.Cidr : null,
                store.DegradeReason,
                null,
                /* postgres.port is file-only — there is no config_service column able to disagree with it — so
                   this surface's port is never a guess and the sweep is always authoritative. */
                SweepOtherPorts: true));
        }

        /* #2414: the SAME resolver the MCP/web supervisors bind on, so the rule this verb writes is named for the
           port the endpoint serves. A null store answer resolves to the file value with Origin = File, which the
           describer turns into the loud fallback disclosure rather than a silent guess. */
        var mcpToggle = DarlingHostBinding.ResolveEndpointToggle(mcpStore, config.Mcp.Enabled, config.Mcp.Port);
        var mcpBind = DarlingMcpHostService.ResolveMcpBind(config.Mcp, managed);
        var mcpExposed = mcpBind.Mode == DarlingMcpHostService.McpBindMode.NetworkAndLoopback;
        var mcpOpen = mcpExposed && mcpToggle.Enabled;
        plans.Add(new FirewallRulePlan(
            "MCP",
            DarlingMcpHostService.McpFirewallRuleName(mcpToggle.Port),
            mcpToggle.Port,
            mcpOpen ? FirewallRuleAction.Open : FirewallRuleAction.Remove,
            mcpOpen ? CanonicalCidrOrNull(config.Mcp.Network?.AllowFrom) : null,
            mcpOpen
                ? null
                : mcpExposed
                    ? DescribeDisabledSurface(mcpToggle, "mcp", storeUnavailableReason)
                    : DescribeLoopbackReason(mcpBind.Reason, "mcp"),
            mcpOpen
                ? DarlingHostBinding.DescribeFirewallPortAuthority(mcpToggle, "mcp", "MCP", config.Mcp.Port, storeUnavailableReason)
                : null,
            /* NOT !mcpOpen: a Remove reached because the FILE said disabled is exactly as much a guess as a
               stale port, because --enable-mcp and the Viewer write only the store. mcpExposed is the half
               that is certain — network.* is file-only, so the control plane can never expose what the file
               does not. */
            SweepOtherPorts: !mcpExposed || mcpToggle.Origin == DarlingHostBinding.EndpointToggleOrigin.ControlPlane));

        var webToggle = DarlingHostBinding.ResolveEndpointToggle(webStore, config.Web.Enabled, config.Web.Port);
        var webBind = DarlingWebHostService.ResolveWebBind(config.Web, managed);
        var webExposed = webBind.Mode == DarlingHostBinding.BindMode.NetworkAndLoopback;
        var webOpen = webExposed && webToggle.Enabled;
        plans.Add(new FirewallRulePlan(
            "web dashboard",
            DarlingWebHostService.WebFirewallRuleName(webToggle.Port),
            webToggle.Port,
            webOpen ? FirewallRuleAction.Open : FirewallRuleAction.Remove,
            webOpen ? CanonicalCidrOrNull(config.Web.Network?.AllowFrom) : null,
            webOpen
                ? null
                : webExposed
                    ? DescribeDisabledSurface(webToggle, "web", storeUnavailableReason)
                    : DescribeLoopbackReason((DarlingMcpHostService.McpBindReason)webBind.Reason, "web"),
            webOpen
                ? DarlingHostBinding.DescribeFirewallPortAuthority(webToggle, "web", "web dashboard", config.Web.Port, storeUnavailableReason)
                : null,
            SweepOtherPorts: !webExposed || webToggle.Origin == DarlingHostBinding.EndpointToggleOrigin.ControlPlane));

        return plans;
    }

    /// <summary>The parser's canonical CIDR, or null when it will not parse. The bind resolvers have already
    /// refused exposure in the null case, so an Open plan always carries a real CIDR; this never forwards the
    /// caller's raw string into a PowerShell command (#1646).</summary>
    private static string? CanonicalCidrOrNull(string? allowFrom) =>
        ClassifyAllowFrom(allowFrom, out var canonical) == EndpointAllowFromVerdict.Valid ? canonical : null;

    /// <summary>
    /// Why a surface that IS configured for LAN exposure still gets no rule: it is switched OFF (#2436), so
    /// the supervisor never starts it and there is nothing behind the port.
    ///
    /// <para><b>The decision, because the alternative is arguable.</b> Leaving the rule open would mean an
    /// operator who enables the endpoint from the Viewer's Settings — a store write the running service picks
    /// up within one poll interval, with no elevated step anywhere in it — finds the LAN path already open.
    /// That convenience is real, and it is what this verb used to do. It loses to three things. The product's
    /// own posture everywhere else is that no listener means no rule: <c>--disable-mcp</c> sweeps the surface,
    /// <see cref="DarlingMcpHostService"/>'s stop path documents an admin removing the rule with THIS verb,
    /// and #2414 was fixed precisely because an inbound allow on a port nothing serves is the inverse of what
    /// scoping a rule to a port is for. Worse, the two disagreed: <c>--disable-mcp</c> closed the port and the
    /// next <c>--configure-firewall</c> — which every upgrade runs — silently re-opened it, so the disable verb
    /// did not stay done. And the convenience is not lost, only deferred to one elevated action the service
    /// already asks for by name: its start-up check reports the missing rule and prints the command.</para>
    ///
    /// <para>Two messages, on the same three-state honesty <see cref="DarlingHostBinding.DescribeFirewallPortAuthority"/>
    /// applies to the port — and for a reason review had to point out. <c>Origin == File</c> is NOT "fresh
    /// install": <see cref="TryReadEndpointTogglesAsync"/> collapses BYO, a missing credential, a timeout and
    /// every other connection failure into the same answer, so this branch is also reached on a long-lived box
    /// whose store is merely unreachable this minute and may well hold <c>mcp_enabled = true</c>. Asserting the
    /// endpoint is off there would contradict the sweep-declined line printed a few lines later in the same
    /// run, which says the opposite — that the off may be stale and the rule may be the live one — and it is
    /// the sweep-declined line that is right.</para>
    /// </summary>
    private static string DescribeDisabledSurface(
        DarlingHostBinding.EndpointToggle toggle, string section, string? storeUnavailableReason)
        => toggle.Origin == DarlingHostBinding.EndpointToggleOrigin.ControlPlane
            ? $"{section}.network exposes this endpoint, but the CONTROL PLANE has it off "
                + $"(config.config_service.{section}_enabled = false), so the service does not start it and no rule "
                + $"belongs on that port — turn it on with --enable-{section} or in the Viewer's Settings, then "
                + "re-run --configure-firewall from an elevated prompt"
            : $"{section}.network exposes this endpoint, but the control plane could NOT be read "
                + $"({storeUnavailableReason ?? "reason unknown"}), so this run goes on darling.json's "
                + $"{section}.enabled = false and opens nothing. On a box whose store has never been written — the "
                + $"normal state at install time — that is right: config.config_service.{section}_enabled is SEEDED "
                + "from this value, so the endpoint will not start. On a box that has run before it may be stale, "
                + $"because --enable-{section} and the Viewer's Settings write only the control plane and never back "
                + "to the file; if the endpoint IS enabled there, re-run --configure-firewall once the store is up "
                + "and it will open the port";

    /// <summary>Why a surface is loopback-only, when the reason is a DEGRADE worth printing. A plain
    /// loopback-by-default config is the normal case and gets no note.</summary>
    private static string? DescribeLoopbackReason(DarlingMcpHostService.McpBindReason reason, string section) =>
        reason switch
        {
            DarlingMcpHostService.McpBindReason.TokenMissing =>
                $"{section}.network is set but its token is missing or unreadable, so the service fail-closes this endpoint to loopback",
            DarlingMcpHostService.McpBindReason.AllowFromInvalid =>
                $"{section}.network.allowFrom is missing or not a valid CIDR, so the service fail-closes this endpoint to loopback",
            DarlingMcpHostService.McpBindReason.ManagedModeRequired =>
                $"{section}.network is set but postgres.managed = false; LAN exposure is managed-mode only and is ignored",
            _ => null,
        };

    /// <summary>
    /// One target of <see cref="HardenFiles"/>: a path, whether the interactive operator legitimately reads it,
    /// and what it is called in the report. Kept as data so the list is readable as a policy rather than as
    /// control flow — which file gets INTERACTIVE read is the only judgement in this verb, and it should be
    /// visible at a glance.
    /// </summary>
    private readonly record struct HardenTarget(string Path, bool AllowInteractive, bool IsDirectory, string What);

    /// <summary>
    /// Re-applies the secret-file ACLs, elevated (#2352).
    ///
    /// <para><b>Why this exists as a verb.</b> The service already computes the correct DACL
    /// (<see cref="DarlingFileSecurity.HardenFile"/>) and already detects when the real one is wrong
    /// (<see cref="DarlingFileSecurity.IsReadableByOrdinaryUsers"/>). What it lacks is authority: re-ACLing a
    /// file it does not own needs WRITE_DAC, and taking ownership needs a privilege a virtual service account is
    /// not granted, so it can only log the remedy and continue. Until now the only thing that ever APPLIED the
    /// rule to an existing install was <c>install-darling.ps1</c>, which leaves anyone who registered the exe by
    /// hand — the README's own <c>sc create</c> path — typing three <c>icacls</c> lines out of a log message.</para>
    ///
    /// <para><b>It verifies rather than claims.</b> Every target is re-read after the attempt and reported as
    /// SECURED or STILL READABLE, and the exit code is 1 if anything is still exposed. "We tried" is not the
    /// same statement as "the secret is not readable" — a distinction <see cref="TryHardenExportedSecret"/>'s own
    /// contract already makes, and the reason a permissions call that silently did nothing was able to hide.</para>
    ///
    /// <para>Idempotent, so it is safe on a healthy box and can simply live in a runbook. Missing targets are
    /// skipped quietly: a BYO-Postgres install has no managed credential files, and their absence is not a
    /// fault.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static int HardenFiles(string? configPath, TextWriter output, TextWriter error)
    {
        var resolvedConfig = DarlingConfig.ResolveConfigPath(configPath);

        /* The managed store's directory is derived from config when it loads, but this verb has to work when
           darling.json is exactly what is unreadable — that is the failure it exists to repair. So a config that
           will not load is a warning, not a stop: the config file itself is still hardened, and the store
           targets fall back to the documented default location. */
        string? dataDirectory = null;
        try
        {
            var config = DarlingConfig.Load(configPath);
            dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
        }
        catch (Exception ex)
        {
            output.WriteLine($"NOTE: could not load {resolvedConfig} ({ex.Message}).");
            output.WriteLine("      Continuing with the documented default store location — hardening darling.json");
            output.WriteLine("      is very often what makes it loadable again.");
            dataDirectory = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "PerformanceMonitorDarling", "pg");
        }

        var storeRoot = Path.GetDirectoryName(Path.GetFullPath(dataDirectory));
        var targets = new List<HardenTarget>
        {
            /* INTERACTIVE read, alone in this list: the Viewer (ViewerSettings.ResolveConfigPath) and the CLI
               verbs run as the operator and must still read the live config. Nothing reads a backup (#1769). */
            new(resolvedConfig, AllowInteractive: true, IsDirectory: false, "the live config"),
        };

        foreach (var backup in SafeEnumerate(Path.GetDirectoryName(resolvedConfig), "darling.json.bak-*"))
        {
            targets.Add(new(backup, AllowInteractive: false, IsDirectory: false, "a config backup"));
        }

        if (!string.IsNullOrEmpty(storeRoot))
        {
            targets.Add(new(storeRoot, AllowInteractive: false, IsDirectory: true, "the store directory"));
            targets.Add(new(Path.Combine(storeRoot, "pg-credential.dpapi"), false, false, "the store credential"));
            targets.Add(new(Path.Combine(storeRoot, "pg-admin-credential.dpapi"), false, false, "the admin credential"));
        }

        /* #2371: harden for the account the SERVICE runs as, not for whoever is running THIS. The verb is
           documented to be run elevated and exists because the service cannot re-ACL a file it does not own,
           so the caller is never the service — and resolving from the caller granted the operator and stripped
           the service, leaving an install that worked until its next restart. Falls back to the caller when
           the service is not registered (a console run, or hardening a tree before install), which is the only
           case where those two are legitimately the same account. */
        var registered = DarlingFileSecurity.RegisteredServiceAccount(ServiceName);
        if (registered is not null)
        {
            DarlingFileSecurity.HardenForAccount(registered);
        }

        output.WriteLine($"Hardening for service account: {DarlingFileSecurity.ServiceAccountDisplayName}");
        if (registered is null)
        {
            output.WriteLine(
                $"  (the '{ServiceName}' service is not registered on this machine, so this is the account " +
                "you are running as - install the service first if you expected its own account here)");
        }

        output.WriteLine();

        var exposed = 0;
        var touched = 0;

        foreach (var target in targets)
        {
            var exists = target.IsDirectory ? Directory.Exists(target.Path) : File.Exists(target.Path);
            if (!exists)
            {
                continue;
            }

            touched++;

            try
            {
                if (target.IsDirectory)
                {
                    /* Traverse, not read: the operator's Viewer needs to walk to the config, never to read the
                       credential blobs sitting in here. Mirrors DarlingManagedPostgres' own call. */
                    DarlingFileSecurity.HardenDirectory(target.Path, allowInteractiveTraverse: true);
                }
                else
                {
                    DarlingFileSecurity.HardenFile(target.Path, target.AllowInteractive);
                }
            }
            catch (Exception ex)
            {
                error.WriteLine($"  FAILED   {target.Path} ({target.What}): {ex.Message}");
                error.WriteLine($"           {DarlingFileSecurity.DescribeOwnerAndExposure(target.Path)}");
                exposed++;
                continue;
            }

            /* The claim is the re-read, not the call that returned without throwing. */
            if (DarlingFileSecurity.IsReadableByOrdinaryUsers(target.Path))
            {
                error.WriteLine($"  STILL READABLE  {target.Path} ({target.What})");
                error.WriteLine($"           {DarlingFileSecurity.DescribeOwnerAndExposure(target.Path)}");
                exposed++;
            }
            else if (!DarlingFileSecurity.GrantsHardenedAccount(target.Path))
            {
                /* #2371: private is only half of correct. An ACL that excludes ordinary users but also excludes
                   the service is a locked-out install, and it reports as SECURED under the readability check
                   alone — then fails on the next start, far enough away that nobody connects the two. */
                error.WriteLine($"  LOCKED OUT  {target.Path} ({target.What})");
                error.WriteLine(
                    $"           secured, but {DarlingFileSecurity.ServiceAccountDisplayName} cannot read it - " +
                    "the service would fail on its next start. Grant that account and re-run.");
                exposed++;
            }
            else
            {
                output.WriteLine($"  SECURED  {target.Path} ({target.What})");
            }
        }

        output.WriteLine();

        if (touched == 0)
        {
            error.WriteLine("Nothing to harden: no config and no store files were found at the expected paths.");
            error.WriteLine($"Looked for the config at {resolvedConfig}. Pass an explicit path as the second argument.");
            return 1;
        }

        if (exposed > 0)
        {
            error.WriteLine($"{exposed} of {touched} item(s) are STILL readable by ordinary users.");
            error.WriteLine("Run this from an ELEVATED prompt. If it was already elevated, the owner is the");
            error.WriteLine("problem: ownership carries WRITE_DAC, so take ownership first, then re-run.");
            return 1;
        }

        output.WriteLine($"All {touched} item(s) secured. The service re-asserts these ACLs at every start.");
        return 0;
    }

    /// <summary>Directory enumeration that treats an unreadable or missing folder as empty — this verb runs
    /// precisely when permissions are broken, so a throw here would defeat its purpose.</summary>
    private static IEnumerable<string> SafeEnumerate(string? directory, string pattern)
    {
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory))
        {
            return Array.Empty<string>();
        }

        try
        {
            return Directory.EnumerateFiles(directory, pattern).ToList();
        }
        catch (Exception)
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>What the NOT-elevated <c>--configure-firewall</c> branch owes the operator for one surface
    /// (#2445), and therefore whether an elevated shell really has work to do.</summary>
    public enum FirewallHandoff
    {
        /// <summary>Nothing to hand over — the desired state already holds, or this run is not entitled to ask
        /// for the change. Prints no command and does not make the verb fail.</summary>
        Nothing,

        /// <summary>A rule has to be CREATED. Prints the enable command and fails the verb, exactly as this
        /// branch always has for an Open plan.</summary>
        OpenCommand,

        /// <summary>A rule the desired state forbids was FOUND by the read-only probe. Prints the same sweep
        /// command the elevated path would have run, and fails the verb — there is measured elevated work.</summary>
        SweepCommand,

        /// <summary>The probe could not answer, so whether such a rule exists is unknown. Prints the sweep
        /// command, because it is a no-op when there is nothing to remove and the operator can only act on what
        /// they are handed — but does NOT fail the verb. An exit code here is a claim about the FIREWALL, and
        /// "the probe did not answer" is not one.</summary>
        SweepCommandUnverified,
    }

    /// <summary>
    /// PURE: what the not-elevated branch owes ONE plan. <paramref name="rulesFound"/> is the read-only probe's
    /// answer for this surface's wildcard — null when it could not answer — and is only ever consulted for a
    /// Remove, because only there does the answer change anything.
    ///
    /// <para><b>Why the Remove half is measured and the Open half is not.</b> An Open plan already knows the
    /// verb's whole job: the rule has to exist with these exact parameters, the enable command is idempotent
    /// (it removes its own DisplayName first), and a probe could at best say a rule with that name exists —
    /// not that its port, direction and RemoteAddress are the ones this config asks for. So the probe would buy
    /// nothing there and this keeps the pre-#2445 behaviour exactly. A Remove is the opposite: the desired
    /// state is the ABSENCE of a rule, absence is the overwhelmingly common case (every default loopback
    /// install), and the two things this branch has to decide — whether to print a command at all, and whether
    /// to report failure — are BOTH answered by "is one actually there". The alternative considered and
    /// rejected was gating on <c>Note is not null</c>, which is a proxy for "a rule might exist" and is wrong in
    /// both directions: it prints a sweep for a fail-closed surface that never had a rule, and stays silent
    /// about a plain-loopback surface holding a stale rule from an exposure someone turned off by hand — the
    /// exact state <see cref="FirewallRuleVerdict.LoopbackStaleRule"/> exists to name.</para>
    ///
    /// <para><b>Why a withheld sweep hands over nothing at all.</b> #2436 withholds
    /// <see cref="FirewallRulePlan.SweepOtherPorts"/> for a surface darling.json exposes on a run that could not
    /// read the control plane, because there the file's "switched off" may be years stale and the rule matching
    /// the wildcard may be the one the endpoint is being served on. Printing that sweep command would hand the
    /// operator, by copy and paste, the very outage the elevated path just declined to cause — and the operator
    /// pasting it has no way to see that it was declined. The plan's <see cref="FirewallRulePlan.Note"/> is
    /// printed above and says why; the remedy is a re-run with the store up, not a command.</para>
    /// </summary>
    public static FirewallHandoff ClassifyNotElevatedHandoff(
        FirewallRuleAction action, bool sweepOtherPorts, bool? rulesFound) =>
        action == FirewallRuleAction.Open ? FirewallHandoff.OpenCommand
        : !sweepOtherPorts ? FirewallHandoff.Nothing
        : rulesFound switch
        {
            true => FirewallHandoff.SweepCommand,
            false => FirewallHandoff.Nothing,
            _ => FirewallHandoff.SweepCommandUnverified,
        };

    /// <summary>
    /// PURE: whether a not-elevated run has to report failure — true only where an elevated shell really has
    /// work to do, which is what the exit code is supposed to mean.
    /// <para><see cref="FirewallHandoff.SweepCommandUnverified"/> deliberately does not count. A non-zero exit
    /// from this verb is a signal other things act on — <c>install-darling.ps1</c> prints a re-run banner on
    /// it — and turning "the probe did not answer" into that signal would report drift the run never saw.
    /// Under-reporting there is the safe direction: the running service probes the same rule on every start
    /// and WARNs a stale one by name (<see cref="DarlingFirewallCheck"/>), so an unmeasured rule is found
    /// within one service start, whereas a bogus failure teaches operators to ignore the banner.</para>
    /// </summary>
    public static bool NotElevatedRunHasWork(IEnumerable<FirewallHandoff> handoffs) =>
        handoffs.Any(h => h is FirewallHandoff.OpenCommand or FirewallHandoff.SweepCommand);

    /// <summary>
    /// Read-only "does ANY rule for this surface exist" for the not-elevated branch (#2445). True/false when the
    /// probe answered, null when it could not. Never writes anything and never throws except on cancellation.
    ///
    /// <para>It runs <see cref="DarlingFirewallCheck.BuildProbeCommand"/> — the probe the RUNNING service
    /// already uses, shaped to exit 0 whether or not a rule matches — against
    /// <see cref="DarlingFirewallCheck.SurfaceRuleWildcard"/>, which is the same wildcard the elevated sweep
    /// deletes by. That pairing is the point rather than convenience: a narrower question would report "nothing
    /// to do" about rules the sweep would still collect, and a wider one would offer a command covering rules
    /// this product does not own. Probe and remedy share the builders, so they cannot come to disagree.</para>
    ///
    /// <para>Affordable precisely HERE, in the one branch that by definition holds no privilege:
    /// <c>Get-NetFirewallRule</c> needs no elevation — reads succeed under a restricted token where writes
    /// return PermissionDenied — and this costs at most one bounded PowerShell round trip per surface, only on
    /// a path <c>install-darling.ps1</c> cannot take, since it refuses to run unelevated at all.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static async Task<bool?> ProbeSurfaceRulesAsync(string wildcard, CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, psOutput) = await DarlingManagedPostgres.RunPowerShellAsync(
                DarlingFirewallCheck.BuildProbeCommand(wildcard), cancellationToken);

            /* The probe exits 0 for BOTH answers, so a non-zero exit means the probe itself broke; do not read
               a count out of a run that failed. Same reasoning as DarlingFirewallCheck.CheckAsync. */
            return exitCode == 0 ? DarlingFirewallCheck.TryParseProbeOutput(psOutput) : null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            /* A missing or broken powershell.exe is "could not tell", never "no rule" — guessing absence would
               manufacture a clean bill of health for a firewall this run never read. */
            return null;
        }
    }

    /// <summary>
    /// Creates or removes every scoped Darling firewall rule so the live firewall matches darling.json.
    /// Requires elevation (that is the entire point of the verb) and is idempotent — safe to re-run on every
    /// upgrade, which is exactly how install-darling.ps1 uses it. Returns 0 when the firewall ends up matching
    /// the config, 1 when it could not be made to.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> ConfigureFirewallAsync(
        string? configPath, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        /* #2414: ask the control plane which ports the MCP/web endpoints are actually bound to, because that —
           not darling.json's seed — is what the rule has to be named for. BEST EFFORT on purpose. This verb is
           called by install-darling.ps1 before the service has ever run, when there is no store to ask and
           darling.json's port is the only truth there is (the store row is SEEDED from it), so requiring a
           reachable store would fail every fresh install. What it must not do is fall back QUIETLY: the plan
           carries a PortNote that names the port it used, why it could not do better, and what makes that port
           wrong, printed below on the same footing as the rules themselves. */
        var (mcpStore, webStore, storeUnavailable) = await TryReadEndpointTogglesAsync(config, cancellationToken);
        var plans = PlanFirewallRules(config, mcpStore, webStore, storeUnavailable);
        var toOpen = plans.Count(p => p.Action == FirewallRuleAction.Open);

        output.WriteLine("Reconciling the scoped Windows Firewall rules to match darling.json.");
        output.WriteLine(
            "These rules are managed HERE, elevated. The service account cannot create them by design, so the " +
            "running service only verifies them and reports what it finds.");
        output.WriteLine();

        /* Print the port provenance BEFORE the not-elevated branch below, so the operator who is about to paste
           these commands by hand can see which plane chose the ports baked into them. */
        var portNotes = plans.Select(p => p.PortNote).Where(n => n is not null).ToList();
        foreach (var note in portNotes)
        {
            output.WriteLine(note);
        }

        if (portNotes.Count > 0)
        {
            output.WriteLine();
        }

        if (!IsElevated())
        {
            /* Not elevated. When nothing is exposed there is genuinely nothing to do and a hard failure would
               be a lie (and would fail an otherwise fine loopback install); when something IS exposed this is a
               real, actionable failure, so exit non-zero AND print every command to run by hand. */
            /* #2436: the per-surface notes, for EVERY plan and before either branch below. The elevated path
               prints them as it works; this path used to print none at all, so the operator who most needs
               them — the one who cannot act on them yet — was the one who never saw them. They are also the
               only place a surface says it is fully LAN-configured and switched OFF, which is a state that did
               not exist here before this change: "no rule is needed" would otherwise read as "your exposure
               config did not take", and in the mixed case a disabled surface's stale rule would be dropped
               silently while its exposed sibling got a command. */
            foreach (var plan in plans.Where(p => p.Note is not null))
            {
                output.WriteLine($"{plan.Surface}: {plan.Note}.");
            }

            /* #2445: MEASURE the Remove half instead of guessing at it. This branch used to print a command
               only for Open plans, so a run whose whole work was a removal said "nothing needs an open port",
               exited 0, and left a stale inbound allow rule sitting there with nothing offered to close it.
               #2436 made that shape ordinary rather than exotic — a surface is now a Remove when the control
               plane has it switched OFF, so it covers an admin who just ran --disable-mcp and re-ran this verb
               from a normal prompt.

               The read-only probe is what makes all three behaviours honest at once, and it is affordable here
               for a reason worth stating: Get-NetFirewallRule needs no elevation, so this branch could always
               have looked and simply never did. Without it the only gate available is a proxy for "a rule might
               exist", which would print sweep commands on every default loopback install and STILL miss a stale
               rule on a surface that has no note. With it, a clean box prints nothing extra and exits 0 exactly
               as before, and a box with a stale rule gets that rule's own sweep command and a non-zero exit.
               Only the Remove plans that are permitted to sweep are probed, so the cost is at most one bounded
               PowerShell round trip per such surface, on a path install-darling.ps1 cannot reach — it fails
               outright when not elevated, so the re-run banner this could have fired is not on that path. */
            var handoffs = new List<(FirewallRulePlan Plan, FirewallHandoff Handoff)>();
            foreach (var plan in plans)
            {
                bool? rulesFound = null;
                if (plan.Action == FirewallRuleAction.Remove && plan.SweepOtherPorts)
                {
                    rulesFound = await ProbeSurfaceRulesAsync(
                        DarlingFirewallCheck.SurfaceRuleWildcard(plan.RuleName), cancellationToken);
                }

                handoffs.Add((plan, ClassifyNotElevatedHandoff(plan.Action, plan.SweepOtherPorts, rulesFound)));
            }

            if (handoffs.All(h => h.Handoff == FirewallHandoff.Nothing))
            {
                output.WriteLine(
                    "No endpoint wants an open port, and no scoped Darling rule is open that should not be, so " +
                    "there is nothing for an elevated shell to do. (This shell is not elevated, but reading the " +
                    "firewall does not need elevation — so that is measured, not assumed.)");
                return 0;
            }

            var hasWork = NotElevatedRunHasWork(handoffs.Select(h => h.Handoff));

            error.WriteLine("This shell is not elevated, so NO firewall rule was changed. Run these in an ELEVATED PowerShell:");
            foreach (var (plan, handoff) in handoffs)
            {
                switch (handoff)
                {
                    case FirewallHandoff.OpenCommand:
                        error.WriteLine("  " + DarlingManagedPostgres.BuildFirewallEnableCommand(plan.RuleName, plan.Port, plan.Cidr!));
                        break;

                    /* The same builder and the same wildcard the elevated path would have used, so the command
                       an operator pastes and the command the verb runs cannot drift apart. */
                    case FirewallHandoff.SweepCommand:
                        error.WriteLine(
                            $"  # {plan.Surface}: a scoped rule is open on this surface and none belongs on any port.");
                        error.WriteLine("  " + DarlingManagedPostgres.BuildFirewallSweepCommand(
                            DarlingFirewallCheck.SurfaceRuleWildcard(plan.RuleName)));
                        break;

                    case FirewallHandoff.SweepCommandUnverified:
                        error.WriteLine(
                            $"  # {plan.Surface}: the read-only rule probe gave no usable answer, so this may be a no-op.");
                        error.WriteLine("  " + DarlingManagedPostgres.BuildFirewallSweepCommand(
                            DarlingFirewallCheck.SurfaceRuleWildcard(plan.RuleName)));
                        break;

                    case FirewallHandoff.Nothing:
                    default:
                        break;
                }
            }

            if (!hasWork)
            {
                error.WriteLine(
                    "Returning 0: nothing above is CONFIRMED work. The read-only probe could not answer for at " +
                    "least one surface, so those commands are offered as a precaution — and a non-zero exit is a " +
                    "claim about the firewall, which this run has no measurement to support.");
            }

            return hasWork ? 1 : 0;
        }

        var failures = 0;
        foreach (var plan in plans)
        {
            if (plan.Note is not null)
            {
                output.WriteLine($"{plan.Surface}: {plan.Note}.");
            }

            /* Sweep this surface's rules for EVERY port FIRST, as its own step. The port is part of the rule
               name, so changing a port does not update a rule — it makes a different one and strands the old as
               an inbound allow rule on a port nothing serves. Reconciling by exact name could never reach that.
               Its own step, not concatenated ahead of the open below, because the sweep command ends in exit 0
               and would otherwise terminate the shell before the rule was created.

               #2436: and the sweep's authority to remove OTHER ports comes from knowing which port is live. On
               an Open plan whose port fell back to darling.json's seed because the store could not answer, that
               is precisely the knowledge missing — a rule on another port is as likely to be the one the
               endpoint is being served on as it is to be stale, and removing it turns the elevated verb an
               operator was told to run into an outage of the surface it was meant to repair. That is not
               hypothetical: install-darling.ps1 runs this verb with the service STOPPED, so on an upgrade of a
               box whose port was moved in the Viewer it is the working rule that matched the wildcard. The
               enable command below removes its own exact DisplayName first, so declining the sweep costs
               nothing in idempotence — it defers the cleanup to a run that can see the control plane, which is
               what the installer's post-start reconcile is for. */
            var wildcard = DarlingFirewallCheck.SurfaceRuleWildcard(plan.RuleName);
            if (plan.SweepOtherPorts)
            {
                if (!await TryRunFirewallStepAsync(
                    DarlingManagedPostgres.BuildFirewallSweepCommand(wildcard),
                    plan.Action == FirewallRuleAction.Remove
                        ? $"{plan.Surface}: no rule belongs on any port (removed any '{wildcard}')."
                        : $"{plan.Surface}: cleared any previous rule matching '{wildcard}'.",
                    $"{plan.Surface}: could not remove the rule(s) matching '{wildcard}'",
                    output, error, cancellationToken))
                {
                    failures++;
                }
            }
            else
            {
                output.WriteLine(plan.Action == FirewallRuleAction.Open
                    ? $"{plan.Surface}: leaving any rule on ANOTHER port alone. This run could not read the control " +
                      "plane, so it cannot tell a rule stranded by a port change from the rule the endpoint is " +
                      "actually being served on — re-run --configure-firewall with the service up to collect it."
                    : $"{plan.Surface}: leaving this surface's existing rules alone rather than removing them. " +
                      "darling.json exposes this endpoint and says it is switched off, but the --enable-* verbs and " +
                      "the Viewer write only the control plane, which this run could not read — so that off may be " +
                      "stale and the rule may be the one the endpoint is being served on. Nothing is listening while " +
                      "the service is stopped; re-run --configure-firewall with it up to reconcile.");
            }

            if (plan.Action == FirewallRuleAction.Remove)
            {
                /* Loopback-only: the desired state is no rule at all, on any port — the sweep WAS the work. */
                continue;
            }

            if (!await TryRunFirewallStepAsync(
                DarlingManagedPostgres.BuildFirewallEnableCommand(plan.RuleName, plan.Port, plan.Cidr!),
                $"{plan.Surface}: opened '{plan.RuleName}' (TCP {plan.Port}, inbound, from {plan.Cidr}).",
                $"{plan.Surface}: could not open the rule '{plan.RuleName}'",
                output, error, cancellationToken))
            {
                failures++;
            }
        }

        output.WriteLine();
        if (failures > 0)
        {
            error.WriteLine($"{failures} firewall rule(s) could not be reconciled — see the commands above.");
            return 1;
        }

        /* Not "every endpoint is loopback-only" any more: since #2436 a surface is also a Remove when it is
           fully LAN-configured and the control plane has it switched OFF. That is the same misleading summary
           #2442 fixed one branch up, and the per-surface lines above already carry the real reason. */
        output.WriteLine(toOpen == 0
            ? "Done. No endpoint wants an open port, so no port was opened."
            : $"Done. {toOpen} endpoint(s) exposed on the LAN; their scoped rules are in place.");
        return 0;
    }

    /// <summary>
    /// BEST-EFFORT read of the control plane's effective endpoint toggles for <c>--configure-firewall</c> (#2414).
    /// Returns nulls plus a human reason on every failure and NEVER throws except on caller cancellation.
    ///
    /// <para><b>Why best-effort and not required.</b> Every other consumer of these values runs inside the
    /// service, where the store is a precondition. This verb is the opposite: install-darling.ps1 calls it while
    /// elevated, before the service has ever started, on a box where <c>config.config_service</c> does not exist
    /// yet — and it is correct there, because the store row is SEEDED from darling.json's ports, so at that moment
    /// the file IS the control plane's future answer. Refusing would fail every fresh networked install to close a
    /// window in which the file cannot be wrong.</para>
    ///
    /// <para><b>Why it must still say so.</b> The same fallback on a box whose port HAS been moved in the Viewer is
    /// the whole of #2414: a rule for a port nothing serves, recreated on every run of the verb that is supposed to
    /// fix it. So the reason travels back with the nulls and is printed, rather than being swallowed into a silent
    /// default. Bounded to <see cref="ServiceCommandDeadlines.CliStoreReadSeconds"/> because "the store is down"
    /// and "the store is slow" must not differ in how long an installer hangs — the same bound the command itself
    /// now carries, so the budget and the deadline cannot disagree about one wait.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    private static async Task<((bool Enabled, int Port)? Mcp, (bool Enabled, int Port)? Web, string? Unavailable)>
        TryReadEndpointTogglesAsync(DarlingConfig config, CancellationToken cancellationToken)
    {
        var postgres = config.Postgres;
        if (postgres is null || !postgres.Managed)
        {
            /* BYO: config_service lives in the operator's own PostgreSQL and this verb holds no credential for it.
               Harmless in practice — LAN exposure for MCP/web is managed-mode only, so both surfaces plan Remove,
               which sweeps every port and needs no port at all. */
            return (null, null,
                "postgres.managed is false, so config.config_service lives in YOUR PostgreSQL and this verb has no credential for it");
        }

        string? connectionString;
        try
        {
            connectionString = DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres);
        }
        catch (Exception ex)
        {
            return (null, null, $"the stored store credential could not be read ({ex.Message})");
        }

        if (connectionString is null)
        {
            return (null, null,
                "the service has not initialized the managed store yet, so there is no owner credential to read it with");
        }

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.CliStoreReadSeconds));

        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(budget.Token);
            await using var command = new NpgsqlCommand(ReadEndpointTogglesSql, connection) { CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds };
            await using var reader = await command.ExecuteReaderAsync(budget.Token);
            if (!await reader.ReadAsync(budget.Token))
            {
                return (null, null, "config.config_service has no id = 1 row yet — the service has never seeded the store");
            }

            return ((reader.GetBoolean(0), reader.GetInt32(1)), (reader.GetBoolean(2), reader.GetInt32(3)), null);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (null, null,
                $"the store did not answer within {ServiceCommandDeadlines.CliStoreReadSeconds} seconds");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (null, null, $"the store did not answer ({ex.Message})");
        }
    }

    /// <summary>Runs one reconcile step and reports it. Returns false on failure, after printing the exact
    /// command so an operator can finish by hand. Never throws except on cancellation.</summary>
    [SupportedOSPlatform("windows")]
    private static async Task<bool> TryRunFirewallStepAsync(
        string command, string successLine, string failurePrefix, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        try
        {
            var (exitCode, psOutput) = await DarlingManagedPostgres.RunPowerShellAsync(command, cancellationToken);
            if (exitCode == 0)
            {
                output.WriteLine(successLine);
                return true;
            }

            error.WriteLine($"{failurePrefix} (exit {exitCode}: {psOutput}). Run this by hand:");
            error.WriteLine("  " + command);
            return false;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            error.WriteLine($"{failurePrefix} ({ex.Message}). Run this by hand:");
            error.WriteLine("  " + command);
            return false;
        }
    }

    /* ================================================================================================
       --backfill-rollups: the staged, disk-preflighted rollup backfill (#1759 Phase 2).

       Why an operator verb and not a startup step: the #1680 arming gate is ALL-OR-NOTHING, so a store
       with a year of raw has to materialize the WHOLE history before the first purge arms and reclaims
       anything. Peak disk comes BEFORE any relief. Running that automatically at service start, on the
       exact stores worst affected (one already down to ~150 GB free), is a plausible disk-exhaustion
       event -- so it is explicit, preflighted, and refuses with numbers rather than filling the volume.

       This verb's job ENDS AT COVERAGE. It never arms a retention policy: the arming gate already
       self-heals, checking coverage on every service start and releasing the held purges by itself once
       a rollup genuinely covers raw. Arming here would duplicate that decision in a second place, and
       the whole reason nothing has been lost on these stores is that exactly one thing decides it.
       ================================================================================================ */

    /// <summary>
    /// <c>--collapse-legacy-slices</c> (#1912): repair the Query Store rows collected before #1907, then
    /// re-materialize the rollups they fed.
    ///
    /// <para>Before #1907 the collector stored Query Store's FLUSHED and still-IN-MEMORY slice of one interval
    /// as two rows. They are ADDITIVE, so the read-side dedup — which keeps one row per key — reports a
    /// fraction of the interval's work, and the rollups materialized that fraction. #1907 made the choice
    /// deterministic; this makes it correct, for the rows it can still reach.</para>
    ///
    /// <para><b>Bounded on purpose, and the bound is raw retention.</b> Only rows still in the raw tier can be
    /// collapsed, so on a Darling store the repair reaches the last few days — which at upgrade time is the
    /// operator's most-looked-at recent history, and is why running it PROMPTLY after upgrading is what makes
    /// it worth anything. Everything older keeps its understated counts permanently: the daily tiers are kept
    /// indefinitely and cannot be rebuilt from raw that no longer exists. That residue is disclosed in the
    /// release notes rather than papered over, the same shape #1849 used for the inflated-rollup boundary.</para>
    ///
    /// <para><b>The refresh is CLAMPED to the rows actually collapsed, and that is a safety property rather
    /// than an optimization.</b> A refresh whose range lies entirely within DROPPED raw chunks DESTROYS the
    /// materialization there — with force and without, measured on PG 18.4 + TimescaleDB 2.28.1 and pinned by
    /// <c>QueryStoreSliceRepairLiveTests</c>. Aiming at a nominal "pre-fix period" instead of at the collapsed
    /// rows' own span would therefore blank the retained hourly tier and the indefinitely-kept daily below raw's
    /// floor, which is precisely the history #1759/#1793 forbid destroying. Collapsed rows are inside raw's
    /// extent by definition, so deriving the window from them cannot reach under it.</para>
    ///
    /// <para><b>Safe to re-run.</b> The pre-fix signature — two or more rows sharing the whole dedup key AND
    /// <c>collection_time</c> — cannot match a row collected since #1907, which emits at most one row per
    /// interval per cycle. A second run finds nothing and says so.</para>
    ///
    /// <para>Returns 0 when the repair completed or had nothing to do, 1 on a load/mode/credential error.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> CollapseLegacySlicesAsync(
        string? configPath, bool dryRun, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        var connectionString = postgres.Managed
            ? DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres)
            : postgres.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            error.WriteLine(postgres.Managed
                ? DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres)
                : "postgres.connectionString is empty, so there is no store to repair.");
            return 1;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not connect to the store: {ex.Message}");
            return 1;
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling — Query Store slice repair (--collapse-legacy-slices)");
        output.WriteLine();

        QueryStoreSliceRepair.Survey survey;
        try
        {
            survey = await QueryStoreSliceRepair.SurveyAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not survey query_store_stats: {ex.Message}");
            return 1;
        }

        if (!survey.HasWork)
        {
            output.WriteLine("  Nothing to repair — no Query Store rows carry the pre-fix split-slice signature.");
            output.WriteLine();
            output.WriteLine("  That is the expected result on a store that has only ever run builds carrying the");
            output.WriteLine("  collection-side fix, and on any store where a previous run already repaired them.");
            return 0;
        }

        output.WriteLine($"  Split intervals found : {survey.SplitGroups:N0}");
        output.WriteLine($"  Rows involved         : {survey.SplitRows:N0}  (collapsing to {survey.SplitGroups:N0}, removing {survey.RowsRemoved:N0})");
        output.WriteLine($"  Collection-time span  : {survey.OldestUtc:yyyy-MM-dd HH:mm} .. {survey.NewestUtc:yyyy-MM-dd HH:mm} UTC");
        output.WriteLine();
        output.WriteLine("  Only rows still held in the raw tier can be repaired. Query Store history older than");
        output.WriteLine("  raw retention keeps its understated counts permanently — see the release notes.");
        output.WriteLine();

        if (dryRun)
        {
            output.WriteLine("  DRY RUN — nothing was changed.");
            return 0;
        }

        /* SLICED, not one call over the whole span. CollapseSliceAsync runs each slice in ONE
           transaction, and that transaction takes locks on the raw chunks it touches — which the compression
           policy also wants. Handing it the entire survey span would make one long transaction sitting across
           however much history the store keeps, which is exactly the lock-duration family that has bitten this
           repo before (#1564/#1567).

           Slice width is ADAPTIVE (#2105 round three): a day is the fast default, but on the field store
           that motivated this the FIRST day-wide stage aggregation blew through the 15-minute statement
           timeout — the operator watched it die at minute ~15 with the bare stream exception, three walls
           deep. A failed slice now halves the window and retries the SAME start (the shared
           QueryStoreBackfillState.AdaptiveSpan schedule the backfill worker uses, 24h base → 22.5m floor),
           a completed slice resets to full width, and only a slice that fails AT the floor gives up to the
           existing re-run message. Narrowing is announced so the operator sees progress, not a hang.

           The half-open upper bound includes the newest collapsed row — the survey reports that instant
           itself, not a bound past it — hence the final slice's one-second nudge. */
        long removed = 0;
        var sliceStart = survey.OldestUtc!.Value.Date;
        var spanStart = sliceStart;
        var collapseEnd = survey.NewestUtc!.Value.AddSeconds(1);
        var fullWidth = TimeSpan.FromDays(1);
        var consecutiveFailures = 0;
        var retriedAtWidth = false;

        while (sliceStart < collapseEnd)
        {
            var span = QueryStoreBackfillState.AdaptiveSpan(fullWidth, consecutiveFailures);
            var sliceEnd = sliceStart + span;
            if (sliceEnd > collapseEnd)
            {
                sliceEnd = collapseEnd;
            }

            /* The width the slice ACTUALLY covers — the final slice clamps to the range end, so the
               nominal AdaptiveSpan width can overstate it, and both the retry decision and the operator
               messages must speak in real terms (review catch). */
            var actualWidth = sliceEnd - sliceStart;

            try
            {
                var sliceRemoved = await QueryStoreSliceRepair.CollapseSliceAsync(
                    connection, sliceStart, sliceEnd, cancellationToken);
                removed += sliceRemoved;

                /* Per-slice progress (#2105 operator feedback): the run used to be SILENT between the
                   survey banner and DONE — on a big backlog that is an hour-plus of blank console that
                   reads as a hang, on the exact stores where trust in this verb is already bruised.
                   Percent is of the survey's own span, so it always ends at 100. */
                var pctDone = 100.0 * (sliceEnd - spanStart).Ticks / (collapseEnd - spanStart).Ticks;
                output.WriteLine($"  [OK] {sliceStart:yyyy-MM-dd HH:mm} +{actualWidth.TotalMinutes:F0}m — {sliceRemoved:N0} removed ({pctDone:F0}% of span, {removed:N0} total)");

                consecutiveFailures = 0;
                retriedAtWidth = false;
                sliceStart = sliceEnd;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var next = NextNarrowingFailureCount(fullWidth, consecutiveFailures, actualWidth);

                /* A slice already at/below the adaptive floor (usually the clamped final tail — nothing
                   says the leftover is ≥ the floor) can't be narrowed, but its likeliest failure is the
                   transient/connection kind the fresh-connection retry exists for — so it earns ONE
                   same-width retry before the give-up (review catch: giving up on the tail's first
                   failure silently exempted the run's usual last slice from the retry mechanism). */
                var sameWidthRetry = next is null && !retriedAtWidth;

                if (next is int || sameWidthRetry)
                {
                    /* The statement-timeout failure this loop exists to survive surfaces as a broken
                       STREAM, not a clean server-side cancel — the connection underneath is very likely
                       dead, and retrying on it would fail instantly through every halving step (review
                       catch). Cycle it: close is safe on a broken connection, and reopen draws a fresh
                       physical connection. Session state doesn't matter — the slice's SET LOCAL and
                       per-command timeouts are transaction/command scoped. A failed REOPEN degrades to
                       the same clean idempotent-rerun message as every other failure here, never an
                       unhandled crash (review catch — this verb has no caller safety net). */
                    try
                    {
                        await connection.CloseAsync();
                        await connection.OpenAsync(cancellationToken);
                    }
                    catch (Exception reopenEx) when (reopenEx is not OperationCanceledException)
                    {
                        error.WriteLine($"  The collapse failed after {removed:N0} row(s); the slice at {sliceStart:yyyy-MM-dd HH:mm} failed ({FirstLineOf(ex.Message)}) and the store connection could not be reopened: {FirstLineOf(reopenEx.Message)}");
                        error.WriteLine("  Slices already committed are safe. Re-run to continue — the repair is idempotent.");
                        return 1;
                    }

                    if (next is int narrowerFailures)
                    {
                        consecutiveFailures = narrowerFailures;
                        retriedAtWidth = false;
                        var narrower = QueryStoreBackfillState.AdaptiveSpan(fullWidth, narrowerFailures);
                        output.WriteLine($"  [RETRY] slice {sliceStart:yyyy-MM-dd HH:mm} +{actualWidth.TotalMinutes:F0}m failed ({FirstLineOf(ex.Message)}); narrowing to {narrower.TotalMinutes:F0}m and retrying.");
                    }
                    else
                    {
                        retriedAtWidth = true;
                        output.WriteLine($"  [RETRY] slice {sliceStart:yyyy-MM-dd HH:mm} +{actualWidth.TotalMinutes:F0}m failed ({FirstLineOf(ex.Message)}); already at the narrowest width — retrying once on a fresh connection.");
                    }

                    continue;
                }

                /* Narrowing exhausted AND the same-width retry spent — this range cannot be repaired
                   unattended. Each slice is its own transaction, so earlier slices are already committed
                   and are not lost — and the collapse is idempotent, so re-running picks up where this
                   stopped. */
                error.WriteLine($"  The collapse failed after {removed:N0} row(s); the failing {actualWidth.TotalMinutes:F0}m slice at {sliceStart:yyyy-MM-dd HH:mm} was rolled back: {ex.Message}");
                error.WriteLine("  Slices already committed are safe. Re-run to continue — the repair is idempotent.");
                return 1;
            }
        }

        output.WriteLine($"  Collapsed. Rows removed: {removed:N0}");
        output.WriteLine();

        /* Re-materialize every rollup fed by query_store_stats, in the dependency order RollupBackfill.Targets
           already carries — a rollup refreshed before its source materializes nothing, reports success, and
           consumes the invalidations that covered the range. The window is the collapsed rows' own span,
           widened to whole buckets so a partially-covered bucket is recomputed rather than left half-old. */
        var affected = RollupBackfill.Targets
            .Where(t => string.Equals(t.RawTable, QueryStoreSliceRepair.Table, StringComparison.Ordinal))
            .ToList();

        /* ONE disclosure for the whole run, so a pre-2.21 store's missing `options` parameter is reported once
           rather than per rollup — and reported at all, which is the point of it being required (#1797). */
        var disclosure = new RefreshDisclosure(message => output.WriteLine($"  [NOTE] {message}"));

        var refreshed = 0;
        foreach (var target in affected)
        {
            var from = Floor(survey.OldestUtc!.Value, target.BucketWidth);
            var to = Floor(survey.NewestUtc!.Value, target.BucketWidth) + target.BucketWidth;

            try
            {
                await RollupBackfill.RepairAsync(connection, target.View, from, to, disclosure, cancellationToken);
                refreshed++;
                output.WriteLine($"  [OK]   {target.View}: re-materialized {from:yyyy-MM-dd HH:mm} .. {to:yyyy-MM-dd HH:mm} UTC");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error.WriteLine($"  [FAIL] {target.View}: {ex.Message}");
            }
        }

        output.WriteLine();
        output.WriteLine(refreshed == affected.Count
            ? $"  DONE — {removed:N0} row(s) collapsed, {refreshed} rollup(s) re-materialized."
            : $"  PARTIAL — {removed:N0} row(s) collapsed, {refreshed} of {affected.Count} rollup(s) re-materialized. The collapse itself is committed; re-run to retry the rollups.");

        return 0;
    }

    /// <summary>Floors an instant to its bucket, so a refresh window covers whole buckets on both edges.</summary>
    private static DateTime Floor(DateTime value, TimeSpan bucket)
        => bucket <= TimeSpan.Zero ? value : new DateTime(value.Ticks - (value.Ticks % bucket.Ticks), value.Kind);

    /// <summary>
    /// The collapse loop's narrowing decision, pure so it pins without a live timeout: the smallest
    /// failure count whose <see cref="QueryStoreBackfillState.AdaptiveSpan"/> width actually narrows a
    /// slice that COVERED <paramref name="actualWidth"/> (a clamped final slice can be narrower than
    /// several nominal halving steps, and re-running an identical window just re-hits the same wall), or
    /// null when no step can — the slice already sits at/below the adaptive floor, where the caller's
    /// one same-width fresh-connection retry is the only move left.
    /// </summary>
    internal static int? NextNarrowingFailureCount(TimeSpan fullWidth, int consecutiveFailures, TimeSpan actualWidth)
    {
        var next = consecutiveFailures + 1;
        var narrower = QueryStoreBackfillState.AdaptiveSpan(fullWidth, next);
        while (narrower >= actualWidth)
        {
            var evenNarrower = QueryStoreBackfillState.AdaptiveSpan(fullWidth, next + 1);
            if (evenNarrower >= narrower)
            {
                return null;
            }

            next++;
            narrower = evenNarrower;
        }

        return next;
    }

    /// <summary>An exception message's first line, CR-trimmed — one-line operator output must stay one line.</summary>
    private static string FirstLineOf(string message)
        => message.Split('\n')[0].TrimEnd('\r');

    /// <summary>How <c>--recompress-plan-dim</c> handles the closing VACUUM FULL (#2076).</summary>
    public enum RecompressVacuumMode
    {
        /// <summary>Compact after a CONVERGED, zero-failure conversion — the default, because the moment of
        /// convergence is the moment of maximum bloat, and shipping the saving only as internal free space
        /// leaves operators wondering why df never moved.</summary>
        Auto,

        /// <summary>Compact even when there is nothing left to convert (<c>--vacuum-full</c>) — for a store
        /// converted by an earlier run/build whose file still sits at its high-water mark.</summary>
        Force,

        /// <summary>Never compact (<c>--no-vacuum-full</c>) — for operators who want the exclusive-lock
        /// window scheduled separately. The conversion's saving stays internal until they take it.</summary>
        Skip,
    }

    /// <summary>
    /// Parses the verb's trailing arguments. PURE so the flag grammar pins: <c>--dry-run</c>,
    /// <c>--vacuum-full</c>, <c>--no-vacuum-full</c> in any order, at most one bare argument (the config
    /// path). Unknown flags are NOT config paths — refusing beats silently treating a typo as a file.
    /// </summary>
    public static (string? ConfigPath, bool DryRun, RecompressVacuumMode Mode, string? Error) ParseRecompressArgs(
        ReadOnlySpan<string> rest)
    {
        string? configPath = null;
        var dryRun = false;
        var mode = RecompressVacuumMode.Auto;

        foreach (var arg in rest)
        {
            if (string.Equals(arg, "--dry-run", StringComparison.OrdinalIgnoreCase))
            {
                dryRun = true;
            }
            else if (string.Equals(arg, "--vacuum-full", StringComparison.OrdinalIgnoreCase))
            {
                mode = RecompressVacuumMode.Force;
            }
            else if (string.Equals(arg, "--no-vacuum-full", StringComparison.OrdinalIgnoreCase))
            {
                mode = RecompressVacuumMode.Skip;
            }
            else if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                return (null, false, RecompressVacuumMode.Auto, $"unknown option '{arg}' for --recompress-plan-dim");
            }
            else
            {
                configPath = arg;
            }
        }

        return (configPath, dryRun, mode, null);
    }

    /// <summary>
    /// #2171: whether the store's plan_xml_compression setting is 'none' — the mode where the live
    /// writer stores plans as plain text and recompression would fight it forever. Reads defensively:
    /// the column arrives at V62, and the verb must keep working against the older stores it exists
    /// to convert, so a missing column (42703) is "no mode to conflict with". Public-for-tests via
    /// the live suite; the verb is its only production caller.
    /// </summary>
    internal static async Task<bool> StoreIsSetToPlainTextPlansAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var codec = new NpgsqlCommand(
                "SELECT plan_xml_compression FROM config_service WHERE id = 1", connection)
            {
                CommandTimeout = ServiceCommandDeadlines.CliStoreReadSeconds,
            };

            return await codec.ExecuteScalarAsync(cancellationToken) is string mode
                && string.Equals(mode.Trim(), "none", StringComparison.OrdinalIgnoreCase);
        }
        catch (PostgresException ex) when (ex.SqlState == "42703")
        {
            /* Pre-V62 store: no column, no mode to conflict with — proceed. */
            return false;
        }
    }

    /// <summary>
    /// <c>--recompress-plan-dim</c> (#2076): convert the plan dimension's pre-V54 text rows to the gzip form
    /// V54's write path produces (#2069), in bounded batches, while the service keeps running.
    ///
    /// <para><b>Why a verb.</b> V54 left old rows to convert by GC attrition, but the dimension GC retires a
    /// row only when its digest stops being re-seen — a STABLE plan's text row never ages out, so the tail
    /// never converts on its own. Rewriting the store's largest table runs when a person decides it should
    /// (the <c>--collapse-legacy-slices</c> rationale), with <c>--dry-run</c> measuring the real ratio on
    /// this store's own content first.</para>
    ///
    /// <para><b>No outage.</b> Each 1,000-row batch is one transaction against rows the collectors only ever
    /// touch via <c>ON CONFLICT ... SET last_seen</c>; the service stays up, collection keeps running, and an
    /// interrupted run resumes from wherever it stopped (the fetch predicate IS the resume point). Every
    /// row's gzip bytes are round-trip verified before its text is nulled — a row that fails keeps its text
    /// and is counted, never converted blind.</para>
    ///
    /// <para><b>Disclosed limit.</b> Converts live content; does not shrink the file — PostgreSQL returns old
    /// row versions as reusable space INSIDE the relation, so the observable outcome is the dimension's
    /// growth flatlining. Handing space back to the volume is a separate one-time VACUUM FULL/repack.</para>
    ///
    /// <para>Returns 0 when the conversion completed (or had nothing to do), 1 on a load/mode/credential
    /// error or when any row failed round-trip verification (those rows keep their text; re-run after
    /// investigating).</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> RecompressPlanDimAsync(
        string? configPath, bool dryRun, RecompressVacuumMode vacuumMode, TextWriter output, TextWriter error,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        var connectionString = postgres.Managed
            ? DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres)
            : postgres.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            error.WriteLine(postgres.Managed
                ? DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres)
                : "postgres.connectionString is empty, so there is no store to convert.");
            return 1;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not connect to the store: {ex.Message}");
            return 1;
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling — plan-dimension recompression (--recompress-plan-dim)");
        output.WriteLine();

        /* #2171: a store configured plan_xml_compression = 'none' WANTS text rows — the operator chose
           direct-SQL readability, and this verb would convert exactly the rows the live writer keeps
           producing, the two fighting forever. Refuse with the way out rather than silently churning. */
        bool plainTextPlans;
        try
        {
            plainTextPlans = await StoreIsSetToPlainTextPlansAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2874: the only store call in this verb with no handler, and the one whose failure was
               therefore an unhandled CONSOLE STACK TRACE rather than a message — Program.cs's verb
               dispatch has no try/catch of its own, so anything escaping here escapes the process.
               StoreIsSetToPlainTextPlansAsync catches exactly one thing, SQLSTATE 42703 for the pre-V62
               store it exists to convert; a command deadline is an NpgsqlException wrapping a
               TimeoutException, which is neither that nor any PostgresException, so it walked straight
               out. A deadline on its own would have changed only how long the trace took to appear. */
            error.WriteLine($"Could not read the store's plan_xml_compression setting: {ex.Message}");
            return 1;
        }

        if (plainTextPlans)
        {
            error.WriteLine(
                "This store is configured plan_xml_compression = 'none' (plans deliberately stored as " +
                "plain text for direct-SQL consumers, #2171). Recompressing would convert rows the live " +
                "writer keeps producing as text - the two would fight forever. If you want gzip storage " +
                "back, set plan_xml_compression = 'gzip' in the store's service settings first, then " +
                "re-run this verb.");
            return 1;
        }

        PlanDimRecompression.Survey survey;
        try
        {
            survey = await PlanDimRecompression.SurveyAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not survey query_plan_dim: {ex.Message}");
            return 1;
        }

        output.WriteLine($"  {survey.Pending:N0} text row(s) to convert; {survey.Converted:N0} already gzip; {survey.Total:N0} total.");
        output.WriteLine($"  Relation size (table + TOAST + indexes): {survey.RelationBytes / (1024.0 * 1024 * 1024):N1} GB.");
        output.WriteLine();

        if (!survey.HasWork)
        {
            output.WriteLine("  Nothing to convert — every dimension row already carries gzip content.");
            output.WriteLine("  That is the expected end state: V54 writes gzip, and this verb (or GC attrition)");
            output.WriteLine("  has already converted whatever text rows existed.");

            if (vacuumMode == RecompressVacuumMode.Force && !dryRun)
            {
                return await CompactPlanDimAsync(connection, output, error, cancellationToken);
            }

            output.WriteLine("  (Pass --vacuum-full to compact a previously-converted store whose file still");
            output.WriteLine("  sits at its pre-conversion high-water mark.)");
            return 0;
        }

        if (dryRun)
        {
            PlanDimRecompression.Result sample;
            try
            {
                sample = await PlanDimRecompression.SampleAsync(connection, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error.WriteLine($"Could not sample query_plan_dim: {ex.Message}");
                return 1;
            }

            if (sample.Rows == 0)
            {
                output.WriteLine("  DRY RUN — the sample fetch returned no rows; nothing further to report.");
                return 0;
            }

            var ratio = sample.GzipBytes == 0 ? 0 : (double)sample.TextBytes / sample.GzipBytes;
            var avgGzip = sample.GzipBytes / (double)sample.Rows;
            var projected = survey.Pending * avgGzip;
            output.WriteLine($"  DRY RUN — sampled {sample.Rows:N0} pending row(s), compressed in memory, wrote nothing:");
            output.WriteLine($"    measured ratio: {ratio:N1}x (raw text -> gzip), average {avgGzip / 1024.0:N1} KB per plan");
            output.WriteLine($"    projected gzip content for all {survey.Pending:N0} pending rows: ~{projected / (1024.0 * 1024 * 1024):N1} GB");
            output.WriteLine();
            output.WriteLine("  The real run converts in 1,000-row batches (one transaction each) while the service");
            output.WriteLine("  runs; it is safe to interrupt and re-run. When the conversion CONVERGES it ends with");
            output.WriteLine("  VACUUM FULL, which rewrites the dimension to its live content and returns the freed");
            output.WriteLine("  space to the volume — that step takes an EXCLUSIVE lock and is preflighted against");
            output.WriteLine("  free disk. Size the lock window by your dimension: measured ~46 minutes on a 174 GB");
            output.WriteLine("  dimension (7.1M plans), during which collection freshness DEGRADES and recovers within");
            output.WriteLine("  minutes after. --no-vacuum-full skips it to schedule that window separately.");
            return 0;
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var pending = survey.Pending;
        long lastNarrated = 0;
        PlanDimRecompression.Result result;
        try
        {
            result = await PlanDimRecompression.ConvertAsync(
                connection,
                running =>
                {
                    /* Narrate every ~25k rows: enough to prove liveness on a multi-hour run without
                       scrolling the console into noise. */
                    if (running.Rows - lastNarrated < 25_000)
                    {
                        return;
                    }

                    lastNarrated = running.Rows;
                    var pct = pending == 0 ? 100 : running.Rows * 100.0 / pending;
                    var rate = running.Rows / Math.Max(stopwatch.Elapsed.TotalSeconds, 1);
                    var etaSeconds = rate <= 0 ? 0 : (pending - running.Rows) / rate;
                    output.WriteLine(
                        $"  {running.Rows:N0} / {pending:N0} ({pct:N1}%) — " +
                        $"{running.TextBytes / (1024.0 * 1024 * 1024):N1} GB text -> {running.GzipBytes / (1024.0 * 1024 * 1024):N1} GB gzip — " +
                        $"~{TimeSpan.FromSeconds(etaSeconds):hh\\:mm\\:ss} remaining");
                },
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Conversion failed mid-run: {ex.Message}");
            error.WriteLine("Committed batches are converted; re-running resumes from the remainder.");
            return 1;
        }

        PlanDimRecompression.Survey after;
        try
        {
            after = await PlanDimRecompression.SurveyAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Post-run survey failed: {ex.Message}");
            after = default;
        }

        var finalRatio = result.GzipBytes == 0 ? 0 : (double)result.TextBytes / result.GzipBytes;
        output.WriteLine();
        output.WriteLine($"  DONE in {stopwatch.Elapsed:hh\\:mm\\:ss} — {result.Rows:N0} row(s) converted, " +
            $"{result.TextBytes / (1024.0 * 1024 * 1024):N1} GB text -> {result.GzipBytes / (1024.0 * 1024 * 1024):N1} GB gzip ({finalRatio:N1}x).");
        if (after.Total > 0)
        {
            output.WriteLine($"  Remaining text rows: {after.Pending:N0} (new sightings during the run convert on the next pass).");
        }

        if (result.VerifyFailures > 0)
        {
            error.WriteLine($"  [WARN] {result.VerifyFailures:N0} row(s) FAILED round-trip verification and kept their text unchanged.");
            error.WriteLine("  Investigate before re-running; those rows are readable exactly as before.");
            error.WriteLine("  VACUUM FULL was skipped — compaction can wait until the conversion is clean.");
            return 1;
        }

        if (vacuumMode == RecompressVacuumMode.Skip)
        {
            output.WriteLine("  --no-vacuum-full: the freed space stays INTERNAL to the relation (growth is flat,");
            output.WriteLine("  but the file keeps its high-water mark). Re-run with --vacuum-full to compact later.");
            return 0;
        }

        if (after.Total > 0 && after.Pending > 0)
        {
            output.WriteLine($"  {after.Pending:N0} row(s) arrived mid-run and are still text — re-run to convert them;");
            output.WriteLine("  compaction will run when the conversion converges.");
            return 0;
        }

        return await CompactPlanDimAsync(connection, output, error, cancellationToken);
    }

    /// <summary>
    /// The closing VACUUM FULL (#2076): rewrites the dimension to its live content so the conversion's
    /// saving reaches the VOLUME, not just the relation's internal free-space map. Preflighted against free
    /// disk on the store's own volume (the rewrite needs room for the compacted copy while the original
    /// still exists); an unmeasurable volume is a REFUSAL, not a shrug — "probably fits" is not a plan for
    /// a rewrite of the store's largest table. Returns 0 on success, 1 on a preflight refusal or a failed
    /// vacuum (the conversion itself is already committed either way).
    /// </summary>
    private static async Task<int> CompactPlanDimAsync(
        NpgsqlConnection connection, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        long estimated;
        try
        {
            estimated = await PlanDimRecompression.EstimateCompactedBytesAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"  Could not estimate the compacted size: {ex.Message}");
            return 1;
        }

        var (freeBytes, freeError) = await ResolveStoreFreeSpaceAsync(connection, cancellationToken);
        if (freeError is not null)
        {
            error.WriteLine($"  REFUSING to VACUUM FULL: {freeError}");
            error.WriteLine("  The conversion is committed; re-run with --vacuum-full on the store host (or after");
            error.WriteLine("  fixing the above) to compact.");
            return 1;
        }

        /* 1.2x headroom on a sampled estimate: the rewrite holds old + new copies simultaneously, and the
           new copy is the estimate — the old one is already on disk and costs nothing extra. */
        var required = (long)(estimated * 1.2);
        if (freeBytes < required)
        {
            error.WriteLine("  REFUSING to VACUUM FULL: not enough free disk for the compacted copy.");
            error.WriteLine($"    estimated compacted size : {estimated / (1024.0 * 1024 * 1024):N1} GB");
            error.WriteLine($"    required (x1.2 headroom) : {required / (1024.0 * 1024 * 1024):N1} GB");
            error.WriteLine($"    free on the store volume : {freeBytes / (1024.0 * 1024 * 1024):N1} GB");
            error.WriteLine("  The conversion is committed and its space is reusable internally; free up disk and");
            error.WriteLine("  re-run with --vacuum-full to compact.");
            return 1;
        }

        var before = (await PlanDimRecompression.SurveyAsync(connection, cancellationToken)).RelationBytes;
        output.WriteLine();
        output.WriteLine($"  Compacting (VACUUM FULL): rewriting ~{estimated / (1024.0 * 1024 * 1024):N1} GB of live content.");
        output.WriteLine("  This takes an EXCLUSIVE lock on the plan dimension — collections DEGRADE until it");
        output.WriteLine("  finishes and recover within minutes after (measured: ~46 minutes of lock on a 174 GB");
        output.WriteLine("  dimension; scale by yours). The service does not need to stop.");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await PlanDimRecompression.VacuumFullAsync(connection, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"  VACUUM FULL failed: {ex.Message}");
            error.WriteLine("  The original relation is untouched (the rewrite is transactional); the conversion");
            error.WriteLine("  is committed. Re-run with --vacuum-full to retry the compaction.");
            return 1;
        }

        var after = (await PlanDimRecompression.SurveyAsync(connection, cancellationToken)).RelationBytes;
        output.WriteLine($"  COMPACTED in {stopwatch.Elapsed:hh\\:mm\\:ss} — " +
            $"{before / (1024.0 * 1024 * 1024):N1} GB -> {after / (1024.0 * 1024 * 1024):N1} GB on disk.");
        return 0;
    }

    /// <summary>
    /// What <c>--add-server</c> prints when stdin carries nothing — to STDOUT, per the [#2097] lesson that a
    /// prompt or error on STDERR is invisible in the ISE and some integrated terminals, so a verb that writes
    /// only there reads as hung.
    /// </summary>
    public static string AddServerUsageText() =>
        "Nothing arrived on stdin, so no server was added." + Environment.NewLine +
        Environment.NewLine +
        "--add-server (or --add-servers) reads a JSON ARRAY of servers from stdin — the same shape the" + Environment.NewLine +
        "add_servers MCP tool takes." + Environment.NewLine +
        "The password is read from stdin rather than the command line on purpose: an argument is visible in the" + Environment.NewLine +
        "process list and in shell history." + Environment.NewLine +
        Environment.NewLine +
        "  PowerShell:  Get-Content servers.json | .\\PerformanceMonitor.Darling.Service.exe --add-server" + Environment.NewLine +
        "  cmd:         type servers.json | PerformanceMonitor.Darling.Service.exe --add-server" + Environment.NewLine +
        Environment.NewLine +
        "servers.json, SQL Server and PostgreSQL:" + Environment.NewLine +
        "  [" + Environment.NewLine +
        "    {\"host\":\"sql01\",\"auth\":\"integrated\"}," + Environment.NewLine +
        "    {\"host\":\"aurora.cluster-abc.us-east-1.rds.amazonaws.com\",\"engine\":\"postgres\"," + Environment.NewLine +
        "     \"auth\":\"SQL\",\"username\":\"darling_monitor\",\"password\":\"...\"}" + Environment.NewLine +
        "  ]";

    /// <summary>
    /// Renders the <c>add_servers</c> result JSON as operator lines plus an exit code. PURE, so the formatting and
    /// the exit-code policy pin without a store — the same split <see cref="FormatProbeLine"/> uses.
    ///
    /// <para>Exit 0 requires that something landed and nothing failed. A batch of pure duplicates exits 0: re-running
    /// the same file is idempotent, not an error. Nothing at all landed (an empty array, or every entry rejected)
    /// exits 1, because a verb that changed nothing must not report success to a deployment script.</para>
    /// </summary>
    internal static (IReadOnlyList<string> Lines, int ExitCode) FormatAddServerOutcome(string resultJson)
    {
        var lines = new List<string>();
        try
        {
            using var document = JsonDocument.Parse(resultJson);
            var root = document.RootElement;

            /* The whole-payload rejection shape — {status, message} with no per-server results. */
            if (!root.TryGetProperty("results", out var results) || results.ValueKind != JsonValueKind.Array)
            {
                var status = root.TryGetProperty("status", out var s) ? s.GetString() : "error";
                var message = root.TryGetProperty("message", out var m) ? m.GetString() : resultJson;
                lines.Add($"  [{status?.ToUpperInvariant()}] {message}");
                return (lines, 1);
            }

            foreach (var result in results.EnumerateArray())
            {
                var server = result.TryGetProperty("server", out var sv) ? sv.GetString() : null;
                var status = (result.TryGetProperty("status", out var st) ? st.GetString() : null) ?? string.Empty;
                var detail = result.TryGetProperty("detail", out var dt) ? dt.GetString() : null;
                var tag = status switch
                {
                    "added" => "ADDED",
                    "duplicate" => "SKIP",
                    "connection_failed" => "FAIL",
                    "invalid" => "INVALID",
                    _ => status.ToUpperInvariant(),
                };
                lines.Add(string.IsNullOrWhiteSpace(detail)
                    ? $"  [{tag}] {server ?? "(unnamed)"}"
                    : $"  [{tag}] {server ?? "(unnamed)"}: {detail}");
            }

            var added = root.TryGetProperty("added", out var a) ? a.GetInt32() : 0;
            var skipped = root.TryGetProperty("skipped", out var k) ? k.GetInt32() : 0;
            var failed = root.TryGetProperty("failed", out var f) ? f.GetInt32() : 0;

            lines.Add(string.Empty);
            lines.Add(string.Format(
                CultureInfo.InvariantCulture,
                "{0} added, {1} already registered, {2} failed.",
                added,
                skipped,
                failed));

            if (added > 0)
            {
                /* The registry write bumps config_version through trg_bump_monitored_servers, which the worker
                   polls every sweep — so say the restart is unnecessary rather than leaving them to wonder. */
                lines.Add("The running service picks these up on its next config poll; no restart is needed.");
            }

            return (lines, failed > 0 || (added == 0 && skipped == 0) ? 1 : 0);
        }
        catch (JsonException)
        {
            /* Not every failure arrives as JSON. AddServersAsync's catch-all returns McpHelpers.FormatError,
               which is PLAIN TEXT ("Error during add_servers: ..."), so a genuine store failure that happens
               AFTER the request parsed — a dropped connection mid-batch, a constraint violation — lands here.
               That text IS the message the operator needs; wrapping it in "could not parse" buries the one line
               that explains the failure, precisely when the verb is being used as a deployment gate. Only
               something that looked like JSON and was not gets the parse wrapper. */
            var text = resultJson?.Trim() ?? string.Empty;
            lines.Add(text.StartsWith('{') || text.StartsWith('[')
                ? $"  Could not parse the result: {text}"
                : $"  {text}");
            return (lines, 1);
        }
    }

    /// <summary>
    /// <c>--add-server</c> (#2256): registers monitored server(s) in the store from a JSON array on stdin, through
    /// the SAME <see cref="DarlingMcpServerAdminTools.AddServers"/> path the MCP tool uses — so validation, dedupe,
    /// the in-process connection probe, password encryption and the identity computation are shared rather than
    /// reimplemented. <c>server_id</c> in particular is a hash of the storage name, which is exactly the part an
    /// operator cannot safely produce by hand.
    ///
    /// <para>Why this exists: the store is authoritative after the first seed, so darling.json edits are ignored,
    /// and the web surface deliberately excludes the write tools. A headless host — the field report ran Windows
    /// Server 2012, which cannot run the Viewer at all — had no supported path.</para>
    /// </summary>
    public static async Task<int> AddServerAsync(
        string? configPath, TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        var json = input is null ? null : await input.ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(json))
        {
            output.WriteLine(AddServerUsageText());
            return 1;
        }

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        string? connectionString;
        if (postgres.Managed)
        {
            /* The managed store credential is DPAPI, so it can only be read on Windows. Bring-your-own needs no
               such guard, which is why this is scoped to the managed branch rather than the whole verb — a Linux
               host pointed at its own Postgres can register servers. */
            if (!OperatingSystem.IsWindows())
            {
                error.WriteLine("A managed Postgres store keeps its credential in DPAPI, so --add-server needs Windows. "
                    + "A bring-your-own store (postgres.connectionString) works on any platform.");
                return 1;
            }

            connectionString = DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres);
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                /* Emitted HERE, inside the branch the guard above proved is Windows, rather than from a shared
                   check below keyed on postgres.Managed. The sibling verbs can write it below because they carry
                   [SupportedOSPlatform("windows")] on the whole method; this one deliberately does not, and a
                   bool is not something the platform analyzer can correlate with an earlier OS guard — so the
                   call has to sit where Windows is provable rather than where it merely happens to hold. */
                error.WriteLine(DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres));
                return 1;
            }
        }
        else
        {
            connectionString = postgres.ConnectionString;
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                error.WriteLine("postgres.connectionString is empty, so there is no store to register a server in.");
                return 1;
            }
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling — register monitored server(s) (--add-server)");
        output.WriteLine();

        string resultJson;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            resultJson = await DarlingMcpServerAdminTools.AddServers(dataSource, json);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not reach the store: {ex.Message}");
            return 1;
        }

        var (lines, exitCode) = FormatAddServerOutcome(resultJson);
        foreach (var line in lines)
        {
            output.WriteLine(line);
        }

        return exitCode;
    }

    /// <summary>
    /// Materializes the query-acceleration rollups back over pre-existing history so the held raw retention
    /// policies can arm themselves (#1759 Phase 2). Runs while the service is UP.
    ///
    /// <para><b>Concurrency.</b> <c>refresh_continuous_aggregate</c> takes no lock that blocks writers on the
    /// source hypertable, so collection keeps running throughout; readers are likewise unaffected (Phase 1 has
    /// them on raw for these windows anyway, and a window whose coverage a slice has just filled starts
    /// resolving to the rollup at the next probe). What the refresh DOES contend with is the compression policy
    /// on the same chunks, which is why slices are one chunk wide — a short window cannot sit across a
    /// compression job long enough to deadlock (the #1778 watch). It cannot run inside a transaction
    /// (<c>PreventInTransactionBlock</c>), so each slice is its own statement and partial progress survives an
    /// abort.
    ///
    /// <para><b>Resumable and idempotent.</b> Every pass re-plans from the MEASURED coverage floor, so an
    /// interrupted run continues where it stopped and a completed one converges to a no-op.</para>
    ///
    /// <para>Returns 0 when every rollup reached coverage (or already had it), 1 on a load/mode/credential
    /// error, on a preflight refusal, or when any rollup finished short of raw.</para>
    /// </summary>
    [SupportedOSPlatform("windows")]
    public static async Task<int> BackfillRollupsAsync(
        string? configPath, bool dryRun, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(configPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        /* Managed reads the service's own DPAPI-protected owner credential; bring-your-own uses the operator's
           configured string. Both are supported — this is a STORE operation, not a Windows one. */
        var connectionString = postgres.Managed
            ? DarlingManagedPostgres.TryBuildConnectionStringFromStoredCredential(postgres)
            : postgres.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            error.WriteLine(postgres.Managed
                ? DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres)
                : "postgres.connectionString is empty, so there is no store to back fill.");
            return 1;
        }

        await using var connection = new NpgsqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not connect to the store: {ex.Message}");
            return 1;
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling — rollup backfill (--backfill-rollups)");
        output.WriteLine();

        /* Plan every rollup. The list is in DEPENDENCY ORDER (hourlies, then the dailies sourced from them),
           and the run loop below preserves it — a daily refreshed over a range its hourly has not materialized
           reads an empty source, materializes nothing, reports success, and CONSUMES the invalidations that
           covered the range, so a later correct-order pass no-ops over the hole. */
        var plans = new List<(RollupBackfillTarget Target, RollupBackfillPlan Plan)>();
        var refusedPlans = new List<string>();
        var rawBytesCache = new Dictionary<string, long>(StringComparer.Ordinal);
        var rawOldestCache = new Dictionary<string, DateTime?>(StringComparer.Ordinal);
        foreach (var target in RollupBackfill.Targets)
        {
            RollupBackfillProbe probe;
            try
            {
                probe = await RollupBackfill.ProbeAsync(connection, target.View, target.Source, target.SourceTimeColumn, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* A missing rollup (a partially-built store) or a store without TimescaleDB lands here. Report
                   and carry on: the rollups are independent, and an operator who cannot fix one should not be
                   blocked from backfilling the rest. */
                error.WriteLine($"  [SKIP] {target.View}: cannot be probed ({ex.Message}).");
                continue;
            }

            /* The UNCALIBRATED estimate stays bounded by RAW's size even for a hierarchical daily, whose
               convergence target is now its hourly. Raw is the larger relation, so it is the conservative
               bound, and this branch only runs when the rollup has materialized nothing to measure — where
               erring high is the whole point (#1759). Deliberately not switched to the source with the rest of
               #1798: sizing a CAGG source would need its materialization hypertable, which buys accuracy in
               precisely the case where accuracy is not what is wanted. */
            if (!rawBytesCache.TryGetValue(target.RawTable, out var rawBytes))
            {
                rawBytes = await RollupBackfill.RawBytesAsync(connection, target.RawTable, cancellationToken);
                rawBytesCache[target.RawTable] = rawBytes;
            }

            /* BOUND THE PREFLIGHT AGAINST WHERE THE SOURCE WILL END UP, not where it starts now. A daily's
               source is an hourly that this same run deepens FIRST, so estimating from the pre-backfill floor
               under-counts precisely the region the run then writes — and the printed number is the commitment
               the operator accepted. Same invariant as the rows-per-bucket under-estimate, and it bites on the
               store shape where disk is tightest. A no-op for the hourlies, whose source IS raw. */
            if (!rawOldestCache.TryGetValue(target.RawTable, out var rootRawOldest))
            {
                rootRawOldest = await RollupBackfill.OldestInstantAsync(connection, target.RawTable, "collection_time", cancellationToken);
                rawOldestCache[target.RawTable] = rootRawOldest;
            }

            plans.Add((target, RollupBackfill.Plan(
                target.View, RollupBackfill.EventualSourceFloor(probe.SourceOldestUtc, rootRawOldest), probe.CoverageOldestUtc,
                probe.MaterializedBuckets, probe.MaterializedBytes, rawBytes,
                target.BucketWidth)));
        }

        var work = plans.Where(p => !p.Plan.IsComplete).ToList();
        foreach (var (_, done) in plans.Where(p => p.Plan.IsComplete))
        {
            /* A refusal is NOT a skip. "Nothing to do" is a success an operator can scroll past; a plan the
               store's own data makes nonsense of names a row someone has to go and look at, and printing it as
               an [OK] line would bury it. */
            if (done.Refusal is string refusal)
            {
                error.WriteLine($"  [REFUSED] {done.View}: {refusal}");
                refusedPlans.Add($"{done.View}: {refusal}");
            }
            else
            {
                output.WriteLine($"  [OK]   {done.View}: nothing to do — {done.SkipReason}.");
            }
        }

        if (work.Count == 0)
        {
            output.WriteLine();
            if (refusedPlans.Count > 0)
            {
                /* Never "all covered, you are done" when a rollup was refused: that rollup is NOT covered, its
                   retention policy will stay held, and saying otherwise sends the operator away from a corrupt
                   row they need to fix. */
                error.WriteLine("REFUSED. Nothing was materialized for these rollups, and their retention policies stay held:");
                foreach (var refusal in refusedPlans)
                {
                    error.WriteLine("  - " + refusal);
                }

                return 1;
            }

            output.WriteLine("Every rollup already covers its own source. Any retention policy still held will arm itself on the next service start.");
            return 0;
        }

        var totalEstimate = work.Sum(w => w.Plan.EstimatedBytes);
        var anyUncalibrated = work.Exists(w => !w.Plan.Calibrated);

        output.WriteLine("Plan:");
        foreach (var (_, plan) in work)
        {
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"  {plan.View}: materialize {plan.FromUtc:yyyy-MM-dd} -> {plan.ToUtc:yyyy-MM-dd} ({plan.BucketsToAdd:N0} buckets in {plan.Slices:N0} slices), estimated {plan.EstimatedSize}")
                + (plan.Calibrated ? "" : " (UNCALIBRATED upper bound — this rollup has materialized nothing to measure)"));
        }

        output.WriteLine();
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"Total estimated materialization: {RollupBackfillPlan.FormatBytes(totalEstimate)}; expected duration about {DescribeDuration(RollupBackfill.EstimatedDuration(totalEstimate))} at the ~16 MB/s throughput measured on this host class."));

        if (anyUncalibrated)
        {
            output.WriteLine(
                "NOTE: at least one estimate is an UPPER BOUND, not a measurement — that rollup has materialized " +
                "nothing this could be calibrated against. It is deliberately generous: refusing a backfill that " +
                "would have fit is recoverable, filling the volume is not.");
        }

        var (freeBytes, freeError) = await ResolveStoreFreeSpaceAsync(connection, cancellationToken);
        if (freeError is not null)
        {
            error.WriteLine();
            error.WriteLine("REFUSING: " + freeError);
            error.WriteLine(
                "This backfill materializes history BEFORE any purge can reclaim anything, so running it without " +
                "knowing the free space is exactly the disk-exhaustion risk it exists to avoid.");
            return 1;
        }

        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"Free space on the store volume: {RollupBackfillPlan.FormatBytes(freeBytes)}; required: {RollupBackfillPlan.FormatBytes(RollupBackfill.RequiredBytes(totalEstimate))}."));

        if (!RollupBackfill.HasRoom(totalEstimate, freeBytes))
        {
            error.WriteLine();
            error.WriteLine(FormatDiskRefusal(totalEstimate, freeBytes));
            return 1;
        }

        if (dryRun)
        {
            output.WriteLine();
            output.WriteLine("--dry-run: nothing was materialized. Re-run without --dry-run to proceed.");
            return 0;
        }

        output.WriteLine();
        /* Slices run NEWEST-FIRST (see RollupBackfill.Slices), so coverage extends DOWNWARD toward raw's oldest
           row and the measured floor is a truthful progress cursor rather than a value the first slice pins.
           That is what makes this claim safe to make: an interrupted run reports SHORT, not DONE. */
        output.WriteLine("Backfilling, newest-first, so coverage extends downward toward the oldest raw row.");
        output.WriteLine("Safe to interrupt: every completed slice is committed, an interrupted run reports SHORT rather");
        output.WriteLine("than claiming success, and re-running resumes from the measured floor.");

        /* THE DEGRADE HAS TO REACH THE OPERATOR. RefreshDisclosure carries both the run's options capability
           (latched once, so a pre-2.21 store pays one failed call rather than one per slice) and the sink that
           says so. It is a REQUIRED argument precisely because the first cut made it a trailing optional and
           both call sites silently omitted it — degrade-and-be-silent, while the call-site doc claimed the
           contract was held. Routed to stderr alongside the REFUSED/SHORT lines. */
        var disclosure = new RefreshDisclosure(message => error.WriteLine("  NOTE: " + message));

        var shortfalls = new List<string>();
        foreach (var (target, plannedUpFront) in work)
        {
            output.WriteLine();
            output.WriteLine($"  {plannedUpFront.View}:");

            /* RE-PLAN FROM LIVE MEASUREMENTS, because a hierarchical daily's target MOVES during this run.
               The plans above were all taken before any slice ran — necessary, since the disk preflight has to
               total the whole job before committing to any of it — but a daily converges to its HOURLY, and
               that hourly is backfilled EARLIER IN THIS SAME LOOP. Planning a daily from the pre-backfill
               snapshot aims it at where its source used to start, so it does almost nothing and is then judged
               against where its source now starts: SHORT on every daily, every time. Caught by the verb's own
               end-to-end test going red on exactly that.

               Re-planning here is the same measured-floor idiom that makes resume work, applied one level up:
               the up-front pass sizes the job, this one aims it. */
            RollupBackfillPlan plan;
            try
            {
                var live = await RollupBackfill.ProbeAsync(connection, target.View, target.Source, target.SourceTimeColumn, cancellationToken);
                plan = RollupBackfill.Plan(
                    target.View, live.SourceOldestUtc, live.CoverageOldestUtc,
                    live.MaterializedBuckets, live.MaterializedBytes,
                    rawBytesCache.TryGetValue(target.RawTable, out var cachedRawBytes) ? cachedRawBytes : 0,
                    target.BucketWidth);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                error.WriteLine($"    FAILED to re-probe before running: {ex.Message}");
                shortfalls.Add($"{plannedUpFront.View} could not be re-probed before its slices ran");
                continue;
            }

            if (plan.Refusal is string replanRefusal)
            {
                /* A REFUSAL IS NOT A COMPLETION, and this is a GUARD rather than an assertion that the case
                   cannot arise. RollupBackfillPlan.Absurd sets IsComplete as well as Refusal, so testing
                   IsComplete first — as an earlier cut did — printed "already covers" over a refusal and
                   counted it as success.

                   On today's code a refused re-plan is close to unreachable: a corrupt source timestamp makes
                   the HOURLY's up-front plan refuse on the same row, and since a daily is now planned against
                   its source's eventual floor, the daily refuses up front too — so neither reaches this loop.
                   But "close to unreachable" is an argument about the current call graph, not a property, and
                   the failure it protects against is slicing a window the planner already rejected as garbage.
                   The guard costs three lines; the argument would have to be re-derived by every future
                   reader. */
                error.WriteLine($"    [REFUSED] {target.View}: {replanRefusal}");
                shortfalls.Add($"{target.View}: {replanRefusal}");
                continue;
            }

            if (plan.IsComplete)
            {
                /* Its source moved under it and it is already covered — normal for a daily whose hourly
                   turned out to reach no further back than the daily already did. */
                output.WriteLine($"    already covers {target.Source} — nothing to do.");
                continue;
            }

            var completed = 0L;
            var sliceFailed = false;
            foreach (var (from, to) in RollupBackfill.Slices(plan.FromUtc, plan.ToUtc))
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var floor = await RollupBackfill.RunSliceAsync(connection, plan.View, from, to, disclosure, cancellationToken);
                    completed++;
                    output.WriteLine(string.Create(
                        CultureInfo.InvariantCulture,
                        $"    [{completed:N0}/{plan.Slices:N0}] {from:yyyy-MM-dd} -> {to:yyyy-MM-dd}; coverage now starts {floor:yyyy-MM-dd HH:mm}"));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    error.WriteLine($"    FAILED at {from:yyyy-MM-dd}: {ex.Message}");
                    sliceFailed = true;
                    break;
                }
            }

            /* CONVERGENCE IS MEASURED, NEVER INFERRED. A refresh that stops on its internal batch cap logs
               server-side and returns success to the client, so "the calls did not throw" is no evidence that
               the range was materialized. The only honest check is to re-read the floor from DATA and compare
               it to the raw row it had to reach. */
            var finalFloor = await RollupBackfill.ReadCoverageFloorAsync(connection, plan.View, cancellationToken);
            var after = await RollupBackfill.ProbeAsync(connection, plan.View, target.Source, target.SourceTimeColumn, cancellationToken);
            var reached = finalFloor is not null && after.SourceOldestUtc is not null && finalFloor <= after.SourceOldestUtc;

            /* Short WITHOUT a slice having failed means the refreshes reported success over a range they did
               not materialize — the signature of an earlier pass cut short, whose invalidation records a plain
               refresh will now skip straight over. That is the one case the forced form repairs, and the only
               case it is worth paying for. */
            if (!reached && !sliceFailed && after.SourceOldestUtc is not null)
            {
                output.WriteLine($"    {plan.View} is still short after every slice; escalating to a forced refresh over the range.");
                try
                {
                    finalFloor = await RollupBackfill.RepairAsync(connection, plan.View, plan.FromUtc, plan.ToUtc, disclosure, cancellationToken);
                    reached = finalFloor is not null && finalFloor <= after.SourceOldestUtc;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    /* force arrived in TimescaleDB 2.18 — an older bring-your-own store raises 42883 here.
                       Reported, not fatal: the shortfall is already going to be reported below. */
                    error.WriteLine($"    forced refresh unavailable on this store ({ex.Message}).");
                }
            }

            if (reached)
            {
                output.WriteLine($"    COVERED: {plan.View} now starts at {finalFloor:yyyy-MM-dd HH:mm}, at or before {target.Source}'s oldest row ({after.SourceOldestUtc:yyyy-MM-dd HH:mm}) — which is what the arming gate measures for this tier.");
            }
            else
            {
                shortfalls.Add($"{plan.View} reaches back to {finalFloor:yyyy-MM-dd HH:mm} but its source {target.Source} reaches back to {after.SourceOldestUtc:yyyy-MM-dd HH:mm}");
                error.WriteLine(
                    $"    SHORT: {plan.View} did not reach its source {target.Source}'s oldest row" +
                    (sliceFailed
                        ? " (a slice failed above)."
                        : " even though every slice reported success — a refresh that stops on its internal batch cap is silent to the client. Re-run to continue."));
            }
        }

        output.WriteLine();
        /* A refusal from the planning pass counts as a shortfall here too: those rollups were never attempted,
           so DONE would be a lie about them even when every rollup that DID run reached coverage. */
        shortfalls.AddRange(refusedPlans);

        if (shortfalls.Count > 0)
        {
            error.WriteLine("INCOMPLETE. These rollups do not yet cover their sources, so their retention policies stay held:");
            foreach (var shortfall in shortfalls)
            {
                error.WriteLine("  - " + shortfall);
            }

            error.WriteLine();
            error.WriteLine("Re-run --backfill-rollups; it resumes from the measured floor.");
            return 1;
        }

        output.WriteLine("DONE. Every rollup now covers its own source, which is what each retention policy's arming gate measures.");
        output.WriteLine();
        output.WriteLine("NEXT: restart the PerformanceMonitor Darling service. The arming gate checks coverage at");
        output.WriteLine("startup and releases the held retention policies by itself — there is no arming step here and");
        output.WriteLine("nothing to run by hand. The startup log line reading");
        output.WriteLine("  'N/N retention policies in place, N armed, 0 held paused pending backfill'");
        output.WriteLine("is the confirmation; the first purge then reclaims the raw tables in one pass.");
        output.WriteLine();
        output.WriteLine($"Do not delay the restart. The hourly rollups carry their OWN retention policy ({TimescaleSupport.HourlyRetentionInterval}), already");
        output.WriteLine("armed on these stores, which will trim the coverage this run just built when it next fires");
        output.WriteLine("(roughly daily). Restarting now is what lets the raw policies arm off that coverage first. If");
        output.WriteLine("the trim wins the race nothing is lost — raw is still held — and re-running this verb rebuilds it.");
        return 0;
    }

    /// <summary>
    /// Free space on the volume the store's data directory lives on. Asked of the STORE
    /// (<c>current_setting('data_directory')</c>) rather than derived from config, so it is right in
    /// bring-your-own mode too. Returns an error string rather than a number in the two cases where measuring
    /// would measure the WRONG volume: the login cannot read the setting, or the store is on another host so
    /// the path does not exist here.
    /// </summary>
    private static async Task<(long FreeBytes, string? Error)> ResolveStoreFreeSpaceAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var dataDirectory = await RollupBackfill.DataDirectoryAsync(connection, cancellationToken);
        if (string.IsNullOrWhiteSpace(dataDirectory))
        {
            return (0, "could not read the store's data_directory, so the free space on the volume that will grow is unknown. The login needs superuser or pg_read_all_settings.");
        }

        if (!Directory.Exists(dataDirectory))
        {
            return (0, $"the store's data directory ({dataDirectory}) does not exist on this machine, so the store is on another host and this machine's free space is not the number that matters. Run --backfill-rollups ON the store host.");
        }

        try
        {
            return (new DriveInfo(Path.GetPathRoot(Path.GetFullPath(dataDirectory))!).AvailableFreeSpace, null);
        }
        catch (Exception ex)
        {
            return (0, $"could not read free space for {dataDirectory}: {ex.Message}.");
        }
    }

    /// <summary>
    /// The refusal, with the exact shortfall and the two things an operator can actually do about it. PURE, so
    /// the numbers and the options pin in a unit test — which matters precisely because this message only ever
    /// appears in the situation nobody wants to reproduce by hand.
    /// </summary>
    public static string FormatDiskRefusal(long estimatedBytes, long freeBytes)
    {
        var required = RollupBackfill.RequiredBytes(estimatedBytes);
        var shortfall = required - freeBytes;

        return string.Create(CultureInfo.InvariantCulture, $@"REFUSING: not enough free space to back fill safely.

  Estimated materialization : {RollupBackfillPlan.FormatBytes(estimatedBytes)}
  Required free space       : {RollupBackfillPlan.FormatBytes(required)}  (estimate x {RollupBackfill.SafetyFactor:0.##} headroom + {RollupBackfillPlan.FormatBytes(RollupBackfill.ReserveBytes)} reserve)
  Free space now            : {RollupBackfillPlan.FormatBytes(freeBytes)}
  SHORT BY                  : {RollupBackfillPlan.FormatBytes(shortfall)}

The rollups have to materialize the WHOLE history before the arming gate will release the raw
retention policies, so peak disk comes BEFORE any reclaim. Running this now would fill the volume
and take the store down without ever reaching the point where it frees anything.

Your options:
  1. Grow the volume by at least {RollupBackfillPlan.FormatBytes(shortfall)} and re-run. This is the option that ends
     with the raw purges armed and the space reclaimed permanently.
  2. Accept waiting. Nothing is broken and nothing is being lost: the arming gate is holding the raw
     purges closed precisely because the rollups do not cover this history yet, and reads of those old
     windows are served from raw. Raw keeps growing at full rate until the backfill happens, so this
     buys time rather than solving it.

Re-run with --dry-run at any time to re-check the numbers; the plan is recomputed from the store.");
    }

    /// <summary>A duration an operator can plan around ("about 3 hours"), not a timespan literal.</summary>
    private static string DescribeDuration(TimeSpan duration)
    {
        if (duration < TimeSpan.FromMinutes(1))
        {
            return "under a minute";
        }

        if (duration < TimeSpan.FromHours(1))
        {
            return string.Create(CultureInfo.InvariantCulture, $"{duration.TotalMinutes:0} minutes");
        }

        return duration < TimeSpan.FromDays(1)
            ? string.Create(CultureInfo.InvariantCulture, $"{duration.TotalHours:0.#} hours")
            : string.Create(CultureInfo.InvariantCulture, $"{duration.TotalDays:0.#} days");
    }
}
