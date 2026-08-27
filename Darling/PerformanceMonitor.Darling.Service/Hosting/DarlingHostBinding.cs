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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// The surface-agnostic bind/auth helpers shared by the two optional Kestrel hosts — the MCP host
/// (<see cref="Mcp.DarlingMcpHostService"/>) and the web host (<see cref="Mcp.DarlingWebHostService"/>).
/// Extracted so the LAN-exposure decision ladder, the in-app CIDR check, the loopback-listener collision
/// guard, and the constant-time token compare live ONCE, with ONE test suite, instead of being copy-pasted
/// per surface (darling-network-endpoints anti-drift). The MCP host is refactored onto this class (its
/// behavior byte-for-byte unchanged — its pre-existing tests stay green untouched, the refactor proof); the
/// web host is built on it from the start.
///
/// <para>Everything here is PURE (no logger, no server). It used to also host the best-effort firewall
/// reconcile; that moved to <see cref="DarlingFirewallCheck"/> when the runtime stopped writing rules and
/// started only verifying them (#1771). The (mode, reason) split lets the caller map a reason to a log
/// severity (Round-4 #7) while the pure ladder stays free of side effects.</para>
/// </summary>
internal static class DarlingHostBinding
{
    /// <summary>The effective bind. <see cref="LoopbackOnly"/> is the secure default;
    /// <see cref="NetworkAndLoopback"/> binds the LAN interface behind the surface's auth gate.</summary>
    internal enum BindMode
    {
        LoopbackOnly,
        NetworkAndLoopback,
    }

    /// <summary>WHY the bind resolved as it did — the caller maps this to a severity (Round-4 #7).
    /// Member order is pinned equal to the MCP host's nested <c>McpBindReason</c> (a test enforces it) so the
    /// MCP forwarders can cast between them.</summary>
    internal enum BindReason
    {
        /// <summary>No network block, or a loopback/absent listen — the byte-for-byte-today loopback server.</summary>
        LoopbackByDefault,

        /// <summary>All preconditions met: non-loopback listen + managed + token present + valid allowFrom CIDR.</summary>
        NetworkExposed,

        /// <summary>network.* is set but postgres.managed = false and the process is not containerized —
        /// network exposure is managed-mode-or-container only (a warning). In a container (#1804) the
        /// compose port mapping is the boundary the BYO reverse-proxy rule was standing in for, and a
        /// loopback bind would be unreachable through it, so exposure is honored there.</summary>
        ManagedModeRequired,

        /// <summary>Exposed + managed but the listen value is not a parseable IP — fail-closed to loopback.</summary>
        ListenInvalid,

        /// <summary>Exposed + managed but no bearer token — fail-closed to loopback.</summary>
        TokenMissing,

        /// <summary>Exposed + managed + token but allowFrom is missing/not a valid CIDR or its family does not match the listen — fail-closed to loopback.</summary>
        AllowFromInvalid,
    }

    /// <summary>The (mode, reason) pair returned by <see cref="ResolveBind"/>.</summary>
    internal readonly record struct BindDecision(BindMode Mode, BindReason Reason);

    /// <summary>
    /// Whether this process runs inside a container (#1804) — the official .NET images set
    /// <c>DOTNET_RUNNING_IN_CONTAINER=true</c>, and the Dockerfile shipped with the compose distribution
    /// rides those images. IMPURE (environment read), which is why it lives beside — never inside — the
    /// pure <see cref="ResolveBind"/> ladder: the callers read it once and pass it in, so the decision
    /// table stays testable without environment games.
    /// </summary>
    internal static bool IsRunningInContainer
        => string.Equals(
            Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// PURE resolution of an effective host bind from the surface-agnostic inputs. Returns
    /// <see cref="BindMode.NetworkAndLoopback"/> ONLY when <paramref name="listen"/> is a genuine network
    /// (non-loopback) address (via <see cref="DarlingNetwork.IsExposedListenAddress"/> — so <c>127.0.0.1</c>
    /// stays loopback, never a network bind/collision) that ALSO parses as an IP, AND <paramref name="managed"/>
    /// is true, AND a bearer token is present (<paramref name="tokenPresent"/> — presence only; the host
    /// decrypts later), AND <paramref name="allowFrom"/> is a valid CIDR of the SAME address family as the
    /// listen. Otherwise loopback-only with the specific reason. Never throws; never consults a logger.
    /// <paramref name="networkConfigured"/> is "the surface's network block has any field set" — used only for
    /// the BYO "network.* is ignored" notice (the network path never runs in BYO).
    /// </summary>
    internal static BindDecision ResolveBind(
        string? listen, string? allowFrom, bool tokenPresent, bool networkConfigured, bool managed,
        bool inContainer = false)
    {
        var exposed = DarlingNetwork.IsExposedListenAddress(listen);

        /* #1804: a containerized BYO deployment (compose) is exposure-capable — the port mapping is the
           boundary the BYO reverse-proxy rule was standing in for, and a loopback bind would be dead
           through it. The token/CIDR preconditions below apply identically either way. */
        var exposureCapable = managed || inContainer;

        if (!exposed)
        {
            /* Not exposed = the secure default. The one exception worth a word: a BYO store with a network
               block set at all (even a loopback/partial one) is ignored -> the managed-mode notice. */
            if (!exposureCapable && networkConfigured)
            {
                return new BindDecision(BindMode.LoopbackOnly, BindReason.ManagedModeRequired);
            }

            return new BindDecision(BindMode.LoopbackOnly, BindReason.LoopbackByDefault);
        }

        /* Exposed. Managed-or-container only: uncontained BYO never binds the network path, and this
           dominates a missing/invalid listen/token/allowFrom so the operator sees the actionable
           "managed only" notice first. */
        if (!exposureCapable)
        {
            return new BindDecision(BindMode.LoopbackOnly, BindReason.ManagedModeRequired);
        }

        /* The listen must be a parseable IP. IsExposedListenAddress treats a non-IP value (localhost, a
           hostname, "*") as "exposed" so it is never silently bound; here it degrades to loopback rather than
           throwing when the host later does IPAddress.Parse. exposed => listen is non-empty. */
        if (!IPAddress.TryParse(listen!.Trim(), out var listenIp))
        {
            return new BindDecision(BindMode.LoopbackOnly, BindReason.ListenInvalid);
        }

        /* Token presence only (no decryption here — that is an effectful, Windows-only step the host does). */
        if (!tokenPresent)
        {
            return new BindDecision(BindMode.LoopbackOnly, BindReason.TokenMissing);
        }

        /* allowFrom must be a valid CIDR (host bits zeroed) whose address family matches the listen: a
           mismatched family would bind one family while the in-app CIDR check rejects the other, 403-ing every
           network client — fail-closed but silently non-functional, so degrade with a clear reason instead. */
        if (string.IsNullOrWhiteSpace(allowFrom) || !IPNetwork.TryParse(allowFrom.Trim(), out var cidr))
        {
            return new BindDecision(BindMode.LoopbackOnly, BindReason.AllowFromInvalid);
        }

        if (cidr.BaseAddress.AddressFamily != listenIp.AddressFamily)
        {
            return new BindDecision(BindMode.LoopbackOnly, BindReason.AllowFromInvalid);
        }

        return new BindDecision(BindMode.NetworkAndLoopback, BindReason.NetworkExposed);
    }

    /// <summary>
    /// PURE severity map for a <see cref="ResolveBind"/> reason (Round-4 #7): the fail-closed degrades
    /// (<see cref="BindReason.ListenInvalid"/>/<see cref="BindReason.TokenMissing"/>/
    /// <see cref="BindReason.AllowFromInvalid"/>) are <see cref="LogLevel.Critical"/>, the BYO "ignored"
    /// notice (<see cref="BindReason.ManagedModeRequired"/>) is <see cref="LogLevel.Warning"/>, and the
    /// non-degrade reasons (<see cref="BindReason.NetworkExposed"/>/<see cref="BindReason.LoopbackByDefault"/>)
    /// are silent (null).
    /// </summary>
    internal static LogLevel? MapBindReasonSeverity(BindReason reason) => reason switch
    {
        BindReason.ListenInvalid => LogLevel.Critical,
        BindReason.TokenMissing => LogLevel.Critical,
        BindReason.AllowFromInvalid => LogLevel.Critical,
        BindReason.ManagedModeRequired => LogLevel.Warning,
        _ => null,
    };

    /// <summary>
    /// Whether to ALSO bind the two loopback families beside the network listener. Skipped when the listen
    /// value is itself a loopback address (already covered) or a wildcard (<c>0.0.0.0</c> covers IPv4 loopback,
    /// <c>::</c> the IPv6) — binding an explicit loopback on the same port then would collide (WSAEADDRINUSE).
    /// For a specific LAN IP the loopback binds are added so a local client resolving "localhost" still reaches
    /// the server (which, in network mode, now also requires the surface's auth gate).
    /// </summary>
    internal static bool ShouldAddLoopbackListeners(IPAddress listenIp)
        => !(IPAddress.IsLoopback(listenIp)
             || listenIp.Equals(IPAddress.Any)
             || listenIp.Equals(IPAddress.IPv6Any));

    /// <summary>
    /// PURE in-app CIDR check: is <paramref name="remoteIp"/> allowed? Loopback (<c>127.0.0.0/8</c> or
    /// <c>::1</c>, incl. an IPv4-mapped-IPv6 form) is ALWAYS allowed — it is not in
    /// <paramref name="allowedCidr"/>, so otherwise the loopback bind's local clients would be rejected.
    /// Everything else must fall inside the CIDR. A null remote (unverifiable origin) fails closed.
    /// </summary>
    internal static bool IsRemoteAddressAllowed(IPAddress? remoteIp, IPNetwork allowedCidr)
    {
        if (remoteIp is null)
        {
            return false;
        }

        var ip = remoteIp.IsIPv4MappedToIPv6 ? remoteIp.MapToIPv4() : remoteIp;
        return IPAddress.IsLoopback(ip) || allowedCidr.Contains(ip);
    }

    /// <summary>
    /// PURE Host-header allowlist — the DNS-rebinding guard both Darling hosts install as their FIRST
    /// middleware, in BOTH bind modes. The decision itself lives in
    /// <see cref="PerformanceMonitor.Common.HostHeaderGuard"/> so Lite's MCP host runs the SAME code (#1648);
    /// this forwarder keeps it reachable where the rest of the two hosts' bind/auth helpers live, so neither
    /// host reaches past this class. Pass <paramref name="networkListenIp"/> = null in loopback-only mode.
    /// </summary>
    internal static bool IsAllowedHost(string? host, IPAddress? networkListenIp)
        => HostHeaderGuard.IsAllowedHost(host, networkListenIp);

    /// <summary>
    /// PURE constant-time token comparison. Both tokens are hashed to a fixed 32 bytes first, so the compare
    /// is constant-time regardless of length and leaks neither token nor its length. An empty
    /// <paramref name="expected"/> never authorizes (the caller should also guard). Used by the MCP host's
    /// Bearer-header check and the web host's <c>?token=</c> check alike.
    /// </summary>
    internal static bool FixedTimeTokenEquals(string? presented, string? expected)
    {
        if (string.IsNullOrEmpty(expected) || string.IsNullOrEmpty(presented))
        {
            return false;
        }

        var expectedHash = SHA256.HashData(Encoding.UTF8.GetBytes(expected));
        var presentedHash = SHA256.HashData(Encoding.UTF8.GetBytes(presented));
        return CryptographicOperations.FixedTimeEquals(expectedHash, presentedHash);
    }

    /* ---------------------------------------------------------------------------------------------------
       WHICH PLANE decided this endpoint is on, and on what port (#2389). One <c>mcp</c> object in darling.json
       has TWO owners: <c>enabled</c>/<c>port</c> are a first-run SEED that the store's config_service row
       overrides forever after, while <c>network.*</c> (listen / allowFrom / the DPAPI token) is file-only and
       restart-only. Nothing in the file distinguishes them, so an operator who edits mcp.enabled, restarts and
       greps the log finds "Starting MCP server on http://<lan-ip>:5152" -- true, and then contradicted five
       seconds later when the worker's first publish arrives and the supervisor reconciles to the store.

       network.* deliberately STAYS file-only rather than being moved into the store to match: the DPAPI token
       is LocalMachine-scoped, so a blob in config_service is undecryptable on any other host that reads that
       store, and a bind address + CIDR + token living in config_service would let a REMOTE admin store
       connection -- the pivot the postgres.network role warning already names -- re-point this listener onto a
       LAN interface behind a credential of its own choosing. Exposure should require touching the host. So the
       split is kept and made VISIBLE instead: the effective values carry their origin into the start line, and
       a disagreement is reported at the point of override rather than left to be inferred from two INFO lines.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>Which plane supplied the effective enable/port a host supervisor is acting on (#2389).</summary>
    internal enum EndpointToggleOrigin
    {
        /// <summary>darling.json, because the worker has not published yet (still bootstrapping, or it never
        /// reached the store). PROVISIONAL: the control plane can contradict it within one poll interval.</summary>
        File,

        /// <summary>The store's <c>config.config_service</c> row, published by the worker. Authoritative.</summary>
        ControlPlane,
    }

    /// <summary>The effective (enabled, port) a supervisor acts on, WITH its provenance (#2389).
    /// <see cref="EnabledOverridden"/> / <see cref="PortOverridden"/> are true only when the control plane
    /// supplied a value that DIFFERS from darling.json's -- the reportable disagreement.</summary>
    internal readonly record struct EndpointToggle(
        bool Enabled, int Port, EndpointToggleOrigin Origin, bool EnabledOverridden, bool PortOverridden);

    /// <summary>
    /// PURE: the store row wins whenever the worker has published one -- byte-for-byte the old
    /// <c>published?.Enabled ?? config.Mcp.Enabled</c> pair -- but it also reports WHERE the values came from
    /// and whether they contradict the file, which a null-coalesce structurally cannot.
    /// </summary>
    internal static EndpointToggle ResolveEndpointToggle((bool Enabled, int Port)? published, bool fileEnabled, int filePort)
        => published is null
            ? new EndpointToggle(fileEnabled, filePort, EndpointToggleOrigin.File, false, false)
            : new EndpointToggle(
                published.Value.Enabled,
                published.Value.Port,
                EndpointToggleOrigin.ControlPlane,
                EnabledOverridden: published.Value.Enabled != fileEnabled,
                PortOverridden: published.Value.Port != filePort);

    /// <summary>
    /// PURE: the provenance clause the start line carries, so "Starting ... on http://..." says on whose
    /// authority it is starting -- and admits when it is running on file values the control plane has not
    /// weighed in on yet, which is the line the operator greps and stops reading.
    /// </summary>
    internal static string DescribeToggleOrigin(EndpointToggle toggle)
        => toggle.Origin == EndpointToggleOrigin.ControlPlane
            ? "the control plane (config.config_service)"
            : "darling.json (PROVISIONAL - the control plane has not published yet and may stop or rebind this server)";

    /// <summary>
    /// PURE: the one-line report of a control-plane override, or null when the two planes agree (or nothing is
    /// published yet, in which case there is nothing to disagree with). <paramref name="section"/> is the
    /// darling.json object name, the config_service column prefix, and the CLI verb suffix at once ("mcp" -&gt;
    /// mcp.enabled / config_service.mcp_enabled / --enable-mcp), which is what keeps the MCP and web wordings
    /// from drifting apart. The file values are passed in rather than re-derived so the message quotes what the
    /// caller actually loaded.
    /// </summary>
    internal static string? DescribeToggleOverride(
        EndpointToggle toggle, string section, string surface, bool fileEnabled, int filePort)
    {
        if (toggle.Origin != EndpointToggleOrigin.ControlPlane || (!toggle.EnabledOverridden && !toggle.PortOverridden))
        {
            return null;
        }

        var fields = new List<string>(2);
        if (toggle.EnabledOverridden)
        {
            fields.Add(
                $"enabled is {(fileEnabled ? "true" : "false")} in darling.json ({section}.enabled) but "
                + $"{(toggle.Enabled ? "true" : "false")} in config.config_service.{section}_enabled");
        }

        if (toggle.PortOverridden)
        {
            fields.Add($"port is {filePort} in darling.json ({section}.port) but {toggle.Port} in config.config_service.{section}_port");
        }

        /* The ownership disclosure is unconditional because it is true in every state; the CONSEQUENCE is not.
           Review catch: a single closing sentence claiming "the control plane keeps this endpoint off" is wrong
           whenever the control plane turned it ON despite the file, and wrong for a port-only mismatch where
           both planes agree it runs -- which is the likelier real case. So the state-specific half is emitted
           only in the state it describes. */
        var consequence = toggle.Enabled
            ? " It is what this RUNNING endpoint is bound and gated by, and no store setting can move it."
            : " It is not what is keeping this endpoint down, and it takes effect as written the moment the "
                + "control plane enables it.";

        return $"{surface} configuration disagrees across the two planes and the CONTROL PLANE WINS: "
            + string.Join("; ", fields)
            + $". After the first run darling.json's {section}.enabled/{section}.port are only the SEED -- change them with "
            + $"--enable-{section}/--disable-{section} or the Viewer's Settings, or the file values will keep being ignored. "
            + $"The {section}.network block is the OPPOSITE: file-only, restart-only, no store equivalent -- the control "
            + "plane cannot change where this endpoint binds or what token it requires."
            + consequence;
    }

    /* ---------------------------------------------------------------------------------------------------
       #2414: the same split, one layer down -- which port a FIREWALL rule is named for.

       A scoped rule carries its port inside its DisplayName ("PerformanceMonitor Darling MCP (port 5152)"),
       so the port is not a parameter of the rule, it IS the rule's identity. The endpoint binds the CONTROL
       PLANE's port; every firewall surface used to derive both the name and the -LocalPort from darling.json.
       Those two agree right up until somebody moves the port in the Viewer's Settings, and then the elevated
       verb opens a rule for a port nothing serves while leaving the served port shut -- an unreachable
       endpoint AND an inbound allow rule with no listener behind it, which is the exact inverse of what
       scoping the rule to a port was for.

       So a firewall verb resolves its port through ResolveEndpointToggle, the same call the two supervisors
       bind on, and then SAYS which plane answered. Saying it is not decoration: a verb that opens a port and
       guesses at which one silently is precisely how this defect stayed invisible for as long as it did, and
       an operator who has to be told to re-run something needs to know what the last run actually did. The
       describer is pure so the wording pins in a test rather than being discovered in the field.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// PURE: what a firewall verb discloses about the port it just scoped a rule to (#2414). Three states,
    /// because they call for three different things from the reader:
    /// <list type="bullet">
    /// <item>the control plane answered and DISAGREES with darling.json -- the defect's own precondition, so
    /// it names both ports and says what naming the rule for the file's one would have cost;</item>
    /// <item>the control plane answered and agrees -- a one-line confirmation, so an operator can see the
    /// authoritative read happened rather than having to infer it from silence;</item>
    /// <item>the control plane could NOT be read -- the file's seed was used, and this says so, why, and what
    /// makes it wrong, because a firewall verb that falls back without saying so re-creates the bug quietly.</item>
    /// </list>
    /// <paramref name="section"/> is the darling.json object name, the config_service column prefix and the
    /// CLI verb suffix at once ("mcp" -&gt; mcp.port / config_service.mcp_port / --enable-mcp), which is what
    /// keeps the MCP and web wordings from drifting apart -- the same seam <see cref="DescribeToggleOverride"/>
    /// uses. <paramref name="filePort"/> is passed in rather than re-derived so the message quotes the value
    /// the caller actually loaded.
    /// </summary>
    internal static string DescribeFirewallPortAuthority(
        EndpointToggle toggle, string section, string surface, int filePort, string? storeUnavailableReason)
    {
        if (toggle.Origin == EndpointToggleOrigin.ControlPlane)
        {
            return toggle.PortOverridden
                ? $"Firewall: scoping the {surface} rule to port {toggle.Port} -- the CONTROL PLANE's port "
                    + $"(config.config_service.{section}_port), which is the port this endpoint actually binds. "
                    + $"darling.json says {section}.port = {filePort}, but after the first run that is only the seed: "
                    + $"a rule named for {filePort} would leave the served port closed to the LAN and hold an inbound "
                    + "allow rule open on a port nothing is listening on."
                : $"Firewall: scoping the {surface} rule to port {toggle.Port}, confirmed against the control plane "
                    + $"(config.config_service.{section}_port) -- darling.json's {section}.port agrees.";
        }

        return $"Firewall: could NOT read the control plane ({storeUnavailableReason ?? "reason unknown"}), so the "
            + $"{surface} rule is scoped to darling.json's {section}.port = {filePort}. That value is the FIRST-RUN "
            + "SEED: it is the right port on a box whose store has never been written -- the normal state at install "
            + "time, which is why this verb uses it rather than refusing -- and it is the WRONG port the moment the "
            + $"{surface} port has been changed in the Viewer's Settings or with --enable-{section}. If the endpoint "
            + "is unreachable from the LAN after this, re-run --configure-firewall once the store is up: it resolves "
            + "the effective port and moves the rule.";
    }

    /* ---------------------------------------------------------------------------------------------------
       WHERE the network block came from, and when it can change (#2479, item 6). The other half of the same
       split #2389 made visible. A network block is loaded ONCE, at start-up, and held for the process
       lifetime by design: exposure should require touching the host, and the DPAPI token is LocalMachine-
       scoped so it could not live in the store anyway. That is the right design and it is not going to
       change -- but it produces the "I enabled it and it is still loopback-bound" trap in another costume,
       because nothing said the value was frozen the moment the process started.

       #2389/#2411 fixed the seed-versus-store confusion by making the split VISIBLE rather than by removing
       it, and #2414 did the same one layer down for the firewall port. This is that treatment applied to
       the third face of the same object: the endpoint states plainly, at every start, whether a network
       block was read from the file, what it decided, and that editing darling.json now changes nothing
       until a restart. Said in BOTH modes on purpose -- the loopback line was the one that carried no
       mention of the block at all, and loopback-when-you-expected-LAN is precisely the state an operator
       is trying to diagnose.
       --------------------------------------------------------------------------------------------------- */

    /// <summary>
    /// PURE: the one-line start-up statement of where <c>{section}.network</c> came from and when it can
    /// change. <paramref name="section"/> is the darling.json object name and the CLI verb suffix at once
    /// ("mcp" -&gt; mcp.network / --enable-mcp), which is what keeps the two hosts' wordings from drifting.
    ///
    /// <para>Three states, three different consequences, and only the true one is emitted. A single line
    /// claiming "restart to apply" would be wrong for an endpoint already serving the LAN exactly as
    /// configured, which is the likeliest state and the one where a spurious call to action costs the most
    /// credibility.</para>
    /// </summary>
    /// <param name="section">"mcp" or "web" - the darling.json object name.</param>
    /// <param name="surface">"MCP" or "Web dashboard" - how the log names this endpoint elsewhere.</param>
    /// <param name="blockConfigured">A <c>{section}.network</c> block was present in the file.</param>
    /// <param name="exposed">This endpoint actually bound a LAN address as a result.</param>
    /// <param name="listen">The bound address, when exposed.</param>
    /// <param name="allowFrom">The source CIDR, when exposed.</param>
    internal static string DescribeNetworkBlockLifetime(
        string section,
        string surface,
        bool blockConfigured,
        bool exposed,
        string? listen,
        string? allowFrom)
    {
        /* Common to all three: what CAN be changed without a restart, so naming what cannot does not read
           as "this endpoint is immovable". The control plane really does stop and rebind this server
           live -- it just cannot touch these three fields. */
        var storeHalf =
            $" The control plane still owns whether this endpoint runs and on which port "
            + $"(config.config_service.{section}_enabled / {section}_port, moved by --enable-{section} / "
            + $"--disable-{section} or the Viewer's Settings, and applied without a restart) - it simply "
            + "cannot move where this binds, who may reach it, or what token it requires.";

        if (!blockConfigured)
        {
            return $"{surface} network exposure: darling.json has no {section}.network block, so this endpoint is "
                + "loopback-only. Adding one is a FILE edit that takes effect on the next service RESTART; there "
                + "is no store setting and no Viewer control that can expose it."
                + storeHalf;
        }

        if (exposed)
        {
            return $"{surface} network exposure: {section}.network was read from darling.json at start-up and is "
                + $"FIXED for the lifetime of this process - LAN-bound on {listen} for {allowFrom}, behind the "
                + "configured token, until the service is RESTARTED. Editing darling.json now changes nothing "
                + "until then."
                + storeHalf;
        }

        /* The trap, named. An operator who edited the block and restarted nothing sees this and has their
           answer; one who DID restart is pointed at the fail-closed reason already logged above rather than
           being told the same thing twice in different words. */
        return $"{surface} network exposure: darling.json DEFINES {section}.network, but this endpoint is bound "
            + "loopback-only. That block was read at start-up and is FIXED for the lifetime of this process, so "
            + "editing darling.json now changes nothing until the service is RESTARTED. If you just edited it to "
            + $"expose this endpoint, restart the service; if you already restarted, the {section}.network line "
            + "logged above says why the exposure was refused."
            + storeHalf;
    }
}
