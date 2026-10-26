/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The rules the MCP <c>add_servers</c> tool applies to the client that calls it, held apart from the tool so the
/// tool body stays the shared onboarding path. The host builds one instance from <c>darling.json</c> and seats it
/// in DI (<c>DarlingMcpHostService.ConfigureMcpServices</c>); the tool takes it as a service parameter, so it never
/// reaches the advertised schema and a client cannot supply its own. The operator's <c>--add-server</c> command
/// calls the same onboarding core with no policy, so it is not gated.
///
/// <para><b>Windows authentication over an unverified connection.</b> <c>add_servers</c> defaults to Windows
/// authentication, where the service presents its own computer account to the host the caller names. A caller that
/// also sets <c>trust_server_certificate</c>, or an <c>encrypt_mode</c> that is not Mandatory or Strict, has told
/// the service to connect without confirming which server answered. An entry with that combination is refused
/// unless <c>mcp.allowWindowsAuthOverUnverifiedTls</c> is on in <c>darling.json</c> (default off). SQL authentication
/// and the Entra modes are not affected.</para>
/// </summary>
public sealed class DarlingMcpAddServersPolicy
{
    /// <summary>The darling.json key that lets <c>add_servers</c> accept Windows authentication over a
    /// connection whose server identity is not verified.</summary>
    internal const string UnverifiedWindowsAuthSetting = "mcp.allowWindowsAuthOverUnverifiedTls";

    private readonly DarlingUnverifiedWindowsAuthScreen _screen;

    /// <summary>Builds the policy from <c>mcp.allowWindowsAuthOverUnverifiedTls</c>.</summary>
    internal DarlingMcpAddServersPolicy(bool allowWindowsAuthOverUnverifiedTls)
    {
        _screen = new DarlingUnverifiedWindowsAuthScreen(allowWindowsAuthOverUnverifiedTls, UnverifiedWindowsAuthSetting);
    }

    /// <summary>The policy of a host whose <c>darling.json</c> says nothing: every setting off.</summary>
    internal static DarlingMcpAddServersPolicy Default { get; } = new(allowWindowsAuthOverUnverifiedTls: false);

    /// <summary>
    /// Why <paramref name="server"/> must not be added, or null when it may be. PURE: it reads only the parsed
    /// entry and the setting.
    ///
    /// <para>"Windows authentication" here is the connector's own definition: the arm of
    /// <c>MonitoredServerConnection.BuildConnectionString</c> that sets integrated security, which is every mode
    /// that is not SQL, service principal or managed identity. "Not verified" is <c>TrustServerCertificate</c>, or
    /// an <c>EncryptMode</c> of Optional, the only mode the connector does not map to Mandatory or Strict.</para>
    /// </summary>
    internal string? Refusal(MonitoredServer server) => _screen.Refusal(server);

    /// <summary>The same rule as an object of its own, for the web routes to run (they take the rule, not the policy).</summary>
    internal DarlingUnverifiedWindowsAuthScreen WindowsAuthScreen => _screen;

    /// <summary>
    /// Moves every entry <paramref name="policy"/> refuses out of <paramref name="entries"/> and into
    /// <paramref name="results"/> as an <c>invalid</c> row (counted as failed, like any other entry the tool will
    /// not add), and returns the entries that may go on. Runs before the store is opened or a probe is made, so a
    /// refused entry costs no connection. A null policy is the operator's own command: nothing is refused.
    /// </summary>
    internal static List<DarlingMcpServerAdminTools.ParsedServerEntry> Screen(
        DarlingMcpAddServersPolicy? policy,
        List<DarlingMcpServerAdminTools.ParsedServerEntry> entries,
        List<DarlingMcpServerAdminTools.ServerResult> results)
    {
        return DarlingUnverifiedWindowsAuthScreen.Screen(policy?.WindowsAuthScreen, entries, results);
    }
}
