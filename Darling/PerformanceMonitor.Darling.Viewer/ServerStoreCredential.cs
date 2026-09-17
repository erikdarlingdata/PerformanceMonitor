/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Maps a viewer auth choice onto the <c>config.config_monitored_servers.auth</c> value the Darling
/// SERVICE can actually honor. As of #3484 the service's connect path
/// (<c>PerformanceMonitor.Darling.Service.MonitoredServerConnection</c>) supports four modes —
/// <see cref="Integrated"/> (Windows), <see cref="Sql"/> (SQL login + password), <see cref="ServicePrincipal"/>
/// (Entra application/client id + client secret) and <see cref="ManagedIdentity"/> (system- or user-assigned)
/// — so those are the ones the control-plane writes.
///
/// <para>The viewer UI (a faithful Lite port) also offers the INTERACTIVE Entra modes (MFA, device code,
/// default credential). Those the Darling service still cannot honor — it is a headless 24/7 collector with
/// nobody in front of it to answer a broker prompt or read a code that expires in minutes — so writing one
/// into the store would produce a row the service rejects. Rather than persist an un-honorable definition, the
/// write path BLOCKS those modes with <see cref="UnsupportedAuthMessage"/>.</para>
///
/// <para><b>The mapping is a whitelist, and that is the load-bearing part when a new auth mode ships.</b>
/// <see cref="MapAuth"/> names the honorable modes and returns null for everything else, so an interactive
/// mode added to <see cref="AuthenticationTypes"/> is rejected here on the day it is added rather than on the
/// day someone remembers to reject it — a blacklist would have admitted it silently and written a row the
/// service cannot connect with (#3214's broker-free and #3196's device-code modes both arrived that way and
/// needed no edit here).</para>
/// </summary>
public static class ServerStoreCredential
{
    /// <summary>Windows / integrated security — no per-server secret.</summary>
    public const string Integrated = "integrated";

    /// <summary>SQL login — username + a DPAPI-protected password blob.</summary>
    public const string Sql = "sql";

    /// <summary>Microsoft Entra service principal — client id in username, client secret in the DPAPI blob;
    /// non-interactive, so the headless service honors it (#3484). Pinned equal to the service's
    /// <c>MonitoredServer.UsesServicePrincipal</c>.</summary>
    public const string ServicePrincipal = "serviceprincipal";

    /// <summary>Azure managed identity — non-interactive and secret-less (#3484). Pinned equal to the
    /// service's <c>MonitoredServer.UsesManagedIdentity</c>.</summary>
    public const string ManagedIdentity = "managedidentity";

    /// <summary>
    /// The store <c>auth</c> value for a viewer <see cref="AuthenticationTypes"/> mode, or null when the
    /// Darling service cannot honor it. As of #3484 the service connects with Windows, SQL, and the two
    /// NON-interactive Entra modes (service principal, managed identity); the INTERACTIVE Entra modes (MFA,
    /// device-code, default-credential) still map to null — a headless collector has nobody to answer a broker
    /// prompt — so callers treat null as "block the save". The whitelist stays load-bearing: a new interactive
    /// mode added to <see cref="AuthenticationTypes"/> is rejected here on the day it is added.
    /// </summary>
    public static string? MapAuth(string? authenticationType) => authenticationType switch
    {
        AuthenticationTypes.Windows => Integrated,
        AuthenticationTypes.SqlServer => Sql,
        AuthenticationTypes.ServicePrincipal => ServicePrincipal,
        AuthenticationTypes.ManagedIdentity => ManagedIdentity,
        _ => null,
    };

    /// <summary>True when the Darling service can connect with this viewer auth mode (Windows, SQL, or the two
    /// non-interactive Entra modes).</summary>
    public static bool IsSupported(string? authenticationType) => MapAuth(authenticationType) is not null;

    /// <summary>True when the mapped store auth needs a per-server secret in the DPAPI blob: SQL (password) and
    /// service principal (client secret). Windows and managed identity carry none.</summary>
    public static bool RequiresSecret(string? authenticationType) =>
        MapAuth(authenticationType) is Sql or ServicePrincipal;

    /// <summary>
    /// The user-facing message shown when an INTERACTIVE Entra mode is chosen (the headless service can't
    /// honor it). Names the four supported modes rather than restating the growing list of interactive ones;
    /// the whitelist in <see cref="MapAuth"/> is what stays correct as modes are added.
    /// </summary>
    public const string UnsupportedAuthMessage =
        "The Darling service is a headless collector, so the interactive Microsoft Entra modes (MFA, device " +
        "code, default credential) are not supported — there is nobody in front of it to answer a broker " +
        "prompt. Choose Windows, SQL, Service Principal, or Managed Identity (both non-interactive), or a SQL " +
        "credential profile.";
}
