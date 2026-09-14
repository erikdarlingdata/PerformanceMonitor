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
/// SERVICE can actually honor. The service's connect path
/// (<c>PerformanceMonitor.Darling.Service.MonitoredServerConnection</c>) supports exactly two modes —
/// <see cref="Integrated"/> (Windows / integrated security) and <see cref="Sql"/> (SQL login + password) —
/// so those are the only two the control-plane writes.
///
/// <para>The viewer UI (a faithful Lite port) also offers three Entra modes of its own, and Lite offers
/// more than the viewer does. The Darling service has NO Azure/AAD connect path (verified: nothing in the
/// Service project builds an access token), so writing any of those into the store would produce a row the
/// service rejects or silently mis-connects as integrated. Rather than persist an un-honorable definition,
/// the write path BLOCKS those modes with <see cref="UnsupportedAuthMessage"/>. Adding an Azure connect path
/// is a Service-project feature, out of scope here — surfaced in the PR, not silently dropped.</para>
///
/// <para><b>The mapping is a whitelist, and that is the load-bearing part when a new auth mode ships.</b>
/// <see cref="MapAuth"/> names the two honorable modes and returns null for everything else, so a mode added
/// to <see cref="AuthenticationTypes"/> is rejected here on the day it is added rather than on the day someone
/// remembers to reject it — #3214's broker-free Entra mode arrived that way and needed no edit to this file.
/// A blacklist would have admitted it silently and written a row the service cannot connect with. #3196's
/// device-code mode arrived the same way and also needed no edit here, and is Lite-only for a second reason
/// besides the missing connect path: it is INTERACTIVE, and the Darling service is a headless 24/7 collector
/// with nobody in front of it to read a code that expires in three minutes. The reason is the same for every
/// Azure mode and does not weaken as more are added: the service acquires no tokens at all, so there is
/// nothing for a new Entra mode to be honored BY.</para>
/// </summary>
public static class ServerStoreCredential
{
    /// <summary>Windows / integrated security — no per-server secret.</summary>
    public const string Integrated = "integrated";

    /// <summary>SQL login — username + a DPAPI-protected password blob.</summary>
    public const string Sql = "sql";

    /// <summary>
    /// The store <c>auth</c> value for a viewer <see cref="AuthenticationTypes"/> mode, or null when the
    /// Darling service cannot honor it (every Azure/Entra mode). Callers treat null as "block the save".
    /// </summary>
    public static string? MapAuth(string? authenticationType) => authenticationType switch
    {
        AuthenticationTypes.Windows => Integrated,
        AuthenticationTypes.SqlServer => Sql,
        _ => null,
    };

    /// <summary>True when the Darling service can connect with this viewer auth mode (Windows or SQL).</summary>
    public static bool IsSupported(string? authenticationType) => MapAuth(authenticationType) is not null;

    /// <summary>True when the mapped store auth needs a per-server secret (SQL only).</summary>
    public static bool RequiresSecret(string? authenticationType) =>
        MapAuth(authenticationType) == Sql;

    /// <summary>
    /// The user-facing message shown when an Azure/Entra auth mode is chosen (service can't honor it).
    ///
    /// <para>States the two modes that ARE honored rather than listing the ones that are not, because
    /// the list of Azure modes grows: this message named three of them and was already a mode behind by
    /// the time <see cref="AuthenticationTypes.EntraDefaultCredential"/> shipped, and would have been two
    /// behind after <see cref="AuthenticationTypes.EntraDeviceCode"/>. The whitelist in
    /// <see cref="MapAuth"/> is the thing that stays correct as modes are added; the message now says the
    /// same thing the whitelist says instead of restating its complement.</para>
    /// </summary>
    public const string UnsupportedAuthMessage =
        "The Darling service connects with Windows (integrated) or SQL authentication only — it acquires " +
        "no Azure tokens at all, so no Microsoft Entra mode is supported by its collection path yet. " +
        "Choose Windows or SQL authentication, or a SQL credential profile.";
}
