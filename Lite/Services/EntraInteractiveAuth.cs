/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Makes <c>Authentication=ActiveDirectoryInteractive</c> (Microsoft Entra MFA) work by giving MSAL a
/// parent window handle (#2184).
///
/// <para>Why this has to exist: current <c>Microsoft.Data.SqlClient</c> (7.0.2 here) routes interactive
/// Entra auth through the Windows <b>WAM broker</b>, and WAM requires the calling application to supply
/// the HWND that will own its account picker. An application that never supplies one does not get a
/// prompt — it gets <c>0xwindow_handle_required</c> / "A window handle must be configured" and the
/// connection fails outright. Lite set the authentication mode in
/// <see cref="Models.ServerConnection.ApplyAuthentication"/> but never registered a provider, so Entra
/// MFA was broken for every user on a current build, not for some particular tenant. Old builds worked
/// because they predate WAM being SqlClient's default and fell back to a browser.</para>
///
/// <para>Registration is process-wide. <see cref="SqlAuthenticationProvider.SetProvider"/> installs
/// against the authentication METHOD, not a connection, so one call at startup covers every
/// <c>SqlConnection</c> the process opens — the Add/Edit dialog's Test Connection and every collector
/// loop — without threading a handle through call sites that could each forget it.</para>
///
/// <para>Same seam as Performance Studio's fix
/// (<see href="https://github.com/erikdarlingdata/PerformanceStudio/pull/426">PerformanceStudio#426</see>),
/// which was verified against a real Entra-MFA tenant by the reporter of #2184; only the handle source
/// differs (WPF's <c>WindowInteropHelper</c> here vs Avalonia's platform handle there).</para>
/// </summary>
public static class EntraInteractiveAuth
{
    private static readonly object Gate = new();
    private static bool _registered;

    /// <summary>
    /// Registers the interactive-auth provider, resolving the parent window through
    /// <paramref name="parentWindowHandleProvider"/> at the moment MSAL asks for it.
    ///
    /// <para>The handle is fetched per prompt rather than captured once, deliberately: a window's HWND
    /// does not exist until the window is sourced, and the right parent is whichever window is actually
    /// in front when a connection fires — usually the Add/Edit Server dialog, not the main window
    /// behind it.</para>
    /// </summary>
    /// <param name="parentWindowHandleProvider">
    /// Returns the owning window handle, or <see cref="IntPtr.Zero"/> when no window is available.
    /// Zero degrades to MSAL's normal no-handle failure rather than a crash.
    /// </param>
    /// <returns>True if this call registered the provider; false if already registered.</returns>
    public static bool Register(Func<IntPtr> parentWindowHandleProvider)
    {
        ArgumentNullException.ThrowIfNull(parentWindowHandleProvider);

        lock (Gate)
        {
            if (_registered)
                return false;

            var provider = new ActiveDirectoryAuthenticationProvider();

            /* Func<object> rather than Func<IntPtr> is MSAL's shape — it takes an Android Activity on
               mobile and an HWND on Windows. Boxing the IntPtr is the intended usage. */
            provider.SetParentActivityOrWindowFunc(() => parentWindowHandleProvider());

            SqlAuthenticationProvider.SetProvider(SqlAuthenticationMethod.ActiveDirectoryInteractive, provider);

            _registered = true;
            return true;
        }
    }

    /* Test-only escape hatch for the one-way registration flag. Registration is deliberately
       irreversible in production — SqlAuthenticationProvider offers no unregister — so a test that
       needs to observe registration order resets the flag rather than the provider. The provider may
       stay registered with SqlClient; tests open no interactive connections, so that is inert. */
    internal static void ResetRegistrationForTests()
    {
        lock (Gate)
        {
            _registered = false;
        }
    }
}
